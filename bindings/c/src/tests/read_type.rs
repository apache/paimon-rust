// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use paimon::spec::{ArrayType, DataField, MapType, RowType};
use std::io::Cursor;

fn nested_schema() -> TableSchema {
    let row = DataType::Row(RowType::new(vec![
        DataField::new(0, "keep".into(), DataType::Int(IntType::new())),
        DataField::new(
            1,
            "drop".into(),
            DataType::VarChar(VarCharType::string_type()),
        ),
    ]));
    TableSchema::new(
        0,
        &Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("Row", row.clone())
            .column("array", DataType::Array(ArrayType::new(row.clone())))
            .column(
                "map",
                DataType::Map(MapType::new(
                    DataType::VarChar(
                        VarCharType::with_nullable(false, VarCharType::MAX_LENGTH).unwrap(),
                    ),
                    row,
                )),
            )
            .build()
            .unwrap(),
    )
}

fn prune(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Row(row) => DataType::Row(RowType::with_nullable(
            data_type.is_nullable(),
            vec![row.fields()[0].clone()],
        )),
        DataType::Array(array) => DataType::Array(ArrayType::with_nullable(
            data_type.is_nullable(),
            prune(array.element_type()),
        )),
        DataType::Map(map) => DataType::Map(MapType::with_nullable(
            data_type.is_nullable(),
            map.key_type().clone(),
            prune(map.value_type()),
        )),
        other => other.clone(),
    }
}

fn projected_fields(schema: &TableSchema) -> Vec<DataField> {
    // Reorder roots while preserving their IDs and selected nested field IDs.
    [3, 1, 2, 0]
        .into_iter()
        .map(|index| {
            let field = &schema.fields()[index];
            DataField::new(field.id(), field.name().into(), prune(field.data_type()))
        })
        .collect()
}

unsafe fn declared_read_type(builder: *mut paimon_read_builder) -> Vec<DataField> {
    let result = paimon_read_builder_new_read(builder);
    assert!(result.error.is_null());
    let state = &*((*result.read).inner as *const TableReadState);
    let fields = state.read_type.clone();
    paimon_table_read_free(result.read);
    fields
}

unsafe fn collect_batches(builder: *mut paimon_read_builder) -> Vec<RecordBatch> {
    let scan = paimon_read_builder_new_scan(builder);
    assert!(scan.error.is_null());
    let plan = paimon_table_scan_plan(scan.scan);
    assert!(plan.error.is_null());
    let read = paimon_read_builder_new_read(builder);
    assert!(read.error.is_null());
    let result = paimon_table_read_to_arrow(read.read, plan.plan, 0, usize::MAX);
    assert!(result.error.is_null());
    paimon_table_read_free(read.read);
    paimon_plan_free(plan.plan);
    paimon_table_scan_free(scan.scan);

    let mut batches = Vec::new();
    loop {
        let next = paimon_record_batch_reader_next(result.reader);
        assert!(next.error.is_null());
        if next.batch.array.is_null() {
            break;
        }
        let array = ptr::replace(
            next.batch.array.cast::<FFI_ArrowArray>(),
            FFI_ArrowArray::empty(),
        );
        let schema = ptr::replace(
            next.batch.schema.cast::<FFI_ArrowSchema>(),
            FFI_ArrowSchema::empty(),
        );
        let data = arrow_array::ffi::from_ffi(array, &schema).unwrap();
        paimon_arrow_batch_free(next.batch);
        batches.push(RecordBatch::from(StructArray::from(data)));
    }
    paimon_record_batch_reader_free(result.reader);
    batches
}

fn json_batch(fields: &[DataField], json: &str) -> RecordBatch {
    arrow::json::ReaderBuilder::new(paimon::arrow::build_target_arrow_schema(fields).unwrap())
        .build(Cursor::new(json))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
}

#[test]
fn structured_read_type_projects_nested_containers_through_c() {
    let schema = nested_schema();
    let projected = projected_fields(&schema);
    let json = r#"{"id":1,"Row":{"keep":7,"drop":"x"},"array":[{"keep":8,"drop":"y"},null],"map":{"k":{"keep":9,"drop":"z"}}}
{"id":2,"Row":null,"array":[],"map":{}}
{"id":3,"Row":{"keep":null,"drop":"unused"},"array":null,"map":null}
"#;
    let path = "memory:/ffi_nested_read_type";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let batch = json_batch(schema.fields(), json);
    let table = Table::new(
        file_io,
        Identifier::new("default", "nested"),
        path.into(),
        schema,
        None,
    );
    write_data_rust(&table, &[batch]);
    unsafe {
        let handle = wrap_table(table);
        let builder = paimon_table_new_read_builder(handle).read_builder;
        let json_type = CString::new(serde_json::to_string(&projected).unwrap()).unwrap();
        assert!(paimon_read_builder_with_read_type_json(builder, json_type.as_ptr()).is_null());
        drop(json_type);
        assert_eq!(declared_read_type(builder), projected);
        let batches = collect_batches(builder);
        assert_eq!(batches, vec![json_batch(&projected, json)]);

        let empty = CString::new("[]").unwrap();
        assert!(paimon_read_builder_with_read_type_json(builder, empty.as_ptr()).is_null());
        let batches = collect_batches(builder);
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert!(batches.iter().all(|batch| batch.num_columns() == 0));
        paimon_read_builder_free(builder);
        unwrap_table(handle);
    }
}

#[test]
fn structured_read_type_preserves_selection_on_errors_and_follows_core_precedence() {
    unsafe {
        let schema = nested_schema();
        let projected = projected_fields(&schema);
        let original = schema.fields().to_vec();
        let table = wrap_table(Table::new(
            memory_file_io(),
            Identifier::new("default", "nested"),
            "memory:/ffi_read_type_options".into(),
            schema,
            None,
        ));
        let builder = paimon_table_new_read_builder(table).read_builder;
        let json = CString::new(serde_json::to_string(&projected).unwrap()).unwrap();
        assert!(paimon_read_builder_with_read_type_json(builder, json.as_ptr()).is_null());
        for bytes in [b"{}\0".as_slice(), b"null\0", b"[\0", b"\xff\0"] {
            let error = paimon_read_builder_with_read_type_json(builder, bytes.as_ptr().cast());
            assert!(!error.is_null());
            assert_eq!((*error).code, PaimonErrorCode::InvalidInput as i32);
            paimon_error_free(error);
            assert_eq!(declared_read_type(builder), projected);
        }
        let error = paimon_read_builder_with_read_type_json(ptr::null_mut(), json.as_ptr());
        assert!(!error.is_null());
        paimon_error_free(error);

        let name = CString::new("id").unwrap();
        let names = [name.as_ptr(), ptr::null()];
        assert!(paimon_read_builder_with_projection(builder, names.as_ptr()).is_null());
        assert_eq!(declared_read_type(builder), vec![original[0].clone()]);
        assert!(paimon_read_builder_with_read_type_json(builder, json.as_ptr()).is_null());
        assert!(paimon_read_builder_with_case_sensitive(builder, false).is_null());
        assert_eq!(declared_read_type(builder), projected);
        assert!(paimon_read_builder_with_projection(builder, ptr::null()).is_null());
        assert_eq!(declared_read_type(builder), original);
        assert!(paimon_read_builder_with_projection(builder, names.as_ptr()).is_null());
        assert!(paimon_read_builder_with_read_type_json(builder, ptr::null()).is_null());
        assert_eq!(declared_read_type(builder), original);
        paimon_read_builder_free(builder);
        unwrap_table(table);
    }
}
