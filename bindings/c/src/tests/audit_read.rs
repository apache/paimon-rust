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
use crate::audit_read::*;
use arrow_array::{Int64Array, Int8Array};

fn audit_table(sequence: bool) -> Table {
    let path = format!("memory:/ffi_audit_{sequence}");
    let io = memory_file_io();
    setup_table_dirs(&io, &path);
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("write-only", "true")
        .option("table-read.sequence-number.enabled", sequence.to_string())
        .build()
        .unwrap();
    let table = Table::new(
        io,
        Identifier::new("default", "audit"),
        path,
        TableSchema::new(0, &schema),
        None,
    );
    write_data_rust(&table, &[make_batch(vec![1, 2], vec!["old", "keep"])]);
    let batch = make_batch(vec![1, 3], vec!["deleted", "added"]);
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(ArrowField::new(
        "_VALUE_KIND",
        ArrowDataType::Int8,
        false,
    )));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int8Array::from(vec![3, 0])));
    write_data_rust(
        &table,
        &[RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap()],
    );
    table
}

unsafe fn batches(reader: *mut paimon_record_batch_reader) -> Vec<RecordBatch> {
    let mut output = Vec::new();
    loop {
        let next = paimon_record_batch_reader_next(reader);
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
        output.push(RecordBatch::from(StructArray::from(data)));
    }
    paimon_record_batch_reader_free(reader);
    output
}

#[test]
fn current_audit_retains_deletes_and_owns_its_stream_and_schema() {
    for sequence in [false, true] {
        unsafe {
            let table = wrap_table(audit_table(sequence));
            assert_eq!(
                read_rows_ffi(table),
                vec![(2, "keep".into()), (3, "added".into())]
            );
            let mut schema = FFI_ArrowSchema::empty();
            assert!(paimon_table_export_audit_arrow_schema(
                table,
                (&mut schema as *mut FFI_ArrowSchema).cast()
            )
            .is_null());
            let duplicate = paimon_table_export_audit_arrow_schema(
                table,
                (&mut schema as *mut FFI_ArrowSchema).cast(),
            );
            assert!(!duplicate.is_null());
            paimon_error_free(duplicate);
            let builder = paimon_table_new_read_builder(table);
            assert!(builder.error.is_null());
            // A read limit must not prune away the winning tombstone's merge group.
            assert!(paimon_read_builder_with_limit(builder.read_builder, 1).is_null());
            let plan = paimon_read_builder_plan_audit(builder.read_builder);
            assert!(plan.error.is_null());
            let read = paimon_read_builder_new_read(builder.read_builder);
            assert!(read.error.is_null());
            let stream = paimon_table_read_to_audit_log_arrow(read.read, plan.plan, 0, usize::MAX);
            assert!(stream.error.is_null());
            paimon_table_read_free(read.read);
            paimon_plan_free(plan.plan);
            paimon_read_builder_free(builder.read_builder);
            unwrap_table(table);

            let expected_schema = ArrowSchema::try_from(&schema).unwrap();
            let output = batches(stream.reader);
            let mut rows = Vec::new();
            for batch in output {
                assert_eq!(batch.schema().as_ref(), &expected_schema);
                let kinds = batch
                    .column_by_name("rowkind")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let ids = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let names = batch
                    .column_by_name("name")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                if sequence {
                    let seq = batch
                        .column_by_name("_SEQUENCE_NUMBER")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    assert_eq!(seq.null_count(), 0);
                }
                for row in 0..batch.num_rows() {
                    rows.push((
                        ids.value(row),
                        kinds.value(row).to_string(),
                        names.value(row).to_string(),
                    ));
                }
            }
            rows.sort();
            assert_eq!(
                rows,
                vec![
                    (1, "-D".into(), "deleted".into()),
                    (2, "+I".into(), "keep".into()),
                    (3, "+I".into(), "added".into())
                ]
            );
        }
    }
}

#[test]
fn current_audit_executes_imported_splits_and_system_only_projection() {
    unsafe {
        let table = wrap_table(audit_table(false));
        let builder = paimon_table_new_read_builder(table).read_builder;
        let plan = paimon_read_builder_plan_audit(builder);
        assert!(plan.error.is_null());
        let core_plan = &*((*plan.plan).inner as *const paimon::Plan);
        assert_eq!(core_plan.splits().len(), 1);
        let encoded = core_plan.splits()[0].serialize().unwrap();
        let imported = paimon_plan_from_split_bytes(encoded.as_ptr(), encoded.len());
        assert!(imported.error.is_null());
        drop(encoded);
        let empty_projection = [ptr::null()];
        assert!(paimon_read_builder_with_projection(builder, empty_projection.as_ptr()).is_null());
        let read = paimon_read_builder_new_read(builder).read;
        for (offset, length) in [(0, 0), (usize::MAX, usize::MAX)] {
            let result = paimon_table_read_to_audit_log_arrow(read, imported.plan, offset, length);
            assert!(result.error.is_null());
            assert!(batches(result.reader).is_empty());
        }
        let result = paimon_table_read_to_audit_log_arrow(read, imported.plan, 0, 1);
        assert!(result.error.is_null());
        let output = batches(result.reader);
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert!(output
            .iter()
            .all(|batch| batch.num_columns() == 1 && batch.schema().field(0).name() == "rowkind"));
        paimon_table_read_free(read);
        paimon_plan_free(imported.plan);
        paimon_plan_free(plan.plan);
        paimon_read_builder_free(builder);
        unwrap_table(table);
    }
}

#[test]
fn current_audit_applies_value_filters_after_merge_and_resolves_projection_case() {
    unsafe {
        let table = wrap_table(audit_table(false));
        for (name, expected_count) in [("old", 0), ("deleted", 1)] {
            let builder = paimon_table_new_read_builder(table).read_builder;
            assert!(paimon_read_builder_with_case_sensitive(builder, false).is_null());
            let column = CString::new("NAME").unwrap();
            let projection = [column.as_ptr(), ptr::null()];
            assert!(paimon_read_builder_with_projection(builder, projection.as_ptr()).is_null());
            let filter_column = CString::new("name").unwrap();
            let datum = paimon_datum {
                tag: 7,
                int_val: 0,
                double_val: 0.0,
                str_data: name.as_ptr(),
                str_len: name.len(),
                int_val2: 0,
                uint_val: 0,
                uint_val2: 0,
            };
            let predicate = paimon_predicate_equal(table, filter_column.as_ptr(), datum);
            assert!(predicate.error.is_null());
            assert!(paimon_read_builder_with_filter(builder, predicate.predicate).is_null());
            let plan = paimon_read_builder_plan_audit(builder);
            assert!(plan.error.is_null());
            let read = paimon_read_builder_new_read(builder);
            assert!(read.error.is_null());
            let stream = paimon_table_read_to_audit_log_arrow(read.read, plan.plan, 0, usize::MAX);
            assert!(stream.error.is_null());
            let output = batches(stream.reader);
            assert_eq!(
                output.iter().map(RecordBatch::num_rows).sum::<usize>(),
                expected_count
            );
            for batch in output {
                assert_eq!(batch.num_columns(), 2);
                let kinds = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let names = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    assert_eq!(kinds.value(row), "-D");
                    assert_eq!(names.value(row), name);
                }
            }
            paimon_table_read_free(read.read);
            paimon_plan_free(plan.plan);
            paimon_read_builder_free(builder);
        }
        unwrap_table(table);
    }
}

#[test]
fn current_audit_reports_null_arguments_and_accepts_empty_tables() {
    unsafe {
        let plan = paimon_read_builder_plan_audit(ptr::null());
        assert!(!plan.error.is_null());
        assert!(plan.plan.is_null());
        paimon_error_free(plan.error);
        let stream = paimon_table_read_to_audit_log_arrow(ptr::null(), ptr::null(), 0, 1);
        assert!(!stream.error.is_null());
        assert!(stream.reader.is_null());
        paimon_error_free(stream.error);
        let mut schema = FFI_ArrowSchema::empty();
        let error = paimon_table_export_audit_arrow_schema(
            ptr::null(),
            (&mut schema as *mut FFI_ArrowSchema).cast(),
        );
        assert!(!error.is_null());
        assert!(schema.release.is_none());
        paimon_error_free(error);

        let table = wrap_table(Table::new(
            memory_file_io(),
            Identifier::new("default", "empty"),
            "memory:/ffi_audit_empty".into(),
            simple_table_schema(),
            None,
        ));
        let builder = paimon_table_new_read_builder(table).read_builder;
        let plan = paimon_read_builder_plan_audit(builder);
        assert!(plan.error.is_null());
        assert_eq!(paimon_plan_num_splits(plan.plan), 0);
        let read = paimon_read_builder_new_read(builder).read;
        let missing = paimon_table_read_to_audit_log_arrow(read, ptr::null(), 0, 1);
        assert!(!missing.error.is_null());
        paimon_error_free(missing.error);
        let result = paimon_table_read_to_audit_log_arrow(read, plan.plan, 0, usize::MAX);
        assert!(result.error.is_null());
        assert!(batches(result.reader).is_empty());
        paimon_table_read_free(read);
        paimon_plan_free(plan.plan);
        paimon_read_builder_free(builder);
        unwrap_table(table);
    }
}
