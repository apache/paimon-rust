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

//! Tests for the paimon-c FFI bindings.
//!
//! Covers: read path (table, scan, plan, predicates, record batch streaming),
//! write path (write builder, write_arrow_batch, prepare_commit, commit,
//! overwrite, truncate, abort), and full write->read roundtrip.
//!
//! IMPORTANT: C FFI functions internally use `runtime().block_on()`. Tests
//! must NOT wrap C FFI calls inside another `block_on`. Use the global
//! runtime only for Rust-API setup (write_data_rust, setup_table_dirs).

use std::ffi::{c_void, CString};
use std::mem::ManuallyDrop;
use std::process::Command;
use std::ptr;
use std::sync::Arc;

use arrow::buffer::NullBuffer;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, Int32Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use paimon::catalog::Identifier;
use paimon::io::FileIOBuilder;
use paimon::spec::{DataType, IntType, Schema, TableSchema, VarCharType};
use paimon::table::{SnapshotManager, Table};

use crate::error::*;
use crate::table::*;
use crate::types::*;
use crate::write::*;

// =========================================================================
//  Helpers
// =========================================================================

fn memory_file_io() -> paimon::io::FileIO {
    FileIOBuilder::new("memory").build().unwrap()
}

fn simple_table_schema() -> TableSchema {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .build()
        .unwrap();
    TableSchema::new(0, &schema)
}

fn not_null_table_schema() -> TableSchema {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::with_nullable(false)))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .build()
        .unwrap();
    TableSchema::new(0, &schema)
}

unsafe fn wrap_table(table: Table) -> *mut paimon_table {
    let inner = Box::into_raw(Box::new(table)) as *mut c_void;
    Box::into_raw(Box::new(paimon_table { inner }))
}

unsafe fn unwrap_table(table: *mut paimon_table) {
    let wrapper = Box::from_raw(table);
    if !wrapper.inner.is_null() {
        drop(Box::from_raw(wrapper.inner as *mut Table));
    }
}

fn make_batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

fn make_type_mismatch_batch(ids: Vec<&str>, names: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Utf8, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

fn make_nullable_id_batch(ids: Vec<Option<i32>>, names: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, true),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

fn export_batch_to_ffi(
    batch: RecordBatch,
) -> (
    Box<ManuallyDrop<FFI_ArrowArray>>,
    Box<ManuallyDrop<FFI_ArrowSchema>>,
) {
    let struct_array = StructArray::from(batch);
    let data = struct_array.to_data();
    let ffi_array = FFI_ArrowArray::new(&data);
    let ffi_schema = FFI_ArrowSchema::try_from(data.data_type()).unwrap();
    (
        Box::new(ManuallyDrop::new(ffi_array)),
        Box::new(ManuallyDrop::new(ffi_schema)),
    )
}

fn export_array_to_ffi(
    array: &dyn Array,
) -> (
    Box<ManuallyDrop<FFI_ArrowArray>>,
    Box<ManuallyDrop<FFI_ArrowSchema>>,
) {
    let data = array.to_data();
    (
        Box::new(ManuallyDrop::new(FFI_ArrowArray::new(&data))),
        Box::new(ManuallyDrop::new(
            FFI_ArrowSchema::try_from(data.data_type()).unwrap(),
        )),
    )
}

fn run_current_test_in_child(test_name: &str, env_name: &str, env_value: &str) -> bool {
    Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(env_name, env_value)
        .status()
        .unwrap()
        .success()
}

/// Use Rust API to write data (runs on global runtime via block_on).
fn write_data_rust(table: &Table, batches: &[RecordBatch]) {
    crate::runtime().block_on(async {
        let wb = table.new_write_builder();
        let mut tw = wb.new_write().unwrap();
        for batch in batches {
            tw.write_arrow_batch(batch).await.unwrap();
        }
        wb.new_commit()
            .commit(tw.prepare_commit().await.unwrap())
            .await
            .unwrap();
    });
}

/// Create directories needed by paimon (runs on global runtime via block_on).
fn setup_table_dirs(file_io: &paimon::io::FileIO, path: &str) {
    crate::runtime().block_on(async {
        file_io.mkdirs(&format!("{path}/snapshot/")).await.unwrap();
        file_io.mkdirs(&format!("{path}/manifest/")).await.unwrap();
    });
}

/// Collect (id, name) rows from a C FFI record batch reader.
/// Called OUTSIDE of any block_on — the C FFI functions use block_on internally.
unsafe fn collect_rows(reader: *mut paimon_record_batch_reader) -> Vec<(i32, String)> {
    let mut rows = Vec::new();
    loop {
        let result = paimon_record_batch_reader_next(reader);
        assert!(result.error.is_null(), "reader_next should not error");
        if result.batch.array.is_null() {
            // End of stream — still need to free the empty batch structs
            break;
        }

        // Take ownership of FFI structs via ptr::read (bitwise copy).
        let ffi_array = ptr::read(result.batch.array as *const FFI_ArrowArray);
        let ffi_schema = ptr::read(result.batch.schema as *const FFI_ArrowSchema);

        // from_ffi consumes ffi_array (by value), borrows ffi_schema.
        let data = arrow_array::ffi::from_ffi(ffi_array, &ffi_schema).unwrap();

        // Zero out the original Box allocations so release is a no-op.
        // The data ownership was transferred to `data` via from_ffi.
        ptr::write(
            result.batch.array as *mut FFI_ArrowArray,
            FFI_ArrowArray::empty(),
        );
        ptr::write(
            result.batch.schema as *mut FFI_ArrowSchema,
            FFI_ArrowSchema::empty(),
        );
        paimon_arrow_batch_free(result.batch);

        let struct_array = StructArray::from(data);
        let batch = RecordBatch::from(struct_array);

        let id_arr = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let name_arr = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let name = if name_arr.is_null(i) {
                String::new()
            } else {
                name_arr.value(i).to_string()
            };
            rows.push((id_arr.value(i), name));
        }

        // ffi_schema drops here — its release frees schema-owned memory
        // (format string, private_data), which is correct per the C Data
        // Interface spec.
    }
    rows.sort_by_key(|r| r.0);
    rows
}

/// Full read via C FFI: read_builder -> scan -> plan -> read -> stream -> rows.
/// Called OUTSIDE of any block_on — the C FFI functions use block_on internally.
unsafe fn read_rows_ffi(table: *const paimon_table) -> Vec<(i32, String)> {
    let rb_result = paimon_table_new_read_builder(table);
    assert!(rb_result.error.is_null());
    let rb = rb_result.read_builder;

    let scan_result = paimon_read_builder_new_scan(rb);
    assert!(scan_result.error.is_null());
    let scan = scan_result.scan;

    let plan_result = paimon_table_scan_plan(scan);
    assert!(plan_result.error.is_null());
    let plan = plan_result.plan;

    let read_result = paimon_read_builder_new_read(rb);
    assert!(read_result.error.is_null());
    let read = read_result.read;

    let reader_result = paimon_table_read_to_arrow(read, plan, 0, usize::MAX);
    assert!(reader_result.error.is_null());
    let reader = reader_result.reader;

    let rows = collect_rows(reader);

    paimon_record_batch_reader_free(reader);
    paimon_table_read_free(read);
    paimon_plan_free(plan);
    paimon_table_scan_free(scan);
    paimon_read_builder_free(rb);

    rows
}

// =========================================================================
//  Read path tests
// =========================================================================

#[test]
fn test_read_empty_table() {
    let path = "memory:/test_read_empty";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io.clone(),
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };
    let rows = unsafe { read_rows_ffi(handle) };
    assert!(rows.is_empty());
    unsafe { unwrap_table(handle) };
}

#[test]
fn test_read_with_data() {
    let path = "memory:/test_read_with_data";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io.clone(),
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    write_data_rust(&table, &[make_batch(vec![1, 2, 3], vec!["a", "b", "c"])]);
    let handle = unsafe { wrap_table(table) };
    let rows = unsafe { read_rows_ffi(handle) };
    assert_eq!(
        rows,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())]
    );
    unsafe { unwrap_table(handle) };
}

#[test]
fn test_read_with_projection() {
    let path = "memory:/test_read_proj";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io.clone(),
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    write_data_rust(&table, &[make_batch(vec![1, 2], vec!["x", "y"])]);
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let rb_result = paimon_table_new_read_builder(handle);
        assert!(rb_result.error.is_null());
        let rb = rb_result.read_builder;

        let col = CString::new("id").unwrap();
        let cols = [col.as_ptr(), ptr::null()];
        let err = paimon_read_builder_with_projection(rb, cols.as_ptr());
        assert!(err.is_null());

        let scan_result = paimon_read_builder_new_scan(rb);
        assert!(scan_result.error.is_null());
        let scan = scan_result.scan;

        let plan_result = paimon_table_scan_plan(scan);
        assert!(plan_result.error.is_null());
        let plan = plan_result.plan;

        let read_result = paimon_read_builder_new_read(rb);
        assert!(read_result.error.is_null());
        let read = read_result.read;

        let reader_result = paimon_table_read_to_arrow(read, plan, 0, usize::MAX);
        assert!(reader_result.error.is_null());
        let reader = reader_result.reader;

        let result = paimon_record_batch_reader_next(reader);
        assert!(result.error.is_null());
        assert!(!result.batch.array.is_null());

        let ffi_schema = ptr::read(result.batch.schema as *const FFI_ArrowSchema);
        let arrow_schema: ArrowSchema = (&ffi_schema).try_into().unwrap();
        assert_eq!(arrow_schema.fields().len(), 1);
        assert_eq!(arrow_schema.field(0).name(), "id");
        std::mem::forget(ffi_schema);

        paimon_arrow_batch_free(result.batch);

        paimon_record_batch_reader_free(reader);
        paimon_table_read_free(read);
        paimon_plan_free(plan);
        paimon_table_scan_free(scan);
        paimon_read_builder_free(rb);
    }

    unsafe { unwrap_table(handle) };
}

// =========================================================================
//  Predicate tests
// =========================================================================

unsafe fn build_predicate_equal(
    table: *const paimon_table,
    column: &str,
    int_val: i32,
) -> *mut paimon_predicate {
    let col = CString::new(column).unwrap();
    let datum = paimon_datum {
        tag: 3,
        int_val: int_val as i64,
        double_val: 0.0,
        str_data: ptr::null(),
        str_len: 0,
        int_val2: 0,
        uint_val: 0,
        uint_val2: 0,
    };
    let result = paimon_predicate_equal(table, col.as_ptr(), datum);
    assert!(result.error.is_null());
    result.predicate
}

#[test]
fn test_predicate_basics() {
    let path = "memory:/test_predicate_basics";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io.clone(),
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    write_data_rust(&table, &[make_batch(vec![1, 2, 3], vec!["a", "b", "c"])]);
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let pred = build_predicate_equal(handle, "id", 1);
        assert!(!pred.is_null());
        paimon_predicate_free(pred);

        let col = CString::new("name").unwrap();
        let nn_result = paimon_predicate_is_not_null(handle, col.as_ptr());
        assert!(nn_result.error.is_null());
        paimon_predicate_free(nn_result.predicate);

        let idcol = CString::new("id").unwrap();
        let datum1 = paimon_datum {
            tag: 3,
            int_val: 1,
            double_val: 0.0,
            str_data: ptr::null(),
            str_len: 0,
            int_val2: 0,
            uint_val: 0,
            uint_val2: 0,
        };
        let datum2 = paimon_datum {
            tag: 3,
            int_val: 2,
            double_val: 0.0,
            str_data: ptr::null(),
            str_len: 0,
            int_val2: 0,
            uint_val: 0,
            uint_val2: 0,
        };
        let datums = [datum1, datum2];
        let in_result = paimon_predicate_is_in(handle, idcol.as_ptr(), datums.as_ptr(), 2);
        assert!(in_result.error.is_null());
        paimon_predicate_free(in_result.predicate);
    }

    unsafe { unwrap_table(handle) };
}

#[test]
fn test_predicate_scan_filter() {
    let path = "memory:/test_predicate_filter";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io.clone(),
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    write_data_rust(
        &table,
        &[make_batch(vec![1, 2, 3, 4], vec!["a", "b", "c", "d"])],
    );
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let rb_result = paimon_table_new_read_builder(handle);
        assert!(rb_result.error.is_null());
        let rb = rb_result.read_builder;

        let col = CString::new("id").unwrap();
        let datum = paimon_datum {
            tag: 3,
            int_val: 3,
            double_val: 0.0,
            str_data: ptr::null(),
            str_len: 0,
            int_val2: 0,
            uint_val: 0,
            uint_val2: 0,
        };
        let pred_result = paimon_predicate_less_than(handle, col.as_ptr(), datum);
        assert!(pred_result.error.is_null());

        let err = paimon_read_builder_with_filter(rb, pred_result.predicate);
        assert!(err.is_null());

        let scan_result = paimon_read_builder_new_scan(rb);
        assert!(scan_result.error.is_null());
        let scan = scan_result.scan;

        let plan_result = paimon_table_scan_plan(scan);
        assert!(plan_result.error.is_null());
        let plan = plan_result.plan;

        let read_result = paimon_read_builder_new_read(rb);
        assert!(read_result.error.is_null());
        let read = read_result.read;

        let reader_result = paimon_table_read_to_arrow(read, plan, 0, usize::MAX);
        assert!(reader_result.error.is_null());
        let reader = reader_result.reader;

        let rows = collect_rows(reader);
        assert_eq!(rows, vec![(1, "a".into()), (2, "b".into())]);

        paimon_record_batch_reader_free(reader);
        paimon_table_read_free(read);
        paimon_plan_free(plan);
        paimon_table_scan_free(scan);
        paimon_read_builder_free(rb);
    }

    unsafe { unwrap_table(handle) };
}

#[test]
fn test_predicate_and_or_not() {
    let path = "memory:/test_predicate_combinators";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io.clone(),
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    write_data_rust(&table, &[make_batch(vec![1, 2, 3], vec!["a", "b", "c"])]);
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let col = CString::new("id").unwrap();
        let datum1 = paimon_datum {
            tag: 3,
            int_val: 1,
            double_val: 0.0,
            str_data: ptr::null(),
            str_len: 0,
            int_val2: 0,
            uint_val: 0,
            uint_val2: 0,
        };
        let datum2 = paimon_datum {
            tag: 3,
            int_val: 3,
            double_val: 0.0,
            str_data: ptr::null(),
            str_len: 0,
            int_val2: 0,
            uint_val: 0,
            uint_val2: 0,
        };

        let p1 = paimon_predicate_greater_than(handle, col.as_ptr(), datum1);
        let p2 = paimon_predicate_less_than(handle, col.as_ptr(), datum2);
        assert!(p1.error.is_null() && p2.error.is_null());

        let p_and = paimon_predicate_and(p1.predicate, p2.predicate);
        assert!(!p_and.is_null());

        let rb_result = paimon_table_new_read_builder(handle);
        let rb = rb_result.read_builder;
        paimon_read_builder_with_filter(rb, p_and);

        let scan = paimon_read_builder_new_scan(rb);
        let plan = paimon_table_scan_plan(scan.scan);
        let read = paimon_read_builder_new_read(rb);
        let reader = paimon_table_read_to_arrow(read.read, plan.plan, 0, usize::MAX);

        let rows = collect_rows(reader.reader);
        assert_eq!(rows, vec![(2, "b".into())]);

        paimon_record_batch_reader_free(reader.reader);
        paimon_table_read_free(read.read);
        paimon_plan_free(plan.plan);
        paimon_table_scan_free(scan.scan);
        paimon_read_builder_free(rb);
    }

    unsafe { unwrap_table(handle) };
}

// =========================================================================
//  Write path tests
// =========================================================================

#[test]
fn test_write_new_builder_and_free() {
    let path = "memory:/test_write_builder";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };
    unsafe {
        let result = paimon_table_new_write_builder(handle);
        assert!(result.error.is_null());
        assert!(!result.write_builder.is_null());
        paimon_write_builder_free(result.write_builder);
        unwrap_table(handle);
    }
}

#[test]
fn test_write_commit_read_roundtrip() {
    let path = "memory:/test_write_roundtrip";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let wb_result = paimon_table_new_write_builder(handle);
        assert!(wb_result.error.is_null());
        let wb = wb_result.write_builder;

        let tw_result = paimon_write_builder_new_write(wb);
        assert!(tw_result.error.is_null());
        let tw = tw_result.write;

        let batch = make_batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        let (array_box, schema_box) = export_batch_to_ffi(batch);
        let array_ptr = (&**array_box) as *const FFI_ArrowArray as *mut c_void;
        let schema_ptr = (&**schema_box) as *const FFI_ArrowSchema as *mut c_void;

        let err = paimon_table_write_write_arrow_batch(tw, array_ptr, schema_ptr);
        assert!(err.is_null());

        let pc_result = paimon_table_write_prepare_commit(tw);
        assert!(pc_result.error.is_null());
        assert!(!pc_result.messages.is_null());

        let tc_result = paimon_write_builder_new_commit(wb);
        assert!(tc_result.error.is_null());
        let tc = tc_result.commit;

        let err = paimon_table_commit_commit(tc, pc_result.messages);
        assert!(err.is_null());
        paimon_commit_messages_free(pc_result.messages);

        let rows = read_rows_ffi(handle);
        assert_eq!(
            rows,
            vec![(1, "a".into()), (2, "b".into()), (3, "c".into())]
        );

        paimon_table_commit_free(tc);
        paimon_table_write_free(tw);
        paimon_write_builder_free(wb);
        unwrap_table(handle);
    }
}

#[test]
fn test_write_arrow_batch_moves_ffi_structs() {
    let path = "memory:/test_write_arrow_batch_moves_ffi_structs";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let wb = paimon_table_new_write_builder(handle).write_builder;
        let tw = paimon_write_builder_new_write(wb).write;
        let (array, schema) = export_batch_to_ffi(make_batch(vec![1], vec!["a"]));

        let err = paimon_table_write_write_arrow_batch(
            tw,
            (&**array) as *const FFI_ArrowArray as *mut c_void,
            (&**schema) as *const FFI_ArrowSchema as *mut c_void,
        );
        assert!(err.is_null());
        assert!(array.is_released(), "import must clear ArrowArray.release");
        assert!(
            schema.release.is_none(),
            "import must clear ArrowSchema.release"
        );

        paimon_table_write_free(tw);
        paimon_write_builder_free(wb);
        unwrap_table(handle);
    }
}

#[test]
fn test_write_arrow_batch_rejects_table_schema_mismatch() {
    let path = "memory:/test_write_arrow_batch_rejects_table_schema_mismatch";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let wb = paimon_table_new_write_builder(handle).write_builder;
        let tw = paimon_write_builder_new_write(wb).write;
        let (array, schema) = export_batch_to_ffi(make_type_mismatch_batch(vec!["1"], vec!["a"]));

        let err = paimon_table_write_write_arrow_batch(
            tw,
            (&**array) as *const FFI_ArrowArray as *mut c_void,
            (&**schema) as *const FFI_ArrowSchema as *mut c_void,
        );
        assert!(!err.is_null(), "schema mismatch must be rejected");
        paimon_error_free(err);

        paimon_table_write_free(tw);
        paimon_write_builder_free(wb);
        unwrap_table(handle);
    }
}

#[test]
fn test_write_arrow_batch_rejects_null_for_not_null_field() {
    let path = "memory:/test_write_arrow_batch_rejects_null_for_not_null_field";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        not_null_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let wb = paimon_table_new_write_builder(handle).write_builder;
        let tw = paimon_write_builder_new_write(wb).write;
        let (array, schema) = export_batch_to_ffi(make_nullable_id_batch(vec![None], vec!["a"]));

        let err = paimon_table_write_write_arrow_batch(
            tw,
            (&**array) as *const FFI_ArrowArray as *mut c_void,
            (&**schema) as *const FFI_ArrowSchema as *mut c_void,
        );
        assert!(!err.is_null(), "NULL must be rejected for a NOT NULL field");
        paimon_error_free(err);

        paimon_table_write_free(tw);
        paimon_write_builder_free(wb);
        unwrap_table(handle);
    }
}

#[test]
fn test_write_arrow_batch_rejects_invalid_root_arrays_without_aborting() {
    const CHILD_ENV: &str = "PAIMON_C_INVALID_ROOT_CHILD";
    if let Ok(mode) = std::env::var(CHILD_ENV) {
        let path = format!("memory:/test_invalid_root_{mode}");
        let file_io = memory_file_io();
        setup_table_dirs(&file_io, &path);
        let table = Table::new(
            file_io,
            Identifier::new("default", "test"),
            path,
            simple_table_schema(),
            None,
        );
        let handle = unsafe { wrap_table(table) };

        unsafe {
            let wb = paimon_table_new_write_builder(handle).write_builder;
            let tw = paimon_write_builder_new_write(wb).write;
            let (array, schema) = if mode == "non_struct" {
                let array = Int32Array::from(vec![1]);
                export_array_to_ffi(&array)
            } else {
                let fields =
                    vec![Arc::new(ArrowField::new("id", ArrowDataType::Int32, false))].into();
                let array = StructArray::new(
                    fields,
                    vec![Arc::new(Int32Array::from(vec![1]))],
                    Some(NullBuffer::new_null(1)),
                );
                export_array_to_ffi(&array)
            };
            let err = paimon_table_write_write_arrow_batch(
                tw,
                (&**array) as *const FFI_ArrowArray as *mut c_void,
                (&**schema) as *const FFI_ArrowSchema as *mut c_void,
            );
            assert!(!err.is_null(), "invalid root array must return an error");
            paimon_error_free(err);
            paimon_table_write_free(tw);
            paimon_write_builder_free(wb);
            unwrap_table(handle);
        }
        return;
    }

    for mode in ["non_struct", "nullable_struct"] {
        assert!(
            run_current_test_in_child(
                "tests::test_write_arrow_batch_rejects_invalid_root_arrays_without_aborting",
                CHILD_ENV,
                mode,
            ),
            "{mode} input must return an error instead of aborting the process"
        );
    }
}

#[test]
fn test_commit_rejects_messages_from_another_table() {
    let file_io = memory_file_io();
    let source_path = "memory:/test_commit_provenance_source";
    let target_path = "memory:/test_commit_provenance_target";
    setup_table_dirs(&file_io, source_path);
    setup_table_dirs(&file_io, target_path);
    let source = Table::new(
        file_io.clone(),
        Identifier::new("default", "source"),
        source_path.to_string(),
        simple_table_schema(),
        None,
    );
    let target = Table::new(
        file_io,
        Identifier::new("default", "target"),
        target_path.to_string(),
        simple_table_schema(),
        None,
    );
    let source_handle = unsafe { wrap_table(source) };
    let target_handle = unsafe { wrap_table(target) };

    unsafe {
        let source_wb = paimon_table_new_write_builder(source_handle).write_builder;
        let source_tw = paimon_write_builder_new_write(source_wb).write;
        let (array, schema) = export_batch_to_ffi(make_batch(vec![1], vec!["a"]));
        let err = paimon_table_write_write_arrow_batch(
            source_tw,
            (&**array) as *const FFI_ArrowArray as *mut c_void,
            (&**schema) as *const FFI_ArrowSchema as *mut c_void,
        );
        assert!(err.is_null());
        let messages = paimon_table_write_prepare_commit(source_tw).messages;

        let target_wb = paimon_table_new_write_builder(target_handle).write_builder;
        let target_commit = paimon_write_builder_new_commit(target_wb).commit;
        let err = paimon_table_commit_commit(target_commit, messages);
        assert!(
            !err.is_null(),
            "a committer must reject messages prepared for another table"
        );
        paimon_error_free(err);

        paimon_commit_messages_free(messages);
        paimon_table_commit_free(target_commit);
        paimon_write_builder_free(target_wb);
        paimon_table_write_free(source_tw);
        paimon_write_builder_free(source_wb);
        unwrap_table(target_handle);
        unwrap_table(source_handle);
    }
}

#[test]
fn test_commit_rejects_messages_from_different_builder_identity() {
    let path = "memory:/test_commit_builder_provenance";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let writer_wb = paimon_table_new_write_builder(handle).write_builder;
        let tw = paimon_write_builder_new_write(writer_wb).write;
        let (array, schema) = export_batch_to_ffi(make_batch(vec![1], vec!["a"]));
        let err = paimon_table_write_write_arrow_batch(
            tw,
            (&**array) as *const FFI_ArrowArray as *mut c_void,
            (&**schema) as *const FFI_ArrowSchema as *mut c_void,
        );
        assert!(err.is_null());
        let messages = paimon_table_write_prepare_commit(tw).messages;

        let other_wb = paimon_table_new_write_builder(handle).write_builder;
        let wrong_commit = paimon_write_builder_new_commit(other_wb).commit;
        let err = paimon_table_commit_commit(wrong_commit, messages);
        assert!(
            !err.is_null(),
            "messages from another commit_user must be rejected"
        );
        paimon_error_free(err);

        let correct_commit = paimon_write_builder_new_commit(writer_wb).commit;
        let err = paimon_table_commit_commit(correct_commit, messages);
        assert!(err.is_null(), "rejected messages must remain reusable");

        paimon_commit_messages_free(messages);
        paimon_table_commit_free(correct_commit);
        paimon_table_commit_free(wrong_commit);
        paimon_write_builder_free(other_wb);
        paimon_table_write_free(tw);
        paimon_write_builder_free(writer_wb);
        unwrap_table(handle);
    }
}

#[test]
fn test_commit_messages_live_until_explicit_free() {
    const CHILD_ENV: &str = "PAIMON_C_MESSAGES_LIFETIME_CHILD";
    if std::env::var_os(CHILD_ENV).is_some() {
        let path = "memory:/test_commit_messages_lifetime";
        let file_io = memory_file_io();
        setup_table_dirs(&file_io, path);
        let table = Table::new(
            file_io,
            Identifier::new("default", "test"),
            path.to_string(),
            simple_table_schema(),
            None,
        );
        let handle = unsafe { wrap_table(table) };

        unsafe {
            let wb = paimon_table_new_write_builder(handle).write_builder;
            let tw = paimon_write_builder_new_write(wb).write;
            let (array, schema) = export_batch_to_ffi(make_batch(vec![1], vec!["a"]));
            let err = paimon_table_write_write_arrow_batch(
                tw,
                (&**array) as *const FFI_ArrowArray as *mut c_void,
                (&**schema) as *const FFI_ArrowSchema as *mut c_void,
            );
            assert!(err.is_null());
            let messages = paimon_table_write_prepare_commit(tw).messages;
            let commit = paimon_write_builder_new_commit(wb).commit;
            let err = paimon_table_commit_commit(commit, messages);
            assert!(err.is_null());

            paimon_commit_messages_free(messages);
            paimon_table_commit_free(commit);
            paimon_table_write_free(tw);
            paimon_write_builder_free(wb);
            unwrap_table(handle);
        }
        return;
    }

    assert!(
        run_current_test_in_child(
            "tests::test_commit_messages_live_until_explicit_free",
            CHILD_ENV,
            "1",
        ),
        "commit must not destroy a handle that callers are required to free"
    );
}

#[test]
fn test_caller_supplied_commit_identity_is_shared_and_persisted() {
    let path = "memory:/test_caller_supplied_commit_identity";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io.clone(),
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };
    let commit_user = CString::new("doris-load-job-42").unwrap();

    unsafe {
        let writer_wb =
            paimon_table_new_write_builder_with_commit_user(handle, commit_user.as_ptr())
                .write_builder;
        let committer_wb =
            paimon_table_new_write_builder_with_commit_user(handle, commit_user.as_ptr())
                .write_builder;
        let tw = paimon_write_builder_new_write(writer_wb).write;
        let (array, schema) = export_batch_to_ffi(make_batch(vec![1], vec!["a"]));
        let err = paimon_table_write_write_arrow_batch(
            tw,
            (&**array) as *const FFI_ArrowArray as *mut c_void,
            (&**schema) as *const FFI_ArrowSchema as *mut c_void,
        );
        assert!(err.is_null());
        let messages = paimon_table_write_prepare_commit(tw).messages;
        let commit = paimon_write_builder_new_commit(committer_wb).commit;
        let err = paimon_table_commit_commit_with_identifier(commit, messages, 42);
        assert!(err.is_null());

        let retry_wb =
            paimon_table_new_write_builder_with_commit_user(handle, commit_user.as_ptr())
                .write_builder;
        let retry_commit = paimon_write_builder_new_commit(retry_wb).commit;
        let err = paimon_table_commit_filter_and_commit_with_identifier(retry_commit, messages, 42);
        assert!(
            err.is_null(),
            "retrying the same identity must be idempotent"
        );

        paimon_commit_messages_free(messages);
        paimon_table_commit_free(retry_commit);
        paimon_write_builder_free(retry_wb);
        paimon_table_commit_free(commit);
        paimon_table_write_free(tw);
        paimon_write_builder_free(committer_wb);
        paimon_write_builder_free(writer_wb);
        unwrap_table(handle);
    }

    let snapshot = crate::runtime()
        .block_on(SnapshotManager::new(file_io, path.to_string()).get_latest_snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.commit_user(), "doris-load-job-42");
    assert_eq!(snapshot.commit_identifier(), 42);
    assert_eq!(snapshot.id(), 1, "retry must not create another snapshot");
}

#[test]
fn test_commit_messages_merge_preserves_all_writer_files() {
    let path = "memory:/test_commit_messages_merge";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };
    let commit_user = CString::new("distributed-job-7").unwrap();

    unsafe {
        let wb1 = paimon_table_new_write_builder_with_commit_user(handle, commit_user.as_ptr())
            .write_builder;
        let wb2 = paimon_table_new_write_builder_with_commit_user(handle, commit_user.as_ptr())
            .write_builder;
        let tw1 = paimon_write_builder_new_write(wb1).write;
        let tw2 = paimon_write_builder_new_write(wb2).write;

        for (tw, ids, names) in [(tw1, vec![1], vec!["a"]), (tw2, vec![2], vec!["b"])] {
            let (array, schema) = export_batch_to_ffi(make_batch(ids, names));
            let err = paimon_table_write_write_arrow_batch(
                tw,
                (&**array) as *const FFI_ArrowArray as *mut c_void,
                (&**schema) as *const FFI_ArrowSchema as *mut c_void,
            );
            assert!(err.is_null());
        }

        let messages1 = paimon_table_write_prepare_commit(tw1).messages;
        let messages2 = paimon_table_write_prepare_commit(tw2).messages;
        let err = paimon_commit_messages_merge(messages1, messages2);
        assert!(err.is_null());

        let commit = paimon_write_builder_new_commit(wb1).commit;
        let err = paimon_table_commit_commit_with_identifier(commit, messages1, 7);
        assert!(err.is_null());
        assert_eq!(
            read_rows_ffi(handle),
            vec![(1, "a".into()), (2, "b".into())]
        );

        paimon_commit_messages_free(messages2);
        paimon_commit_messages_free(messages1);
        paimon_table_commit_free(commit);
        paimon_table_write_free(tw2);
        paimon_table_write_free(tw1);
        paimon_write_builder_free(wb2);
        paimon_write_builder_free(wb1);
        unwrap_table(handle);
    }
}

#[test]
fn test_write_multiple_batches() {
    let path = "memory:/test_write_multi_batch";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let wb_result = paimon_table_new_write_builder(handle);
        let wb = wb_result.write_builder;

        let tw_result = paimon_write_builder_new_write(wb);
        let tw = tw_result.write;

        for (ids, names) in [(vec![1], vec!["a"]), (vec![2], vec!["b"])] {
            let batch = make_batch(ids, names);
            let (ab, sb) = export_batch_to_ffi(batch);
            let err = paimon_table_write_write_arrow_batch(
                tw,
                (&**ab) as *const FFI_ArrowArray as *mut c_void,
                (&**sb) as *const FFI_ArrowSchema as *mut c_void,
            );
            assert!(err.is_null());
        }

        let pc_result = paimon_table_write_prepare_commit(tw);
        assert!(pc_result.error.is_null());

        let tc_result = paimon_write_builder_new_commit(wb);
        let tc = tc_result.commit;
        let err = paimon_table_commit_commit(tc, pc_result.messages);
        assert!(err.is_null());
        paimon_commit_messages_free(pc_result.messages);

        let rows = read_rows_ffi(handle);
        assert_eq!(rows, vec![(1, "a".into()), (2, "b".into())]);

        paimon_table_commit_free(tc);
        paimon_table_write_free(tw);
        paimon_write_builder_free(wb);
        unwrap_table(handle);
    }
}

#[test]
fn test_commit_empty_messages_noop() {
    let path = "memory:/test_commit_empty";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let wb_result = paimon_table_new_write_builder(handle);
        let wb = wb_result.write_builder;

        let tw_result = paimon_write_builder_new_write(wb);
        let tw = tw_result.write;
        let pc_result = paimon_table_write_prepare_commit(tw);
        assert!(pc_result.error.is_null());
        assert!(!pc_result.messages.is_null());

        let tc_result = paimon_write_builder_new_commit(wb);
        let tc = tc_result.commit;
        let err = paimon_table_commit_commit(tc, pc_result.messages);
        assert!(err.is_null());
        paimon_commit_messages_free(pc_result.messages);

        let rows = read_rows_ffi(handle);
        assert!(rows.is_empty());

        paimon_table_commit_free(tc);
        paimon_table_write_free(tw);
        paimon_write_builder_free(wb);
        unwrap_table(handle);
    }
}

#[test]
fn test_write_overwrite_mode() {
    let path = "memory:/test_write_overwrite";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };

    unsafe {
        // First commit: write [1, 2]
        {
            let wb_result = paimon_table_new_write_builder(handle);
            let wb = wb_result.write_builder;
            let tw_result = paimon_write_builder_new_write(wb);
            let tw = tw_result.write;

            let batch = make_batch(vec![1, 2], vec!["a", "b"]);
            let (ab, sb) = export_batch_to_ffi(batch);
            paimon_table_write_write_arrow_batch(
                tw,
                (&**ab) as *const FFI_ArrowArray as *mut c_void,
                (&**sb) as *const FFI_ArrowSchema as *mut c_void,
            );

            let pc = paimon_table_write_prepare_commit(tw);
            let tc_result = paimon_write_builder_new_commit(wb);
            paimon_table_commit_commit(tc_result.commit, pc.messages);
            paimon_commit_messages_free(pc.messages);

            paimon_table_commit_free(tc_result.commit);
            paimon_table_write_free(tw);
            paimon_write_builder_free(wb);
        }

        // Second commit with overwrite: write [3, 4]
        {
            let wb_result = paimon_table_new_write_builder(handle);
            let wb = wb_result.write_builder;

            let err = paimon_write_builder_with_overwrite(wb);
            assert!(err.is_null());

            let tw_result = paimon_write_builder_new_write(wb);
            let tw = tw_result.write;

            let batch = make_batch(vec![3, 4], vec!["c", "d"]);
            let (ab, sb) = export_batch_to_ffi(batch);
            paimon_table_write_write_arrow_batch(
                tw,
                (&**ab) as *const FFI_ArrowArray as *mut c_void,
                (&**sb) as *const FFI_ArrowSchema as *mut c_void,
            );

            let pc = paimon_table_write_prepare_commit(tw);
            let tc_result = paimon_write_builder_new_commit(wb);
            paimon_table_commit_overwrite(tc_result.commit, pc.messages);
            paimon_commit_messages_free(pc.messages);

            paimon_table_commit_free(tc_result.commit);
            paimon_table_write_free(tw);
            paimon_write_builder_free(wb);
        }

        let rows = read_rows_ffi(handle);
        assert_eq!(rows, vec![(3, "c".into()), (4, "d".into())]);

        unwrap_table(handle);
    }
}

#[test]
fn test_truncate_table() {
    let path = "memory:/test_truncate";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };

    unsafe {
        // Write some data first
        {
            let wb_result = paimon_table_new_write_builder(handle);
            let wb = wb_result.write_builder;
            let tw_result = paimon_write_builder_new_write(wb);
            let tw = tw_result.write;
            let batch = make_batch(vec![1, 2], vec!["a", "b"]);
            let (ab, sb) = export_batch_to_ffi(batch);
            paimon_table_write_write_arrow_batch(
                tw,
                (&**ab) as *const FFI_ArrowArray as *mut c_void,
                (&**sb) as *const FFI_ArrowSchema as *mut c_void,
            );
            let pc = paimon_table_write_prepare_commit(tw);
            let tc_result = paimon_write_builder_new_commit(wb);
            paimon_table_commit_commit(tc_result.commit, pc.messages);
            paimon_commit_messages_free(pc.messages);
            paimon_table_commit_free(tc_result.commit);
            paimon_table_write_free(tw);
            paimon_write_builder_free(wb);
        }

        // Truncate
        {
            let wb_result = paimon_table_new_write_builder(handle);
            let wb = wb_result.write_builder;
            let tc_result = paimon_write_builder_new_commit(wb);
            let err = paimon_table_commit_truncate_table(tc_result.commit);
            assert!(err.is_null());
            paimon_table_commit_free(tc_result.commit);
            paimon_write_builder_free(wb);
        }

        let rows = read_rows_ffi(handle);
        assert!(rows.is_empty());

        unwrap_table(handle);
    }
}

#[test]
fn test_abort_commit() {
    let path = "memory:/test_abort";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let wb_result = paimon_table_new_write_builder(handle);
        let wb = wb_result.write_builder;

        let tw_result = paimon_write_builder_new_write(wb);
        let tw = tw_result.write;

        let batch = make_batch(vec![1], vec!["a"]);
        let (ab, sb) = export_batch_to_ffi(batch);
        let err = paimon_table_write_write_arrow_batch(
            tw,
            (&**ab) as *const FFI_ArrowArray as *mut c_void,
            (&**sb) as *const FFI_ArrowSchema as *mut c_void,
        );
        assert!(err.is_null());

        let pc_result = paimon_table_write_prepare_commit(tw);
        assert!(pc_result.error.is_null());

        let tc_result = paimon_write_builder_new_commit(wb);
        let tc = tc_result.commit;
        let err = paimon_table_commit_abort(tc, pc_result.messages);
        assert!(err.is_null());
        paimon_commit_messages_free(pc_result.messages);

        let rows = read_rows_ffi(handle);
        assert!(rows.is_empty());

        paimon_table_commit_free(tc);
        paimon_table_write_free(tw);
        paimon_write_builder_free(wb);
        unwrap_table(handle);
    }
}

#[test]
fn test_null_pointer_handling() {
    unsafe {
        let result = paimon_table_new_write_builder(ptr::null());
        assert!(!result.error.is_null());
        assert!(result.write_builder.is_null());
        paimon_error_free(result.error);

        let result = paimon_write_builder_new_write(ptr::null());
        assert!(!result.error.is_null());
        assert!(result.write.is_null());
        paimon_error_free(result.error);

        let result = paimon_write_builder_new_commit(ptr::null());
        assert!(!result.error.is_null());
        assert!(result.commit.is_null());
        paimon_error_free(result.error);

        let err =
            paimon_table_write_write_arrow_batch(ptr::null_mut(), ptr::null_mut(), ptr::null_mut());
        assert!(!err.is_null());
        paimon_error_free(err);

        let result = paimon_table_write_prepare_commit(ptr::null_mut());
        assert!(!result.error.is_null());
        assert!(result.messages.is_null());
        paimon_error_free(result.error);

        let err = paimon_table_commit_commit(ptr::null(), ptr::null_mut());
        assert!(!err.is_null());
        paimon_error_free(err);

        paimon_write_builder_free(ptr::null_mut());
        paimon_table_write_free(ptr::null_mut());
        paimon_table_commit_free(ptr::null_mut());
        paimon_commit_messages_free(ptr::null_mut());
    }
}

#[test]
fn test_two_commits_same_builder() {
    let path = "memory:/test_two_commits";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let wb_result = paimon_table_new_write_builder(handle);
        let wb = wb_result.write_builder;

        // First commit
        {
            let tw_result = paimon_write_builder_new_write(wb);
            let tw = tw_result.write;
            let batch = make_batch(vec![1], vec!["a"]);
            let (ab, sb) = export_batch_to_ffi(batch);
            paimon_table_write_write_arrow_batch(
                tw,
                (&**ab) as *const FFI_ArrowArray as *mut c_void,
                (&**sb) as *const FFI_ArrowSchema as *mut c_void,
            );
            let pc = paimon_table_write_prepare_commit(tw);
            let tc_result = paimon_write_builder_new_commit(wb);
            paimon_table_commit_commit(tc_result.commit, pc.messages);
            paimon_commit_messages_free(pc.messages);
            paimon_table_commit_free(tc_result.commit);
            paimon_table_write_free(tw);
        }

        // Second commit with same builder
        {
            let tw_result = paimon_write_builder_new_write(wb);
            let tw = tw_result.write;
            let batch = make_batch(vec![2], vec!["b"]);
            let (ab, sb) = export_batch_to_ffi(batch);
            paimon_table_write_write_arrow_batch(
                tw,
                (&**ab) as *const FFI_ArrowArray as *mut c_void,
                (&**sb) as *const FFI_ArrowSchema as *mut c_void,
            );
            let pc = paimon_table_write_prepare_commit(tw);
            let tc_result = paimon_write_builder_new_commit(wb);
            paimon_table_commit_commit(tc_result.commit, pc.messages);
            paimon_commit_messages_free(pc.messages);
            paimon_table_commit_free(tc_result.commit);
            paimon_table_write_free(tw);
        }

        paimon_write_builder_free(wb);

        let rows = read_rows_ffi(handle);
        assert_eq!(rows, vec![(1, "a".into()), (2, "b".into())]);

        unwrap_table(handle);
    }
}
