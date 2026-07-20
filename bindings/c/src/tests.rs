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
//! overwrite, truncate, abort), full write->read roundtrip, and vector search
//! materialized reads across primary-key and data-evolution (append) tables.
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
use crate::vector_search::*;
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

// =========================================================================
//  Vector search tests (materialized reads)
// =========================================================================
//
// Two storage shapes are exercised end-to-end through the C `execute_read`
// terminal, each compared against an independent core Rust
// `VectorSearchBuilder::execute_read()` reference:
//
//   * A primary-key vector table backed by a real vindex IVF-flat ANN segment
//     built in-process (bucket-local ANN search, residual filter supported).
//   * A data-evolution (append) vector table whose global index is produced by
//     the public `new_vindex_index_build_builder(...).execute()` path.
//
// Both fixtures live entirely on the in-memory FileIO, so no temp dirs or
// on-disk schema files are needed: the written data file keeps `schema_id == 0`,
// matching the table, so the read path never reloads a schema from disk.
//
// The materialized stream carries the user table columns plus a unified
// `__paimon_search_score` Float32 column; row order is best-first.

use std::collections::HashMap;

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, ListBuilder};
use arrow_array::{ArrayRef, Float32Array};
use bytes::Bytes;
use futures::TryStreamExt;
use paimon::io::FileIO;
use paimon::spec::{
    ArrayType, DataFileMeta, Datum, FloatType, GlobalIndexMeta, IndexFileMeta, Predicate,
    PredicateBuilder, VectorType,
};
use paimon::table::{CommitMessage, TableCommit};

use paimon_vindex_core::index::{VectorIndexConfig, VectorIndexTrainer, VectorIndexWriter};
use paimon_vindex_core::io::PosWriter;

/// Unified score column materialized by `execute_read` (Float32).
const SCORE_COLUMN: &str = "__paimon_search_score";
/// Vector dimension for the primary-key fixtures.
const PK_DIM: usize = 4;
/// Primary-key vector column name (shared by both storage fixtures).
const VECTOR_COLUMN: &str = "embedding";
/// vindex index type used for both the PK ANN segment and the DE global index.
const INDEX_TYPE: &str = "ivf-flat";

// --- Primary-key vector fixture ------------------------------------------

/// Table options routing searches into the primary-key vector branch. A single
/// bucket keeps one data file; `deletion-vectors.enabled` satisfies the residual
/// guard (`with_filter`) without any row actually being deleted.
fn pk_vector_options() -> Vec<(String, String)> {
    vec![
        ("bucket".to_string(), "1".to_string()),
        ("deletion-vectors.enabled".to_string(), "true".to_string()),
        (
            "pk-vector.index.columns".to_string(),
            VECTOR_COLUMN.to_string(),
        ),
        (
            format!("fields.{VECTOR_COLUMN}.pk-vector.index.type"),
            INDEX_TYPE.to_string(),
        ),
        (
            format!("fields.{VECTOR_COLUMN}.pk-vector.distance.metric"),
            "l2".to_string(),
        ),
    ]
}

/// Primary-key schema `(id INT PRIMARY KEY, embedding VECTOR<FLOAT>)`.
fn pk_vector_schema() -> TableSchema {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            VECTOR_COLUMN,
            DataType::Vector(
                VectorType::try_new(true, PK_DIM as u32, DataType::Float(FloatType::new()))
                    .unwrap(),
            ),
        )
        .primary_key(["id"]);
    for (k, v) in pk_vector_options() {
        builder = builder.option(k, v);
    }
    TableSchema::new(0, &builder.build().unwrap())
}

/// Arrow batch matching the PK schema: `id` (== physical position) plus a
/// `FixedSizeList<Float32>` vector column.
fn pk_data_batch(vectors: &[[f32; PK_DIM]]) -> RecordBatch {
    let ids: Vec<i32> = (0..vectors.len() as i32).collect();
    let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
    let mut vector_builder = FixedSizeListBuilder::new(Float32Builder::new(), PK_DIM as i32)
        .with_field(element_field.clone());
    for vector in vectors {
        for &value in vector {
            vector_builder.values().append_value(value);
        }
        vector_builder.append(true);
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new(
            VECTOR_COLUMN,
            ArrowDataType::FixedSizeList(element_field, PK_DIM as i32),
            true,
        ),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(vector_builder.finish()) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Encode one Java `DataOutput#writeUTF` value (u16-BE length + modified UTF-8),
/// as `PkVectorSourceMeta` expects.
fn java_write_utf(s: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for c in s.encode_utf16() {
        if (0x0001..=0x007F).contains(&c) {
            body.push(c as u8);
        } else if c > 0x07FF {
            body.push(0xE0 | (c >> 12) as u8);
            body.push(0x80 | ((c >> 6) & 0x3F) as u8);
            body.push(0x80 | (c & 0x3F) as u8);
        } else {
            body.push(0xC0 | (c >> 6) as u8);
            body.push(0x80 | (c & 0x3F) as u8);
        }
    }
    let mut out = (body.len() as u16).to_be_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

/// Assemble the `_SOURCE_META` frame the way Java `PkVectorSourceMeta` writes it:
/// `i32-BE version=1`, `i32-BE data_level`, `i32-BE count`, then per source file a
/// `writeUTF` name and an `i64-BE` row count.
fn source_meta_bytes(data_level: i32, files: &[(&str, i64)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&1i32.to_be_bytes());
    out.extend_from_slice(&data_level.to_be_bytes());
    out.extend_from_slice(&(files.len() as i32).to_be_bytes());
    for (name, rows) in files {
        out.extend_from_slice(&java_write_utf(name));
        out.extend_from_slice(&rows.to_be_bytes());
    }
    out
}

/// Build a real vindex IVF-flat ANN segment over `vectors` (label == physical
/// position) and write it into `{table}/index/{file_name}`. `nlist = 1` keeps the
/// single inverted list exhaustive, so the search is exact.
async fn write_ann_segment(
    file_io: &FileIO,
    table_location: &str,
    file_name: &str,
    vectors: &[[f32; PK_DIM]],
) -> u64 {
    let n = vectors.len();
    let flat: Vec<f32> = vectors.iter().flat_map(|v| v.iter().copied()).collect();
    let ids: Vec<i64> = (0..n as i64).collect();

    let native_options = HashMap::from([
        ("index.type".to_string(), "ivf_flat".to_string()),
        ("dimension".to_string(), PK_DIM.to_string()),
        ("nlist".to_string(), "1".to_string()),
        ("metric".to_string(), "l2".to_string()),
    ]);
    let config = VectorIndexConfig::from_options(&native_options).unwrap();
    let training = VectorIndexTrainer::train(config, &flat, n).unwrap();
    let mut writer = VectorIndexWriter::new(training);
    writer.add_vectors(&ids, &flat, n).unwrap();
    let mut bytes = Vec::new();
    {
        let mut output = PosWriter::new(&mut bytes);
        writer.write(&mut output).unwrap();
    }

    let index_dir = format!("{}/index", table_location.trim_end_matches('/'));
    file_io.mkdirs(&index_dir).await.unwrap();
    let index_path = format!("{index_dir}/{file_name}");
    let file_size = bytes.len() as u64;
    file_io
        .new_output(&index_path)
        .unwrap()
        .write(Bytes::from(bytes))
        .await
        .unwrap();
    file_size
}

/// Build a complete, self-contained primary-key vector table over `vectors` on
/// the in-memory FileIO: write a real data file, apply the two PK-vector
/// constraints to its meta (compacted, non-level-0; Java source-meta frame),
/// build+commit a real vindex ANN segment, and return the opened table.
fn build_pk_vector_table(path: &str, vectors: &[[f32; PK_DIM]]) -> Table {
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io.clone(),
        Identifier::new("default", "pkvector"),
        path.to_string(),
        pk_vector_schema(),
        None,
    );

    crate::runtime().block_on(async {
        // Write a real data file via the public write path to obtain a genuine
        // DataFileMeta (name, row count, stats, file size).
        let write_builder = table.new_write_builder();
        let mut writer = write_builder.new_write().unwrap();
        writer
            .write_arrow_batch(&pk_data_batch(vectors))
            .await
            .unwrap();
        let write_messages = writer.prepare_commit().await.unwrap();
        assert_eq!(write_messages.len(), 1, "single bucket -> one message");
        let written = &write_messages[0];
        assert_eq!(written.new_files.len(), 1, "single data file expected");
        let base_meta = written.new_files[0].clone();
        let bucket = written.bucket;
        let partition = written.partition.clone();
        let data_file_name = base_meta.file_name.clone();
        let row_count = base_meta.row_count;

        // Constraint 1: only a compacted, non-level-0 file backs the PK-vector
        // index. Pin first_row_id = 0 so global row id == physical position.
        let indexed_meta = DataFileMeta {
            level: 1,
            file_source: Some(1),
            first_row_id: Some(0),
            ..base_meta
        };

        // Build and persist the real vindex ANN segment.
        let index_file_name = "vector-ivf-flat-pk-c.index".to_string();
        let index_file_size = write_ann_segment(&file_io, path, &index_file_name, vectors).await;

        // Constraint 2: GlobalIndexMeta.source_meta is the Java PkVectorSourceMeta
        // frame naming the backing data file in ordinal order.
        let vector_field_id = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name() == VECTOR_COLUMN)
            .expect("vector field present")
            .id();
        let index_file = IndexFileMeta {
            index_type: INDEX_TYPE.to_string(),
            file_name: index_file_name,
            file_size: i32::try_from(index_file_size).unwrap(),
            row_count: i32::try_from(row_count).unwrap(),
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: 0,
                row_range_end: row_count - 1,
                index_field_id: vector_field_id,
                extra_field_ids: None,
                source_meta: Some(source_meta_bytes(
                    indexed_meta.level,
                    &[(&data_file_name, row_count)],
                )),
                index_meta: None,
            }),
        };

        // Commit the indexed data file together with the ANN segment.
        let mut message = CommitMessage::new(partition, bucket, vec![indexed_meta]);
        message.new_index_files = vec![index_file];
        TableCommit::new(table.clone(), "pkvector-c".to_string())
            .commit(vec![message])
            .await
            .unwrap();
    });

    table
}

/// Empty primary-key vector table (options set, no data): the PK branch resolves
/// but the plan is empty, so a search returns an empty (EOF) stream.
fn build_pk_vector_table_empty(path: &str) -> Table {
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    Table::new(
        file_io,
        Identifier::new("default", "pkvector_empty"),
        path.to_string(),
        pk_vector_schema(),
        None,
    )
}

/// Fixture: distances 1 < 41 < 67 < 181; top-3 = rows 0, 4, 5.
fn pk_fixture_smoke() -> ([f32; PK_DIM], Vec<[f32; PK_DIM]>) {
    let query = [9.0, 0.0, 0.0, 0.0];
    let vectors = vec![
        [10.0, 0.0, 0.0, 0.0],
        [0.0, 10.0, 0.0, 0.0],
        [0.0, 0.0, 10.0, 0.0],
        [0.0, 0.0, 0.0, 10.0],
        [5.0, 5.0, 0.0, 0.0],
        [1.0, 1.0, 1.0, 1.0],
    ];
    (query, vectors)
}

/// Residual fixture: unfiltered top-3 = [0, 1, 2]; with `id >= 3` the top-3
/// becomes [4, 5, 3], disjoint from the unfiltered set.
fn pk_fixture_residual() -> ([f32; PK_DIM], Vec<[f32; PK_DIM]>) {
    let query = [10.0, 0.0, 0.0, 0.0];
    let vectors = vec![
        [10.0, 0.0, 0.0, 0.0], // pos 0 -> 0
        [9.0, 0.0, 0.0, 0.0],  // pos 1 -> 1
        [8.0, 0.0, 0.0, 0.0],  // pos 2 -> 4
        [5.0, 0.0, 0.0, 0.0],  // pos 3 -> 25
        [7.0, 0.0, 0.0, 0.0],  // pos 4 -> 9
        [6.0, 0.0, 0.0, 0.0],  // pos 5 -> 16
    ];
    (query, vectors)
}

// --- Data-evolution (append) vector fixture ------------------------------

/// Options enabling the data-evolution global-index vindex build/search path.
fn append_vector_options() -> HashMap<String, String> {
    HashMap::from([
        ("row-tracking.enabled".to_string(), "true".to_string()),
        ("data-evolution.enabled".to_string(), "true".to_string()),
        ("global-index.enabled".to_string(), "true".to_string()),
        (
            "global-index.row-count-per-shard".to_string(),
            "10".to_string(),
        ),
        ("ivf-flat.dimension".to_string(), "2".to_string()),
        // Single inverted list keeps the ANN search exhaustive (exact, stable
        // ordering), so the C result matches the Rust reference deterministically.
        ("ivf-flat.nlist".to_string(), "1".to_string()),
    ])
}

/// Arrow batch for the DE table: `id` INT plus an `embedding` `List<Float32>`.
fn append_vector_batch(ids: Vec<i32>, vectors: Vec<[f32; 2]>) -> RecordBatch {
    let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
    let mut vector_builder =
        ListBuilder::new(Float32Builder::new()).with_field(element_field.clone());
    for vector in vectors {
        for value in vector {
            vector_builder.values().append_value(value);
        }
        vector_builder.append(true);
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("embedding", ArrowDataType::List(element_field), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(vector_builder.finish()) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Build a data-evolution vector table: write vectors via the public write path,
/// then build the global vindex index via `new_vindex_index_build_builder`.
fn build_append_vector_table(path: &str) -> Table {
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .options(append_vector_options())
        .build()
        .unwrap();
    let table = Table::new(
        file_io,
        Identifier::new("default", "devector"),
        path.to_string(),
        TableSchema::new(0, &schema),
        None,
    );

    crate::runtime().block_on(async {
        let write_builder = table.new_write_builder();
        let mut writer = write_builder.new_write().unwrap();
        writer
            .write_arrow_batch(&append_vector_batch(
                vec![0, 1, 2, 3, 4, 5],
                vec![
                    [1.0, 0.0],
                    [0.0, 1.0],
                    [0.9, 0.1],
                    [0.1, 0.9],
                    [0.8, 0.2],
                    [0.2, 0.8],
                ],
            ))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        write_builder.new_commit().commit(messages).await.unwrap();

        let built = table
            .new_vindex_index_build_builder(INDEX_TYPE)
            .with_index_column("embedding")
            .execute()
            .await
            .unwrap();
        assert!(built > 0, "DE fixture must build at least one index shard");
    });

    table
}

// --- Shared harness: core Rust reference + C read bridges ----------------

/// Import one `paimon_arrow_batch` (Arrow C Data Interface) into a RecordBatch,
/// mirroring `collect_rows`: take ownership of the FFI structs via `ptr::read`,
/// hand the array to `from_ffi`, then neutralize the originals so the caller's
/// `paimon_arrow_batch_free` release is a no-op. The imported schema's memory is
/// released when the local `ffi_schema` drops at the end of this call.
unsafe fn import_batch(batch: &paimon_arrow_batch) -> RecordBatch {
    let ffi_array = ptr::read(batch.array as *const FFI_ArrowArray);
    let ffi_schema = ptr::read(batch.schema as *const FFI_ArrowSchema);
    let data = arrow_array::ffi::from_ffi(ffi_array, &ffi_schema).unwrap();
    ptr::write(batch.array as *mut FFI_ArrowArray, FFI_ArrowArray::empty());
    ptr::write(
        batch.schema as *mut FFI_ArrowSchema,
        FFI_ArrowSchema::empty(),
    );
    RecordBatch::from(StructArray::from(data))
}

/// `(id INT32, score FLOAT32)` pairs from a materialized search batch. Panics if
/// the unified score column is missing, pinning the read contract.
fn batch_id_score_pairs(batch: &RecordBatch) -> Vec<(i32, f32)> {
    let ids = batch
        .column_by_name("id")
        .expect("id column present")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("id is Int32");
    let score_idx = batch
        .schema()
        .index_of(SCORE_COLUMN)
        .expect("unified score column present");
    let scores = batch
        .column(score_idx)
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("score is Float32");
    (0..batch.num_rows())
        .map(|i| (ids.value(i), scores.value(i)))
        .collect()
}

/// Core Rust reference: run `execute_read` and return (row count, score-column
/// present) — the shape the C path is compared against. Runs on the global
/// runtime.
fn rust_execute_read_rows(
    table: &Table,
    column: &str,
    query: Vec<f32>,
    limit: usize,
) -> (usize, bool) {
    crate::runtime().block_on(async {
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_column(column)
            .with_query_vector(query)
            .with_limit(limit);
        let mut stream = builder.execute_read().await.unwrap();
        let (mut rows, mut has_score) = (0usize, false);
        while let Some(b) = stream.try_next().await.unwrap() {
            rows += b.num_rows();
            has_score |= b.schema().index_of(SCORE_COLUMN).is_ok();
        }
        (rows, has_score)
    })
}

/// Core Rust reference: sorted `(id, score)` pairs materialized by `execute_read`.
fn rust_execute_read_pairs(
    table: &Table,
    column: &str,
    query: Vec<f32>,
    limit: usize,
    filter: Option<Predicate>,
) -> Vec<(i32, f32)> {
    crate::runtime().block_on(async {
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_column(column)
            .with_query_vector(query)
            .with_limit(limit);
        if let Some(f) = filter {
            builder.with_filter(f);
        }
        let mut stream = builder.execute_read().await.unwrap();
        let mut pairs = Vec::new();
        while let Some(b) = stream.try_next().await.unwrap() {
            pairs.extend(batch_id_score_pairs(&b));
        }
        pairs.sort_by_key(|p| p.0);
        pairs
    })
}

/// Build a `>=` predicate on an integer column via the public C predicate API.
unsafe fn build_predicate_ge(
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
    let result = paimon_predicate_greater_or_equal(table, col.as_ptr(), datum);
    assert!(result.error.is_null());
    result.predicate
}

/// Construct + configure a C vector-search builder (column, query, limit, and an
/// optional filter that `with_filter` consumes on success). The caller drives the
/// terminal and frees the builder.
unsafe fn c_vector_builder(
    handle: *const paimon_table,
    column: &str,
    query: &[f32],
    limit: usize,
    filter: *mut paimon_predicate,
) -> *mut paimon_vector_search_builder {
    let builder_result = paimon_table_new_vector_search_builder(handle);
    assert!(builder_result.error.is_null());
    let builder = builder_result.builder;

    let col = CString::new(column).unwrap();
    assert!(paimon_vector_search_builder_with_vector_column(builder, col.as_ptr()).is_null());
    assert!(
        paimon_vector_search_builder_with_query_vector(builder, query.as_ptr(), query.len())
            .is_null()
    );
    assert!(paimon_vector_search_builder_with_limit(builder, limit).is_null());
    if !filter.is_null() {
        assert!(paimon_vector_search_builder_with_filter(builder, filter).is_null());
    }
    builder
}

/// C path: run `execute_read` on a configured builder, drain the reader, and
/// return (row count, score-column present). Frees the builder and reader.
unsafe fn c_execute_read_rows(builder: *mut paimon_vector_search_builder) -> (usize, bool) {
    let result = paimon_vector_search_builder_execute_read(builder);
    paimon_vector_search_builder_free(builder);
    assert!(result.error.is_null(), "execute_read should not error");
    assert!(!result.reader.is_null());

    let mut rows = 0usize;
    let mut has_score = false;
    loop {
        let next = paimon_record_batch_reader_next(result.reader);
        assert!(next.error.is_null());
        if next.batch.array.is_null() {
            break; // EOF
        }
        let batch = import_batch(&next.batch);
        rows += batch.num_rows();
        has_score |= batch.schema().index_of(SCORE_COLUMN).is_ok();
        paimon_arrow_batch_free(next.batch);
    }
    paimon_record_batch_reader_free(result.reader);
    (rows, has_score)
}

/// C path: sorted `(id, score)` pairs materialized by a configured builder's
/// `execute_read`. Frees the builder and reader.
unsafe fn c_execute_read_pairs(builder: *mut paimon_vector_search_builder) -> Vec<(i32, f32)> {
    let result = paimon_vector_search_builder_execute_read(builder);
    paimon_vector_search_builder_free(builder);
    assert!(result.error.is_null(), "execute_read should not error");
    assert!(!result.reader.is_null());

    let mut pairs = Vec::new();
    loop {
        let next = paimon_record_batch_reader_next(result.reader);
        assert!(next.error.is_null());
        if next.batch.array.is_null() {
            break; // EOF
        }
        let batch = import_batch(&next.batch);
        pairs.extend(batch_id_score_pairs(&batch));
        paimon_arrow_batch_free(next.batch);
    }
    paimon_record_batch_reader_free(result.reader);
    pairs.sort_by_key(|p| p.0);
    pairs
}

/// Read a `paimon_error`'s UTF-8 message.
unsafe fn error_message(err: *mut paimon_error) -> String {
    let bytes = &(*err).message;
    let slice = std::slice::from_raw_parts(bytes.data, bytes.len);
    String::from_utf8_lossy(slice).to_string()
}

// --- Tests ----------------------------------------------------------------

#[test]
fn vector_search_pk_table_read_matches_rust() {
    let path = "memory:/vsearch_pk_read";
    let (query, vectors) = pk_fixture_smoke();
    let table = build_pk_vector_table(path, &vectors);

    // Independent core reference: row count + score column presence and the
    // materialized (id, score) pairs.
    let (rust_rows, rust_has_score) =
        rust_execute_read_rows(&table, VECTOR_COLUMN, query.to_vec(), 3);
    let rust_pairs = rust_execute_read_pairs(&table, VECTOR_COLUMN, query.to_vec(), 3, None);
    assert_eq!(rust_rows, 3, "PK fixture top-3 must materialize 3 rows");
    assert!(
        rust_has_score,
        "reference must carry the unified score column"
    );

    let handle = unsafe { wrap_table(table) };
    unsafe {
        let builder = c_vector_builder(handle, VECTOR_COLUMN, &query, 3, ptr::null_mut());
        let (c_rows, c_has_score) = c_execute_read_rows(builder);
        assert_eq!(
            c_rows, rust_rows,
            "C row count must match the Rust reference"
        );
        assert_eq!(
            c_has_score, rust_has_score,
            "score-column presence must match"
        );

        let builder = c_vector_builder(handle, VECTOR_COLUMN, &query, 3, ptr::null_mut());
        let c_pairs = c_execute_read_pairs(builder);
        assert_eq!(c_pairs.len(), rust_pairs.len());
        for ((c_id, c_score), (r_id, r_score)) in c_pairs.iter().zip(&rust_pairs) {
            assert_eq!(c_id, r_id, "C row ids must match the Rust reference");
            assert!(
                (c_score - r_score).abs() < 1e-6,
                "score diverges: {c_score} vs {r_score}"
            );
        }
        unwrap_table(handle);
    }
}

#[test]
fn vector_search_append_table_read_matches_rust() {
    let path = "memory:/vsearch_append_read";
    let table = build_append_vector_table(path);
    let query = vec![1.0f32, 0.0];

    let (rust_rows, rust_has_score) = rust_execute_read_rows(&table, "embedding", query.clone(), 3);
    let rust_pairs = rust_execute_read_pairs(&table, "embedding", query.clone(), 3, None);
    assert!(rust_rows > 0, "DE fixture must materialize hits");
    assert!(
        rust_has_score,
        "reference must carry the unified score column"
    );

    let handle = unsafe { wrap_table(table) };
    unsafe {
        let builder = c_vector_builder(handle, "embedding", &query, 3, ptr::null_mut());
        let (c_rows, c_has_score) = c_execute_read_rows(builder);
        assert_eq!(
            c_rows, rust_rows,
            "C row count must match the Rust reference"
        );
        assert_eq!(
            c_has_score, rust_has_score,
            "score-column presence must match"
        );

        // The data-evolution global-index path does not promise a stable order
        // across two separate reads, so compare the (id, score) hits as an
        // id-keyed set rather than over-asserting an order neither read promises.
        let builder = c_vector_builder(handle, "embedding", &query, 3, ptr::null_mut());
        let c_pairs = c_execute_read_pairs(builder);
        assert_eq!(c_pairs.len(), rust_pairs.len());
        for ((c_id, c_score), (r_id, r_score)) in c_pairs.iter().zip(&rust_pairs) {
            assert_eq!(c_id, r_id, "C row ids must match the Rust reference set");
            assert!(
                (c_score - r_score).abs() < 1e-6,
                "score diverges: {c_score} vs {r_score}"
            );
        }
        unwrap_table(handle);
    }
}

#[test]
fn vector_search_pk_filter_excludes_neighbor() {
    let path = "memory:/vsearch_pk_filter_read";
    let (query, vectors) = pk_fixture_residual();
    let table = build_pk_vector_table(path, &vectors);

    // Guard: the nearest neighbor (id 0) is present in the unfiltered top-3, so a
    // working residual filter of `id >= 3` must exclude it.
    let unfiltered = rust_execute_read_pairs(&table, VECTOR_COLUMN, query.to_vec(), 3, None);
    let unfiltered_ids: Vec<i32> = unfiltered.iter().map(|(id, _)| *id).collect();
    assert!(
        unfiltered_ids.contains(&0),
        "fixture guard: id 0 must be an unfiltered neighbor"
    );

    // Independent filtered reference via the core Rust path.
    let rust_filter = PredicateBuilder::new(table.schema().fields())
        .greater_or_equal("id", Datum::Int(3))
        .unwrap();
    let rust_pairs =
        rust_execute_read_pairs(&table, VECTOR_COLUMN, query.to_vec(), 3, Some(rust_filter));

    let handle = unsafe { wrap_table(table) };
    unsafe {
        let predicate = build_predicate_ge(handle, "id", 3);
        let builder = c_vector_builder(handle, VECTOR_COLUMN, &query, 3, predicate);
        let c_pairs = c_execute_read_pairs(builder);

        let c_ids: Vec<i32> = c_pairs.iter().map(|(id, _)| *id).collect();
        for excluded in [0i32, 1, 2] {
            assert!(
                !c_ids.contains(&excluded),
                "filtered read must exclude neighbor {excluded}"
            );
        }
        assert_eq!(
            c_pairs, rust_pairs,
            "filtered pairs must match the reference"
        );
        unwrap_table(handle);
    }
}

#[test]
fn vector_search_append_filter_returns_invalid_input() {
    let path = "memory:/vsearch_append_filter_err";
    let table = build_append_vector_table(path);
    let handle = unsafe { wrap_table(table) };
    unsafe {
        let predicate = build_predicate_ge(handle, "id", 1);
        let builder = c_vector_builder(handle, "embedding", &[1.0f32, 0.0], 3, predicate);
        let result = paimon_vector_search_builder_execute_read(builder);
        paimon_vector_search_builder_free(builder);

        assert!(
            result.reader.is_null(),
            "errored read must not yield a reader"
        );
        assert!(!result.error.is_null(), "DE filter must fail loud");
        assert_eq!(
            (*result.error).code,
            PaimonErrorCode::InvalidInput as i32,
            "DE filter error must map to InvalidInput"
        );
        let message = error_message(result.error);
        assert!(
            message.contains("primary-key vector path"),
            "unexpected error message: {message}"
        );
        paimon_error_free(result.error);
        unwrap_table(handle);
    }
}

#[test]
fn vector_search_unknown_column_returns_invalid_input() {
    // A typo'd vector column must surface as an input error through the C API,
    // not a silent empty (EOF) reader.
    let path = "memory:/vsearch_unknown_col_err";
    let table = build_append_vector_table(path);
    let handle = unsafe { wrap_table(table) };
    unsafe {
        let builder =
            c_vector_builder(handle, "does_not_exist", &[1.0f32, 0.0], 3, ptr::null_mut());
        let result = paimon_vector_search_builder_execute_read(builder);
        paimon_vector_search_builder_free(builder);

        assert!(
            result.reader.is_null(),
            "errored read must not yield a reader"
        );
        assert!(!result.error.is_null(), "unknown column must fail loud");
        assert_eq!(
            (*result.error).code,
            PaimonErrorCode::InvalidInput as i32,
            "unknown column error must map to InvalidInput"
        );
        let message = error_message(result.error);
        assert!(
            message.contains("does not exist"),
            "unexpected error message: {message}"
        );
        paimon_error_free(result.error);
        unwrap_table(handle);
    }
}

#[test]
fn vector_search_scalar_column_returns_invalid_input() {
    // A scalar (non-vector) column must surface as an input error, not an empty
    // reader.
    let path = "memory:/vsearch_scalar_col_err";
    let table = build_append_vector_table(path);
    let handle = unsafe { wrap_table(table) };
    unsafe {
        // "id" is a scalar Int column on the append vector table.
        let builder = c_vector_builder(handle, "id", &[1.0f32, 0.0], 3, ptr::null_mut());
        let result = paimon_vector_search_builder_execute_read(builder);
        paimon_vector_search_builder_free(builder);

        assert!(
            result.reader.is_null(),
            "errored read must not yield a reader"
        );
        assert!(!result.error.is_null(), "scalar column must fail loud");
        assert_eq!(
            (*result.error).code,
            PaimonErrorCode::InvalidInput as i32,
            "scalar column error must map to InvalidInput"
        );
        let message = error_message(result.error);
        assert!(
            message.contains("must be a FLOAT vector column"),
            "unexpected error message: {message}"
        );
        paimon_error_free(result.error);
        unwrap_table(handle);
    }
}

#[test]
fn vector_search_rejects_invalid_query_vector() {
    let path = "memory:/vsearch_setter_validation";
    let table = build_pk_vector_table_empty(path);
    let handle = unsafe { wrap_table(table) };
    unsafe {
        let builder_result = paimon_table_new_vector_search_builder(handle);
        assert!(builder_result.error.is_null());
        let builder = builder_result.builder;

        // Null data with a non-zero length is rejected at the setter.
        let err_null = paimon_vector_search_builder_with_query_vector(builder, ptr::null(), 5);
        assert!(!err_null.is_null());
        assert_eq!((*err_null).code, PaimonErrorCode::InvalidInput as i32);
        paimon_error_free(err_null);

        // A zero-length query is rejected even with a valid pointer.
        let data = [1.0f32];
        let err_empty = paimon_vector_search_builder_with_query_vector(builder, data.as_ptr(), 0);
        assert!(!err_empty.is_null());
        assert_eq!((*err_empty).code, PaimonErrorCode::InvalidInput as i32);
        paimon_error_free(err_empty);

        paimon_vector_search_builder_free(builder);
        unwrap_table(handle);
    }
}

#[test]
fn vector_search_empty_result_is_eof_stream() {
    let path = "memory:/vsearch_empty_read";
    let table = build_pk_vector_table_empty(path);
    let handle = unsafe { wrap_table(table) };
    unsafe {
        let builder = c_vector_builder(
            handle,
            VECTOR_COLUMN,
            &[1.0f32, 2.0, 3.0, 4.0],
            5,
            ptr::null_mut(),
        );
        let result = paimon_vector_search_builder_execute_read(builder);
        paimon_vector_search_builder_free(builder);

        // An empty table is a normal EOF stream, not an error: a reader is
        // returned and the first `_next` yields a null batch with no error.
        assert!(result.error.is_null());
        assert!(!result.reader.is_null());
        let next = paimon_record_batch_reader_next(result.reader);
        assert!(next.error.is_null());
        assert!(next.batch.array.is_null());
        assert!(next.batch.schema.is_null());
        paimon_record_batch_reader_free(result.reader);
        unwrap_table(handle);
    }
}
