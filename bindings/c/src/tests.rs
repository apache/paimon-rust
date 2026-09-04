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

use std::collections::HashMap;
use std::ffi::{c_void, CString};
use std::mem::ManuallyDrop;
use std::process::Command;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::buffer::NullBuffer;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, Int32Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use paimon::catalog::Identifier;
use paimon::io::FileIOBuilder;
use paimon::spec::{
    BlobDescriptor, CommitKind, DataType, IntType, Schema, TableSchema, VarCharType,
};
use paimon::table::{SnapshotManager, Table};

use crate::blob_reader::*;
use crate::catalog::*;
use crate::error::*;
use crate::file_io::*;
use crate::identifier::*;
use crate::stream::*;
use crate::table::*;
use crate::types::*;
use crate::vector_search::*;
use crate::write::*;

#[test]
fn test_catalog_create_and_drop_table_from_schema_json() {
    let directory = tempfile::tempdir().unwrap();
    let warehouse = CString::new(directory.path().to_string_lossy().as_bytes()).unwrap();
    let warehouse_key = CString::new("warehouse").unwrap();
    let options = [paimon_option {
        key: warehouse_key.as_ptr(),
        value: warehouse.as_ptr(),
    }];
    let catalog_result = unsafe { paimon_catalog_create(options.as_ptr(), options.len()) };
    assert!(catalog_result.error.is_null());
    assert!(!catalog_result.catalog.is_null());

    let catalog = unsafe { &*((*catalog_result.catalog).inner as *const Arc<dyn paimon::Catalog>) };
    crate::runtime()
        .block_on(catalog.create_database("default", true, HashMap::new()))
        .unwrap();

    let database = CString::new("default").unwrap();
    let table_name = CString::new("ffi_ddl").unwrap();
    let identifier_result =
        unsafe { paimon_identifier_new(database.as_ptr(), table_name.as_ptr()) };
    assert!(identifier_result.error.is_null());
    assert!(!identifier_result.identifier.is_null());

    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::with_nullable(false)))
        .option("bucket", "1")
        .option("bucket-key", "id")
        .build()
        .unwrap();
    let schema_json = CString::new(serde_json::to_string(&schema).unwrap()).unwrap();

    let create_error = unsafe {
        paimon_catalog_create_table_from_schema_json(
            catalog_result.catalog,
            identifier_result.identifier,
            schema_json.as_ptr(),
            false,
        )
    };
    assert!(create_error.is_null());

    let table_result =
        unsafe { paimon_catalog_get_table(catalog_result.catalog, identifier_result.identifier) };
    assert!(table_result.error.is_null());
    assert!(!table_result.table.is_null());
    unsafe { paimon_table_free(table_result.table) };

    let duplicate_error = unsafe {
        paimon_catalog_create_table_from_schema_json(
            catalog_result.catalog,
            identifier_result.identifier,
            schema_json.as_ptr(),
            false,
        )
    };
    assert!(!duplicate_error.is_null());
    assert_eq!(
        unsafe { (*duplicate_error).code },
        PAIMON_ERROR_ALREADY_EXISTS
    );
    unsafe { paimon_error_free(duplicate_error) };
    assert!(unsafe {
        paimon_catalog_create_table_from_schema_json(
            catalog_result.catalog,
            identifier_result.identifier,
            schema_json.as_ptr(),
            true,
        )
    }
    .is_null());

    assert!(unsafe {
        paimon_catalog_drop_table(catalog_result.catalog, identifier_result.identifier, false)
    }
    .is_null());
    let missing =
        unsafe { paimon_catalog_get_table(catalog_result.catalog, identifier_result.identifier) };
    assert!(missing.table.is_null());
    assert!(!missing.error.is_null());
    unsafe { paimon_error_free(missing.error) };
    assert!(unsafe {
        paimon_catalog_drop_table(catalog_result.catalog, identifier_result.identifier, true)
    }
    .is_null());

    unsafe {
        paimon_identifier_free(identifier_result.identifier);
        paimon_catalog_free(catalog_result.catalog);
    }
}

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

fn partitioned_postpone_table_schema() -> TableSchema {
    let schema = Schema::builder()
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .primary_key(["pt", "id"])
        .partition_keys(["pt"])
        .option("bucket", "-2")
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

unsafe fn table_ref<'a>(table: *const paimon_table) -> &'a Table {
    &*((*table).inner as *const Table)
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

fn make_partitioned_write_batch(pts: Vec<&str>, ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("pt", ArrowDataType::Utf8, false),
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(pts)),
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

fn make_postpone_bucket_plan_batch(partitions: Vec<&str>, counts: Vec<i32>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("pt", ArrowDataType::Utf8, false),
        ArrowField::new("total_buckets", ArrowDataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(partitions)),
            Arc::new(Int32Array::from(counts)),
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

/// Collect the audit-log row-kind strings while exercising Arrow ownership.
unsafe fn collect_rowkinds(reader: *mut paimon_record_batch_reader) -> Vec<String> {
    let mut kinds = Vec::new();
    loop {
        let result = paimon_record_batch_reader_next(reader);
        assert!(result.error.is_null(), "reader_next should not error");
        if result.batch.array.is_null() {
            break;
        }
        let ffi_array = ptr::read(result.batch.array as *const FFI_ArrowArray);
        let ffi_schema = ptr::read(result.batch.schema as *const FFI_ArrowSchema);
        let data = arrow_array::ffi::from_ffi(ffi_array, &ffi_schema).unwrap();
        ptr::write(
            result.batch.array as *mut FFI_ArrowArray,
            FFI_ArrowArray::empty(),
        );
        ptr::write(
            result.batch.schema as *mut FFI_ArrowSchema,
            FFI_ArrowSchema::empty(),
        );
        paimon_arrow_batch_free(result.batch);

        let batch = RecordBatch::from(StructArray::from(data));
        let rowkind = batch
            .column_by_name("rowkind")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            kinds.push(rowkind.value(index).to_string());
        }
    }
    kinds
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

#[test]
fn test_stream_scan_tails_snapshots_and_restores_cursor() {
    let path = "memory:/test_stream_scan_tails_snapshots";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table.clone()) };

    unsafe {
        let rb_result = paimon_table_new_read_builder(handle);
        assert!(rb_result.error.is_null());
        let rb = rb_result.read_builder;
        let read_result = paimon_read_builder_new_read(rb);
        assert!(read_result.error.is_null());
        let read = read_result.read;

        let mut options = std::mem::MaybeUninit::<paimon_stream_scan_options>::uninit();
        assert!(paimon_stream_scan_options_init(options.as_mut_ptr()).is_null());
        let mut options = options.assume_init();
        options.startup_mode = PAIMON_STREAM_STARTUP_LATEST;
        options.follow_up_mode = PAIMON_STREAM_FOLLOW_UP_DELTA;

        let scan_result = paimon_read_builder_new_stream_scan(rb, &options);
        assert!(scan_result.error.is_null());
        let scan = scan_result.scan;
        assert_eq!(paimon_stream_scan_checkpoint(scan), 1);

        // Commit before the first poll. Eager initialization at scan creation
        // must retain snapshot 1 instead of treating it as pre-existing data.
        write_data_rust(&table, &[make_batch(vec![1], vec!["first"])]);
        let first = paimon_stream_scan_poll(scan);
        assert!(first.error.is_null());
        assert_eq!(first.status, PAIMON_STREAM_POLL_DATA);
        assert_eq!(first.snapshot_id, 1);
        assert_eq!(first.next_snapshot_id, 2);
        assert_eq!(paimon_stream_scan_checkpoint(scan), 2);
        assert_eq!(paimon_stream_plan_is_full(first.plan), 0);

        let serialized = paimon_stream_plan_serialize(first.plan);
        assert!(serialized.error.is_null());
        let checkpoint_bytes =
            std::slice::from_raw_parts(serialized.bytes.data, serialized.bytes.len).to_vec();
        paimon_bytes_free(serialized.bytes);
        let restored_plan =
            paimon_stream_plan_deserialize(checkpoint_bytes.as_ptr(), checkpoint_bytes.len());
        assert!(restored_plan.error.is_null());
        assert_eq!(restored_plan.status, PAIMON_STREAM_POLL_DATA);
        assert_eq!(restored_plan.snapshot_id, 1);
        assert_eq!(restored_plan.next_snapshot_id, 2);
        let restored_reader = paimon_stream_plan_read_to_arrow(
            read,
            restored_plan.plan,
            0,
            usize::MAX,
            PAIMON_STREAM_READ_DATA,
        );
        assert!(restored_reader.error.is_null());
        assert_eq!(
            collect_rows(restored_reader.reader),
            vec![(1, "first".into())]
        );
        paimon_record_batch_reader_free(restored_reader.reader);

        let mismatched_builder = paimon_table_new_read_builder(handle);
        assert!(mismatched_builder.error.is_null());
        assert!(
            paimon_read_builder_with_case_sensitive(mismatched_builder.read_builder, false,)
                .is_null()
        );
        let mismatched_read = paimon_read_builder_new_read(mismatched_builder.read_builder);
        assert!(mismatched_read.error.is_null());
        let mismatched_result = paimon_stream_plan_read_to_arrow(
            mismatched_read.read,
            restored_plan.plan,
            0,
            usize::MAX,
            PAIMON_STREAM_READ_DATA,
        );
        assert!(mismatched_result.reader.is_null());
        assert!(!mismatched_result.error.is_null());
        assert_eq!(
            (*mismatched_result.error).code,
            PaimonErrorCode::InvalidInput as i32
        );
        paimon_error_free(mismatched_result.error);
        paimon_table_read_free(mismatched_read.read);
        paimon_read_builder_free(mismatched_builder.read_builder);

        let branch_table = Table::from_resolved_schema(
            table.file_io().clone(),
            Identifier::new("default", "test"),
            path.to_string(),
            table.schema().clone(),
            "branch-review",
        )
        .unwrap();
        let branch_handle = wrap_table(branch_table);
        let branch_builder = paimon_table_new_read_builder(branch_handle);
        assert!(branch_builder.error.is_null());
        let branch_read = paimon_read_builder_new_read(branch_builder.read_builder);
        assert!(branch_read.error.is_null());
        let branch_result = paimon_stream_plan_read_to_arrow(
            branch_read.read,
            restored_plan.plan,
            0,
            usize::MAX,
            PAIMON_STREAM_READ_DATA,
        );
        assert!(branch_result.reader.is_null());
        assert!(!branch_result.error.is_null());
        assert_eq!(
            (*branch_result.error).code,
            PaimonErrorCode::InvalidInput as i32
        );
        paimon_error_free(branch_result.error);
        paimon_table_read_free(branch_read.read);
        paimon_read_builder_free(branch_builder.read_builder);
        unwrap_table(branch_handle);

        paimon_stream_plan_free(restored_plan.plan);

        let first_reader = paimon_stream_plan_read_to_arrow(
            read,
            first.plan,
            0,
            usize::MAX,
            PAIMON_STREAM_READ_DATA,
        );
        assert!(first_reader.error.is_null());
        assert_eq!(collect_rows(first_reader.reader), vec![(1, "first".into())]);
        paimon_record_batch_reader_free(first_reader.reader);

        let audit_reader = paimon_stream_plan_read_to_arrow(
            read,
            first.plan,
            0,
            usize::MAX,
            PAIMON_STREAM_READ_AUDIT_LOG,
        );
        assert!(audit_reader.error.is_null());
        assert_eq!(collect_rowkinds(audit_reader.reader), vec!["+I"]);
        paimon_record_batch_reader_free(audit_reader.reader);
        paimon_stream_plan_free(first.plan);

        write_data_rust(&table, &[make_batch(vec![2], vec!["second"])]);
        let second = paimon_stream_scan_poll(scan);
        assert!(second.error.is_null());
        assert_eq!(second.status, PAIMON_STREAM_POLL_DATA);
        assert_eq!(second.snapshot_id, 2);
        assert_eq!(second.next_snapshot_id, 3);
        let second_reader = paimon_stream_plan_read_to_arrow(
            read,
            second.plan,
            0,
            usize::MAX,
            PAIMON_STREAM_READ_DATA,
        );
        assert!(second_reader.error.is_null());
        assert_eq!(
            collect_rows(second_reader.reader),
            vec![(2, "second".into())]
        );
        paimon_record_batch_reader_free(second_reader.reader);
        paimon_stream_plan_free(second.plan);

        // Restoring nextSnapshotId=2 replays snapshot 2 rather than losing it.
        assert!(paimon_stream_scan_restore(scan, 2).is_null());
        let replay = paimon_stream_scan_poll(scan);
        assert!(replay.error.is_null());
        assert_eq!(replay.status, PAIMON_STREAM_POLL_DATA);
        assert_eq!(replay.snapshot_id, 2);
        paimon_stream_plan_free(replay.plan);

        paimon_stream_scan_free(scan);
        paimon_table_read_free(read);
        paimon_read_builder_free(rb);
        unwrap_table(handle);
    }
}

#[test]
fn test_stream_scan_latest_full_then_waits_for_follow_up() {
    let path = "memory:/test_stream_scan_latest_full";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        simple_table_schema(),
        None,
    );
    write_data_rust(&table, &[make_batch(vec![7], vec!["existing"])]);
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let rb_result = paimon_table_new_read_builder(handle);
        assert!(rb_result.error.is_null());
        let mut options = std::mem::MaybeUninit::<paimon_stream_scan_options>::uninit();
        assert!(paimon_stream_scan_options_init(options.as_mut_ptr()).is_null());
        let options = options.assume_init();
        let scan_result = paimon_read_builder_new_stream_scan(rb_result.read_builder, &options);
        assert!(scan_result.error.is_null());

        let full = paimon_stream_scan_poll(scan_result.scan);
        assert!(full.error.is_null());
        assert_eq!(full.status, PAIMON_STREAM_POLL_DATA);
        assert_eq!(full.snapshot_id, 1);
        assert_eq!(paimon_stream_plan_is_full(full.plan), 1);
        assert_eq!(paimon_stream_scan_checkpoint(scan_result.scan), 2);
        paimon_stream_plan_free(full.plan);

        let waiting = paimon_stream_scan_poll(scan_result.scan);
        assert!(waiting.error.is_null());
        assert_eq!(waiting.status, PAIMON_STREAM_POLL_WAITING);

        paimon_stream_scan_free(scan_result.scan);
        paimon_read_builder_free(rb_result.read_builder);
        unwrap_table(handle);
    }
}

// =========================================================================
//  Catalog-free table construction tests
// =========================================================================

#[derive(Default)]
struct TestFileCacheState {
    blocks: Mutex<HashMap<(String, u64), Vec<u8>>>,
    hits: AtomicUsize,
    misses: AtomicUsize,
    puts: AtomicUsize,
    invalidations: AtomicUsize,
    destroys: AtomicUsize,
}

struct TestFileCacheContext {
    state: Arc<TestFileCacheState>,
}

unsafe extern "C" fn test_file_cache_get(
    context: *mut c_void,
    path_data: *const u8,
    path_length: usize,
    offset: u64,
    length: usize,
    output: *mut u8,
) -> i64 {
    let context = &*(context as *const TestFileCacheContext);
    let path =
        String::from_utf8_lossy(std::slice::from_raw_parts(path_data, path_length)).into_owned();
    let blocks = context.state.blocks.lock().unwrap();
    let Some(data) = blocks
        .get(&(path, offset))
        .filter(|data| data.len() == length)
    else {
        context.state.misses.fetch_add(1, Ordering::SeqCst);
        return -1;
    };
    ptr::copy_nonoverlapping(data.as_ptr(), output, length);
    context.state.hits.fetch_add(1, Ordering::SeqCst);
    length as i64
}

unsafe extern "C" fn test_file_cache_short_get(
    _context: *mut c_void,
    _path_data: *const u8,
    _path_length: usize,
    _offset: u64,
    length: usize,
    output: *mut u8,
) -> i64 {
    if length > 0 {
        output.write(0);
    }
    length.saturating_sub(1) as i64
}

unsafe extern "C" fn test_file_cache_put(
    context: *mut c_void,
    path_data: *const u8,
    path_length: usize,
    offset: u64,
    data: *const u8,
    length: usize,
) -> i32 {
    let context = &*(context as *const TestFileCacheContext);
    let path =
        String::from_utf8_lossy(std::slice::from_raw_parts(path_data, path_length)).into_owned();
    let data = std::slice::from_raw_parts(data, length).to_vec();
    context
        .state
        .blocks
        .lock()
        .unwrap()
        .insert((path, offset), data);
    context.state.puts.fetch_add(1, Ordering::SeqCst);
    0
}

unsafe extern "C" fn test_file_cache_invalidate_path(
    context: *mut c_void,
    path_data: *const u8,
    path_length: usize,
) -> i32 {
    let context = &*(context as *const TestFileCacheContext);
    let path =
        String::from_utf8_lossy(std::slice::from_raw_parts(path_data, path_length)).into_owned();
    context
        .state
        .blocks
        .lock()
        .unwrap()
        .retain(|(cached_path, _), _| cached_path != &path);
    context.state.invalidations.fetch_add(1, Ordering::SeqCst);
    0
}

unsafe extern "C" fn test_file_cache_invalidate_prefix(
    context: *mut c_void,
    prefix_data: *const u8,
    prefix_length: usize,
) -> i32 {
    let context = &*(context as *const TestFileCacheContext);
    let prefix = String::from_utf8_lossy(std::slice::from_raw_parts(prefix_data, prefix_length))
        .into_owned();
    context
        .state
        .blocks
        .lock()
        .unwrap()
        .retain(|(cached_path, _), _| !cached_path.starts_with(&prefix));
    context.state.invalidations.fetch_add(1, Ordering::SeqCst);
    0
}

unsafe extern "C" fn test_file_cache_destroy(context: *mut c_void) {
    let context = Box::from_raw(context as *mut TestFileCacheContext);
    context.state.destroys.fetch_add(1, Ordering::SeqCst);
}

fn test_file_cache_callbacks(state: Arc<TestFileCacheState>) -> paimon_file_cache_callbacks_v1 {
    paimon_file_cache_callbacks_v1 {
        context: Box::into_raw(Box::new(TestFileCacheContext { state })) as *mut c_void,
        get: Some(test_file_cache_get),
        put: Some(test_file_cache_put),
        invalidate_path: Some(test_file_cache_invalidate_path),
        invalidate_prefix: Some(test_file_cache_invalidate_prefix),
        destroy: Some(test_file_cache_destroy),
    }
}

#[test]
fn test_file_io_external_cache_miss_populates_and_hit_reuses_blocks() {
    let state = Arc::new(TestFileCacheState::default());
    let callbacks = test_file_cache_callbacks(state.clone());
    let root = CString::new("memory:/ffi_external_cache").unwrap();
    let whitelist = CString::new("meta").unwrap();

    unsafe {
        let result = paimon_file_io_create_with_cache_v1(
            root.as_ptr(),
            ptr::null(),
            0,
            &callbacks,
            4,
            whitelist.as_ptr(),
        );
        assert!(result.error.is_null());
        let file_io = file_io_ref(result.file_io);
        let path = "memory:/ffi_external_cache/snapshot/snapshot-1";
        crate::runtime()
            .block_on(
                file_io
                    .new_output(path)
                    .unwrap()
                    .write(bytes::Bytes::from_static(b"abcdefgh")),
            )
            .unwrap();

        let first = crate::runtime()
            .block_on(file_io.new_input(path).unwrap().read())
            .unwrap();
        let second = crate::runtime()
            .block_on(file_io.new_input(path).unwrap().read())
            .unwrap();
        assert_eq!(first.as_ref(), b"abcdefgh");
        assert_eq!(second.as_ref(), b"abcdefgh");
        assert_eq!(state.puts.load(Ordering::SeqCst), 2);
        assert!(state.misses.load(Ordering::SeqCst) >= 1);
        assert_eq!(state.hits.load(Ordering::SeqCst), 2);
        assert_eq!(state.invalidations.load(Ordering::SeqCst), 1);

        paimon_file_io_free(result.file_io);
        assert_eq!(state.destroys.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn test_table_with_file_io_retains_cache_context_until_table_is_freed() {
    let state = Arc::new(TestFileCacheState::default());
    let callbacks = test_file_cache_callbacks(state.clone());
    let path = CString::new("memory:/ffi_file_io_table").unwrap();
    let file_io = unsafe {
        paimon_file_io_create_with_cache_v1(
            path.as_ptr(),
            ptr::null(),
            0,
            &callbacks,
            4,
            ptr::null(),
        )
    };
    assert!(file_io.error.is_null());

    let schema_json = CString::new(serde_json::to_string(&simple_table_schema()).unwrap()).unwrap();
    let database = CString::new("default").unwrap();
    let table_name = CString::new("test").unwrap();
    unsafe {
        let table = paimon_table_from_schema_json_with_file_io(
            file_io.file_io,
            path.as_ptr(),
            schema_json.as_ptr(),
            database.as_ptr(),
            table_name.as_ptr(),
            ptr::null(),
        );
        assert!(table.error.is_null());

        paimon_file_io_free(file_io.file_io);
        assert_eq!(state.destroys.load(Ordering::SeqCst), 0);
        paimon_table_free(table.table);
        assert_eq!(state.destroys.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn test_file_io_external_cache_rejects_invalid_callback_contract() {
    let path = CString::new("memory:/ffi_invalid_cache").unwrap();
    let callbacks = paimon_file_cache_callbacks_v1 {
        context: ptr::null_mut(),
        get: None,
        put: None,
        invalidate_path: None,
        invalidate_prefix: None,
        destroy: None,
    };
    unsafe {
        let missing_get = paimon_file_io_create_with_cache_v1(
            path.as_ptr(),
            ptr::null(),
            0,
            &callbacks,
            4,
            ptr::null(),
        );
        assert!(missing_get.file_io.is_null());
        assert_eq!(
            (*missing_get.error).code,
            PaimonErrorCode::InvalidInput as i32
        );
        paimon_error_free(missing_get.error);

        let zero_block = paimon_file_io_create_with_cache_v1(
            path.as_ptr(),
            ptr::null(),
            0,
            &paimon_file_cache_callbacks_v1 {
                get: Some(test_file_cache_get),
                ..callbacks
            },
            0,
            ptr::null(),
        );
        assert!(zero_block.file_io.is_null());
        assert_eq!(
            (*zero_block.error).code,
            PaimonErrorCode::InvalidInput as i32
        );
        paimon_error_free(zero_block.error);
    }
}

#[test]
fn test_file_io_external_cache_short_hit_fails_open_to_storage() {
    let callbacks = paimon_file_cache_callbacks_v1 {
        context: ptr::null_mut(),
        get: Some(test_file_cache_short_get),
        put: None,
        invalidate_path: None,
        invalidate_prefix: None,
        destroy: None,
    };
    let root = CString::new("memory:/ffi_short_cache_hit").unwrap();
    let whitelist = CString::new("meta").unwrap();
    unsafe {
        let result = paimon_file_io_create_with_cache_v1(
            root.as_ptr(),
            ptr::null(),
            0,
            &callbacks,
            4,
            whitelist.as_ptr(),
        );
        assert!(result.error.is_null());
        let file_io = file_io_ref(result.file_io);
        let path = "memory:/ffi_short_cache_hit/snapshot/snapshot-1";
        crate::runtime()
            .block_on(
                file_io
                    .new_output(path)
                    .unwrap()
                    .write(bytes::Bytes::from_static(b"source")),
            )
            .unwrap();

        let actual = crate::runtime()
            .block_on(file_io.new_input(path).unwrap().read())
            .unwrap();
        assert_eq!(actual.as_ref(), b"source");
        paimon_file_io_free(result.file_io);
    }
}

#[test]
fn test_table_from_schema_json_preserves_resolved_schema_and_branch() {
    let path = CString::new("memory:/test_resolved_branch").unwrap();
    let database = CString::new("default").unwrap();
    let table_name = CString::new("test").unwrap();
    let branch = CString::new("dev").unwrap();
    let schema = simple_table_schema();
    let schema_json = CString::new(serde_json::to_string(&schema).unwrap()).unwrap();
    let option_key = CString::new("storage-only-option").unwrap();
    let option_value = CString::new("secret").unwrap();
    let storage_options = [paimon_option {
        key: option_key.as_ptr(),
        value: option_value.as_ptr(),
    }];

    unsafe {
        let result = paimon_table_from_schema_json(
            path.as_ptr(),
            schema_json.as_ptr(),
            database.as_ptr(),
            table_name.as_ptr(),
            branch.as_ptr(),
            storage_options.as_ptr(),
            storage_options.len(),
        );

        assert!(result.error.is_null());
        assert!(!result.table.is_null());
        let table = table_ref(result.table);
        assert_eq!(table.location(), "memory:/test_resolved_branch");
        assert_eq!(table.identifier(), &Identifier::new("default", "test"));
        // The resolved schema is preserved as-is (no normalization or mutation).
        assert_eq!(table.schema(), &schema);
        assert_eq!(table.branch(), "dev");
        assert!(table.is_branch_reference());
        assert_eq!(
            table.schema_manager().schema_path(schema.id()),
            "memory:/test_resolved_branch/branch/branch-dev/schema/schema-0"
        );
        assert_eq!(
            table.snapshot_manager().snapshot_path(1),
            "memory:/test_resolved_branch/branch/branch-dev/snapshot/snapshot-1"
        );
        assert_eq!(
            table.tag_manager().tag_path("release"),
            "memory:/test_resolved_branch/branch/branch-dev/tag/tag-release"
        );
        assert!(!table.schema().options().contains_key("storage-only-option"));

        let read_builder = paimon_table_new_read_builder(result.table);
        assert!(read_builder.error.is_null());
        assert!(!read_builder.read_builder.is_null());
        paimon_read_builder_free(read_builder.read_builder);

        paimon_table_free(result.table);
    }
}

#[test]
fn test_table_from_schema_json_rejects_missing_primary_key_column() {
    // A syntactically valid schema whose primaryKeys reference a non-existent
    // column must be rejected at construction, not panic later (e.g. the write
    // path unwraps a PartitionComputer built from missing columns).
    let path = CString::new("memory:/test_resolved_bad_pk").unwrap();
    let database = CString::new("default").unwrap();
    let table_name = CString::new("test").unwrap();
    let branch = CString::new("main").unwrap();

    // simple_table_schema has columns id, name; declare a PK on a missing column.
    let base = simple_table_schema();
    let bad_json = serde_json::to_value(&base).unwrap();
    let mut bad_json = bad_json;
    bad_json["primaryKeys"] = serde_json::json!(["missing"]);
    let schema_json = CString::new(serde_json::to_string(&bad_json).unwrap()).unwrap();

    unsafe {
        let result = paimon_table_from_schema_json(
            path.as_ptr(),
            schema_json.as_ptr(),
            database.as_ptr(),
            table_name.as_ptr(),
            branch.as_ptr(),
            std::ptr::null(),
            0,
        );
        assert!(result.table.is_null());
        assert!(!result.error.is_null());
        assert_eq!((*result.error).code, PaimonErrorCode::InvalidInput as i32);
        paimon_error_free(result.error);
    }
}

#[test]
fn test_table_from_schema_json_rejects_reserved_field_name() {
    // A user column colliding with a system field name (_ROW_ID) would be
    // silently replaced by the system row number on read; reject at construction.
    let path = CString::new("memory:/test_resolved_reserved_field").unwrap();
    let database = CString::new("default").unwrap();
    let table_name = CString::new("test").unwrap();
    let branch = CString::new("main").unwrap();

    let base = simple_table_schema();
    let mut bad_json = serde_json::to_value(&base).unwrap();
    // Rename the second field ("name") to the reserved system name "_ROW_ID".
    bad_json["fields"][1]["name"] = serde_json::json!("_ROW_ID");
    let schema_json = CString::new(serde_json::to_string(&bad_json).unwrap()).unwrap();

    unsafe {
        let result = paimon_table_from_schema_json(
            path.as_ptr(),
            schema_json.as_ptr(),
            database.as_ptr(),
            table_name.as_ptr(),
            branch.as_ptr(),
            std::ptr::null(),
            0,
        );
        assert!(result.table.is_null());
        assert!(!result.error.is_null());
        assert_eq!((*result.error).code, PaimonErrorCode::InvalidInput as i32);
        paimon_error_free(result.error);
    }
}

#[test]
fn test_table_from_schema_json_main_branch_uses_main_schema_directory() {
    let path = CString::new("memory:/test_resolved_main").unwrap();
    let database = CString::new("default").unwrap();
    let table_name = CString::new("test").unwrap();
    let branch = CString::new("main").unwrap();
    let schema = simple_table_schema();
    let schema_json = CString::new(serde_json::to_string(&schema).unwrap()).unwrap();

    unsafe {
        let result = paimon_table_from_schema_json(
            path.as_ptr(),
            schema_json.as_ptr(),
            database.as_ptr(),
            table_name.as_ptr(),
            branch.as_ptr(),
            std::ptr::null(),
            0,
        );

        assert!(result.error.is_null());
        assert!(!result.table.is_null());
        let table = table_ref(result.table);
        assert_eq!(table.branch(), "main");
        assert!(!table.is_branch_reference());
        assert_eq!(
            table.schema_manager().schema_path(schema.id()),
            "memory:/test_resolved_main/schema/schema-0"
        );

        paimon_table_free(result.table);
    }
}

#[test]
fn test_table_from_schema_json_null_branch_defaults_to_main() {
    let path = CString::new("memory:/test_resolved_null_branch").unwrap();
    let database = CString::new("default").unwrap();
    let table_name = CString::new("test").unwrap();
    let schema = simple_table_schema();
    let schema_json = CString::new(serde_json::to_string(&schema).unwrap()).unwrap();

    unsafe {
        let result = paimon_table_from_schema_json(
            path.as_ptr(),
            schema_json.as_ptr(),
            database.as_ptr(),
            table_name.as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            0,
        );

        assert!(result.error.is_null());
        assert!(!result.table.is_null());
        let table = table_ref(result.table);
        assert_eq!(table.branch(), "main");
        assert!(!table.is_branch_reference());
        assert_eq!(
            table.schema_manager().schema_path(schema.id()),
            "memory:/test_resolved_null_branch/schema/schema-0"
        );

        paimon_table_free(result.table);
    }
}

#[test]
fn test_table_from_schema_json_rejects_invalid_input() {
    let path = CString::new("memory:/test_resolved_invalid").unwrap();
    let malformed_schema = CString::new("not-json").unwrap();
    let database = CString::new("default").unwrap();
    let table_name = CString::new("test").unwrap();
    let branch = CString::new("main").unwrap();

    unsafe {
        let malformed = paimon_table_from_schema_json(
            path.as_ptr(),
            malformed_schema.as_ptr(),
            database.as_ptr(),
            table_name.as_ptr(),
            branch.as_ptr(),
            std::ptr::null(),
            0,
        );
        assert!(malformed.table.is_null());
        assert!(!malformed.error.is_null());
        assert_eq!(
            (*malformed.error).code,
            PaimonErrorCode::InvalidInput as i32
        );
        paimon_error_free(malformed.error);

        let schema = simple_table_schema();
        let schema_json = CString::new(serde_json::to_string(&schema).unwrap()).unwrap();
        let null_path = paimon_table_from_schema_json(
            std::ptr::null(),
            schema_json.as_ptr(),
            database.as_ptr(),
            table_name.as_ptr(),
            branch.as_ptr(),
            std::ptr::null(),
            0,
        );
        assert!(null_path.table.is_null());
        assert!(!null_path.error.is_null());
        assert_eq!(
            (*null_path.error).code,
            PaimonErrorCode::InvalidInput as i32
        );
        paimon_error_free(null_path.error);

        let null_options = paimon_table_from_schema_json(
            path.as_ptr(),
            schema_json.as_ptr(),
            database.as_ptr(),
            table_name.as_ptr(),
            branch.as_ptr(),
            std::ptr::null(),
            1,
        );
        assert!(null_options.table.is_null());
        assert!(!null_options.error.is_null());
        assert_eq!(
            (*null_options.error).code,
            PaimonErrorCode::InvalidInput as i32
        );
        paimon_error_free(null_options.error);

        let invalid_branch = CString::new("../dev").unwrap();
        let unsafe_branch = paimon_table_from_schema_json(
            path.as_ptr(),
            schema_json.as_ptr(),
            database.as_ptr(),
            table_name.as_ptr(),
            invalid_branch.as_ptr(),
            std::ptr::null(),
            0,
        );
        assert!(unsafe_branch.table.is_null());
        assert!(!unsafe_branch.error.is_null());
        assert_eq!(
            (*unsafe_branch.error).code,
            PaimonErrorCode::InvalidInput as i32
        );
        paimon_error_free(unsafe_branch.error);
    }
}

#[test]
fn test_table_from_schema_json_rejects_invalid_identifier() {
    let path = CString::new("memory:/test_resolved_bad_ident").unwrap();
    let schema = simple_table_schema();
    let schema_json = CString::new(serde_json::to_string(&schema).unwrap()).unwrap();
    let branch = CString::new("main").unwrap();
    // Path separators are rejected by Identifier::validate.
    let database = CString::new("default").unwrap();
    let table_name = CString::new("nested/table").unwrap();

    unsafe {
        let result = paimon_table_from_schema_json(
            path.as_ptr(),
            schema_json.as_ptr(),
            database.as_ptr(),
            table_name.as_ptr(),
            branch.as_ptr(),
            std::ptr::null(),
            0,
        );
        assert!(result.table.is_null());
        assert!(!result.error.is_null());
        assert_eq!((*result.error).code, PaimonErrorCode::InvalidInput as i32);
        paimon_error_free(result.error);
    }
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
fn test_fixed_commit_rejects_mismatched_overwrite_mode() {
    let path = "memory:/test_fixed_commit_overwrite_mode";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io.clone(),
        Identifier::new("default", "test"),
        path.to_string(),
        partitioned_postpone_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };
    let commit_user = CString::new("fixed-overwrite-mode-job").unwrap();

    unsafe {
        let append_wb = paimon_table_new_postpone_fixed_bucket_write_builder_with_commit_user(
            handle,
            commit_user.as_ptr(),
        )
        .write_builder;
        let (array, schema) =
            export_batch_to_ffi(make_postpone_bucket_plan_batch(vec!["p"], vec![1]));
        assert!(paimon_postpone_fixed_bucket_write_builder_with_bucket_plan(
            append_wb,
            (&**array) as *const FFI_ArrowArray as *mut c_void,
            (&**schema) as *const FFI_ArrowSchema as *mut c_void,
        )
        .is_null());
        let tw = paimon_postpone_fixed_bucket_write_builder_new_write(append_wb).write;
        let (array, schema) = export_batch_to_ffi(make_partitioned_write_batch(
            vec!["p"],
            vec![1],
            vec!["append"],
        ));
        assert!(paimon_postpone_fixed_bucket_table_write_write_arrow_batch(
            tw,
            (&**array) as *const FFI_ArrowArray as *mut c_void,
            (&**schema) as *const FFI_ArrowSchema as *mut c_void,
        )
        .is_null());
        let messages = paimon_postpone_fixed_bucket_table_write_prepare_commit(tw).messages;

        let overwrite_wb = paimon_table_new_postpone_fixed_bucket_write_builder_with_commit_user(
            handle,
            commit_user.as_ptr(),
        )
        .write_builder;
        assert!(paimon_postpone_fixed_bucket_write_builder_with_overwrite(overwrite_wb).is_null());
        let commit = paimon_postpone_fixed_bucket_write_builder_new_commit(overwrite_wb).commit;
        let error = paimon_postpone_fixed_bucket_table_commit_commit(commit, messages);
        assert!(!error.is_null());
        assert!(error_message(error).contains("different overwrite mode"));
        paimon_error_free(error);

        assert!(crate::runtime()
            .block_on(SnapshotManager::new(file_io, path.to_string()).get_latest_snapshot())
            .unwrap()
            .is_none());

        paimon_postpone_fixed_bucket_table_commit_free(commit);
        paimon_postpone_fixed_bucket_write_builder_free(overwrite_wb);
        paimon_postpone_fixed_bucket_commit_messages_free(messages);
        paimon_postpone_fixed_bucket_table_write_free(tw);
        paimon_postpone_fixed_bucket_write_builder_free(append_wb);
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
fn test_stream_write_v1_reuses_writer_across_monotonic_checkpoints() {
    let path = "memory:/test_stream_write_v1_reuses_writer";
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
    let commit_user = CString::new("stream-write-job-9").unwrap();

    unsafe {
        let wb_result =
            paimon_table_new_write_builder_with_commit_user(handle, commit_user.as_ptr());
        assert!(wb_result.error.is_null());
        let wb = wb_result.write_builder;

        let tw_result = paimon_write_builder_new_write(wb);
        assert!(tw_result.error.is_null());
        let tw = tw_result.write;

        let commit_result = paimon_write_builder_new_commit(wb);
        assert!(commit_result.error.is_null());
        let commit = commit_result.commit;

        // Checkpoint 100: retain the prepared messages until a successful
        // filter-and-commit confirms an intentionally lost commit ACK.
        let (array, schema) = export_batch_to_ffi(make_batch(vec![1], vec!["first"]));
        let error = paimon_table_write_write_arrow_batch(
            tw,
            (&**array) as *const FFI_ArrowArray as *mut c_void,
            (&**schema) as *const FFI_ArrowSchema as *mut c_void,
        );
        assert!(error.is_null());

        let prepared_100 = paimon_table_write_prepare_commit(tw);
        assert!(prepared_100.error.is_null());
        let error = paimon_table_commit_commit_with_identifier(commit, prepared_100.messages, 100);
        assert!(error.is_null());

        let error = paimon_table_commit_filter_and_commit_with_identifier(
            commit,
            prepared_100.messages,
            100,
        );
        assert!(
            error.is_null(),
            "a retry after a lost commit ACK must be idempotent"
        );
        paimon_commit_messages_free(prepared_100.messages);

        // Checkpoint 101 deliberately reuses both the writer and committer.
        // prepare_commit must drain only the data written since checkpoint 100.
        let (array, schema) = export_batch_to_ffi(make_batch(vec![2], vec!["second"]));
        let error = paimon_table_write_write_arrow_batch(
            tw,
            (&**array) as *const FFI_ArrowArray as *mut c_void,
            (&**schema) as *const FFI_ArrowSchema as *mut c_void,
        );
        assert!(error.is_null());

        let prepared_101 = paimon_table_write_prepare_commit(tw);
        assert!(prepared_101.error.is_null());
        let error = paimon_table_commit_commit_with_identifier(commit, prepared_101.messages, 101);
        assert!(error.is_null());
        paimon_commit_messages_free(prepared_101.messages);

        assert_eq!(
            read_rows_ffi(handle),
            vec![(1, "first".into()), (2, "second".into())]
        );

        // A later prepared checkpoint can be abandoned without publishing a
        // snapshot or making its rows visible.
        let (array, schema) = export_batch_to_ffi(make_batch(vec![3], vec!["aborted"]));
        let error = paimon_table_write_write_arrow_batch(
            tw,
            (&**array) as *const FFI_ArrowArray as *mut c_void,
            (&**schema) as *const FFI_ArrowSchema as *mut c_void,
        );
        assert!(error.is_null());

        let abandoned = paimon_table_write_prepare_commit(tw);
        assert!(abandoned.error.is_null());
        let abandoned_prepared = paimon_commit_messages_prepare(abandoned.messages, 102);
        assert!(abandoned_prepared.error.is_null());
        paimon_commit_messages_free(abandoned.messages);
        let error = paimon_table_commit_abort_prepared(commit, abandoned_prepared.prepared);
        assert!(error.is_null());
        paimon_prepared_commit_free(abandoned_prepared.prepared);

        assert_eq!(
            read_rows_ffi(handle),
            vec![(1, "first".into()), (2, "second".into())]
        );

        paimon_table_commit_free(commit);
        paimon_table_write_free(tw);
        paimon_write_builder_free(wb);
        unwrap_table(handle);
    }

    let snapshots = crate::runtime().block_on(async {
        let manager = SnapshotManager::new(file_io, path.to_string());
        (
            manager.get_snapshot(1).await.unwrap(),
            manager.get_snapshot(2).await.unwrap(),
            manager.get_latest_snapshot_id().await.unwrap(),
        )
    });
    assert_eq!(snapshots.0.commit_user(), "stream-write-job-9");
    assert_eq!(snapshots.0.commit_identifier(), 100);
    assert_eq!(snapshots.1.commit_user(), "stream-write-job-9");
    assert_eq!(snapshots.1.commit_identifier(), 101);
    assert_eq!(
        snapshots.2,
        Some(2),
        "the retry and abort must not publish snapshots"
    );
}

#[test]
fn test_prepared_commit_roundtrip_and_lost_ack_retry() {
    let path = "memory:/test_prepared_commit_roundtrip";
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
    let commit_user = CString::new("durable-stream-job-5").unwrap();

    unsafe {
        let writer_builder =
            paimon_table_new_write_builder_with_commit_user(handle, commit_user.as_ptr());
        assert!(writer_builder.error.is_null());
        let writer_builder = writer_builder.write_builder;

        let writer = paimon_write_builder_new_write(writer_builder);
        assert!(writer.error.is_null());
        let writer = writer.write;

        let (array, schema) = export_batch_to_ffi(make_batch(vec![5], vec!["durable"]));
        let error = paimon_table_write_write_arrow_batch(
            writer,
            (&**array) as *const FFI_ArrowArray as *mut c_void,
            (&**schema) as *const FFI_ArrowSchema as *mut c_void,
        );
        assert!(error.is_null());

        let messages = paimon_table_write_prepare_commit(writer);
        assert!(messages.error.is_null());
        let reserved = paimon_commit_messages_prepare(messages.messages, i64::MAX);
        assert!(reserved.prepared.is_null());
        assert!(!reserved.error.is_null());
        assert_eq!((*reserved.error).code, PaimonErrorCode::InvalidInput as i32);
        paimon_error_free(reserved.error);
        let prepared = paimon_commit_messages_prepare(messages.messages, 500);
        assert!(prepared.error.is_null());
        assert_eq!(paimon_prepared_commit_identifier(prepared.prepared), 500);

        let serialized = paimon_prepared_commit_serialize(prepared.prepared);
        assert!(serialized.error.is_null());
        assert!(!serialized.bytes.data.is_null());
        assert!(serialized.bytes.len > 0);

        let mut unsafe_checkpoint: serde_json::Value = serde_json::from_slice(
            std::slice::from_raw_parts(serialized.bytes.data, serialized.bytes.len),
        )
        .unwrap();
        unsafe_checkpoint["messages"][0]["new_files"][0]["_EXTERNAL_PATH"] =
            serde_json::json!("file:/tmp/not-owned-by-the-prepared-commit");
        let unsafe_checkpoint = serde_json::to_vec(&unsafe_checkpoint).unwrap();
        let rejected =
            paimon_prepared_commit_deserialize(unsafe_checkpoint.as_ptr(), unsafe_checkpoint.len());
        assert!(rejected.prepared.is_null());
        assert!(!rejected.error.is_null());
        assert_eq!((*rejected.error).code, PaimonErrorCode::InvalidInput as i32);
        paimon_error_free(rejected.error);

        let mut duplicated_checkpoint: serde_json::Value = serde_json::from_slice(
            std::slice::from_raw_parts(serialized.bytes.data, serialized.bytes.len),
        )
        .unwrap();
        let duplicate_message = duplicated_checkpoint["messages"][0].clone();
        duplicated_checkpoint["messages"]
            .as_array_mut()
            .unwrap()
            .push(duplicate_message);
        let duplicated_checkpoint = serde_json::to_vec(&duplicated_checkpoint).unwrap();

        let mut conflicting_checkpoint: serde_json::Value = serde_json::from_slice(
            std::slice::from_raw_parts(serialized.bytes.data, serialized.bytes.len),
        )
        .unwrap();
        let mut conflicting_message = conflicting_checkpoint["messages"][0].clone();
        conflicting_message["new_files"][0]["_FILE_SIZE"] = serde_json::json!(123456789);
        conflicting_checkpoint["messages"]
            .as_array_mut()
            .unwrap()
            .push(conflicting_message);
        let conflicting_checkpoint = serde_json::to_vec(&conflicting_checkpoint).unwrap();
        let rejected = paimon_prepared_commit_deserialize(
            conflicting_checkpoint.as_ptr(),
            conflicting_checkpoint.len(),
        );
        assert!(rejected.prepared.is_null());
        assert!(!rejected.error.is_null());
        assert!(error_message(rejected.error).contains("same file identity"));
        paimon_error_free(rejected.error);

        // The serialized bytes, rather than either in-process source handle,
        // are the durable checkpoint boundary.
        paimon_commit_messages_free(messages.messages);
        paimon_prepared_commit_free(prepared.prepared);

        let restored = paimon_prepared_commit_deserialize(
            duplicated_checkpoint.as_ptr(),
            duplicated_checkpoint.len(),
        );
        assert!(restored.error.is_null());
        assert_eq!(paimon_prepared_commit_identifier(restored.prepared), 500);

        let first_committer_builder =
            paimon_table_new_write_builder_with_commit_user(handle, commit_user.as_ptr());
        assert!(first_committer_builder.error.is_null());
        let first_committer_builder = first_committer_builder.write_builder;
        let first_committer = paimon_write_builder_new_commit(first_committer_builder);
        assert!(first_committer.error.is_null());
        let error = paimon_table_commit_commit_prepared(first_committer.commit, restored.prepared);
        assert!(error.is_null());

        // A stale abort request after a successful commit (including a lost
        // acknowledgement recovered by identifier) must not delete files now
        // referenced by the committed snapshot.
        let error = paimon_table_commit_abort_prepared(first_committer.commit, restored.prepared);
        assert!(error.is_null());
        assert_eq!(read_rows_ffi(handle), vec![(5, "durable".into())]);

        // Treat the successful return above as a lost ACK. Discard all
        // in-memory commit state, recover from the same durable bytes, and
        // retry through the identifier-filtering commit path.
        paimon_prepared_commit_free(restored.prepared);
        paimon_table_commit_free(first_committer.commit);
        paimon_write_builder_free(first_committer_builder);

        let retry = paimon_prepared_commit_deserialize(
            serialized.bytes.data.cast_const(),
            serialized.bytes.len,
        );
        assert!(retry.error.is_null());
        let retry_committer_builder =
            paimon_table_new_write_builder_with_commit_user(handle, commit_user.as_ptr());
        assert!(retry_committer_builder.error.is_null());
        let retry_committer_builder = retry_committer_builder.write_builder;
        let retry_committer = paimon_write_builder_new_commit(retry_committer_builder);
        assert!(retry_committer.error.is_null());
        let error = paimon_table_commit_commit_prepared(retry_committer.commit, retry.prepared);
        assert!(
            error.is_null(),
            "recovered commit_prepared must filter a previously committed identifier"
        );

        paimon_prepared_commit_free(retry.prepared);
        paimon_bytes_free(serialized.bytes);
        paimon_table_commit_free(retry_committer.commit);
        paimon_write_builder_free(retry_committer_builder);

        assert_eq!(read_rows_ffi(handle), vec![(5, "durable".into())]);

        paimon_table_write_free(writer);
        paimon_write_builder_free(writer_builder);
        unwrap_table(handle);
    }

    let snapshot = crate::runtime()
        .block_on(SnapshotManager::new(file_io, path.to_string()).get_latest_snapshot())
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.id(), 1, "the lost-ACK retry must be a no-op");
    assert_eq!(snapshot.commit_user(), "durable-stream-job-5");
    assert_eq!(snapshot.commit_identifier(), 500);
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
        let err = paimon_commit_messages_merge(messages1, messages2);
        assert!(
            err.is_null(),
            "re-merging the same fragment must be a no-op"
        );

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
fn test_prepared_commit_merge_preserves_parallel_writer_files() {
    let path = "memory:/test_prepared_commit_merge";
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
    let commit_user = CString::new("durable-distributed-job-700").unwrap();

    unsafe {
        let wb1 = paimon_table_new_write_builder_with_commit_user(handle, commit_user.as_ptr())
            .write_builder;
        let wb2 = paimon_table_new_write_builder_with_commit_user(handle, commit_user.as_ptr())
            .write_builder;
        let tw1 = paimon_write_builder_new_write(wb1).write;
        let tw2 = paimon_write_builder_new_write(wb2).write;

        for (writer, ids, names) in [
            (tw1, vec![10], vec!["left"]),
            (tw2, vec![20], vec!["right"]),
        ] {
            let (array, schema) = export_batch_to_ffi(make_batch(ids, names));
            let error = paimon_table_write_write_arrow_batch(
                writer,
                (&**array) as *const FFI_ArrowArray as *mut c_void,
                (&**schema) as *const FFI_ArrowSchema as *mut c_void,
            );
            assert!(error.is_null());
        }

        let messages1 = paimon_table_write_prepare_commit(tw1);
        assert!(messages1.error.is_null());
        let messages2 = paimon_table_write_prepare_commit(tw2);
        assert!(messages2.error.is_null());
        let prepared1 = paimon_commit_messages_prepare(messages1.messages, 700);
        assert!(prepared1.error.is_null());
        let prepared2 = paimon_commit_messages_prepare(messages2.messages, 700);
        assert!(prepared2.error.is_null());
        paimon_commit_messages_free(messages2.messages);
        paimon_commit_messages_free(messages1.messages);

        let error = paimon_prepared_commit_merge(prepared1.prepared, prepared2.prepared);
        assert!(error.is_null());
        let error = paimon_prepared_commit_merge(prepared1.prepared, prepared2.prepared);
        assert!(
            error.is_null(),
            "re-merging the same durable fragment must be a no-op"
        );
        let commit = paimon_write_builder_new_commit(wb1);
        assert!(commit.error.is_null());
        let error = paimon_table_commit_commit_prepared(commit.commit, prepared1.prepared);
        assert!(error.is_null());

        assert_eq!(
            read_rows_ffi(handle),
            vec![(10, "left".into()), (20, "right".into())]
        );

        paimon_table_commit_free(commit.commit);
        paimon_prepared_commit_free(prepared2.prepared);
        paimon_prepared_commit_free(prepared1.prepared);
        paimon_table_write_free(tw2);
        paimon_table_write_free(tw1);
        paimon_write_builder_free(wb2);
        paimon_write_builder_free(wb1);
        unwrap_table(handle);
    }
}

#[test]
fn test_postpone_bucket_plan_arrow_ownership_on_errors() {
    let path = "memory:/test_postpone_bucket_plan_arrow_ownership";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        partitioned_postpone_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };

    unsafe {
        let (mut array, mut schema) =
            export_batch_to_ffi(make_postpone_bucket_plan_batch(vec!["p"], vec![1]));
        let error = paimon_postpone_fixed_bucket_write_builder_with_bucket_plan(
            ptr::null_mut(),
            (&**array) as *const FFI_ArrowArray as *mut c_void,
            (&**schema) as *const FFI_ArrowSchema as *mut c_void,
        );
        assert!(!error.is_null());
        assert!(!array.is_released());
        assert!(schema.release.is_some());
        paimon_error_free(error);
        ManuallyDrop::drop(array.as_mut());
        ManuallyDrop::drop(schema.as_mut());

        let fixed = paimon_table_new_postpone_fixed_bucket_write_builder(handle).write_builder;
        let (array, schema) =
            export_batch_to_ffi(make_postpone_bucket_plan_batch(vec!["p"], vec![0]));
        let error = paimon_postpone_fixed_bucket_write_builder_with_bucket_plan(
            fixed,
            (&**array) as *const FFI_ArrowArray as *mut c_void,
            (&**schema) as *const FFI_ArrowSchema as *mut c_void,
        );
        assert!(!error.is_null());
        assert!(array.is_released());
        assert!(schema.release.is_none());
        paimon_error_free(error);

        paimon_postpone_fixed_bucket_write_builder_free(fixed);
        unwrap_table(handle);
    }
}

#[test]
fn test_distributed_postpone_writers_share_bucket_plan() {
    let path = "memory:/test_distributed_postpone_bucket_plan";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        partitioned_postpone_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };
    let commit_user = CString::new("distributed-postpone-job").unwrap();

    unsafe {
        let normal = paimon_table_new_write_builder(handle);
        assert!(normal.error.is_null());
        assert!(!normal.write_builder.is_null());
        paimon_write_builder_free(normal.write_builder);

        let fixed = paimon_table_new_postpone_fixed_bucket_write_builder(handle);
        assert!(fixed.error.is_null());
        assert!(!fixed.write_builder.is_null());
        let write = paimon_postpone_fixed_bucket_write_builder_new_write(fixed.write_builder);
        assert!(write.write.is_null());
        assert!(error_message(write.error).contains("bucket plan is required"));
        paimon_error_free(write.error);
        paimon_postpone_fixed_bucket_write_builder_free(fixed.write_builder);

        let wb1 = paimon_table_new_postpone_fixed_bucket_write_builder_with_commit_user(
            handle,
            commit_user.as_ptr(),
        )
        .write_builder;
        let wb2 = paimon_table_new_postpone_fixed_bucket_write_builder_with_commit_user(
            handle,
            commit_user.as_ptr(),
        )
        .write_builder;

        for wb in [wb1, wb2] {
            assert!(paimon_postpone_fixed_bucket_write_builder_with_overwrite(wb).is_null());
            let (array, schema) = export_batch_to_ffi(make_postpone_bucket_plan_batch(
                vec!["p1", "p2"],
                vec![3, 3],
            ));
            let error = paimon_postpone_fixed_bucket_write_builder_with_bucket_plan(
                wb,
                (&**array) as *const FFI_ArrowArray as *mut c_void,
                (&**schema) as *const FFI_ArrowSchema as *mut c_void,
            );
            assert!(error.is_null());
        }

        let tw1 = paimon_postpone_fixed_bucket_write_builder_new_write(wb1).write;
        let tw2 = paimon_postpone_fixed_bucket_write_builder_new_write(wb2).write;
        for (tw, partitions, ids, names) in [
            (tw1, vec!["p1"], vec![1], vec!["a"]),
            (
                tw2,
                vec!["p2", "p2", "p2", "p2"],
                vec![2, 3, 4, 5],
                vec!["b", "c", "d", "e"],
            ),
        ] {
            let (array, schema) =
                export_batch_to_ffi(make_partitioned_write_batch(partitions, ids, names));
            let error = paimon_postpone_fixed_bucket_table_write_write_arrow_batch(
                tw,
                (&**array) as *const FFI_ArrowArray as *mut c_void,
                (&**schema) as *const FFI_ArrowSchema as *mut c_void,
            );
            assert!(error.is_null());
        }

        let messages1 = paimon_postpone_fixed_bucket_table_write_prepare_commit(tw1).messages;
        let messages2 = paimon_postpone_fixed_bucket_table_write_prepare_commit(tw2).messages;
        for messages in [messages1, messages2] {
            let state = &*((*messages).inner as *const PostponeFixedBucketCommitMessagesState);
            assert!(!state.messages.is_empty());
            assert!(state
                .messages
                .iter()
                .all(|message| message.total_buckets == Some(3)));
        }
        let error = paimon_postpone_fixed_bucket_commit_messages_merge(messages1, messages2);
        assert!(error.is_null());
        let commit = paimon_postpone_fixed_bucket_write_builder_new_commit(wb1).commit;
        let error = paimon_postpone_fixed_bucket_table_commit_commit(commit, messages1);
        assert!(error.is_null());
        let snapshot = crate::runtime()
            .block_on(
                SnapshotManager::new(table_ref(handle).file_io().clone(), path.to_string())
                    .get_latest_snapshot(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.commit_kind(), &CommitKind::OVERWRITE);

        paimon_postpone_fixed_bucket_table_commit_free(commit);
        paimon_postpone_fixed_bucket_commit_messages_free(messages2);
        paimon_postpone_fixed_bucket_commit_messages_free(messages1);
        paimon_postpone_fixed_bucket_table_write_free(tw2);
        paimon_postpone_fixed_bucket_table_write_free(tw1);
        paimon_postpone_fixed_bucket_write_builder_free(wb2);
        paimon_postpone_fixed_bucket_write_builder_free(wb1);
        unwrap_table(handle);
    }
}

#[test]
fn test_distributed_postpone_writers_reject_overlapping_bucket_ownership() {
    let path = "memory:/test_distributed_postpone_overlapping_ownership";
    let file_io = memory_file_io();
    setup_table_dirs(&file_io, path);
    let table = Table::new(
        file_io,
        Identifier::new("default", "test"),
        path.to_string(),
        partitioned_postpone_table_schema(),
        None,
    );
    let handle = unsafe { wrap_table(table) };
    let commit_user = CString::new("overlapping-postpone-job").unwrap();

    unsafe {
        let wb1 = paimon_table_new_postpone_fixed_bucket_write_builder_with_commit_user(
            handle,
            commit_user.as_ptr(),
        )
        .write_builder;
        let wb2 = paimon_table_new_postpone_fixed_bucket_write_builder_with_commit_user(
            handle,
            commit_user.as_ptr(),
        )
        .write_builder;
        for wb in [wb1, wb2] {
            let (array, schema) =
                export_batch_to_ffi(make_postpone_bucket_plan_batch(vec!["p"], vec![1]));
            assert!(paimon_postpone_fixed_bucket_write_builder_with_bucket_plan(
                wb,
                (&**array) as *const FFI_ArrowArray as *mut c_void,
                (&**schema) as *const FFI_ArrowSchema as *mut c_void,
            )
            .is_null());
        }

        let tw1 = paimon_postpone_fixed_bucket_write_builder_new_write(wb1).write;
        let tw2 = paimon_postpone_fixed_bucket_write_builder_new_write(wb2).write;
        for (tw, name) in [(tw1, "first"), (tw2, "second")] {
            let (array, schema) =
                export_batch_to_ffi(make_partitioned_write_batch(vec!["p"], vec![1], vec![name]));
            assert!(paimon_postpone_fixed_bucket_table_write_write_arrow_batch(
                tw,
                (&**array) as *const FFI_ArrowArray as *mut c_void,
                (&**schema) as *const FFI_ArrowSchema as *mut c_void,
            )
            .is_null());
        }

        let messages1 = paimon_postpone_fixed_bucket_table_write_prepare_commit(tw1).messages;
        let messages2 = paimon_postpone_fixed_bucket_table_write_prepare_commit(tw2).messages;
        let error = paimon_postpone_fixed_bucket_commit_messages_merge(messages1, messages2);
        assert!(error.is_null());
        let commit = paimon_postpone_fixed_bucket_write_builder_new_commit(wb1).commit;
        let error = paimon_postpone_fixed_bucket_table_commit_commit(commit, messages1);
        assert!(!error.is_null());
        assert!(error_message(error).contains("writer ownership conflict for bucket 0"));
        paimon_error_free(error);

        paimon_postpone_fixed_bucket_table_commit_free(commit);
        paimon_postpone_fixed_bucket_commit_messages_free(messages2);
        paimon_postpone_fixed_bucket_commit_messages_free(messages1);
        paimon_postpone_fixed_bucket_table_write_free(tw2);
        paimon_postpone_fixed_bucket_table_write_free(tw1);
        paimon_postpone_fixed_bucket_write_builder_free(wb2);
        paimon_postpone_fixed_bucket_write_builder_free(wb1);
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
        for invalid in [-1, i64::MAX] {
            let err = paimon_table_commit_commit_with_identifier(tc, pc_result.messages, invalid);
            assert!(!err.is_null());
            assert_eq!((*err).code, PaimonErrorCode::InvalidInput as i32);
            paimon_error_free(err);
            let err = paimon_table_commit_truncate_table_with_identifier(tc, invalid);
            assert!(!err.is_null());
            assert_eq!((*err).code, PaimonErrorCode::InvalidInput as i32);
            paimon_error_free(err);
        }

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

        let result = paimon_table_new_postpone_fixed_bucket_write_builder(ptr::null());
        assert!(!result.error.is_null());
        assert!(result.write_builder.is_null());
        paimon_error_free(result.error);

        let result = paimon_write_builder_new_write(ptr::null());
        assert!(!result.error.is_null());
        assert!(result.write.is_null());
        paimon_error_free(result.error);

        let result = paimon_postpone_fixed_bucket_write_builder_new_write(ptr::null());
        assert!(!result.error.is_null());
        assert!(result.write.is_null());
        paimon_error_free(result.error);

        let result = paimon_write_builder_new_commit(ptr::null());
        assert!(!result.error.is_null());
        assert!(result.commit.is_null());
        paimon_error_free(result.error);

        let result = paimon_postpone_fixed_bucket_write_builder_new_commit(ptr::null());
        assert!(!result.error.is_null());
        assert!(result.commit.is_null());
        paimon_error_free(result.error);

        let err = paimon_postpone_fixed_bucket_write_builder_with_bucket_plan(
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
        );
        assert!(!err.is_null());
        paimon_error_free(err);

        let err =
            paimon_table_write_write_arrow_batch(ptr::null_mut(), ptr::null_mut(), ptr::null_mut());
        assert!(!err.is_null());
        paimon_error_free(err);

        let err = paimon_postpone_fixed_bucket_table_write_write_arrow_batch(
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
        );
        assert!(!err.is_null());
        paimon_error_free(err);

        let result = paimon_table_write_prepare_commit(ptr::null_mut());
        assert!(!result.error.is_null());
        assert!(result.messages.is_null());
        paimon_error_free(result.error);

        let result = paimon_postpone_fixed_bucket_table_write_prepare_commit(ptr::null_mut());
        assert!(!result.error.is_null());
        assert!(result.messages.is_null());
        paimon_error_free(result.error);

        let err = paimon_table_commit_commit(ptr::null(), ptr::null_mut());
        assert!(!err.is_null());
        paimon_error_free(err);

        let err = paimon_postpone_fixed_bucket_table_commit_commit(ptr::null(), ptr::null_mut());
        assert!(!err.is_null());
        paimon_error_free(err);

        paimon_write_builder_free(ptr::null_mut());
        paimon_table_write_free(ptr::null_mut());
        paimon_table_commit_free(ptr::null_mut());
        paimon_commit_messages_free(ptr::null_mut());
        paimon_postpone_fixed_bucket_write_builder_free(ptr::null_mut());
        paimon_postpone_fixed_bucket_table_write_free(ptr::null_mut());
        paimon_postpone_fixed_bucket_table_commit_free(ptr::null_mut());
        paimon_postpone_fixed_bucket_commit_messages_free(ptr::null_mut());
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
            file_size: i64::try_from(index_file_size).unwrap(),
            row_count,
            deletion_vectors_ranges: None,
            external_path: None,
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

// =========================================================================
//  paimon_plan_from_split_bytes
// =========================================================================

#[test]
fn plan_from_split_bytes_round_trips() {
    let path = "memory:/test_plan_from_split_bytes";
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

    // Obtain a real DataSplit from a plan via the Rust API, then serialize it.
    let splits = crate::runtime().block_on(async {
        let rb = table.new_read_builder();
        let scan = rb.new_scan();
        scan.plan().await.unwrap().splits().to_vec()
    });
    assert!(!splits.is_empty(), "expected at least one planned split");
    let bytes = splits[0].serialize().unwrap();

    let result = unsafe { paimon_plan_from_split_bytes(bytes.as_ptr(), bytes.len()) };
    assert!(result.error.is_null());
    assert_eq!(unsafe { paimon_plan_num_splits(result.plan) }, 1);
    unsafe { paimon_plan_free(result.plan) };
}

#[test]
fn plan_from_split_bytes_rejects_null_and_empty() {
    let r = unsafe { paimon_plan_from_split_bytes(std::ptr::null(), 0) };
    assert!(r.plan.is_null());
    assert!(!r.error.is_null());
    unsafe { paimon_error_free(r.error) };

    let dummy = [0u8; 1];
    let r2 = unsafe { paimon_plan_from_split_bytes(dummy.as_ptr(), 0) };
    assert!(r2.plan.is_null());
    assert!(!r2.error.is_null());
    unsafe { paimon_error_free(r2.error) };
}

#[test]
fn plan_from_split_bytes_rejects_garbage() {
    let garbage = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let r = unsafe { paimon_plan_from_split_bytes(garbage.as_ptr(), garbage.len()) };
    assert!(r.plan.is_null());
    assert!(!r.error.is_null());
    unsafe { paimon_error_free(r.error) };
}

// --- Vector-search projection through execute_read ------------------------

/// Core Rust reference: sorted output column names materialized by `execute_read`
/// under an optional projection.
fn rust_execute_read_column_names(
    table: &Table,
    column: &str,
    query: Vec<f32>,
    limit: usize,
    projection: Option<&[&str]>,
) -> Vec<String> {
    crate::runtime().block_on(async {
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_column(column)
            .with_query_vector(query)
            .with_limit(limit);
        if let Some(cols) = projection {
            builder.with_projection(cols);
        }
        let mut stream = builder.execute_read().await.unwrap();
        let mut names: Vec<String> = Vec::new();
        while let Some(b) = stream.try_next().await.unwrap() {
            names = b
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect();
        }
        names.sort();
        names
    })
}

/// C path: sorted output column names materialized by a configured builder's
/// `execute_read`. Frees the builder and reader.
unsafe fn c_execute_read_column_names(builder: *mut paimon_vector_search_builder) -> Vec<String> {
    let result = paimon_vector_search_builder_execute_read(builder);
    paimon_vector_search_builder_free(builder);
    assert!(result.error.is_null(), "execute_read should not error");
    assert!(!result.reader.is_null());

    let mut names: Vec<String> = Vec::new();
    loop {
        let next = paimon_record_batch_reader_next(result.reader);
        assert!(next.error.is_null());
        if next.batch.array.is_null() {
            break; // EOF
        }
        let batch = import_batch(&next.batch);
        names = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        paimon_arrow_batch_free(next.batch);
    }
    paimon_record_batch_reader_free(result.reader);
    names.sort();
    names
}

#[test]
fn vector_search_pk_projection_restricts_columns() {
    // Projecting a subset through execute_read must materialize only those user
    // columns plus the score column — the unprojected vector column is excluded.
    let path = "memory:/vsearch_pk_projection";
    let (query, vectors) = pk_fixture_smoke();
    let table = build_pk_vector_table(path, &vectors);

    // Reference: unprojected read materializes every user column (id, embedding);
    // projecting ["id"] yields id + score only. Sorted, `__paimon_search_score`
    // (leading underscore) precedes `id`.
    let full = rust_execute_read_column_names(&table, VECTOR_COLUMN, query.to_vec(), 3, None);
    assert!(
        full.contains(&"id".to_string()) && full.contains(&VECTOR_COLUMN.to_string()),
        "unprojected read must materialize every user column, got {full:?}"
    );
    let rust_projected =
        rust_execute_read_column_names(&table, VECTOR_COLUMN, query.to_vec(), 3, Some(&["id"]));
    assert_eq!(
        rust_projected,
        vec![SCORE_COLUMN.to_string(), "id".to_string()],
        "reference projection ['id'] must yield id + score only"
    );

    let handle = unsafe { wrap_table(table) };
    unsafe {
        let builder = c_vector_builder(handle, VECTOR_COLUMN, &query, 3, ptr::null_mut());
        let id = CString::new("id").unwrap();
        let cols = [id.as_ptr(), ptr::null()];
        assert!(
            paimon_vector_search_builder_with_projection(builder, cols.as_ptr()).is_null(),
            "with_projection must accept a valid column"
        );
        let c_names = c_execute_read_column_names(builder);
        assert_eq!(
            c_names, rust_projected,
            "C projected columns must match the Rust reference (id + score, no vector column)"
        );
        assert!(
            !c_names.contains(&VECTOR_COLUMN.to_string()),
            "projected read must exclude the unprojected vector column"
        );
        unwrap_table(handle);
    }
}

#[test]
fn vector_search_projection_unknown_column_errors_at_execute_read() {
    // The C setter defers column-name validation (the core vector builder's
    // with_projection is infallible). An unknown projected column must therefore
    // surface as an error from execute_read, not a silent empty/EOF reader.
    let path = "memory:/vsearch_pk_projection_unknown";
    let (query, vectors) = pk_fixture_smoke();
    let table = build_pk_vector_table(path, &vectors);
    let handle = unsafe { wrap_table(table) };
    unsafe {
        let builder = c_vector_builder(handle, VECTOR_COLUMN, &query, 3, ptr::null_mut());
        let bad = CString::new("does_not_exist").unwrap();
        let cols = [bad.as_ptr(), ptr::null()];
        // The setter itself accepts the name (validation is deferred).
        assert!(
            paimon_vector_search_builder_with_projection(builder, cols.as_ptr()).is_null(),
            "with_projection defers validation, so the setter must not error"
        );
        let result = paimon_vector_search_builder_execute_read(builder);
        paimon_vector_search_builder_free(builder);
        assert!(
            result.reader.is_null(),
            "an unknown projected column must not yield a reader"
        );
        assert!(
            !result.error.is_null(),
            "an unknown projected column must fail loud at execute_read"
        );
        paimon_error_free(result.error);
        unwrap_table(handle);
    }
}

#[test]
fn blob_reader_reads_batch_and_owns_output_buffers() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), b"abcdefghij").unwrap();
    let uri = url::Url::from_file_path(file.path()).unwrap().to_string();
    let mut descriptors = vec![
        BlobDescriptor::new(uri.clone(), 3, -1).serialize(),
        BlobDescriptor::new(uri.clone(), 1, 3).serialize(),
        BlobDescriptor::new(uri, 5, 0).serialize(),
    ];
    let slices = descriptors
        .iter()
        .map(|value| paimon_byte_slice {
            data: value.as_ptr(),
            len: value.len(),
        })
        .collect::<Vec<_>>();

    unsafe {
        let created = paimon_blob_reader_new(ptr::null(), 0);
        assert!(created.error.is_null());
        assert!(!created.reader.is_null());

        let result = paimon_blob_reader_read_blobs(created.reader, slices.as_ptr(), slices.len());
        assert!(result.error.is_null());
        assert_eq!(result.blobs.len, 3);

        descriptors.clear();
        paimon_blob_reader_free(created.reader);
        let values = std::slice::from_raw_parts(result.blobs.data, result.blobs.len)
            .iter()
            .map(|value| std::slice::from_raw_parts(value.data, value.len).to_vec())
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![b"defghij".to_vec(), b"bcd".to_vec(), Vec::new()]
        );
        paimon_bytes_array_free(result.blobs);
    }
}

#[test]
fn blob_reader_from_table_keeps_file_io_alive() {
    let file_io = memory_file_io();
    let uri = "memory:/blob_reader_from_table";
    crate::runtime().block_on(async {
        file_io
            .new_output(uri)
            .unwrap()
            .write(bytes::Bytes::from_static(b"abcdefghij"))
            .await
            .unwrap();
    });
    let table = Table::new(
        file_io,
        Identifier::new("default", "blob_table"),
        "memory:/blob_table".to_string(),
        simple_table_schema(),
        None,
    );
    let table = unsafe { wrap_table(table) };
    let descriptor = BlobDescriptor::new(uri.to_string(), 2, 4).serialize();
    let descriptor_slice = paimon_byte_slice {
        data: descriptor.as_ptr(),
        len: descriptor.len(),
    };

    unsafe {
        let created = paimon_table_new_blob_reader(table);
        assert!(created.error.is_null());
        assert!(!created.reader.is_null());
        unwrap_table(table);

        let result = paimon_blob_reader_read_blobs(created.reader, &descriptor_slice, 1);
        assert!(result.error.is_null());
        let values = std::slice::from_raw_parts(result.blobs.data, result.blobs.len);
        assert_eq!(
            std::slice::from_raw_parts(values[0].data, values[0].len),
            b"cdef"
        );

        paimon_bytes_array_free(result.blobs);
        paimon_blob_reader_free(created.reader);
    }
}

#[test]
fn blob_reader_handles_empty_and_error_batches() {
    unsafe {
        let null_table = paimon_table_new_blob_reader(ptr::null());
        assert!(null_table.reader.is_null());
        assert!(!null_table.error.is_null());
        paimon_error_free(null_table.error);

        let created = paimon_blob_reader_new(ptr::null(), 0);
        assert!(created.error.is_null());

        let empty = paimon_blob_reader_read_blobs(created.reader, ptr::null(), 0);
        assert!(empty.error.is_null());
        assert!(empty.blobs.data.is_null());
        assert_eq!(empty.blobs.len, 0);
        paimon_bytes_array_free(empty.blobs);

        let invalid_bytes = [0_u8; 1];
        let invalid_slice = paimon_byte_slice {
            data: invalid_bytes.as_ptr(),
            len: invalid_bytes.len(),
        };
        let invalid = paimon_blob_reader_read_blobs(created.reader, &invalid_slice, 1);
        assert!(!invalid.error.is_null());
        assert!(invalid.blobs.data.is_null());
        paimon_error_free(invalid.error);

        let null_slice = paimon_byte_slice {
            data: ptr::null(),
            len: 1,
        };
        let null_data = paimon_blob_reader_read_blobs(created.reader, &null_slice, 1);
        assert!(!null_data.error.is_null());
        assert!(null_data.blobs.data.is_null());
        paimon_error_free(null_data.error);

        paimon_blob_reader_free(created.reader);
        paimon_blob_reader_free(ptr::null_mut());
    }
}

#[test]
fn blob_stream_reads_chunks_and_outlives_reader() {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), b"abcdefghij").unwrap();
    let uri = url::Url::from_file_path(file.path()).unwrap().to_string();
    let descriptor = BlobDescriptor::new(uri, 2, 5).serialize();

    unsafe {
        let created = paimon_blob_reader_new(ptr::null(), 0);
        assert!(created.error.is_null());
        let opened =
            paimon_blob_reader_open_blob(created.reader, descriptor.as_ptr(), descriptor.len());
        assert!(opened.error.is_null());
        assert!(!opened.stream.is_null());
        paimon_blob_reader_free(created.reader);

        let mut buffer = [0xFF_u8; 3];
        let first = paimon_blob_stream_read(opened.stream, buffer.as_mut_ptr(), buffer.len());
        assert!(first.error.is_null());
        assert_eq!(first.bytes_read, 3);
        assert_eq!(&buffer, b"cde");

        let seek = paimon_blob_stream_seek(opened.stream, -2, 2);
        assert!(seek.error.is_null());
        assert_eq!(seek.position, 3);

        buffer.fill(0xFF);
        let second = paimon_blob_stream_read(opened.stream, buffer.as_mut_ptr(), buffer.len());
        assert!(second.error.is_null());
        assert_eq!(second.bytes_read, 2);
        assert_eq!(&buffer[..2], b"fg");
        assert_eq!(buffer[2], 0xFF);

        let end = paimon_blob_stream_read(opened.stream, buffer.as_mut_ptr(), buffer.len());
        assert!(end.error.is_null());
        assert_eq!(end.bytes_read, 0);

        paimon_blob_stream_free(opened.stream);
        paimon_blob_stream_free(ptr::null_mut());
    }
}

#[test]
fn blob_stream_validates_handles_and_buffers() {
    unsafe {
        let null_reader = paimon_blob_reader_open_blob(ptr::null(), ptr::null(), 0);
        assert!(null_reader.stream.is_null());
        assert!(!null_reader.error.is_null());
        paimon_error_free(null_reader.error);

        let created = paimon_blob_reader_new(ptr::null(), 0);
        let invalid = paimon_blob_reader_open_blob(created.reader, ptr::null(), 0);
        assert!(invalid.stream.is_null());
        assert!(!invalid.error.is_null());
        paimon_error_free(invalid.error);

        let file = tempfile::NamedTempFile::new().unwrap();
        let uri = url::Url::from_file_path(file.path()).unwrap().to_string();
        let descriptor = BlobDescriptor::new(uri, 0, 0).serialize();
        let opened =
            paimon_blob_reader_open_blob(created.reader, descriptor.as_ptr(), descriptor.len());
        assert!(opened.error.is_null());

        let null_buffer = paimon_blob_stream_read(opened.stream, ptr::null_mut(), 1);
        assert!(!null_buffer.error.is_null());
        paimon_error_free(null_buffer.error);

        let zero = paimon_blob_stream_read(opened.stream, ptr::null_mut(), 0);
        assert!(zero.error.is_null());
        assert_eq!(zero.bytes_read, 0);

        let null_stream = paimon_blob_stream_read(ptr::null_mut(), ptr::null_mut(), 0);
        assert!(!null_stream.error.is_null());
        paimon_error_free(null_stream.error);

        let invalid_seek = paimon_blob_stream_seek(opened.stream, -1, 0);
        assert!(!invalid_seek.error.is_null());
        paimon_error_free(invalid_seek.error);

        let null_seek = paimon_blob_stream_seek(ptr::null_mut(), 0, 0);
        assert!(!null_seek.error.is_null());
        paimon_error_free(null_seek.error);

        paimon_blob_stream_free(opened.stream);
        paimon_blob_reader_free(created.reader);
    }
}
