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

use std::ffi::c_void;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use paimon::spec::{DataField, Predicate};
use paimon::table::{
    CommitMessage, PostponeBucketPlan, PostponeFixedBucketTableCommit,
    PostponeFixedBucketTableWrite, Table, TableCommit, TableWrite,
};

/// C-compatible key-value pair for options.
#[repr(C)]
pub struct paimon_option {
    pub key: *const std::ffi::c_char,
    pub value: *const std::ffi::c_char,
}

/// C-compatible byte buffer.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct paimon_bytes {
    pub data: *mut u8,
    pub len: usize,
}

impl paimon_bytes {
    pub fn new(v: Vec<u8>) -> Self {
        let boxed = v.into_boxed_slice();
        let len = boxed.len();
        let data = Box::into_raw(boxed) as *mut u8;
        Self { data, len }
    }
}

/// Free a paimon_bytes buffer.
///
/// # Safety
/// Only call with bytes returned from paimon C functions.
#[no_mangle]
pub unsafe extern "C" fn paimon_bytes_free(bytes: paimon_bytes) {
    if !bytes.data.is_null() {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
            bytes.data, bytes.len,
        )));
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct paimon_byte_slice {
    pub data: *const u8,
    pub len: usize,
}

#[repr(C)]
pub struct paimon_bytes_array {
    pub data: *mut paimon_bytes,
    pub len: usize,
}

impl paimon_bytes_array {
    pub fn empty() -> Self {
        Self {
            data: std::ptr::null_mut(),
            len: 0,
        }
    }

    pub fn new(values: Vec<Vec<u8>>) -> Self {
        if values.is_empty() {
            return Self::empty();
        }
        let boxed = values
            .into_iter()
            .map(paimon_bytes::new)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let len = boxed.len();
        let data = Box::into_raw(boxed) as *mut paimon_bytes;
        Self { data, len }
    }
}

/// # Safety
/// `array` was returned by `paimon_blob_reader_read_blobs`.
#[no_mangle]
pub unsafe extern "C" fn paimon_bytes_array_free(array: paimon_bytes_array) {
    if array.data.is_null() {
        return;
    }
    let values = Box::from_raw(std::ptr::slice_from_raw_parts_mut(array.data, array.len));
    for value in values.iter().copied() {
        paimon_bytes_free(value);
    }
}

/// Opaque wrapper around a heap-allocated Rust object.
#[repr(C)]
pub struct paimon_catalog {
    pub inner: *mut c_void,
}

/// Opaque wrapper around a cloneable Paimon FileIO.
#[repr(C)]
pub struct paimon_file_io {
    pub inner: *mut c_void,
}

/// Version 1 callbacks for an externally managed file-block cache.
///
/// Callbacks may run concurrently on arbitrary Rust runtime blocking threads.
/// They must not unwind across the C ABI. `get` returns the number of bytes
/// copied into `output`; return `-1` for a miss and any value other than the
/// requested length for a fail-open miss. All callback buffers and paths are
/// borrowed only for the duration of the call. Paths use pointer-plus-length
/// because canonical storage keys may contain embedded NUL separators.
#[repr(C)]
pub struct paimon_file_cache_callbacks_v1 {
    pub context: *mut c_void,
    pub get: Option<
        unsafe extern "C" fn(
            context: *mut c_void,
            path_data: *const u8,
            path_length: usize,
            offset: u64,
            length: usize,
            output: *mut u8,
        ) -> i64,
    >,
    pub put: Option<
        unsafe extern "C" fn(
            context: *mut c_void,
            path_data: *const u8,
            path_length: usize,
            offset: u64,
            data: *const u8,
            length: usize,
        ) -> i32,
    >,
    pub invalidate_path: Option<
        unsafe extern "C" fn(context: *mut c_void, path_data: *const u8, path_length: usize) -> i32,
    >,
    pub invalidate_prefix: Option<
        unsafe extern "C" fn(
            context: *mut c_void,
            prefix_data: *const u8,
            prefix_length: usize,
        ) -> i32,
    >,
    /// Releases `context` after the last FileIO/table clone is dropped.
    pub destroy: Option<unsafe extern "C" fn(context: *mut c_void)>,
}

#[repr(C)]
pub struct paimon_blob_reader {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_blob_stream {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_identifier {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_table {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_read_builder {
    pub inner: *mut c_void,
}

/// Internal state for ReadBuilder that stores table, projection columns, and filter.
pub(crate) struct ReadBuilderState {
    pub table: Table,
    pub projected_columns: Option<Vec<String>>,
    pub filter: Option<Predicate>,
    pub case_sensitive: bool,
}

/// Internal state for TableScan that stores table and filter.
pub(crate) struct TableScanState {
    pub table: Table,
    pub filter: Option<Predicate>,
}

#[repr(C)]
pub struct paimon_table_scan {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_table_read {
    pub inner: *mut c_void,
}

/// Internal state for TableRead that stores table, projected read type, and data predicates.
pub(crate) struct TableReadState {
    pub table: Table,
    pub read_type: Vec<DataField>,
    pub data_predicates: Vec<Predicate>,
    /// The caller's filter as they set it, before the read builder normalized it into
    /// partition and data halves. `data_predicates` above is only the data half, so it
    /// is not enough to rebuild a read that must know about the partition half --
    /// which a scan-bypassing read does, or it would silently ignore `pt = 'A'`. See
    /// `paimon_table_read_to_arrow_indexed`.
    pub filter: Option<Predicate>,
}

#[repr(C)]
pub struct paimon_plan {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_record_batch_reader {
    pub inner: *mut c_void,
}

/// Opaque wrapper around a Predicate.
#[repr(C)]
pub struct paimon_predicate {
    pub inner: *mut c_void,
}

/// Opaque wrapper around a vector-search builder.
#[repr(C)]
pub struct paimon_vector_search_builder {
    pub inner: *mut c_void,
}

/// Opaque handle to the splits a primary-key vector search selected: one per data
/// file, each carrying that file's chosen physical rows and their scores.
///
/// An IN-PROCESS handle, deliberately not bytes -- the search and the read both run
/// inside this library, in one address space, so serializing would be a pure round
/// trip. Pass it to `paimon_table_read_to_arrow_indexed`, which BORROWS it, so one
/// search can be read again under a different projection. Free it with
/// `paimon_vector_search_splits_free`.
#[repr(C)]
pub struct paimon_vector_search_splits {
    pub inner: *mut c_void,
}

/// Internal state for a vector-search builder: the table plus the query
/// parameters accumulated by the setters before the search is run.
pub(crate) struct VectorSearchState {
    // Read by the search terminal that runs the accumulated query.
    pub table: Table,
    pub vector_column: Option<String>,
    pub query_vector: Option<Vec<f32>>,
    pub limit: Option<usize>,
    pub options: std::collections::HashMap<String, String>,
    pub filter: Option<Predicate>,
    // Optional column projection applied by `execute_read` (plus the always-appended
    // `__paimon_search_score`). `None` materializes every user column.
    pub projection: Option<Vec<String>>,
}

/// A typed literal value for predicate comparison, passed across FFI.
///
/// # Design
///
/// We use a tagged flat struct instead of opaque heap-allocated handles
/// (like DuckDB's `duckdb_value`). The trade-off:
///
/// - **Pro**: Zero allocation — the entire datum is passed by value on the
///   stack, with no heap round-trips or free calls needed. This keeps the
///   FFI surface minimal and the Go/C caller simple.
/// - **Con**: The struct is larger than any single variant needs, wasting
///   some bytes per datum (currently ~56 bytes vs. ~16 for the largest
///   single variant).
///
/// Since datums are only used for predicate construction (not a hot path),
/// the extra size is acceptable.
///
/// # Tags
///
/// - 0: Bool, 1: TinyInt, 2: SmallInt, 3: Int, 4: Long
/// - 5: Float, 6: Double, 7: String, 8: Date, 9: Time
/// - 10: Timestamp, 11: LocalZonedTimestamp, 12: Decimal, 13: Bytes
///
/// `tag` determines which value fields are valid:
/// - `Bool`/`TinyInt`/`SmallInt`/`Int`/`Long`/`Date`/`Time` → `int_val`
/// - `Float`/`Double` → `double_val`
/// - `String`/`Bytes` → `str_data` + `str_len`
/// - `Timestamp`/`LocalZonedTimestamp` → `int_val` (millis) + `int_val2` (nanos)
/// - `Decimal` → `int_val` + `int_val2` (unscaled i128) + `uint_val` (precision) + `uint_val2` (scale)
#[repr(C)]
#[derive(Default)]
pub struct paimon_datum {
    pub tag: i32,
    pub int_val: i64,
    pub double_val: f64,
    pub str_data: *const u8,
    pub str_len: usize,
    pub int_val2: i64,
    pub uint_val: u32,
    pub uint_val2: u32,
}

/// A single Arrow record batch exported via the Arrow C Data Interface.
///
/// `array` and `schema` point to heap-allocated ArrowArray and ArrowSchema
/// structs. After importing the data, call `paimon_arrow_batch_free` to free
/// the container structs.
#[repr(C)]
pub struct paimon_arrow_batch {
    /// Pointer to a heap-allocated ArrowArray.
    pub array: *mut c_void,
    /// Pointer to a heap-allocated ArrowSchema.
    pub schema: *mut c_void,
}

// === Write/Commit opaque types ===

pub(crate) struct WriteBuilderState {
    pub table: Table,
    pub commit_user: String,
    pub overwrite: bool,
}

pub(crate) struct PostponeFixedBucketWriteBuilderState {
    pub table: Table,
    pub commit_user: String,
    pub overwrite: bool,
    pub bucket_plan: Option<PostponeBucketPlan>,
}

pub(crate) struct TableWriteState {
    pub write: Box<TableWrite>,
    pub overwrite: bool,
    pub target_schema: Arc<ArrowSchema>,
    pub table_location: String,
    pub commit_user: String,
}

pub(crate) struct PostponeFixedBucketTableWriteState {
    pub write: Box<PostponeFixedBucketTableWrite>,
    pub overwrite: bool,
    pub target_schema: Arc<ArrowSchema>,
    pub table_location: String,
    pub commit_user: String,
}

pub(crate) struct TableCommitState {
    pub commit: TableCommit,
    pub overwrite: bool,
    pub table_location: String,
    pub commit_user: String,
}

pub(crate) struct PostponeFixedBucketTableCommitState {
    pub commit: PostponeFixedBucketTableCommit,
    pub overwrite: bool,
    pub table_location: String,
    pub commit_user: String,
}

pub(crate) struct CommitMessagesState {
    pub messages: Vec<CommitMessage>,
    pub overwrite: bool,
    pub table_location: String,
    pub commit_user: String,
}

pub(crate) struct PostponeFixedBucketCommitMessagesState {
    pub messages: Vec<CommitMessage>,
    pub overwrite: bool,
    pub table_location: String,
    pub commit_user: String,
}

#[repr(C)]
pub struct paimon_write_builder {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_table_write {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_table_commit {
    pub inner: *mut c_void,
}

/// Opaque container for commit messages and their originating write context.
#[repr(C)]
pub struct paimon_commit_messages {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_postpone_fixed_bucket_write_builder {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_postpone_fixed_bucket_table_write {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_postpone_fixed_bucket_table_commit {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_postpone_fixed_bucket_commit_messages {
    pub inner: *mut c_void,
}
