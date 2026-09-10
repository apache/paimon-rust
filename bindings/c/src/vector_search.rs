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

//! C FFI bindings for vector search.
//!
//! Wraps the Rust vector-search builder over the C ABI: a
//! `paimon_vector_search_builder` is created from a table, then configured with
//! the query vector, target column, result limit, options, and an optional
//! scalar filter. The builder is storage-agnostic — it targets both
//! primary-key and append / data-evolution tables.
//!
//! This module provides the builder constructor, its setters, the terminal
//! that runs the search and returns a streaming Arrow reader, and the free
//! function.

use std::collections::HashMap;
use std::ffi::{c_char, c_void};

use paimon::spec::Predicate;
use paimon::table::{BucketVectorSearchSplit, PkVectorIndexedSplit, Table};

use crate::error::{check_non_null, paimon_error, validate_cstr, PaimonErrorCode};
use crate::result::{
    paimon_result_record_batch_reader, paimon_result_vector_search_builder,
    paimon_result_vector_search_splits,
};
use crate::runtime;
use crate::types::*;

/// Create a new vector-search builder from a Table.
///
/// # Safety
/// `table` must be a valid pointer from `paimon_catalog_get_table` or
/// `paimon_table_from_schema_json`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_new_vector_search_builder(
    table: *const paimon_table,
) -> paimon_result_vector_search_builder {
    if let Err(e) = check_non_null(table, "table") {
        return paimon_result_vector_search_builder {
            builder: std::ptr::null_mut(),
            error: e,
        };
    }
    let table_ref = &*((*table).inner as *const Table);
    let state = VectorSearchState {
        table: table_ref.clone(),
        vector_column: None,
        query_vector: None,
        limit: None,
        options: HashMap::new(),
        filter: None,
        projection: None,
    };
    let inner = Box::into_raw(Box::new(state)) as *mut c_void;
    paimon_result_vector_search_builder {
        builder: Box::into_raw(Box::new(paimon_vector_search_builder { inner })),
        error: std::ptr::null_mut(),
    }
}

/// Set the target vector column for a vector-search builder.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns error). `column` must be a valid C string.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_with_vector_column(
    b: *mut paimon_vector_search_builder,
    column: *const c_char,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(b, "b") {
        return e;
    }
    let col = match validate_cstr(column, "vector column") {
        Ok(s) => s,
        Err(e) => return e,
    };
    let state = &mut *((*b).inner as *mut VectorSearchState);
    state.vector_column = Some(col);
    std::ptr::null_mut()
}

/// Set the query vector for a vector-search builder.
///
/// The `len` floats at `data` are copied into the builder; the caller retains
/// ownership of `data`. An empty vector (`len == 0`) is rejected.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns error). `data` must point to `len` `f32` values when `len > 0`.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_with_query_vector(
    b: *mut paimon_vector_search_builder,
    data: *const f32,
    len: usize,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(b, "b") {
        return e;
    }
    if len == 0 {
        return paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "query vector must not be empty".to_string(),
        );
    }
    if data.is_null() {
        return paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "null query vector pointer with non-zero length".to_string(),
        );
    }
    let state = &mut *((*b).inner as *mut VectorSearchState);
    state.query_vector = Some(std::slice::from_raw_parts(data, len).to_vec());
    std::ptr::null_mut()
}

/// Set the maximum number of results for a vector-search builder.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_with_limit(
    b: *mut paimon_vector_search_builder,
    limit: usize,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(b, "b") {
        return e;
    }
    let state = &mut *((*b).inner as *mut VectorSearchState);
    state.limit = Some(limit);
    std::ptr::null_mut()
}

/// Set scan/search options for a vector-search builder.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns error). `options` must be a valid pointer to `len`
/// `paimon_option` values, or null when `len` is 0.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_with_options(
    b: *mut paimon_vector_search_builder,
    options: *const paimon_option,
    len: usize,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(b, "b") {
        return e;
    }
    if options.is_null() && len > 0 {
        return paimon_error::new(
            PaimonErrorCode::InvalidInput,
            "null options pointer with non-zero length".to_string(),
        );
    }
    let mut map = HashMap::with_capacity(len);
    if len > 0 {
        let slice = std::slice::from_raw_parts(options, len);
        for opt in slice {
            let key = match validate_cstr(opt.key, "option key") {
                Ok(s) => s,
                Err(e) => return e,
            };
            let value = match validate_cstr(opt.value, "option value") {
                Ok(s) => s,
                Err(e) => return e,
            };
            map.insert(key, value);
        }
    }
    let state = &mut *((*b).inner as *mut VectorSearchState);
    state.options = map;
    std::ptr::null_mut()
}

/// Set an optional scalar residual filter for a vector-search builder.
///
/// The predicate is consumed (ownership transferred to the builder). Pass null
/// to clear any previously set filter.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns error). `predicate` must be a valid pointer from a
/// `paimon_predicate_*` function, or null.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_with_filter(
    b: *mut paimon_vector_search_builder,
    predicate: *mut paimon_predicate,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(b, "b") {
        return e;
    }

    let state = &mut *((*b).inner as *mut VectorSearchState);

    if predicate.is_null() {
        state.filter = None;
        return std::ptr::null_mut();
    }

    let pred_wrapper = Box::from_raw(predicate);
    let pred = Box::from_raw(pred_wrapper.inner as *mut Predicate);
    state.filter = Some(*pred);
    std::ptr::null_mut()
}

/// Restrict the columns materialized by `paimon_vector_search_builder_execute_read`
/// to `columns` (plus the always-appended `__paimon_search_score`). Without this
/// call `execute_read` materializes every user table column. Only affects
/// `execute_read`.
///
/// `columns` is a null-terminated array of null-terminated C strings; output
/// order follows the caller-specified order. An empty list is a valid zero-column
/// projection (only the score column is materialized). Pass null to clear any
/// previously set projection.
///
/// Unlike `paimon_read_builder_with_projection`, this does not validate column
/// names eagerly: the vector builder resolves the projection against the schema
/// when the search runs, so an unknown column surfaces as an error from
/// `paimon_vector_search_builder_execute_read`.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns error). `columns` must be a null-terminated array of
/// null-terminated C strings, or null to clear the projection.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_with_projection(
    b: *mut paimon_vector_search_builder,
    columns: *const *const c_char,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(b, "b") {
        return e;
    }

    let state = &mut *((*b).inner as *mut VectorSearchState);

    if columns.is_null() {
        state.projection = None;
        return std::ptr::null_mut();
    }

    let mut col_names = Vec::new();
    let mut ptr = columns;
    while !(*ptr).is_null() {
        let c_str = std::ffi::CStr::from_ptr(*ptr);
        match c_str.to_str() {
            Ok(s) => col_names.push(s.to_string()),
            Err(e) => {
                return paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!("Invalid UTF-8 in projection column name: {e}"),
                );
            }
        }
        ptr = ptr.add(1);
    }

    state.projection = Some(col_names);
    std::ptr::null_mut()
}

/// Free a paimon_vector_search_builder.
///
/// # Safety
/// Only call with a builder returned from `paimon_table_new_vector_search_builder`.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_free(b: *mut paimon_vector_search_builder) {
    if !b.is_null() {
        let wrapper = Box::from_raw(b);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut VectorSearchState));
        }
    }
}

/// Execute the vector search and return a streaming Arrow reader over the
/// materialized rows (projected user columns plus `__paimon_search_score`).
/// Works for both primary-key and data-evolution tables. Consume via
/// `paimon_record_batch_reader_next` and free with `paimon_record_batch_reader_free`.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns an error result).
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_execute_read(
    b: *mut paimon_vector_search_builder,
) -> paimon_result_record_batch_reader {
    if let Err(e) = check_non_null(b, "b") {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: e,
        };
    }
    let state = &*((*b).inner as *const VectorSearchState);

    let mut builder = state.table.new_vector_search_builder();
    if let Some(col) = &state.vector_column {
        builder.with_vector_column(col);
    }
    if let Some(v) = &state.query_vector {
        builder.with_query_vector(v.clone());
    }
    if let Some(limit) = state.limit {
        builder.with_limit(limit);
    }
    if !state.options.is_empty() {
        builder.with_options(state.options.clone());
    }
    if let Some(f) = &state.filter {
        builder.with_filter(f.clone());
    }
    if let Some(cols) = &state.projection {
        let col_refs: Vec<&str> = cols.iter().map(String::as_str).collect();
        builder.with_projection(&col_refs);
    }

    match runtime().block_on(builder.execute_read()) {
        Ok(stream) => {
            let reader = Box::new(stream);
            let wrapper = Box::new(paimon_record_batch_reader {
                inner: Box::into_raw(reader) as *mut c_void,
            });
            paimon_result_record_batch_reader {
                reader: Box::into_raw(wrapper),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

/// Run the vector search over bucket splits a Java planner produced, and stream
/// the materialized rows.
///
/// This is the entry point for an engine that plans in Paimon Java and executes
/// here: the planner emits one `BucketVectorSearchSplit` per bucket and ships its
/// bytes to a worker, which calls this. `splits` points at `count` buffers and
/// `split_lens` at their lengths; both arrays must hold `count` entries. The
/// buffers are only read for the duration of the call.
///
/// The splits are the plan -- their payload files, per-file row ranges and pinned
/// snapshot are used as given, and the table's index manifest is not read. Search,
/// optional refine, Top-K and materialization are the same as
/// `paimon_vector_search_builder_execute_read`, so the output is the projected
/// user columns plus `__paimon_search_score`, best-first. The Top-K is local to
/// the splits passed in; a caller distributing one call per bucket merges the
/// per-bucket results itself.
///
/// Only a primary-key vector column can be read this way; a data-evolution table
/// returns an error rather than an answer from a different plan. Consume via
/// `paimon_record_batch_reader_next` and free with
/// `paimon_record_batch_reader_free`.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns an error result). `splits` and `split_lens` must each point at
/// `count` valid entries, and each `splits[i]` at `split_lens[i]` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_execute_read_for_bucket_splits(
    b: *mut paimon_vector_search_builder,
    splits: *const *const u8,
    split_lens: *const usize,
    count: usize,
) -> paimon_result_record_batch_reader {
    if let Err(e) = check_non_null(b, "b") {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: e,
        };
    }
    // A `#[repr(C)]` wrapper can arrive zero-initialized, which passes the null-POINTER
    // check above while carrying a null `inner`. The state is dereferenced below, so
    // that has to be caught here rather than as a null dereference inside the library.
    if (*b).inner.is_null() {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::InvalidInput,
                concat!(
                    "paimon_vector_search_builder_execute_read_for_bucket_splits: ",
                    "builder is not initialized"
                )
                .to_string(),
            ),
        };
    }
    if splits.is_null() || split_lens.is_null() || count == 0 {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "paimon_vector_search_builder_execute_read_for_bucket_splits: null or empty splits"
                    .to_string(),
            ),
        };
    }

    let ptrs = std::slice::from_raw_parts(splits, count);
    let lens = std::slice::from_raw_parts(split_lens, count);
    let mut buffers: Vec<&[u8]> = Vec::with_capacity(count);
    for (i, (&ptr, &len)) in ptrs.iter().zip(lens).enumerate() {
        // A null or empty buffer cannot be a split, and reaching the decoder with
        // one would report it as corrupt data rather than as the caller's error.
        if ptr.is_null() || len == 0 {
            return paimon_result_record_batch_reader {
                reader: std::ptr::null_mut(),
                error: paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!(
                        "paimon_vector_search_builder_execute_read_for_bucket_splits: \
                         split {i} is null or empty"
                    ),
                ),
            };
        }
        buffers.push(std::slice::from_raw_parts(ptr, len));
    }

    let state = &*((*b).inner as *const VectorSearchState);
    let mut builder = state.table.new_vector_search_builder();
    if let Some(col) = &state.vector_column {
        builder.with_vector_column(col);
    }
    if let Some(v) = &state.query_vector {
        builder.with_query_vector(v.clone());
    }
    if let Some(limit) = state.limit {
        builder.with_limit(limit);
    }
    if !state.options.is_empty() {
        builder.with_options(state.options.clone());
    }
    if let Some(f) = &state.filter {
        builder.with_filter(f.clone());
    }
    if let Some(cols) = &state.projection {
        let col_refs: Vec<&str> = cols.iter().map(String::as_str).collect();
        builder.with_projection(&col_refs);
    }

    match runtime().block_on(builder.execute_read_for_bucket_splits(&buffers)) {
        Ok(stream) => {
            let reader = Box::new(stream);
            let wrapper = Box::new(paimon_record_batch_reader {
                inner: Box::into_raw(reader) as *mut c_void,
            });
            paimon_result_record_batch_reader {
                reader: Box::into_raw(wrapper),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

/// Search bucket splits a Java planner produced, and return WHAT the search found
/// -- not the rows.
///
/// Step one of the two-step primary-key vector read. The splits are the plan: their
/// payload files, per-file row ranges and the snapshot they name are used as given, and the
/// table's index manifest is not read. Search, optional refine and Top-K happen
/// here; the Top-K is local to the splits passed in, so a caller distributing one
/// call per bucket merges the per-bucket results itself -- on the rows it reads, over
/// `__paimon_search_score`, since a split cannot be trimmed from outside.
///
/// Each call returns its OWN handle, and `paimon_table_read_to_arrow_indexed` validates
/// the handle it is given: one snapshot, no data file twice. It cannot validate across
/// handles, so a caller reading several separately is responsible for their being one
/// search generation.
///
/// Step two is an ORDINARY read the caller drives:
/// `paimon_table_new_read_builder` (with its own `with_projection`) ->
/// `paimon_read_builder_new_read` -> `paimon_table_read_to_arrow_indexed`.
/// Separating them is what lets a caller learn how many data files matched -- via
/// `paimon_vector_search_splits_count` -- and choose its own projection, before the
/// selected rows are materialized. (Scores and per-file row counts are readable from a
/// Rust caller through `PkVectorIndexedSplit`; this ABI exposes only the count.) (The search itself may still open data files, for exact fallback,
/// a residual filter, or reranking.)
///
/// The builder's `with_filter` still applies -- it is the pre-Top-K scalar residual.
/// Its `with_projection` is REJECTED rather than ignored: projection belongs to the
/// read, and silently returning columns a caller did not ask for is worse than saying
/// so.
///
/// Only a primary-key vector column can be searched this way; a data-evolution
/// table returns an error rather than an answer from a different plan. Free the
/// result with `paimon_vector_search_splits_free`.
///
/// # Safety
/// `b` must be a valid pointer from `paimon_table_new_vector_search_builder`, or
/// null (returns an error result). `splits` and `split_lens` must each point at
/// `count` valid entries, and each `splits[i]` at `split_lens[i]` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_builder_search_for_bucket_splits(
    b: *mut paimon_vector_search_builder,
    splits: *const *const u8,
    split_lens: *const usize,
    count: usize,
) -> paimon_result_vector_search_splits {
    if let Err(e) = check_non_null(b, "b") {
        return paimon_result_vector_search_splits {
            splits: std::ptr::null_mut(),
            error: e,
        };
    }
    if splits.is_null() || split_lens.is_null() || count == 0 {
        return paimon_result_vector_search_splits {
            splits: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "paimon_vector_search_builder_search_for_bucket_splits: null or empty splits"
                    .to_string(),
            ),
        };
    }

    let ptrs = std::slice::from_raw_parts(splits, count);
    let lens = std::slice::from_raw_parts(split_lens, count);
    let mut buffers: Vec<&[u8]> = Vec::with_capacity(count);
    for (i, (&ptr, &len)) in ptrs.iter().zip(lens).enumerate() {
        // A null or empty buffer cannot be a split, and reaching the decoder with
        // one would report it as corrupt data rather than as the caller's error.
        if ptr.is_null() || len == 0 {
            return paimon_result_vector_search_splits {
                splits: std::ptr::null_mut(),
                error: paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!(
                        "paimon_vector_search_builder_search_for_bucket_splits: \
                         split {i} is null or empty"
                    ),
                ),
            };
        }
        buffers.push(std::slice::from_raw_parts(ptr, len));
    }

    // `paimon_vector_search_builder` is `#[repr(C)]` with a public field, so a caller
    // can hand over a zero-initialized wrapper. The outer pointer being non-null says
    // nothing about `inner`; dereferencing a null one here is UB, not an error result.
    if (*b).inner.is_null() {
        return paimon_result_vector_search_splits {
            splits: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "paimon_vector_search_builder_search_for_bucket_splits: builder is not \
                 initialized"
                    .to_string(),
            ),
        };
    }
    let state = &*((*b).inner as *const VectorSearchState);
    let mut builder = state.table.new_vector_search_builder();
    if let Some(col) = &state.vector_column {
        builder.with_vector_column(col);
    }
    if let Some(v) = &state.query_vector {
        builder.with_query_vector(v.clone());
    }
    if let Some(limit) = state.limit {
        builder.with_limit(limit);
    }
    if !state.options.is_empty() {
        builder.with_options(state.options.clone());
    }
    if let Some(f) = &state.filter {
        builder.with_filter(f.clone());
    }
    if let Some(columns) = &state.projection {
        let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
        builder.with_projection(&columns);
    }
    let result = runtime().block_on(async {
        let read = builder.new_vector_read().map_err(|error| match error {
            paimon::Error::DataInvalid { message, source } if state.projection.is_some() => {
                paimon::Error::DataInvalid {
                    message: format!(
                        "{message}. In the C API, set projection with \
                         paimon_read_builder_with_projection on the read builder used by \
                         paimon_table_read_to_arrow_indexed"
                    ),
                    source,
                }
            }
            error => error,
        })?;
        let splits = buffers
            .iter()
            .map(|bytes| BucketVectorSearchSplit::deserialize(bytes))
            .collect::<paimon::Result<Vec<_>>>()?;
        read.read(splits).await
    });
    match result {
        Ok(result) => {
            let wrapper = Box::new(paimon_vector_search_splits {
                inner: Box::into_raw(Box::new(result)) as *mut c_void,
            });
            paimon_result_vector_search_splits {
                splits: Box::into_raw(wrapper),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_vector_search_splits {
            splits: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

/// How many data files the search selected rows from.
///
/// Check the search result's `error` FIRST. This returns `0` for a null or
/// uninitialized handle just as it does for a search that matched nothing, so a caller
/// that skips the error check cannot tell a failed search from an empty one and would
/// report a silently truncated result. Once `error` is null, `0` does mean nothing
/// matched, and the read can be skipped.
///
/// # Safety
/// `splits` must be a valid pointer from a search, or null.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_splits_count(
    splits: *const paimon_vector_search_splits,
) -> usize {
    if splits.is_null() || (*splits).inner.is_null() {
        return 0;
    }
    (*((*splits).inner as *const Vec<PkVectorIndexedSplit>)).len()
}

/// Free the splits a search returned. Null is a no-op. They may be read any number of
/// times before this: the read borrows them and never consumes them.
///
/// A zero-initialized wrapper is also a no-op. `inner` is read BEFORE the outer
/// pointer is reclaimed, because reclaiming it assumes the search allocated it --
/// a caller that declares `paimon_vector_search_splits s = {0};` and frees `&s`
/// would otherwise have stack storage handed to the allocator. The other terminals
/// here check `inner` for the same reason; this one has to check it first.
///
/// # Safety
/// `splits` must be a valid pointer from a search and not already freed, or null.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_search_splits_free(
    splits: *mut paimon_vector_search_splits,
) {
    if splits.is_null() || (*splits).inner.is_null() {
        return;
    }
    let wrapper = Box::from_raw(splits);
    drop(Box::from_raw(
        wrapper.inner as *mut Vec<PkVectorIndexedSplit>,
    ));
}

/// Read the rows a primary-key vector search selected -- step two of the two-step
/// read.
///
/// Output columns are this read's own projection plus `__paimon_search_score`;
/// `_PKEY_VECTOR_POSITION` is stripped and a projection naming `_ROW_ID` is rejected.
/// Rows come back in PHYSICAL order carrying `__paimon_search_score`, not ranked --
/// sort on that column if you want them best-first, as Java's caller does. A scalar
/// filter, a limit, or explicit row ranges on this read are REJECTED: this read runs no
/// scan, so a filter would be applied after Top-K and return fewer rows than asked for,
/// while a limit and row ranges would not be applied at all. Put the predicate and the
/// limit on the SEARCH builder, where they apply before Top-K; row ranges have no
/// equivalent there, because which rows are read is what the search itself decides.
///
/// `splits` is borrowed, not consumed -- read them again under a different projection
/// if you like, and free them separately. PRECONDITION, unchecked: they must come from
/// a search over the SAME table this read belongs to; they name data files by path, so
/// another table's splits would be opened under this table's schema. Consume the reader via
/// `paimon_record_batch_reader_next` and free it with
/// `paimon_record_batch_reader_free`.
///
/// # Safety
/// `read` must be a valid pointer from `paimon_read_builder_new_read`, and `splits` a
/// valid pointer from a search; either being null returns an error result. Passing
/// freed splits is undefined behaviour and cannot be detected here -- the free
/// releases the handle itself, so there is nothing left to inspect. Same contract as
/// every other handle in this ABI.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_read_to_arrow_indexed(
    read: *const paimon_table_read,
    splits: *const paimon_vector_search_splits,
) -> paimon_result_record_batch_reader {
    if let Err(e) = check_non_null(read, "read") {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: e,
        };
    }
    if let Err(e) = check_non_null(splits, "splits") {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: e,
        };
    }
    // Both wrappers are `#[repr(C)]` with public fields, so a zero-initialized one
    // passes the null check above while carrying a null `inner`.
    if (*read).inner.is_null() || (*splits).inner.is_null() {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "paimon_table_read_to_arrow_indexed: read or splits is not initialized".to_string(),
            ),
        };
    }
    let search_splits = &*((*splits).inner as *const Vec<PkVectorIndexedSplit>);
    // Rebuilt on every call, because no live `TableRead` crosses the boundary. Unlike
    // `paimon_table_read_to_arrow`, this rebuild goes back through the READ BUILDER
    // rather than `TableRead::new`: this read bypasses scan planning, so it must
    // REFUSE a filter rather than ignore it, and the surviving `data_predicates` do
    // not say whether one was set -- normalization drops a partition conjunct and
    // `_ROW_ID` extraction drops an exact row-id one. Replaying the caller's original
    // filter through the builder is what sets that bit; the setter itself is
    // deliberately crate-private, so a caller cannot clear it.
    let state = &*((*read).inner as *const TableReadState);
    let mut read_builder = state.table.new_read_builder();
    read_builder.with_read_type(state.read_type.clone());
    if let Some(filter) = &state.filter {
        read_builder.with_filter(filter.clone());
    }
    let table_read = match read_builder.new_read() {
        Ok(table_read) => table_read,
        Err(e) => {
            return paimon_result_record_batch_reader {
                reader: std::ptr::null_mut(),
                error: paimon_error::from_paimon(e),
            }
        }
    };
    match table_read.to_arrow_indexed(search_splits) {
        Ok(stream) => {
            let wrapper = Box::new(paimon_record_batch_reader {
                inner: Box::into_raw(Box::new(stream)) as *mut c_void,
            });
            paimon_result_record_batch_reader {
                reader: Box::into_raw(wrapper),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

// --- C ABI signature guards -------------------------------------------------
//
// These symbols are called across the FFI boundary with fixed argument counts:
// bindings prepare a libffi call interface (CIF) per symbol, and external
// consumers link against the generated headers (e.g. Doris integrations).
// Adding or reordering a parameter on one of these existing symbols silently
// breaks every such caller — the extra argument is read from an undefined
// register/stack slot at the ABI boundary.
//
// These compile-time assertions pin the existing signatures. To add behavior,
// introduce a new symbol instead of changing one of these; touching a signature
// here will fail to compile.
const _: unsafe extern "C" fn(*const paimon_table) -> paimon_result_vector_search_builder =
    paimon_table_new_vector_search_builder;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
    *const c_char,
) -> *mut paimon_error = paimon_vector_search_builder_with_vector_column;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
    *const f32,
    usize,
) -> *mut paimon_error = paimon_vector_search_builder_with_query_vector;
const _: unsafe extern "C" fn(*mut paimon_vector_search_builder, usize) -> *mut paimon_error =
    paimon_vector_search_builder_with_limit;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
    *const paimon_option,
    usize,
) -> *mut paimon_error = paimon_vector_search_builder_with_options;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
    *mut paimon_predicate,
) -> *mut paimon_error = paimon_vector_search_builder_with_filter;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
    *const *const c_char,
) -> *mut paimon_error = paimon_vector_search_builder_with_projection;
const _: unsafe extern "C" fn(*mut paimon_vector_search_builder) =
    paimon_vector_search_builder_free;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
) -> paimon_result_record_batch_reader = paimon_vector_search_builder_execute_read;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
    *const *const u8,
    *const usize,
    usize,
) -> paimon_result_record_batch_reader =
    paimon_vector_search_builder_execute_read_for_bucket_splits;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
    *const *const u8,
    *const usize,
    usize,
) -> paimon_result_vector_search_splits = paimon_vector_search_builder_search_for_bucket_splits;
const _: unsafe extern "C" fn(*const paimon_vector_search_splits) -> usize =
    paimon_vector_search_splits_count;
const _: unsafe extern "C" fn(*mut paimon_vector_search_splits) = paimon_vector_search_splits_free;
const _: unsafe extern "C" fn(
    *const paimon_table_read,
    *const paimon_vector_search_splits,
) -> paimon_result_record_batch_reader = paimon_table_read_to_arrow_indexed;
