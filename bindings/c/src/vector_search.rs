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
use paimon::table::Table;

use crate::error::{check_non_null, paimon_error, validate_cstr, PaimonErrorCode};
use crate::result::{paimon_result_record_batch_reader, paimon_result_vector_search_builder};
use crate::runtime;
use crate::types::*;

/// Create a new vector-search builder from a Table.
///
/// # Safety
/// `table` must be a valid pointer from `paimon_catalog_get_table`, or null (returns error).
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
const _: unsafe extern "C" fn(*mut paimon_vector_search_builder) =
    paimon_vector_search_builder_free;
const _: unsafe extern "C" fn(
    *mut paimon_vector_search_builder,
) -> paimon_result_record_batch_reader = paimon_vector_search_builder_execute_read;
