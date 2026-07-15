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

use std::collections::HashMap;
use std::ffi::c_void;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, StructArray};
use futures::StreamExt;
use paimon::spec::{DataField, DataType, Datum, Predicate, PredicateBuilder};
use paimon::table::{ArrowRecordBatchStream, Table};
use paimon::Plan;

use crate::error::{check_non_null, paimon_error, validate_cstr, PaimonErrorCode};
use crate::result::{
    paimon_result_new_read, paimon_result_next_batch, paimon_result_plan, paimon_result_predicate,
    paimon_result_read_builder, paimon_result_record_batch_reader, paimon_result_table_scan,
};
use crate::runtime;
use crate::types::*;

// Helper to free a wrapper struct that contains a Table clone.
unsafe fn free_table_wrapper<T>(ptr: *mut T, get_inner: impl FnOnce(&T) -> *mut c_void) {
    if !ptr.is_null() {
        let wrapper = Box::from_raw(ptr);
        let inner = get_inner(&wrapper);
        if !inner.is_null() {
            drop(Box::from_raw(inner as *mut Table));
        }
    }
}

// Helper to box a ReadBuilderState and return a raw pointer.
unsafe fn box_read_builder_state(state: ReadBuilderState) -> *mut paimon_read_builder {
    let inner = Box::into_raw(Box::new(state)) as *mut c_void;
    Box::into_raw(Box::new(paimon_read_builder { inner }))
}

// Helper to box a TableReadState and return a raw pointer.
unsafe fn box_table_read_state(state: TableReadState) -> *mut paimon_table_read {
    let inner = Box::into_raw(Box::new(state)) as *mut c_void;
    Box::into_raw(Box::new(paimon_table_read { inner }))
}

// ======================= Table ===============================

/// Free a paimon_table.
///
/// # Safety
/// Only call with a table returned from `paimon_catalog_get_table`.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_free(table: *mut paimon_table) {
    free_table_wrapper(table, |t| t.inner);
}

/// Time-travel selector option names, in the core's resolution priority order.
const TIME_TRAVEL_SELECTORS: [&str; 4] = [
    "scan.timestamp-millis",
    "scan.version",
    "scan.snapshot-id",
    "scan.tag-name",
];

/// Build a `ReadBuilderState` from a table and a scan-option map, resolving any
/// time-travel selector at construction time.
///
/// Rejects more than one time-travel selector up front (the core silently falls
/// back on a conflict, which would misattribute the failure). Otherwise runs the
/// core's `copy_with_time_travel`, which validates unsupported scan options and
/// resolves the selector; a set selector that does not resolve to a snapshot is
/// an error, so a mistyped or missing selector can never silently read latest.
unsafe fn new_read_builder_state(
    table: &Table,
    options: HashMap<String, String>,
) -> Result<ReadBuilderState, *mut paimon_error> {
    let present: Vec<&str> = TIME_TRAVEL_SELECTORS
        .iter()
        .copied()
        .filter(|name| options.contains_key(*name))
        .collect();
    if present.len() > 1 {
        return Err(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            format!(
                "Only one time-travel selector may be set, found: {}",
                present.join(", ")
            ),
        ));
    }
    let selector = TIME_TRAVEL_SELECTORS
        .iter()
        .find_map(|&name| options.get(name).map(|v| (name.to_string(), v.clone())));

    let resolved = match runtime().block_on(table.copy_with_time_travel(options)) {
        Ok(t) => t,
        Err(e) => return Err(paimon_error::from_paimon(e)),
    };

    if let Some((name, value)) = selector {
        if !resolved.has_resolved_travel_snapshot() {
            return Err(paimon_error::new(
                PaimonErrorCode::InvalidInput,
                format!("time-travel selector {name}={value} did not resolve to any snapshot"),
            ));
        }
    }

    Ok(ReadBuilderState {
        table: resolved,
        projected_columns: None,
        filter: None,
        case_sensitive: true,
    })
}

/// Create a new ReadBuilder from a Table.
///
/// # Safety
/// `table` must be a valid pointer from `paimon_catalog_get_table`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_new_read_builder(
    table: *const paimon_table,
) -> paimon_result_read_builder {
    paimon_table_new_read_builder_with_options(table, std::ptr::null(), 0)
}

/// Create a ReadBuilder from a Table with scan options (e.g. time-travel
/// selectors `scan.snapshot-id` / `scan.tag-name` / `scan.timestamp-millis` /
/// `scan.version`). At most one time-travel selector may be set. A selector that
/// does not resolve to a snapshot is an error (never a silent read-of-latest).
///
/// # Safety
/// `table` must be a valid pointer. `options` must be a valid pointer to
/// `options_len` `paimon_option` values, or null when `options_len` is 0.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_new_read_builder_with_options(
    table: *const paimon_table,
    options: *const paimon_option,
    options_len: usize,
) -> paimon_result_read_builder {
    if let Err(e) = check_non_null(table, "table") {
        return paimon_result_read_builder {
            read_builder: std::ptr::null_mut(),
            error: e,
        };
    }
    if options.is_null() && options_len > 0 {
        return paimon_result_read_builder {
            read_builder: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "null options pointer with non-zero length".to_string(),
            ),
        };
    }
    let mut map = HashMap::with_capacity(options_len);
    if options_len > 0 {
        let slice = std::slice::from_raw_parts(options, options_len);
        for opt in slice {
            let key = match validate_cstr(opt.key, "option key") {
                Ok(s) => s,
                Err(e) => {
                    return paimon_result_read_builder {
                        read_builder: std::ptr::null_mut(),
                        error: e,
                    }
                }
            };
            let value = match validate_cstr(opt.value, "option value") {
                Ok(s) => s,
                Err(e) => {
                    return paimon_result_read_builder {
                        read_builder: std::ptr::null_mut(),
                        error: e,
                    }
                }
            };
            map.insert(key, value);
        }
    }
    let table_ref = &*((*table).inner as *const Table);
    match new_read_builder_state(table_ref, map) {
        Ok(state) => paimon_result_read_builder {
            read_builder: box_read_builder_state(state),
            error: std::ptr::null_mut(),
        },
        Err(e) => paimon_result_read_builder {
            read_builder: std::ptr::null_mut(),
            error: e,
        },
    }
}

// ======================= ReadBuilder ===============================

/// Free a paimon_read_builder.
///
/// # Safety
/// Only call with a read_builder returned from `paimon_table_new_read_builder`.
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_free(rb: *mut paimon_read_builder) {
    if !rb.is_null() {
        let wrapper = Box::from_raw(rb);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut ReadBuilderState));
        }
    }
}

/// Set column projection for a ReadBuilder.
///
/// The `columns` parameter is a null-terminated array of null-terminated C strings.
/// Output order follows the caller-specified order. An empty list is a valid
/// zero-column projection. An obvious typo — a name that matches no field under
/// any case sensitivity — is rejected by this call. Case-dependent resolution
/// (a name that matches only case-insensitively, or a case-fold ambiguity) is
/// deferred to `paimon_read_builder_new_read`, which uses the case sensitivity
/// effective then, so this stays order-independent with
/// `paimon_read_builder_with_case_sensitive`.
///
/// # Safety
/// `rb` must be a valid pointer from `paimon_table_new_read_builder`, or null (returns error).
/// `columns` must be a null-terminated array of null-terminated C strings, or null for no projection.
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_with_projection(
    rb: *mut paimon_read_builder,
    columns: *const *const std::ffi::c_char,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(rb, "rb") {
        return e;
    }

    let state = &mut *((*rb).inner as *mut ReadBuilderState);

    if columns.is_null() {
        state.projected_columns = None;
        return std::ptr::null_mut();
    }

    let mut col_names = Vec::new();
    let mut ptr = columns;
    while !(*ptr).is_null() {
        let c_str = std::ffi::CStr::from_ptr(*ptr);
        match c_str.to_str() {
            Ok(s) => col_names.push(s.to_string()),
            Err(e) => {
                return paimon_error::from_paimon(paimon::Error::ConfigInvalid {
                    message: format!("Invalid UTF-8 in column name: {e}"),
                });
            }
        }
        ptr = ptr.add(1);
    }

    // Best-effort early validation for obvious typos (columns that cannot match
    // under any case sensitivity). Core `with_projection` performs this
    // case-independent check and otherwise stores names for lazy resolution, so
    // this stays order-independent with `paimon_read_builder_with_case_sensitive`;
    // case-dependent resolution/ambiguity errors surface later from
    // `paimon_read_builder_new_read`.
    let col_refs: Vec<&str> = col_names.iter().map(String::as_str).collect();
    if let Err(e) = state.table.new_read_builder().with_projection(&col_refs) {
        return paimon_error::from_paimon(e);
    }

    state.projected_columns = Some(col_names);
    std::ptr::null_mut()
}

/// Set whether column-name matching for **projection** is case-sensitive for
/// this ReadBuilder. Defaults to `true` (exact match). When `false`, projected
/// column names are matched by ASCII case-folding and an ambiguous
/// (case-colliding) request errors.
///
/// This does **not** affect predicate resolution: a predicate is resolved when
/// it is constructed, so its case sensitivity is chosen by which constructor
/// you call — `paimon_predicate_*` (case-sensitive) or the additive
/// `paimon_predicate_*_with_case_sensitive` variant — independently of this
/// setting.
///
/// # Safety
/// `rb` must be a valid pointer from `paimon_table_new_read_builder`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_with_case_sensitive(
    rb: *mut paimon_read_builder,
    case_sensitive: bool,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(rb, "rb") {
        return e;
    }
    let state = &mut *((*rb).inner as *mut ReadBuilderState);
    state.case_sensitive = case_sensitive;
    std::ptr::null_mut()
}

/// Set a filter predicate for scan planning.
///
/// The predicate is consumed (ownership transferred to the read builder).
/// Pass null to clear any previously set filter.
///
/// # Safety
/// `rb` must be a valid pointer from `paimon_table_new_read_builder`, or null (returns error).
/// `predicate` must be a valid pointer from a `paimon_predicate_*` function, or null.
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_with_filter(
    rb: *mut paimon_read_builder,
    predicate: *mut paimon_predicate,
) -> *mut paimon_error {
    if let Err(e) = check_non_null(rb, "rb") {
        return e;
    }

    let state = &mut *((*rb).inner as *mut ReadBuilderState);

    if predicate.is_null() {
        state.filter = None;
        return std::ptr::null_mut();
    }

    let pred_wrapper = Box::from_raw(predicate);
    let pred = Box::from_raw(pred_wrapper.inner as *mut Predicate);
    state.filter = Some(*pred);
    std::ptr::null_mut()
}

/// Create a new TableScan from a ReadBuilder.
///
/// # Safety
/// `rb` must be a valid pointer from `paimon_table_new_read_builder`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_new_scan(
    rb: *const paimon_read_builder,
) -> paimon_result_table_scan {
    if let Err(e) = check_non_null(rb, "rb") {
        return paimon_result_table_scan {
            scan: std::ptr::null_mut(),
            error: e,
        };
    }
    let state = &*((*rb).inner as *const ReadBuilderState);
    let scan_state = TableScanState {
        table: state.table.clone(),
        filter: state.filter.clone(),
    };
    let inner = Box::into_raw(Box::new(scan_state)) as *mut c_void;
    paimon_result_table_scan {
        scan: Box::into_raw(Box::new(paimon_table_scan { inner })),
        error: std::ptr::null_mut(),
    }
}

/// Create a new TableRead from a ReadBuilder.
///
/// # Safety
/// `rb` must be a valid pointer from `paimon_table_new_read_builder`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_new_read(
    rb: *const paimon_read_builder,
) -> paimon_result_new_read {
    if let Err(e) = check_non_null(rb, "rb") {
        return paimon_result_new_read {
            read: std::ptr::null_mut(),
            error: e,
        };
    }
    let state = &*((*rb).inner as *const ReadBuilderState);
    let mut rb_rust = state.table.new_read_builder();
    rb_rust.with_case_sensitive(state.case_sensitive);

    // Apply projection if set
    if let Some(ref columns) = state.projected_columns {
        let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        if let Err(e) = rb_rust.with_projection(&col_refs) {
            return paimon_result_new_read {
                read: std::ptr::null_mut(),
                error: paimon_error::from_paimon(e),
            };
        }
    }

    // Apply filter if set
    if let Some(ref filter) = state.filter {
        rb_rust.with_filter(filter.clone());
    }

    match rb_rust.new_read() {
        Ok(table_read) => {
            let read_state = TableReadState {
                table: state.table.clone(),
                read_type: table_read.read_type().to_vec(),
                data_predicates: table_read.data_predicates().to_vec(),
            };
            paimon_result_new_read {
                read: box_table_read_state(read_state),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_new_read {
            read: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

// ======================= TableScan ===============================

/// Free a paimon_table_scan.
///
/// # Safety
/// Only call with a scan returned from `paimon_read_builder_new_scan`.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_scan_free(scan: *mut paimon_table_scan) {
    if !scan.is_null() {
        let wrapper = Box::from_raw(scan);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut TableScanState));
        }
    }
}

/// Execute a scan plan to get splits.
///
/// # Safety
/// `scan` must be a valid pointer from `paimon_read_builder_new_scan`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_scan_plan(
    scan: *const paimon_table_scan,
) -> paimon_result_plan {
    if let Err(e) = check_non_null(scan, "scan") {
        return paimon_result_plan {
            plan: std::ptr::null_mut(),
            error: e,
        };
    }
    let scan_state = &*((*scan).inner as *const TableScanState);
    let mut rb = scan_state.table.new_read_builder();
    if let Some(ref filter) = scan_state.filter {
        rb.with_filter(filter.clone());
    }
    let table_scan = rb.new_scan();

    match runtime().block_on(table_scan.plan()) {
        Ok(plan) => {
            let wrapper = Box::new(paimon_plan {
                inner: Box::into_raw(Box::new(plan)) as *mut c_void,
            });
            paimon_result_plan {
                plan: Box::into_raw(wrapper),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_plan {
            plan: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

// ======================= Plan ===============================

/// Free a paimon_plan.
///
/// # Safety
/// Only call with a plan returned from `paimon_table_scan_plan`.
#[no_mangle]
pub unsafe extern "C" fn paimon_plan_free(plan: *mut paimon_plan) {
    if !plan.is_null() {
        let p = Box::from_raw(plan);
        if !p.inner.is_null() {
            drop(Box::from_raw(p.inner as *mut Plan));
        }
    }
}

/// Return the number of data splits in a plan.
///
/// # Safety
/// `plan` must be a valid pointer from `paimon_table_scan_plan`, or null (returns 0).
#[no_mangle]
pub unsafe extern "C" fn paimon_plan_num_splits(plan: *const paimon_plan) -> usize {
    if plan.is_null() {
        return 0;
    }
    let plan_ref = &*((*plan).inner as *const Plan);
    plan_ref.splits().len()
}

// ======================= TableRead ===============================

/// Free a paimon_table_read.
///
/// # Safety
/// Only call with a read returned from `paimon_read_builder_new_read`.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_read_free(read: *mut paimon_table_read) {
    if !read.is_null() {
        let wrapper = Box::from_raw(read);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut TableReadState));
        }
    }
}

/// Read table data as Arrow record batches via a streaming reader.
///
/// Returns a `paimon_record_batch_reader` that yields one batch at a time
/// via `paimon_record_batch_reader_next`. This avoids loading all batches
/// into memory at once.
///
/// `offset` and `length` select a contiguous sub-range of splits from the
/// plan. The range is clamped to the available splits (out-of-range values
/// are silently adjusted).
///
/// # Safety
/// `read` and `plan` must be valid pointers from previous paimon C calls, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_read_to_arrow(
    read: *const paimon_table_read,
    plan: *const paimon_plan,
    offset: usize,
    length: usize,
) -> paimon_result_record_batch_reader {
    if let Err(e) = check_non_null(read, "read") {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: e,
        };
    }
    if let Err(e) = check_non_null(plan, "plan") {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error: e,
        };
    }

    let state = &*((*read).inner as *const TableReadState);
    let plan_ref = &*((*plan).inner as *const Plan);
    let all_splits = plan_ref.splits();
    let start = offset.min(all_splits.len());
    let end = (offset.saturating_add(length)).min(all_splits.len());
    let selected = &all_splits[start..end];

    let table_read = paimon::table::TableRead::new(
        &state.table,
        state.read_type.clone(),
        state.data_predicates.clone(),
    );

    match table_read.to_arrow(selected) {
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

// ======================= RecordBatchReader ===============================

/// Get the next Arrow record batch from the reader.
///
/// When the stream is exhausted, both `batch.array` and `batch.schema` will
/// be null. On error, `error` will be non-null.
///
/// After importing each batch, call `paimon_arrow_batch_free` to free the
/// ArrowArray and ArrowSchema container structs.
///
/// # Safety
/// `reader` must be a valid pointer from `paimon_table_read_to_arrow`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_record_batch_reader_next(
    reader: *mut paimon_record_batch_reader,
) -> paimon_result_next_batch {
    if let Err(e) = check_non_null(reader, "reader") {
        return paimon_result_next_batch {
            batch: paimon_arrow_batch {
                array: std::ptr::null_mut(),
                schema: std::ptr::null_mut(),
            },
            error: e,
        };
    }

    let stream = &mut *((*reader).inner as *mut ArrowRecordBatchStream);

    match runtime().block_on(stream.next()) {
        Some(Ok(batch)) => {
            let schema = batch.schema();
            let struct_array = StructArray::from(batch);
            let ffi_array = FFI_ArrowArray::new(&struct_array.to_data());
            let ffi_schema = match FFI_ArrowSchema::try_from(schema.as_ref()) {
                Ok(s) => s,
                Err(e) => {
                    return paimon_result_next_batch {
                        batch: paimon_arrow_batch {
                            array: std::ptr::null_mut(),
                            schema: std::ptr::null_mut(),
                        },
                        error: paimon_error::from_paimon(paimon::Error::UnexpectedError {
                            message: format!("Failed to export Arrow schema: {e}"),
                            source: Some(Box::new(e)),
                        }),
                    };
                }
            };

            let array_ptr = Box::into_raw(Box::new(ffi_array)) as *mut c_void;
            let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as *mut c_void;

            paimon_result_next_batch {
                batch: paimon_arrow_batch {
                    array: array_ptr,
                    schema: schema_ptr,
                },
                error: std::ptr::null_mut(),
            }
        }
        Some(Err(e)) => paimon_result_next_batch {
            batch: paimon_arrow_batch {
                array: std::ptr::null_mut(),
                schema: std::ptr::null_mut(),
            },
            error: paimon_error::from_paimon(e),
        },
        None => paimon_result_next_batch {
            batch: paimon_arrow_batch {
                array: std::ptr::null_mut(),
                schema: std::ptr::null_mut(),
            },
            error: std::ptr::null_mut(),
        },
    }
}

/// Free a paimon_record_batch_reader.
///
/// # Safety
/// Only call with a reader returned from `paimon_table_read_to_arrow`.
#[no_mangle]
pub unsafe extern "C" fn paimon_record_batch_reader_free(reader: *mut paimon_record_batch_reader) {
    if !reader.is_null() {
        let wrapper = Box::from_raw(reader);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut ArrowRecordBatchStream));
        }
    }
}

/// Free the ArrowArray and ArrowSchema container structs for a single batch.
///
/// # Safety
/// `batch` must contain valid pointers returned by `paimon_record_batch_reader_next`.
#[no_mangle]
pub unsafe extern "C" fn paimon_arrow_batch_free(batch: paimon_arrow_batch) {
    if !batch.array.is_null() {
        drop(Box::from_raw(batch.array as *mut FFI_ArrowArray));
    }
    if !batch.schema.is_null() {
        drop(Box::from_raw(batch.schema as *mut FFI_ArrowSchema));
    }
}

// ======================= Predicate ===============================

/// Convert a C datum to a Rust Datum.
unsafe fn datum_from_c(d: &paimon_datum) -> Result<Datum, *mut paimon_error> {
    match d.tag {
        0 => Ok(Datum::Bool(d.int_val != 0)),
        1 => Ok(Datum::TinyInt(d.int_val as i8)),
        2 => Ok(Datum::SmallInt(d.int_val as i16)),
        3 => Ok(Datum::Int(d.int_val as i32)),
        4 => Ok(Datum::Long(d.int_val)),
        5 => Ok(Datum::Float(d.double_val as f32)),
        6 => Ok(Datum::Double(d.double_val)),
        7 => {
            if d.str_len == 0 {
                return Ok(Datum::String(String::new()));
            }
            if d.str_data.is_null() {
                return Err(paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    "null string data in datum with non-zero length".to_string(),
                ));
            }
            let bytes = std::slice::from_raw_parts(d.str_data, d.str_len);
            let s = std::str::from_utf8(bytes).map_err(|e| {
                paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!("invalid UTF-8 in datum string: {e}"),
                )
            })?;
            Ok(Datum::String(s.to_string()))
        }
        8 => Ok(Datum::Date(d.int_val as i32)),
        9 => Ok(Datum::Time(d.int_val as i32)),
        10 => Ok(Datum::Timestamp {
            millis: d.int_val,
            nanos: d.int_val2 as i32,
        }),
        11 => Ok(Datum::LocalZonedTimestamp {
            millis: d.int_val,
            nanos: d.int_val2 as i32,
        }),
        12 => {
            let unscaled = ((d.int_val2 as i128) << 64) | (d.int_val as u64 as i128);
            Ok(Datum::Decimal {
                unscaled,
                precision: d.uint_val,
                scale: d.uint_val2,
            })
        }
        13 => {
            if d.str_data.is_null() && d.str_len > 0 {
                return Err(paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    "null bytes data in datum".to_string(),
                ));
            }
            let bytes = if d.str_len > 0 {
                std::slice::from_raw_parts(d.str_data, d.str_len).to_vec()
            } else {
                Vec::new()
            };
            Ok(Datum::Bytes(bytes))
        }
        _ => Err(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            format!("unknown datum tag: {}", d.tag),
        )),
    }
}

/// Map a C escape byte to the core's `Option<char>` escape parameter.
///
/// `0` (NUL) means "use the core default escape" (`None`; the core resolves it
/// to `\`). Any other byte is the escape character. `c_char` is signed on the
/// target platforms, so the byte is taken through `u8` before `char` to avoid
/// sign extension.
fn escape_char_from_c(escape: std::ffi::c_char) -> Option<char> {
    if escape == 0 {
        None
    } else {
        Some(escape as u8 as char)
    }
}

/// Coerce an integer-family datum to match the target column's integer type.
///
/// FFI callers (e.g. Go) often pass a narrower integer literal (Int) for a
/// wider column (BigInt). This function widens or narrows the datum to match,
/// checking range for narrowing conversions.
///
/// Non-integer datums or non-integer columns are returned as-is.
fn coerce_integer_datum(
    datum: Datum,
    fields: &[DataField],
    column: &str,
    case_sensitive: bool,
) -> Result<Datum, *mut paimon_error> {
    let val = match &datum {
        Datum::TinyInt(v) => *v as i64,
        Datum::SmallInt(v) => *v as i64,
        Datum::Int(v) => *v as i64,
        Datum::Long(v) => *v,
        _ => return Ok(datum),
    };

    // Resolve the column with the same case sensitivity as PredicateBuilder.
    // A non-unique (absent or ambiguous) match is left uncoerced so the
    // PredicateBuilder produces the proper not-found / ambiguous error.
    let field = if case_sensitive {
        fields.iter().find(|f| f.name() == column)
    } else {
        let mut hits = fields
            .iter()
            .filter(|f| f.name().eq_ignore_ascii_case(column));
        match (hits.next(), hits.next()) {
            (Some(f), None) => Some(f),
            _ => None,
        }
    };
    let Some(field) = field else {
        // Column not found / ambiguous; let PredicateBuilder produce the error.
        return Ok(datum);
    };

    match field.data_type() {
        DataType::TinyInt(_) if !matches!(datum, Datum::TinyInt(_)) => {
            if val < i8::MIN as i64 || val > i8::MAX as i64 {
                Err(paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!("value {val} out of range for TinyInt column '{column}'"),
                ))
            } else {
                Ok(Datum::TinyInt(val as i8))
            }
        }
        DataType::SmallInt(_) if !matches!(datum, Datum::SmallInt(_)) => {
            if val < i16::MIN as i64 || val > i16::MAX as i64 {
                Err(paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!("value {val} out of range for SmallInt column '{column}'"),
                ))
            } else {
                Ok(Datum::SmallInt(val as i16))
            }
        }
        DataType::Int(_) if !matches!(datum, Datum::Int(_)) => {
            if val < i32::MIN as i64 || val > i32::MAX as i64 {
                Err(paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    format!("value {val} out of range for Int column '{column}'"),
                ))
            } else {
                Ok(Datum::Int(val as i32))
            }
        }
        DataType::BigInt(_) if !matches!(datum, Datum::Long(_)) => Ok(Datum::Long(val)),
        _ => Ok(datum),
    }
}

/// Helper to build a leaf predicate that takes a datum, via PredicateBuilder.
unsafe fn build_leaf_predicate_datum(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: &paimon_datum,
    case_sensitive: bool,
    build_fn: impl FnOnce(&PredicateBuilder, &str, Datum) -> paimon::Result<Predicate>,
) -> paimon_result_predicate {
    if let Err(e) = check_non_null(table, "table") {
        return paimon_result_predicate {
            predicate: std::ptr::null_mut(),
            error: e,
        };
    }
    let col_name = match validate_cstr(column, "column") {
        Ok(s) => s,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };

    let d = match datum_from_c(datum) {
        Ok(d) => d,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };

    let table_ref = &*((*table).inner as *const Table);
    let fields = table_ref.schema().fields();

    let d = match coerce_integer_datum(d, fields, &col_name, case_sensitive) {
        Ok(d) => d,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };

    let pb = PredicateBuilder::new_with_case_sensitive(fields, case_sensitive);
    match build_fn(&pb, &col_name, d) {
        Ok(pred) => {
            let inner = Box::into_raw(Box::new(pred)) as *mut c_void;
            paimon_result_predicate {
                predicate: Box::into_raw(Box::new(paimon_predicate { inner })),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_predicate {
            predicate: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

/// Helper to build a leaf predicate without a datum (IS NULL / IS NOT NULL).
unsafe fn build_leaf_predicate(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    case_sensitive: bool,
    build_fn: impl FnOnce(&PredicateBuilder, &str) -> paimon::Result<Predicate>,
) -> paimon_result_predicate {
    if let Err(e) = check_non_null(table, "table") {
        return paimon_result_predicate {
            predicate: std::ptr::null_mut(),
            error: e,
        };
    }
    let col_name = match validate_cstr(column, "column") {
        Ok(s) => s,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };
    let table_ref = &*((*table).inner as *const Table);
    let pb = PredicateBuilder::new_with_case_sensitive(table_ref.schema().fields(), case_sensitive);
    match build_fn(&pb, &col_name) {
        Ok(pred) => {
            let inner = Box::into_raw(Box::new(pred)) as *mut c_void;
            paimon_result_predicate {
                predicate: Box::into_raw(Box::new(paimon_predicate { inner })),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_predicate {
            predicate: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

/// Create an equality predicate: `column = datum` (case-sensitive column match).
///
/// For case-insensitive column matching use
/// `paimon_predicate_equal_with_case_sensitive`.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_equal(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| pb.equal(col, d))
}

/// Create an equality predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_equal_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.equal(col, d)
    })
}

/// Create a not-equal predicate: `column != datum` (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_not_equal(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| {
        pb.not_equal(col, d)
    })
}

/// Create a not-equal predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_not_equal_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.not_equal(col, d)
    })
}

/// Create a less-than predicate: `column < datum` (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_less_than(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| {
        pb.less_than(col, d)
    })
}

/// Create a less-than predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_less_than_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.less_than(col, d)
    })
}

/// Create a less-or-equal predicate: `column <= datum` (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_less_or_equal(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| {
        pb.less_or_equal(col, d)
    })
}

/// Create a less-or-equal predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_less_or_equal_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.less_or_equal(col, d)
    })
}

/// Create a greater-than predicate: `column > datum` (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_greater_than(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| {
        pb.greater_than(col, d)
    })
}

/// Create a greater-than predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_greater_than_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.greater_than(col, d)
    })
}

/// Create a greater-or-equal predicate: `column >= datum` (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_greater_or_equal(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| {
        pb.greater_or_equal(col, d)
    })
}

/// Create a greater-or-equal predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_greater_or_equal_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.greater_or_equal(col, d)
    })
}

/// Create an IS NULL predicate (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_null(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
) -> paimon_result_predicate {
    build_leaf_predicate(table, column, true, |pb, col| pb.is_null(col))
}

/// Create an IS NULL predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_null_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate(table, column, case_sensitive, |pb, col| pb.is_null(col))
}

/// Create an IS NOT NULL predicate (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_not_null(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
) -> paimon_result_predicate {
    build_leaf_predicate(table, column, true, |pb, col| pb.is_not_null(col))
}

/// Create an IS NOT NULL predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_not_null_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate(table, column, case_sensitive, |pb, col| pb.is_not_null(col))
}

/// Create an IN predicate: `column IN (datum1, datum2, ...)` (case-sensitive column match).
///
/// # Safety
/// `table`, `column`, and `datums` must be valid pointers. `datums_len` must be the length.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_in(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datums: *const paimon_datum,
    datums_len: usize,
) -> paimon_result_predicate {
    build_leaf_predicate_datums(
        table,
        column,
        datums,
        datums_len,
        true,
        |pb, col, values| pb.is_in(col, values),
    )
}

/// Create an IN predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table`, `column`, and `datums` must be valid pointers. `datums_len` must be the length.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_in_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datums: *const paimon_datum,
    datums_len: usize,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datums(
        table,
        column,
        datums,
        datums_len,
        case_sensitive,
        |pb, col, values| pb.is_in(col, values),
    )
}

/// Create a NOT IN predicate: `column NOT IN (datum1, datum2, ...)` (case-sensitive column match).
///
/// # Safety
/// `table`, `column`, and `datums` must be valid pointers. `datums_len` must be the length.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_not_in(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datums: *const paimon_datum,
    datums_len: usize,
) -> paimon_result_predicate {
    build_leaf_predicate_datums(
        table,
        column,
        datums,
        datums_len,
        true,
        |pb, col, values| pb.is_not_in(col, values),
    )
}

/// Create a NOT IN predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table`, `column`, and `datums` must be valid pointers. `datums_len` must be the length.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_not_in_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datums: *const paimon_datum,
    datums_len: usize,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datums(
        table,
        column,
        datums,
        datums_len,
        case_sensitive,
        |pb, col, values| pb.is_not_in(col, values),
    )
}

/// Create a starts-with predicate: `column LIKE 'datum%'` (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_starts_with(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| {
        pb.starts_with(col, d)
    })
}

/// Create a starts-with predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_starts_with_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.starts_with(col, d)
    })
}

/// Create an ends-with predicate: `column LIKE '%datum'` (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_ends_with(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| {
        pb.ends_with(col, d)
    })
}

/// Create an ends-with predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_ends_with_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.ends_with(col, d)
    })
}

/// Create a contains predicate: `column LIKE '%datum%'` (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_contains(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, true, |pb, col, d| {
        pb.contains(col, d)
    })
}

/// Create a contains predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_contains_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, case_sensitive, |pb, col, d| {
        pb.contains(col, d)
    })
}

/// Create a LIKE predicate: `column LIKE pattern ESCAPE escape` (case-sensitive
/// column match). `escape == 0` uses the default escape character.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_like(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    pattern: paimon_datum,
    escape: std::ffi::c_char,
) -> paimon_result_predicate {
    let escape_opt = escape_char_from_c(escape);
    build_leaf_predicate_datum(table, column, &pattern, true, move |pb, col, d| {
        pb.like(col, d, escape_opt)
    })
}

/// Create a LIKE predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_like_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    pattern: paimon_datum,
    escape: std::ffi::c_char,
    case_sensitive: bool,
) -> paimon_result_predicate {
    let escape_opt = escape_char_from_c(escape);
    build_leaf_predicate_datum(
        table,
        column,
        &pattern,
        case_sensitive,
        move |pb, col, d| pb.like(col, d, escape_opt),
    )
}

/// Create a BETWEEN predicate: `low <= column <= high` (inclusive, case-sensitive
/// column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_between(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    low: paimon_datum,
    high: paimon_datum,
) -> paimon_result_predicate {
    let datums = [low, high];
    build_leaf_predicate_datums(table, column, datums.as_ptr(), 2, true, |pb, col, ds| {
        pb.between(col, ds[0].clone(), ds[1].clone())
    })
}

/// Create a BETWEEN predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_between_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    low: paimon_datum,
    high: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    let datums = [low, high];
    build_leaf_predicate_datums(
        table,
        column,
        datums.as_ptr(),
        2,
        case_sensitive,
        |pb, col, ds| pb.between(col, ds[0].clone(), ds[1].clone()),
    )
}

/// Create a NOT BETWEEN predicate: `column < low OR column > high`
/// (case-sensitive column match).
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_not_between(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    low: paimon_datum,
    high: paimon_datum,
) -> paimon_result_predicate {
    let datums = [low, high];
    build_leaf_predicate_datums(table, column, datums.as_ptr(), 2, true, |pb, col, ds| {
        pb.not_between(col, ds[0].clone(), ds[1].clone())
    })
}

/// Create a NOT BETWEEN predicate with configurable column-name case sensitivity.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_not_between_with_case_sensitive(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    low: paimon_datum,
    high: paimon_datum,
    case_sensitive: bool,
) -> paimon_result_predicate {
    let datums = [low, high];
    build_leaf_predicate_datums(
        table,
        column,
        datums.as_ptr(),
        2,
        case_sensitive,
        |pb, col, ds| pb.not_between(col, ds[0].clone(), ds[1].clone()),
    )
}

/// Helper to build a predicate from a datum array (IN / NOT IN, BETWEEN / NOT
/// BETWEEN).
unsafe fn build_leaf_predicate_datums(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datums: *const paimon_datum,
    datums_len: usize,
    case_sensitive: bool,
    build_fn: impl FnOnce(&PredicateBuilder, &str, Vec<Datum>) -> paimon::Result<Predicate>,
) -> paimon_result_predicate {
    if let Err(e) = check_non_null(table, "table") {
        return paimon_result_predicate {
            predicate: std::ptr::null_mut(),
            error: e,
        };
    }
    let col_name = match validate_cstr(column, "column") {
        Ok(s) => s,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };

    if datums.is_null() && datums_len > 0 {
        return paimon_result_predicate {
            predicate: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "null datums pointer with non-zero length".to_string(),
            ),
        };
    }

    let slice = if datums_len > 0 {
        std::slice::from_raw_parts(datums, datums_len)
    } else {
        &[]
    };
    let values: Result<Vec<Datum>, _> = slice.iter().map(|d| datum_from_c(d)).collect();
    let values = match values {
        Ok(v) => v,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };

    let table_ref = &*((*table).inner as *const Table);
    let fields = table_ref.schema().fields();

    let values: Result<Vec<Datum>, _> = values
        .into_iter()
        .map(|d| coerce_integer_datum(d, fields, &col_name, case_sensitive))
        .collect();
    let values = match values {
        Ok(v) => v,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };

    let pb = PredicateBuilder::new_with_case_sensitive(fields, case_sensitive);
    match build_fn(&pb, &col_name, values) {
        Ok(pred) => {
            let inner = Box::into_raw(Box::new(pred)) as *mut c_void;
            paimon_result_predicate {
                predicate: Box::into_raw(Box::new(paimon_predicate { inner })),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_predicate {
            predicate: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

/// Combine two predicates with AND. Consumes both inputs.
///
/// # Safety
/// `a` and `b` must be valid pointers from predicate functions.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_and(
    a: *mut paimon_predicate,
    b: *mut paimon_predicate,
) -> *mut paimon_predicate {
    let pred_a = *Box::from_raw(Box::from_raw(a).inner as *mut Predicate);
    let pred_b = *Box::from_raw(Box::from_raw(b).inner as *mut Predicate);
    let combined = Predicate::and(vec![pred_a, pred_b]);
    let inner = Box::into_raw(Box::new(combined)) as *mut c_void;
    Box::into_raw(Box::new(paimon_predicate { inner }))
}

/// Combine two predicates with OR. Consumes both inputs.
///
/// # Safety
/// `a` and `b` must be valid pointers from predicate functions.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_or(
    a: *mut paimon_predicate,
    b: *mut paimon_predicate,
) -> *mut paimon_predicate {
    let pred_a = *Box::from_raw(Box::from_raw(a).inner as *mut Predicate);
    let pred_b = *Box::from_raw(Box::from_raw(b).inner as *mut Predicate);
    let combined = Predicate::or(vec![pred_a, pred_b]);
    let inner = Box::into_raw(Box::new(combined)) as *mut c_void;
    Box::into_raw(Box::new(paimon_predicate { inner }))
}

/// Negate a predicate with NOT. Consumes the input.
///
/// # Safety
/// `p` must be a valid pointer from a predicate function.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_not(p: *mut paimon_predicate) -> *mut paimon_predicate {
    let pred = *Box::from_raw(Box::from_raw(p).inner as *mut Predicate);
    let negated = Predicate::negate(pred);
    let inner = Box::into_raw(Box::new(negated)) as *mut c_void;
    Box::into_raw(Box::new(paimon_predicate { inner }))
}

/// Free a paimon_predicate.
///
/// # Safety
/// Only call with a predicate returned from paimon predicate functions.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_free(p: *mut paimon_predicate) {
    if !p.is_null() {
        let wrapper = Box::from_raw(p);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut Predicate));
        }
    }
}

// --- C ABI signature guards -------------------------------------------------
//
// The `paimon_predicate_*` constructors are called across the FFI boundary with
// fixed argument counts: the Go binding prepares a libffi call interface (CIF)
// per symbol (see `bindings/go/predicate.go`), and external consumers can link
// against the generated headers (e.g. Doris integrations). Adding a parameter to
// one of these existing symbols silently breaks every such caller — the extra
// argument is read from an undefined register/stack slot at the ABI boundary.
//
// These compile-time assertions pin the existing signatures. To add behavior
// (e.g. case-insensitive column matching), introduce a new
// `paimon_predicate_*_with_case_sensitive` symbol instead of changing one of
// these; touching a signature here will fail to compile.
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_equal;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_not_equal;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_less_than;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_less_or_equal;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_greater_than;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_greater_or_equal;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
) -> paimon_result_predicate = paimon_predicate_is_null;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
) -> paimon_result_predicate = paimon_predicate_is_not_null;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    *const paimon_datum,
    usize,
) -> paimon_result_predicate = paimon_predicate_is_in;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    *const paimon_datum,
    usize,
) -> paimon_result_predicate = paimon_predicate_is_not_in;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_starts_with;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_ends_with;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_contains;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
    std::ffi::c_char,
) -> paimon_result_predicate = paimon_predicate_like;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_between;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const std::ffi::c_char,
    paimon_datum,
    paimon_datum,
) -> paimon_result_predicate = paimon_predicate_not_between;

// Read builder ABI signature guards. These pin the C-linked read-builder
// constructors so an accidental signature change fails to compile rather than
// silently breaking header consumers. To add behavior, introduce a new
// `paimon_table_new_read_builder_*` symbol instead of changing one of these.
const _: unsafe extern "C" fn(*const paimon_table) -> paimon_result_read_builder =
    paimon_table_new_read_builder;
const _: unsafe extern "C" fn(
    *const paimon_table,
    *const paimon_option,
    usize,
) -> paimon_result_read_builder = paimon_table_new_read_builder_with_options;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::paimon_error_free;
    use paimon::catalog::Identifier;
    use paimon::io::FileIOBuilder;
    use paimon::spec::{DataType, IntType, Schema, TableSchema, VarCharType};
    use paimon::table::Table;
    use std::ffi::CString;

    /// Build an in-memory table with one varchar column `name` and one int
    /// column `age`, boxed in the exact wrapper shape `paimon_table_free`
    /// expects, so the same free path is exercised.
    fn boxed_test_table() -> *mut paimon_table {
        let schema = Schema::builder()
            .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
            .column("age", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let table = Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "c_predicate_test"),
            "memory:/c_predicate_test".to_string(),
            TableSchema::new(0, &schema),
            None,
        );
        let inner = Box::into_raw(Box::new(table)) as *mut std::ffi::c_void;
        Box::into_raw(Box::new(paimon_table { inner }))
    }

    /// A string `paimon_datum` borrowing `s` (kept alive by the caller).
    fn string_datum(s: &std::ffi::CStr) -> paimon_datum {
        let bytes = s.to_bytes();
        paimon_datum {
            tag: 7,
            int_val: 0,
            double_val: 0.0,
            str_data: bytes.as_ptr(),
            str_len: bytes.len(),
            int_val2: 0,
            uint_val: 0,
            uint_val2: 0,
        }
    }

    /// An int (tag 3) `paimon_datum`.
    fn int_datum(v: i64) -> paimon_datum {
        paimon_datum {
            tag: 3,
            int_val: v,
            double_val: 0.0,
            str_data: std::ptr::null(),
            str_len: 0,
            int_val2: 0,
            uint_val: 0,
            uint_val2: 0,
        }
    }

    /// Assert the result is a built predicate (predicate non-null, error null)
    /// and free it.
    unsafe fn assert_ok_and_free(result: paimon_result_predicate) {
        assert!(!result.predicate.is_null(), "expected a predicate");
        assert!(result.error.is_null(), "expected no error");
        paimon_predicate_free(result.predicate);
    }

    /// Assert the result is an error (predicate null, error non-null) and free it.
    unsafe fn assert_err_and_free(result: paimon_result_predicate) {
        assert!(result.predicate.is_null(), "expected no predicate");
        assert!(!result.error.is_null(), "expected an error");
        paimon_error_free(result.error);
    }

    #[test]
    fn escape_char_from_c_maps_sentinel_and_bytes() {
        assert_eq!(escape_char_from_c(0), None);
        assert_eq!(escape_char_from_c(b'\\' as std::ffi::c_char), Some('\\'));
    }

    #[test]
    fn string_ops_build_predicates() {
        unsafe {
            let table = boxed_test_table();
            let col = CString::new("name").unwrap();
            let pat = CString::new("ab").unwrap();

            assert_ok_and_free(paimon_predicate_starts_with(
                table,
                col.as_ptr(),
                string_datum(&pat),
            ));
            assert_ok_and_free(paimon_predicate_ends_with(
                table,
                col.as_ptr(),
                string_datum(&pat),
            ));
            assert_ok_and_free(paimon_predicate_contains(
                table,
                col.as_ptr(),
                string_datum(&pat),
            ));

            paimon_table_free(table);
        }
    }

    #[test]
    fn like_builds_and_rejects_bad_escape() {
        unsafe {
            let table = boxed_test_table();
            let col = CString::new("name").unwrap();
            let pat = CString::new("ab%").unwrap();

            // Default escape (sentinel 0) -> resolves to core default '\'.
            assert_ok_and_free(paimon_predicate_like(
                table,
                col.as_ptr(),
                string_datum(&pat),
                0,
            ));
            // Explicit backslash escape -> accepted.
            assert_ok_and_free(paimon_predicate_like(
                table,
                col.as_ptr(),
                string_datum(&pat),
                b'\\' as std::ffi::c_char,
            ));
            // Non-backslash escape -> core rejects -> error result.
            assert_err_and_free(paimon_predicate_like(
                table,
                col.as_ptr(),
                string_datum(&pat),
                b'/' as std::ffi::c_char,
            ));

            paimon_table_free(table);
        }
    }

    #[test]
    fn contains_rejects_non_string_datum() {
        unsafe {
            let table = boxed_test_table();
            let col = CString::new("name").unwrap();
            // An int datum into a string op -> core's typed error, not a panic.
            assert_err_and_free(paimon_predicate_contains(table, col.as_ptr(), int_datum(5)));
            paimon_table_free(table);
        }
    }

    #[test]
    fn range_ops_build_predicates() {
        unsafe {
            let table = boxed_test_table();
            let col = CString::new("age").unwrap();

            // low <= high -> normal predicate.
            assert_ok_and_free(paimon_predicate_between(
                table,
                col.as_ptr(),
                int_datum(10),
                int_datum(20),
            ));
            assert_ok_and_free(paimon_predicate_not_between(
                table,
                col.as_ptr(),
                int_datum(10),
                int_datum(20),
            ));
            // low > high -> core short-circuits (AlwaysFalse / is_not_null),
            // still a built predicate, not an error.
            assert_ok_and_free(paimon_predicate_between(
                table,
                col.as_ptr(),
                int_datum(20),
                int_datum(10),
            ));

            paimon_table_free(table);
        }
    }

    #[test]
    fn with_case_sensitive_variants_build_predicates() {
        // Touch every `_with_case_sensitive` symbol so all twelve new FFI
        // entry points are exercised, not just the default-case-sensitive ones.
        unsafe {
            let table = boxed_test_table();
            let name = CString::new("name").unwrap();
            let age = CString::new("age").unwrap();
            let pat = CString::new("ab").unwrap();
            let like_pat = CString::new("ab%").unwrap();

            assert_ok_and_free(paimon_predicate_starts_with_with_case_sensitive(
                table,
                name.as_ptr(),
                string_datum(&pat),
                false,
            ));
            assert_ok_and_free(paimon_predicate_ends_with_with_case_sensitive(
                table,
                name.as_ptr(),
                string_datum(&pat),
                false,
            ));
            assert_ok_and_free(paimon_predicate_contains_with_case_sensitive(
                table,
                name.as_ptr(),
                string_datum(&pat),
                false,
            ));
            assert_ok_and_free(paimon_predicate_like_with_case_sensitive(
                table,
                name.as_ptr(),
                string_datum(&like_pat),
                0,
                false,
            ));
            assert_ok_and_free(paimon_predicate_between_with_case_sensitive(
                table,
                age.as_ptr(),
                int_datum(10),
                int_datum(20),
                false,
            ));
            assert_ok_and_free(paimon_predicate_not_between_with_case_sensitive(
                table,
                age.as_ptr(),
                int_datum(10),
                int_datum(20),
                false,
            ));

            paimon_table_free(table);
        }
    }

    /// Assert the result is a built read builder (read_builder non-null, error
    /// null) and free it.
    unsafe fn assert_rb_ok_and_free(r: paimon_result_read_builder) {
        assert!(!r.read_builder.is_null(), "expected a read builder");
        assert!(r.error.is_null(), "expected no error");
        paimon_read_builder_free(r.read_builder);
    }

    /// Assert the result is an error (read_builder null, error non-null) and free
    /// it.
    unsafe fn assert_rb_err_and_free(r: paimon_result_read_builder) {
        assert!(r.read_builder.is_null(), "expected no read builder");
        assert!(!r.error.is_null(), "expected an error");
        paimon_error_free(r.error);
    }

    /// Assert the result is an error, extract its code and message, then free the
    /// error exactly once. Lets a test pin the error identity (which failure
    /// fired) instead of only its shape.
    unsafe fn assert_rb_err_code_message(r: paimon_result_read_builder) -> (i32, String) {
        assert!(r.read_builder.is_null(), "expected no read builder");
        assert!(!r.error.is_null(), "expected an error");
        let err = &*r.error;
        let code = err.code;
        let message = if err.message.data.is_null() {
            String::new()
        } else {
            let bytes = std::slice::from_raw_parts(err.message.data, err.message.len);
            String::from_utf8_lossy(bytes).into_owned()
        };
        paimon_error_free(r.error);
        (code, message)
    }

    /// A `paimon_option` borrowing `key`/`value` (kept alive by the caller).
    fn opt(key: &std::ffi::CStr, value: &std::ffi::CStr) -> paimon_option {
        paimon_option {
            key: key.as_ptr(),
            value: value.as_ptr(),
        }
    }

    #[test]
    fn empty_options_builds_like_plain_new_read_builder() {
        unsafe {
            let table = boxed_test_table();
            // Null pointer + zero length.
            assert_rb_ok_and_free(paimon_table_new_read_builder_with_options(
                table,
                std::ptr::null(),
                0,
            ));
            // Non-null pointer to a zero-length array + zero length.
            let empty: [paimon_option; 0] = [];
            assert_rb_ok_and_free(paimon_table_new_read_builder_with_options(
                table,
                empty.as_ptr(),
                0,
            ));
            // The plain entry point (delegates with an empty map).
            assert_rb_ok_and_free(paimon_table_new_read_builder(table));
            paimon_table_free(table);
        }
    }

    #[test]
    fn non_selector_option_builds() {
        unsafe {
            let table = boxed_test_table();
            let k = CString::new("some.unrelated.option").unwrap();
            let v = CString::new("value").unwrap();
            let opts = [opt(&k, &v)];
            assert_rb_ok_and_free(paimon_table_new_read_builder_with_options(
                table,
                opts.as_ptr(),
                1,
            ));
            paimon_table_free(table);
        }
    }

    #[test]
    fn more_than_one_selector_is_rejected() {
        unsafe {
            let table = boxed_test_table();
            let k1 = CString::new("scan.snapshot-id").unwrap();
            let v1 = CString::new("1").unwrap();
            let k2 = CString::new("scan.tag-name").unwrap();
            let v2 = CString::new("t").unwrap();
            let opts = [opt(&k1, &v1), opt(&k2, &v2)];
            let (code, message) = assert_rb_err_code_message(
                paimon_table_new_read_builder_with_options(table, opts.as_ptr(), 2),
            );
            // Binding-constructed rejection: InvalidInput naming both selectors.
            assert_eq!(code, PaimonErrorCode::InvalidInput as i32);
            assert!(
                message.contains("scan.snapshot-id") && message.contains("scan.tag-name"),
                "message should name both selectors, got: {message}"
            );
            paimon_table_free(table);
        }
    }

    #[test]
    fn unsupported_scan_option_is_rejected() {
        unsafe {
            let table = boxed_test_table();
            let k = CString::new("scan.watermark").unwrap();
            let v = CString::new("0").unwrap();
            let opts = [opt(&k, &v)];
            // Core's validate_scan_options rejects this before resolution; the
            // binding surfaces core's Unsupported code.
            let (code, message) = assert_rb_err_code_message(
                paimon_table_new_read_builder_with_options(table, opts.as_ptr(), 1),
            );
            assert_eq!(code, PaimonErrorCode::Unsupported as i32);
            assert!(
                message.contains("not supported"),
                "message should say the option is not supported, got: {message}"
            );
            paimon_table_free(table);
        }
    }

    #[test]
    fn malformed_selector_value_does_not_silently_read_latest() {
        unsafe {
            let table = boxed_test_table();
            let k = CString::new("scan.snapshot-id").unwrap();
            let v = CString::new("abc").unwrap();
            let opts = [opt(&k, &v)];
            // Core swallows the parse error and falls back; the binding reports
            // the unified "did not resolve" error rather than building a
            // latest-reading builder.
            let (code, message) = assert_rb_err_code_message(
                paimon_table_new_read_builder_with_options(table, opts.as_ptr(), 1),
            );
            assert_eq!(code, PaimonErrorCode::InvalidInput as i32);
            assert!(
                message.contains("did not resolve"),
                "message should report the selector did not resolve, got: {message}"
            );
            paimon_table_free(table);
        }
    }

    #[test]
    fn null_options_with_nonzero_length_is_rejected() {
        unsafe {
            let table = boxed_test_table();
            assert_rb_err_and_free(paimon_table_new_read_builder_with_options(
                table,
                std::ptr::null(),
                2,
            ));
            paimon_table_free(table);
        }
    }
}
