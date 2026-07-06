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

/// Create a new ReadBuilder from a Table.
///
/// # Safety
/// `table` must be a valid pointer from `paimon_catalog_get_table`, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_table_new_read_builder(
    table: *const paimon_table,
) -> paimon_result_read_builder {
    if let Err(e) = check_non_null(table, "table") {
        return paimon_result_read_builder {
            read_builder: std::ptr::null_mut(),
            error: e,
        };
    }
    let table_ref = &*((*table).inner as *const Table);
    let state = ReadBuilderState {
        table: table_ref.clone(),
        projected_columns: None,
        filter: None,
    };
    paimon_result_read_builder {
        read_builder: box_read_builder_state(state),
        error: std::ptr::null_mut(),
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
/// Output order follows the caller-specified order. Unknown or duplicate names
/// are validated immediately; an empty list is a valid zero-column projection.
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

    let col_refs: Vec<&str> = col_names.iter().map(String::as_str).collect();
    if let Err(e) = state.table.new_read_builder().with_projection(&col_refs) {
        return paimon_error::from_paimon(e);
    }

    state.projected_columns = Some(col_names);
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
) -> Result<Datum, *mut paimon_error> {
    let val = match &datum {
        Datum::TinyInt(v) => *v as i64,
        Datum::SmallInt(v) => *v as i64,
        Datum::Int(v) => *v as i64,
        Datum::Long(v) => *v,
        _ => return Ok(datum),
    };

    let Some(field) = fields.iter().find(|f| f.name() == column) else {
        // Column not found; let PredicateBuilder produce the proper error.
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

    let d = match coerce_integer_datum(d, fields, &col_name) {
        Ok(d) => d,
        Err(e) => {
            return paimon_result_predicate {
                predicate: std::ptr::null_mut(),
                error: e,
            }
        }
    };

    let pb = PredicateBuilder::new(fields);
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
    let pb = PredicateBuilder::new(table_ref.schema().fields());
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

/// Create an equality predicate: `column = datum`.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_equal(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, |pb, col, d| pb.equal(col, d))
}

/// Create a not-equal predicate: `column != datum`.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_not_equal(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, |pb, col, d| pb.not_equal(col, d))
}

/// Create a less-than predicate: `column < datum`.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_less_than(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, |pb, col, d| pb.less_than(col, d))
}

/// Create a less-or-equal predicate: `column <= datum`.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_less_or_equal(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, |pb, col, d| pb.less_or_equal(col, d))
}

/// Create a greater-than predicate: `column > datum`.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_greater_than(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, |pb, col, d| pb.greater_than(col, d))
}

/// Create a greater-or-equal predicate: `column >= datum`.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_greater_or_equal(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datum: paimon_datum,
) -> paimon_result_predicate {
    build_leaf_predicate_datum(table, column, &datum, |pb, col, d| {
        pb.greater_or_equal(col, d)
    })
}

/// Create an IS NULL predicate.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_null(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
) -> paimon_result_predicate {
    build_leaf_predicate(table, column, |pb, col| pb.is_null(col))
}

/// Create an IS NOT NULL predicate.
///
/// # Safety
/// `table` and `column` must be valid pointers.
#[no_mangle]
pub unsafe extern "C" fn paimon_predicate_is_not_null(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
) -> paimon_result_predicate {
    build_leaf_predicate(table, column, |pb, col| pb.is_not_null(col))
}

/// Create an IN predicate: `column IN (datum1, datum2, ...)`.
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
    build_leaf_predicate_datums(table, column, datums, datums_len, |pb, col, values| {
        pb.is_in(col, values)
    })
}

/// Create a NOT IN predicate: `column NOT IN (datum1, datum2, ...)`.
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
    build_leaf_predicate_datums(table, column, datums, datums_len, |pb, col, values| {
        pb.is_not_in(col, values)
    })
}

/// Helper to build an IN/NOT IN predicate with a datum array.
unsafe fn build_leaf_predicate_datums(
    table: *const paimon_table,
    column: *const std::ffi::c_char,
    datums: *const paimon_datum,
    datums_len: usize,
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
        .map(|d| coerce_integer_datum(d, fields, &col_name))
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

    let pb = PredicateBuilder::new(fields);
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
