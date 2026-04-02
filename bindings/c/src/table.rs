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
use paimon::table::{ArrowRecordBatchStream, Table};
use paimon::Plan;

use crate::error::{check_non_null, paimon_error};
use crate::result::{
    paimon_result_new_read, paimon_result_next_batch, paimon_result_plan,
    paimon_result_read_builder, paimon_result_record_batch_reader, paimon_result_table_scan,
};
use crate::runtime;
use crate::types::*;

// Helper to box a Table clone into a wrapper struct and return a raw pointer.
unsafe fn box_table_wrapper<T>(table: &Table, make: impl FnOnce(*mut c_void) -> T) -> *mut T {
    let inner = Box::into_raw(Box::new(table.clone())) as *mut c_void;
    Box::into_raw(Box::new(make(inner)))
}

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
/// cause `paimon_read_builder_new_read()` to fail; an empty list is a valid
/// zero-column projection.
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

    state.projected_columns = Some(col_names);
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
    paimon_result_table_scan {
        scan: box_table_wrapper(&state.table, |inner| paimon_table_scan { inner }),
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
        rb_rust.with_projection(&col_refs);
    }

    match rb_rust.new_read() {
        Ok(table_read) => {
            let read_state = TableReadState {
                table: state.table.clone(),
                read_type: table_read.read_type().to_vec(),
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
    free_table_wrapper(scan, |s| s.inner);
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
    let table = &*((*scan).inner as *const Table);
    let rb = table.new_read_builder();
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

    // Create TableRead with the stored read_type (projection)
    let table_read = paimon::table::TableRead::new(&state.table, state.read_type.clone());

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
