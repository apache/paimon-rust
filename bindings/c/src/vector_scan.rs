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

//! Common vector plans for DE and PK execution.
use crate::error::{check_non_null, paimon_error, PaimonErrorCode};
use crate::result::paimon_result_vector_plan;
use crate::runtime;
use crate::types::*;
use paimon::table::{BucketVectorSearchSplit, VectorScan, VectorScanPlan};
use std::ffi::c_void;

fn wrap_plan(result: paimon::Result<VectorScanPlan>) -> paimon_result_vector_plan {
    match result {
        Ok(plan) => paimon_result_vector_plan {
            plan: Box::into_raw(Box::new(paimon_vector_plan {
                inner: Box::into_raw(Box::new(plan)) as *mut c_void,
            })),
            error: std::ptr::null_mut(),
        },
        Err(error) => paimon_result_vector_plan {
            plan: std::ptr::null_mut(),
            error: paimon_error::from_paimon(error),
        },
    }
}

unsafe fn scan_ref<'a>(
    scan: *const paimon_vector_scan,
) -> Result<&'a VectorScan, *mut paimon_error> {
    check_non_null(scan, "scan")?;
    check_non_null((*scan).inner, "scan is not initialized")?;
    Ok(&*((*scan).inner as *const VectorScan))
}

/// Resolve a snapshot and plan vector search for either a DE or PK table.
/// The returned plan is independent of the scan; free it with paimon_vector_plan_free.
/// # Safety
/// scan must be a live handle from paimon_vector_search_builder_new_scan, or null (error).
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_scan_plan(
    scan: *const paimon_vector_scan,
) -> paimon_result_vector_plan {
    match scan_ref(scan) {
        Ok(scan) => wrap_plan(runtime().block_on(scan.plan())),
        Err(error) => paimon_result_vector_plan {
            plan: std::ptr::null_mut(),
            error,
        },
    }
}

/// Construct a common read plan from already-decoded Java PK bucket splits.
/// No snapshot or index manifest is read. Handles are borrowed and copied;
/// callers may free them as soon as this returns. Failure leaves inputs intact.
/// Empty input, DE scans and mixed snapshots are rejected.
/// # Safety
/// scan must be a live scan handle. splits must point to count live split pointers;
/// null pointers, uninitialized handles and zero/oversized counts return an error.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_scan_plan_from_bucket_splits(
    scan: *const paimon_vector_scan,
    splits: *const *const paimon_bucket_vector_search_split,
    count: usize,
) -> paimon_result_vector_plan {
    let scan = match scan_ref(scan) {
        Ok(scan) => scan,
        Err(error) => {
            return paimon_result_vector_plan {
                plan: std::ptr::null_mut(),
                error,
            }
        }
    };
    if splits.is_null()
        || count == 0
        || count
            > isize::MAX as usize / std::mem::size_of::<*const paimon_bucket_vector_search_split>()
    {
        return paimon_result_vector_plan {
            plan: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "null, empty or oversized split array".to_string(),
            ),
        };
    }
    let mut decoded = Vec::with_capacity(count);
    for &split in std::slice::from_raw_parts(splits, count) {
        if split.is_null() || (*split).inner.is_null() {
            return paimon_result_vector_plan {
                plan: std::ptr::null_mut(),
                error: paimon_error::new(
                    PaimonErrorCode::InvalidInput,
                    "split is null or not initialized".to_string(),
                ),
            };
        }
        decoded.push((&*((*split).inner as *const BucketVectorSearchSplit)).clone());
    }
    wrap_plan(scan.plan_from_bucket_splits(decoded))
}

/// Free a vector scan. Null is accepted.
/// # Safety
/// scan must be a live owned scan handle, or null.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_scan_free(scan: *mut paimon_vector_scan) {
    if !scan.is_null() {
        let wrapper = Box::from_raw(scan);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut VectorScan));
        }
    }
}

/// Free a vector plan. Null is accepted.
/// # Safety
/// plan must be a live owned vector plan handle, or null.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_plan_free(plan: *mut paimon_vector_plan) {
    if !plan.is_null() {
        let wrapper = Box::from_raw(plan);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut VectorScanPlan));
        }
    }
}

const _: unsafe extern "C" fn(*const paimon_vector_scan) -> paimon_result_vector_plan =
    paimon_vector_scan_plan;
const _: unsafe extern "C" fn(
    *const paimon_vector_scan,
    *const *const paimon_bucket_vector_search_split,
    usize,
) -> paimon_result_vector_plan = paimon_vector_scan_plan_from_bucket_splits;
