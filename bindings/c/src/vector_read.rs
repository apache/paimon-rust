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

//! Executes common vector plans through the C ABI.
use crate::error::check_non_null;
use crate::result::paimon_result_record_batch_reader;
use crate::runtime;
use crate::types::*;
use crate::vector_search::{materialize_search_result, wrap_vector_stream};
use paimon::table::VectorScanPlan;

/// Search a DE or PK plan and read projected rows plus __paimon_search_score.
/// The plan is borrowed and can be reused by other queries. Neither the plan nor
/// the reader needs to remain alive after the returned Arrow stream is created.
/// # Safety
/// read and plan must be live handles from vector API constructors, or null (error).
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_read_read(
    read: *const paimon_vector_read,
    plan: *const paimon_vector_plan,
) -> paimon_result_record_batch_reader {
    let validation = check_non_null(read, "read")
        .and_then(|_| check_non_null((*read).inner, "read is not initialized"))
        .and_then(|_| check_non_null(plan, "plan"))
        .and_then(|_| check_non_null((*plan).inner, "plan is not initialized"));
    if let Err(error) = validation {
        return paimon_result_record_batch_reader {
            reader: std::ptr::null_mut(),
            error,
        };
    }
    let state = &*((*read).inner as *const VectorReadState);
    let plan = (&*((*plan).inner as *const VectorScanPlan)).clone();
    wrap_vector_stream(runtime().block_on(async {
        let result = state.read.read(plan).await?;
        materialize_search_result(result, state.projection.as_deref()).await
    }))
}

/// Free a vector reader. Null is accepted.
/// # Safety
/// read must be a live owned vector reader handle, or null.
#[no_mangle]
pub unsafe extern "C" fn paimon_vector_read_free(read: *mut paimon_vector_read) {
    if !read.is_null() {
        let wrapper = Box::from_raw(read);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut VectorReadState));
        }
    }
}

const _: unsafe extern "C" fn(
    *const paimon_vector_read,
    *const paimon_vector_plan,
) -> paimon_result_record_batch_reader = paimon_vector_read_read;
const _: unsafe extern "C" fn(*mut paimon_vector_read) = paimon_vector_read_free;
