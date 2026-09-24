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

use paimon::resource::ResourceContext;

use crate::error::{check_non_null, paimon_error};
use crate::result::paimon_result_resource_context;
use crate::types::{paimon_resource_context, paimon_resource_metrics};

/// Create a shared reader and writer reservation budget in bytes.
/// Zero rejects nonempty reservations.
#[no_mangle]
pub extern "C" fn paimon_resource_context_create(
    memory_limit_bytes: usize,
) -> paimon_result_resource_context {
    match ResourceContext::builder()
        .memory_limit(memory_limit_bytes)
        .build()
    {
        Ok(resources) => paimon_result_resource_context {
            context: Box::into_raw(Box::new(paimon_resource_context {
                inner: Box::into_raw(Box::new(resources)) as *mut c_void,
            })),
            error: std::ptr::null_mut(),
        },
        Err(error) => paimon_result_resource_context {
            context: std::ptr::null_mut(),
            error: paimon_error::from_paimon(error),
        },
    }
}

/// Read current and peak reservation bytes from a shared context.
///
/// The counters are sampled independently. They are not process allocation metrics.
///
/// # Safety
/// `context` must be a valid resource context handle, or null (returns error).
/// `metrics` must point to writable storage, or be null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_resource_context_metrics(
    context: *const paimon_resource_context,
    metrics: *mut paimon_resource_metrics,
) -> *mut paimon_error {
    if let Err(error) = check_non_null(context, "context") {
        return error;
    }
    if let Err(error) = check_non_null(metrics, "metrics") {
        return error;
    }

    let resources = &*((*context).inner as *const ResourceContext);
    let snapshot = resources.metrics();
    *metrics = paimon_resource_metrics {
        reserved_memory_bytes: snapshot.reserved_memory_bytes,
        peak_reserved_memory_bytes: snapshot.peak_reserved_memory_bytes,
    };
    std::ptr::null_mut()
}

/// Free a resource context handle. Builders, streams, and writers retain their own clones.
///
/// # Safety
/// `context` must be a handle returned by `paimon_resource_context_create`, or null.
#[no_mangle]
pub unsafe extern "C" fn paimon_resource_context_free(context: *mut paimon_resource_context) {
    if !context.is_null() {
        let wrapper = Box::from_raw(context);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut ResourceContext));
        }
    }
}
