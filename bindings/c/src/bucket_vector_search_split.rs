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

//! Decodes the standalone Java PKVSPLIT format outside search execution.
use crate::error::{paimon_error, PaimonErrorCode};
use crate::result::paimon_result_bucket_vector_search_split;
use crate::types::paimon_bucket_vector_search_split;
use paimon::table::BucketVectorSearchSplit;
use std::ffi::c_void;

/// Decode one Java BucketVectorSearchSplit.serialize buffer. The returned handle
/// owns the decoded metadata; input bytes may be released immediately. This does
/// not accept Java ObjectOutputStream envelopes or DE index/raw split formats.
/// Free the handle with paimon_bucket_vector_search_split_free.
///
/// # Safety
/// data must point to len readable bytes. Null/empty/oversized buffers return an error.
#[no_mangle]
pub unsafe extern "C" fn paimon_bucket_vector_search_split_deserialize(
    data: *const u8,
    len: usize,
) -> paimon_result_bucket_vector_search_split {
    if data.is_null() || len == 0 || len > isize::MAX as usize {
        return paimon_result_bucket_vector_search_split {
            split: std::ptr::null_mut(),
            error: paimon_error::new(
                PaimonErrorCode::InvalidInput,
                "null, empty or oversized bucket split buffer".to_string(),
            ),
        };
    }
    match BucketVectorSearchSplit::deserialize(std::slice::from_raw_parts(data, len)) {
        Ok(split) => paimon_result_bucket_vector_search_split {
            split: Box::into_raw(Box::new(paimon_bucket_vector_search_split {
                inner: Box::into_raw(Box::new(split)) as *mut c_void,
            })),
            error: std::ptr::null_mut(),
        },
        Err(error) => paimon_result_bucket_vector_search_split {
            split: std::ptr::null_mut(),
            error: paimon_error::from_paimon(error),
        },
    }
}

/// Free a decoded split. Null is accepted.
/// # Safety
/// split must be a live handle returned by paimon_bucket_vector_search_split_deserialize, or null.
#[no_mangle]
pub unsafe extern "C" fn paimon_bucket_vector_search_split_free(
    split: *mut paimon_bucket_vector_search_split,
) {
    if !split.is_null() {
        let wrapper = Box::from_raw(split);
        if !wrapper.inner.is_null() {
            drop(Box::from_raw(wrapper.inner as *mut BucketVectorSearchSplit));
        }
    }
}

const _: unsafe extern "C" fn(*const u8, usize) -> paimon_result_bucket_vector_search_split =
    paimon_bucket_vector_search_split_deserialize;
const _: unsafe extern "C" fn(*mut paimon_bucket_vector_search_split) =
    paimon_bucket_vector_search_split_free;
