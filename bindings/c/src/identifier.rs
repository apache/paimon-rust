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

use std::ffi::{c_char, c_void};

use paimon::catalog::Identifier;

use crate::error::validate_cstr;
use crate::result::paimon_result_identifier_new;
use crate::types::paimon_identifier;

/// Create a new Identifier.
///
/// # Safety
/// `database` and `object` must be valid null-terminated C strings, or null (returns error).
#[no_mangle]
pub unsafe extern "C" fn paimon_identifier_new(
    database: *const c_char,
    object: *const c_char,
) -> paimon_result_identifier_new {
    let db = match validate_cstr(database, "database") {
        Ok(s) => s,
        Err(e) => {
            return paimon_result_identifier_new {
                identifier: std::ptr::null_mut(),
                error: e,
            }
        }
    };
    let obj = match validate_cstr(object, "object") {
        Ok(s) => s,
        Err(e) => {
            return paimon_result_identifier_new {
                identifier: std::ptr::null_mut(),
                error: e,
            }
        }
    };
    let wrapper = Box::new(paimon_identifier {
        inner: Box::into_raw(Box::new(Identifier::new(db, obj))) as *mut c_void,
    });
    paimon_result_identifier_new {
        identifier: Box::into_raw(wrapper),
        error: std::ptr::null_mut(),
    }
}

/// Free a paimon_identifier.
///
/// # Safety
/// Only call with an identifier returned from `paimon_identifier_new`.
#[no_mangle]
pub unsafe extern "C" fn paimon_identifier_free(id: *mut paimon_identifier) {
    if !id.is_null() {
        let i = Box::from_raw(id);
        if !i.inner.is_null() {
            drop(Box::from_raw(i.inner as *mut Identifier));
        }
    }
}
