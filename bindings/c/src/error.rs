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

use std::ffi::{c_char, CStr};

use crate::types::paimon_bytes;

/// Error codes for paimon C API.
#[repr(i32)]
pub enum PaimonErrorCode {
    Unexpected = 0,
    Unsupported = 1,
    NotFound = 2,
    AlreadyExists = 3,
    InvalidInput = 4,
    IoError = 5,
}

/// C-compatible error type.
#[repr(C)]
pub struct paimon_error {
    pub code: i32,
    pub message: paimon_bytes,
}

impl paimon_error {
    pub fn new(code: PaimonErrorCode, msg: String) -> *mut Self {
        Box::into_raw(Box::new(Self {
            code: code as i32,
            message: paimon_bytes::new(msg.into_bytes()),
        }))
    }

    pub fn from_paimon(e: paimon::Error) -> *mut Self {
        let code = match &e {
            paimon::Error::Unsupported { .. } | paimon::Error::IoUnsupported { .. } => {
                PaimonErrorCode::Unsupported
            }
            paimon::Error::TableNotExist { .. }
            | paimon::Error::DatabaseNotExist { .. }
            | paimon::Error::ColumnNotExist { .. } => PaimonErrorCode::NotFound,
            paimon::Error::TableAlreadyExist { .. }
            | paimon::Error::DatabaseAlreadyExist { .. }
            | paimon::Error::ColumnAlreadyExist { .. } => PaimonErrorCode::AlreadyExists,
            paimon::Error::ConfigInvalid { .. }
            | paimon::Error::DataTypeInvalid { .. }
            | paimon::Error::DataInvalid { .. }
            | paimon::Error::IdentifierInvalid { .. } => PaimonErrorCode::InvalidInput,
            paimon::Error::IoUnexpected { .. } => PaimonErrorCode::IoError,
            _ => PaimonErrorCode::Unexpected,
        };
        Self::new(code, e.to_string())
    }
}

/// Free a paimon_error.
///
/// # Safety
/// Only call with errors returned from paimon C functions.
#[no_mangle]
pub unsafe extern "C" fn paimon_error_free(err: *mut paimon_error) {
    if !err.is_null() {
        let e = Box::from_raw(err);
        paimon_bytes_free(e.message);
    }
}

// Re-use the bytes free from types - but we need it here for the error drop
use crate::types::paimon_bytes_free;

/// Validate a C string pointer: checks for null and valid UTF-8.
///
/// Returns `Ok(String)` on success, or `Err(*mut paimon_error)` if the
/// pointer is null or the string contains invalid UTF-8.
///
/// # Safety
/// If `ptr` is non-null, it must point to a valid null-terminated C string.
pub unsafe fn validate_cstr(ptr: *const c_char, name: &str) -> Result<String, *mut paimon_error> {
    if ptr.is_null() {
        return Err(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            format!("null pointer passed for `{name}`"),
        ));
    }
    CStr::from_ptr(ptr)
        .to_str()
        .map(|s| s.to_owned())
        .map_err(|e| {
            paimon_error::new(
                PaimonErrorCode::InvalidInput,
                format!("`{name}` is not valid UTF-8: {e}"),
            )
        })
}

/// Check that a pointer is non-null, returning an error if it is.
pub fn check_non_null<T>(ptr: *const T, name: &str) -> Result<(), *mut paimon_error> {
    if ptr.is_null() {
        Err(paimon_error::new(
            PaimonErrorCode::InvalidInput,
            format!("null pointer passed for `{name}`"),
        ))
    } else {
        Ok(())
    }
}
