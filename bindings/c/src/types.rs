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

use paimon::spec::DataField;
use paimon::table::Table;

/// C-compatible key-value pair for options.
#[repr(C)]
pub struct paimon_option {
    pub key: *const std::ffi::c_char,
    pub value: *const std::ffi::c_char,
}

/// C-compatible byte buffer.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct paimon_bytes {
    pub data: *mut u8,
    pub len: usize,
}

impl paimon_bytes {
    pub fn new(v: Vec<u8>) -> Self {
        let boxed = v.into_boxed_slice();
        let len = boxed.len();
        let data = Box::into_raw(boxed) as *mut u8;
        Self { data, len }
    }
}

/// Free a paimon_bytes buffer.
///
/// # Safety
/// Only call with bytes returned from paimon C functions.
#[no_mangle]
pub unsafe extern "C" fn paimon_bytes_free(bytes: paimon_bytes) {
    if !bytes.data.is_null() {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
            bytes.data, bytes.len,
        )));
    }
}

/// Opaque wrapper around a heap-allocated Rust object.
#[repr(C)]
pub struct paimon_catalog {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_identifier {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_table {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_read_builder {
    pub inner: *mut c_void,
}

/// Internal state for ReadBuilder that stores table and projection columns.
pub(crate) struct ReadBuilderState {
    pub table: Table,
    pub projected_columns: Option<Vec<String>>,
}

#[repr(C)]
pub struct paimon_table_scan {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_table_read {
    pub inner: *mut c_void,
}

/// Internal state for TableRead that stores table and projected read type.
pub(crate) struct TableReadState {
    pub table: Table,
    pub read_type: Vec<DataField>,
}

#[repr(C)]
pub struct paimon_plan {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct paimon_record_batch_reader {
    pub inner: *mut c_void,
}

/// A single Arrow record batch exported via the Arrow C Data Interface.
///
/// `array` and `schema` point to heap-allocated ArrowArray and ArrowSchema
/// structs. After importing the data, call `paimon_arrow_batch_free` to free
/// the container structs.
#[repr(C)]
pub struct paimon_arrow_batch {
    /// Pointer to a heap-allocated ArrowArray.
    pub array: *mut c_void,
    /// Pointer to a heap-allocated ArrowSchema.
    pub schema: *mut c_void,
}
