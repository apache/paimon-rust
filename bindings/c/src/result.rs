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

use crate::error::paimon_error;
use crate::types::*;

#[repr(C)]
pub struct paimon_result_catalog_new {
    pub catalog: *mut paimon_catalog,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_identifier_new {
    pub identifier: *mut paimon_identifier,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_get_table {
    pub table: *mut paimon_table,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_new_read {
    pub read: *mut paimon_table_read,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_read_builder {
    pub read_builder: *mut paimon_read_builder,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_table_scan {
    pub scan: *mut paimon_table_scan,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_plan {
    pub plan: *mut paimon_plan,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_record_batch_reader {
    pub reader: *mut paimon_record_batch_reader,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_predicate {
    pub predicate: *mut paimon_predicate,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_next_batch {
    pub batch: paimon_arrow_batch,
    pub error: *mut paimon_error,
}

// === Write/Commit result types ===

#[repr(C)]
pub struct paimon_result_write_builder {
    pub write_builder: *mut paimon_write_builder,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_table_write {
    pub write: *mut paimon_table_write,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_table_commit {
    pub commit: *mut paimon_table_commit,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_prepare_commit {
    pub messages: *mut paimon_commit_messages,
    pub error: *mut paimon_error,
}
