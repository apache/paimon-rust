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

//! WriteBuilder for table write API.
//!
//! Reference: [pypaimon WriteBuilder](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/write/write_builder.py)

use crate::table::{Table, TableCommit, TableWrite};
use uuid::Uuid;

/// Builder for creating table writers and committers.
///
/// Provides `new_write` and `new_commit` methods, with optional
/// `overwrite` support for partition-level overwrites.
pub struct WriteBuilder<'a> {
    table: &'a Table,
    commit_user: String,
}

impl<'a> WriteBuilder<'a> {
    pub fn new(table: &'a Table) -> Self {
        Self {
            table,
            commit_user: Uuid::new_v4().to_string(),
        }
    }

    /// Create a new TableCommit for committing write results.
    pub fn new_commit(&self) -> TableCommit {
        TableCommit::new(self.table.clone(), self.commit_user.clone())
    }

    /// Create a new TableWrite for writing Arrow data.
    ///
    /// For primary-key tables, sequence numbers are lazily scanned per partition
    /// when the first writer for that partition is created.
    pub fn new_write(&self) -> crate::Result<TableWrite> {
        TableWrite::new(self.table, self.commit_user.clone())
    }
}
