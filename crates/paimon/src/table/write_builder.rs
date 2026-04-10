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

use crate::spec::Datum;
use crate::table::{Table, TableCommit};
use std::collections::HashMap;
use uuid::Uuid;

/// Builder for creating table writers and committers.
///
/// Provides `new_write` (TODO) and `new_commit` methods, with optional
/// `overwrite` support for partition-level overwrites.
pub struct WriteBuilder<'a> {
    table: &'a Table,
    commit_user: String,
    overwrite_partition: Option<HashMap<String, Datum>>,
}

impl<'a> WriteBuilder<'a> {
    pub fn new(table: &'a Table) -> Self {
        Self {
            table,
            commit_user: Uuid::new_v4().to_string(),
            overwrite_partition: None,
        }
    }

    /// Set overwrite mode. If `partition` is None, overwrites the entire table.
    /// If `partition` is Some, overwrites only the specified partition.
    pub fn overwrite(&mut self, partition: Option<HashMap<String, Datum>>) -> &mut Self {
        self.overwrite_partition = Some(partition.unwrap_or_default());
        self
    }

    /// Create a new TableCommit for committing write results.
    pub fn new_commit(&self) -> TableCommit {
        TableCommit::new(
            self.table.clone(),
            self.commit_user.clone(),
            self.overwrite_partition.clone(),
        )
    }

    // TODO: pub fn new_write(&self) -> TableWrite { ... }
}
