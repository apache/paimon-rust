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

//! Write builder for Java-compatible `type=format-table` metadata.

use super::write_builder::validate_commit_user;
use super::{DataEvolutionDeleteWriter, Table, TableCommit, TableUpdate, TableWrite};
use uuid::Uuid;

pub(crate) struct FormatWriteBuilder<'a> {
    table: &'a Table,
    commit_user: String,
}

impl<'a> FormatWriteBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            commit_user: Uuid::new_v4().to_string(),
        }
    }

    pub(crate) fn commit_user(&self) -> &str {
        &self.commit_user
    }

    pub(crate) fn with_commit_user(
        mut self,
        commit_user: impl Into<String>,
    ) -> crate::Result<Self> {
        let commit_user = commit_user.into();
        validate_commit_user(&commit_user)?;
        self.commit_user = commit_user;
        Ok(self)
    }

    pub(crate) fn with_overwrite(self) -> Self {
        self
    }

    pub(crate) fn new_commit(&self) -> TableCommit {
        TableCommit::new(self.table.clone(), self.commit_user.clone())
    }

    pub(crate) fn new_write(&self) -> crate::Result<TableWrite> {
        Err(crate::Error::Unsupported {
            message: "Writing format tables is not supported by the Rust client yet".to_string(),
        })
    }

    pub(crate) fn new_update(&self, _update_columns: Vec<String>) -> crate::Result<TableUpdate> {
        Err(crate::Error::Unsupported {
            message: "Updating format tables is not supported by the Rust client yet".to_string(),
        })
    }

    pub(crate) fn new_delete(&self) -> crate::Result<DataEvolutionDeleteWriter> {
        Err(crate::Error::Unsupported {
            message: "Deleting from format tables is not supported by the Rust client yet"
                .to_string(),
        })
    }
}
