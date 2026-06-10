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

//! Table update API for row-ID-based data evolution updates.

use arrow_array::RecordBatch;

use crate::table::{CommitMessage, DataEvolutionWriter, Table};

/// Table-level update API for data-evolution row-id updates.
///
/// `TableUpdate` wraps the lower-level [`DataEvolutionWriter`] so callers can
/// create update writers through [`WriteBuilder`](super::WriteBuilder).
#[must_use = "update must be used to call prepare_commit()"]
pub struct TableUpdate {
    writer: DataEvolutionWriter,
}

impl TableUpdate {
    pub(crate) fn new(table: &Table, update_columns: Vec<String>) -> crate::Result<Self> {
        Ok(Self {
            writer: DataEvolutionWriter::new(table, update_columns)?,
        })
    }

    /// Add a batch of matched rows to update.
    ///
    /// The batch must contain a non-null `_ROW_ID` column plus the update
    /// columns passed to [`WriteBuilder::new_update`](super::WriteBuilder::new_update).
    pub fn add_matched_batch(&mut self, batch: RecordBatch) -> crate::Result<()> {
        self.writer.add_matched_batch(batch)
    }

    /// Prepare commit messages for the caller to commit via [`TableCommit`](super::TableCommit).
    #[must_use = "commit messages must be passed to TableCommit"]
    pub async fn prepare_commit(self) -> crate::Result<Vec<CommitMessage>> {
        self.writer.prepare_commit().await
    }
}
