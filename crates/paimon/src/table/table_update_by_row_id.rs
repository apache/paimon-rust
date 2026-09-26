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

//! Row-ID updates sharing one target snapshot and file index.

use super::data_evolution_writer::RowIdFileIndex;
use crate::table::{CommitMessage, DataEvolutionWriter, Table, TableCommit};
use arrow_array::RecordBatch;
use std::collections::{HashMap, HashSet};

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Update columns selected per input table, as in PyPaimon.
///
/// Each call to `update_columns` stages its files immediately and returns all
/// messages accumulated so far. Different calls may update different columns
/// of the same file group; updating a common column of that group is rejected.
pub struct TableUpdateByRowId {
    table: Table,
    commit_user: String,
    index: RowIdFileIndex,
    updated: HashMap<String, HashSet<i64>>,
    messages: Vec<CommitMessage>,
}

impl TableUpdateByRowId {
    pub(crate) async fn new(table: &Table, commit_user: String) -> crate::Result<Self> {
        Self::with_index(table, commit_user, RowIdFileIndex::load(table, None).await?)
    }

    pub(super) fn with_index(
        table: &Table,
        commit_user: String,
        index: RowIdFileIndex,
    ) -> crate::Result<Self> {
        // Validate table layout without fixing any update columns.
        let _ = DataEvolutionWriter::new(table, Vec::new())?;
        Ok(Self {
            table: table.clone(),
            commit_user,
            index,
            updated: HashMap::new(),
            messages: Vec::new(),
        })
    }

    /// Stage one logical Arrow table. Its chunks may share a file group.
    pub async fn update_columns(
        &mut self,
        batches: Vec<RecordBatch>,
        column_names: Vec<String>,
    ) -> crate::Result<Vec<CommitMessage>> {
        if column_names.is_empty() {
            return Err(invalid("column_names cannot be empty"));
        }
        let mut seen = HashSet::new();
        let columns = column_names
            .into_iter()
            .filter(|name| seen.insert(name.clone()))
            .collect::<Vec<_>>();
        let mut writer = DataEvolutionWriter::new(&self.table, columns.clone())?;
        for batch in &batches {
            if batch.column_by_name("_ROW_ID").is_none() {
                return Err(invalid("Input data must contain _ROW_ID column"));
            }
        }
        writer.add_matched_group(batches.clone())?;
        let first_row_ids = self.index.matched_first_row_ids(&batches)?;
        for column in &columns {
            if let Some(previous) = self.updated.get(column) {
                let overlap: Vec<_> = first_row_ids.intersection(previous).copied().collect();
                if !overlap.is_empty() {
                    return Err(invalid(format!(
                        "Input batches contain overlapping first_row_ids by column {column}: {overlap:?}"
                    )));
                }
            }
        }
        let messages = writer.prepare_commit_with_index(&self.index).await?;
        for column in columns {
            self.updated
                .entry(column)
                .or_default()
                .extend(&first_row_ids);
        }
        self.messages.extend(messages);
        Ok(self.messages.clone())
    }

    /// Cumulative messages; commit this collection once after all updates.
    pub fn commit_messages(&self) -> &[CommitMessage] {
        &self.messages
    }

    /// Abort staged, uncommitted files after a failed operation.
    pub async fn abort(&mut self) -> crate::Result<()> {
        let result = TableCommit::new(self.table.clone(), self.commit_user.clone())
            .abort(&self.messages)
            .await;
        self.messages.clear();
        self.updated.clear();
        result
    }
}
