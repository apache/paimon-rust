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

//! High-level data-evolution update operations.
//!
//! Mirrors PyPaimon's `TableUpdate`: this object owns update configuration
//! and creates a [`TableUpdateByRowId`](super::TableUpdateByRowId) only when
//! matched rows need to be written.

use std::collections::HashSet;

use arrow_array::RecordBatch;
use indexmap::IndexMap;

use crate::table::{CommitMessage, Table, TableUpdateByRowId};

const ROW_ID: &str = "_ROW_ID";

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Configured update operations for one table and commit user.
///
/// Like PyPaimon's batch and stream update objects, this object can prepare
/// more than one operation. The caller commits each result separately with
/// [`TableCommit`](super::TableCommit).
///
/// Row-ID updates infer columns from the input unless configured with
/// [`with_update_type`](Self::with_update_type). Key-based upserts currently
/// require complete Arrow rows and an unpartitioned table.
#[derive(Clone)]
pub struct TableUpdate {
    table: Table,
    commit_user: String,
    update_cols: Option<Vec<String>>,
}

impl TableUpdate {
    pub(crate) fn new(table: &Table, commit_user: String) -> Self {
        Self {
            table: table.clone(),
            commit_user,
            update_cols: None,
        }
    }

    /// Select columns to update when a key or row ID matches. Duplicate names
    /// are removed in first-occurrence order, as in PyPaimon.
    pub fn with_update_type(&mut self, update_cols: Vec<String>) -> crate::Result<&mut Self> {
        let schema = self.table.schema();
        let fields = schema.fields();
        let mut seen = HashSet::new();
        let mut columns = Vec::new();
        for name in update_cols {
            if !fields.iter().any(|field| field.name() == name) {
                return Err(invalid(format!("Column {name} is not in table schema.")));
            }
            if seen.insert(name.clone()) {
                columns.push(name);
            }
        }
        self.update_cols = (columns.len() != fields.len()).then_some(columns);
        Ok(self)
    }

    fn all_fields(&self) -> Vec<String> {
        self.table
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect()
    }

    fn columns_for_batch(&self, batch: &RecordBatch) -> Vec<String> {
        self.update_cols.clone().unwrap_or_else(|| {
            batch
                .schema()
                .fields()
                .iter()
                .filter(|field| field.name() != ROW_ID)
                .map(|field| field.name().to_string())
                .collect()
        })
    }

    fn columns_for_input(&self, batches: &[RecordBatch]) -> crate::Result<Option<Vec<String>>> {
        let Some(first) = batches.first() else {
            return Ok(None);
        };
        let columns = self.columns_for_batch(first);
        if columns.is_empty() {
            return Err(invalid("column_names cannot be empty"));
        }
        for batch in batches {
            if batch.column_by_name(ROW_ID).is_none() {
                return Err(invalid("Input data must contain _ROW_ID column"));
            }
            if self.update_cols.is_none() && !same_columns(&columns, &self.columns_for_batch(batch))
            {
                return Err(invalid(
                    "Arrow batches in one input table must have the same columns",
                ));
            }
        }
        Ok(Some(columns))
    }

    /// Create the low-level writer for callers that already have matched rows.
    pub fn new_update_by_row_id(&self) -> crate::Result<TableUpdateByRowId> {
        TableUpdateByRowId::new(
            &self.table,
            self.update_cols
                .clone()
                .unwrap_or_else(|| self.all_fields()),
        )
    }

    /// Update existing rows from Arrow batches containing `_ROW_ID`. When no
    /// update type was selected, the input's non-`_ROW_ID` columns are used.
    pub async fn update_by_arrow_with_row_id(
        &self,
        batches: Vec<RecordBatch>,
    ) -> crate::Result<Vec<CommitMessage>> {
        let Some(columns) = self.columns_for_input(&batches)? else {
            return Ok(Vec::new());
        };
        let mut writer = TableUpdateByRowId::new(&self.table, columns)?;
        for batch in batches {
            writer.add_matched_batch(batch)?;
        }
        writer.prepare_commit().await
    }

    /// Apply multiple logical input tables while preserving group boundaries.
    /// Groups updating the same target file group and any common column conflict.
    /// Without an explicit update type, each input table selects its own columns.
    pub async fn update_by_arrow_batches_with_row_id(
        &self,
        groups: Vec<Vec<RecordBatch>>,
    ) -> crate::Result<Vec<CommitMessage>> {
        let mut inputs = Vec::new();
        for group in groups {
            if let Some(columns) = self.columns_for_input(&group)? {
                inputs.push((columns, group));
            }
        }
        let Some((first_columns, _)) = inputs.first() else {
            return Ok(Vec::new());
        };
        if inputs
            .iter()
            .all(|(columns, _)| same_columns(first_columns, columns))
        {
            let mut writer = TableUpdateByRowId::new(&self.table, first_columns.clone())?;
            for (_, group) in inputs {
                writer.add_matched_group(group)?;
            }
            return writer.prepare_commit().await;
        }
        self.update_mixed_column_groups(inputs).await
    }

    async fn update_mixed_column_groups(
        &self,
        inputs: Vec<(Vec<String>, Vec<RecordBatch>)>,
    ) -> crate::Result<Vec<CommitMessage>> {
        let has_rows = inputs
            .iter()
            .any(|(_, group)| group.iter().any(|batch| batch.num_rows() > 0));
        // A writer per column preserves the per-column overlap rule even when
        // two input tables select intersecting but different column sets.
        let mut writers = IndexMap::<String, TableUpdateByRowId>::new();
        for (columns, group) in inputs {
            for column in columns {
                let batches = group
                    .iter()
                    .map(|batch| {
                        let schema = batch.schema();
                        let indices = [schema.index_of(ROW_ID), schema.index_of(&column)]
                            .into_iter()
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|error| invalid(error.to_string()))?;
                        batch
                            .project(&indices)
                            .map_err(|error| invalid(error.to_string()))
                    })
                    .collect::<crate::Result<Vec<_>>>()?;
                if !writers.contains_key(&column) {
                    writers.insert(
                        column.clone(),
                        TableUpdateByRowId::new(&self.table, vec![column.clone()])?,
                    );
                }
                writers
                    .get_mut(&column)
                    .unwrap()
                    .add_matched_group(batches)?;
            }
        }

        // Every column must read the same base snapshot. Results prepared by
        // earlier column writers are aborted if a later column fails.
        crate::spec::CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        let snapshot = super::time_travel::resolve_snapshot(&self.table).await?;
        if snapshot.is_none() && has_rows {
            return Err(invalid("No files with row tracking found in target table"));
        }
        let mut messages = Vec::new();
        for (_, mut writer) in writers {
            if let Some(snapshot) = &snapshot {
                writer.pin_read_snapshot(snapshot.id());
            }
            match writer.prepare_commit().await {
                Ok(prepared) => messages.extend(prepared),
                Err(error) => {
                    let commit =
                        super::TableCommit::new(self.table.clone(), self.commit_user.clone());
                    let _ = commit.abort(&messages).await;
                    return Err(error);
                }
            }
        }
        Ok(messages)
    }

    /// Upsert complete Arrow rows by composite key through the core upsert
    /// writer. Existing keys update every matching row ID; new keys append.
    pub async fn upsert_by_arrow_with_key(
        &self,
        batches: Vec<RecordBatch>,
        upsert_keys: Vec<String>,
    ) -> crate::Result<Vec<CommitMessage>> {
        let mut writer = self
            .table
            .new_write_builder()
            .with_commit_user(self.commit_user.clone())?
            .new_upsert(
                upsert_keys,
                self.update_cols
                    .clone()
                    .unwrap_or_else(|| self.all_fields()),
            )?;
        for batch in batches {
            writer.add_batch(batch)?;
        }
        writer.prepare_commit().await
    }

    /// Delete rows by row ID using deletion vectors.
    pub async fn delete_by_row_id(&self, row_ids: Vec<i64>) -> crate::Result<Vec<CommitMessage>> {
        if row_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut writer = self
            .table
            .new_write_builder()
            .with_commit_user(self.commit_user.clone())?
            .new_delete()?;
        writer.add_row_ids(row_ids)?;
        writer.prepare_commit().await
    }
}

fn same_columns(left: &[String], right: &[String]) -> bool {
    left.len() == right.len() && left.iter().all(|column| right.contains(column))
}
