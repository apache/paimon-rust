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
/// require complete Arrow rows and match keys within each partition.
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

    /// Create a row-ID updater sharing one snapshot across per-call columns.
    pub async fn new_update_by_row_id(&self) -> crate::Result<TableUpdateByRowId> {
        TableUpdateByRowId::new(&self.table, self.commit_user.clone()).await
    }

    /// Update existing rows from chunks of one Arrow table containing `_ROW_ID`.
    pub async fn update_by_arrow_with_row_id(
        &self,
        batches: Vec<RecordBatch>,
    ) -> crate::Result<Vec<CommitMessage>> {
        let Some(columns) = self.columns_for_input(&batches)? else {
            return Ok(Vec::new());
        };
        self.new_update_by_row_id()
            .await?
            .update_columns(batches, columns)
            .await
    }

    /// Update logical Arrow tables from a fallible iterator. Create the shared
    /// snapshot/file index after receiving the first table, as in PyPaimon.
    /// Abort staged files on an input, conversion, or update failure.
    pub async fn update_by_arrow_batches_with_row_id<I>(
        &self,
        groups: I,
    ) -> crate::Result<Vec<CommitMessage>>
    where
        I: IntoIterator<Item = crate::Result<Vec<RecordBatch>>>,
    {
        let mut writer = None;
        let result = async {
            for group in groups {
                let group = group?;
                if let Some(columns) = self.columns_for_input(&group)? {
                    if writer.is_none() {
                        writer = Some(self.new_update_by_row_id().await?);
                    }
                    writer
                        .as_mut()
                        .unwrap()
                        .update_columns(group, columns)
                        .await?;
                }
            }
            Ok(writer
                .as_ref()
                .map_or_else(Vec::new, |writer| writer.commit_messages().to_vec()))
        }
        .await;
        if result.is_err() {
            if let Some(writer) = &mut writer {
                let _ = writer.abort().await;
            }
        }
        result
    }

    /// Update rows matching a predicate. Scalar assignments and functions run
    /// per logical file group; Arrow arrays span all matched rows in scan order.
    /// Functions receive only `read_columns` followed by `_ROW_ID`.
    pub async fn update_by_predicate(
        &self,
        predicate: Option<crate::spec::Predicate>,
        assignments: Vec<(String, super::UpdateAssignment)>,
        read_columns: Vec<String>,
    ) -> crate::Result<Vec<CommitMessage>> {
        super::table_update_predicate::update(
            &self.table,
            &self.commit_user,
            predicate,
            assignments,
            read_columns,
        )
        .await
    }

    /// Upsert complete Arrow rows by composite key through the core upsert
    /// writer. Keys match within each partition, including when partition
    /// columns are omitted from `upsert_keys`. Existing keys update every
    /// matching row ID; new keys append.
    pub async fn upsert_by_arrow_with_key(
        &self,
        batches: Vec<RecordBatch>,
        upsert_keys: Vec<String>,
    ) -> crate::Result<Vec<CommitMessage>> {
        let mut writer = super::table_upsert::TableUpsert::new(
            &self.table,
            self.commit_user.clone(),
            upsert_keys,
            self.update_cols
                .clone()
                .filter(|columns| !columns.is_empty())
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
