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

use crate::spec::DataField;
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
/// [`with_update_type`](Self::with_update_type). Key-based upserts accept a
/// column subset including the keys and match keys within each partition.
/// Operations may select a historical read view. Matched updates retain its
/// snapshot as the conflict baseline; upsert and MERGE append unmatched rows
/// using that view's schema and assign fresh row IDs at publication. Publication
/// resolves the latest snapshot and current table schema independently of the
/// selected read view.
#[derive(Clone)]
pub struct TableUpdate {
    table: Table,
    commit_user: String,
    update_cols: Option<Vec<String>>,
    plan: Option<super::Plan>,
    blob_uri_reader_factory: Option<std::sync::Arc<dyn crate::io::UriReaderFactory>>,
}

impl TableUpdate {
    pub(crate) fn new(table: &Table, commit_user: String) -> Self {
        Self {
            table: table.clone(),
            commit_user,
            update_cols: None,
            plan: None,
            blob_uri_reader_factory: None,
        }
    }

    /// Reuse a captured `plan_all` result for row-ID updates and deletes.
    ///
    /// Each split must contain complete logical file groups, including their
    /// partial-column files and deletion vectors. Splits may be distributed
    /// among workers; the supplied row IDs must belong to that worker's groups.
    /// Arbitrarily trimmed or incremental splits are not mutation plans.
    /// Workers authorize the whole table again and retain the captured snapshot
    /// as the conflict baseline without replanning its data manifests. Deletes
    /// still read the captured index manifest to preserve all deletion vectors
    /// when replacing a bucket's shared index file. Assign all deletes for one
    /// partition/bucket to the same worker.
    /// Predicate, key-based, MERGE and shard operations reject this setting;
    /// use a separately configured updater for those operations.
    pub fn with_plan(&mut self, plan: super::Plan) -> crate::Result<&mut Self> {
        super::RowIdPlan::from_plan(plan.clone())?;
        self.plan = Some(plan);
        Ok(self)
    }

    fn ensure_unplanned(&self) -> crate::Result<()> {
        if self.plan.is_some() {
            return Err(crate::Error::Unsupported {
                message: "with_plan supports only row-ID update and delete operations".into(),
            });
        }
        Ok(())
    }

    /// Configure readers for Blob input references. The same factory supplies
    /// matched updates and appended rows in upsert and MERGE operations.
    pub fn with_blob_uri_reader_factory(
        &mut self,
        factory: Option<std::sync::Arc<dyn crate::io::UriReaderFactory>>,
    ) -> &mut Self {
        self.blob_uri_reader_factory = factory;
        self
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

    /// Read and replace columns over one balanced shard of whole file groups.
    ///
    /// `read_type` configures this operation's reader, independently of the
    /// update columns. It preserves field IDs and nested read metadata, just
    /// like [`ReadBuilder::with_read_type`](super::ReadBuilder::with_read_type).
    /// `None` reads all table fields; `Some(vec![])` returns zero-column batches
    /// with their row counts preserved. Row IDs are tracked internally.
    pub async fn new_shard_updater(
        &self,
        shard: usize,
        shard_count: usize,
        read_type: Option<Vec<DataField>>,
    ) -> crate::Result<super::ShardTableUpdater> {
        self.ensure_unplanned()?;
        let mut updater = super::ShardTableUpdater::new(
            &self.table,
            read_type,
            self.update_cols
                .clone()
                .unwrap_or_else(|| self.all_fields()),
            shard,
            shard_count,
        )
        .await?;
        updater.with_blob_uri_reader_factory(self.blob_uri_reader_factory.clone());
        Ok(updater)
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
        let mut writer = match &self.plan {
            Some(plan) => TableUpdateByRowId::with_plan(&self.table, plan).await?,
            None => TableUpdateByRowId::new(&self.table).await?,
        };
        writer.with_blob_uri_reader_factory(self.blob_uri_reader_factory.clone());
        Ok(writer)
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
        // Preserve earlier prepared groups on failure. Never delete files
        // based on CommitMessage.
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
        self.ensure_unplanned()?;
        super::table_update_predicate::update(
            &self.table,
            predicate,
            assignments,
            read_columns,
            self.blob_uri_reader_factory.clone(),
        )
        .await
    }

    /// Upsert Arrow rows by composite key through the core upsert
    /// writer. Keys match within each partition, including when partition
    /// columns are omitted from `upsert_keys`. Existing keys update every
    /// matching row ID; new keys append all input columns. Omitted columns on
    /// matched rows retain their values; on new rows they are absent from the file.
    /// After last-write-wins, matched values follow row-ID update conversion
    /// rules; new rows must satisfy the ordinary Arrow write type.
    /// Numeric keys may use different Arrow types only when conversion keeps
    /// their exact mathematical value. This matching conversion does not alter
    /// the original key columns used as update or append payloads.
    /// Input batches may have different non-key columns. After last-write-wins
    /// deduplication, matched rows must supply their update columns and appended
    /// rows in each partition must share a field set.
    pub async fn upsert_by_arrow_with_key(
        &self,
        batches: Vec<RecordBatch>,
        upsert_keys: Vec<String>,
    ) -> crate::Result<Vec<CommitMessage>> {
        self.ensure_unplanned()?;
        let mut writer = super::table_upsert::TableUpsert::new(
            &self.table,
            self.commit_user.clone(),
            upsert_keys,
            self.update_cols
                .clone()
                .filter(|columns| !columns.is_empty())
                .unwrap_or_else(|| self.all_fields()),
        )?;
        writer.with_blob_uri_reader_factory(self.blob_uri_reader_factory.clone());
        for batch in batches {
            writer.add_batch(batch)?;
        }
        writer.prepare_commit().await
    }

    /// Prepare MERGE actions against one target snapshot. Clauses run in order,
    /// SQL NULL keys do not match, and ambiguous source matches are rejected.
    pub async fn merge_into(
        &self,
        source: super::MergeSource,
        on: Vec<(String, String)>,
        when_matched: Vec<super::WhenMatched>,
        when_not_matched: Vec<super::WhenNotMatched>,
    ) -> crate::Result<Vec<CommitMessage>> {
        self.ensure_unplanned()?;
        super::table_merge_into::merge_into(
            &self.table,
            &self.commit_user,
            source,
            on,
            when_matched,
            when_not_matched,
            self.blob_uri_reader_factory.clone(),
        )
        .await
    }

    /// Delete rows matching a predicate against one captured snapshot.
    ///
    /// Partition-only predicates remove complete data and index metadata,
    /// including pending files, for append and primary-key tables as well as
    /// data-evolution tables, without requiring deletion vectors. All other
    /// predicates (including `None`, which matches all rows) require a
    /// data-evolution table with row tracking and deletion vectors.
    /// Returned messages carry the snapshot baseline for conflict detection;
    /// the caller publishes them through the matching builder's committer.
    pub async fn delete_by_predicate(
        &self,
        predicate: Option<crate::spec::Predicate>,
    ) -> crate::Result<Vec<CommitMessage>> {
        self.ensure_unplanned()?;
        super::table_update_delete::delete(&self.table, predicate).await
    }

    /// Delete rows by row ID using deletion vectors.
    pub async fn delete_by_row_id(
        &self,
        row_ids: impl IntoIterator<Item = i64>,
    ) -> crate::Result<Vec<CommitMessage>> {
        let mut row_ids = row_ids.into_iter().peekable();
        if row_ids.peek().is_none() {
            return Ok(Vec::new());
        }
        // The high-level updater selects a read snapshot; the low-level
        // ordinary write builder continues rejecting historical handles.
        let mut writer = super::DataEvolutionDeleteWriter::new(&self.table)?;
        writer.add_row_ids(row_ids)?;
        match &self.plan {
            Some(plan) => writer.prepare_commit_with_plan(plan).await,
            None => writer.prepare_commit().await,
        }
    }
}

fn same_columns(left: &[String], right: &[String]) -> bool {
    left.len() == right.len() && left.iter().all(|column| right.contains(column))
}

#[cfg(test)]
#[path = "table_update_plan_tests.rs"]
mod plan_tests;
