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

use super::data_evolution_writer::{group_by_normal_row_id, RowIdFileIndex};
use crate::table::{CommitMessage, DataEvolutionWriter, DataSplit, Plan, Table};
use arrow_array::RecordBatch;
use std::collections::{HashMap, HashSet};

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Routing metadata for complete row-tracked file groups in a captured plan.
///
/// Constructing this object performs no I/O and grants no file access. The
/// reader or mutation executing the retained plan must authorize independently.
#[derive(Debug, Clone)]
pub struct RowIdPlan {
    plan: Plan,
    groups: Vec<(i64, i64, DataSplit)>,
}

impl RowIdPlan {
    pub fn from_plan(plan: Plan) -> crate::Result<Self> {
        if plan.snapshot_id().is_none() && !plan.splits().is_empty() {
            return Err(invalid("A row-ID plan requires its captured snapshot ID"));
        }
        let mut groups = Vec::new();
        for split in plan.splits() {
            if Some(split.snapshot_id()) != plan.snapshot_id()
                || split.is_streaming()
                || split.row_ranges().is_some()
                || split.scores().is_some()
            {
                return Err(invalid(
                    "A row-ID plan requires full snapshot file groups without row selection",
                ));
            }
            if split.data_files().is_empty() {
                return Err(invalid("A row-ID split must contain data files"));
            }
            if split
                .data_deletion_files()
                .is_some_and(|files| files.len() != split.data_files().len())
            {
                return Err(invalid(
                    "Row-ID deletion metadata must match the captured data files",
                ));
            }
            let mut names = HashSet::new();
            for file in split.data_files() {
                if file.row_id_range().is_none() || !names.insert(&file.file_name) {
                    return Err(invalid(
                        "A row-ID split contains invalid or duplicate file metadata",
                    ));
                }
            }
            let file_groups = group_by_normal_row_id(split.data_files().to_vec());
            if file_groups.is_empty() {
                return Err(invalid("A row-ID split requires a normal anchor file"));
            }
            let mut grouped = HashSet::new();
            for files in file_groups {
                let anchor = super::source::data_evolution_anchor_file(&files)?;
                let (first, last) = anchor
                    .row_id_range()
                    .ok_or_else(|| invalid("Invalid row-ID anchor range"))?;
                for file in &files {
                    grouped.insert(file.file_name.clone());
                    if super::source::is_data_evolution_normal_file(file)
                        && file.row_id_range() != Some((first, last))
                    {
                        return Err(invalid(
                            "Overlapping row-ID anchors must have identical ranges",
                        ));
                    }
                }
                groups.push((first, last, split.for_data_evolution_files(files)));
            }
            if grouped.len() != names.len() {
                return Err(invalid(
                    "A row-ID split contains a dedicated file without its normal anchor",
                ));
            }
        }
        groups.sort_by_key(|(first, _, _)| *first);
        if groups.windows(2).any(|pair| pair[0].1 >= pair[1].0) {
            return Err(invalid("A row-ID plan contains overlapping file groups"));
        }
        Ok(Self { plan, groups })
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.plan.snapshot_id()
    }

    pub fn groups(&self) -> &[(i64, i64, DataSplit)] {
        &self.groups
    }

    pub fn plan(&self) -> &Plan {
        &self.plan
    }
}

/// Update columns selected per input table, as in PyPaimon.
///
/// Each call to `update_columns` stages its files immediately and returns all
/// messages accumulated so far. Different calls may update different columns
/// of the same file group; updating a common column of that group is rejected.
pub struct TableUpdateByRowId {
    table: Table,
    index: RowIdFileIndex,
    updated: HashMap<i32, HashSet<i64>>,
    messages: Vec<CommitMessage>,
    blob_uri_reader_factory: Option<std::sync::Arc<dyn crate::io::UriReaderFactory>>,
}

impl TableUpdateByRowId {
    pub(crate) async fn new(table: &Table) -> crate::Result<Self> {
        Self::with_index(table, RowIdFileIndex::load(table, None).await?)
    }

    pub(super) async fn with_plan(table: &Table, plan: &Plan) -> crate::Result<Self> {
        Self::with_index(table, RowIdFileIndex::load_plan(table, plan).await?)
    }

    pub(super) fn with_index(table: &Table, index: RowIdFileIndex) -> crate::Result<Self> {
        // Validate table layout without fixing any update columns.
        let _ = DataEvolutionWriter::new(table, Vec::new())?;
        Ok(Self {
            table: table.clone(),
            index,
            updated: HashMap::new(),
            messages: Vec::new(),
            blob_uri_reader_factory: None,
        })
    }

    /// URI reader selection for referenced Blob values. Core performs the
    /// bounded or streamed copy; callers need not materialize payloads.
    pub fn with_blob_uri_reader_factory(
        &mut self,
        factory: Option<std::sync::Arc<dyn crate::io::UriReaderFactory>>,
    ) -> &mut Self {
        self.blob_uri_reader_factory = factory;
        self
    }

    /// Stage one logical Arrow table. Its chunks may share a file group.
    /// Partition columns may carry their existing values. Changing them is
    /// rejected before writing, because a move requires delete + insert.
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
        let mut writer = DataEvolutionWriter::for_row_id(&self.table, columns.clone())?;
        writer.with_blob_uri_reader_factory(self.blob_uri_reader_factory.clone());
        let batches = batches
            .into_iter()
            .map(super::update_input::normalize_row_ids)
            .collect::<crate::Result<Vec<_>>>()?
            .into_iter()
            // Keep schema validation for empty inputs, then let both the
            // writer and file index consume the same logical rows.
            .filter(|batch| batch.num_rows() > 0)
            .collect::<Vec<_>>();
        writer.add_matched_group(batches.clone())?;
        let first_row_ids = self.index.matched_first_row_ids(&batches)?;
        // Use the same leaf identities as commit-time conflict detection.
        // Whole ROW and child paths must not produce two versions of one leaf.
        let leaf_ids = super::data_evolution_fields::write_leaf_ids(
            self.table.schema().fields(),
            Some(&columns),
        )?;
        for id in &leaf_ids {
            if let Some(previous) = self.updated.get(id) {
                let overlap: Vec<_> = first_row_ids.intersection(previous).copied().collect();
                if !overlap.is_empty() {
                    return Err(invalid(format!(
                        "Input batches contain overlapping first_row_ids by column {columns:?} (field ID {id}): {overlap:?}"
                    )));
                }
            }
        }
        let messages = writer.prepare_commit_with_index(&self.index).await?;
        for id in leaf_ids {
            self.updated.entry(id).or_default().extend(&first_row_ids);
        }
        self.messages.extend(messages);
        Ok(self.messages.clone())
    }

    /// Cumulative messages; commit this collection once after all updates.
    pub fn commit_messages(&self) -> &[CommitMessage] {
        &self.messages
    }

    /// Reset update state while preserving prepared files.
    pub async fn abort(&mut self) -> crate::Result<()> {
        // Never delete CommitMessage files. They may already be published even
        // when the caller received an exception from commit.
        self.messages.clear();
        self.updated.clear();
        Ok(())
    }
}
