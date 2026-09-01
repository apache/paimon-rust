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

mod extraction;
mod planning;
mod timing;
mod validation;
mod writer;

use planning::plan_vindex_shards;
use timing::vector_index_build_timing_enabled;
use validation::{checked_i32, find_index_field, validate_table_options, validate_vector_field};

use crate::spec::CoreOptions;
use crate::table::{CommitMessage, RowRange, SnapshotManager, Table, TableCommit};
use crate::vindex::{is_vindex_index_type, VindexVectorIndexOptions};
use crate::{Error, Result};
use std::collections::HashMap;
use std::time::Instant;

pub struct VindexIndexBuildBuilder<'a> {
    table: &'a Table,
    index_column: Option<String>,
    index_type: String,
    options: HashMap<String, String>,
}

impl<'a> VindexIndexBuildBuilder<'a> {
    pub(crate) fn new(table: &'a Table, index_type: &str) -> Self {
        Self {
            table,
            index_column: None,
            index_type: index_type.to_string(),
            options: HashMap::new(),
        }
    }

    pub fn with_index_column(&mut self, column: &str) -> &mut Self {
        self.index_column = Some(column.to_string());
        self
    }

    pub fn with_options(&mut self, options: HashMap<String, String>) -> &mut Self {
        self.options = options;
        self
    }

    pub async fn execute(&self) -> Result<usize> {
        // Building the index scans the table's rows.
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;

        self.table.ensure_not_branch_reference_for_write()?;

        if !is_vindex_index_type(&self.index_type) {
            return Err(Error::DataInvalid {
                message: format!("Unsupported vindex index type: {}", self.index_type),
                source: None,
            });
        }

        let index_column = self
            .index_column
            .as_deref()
            .ok_or_else(|| Error::DataInvalid {
                message: "vindex index column is required".to_string(),
                source: None,
            })?;

        let core_options = CoreOptions::new(self.table.schema().options());
        validate_table_options(self.table, &core_options)?;
        let rows_per_shard = core_options.global_index_row_count_per_shard()?;

        let index_field = find_index_field(self.table, index_column)?;
        validate_vector_field(index_field)?;
        let vindex_options = VindexVectorIndexOptions::new(
            self.table.schema().options(),
            &self.options,
            &self.index_type,
            index_field,
        )?;
        let dimension = checked_i32(
            vindex_options.dimension() as u64,
            "vindex dimension is too large for Rust builder",
        )?;
        let index_meta =
            serde_json::to_vec(&vindex_options.native_options).map_err(|e| Error::DataInvalid {
                message: format!("Failed to serialize vindex options metadata: {e}"),
                source: Some(Box::new(e)),
            })?;

        let snapshot_manager = SnapshotManager::new(
            self.table.file_io().clone(),
            self.table.location().to_string(),
        );
        let snapshot = snapshot_manager
            .get_latest_snapshot()
            .await?
            .ok_or_else(|| Error::DataInvalid {
                message: "Cannot build vindex index without a snapshot".to_string(),
                source: None,
            })?;

        let manifest_entries = self
            .table
            .new_read_builder()
            .new_scan()
            .with_scan_all_files()
            .plan_manifest_entries(&snapshot)
            .await?;
        let indexed = crate::table::global_index_build_common::indexed_row_ranges(
            self.table,
            snapshot.index_manifest(),
            &self.index_type,
            index_field.id(),
            None, // single-column build; no extra fields today
        )
        .await?;
        let shards = plan_vindex_shards(
            self.table.location(),
            self.table.schema().partition_keys(),
            self.table.schema().fields(),
            &core_options,
            snapshot.id(),
            manifest_entries,
            rows_per_shard,
            &indexed,
        )?;
        if shards.is_empty() {
            return Ok(0);
        }

        crate::table::global_index_build_common::validate_existing_index_overlap(
            self.table,
            snapshot.index_manifest(),
            &self.index_type,
            index_field.id(),
            None,
            &shards
                .iter()
                .map(|shard| RowRange::new(shard.row_range_start, shard.row_range_end))
                .collect::<Vec<_>>(),
        )
        .await?;

        let commit = TableCommit::new(
            self.table.clone(),
            format!(
                "global-index-{}-create-{}",
                self.index_type,
                uuid::Uuid::new_v4()
            ),
        );
        let shard_count = shards.len();
        let mut messages = Vec::with_capacity(shard_count);
        let mut timings = Vec::with_capacity(shard_count);
        for shard in shards {
            let built = match self
                .build_index_file(
                    &shard,
                    index_column,
                    dimension,
                    index_field.id(),
                    &vindex_options,
                    index_meta.clone(),
                )
                .await
            {
                Ok(index_file) => index_file,
                Err(error) => {
                    let _ = commit.abort(&messages).await;
                    return Err(error);
                }
            };
            let mut message = CommitMessage::new(shard.partition_bytes.clone(), 0, vec![]);
            message.new_index_files = vec![built.meta];
            messages.push(message);
            if let Some(timing) = built.timing {
                timings.push(timing);
            }
        }

        let commit_start = vector_index_build_timing_enabled().then(Instant::now);
        commit
            .commit_if_latest_snapshot(messages, snapshot.id())
            .await?;
        if let Some(commit_start) = commit_start {
            let commit = commit_start.elapsed();
            for timing in timings {
                timing.log(&self.index_type, commit);
            }
        }

        Ok(shard_count)
    }
}

#[cfg(test)]
mod tests;
