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
mod pipeline;
mod planning;
pub(super) mod timing;
mod validation;
mod writer;

use planning::plan_vindex_shards;
use validation::{checked_i32, find_index_field, validate_table_options, validate_vector_field};

use crate::spec::{CoreOptions, Predicate};
use crate::table::{CommitMessage, RowRange, Table};
use crate::vindex::{is_vindex_index_type, VindexVectorIndexOptions};
use crate::{Error, Result};
use std::collections::HashMap;

pub(super) struct VindexIndexBuildBuilder<'a> {
    table: &'a Table,
    index_column: Option<String>,
    index_type: String,
    options: HashMap<String, String>,
    pub(super) partition_filter: Option<Predicate>,
}

impl<'a> VindexIndexBuildBuilder<'a> {
    pub(crate) fn new(table: &'a Table, index_type: &str) -> Self {
        Self {
            table,
            index_column: None,
            index_type: index_type.to_string(),
            options: HashMap::new(),
            partition_filter: None,
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

    pub(super) async fn prepare(
        &self,
    ) -> Result<(
        Option<i64>,
        Vec<CommitMessage>,
        Vec<timing::VectorIndexBuildTiming>,
    )> {
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

        let mut merged_options = self.table.schema().options().clone();
        merged_options.extend(self.options.clone());
        let core_options = CoreOptions::new(&merged_options);
        validate_table_options(self.table, &core_options)?;
        let rows_per_shard = core_options.global_index_row_count_per_shard()?;
        let parallelism = core_options.global_index_build_parallelism()?;

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

        let snapshot_manager = self.table.snapshot_manager();
        let Some(snapshot) = snapshot_manager.get_latest_snapshot().await? else {
            return Ok((None, vec![], vec![]));
        };

        let mut read_builder = self.table.new_read_builder();
        if let Some(filter) = &self.partition_filter {
            read_builder.with_filter(filter.clone());
        }
        let manifest_entries = read_builder
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
            return Ok((Some(snapshot.id()), vec![], vec![]));
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

        let prepared = super::global_index_build_common::preparation::prepare_shards(
            self.table,
            shards,
            parallelism,
            |shard| {
                let vindex_options = &vindex_options;
                let index_meta = index_meta.clone();
                async move {
                    let built = self
                        .build_index_file(
                            &shard,
                            index_column,
                            dimension,
                            index_field.id(),
                            vindex_options,
                            index_meta,
                        )
                        .await?;
                    Ok(built.map(|built| {
                        let mut message = CommitMessage::new(shard.partition_bytes, 0, vec![]);
                        message.new_index_files = vec![built.meta];
                        (message, built.timing)
                    }))
                }
            },
        )
        .await?;
        let mut messages = Vec::with_capacity(prepared.len());
        let mut timings = Vec::with_capacity(prepared.len());
        for (message, timing) in prepared {
            messages.push(message);
            if let Some(timing) = timing {
                timings.push(timing);
            }
        }

        Ok((Some(snapshot.id()), messages, timings))
    }
}

#[cfg(test)]
mod tests;
