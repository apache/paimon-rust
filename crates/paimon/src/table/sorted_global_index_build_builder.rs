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
mod validation;
mod writer;

#[cfg(test)]
use extraction::*;
use planning::plan_sorted_index_shards;
#[cfg(test)]
use planning::SortedGlobalIndexShard;
use validation::*;

use super::bitmap_global_index_format::{make_bitmap_key_comparator, serialize_bitmap_datum};
use super::global_index_types::{
    normalize_queryable_global_index_type, BITMAP_GLOBAL_INDEX_TYPE, BTREE_GLOBAL_INDEX_TYPE,
    FM_GLOBAL_INDEX_TYPE,
};
use super::sorted_global_index_options::SortedIndexWriteOptions;
use crate::btree::key_serde::KeyComparator;
use crate::btree::{make_key_comparator, serialize_datum};
use crate::fm_index::{FMOptions, FMWriteOptions};
use crate::spec::{CoreOptions, DataType, Datum};
use crate::table::{CommitMessage, RowRange, SnapshotManager, Table, TableCommit};
use crate::{Error, Result};
use std::collections::HashMap;

const INDEX_DIR: &str = "index";

type SortedIndexKeyRow = (Option<Vec<u8>>, i64);
type SerializeKeyFn = fn(&Datum, &DataType) -> Vec<u8>;

enum GlobalIndexWriteOptions {
    Sorted(SortedIndexWriteOptions),
    FM(FMWriteOptions),
}

fn make_index_key_codec(index_type: &str, data_type: &DataType) -> (KeyComparator, SerializeKeyFn) {
    match index_type {
        BTREE_GLOBAL_INDEX_TYPE => (make_key_comparator(data_type), serialize_datum),
        BITMAP_GLOBAL_INDEX_TYPE => (
            make_bitmap_key_comparator(data_type),
            serialize_bitmap_datum,
        ),
        _ => unreachable!("normalized sorted global index type"),
    }
}

pub struct SortedGlobalIndexBuildBuilder<'a> {
    table: &'a Table,
    index_column: Option<String>,
    index_type: String,
    options: HashMap<String, String>,
}

/// Backward-compatible name retained for callers that used the original
/// BTree-only builder API before it also supported bitmap and multivalue.
pub type BTreeGlobalIndexBuildBuilder<'a> = SortedGlobalIndexBuildBuilder<'a>;

impl<'a> SortedGlobalIndexBuildBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            index_column: None,
            index_type: BTREE_GLOBAL_INDEX_TYPE.to_string(),
            options: HashMap::new(),
        }
    }

    pub fn with_index_column(&mut self, column: &str) -> &mut Self {
        self.index_column = Some(column.to_string());
        self
    }

    pub fn with_index_type(&mut self, index_type: &str) -> &mut Self {
        self.index_type = index_type.to_string();
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

        let index_type = normalize_queryable_global_index_type(&self.index_type).ok_or_else(|| {
            Error::Unsupported {
                message: format!(
                    "Scalar global index build only supports index_type => 'btree', 'bitmap', 'multivalue', or 'fm', got '{}'",
                    self.index_type
                ),
            }
        })?;
        let index_column = self
            .index_column
            .as_deref()
            .ok_or_else(|| Error::DataInvalid {
                message: "Sorted global index column is required".to_string(),
                source: None,
            })?;

        let mut resolved_options = self.table.schema().options().clone();
        resolved_options.extend(self.options.clone());
        let core_options = CoreOptions::new(&resolved_options);
        validate_table_options(self.table, &core_options)?;
        let records_per_range = core_options.sorted_index_records_per_range()?;
        let write_options = if index_type == FM_GLOBAL_INDEX_TYPE {
            GlobalIndexWriteOptions::FM(FMOptions::from_options(&resolved_options)?.write)
        } else {
            GlobalIndexWriteOptions::Sorted(SortedIndexWriteOptions::from_options(
                index_type,
                &resolved_options,
            )?)
        };

        let index_field = find_index_field(self.table, index_column)?;
        index_key_type(index_type, index_field)?;

        let snapshot_manager = SnapshotManager::new(
            self.table.file_io().clone(),
            self.table.location().to_string(),
        );
        let snapshot = snapshot_manager
            .get_latest_snapshot()
            .await?
            .ok_or_else(|| Error::DataInvalid {
                message: "Cannot build sorted global index without a snapshot".to_string(),
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
            index_type,
            index_field.id(),
            None, // single-column build; no extra fields today
        )
        .await?;

        let shards = plan_sorted_index_shards(
            self.table.location(),
            self.table.schema().partition_keys(),
            self.table.schema().fields(),
            &core_options,
            snapshot.id(),
            manifest_entries,
            records_per_range,
            &indexed,
        )?;
        if shards.is_empty() {
            return Ok(0);
        }

        crate::table::global_index_build_common::validate_existing_index_overlap(
            self.table,
            snapshot.index_manifest(),
            index_type,
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
                index_type,
                uuid::Uuid::new_v4()
            ),
        );
        let shard_count = shards.len();
        let mut messages = Vec::with_capacity(shard_count);
        for shard in shards {
            let index_file = match self
                .build_index_file(&shard, index_field, index_column, &write_options)
                .await
            {
                Ok(index_file) => index_file,
                Err(error) => {
                    let _ = commit.abort(&messages).await;
                    return Err(error);
                }
            };
            let mut message =
                CommitMessage::new(shard.partition_bytes.clone(), shard.source_bucket, vec![]);
            message.new_index_files = vec![index_file];
            messages.push(message);
        }

        commit
            .commit_if_latest_snapshot(messages, snapshot.id())
            .await?;

        Ok(shard_count)
    }
}

#[cfg(test)]
mod tests;
