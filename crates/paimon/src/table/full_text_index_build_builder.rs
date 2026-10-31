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

//! Full-text global index build for data-evolution tables.
//!
//! Mirrors Java's generic global-index build (`GenericIndexTopoBuilder`) driving
//! `NativeFullTextGlobalIndexWriter`: the table's row-id space is cut into
//! `global-index.row-count-per-shard` shards, each shard's text column is fed to
//! the shared `paimon-ftindex-core` writer with shard-relative row ids, and every
//! non-empty shard becomes one unpublished `full-text` index file. Java writes the same archive through the same native core, so the
//! files are readable by both `FullTextSearchBuilder` and Java.

use crate::spec::{
    CoreOptions, DataField, DataType, GlobalIndexMeta, IndexFileMeta, Predicate, ROW_ID_FIELD_NAME,
};
use crate::table::global_index_build_common::vector::{
    find_index_field, plan_vector_index_shards, VectorIndexShard,
};
use crate::table::global_index_build_common::{
    indexed_row_ranges, validate_existing_index_overlap,
};
use crate::table::global_index_types::FULL_TEXT_GLOBAL_INDEX_TYPE;
#[cfg(test)]
use crate::table::index_build::full_text::ShardCursor;
use crate::table::index_build::{
    full_text::{self, full_text_config, native_full_text_options, serialize_index_meta},
    IndexFileWriter,
};
use crate::table::index_file_path::IndexFileLocation;
use crate::table::{CommitMessage, DataSplitBuilder, RowRange, Table};
use crate::{Error, Result};
use std::collections::HashMap;

pub(super) struct FullTextIndexBuildBuilder<'a> {
    table: &'a Table,
    index_column: Option<String>,
    options: HashMap<String, String>,
    pub(super) partition_filter: Option<Predicate>,
    pub(super) view: Option<super::global_index_build_common::GlobalIndexBuildView>,
}

impl<'a> FullTextIndexBuildBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            index_column: None,
            options: HashMap::new(),
            partition_filter: None,
            view: None,
        }
    }

    pub fn with_index_column(&mut self, column: &str) -> &mut Self {
        self.index_column = Some(column.to_string());
        self
    }

    /// Build options layered over the table options, like the `options`
    /// argument of Java `CreateGlobalIndexProcedure`.
    pub fn with_options(&mut self, options: HashMap<String, String>) -> &mut Self {
        self.options = options;
        self
    }

    pub(super) async fn prepare(&self) -> Result<(Option<i64>, Vec<CommitMessage>)> {
        // Building the index scans the table's rows.
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;

        self.table.ensure_not_branch_reference_for_write()?;

        let index_column = self
            .index_column
            .as_deref()
            .ok_or_else(|| Error::DataInvalid {
                message: "Full-text index column is required".to_string(),
                source: None,
            })?;

        let table_core_options = CoreOptions::new(self.table.schema().options());
        validate_table_options(self.table, &table_core_options)?;

        // Java reads the shard size from the table options merged with the
        // procedure options.
        let mut merged_options = self.table.schema().options().clone();
        merged_options.extend(self.options.clone());
        let core_options = CoreOptions::new(&merged_options);
        let rows_per_shard = core_options.global_index_row_count_per_shard()?;
        let parallelism = core_options.global_index_build_parallelism()?;

        let index_field = find_index_field(self.table, index_column)?;
        validate_text_field(index_field)?;

        let native_options = native_full_text_options(&merged_options);
        // Fail on bad tokenizer or field options before any data is read.
        full_text_config(&native_options)?;
        let index_meta = serialize_index_meta(&native_options)?;

        let view = super::global_index_build_common::GlobalIndexBuildView::resolve(
            self.table,
            self.view.as_ref(),
        )
        .await?;
        let Some(snapshot) = view.snapshot() else {
            return Ok((None, vec![]));
        };

        let manifest_entries = view
            .manifest_entries(self.table, self.partition_filter.as_ref())
            .await?;
        let indexed = indexed_row_ranges(
            view.index_entries(),
            FULL_TEXT_GLOBAL_INDEX_TYPE,
            index_field.id(),
            None,
        );
        let shards = plan_vector_index_shards(
            self.table.location(),
            self.table.schema().partition_keys(),
            self.table.schema().fields(),
            &table_core_options,
            snapshot.id(),
            manifest_entries,
            rows_per_shard,
            &indexed,
            "full-text",
        )?;
        if shards.is_empty() {
            return Ok((Some(snapshot.id()), vec![]));
        }

        validate_existing_index_overlap(
            view.index_entries(),
            FULL_TEXT_GLOBAL_INDEX_TYPE,
            index_field.id(),
            None,
            &shards
                .iter()
                .map(|shard| RowRange::new(shard.row_range_start, shard.row_range_end))
                .collect::<Vec<_>>(),
        )?;

        let prepared = super::global_index_build_common::preparation::prepare_shards(
            self.table,
            shards,
            parallelism,
            |shard| {
                let native_options = &native_options;
                let index_meta = &index_meta;
                async move {
                    let file = self
                        .build_shard(
                            &shard,
                            index_column,
                            native_options,
                            index_field.id(),
                            index_meta,
                        )
                        .await?;
                    // Java skips a shard whose writer saw no rows.
                    Ok(file.map(|file| {
                        let mut message = CommitMessage::new(shard.partition_bytes, 0, vec![]);
                        message.new_index_files = vec![file];
                        (message, ())
                    }))
                }
            },
        )
        .await?;
        let messages = prepared.into_iter().map(|(message, ())| message).collect();

        Ok((Some(snapshot.id()), messages))
    }

    async fn build_shard(
        &self,
        shard: &VectorIndexShard,
        index_column: &str,
        native_options: &HashMap<String, String>,
        index_field_id: i32,
        index_meta: &[u8],
    ) -> Result<Option<IndexFileMeta>> {
        let split = DataSplitBuilder::new()
            .with_snapshot(shard.snapshot_id)
            .with_partition(shard.partition.clone())
            .with_bucket(shard.source_bucket)
            .with_bucket_path(shard.bucket_path.clone())
            .with_total_buckets(shard.total_buckets)
            .with_data_files(shard.files.clone())
            .with_row_ranges(vec![RowRange::new(
                shard.row_range_start,
                shard.row_range_end,
            )])
            .build()?;
        let mut read_builder = self.table.new_read_builder();
        read_builder.with_projection(&[index_column, ROW_ID_FIELD_NAME])?;
        let read = read_builder.new_read()?;
        let stream = read.to_arrow(&[split])?;

        let meta = GlobalIndexMeta {
            row_range_start: shard.row_range_start,
            row_range_end: shard.row_range_end,
            index_field_id,
            extra_field_ids: None,
            source_meta: Some(
                crate::spec::DataEvolutionIndexSourceMeta::new(shard.snapshot_id)?.serialize(),
            ),
            index_meta: None,
        };
        IndexFileWriter::new(
            self.table,
            FULL_TEXT_GLOBAL_INDEX_TYPE,
            crate::table::global_index_schema::index_file_name(
                FULL_TEXT_GLOBAL_INDEX_TYPE,
                self.table.schema().id(),
            ),
            IndexFileLocation::Global {
                table_path: self.table.location().trim_end_matches('/'),
            },
            "",
            meta,
        )
        .await?
        .write(self.table, |output| {
            full_text::write_payload(
                stream,
                output,
                index_column,
                RowRange::new(shard.row_range_start, shard.row_range_end),
                native_options,
                index_meta.to_vec(),
            )
        })
        .await
    }
}

fn validate_table_options(table: &Table, core_options: &CoreOptions) -> Result<()> {
    super::global_index_build_common::validate_build_table(table)?;
    if core_options.deletion_vectors_enabled() {
        return Err(Error::Unsupported {
            message:
                "Full-text index build does not support tables with deletion-vectors.enabled=true"
                    .to_string(),
        });
    }
    Ok(())
}

/// Java `NativeFullTextGlobalIndexWriter` accepts only character strings.
fn validate_text_field(field: &DataField) -> Result<()> {
    if !matches!(field.data_type(), DataType::Char(_) | DataType::VarChar(_)) {
        return Err(Error::Unsupported {
            message: format!(
                "Full-text index requires a character string column, got {:?} for column '{}'",
                field.data_type(),
                field.name()
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
