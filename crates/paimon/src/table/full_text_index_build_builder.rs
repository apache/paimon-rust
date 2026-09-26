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
//! non-empty shard becomes one `full-text` index file committed in a single
//! snapshot. Java writes the same archive through the same native core, so the
//! files are readable by both `FullTextSearchBuilder` and Java.

use crate::spec::{
    CoreOptions, DataField, DataType, GlobalIndexMeta, IndexFileMeta, ROW_ID_FIELD_NAME,
};
use crate::table::global_index_build_common::vector::{
    find_index_field, plan_vector_index_shards, VectorIndexShard,
};
use crate::table::global_index_build_common::{
    copy_local_file_to_output, indexed_row_ranges, validate_existing_index_overlap,
};
use crate::table::global_index_types::FULL_TEXT_GLOBAL_INDEX_TYPE;
use crate::table::{
    CommitMessage, DataSplitBuilder, RowRange, SnapshotManager, Table, TableCommit,
};
use crate::{Error, Result};
use arrow_array::cast::AsArray;
use arrow_array::{Array, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow_schema::DataType as ArrowDataType;
use futures::TryStreamExt;
use paimon_ftindex_core::io::PosWriter;
use paimon_ftindex_core::{FullTextIndexConfig, FullTextIndexWriter};
use std::collections::{BTreeMap, HashMap};

const INDEX_DIR: &str = "index";
/// Java `NativeFullTextIndexOptions.FULL_TEXT_PREFIX`: only these options reach
/// the native writer, with the prefix removed.
const FULL_TEXT_OPTION_PREFIX: &str = "full-text.";

pub struct FullTextIndexBuildBuilder<'a> {
    table: &'a Table,
    index_column: Option<String>,
    options: HashMap<String, String>,
}

impl<'a> FullTextIndexBuildBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            index_column: None,
            options: HashMap::new(),
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

    /// Build full-text index files for every row range of the latest snapshot
    /// that is not yet covered by a full-text index on the column. Returns the
    /// number of index files committed.
    pub async fn execute(&self) -> Result<usize> {
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
        let rows_per_shard =
            CoreOptions::new(&merged_options).global_index_row_count_per_shard()?;

        let index_field = find_index_field(self.table, index_column)?;
        validate_text_field(index_field)?;

        let native_options = native_full_text_options(&merged_options);
        // Fail on bad tokenizer or field options before any data is read.
        full_text_config(&native_options)?;
        let index_meta = serialize_index_meta(&native_options)?;

        let snapshot_manager = SnapshotManager::new(
            self.table.file_io().clone(),
            self.table.location().to_string(),
        );
        let snapshot = snapshot_manager
            .get_latest_snapshot()
            .await?
            .ok_or_else(|| Error::DataInvalid {
                message: "Cannot build full-text index without a snapshot".to_string(),
                source: None,
            })?;

        let manifest_entries = self
            .table
            .new_read_builder()
            .new_scan()
            .with_scan_all_files()
            .plan_manifest_entries(&snapshot)
            .await?;
        let indexed = indexed_row_ranges(
            self.table,
            snapshot.index_manifest(),
            FULL_TEXT_GLOBAL_INDEX_TYPE,
            index_field.id(),
            None,
        )
        .await?;
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
            return Ok(0);
        }

        validate_existing_index_overlap(
            self.table,
            snapshot.index_manifest(),
            FULL_TEXT_GLOBAL_INDEX_TYPE,
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
                FULL_TEXT_GLOBAL_INDEX_TYPE,
                uuid::Uuid::new_v4()
            ),
        );
        let mut messages: Vec<CommitMessage> = Vec::with_capacity(shards.len());
        for shard in shards {
            let build_result = self
                .build_shard(
                    &shard,
                    index_column,
                    &native_options,
                    index_field.id(),
                    &index_meta,
                )
                .await;
            let index_file = match build_result {
                Ok(index_file) => index_file,
                Err(error) => {
                    let _ = commit.abort(&messages).await;
                    return Err(error);
                }
            };
            // Java skips a shard whose writer saw no rows.
            let Some(index_file) = index_file else {
                continue;
            };
            let mut message = CommitMessage::new(shard.partition_bytes.clone(), 0, vec![]);
            message.new_index_files = vec![index_file];
            messages.push(message);
        }
        if messages.is_empty() {
            return Ok(0);
        }

        let file_count = messages.len();
        commit
            .commit_if_latest_snapshot(messages, snapshot.id())
            .await?;
        Ok(file_count)
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
        let mut stream = read.to_arrow(&[split])?;

        let config = full_text_config(native_options)?;
        let mut writer =
            run_blocking(move || FullTextIndexWriter::new(config).map_err(full_text_error)).await?;
        let mut cursor = ShardCursor::new(shard.row_range_start, shard.row_range_end);
        while let Some(batch) = stream.try_next().await? {
            let documents = cursor.documents(&batch, index_column)?;
            writer = run_blocking(move || {
                for (row_id, text) in documents {
                    writer.add_document(row_id, text).map_err(full_text_error)?;
                }
                Ok(writer)
            })
            .await?;
        }
        if cursor.row_count == 0 {
            return Ok(None);
        }

        let local_index = run_blocking(move || {
            let mut file = tempfile::NamedTempFile::new().map_err(|e| Error::UnexpectedError {
                message: format!("Failed to create temporary full-text index file: {e}"),
                source: None,
            })?;
            writer
                .write(&mut PosWriter::new(file.as_file_mut()))
                .map_err(full_text_error)?;
            file.as_file_mut()
                .sync_all()
                .map_err(|e| Error::UnexpectedError {
                    message: format!("Failed to flush temporary full-text index file: {e}"),
                    source: None,
                })?;
            Ok(file)
        })
        .await?;

        let table_location = self.table.location().trim_end_matches('/');
        let file_name = format!(
            "{FULL_TEXT_GLOBAL_INDEX_TYPE}-global-index-{}.index",
            uuid::Uuid::new_v4()
        );
        let index_path = format!("{table_location}/{INDEX_DIR}/{file_name}");
        self.table
            .file_io()
            .mkdirs(&format!("{table_location}/{INDEX_DIR}/"))
            .await?;
        let upload: Result<i64> = async {
            copy_local_file_to_output(
                local_index.path(),
                self.table.file_io().new_output(&index_path)?,
            )
            .await?;
            let status = self.table.file_io().get_status(&index_path).await?;
            i64::try_from(status.size).map_err(|_| Error::DataInvalid {
                message: format!(
                    "Full-text index file '{file_name}' is too large: {} bytes",
                    status.size
                ),
                source: None,
            })
        }
        .await;
        let file_size = match upload {
            Ok(file_size) => file_size,
            Err(error) => {
                let _ = self.table.file_io().delete_file(&index_path).await;
                return Err(error);
            }
        };

        Ok(Some(IndexFileMeta {
            index_type: FULL_TEXT_GLOBAL_INDEX_TYPE.to_string(),
            file_name,
            file_size,
            row_count: cursor.row_count,
            deletion_vectors_ranges: None,
            external_path: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: shard.row_range_start,
                row_range_end: shard.row_range_end,
                index_field_id,
                extra_field_ids: None,
                source_meta: None,
                index_meta: Some(index_meta.to_vec()),
            }),
        }))
    }
}

/// Walks one shard's read batches in row-id order and turns them into
/// shard-relative `(row_id, text)` documents, like the loop in Java
/// `GenericIndexTopoBuilder.BuildIndexOperator#processElement`.
struct ShardCursor {
    row_range_start: i64,
    row_range_end: i64,
    last_row_id: Option<i64>,
    /// Rows inside the shard range, NULL text included (Java `rowCount`).
    row_count: i64,
}

impl ShardCursor {
    fn new(row_range_start: i64, row_range_end: i64) -> Self {
        Self {
            row_range_start,
            row_range_end,
            last_row_id: None,
            row_count: 0,
        }
    }

    fn documents(&mut self, batch: &RecordBatch, index_column: &str) -> Result<Vec<(i64, String)>> {
        let text_index = batch
            .schema()
            .index_of(index_column)
            .map_err(|e| Error::DataInvalid {
                message: format!("Full-text column '{index_column}' not found in read batch: {e}"),
                source: None,
            })?;
        let row_id_index =
            batch
                .schema()
                .index_of(ROW_ID_FIELD_NAME)
                .map_err(|e| Error::DataInvalid {
                    message: format!("_ROW_ID column not found in read batch: {e}"),
                    source: None,
                })?;
        let row_ids = batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::DataInvalid {
                message: "Full-text index build requires Int64 _ROW_ID".to_string(),
                source: None,
            })?;
        let texts = batch.column(text_index);
        let texts = TextColumn::try_new(texts.as_ref())?;

        let mut documents = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if row_ids.is_null(row) {
                return Err(Error::DataInvalid {
                    message: "Full-text index build found null _ROW_ID".to_string(),
                    source: None,
                });
            }
            let row_id = row_ids.value(row);
            if let Some(last_row_id) = self.last_row_id {
                if row_id < last_row_id {
                    return Err(Error::DataInvalid {
                        message: format!(
                            "Row IDs are not monotonically increasing: previous={last_row_id}, \
                             current={row_id} in shard [{}, {}]",
                            self.row_range_start, self.row_range_end
                        ),
                        source: None,
                    });
                }
            }
            self.last_row_id = Some(row_id);
            if row_id < self.row_range_start || row_id > self.row_range_end {
                continue;
            }
            self.row_count += 1;
            if let Some(text) = texts.value(row) {
                documents.push((row_id - self.row_range_start, text.to_string()));
            }
        }
        Ok(documents)
    }
}

/// The Arrow string layouts a CHAR/VARCHAR column can be read as.
enum TextColumn<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> TextColumn<'a> {
    fn try_new(array: &'a dyn Array) -> Result<Self> {
        match array.data_type() {
            ArrowDataType::Utf8 => Ok(Self::Utf8(array.as_string::<i32>())),
            ArrowDataType::LargeUtf8 => Ok(Self::LargeUtf8(array.as_string::<i64>())),
            ArrowDataType::Utf8View => Ok(Self::Utf8View(array.as_string_view())),
            other => Err(Error::DataInvalid {
                message: format!(
                    "Full-text index build requires a string column, got Arrow type {other}"
                ),
                source: None,
            }),
        }
    }

    fn value(&self, row: usize) -> Option<&'a str> {
        match self {
            Self::Utf8(array) => array.is_valid(row).then(|| array.value(row)),
            Self::LargeUtf8(array) => array.is_valid(row).then(|| array.value(row)),
            Self::Utf8View(array) => array.is_valid(row).then(|| array.value(row)),
        }
    }
}

fn validate_table_options(table: &Table, core_options: &CoreOptions) -> Result<()> {
    if !table.schema().primary_keys().is_empty() {
        return Err(Error::Unsupported {
            message: "Full-text index build does not support primary-key tables; \
                      use 'pk-full-text.index.columns' instead"
                .to_string(),
        });
    }
    if !core_options.row_tracking_enabled() {
        return Err(Error::DataInvalid {
            message: "Full-text index build requires 'row-tracking.enabled' = 'true'".to_string(),
            source: None,
        });
    }
    if !core_options.data_evolution_enabled() {
        return Err(Error::DataInvalid {
            message: "Full-text index build requires 'data-evolution.enabled' = 'true'".to_string(),
            source: None,
        });
    }
    if !core_options.global_index_enabled() {
        return Err(Error::DataInvalid {
            message: "Full-text index build requires 'global-index.enabled' = 'true'".to_string(),
            source: None,
        });
    }
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

/// Java `options.removePrefix("full-text.")`.
fn native_full_text_options(options: &HashMap<String, String>) -> HashMap<String, String> {
    options
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix(FULL_TEXT_OPTION_PREFIX)
                .map(|native_key| (native_key.to_string(), value.clone()))
        })
        .collect()
}

fn full_text_config(native_options: &HashMap<String, String>) -> Result<FullTextIndexConfig> {
    FullTextIndexConfig::from_options(native_options).map_err(|e| Error::ConfigInvalid {
        message: format!("Invalid full-text index options: {e}"),
    })
}

/// Java `NativeFullTextIndexOptions#serialize`: the native options as a flat
/// JSON object. Keys are sorted so the metadata is deterministic.
fn serialize_index_meta(native_options: &HashMap<String, String>) -> Result<Vec<u8>> {
    let sorted = native_options.iter().collect::<BTreeMap<_, _>>();
    serde_json::to_vec(&sorted).map_err(|e| Error::DataInvalid {
        message: format!("Failed to serialize full-text index metadata: {e}"),
        source: Some(Box::new(e)),
    })
}

fn full_text_error(error: impl std::fmt::Display) -> Error {
    Error::UnexpectedError {
        message: format!("Full-text index error: {error}"),
        source: None,
    }
}

/// The native writer is synchronous and CPU-bound; keep it off the async workers.
async fn run_blocking<T, F>(task: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(task)
        .await
        .map_err(|e| Error::UnexpectedError {
            message: format!("Full-text index build task failed: {e}"),
            source: None,
        })?
}

#[cfg(test)]
mod tests;
