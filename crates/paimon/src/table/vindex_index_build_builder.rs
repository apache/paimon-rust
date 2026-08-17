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

use crate::spec::{
    bucket_dir_name, BinaryRow, CoreOptions, DataField, DataFileMeta, DataType, FileKind,
    GlobalIndexMeta, IndexFileMeta, ROW_ID_FIELD_NAME,
};
use crate::table::source::exclude_row_ranges;
use crate::table::{
    CommitMessage, DataSplit, DataSplitBuilder, RowRange, SnapshotManager, Table, TableCommit,
};
use crate::vindex::{is_vindex_index_type, VindexVectorIndexOptions};
use crate::{Error, Result};
use arrow_array::{Array, FixedSizeListArray, Float32Array, Int64Array, ListArray, RecordBatch};
use arrow_buffer::MutableBuffer;
use futures::TryStreamExt;
use paimon_vindex_core::index::{VectorIndexTrainer, VectorIndexWriter};
use paimon_vindex_core::io::PosWriter;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use tokio::io::AsyncWriteExt;
use tokio_util::io::SyncIoBridge;

const INDEX_DIR: &str = "index";
const VECTOR_BUFFER_BYTES: usize = 8 * 1024 * 1024;

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
        for shard in shards {
            let index_file = match self
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
            message.new_index_files = vec![index_file];
            messages.push(message);
        }

        commit
            .commit_if_latest_snapshot(messages, snapshot.id())
            .await?;

        Ok(shard_count)
    }

    async fn build_index_file(
        &self,
        shard: &VindexIndexShard,
        index_column: &str,
        dimension: i32,
        index_field_id: i32,
        options: &VindexVectorIndexOptions,
        index_meta: Vec<u8>,
    ) -> Result<IndexFileMeta> {
        let row_count = checked_row_count(shard.row_range_start, shard.row_range_end)?;
        let row_count_usize = usize::try_from(row_count).map_err(|e| Error::DataInvalid {
            message: format!("Invalid vindex row count: {row_count}"),
            source: Some(Box::new(e)),
        })?;
        let dimension_usize = usize::try_from(dimension).map_err(|e| Error::DataInvalid {
            message: format!("Invalid vindex dimension: {dimension}"),
            source: Some(Box::new(e)),
        })?;
        if dimension_usize == 0 {
            return Err(Error::DataInvalid {
                message: "vindex vector dimension must be positive".to_string(),
                source: None,
            });
        }
        let expected_bytes = checked_vector_bytes(row_count_usize, dimension_usize)?;
        let training_vector_count =
            checked_training_vector_count(row_count_usize, options.train_sample_ratio)?;
        let training_buffer_rows =
            (VECTOR_BUFFER_BYTES / checked_vector_bytes(1, dimension_usize)?).max(1);
        let training_buffer_floats = training_buffer_rows
            .checked_mul(dimension_usize)
            .ok_or_else(|| Error::DataInvalid {
                message: "vindex training buffer length overflows usize".to_string(),
                source: None,
            })?;

        let mut trainer =
            VectorIndexTrainer::new(options.config.clone()).map_err(|e| Error::DataInvalid {
                message: format!("Failed to initialize vindex trainer: {e}"),
                source: Some(Box::new(e)),
            })?;
        let raw_file = tempfile::tempfile().map_err(|e| Error::UnexpectedError {
            message: format!("Failed to create temporary vindex vector file: {e}"),
            source: Some(Box::new(e)),
        })?;
        let mut raw_file = tokio::fs::File::from_std(raw_file);
        let split = data_split_for_shard(shard)?;
        let mut read_builder = self.table.new_read_builder();
        read_builder.with_projection(&[index_column, ROW_ID_FIELD_NAME])?;
        let read = read_builder.new_read()?;
        let mut batches = read.to_arrow(&[split])?;
        let mut expected_row_id = shard.row_range_start;
        let mut rows_seen = 0usize;
        let mut bytes_written = 0usize;
        let mut next_training_sample = 0usize;
        let mut training_buffer = Vec::with_capacity(training_buffer_floats);

        while let Some(batch) = batches.try_next().await? {
            let vectors =
                validate_vector_batch(&batch, index_column, dimension_usize, &mut expected_row_id)?;
            let batch_end =
                rows_seen
                    .checked_add(vectors.row_count)
                    .ok_or_else(|| Error::DataInvalid {
                        message: "vindex streamed row count overflows usize".to_string(),
                        source: None,
                    })?;

            if training_vector_count == row_count_usize {
                trainer
                    .add_training_vectors_mut(vectors.values, vectors.row_count)
                    .map_err(|e| Error::DataInvalid {
                        message: format!("Failed to add vindex training vectors: {e}"),
                        source: Some(Box::new(e)),
                    })?;
            } else {
                while next_training_sample < training_vector_count {
                    let sample_row = checked_training_sample_index(
                        next_training_sample,
                        row_count_usize,
                        training_vector_count,
                    )?;
                    if sample_row >= batch_end {
                        break;
                    }
                    let start = (sample_row - rows_seen) * dimension_usize;
                    training_buffer
                        .extend_from_slice(&vectors.values[start..start + dimension_usize]);
                    next_training_sample += 1;
                    if training_buffer.len() == training_buffer_floats {
                        trainer
                            .add_training_vectors_mut(
                                &training_buffer,
                                training_buffer.len() / dimension_usize,
                            )
                            .map_err(|e| Error::DataInvalid {
                                message: format!("Failed to add vindex training vectors: {e}"),
                                source: Some(Box::new(e)),
                            })?;
                        training_buffer.clear();
                    }
                }
            }

            raw_file
                .write_all(vectors.bytes)
                .await
                .map_err(|e| Error::UnexpectedError {
                    message: format!("Failed to spill vindex vectors: {e}"),
                    source: Some(Box::new(e)),
                })?;
            bytes_written = bytes_written
                .checked_add(vectors.bytes.len())
                .ok_or_else(|| Error::DataInvalid {
                    message: "vindex spilled byte count overflows usize".to_string(),
                    source: None,
                })?;
            rows_seen = batch_end;
        }

        if !training_buffer.is_empty() {
            trainer
                .add_training_vectors_mut(&training_buffer, training_buffer.len() / dimension_usize)
                .map_err(|e| Error::DataInvalid {
                    message: format!("Failed to add vindex training vectors: {e}"),
                    source: Some(Box::new(e)),
                })?;
        }
        if rows_seen != row_count_usize
            || expected_row_id
                != shard
                    .row_range_end
                    .checked_add(1)
                    .ok_or_else(|| Error::DataInvalid {
                        message: "vindex row range end overflows i64".to_string(),
                        source: None,
                    })?
            || (training_vector_count != row_count_usize
                && next_training_sample != training_vector_count)
            || bytes_written != expected_bytes
        {
            return Err(Error::DataInvalid {
                message: format!(
                    "vindex streamed data mismatch: rows={rows_seen}/{row_count_usize}, training={next_training_sample}/{training_vector_count}, bytes={bytes_written}/{expected_bytes}"
                ),
                source: None,
            });
        }
        raw_file.flush().await.map_err(|e| Error::UnexpectedError {
            message: format!("Failed to flush temporary vindex vector file: {e}"),
            source: Some(Box::new(e)),
        })?;
        let raw_file_len = raw_file
            .metadata()
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to inspect temporary vindex vector file: {e}"),
                source: Some(Box::new(e)),
            })?
            .len();
        if raw_file_len != expected_bytes as u64 {
            return Err(Error::DataInvalid {
                message: format!(
                    "temporary vindex vector file size mismatch: {raw_file_len}/{expected_bytes}"
                ),
                source: None,
            });
        }
        let raw_file = raw_file.into_std().await;

        let writer = tokio::task::spawn_blocking(move || -> std::io::Result<VectorIndexWriter> {
            let training = trainer.finish()?;
            let mut writer = VectorIndexWriter::new(training);
            let mut raw_file = raw_file;
            raw_file.seek(SeekFrom::Start(0))?;
            let batch_rows = training_buffer_rows.min(row_count_usize);
            let batch_bytes = checked_std_vector_bytes(batch_rows, dimension_usize)?;
            let mut buffer = MutableBuffer::new(batch_bytes);
            let mut ids = Vec::with_capacity(batch_rows);
            let mut rows_added = 0usize;
            while rows_added < row_count_usize {
                let rows = batch_rows.min(row_count_usize - rows_added);
                buffer.resize(checked_std_vector_bytes(rows, dimension_usize)?, 0);
                raw_file.read_exact(buffer.as_slice_mut())?;
                ids.clear();
                for row in rows_added..rows_added + rows {
                    ids.push(i64::try_from(row).map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "vindex row id does not fit i64",
                        )
                    })?);
                }
                writer.add_vectors(&ids, buffer.typed_data::<f32>(), rows)?;
                rows_added += rows;
            }
            let mut trailing = [0u8; 1];
            if raw_file.read(&mut trailing)? != 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "temporary vindex vector file contains trailing bytes",
                ));
            }
            Ok(writer)
        })
        .await
        .map_err(|e| Error::UnexpectedError {
            message: format!("vindex training task failed: {e}"),
            source: None,
        })?
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to train or add vectors to vindex index: {e}"),
            source: Some(Box::new(e)),
        })?;

        self.table
            .file_io()
            .mkdirs(&format!(
                "{}/{INDEX_DIR}/",
                self.table.location().trim_end_matches('/')
            ))
            .await?;
        let file_name = format!(
            "vector-{}-global-index-{}.index",
            self.index_type,
            uuid::Uuid::new_v4()
        );
        let index_path = format!(
            "{}/{INDEX_DIR}/{}",
            self.table.location().trim_end_matches('/'),
            file_name
        );
        let write_result = async {
            let async_writer = self
                .table
                .file_io()
                .new_output(&index_path)?
                .async_writer()
                .await?;
            let mut output = SyncIoBridge::new(async_writer);
            tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                let mut writer = writer;
                writer.write(&mut PosWriter::new(&mut output))?;
                output.shutdown()
            })
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("vindex serialization task failed: {e}"),
                source: None,
            })?
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to stream vindex index: {e}"),
                source: Some(Box::new(e)),
            })?;
            self.table.file_io().get_status(&index_path).await
        }
        .await;
        let status = match write_result {
            Ok(status) => status,
            Err(error) => {
                let _ = self.table.file_io().delete_file(&index_path).await;
                return Err(error);
            }
        };
        Ok(IndexFileMeta {
            index_type: self.index_type.clone(),
            file_name,
            file_size: checked_i64(
                status.size,
                "Index file is too large for Rust IndexFileMeta",
            )?,
            row_count,
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: shard.row_range_start,
                row_range_end: shard.row_range_end,
                index_field_id,
                extra_field_ids: None,
                source_meta: None,
                index_meta: Some(index_meta),
            }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VindexIndexShard {
    pub partition: BinaryRow,
    pub partition_bytes: Vec<u8>,
    pub files: Vec<DataFileMeta>,
    pub row_range_start: i64,
    pub row_range_end: i64,
    snapshot_id: i64,
    source_bucket: i32,
    total_buckets: i32,
    bucket_path: String,
}

fn validate_table_options(table: &Table, core_options: &CoreOptions) -> Result<()> {
    if !core_options.row_tracking_enabled() {
        return Err(Error::DataInvalid {
            message: "vindex index build requires 'row-tracking.enabled' = 'true'".to_string(),
            source: None,
        });
    }
    if !core_options.data_evolution_enabled() {
        return Err(Error::DataInvalid {
            message: "vindex index build requires 'data-evolution.enabled' = 'true'".to_string(),
            source: None,
        });
    }
    if !core_options.global_index_enabled() {
        return Err(Error::DataInvalid {
            message: "vindex index build requires 'global-index.enabled' = 'true'".to_string(),
            source: None,
        });
    }
    if !table.schema().primary_keys().is_empty() {
        return Err(Error::Unsupported {
            message: "vindex index build does not support primary-key tables".to_string(),
        });
    }
    if core_options.deletion_vectors_enabled() {
        return Err(Error::Unsupported {
            message:
                "vindex index build does not support tables with deletion-vectors.enabled=true"
                    .to_string(),
        });
    }
    Ok(())
}

fn find_index_field<'a>(table: &'a Table, column: &str) -> Result<&'a DataField> {
    table
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == column)
        .ok_or_else(|| Error::ColumnNotExist {
            full_name: table.identifier().full_name(),
            column: column.to_string(),
        })
}

fn validate_vector_field(field: &DataField) -> Result<()> {
    let is_array_float = matches!(
        field.data_type(),
        DataType::Array(array) if matches!(array.element_type(), DataType::Float(_))
    );
    let is_vector_float = matches!(
        field.data_type(),
        DataType::Vector(vector) if matches!(vector.element_type(), DataType::Float(_))
    );
    if !is_array_float && !is_vector_float {
        return Err(Error::DataInvalid {
            message: format!(
                "vindex index requires ARRAY<FLOAT> or VECTOR<FLOAT> column, got {:?} for column '{}'",
                field.data_type(),
                field.name()
            ),
            source: None,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn plan_vindex_shards(
    table_location: &str,
    partition_keys: &[String],
    schema_fields: &[DataField],
    core_options: &CoreOptions,
    snapshot_id: i64,
    entries: Vec<crate::spec::ManifestEntry>,
    rows_per_shard: i64,
    indexed: &[RowRange],
) -> Result<Vec<VindexIndexShard>> {
    if rows_per_shard <= 0 {
        return Err(Error::DataInvalid {
            message: format!(
                "Option 'global-index.row-count-per-shard' must be greater than 0, got: {rows_per_shard}"
            ),
            source: None,
        });
    }

    let mut by_partition_bucket: HashMap<(Vec<u8>, i32, i32), Vec<DataFileMeta>> = HashMap::new();
    for entry in entries {
        if *entry.kind() != FileKind::Add {
            continue;
        }
        if entry.file().first_row_id.is_none() {
            return Err(Error::DataInvalid {
                message: format!(
                    "Data file '{}' is missing first_row_id; cannot build a complete vindex index",
                    entry.file().file_name
                ),
                source: None,
            });
        }
        let (partition, bucket, total_buckets, file) = entry.into_parts();
        by_partition_bucket
            .entry((partition, bucket, total_buckets))
            .or_default()
            .push(file);
    }

    let mut result = Vec::new();
    for ((partition_bytes, source_bucket, total_buckets), files) in by_partition_bucket {
        let partition = if partition_keys.is_empty() {
            BinaryRow::new(0)
        } else {
            BinaryRow::from_serialized_bytes(&partition_bytes)?
        };
        let bucket_path = bucket_path(
            table_location,
            partition_keys,
            schema_fields,
            core_options,
            &partition,
            source_bucket,
        )?;
        let mut files_by_shard: HashMap<i64, Vec<DataFileMeta>> = HashMap::new();
        for file in files {
            let (file_start, file_end) = file.row_id_range().ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "Data file '{}' is missing first_row_id; cannot build a complete vindex index",
                    file.file_name
                ),
                source: None,
            })?;
            let start_shard = file_start / rows_per_shard;
            let end_shard = file_end / rows_per_shard;
            for shard_id in start_shard..=end_shard {
                files_by_shard
                    .entry(shard_id * rows_per_shard)
                    .or_default()
                    .push(file.clone());
            }
        }

        let mut shard_starts = files_by_shard.keys().copied().collect::<Vec<_>>();
        shard_starts.sort_unstable();
        for shard_start in shard_starts {
            let shard_end = shard_start + rows_per_shard - 1;
            let mut shard_files = files_by_shard.remove(&shard_start).unwrap_or_default();
            shard_files.sort_by_key(|file| file.first_row_id);
            let groups = group_contiguous_files(shard_files)?;
            for group in groups {
                let group_start = group
                    .first()
                    .and_then(|file| file.first_row_id)
                    .expect("planned groups are non-empty and row-id assigned");
                let group_end = group
                    .iter()
                    .map(|file| file.row_id_range().unwrap().1)
                    .max()
                    .unwrap();
                // Coverage of this group clamped to the current shard cell. Then
                // subtract the already-indexed ranges so the build only covers
                // the gap. Because grid-clamp and gap-subtraction are both range
                // intersections, applying the gap here is equivalent to btree's
                // "exclude then split" -- and each surviving segment stays inside
                // one shard cell, preserving per-shard row-id contiguity (the
                // reader errors on a row-id gap within a shard).
                let coverage_start = group_start.max(shard_start);
                let coverage_end = group_end.min(shard_end);
                let build_segments =
                    exclude_row_ranges(&[RowRange::new(coverage_start, coverage_end)], indexed);
                for seg in build_segments {
                    result.push(VindexIndexShard {
                        partition: partition.clone(),
                        partition_bytes: partition_bytes.clone(),
                        files: group.clone(),
                        row_range_start: seg.from(),
                        row_range_end: seg.to(),
                        snapshot_id,
                        source_bucket,
                        total_buckets,
                        bucket_path: bucket_path.clone(),
                    });
                }
            }
        }
    }
    result.sort_by(|a, b| {
        a.partition
            .to_serialized_bytes()
            .cmp(&b.partition.to_serialized_bytes())
            .then(a.source_bucket.cmp(&b.source_bucket))
            .then(a.row_range_start.cmp(&b.row_range_start))
    });
    Ok(result)
}

fn group_contiguous_files(mut files: Vec<DataFileMeta>) -> Result<Vec<Vec<DataFileMeta>>> {
    if files.is_empty() {
        return Ok(Vec::new());
    }
    files.sort_by_key(|file| file.first_row_id);
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut current_end = None;
    for file in files {
        let (file_start, file_end) = file.row_id_range().ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Data file '{}' is missing first_row_id; cannot build a complete vindex index",
                file.file_name
            ),
            source: None,
        })?;
        match current_end {
            None => {
                current.push(file);
                current_end = Some(file_end);
            }
            Some(end) if file_start <= end + 1 => {
                current.push(file);
                current_end = Some(end.max(file_end));
            }
            Some(_) => {
                groups.push(std::mem::take(&mut current));
                current.push(file);
                current_end = Some(file_end);
            }
        }
    }
    if !current.is_empty() {
        groups.push(current);
    }
    Ok(groups)
}

fn bucket_path(
    table_location: &str,
    partition_keys: &[String],
    schema_fields: &[DataField],
    core_options: &CoreOptions,
    partition: &BinaryRow,
    bucket: i32,
) -> Result<String> {
    let base = table_location.trim_end_matches('/');
    if partition_keys.is_empty() {
        return Ok(format!("{base}/{}", bucket_dir_name(bucket)));
    }
    let computer = crate::spec::PartitionComputer::new(
        partition_keys,
        schema_fields,
        core_options.partition_default_name(),
        core_options.legacy_partition_name(),
    )?;
    Ok(format!(
        "{base}/{}{}",
        computer.generate_partition_path(partition)?,
        bucket_dir_name(bucket)
    ))
}

fn data_split_for_shard(shard: &VindexIndexShard) -> Result<DataSplit> {
    DataSplitBuilder::new()
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
        .build()
}

struct ValidatedVectorBatch<'a> {
    values: &'a [f32],
    bytes: &'a [u8],
    row_count: usize,
}

fn validate_vector_batch<'a>(
    batch: &'a RecordBatch,
    index_column: &str,
    dimension: usize,
    expected_row_id: &mut i64,
) -> Result<ValidatedVectorBatch<'a>> {
    let vector_index = batch
        .schema()
        .index_of(index_column)
        .map_err(|e| Error::DataInvalid {
            message: format!("Vector column '{index_column}' not found in read batch: {e}"),
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
    let column = batch.column(vector_index);
    let (values, start, end) = if let Some(array) = column.as_any().downcast_ref::<ListArray>() {
        if array.null_count() != 0 {
            return Err(Error::DataInvalid {
                message: "vindex vector extraction found null vector row".to_string(),
                source: None,
            });
        }
        let offsets = array.value_offsets();
        for offsets in offsets.windows(2) {
            let actual = offsets[1] - offsets[0];
            if actual != dimension as i32 {
                return Err(Error::DataInvalid {
                    message: format!(
                        "vindex vector dimension mismatch: expected {dimension}, got {actual}"
                    ),
                    source: None,
                });
            }
        }
        let start = usize::try_from(offsets[0]).map_err(|e| Error::DataInvalid {
            message: "vindex vector offset is negative".to_string(),
            source: Some(Box::new(e)),
        })?;
        let end = usize::try_from(offsets[offsets.len() - 1]).map_err(|e| Error::DataInvalid {
            message: "vindex vector offset is negative".to_string(),
            source: Some(Box::new(e)),
        })?;
        (array.values(), start, end)
    } else if let Some(array) = column.as_any().downcast_ref::<FixedSizeListArray>() {
        let actual = usize::try_from(array.value_length()).map_err(|e| Error::DataInvalid {
            message: format!(
                "Invalid vindex FixedSizeList dimension: {}",
                array.value_length()
            ),
            source: Some(Box::new(e)),
        })?;
        if actual != dimension {
            return Err(Error::DataInvalid {
                message: format!(
                    "vindex vector dimension mismatch: expected {dimension}, got {actual}"
                ),
                source: None,
            });
        }
        if array.null_count() != 0 {
            return Err(Error::DataInvalid {
                message: "vindex vector extraction found null vector row".to_string(),
                source: None,
            });
        }
        let end = batch
            .num_rows()
            .checked_mul(dimension)
            .ok_or_else(|| Error::DataInvalid {
                message: "vindex batch vector length overflows usize".to_string(),
                source: None,
            })?;
        (array.values(), 0, end)
    } else {
        return Err(Error::DataInvalid {
            message:
                "vindex vector extraction requires Arrow List<Float32> or FixedSizeList<Float32>"
                    .to_string(),
            source: None,
        });
    };
    let values = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| Error::DataInvalid {
            message: "vindex vector extraction requires Float32 vector elements".to_string(),
            source: None,
        })?;
    if values.null_count() != 0
        && values
            .nulls()
            .is_some_and(|nulls| nulls.slice(start, end - start).null_count() != 0)
    {
        return Err(Error::DataInvalid {
            message: "vindex vector extraction found null vector element".to_string(),
            source: None,
        });
    }
    let row_ids = batch
        .column(row_id_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::DataInvalid {
            message: "vindex vector extraction requires non-null Int64 _ROW_ID".to_string(),
            source: None,
        })?;
    if row_ids.null_count() != 0 {
        return Err(Error::DataInvalid {
            message: "vindex vector extraction found null _ROW_ID".to_string(),
            source: None,
        });
    }
    for row_id in row_ids.values() {
        if *row_id != *expected_row_id {
            return Err(Error::DataInvalid {
                message: format!(
                    "vindex vector extraction expected _ROW_ID {}, got {}",
                    expected_row_id, row_id
                ),
                source: None,
            });
        }
        *expected_row_id = expected_row_id
            .checked_add(1)
            .ok_or_else(|| Error::DataInvalid {
                message: "vindex expected row id overflows i64".to_string(),
                source: None,
            })?;
    }

    let byte_start = checked_vector_bytes(start, 1)?;
    let byte_end = checked_vector_bytes(end, 1)?;
    Ok(ValidatedVectorBatch {
        values: &values.values()[start..end],
        bytes: &values.values().inner().as_slice()[byte_start..byte_end],
        row_count: batch.num_rows(),
    })
}

fn checked_vector_bytes(row_count: usize, dimension: usize) -> Result<usize> {
    row_count
        .checked_mul(dimension)
        .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "vindex vector byte length overflows: row_count={row_count}, dimension={dimension}"
            ),
            source: None,
        })
}

fn checked_std_vector_bytes(row_count: usize, dimension: usize) -> std::io::Result<usize> {
    row_count
        .checked_mul(dimension)
        .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "vindex vector byte length overflows usize",
            )
        })
}

fn checked_training_vector_count(row_count: usize, ratio: f64) -> Result<usize> {
    if row_count == 0 || !(ratio > 0.0 && ratio <= 1.0) {
        return Err(Error::DataInvalid {
            message: format!(
                "Invalid vindex training sample: row_count={row_count}, ratio={ratio}; expected a positive row count and ratio in (0, 1]"
            ),
            source: None,
        });
    }
    Ok(((row_count as f64 * ratio).ceil() as usize).clamp(1, row_count))
}

fn checked_training_sample_index(sample: usize, rows: usize, samples: usize) -> Result<usize> {
    sample
        .checked_mul(rows / samples)
        .and_then(|base| {
            sample
                .checked_mul(rows % samples)
                .and_then(|remainder| base.checked_add(remainder / samples))
        })
        .ok_or_else(|| Error::DataInvalid {
            message: "vindex training sample index overflows usize".to_string(),
            source: None,
        })
}

fn checked_i32(value: u64, context: &str) -> Result<i32> {
    i32::try_from(value).map_err(|_| Error::DataInvalid {
        message: format!("{context}: {value}"),
        source: None,
    })
}

fn checked_i64(value: u64, context: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| Error::DataInvalid {
        message: format!("{context}: {value}"),
        source: None,
    })
}

fn checked_row_count(row_range_start: i64, row_range_end: i64) -> Result<i32> {
    if row_range_end < row_range_start {
        return Err(Error::DataInvalid {
            message: format!("Invalid vindex row range [{row_range_start}, {row_range_end}]"),
            source: None,
        });
    }
    row_range_end
        .checked_sub(row_range_start)
        .and_then(|count| count.checked_add(1))
        .and_then(|count| i32::try_from(count).ok())
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "vindex row count is too large for Rust IndexFileMeta: [{row_range_start}, {row_range_end}]"
            ),
            source: None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        ArrayType, FloatType, IndexManifest, IntType, ManifestEntry, Schema, TableSchema,
    };
    use crate::table::TableWrite;
    use crate::vindex::IVF_FLAT_IDENTIFIER;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, Int64Builder, ListBuilder};
    use arrow_array::{ArrayRef, Int32Array};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use chrono::{DateTime, Utc};
    use std::sync::Arc;

    fn data_file(name: &str, first_row_id: Option<i64>, row_count: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 128,
            row_count,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::new(vec![], vec![], vec![]),
            value_stats: BinaryTableStats::new(vec![], vec![], vec![]),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: Some(
                "2024-09-06T07:45:55.039+00:00"
                    .parse::<DateTime<Utc>>()
                    .unwrap(),
            ),
            delete_row_count: None,
            embedded_index: None,
            first_row_id,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
        }
    }

    fn manifest_entry(file: DataFileMeta) -> ManifestEntry {
        ManifestEntry::new(FileKind::Add, vec![], 0, 1, file, 2)
    }

    fn table_options(rows_per_shard: &str) -> HashMap<String, String> {
        HashMap::from([
            ("row-tracking.enabled".to_string(), "true".to_string()),
            ("data-evolution.enabled".to_string(), "true".to_string()),
            ("global-index.enabled".to_string(), "true".to_string()),
            (
                "global-index.row-count-per-shard".to_string(),
                rows_per_shard.to_string(),
            ),
        ])
    }

    fn test_table(options: HashMap<String, String>) -> Table {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            )
            .options(options)
            .build()
            .unwrap();
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "test_table"),
            "memory:/test_vindex_builder".to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    fn plan(entries: Vec<ManifestEntry>, rows_per_shard: i64) -> Result<Vec<VindexIndexShard>> {
        plan_with_indexed(entries, rows_per_shard, &[])
    }

    fn plan_with_indexed(
        entries: Vec<ManifestEntry>,
        rows_per_shard: i64,
        indexed: &[RowRange],
    ) -> Result<Vec<VindexIndexShard>> {
        let table = test_table(table_options(&rows_per_shard.to_string()));
        let core = CoreOptions::new(table.schema().options());
        plan_vindex_shards(
            table.location(),
            table.schema().partition_keys(),
            table.schema().fields(),
            &core,
            1,
            entries,
            rows_per_shard,
            indexed,
        )
    }

    #[test]
    fn test_planner_splits_single_file_across_shards() {
        let shards = plan(vec![manifest_entry(data_file("a", Some(0), 25))], 10).unwrap();

        assert_eq!(
            shards
                .iter()
                .map(|s| (s.row_range_start, s.row_range_end))
                .collect::<Vec<_>>(),
            vec![(0, 9), (10, 19), (20, 24)]
        );
    }

    #[test]
    fn test_planner_rejects_missing_first_row_id() {
        let err = plan(vec![manifest_entry(data_file("a", None, 5))], 10)
            .expect_err("missing first_row_id should fail");
        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("missing first_row_id"))
        );
    }

    #[test]
    fn test_validate_vector_field_accepts_array_float() {
        let field = DataField::new(
            0,
            "embedding".to_string(),
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        );
        assert!(validate_vector_field(&field).is_ok());
    }

    fn vector_batch(rows: Vec<Option<Vec<Option<f32>>>>, row_ids: Vec<Option<i64>>) -> RecordBatch {
        let mut vector_builder = ListBuilder::new(Float32Builder::new());
        for row in rows {
            match row {
                Some(values) => {
                    for value in values {
                        match value {
                            Some(value) => vector_builder.values().append_value(value),
                            None => vector_builder.values().append_null(),
                        }
                    }
                    vector_builder.append(true);
                }
                None => vector_builder.append(false),
            }
        }
        let mut row_id_builder = Int64Builder::new();
        for row_id in row_ids {
            match row_id {
                Some(value) => row_id_builder.append_value(value),
                None => row_id_builder.append_null(),
            }
        }
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "embedding",
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "item",
                    ArrowDataType::Float32,
                    true,
                ))),
                true,
            ),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(vector_builder.finish()) as ArrayRef,
                Arc::new(row_id_builder.finish()) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn extract_vectors_from_batches(
        batches: &[RecordBatch],
        index_column: &str,
        dimension: i32,
        row_range_start: i64,
        expected_row_count: i64,
    ) -> Result<Vec<f32>> {
        let dimension = usize::try_from(dimension).map_err(|e| Error::DataInvalid {
            message: format!("Invalid vindex dimension: {dimension}"),
            source: Some(Box::new(e)),
        })?;
        let mut expected_row_id = row_range_start;
        let mut vectors = Vec::new();
        for batch in batches {
            vectors.extend_from_slice(
                validate_vector_batch(batch, index_column, dimension, &mut expected_row_id)?.values,
            );
        }
        if expected_row_id - row_range_start != expected_row_count {
            return Err(Error::DataInvalid {
                message: format!(
                    "vindex vector extraction expected {expected_row_count} rows, got {}",
                    expected_row_id - row_range_start
                ),
                source: None,
            });
        }
        Ok(vectors)
    }

    #[test]
    fn test_extract_vectors_accepts_list_float32_and_row_ids() {
        let batch = vector_batch(
            vec![
                Some(vec![Some(1.0), Some(2.0)]),
                Some(vec![Some(3.0), Some(4.0)]),
            ],
            vec![Some(10), Some(11)],
        );

        let vectors = extract_vectors_from_batches(&[batch], "embedding", 2, 10, 2).unwrap();

        assert_eq!(vectors, vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn test_extract_vectors_rejects_dimension_mismatch() {
        let batch = vector_batch(vec![Some(vec![Some(1.0)])], vec![Some(0)]);

        let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
            .expect_err("dimension mismatch should fail");

        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("dimension mismatch"))
        );
    }

    #[test]
    fn test_extract_vectors_handles_sliced_list_offsets() {
        let batch = vector_batch(
            vec![
                Some(vec![None, Some(0.0)]),
                Some(vec![Some(1.0), Some(2.0)]),
                Some(vec![Some(3.0), Some(4.0)]),
            ],
            vec![Some(9), Some(10), Some(11)],
        )
        .slice(1, 2);

        let vectors = extract_vectors_from_batches(&[batch], "embedding", 2, 10, 2).unwrap();

        assert_eq!(vectors, vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn test_extract_vectors_handles_sliced_fixed_size_list() {
        let mut vectors = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        for row in [[0.0, 0.0], [1.0, 2.0], [3.0, 4.0]] {
            vectors.values().append_slice(&row);
            vectors.append(true);
        }
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "embedding",
                ArrowDataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", ArrowDataType::Float32, true)),
                    2,
                ),
                true,
            ),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(vectors.finish()) as ArrayRef,
                Arc::new(Int64Array::from(vec![9, 10, 11])) as ArrayRef,
            ],
        )
        .unwrap()
        .slice(1, 2);

        let vectors = extract_vectors_from_batches(&[batch], "embedding", 2, 10, 2).unwrap();

        assert_eq!(vectors, vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn test_training_sample_count_and_indexes_match_java() {
        assert_eq!(checked_training_vector_count(10, 0.01).unwrap(), 1);
        assert_eq!(checked_training_vector_count(10, 0.25).unwrap(), 3);
        assert_eq!(checked_training_vector_count(10, 1.0).unwrap(), 10);
        assert_eq!(checked_training_vector_count(3, 0.9).unwrap(), 3);
        assert_eq!(
            (0..4)
                .map(|sample| checked_training_sample_index(sample, 10, 4).unwrap())
                .collect::<Vec<_>>(),
            vec![0, 2, 5, 7]
        );
        assert!(checked_vector_bytes(usize::MAX, 2).is_err());
        assert!(checked_training_sample_index(usize::MAX, usize::MAX, 1).is_err());
    }

    fn test_table_with_io(file_io: FileIO, table_path: &str, schema: Schema) -> Table {
        Table::new(
            file_io,
            Identifier::new("default", "test_table"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    fn vindex_schema_builder(options: HashMap<String, String>) -> crate::spec::SchemaBuilder {
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            )
            .options(options)
    }

    fn vindex_e2e_options(rows_per_shard: &str) -> HashMap<String, String> {
        let mut options = table_options(rows_per_shard);
        // A small, valid IVF config so the (optional) native build can run; the
        // no-op/incremental fix is exercised before or independently of it.
        options.insert("ivf-flat.dimension".to_string(), "2".to_string());
        options.insert("ivf-flat.nlist".to_string(), "2".to_string());
        options
    }

    fn vindex_e2e_table(table_path: &str, rows_per_shard: &str) -> Table {
        test_table_with_io(
            FileIOBuilder::new("memory").build().unwrap(),
            table_path,
            vindex_schema_builder(vindex_e2e_options(rows_per_shard))
                .build()
                .unwrap(),
        )
    }

    async fn setup_dirs(file_io: &FileIO, table_path: &str) {
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();
    }

    fn build_vector_batch(ids: Vec<i32>, vectors: Vec<Vec<f32>>) -> RecordBatch {
        let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
        let mut vector_builder =
            ListBuilder::new(Float32Builder::new()).with_field(element_field.clone());
        for vector in vectors {
            for value in vector {
                vector_builder.values().append_value(value);
            }
            vector_builder.append(true);
        }
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("embedding", ArrowDataType::List(element_field), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(vector_builder.finish()) as ArrayRef,
            ],
        )
        .unwrap()
    }

    async fn write_vectors(table: &Table, ids: Vec<i32>, vectors: Vec<Vec<f32>>) {
        let mut table_write = TableWrite::new(table, "test-user".to_string()).unwrap();
        table_write
            .write_arrow_batch(&build_vector_batch(ids, vectors))
            .await
            .unwrap();
        let messages = table_write.prepare_commit().await.unwrap();
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(messages)
            .await
            .unwrap();
    }

    /// Commit a synthetic vindex `IndexFileMeta` covering `[start, end]` for
    /// `field_id` directly into the index manifest, without invoking the native
    /// builder. Mirrors the Lumina/btree tests so the incremental gap logic can
    /// be exercised without a trained vector index. Writes the same `index_type`
    /// (`ivf-flat`) the builder-under-test uses, so the gap helper matches it.
    async fn commit_synthetic_vindex_index(table: &Table, field_id: i32, start: i64, end: i64) {
        let synthetic = IndexFileMeta {
            index_type: IVF_FLAT_IDENTIFIER.to_string(),
            file_name: format!("vector-ivf-flat-synthetic-{start}-{end}.index"),
            file_size: 1,
            row_count: (end - start + 1) as i32,
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: start,
                row_range_end: end,
                index_field_id: field_id,
                extra_field_ids: None,
                source_meta: None,
                index_meta: None,
            }),
        };
        let mut message = CommitMessage::new(BinaryRow::new(0).to_serialized_bytes(), 0, vec![]);
        message.new_index_files = vec![synthetic];
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(vec![message])
            .await
            .unwrap();
    }

    async fn latest_vindex_index_files(table: &Table) -> Vec<IndexFileMeta> {
        let snapshot_manager =
            SnapshotManager::new(table.file_io().clone(), table.location().to_string());
        let snapshot = snapshot_manager
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        let Some(index_manifest_name) = snapshot.index_manifest() else {
            return Vec::new();
        };
        IndexManifest::read(
            table.file_io(),
            &snapshot_manager.manifest_path(index_manifest_name),
        )
        .await
        .unwrap()
        .into_iter()
        .filter(|entry| {
            entry.kind == FileKind::Add && entry.index_file.index_type == IVF_FLAT_IDENTIFIER
        })
        .map(|entry| entry.index_file)
        .collect()
    }

    /// Row-id coverage of the committed data files, read back from the data
    /// manifest (never hard-coded) and merged into contiguous ranges. Mirrors
    /// how `execute` gathers `manifest_entries`.
    async fn data_row_id_coverage(table: &Table) -> Vec<RowRange> {
        let snapshot_manager =
            SnapshotManager::new(table.file_io().clone(), table.location().to_string());
        let snapshot = snapshot_manager
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        let entries = table
            .new_read_builder()
            .new_scan()
            .with_scan_all_files()
            .plan_manifest_entries(&snapshot)
            .await
            .unwrap();
        let ranges = entries
            .iter()
            .filter(|entry| *entry.kind() == FileKind::Add)
            .filter_map(|entry| {
                entry
                    .file()
                    .row_id_range()
                    .map(|(start, end)| RowRange::new(start, end))
            })
            .collect::<Vec<_>>();
        crate::table::merge_row_ranges(ranges)
    }

    /// Second build with the whole coverage already indexed must be a clean
    /// no-op (returns 0), not an overlap error. Reaches `Ok(0)` before the
    /// native build, so it runs in CI without a trained index. This is the core
    /// bug fix: today the second call errors with the overlap message.
    #[tokio::test]
    async fn vindex_second_build_without_new_data_is_noop() {
        let table_path = "memory:/test_vindex_second_build_noop";
        let table = vindex_e2e_table(table_path, "10");
        setup_dirs(table.file_io(), table_path).await;

        write_vectors(&table, vec![1, 2], vec![vec![1.0, 0.0], vec![0.0, 1.0]]).await;
        write_vectors(&table, vec![3], vec![vec![1.0, 1.0]]).await;

        // Fully index the coverage via a synthetic manifest entry.
        let coverage = data_row_id_coverage(&table).await;
        assert_eq!(coverage.len(), 1, "data must be one contiguous range");
        let field_id = find_index_field(&table, "embedding").unwrap().id();
        commit_synthetic_vindex_index(&table, field_id, coverage[0].from(), coverage[0].to()).await;

        let names_before = latest_vindex_index_files(&table)
            .await
            .iter()
            .map(|f| f.file_name.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(!names_before.is_empty());

        let built = table
            .new_vindex_index_build_builder(IVF_FLAT_IDENTIFIER)
            .with_index_column("embedding")
            .execute()
            .await
            .unwrap();
        assert_eq!(built, 0, "fully-indexed table must build nothing on re-run");

        let names_after = latest_vindex_index_files(&table)
            .await
            .iter()
            .map(|f| f.file_name.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            names_before, names_after,
            "re-run must not add or remove index manifest entries"
        );
    }

    /// Real end-to-end incremental build. `paimon-vindex-core` is pure Rust and
    /// trains/serializes an IVF-flat index in CI without a native lib, so this
    /// asserts SUCCESS end-to-end (mirroring btree's incremental test): build #1
    /// indexes the initial rows, an appended batch is indexed by build #2, every
    /// new index file's row range lies entirely in the appended gap `[n, ..]`
    /// (`n` derived from the manifest, never hard-coded), and build-#1's index
    /// files are retained untouched (append-only). No overlap error, no tolerated
    /// native-build failure -- the build must actually succeed.
    #[tokio::test]
    async fn vindex_incremental_build_indexes_only_new_rows() {
        let table_path = "memory:/test_vindex_incremental";
        let table = vindex_e2e_table(table_path, "10");
        setup_dirs(table.file_io(), table_path).await;

        // Build #1 over the initial batch via a real end-to-end build.
        write_vectors(
            &table,
            vec![1, 2, 3],
            vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![1.0, 1.0]],
        )
        .await;
        let first_built = table
            .new_vindex_index_build_builder(IVF_FLAT_IDENTIFIER)
            .with_index_column("embedding")
            .with_options(HashMap::from([(
                "ivf-flat.train.sample-ratio".to_string(),
                "0.9".to_string(),
            )]))
            .execute()
            .await
            .unwrap();
        assert!(first_built > 0, "first build must index the initial rows");

        // First appended row-id, derived from the data manifest (never hard-coded).
        let indexed_coverage = data_row_id_coverage(&table).await;
        assert_eq!(indexed_coverage.len(), 1);
        let n = indexed_coverage[0].to() + 1;

        let first_names = latest_vindex_index_files(&table)
            .await
            .iter()
            .map(|f| f.file_name.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(first_names.len(), 1, "one shard must write one index file");

        // Append a second batch (new row-ids [n..]).
        write_vectors(
            &table,
            vec![4, 5, 6],
            vec![vec![2.0, 0.0], vec![0.0, 2.0], vec![2.0, 2.0]],
        )
        .await;

        // End-to-end: build #2 must SUCCEED and index the appended rows.
        let second_built = table
            .new_vindex_index_build_builder(IVF_FLAT_IDENTIFIER)
            .with_index_column("embedding")
            .execute()
            .await
            .unwrap();
        assert!(second_built > 0, "appended rows must be indexed");

        let all_files = latest_vindex_index_files(&table).await;
        let all_names = all_files
            .iter()
            .map(|f| f.file_name.clone())
            .collect::<std::collections::BTreeSet<_>>();

        // Every build-#1 file is still present (append-only, no rewrite/delete).
        assert!(
            first_names.iter().all(|name| all_names.contains(name)),
            "build #1 index files must be retained untouched"
        );

        // Every build-#2 file covers only the appended gap [n, ..], never the
        // already-indexed prefix.
        let new_files = all_files
            .iter()
            .filter(|f| !first_names.contains(&f.file_name))
            .collect::<Vec<_>>();
        assert!(!new_files.is_empty(), "build #2 must add new index files");
        for file in new_files {
            let meta = file
                .global_index_meta
                .as_ref()
                .expect("global index meta on new vindex file");
            assert!(
                meta.row_range_start >= n,
                "new index file range must start at or after {n}, got [{}, {}]",
                meta.row_range_start,
                meta.row_range_end
            );
        }
    }

    #[tokio::test]
    async fn vindex_build_cleans_written_shards_when_later_shard_fails() {
        let table_path = "memory:/test_vindex_abort_written_shard";
        let table = vindex_e2e_table(table_path, "2");
        setup_dirs(table.file_io(), table_path).await;
        write_vectors(
            &table,
            vec![1, 2, 3],
            vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![1.0]],
        )
        .await;

        let error = table
            .new_vindex_index_build_builder(IVF_FLAT_IDENTIFIER)
            .with_index_column("embedding")
            .execute()
            .await
            .expect_err("the second shard has an invalid vector dimension");

        assert!(error.to_string().contains("dimension mismatch"));
        assert!(table
            .file_io()
            .list_status(&format!("{table_path}/{INDEX_DIR}/"))
            .await
            .unwrap()
            .is_empty());
    }

    /// A field that already carries a DIFFERENT index type (`lumina`) over an
    /// overlapping row range must not block a vindex (`ivf-flat`) build on the
    /// same field: the two indexes have distinct identities and coexist. Before
    /// the full-identity fix, the overlap guard keyed only on field id + range
    /// and spuriously rejected this build with the "overlaps requested row
    /// range" error.
    #[tokio::test]
    async fn vindex_build_coexists_with_different_index_type_on_same_field() {
        let table_path = "memory:/test_vindex_coexist_diff_type";
        let table = vindex_e2e_table(table_path, "10");
        setup_dirs(table.file_io(), table_path).await;

        write_vectors(
            &table,
            vec![1, 2, 3],
            vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![1.0, 1.0]],
        )
        .await;

        // Pre-existing `lumina` index covering the full data range on the SAME
        // field the vindex build will target.
        let coverage = data_row_id_coverage(&table).await;
        assert_eq!(coverage.len(), 1, "data must be one contiguous range");
        let field_id = find_index_field(&table, "embedding").unwrap().id();
        let lumina = IndexFileMeta {
            index_type: "lumina".to_string(),
            file_name: "lumina-synthetic-0.index".to_string(),
            file_size: 1,
            row_count: (coverage[0].to() - coverage[0].from() + 1) as i32,
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: coverage[0].from(),
                row_range_end: coverage[0].to(),
                index_field_id: field_id,
                extra_field_ids: None,
                source_meta: None,
                index_meta: None,
            }),
        };
        let mut message = CommitMessage::new(BinaryRow::new(0).to_serialized_bytes(), 0, vec![]);
        message.new_index_files = vec![lumina];
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(vec![message])
            .await
            .unwrap();

        // Building `ivf-flat` on the same field must NOT trip the overlap guard.
        // A native-build failure over the tiny synthetic dataset is tolerated;
        // only the overlap error is forbidden.
        let result = table
            .new_vindex_index_build_builder(IVF_FLAT_IDENTIFIER)
            .with_index_column("embedding")
            .execute()
            .await;
        match result {
            Ok(_) => {}
            Err(Error::DataInvalid { message, .. }) => {
                assert!(
                    !message.contains("overlaps requested row range"),
                    "vindex build must coexist with a different-type index on the same field; got: {message}"
                );
            }
            Err(other) => panic!("unexpected error from vindex build: {other:?}"),
        }
    }

    /// Regression: a first build (no existing index) must equal the pre-change
    /// full build -- subtracting an empty `indexed` yields full coverage.
    #[test]
    fn vindex_first_build_indexes_full_coverage() {
        let full = plan(vec![manifest_entry(data_file("a", Some(0), 25))], 10).unwrap();
        let gapped =
            plan_with_indexed(vec![manifest_entry(data_file("a", Some(0), 25))], 10, &[]).unwrap();
        // Empty `indexed` must not alter the shard layout.
        assert_eq!(
            full.iter()
                .map(|s| (s.row_range_start, s.row_range_end))
                .collect::<Vec<_>>(),
            gapped
                .iter()
                .map(|s| (s.row_range_start, s.row_range_end))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            full.iter()
                .map(|s| (s.row_range_start, s.row_range_end))
                .collect::<Vec<_>>(),
            vec![(0, 9), (10, 19), (20, 24)],
            "first build must cover the full row range across shards"
        );
    }

    /// Planner-level mid-coverage hole, mirroring btree/lumina: with a single
    /// shard cell (rows_per_shard large enough to hold all data) the grid never
    /// splits, so the only split is the indexed hole itself. An indexed range
    /// strictly inside the data coverage must carve the build into exactly the
    /// two contiguous segments on either side of the hole -- both bounds pinned,
    /// and neither segment may span or touch the hole.
    #[test]
    fn vindex_plan_splits_gap_around_mid_coverage_indexed_hole() {
        // Data row-ids [0, 9]; one shard cell [0, 99] so the grid never splits.
        let n = 9;
        let hole_start = 4;
        let hole_end = 6;
        let shards = plan_with_indexed(
            vec![manifest_entry(data_file("a", Some(0), n + 1))],
            100,
            &[RowRange::new(hole_start, hole_end)],
        )
        .unwrap();

        let ranges = shards
            .iter()
            .map(|s| (s.row_range_start, s.row_range_end))
            .collect::<Vec<_>>();
        // Exactly the two contiguous segments around the hole.
        assert_eq!(
            ranges,
            vec![(0, hole_start - 1), (hole_end + 1, n)],
            "mid-coverage hole must split into exactly the two segments around it"
        );
        // Every emitted range is contiguous and none spans or touches the hole.
        for (start, end) in &ranges {
            assert!(end >= start, "range must be non-empty: [{start}, {end}]");
            assert!(
                *end < hole_start || *start > hole_end,
                "shard [{start}, {end}] must not overlap indexed hole [{hole_start}, {hole_end}]"
            );
        }
        // Together the shards cover exactly coverage - indexed.
        let expected = exclude_row_ranges(
            &[RowRange::new(0, n)],
            &[RowRange::new(hole_start, hole_end)],
        )
        .into_iter()
        .map(|r| (r.from(), r.to()))
        .collect::<Vec<_>>();
        assert_eq!(
            ranges, expected,
            "shards must cover exactly coverage minus the indexed hole"
        );
    }

    /// Planner-level incremental prefix. Strengthens
    /// `vindex_incremental_build_indexes_only_new_rows`, which asserts only a
    /// one-sided lower bound (`row_range_start >= n`): an indexed prefix [0, k]
    /// must leave EXACTLY the suffix [k+1, N] on both bounds, split along the
    /// shard grid, with nothing re-indexed inside the prefix.
    #[test]
    fn vindex_plan_incremental_prefix_leaves_suffix() {
        // Data row-ids [0, 24], rows_per_shard = 10 -> cells [0,9],[10,19],[20,29].
        // Indexed prefix [0, 9] fully fills the first cell, so the build must be
        // exactly [10, 19] and [20, 24] (the suffix split along the grid).
        let n = 24;
        let k = 9; // prefix [0, k] == the first full shard cell
        let shards = plan_with_indexed(
            vec![manifest_entry(data_file("a", Some(0), n + 1))],
            10,
            &[RowRange::new(0, k)],
        )
        .unwrap();

        let ranges = shards
            .iter()
            .map(|s| (s.row_range_start, s.row_range_end))
            .collect::<Vec<_>>();
        assert_eq!(
            ranges,
            vec![(k + 1, 19), (20, n)],
            "indexed prefix must leave exactly the suffix, split along the shard grid"
        );
        // Both bounds pinned (this is what the one-sided existing check omits).
        assert_eq!(ranges.first().unwrap().0, k + 1, "suffix must start at k+1");
        assert_eq!(ranges.last().unwrap().1, n, "suffix must end at N");
        // Contiguous, and no shard reaches back into the indexed prefix.
        for pair in ranges.windows(2) {
            assert_eq!(
                pair[1].0,
                pair[0].1 + 1,
                "ranges must be contiguous: {:?} then {:?}",
                pair[0],
                pair[1]
            );
        }
        for (start, end) in &ranges {
            assert!(
                *start > k,
                "shard [{start}, {end}] must not re-index the prefix [0, {k}]"
            );
        }
    }
}
