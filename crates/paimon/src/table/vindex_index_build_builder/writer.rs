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

use super::extraction::{data_split_for_shard, local_ids, validate_vector_batch};
use super::pipeline::GranuleBuildOutcome;
use super::planning::VindexIndexShard;
use super::timing::{vector_index_build_timing_enabled, VectorIndexBuildTiming};
use super::validation::{
    checked_i64, checked_row_count, checked_std_vector_bytes, checked_training_sample_index,
    checked_training_vector_count, checked_vector_bytes,
};
use super::VindexIndexBuildBuilder;
use crate::spec::{GlobalIndexMeta, IndexFileMeta, ROW_ID_FIELD_NAME};
use crate::table::data_file_reader::DataFileReadTiming;
use crate::table::table_read::configured_parquet_read_budget;
use crate::vindex::{VindexVectorIndexOptions, DISKANN_IDENTIFIER};
use crate::{Error, Result};
use arrow_buffer::MutableBuffer;
use futures::TryStreamExt;
use paimon_vindex_core::autotune::default_training_vector_count;
use paimon_vindex_core::index::{VectorIndexConfig, VectorIndexTrainer, VectorIndexWriter};
use paimon_vindex_core::io::PosWriter;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio_util::io::SyncIoBridge;

const VECTOR_BUFFER_BYTES: usize = 8 * 1024 * 1024;

pub(super) struct BuiltIndexFile {
    pub(super) meta: IndexFileMeta,
    pub(super) timing: Option<VectorIndexBuildTiming>,
}

impl<'a> VindexIndexBuildBuilder<'a> {
    pub(super) async fn build_index_file(
        &self,
        shard: &VindexIndexShard,
        index_column: &str,
        dimension: i32,
        index_field_id: i32,
        options: &VindexVectorIndexOptions,
        index_meta: Vec<u8>,
    ) -> Result<Option<BuiltIndexFile>> {
        let rows = usize::try_from(checked_row_count(
            shard.row_range_start,
            shard.row_range_end,
        )?)
        .map_err(|error| Error::DataInvalid {
            message: "vindex row count does not fit usize".into(),
            source: Some(Box::new(error)),
        })?;
        let use_granule = self.index_type != DISKANN_IDENTIFIER
            && options.granule_build_enabled
            && !options.needs_vector_count()
            // Invalid training options still produce no file for an all-NULL
            // shard, like Java. Full spill determines whether training is needed.
            && options.training_config(rows).is_ok();
        // Auto IVF sizing needs the complete non-null cardinality, like Java.
        // Explicit nlist/expected-count builds retain the granule fast path.
        log::info!(
            "vindex build strategy: index_type={}, strategy={}",
            self.index_type,
            if use_granule { "granule" } else { "full-spill" }
        );
        if use_granule {
            match self
                .build_index_file_granule(
                    shard,
                    index_column,
                    dimension,
                    index_field_id,
                    options,
                    index_meta.clone(),
                )
                .await?
            {
                GranuleBuildOutcome::Built(built) => return Ok(Some(*built)),
                GranuleBuildOutcome::Sparse => {
                    // Only private in-memory/spilled preparation has happened. The
                    // pinned snapshot is reread with sparse row IDs and valid-only training.
                    log::info!("vindex granule source contains NULL vectors; use sparse full-spill preparation");
                }
            }
        }
        self.build_full_spill_index_file(
            shard,
            index_column,
            dimension,
            index_field_id,
            options,
            index_meta,
        )
        .await
    }

    async fn build_full_spill_index_file(
        &self,
        shard: &VindexIndexShard,
        index_column: &str,
        dimension: i32,
        index_field_id: i32,
        options: &VindexVectorIndexOptions,
        index_meta: Vec<u8>,
    ) -> Result<Option<BuiltIndexFile>> {
        let timing_enabled = vector_index_build_timing_enabled();
        let total_start = timing_enabled.then(Instant::now);
        let mut source_batch_wait = Duration::ZERO;
        let mut raw_temp_write = Duration::ZERO;
        let read_timing = timing_enabled.then(|| Arc::new(DataFileReadTiming::default()));
        let parquet_read_budget = if timing_enabled {
            let budget = configured_parquet_read_budget(self.table)?;
            budget.enable_diagnostics();
            Some(budget)
        } else {
            None
        };
        let mut batch_count = 0usize;
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
        checked_vector_bytes(row_count_usize, dimension_usize)?;
        let mut raw_file = temporary_vector_file("vectors")?;
        let mut id_file = temporary_vector_file("row IDs")?;
        let split = data_split_for_shard(shard)?;
        let mut read_builder = self.table.new_read_builder();
        read_builder.with_projection(&[index_column, ROW_ID_FIELD_NAME])?;
        let read = read_builder.new_read()?;
        let read = match read_timing.as_ref() {
            Some(timing) => read.with_data_file_read_timing(Arc::clone(timing)),
            None => read,
        };
        let read = match parquet_read_budget.as_ref() {
            Some(budget) => read.with_parquet_read_budget(Arc::clone(budget)),
            None => read,
        };
        let mut batches = read.to_arrow(&[split])?;
        let mut expected_row_id = shard.row_range_start;
        let mut rows_seen = 0usize;
        let mut vector_count = 0usize;
        let mut bytes_written = 0usize;
        loop {
            let source_start = timing_enabled.then(Instant::now);
            let batch = batches.try_next().await?;
            if let Some(start) = source_start {
                source_batch_wait = source_batch_wait.saturating_add(start.elapsed());
            }
            let Some(batch) = batch else { break };
            batch_count += 1;
            let vectors =
                validate_vector_batch(&batch, index_column, dimension_usize, &mut expected_row_id)?;
            rows_seen =
                rows_seen
                    .checked_add(vectors.source_rows)
                    .ok_or_else(|| Error::DataInvalid {
                        message: "vindex streamed row count overflows usize".into(),
                        source: None,
                    })?;
            vector_count = vector_count
                .checked_add(vectors.vector_count)
                .ok_or_else(|| Error::DataInvalid {
                    message: "vindex streamed vector count overflows usize".into(),
                    source: None,
                })?;
            let ids: arrow_buffer::ScalarBuffer<i64> =
                local_ids(&vectors.row_ids, shard.row_range_start, row_count_usize)?.into();
            let raw_write_start = timing_enabled.then(Instant::now);
            raw_file
                .write_all(vectors.bytes())
                .await
                .map_err(spill_error)?;
            id_file
                .write_all(ids.inner().as_slice())
                .await
                .map_err(spill_error)?;
            if let Some(start) = raw_write_start {
                raw_temp_write = raw_temp_write.saturating_add(start.elapsed());
            }
            bytes_written = bytes_written
                .checked_add(vectors.bytes().len())
                .ok_or_else(|| Error::DataInvalid {
                    message: "vindex spilled byte count overflows usize".into(),
                    source: None,
                })?;
        }
        let expected_end =
            shard
                .row_range_end
                .checked_add(1)
                .ok_or_else(|| Error::DataInvalid {
                    message: "vindex row range end overflows i64".into(),
                    source: None,
                })?;
        if rows_seen != row_count_usize
            || expected_row_id != expected_end
            || bytes_written != checked_vector_bytes(vector_count, dimension_usize)?
        {
            return Err(Error::DataInvalid {
                message: format!("vindex streamed data mismatch: rows={rows_seen}/{row_count_usize}, vectors={vector_count}, bytes={bytes_written}"), source: None,
            });
        }
        // Like Java NativeVectorGlobalIndexWriter.finish: no vectors means no file.
        if vector_count == 0 {
            return Ok(None);
        }
        raw_file.flush().await.map_err(spill_error)?;
        id_file.flush().await.map_err(spill_error)?;
        if raw_file.metadata().await.map_err(spill_error)?.len() != bytes_written as u64
            || id_file.metadata().await.map_err(spill_error)?.len()
                != checked_vector_bytes(vector_count, 2)? as u64
        {
            return Err(Error::DataInvalid {
                message: "temporary vindex vector or row ID file size mismatch".into(),
                source: None,
            });
        }
        let raw_file = raw_file.into_std().await;
        let id_file = id_file.into_std().await;
        let training_vector_count =
            checked_training_vector_count(vector_count, options.train_sample_ratio)?;
        let config = options.training_config(vector_count)?;
        let training_rows_retained = if timing_enabled {
            default_training_vector_count(training_vector_count, config.nlist()).unwrap_or(0)
        } else {
            0
        };
        let ratio = options.train_sample_ratio;
        let (writer, train_finish, raw_temp_reread, index_add) =
            tokio::task::spawn_blocking(move || {
                train_and_add_spilled_vectors(
                    raw_file,
                    id_file,
                    config,
                    vector_count,
                    dimension_usize,
                    ratio,
                    timing_enabled,
                )
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

        let serialize_upload_start = timing_enabled.then(Instant::now);
        let meta = self
            .finish_index_file(writer, shard, index_field_id, index_meta, row_count)
            .await?;
        let serialize_upload =
            serialize_upload_start.map_or(Duration::ZERO, |start| start.elapsed());
        let (oss_read, parquet_decode) = read_timing
            .as_ref()
            .map_or((Duration::ZERO, Duration::ZERO), |timing| {
                (timing.file_read(), timing.parquet_decode())
            });
        let (file_schema_open, first_batch_wait, remaining_batch_wait) = read_timing
            .as_ref()
            .map_or((Duration::ZERO, Duration::ZERO, Duration::ZERO), |timing| {
                timing.file_waits()
            });
        let parquet_diagnostics = parquet_read_budget
            .as_ref()
            .map_or_else(Default::default, |budget| budget.diagnostics());
        let timing = total_start.map(|start| VectorIndexBuildTiming {
            total_without_commit: start.elapsed(),
            source_batch_wait,
            oss_read,
            parquet_decode,
            file_schema_open,
            first_batch_wait,
            remaining_batch_wait,
            parquet_row_group_count: parquet_diagnostics.row_group_count,
            parquet_projected_bytes_min: parquet_diagnostics.projected_bytes_min,
            parquet_projected_bytes_max: parquet_diagnostics.projected_bytes_max,
            parquet_projected_bytes_total: parquet_diagnostics.projected_bytes_total,
            parquet_peak_inflight_row_groups: parquet_diagnostics.peak_inflight,
            raw_temp_write,
            granule_spill_write: Duration::ZERO,
            train_finish,
            raw_temp_reread,
            granule_spill_read: Duration::ZERO,
            index_add,
            serialize_upload,
            rows: row_count_usize,
            training_rows_seen: training_vector_count,
            training_rows_retained,
            batch_count,
            raw_temp_bytes: bytes_written,
            granule_spill_bytes: 0,
            index_bytes: meta.file_size as u64,
            data_file_count: shard.files.len(),
            file_name: meta.file_name.clone(),
        });
        Ok(Some(BuiltIndexFile { meta, timing }))
    }

    pub(super) async fn finish_index_file(
        &self,
        writer: VectorIndexWriter,
        shard: &VindexIndexShard,
        index_field_id: i32,
        index_meta: Vec<u8>,
        row_count: i64,
    ) -> Result<IndexFileMeta> {
        let file_name = format!(
            "vector-{}-global-index-{}.index",
            self.index_type,
            uuid::Uuid::new_v4()
        );
        let (index_path, external_path) =
            crate::table::global_index_build_common::prepare_index_file_path(
                self.table, &file_name,
            )
            .await?;
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
            external_path,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: shard.row_range_start,
                row_range_end: shard.row_range_end,
                index_field_id,
                extra_field_ids: None,
                source_meta: Some(
                    crate::spec::DataEvolutionIndexSourceMeta::new(shard.snapshot_id)?.serialize(),
                ),
                index_meta: Some(index_meta),
            }),
        })
    }
}

fn spill_error(error: std::io::Error) -> Error {
    Error::UnexpectedError {
        message: format!("Failed to spill vindex vectors: {error}"),
        source: Some(Box::new(error)),
    }
}

fn temporary_vector_file(kind: &str) -> Result<tokio::fs::File> {
    tempfile::tempfile()
        .map(tokio::fs::File::from_std)
        .map_err(|error| Error::UnexpectedError {
            message: format!("Failed to create temporary vindex {kind} file: {error}"),
            source: Some(Box::new(error)),
        })
}

/// Java samples the compacted non-null vector stream, then adds the original relative IDs.
fn train_and_add_spilled_vectors(
    mut raw_file: std::fs::File,
    mut id_file: std::fs::File,
    config: VectorIndexConfig,
    vector_count: usize,
    dimension: usize,
    ratio: f64,
    timing_enabled: bool,
) -> std::io::Result<(VectorIndexWriter, Duration, Duration, Duration)> {
    let train_start = timing_enabled.then(Instant::now);
    let samples =
        checked_training_vector_count(vector_count, ratio).map_err(std::io::Error::other)?;
    let batch_rows = (VECTOR_BUFFER_BYTES / checked_std_vector_bytes(1, dimension)?)
        .max(1)
        .min(vector_count);
    let mut buffer = MutableBuffer::new(checked_std_vector_bytes(batch_rows, dimension)?);
    let mut sample_buffer = Vec::with_capacity(batch_rows * dimension);
    let mut trainer = VectorIndexTrainer::new(config)?;
    raw_file.seek(SeekFrom::Start(0))?;
    let mut rows_seen = 0;
    let mut selected = 0;
    while rows_seen < vector_count {
        let rows = batch_rows.min(vector_count - rows_seen);
        buffer.resize(checked_std_vector_bytes(rows, dimension)?, 0);
        raw_file.read_exact(buffer.as_slice_mut())?;
        let values = buffer.typed_data::<f32>();
        while selected < samples {
            let sample = checked_training_sample_index(selected, vector_count, samples)
                .map_err(std::io::Error::other)?;
            if sample >= rows_seen + rows {
                break;
            }
            let offset = (sample - rows_seen) * dimension;
            sample_buffer.extend_from_slice(&values[offset..offset + dimension]);
            selected += 1;
        }
        if !sample_buffer.is_empty() {
            trainer.add_training_vectors_mut(&sample_buffer, sample_buffer.len() / dimension)?;
            sample_buffer.clear();
        }
        rows_seen += rows;
    }
    if selected != samples {
        return Err(std::io::Error::other(
            "vindex training sample count mismatch",
        ));
    }
    let training = trainer.finish()?;
    let train_finish = train_start.map_or(Duration::ZERO, |start| start.elapsed());
    let mut writer = VectorIndexWriter::new(training);
    let mut raw_temp_reread = Duration::ZERO;
    let mut index_add = Duration::ZERO;
    raw_file.seek(SeekFrom::Start(0))?;
    id_file.seek(SeekFrom::Start(0))?;
    let mut ids = MutableBuffer::new(batch_rows * std::mem::size_of::<i64>());
    let mut rows_added = 0;
    while rows_added < vector_count {
        let rows = batch_rows.min(vector_count - rows_added);
        buffer.resize(checked_std_vector_bytes(rows, dimension)?, 0);
        ids.resize(rows * std::mem::size_of::<i64>(), 0);
        let read_start = timing_enabled.then(Instant::now);
        raw_file.read_exact(buffer.as_slice_mut())?;
        id_file.read_exact(ids.as_slice_mut())?;
        if let Some(start) = read_start {
            raw_temp_reread = raw_temp_reread.saturating_add(start.elapsed());
        }
        let add_start = timing_enabled.then(Instant::now);
        writer.add_vectors(ids.typed_data::<i64>(), buffer.typed_data::<f32>(), rows)?;
        if let Some(start) = add_start {
            index_add = index_add.saturating_add(start.elapsed());
        }
        rows_added += rows;
    }
    let mut trailing = [0u8; 1];
    if raw_file.read(&mut trailing)? != 0 || id_file.read(&mut trailing)? != 0 {
        return Err(std::io::Error::other(
            "temporary vindex vector or row ID file contains trailing bytes",
        ));
    }
    Ok((writer, train_finish, raw_temp_reread, index_add))
}
