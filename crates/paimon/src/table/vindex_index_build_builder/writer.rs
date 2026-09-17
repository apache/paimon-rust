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

use super::extraction::{data_split_for_shard, validate_vector_batch};
use super::planning::VindexIndexShard;
use super::timing::VectorIndexBuildTiming;
use super::validation::{
    checked_i64, checked_row_count, checked_std_vector_bytes, checked_training_sample_index,
    checked_training_vector_count, checked_vector_bytes,
};
use super::VindexIndexBuildBuilder;
use crate::spec::{GlobalIndexMeta, IndexFileMeta, ROW_ID_FIELD_NAME};
use crate::vindex::{VindexVectorIndexOptions, DISKANN_IDENTIFIER};
use crate::{Error, Result};
use arrow_buffer::MutableBuffer;
use futures::TryStreamExt;
use paimon_vindex_core::index::{VectorIndexTrainer, VectorIndexWriter};
use paimon_vindex_core::io::PosWriter;
use std::io::{Read, Seek, SeekFrom};
use tokio::io::AsyncWriteExt;
use tokio_util::io::SyncIoBridge;

const INDEX_DIR: &str = "index";
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
    ) -> Result<BuiltIndexFile> {
        if self.index_type != DISKANN_IDENTIFIER {
            return self
                .build_index_file_granule(
                    shard,
                    index_column,
                    dimension,
                    index_field_id,
                    options,
                    index_meta,
                )
                .await;
        }
        self.build_diskann_index_file(
            shard,
            index_column,
            dimension,
            index_field_id,
            options,
            index_meta,
        )
        .await
    }

    async fn build_diskann_index_file(
        &self,
        shard: &VindexIndexShard,
        index_column: &str,
        dimension: i32,
        index_field_id: i32,
        options: &VindexVectorIndexOptions,
        index_meta: Vec<u8>,
    ) -> Result<BuiltIndexFile> {
        let row_count = checked_row_count(shard.row_range_start, shard.row_range_end)?;
        let row_count_usize = usize::try_from(row_count).map_err(|e| Error::DataInvalid {
            message: format!("Invalid vindex row count: {row_count}"),
            source: Some(Box::new(e)),
        })?;
        let dimension = usize::try_from(dimension).map_err(|e| Error::DataInvalid {
            message: format!("Invalid vindex dimension: {dimension}"),
            source: Some(Box::new(e)),
        })?;
        if dimension == 0 {
            return Err(Error::DataInvalid {
                message: "vindex vector dimension must be positive".to_string(),
                source: None,
            });
        }
        let expected_bytes = checked_vector_bytes(row_count_usize, dimension)?;
        let training_vector_count =
            checked_training_vector_count(row_count_usize, options.train_sample_ratio)?;
        let buffer_rows = (VECTOR_BUFFER_BYTES / checked_vector_bytes(1, dimension)?).max(1);
        let buffer_floats =
            buffer_rows
                .checked_mul(dimension)
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
        let mut batches = read_builder.new_read()?.to_arrow(&[split])?;
        let mut expected_row_id = shard.row_range_start;
        let mut rows_seen = 0usize;
        let mut bytes_written = 0usize;
        let mut next_training_sample = 0usize;
        let mut training_buffer = Vec::with_capacity(buffer_floats);

        while let Some(batch) = batches.try_next().await? {
            let vectors =
                validate_vector_batch(&batch, index_column, dimension, &mut expected_row_id)?;
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
                    let sample = checked_training_sample_index(
                        next_training_sample,
                        row_count_usize,
                        training_vector_count,
                    )?;
                    if sample >= batch_end {
                        break;
                    }
                    let offset = (sample - rows_seen) * dimension;
                    training_buffer.extend_from_slice(&vectors.values[offset..offset + dimension]);
                    next_training_sample += 1;
                    if training_buffer.len() == buffer_floats {
                        trainer
                            .add_training_vectors_mut(
                                &training_buffer,
                                training_buffer.len() / dimension,
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
                .add_training_vectors_mut(&training_buffer, training_buffer.len() / dimension)
                .map_err(|e| Error::DataInvalid {
                    message: format!("Failed to add vindex training vectors: {e}"),
                    source: Some(Box::new(e)),
                })?;
        }
        let expected_end =
            shard
                .row_range_end
                .checked_add(1)
                .ok_or_else(|| Error::DataInvalid {
                    message: "vindex row range end overflows i64".to_string(),
                    source: None,
                })?;
        if rows_seen != row_count_usize
            || expected_row_id != expected_end
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
        let writer = tokio::task::spawn_blocking(move || -> std::io::Result<_> {
            let training = trainer.finish()?;
            let mut writer = VectorIndexWriter::new(training);
            let mut raw_file = raw_file;
            raw_file.seek(SeekFrom::Start(0))?;
            let batch_rows = buffer_rows.min(row_count_usize);
            let mut buffer = MutableBuffer::new(checked_std_vector_bytes(batch_rows, dimension)?);
            let mut ids = Vec::with_capacity(batch_rows);
            let mut rows_added = 0usize;
            while rows_added < row_count_usize {
                let rows = batch_rows.min(row_count_usize - rows_added);
                buffer.resize(checked_std_vector_bytes(rows, dimension)?, 0);
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

        let meta = self
            .finish_index_file(writer, shard, index_field_id, index_meta, row_count)
            .await?;
        Ok(BuiltIndexFile { meta, timing: None })
    }

    pub(super) async fn finish_index_file(
        &self,
        writer: VectorIndexWriter,
        shard: &VindexIndexShard,
        index_field_id: i32,
        index_meta: Vec<u8>,
        row_count: i64,
    ) -> Result<IndexFileMeta> {
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
            external_path: None,
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
