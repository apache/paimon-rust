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

//! Shared sparse-vector spill, training, and native serialization.
use super::vector_extraction::{local_ids, validate_vector_batch};
use super::vector_validation::{
    checked_std_vector_bytes, checked_training_sample_index, checked_training_vector_count,
    checked_vector_bytes,
};
use super::{error, IndexPayload};
use crate::io::OutputFile;
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::vindex::VindexVectorIndexOptions;
use crate::{Error, Result};
use arrow_array::RecordBatch;
use arrow_buffer::MutableBuffer;
use futures::TryStreamExt;
use paimon_vindex_core::autotune::default_training_vector_count;
use paimon_vindex_core::index::{VectorIndexConfig, VectorIndexTrainer, VectorIndexWriter};
use paimon_vindex_core::io::PosWriter;
use std::io::{Read, Seek, SeekFrom};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio_util::io::SyncIoBridge;
const VECTOR_BUFFER_BYTES: usize = 8 * 1024 * 1024;

pub(in crate::table) struct VectorSpill {
    raw: tokio::fs::File,
    ids: tokio::fs::File,
    range: RowRange,
    dimension: usize,
    next_id: i64,
    rows: usize,
    vectors: usize,
    bytes: usize,
}

impl VectorSpill {
    pub fn new(range: RowRange, dimension: usize) -> Result<Self> {
        let count = usize::try_from(super::row_count(&range)?).map_err(error)?;
        if dimension == 0 {
            return Err(Error::DataInvalid {
                message: "vindex vector dimension must be positive".into(),
                source: None,
            });
        }
        checked_vector_bytes(count, dimension)?;
        Ok(Self {
            raw: temporary_vector_file("vectors")?,
            ids: temporary_vector_file("row IDs")?,
            next_id: range.from(),
            range,
            dimension,
            rows: 0,
            vectors: 0,
            bytes: 0,
        })
    }
    pub fn bytes_written(&self) -> usize {
        self.bytes
    }

    pub async fn push_batch(
        &mut self,
        batch: &RecordBatch,
        column: &str,
        timing_enabled: bool,
    ) -> Result<Duration> {
        let vectors = validate_vector_batch(batch, column, self.dimension, &mut self.next_id)?;
        self.rows =
            self.rows
                .checked_add(vectors.source_rows)
                .ok_or_else(|| Error::DataInvalid {
                    message: "vindex streamed row count overflows usize".into(),
                    source: None,
                })?;
        self.vectors = self
            .vectors
            .checked_add(vectors.vector_count)
            .ok_or_else(|| Error::DataInvalid {
                message: "vindex streamed vector count overflows usize".into(),
                source: None,
            })?;
        let count = usize::try_from(super::row_count(&self.range)?).map_err(error)?;
        let ids: arrow_buffer::ScalarBuffer<i64> =
            local_ids(&vectors.row_ids, self.range.from(), count)?.into();
        let start = timing_enabled.then(Instant::now);
        self.raw
            .write_all(vectors.bytes())
            .await
            .map_err(spill_error)?;
        self.ids
            .write_all(ids.inner().as_slice())
            .await
            .map_err(spill_error)?;
        self.bytes = self
            .bytes
            .checked_add(vectors.bytes().len())
            .ok_or_else(|| Error::DataInvalid {
                message: "vindex spilled byte count overflows usize".into(),
                source: None,
            })?;
        Ok(start.map_or(Duration::ZERO, |s| s.elapsed()))
    }

    pub async fn finish(mut self) -> Result<Option<SpilledVectors>> {
        let rows = usize::try_from(super::row_count(&self.range)?).map_err(error)?;
        let end = self
            .range
            .to()
            .checked_add(1)
            .ok_or_else(|| Error::DataInvalid {
                message: "vindex row range end overflows i64".into(),
                source: None,
            })?;
        if self.rows != rows
            || self.next_id != end
            || self.bytes != checked_vector_bytes(self.vectors, self.dimension)?
        {
            return Err(Error::DataInvalid {
                message: format!(
                    "vindex streamed data mismatch: rows={}/{rows}, vectors={}, bytes={}",
                    self.rows, self.vectors, self.bytes
                ),
                source: None,
            });
        }
        if self.vectors == 0 {
            return Ok(None);
        }
        self.raw.flush().await.map_err(spill_error)?;
        self.ids.flush().await.map_err(spill_error)?;
        if self.raw.metadata().await.map_err(spill_error)?.len() != self.bytes as u64
            || self.ids.metadata().await.map_err(spill_error)?.len()
                != checked_vector_bytes(self.vectors, 2)? as u64
        {
            return Err(Error::DataInvalid {
                message: "temporary vindex vector or row ID file size mismatch".into(),
                source: None,
            });
        }
        Ok(Some(SpilledVectors {
            raw: self.raw.into_std().await,
            ids: self.ids.into_std().await,
            count: self.vectors,
            dimension: self.dimension,
            range: self.range,
        }))
    }
}

pub(in crate::table) struct SpilledVectors {
    raw: std::fs::File,
    ids: std::fs::File,
    count: usize,
    dimension: usize,
    range: RowRange,
}

/// A data-dependent training configuration failure, distinct from native I/O
/// or training execution errors. Maintenance may retry a larger source unit.
#[derive(Debug)]
struct TrainingRequirement {
    range: RowRange,
    vectors: usize,
    cause: Error,
}

impl std::fmt::Display for TrainingRequirement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} vectors in row range [{}, {}]: {}",
            self.vectors,
            self.range.from(),
            self.range.to(),
            self.cause
        )
    }
}

impl std::error::Error for TrainingRequirement {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

pub(in crate::table) fn training_range(error: &Error) -> Option<RowRange> {
    let mut cause: &(dyn std::error::Error + 'static) = error;
    loop {
        if let Some(requirement) = cause.downcast_ref::<TrainingRequirement>() {
            return Some(requirement.range.clone());
        }
        cause = cause.source()?;
    }
}
pub(in crate::table) struct TrainedVectors {
    pub writer: VectorIndexWriter,
    pub train_finish: Duration,
    pub raw_temp_reread: Duration,
    pub index_add: Duration,
    pub training_vector_count: usize,
    pub training_rows_retained: usize,
}
impl SpilledVectors {
    pub async fn train(
        self,
        options: &VindexVectorIndexOptions,
        timing_enabled: bool,
    ) -> Result<TrainedVectors> {
        let training_vector_count =
            checked_training_vector_count(self.count, options.train_sample_ratio)?;
        let config = options
            .training_config(self.count)
            .map_err(|cause| Error::DataInvalid {
                message: cause.to_string(),
                source: Some(Box::new(TrainingRequirement {
                    range: self.range.clone(),
                    vectors: self.count,
                    cause,
                })),
            })?;
        let training_rows_retained = if timing_enabled {
            default_training_vector_count(training_vector_count, config.nlist()).unwrap_or(0)
        } else {
            0
        };
        let ratio = options.train_sample_ratio;
        let (writer, train_finish, raw_temp_reread, index_add) =
            tokio::task::spawn_blocking(move || {
                train_and_add_spilled_vectors(
                    self.raw,
                    self.ids,
                    config,
                    self.count,
                    self.dimension,
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
                message: "Failed to train or add vectors to vindex index".into(),
                source: Some(Box::new(e)),
            })?;
        Ok(TrainedVectors {
            writer,
            train_finish,
            raw_temp_reread,
            index_add,
            training_vector_count,
            training_rows_retained,
        })
    }
}

pub(in crate::table) async fn serialize_writer(
    mut writer: VectorIndexWriter,
    output: OutputFile,
) -> Result<()> {
    let mut output = SyncIoBridge::new(output.async_writer().await?);
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        writer.write(&mut PosWriter::new(&mut output))?;
        output.shutdown()
    })
    .await
    .map_err(|e| Error::UnexpectedError {
        message: format!("vindex serialization task failed: {e}"),
        source: None,
    })?
    .map_err(|e| Error::UnexpectedError {
        message: "Failed to stream vindex index".into(),
        source: Some(Box::new(e)),
    })
}

pub(in crate::table) async fn write_payload(
    mut batches: ArrowRecordBatchStream,
    output: OutputFile,
    column: &str,
    options: &VindexVectorIndexOptions,
    range: RowRange,
    index_meta: Vec<u8>,
) -> Result<Option<IndexPayload>> {
    let mut spill = VectorSpill::new(range, options.dimension())?;
    while let Some(batch) = batches.try_next().await? {
        spill.push_batch(&batch, column, false).await?;
    }
    let Some(spill) = spill.finish().await? else {
        return Ok(None);
    };
    let row_count = i64::try_from(spill.count).map_err(error)?;
    let trained = spill.train(options, false).await?;
    serialize_writer(trained.writer, output).await?;
    Ok(Some(IndexPayload {
        row_count,
        index_meta,
    }))
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
