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

use super::extraction::{
    data_split_for_shard_ranges, extract_vector_batch, validate_vector_batch_ranges,
};
use super::planning::VindexIndexShard;
use super::timing::{vector_index_build_timing_enabled, VectorIndexBuildTiming};
use super::validation::{
    checked_row_count, checked_training_sample_index, checked_training_vector_count,
    checked_vector_bytes,
};
use super::writer::BuiltIndexFile;
use super::VindexIndexBuildBuilder;
use crate::arrow::format::parquet::{
    coalesced_parquet_range_bytes, parquet_granules, ParquetGranule,
};
use crate::spec::ROW_ID_FIELD_NAME;
use crate::table::data_file_reader::DataFileReadTiming;
use crate::table::table_read::configured_parquet_read_budget;
use crate::table::{merge_row_ranges, ArrowRecordBatchStream, RowRange};
use crate::vindex::VindexVectorIndexOptions;
use crate::{Error, Result};
use arrow_array::RecordBatch;
use arrow_buffer::MutableBuffer;
use futures::{StreamExt, TryStreamExt};
use paimon_vindex_core::autotune::default_training_vector_count;
use paimon_vindex_core::index::{VectorIndexTrainer, VectorIndexTraining, VectorIndexWriter};
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const MIN_STRATA: usize = 256;
const ROWS_PER_STRATUM: usize = 128;
const FIRST_BYTES_NUMERATOR: u64 = 3;
const FIRST_BYTES_DENOMINATOR: u64 = 10;
const QUEUE_CAPACITY: usize = 2;
const BUFFER_BYTES: usize = 8 * 1024 * 1024;
const REPLAY_TARGET_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug)]
struct Granule {
    range: RowRange,
    file_index: usize,
    byte_ranges: Vec<Range<u64>>,
}

#[derive(Debug)]
pub(super) struct GranulePlan {
    pub(super) first: Vec<RowRange>,
    pub(super) rest: Vec<RowRange>,
    first_rows: usize,
    // None preserves full-spill's global stride sampling when first covers the shard.
    training: Option<Vec<i64>>,
}

struct SpillRecord {
    ids: Vec<i64>,
    bytes: Vec<u8>,
}

enum AddItem {
    Batch(RecordBatch, Vec<i64>),
    Spilled(Vec<i64>, MutableBuffer),
}

type SpillTask = oneshot::Receiver<std::io::Result<(std::fs::File, u64, Duration)>>;
type ConsumerTask = oneshot::Receiver<Result<(VectorIndexWriter, usize, usize, Duration)>>;
type TrainingTask = JoinHandle<std::io::Result<(VectorIndexTraining, Duration)>>;
type ReplayTask = JoinHandle<std::io::Result<(usize, Duration)>>;

fn spawn_channel_worker<T, F>(name: &'static str, worker: F) -> Result<oneshot::Receiver<T>>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let _ = sender.send(worker());
        })
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to spawn {name} worker: {e}"),
            source: Some(Box::new(e)),
        })?;
    Ok(receiver)
}

struct SpillWriter {
    sender: mpsc::Sender<SpillRecord>,
    task: SpillTask,
}

impl SpillWriter {
    async fn finish(self) -> Result<(std::fs::File, u64, Duration)> {
        drop(self.sender);
        join_spill(self.task).await
    }
}

struct LivePipeline {
    sender: mpsc::Sender<AddItem>,
    consumer: ConsumerTask,
    replay: ReplayTask,
    spill_bytes: u64,
    spill_write: Duration,
}

fn spawn_spill_writer(timing_enabled: bool) -> Result<SpillWriter> {
    let file = tempfile::tempfile().map_err(|e| Error::UnexpectedError {
        message: format!("Failed to create temporary vindex vector file: {e}"),
        source: Some(Box::new(e)),
    })?;
    let (sender, mut receiver) = mpsc::channel::<SpillRecord>(QUEUE_CAPACITY);
    let task = spawn_channel_worker("paimon-vindex-spill", move || -> std::io::Result<_> {
        let mut writer = BufWriter::with_capacity(BUFFER_BYTES, file);
        let mut spill_bytes = 0u64;
        let mut spill_write = Duration::ZERO;
        while let Some(record) = receiver.blocking_recv() {
            let write_start = timing_enabled.then(Instant::now);
            let count = record.ids.len() as u64;
            writer.write_all(&count.to_le_bytes())?;
            for id in record.ids {
                writer.write_all(&id.to_le_bytes())?;
            }
            writer.write_all(&record.bytes)?;
            spill_bytes = spill_bytes
                .saturating_add(8)
                .saturating_add(count.saturating_mul(8))
                .saturating_add(record.bytes.len() as u64);
            if let Some(start) = write_start {
                spill_write = spill_write.saturating_add(start.elapsed());
            }
        }
        let write_start = timing_enabled.then(Instant::now);
        writer.flush()?;
        let file = writer.into_inner().map_err(|e| e.into_error())?;
        if let Some(start) = write_start {
            spill_write = spill_write.saturating_add(start.elapsed());
        }
        Ok((file, spill_bytes, spill_write))
    })?;
    Ok(SpillWriter { sender, task })
}

async fn join_spill(task: SpillTask) -> Result<(std::fs::File, u64, Duration)> {
    task.await
        .map_err(|e| Error::UnexpectedError {
            message: format!("vindex spill task failed: {e}"),
            source: None,
        })?
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to spill vindex vectors: {e}"),
            source: Some(Box::new(e)),
        })
}

fn spawn_add_consumer(
    writer: VectorIndexWriter,
    mut receiver: mpsc::Receiver<AddItem>,
    index_column: String,
    dimension: usize,
    timing_enabled: bool,
) -> Result<ConsumerTask> {
    spawn_channel_worker("paimon-vindex-add", move || -> Result<_> {
        let mut writer = writer;
        let mut rows_added = 0usize;
        let mut replay_rows = 0usize;
        let mut index_add = Duration::ZERO;
        while let Some(item) = receiver.blocking_recv() {
            let add_start = timing_enabled.then(Instant::now);
            match item {
                AddItem::Batch(batch, ids) => {
                    let vectors = extract_vector_batch(&batch, &index_column, dimension)?;
                    if ids.len() != vectors.row_count {
                        return Err(Error::DataInvalid {
                            message: "vindex add batch id count mismatch".to_string(),
                            source: None,
                        });
                    }
                    writer
                        .add_vectors(&ids, vectors.values, vectors.row_count)
                        .map_err(|e| Error::UnexpectedError {
                            message: format!("Failed to add vectors to vindex index: {e}"),
                            source: Some(Box::new(e)),
                        })?;
                    rows_added += vectors.row_count;
                }
                AddItem::Spilled(ids, buffer) => {
                    let values = buffer.typed_data::<f32>();
                    if values.len() != ids.len() * dimension {
                        return Err(Error::DataInvalid {
                            message: "vindex spilled vector length mismatch".to_string(),
                            source: None,
                        });
                    }
                    writer.add_vectors(&ids, values, ids.len()).map_err(|e| {
                        Error::UnexpectedError {
                            message: format!("Failed to add spilled vectors to vindex index: {e}"),
                            source: Some(Box::new(e)),
                        }
                    })?;
                    rows_added += ids.len();
                    replay_rows += ids.len();
                }
            }
            if let Some(start) = add_start {
                index_add = index_add.saturating_add(start.elapsed());
            }
        }
        Ok((writer, rows_added, replay_rows, index_add))
    })
}

async fn join_consumer(task: ConsumerTask) -> Result<(VectorIndexWriter, usize, usize, Duration)> {
    task.await.map_err(|e| Error::UnexpectedError {
        message: format!("vindex add task failed: {e}"),
        source: None,
    })?
}

async fn join_training(task: TrainingTask) -> Result<(VectorIndexTraining, Duration)> {
    task.await
        .map_err(|e| Error::UnexpectedError {
            message: format!("vindex training task failed: {e}"),
            source: None,
        })?
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to train vindex index: {e}"),
            source: Some(Box::new(e)),
        })
}

async fn start_live_pipeline(
    training: TrainingTask,
    spill: SpillWriter,
    index_column: String,
    dimension: usize,
    timing_enabled: bool,
) -> Result<(LivePipeline, Duration)> {
    let trained = join_training(training).await;
    let spilled = spill.finish().await;
    let (trained, train_finish) = trained?;
    let (file, spill_bytes, spill_write) = spilled?;
    let (sender, receiver) = mpsc::channel(QUEUE_CAPACITY);
    let consumer = spawn_add_consumer(
        VectorIndexWriter::new(trained),
        receiver,
        index_column,
        dimension,
        timing_enabled,
    )?;
    let replay = spawn_replay(file, sender.clone(), dimension, timing_enabled);
    Ok((
        LivePipeline {
            sender,
            consumer,
            replay,
            spill_bytes,
            spill_write,
        },
        train_finish,
    ))
}

async fn finish_live_pipeline(
    pipeline: LivePipeline,
) -> (
    Result<(VectorIndexWriter, usize, usize, Duration)>,
    Result<(usize, Duration)>,
    u64,
    Duration,
) {
    let replay = pipeline
        .replay
        .await
        .map_err(|e| Error::UnexpectedError {
            message: format!("vindex replay task failed: {e}"),
            source: None,
        })
        .and_then(|result| {
            result.map_err(|e| Error::UnexpectedError {
                message: format!("Failed to replay spilled vindex vectors: {e}"),
                source: Some(Box::new(e)),
            })
        });
    drop(pipeline.sender);
    let consumer = join_consumer(pipeline.consumer).await;
    (consumer, replay, pipeline.spill_bytes, pipeline.spill_write)
}

fn spawn_replay(
    mut file: std::fs::File,
    sender: mpsc::Sender<AddItem>,
    dimension: usize,
    timing_enabled: bool,
) -> ReplayTask {
    tokio::task::spawn_blocking(move || -> std::io::Result<(usize, Duration)> {
        let mut spill_read = Duration::ZERO;
        let read_start = timing_enabled.then(Instant::now);
        file.seek(SeekFrom::Start(0))?;
        if let Some(start) = read_start {
            spill_read = spill_read.saturating_add(start.elapsed());
        }
        let mut reader = BufReader::with_capacity(BUFFER_BYTES, file);
        let mut rows = 0usize;
        let mut ids = Vec::new();
        let mut vectors = MutableBuffer::new(REPLAY_TARGET_BYTES);
        loop {
            let read_start = timing_enabled.then(Instant::now);
            let mut header = [0u8; 8];
            let read = reader.read(&mut header)?;
            if read == 0 {
                if let Some(start) = read_start {
                    spill_read = spill_read.saturating_add(start.elapsed());
                }
                break;
            }
            reader.read_exact(&mut header[read..])?;
            let count = usize::try_from(u64::from_le_bytes(header)).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid spill row count")
            })?;
            let id_bytes_len = count.checked_mul(8).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "spill id length overflow")
            })?;
            let mut id_bytes = vec![0u8; id_bytes_len];
            reader.read_exact(&mut id_bytes)?;
            let (id_chunks, remainder) = id_bytes.as_chunks::<8>();
            debug_assert!(remainder.is_empty());
            ids.extend(id_chunks.iter().map(|bytes| i64::from_le_bytes(*bytes)));
            let vector_bytes = count
                .checked_mul(dimension)
                .and_then(|value| value.checked_mul(4))
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "spill vector length overflow",
                    )
                })?;
            let offset = vectors.len();
            vectors.resize(offset + vector_bytes, 0);
            reader.read_exact(&mut vectors.as_slice_mut()[offset..])?;
            if let Some(start) = read_start {
                spill_read = spill_read.saturating_add(start.elapsed());
            }
            rows += count;
            if vectors.len() >= REPLAY_TARGET_BYTES {
                let item = AddItem::Spilled(
                    std::mem::take(&mut ids),
                    std::mem::replace(&mut vectors, MutableBuffer::new(REPLAY_TARGET_BYTES)),
                );
                if sender.blocking_send(item).is_err() {
                    return Ok((rows, spill_read));
                }
            }
        }
        if !ids.is_empty() {
            let _ = sender.blocking_send(AddItem::Spilled(ids, vectors));
        }
        Ok((rows, spill_read))
    })
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn shard_seed(shard: &VindexIndexShard) -> u64 {
    let mut seed = 0u64;
    let mut absorb = |value: u64| {
        let mut state = seed ^ value;
        seed = splitmix64(&mut state);
    };
    absorb(shard.snapshot_id as u64);
    absorb(shard.source_bucket as u64);
    absorb(shard.row_range_start as u64);
    absorb(shard.row_range_end as u64);
    absorb(shard.partition_bytes.len() as u64);
    for chunk in shard.partition_bytes.chunks(8) {
        let mut word = [0u8; 8];
        word[..chunk.len()].copy_from_slice(chunk);
        absorb(u64::from_le_bytes(word));
    }
    seed
}

fn random_below(state: &mut u64, bound: usize) -> usize {
    let bound = bound as u64;
    let threshold = bound.wrapping_neg() % bound;
    loop {
        let value = splitmix64(state);
        if value >= threshold {
            return (value % bound) as usize;
        }
    }
}

fn pick_indices(total: usize, count: usize, state: &mut u64) -> impl Iterator<Item = usize> + '_ {
    (0..count).map(move |index| {
        let start = (index as u128 * total as u128 / count as u128) as usize;
        let end = ((index + 1) as u128 * total as u128 / count as u128) as usize;
        start + random_below(state, end - start)
    })
}

// Boundaries depend only on the layout, never on which granules were sampled.
// Keep original indices: a unit can cross files, but physical reads cannot.
fn sampling_units(granules: &[Granule]) -> Vec<Range<usize>> {
    let mut units: Vec<Range<usize>> = Vec::new();
    let mut start = 0;
    for end in 1..=granules.len() {
        let rows = granules[end - 1].range.to() - granules[start].range.from() + 1;
        if rows as usize >= 2 * ROWS_PER_STRATUM {
            units.push(start..end);
            start = end;
        }
    }
    if start < granules.len() {
        if let Some(last) = units.last_mut() {
            last.end = granules.len();
        } else {
            units.push(0..granules.len());
        }
    }
    units
}

fn unit_training_offsets(rows: usize, quota: usize, offset: usize) -> impl Iterator<Item = usize> {
    (0..quota).map(move |index| {
        ((index as u128 * rows as u128 + offset as u128) / quota as u128) as usize
    })
}

fn granule_bytes(granules: &[Granule], selected: Option<&HashSet<usize>>) -> u64 {
    let mut files = HashMap::<usize, Vec<Range<u64>>>::new();
    for (index, granule) in granules.iter().enumerate() {
        if selected.is_none_or(|selected| selected.contains(&index)) {
            files
                .entry(granule.file_index)
                .or_default()
                .extend(granule.byte_ranges.iter().cloned());
        }
    }
    files
        .values()
        .map(|ranges| coalesced_parquet_range_bytes(ranges))
        .sum()
}

fn select_first(
    granules: &[Granule],
    eligible: usize,
    retained: usize,
    mut seed: u64,
) -> Result<GranulePlan> {
    // Callers have validated that granules partition one nonempty shard.
    let range = RowRange::new(
        granules[0].range.from(),
        granules.last().unwrap().range.to(),
    );
    let rows = checked_row_count(range.from(), range.to())? as usize;
    let whole_shard = || GranulePlan {
        first: vec![range.clone()],
        rest: Vec::new(),
        first_rows: rows,
        training: None,
    };
    if rows < 2 * ROWS_PER_STRATUM {
        return Ok(whole_shard());
    }
    let units = sampling_units(granules);
    let strata = retained
        .div_ceil(ROWS_PER_STRATUM)
        .max(MIN_STRATA)
        .min(rows);
    let candidates = strata
        .checked_mul(ROWS_PER_STRATUM)
        .ok_or_else(|| Error::DataInvalid {
            message: "vindex training candidate count overflows usize".to_string(),
            source: None,
        })?;
    let mut quotas = vec![0usize; units.len()];
    for row in pick_indices(rows, strata, &mut seed) {
        let row_id = range.from() + row as i64;
        let unit = units.partition_point(|unit| granules[unit.end - 1].range.to() < row_id);
        quotas[unit] += ROWS_PER_STRATUM;
    }
    let mut selected = HashSet::new();
    for (unit, quota) in units.iter().zip(&quotas) {
        let unit_rows = granules[unit.end - 1].range.to() - granules[unit.start].range.from() + 1;
        if *quota > unit_rows as usize {
            return Ok(whole_shard());
        }
        if *quota > 0 {
            selected.extend(unit.clone());
        }
    }
    let first_bytes = granule_bytes(granules, Some(&selected));
    let total_bytes = granule_bytes(granules, None);
    if selected.len() == granules.len()
        || selected.len() < MIN_STRATA.min(granules.len())
        || total_bytes == 0
        || first_bytes.saturating_mul(FIRST_BYTES_DENOMINATOR)
            > total_bytes.saturating_mul(FIRST_BYTES_NUMERATOR)
    {
        return Ok(whole_shard());
    }

    let mut training = Vec::with_capacity(candidates);
    for (unit, quota) in units.iter().zip(quotas) {
        if quota == 0 {
            continue;
        }
        let start = granules[unit.start].range.from();
        let unit_rows = (granules[unit.end - 1].range.to() - start + 1) as usize;
        let offset = random_below(&mut seed, unit_rows);
        training
            .extend(unit_training_offsets(unit_rows, quota, offset).map(|row| start + row as i64));
    }
    debug_assert_eq!(training.len(), candidates);
    if eligible < candidates {
        for index in 0..eligible {
            let other = index + random_below(&mut seed, candidates - index);
            training.swap(index, other);
        }
        training.truncate(eligible);
        training.sort_unstable();
    }
    let mut first = Vec::with_capacity(selected.len());
    let mut rest = Vec::with_capacity(granules.len() - selected.len());
    let mut first_rows = 0;
    for (index, granule) in granules.iter().enumerate() {
        if selected.contains(&index) {
            first_rows += granule.range.count() as usize;
            first.push(granule.range.clone());
        } else {
            rest.push(granule.range.clone());
        }
    }
    Ok(GranulePlan {
        first: merge_row_ranges(first),
        rest: merge_row_ranges(rest),
        first_rows,
        training: Some(training),
    })
}

fn append_shard_granules(
    granules: &mut Vec<Granule>,
    file_index: usize,
    file_start: i64,
    shard_range: &RowRange,
    file_granules: Vec<ParquetGranule>,
) -> Result<()> {
    for granule in file_granules {
        let from = file_start
            .checked_add(granule.first_row)
            .ok_or_else(|| Error::DataInvalid {
                message: "vindex granule row id overflows i64".to_string(),
                source: None,
            })?;
        let to = from
            .checked_add(granule.row_count - 1)
            .ok_or_else(|| Error::DataInvalid {
                message: "vindex granule row range overflows i64".to_string(),
                source: None,
            })?;
        if let Some(range) = shard_range.intersect_inclusive(from, to) {
            granules.push(Granule {
                range,
                file_index,
                byte_ranges: granule.byte_ranges,
            });
        }
    }
    Ok(())
}

fn granules_partition_shard(granules: &[Granule], shard_range: &RowRange) -> bool {
    granules
        .first()
        .is_some_and(|granule| granule.range.from() == shard_range.from())
        && granules
            .last()
            .is_some_and(|granule| granule.range.to() == shard_range.to())
        && granules
            .windows(2)
            .all(|pair| pair[0].range.to().checked_add(1) == Some(pair[1].range.from()))
}

fn local_ids(row_ids: &[i64], start: i64, row_count: usize) -> Result<Vec<i64>> {
    let end = start
        .checked_add(i64::try_from(row_count).map_err(|e| Error::DataInvalid {
            message: "vindex row count does not fit i64".to_string(),
            source: Some(Box::new(e)),
        })?)
        .ok_or_else(|| Error::DataInvalid {
            message: "vindex row range overflows i64".to_string(),
            source: None,
        })?;
    row_ids
        .iter()
        .map(|row_id| {
            if *row_id < start || *row_id >= end {
                Err(Error::DataInvalid {
                    message: format!("vindex row id {row_id} is outside shard [{start}, {end})"),
                    source: None,
                })
            } else {
                Ok(*row_id - start)
            }
        })
        .collect()
}

impl<'a> VindexIndexBuildBuilder<'a> {
    fn open_vector_stream(
        &self,
        shard: &VindexIndexShard,
        ranges: Vec<RowRange>,
        index_column: &str,
        read_timing: Option<&Arc<DataFileReadTiming>>,
        parquet_read_budget: Option<&Arc<crate::arrow::ReadBudget>>,
    ) -> Result<ArrowRecordBatchStream> {
        let split = data_split_for_shard_ranges(shard, ranges)?;
        let mut read_builder = self.table.new_read_builder();
        read_builder.with_projection(&[index_column, ROW_ID_FIELD_NAME])?;
        let read = read_builder.new_read()?;
        let read = match read_timing {
            Some(timing) => read.with_data_file_read_timing(Arc::clone(timing)),
            None => read,
        };
        let read = match parquet_read_budget {
            Some(budget) => read.with_parquet_read_budget(Arc::clone(budget)),
            None => read,
        };
        read.to_arrow(&[split])
    }

    pub(super) async fn plan_granules(
        &self,
        shard: &VindexIndexShard,
        index_column: &str,
        eligible: usize,
        retained: usize,
    ) -> Result<GranulePlan> {
        let shard_range = RowRange::new(shard.row_range_start, shard.row_range_end);
        let mut granules = Vec::new();
        let mut use_whole_shard = false;
        let mut parquet_files = Vec::new();

        for (file_index, file) in shard.files.iter().enumerate() {
            if file
                .write_cols
                .as_ref()
                .is_some_and(|columns| !columns.iter().any(|column| column == index_column))
            {
                continue;
            }
            let Some((file_start, file_end)) = file.row_id_range() else {
                use_whole_shard = true;
                break;
            };
            let Some(range) = shard_range.intersect_inclusive(file_start, file_end) else {
                continue;
            };
            let file_size = u64::try_from(file.file_size).map_err(|e| Error::DataInvalid {
                message: format!(
                    "Invalid data file size for '{}': {}",
                    file.file_name, file.file_size
                ),
                source: Some(Box::new(e)),
            })?;
            let path = file.data_file_path(&shard.bucket_path);
            if path.to_ascii_lowercase().ends_with(".parquet") {
                parquet_files.push((file_index, path, file_size, file_start, file_end));
            } else {
                granules.push(Granule {
                    range,
                    file_index,
                    byte_ranges: std::iter::once(0..file_size).collect(),
                });
            }
        }

        if !use_whole_shard && !parquet_files.is_empty() {
            let page_index_enabled = self
                .table
                .schema()
                .core_options()
                .parquet_filter_column_index_enabled()?;
            let concurrency = self
                .table
                .schema()
                .core_options()
                .parquet_row_group_parallelism()?
                .max(1);
            let file_io = self.table.file_io();
            let mut results = futures::stream::iter(parquet_files)
                .map(
                    |(file_index, path, file_size, file_start, file_end)| async move {
                        let input = file_io.new_input(&path)?;
                        let reader = Box::new(input.reader().await?);
                        let (granules, _) =
                            parquet_granules(reader, file_size, index_column, page_index_enabled)
                                .await?;
                        Ok::<_, Error>((file_index, file_start, file_end, granules))
                    },
                )
                .buffer_unordered(concurrency);
            while let Some(result) = results.next().await {
                match result {
                    Ok((file_index, file_start, file_end, file_granules)) => {
                        let covered = file_granules
                            .iter()
                            .map(|granule| granule.row_count)
                            .sum::<i64>();
                        if covered != file_end - file_start + 1 {
                            use_whole_shard = true;
                            break;
                        }
                        append_shard_granules(
                            &mut granules,
                            file_index,
                            file_start,
                            &shard_range,
                            file_granules,
                        )?;
                    }
                    Err(error) => {
                        log::warn!(
                            "vindex granule metadata read failed; using the whole shard as the first granule: {error}"
                        );
                        use_whole_shard = true;
                        break;
                    }
                }
            }
        }

        granules.sort_by_key(|granule| granule.range.from());
        if !use_whole_shard {
            // Data Evolution may retain multiple physical providers for the same logical rows.
            // Let the reader choose the provider instead of counting those rows twice here.
            use_whole_shard = !granules_partition_shard(&granules, &shard_range);
        }
        if use_whole_shard || granules.is_empty() {
            granules = vec![Granule {
                range: shard_range,
                file_index: 0,
                byte_ranges: std::iter::once(
                    0..shard
                        .files
                        .iter()
                        .map(|file| file.file_size.max(0) as u64)
                        .sum(),
                )
                .collect(),
            }];
        }

        select_first(&granules, eligible, retained, shard_seed(shard))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn build_index_file_granule(
        &self,
        shard: &VindexIndexShard,
        index_column: &str,
        dimension: i32,
        index_field_id: i32,
        options: &VindexVectorIndexOptions,
        index_meta: Vec<u8>,
    ) -> Result<BuiltIndexFile> {
        let timing_enabled = vector_index_build_timing_enabled();
        let total_start = timing_enabled.then(Instant::now);
        let mut source_batch_wait = Duration::ZERO;
        let mut batch_count = 0usize;
        let read_timing = timing_enabled.then(|| Arc::new(DataFileReadTiming::default()));
        let parquet_read_budget = if timing_enabled {
            let budget = configured_parquet_read_budget(self.table)?;
            budget.enable_diagnostics();
            Some(budget)
        } else {
            None
        };
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
        checked_vector_bytes(row_count_usize, dimension)?;
        let eligible = checked_training_vector_count(row_count_usize, options.train_sample_ratio)?;
        let retained =
            default_training_vector_count(eligible, options.config.nlist()).unwrap_or(eligible);
        let plan = self
            .plan_granules(shard, index_column, eligible, retained)
            .await?;

        let mut trainer =
            VectorIndexTrainer::new(options.config.clone()).map_err(|e| Error::DataInvalid {
                message: format!("Failed to initialize vindex trainer: {e}"),
                source: Some(Box::new(e)),
            })?;
        let mut spill = Some(spawn_spill_writer(timing_enabled)?);
        let training_rows = plan.training.as_ref().map_or(eligible, Vec::len);
        let training_buffer_rows = (BUFFER_BYTES / checked_vector_bytes(1, dimension)?).max(1);
        let training_buffer_floats = training_buffer_rows * dimension;
        let mut training_buffer = Vec::with_capacity(training_buffer_floats);
        let mut next_training_sample = 0usize;
        let mut first_rows = 0usize;
        let mut range_index = 0usize;
        let first_result: Result<()> = async {
            let mut stream = self.open_vector_stream(
                shard,
                plan.first.clone(),
                index_column,
                read_timing.as_ref(),
                parquet_read_budget.as_ref(),
            )?;
            let mut expected_row_id = plan.first[0].from();
            loop {
                let source_start = timing_enabled.then(Instant::now);
                let batch = stream.try_next().await?;
                if let Some(start) = source_start {
                    source_batch_wait = source_batch_wait.saturating_add(start.elapsed());
                }
                let Some(batch) = batch else { break };
                batch_count += 1;
                let vectors = validate_vector_batch_ranges(
                    &batch,
                    index_column,
                    dimension,
                    &plan.first,
                    &mut range_index,
                    &mut expected_row_id,
                )?;
                let batch_end = first_rows.checked_add(vectors.row_count).ok_or_else(|| {
                    Error::DataInvalid {
                        message: "vindex first-batch row count overflows usize".to_string(),
                        source: None,
                    }
                })?;
                while next_training_sample < training_rows {
                    let row = if let Some(training) = &plan.training {
                        let sample = training[next_training_sample];
                        match vectors.row_ids.binary_search(&sample) {
                            Ok(row) => row,
                            Err(row) if row == vectors.row_count => break,
                            Err(_) => return Err(Error::DataInvalid {
                                message: format!("Missing vindex training row {sample}"),
                                source: None,
                            }),
                        }
                    } else {
                        let sample = checked_training_sample_index(
                            next_training_sample,
                            plan.first_rows,
                            training_rows,
                        )?;
                        if sample >= batch_end {
                            break;
                        }
                        sample - first_rows
                    };
                    let offset = row * dimension;
                    training_buffer.extend_from_slice(&vectors.values[offset..offset + dimension]);
                    next_training_sample += 1;
                    if training_buffer.len() == training_buffer_floats {
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
                let record = SpillRecord {
                    ids: local_ids(vectors.row_ids, shard.row_range_start, row_count_usize)?,
                    bytes: vectors.bytes.to_vec(),
                };
                first_rows = batch_end;
                if spill.as_ref().unwrap().sender.send(record).await.is_err() {
                    return Err(Error::UnexpectedError {
                        message: "vindex spill writer stopped unexpectedly".to_string(),
                        source: None,
                    });
                }
            }
            if !training_buffer.is_empty() {
                trainer
                    .add_training_vectors_mut(&training_buffer, training_buffer.len() / dimension)
                    .map_err(|e| Error::DataInvalid {
                        message: format!("Failed to add vindex training vectors: {e}"),
                        source: Some(Box::new(e)),
                    })?;
            }
            if first_rows != plan.first_rows
                || range_index != plan.first.len()
                || next_training_sample != training_rows
            {
                return Err(Error::DataInvalid {
                    message: format!(
                        "vindex first-batch mismatch: rows={first_rows}/{}, ranges={range_index}/{}, training={next_training_sample}/{training_rows}",
                        plan.first_rows,
                        plan.first.len()
                    ),
                    source: None,
                });
            }
            Ok(())
        }
        .await;
        if let Err(error) = first_result {
            return match spill.take().unwrap().finish().await {
                Ok(_) => Err(error),
                Err(spill_error) => Err(spill_error),
            };
        }

        let mut training: Option<TrainingTask> = Some(tokio::task::spawn_blocking(
            move || -> std::io::Result<_> {
                let start = timing_enabled.then(Instant::now);
                let training = trainer.finish()?;
                Ok((
                    training,
                    start.map_or(Duration::ZERO, |start| start.elapsed()),
                ))
            },
        ));
        let mut live: Option<LivePipeline> = None;
        let mut train_finish = Duration::ZERO;
        let index_column = index_column.to_string();

        macro_rules! go_live {
            () => {{
                let (pipeline, duration) = start_live_pipeline(
                    training.take().expect("training task"),
                    spill.take().expect("spill writer"),
                    index_column.clone(),
                    dimension,
                    timing_enabled,
                )
                .await?;
                train_finish = duration;
                live = Some(pipeline);
            }};
        }

        let mut rest_rows = 0usize;
        let producer_result: Result<()> = async {
            if !plan.rest.is_empty() {
                let mut stream = self.open_vector_stream(
                    shard,
                    plan.rest.clone(),
                    &index_column,
                    read_timing.as_ref(),
                    parquet_read_budget.as_ref(),
                )?;
                let mut range_index = 0usize;
                let mut expected_row_id = plan.rest[0].from();
                loop {
                    let source_start = timing_enabled.then(Instant::now);
                    let batch = stream.try_next().await?;
                    if let Some(start) = source_start {
                        source_batch_wait = source_batch_wait.saturating_add(start.elapsed());
                    }
                    let Some(batch) = batch else { break };
                    batch_count += 1;
                    if live.is_none() && training.as_ref().is_some_and(|task| task.is_finished()) {
                        go_live!();
                    }
                    let vectors = validate_vector_batch_ranges(
                        &batch,
                        &index_column,
                        dimension,
                        &plan.rest,
                        &mut range_index,
                        &mut expected_row_id,
                    )?;
                    let ids = local_ids(vectors.row_ids, shard.row_range_start, row_count_usize)?;
                    rest_rows = rest_rows.checked_add(vectors.row_count).ok_or_else(|| {
                        Error::DataInvalid {
                            message: "vindex remaining row count overflows usize".to_string(),
                            source: None,
                        }
                    })?;
                    if let Some(pipeline) = &live {
                        if pipeline
                            .sender
                            .send(AddItem::Batch(batch, ids))
                            .await
                            .is_err()
                        {
                            return Err(Error::UnexpectedError {
                                message: "vindex add consumer stopped unexpectedly".to_string(),
                                source: None,
                            });
                        }
                    } else {
                        let record = SpillRecord {
                            ids,
                            bytes: vectors.bytes.to_vec(),
                        };
                        if spill.as_ref().unwrap().sender.send(record).await.is_err() {
                            return Err(Error::UnexpectedError {
                                message: "vindex spill writer stopped unexpectedly".to_string(),
                                source: None,
                            });
                        }
                    }
                }
                if rest_rows != row_count_usize - plan.first_rows
                    || range_index != plan.rest.len()
                {
                    return Err(Error::DataInvalid {
                        message: format!(
                            "vindex remaining-batch mismatch: rows={rest_rows}/{}, ranges={range_index}/{}",
                            row_count_usize - plan.first_rows,
                            plan.rest.len()
                        ),
                        source: None,
                    });
                }
            }
            Ok(())
        }
        .await;

        if let Err(producer_error) = producer_result {
            if let Some(pipeline) = live.take() {
                let (consumer, replay, _, _) = finish_live_pipeline(pipeline).await;
                consumer?;
                replay?;
            } else {
                let spill_result = match spill.take() {
                    Some(spill) => spill.finish().await.map(|_| ()),
                    None => Ok(()),
                };
                let training_result = match training.take() {
                    Some(training) => join_training(training).await.map(|_| ()),
                    None => Ok(()),
                };
                spill_result?;
                training_result?;
            }
            return Err(producer_error);
        }
        if live.is_none() {
            go_live!();
        }

        let (consumer, replay, granule_spill_bytes, granule_spill_write) =
            finish_live_pipeline(live.unwrap()).await;
        let (writer, rows_added, consumer_replay_rows, index_add) = consumer?;
        let (replay_rows, granule_spill_read) = replay?;
        if rows_added != row_count_usize || replay_rows != consumer_replay_rows {
            return Err(Error::DataInvalid {
                message: format!(
                    "vindex pipelined add mismatch: rows={rows_added}/{row_count_usize}, replay={consumer_replay_rows}/{replay_rows}"
                ),
                source: None,
            });
        }

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
            raw_temp_write: Duration::ZERO,
            granule_spill_write,
            train_finish,
            raw_temp_reread: Duration::ZERO,
            granule_spill_read,
            index_add,
            serialize_upload,
            rows: row_count_usize,
            training_rows_seen: training_rows,
            training_rows_retained: retained,
            batch_count,
            raw_temp_bytes: 0,
            granule_spill_bytes,
            index_bytes: meta.file_size as u64,
            data_file_count: shard.files.len(),
            file_name: meta.file_name.clone(),
        });
        Ok(BuiltIndexFile { meta, timing })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn granules_with_rows(rows: impl IntoIterator<Item = usize>) -> Vec<Granule> {
        let mut start = 0;
        rows.into_iter()
            .enumerate()
            .map(|(file_index, rows)| {
                let range = RowRange::new(start, start + rows as i64 - 1);
                start += rows as i64;
                Granule {
                    range,
                    file_index,
                    byte_ranges: std::iter::once(0..rows as u64).collect(),
                }
            })
            .collect()
    }

    #[test]
    fn a_single_large_granule_is_not_excluded_from_training() {
        for large_index in [0, 2_048, 4_095] {
            let granules = granules_with_rows((0..4_096).map(|index| {
                if index == large_index {
                    524_288
                } else {
                    128
                }
            }));
            for seed in 0..32 {
                let plan = select_first(&granules, 65_536, 65_536, seed).unwrap();
                // Row-proportional bytes force whole-shard when the large granule is hit.
                assert!(plan.training.is_none(), "index {large_index}, seed {seed}");
                assert_eq!(plan.first_rows, 4_095 * 128 + 524_288);
                assert!(plan.rest.is_empty());
            }
        }
    }

    #[test]
    fn static_units_merge_short_pages_and_the_trailing_remainder() {
        for page_rows in [64, 85, 128, 170, 256, 341] {
            let granules = granules_with_rows(vec![page_rows; 101]);
            let units = sampling_units(&granules);
            assert_eq!(units.first().unwrap().start, 0);
            assert_eq!(units.last().unwrap().end, granules.len());
            assert!(units.windows(2).all(|pair| pair[0].end == pair[1].start));
            assert!(units.iter().all(|unit| unit.len() * page_rows >= 256));
        }
        // A row-group tail can join the next file's first page; retain both indices.
        let granules = granules_with_rows([341, 40, 341, 40]);
        assert_eq!(sampling_units(&granules), vec![0..1, 1..4]);
        assert_ne!(granules[1].file_index, granules[2].file_index);

        // 170-row pages need two pages per unit, doubling first-pass rows here.
        let granules = granules_with_rows(vec![170; 16_384]);
        for seed in 0..3 {
            let plan = select_first(&granules, 65_536, 65_536, seed).unwrap();
            assert_eq!(plan.first_rows, 512 * 340);
            assert_eq!(plan.training.unwrap().len(), 65_536);
        }
    }

    #[test]
    fn systematic_unit_rows_have_equal_marginal_probability() {
        for (rows, quota) in [(341, 128), (256, 256), (129, 128)] {
            let mut counts = vec![0; rows];
            for offset in 0..rows {
                let selected = unit_training_offsets(rows, quota, offset).collect::<Vec<_>>();
                assert_eq!(selected.len(), quota);
                assert!(selected.windows(2).all(|pair| pair[0] < pair[1]));
                for row in selected {
                    assert!(row < rows);
                    counts[row] += 1;
                }
            }
            assert!(counts.iter().all(|count| *count == quota));
        }
    }

    #[test]
    fn training_shrink_preserves_exact_counts_and_shard_offsets() {
        let mut granules = granules_with_rows(vec![256; 4_096]);
        for granule in &mut granules {
            granule.range = RowRange::new(granule.range.from() + 17, granule.range.to() + 17);
        }
        for eligible in [1, 100, 127, 128, 129, 32_767, 32_768] {
            for seed in 0..3 {
                let plan = select_first(&granules, eligible, eligible, seed).unwrap();
                let training = plan.training.unwrap();
                assert_eq!(training.len(), eligible);
                assert!(training.windows(2).all(|pair| pair[0] < pair[1]));
                assert!(training
                    .iter()
                    .all(|row| (17..17 + 4_096 * 256).contains(row)));
                assert!(training.iter().all(|row| plan
                    .first
                    .iter()
                    .any(|range| { range.from() <= *row && *row <= range.to() })));
            }
        }
        // Three or more hits can exceed a 256-row unit: do not truncate quotas.
        let granules = granules_with_rows(vec![256; 64]);
        let plan = select_first(&granules, 128, 128, 0).unwrap();
        assert!(plan.training.is_none());
        assert_eq!(plan.first_rows, 16_384);
    }

    #[test]
    fn large_granule_training_is_row_weighted_when_the_byte_gate_allows_it() {
        let mut granules =
            granules_with_rows((0..4_096).map(|index| if index == 2_048 { 524_288 } else { 128 }));
        for granule in &mut granules {
            granule.byte_ranges = std::iter::once(0..1).collect();
        }
        let large = &granules[2_048].range;
        for seed in 0..32 {
            let training = select_first(&granules, 65_536, 65_536, seed)
                .unwrap()
                .training
                .unwrap();
            let large_rows = training
                .iter()
                .filter(|row| large.from() <= **row && **row <= large.to())
                .count();
            assert!(
                (32_512..=33_024).contains(&large_rows),
                "seed {seed}: {large_rows}"
            );
        }
    }

    #[test]
    fn minimum_granule_count_can_force_whole_shard_without_the_byte_gate() {
        let mut granules =
            granules_with_rows((0..4_096).map(|index| if index == 0 { 1_048_576 } else { 256 }));
        for granule in &mut granules {
            granule.byte_ranges = std::iter::once(0..1).collect();
        }
        // All units are single granules. Prove quota and byte gates pass, while
        // many row strata hit the same large granule and the count gate fails.
        let rows = granules.last().unwrap().range.to() as usize + 1;
        let mut quotas = vec![0; granules.len()];
        for row in pick_indices(rows, MIN_STRATA, &mut 7) {
            let index = granules.partition_point(|granule| granule.range.to() < row as i64);
            quotas[index] += ROWS_PER_STRATUM;
        }
        let selected = quotas
            .iter()
            .enumerate()
            .filter_map(|(index, quota)| (*quota > 0).then_some(index))
            .collect::<HashSet<_>>();
        assert!(quotas
            .iter()
            .zip(&granules)
            .all(|(quota, granule)| { *quota <= granule.range.count() as usize }));
        assert!(selected.len() < MIN_STRATA);
        assert!(
            granule_bytes(&granules, Some(&selected)) * FIRST_BYTES_DENOMINATOR
                <= granule_bytes(&granules, None) * FIRST_BYTES_NUMERATOR
        );
        assert!(select_first(&granules, 32_768, 32_768, 7)
            .unwrap()
            .training
            .is_none());
    }

    #[test]
    fn stratified_first_and_rest_cover_every_granule_once() {
        let granules = granules_with_rows(vec![256; 4_096]);
        let plan = select_first(&granules, 65_536, 32_768, 7).unwrap();
        let training = plan.training.as_ref().unwrap();
        assert_eq!(training.len(), 32_768);
        assert!(training.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(training.iter().all(|row| plan
            .first
            .iter()
            .any(|range| range.from() <= *row && *row <= range.to())));
        let mut all = plan.first.clone();
        all.extend(plan.rest.clone());
        assert_eq!(
            merge_row_ranges(all),
            vec![RowRange::new(0, 4_096 * 256 - 1)]
        );
        assert_eq!(
            plan.first_rows
                + plan
                    .rest
                    .iter()
                    .map(|range| range.count() as usize)
                    .sum::<usize>(),
            4_096 * 256
        );
        for stratum in 0..256 {
            let (start, end) = (stratum * 4_096, (stratum + 1) * 4_096);
            assert_eq!(
                training
                    .iter()
                    .filter(|row| start <= **row && **row < end)
                    .count(),
                128,
                "stratum {stratum}"
            );
        }
        assert_eq!(
            plan.training,
            select_first(&granules, 65_536, 32_768, 7).unwrap().training
        );
        assert_ne!(
            plan.training,
            select_first(&granules, 65_536, 32_768, 8).unwrap().training
        );
    }

    #[test]
    fn shard_seed_follows_the_whole_shard_identity() {
        let shard = VindexIndexShard {
            partition: crate::spec::BinaryRow::new(0),
            partition_bytes: vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
            files: Vec::new(),
            row_range_start: 0,
            row_range_end: 999,
            snapshot_id: 3,
            source_bucket: 0,
            total_buckets: 2,
            bucket_path: "memory:/t/bucket-0".to_string(),
        };
        let picks = |shard: &VindexIndexShard| {
            pick_indices(4_096, 512, &mut shard_seed(shard)).collect::<Vec<_>>()
        };
        assert_eq!(picks(&shard), picks(&shard.clone()));

        let mut others = vec![shard.clone(); 5];
        others[0].partition_bytes = vec![1, 2, 3, 4, 5, 6, 7, 8, 10];
        others[1].partition_bytes = Vec::new();
        others[2].source_bucket = 1;
        others[3].snapshot_id = 4;
        others[4].row_range_end = 1_999;
        for other in &others {
            assert_ne!(picks(&shard), picks(other), "{other:?}");
        }
    }

    #[test]
    fn first_picks_do_not_alias_with_periodic_granule_order() {
        let granules = granules_with_rows(vec![256; 4_096]);
        for seed in 0..32u64 {
            let training = select_first(&granules, 65_536, 65_536, seed)
                .unwrap()
                .training
                .unwrap();
            for period in [2usize, 4, 8, 16] {
                let mut hits = vec![0usize; period];
                for row in &training {
                    hits[*row as usize / 256 % period] += 1;
                }
                let expected = training.len() / period;
                assert!(
                    hits.iter()
                        .all(|count| *count * 4 > expected && *count < expected * 2),
                    "seed {seed} period {period}: {hits:?}"
                );
            }
        }
    }

    #[test]
    fn unequal_granules_are_sampled_in_proportion() {
        let granules =
            granules_with_rows((0..4_096).map(|index| if index % 8 == 4 { 16 } else { 256 }));
        let short_share = 16.0 / (16.0 + 7.0 * 256.0);
        for seed in 0..32u64 {
            let training = select_first(&granules, 65_536, 65_536, seed)
                .unwrap()
                .training
                .unwrap();
            assert_eq!(training.len(), 65_536);
            let short_rows = training
                .iter()
                .filter(|row| granules.partition_point(|g| g.range.to() < **row) % 8 == 4)
                .count();
            let share = short_rows as f64 / training.len() as f64;
            assert!(
                share > short_share / 2.0 && share < short_share * 2.0,
                "seed {seed}: {share}"
            );
        }
    }

    #[test]
    fn short_granules_use_static_units_instead_of_filling_from_the_head() {
        let granules = granules_with_rows(vec![64; 4_096]);
        let plan = select_first(&granules, 32_768, 32_768, 7).unwrap();
        assert_eq!(plan.first_rows, 65_536);
        let training = plan.training.unwrap();
        for quarter in 0..4 {
            let picked = training
                .iter()
                .filter(|row| **row / 65_536 == quarter)
                .count();
            assert_eq!(picked, 8_192, "quarter {quarter}");
        }
    }

    #[test]
    fn coalesced_page_holes_force_all_first() {
        let mut granules = granules_with_rows(vec![256; 4_096]);
        assert!(select_first(&granules, 65_536, 65_536, 7)
            .unwrap()
            .training
            .is_some());
        for (index, granule) in granules.iter_mut().enumerate() {
            granule.file_index = 0;
            granule.byte_ranges =
                std::iter::once(index as u64 * 512..index as u64 * 512 + 256).collect();
        }
        // Same rows, but coalescing reads the holes too: the byte gate alone changes the path.
        assert!(select_first(&granules, 65_536, 65_536, 7)
            .unwrap()
            .training
            .is_none());
    }

    #[test]
    fn coarse_granules_read_all_first() {
        let granules = (0..16)
            .map(|row| Granule {
                range: RowRange::new(row, row),
                file_index: 0,
                byte_ranges: std::iter::once(row as u64..row as u64 + 1).collect(),
            })
            .collect::<Vec<_>>();
        assert!(select_first(&granules, 8, 8, 7).unwrap().training.is_none());
    }

    #[test]
    fn near_full_shard_in_shared_file_reads_all_first() {
        let file_granules = (0..1_200)
            .map(|row| ParquetGranule {
                first_row: row * 256,
                row_count: 256,
                byte_ranges: std::iter::once(
                    row as u64 * 2 * 1024 * 1024..row as u64 * 2 * 1024 * 1024 + 1,
                )
                .collect(),
            })
            .collect();
        let shard_range = RowRange::new(400 * 256, 700 * 256 - 1);
        let mut granules = Vec::new();

        append_shard_granules(&mut granules, 0, 0, &shard_range, file_granules).unwrap();

        assert_eq!(granules.len(), 300);
        assert!(select_first(&granules, 32_768, 32_768, 7)
            .unwrap()
            .training
            .is_none());
    }

    #[test]
    fn granule_partition_requires_exact_contiguous_coverage() {
        for (ranges, shard, expected) in [
            (vec![], (0, 9), false),
            (vec![(0, 9)], (0, 9), true),
            (vec![(0, 4), (5, 9)], (0, 9), true),
            (vec![(0, 4), (6, 9)], (0, 9), false),
            (vec![(0, 5), (5, 9)], (0, 9), false),
            (vec![(1, 9)], (0, 9), false),
            (vec![(0, 8)], (0, 9), false),
            (vec![(0, 9), (0, 9)], (0, 9), false),
            (vec![(0, 2), (6, 7), (3, 5), (8, 9)], (0, 9), false),
            (vec![(i64::MAX, i64::MAX)], (i64::MAX, i64::MAX), true),
            (
                vec![(i64::MAX - 1, i64::MAX - 1), (i64::MAX, i64::MAX)],
                (i64::MAX - 1, i64::MAX),
                true,
            ),
            (
                vec![(i64::MAX, i64::MAX), (i64::MAX, i64::MAX)],
                (i64::MAX, i64::MAX),
                false,
            ),
        ] {
            let granules = ranges
                .iter()
                .map(|&(from, to)| Granule {
                    range: RowRange::new(from, to),
                    file_index: 0,
                    byte_ranges: vec![],
                })
                .collect::<Vec<_>>();
            assert_eq!(
                granules_partition_shard(&granules, &RowRange::new(shard.0, shard.1)),
                expected,
                "ranges={ranges:?}, shard={shard:?}"
            );
        }
    }

    #[test]
    fn overlapping_data_evolution_providers_do_not_partition_shard() {
        let shard_range = RowRange::new(0, 99);
        let mut granules = Vec::new();
        for file_index in 0..2 {
            append_shard_granules(
                &mut granules,
                file_index,
                0,
                &shard_range,
                vec![ParquetGranule {
                    first_row: 0,
                    row_count: 100,
                    byte_ranges: std::iter::once(0..1).collect(),
                }],
            )
            .unwrap();
        }
        granules.sort_by_key(|granule| granule.range.from());

        assert!(!granules_partition_shard(&granules, &shard_range));
        granules.truncate(1);
        assert!(granules_partition_shard(&granules, &shard_range));
    }

    #[test]
    fn live_pipeline_completes_with_one_blocking_thread() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let vectors = vec![0.0f32, 0.0, 1.0, 1.0, 2.0, 2.0];
                let ids = vec![0, 1, 2];
                let config =
                    paimon_vindex_core::index::VectorIndexConfig::from_options(&HashMap::from([
                        ("index.type".to_string(), "ivf_flat".to_string()),
                        ("dimension".to_string(), "2".to_string()),
                        ("nlist".to_string(), "1".to_string()),
                        ("metric".to_string(), "l2".to_string()),
                    ]))
                    .unwrap();
                let mut trainer = VectorIndexTrainer::new(config).unwrap();
                trainer
                    .add_training_vectors_mut(&vectors, ids.len())
                    .unwrap();

                let spill = spawn_spill_writer(false).unwrap();
                for _ in 0..2 {
                    spill
                        .sender
                        .send(SpillRecord {
                            ids: Vec::new(),
                            bytes: Vec::new(),
                        })
                        .await
                        .unwrap();
                }
                spill
                    .sender
                    .send(SpillRecord {
                        ids,
                        bytes: vectors
                            .iter()
                            .flat_map(|value| value.to_ne_bytes())
                            .collect(),
                    })
                    .await
                    .unwrap();
                let training = tokio::task::spawn_blocking(move || -> std::io::Result<_> {
                    Ok((trainer.finish()?, Duration::ZERO))
                });

                let (pipeline, _) =
                    start_live_pipeline(training, spill, "embedding".to_string(), 2, false)
                        .await
                        .unwrap();
                let (consumer, replay, _, _) = finish_live_pipeline(pipeline).await;
                let (_, rows_added, consumer_replay_rows, _) = consumer.unwrap();
                let (replay_rows, _) = replay.unwrap();
                assert_eq!((rows_added, consumer_replay_rows, replay_rows), (3, 3, 3));
            })
            .await
            .expect("vindex live pipeline deadlocked on the shared blocking pool");
        });
    }

    #[tokio::test]
    async fn spill_reports_written_bytes() {
        let spill = spawn_spill_writer(true).unwrap();
        spill
            .sender
            .send(SpillRecord {
                ids: vec![1, 2],
                bytes: vec![0; 16],
            })
            .await
            .unwrap();

        let (file, bytes, _) = spill.finish().await.unwrap();
        assert_eq!(bytes, 40);

        let (sender, mut receiver) = mpsc::channel(1);
        let (rows, _) = spawn_replay(file, sender, 2, true).await.unwrap().unwrap();
        assert_eq!(rows, 2);
        assert!(matches!(
            receiver.recv().await,
            Some(AddItem::Spilled(ids, _)) if ids == vec![1, 2]
        ));
    }
}
