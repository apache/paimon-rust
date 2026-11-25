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

//! Bounded split execution shared by table readers and language bindings.

use super::{read_limit::take_limited_batch, ArrowRecordBatchStream};
use crate::DataSplit;
use arrow_array::RecordBatch;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

const OUTPUT_BYTE_UNIT: u64 = 1024 * 1024;

fn budget_permits(max_bytes: u64) -> u32 {
    (max_bytes / OUTPUT_BYTE_UNIT).clamp(1, u32::MAX as u64) as u32
}

pub(super) struct BlobOutputBudget {
    pub max_bytes: u64,
    pub fields: HashSet<String>,
    pub unknown_external: bool,
    pub serialize_unknown: bool,
}

pub(super) type SplitReader =
    Arc<dyn Fn(&[DataSplit], Option<usize>) -> crate::Result<ArrowRecordBatchStream> + Send + Sync>;

type NextBatch = (
    usize,
    Option<crate::Result<RecordBatch>>,
    ArrowRecordBatchStream,
);

/// A contiguous minimax partition balances file bytes without changing split order.
fn split_groups(splits: Vec<DataSplit>, parallelism: usize) -> Vec<Vec<DataSplit>> {
    let weights = splits
        .iter()
        .map(|split| {
            split
                .data_files()
                .iter()
                .fold(0_u64, |sum, file| {
                    sum.saturating_add(u64::try_from(file.file_size).unwrap_or(0))
                })
                .max(1)
        })
        .collect::<Vec<_>>();
    let ranges = group_ranges(&weights, parallelism);
    let mut splits = splits.into_iter();
    ranges
        .into_iter()
        .map(|(start, end)| splits.by_ref().take(end - start).collect())
        .collect()
}

fn group_ranges(weights: &[u64], parallelism: usize) -> Vec<(usize, usize)> {
    let workers = parallelism.min(weights.len());
    if workers == 0 {
        return Vec::new();
    }
    let mut low = *weights.iter().max().unwrap();
    let mut high = weights.iter().copied().fold(0_u64, u64::saturating_add);
    while low < high {
        let capacity = low + (high - low) / 2;
        let mut needed = 1;
        let mut current = 0_u64;
        for &weight in weights {
            if current.saturating_add(weight) > capacity {
                needed += 1;
                current = weight;
            } else {
                current = current.saturating_add(weight);
            }
        }
        if needed <= workers {
            high = capacity;
        } else {
            low = capacity + 1;
        }
    }
    let mut ranges = Vec::with_capacity(workers);
    let mut start = 0;
    for worker in 0..workers {
        let left = workers - worker;
        if left == 1 {
            ranges.push((start, weights.len()));
            break;
        }
        let mut end = start;
        let mut weight = 0_u64;
        while end < weights.len() - (left - 1) {
            let next = weight.saturating_add(weights[end]);
            if end > start && next > low {
                break;
            }
            weight = next;
            end += 1;
        }
        ranges.push((start, end));
        start = end;
    }
    ranges
}

pub(super) fn execute(
    splits: Vec<DataSplit>,
    parallelism: usize,
    ordered: bool,
    limit: Option<usize>,
    read: SplitReader,
    blob_output_budget: Option<BlobOutputBudget>,
) -> ArrowRecordBatchStream {
    // Finite LIMIT uses the existing executor to cancel speculative splits at
    // the global row quota; the output budget is for unbounded reads.
    if ordered && parallelism > 1 {
        if limit.is_none() {
            if let Some(budget) = &blob_output_budget {
                let costs = splits
                    .iter()
                    .map(|split| blob_group_cost(std::slice::from_ref(split), budget))
                    .collect::<Vec<_>>();
                if costs.iter().any(Option::is_some) {
                    return merge_items_budgeted(
                        splits,
                        costs,
                        budget_permits(budget.max_bytes) as usize,
                        ordered,
                        parallelism,
                        move |split| read_group(vec![split], Arc::clone(&read), limit),
                    );
                }
            }
        }
        // Keep unscheduled splits as data; create a stream only when a slot opens.
        return merge_items_bounded(splits, true, limit, parallelism, move |split| {
            read_group(vec![split], Arc::clone(&read), limit)
        });
    }
    let groups = split_groups(splits, parallelism);
    if limit.is_none() && groups.len() > 1 {
        if let Some(budget) = &blob_output_budget {
            let costs = groups
                .iter()
                .map(|group| blob_group_cost(group, budget))
                .collect::<Vec<_>>();
            if costs.iter().any(Option::is_some) {
                return merge_items_budgeted(
                    groups,
                    costs,
                    budget_permits(budget.max_bytes) as usize,
                    ordered,
                    parallelism,
                    move |group| read_group(group, Arc::clone(&read), limit),
                );
            }
        }
    }
    let streams = groups
        .into_iter()
        .map(|group| read_group(group, Arc::clone(&read), limit))
        .collect();
    merge_streams(streams, ordered, limit)
}

/// Estimate decoded bytes from managed BLOB files; unknown sizes need opt-in.
fn blob_group_cost(group: &[DataSplit], budget: &BlobOutputBudget) -> Option<u32> {
    let permits = budget_permits(budget.max_bytes);
    if budget.unknown_external {
        return budget.serialize_unknown.then_some(permits);
    }
    let mut file_bytes = 0_u64;
    let mut has_unknown = false;
    for split in group {
        let mut bytes = 0_u64;
        let mut unknown = false;
        for file in split.data_files() {
            if file.file_name.ends_with(".blob")
                && file
                    .write_cols
                    .as_ref()
                    .is_none_or(|cols| cols.iter().any(|col| budget.fields.contains(col)))
            {
                bytes = bytes.saturating_add(u64::try_from(file.file_size).unwrap_or(0));
            } else if file
                .write_cols
                .as_ref()
                .is_none_or(|cols| cols.iter().any(|col| budget.fields.contains(col)))
            {
                unknown = true;
            }
        }
        if unknown || bytes == 0 {
            has_unknown = true;
        }
        file_bytes = file_bytes.max(bytes);
    }
    if has_unknown && budget.serialize_unknown {
        return Some(permits);
    }
    if file_bytes == 0 {
        return None;
    }
    Some(
        file_bytes
            .max(1)
            .div_ceil(OUTPUT_BYTE_UNIT)
            .min(permits as u64) as u32,
    )
}

pub(super) fn blob_budget_applies_to_splits(
    splits: &[DataSplit],
    budget: &BlobOutputBudget,
) -> bool {
    splits
        .iter()
        .any(|split| blob_group_cost(std::slice::from_ref(split), budget).is_some())
}

fn read_group(
    group: Vec<DataSplit>,
    read: SplitReader,
    limit: Option<usize>,
) -> ArrowRecordBatchStream {
    Box::pin(async_stream::try_stream! {
        let mut remaining = limit;
        for split in group {
            if remaining == Some(0) { break; }
            // Open later splits only on demand, with the remaining quota.
            let mut stream = read(std::slice::from_ref(&split), remaining)?;
            while remaining != Some(0) {
                let Some(batch) = stream.next().await else { break; };
                let batch = batch?;
                if batch.num_rows() == 0 { continue; }
                yield take_limited_batch(batch, &mut remaining);
            }
        }
    })
}

fn next_batch(index: usize, mut stream: ArrowRecordBatchStream) -> BoxFuture<'static, NextBatch> {
    async move {
        let batch = stream.next().await;
        (index, batch, stream)
    }
    .boxed()
}

enum BudgetedState<T> {
    Waiting(T),
    Open(ArrowRecordBatchStream),
}

struct BudgetedRead<T> {
    index: usize,
    state: BudgetedState<T>,
    cost: Option<u32>,
}

struct BudgetedBatch<T> {
    read: BudgetedRead<T>,
    batch: Option<crate::Result<RecordBatch>>,
    _permit: Option<OwnedSemaphorePermit>,
}

fn schedule_budgeted<T, F>(
    waiting: &mut VecDeque<BudgetedRead<T>>,
    pending: &mut JoinSet<BudgetedBatch<T>>,
    permits: &Arc<Semaphore>,
    handle: &tokio::runtime::Handle,
    max_workers: usize,
    buffered: usize,
    make_stream: &F,
) where
    T: Send + 'static,
    F: Fn(T) -> ArrowRecordBatchStream,
{
    while pending.len() + buffered < max_workers {
        let Some(read) = waiting.front() else {
            break;
        };
        let permit = match read.cost {
            Some(cost) => {
                let Ok(permit) = Arc::clone(permits).try_acquire_many_owned(cost) else {
                    break;
                };
                Some(permit)
            }
            None => None,
        };
        let BudgetedRead { index, state, cost } = waiting.pop_front().unwrap();
        let mut stream = match state {
            BudgetedState::Waiting(item) => make_stream(item),
            BudgetedState::Open(stream) => stream,
        };
        pending.spawn_on(
            async move {
                let batch = stream.next().await;
                BudgetedBatch {
                    read: BudgetedRead {
                        index,
                        state: BudgetedState::Open(stream),
                        cost,
                    },
                    batch,
                    _permit: permit,
                }
            },
            handle,
        );
    }
}

/// Hold a permit through decoding and until the caller resumes the stream.
fn merge_items_budgeted<T, F>(
    items: Vec<T>,
    costs: Vec<Option<u32>>,
    permits: usize,
    ordered: bool,
    parallelism: usize,
    make_stream: F,
) -> ArrowRecordBatchStream
where
    T: Send + 'static,
    F: Fn(T) -> ArrowRecordBatchStream + Send + 'static,
{
    debug_assert_eq!(items.len(), costs.len());
    debug_assert!(costs
        .iter()
        .flatten()
        .all(|cost| *cost > 0 && *cost as usize <= permits));
    Box::pin(async_stream::try_stream! {
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(error) => {
                drop(items);
                Err(crate::Error::UnexpectedError {
                    message: "Parallel table reads require a Tokio runtime".into(),
                    source: Some(Box::new(error)),
                })?;
                return;
            }
        };
        let count = items.len();
        let permits = Arc::new(Semaphore::new(permits));
        let mut waiting = items.into_iter().zip(costs).enumerate()
            .map(|(index, (item, cost))| BudgetedRead {
                index,
                state: BudgetedState::Waiting(item),
                cost,
            })
            .collect::<VecDeque<_>>();
        let mut pending = JoinSet::new();
        let mut ready = BTreeMap::new();
        let mut completed = BTreeSet::new();
        let mut current = 0;
        schedule_budgeted(&mut waiting, &mut pending, &permits, &handle, parallelism, ready.len(), &make_stream);
        while current < count {
            if ordered && completed.remove(&current) {
                current += 1;
                continue;
            }
            let next = if ordered {
                if let Some(next) = ready.remove(&current) {
                    Ok(Some(next))
                } else {
                    pending.join_next().await.transpose().map_err(worker_error)
                }
            } else {
                pending.join_next().await.transpose().map_err(worker_error)
            };
            let next = match next {
                Ok(Some(next)) => next,
                Ok(None) => break,
                Err(error) => {
                    drop(ready);
                    waiting.clear();
                    pending.shutdown().await;
                    Err(error)?;
                    break;
                }
            };
            let index = next.read.index;
            if next.batch.is_none() {
                drop(next);
                if ordered {
                    completed.insert(index);
                } else {
                    current += 1;
                }
                schedule_budgeted(&mut waiting, &mut pending, &permits, &handle, parallelism, ready.len(), &make_stream);
                continue;
            }
            if ordered && index != current {
                if next.batch.as_ref().is_some_and(Result::is_err) {
                    let BudgetedBatch { read, batch, _permit } = next;
                    let error = batch.unwrap().unwrap_err();
                    drop(read);
                    drop(_permit);
                    drop(ready);
                    waiting.clear();
                    pending.shutdown().await;
                    Err(error)?;
                    break;
                }
                ready.insert(index, next);
                continue;
            }
            let BudgetedBatch { read, batch, _permit } = next;
            match batch.unwrap() {
                Err(error) => {
                    drop(read);
                    drop(_permit);
                    drop(ready);
                    waiting.clear();
                    pending.shutdown().await;
                    Err(error)?;
                    break;
                }
                Ok(batch) => {
                    if batch.num_rows() > 0 {
                        yield batch;
                    }
                    // A paused caller keeps the permit and the current batch.
                    drop(_permit);
                    if ordered { waiting.push_front(read); } else { waiting.push_back(read); }
                    schedule_budgeted(&mut waiting, &mut pending, &permits, &handle, parallelism, ready.len(), &make_stream);
                }
            }
        }
    })
}

#[cfg(test)]
fn merge_streams_budgeted(
    streams: Vec<ArrowRecordBatchStream>,
    costs: Vec<u32>,
    permits: usize,
    ordered: bool,
    parallelism: usize,
) -> ArrowRecordBatchStream {
    merge_items_budgeted(
        streams,
        costs.into_iter().map(Some).collect(),
        permits,
        ordered,
        parallelism,
        |stream| stream,
    )
}

/// Every group has one pending read or one ready batch. Multiple groups run
/// on owned runtime tasks so synchronous decoder work can use separate CPUs.
/// Dropping the output aborts every task through JoinSet's drop implementation.
fn merge_streams(
    streams: Vec<ArrowRecordBatchStream>,
    ordered: bool,
    limit: Option<usize>,
) -> ArrowRecordBatchStream {
    let parallelism = streams.len();
    merge_streams_bounded(streams, ordered, limit, parallelism)
}

fn merge_streams_bounded(
    streams: Vec<ArrowRecordBatchStream>,
    ordered: bool,
    limit: Option<usize>,
    parallelism: usize,
) -> ArrowRecordBatchStream {
    merge_items_bounded(streams, ordered, limit, parallelism, |stream| stream)
}

fn merge_items_bounded<T, F>(
    mut items: Vec<T>,
    ordered: bool,
    limit: Option<usize>,
    parallelism: usize,
    make_stream: F,
) -> ArrowRecordBatchStream
where
    T: Send + 'static,
    F: Fn(T) -> ArrowRecordBatchStream + Send + 'static,
{
    if limit == Some(0) {
        return Box::pin(futures::stream::empty());
    }
    // Ordinary one-worker reads retain the original executor context and avoid
    // requiring a Tokio runtime just to advance a stream.
    if items.len() <= 1 {
        return limit_single_stream(
            items
                .pop()
                .map(make_stream)
                .unwrap_or_else(|| Box::pin(futures::stream::empty())),
            limit,
        );
    }
    Box::pin(async_stream::try_stream! {
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(error) => {
                drop(items);
                Err(crate::Error::UnexpectedError {
                    message: "Parallel table reads require a Tokio runtime".into(),
                    source: Some(Box::new(error)),
                })?;
                return;
            }
        };
        let count = items.len();
        let mut unscheduled = VecDeque::from(items);
        let mut next_index = 0;
        let mut pending = JoinSet::new();
        // A worker panic has no returned stream index. Keep its task ID so a
        // finite ordered read can defer the failure until that split is needed.
        let mut task_indices = HashMap::new();
        for _ in 0..parallelism.min(count) {
            if let Some(item) = unscheduled.pop_front() {
                let index = next_index;
                next_index += 1;
                let task = pending.spawn_on(next_batch(index, make_stream(item)), &handle);
                if ordered && limit.is_some() {
                    task_indices.insert(task.id(), index);
                }
            }
        }
        let mut ready = BTreeMap::new();
        let mut ready_errors = BTreeMap::new();
        let mut completed = BTreeSet::new();
        let mut current = 0;
        let mut remaining = limit;
        while remaining != Some(0) && current < count {
            let next = if ordered {
                loop {
                    if completed.remove(&current) {
                        current += 1;
                        if current == count { break Ok(None); }
                        // Empty speculative splits may have freed all worker
                        // slots while the current split was still blocked.
                        // Refill only as the ordered output position advances.
                        if limit.is_some() {
                            let end = current.saturating_add(parallelism.saturating_mul(2));
                            while next_index < end && pending.len() + ready.len() + ready_errors.len() < parallelism {
                                let Some(item) = unscheduled.pop_front() else { break; };
                                let index = next_index;
                                next_index += 1;
                                let task = pending.spawn_on(next_batch(index, make_stream(item)), &handle);
                                task_indices.insert(task.id(), index);
                            }
                        }
                        continue;
                    }
                    if let Some(error) = ready_errors.remove(&current) { break Err(error); }
                    if let Some(next) = ready.remove(&current) { break Ok(Some(next)); }
                    let next = match pending.join_next_with_id().await {
                        Some(Ok((id, next))) => {
                            task_indices.remove(&id);
                            next
                        }
                        Some(Err(error)) if limit.is_some() => {
                            let index = task_indices.remove(&error.id())
                                .expect("ordered worker must have a split index");
                            ready_errors.insert(index, worker_error(error));
                            continue;
                        }
                        Some(Err(error)) => break Err(worker_error(error)),
                        None => break Ok(None),
                    };
                    if next.1.is_none() {
                        // An empty later split frees its worker slot immediately.
                        // Keep only its completion marker until ordered output
                        // reaches it. A finite LIMIT also caps lookahead.
                        completed.insert(next.0);
                        drop(next.2);
                        let end = current.saturating_add(parallelism.saturating_mul(2));
                        if limit.is_none() || next_index < end {
                            if let Some(item) = unscheduled.pop_front() {
                                let index = next_index;
                                next_index += 1;
                                let task = pending.spawn_on(next_batch(index, make_stream(item)), &handle);
                                if limit.is_some() {
                                    task_indices.insert(task.id(), index);
                                }
                            }
                        }
                        continue;
                    }
                    // A finite ordered LIMIT may finish before a later split
                    // is needed, so defer speculative failures in that case.
                    if next.0 == current || (limit.is_none() && next.1.as_ref().is_some_and(Result::is_err)) {
                        break Ok(Some(next));
                    }
                    ready.insert(next.0, next);
                }
            } else {
                pending.join_next().await.transpose().map_err(worker_error)
            };
            let (index, batch, stream) = match next {
                Ok(Some(next)) => next,
                Ok(None) => break,
                Err(error) => {
                    drop(ready);
                    unscheduled.clear();
                    pending.shutdown().await;
                    Err(error)?;
                    break;
                }
            };
            match batch {
                Some(Err(error)) => {
                    // async_stream's `?` yields before returning. Release every
                    // worker first, even if the caller retains the errored stream.
                    drop(stream);
                    drop(ready);
                    unscheduled.clear();
                    pending.shutdown().await;
                    Err(error)?;
                    break;
                }
                Some(Ok(batch)) => {
                    if batch.num_rows() > 0 {
                        let batch = take_limited_batch(batch, &mut remaining);
                        if remaining == Some(0) {
                            // Reaching the quota cancels unused work before the
                            // final batch is handed to the caller.
                            drop(stream);
                            drop(ready);
                            unscheduled.clear();
                            pending.shutdown().await;
                            yield batch;
                            break;
                        }
                        yield batch;
                    }
                    // Schedule another decode only after the caller resumes.
                    let task = pending.spawn_on(next_batch(index, stream), &handle);
                    if ordered && limit.is_some() {
                        task_indices.insert(task.id(), index);
                    }
                }
                None => {},
            }
        }
    })
}

fn worker_error(error: tokio::task::JoinError) -> crate::Error {
    crate::Error::UnexpectedError {
        message: format!("Table read worker failed: {error}"),
        source: Some(Box::new(error)),
    }
}

fn limit_single_stream(
    mut stream: ArrowRecordBatchStream,
    limit: Option<usize>,
) -> ArrowRecordBatchStream {
    Box::pin(async_stream::try_stream! {
        let mut remaining = limit;
        while remaining != Some(0) {
            let Some(batch) = stream.next().await else { break; };
            match batch {
                Err(error) => {
                    drop(stream);
                    Err(error)?;
                    break;
                }
                Ok(batch) => {
                    if batch.num_rows() == 0 { continue; }
                    let batch = take_limited_batch(batch, &mut remaining);
                    if remaining == Some(0) {
                        drop(stream);
                        yield batch;
                        break;
                    }
                    yield batch;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{BinaryRow, DataFileMeta};
    use crate::table::DataSplitBuilder;
    use arrow_array::Int32Array;
    use futures::channel::oneshot;
    use futures::TryStreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    async fn wait_for_count(count: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while count.load(Ordering::SeqCst) != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker did not reach its expected lifecycle state");
    }

    fn batch(value: i32) -> RecordBatch {
        RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int32Array::from(vec![value])) as arrow_array::ArrayRef,
        )])
        .unwrap()
    }

    fn value(batch: &RecordBatch) -> i32 {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0)
    }

    fn split_file(name: &str, bytes: i64, columns: Option<Vec<&str>>) -> DataFileMeta {
        DataFileMeta {
            file_name: name.into(),
            file_size: bytes,
            row_count: 1,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: columns.map(|columns| columns.into_iter().map(str::to_string).collect()),
            column_max_sequence_numbers: None,
        }
    }

    fn budget_split(files: Vec<DataFileMeta>) -> DataSplit {
        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/blob-budget".into())
            .with_total_buckets(1)
            .with_data_files(files)
            .build()
            .unwrap()
    }

    #[test]
    fn blob_output_reservation_requires_opt_in_for_unknown_sizes() {
        let mut budget = BlobOutputBudget {
            max_bytes: 64 * OUTPUT_BYTE_UNIT,
            fields: HashSet::from(["image".to_string()]),
            unknown_external: false,
            serialize_unknown: false,
        };
        let managed = budget_split(vec![
            split_file("key.parquet", 1024, Some(vec!["clip_id"])),
            split_file(
                "image.blob",
                17 * OUTPUT_BYTE_UNIT as i64,
                Some(vec!["image"]),
            ),
        ]);
        assert_eq!(
            blob_group_cost(std::slice::from_ref(&managed), &budget),
            Some(17)
        );
        let descriptor = budget_split(vec![split_file("refs.parquet", 1024, Some(vec!["image"]))]);
        assert_eq!(
            blob_group_cost(std::slice::from_ref(&descriptor), &budget),
            None
        );
        let mixed = budget_split(vec![
            split_file("refs.parquet", 1024, Some(vec!["image"])),
            split_file(
                "image.blob",
                17 * OUTPUT_BYTE_UNIT as i64,
                Some(vec!["image"]),
            ),
        ]);
        assert_eq!(
            blob_group_cost(std::slice::from_ref(&mixed), &budget),
            Some(17)
        );
        let smaller = budget_split(vec![split_file(
            "other.blob",
            OUTPUT_BYTE_UNIT as i64,
            Some(vec!["image"]),
        )]);
        assert_eq!(
            blob_group_cost(&[mixed.clone(), smaller], &budget),
            Some(17)
        );
        assert_eq!(
            blob_group_cost(&[managed.clone(), descriptor.clone()], &budget),
            Some(17)
        );
        budget.unknown_external = true;
        assert_eq!(
            blob_group_cost(std::slice::from_ref(&managed), &budget),
            None
        );
        budget.serialize_unknown = true;
        assert_eq!(blob_group_cost(&[managed], &budget), Some(64));
        budget.unknown_external = false;
        assert_eq!(blob_group_cost(&[descriptor], &budget), Some(64));
        assert_eq!(blob_group_cost(&[mixed], &budget), Some(64));
    }

    #[tokio::test]
    async fn projected_blob_column_does_not_reserve_unread_blob_files() {
        for ordered in [false, true] {
            let (release, wait) = oneshot::channel();
            let wait = Arc::new(Mutex::new(Some(wait)));
            let second_reads = Arc::new(AtomicUsize::new(0));
            let read: SplitReader = {
                let second_reads = second_reads.clone();
                Arc::new(move |splits, _| {
                    let first = splits[0].data_files()[0].file_name == "first-image.blob";
                    Ok(controlled_stream(
                        vec![if first { 1 } else { 2 }],
                        if first {
                            wait.lock().unwrap().take()
                        } else {
                            None
                        },
                        if first {
                            Arc::new(AtomicUsize::new(0))
                        } else {
                            second_reads.clone()
                        },
                        Arc::new(AtomicUsize::new(0)),
                    ))
                })
            };
            let splits = ["first", "second"]
                .into_iter()
                .map(|name| {
                    budget_split(vec![
                        split_file(
                            &format!("{name}-image.blob"),
                            OUTPUT_BYTE_UNIT as i64,
                            Some(vec!["image"]),
                        ),
                        split_file(
                            &format!("{name}-thumbnail.blob"),
                            2 * 1024 * OUTPUT_BYTE_UNIT as i64,
                            Some(vec!["thumbnail"]),
                        ),
                    ])
                })
                .collect::<Vec<_>>();
            let budget = BlobOutputBudget {
                max_bytes: 2 * OUTPUT_BYTE_UNIT,
                fields: HashSet::from(["image".to_string()]),
                unknown_external: false,
                serialize_unknown: false,
            };
            assert_eq!(blob_group_cost(&splits[..1], &budget), Some(1));
            assert_eq!(blob_group_cost(&splits[1..], &budget), Some(1));
            let mut output = execute(splits, 2, ordered, None, read, Some(budget));
            if ordered {
                assert!(output.next().now_or_never().is_none());
                wait_for_count(&second_reads, 1).await;
            } else {
                let batch = tokio::time::timeout(Duration::from_secs(2), output.next())
                    .await
                    .expect("the second projected BLOB split was serialized")
                    .unwrap()
                    .unwrap();
                assert_eq!(value(&batch), 2);
            }
            release.send(()).unwrap();
            let remaining = output
                .map_ok(|batch| value(&batch))
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(remaining, if ordered { vec![1, 2] } else { vec![1] });
        }
    }

    #[tokio::test]
    async fn unknown_pk_blob_splits_remain_parallel_by_default() {
        let (release, wait) = oneshot::channel();
        let wait = Arc::new(Mutex::new(Some(wait)));
        let read: SplitReader = Arc::new(move |splits, _| {
            let first = splits[0].data_files()[0].file_name == "first.parquet";
            let wait = if first {
                wait.lock().unwrap().take()
            } else {
                None
            };
            Ok(controlled_stream(
                vec![if first { 1 } else { 2 }],
                wait,
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
            ))
        });
        let splits = ["first.parquet", "second.parquet"]
            .into_iter()
            .map(|name| budget_split(vec![split_file(name, 1024, None)]))
            .collect();
        let mut output = execute(
            splits,
            2,
            false,
            None,
            read,
            Some(BlobOutputBudget {
                max_bytes: 64 * OUTPUT_BYTE_UNIT,
                fields: HashSet::from(["image".to_string()]),
                unknown_external: false,
                serialize_unknown: false,
            }),
        );
        let ready = tokio::time::timeout(Duration::from_secs(2), output.next())
            .await
            .expect("second PK split did not run concurrently")
            .unwrap()
            .unwrap();
        assert_eq!(value(&ready), 2);
        release.send(()).unwrap();
        assert_eq!(value(&output.next().await.unwrap().unwrap()), 1);
    }

    #[tokio::test]
    async fn mixed_unknown_splits_keep_known_blob_groups_budgeted() {
        for ordered in [false, true] {
            let (release, wait) = oneshot::channel();
            let wait = Arc::new(Mutex::new(Some(wait)));
            let first_reads = Arc::new(AtomicUsize::new(0));
            let unknown_reads = Arc::new(AtomicUsize::new(0));
            let last_reads = Arc::new(AtomicUsize::new(0));
            let read: SplitReader = {
                let first_reads = first_reads.clone();
                let unknown_reads = unknown_reads.clone();
                let last_reads = last_reads.clone();
                Arc::new(move |splits, _| {
                    let name = splits[0].data_files()[0].file_name.as_str();
                    let (value, wait, reads) = match name {
                        "first.blob" => (1, wait.lock().unwrap().take(), first_reads.clone()),
                        "unknown.parquet" => (2, None, unknown_reads.clone()),
                        "last.blob" => (3, None, last_reads.clone()),
                        _ => unreachable!(),
                    };
                    Ok(controlled_stream(
                        vec![value],
                        wait,
                        reads,
                        Arc::new(AtomicUsize::new(0)),
                    ))
                })
            };
            let splits = vec![
                budget_split(vec![split_file(
                    "first.blob",
                    2 * OUTPUT_BYTE_UNIT as i64,
                    Some(vec!["image"]),
                )]),
                budget_split(vec![split_file(
                    "unknown.parquet",
                    1024,
                    Some(vec!["image"]),
                )]),
                budget_split(vec![split_file(
                    "last.blob",
                    2 * OUTPUT_BYTE_UNIT as i64,
                    Some(vec!["image"]),
                )]),
            ];
            let budget = BlobOutputBudget {
                max_bytes: 2 * OUTPUT_BYTE_UNIT,
                fields: HashSet::from(["image".to_string()]),
                unknown_external: false,
                serialize_unknown: false,
            };
            assert!(blob_budget_applies_to_splits(&splits, &budget));
            let mut output = execute(splits, 3, ordered, None, read, Some(budget));
            if ordered {
                assert!(output.next().now_or_never().is_none());
            } else {
                assert_eq!(value(&output.next().await.unwrap().unwrap()), 2);
            }
            wait_for_count(&unknown_reads, 1).await;
            assert_eq!(last_reads.load(Ordering::SeqCst), 0);
            release.send(()).unwrap();
            let values = tokio::time::timeout(Duration::from_secs(5), async {
                output
                    .map_ok(|batch| value(&batch))
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
            })
            .await
            .expect("mixed BLOB output stalled");
            assert_eq!(values, if ordered { vec![1, 2, 3] } else { vec![1, 3] });
            assert_eq!(first_reads.load(Ordering::SeqCst), 1);
            assert_eq!(last_reads.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn fractional_budget_does_not_admit_groups_over_budget() {
        let max_bytes = OUTPUT_BYTE_UNIT * 3 / 2;
        let splits = (0..2)
            .map(|_| {
                budget_split(vec![split_file(
                    "image.blob",
                    (OUTPUT_BYTE_UNIT * 4 / 5) as i64,
                    Some(vec!["image"]),
                )])
            })
            .collect();
        let reads = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let read: SplitReader = {
            let reads = reads.clone();
            let drops = drops.clone();
            Arc::new(move |_, _| {
                Ok(controlled_stream(
                    vec![1],
                    None,
                    reads.clone(),
                    drops.clone(),
                ))
            })
        };
        let mut output = execute(
            splits,
            2,
            false,
            None,
            read,
            Some(BlobOutputBudget {
                max_bytes,
                fields: HashSet::from(["image".to_string()]),
                unknown_external: false,
                serialize_unknown: false,
            }),
        );
        assert!(output.next().await.unwrap().is_ok());
        tokio::task::yield_now().await;
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert_eq!(output.try_collect::<Vec<_>>().await.unwrap().len(), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    struct DropGuard(Arc<AtomicUsize>);
    impl Drop for DropGuard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn controlled_stream(
        values: Vec<i32>,
        first: Option<oneshot::Receiver<()>>,
        reads: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    ) -> ArrowRecordBatchStream {
        let guard = DropGuard(drops);
        Box::pin(async_stream::try_stream! {
            let _guard = guard;
            if let Some(first) = first { let _ = first.await; }
            for item in values {
                reads.fetch_add(1, Ordering::SeqCst);
                yield batch(item);
            }
        })
    }

    #[test]
    fn contiguous_groups_balance_skewed_files_without_reordering() {
        assert_eq!(
            group_ranges(&[100, 1, 1, 1, 1, 1], 3),
            vec![(0, 1), (1, 5), (5, 6)]
        );
        assert_eq!(group_ranges(&[1, 1, 1, 1], 2), vec![(0, 2), (2, 4)]);
        assert_eq!(group_ranges(&[1, 1], 8), vec![(0, 1), (1, 2)]);
        assert!(group_ranges(&[], 2).is_empty());
    }

    #[tokio::test]
    async fn ordered_execution_prefetches_one_batch_per_group() {
        let (release, wait) = oneshot::channel();
        let reads = Arc::new(AtomicUsize::new(0));
        let later_reads = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut output = merge_streams(
            vec![
                controlled_stream(vec![1, 2], Some(wait), reads.clone(), drops.clone()),
                controlled_stream(vec![3, 4], None, later_reads.clone(), drops.clone()),
            ],
            true,
            None,
        );
        assert!(output.next().now_or_never().is_none());
        wait_for_count(&later_reads, 1).await;
        assert_eq!(later_reads.load(Ordering::SeqCst), 1);
        release.send(()).unwrap();
        assert_eq!(value(&output.next().await.unwrap().unwrap()), 1);
        assert_eq!(later_reads.load(Ordering::SeqCst), 1);
        let rest = output
            .map_ok(|batch| value(&batch))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(rest, vec![2, 3, 4]);
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn ordered_split_window_opens_later_splits_as_earlier_ones_finish() {
        let (release, wait) = oneshot::channel();
        let reads = (0..4)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut first = Some(wait);
        let streams = reads
            .iter()
            .enumerate()
            .map(|(index, reads)| {
                controlled_stream(
                    vec![index as i32],
                    if index == 0 { first.take() } else { None },
                    reads.clone(),
                    drops.clone(),
                )
            })
            .collect();
        let mut output = merge_streams_bounded(streams, true, None, 2);
        assert!(output.next().now_or_never().is_none());
        wait_for_count(&reads[1], 1).await;
        assert_eq!(reads[2].load(Ordering::SeqCst), 0);
        assert_eq!(reads[3].load(Ordering::SeqCst), 0);
        release.send(()).unwrap();
        let values = output
            .map_ok(|batch| value(&batch))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(values, vec![0, 1, 2, 3]);
        assert!(reads.iter().all(|count| count.load(Ordering::SeqCst) == 1));
        assert_eq!(drops.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn ordered_split_window_replaces_empty_later_split_before_first_finishes() {
        for limit in [None, Some(1)] {
            let (release, wait) = oneshot::channel();
            let reads = Arc::new(AtomicUsize::new(0));
            let drops = Arc::new(AtomicUsize::new(0));
            let streams = vec![
                controlled_stream(vec![], Some(wait), reads.clone(), drops.clone()),
                controlled_stream(vec![], None, reads.clone(), drops.clone()),
                controlled_stream(vec![2], None, reads.clone(), drops.clone()),
            ];
            let output = merge_streams_bounded(streams, true, limit, 2);
            let result = tokio::spawn(async move {
                output
                    .map_ok(|batch| value(&batch))
                    .try_collect::<Vec<_>>()
                    .await
            });
            // Split 1 is empty. Its EOF must open split 2 while split 0 is
            // still blocked, rather than waiting for split 0 to finish.
            wait_for_count(&reads, 1).await;
            release.send(()).unwrap();
            assert_eq!(result.await.unwrap().unwrap(), vec![2]);
            assert_eq!(drops.load(Ordering::SeqCst), 3);
        }
    }

    #[tokio::test]
    async fn ordered_limit_discards_unscheduled_splits() {
        let reads = (0..4)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>();
        let drops = Arc::new(AtomicUsize::new(0));
        let streams = reads
            .iter()
            .enumerate()
            .map(|(index, reads)| {
                controlled_stream(vec![index as i32], None, reads.clone(), drops.clone())
            })
            .collect();
        let mut output = merge_streams_bounded(streams, true, Some(1), 2);
        assert_eq!(value(&output.next().await.unwrap().unwrap()), 0);
        assert!(output.next().await.is_none());
        assert!(reads[2..]
            .iter()
            .all(|count| count.load(Ordering::SeqCst) == 0));
        assert_eq!(drops.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn ordered_limit_creates_streams_only_for_scheduled_splits() {
        let created = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&created);
        let mut output =
            merge_items_bounded((0..50_000).collect(), true, Some(1), 2, move |index| {
                count.fetch_add(1, Ordering::SeqCst);
                Box::pin(futures::stream::once(async move { Ok(batch(index)) }))
            });
        assert_eq!(created.load(Ordering::SeqCst), 0);
        assert_eq!(value(&output.next().await.unwrap().unwrap()), 0);
        assert!(output.next().await.is_none());
        assert_eq!(created.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn ordered_limit_caps_lookahead_across_empty_splits() {
        let (release, wait) = oneshot::channel();
        let first = std::sync::Mutex::new(Some(wait));
        let created = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&created);
        let output = merge_items_bounded((0..2_048).collect(), true, Some(1), 2, move |index| {
            count.fetch_add(1, Ordering::SeqCst);
            if index == 0 {
                let wait = first.lock().unwrap().take().unwrap();
                Box::pin(async_stream::try_stream! {
                    let _ = wait.await;
                    yield batch(0);
                })
            } else {
                Box::pin(futures::stream::empty())
            }
        });
        let result = tokio::spawn(async move {
            output
                .map_ok(|batch| value(&batch))
                .try_collect::<Vec<_>>()
                .await
        });
        wait_for_count(&created, 4).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(created.load(Ordering::SeqCst), 4);
        release.send(()).unwrap();
        assert_eq!(result.await.unwrap().unwrap(), vec![0]);
    }

    #[tokio::test]
    async fn ordered_limit_ignores_only_speculative_split_errors() {
        for first_has_row in [true, false] {
            let splits = (0..4)
                .map(|index| {
                    DataSplitBuilder::new()
                        .with_snapshot(1)
                        .with_partition(BinaryRow::new(0))
                        .with_bucket(index)
                        .with_bucket_path(format!("memory:/split-{index}"))
                        .with_data_files(Vec::new())
                        .build()
                        .unwrap()
                })
                .collect();
            let (release, wait) = oneshot::channel();
            let first_wait = std::sync::Mutex::new(Some(wait));
            let failed = Arc::new(AtomicUsize::new(0));
            let failed_read = failed.clone();
            let read: SplitReader = Arc::new(move |splits, _| match splits[0].bucket() {
                0 => {
                    let wait = first_wait.lock().unwrap().take().unwrap();
                    Ok(Box::pin(async_stream::try_stream! {
                        let _ = wait.await;
                        if first_has_row { yield batch(1); }
                    }))
                }
                1 => {
                    failed_read.fetch_add(1, Ordering::SeqCst);
                    Err(crate::Error::DataInvalid {
                        message: "speculative split failed".into(),
                        source: None,
                    })
                }
                _ => Ok(Box::pin(futures::stream::empty())),
            });
            let mut output = execute(splits, 2, true, Some(1), read, None);
            assert!(output.next().now_or_never().is_none());
            wait_for_count(&failed, 1).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(100), output.next())
                    .await
                    .is_err()
            );
            release.send(()).unwrap();
            if first_has_row {
                assert_eq!(value(&output.next().await.unwrap().unwrap()), 1);
                assert!(output.next().await.is_none());
            } else {
                assert!(output
                    .next()
                    .await
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .contains("speculative split failed"));
                assert!(output.next().await.is_none());
            }
        }
    }

    #[tokio::test]
    async fn ordered_limit_defers_speculative_worker_panics() {
        for first_has_row in [true, false] {
            let (release, wait) = oneshot::channel();
            let first = controlled_stream(
                if first_has_row { vec![1] } else { vec![] },
                Some(wait),
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
            );
            let entered = Arc::new(AtomicUsize::new(0));
            let entered_worker = entered.clone();
            let panicking = Box::pin(futures::stream::once(async move {
                entered_worker.fetch_add(1, Ordering::SeqCst);
                panic!("speculative decoder panic");
                #[allow(unreachable_code)]
                Ok(batch(2))
            })) as ArrowRecordBatchStream;
            let mut output = merge_streams_bounded(vec![first, panicking], true, Some(1), 2);
            assert!(output.next().now_or_never().is_none());
            wait_for_count(&entered, 1).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(100), output.next())
                    .await
                    .is_err()
            );
            release.send(()).unwrap();
            if first_has_row {
                assert_eq!(value(&output.next().await.unwrap().unwrap()), 1);
            } else {
                assert!(matches!(
                    output.next().await.unwrap().unwrap_err(),
                    crate::Error::UnexpectedError { .. }
                ));
            }
            assert!(output.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn blob_output_budget_stops_decoding_while_consumer_is_paused() {
        for ordered in [false, true] {
            let reads = Arc::new(AtomicUsize::new(0));
            let drops = Arc::new(AtomicUsize::new(0));
            let streams = (0..32)
                .map(|value| controlled_stream(vec![value], None, reads.clone(), drops.clone()))
                .collect();
            let mut output = merge_streams_budgeted(streams, vec![1; 32], 2, ordered, 32);
            assert!(output.next().await.unwrap().is_ok());
            // The returned batch and at most one later batch have reservations.
            tokio::task::yield_now().await;
            assert!(reads.load(Ordering::SeqCst) <= 2);
            let rest = output.try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(rest.len(), 31);
            assert_eq!(reads.load(Ordering::SeqCst), 32);
            assert_eq!(drops.load(Ordering::SeqCst), 32);
        }
    }

    #[tokio::test]
    async fn ordered_budgeted_readers_are_created_only_when_admitted() {
        let created = Arc::new(AtomicUsize::new(0));
        let (release, wait) = oneshot::channel();
        let wait = Arc::new(Mutex::new(Some(wait)));
        let mut output =
            merge_items_budgeted((0..1_000).collect(), vec![Some(1); 1_000], 2, true, 2, {
                let created = created.clone();
                move |index| {
                    created.fetch_add(1, Ordering::SeqCst);
                    let wait = if index == 0 {
                        wait.lock().unwrap().take()
                    } else {
                        None
                    };
                    controlled_stream(
                        vec![index],
                        wait,
                        Arc::new(AtomicUsize::new(0)),
                        Arc::new(AtomicUsize::new(0)),
                    )
                }
            });
        assert_eq!(created.load(Ordering::SeqCst), 0);
        assert!(output.next().now_or_never().is_none());
        assert_eq!(created.load(Ordering::SeqCst), 2);
        release.send(()).unwrap();
        assert_eq!(value(&output.next().await.unwrap().unwrap()), 0);
        // A paused caller must not create or schedule the remaining readers.
        assert_eq!(created.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn blob_output_budget_keeps_ordered_head_admitted() {
        let (release, wait) = oneshot::channel();
        let reads = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let streams = vec![
            controlled_stream(vec![1, 2], Some(wait), reads.clone(), drops.clone()),
            controlled_stream(vec![3], None, reads.clone(), drops.clone()),
            controlled_stream(vec![4], None, reads.clone(), drops.clone()),
        ];
        let mut output = merge_streams_budgeted(streams, vec![1, 1, 1], 2, true, 3);
        assert!(output.next().now_or_never().is_none());
        release.send(()).unwrap();
        let values = tokio::time::timeout(Duration::from_secs(5), async {
            output
                .map_ok(|batch| value(&batch))
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
        })
        .await
        .expect("ordered BLOB output stalled");
        assert_eq!(values, vec![1, 2, 3, 4]);
        assert_eq!(drops.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn blob_output_budget_keeps_ordered_prefetch_bounded() {
        let (release, wait) = oneshot::channel();
        let reads = (0..4)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut first = Some(wait);
        let streams = reads
            .iter()
            .enumerate()
            .map(|(index, count)| {
                controlled_stream(
                    vec![index as i32],
                    if index == 0 { first.take() } else { None },
                    count.clone(),
                    drops.clone(),
                )
            })
            .collect();
        let mut output = merge_streams_budgeted(streams, vec![1; 4], 4, true, 2);
        assert!(output.next().now_or_never().is_none());
        wait_for_count(&reads[1], 1).await;
        assert_eq!(reads[2].load(Ordering::SeqCst), 0);
        assert_eq!(reads[3].load(Ordering::SeqCst), 0);
        release.send(()).unwrap();
        let values = output
            .map_ok(|batch| value(&batch))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(values, vec![0, 1, 2, 3]);
        assert_eq!(drops.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn blob_output_budget_preserves_global_limit() {
        let splits = (0..4)
            .map(|index| {
                DataSplitBuilder::new()
                    .with_snapshot(1)
                    .with_partition(BinaryRow::new(0))
                    .with_bucket(index)
                    .with_bucket_path(format!("memory:/blob-budget-{index}"))
                    .with_total_buckets(4)
                    .with_data_files(vec![split_file(
                        "image.blob",
                        OUTPUT_BYTE_UNIT as i64,
                        Some(vec!["image"]),
                    )])
                    .build()
                    .unwrap()
            })
            .collect();
        let read: SplitReader = Arc::new(|splits, _| {
            let value = splits[0].bucket();
            Ok(Box::pin(futures::stream::iter(vec![Ok(batch(value))])))
        });
        let output = execute(
            splits,
            2,
            true,
            Some(1),
            read,
            Some(BlobOutputBudget {
                max_bytes: 4 * OUTPUT_BYTE_UNIT,
                fields: HashSet::from(["image".to_string()]),
                unknown_external: false,
                serialize_unknown: false,
            }),
        );
        let values = output
            .map_ok(|batch| value(&batch))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(values, vec![0]);
    }

    #[tokio::test]
    async fn blob_output_budget_releases_failed_readers_before_yielding_error() {
        for (ordered, failing_index) in [(false, 0), (true, 0), (true, 1)] {
            let drops = Arc::new(AtomicUsize::new(0));
            let (_release, wait) = oneshot::channel();
            let blocked = controlled_stream(
                vec![1],
                Some(wait),
                Arc::new(AtomicUsize::new(0)),
                drops.clone(),
            );
            let guard = DropGuard(drops.clone());
            let failing = Box::pin(async_stream::stream! {
                let _guard = guard;
                yield Err(crate::Error::DataInvalid {
                    message: "BLOB read failed".into(),
                    source: None,
                });
                futures::future::pending::<()>().await;
            }) as ArrowRecordBatchStream;
            let streams = if failing_index == 0 {
                vec![failing, blocked]
            } else {
                vec![blocked, failing]
            };
            let mut output = merge_streams_budgeted(streams, vec![1, 1], 2, ordered, 2);
            let error = tokio::time::timeout(Duration::from_secs(5), output.next())
                .await
                .expect("BLOB error was not returned")
                .unwrap()
                .unwrap_err();
            assert!(error.to_string().contains("BLOB read failed"));
            wait_for_count(&drops, 2).await;
        }
    }

    #[tokio::test]
    async fn unordered_execution_yields_ready_groups_and_close_cancels_pending_reads() {
        let (_release, wait) = oneshot::channel();
        let reads = Arc::new(AtomicUsize::new(0));
        let later_reads = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut output = merge_streams(
            vec![
                controlled_stream(vec![1], Some(wait), reads.clone(), drops.clone()),
                controlled_stream(vec![2, 3], None, later_reads.clone(), drops.clone()),
            ],
            false,
            None,
        );
        assert_eq!(value(&output.next().await.unwrap().unwrap()), 2);
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert_eq!(later_reads.load(Ordering::SeqCst), 1);
        drop(output);
        wait_for_count(&drops, 2).await;
        assert_eq!(later_reads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn worker_errors_cancel_other_readers_even_in_ordered_mode() {
        let (_release, wait) = oneshot::channel();
        let reads = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let ready_reads = Arc::new(AtomicUsize::new(0));
        let ready_before_failure = ready_reads.clone();
        let failing = Box::pin(futures::stream::once(async move {
            wait_for_count(&ready_before_failure, 1).await;
            Err(crate::Error::DataInvalid {
                message: "worker failed".into(),
                source: None,
            })
        })) as ArrowRecordBatchStream;
        let mut output = merge_streams(
            vec![
                controlled_stream(vec![1], Some(wait), reads, drops.clone()),
                controlled_stream(vec![2, 3], None, ready_reads.clone(), drops.clone()),
                failing,
            ],
            true,
            None,
        );
        assert!(output
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("worker failed"));
        assert_eq!(ready_reads.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        // Keeping the failed stream alive must not retain another worker.
        assert!(output.next().await.is_none());
    }

    #[tokio::test]
    async fn global_limit_stops_before_requesting_another_batch() {
        let reads = (0..3)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>();
        let drops = Arc::new(AtomicUsize::new(0));
        let streams = reads
            .iter()
            .enumerate()
            .map(|(i, reads)| {
                controlled_stream(
                    vec![i as i32, i as i32 + 10],
                    None,
                    reads.clone(),
                    drops.clone(),
                )
            })
            .collect();
        let mut output = merge_streams(streams, false, Some(1));
        let last = output.next().await.unwrap().unwrap();
        assert_eq!(last.num_rows(), 1);
        let started = reads
            .iter()
            .map(|count| count.load(Ordering::SeqCst))
            .collect::<Vec<_>>();
        assert!(started.iter().all(|count| *count <= 1));
        assert!((1..=3).contains(&started.iter().sum::<usize>()));
        assert_eq!(drops.load(Ordering::SeqCst), 3);
        // The caller may keep the final batch and stream without polling again.
        assert!(output.next().await.is_none());
        assert_eq!(
            reads
                .iter()
                .map(|count| count.load(Ordering::SeqCst))
                .collect::<Vec<_>>(),
            started
        );
    }

    #[test]
    fn one_worker_and_empty_reads_do_not_require_a_tokio_runtime() {
        let reads = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        futures::executor::block_on(async {
            let mut output = merge_streams(
                vec![controlled_stream(
                    vec![1, 2],
                    None,
                    reads.clone(),
                    drops.clone(),
                )],
                false,
                Some(1),
            );
            assert_eq!(value(&output.next().await.unwrap().unwrap()), 1);
            assert_eq!(reads.load(Ordering::SeqCst), 1);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert!(output.next().await.is_none());
            assert!(merge_streams(Vec::new(), true, None).next().await.is_none());
            let guard = DropGuard(drops.clone());
            let failing = Box::pin(async_stream::stream! {
                let _guard = guard;
                yield Err(crate::Error::DataInvalid {
                    message: "serial worker failed".into(), source: None,
                });
                std::future::pending::<()>().await;
            }) as ArrowRecordBatchStream;
            let mut output = merge_streams(vec![failing], false, None);
            assert!(output.next().await.unwrap().is_err());
            assert_eq!(drops.load(Ordering::SeqCst), 2);
            assert!(output.next().await.is_none());
            let zero_drops = Arc::new(AtomicUsize::new(0));
            let streams = (0..2)
                .map(|i| controlled_stream(vec![i], None, reads.clone(), zero_drops.clone()))
                .collect();
            let mut output = merge_streams(streams, false, Some(0));
            assert_eq!(zero_drops.load(Ordering::SeqCst), 2);
            assert!(output.next().await.is_none());
            let parallel_drops = Arc::new(AtomicUsize::new(0));
            let streams = (0..2)
                .map(|i| controlled_stream(vec![i], None, reads.clone(), parallel_drops.clone()))
                .collect();
            let mut output = merge_streams(streams, false, None);
            let error = output.next().await.unwrap().unwrap_err();
            assert!(matches!(&error, crate::Error::UnexpectedError { .. }));
            assert!(error.to_string().contains("Tokio runtime"));
            assert_eq!(parallel_drops.load(Ordering::SeqCst), 2);
            assert!(output.next().await.is_none());
        });
    }

    #[tokio::test]
    async fn a_panicking_worker_cancels_other_workers_before_returning_the_error() {
        let (_release, wait) = oneshot::channel();
        let drops = Arc::new(AtomicUsize::new(0));
        let panicking = Box::pin(futures::stream::once(async {
            panic!("decoder panic");
            #[allow(unreachable_code)]
            Ok(batch(2))
        })) as ArrowRecordBatchStream;
        let mut output = merge_streams(
            vec![
                controlled_stream(
                    vec![1],
                    Some(wait),
                    Arc::new(AtomicUsize::new(0)),
                    drops.clone(),
                ),
                panicking,
            ],
            true,
            None,
        );
        let error = output.next().await.unwrap().unwrap_err();
        assert!(matches!(error, crate::Error::UnexpectedError { .. }));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(output.next().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn independent_workers_poll_synchronous_decoders_on_separate_threads() {
        #[derive(Default)]
        struct State {
            entered: usize,
            active: usize,
            peak: usize,
            threads: std::collections::HashSet<std::thread::ThreadId>,
        }
        for ordered in [false, true] {
            let shared = Arc::new((
                std::sync::Mutex::new(State::default()),
                std::sync::Condvar::new(),
            ));
            let streams = (0..2)
                .map(|value| {
                    let shared = shared.clone();
                    Box::pin(futures::stream::once(async move {
                        // This synchronous portion represents a decoder's CPU work.
                        // The bounded rendezvous lets the former inline executor
                        // finish rather than hanging forever: it records peak=1.
                        let (lock, changed) = &*shared;
                        let mut state = lock.lock().unwrap();
                        state.entered += 1;
                        state.active += 1;
                        state.peak = state.peak.max(state.active);
                        state.threads.insert(std::thread::current().id());
                        changed.notify_all();
                        let deadline = std::time::Instant::now() + Duration::from_secs(2);
                        while state.entered < 2 {
                            let remaining =
                                deadline.saturating_duration_since(std::time::Instant::now());
                            let (next, timeout) = changed.wait_timeout(state, remaining).unwrap();
                            state = next;
                            if timeout.timed_out() {
                                break;
                            }
                        }
                        state.active -= 1;
                        drop(state);
                        Ok(batch(value))
                    })) as ArrowRecordBatchStream
                })
                .collect();
            let batches = merge_streams(streams, ordered, None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(batches.len(), 2);
            let state = shared.0.lock().unwrap();
            assert_eq!(state.peak, 2, "synchronous decoder work was serialized");
            assert_eq!(state.threads.len(), 2);
        }
    }
}
