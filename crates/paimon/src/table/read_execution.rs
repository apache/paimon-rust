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

use super::{
    read_limit::take_limited_batch, schema_manager::SchemaManager, ArrowRecordBatchStream,
};
use crate::spec::{DataFileMeta, TableSchema};
use crate::DataSplit;
use arrow_array::RecordBatch;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

const OUTPUT_BYTE_UNIT: u64 = 1024 * 1024;

fn budget_permits(max_bytes: u64) -> u32 {
    (max_bytes / OUTPUT_BYTE_UNIT).clamp(1, u32::MAX as u64) as u32
}

#[derive(Clone)]
pub(super) struct BlobOutputBudget {
    max_bytes: u64,
    fields: HashSet<String>,
    field_ids: HashSet<i32>,
    unknown_external: bool,
    serialize_unknown: bool,
    table_schema_id: i64,
    file_schema_manager: Option<SchemaManager>,
    implicit_blob_columns: bool,
    historical_schemas: HashMap<i64, (Arc<TableSchema>, bool)>,
}

impl BlobOutputBudget {
    pub(super) fn new(
        max_bytes: u64,
        fields: HashSet<String>,
        schema: &TableSchema,
        schema_manager: SchemaManager,
        unknown_external: bool,
        serialize_unknown: bool,
    ) -> Self {
        let field_ids = schema
            .fields()
            .iter()
            .filter(|field| fields.contains(field.name()))
            .map(|field| field.id())
            .collect();
        let mut budget = Self {
            max_bytes,
            fields,
            field_ids,
            unknown_external,
            serialize_unknown,
            table_schema_id: schema.id(),
            file_schema_manager: Some(schema_manager),
            implicit_blob_columns: false,
            historical_schemas: HashMap::new(),
        };
        budget.implicit_blob_columns = budget.implicit_blob_columns_for(schema);
        budget
    }

    fn implicit_blob_columns_for(&self, schema: &TableSchema) -> bool {
        self.serialize_unknown
            && schema
                .data_file_fields(None)
                .iter()
                .any(|field| self.field_ids.contains(&field.id()))
    }

    fn register_file_schema(&mut self, schema: Arc<TableSchema>) {
        let implicit = self.implicit_blob_columns_for(&schema);
        self.historical_schemas
            .insert(schema.id(), (schema, implicit));
    }

    fn writes_blob_columns(&self, file: &DataFileMeta) -> bool {
        if self.fields.is_empty() {
            return false;
        }
        if file.schema_id == self.table_schema_id {
            return file
                .write_cols
                .as_ref()
                .map_or(self.implicit_blob_columns, |cols| {
                    cols.iter().any(|name| self.fields.contains(name))
                });
        }
        // Until the historical schema is loaded, keep the file in the estimate.
        // The same fallback protects reads if the schema lookup fails.
        let Some((schema, implicit)) = self.historical_schemas.get(&file.schema_id) else {
            return true;
        };
        if file.write_cols.is_none() {
            return *implicit;
        }
        super::data_evolution_reader::resolve_field_ids(file, schema.fields())
            .map_or(true, |ids| ids.iter().any(|id| self.field_ids.contains(id)))
    }

    fn unresolved_file_schemas(&self, splits: &[DataSplit]) -> BTreeSet<i64> {
        if self.fields.is_empty() {
            return BTreeSet::new();
        }
        splits
            .iter()
            .flat_map(|split| split.data_files())
            .filter(|file| {
                file.schema_id != self.table_schema_id
                    && !self.historical_schemas.contains_key(&file.schema_id)
                    && if file.file_name.ends_with(".blob") {
                        file.write_cols.is_some()
                    } else {
                        self.serialize_unknown
                    }
            })
            .map(|file| file.schema_id)
            .collect()
    }
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

/// Divide independent DE files while retaining each supplied split's boundary.
/// Return no plan when the existing splits already use the budget or cannot split.
pub(super) fn independent_file_groups(
    splits: &[DataSplit],
    parallelism: usize,
) -> Option<Vec<Vec<DataSplit>>> {
    let initial_spare = parallelism.saturating_sub(splits.len());
    let mut spare = initial_spare;
    if spare == 0 {
        return None;
    }
    let mut work = Vec::with_capacity(splits.len());
    for split in splits {
        let files = split.data_files();
        // Range coordinates and scores belong to the original selection. Updates
        // and overlapping column versions must retain their reconstruction group.
        if spare == 0
            || files.len() < 2
            || split.is_streaming()
            || split.row_ranges().is_some()
            || split.scores().is_some()
            || !files.iter().all(|file| {
                file.row_count > 0
                    && file
                        .first_row_id
                        .and_then(|start| start.checked_add(file.row_count))
                        .is_some()
            })
            || !super::data_evolution_reader::is_raw_convertible(files)
        {
            work.push(vec![split.clone()]);
            continue;
        }
        let weights = files
            .iter()
            .map(|file| u64::try_from(file.file_size).unwrap_or(0).max(1))
            .collect::<Vec<_>>();
        let groups = group_ranges(&weights, spare + 1);
        spare -= groups.len() - 1;
        work.push(
            groups
                .into_iter()
                .map(|(start, end)| split.for_data_evolution_files(files[start..end].to_vec()))
                .collect(),
        );
    }
    (spare < initial_spare).then_some(work)
}

/// Files can read concurrently, but only original splits may emit out of order.
pub(super) fn execute_file_groups(
    groups: Vec<Vec<DataSplit>>,
    ordered: bool,
    limit: Option<usize>,
    read: SplitReader,
) -> ArrowRecordBatchStream {
    let streams = groups
        .into_iter()
        .map(|files| {
            let parallelism = files.len();
            execute(files, parallelism, true, limit, Arc::clone(&read), None)
        })
        .collect();
    merge_streams(streams, ordered, limit)
}

pub(super) fn execute(
    splits: Vec<DataSplit>,
    parallelism: usize,
    ordered: bool,
    limit: Option<usize>,
    read: SplitReader,
    blob_output_budget: Option<BlobOutputBudget>,
) -> ArrowRecordBatchStream {
    let mut blob_output_budget = blob_output_budget;
    if limit.is_none() && parallelism > 1 && splits.len() > 1 {
        if let Some(budget) = &mut blob_output_budget {
            if let Some(manager) = budget.file_schema_manager.take() {
                if !budget.unresolved_file_schemas(&splits).is_empty() {
                    return execute_with_historical_schemas(
                        splits,
                        parallelism,
                        ordered,
                        read,
                        blob_output_budget.unwrap(),
                        manager,
                    );
                }
            }
        }
    }
    execute_resolved(
        splits,
        parallelism,
        ordered,
        limit,
        read,
        blob_output_budget,
    )
}

fn execute_with_historical_schemas(
    splits: Vec<DataSplit>,
    parallelism: usize,
    ordered: bool,
    read: SplitReader,
    budget: BlobOutputBudget,
    manager: SchemaManager,
) -> ArrowRecordBatchStream {
    let permits = budget_permits(budget.max_bytes) as usize;
    let mut waiting = VecDeque::new();
    let preparing = BudgetPreparations::new();
    let mut loads = HashMap::new();
    let groups = if ordered {
        vec![splits]
    } else {
        split_groups(splits, parallelism)
    };
    let initial_slots = if ordered {
        parallelism.min(groups[0].len())
    } else {
        groups.len()
    };
    let mut groups: Vec<_> = groups.into_iter().map(Vec::into_iter).collect();
    let mut next_split_index = 0;
    // Ordered reads prepare a window of splits from one input queue.
    // Unordered reads prepare the next split in each group.
    let mut advance = move |index: usize,
                            waiting: &mut VecDeque<BudgetedItem<DataSplit>>,
                            preparing: &BudgetPreparations<DataSplit>| {
        let group = if ordered { 0 } else { index };
        let Some(split) = groups[group].next() else {
            return false;
        };
        let index = if ordered {
            let index = next_split_index;
            next_split_index += 1;
            index
        } else {
            index
        };
        let ids = budget.unresolved_file_schemas(std::slice::from_ref(&split));
        if ids.is_empty() {
            let cost = blob_group_cost(std::slice::from_ref(&split), &budget);
            insert_budgeted_item(waiting, (index, split, cost));
            return true;
        }

        // Share requests for the same schema across groups and splits.
        let schemas: Vec<_> = ids
            .into_iter()
            .map(|id| {
                loads
                    .entry(id)
                    .or_insert_with(|| {
                        let manager = manager.clone();
                        async move { manager.schema(id).await.ok() }
                            .boxed()
                            .shared()
                    })
                    .clone()
            })
            .collect();
        let mut budget = budget.clone();
        preparing.push(
            async move {
                for schema in schemas {
                    // Failed lookups leave the conservative estimate in place.
                    if let Some(schema) = schema.await {
                        budget.register_file_schema(schema);
                    }
                }
                let cost = blob_group_cost(std::slice::from_ref(&split), &budget);
                (index, split, cost)
            }
            .boxed(),
        );
        true
    };
    for index in 0..initial_slots {
        advance(index, &mut waiting, &preparing);
    }

    merge_budgeted_groups(
        waiting,
        preparing,
        permits,
        ordered,
        parallelism,
        move |split| read_group(vec![split], Arc::clone(&read), None),
        advance,
    )
}

fn execute_resolved(
    splits: Vec<DataSplit>,
    parallelism: usize,
    ordered: bool,
    limit: Option<usize>,
    read: SplitReader,
    blob_output_budget: Option<BlobOutputBudget>,
) -> ArrowRecordBatchStream {
    // Finite LIMIT uses the existing executor to cancel speculative splits at
    // the global row quota; the output budget is for unbounded reads.
    if ordered && parallelism > 1 && splits.len() > 1 {
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
    let mut file_bytes = 0_u64;
    let mut has_unknown = budget.unknown_external;
    for split in group {
        let mut bytes = 0_u64;
        let mut unknown = false;
        for file in split.data_files() {
            if file.file_name.ends_with(".blob") {
                if file.write_cols.is_none() || budget.writes_blob_columns(file) {
                    let size = u64::try_from(file.file_size).unwrap_or(0);
                    unknown |= size == 0;
                    bytes = bytes.saturating_add(size);
                }
            } else if budget.serialize_unknown {
                unknown |= budget.writes_blob_columns(file);
            }
        }
        has_unknown |= unknown;
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

struct BudgetedBatch {
    index: usize,
    stream: ArrowRecordBatchStream,
    batch: Option<crate::Result<RecordBatch>>,
    permit: Option<OwnedSemaphorePermit>,
}

type BudgetedItem<T> = (usize, T, Option<u32>);
type BudgetPreparations<T> = FuturesUnordered<BoxFuture<'static, BudgetedItem<T>>>;

fn insert_budgeted_item<T>(waiting: &mut VecDeque<BudgetedItem<T>>, item: BudgetedItem<T>) {
    let position = waiting.partition_point(|queued| queued.0 < item.0);
    waiting.insert(position, item);
}

fn poll_budgeted(
    index: usize,
    mut stream: ArrowRecordBatchStream,
    permit: Option<OwnedSemaphorePermit>,
    pending: &mut JoinSet<BudgetedBatch>,
    handle: &tokio::runtime::Handle,
) {
    pending.spawn_on(
        async move {
            let batch = stream.next().await;
            BudgetedBatch {
                index,
                stream,
                batch,
                permit,
            }
        },
        handle,
    );
}

fn schedule_budgeted<T, F>(
    waiting: &mut VecDeque<BudgetedItem<T>>,
    pending: &mut JoinSet<BudgetedBatch>,
    permits: &Arc<Semaphore>,
    handle: &tokio::runtime::Handle,
    max_pending: usize,
    next_admission: &mut Option<usize>,
    make_stream: &F,
) where
    T: Send + 'static,
    F: Fn(T) -> ArrowRecordBatchStream,
{
    while pending.len() < max_pending {
        let available = permits.available_permits();
        // Even if schema lookups finish out of order, never let a later
        // ordered split take a permit ahead of a split still preparing.
        let candidates = if next_admission.is_some() {
            1
        } else {
            waiting.len()
        };
        let Some(position) = waiting
            .iter()
            .take(candidates)
            .position(|(index, _, cost)| {
                next_admission.is_none_or(|next| *index == next)
                    && cost.is_none_or(|cost| cost as usize <= available)
            })
        else {
            break;
        };
        let permit = match waiting[position].2 {
            Some(cost) => {
                let Ok(permit) = Arc::clone(permits).try_acquire_many_owned(cost) else {
                    break;
                };
                Some(permit)
            }
            None => None,
        };
        let (index, item, _) = waiting.remove(position).unwrap();
        poll_budgeted(index, make_stream(item), permit, pending, handle);
        if let Some(next) = next_admission {
            *next += 1;
        }
    }
}

/// Hold a reservation until its reader stops retaining decoded output.
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
    let waiting = items
        .into_iter()
        .zip(costs)
        .enumerate()
        .map(|(index, (item, cost))| (index, item, cost))
        .collect();
    merge_budgeted_groups(
        waiting,
        BudgetPreparations::new(),
        permits,
        ordered,
        parallelism,
        make_stream,
        |_, _, _| false,
    )
}

fn merge_budgeted_groups<T, F>(
    mut waiting: VecDeque<BudgetedItem<T>>,
    mut preparing: BudgetPreparations<T>,
    permits: usize,
    ordered: bool,
    parallelism: usize,
    make_stream: F,
    mut advance: impl FnMut(usize, &mut VecDeque<BudgetedItem<T>>, &BudgetPreparations<T>) -> bool
        + Send
        + 'static,
) -> ArrowRecordBatchStream
where
    T: Send + 'static,
    F: Fn(T) -> ArrowRecordBatchStream + Send + 'static,
{
    Box::pin(async_stream::try_stream! {
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(error) => {
                waiting.clear();
                preparing.clear();
                drop(advance);
                Err(crate::Error::UnexpectedError {
                    message: "Parallel table reads require a Tokio runtime".into(),
                    source: Some(Box::new(error)),
                })?;
                return;
            }
        };
        let mut count = waiting.len() + preparing.len();
        let permits = Arc::new(Semaphore::new(permits));
        let mut pending = JoinSet::new();
        let mut ready = BTreeMap::new();
        let mut completed = BTreeSet::new();
        let mut current = 0;
        let mut next_admission = ordered.then_some(0);
        schedule_budgeted(&mut waiting, &mut pending, &permits, &handle, parallelism, &mut next_admission, &make_stream);
        while current < count {
            if ordered && completed.remove(&current) {
                current += 1;
                continue;
            }
            let next = if let Some(next) = ready.remove(&current) {
                Ok(Some(next))
            } else if preparing.is_empty() {
                pending.join_next().await.transpose().map_err(worker_error)
            } else {
                tokio::select! {
                    Some(item) = preparing.next() => {
                        insert_budgeted_item(&mut waiting, item);
                        schedule_budgeted(
                            &mut waiting,
                            &mut pending,
                            &permits,
                            &handle,
                            parallelism.saturating_sub(ready.len()),
                            &mut next_admission,
                            &make_stream,
                        );
                        continue;
                    }
                    result = pending.join_next(), if !pending.is_empty() => {
                        result.transpose().map_err(worker_error)
                    }
                }
            };
            let next = match next {
                Ok(Some(next)) => next,
                Ok(None) => break,
                Err(error) => {
                    drop(ready);
                    waiting.clear();
                    preparing.clear();
                    drop(advance);
                    pending.shutdown().await;
                    Err(error)?;
                    break;
                }
            };
            let index = next.index;
            if next.batch.is_none() {
                drop(next);
                let advanced = advance(index, &mut waiting, &preparing);
                if ordered {
                    completed.insert(index);
                    // Unlike a continuation of an unordered group, this
                    // split adds a new position to the ordered output.
                    count += usize::from(advanced);
                } else if !advanced {
                    current += 1;
                }
                schedule_budgeted(
                    &mut waiting,
                    &mut pending,
                    &permits,
                    &handle,
                    parallelism.saturating_sub(ready.len()),
                    &mut next_admission,
                    &make_stream,
                );
                continue;
            }
            if ordered && index != current {
                if next.batch.as_ref().is_some_and(Result::is_err) {
                    let BudgetedBatch { stream, batch, permit, .. } = next;
                    let error = batch.unwrap().unwrap_err();
                    drop(stream);
                    drop(permit);
                    drop(ready);
                    waiting.clear();
                    preparing.clear();
                    drop(advance);
                    pending.shutdown().await;
                    Err(error)?;
                    break;
                }
                ready.insert(index, next);
                continue;
            }
            let BudgetedBatch { stream, batch, permit, .. } = next;
            match batch.unwrap() {
                Err(error) => {
                    drop(stream);
                    drop(permit);
                    drop(ready);
                    waiting.clear();
                    preparing.clear();
                    drop(advance);
                    pending.shutdown().await;
                    Err(error)?;
                    break;
                }
                Ok(batch) => {
                    if batch.num_rows() > 0 {
                        yield batch;
                    }
                    // The DE reader can retain the rest of a decoded source batch
                    // across yields. Continue the admitted stream with its permit.
                    poll_budgeted(index, stream, permit, &mut pending, &handle);
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
    fn historical_blob_cost_matches_field_ids_with_default_budget() {
        use crate::spec::{BlobType, DataType, IntType, Schema};

        let schema = |id, projected, other| {
            TableSchema::new(
                id,
                &Schema::builder()
                    .column("id", DataType::Int(IntType::new()))
                    .column(projected, DataType::Blob(BlobType::new()))
                    .column(other, DataType::Blob(BlobType::new()))
                    .option("data-evolution.enabled", "true")
                    .option("row-tracking.enabled", "true")
                    .build()
                    .unwrap(),
            )
        };
        let current = schema(1, "photo", "other");
        let old = schema(0, "image", "photo");
        let io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let mut budget = BlobOutputBudget::new(
            4 * OUTPUT_BYTE_UNIT,
            HashSet::from(["photo".into()]),
            &current,
            SchemaManager::new(io, "memory:/blob-budget".into()),
            false,
            false,
        );
        let selected = budget_split(vec![split_file(
            "old-image.blob",
            2 * OUTPUT_BYTE_UNIT as i64,
            Some(vec!["image"]),
        )]);
        let unrelated = budget_split(vec![split_file(
            "old-photo.blob",
            3 * OUTPUT_BYTE_UNIT as i64,
            Some(vec!["photo"]),
        )]);
        assert_eq!(
            budget.unresolved_file_schemas(std::slice::from_ref(&selected)),
            BTreeSet::from([0])
        );
        assert!(blob_budget_applies_to_splits(
            std::slice::from_ref(&selected),
            &budget
        ));
        budget.register_file_schema(Arc::new(old));
        assert_eq!(blob_group_cost(&[selected], &budget), Some(2));
        assert_eq!(blob_group_cost(&[unrelated], &budget), None);
    }

    #[test]
    fn blob_output_reservation_requires_opt_in_for_unknown_sizes() {
        let mut budget = BlobOutputBudget {
            max_bytes: 64 * OUTPUT_BYTE_UNIT,
            fields: HashSet::from(["image".to_string()]),
            field_ids: HashSet::new(),
            unknown_external: false,
            serialize_unknown: false,
            table_schema_id: 0,
            file_schema_manager: None,
            implicit_blob_columns: false,
            historical_schemas: HashMap::new(),
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
        let with_view = budget_split(vec![
            split_file("view.parquet", 1024, Some(vec!["preview"])),
            split_file(
                "image.blob",
                17 * OUTPUT_BYTE_UNIT as i64,
                Some(vec!["image"]),
            ),
        ]);
        budget.unknown_external = true;
        assert_eq!(
            blob_group_cost(std::slice::from_ref(&with_view), &budget),
            Some(17)
        );
        assert_eq!(
            blob_group_cost(std::slice::from_ref(&mixed), &budget),
            Some(17)
        );
        assert_eq!(
            blob_group_cost(std::slice::from_ref(&descriptor), &budget),
            None
        );
        budget.serialize_unknown = true;
        assert_eq!(blob_group_cost(&[with_view], &budget), Some(64));
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
                field_ids: HashSet::new(),
                unknown_external: false,
                serialize_unknown: false,
                table_schema_id: 0,
                file_schema_manager: None,
                implicit_blob_columns: false,
                historical_schemas: HashMap::new(),
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
                field_ids: HashSet::new(),
                unknown_external: false,
                serialize_unknown: false,
                table_schema_id: 0,
                file_schema_manager: None,
                implicit_blob_columns: false,
                historical_schemas: HashMap::new(),
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
                field_ids: HashSet::new(),
                unknown_external: false,
                serialize_unknown: false,
                table_schema_id: 0,
                file_schema_manager: None,
                implicit_blob_columns: false,
                historical_schemas: HashMap::new(),
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
                field_ids: HashSet::new(),
                unknown_external: false,
                serialize_unknown: false,
                table_schema_id: 0,
                file_schema_manager: None,
                implicit_blob_columns: false,
                historical_schemas: HashMap::new(),
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

    fn independent_split() -> DataSplit {
        use super::super::table_read::tests::{file, split};
        let files = (0..5)
            .map(|i| {
                let mut f = file(&format!("{i}.parquet"), 0, None);
                f.first_row_id = Some(i * 10);
                f
            })
            .collect();
        split(files, true)
    }

    #[tokio::test]
    async fn independent_files_run_concurrently_with_order_and_global_limit() {
        let split = independent_split();
        let work = independent_file_groups(&[split], 3).unwrap();
        let started = Arc::new(AtomicUsize::new(0));
        let workers = started.clone();
        let read: SplitReader = Arc::new(move |group, _| {
            let workers = workers.clone();
            let values = group
                .iter()
                .flat_map(|s| s.data_files())
                .map(|f| (f.first_row_id.unwrap() / 10) as i32)
                .collect::<Vec<_>>();
            Ok(Box::pin(async_stream::try_stream! {
                workers.fetch_add(1, Ordering::SeqCst);
                wait_for_count(&workers, 3).await;
                for v in values { yield batch(v); }
            }))
        });
        let output = execute_file_groups(work, false, Some(4), read)
            .map_ok(|b| value(&b))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(output, vec![0, 1, 2, 3]);
        assert_eq!(started.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn unordered_original_splits_keep_ordered_file_groups() {
        let first = independent_split();
        let mut files = first.data_files().to_vec();
        for file in &mut files {
            file.first_row_id = file.first_row_id.map(|id| id + 100);
        }
        let second = first.for_data_evolution_files(files);
        let work = independent_file_groups(&[first, second], 4).unwrap();
        assert_eq!(work.len(), 2);
        assert_eq!(work.iter().map(Vec::len).sum::<usize>(), 4);
        let (release, wait) = oneshot::channel();
        let wait = std::sync::Mutex::new(Some(wait));
        let reads = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let dropped = drops.clone();
        let read: SplitReader = Arc::new(move |group, _| {
            let values = group
                .iter()
                .flat_map(|s| s.data_files())
                .map(|f| (f.first_row_id.unwrap() / 10) as i32)
                .collect::<Vec<_>>();
            let blocked = if values[0] == 0 {
                wait.lock().unwrap().take()
            } else {
                None
            };
            Ok(controlled_stream(
                values,
                blocked,
                reads.clone(),
                drops.clone(),
            ))
        });
        let mut output = execute_file_groups(work, false, None, read);
        let early = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut values = Vec::new();
            for _ in 0..5 {
                values.push(value(&output.next().await.unwrap().unwrap()));
            }
            values
        })
        .await;
        release.send(()).unwrap();
        assert_eq!(
            early.expect("a later original split must not wait for the first"),
            vec![10, 11, 12, 13, 14]
        );
        let rest = output
            .map_ok(|batch| value(&batch))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(rest, vec![0, 1, 2, 3, 4]);
        assert_eq!(dropped.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn independent_file_work_preserves_merge_groups_and_selections() {
        let original = independent_split();
        assert!(independent_file_groups(std::slice::from_ref(&original), 1).is_none());
        let work = independent_file_groups(std::slice::from_ref(&original), 3).unwrap();
        assert_eq!(work.len(), 1);
        assert_eq!(work[0].len(), 3);
        assert_eq!(
            work[0].iter().map(|s| s.data_files().len()).sum::<usize>(),
            5
        );
        let mut files = original.data_files().to_vec();
        files[1].first_row_id = Some(0); // overlapping column versions need merging
        let overlap = original.for_data_evolution_files(files);
        assert!(independent_file_groups(&[overlap], 8).is_none());
        let selected = original.with_selected_row_ranges(vec![]);
        assert!(independent_file_groups(&[selected], 8).is_none());
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
    async fn blob_output_budget_admits_later_group_only_when_unordered() {
        for ordered in [false, true] {
            let (release, wait) = oneshot::channel();
            let wait = Mutex::new(Some(wait));
            let admitted = Arc::new(Mutex::new(Vec::new()));
            let started = admitted.clone();
            let reads = Arc::new(AtomicUsize::new(0));
            let drops = Arc::new(AtomicUsize::new(0));
            let mut output = merge_items_budgeted(
                vec![0, 1, 2],
                vec![Some(3), Some(4), Some(1)],
                4,
                ordered,
                3,
                move |index| {
                    started.lock().unwrap().push(index);
                    let gate = if index == 0 {
                        wait.lock().unwrap().take()
                    } else {
                        None
                    };
                    controlled_stream(vec![index], gate, reads.clone(), drops.clone())
                },
            );
            assert!(output.next().now_or_never().is_none());
            if ordered {
                assert_eq!(*admitted.lock().unwrap(), vec![0]);
            } else {
                let batch = tokio::time::timeout(Duration::from_secs(5), output.next())
                    .await
                    .expect("the small group did not use the remaining permit")
                    .unwrap()
                    .unwrap();
                assert_eq!(value(&batch), 2);
                assert_eq!(*admitted.lock().unwrap(), vec![0, 2]);
            }
            release.send(()).unwrap();
            let rest = tokio::time::timeout(
                Duration::from_secs(5),
                output.map_ok(|batch| value(&batch)).try_collect::<Vec<_>>(),
            )
            .await
            .expect("a large group could not acquire its budget")
            .unwrap();
            assert_eq!(rest, if ordered { vec![0, 1, 2] } else { vec![0, 1] });
        }
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
                field_ids: HashSet::new(),
                unknown_external: false,
                serialize_unknown: false,
                table_schema_id: 0,
                file_schema_manager: None,
                implicit_blob_columns: false,
                historical_schemas: HashMap::new(),
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
    async fn blob_read_error_cancels_pending_schema_preparation() {
        let drops = Arc::new(AtomicUsize::new(0));
        let guard = DropGuard(drops.clone());
        let preparing = BudgetPreparations::new();
        preparing.push(
            async move {
                let _guard = guard;
                futures::future::pending().await
            }
            .boxed(),
        );
        let ready = VecDeque::from([(0, (), Some(1))]);
        let mut output = merge_budgeted_groups(
            ready,
            preparing,
            1,
            false,
            2,
            |()| {
                Box::pin(futures::stream::once(async {
                    Err(crate::Error::DataInvalid {
                        message: "BLOB read failed".into(),
                        source: None,
                    })
                }))
            },
            |_, _, _| false,
        );
        let batch = tokio::time::timeout(Duration::from_secs(5), output.next())
            .await
            .expect("the pending schema held up a read error");
        assert!(batch.unwrap().is_err());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
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
