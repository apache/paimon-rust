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
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::task::JoinSet;

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
    preserve_split_order: bool,
    limit: Option<usize>,
    read: SplitReader,
) -> ArrowRecordBatchStream {
    let streams = split_groups(splits, parallelism)
        .into_iter()
        .map(|group| {
            let read = Arc::clone(&read);
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
            }) as ArrowRecordBatchStream
        })
        .collect();
    merge_streams(streams, preserve_split_order, limit)
}

fn next_batch(index: usize, mut stream: ArrowRecordBatchStream) -> BoxFuture<'static, NextBatch> {
    async move {
        let batch = stream.next().await;
        (index, batch, stream)
    }
    .boxed()
}

/// Every group has one pending read or one ready batch. Multiple groups run
/// on owned runtime tasks so synchronous decoder work can use separate CPUs.
/// Dropping the output aborts every task through JoinSet's drop implementation.
fn merge_streams(
    mut streams: Vec<ArrowRecordBatchStream>,
    preserve_order: bool,
    limit: Option<usize>,
) -> ArrowRecordBatchStream {
    if limit == Some(0) {
        return Box::pin(futures::stream::empty());
    }
    // Ordinary one-worker reads retain the original executor context and avoid
    // requiring a Tokio runtime just to advance a stream.
    if streams.len() <= 1 {
        return limit_single_stream(
            streams
                .pop()
                .unwrap_or_else(|| Box::pin(futures::stream::empty())),
            limit,
        );
    }
    Box::pin(async_stream::try_stream! {
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(error) => {
                drop(streams);
                Err(crate::Error::UnexpectedError {
                    message: "Parallel table reads require a Tokio runtime".into(),
                    source: Some(Box::new(error)),
                })?;
                return;
            }
        };
        let count = streams.len();
        let mut pending = JoinSet::new();
        for (index, stream) in streams.into_iter().enumerate() {
            pending.spawn_on(next_batch(index, stream), &handle);
        }
        let mut ready = BTreeMap::new();
        let mut current = 0;
        let mut remaining = limit;
        while remaining != Some(0) && current < count {
            let next = if preserve_order {
                loop {
                    if let Some(next) = ready.remove(&current) { break Ok(Some(next)); }
                    let next = match pending.join_next().await {
                        Some(Ok(next)) => next,
                        Some(Err(error)) => break Err(worker_error(error)),
                        None => break Ok(None),
                    };
                    // Surface a failed worker even while an earlier group is
                    // blocked; cleanup happens before exposing the error.
                    if next.0 == current || next.1.as_ref().is_some_and(Result::is_err) {
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
                            pending.shutdown().await;
                            yield batch;
                            break;
                        }
                        yield batch;
                    }
                    // Schedule another decode only after the caller resumes.
                    pending.spawn_on(next_batch(index, stream), &handle);
                }
                None if preserve_order => current += 1,
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
    use arrow_array::Int32Array;
    use futures::channel::oneshot;
    use futures::TryStreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
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
        for preserve_order in [false, true] {
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
            let batches = merge_streams(streams, preserve_order, None)
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
