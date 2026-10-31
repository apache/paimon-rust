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

use std::collections::BTreeMap;
use std::future::Future;

use futures::stream::{FuturesUnordered, StreamExt};

use super::{prepare_results, CompactExecutor, CompactPlan, PreparedCompact};
use crate::table::{CommitMessage, Table, TableCommit};
use crate::{Error, Result};
#[cfg(test)]
mod metadata_tests;

pub(in crate::table) async fn execute_auto_plan(
    table: &Table,
    plan: &CompactPlan,
    executor: &CompactExecutor,
    parallelism: usize,
) -> Result<PreparedCompact> {
    // Total-bucket metadata can change, but tasks writing one physical bucket
    // still run in order. Append/DE may produce several tasks for that bucket.
    let mut buckets = BTreeMap::<_, Vec<_>>::new();
    for task in plan.tasks() {
        buckets
            .entry((task.partition(), task.bucket()))
            .or_default()
            .push(task);
    }
    let inputs = match plan.tasks().first() {
        Some(task) => Some(executor.capture_inputs(task).await?),
        None => None,
    };
    let (results, error) = run_buckets(buckets.into_values(), parallelism, |task| {
        executor.execute_captured(task, inputs.as_ref().expect("nonempty task plan"))
    })
    .await;
    let prepared = match error {
        Some(error) => Err(error),
        None => prepare_results(table, &results),
    };
    if prepared.is_err() {
        // All started tasks have finished. Clean every successful output by
        // its own task, regardless of completion order. No publication began.
        let messages = results
            .iter()
            .map(|result| {
                let mut message = CommitMessage::new(
                    result.task.partition.clone(),
                    result.task.bucket,
                    result.files.clone(),
                );
                message.new_changelog_files = result.changelog_files.clone();
                message.new_index_files = result
                    .index_entries
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .filter(|e| e.kind == crate::spec::FileKind::Add)
                    .map(|e| e.index_file.clone())
                    .collect();
                message
            })
            .collect::<Vec<_>>();
        let _ = TableCommit::new(table.clone(), "auto-compact-cleanup".into())
            .abort(&messages)
            .await;
    }
    prepared
}

async fn run_buckets<T, R, F, Fut>(
    buckets: impl IntoIterator<Item = Vec<T>>,
    parallelism: usize,
    execute: F,
) -> (Vec<R>, Option<Error>)
where
    F: Fn(T) -> Fut,
    Fut: Future<Output = Result<R>>,
{
    let mut pending = buckets.into_iter().map(|bucket| {
        let execute = &execute;
        async move {
            let mut results = Vec::new();
            for task in bucket {
                match execute(task).await {
                    Ok(result) => results.push(result),
                    Err(error) => return (results, Some(error)),
                }
            }
            (results, None)
        }
    });
    let mut running: FuturesUnordered<_> = pending.by_ref().take(parallelism).collect();
    let mut results = Vec::new();
    let mut error = None;
    while let Some((completed, failed)) = running.next().await {
        results.extend(completed);
        if let Some(failed) = failed {
            // An unsupported encoding must not hide an I/O/data failure in
            // another bucket when plain stateful compaction falls back.
            if error.is_none() || matches!(error, Some(Error::Unsupported { .. })) {
                error = Some(failed);
            }
        }
        if error.is_none() {
            if let Some(next) = pending.next() {
                running.push(next);
            }
        }
        // After failure, drain already started buckets without cancelling
        // writers. Queued buckets remain untouched.
    }
    (results, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{mpsc, Semaphore};

    #[tokio::test]
    async fn bounded_buckets_overlap_but_tasks_in_one_bucket_stay_ordered() {
        for parallelism in [1, 2] {
            let gates = Arc::new([Semaphore::new(0), Semaphore::new(0), Semaphore::new(0)]);
            let (started, mut events) = mpsc::unbounded_channel();
            let active = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let per_bucket = Arc::new([
                AtomicUsize::new(0),
                AtomicUsize::new(0),
                AtomicUsize::new(0),
            ]);
            let run = run_buckets(
                vec![vec![(0, 0), (0, 1)], vec![(1, 0)], vec![(2, 0)]],
                parallelism,
                |(bucket, task)| {
                    let (gates, active, peak, per_bucket, started) = (
                        gates.clone(),
                        active.clone(),
                        peak.clone(),
                        per_bucket.clone(),
                        started.clone(),
                    );
                    async move {
                        assert_eq!(per_bucket[bucket].fetch_add(1, Ordering::SeqCst), 0);
                        let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(count, Ordering::SeqCst);
                        started.send((bucket, task)).unwrap();
                        gates[bucket].acquire().await.unwrap().forget();
                        active.fetch_sub(1, Ordering::SeqCst);
                        per_bucket[bucket].fetch_sub(1, Ordering::SeqCst);
                        Ok((bucket, task))
                    }
                },
            );
            let control = async {
                let first = events.recv().await.unwrap();
                assert_eq!(first, (0, 0));
                if parallelism == 2 {
                    assert_eq!(events.recv().await.unwrap(), (1, 0));
                }
                assert_eq!(active.load(Ordering::SeqCst), parallelism);
                gates[0].add_permits(1);
                assert_eq!(events.recv().await.unwrap(), (0, 1));
                // Bucket 2 cannot start until bucket 0's final task completes.
                assert!(events.try_recv().is_err());
                gates[0].add_permits(1);
                if parallelism == 1 {
                    assert_eq!(events.recv().await.unwrap(), (1, 0));
                    gates[1].add_permits(1);
                }
                assert_eq!(events.recv().await.unwrap(), (2, 0));
                gates[1].add_permits(1);
                gates[2].add_permits(1);
            };
            let ((mut results, error), ()) =
                tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(run, control) })
                    .await
                    .expect("bounded buckets must progress without serializing independent work");
            assert!(error.is_none());
            results.sort_unstable();
            assert_eq!(results, [(0, 0), (0, 1), (1, 0), (2, 0)]);
            assert_eq!(peak.load(Ordering::SeqCst), parallelism);
            assert_eq!(active.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn failure_drains_started_buckets_stops_the_queue_and_preserves_hard_errors() {
        let gate = Semaphore::new(0);
        let (started, mut events) = mpsc::unbounded_channel();
        let run = run_buckets(vec![vec![0], vec![1, 2], vec![3]], 2, |task| {
            let (gate, started) = (&gate, &started);
            async move {
                started.send(task).unwrap();
                match task {
                    0 => Err(Error::Unsupported {
                        message: "encoding".into(),
                    }),
                    1 => {
                        gate.acquire().await.unwrap().forget();
                        Ok(1)
                    }
                    2 => Err(Error::UnexpectedError {
                        message: "I/O failure".into(),
                        source: None,
                    }),
                    _ => panic!("queued bucket must not start after failure"),
                }
            }
        });
        let control = async {
            assert_eq!(events.recv().await.unwrap(), 0);
            assert_eq!(events.recv().await.unwrap(), 1);
            gate.add_permits(1);
        };
        let ((results, error), ()) =
            tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(run, control) })
                .await
                .expect("started bucket must be drained");
        assert_eq!(results, [1]);
        assert!(matches!(error, Some(Error::UnexpectedError { .. })));
    }
}
