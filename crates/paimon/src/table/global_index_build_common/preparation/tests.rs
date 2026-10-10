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

use super::*;
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{DataType, GlobalIndexMeta, IndexFileMeta, IntType, Schema, TableSchema};
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

fn table() -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "concurrency"),
        format!("memory:/index-concurrency-{}", uuid::Uuid::new_v4()),
        TableSchema::new(0, &schema),
        None,
    )
}

#[derive(Clone, Copy)]
enum Outcome {
    File,
    ExternalFile,
    Empty,
    Fail,
    Panic,
}

#[derive(Default)]
struct Counters {
    admitted: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
}

struct Active(Arc<Counters>);
impl Active {
    fn new(counters: Arc<Counters>) -> Self {
        let count = counters.active.fetch_add(1, Ordering::SeqCst) + 1;
        counters.peak.fetch_max(count, Ordering::SeqCst);
        Self(counters)
    }
}
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn private_file(table: &Table, ordinal: usize, external: bool) -> CommitMessage {
    let name = format!("private-{ordinal}.index");
    let external_path = external.then(|| format!("{}-external/{name}", table.location()));
    let path = external_path
        .clone()
        .unwrap_or_else(|| format!("{}/index/{name}", table.location()));
    table
        .file_io()
        .new_output(&path)
        .unwrap()
        .write(Bytes::from_static(b"private"))
        .await
        .unwrap();
    let mut message = CommitMessage::new(vec![], 0, vec![]);
    message.new_index_files.push(IndexFileMeta {
        index_type: "ivf-flat".into(),
        file_name: name,
        file_size: 7,
        row_count: 1,
        deletion_vectors_ranges: None,
        external_path,
        global_index_meta: Some(GlobalIndexMeta {
            row_range_start: ordinal as i64,
            row_range_end: ordinal as i64,
            index_field_id: 0,
            extra_field_ids: None,
            source_meta: None,
            index_meta: None,
        }),
    });
    message
}

type Build = JoinHandle<Result<Vec<(CommitMessage, usize)>>>;
struct Running {
    task: Build,
    started: mpsc::UnboundedReceiver<usize>,
    releases: Vec<Option<oneshot::Sender<()>>>,
    counters: Arc<Counters>,
    finished: mpsc::UnboundedReceiver<usize>,
}

fn start(table: &Table, parallelism: usize, outcomes: Vec<Outcome>) -> Running {
    let (started, receiver) = mpsc::unbounded_channel();
    let (finished, finished_receiver) = mpsc::unbounded_channel();
    let mut releases = Vec::new();
    let jobs = outcomes
        .into_iter()
        .enumerate()
        .map(|(ordinal, outcome)| {
            let (sender, receiver) = oneshot::channel();
            releases.push(Some(sender));
            (ordinal, outcome, receiver)
        })
        .collect();
    let owner = table.clone();
    let worker_table = table.clone();
    let counters = Arc::new(Counters::default());
    let observed = counters.clone();
    let task = tokio::spawn(async move {
        prepare_shards(
            &owner,
            jobs,
            parallelism,
            move |(ordinal, outcome, release): (_, _, oneshot::Receiver<()>)| {
                let table = worker_table.clone();
                let started = started.clone();
                let observed = observed.clone();
                let finished = finished.clone();
                observed.admitted.fetch_add(1, Ordering::SeqCst);
                async move {
                    let _active = Active::new(observed);
                    let message = match outcome {
                        Outcome::File | Outcome::ExternalFile => Some(
                            private_file(&table, ordinal, matches!(outcome, Outcome::ExternalFile))
                                .await,
                        ),
                        _ => None,
                    };
                    started.send(ordinal).unwrap();
                    release.await.unwrap();
                    finished.send(ordinal).unwrap();
                    match outcome {
                        Outcome::Empty => Ok(None),
                        Outcome::Fail => Err(Error::UnexpectedError {
                            message: "original shard failure".into(),
                            source: Some(Box::new(std::io::Error::other("original native cause"))),
                        }),
                        Outcome::Panic => panic!("original shard panic"),
                        _ => Ok(message.map(|message| (message, ordinal))),
                    }
                }
            },
        )
        .await
    });
    Running {
        task,
        started: receiver,
        releases,
        counters,
        finished: finished_receiver,
    }
}

impl Running {
    async fn next_started(&mut self) -> usize {
        tokio::time::timeout(Duration::from_secs(10), self.started.recv())
            .await
            .expect("admitted shard must start")
            .expect("build still has work")
    }
    fn release(&mut self, ordinal: usize) {
        self.releases[ordinal].take().unwrap().send(()).unwrap();
    }
    async fn finish(self) -> Result<Vec<(CommitMessage, usize)>> {
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .expect("build must finish")
            .unwrap()
    }
}

#[tokio::test]
async fn bounded_parallel_jobs_return_messages_in_plan_order() {
    let table = table();
    let mut running = start(&table, 2, vec![Outcome::File; 3]);
    assert_eq!(running.next_started().await, 0);
    assert_eq!(running.next_started().await, 1);
    assert_eq!(running.counters.admitted.load(Ordering::SeqCst), 2);
    running.release(1);
    assert_eq!(running.next_started().await, 2);
    running.release(2);
    running.release(0);
    let counters = running.counters.clone();
    let prepared = running.finish().await.unwrap();
    assert_eq!(counters.peak.load(Ordering::SeqCst), 2);
    assert_eq!(counters.active.load(Ordering::SeqCst), 0);
    assert_eq!(
        prepared.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    for (message, ordinal) in prepared {
        assert_eq!(
            message.new_index_files[0]
                .global_index_meta
                .as_ref()
                .unwrap()
                .row_range_start,
            ordinal as i64
        );
        assert!(table
            .file_io()
            .exists(&format!(
                "{}/index/private-{ordinal}.index",
                table.location()
            ))
            .await
            .unwrap());
    }
}

#[tokio::test]
async fn one_worker_remains_sequential_and_empty_outputs_do_not_change_order() {
    let table = table();
    let mut running = start(
        &table,
        1,
        vec![Outcome::File, Outcome::Empty, Outcome::File],
    );
    for ordinal in 0..3 {
        assert_eq!(running.next_started().await, ordinal);
        assert_eq!(running.counters.active.load(Ordering::SeqCst), 1);
        assert_eq!(
            running.counters.admitted.load(Ordering::SeqCst),
            ordinal + 1
        );
        running.release(ordinal);
    }
    let prepared = running.finish().await.unwrap();
    assert_eq!(
        prepared
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
}

#[tokio::test]
async fn a_large_limit_only_admits_available_shards() {
    let table = table();
    let mut running = start(&table, usize::MAX, vec![Outcome::Empty; 2]);
    assert_eq!(running.next_started().await, 0);
    assert_eq!(running.next_started().await, 1);
    assert_eq!(running.counters.admitted.load(Ordering::SeqCst), 2);
    running.release(0);
    running.release(1);
    assert!(running.finish().await.unwrap().is_empty());
}

#[tokio::test]
async fn failure_drains_inflight_jobs_before_deleting_only_private_outputs() {
    for outcome in [Outcome::Fail, Outcome::Panic] {
        for external in [false, true] {
            let table = table();
            let protected = private_file(&table, 99, external).await;
            let protected_path = protected.new_index_files[0]
                .external_path
                .clone()
                .unwrap_or_else(|| format!("{}/index/private-99.index", table.location()));
            let mode = if external {
                Outcome::ExternalFile
            } else {
                Outcome::File
            };
            let mut running = start(&table, 2, vec![mode, outcome, mode]);
            assert_eq!(running.next_started().await, 0);
            assert_eq!(running.next_started().await, 1);
            let path = if external {
                format!("{}-external/private-0.index", table.location())
            } else {
                format!("{}/index/private-0.index", table.location())
            };
            running.release(1);
            // The failed worker returns immediately after this signal; the
            // coordinator then waits on job 0 before it can perform cleanup.
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(10), running.finished.recv())
                    .await
                    .unwrap(),
                Some(1)
            );
            assert_eq!(running.counters.active.load(Ordering::SeqCst), 1);
            assert!(!running.task.is_finished());
            assert!(table.file_io().exists(&path).await.unwrap());
            running.release(0);
            let counters = running.counters.clone();
            let error = running.finish().await.unwrap_err();
            match outcome {
                Outcome::Fail => match error {
                    Error::UnexpectedError { message, source } => {
                        assert_eq!(message, "original shard failure");
                        assert_eq!(source.unwrap().to_string(), "original native cause");
                    }
                    _ => panic!("original failure was replaced"),
                },
                Outcome::Panic => assert!(error.to_string().contains("original shard panic")),
                _ => unreachable!(),
            }
            assert_eq!(counters.active.load(Ordering::SeqCst), 0);
            assert_eq!(counters.admitted.load(Ordering::SeqCst), 2);
            assert!(!table.file_io().exists(&path).await.unwrap());
            assert!(table.file_io().exists(&protected_path).await.unwrap());
        }
    }
}

#[tokio::test]
async fn empty_input_and_zero_limit_never_construct_a_worker() {
    let table = table();
    let make = |_| async {
        panic!("no worker may be constructed");
        #[allow(unreachable_code)]
        Ok::<_, Error>(None::<(CommitMessage, ())>)
    };
    assert!(prepare_shards(&table, Vec::<()>::new(), 1, make)
        .await
        .unwrap()
        .is_empty());
    assert!(prepare_shards(&table, vec![()], 0, make)
        .await
        .unwrap_err()
        .to_string()
        .contains("parallelism"));
}
