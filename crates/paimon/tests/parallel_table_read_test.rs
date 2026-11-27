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

mod common;

use arrow_array::{ArrayRef, Int32Array, LargeBinaryArray, RecordBatch, StringArray};
use bytes::Bytes;
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::arrow::ReadBudget;
use paimon::io::FileBlockCache;
use paimon::resource::ResourceContext;
use paimon::spec::{
    BinaryRow, BlobType, DataType, Datum, IntType, PredicateBuilder, Schema, SchemaChange,
    TableSchema,
};
use paimon::table::{DataSplitBuilder, Table};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

async fn table(mode: &str) -> Table {
    let mut schema = Schema::builder()
        .column("p", DataType::Int(IntType::new()))
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .partition_keys(["p"])
        .option("read.batch-size", "1");
    if mode == "pk" {
        schema = schema.primary_key(["p", "id"]).option("bucket", "1");
    } else if mode == "de" {
        schema = schema
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true");
    }
    let (io, table) = memory_table(
        &format!("memory:/parallel_table_read_{mode}"),
        TableSchema::new(0, &schema.build().unwrap()),
    );
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    for generation in 0..if mode == "pk" { 2 } else { 1 } {
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        for partition in 0..4 {
            let batch = RecordBatch::try_from_iter([
                (
                    "p",
                    Arc::new(Int32Array::from(vec![partition; 3])) as ArrayRef,
                ),
                ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
                (
                    "value",
                    Arc::new(Int32Array::from(vec![generation * 100 + partition; 3])) as ArrayRef,
                ),
            ])
            .unwrap();
            writer.write_arrow_batch(&batch).await.unwrap();
        }
        builder
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
    }
    table
}

fn rows(batches: &[RecordBatch]) -> Vec<(i32, i32, i32)> {
    batches
        .iter()
        .flat_map(|batch| {
            let values = ["p", "id", "value"].map(|name| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
            });
            (0..batch.num_rows()).map(move |row| {
                (
                    values[0].value(row),
                    values[1].value(row),
                    values[2].value(row),
                )
            })
        })
        .collect()
}

#[tokio::test]
async fn parallel_reads_preserve_requested_order_and_enforce_one_global_limit() {
    for mode in ["append", "de", "pk"] {
        let table = table(mode).await;
        let mut splits = table
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .unwrap()
            .splits()
            .to_vec();
        assert_eq!(splits.len(), 4, "{mode}");
        splits.reverse();
        let predicate = PredicateBuilder::new(table.schema().fields())
            .greater_than("id", Datum::Int(1))
            .unwrap();
        let mut builder = table.new_read_builder();
        builder.with_filter(predicate);
        let mut expected = Vec::new();
        for split in &splits {
            let batches = builder
                .new_read()
                .unwrap()
                .to_arrow(std::slice::from_ref(split))
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            expected.extend(rows(&batches));
        }
        assert_eq!(expected.len(), 8);
        if mode == "pk" {
            assert!(expected.iter().all(|row| row.2 >= 100));
        }
        for parallelism in [1, 2, 8] {
            for ordered in [false, true] {
                for limit in [None, Some(0), Some(3), Some(100)] {
                    let mut limited = builder.clone();
                    if let Some(limit) = limit {
                        limited.with_limit(limit);
                    }
                    let read = limited
                        .new_read()
                        .unwrap()
                        .with_parallelism(parallelism)
                        .unwrap()
                        .with_ordered(ordered)
                        // One shared slot must not deadlock while workers wait.
                        .with_parquet_read_budget(Arc::new(
                            ReadBudget::new(1, 1024 * 1024).unwrap(),
                        ));
                    let batches = tokio::time::timeout(
                        std::time::Duration::from_secs(15),
                        read.to_arrow(&splits).unwrap().try_collect::<Vec<_>>(),
                    )
                    .await
                    .expect("parallel read stalled on the shared budget")
                    .unwrap();
                    let result = rows(&batches);
                    assert_eq!(
                        result.len(),
                        limit.unwrap_or(8).min(8),
                        "{mode}, p={parallelism}, limit={limit:?}"
                    );
                    if ordered || parallelism == 1 {
                        assert_eq!(result, expected[..result.len()]);
                    } else {
                        assert!(result.iter().all(|row| expected.contains(row)));
                        let mut unique = result.clone();
                        unique.sort_unstable();
                        unique.dedup();
                        assert_eq!(unique.len(), result.len());
                    }
                }
            }
        }
        let batches = builder
            .new_read()
            .unwrap()
            .with_parallelism(3)
            .unwrap()
            .with_ordered(true)
            .to_arrow_with_row_kind(&splits)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(rows(&batches), expected);
        assert!(batches.iter().all(|batch| batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .all(|kind| kind == Some("+I"))));
        let mut zero = builder.clone();
        zero.with_projection(&[]).unwrap().with_limit(5);
        let batches = zero
            .new_read()
            .unwrap()
            .with_parallelism(3)
            .unwrap()
            .to_arrow(&splits)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
        assert!(batches.iter().all(|batch| batch.num_columns() == 0));
    }
}

#[tokio::test]
async fn independent_data_evolution_files_preserve_filtered_rows_and_limit() {
    let table = table("de").await;
    for start in [4, 7] {
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        let batch = RecordBatch::try_from_iter([
            ("p", Arc::new(Int32Array::from(vec![0; 3])) as ArrayRef),
            (
                "id",
                Arc::new(Int32Array::from(vec![start, start + 1, start + 2])) as ArrayRef,
            ),
            ("value", Arc::new(Int32Array::from(vec![0; 3])) as ArrayRef),
        ])
        .unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        builder
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
    }
    let predicates = PredicateBuilder::new(table.schema().fields());
    let mut builder = table.new_read_builder();
    builder.with_filter(predicates.equal("p", Datum::Int(0)).unwrap());
    let plan = builder.new_scan().plan().await.unwrap();
    let splits = plan.splits();
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].data_files().len(), 3);
    builder.with_filter(predicates.greater_than("id", Datum::Int(1)).unwrap());
    for parallelism in [1, 3] {
        for ordered in [false, true] {
            for limit in [None, Some(5)] {
                let mut selected = builder.clone();
                if let Some(limit) = limit {
                    selected.with_limit(limit);
                }
                let result = selected
                    .new_read()
                    .unwrap()
                    .with_parallelism(parallelism)
                    .unwrap()
                    .with_ordered(ordered)
                    .to_arrow(splits)
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                let result = rows(&result);
                assert_eq!(result.len(), limit.unwrap_or(8));
                if ordered || parallelism == 1 {
                    assert_eq!(
                        result,
                        (2..2 + result.len() as i32)
                            .map(|id| (0, id, 0))
                            .collect::<Vec<_>>()
                    );
                } else {
                    let mut unique = result.clone();
                    unique.sort_unstable();
                    unique.dedup();
                    assert_eq!(unique.len(), result.len());
                    assert!(result
                        .iter()
                        .all(|&(p, id, value)| p == 0 && (2..10).contains(&id) && value == 0));
                }
            }
        }
    }
}

/// Keep the first file blocked while another file can make progress.
#[derive(Debug)]
struct BlockFirstIndependentFile {
    first: String,
    later: String,
    first_started: AtomicBool,
    later_started: AtomicBool,
    released: AtomicBool,
    release: Notify,
}

#[async_trait::async_trait]
impl FileBlockCache for BlockFirstIndependentFile {
    async fn get(&self, path: &str, _range: Range<u64>) -> Option<Bytes> {
        if path.ends_with(&self.later) {
            self.later_started.store(true, Ordering::SeqCst);
        }
        if path.ends_with(&self.first) {
            let released = self.release.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            self.first_started.store(true, Ordering::SeqCst);
            if !self.released.load(Ordering::SeqCst) {
                released.await;
            }
        }
        None
    }

    async fn put(&self, _path: &str, _offset: u64, _data: Bytes) {}
    async fn invalidate_path(&self, _path: &str) {}
    async fn invalidate_prefix(&self, _prefix: &str) {}
}

#[tokio::test]
async fn independent_files_keep_supplied_split_order_in_unordered_reads() {
    let table = table("de").await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(
            &RecordBatch::try_from_iter([
                ("p", Arc::new(Int32Array::from(vec![0; 3])) as ArrayRef),
                ("id", Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef),
                ("value", Arc::new(Int32Array::from(vec![0; 3])) as ArrayRef),
            ])
            .unwrap(),
        )
        .await
        .unwrap();
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    let mut scan = table.new_read_builder();
    let predicate = PredicateBuilder::new(table.schema().fields())
        .equal("p", Datum::Int(0))
        .unwrap();
    scan.with_filter(predicate);
    let plan = scan.new_scan().plan().await.unwrap();
    let splits = plan.splits();
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].data_files().len(), 2);

    for (limit, shared_memory) in [
        (None, false),
        (Some(4), false),
        (None, true),
        (Some(4), true),
    ] {
        let gate = Arc::new(BlockFirstIndependentFile {
            first: splits[0].data_files()[0].file_name.clone(),
            later: splits[0].data_files()[1].file_name.clone(),
            first_started: AtomicBool::new(false),
            later_started: AtomicBool::new(false),
            released: AtomicBool::new(false),
            release: Notify::new(),
        });
        let io = table
            .file_io()
            .clone()
            .with_file_block_cache(gate.clone(), 64 * 1024, "data")
            .unwrap();
        let gated = Table::new(
            io,
            table.identifier().clone(),
            table.location().into(),
            table.schema().clone(),
            None,
        );
        let mut builder = gated.new_read_builder();
        if let Some(limit) = limit {
            builder.with_limit(limit);
        }
        if shared_memory {
            builder.with_resources(
                ResourceContext::builder()
                    .memory_limit(1024 * 1024)
                    .build()
                    .unwrap(),
            );
        }
        // Deliberately use the default unordered mode. Only original splits,
        // not the independent files within one split, may emit out of order.
        let output = builder
            .new_read()
            .unwrap()
            .with_parallelism(2)
            .unwrap()
            .to_arrow(splits)
            .unwrap();
        let (send, mut received) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(
            output
                .inspect_ok(move |batch| {
                    send.send(rows(std::slice::from_ref(batch))).unwrap();
                })
                .try_collect::<Vec<_>>(),
        );
        let concurrent = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !gate.first_started.load(Ordering::SeqCst)
                || (!shared_memory && !gate.later_started.load(Ordering::SeqCst))
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let premature =
            tokio::time::timeout(std::time::Duration::from_millis(100), received.recv()).await;
        let later_started = gate.later_started.load(Ordering::SeqCst);
        gate.released.store(true, Ordering::SeqCst);
        gate.release.notify_waiters();
        let batches = tokio::time::timeout(std::time::Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(concurrent.is_ok(), "expected readers did not start");
        assert_eq!(
            later_started, !shared_memory,
            "shared memory must not be held by a later ordered file"
        );
        assert_eq!(
            rows(&batches),
            (1..=limit.unwrap_or(6) as i32)
                .map(|id| (0, id, 0))
                .collect::<Vec<_>>()
        );
        assert!(
            premature.is_err(),
            "later file emitted before the first file was released"
        );
    }
}

#[tokio::test]
async fn data_evolution_streaming_splits_preserve_order_and_limit() {
    let table = table("de").await;
    let mut planned = table
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .unwrap()
        .splits()
        .to_vec();
    assert_eq!(planned.len(), 4);
    planned.reverse();

    for mixed in [false, true] {
        let splits = planned
            .iter()
            .enumerate()
            .map(|(index, split)| {
                let mut builder = DataSplitBuilder::new()
                    .with_snapshot(split.snapshot_id())
                    .with_partition(split.partition().clone())
                    .with_bucket(split.bucket())
                    .with_bucket_path(split.bucket_path().to_string())
                    .with_total_buckets(split.total_buckets())
                    .with_data_files(split.data_files().to_vec())
                    .with_raw_convertible(split.raw_convertible())
                    // Alternating flags keep any group of two splits mixed.
                    .with_streaming(!mixed || index % 2 == 0);
                if let Some(files) = split.data_deletion_files() {
                    builder = builder.with_data_deletion_files(files.to_vec());
                }
                if let Some(ranges) = split.row_ranges() {
                    builder = builder.with_row_ranges(ranges.to_vec());
                }
                builder.build().unwrap()
            })
            .collect::<Vec<_>>();
        let mut expected = Vec::new();
        for split in &splits {
            let batches = table
                .new_read_builder()
                .new_read()
                .unwrap()
                .to_arrow(std::slice::from_ref(split))
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            expected.extend(rows(&batches));
        }
        assert_eq!(expected.len(), 12);

        for include_row_kind in [false, true] {
            for parallelism in [1, 2] {
                for ordered in [false, true] {
                    for limit in [None, Some(0), Some(1), Some(4), Some(7)] {
                        let label = format!(
                            "mixed={mixed}, row_kind={include_row_kind}, p={parallelism}, ordered={ordered}, limit={limit:?}"
                        );
                        let mut builder = table.new_read_builder();
                        if let Some(limit) = limit {
                            builder.with_limit(limit);
                        }
                        let read = builder
                            .new_read()
                            .unwrap()
                            .with_parallelism(parallelism)
                            .unwrap()
                            .with_ordered(ordered);
                        let stream = if include_row_kind {
                            read.to_arrow_with_row_kind(&splits)
                        } else {
                            read.to_arrow(&splits)
                        }
                        .unwrap();
                        let batches = tokio::time::timeout(
                            std::time::Duration::from_secs(15),
                            stream.try_collect::<Vec<_>>(),
                        )
                        .await
                        .expect("data-evolution split read stalled")
                        .unwrap();
                        let result = rows(&batches);
                        assert_eq!(result.len(), limit.unwrap_or(12).min(12), "{label}");
                        if ordered || parallelism == 1 {
                            assert_eq!(result, expected[..result.len()], "{label}");
                        } else {
                            assert!(result.iter().all(|row| expected.contains(row)), "{label}");
                            let mut unique = result.clone();
                            unique.sort_unstable();
                            unique.dedup();
                            assert_eq!(unique.len(), result.len(), "{label}");
                        }
                        if include_row_kind {
                            assert!(
                                batches.iter().all(|batch| batch
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .unwrap()
                                    .iter()
                                    .all(|kind| kind == Some("+I"))),
                                "{label}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn zero_parallelism_fails_before_any_splits_are_read() {
    let table = table("append").await;
    assert!(table
        .new_read_builder()
        .new_read()
        .unwrap()
        .with_parallelism(0)
        .unwrap_err()
        .to_string()
        .contains("parallelism"));
}

#[tokio::test]
async fn ordinary_projection_ignores_blob_output_budget_option() {
    let table = table("append").await;
    let splits = table
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .unwrap()
        .splits()
        .to_vec();
    let table = table.copy_with_options(std::collections::HashMap::from([(
        "read.blob.output.max-buffered-bytes".into(),
        "invalid".into(),
    )]));
    let batches = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .with_parallelism(2)
        .unwrap()
        .to_arrow(&splits)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 12);
}

#[tokio::test]
async fn finite_blob_reads_do_not_open_payloads_in_later_splits() {
    for primary_key in [false, true] {
        let mut schema = Schema::builder()
            .column("p", DataType::Int(IntType::new()))
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Blob(BlobType::new()))
            .partition_keys(["p"])
            .option("read.batch-size", "1")
            .option("read.blob.output.max-buffered-bytes", "2mb");
        if primary_key {
            schema = schema.primary_key(["p", "id"]).option("bucket", "1");
        } else {
            schema = schema
                .option("data-evolution.enabled", "true")
                .option("row-tracking.enabled", "true");
        }
        let (io, table) = memory_table(
            &format!("memory:/parallel_blob_limit_{primary_key}"),
            TableSchema::new(0, &schema.build().unwrap()),
        );
        setup_dirs(&io, table.location()).await;
        persist_table_schema(&io, table.location(), table.schema()).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        let batch = RecordBatch::try_from_iter([
            ("p", Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef),
            (
                "id",
                Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
            ),
            (
                "payload",
                Arc::new(LargeBinaryArray::from(vec![b"a".as_slice(), b"b", b"c"])) as ArrayRef,
            ),
        ])
        .unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        builder
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        let splits = plan.splits();
        assert_eq!(splits.len(), 3);
        for ordered in [false, true] {
            let batches = table
                .new_read_builder()
                .new_read()
                .unwrap()
                .with_parallelism(8)
                .unwrap()
                .with_ordered(ordered)
                .to_arrow(splits)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let mut payloads = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column_by_name("payload")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .unwrap()
                        .iter()
                        .map(|value| value.unwrap().to_vec())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            payloads.sort();
            assert_eq!(payloads, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        }
        let mut deleted = 0;
        for split in &splits[1..] {
            for status in io.list_status_recursive(split.bucket_path()).await.unwrap() {
                if status.path.ends_with(".blob") {
                    io.delete_file(&status.path).await.unwrap();
                    deleted += 1;
                }
            }
        }
        assert!(deleted > 0);
        let mut limited = table.new_read_builder();
        limited.with_limit(1).with_filter(
            PredicateBuilder::new(table.schema().fields())
                .greater_than("id", Datum::Int(0))
                .unwrap(),
        );
        for ordered in [false, true] {
            let batches = limited
                .new_read()
                .unwrap()
                .with_parallelism(8)
                .unwrap()
                .with_ordered(ordered)
                .to_arrow(splits)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            assert!(batches.iter().all(|batch| batch
                .column_by_name("payload")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(0)
                .len()
                == 1));
        }
        assert!(
            table
                .new_read_builder()
                .new_read()
                .unwrap()
                .with_parallelism(8)
                .unwrap()
                .to_arrow(splits)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .is_err(),
            "the later payloads really are missing"
        );
    }
}

/// Delay the first file's footer until a later file starts reading column data.
/// A cache miss hook supplies this ordering without changing the storage backend.
#[derive(Debug)]
struct DelayedFirstFile {
    first_file: String,
    later_file: String,
    first_waited: AtomicBool,
    later_data_started: AtomicBool,
    manually_released: AtomicBool,
    release_first: Notify,
}

impl DelayedFirstFile {
    fn new(first_file: &str, later_file: &str) -> Self {
        Self {
            first_file: first_file.into(),
            later_file: later_file.into(),
            first_waited: AtomicBool::new(false),
            later_data_started: AtomicBool::new(false),
            manually_released: AtomicBool::new(false),
            release_first: Notify::new(),
        }
    }
}

#[async_trait::async_trait]
impl FileBlockCache for DelayedFirstFile {
    async fn get(&self, path: &str, range: Range<u64>) -> Option<Bytes> {
        if path.ends_with(&self.later_file) && range.start == 0 {
            self.later_data_started.store(true, Ordering::SeqCst);
            self.release_first.notify_waiters();
        }
        if path.ends_with(&self.first_file) {
            let released = self.release_first.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            self.first_waited.store(true, Ordering::SeqCst);
            if !self.later_data_started.load(Ordering::SeqCst)
                && !self.manually_released.load(Ordering::SeqCst)
            {
                released.await;
            }
        }
        None
    }

    async fn put(&self, _path: &str, _offset: u64, _data: Bytes) {}
    async fn invalidate_path(&self, _path: &str) {}
    async fn invalidate_prefix(&self, _prefix: &str) {}
}

#[tokio::test]
async fn ordered_split_reads_do_not_wait_on_a_later_row_groups_budget_permit() {
    const GROUP_ROWS: usize = 150_000;
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .option("read.batch-size", "1")
        .build()
        .unwrap();
    let (io, table) = memory_table(
        "memory:/ordered_shared_row_group_budget",
        TableSchema::new(0, &schema),
    );
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    let batch = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int32Array::from_iter_values(0..(2 * GROUP_ROWS) as i32)) as ArrayRef,
    )])
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(GROUP_ROWS))
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .build();
    let mut parquet = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut parquet, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    let metadata = writer.close().unwrap();
    assert_eq!(metadata.num_row_groups(), 2);
    // Parquet prefetches the final 512 KiB for metadata. Keeping the first
    // data block outside that suffix makes the hook identify column I/O.
    assert!(parquet.len() > 1024 * 1024);

    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch.slice(0, 1)).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let template = &messages[0].new_files[0];
    let bucket_path = format!("{}/bucket-0", table.location());
    io.mkdirs(&bucket_path).await.unwrap();
    let mut splits = Vec::new();
    for name in ["data-first.parquet", "data-later.parquet"] {
        io.new_output(&format!("{bucket_path}/{name}"))
            .unwrap()
            .write(Bytes::copy_from_slice(&parquet))
            .await
            .unwrap();
        let mut file = template.clone();
        file.file_name = name.to_string();
        file.file_size = parquet.len() as i64;
        file.row_count = (2 * GROUP_ROWS) as i64;
        file.external_path = None;
        splits.push(
            DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path(bucket_path.clone())
                .with_total_buckets(1)
                .with_data_files(vec![file])
                .build()
                .unwrap(),
        );
    }
    let raw_io = io.clone();
    let order = Arc::new(DelayedFirstFile::new(
        "data-first.parquet",
        "data-later.parquet",
    ));
    let io = io
        .with_file_block_cache(order.clone(), 64 * 1024, "data")
        .unwrap();
    let table = Table::new(
        io,
        table.identifier().clone(),
        table.location().into(),
        table.schema().clone(),
        None,
    );
    let mut builder = table.new_read_builder();
    builder.with_limit(3);
    let output = builder
        .new_read()
        .unwrap()
        .with_parallelism(2)
        .unwrap()
        .with_ordered(true)
        // Two row-group workers enable Parquet prefetch; the 1 MiB byte
        // window admits only one projected 600 KiB row group at a time.
        .with_parquet_read_budget(Arc::new(ReadBudget::new(2, 1024 * 1024).unwrap()))
        .to_arrow(&splits)
        .unwrap();
    let batches = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        output.try_collect::<Vec<_>>(),
    )
    .await
    .expect("ordered splits deadlocked behind a later row-group permit")
    .unwrap();
    assert!(order.first_waited.load(Ordering::SeqCst));
    assert!(order.later_data_started.load(Ordering::SeqCst));
    let ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, [0, 1, 2]);

    // Both limited and full reads need the current split to get memory first.
    for limit in [Some(3), None] {
        let order = Arc::new(DelayedFirstFile::new(
            "data-first.parquet",
            "data-later.parquet",
        ));
        let budgeted_io = raw_io
            .clone()
            .with_file_block_cache(order.clone(), 64 * 1024, "data")
            .unwrap();
        let budgeted_table = Table::new(
            budgeted_io,
            table.identifier().clone(),
            table.location().into(),
            table.schema().clone(),
            None,
        )
        .copy_with_options([("read.batch-size".into(), "1024".into())].into());
        let mut builder = budgeted_table.new_read_builder();
        if let Some(limit) = limit {
            builder.with_limit(limit);
        }
        builder.with_resources(
            ResourceContext::builder()
                .memory_limit(1024 * 1024)
                .build()
                .unwrap(),
        );
        let output = builder
            .new_read()
            .unwrap()
            .with_parallelism(2)
            .unwrap()
            .with_ordered(true)
            .with_parquet_read_budget(Arc::new(ReadBudget::new(2, 1024 * 1024).unwrap()))
            .to_arrow(&splits)
            .unwrap();
        let result = tokio::spawn(async move { output.try_collect::<Vec<_>>().await });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !order.first_waited.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("current split did not begin reading");

        // Only the later split can release the first file before we do so below.
        let later_started = order.release_first.notified();
        tokio::pin!(later_started);
        later_started.as_mut().enable();
        assert!(!order.later_data_started.load(Ordering::SeqCst));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), later_started)
                .await
                .is_err(),
            "later split started before the current split was released"
        );
        order.manually_released.store(true, Ordering::SeqCst);
        order.release_first.notify_waiters();
        let batches = tokio::time::timeout(std::time::Duration::from_secs(10), result)
            .await
            .expect("current split was blocked by speculative memory admission")
            .unwrap()
            .unwrap();
        let ids = batches.iter().flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        });
        let expected = 0..(2 * GROUP_ROWS) as i32;
        assert!(ids.eq(expected
            .clone()
            .chain(expected)
            .take(limit.unwrap_or(4 * GROUP_ROWS))));
        assert_eq!(
            order.later_data_started.load(Ordering::SeqCst),
            limit.is_none()
        );
    }
}

#[tokio::test]
async fn unordered_blob_budget_does_not_deadlock_with_parquet_prefetch() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(BlobType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("blob-descriptor-field", "payload")
        .option("read.batch-size", "1")
        .option("read.blob.output.max-buffered-bytes", "256mb")
        .build()
        .unwrap();
    let (io, table) = memory_table(
        "memory:/unordered_blob_budget",
        TableSchema::new(0, &schema),
    );
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    let payload_path = "memory:/unordered_blob_budget/payload";
    io.new_output(payload_path)
        .unwrap()
        .write(Bytes::from_static(b"hello"))
        .await
        .unwrap();
    let descriptor = paimon::spec::BlobDescriptor::new(payload_path.into(), 0, 5).serialize();
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from_iter_values(0..16)) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(LargeBinaryArray::from(vec![descriptor.as_slice(); 16])) as ArrayRef,
        ),
    ])
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(8))
        .build();
    let mut parquet = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut parquet, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    assert_eq!(writer.close().unwrap().num_row_groups(), 2);

    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch.slice(0, 1)).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let template = &messages[0].new_files[0];
    let bucket_path = format!("{}/bucket-0", table.location());
    io.mkdirs(&bucket_path).await.unwrap();
    let mut splits = Vec::new();
    for name in ["first.parquet", "second.parquet"] {
        io.new_output(&format!("{bucket_path}/{name}"))
            .unwrap()
            .write(Bytes::copy_from_slice(&parquet))
            .await
            .unwrap();
        let mut file = template.clone();
        file.file_name = name.into();
        file.file_size = parquet.len() as i64;
        file.row_count = 16;
        file.first_row_id = Some(0);
        file.external_path = None;
        splits.push(
            DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path(bucket_path.clone())
                .with_total_buckets(1)
                .with_data_files(vec![file])
                .build()
                .unwrap(),
        );
    }
    let output = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .with_parallelism(2)
        .unwrap()
        .with_ordered(false)
        .with_parquet_read_budget(Arc::new(ReadBudget::new(2, 1024 * 1024).unwrap()))
        .to_arrow(&splits)
        .unwrap();
    let batches = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        output.try_collect::<Vec<_>>(),
    )
    .await
    .expect("BLOB output budget deadlocked with Parquet prefetch")
    .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 32);
}

/// Block the first PK file until another split opens its Parquet file.
#[derive(Debug)]
struct ConcurrentPkFiles {
    first_file: String,
    first_waited: AtomicBool,
    later_started: AtomicBool,
    release: Notify,
}

#[async_trait::async_trait]
impl FileBlockCache for ConcurrentPkFiles {
    async fn get(&self, path: &str, _range: Range<u64>) -> Option<Bytes> {
        if path.ends_with(".parquet") {
            if path.ends_with(&self.first_file) {
                self.first_waited.store(true, Ordering::SeqCst);
                let released = self.release.notified();
                if !self.later_started.load(Ordering::SeqCst) {
                    released.await;
                }
            } else {
                self.later_started.store(true, Ordering::SeqCst);
                self.release.notify_waiters();
            }
        }
        None
    }

    async fn put(&self, _path: &str, _offset: u64, _data: Bytes) {}
    async fn invalidate_path(&self, _path: &str) {}
    async fn invalidate_prefix(&self, _prefix: &str) {}
}

async fn write_partitioned_blobs(
    mode: &str,
    partitions: i32,
    payloads: &[&[u8]],
    batch_size: usize,
    output_budget: Option<&str>,
) -> (paimon::io::FileIO, Table) {
    let mut schema = Schema::builder()
        .column("p", DataType::Int(IntType::new()))
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(BlobType::new()))
        .partition_keys(["p"])
        .option("read.batch-size", batch_size.to_string());
    if mode == "pk" {
        schema = schema.primary_key(["p", "id"]).option("bucket", "1");
    } else {
        schema = schema
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option("data-evolution.write-cols-optimization.enabled", "true");
    }
    if let Some(output_budget) = output_budget {
        schema = schema.option("read.blob.output.max-buffered-bytes", output_budget);
    }
    let schema = schema.build().unwrap();
    let (io, table) = memory_table(
        &format!("memory:/parallel_blobs_{mode}"),
        TableSchema::new(0, &schema),
    );
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    for partition in 0..partitions {
        let batch = RecordBatch::try_from_iter([
            (
                "p",
                Arc::new(Int32Array::from(vec![partition; payloads.len()])) as ArrayRef,
            ),
            (
                "id",
                Arc::new(Int32Array::from_iter_values(0..payloads.len() as i32)) as ArrayRef,
            ),
            (
                "payload",
                Arc::new(LargeBinaryArray::from(payloads.to_vec())) as ArrayRef,
            ),
        ])
        .unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
    }
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    (io, table)
}

#[tokio::test]
async fn managed_pk_blob_splits_read_concurrently_by_default() {
    check_parallel_blob_reads("pk", None).await;
}

#[tokio::test]
async fn optimized_de_blob_splits_read_concurrently_with_explicit_budget() {
    check_parallel_blob_reads("de", Some("4mb")).await;
}

async fn check_parallel_blob_reads(mode: &str, output_budget: Option<&str>) {
    let (io, table) =
        write_partitioned_blobs(mode, 2, &[b"hello", b"world!"], 2048, output_budget).await;
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let splits = plan.splits();
    assert_eq!(splits.len(), 2);
    let parquet_names = splits
        .iter()
        .map(|split| {
            let file = split
                .data_files()
                .iter()
                .find(|f| f.file_name.ends_with(".parquet"))
                .unwrap();
            assert!(file.write_cols.is_none());
            file.file_name.clone()
        })
        .collect::<Vec<_>>();
    if mode == "de" {
        assert!(splits.iter().all(|split| split
            .data_files()
            .iter()
            .any(|file| file.file_name.ends_with(".blob") && file.file_size < 1024 * 1024)));
    }
    let mut read_cases = vec![
        (table.schema().clone(), false),
        (table.schema().clone(), true),
    ];
    if mode == "de" {
        let evolved = table
            .schema()
            .apply_changes(vec![SchemaChange::add_column(
                "new_value".into(),
                DataType::Int(IntType::new()),
            )])
            .unwrap();
        assert_ne!(evolved.id(), table.schema().id());
        persist_table_schema(&io, table.location(), &evolved).await;
        read_cases.push((evolved, false));
    }
    for (schema, ordered) in read_cases {
        let gate = Arc::new(ConcurrentPkFiles {
            first_file: parquet_names[0].clone(),
            first_waited: AtomicBool::new(false),
            later_started: AtomicBool::new(false),
            release: Notify::new(),
        });
        let io = io
            .clone()
            .with_file_block_cache(gate.clone(), 64 * 1024, "data")
            .unwrap();
        let read_table = Table::new(
            io,
            table.identifier().clone(),
            table.location().into(),
            schema,
            None,
        );
        let output = read_table
            .new_read_builder()
            .new_read()
            .unwrap()
            .with_parallelism(2)
            .unwrap()
            .with_ordered(ordered)
            .to_arrow(splits)
            .unwrap();
        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            output.try_collect::<Vec<_>>(),
        )
        .await
        .expect("a small BLOB split serialized the other partition")
        .unwrap();
        assert!(gate.first_waited.load(Ordering::SeqCst));
        assert!(gate.later_started.load(Ordering::SeqCst));
        let mut payloads = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("payload")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap().to_vec())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        payloads.sort();
        assert_eq!(
            payloads,
            vec![
                b"hello".to_vec(),
                b"hello".to_vec(),
                b"world!".to_vec(),
                b"world!".to_vec()
            ]
        );
    }
}

#[tokio::test]
async fn aliased_de_blob_projection_respects_output_budget() {
    let payload = vec![42; 1536 * 1024];
    let (io, table) = write_partitioned_blobs("de", 2, &[&payload], 2048, Some("2mb")).await;
    let fields = table.schema().fields();
    let id = fields.iter().find(|field| field.name() == "id").unwrap();
    let blob = fields
        .iter()
        .find(|field| field.name() == "payload")
        .unwrap()
        .clone()
        .with_name("aliased_payload".into());
    let plan = table
        .new_read_builder()
        .with_read_type(vec![id.clone(), blob.clone()])
        .new_scan()
        .plan()
        .await
        .unwrap();
    let splits = plan.splits();
    assert_eq!(splits.len(), 2);
    let parquet_names = splits
        .iter()
        .map(|split| {
            let files = split.data_files();
            assert!(files.iter().any(|file| file.file_name.ends_with(".blob")
                && (1024 * 1024..2 * 1024 * 1024).contains(&file.file_size)));
            &files
                .iter()
                .find(|file| file.file_name.ends_with(".parquet"))
                .unwrap()
                .file_name
        })
        .collect::<Vec<_>>();
    for ordered in [false, true] {
        let gate = Arc::new(DelayedFirstFile::new(parquet_names[0], parquet_names[1]));
        let read_table = Table::new(
            io.clone()
                .with_file_block_cache(gate.clone(), 64 * 1024, "data")
                .unwrap(),
            table.identifier().clone(),
            table.location().into(),
            table.schema().clone(),
            None,
        );
        let output = read_table
            .new_read_builder()
            .with_read_type(vec![id.clone(), blob.clone()])
            .new_read()
            .unwrap()
            .with_parallelism(2)
            .unwrap()
            .with_ordered(ordered)
            .to_arrow(splits)
            .unwrap();
        let result = tokio::spawn(output.try_collect::<Vec<_>>());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !gate.first_waited.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first split did not open");
        let later_started = gate.release_first.notified();
        tokio::pin!(later_started);
        later_started.as_mut().enable();
        assert!(
            !gate.later_data_started.load(Ordering::SeqCst)
                && tokio::time::timeout(std::time::Duration::from_millis(100), later_started)
                    .await
                    .is_err(),
            "second split bypassed the BLOB output budget (ordered={ordered})"
        );
        gate.manually_released.store(true, Ordering::SeqCst);
        gate.release_first.notify_waiters();

        let batches = tokio::time::timeout(std::time::Duration::from_secs(5), result)
            .await
            .expect("splits stalled after releasing the first reader")
            .unwrap()
            .unwrap();
        let values = batches.iter().flat_map(|batch| {
            batch
                .column_by_name("aliased_payload")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .iter()
        });
        assert!(values.eq([Some(payload.as_slice()); 2]));
    }
}

#[tokio::test]
async fn ordered_single_blob_split_runs_without_tokio() {
    let (_, table) = write_partitioned_blobs("de", 1, &[b"one", b"two"], 2048, Some("4mb")).await;
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    assert_eq!(plan.splits().len(), 1);
    let output = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .with_parallelism(8)
        .unwrap()
        .with_ordered(true)
        .to_arrow(plan.splits())
        .unwrap();
    let batches =
        std::thread::spawn(move || futures::executor::block_on(output.try_collect::<Vec<_>>()))
            .join()
            .unwrap()
            .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
}

#[tokio::test]
async fn unordered_de_keeps_reservation_between_source_batch_slices() {
    use futures::StreamExt;

    let large = vec![42; 1536 * 1024];
    let mut payloads = vec![b"x".as_slice(); 1024];
    payloads.push(&large);
    let (_, table) = write_partitioned_blobs("de", 3, &payloads, 2048, Some("2mb")).await;
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    assert_eq!(plan.splits().len(), 3);
    for split in plan.splits() {
        let blob = split
            .data_files()
            .iter()
            .find(|f| f.file_name.ends_with(".blob"))
            .unwrap();
        assert!((1024 * 1024..2 * 1024 * 1024).contains(&blob.file_size));
    }
    let output = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .with_parallelism(3)
        .unwrap()
        .with_ordered(false)
        .to_arrow(plan.splits())
        .unwrap();
    let (partitions, batch_sizes) =
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut output = output;
            let mut partitions = Vec::new();
            let mut batch_sizes = Vec::new();
            while let Some(batch) = output.next().await {
                let batch = batch.unwrap();
                let partition = batch
                    .column_by_name("p")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                partitions.push(partition.value(0));
                batch_sizes.push(batch.num_rows());
            }
            (partitions, batch_sizes)
        })
        .await
        .expect("a reader stalled while holding the BLOB reservation");
    assert_eq!(batch_sizes, vec![1024, 1, 1024, 1, 1024, 1]);
    for [first, second] in partitions.as_chunks::<2>().0 {
        assert_eq!(
            first, second,
            "another split started while the first still held BLOBs"
        );
    }
}
