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
use paimon::spec::{
    BinaryRow, BlobType, DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema,
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
async fn finite_blob_reads_do_not_open_payloads_in_later_splits() {
    for primary_key in [false, true] {
        let mut schema = Schema::builder()
            .column("p", DataType::Int(IntType::new()))
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Blob(BlobType::new()))
            .partition_keys(["p"])
            .option("read.batch-size", "1");
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
#[derive(Debug, Default)]
struct DelayedFirstFile {
    first_waited: AtomicBool,
    later_data_started: AtomicBool,
    release_first: Notify,
}

#[async_trait::async_trait]
impl FileBlockCache for DelayedFirstFile {
    async fn get(&self, path: &str, range: Range<u64>) -> Option<Bytes> {
        if path.ends_with("data-later.parquet") && range.start == 0 {
            self.later_data_started.store(true, Ordering::SeqCst);
            self.release_first.notify_waiters();
        }
        if path.ends_with("data-first.parquet") {
            self.first_waited.store(true, Ordering::SeqCst);
            let released = self.release_first.notified();
            if !self.later_data_started.load(Ordering::SeqCst) {
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
    let order = Arc::new(DelayedFirstFile::default());
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
}
