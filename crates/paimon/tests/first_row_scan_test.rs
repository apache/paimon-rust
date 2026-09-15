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

use arrow_array::{Int32Array, RecordBatch};
use common::incremental_helpers::{
    make_batch, memory_table, persist_table_schema, pk_schema, setup_dirs, write_batch,
};
use futures::TryStreamExt;
use paimon::spec::{Datum, PredicateBuilder};
use paimon::table::{IncrementalScanMode, Plan, ReadBuilder, Table};

async fn table_with_versions(path: &str, compacted: bool) -> Table {
    let (io, table) = memory_table(
        path,
        pk_schema(&[
            ("merge-engine", "first-row"),
            ("source.split.target-size", "1b"),
            ("source.split.open-file-cost", "1b"),
        ]),
    );
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    let mut messages = writer.prepare_commit().await.unwrap();
    if compacted {
        // A unique, sorted run needs only a level upgrade during compaction.
        for message in &mut messages {
            for file in &mut message.new_files {
                file.level = 1;
            }
        }
    }
    builder.new_commit().commit(messages).await.unwrap();
    write_batch(&table, &make_batch(vec![1, 3], vec![99, 30])).await;
    table
}

async fn rows(builder: &ReadBuilder<'_>, plan: &Plan) -> Vec<(i32, i32)> {
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|i| (ids.value(i), values.value(i)))
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn first_row_batch_reads_compacted_rows_and_skips_new_level_zero() {
    let table = table_with_versions("memory:/first_row/compacted", true).await;
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    assert_eq!(plan.snapshot_id(), Some(2));
    assert!(plan.splits().iter().all(
        |split| split.raw_convertible() && split.data_files().iter().all(|file| file.level > 0)
    ));
    assert_eq!(rows(&builder, &plan).await, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn first_row_all_files_keeps_overlapping_versions_in_one_split() {
    for compacted in [false, true] {
        let path = format!("memory:/first_row/all_files/{compacted}");
        let table = table_with_versions(&path, compacted).await;
        let builder = table.new_read_builder();
        let plan = builder
            .new_scan()
            .with_scan_all_files()
            .plan()
            .await
            .unwrap();
        // Both ranges overlap at key 1, even with a one-byte target split size.
        assert_eq!(plan.splits().len(), 1);
        assert!(!plan.splits()[0].raw_convertible());
        assert_eq!(plan.splits()[0].data_files().len(), 2);
        assert_eq!(rows(&builder, &plan).await, vec![(1, 10), (2, 20), (3, 30)]);
    }
}

#[tokio::test]
async fn first_row_value_filter_runs_after_merging_all_versions() {
    let table = table_with_versions("memory:/first_row/value_filter", false).await;
    for (value, expected) in [(10, vec![(1, 10)]), (99, vec![])] {
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("value", Datum::Int(value))
            .unwrap();
        let mut builder = table.new_read_builder();
        builder.with_filter(predicate);
        let plan = builder
            .new_scan()
            .with_scan_all_files()
            .plan()
            .await
            .unwrap();
        assert_eq!(
            plan.splits()
                .iter()
                .map(|s| s.data_files().len())
                .sum::<usize>(),
            2
        );
        assert_eq!(rows(&builder, &plan).await, expected);
    }
}

#[tokio::test]
async fn first_row_incremental_preserves_events_instead_of_merging() {
    let table = table_with_versions("memory:/first_row/incremental", false).await;
    let builder = table.new_read_builder();
    let plan = builder
        .new_incremental_scan(IncrementalScanMode::Delta, 0, 2)
        .plan_combined_delta()
        .await
        .unwrap();
    assert!(plan.splits().iter().all(|split| split.is_streaming()));
    assert_eq!(
        rows(&builder, &plan).await,
        vec![(1, 10), (1, 99), (2, 20), (3, 30)]
    );
}
