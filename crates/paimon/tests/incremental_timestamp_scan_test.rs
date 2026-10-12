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

use arrow_array::RecordBatch;
use common::incremental_helpers::{
    make_batch, make_batch_with_kinds, memory_table, persist_table_schema, setup_dirs, write_batch,
};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, Schema, TableSchema};
use paimon::table::{MergeAssignment, MergeSource, Table, WhenMatched, WhenNotMatched};
use std::collections::HashMap;
use std::sync::Arc;

async fn table(name: &str, mode: &str, producer: &str) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("changelog-producer", producer);
    if mode == "pk" {
        schema = schema.primary_key(["id"]).option("bucket", "1");
    } else if mode == "evolution" {
        schema = schema
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true");
    }
    let path = format!("memory:/timestamp/{name}");
    let (io, table) = memory_table(&path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, &path).await;
    persist_table_schema(&io, &path, table.schema()).await;
    table
}

async fn set_time(table: &Table, time: i64) {
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let mut json = serde_json::to_value(&snapshot).unwrap();
    json["timeMillis"] = time.into();
    table
        .file_io()
        .new_output(&table.snapshot_manager().snapshot_path(snapshot.id()))
        .unwrap()
        .write(serde_json::to_vec(&json).unwrap().into())
        .await
        .unwrap();
}

async fn append(table: &Table, time: i64, ids: Vec<i32>, values: Vec<i32>) {
    write_batch(table, &make_batch(ids, values)).await;
    set_time(table, time).await;
}

fn window(table: &Table, start: i64, end: i64) -> Table {
    table.copy_with_options(HashMap::from([
        ("scan.mode".into(), "incremental".into()),
        (
            "incremental-between-timestamp".into(),
            format!("{start},{end}"),
        ),
    ]))
}

async fn read(table: &Table, all_files: bool) -> (Option<i64>, Vec<(i32, i32)>) {
    let builder = table.new_read_builder();
    let scan = builder.new_scan();
    let scan = if all_files {
        scan.with_scan_all_files()
    } else {
        scan
    };
    let plan = scan.plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap();
        rows.extend(
            ids.values()
                .iter()
                .copied()
                .zip(values.values().iter().copied()),
        );
    }
    rows.sort_unstable();
    (plan.snapshot_id(), rows)
}

#[tokio::test]
async fn timestamp_options_select_events_and_preserve_end_snapshot() {
    for mode in ["append", "pk", "evolution"] {
        let table = table(mode, mode, "none").await;
        append(&table, 100, vec![1, 2], vec![10, 20]).await;
        append(&table, 200, vec![1, 3], vec![11, 30]).await;
        append(&table, 200, vec![1, 4], vec![12, 40]).await;
        let builder = table.new_write_builder().with_overwrite();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![9], vec![90]))
            .await
            .unwrap();
        builder
            .new_commit()
            .overwrite(writer.prepare_commit().await.unwrap(), None)
            .await
            .unwrap();
        writer.close().await;
        set_time(&table, 300).await;
        append(&table, 400, vec![5], vec![50]).await;
        for (start, end, snapshot, expected) in [
            (100, 200, Some(3), vec![(1, 11), (1, 12), (3, 30), (4, 40)]),
            (200, 300, Some(4), vec![]),
            (100, 300, Some(4), vec![(1, 11), (1, 12), (3, 30), (4, 40)]),
            (100, 150, Some(1), vec![]),
            (0, 100, Some(1), vec![(1, 10), (2, 20)]),
            (200, 400, Some(5), vec![(5, 50)]),
            (100, 100, None, vec![]),
            (500, 600, None, vec![]),
            (0, 50, None, vec![]),
        ] {
            let selected = window(&table, start, end);
            for all_files in [false, true] {
                let (actual_snapshot, rows) = read(&selected, all_files).await;
                assert_eq!(actual_snapshot, snapshot, "{mode}: ({start},{end}]");
                assert_eq!(rows, expected, "{mode}: ({start},{end}]");
            }
            let (plan, trace) = selected
                .new_read_builder()
                .new_scan()
                .plan_with_trace()
                .await
                .unwrap();
            assert_eq!(trace.snapshot_id, snapshot);
            assert_eq!(trace.final_splits, plan.splits().len());
            assert_eq!(
                trace.planned_data_file_bytes,
                plan.planned_data_file_bytes()
            );
        }
    }
}

#[tokio::test]
async fn timestamp_auto_uses_physical_changelog_and_preserves_retracts() {
    let table = table("changelog", "pk", "input").await;
    append(&table, 100, vec![1], vec![10]).await;
    write_batch(
        &table,
        &make_batch_with_kinds(vec![1, 1, 2], vec![10, 11, 20], vec![1, 2, 0]),
    )
    .await;
    set_time(&table, 200).await;
    append(&table, 300, vec![3], vec![30]).await;
    let selected = window(&table, 100, 300);
    let builder = selected.new_read_builder();
    let (plan, trace) = builder.new_scan().plan_with_trace().await.unwrap();
    assert_eq!(plan.snapshot_id(), Some(3));
    assert!(plan.splits().iter().all(|split| split.snapshot_id() == 3));
    assert!(trace.manifest_entries_read >= 2);
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow_with_row_kind(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut kinds = Vec::new();
    for batch in batches {
        kinds.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap()
                .iter()
                .map(|kind| kind.unwrap().to_owned()),
        );
    }
    kinds.sort();
    assert_eq!(kinds, vec!["+I", "+I", "+U", "-U"]);
    assert_eq!(
        read(&window(&table, 100, 200), false).await,
        (Some(2), vec![(1, 10), (1, 11), (2, 20)])
    );
    assert_eq!(read(&table, false).await.1, vec![(1, 11), (2, 20), (3, 30)]);
}

#[tokio::test]
async fn timestamp_windows_are_validated_before_reading_current_data() {
    let table = table("validation", "append", "none").await;
    append(&table, 100, vec![1], vec![10]).await;
    for options in [
        HashMap::from([("incremental-between-timestamp".into(), "200,100".into())]),
        HashMap::from([("incremental-between-timestamp".into(), "one,200".into())]),
        HashMap::from([("incremental-between-timestamp".into(), "100,200,300".into())]),
        HashMap::from([("scan.mode".into(), "incremental".into())]),
        HashMap::from([
            ("incremental-between-timestamp".into(), "0,200".into()),
            ("scan.snapshot-id".into(), "1".into()),
        ]),
        HashMap::from([
            ("incremental-between-timestamp".into(), "0,200".into()),
            ("scan.mode".into(), "from-timestamp".into()),
        ]),
    ] {
        let selected = table.copy_with_options(options);
        let error = selected
            .new_read_builder()
            .new_scan()
            .plan()
            .await
            .unwrap_err();
        assert!(matches!(error, paimon::Error::DataInvalid { .. }));
    }
    let selected = table
        .copy_with_time_travel(HashMap::from([(
            "incremental-between-timestamp".into(),
            "0,100".into(),
        )]))
        .await
        .unwrap();
    assert_eq!(read(&selected, false).await.1, vec![(1, 10)]);
}

#[tokio::test]
async fn empty_and_equal_windows_do_not_select_latest_data() {
    let table = table("empty", "append", "none").await;
    for (start, end) in [(100, 100), (0, 100), (200, 100)] {
        assert_eq!(
            read(&window(&table, start, end), false).await,
            (None, vec![])
        );
    }
    append(&table, 100, vec![1], vec![10]).await;
    assert_eq!(read(&window(&table, 100, 100), false).await, (None, vec![]));
    let selected = window(&table, 100, 100).copy_with_options(HashMap::from([(
        "query-auth.enabled".into(),
        "true".into(),
    )]));
    assert!(selected.new_read_builder().new_scan().plan().await.is_err());
}

#[tokio::test]
async fn core_merge_reads_only_the_timestamp_window_and_keeps_source_independent() {
    let target = table("merge_target", "evolution", "none").await;
    let source = table("merge_source", "append", "none").await;
    append(&target, 100, vec![1], vec![10]).await;
    append(&source, 100, vec![1], vec![-1]).await;
    append(&source, 200, vec![1, 2], vec![11, 20]).await;
    append(&source, 300, vec![3], vec![30]).await;
    let builder = target.new_write_builder();
    let messages = builder
        .new_update()
        .unwrap()
        .merge_into(
            MergeSource::Table(Arc::new(window(&source, 100, 200))),
            vec![("id".into(), "id".into())],
            vec![WhenMatched {
                condition: None,
                delete: false,
                assignments: vec![(
                    "value".into(),
                    MergeAssignment::SourceColumn("value".into()),
                )],
            }],
            vec![WhenNotMatched {
                condition: None,
                assignments: vec![
                    ("id".into(), MergeAssignment::SourceColumn("id".into())),
                    (
                        "value".into(),
                        MergeAssignment::SourceColumn("value".into()),
                    ),
                ],
            }],
        )
        .await
        .unwrap();
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(read(&target, false).await.1, vec![(1, 11), (2, 20)]);
    assert_eq!(
        read(&source, false).await.1,
        vec![(1, -1), (1, 11), (2, 20), (3, 30)]
    );
}

#[tokio::test]
async fn expired_prefix_is_excluded_from_timestamp_history() {
    let table = table("expired_prefix", "append", "none").await;
    for (time, id) in [(100, 1), (200, 2), (300, 3)] {
        append(&table, time, vec![id], vec![id * 10]).await;
    }
    table
        .file_io()
        .delete_file(&table.snapshot_manager().snapshot_path(1))
        .await
        .unwrap();
    assert_eq!(
        read(&window(&table, 0, 250), false).await,
        (Some(2), vec![(2, 20)])
    );
}

#[tokio::test]
async fn invalid_source_window_fails_before_target_file_staging() {
    let target = table("failed_merge_target", "evolution", "none").await;
    let source = table("failed_merge_source", "append", "none").await;
    append(&target, 100, vec![1], vec![10]).await;
    append(&source, 100, vec![1], vec![11]).await;
    let builder = target.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let bucket_path = plan.splits()[0].bucket_path();
    let before_files = target
        .file_io()
        .list_status(bucket_path)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.path)
        .collect::<std::collections::HashSet<_>>();
    let error = target
        .new_write_builder()
        .new_update()
        .unwrap()
        .merge_into(
            MergeSource::Table(Arc::new(window(&source, 200, 100))),
            vec![("id".into(), "id".into())],
            vec![WhenMatched {
                condition: None,
                delete: false,
                assignments: vec![(
                    "value".into(),
                    MergeAssignment::SourceColumn("value".into()),
                )],
            }],
            Vec::new(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, paimon::Error::DataInvalid { message, .. } if message.contains("Ending timestamp"))
    );
    assert_eq!(
        target
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(1)
    );
    assert_eq!(
        target
            .file_io()
            .list_status(bucket_path)
            .await
            .unwrap()
            .into_iter()
            .map(|s| s.path)
            .collect::<std::collections::HashSet<_>>(),
        before_files
    );
}
