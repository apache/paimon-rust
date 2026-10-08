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

use common::incremental_helpers::{
    make_batch, memory_table, persist_table_schema, setup_dirs, write_batch,
};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, Schema, TableSchema};
use paimon::table::{Plan, Table};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

async fn table(name: &str, producer: &str) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("changelog-producer", producer)
        .build()
        .unwrap();
    let (file_io, table) = memory_table(name, TableSchema::new(0, &schema));
    setup_dirs(&file_io, name).await;
    persist_table_schema(&file_io, name, table.schema()).await;
    table
}

async fn values(table: &Table, plan: Plan) -> Vec<(i32, i32)> {
    use arrow_array::Int32Array;
    let batches: Vec<_> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow_with_row_kind(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut values = Vec::new();
    for batch in batches {
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let vals = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        values.extend((0..batch.num_rows()).map(|row| (ids.value(row), vals.value(row))));
    }
    values.sort_unstable();
    values
}

async fn patch_snapshot(table: &Table, id: i64, key: &str, value: serde_json::Value) {
    let path = table.snapshot_manager().snapshot_path(id);
    let bytes = table
        .file_io()
        .new_input(&path)
        .unwrap()
        .read()
        .await
        .unwrap();
    let mut snapshot: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    snapshot[key] = value;
    table
        .file_io()
        .new_output(&path)
        .unwrap()
        .write(serde_json::to_vec(&snapshot).unwrap().into())
        .await
        .unwrap();
}

#[tokio::test]
async fn empty_poll_then_full_initial_and_follow_up() {
    let table = table("memory:/stream-phases", "none").await;
    let mut scan = table.new_stream_scan().unwrap();
    assert!(scan.plan().await.unwrap().is_none());
    assert_eq!(scan.checkpoint(), None);
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    patch_snapshot(&table, 1, "watermark", 42.into()).await;
    let full = scan.plan().await.unwrap().unwrap();
    assert!(!full.splits()[0].is_streaming());
    assert_eq!(values(&table, full).await, vec![(1, 10)]);
    assert_eq!(scan.checkpoint(), Some(2));
    assert_eq!(scan.watermark(), Some(42));
    assert!(scan.plan().await.unwrap().is_none());
    write_batch(&table, &make_batch(vec![1], vec![11])).await;
    let next = scan.plan().await.unwrap().unwrap();
    assert!(next.splits()[0].is_streaming());
    assert_eq!(values(&table, next).await, vec![(1, 11)]);
    assert_eq!(scan.checkpoint(), Some(3));
    scan.restore(Some(1)).unwrap();
    assert_eq!(scan.plan().await.unwrap().unwrap().snapshot_id(), Some(1));
}

#[tokio::test]
async fn stream_scan_owns_table_and_read_configuration() {
    let table = table("memory:/stream-owned", "input").await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    let mut scan = {
        let copy = table.clone();
        let builder = copy.new_read_builder();
        builder.new_stream_scan().unwrap()
    };
    assert_eq!(scan.plan().await.unwrap().unwrap().snapshot_id(), Some(1));
    scan.restore(Some(1)).unwrap();
    assert_eq!(
        values(&table, scan.plan().await.unwrap().unwrap()).await,
        vec![(1, 10)]
    );
}

#[tokio::test]
async fn delta_follow_up_skips_overwrite_and_compact_commits() {
    let table = table("memory:/stream-delta-selection", "none").await;
    for id in 1..=4 {
        write_batch(&table, &make_batch(vec![id], vec![id * 10])).await;
    }
    patch_snapshot(&table, 2, "commitKind", "OVERWRITE".into()).await;
    patch_snapshot(&table, 3, "commitKind", "COMPACT".into()).await;
    let mut scan = table.new_stream_scan().unwrap();
    scan.restore(Some(2)).unwrap();
    let plan = scan.plan().await.unwrap().unwrap();
    assert_eq!(plan.snapshot_id(), Some(4));
    assert_eq!(values(&table, plan).await, vec![(4, 40)]);
    assert_eq!(scan.checkpoint(), Some(5));
}

#[tokio::test]
async fn changelog_follow_up_consumes_overwrite_physical_events() {
    let table = table("memory:/stream-changelog-selection", "input").await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![2], vec![20])).await;
    let first = table.snapshot_manager().get_snapshot(1).await.unwrap();
    patch_snapshot(&table, 2, "commitKind", "OVERWRITE".into()).await;
    patch_snapshot(
        &table,
        2,
        "changelogManifestList",
        first.changelog_manifest_list().unwrap().into(),
    )
    .await;
    let mut scan = table.new_stream_scan().unwrap();
    scan.restore(Some(2)).unwrap();
    let plan = scan.plan().await.unwrap().unwrap();
    assert_eq!(plan.snapshot_id(), Some(2));
    assert_eq!(values(&table, plan).await, vec![(1, 10)]);
}

#[tokio::test]
async fn empty_follow_up_is_skipped_and_initial_empty_plan_is_returned() {
    let table = table("memory:/stream-empty-selection", "none").await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![2], vec![20])).await;
    let mut scan = table.new_stream_scan().unwrap();
    scan.with_bucket_filter(|_| Ok(false));
    let initial = scan.plan().await.unwrap().unwrap();
    assert!(initial.splits().is_empty());
    assert_eq!(initial.snapshot_id(), Some(2));
    scan.restore(Some(1)).unwrap();
    assert!(scan.plan().await.unwrap().is_none());
    assert_eq!(scan.checkpoint(), Some(3));
}

#[tokio::test]
async fn callback_failure_keeps_the_selected_snapshot_retryable() {
    for initial in [false, true] {
        let table = table(&format!("memory:/stream-retry-{initial}"), "none").await;
        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        let fail = Arc::new(AtomicBool::new(true));
        let flag = Arc::clone(&fail);
        let mut scan = table.new_stream_scan().unwrap();
        scan.with_bucket_filter(move |_| {
            if flag.swap(false, Ordering::SeqCst) {
                return Err(paimon::Error::Unsupported {
                    message: "retry this frame".into(),
                });
            }
            Ok(true)
        });
        let checkpoint = if initial { None } else { Some(1) };
        scan.restore(checkpoint).unwrap();
        assert!(scan.plan().await.is_err());
        assert_eq!(scan.checkpoint(), checkpoint);
        assert_eq!(scan.plan().await.unwrap().unwrap().snapshot_id(), Some(1));
    }
}

#[tokio::test]
async fn large_gap_preserves_every_changelog_frame() {
    let table = table("memory:/stream-long-gap", "input").await;
    for value in 1..=24 {
        write_batch(&table, &make_batch(vec![1], vec![value])).await;
    }
    let mut scan = table.new_stream_scan().unwrap();
    scan.restore(Some(1)).unwrap();
    for id in 1..=24 {
        let plan = scan.plan().await.unwrap().unwrap();
        assert_eq!(plan.snapshot_id(), Some(id));
        assert_eq!(values(&table, plan).await, vec![(1, id as i32)]);
    }
    assert!(scan.plan().await.unwrap().is_none());
    assert_eq!(scan.checkpoint(), Some(25));
}

#[tokio::test]
async fn consumer_acknowledgement_and_explicit_restore() {
    let table = table("memory:/stream-consumer", "none").await;
    for id in 1..=2 {
        write_batch(&table, &make_batch(vec![id], vec![id * 10])).await;
    }
    let mut scan = table.new_stream_scan().unwrap();
    scan.with_consumer_id("job")
        .unwrap()
        .restore(Some(1))
        .unwrap();
    scan.plan().await.unwrap().unwrap();
    assert_eq!(table.consumer_manager().get("job").await.unwrap(), None);
    scan.notify_checkpoint_complete(scan.checkpoint())
        .await
        .unwrap();
    assert_eq!(table.consumer_manager().get("job").await.unwrap(), Some(2));
    let mut resumed = table.new_stream_scan().unwrap();
    resumed.with_consumer_id("job").unwrap();
    let plan = resumed.plan().await.unwrap().unwrap();
    assert_eq!(plan.snapshot_id(), Some(2));
    assert!(plan.splits()[0].is_streaming());
    resumed.restore(Some(1)).unwrap();
    assert_eq!(
        resumed.plan().await.unwrap().unwrap().snapshot_id(),
        Some(1)
    );
}

#[tokio::test]
async fn missing_checkpoints_wait_without_skipping_a_hole() {
    let table = table("memory:/stream-missing", "none").await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    let mut scan = table.new_stream_scan().unwrap();
    scan.restore(Some(100)).unwrap();
    for _ in 0..15 {
        assert!(scan.plan().await.unwrap().is_none());
    }
    assert!(scan.plan().await.is_err());
    assert_eq!(scan.checkpoint(), Some(100));
    scan.restore(Some(2)).unwrap();
    for _ in 0..16 {
        assert!(scan.plan().await.unwrap().is_none());
    }
    assert_eq!(scan.checkpoint(), Some(2));
}

#[tokio::test]
async fn query_auth_is_checked_before_empty_polling() {
    let table = table("memory:/stream-query-auth", "none").await;
    let table = table.copy_with_options(HashMap::from([(
        "query-auth.enabled".into(),
        "true".into(),
    )]));
    let mut scan = table.new_stream_scan().unwrap();
    assert!(scan.plan().await.is_err());
    assert_eq!(scan.checkpoint(), None);
}

#[tokio::test]
async fn producer_configuration_uses_typed_case_insensitive_parsing() {
    for producer in ["none", "NONE", "input", "INPUT"] {
        let table = table(&format!("memory:/stream-producer-{producer}"), producer).await;
        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        let mut scan = table.new_stream_scan().unwrap();
        scan.restore(Some(1)).unwrap();
        assert_eq!(
            values(&table, scan.plan().await.unwrap().unwrap()).await,
            vec![(1, 10)]
        );
    }
    let table = table("memory:/stream-invalid-producer", "invalid").await;
    assert!(table.new_stream_scan().is_err());
}

#[tokio::test]
async fn initial_lookup_and_full_compaction_select_java_levels_only_once() {
    use paimon::spec::{Manifest, ManifestEntry, ManifestList};
    let table = table("memory:/stream-initial-levels", "input").await;
    for (id, level) in [0, 1, 5].into_iter().enumerate() {
        write_batch(&table, &make_batch(vec![id as i32], vec![id as i32 * 10])).await;
        let snapshot = table
            .snapshot_manager()
            .get_snapshot(id as i64 + 1)
            .await
            .unwrap();
        let list_path = format!(
            "{}/manifest/{}",
            table.location(),
            snapshot.delta_manifest_list()
        );
        for meta in ManifestList::read(table.file_io(), &list_path)
            .await
            .unwrap()
        {
            let path = format!("{}/manifest/{}", table.location(), meta.file_name());
            let entries = Manifest::read(table.file_io(), &path).await.unwrap();
            let entries: Vec<ManifestEntry> = entries
                .into_iter()
                .map(|entry| {
                    let mut value = serde_json::to_value(entry).unwrap();
                    value["_FILE"]["_LEVEL"] = level.into();
                    serde_json::from_value(value).unwrap()
                })
                .collect();
            Manifest::write(table.file_io(), &path, &entries)
                .await
                .unwrap();
        }
    }
    for (producer, levels, expected) in [
        ("lookup", None, vec![(1, 10), (2, 20)]),
        ("full-compaction", None, vec![(2, 20)]),
        ("full-compaction", Some("2"), vec![(1, 10)]),
    ] {
        let mut options = HashMap::from([("changelog-producer".into(), producer.into())]);
        if let Some(levels) = levels {
            options.insert("num-levels".into(), levels.into());
        }
        let copy = table.copy_with_options(options);
        let mut scan = copy.new_stream_scan().unwrap();
        assert_eq!(
            values(&copy, scan.plan().await.unwrap().unwrap()).await,
            expected
        );
        // The initial level restriction must not hide L0 changelog events.
        scan.restore(Some(1)).unwrap();
        assert_eq!(
            values(&copy, scan.plan().await.unwrap().unwrap()).await,
            vec![(0, 0)]
        );
    }
}
