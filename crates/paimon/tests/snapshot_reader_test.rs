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
    make_batch, make_batch_with_kinds, memory_table, persist_table_schema, setup_dirs, write_batch,
};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, Schema, TableSchema};
use paimon::table::{IncrementalScanMode, Plan, ScanMode, Table};
use std::collections::HashMap;

async fn table(name: &str, engine: &str, producer: &str, buckets: i32) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("changelog-producer", producer)
        .option("source.split.target-size", "1 b")
        .option("source.split.open-file-cost", "1 b");
    match engine {
        "append" => {
            schema = schema.option("bucket", "-1");
        }
        "evolution" => {
            schema = schema
                .option("bucket", "-1")
                .option("row-tracking.enabled", "true")
                .option("data-evolution.enabled", "true");
        }
        _ => {
            schema = schema
                .primary_key(["id"])
                .option("bucket", buckets.to_string())
                .option("merge-engine", engine);
        }
    }
    if engine == "aggregation" {
        schema = schema.option("fields.value.aggregate-function", "sum");
    }
    let path = format!("memory:/snapshot_reader/{name}");
    let (io, table) = memory_table(&path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, &path).await;
    persist_table_schema(&io, &path, table.schema()).await;
    table
}

async fn rows(table: &Table, plan: &Plan) -> Vec<(i32, i32)> {
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
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
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| (ids.value(row), values.value(row))));
    }
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn all_pins_snapshot_and_keeps_batch_merge_semantics_including_level_zero() {
    for engine in [
        "deduplicate",
        "first-row",
        "partial-update",
        "aggregation",
        "append",
        "evolution",
    ] {
        let table = table(engine, engine, "none", 1).await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        write_batch(&table, &make_batch(vec![1, 3], vec![11, 30])).await;
        write_batch(&table, &make_batch(vec![4], vec![40])).await;
        let plan = table
            .new_snapshot_reader()
            .with_snapshot(2)
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(plan.snapshot_id(), Some(2));
        assert!(!plan.splits().is_empty());
        assert!(plan.splits().iter().all(|split| !split.is_streaming()));
        let expected = match engine {
            "append" | "evolution" => vec![(1, 10), (1, 11), (2, 20), (3, 30)],
            "first-row" => vec![(1, 10), (2, 20), (3, 30)],
            "aggregation" => vec![(1, 21), (2, 20), (3, 30)],
            _ => vec![(1, 11), (2, 20), (3, 30)],
        };
        assert_eq!(rows(&table, &plan).await, expected, "{engine}");
        let current = table.new_snapshot_reader().read().await.unwrap();
        assert_eq!(current.snapshot_id(), Some(3));
        assert!(rows(&table, &current).await.contains(&(4, 40)));
    }
}

#[tokio::test]
async fn explicit_snapshot_overrides_startup_options_without_mutating_table() {
    let table = table("startup", "append", "none", 1).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![2], vec![20])).await;
    let selected = table.copy_with_options(HashMap::from([
        ("scan.snapshot-id".into(), "1".into()),
        ("scan.mode".into(), "from-snapshot".into()),
    ]));
    let pinned = selected
        .new_snapshot_reader()
        .with_snapshot(2)
        .unwrap()
        .read()
        .await
        .unwrap();
    assert_eq!(rows(&selected, &pinned).await, vec![(1, 10), (2, 20)]);
    let batch = selected.new_read_builder().new_scan().plan().await.unwrap();
    assert_eq!(batch.snapshot_id(), Some(1));
    assert_eq!(rows(&selected, &batch).await, vec![(1, 10)]);
}

#[tokio::test]
async fn changelog_reads_overwrite_while_batch_range_still_skips_it() {
    let table = table("overwrite", "deduplicate", "input", 1).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![2], vec![20]))
        .await
        .unwrap();
    builder
        .new_commit()
        .overwrite(writer.prepare_commit().await.unwrap(), None)
        .await
        .unwrap();
    writer.close().await;
    // The local overwrite writer does not publish a changelog. Reuse a real
    // physical changelog manifest to model a producer which supplies one;
    // its rows deliberately differ from the overwrite's delta data.
    let first = table.snapshot_manager().get_snapshot(1).await.unwrap();
    let overwrite = table.snapshot_manager().get_snapshot(2).await.unwrap();
    let mut json = serde_json::to_value(overwrite).unwrap();
    json["changelogManifestList"] = first.changelog_manifest_list().unwrap().into();
    table
        .file_io()
        .new_output(&table.snapshot_manager().snapshot_path(2))
        .unwrap()
        .write(serde_json::to_vec(&json).unwrap().into())
        .await
        .unwrap();
    write_batch(&table, &make_batch(vec![3], vec![30])).await;
    let reader = table.new_snapshot_reader().with_snapshot(2).unwrap();
    let events = reader
        .clone()
        .with_mode(ScanMode::Changelog)
        .read()
        .await
        .unwrap();
    assert_eq!(events.snapshot_id(), Some(2));
    assert_eq!(rows(&table, &events).await, vec![(1, 10)]);
    assert!(events.splits().iter().all(|split| split.is_streaming()));
    assert!(events
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .all(|file| file.file_name.starts_with("changelog-")));
    let range = table
        .new_read_builder()
        .new_incremental_scan(IncrementalScanMode::Changelog, 1, 2)
        .plan_combined()
        .await
        .unwrap();
    assert!(range.splits().is_empty());
    assert_eq!(range.snapshot_id(), Some(2));
    let full = reader.with_mode(ScanMode::All).read().await.unwrap();
    assert_eq!(rows(&table, &full).await, vec![(2, 20)]);
}

#[tokio::test]
async fn changelog_keeps_all_row_kinds_and_arrival_order() {
    let table = table("events", "deduplicate", "input", 1).await;
    write_batch(
        &table,
        &make_batch_with_kinds(vec![1, 1, 1, 1], vec![10, 10, 11, 11], vec![0, 1, 2, 3]),
    )
    .await;
    let plan = table
        .new_snapshot_reader()
        .with_snapshot(1)
        .unwrap()
        .with_mode(ScanMode::Changelog)
        .read()
        .await
        .unwrap();
    let builder = table.new_read_builder();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow_with_row_kind(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let kinds: Vec<&str> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap()
                .iter()
                .map(Option::unwrap)
        })
        .collect();
    assert_eq!(kinds, vec!["+I", "-U", "+U", "-D"]);
}

#[tokio::test]
async fn bucket_filter_precedes_packing_and_limit_for_every_mode() {
    let table = table("buckets", "deduplicate", "input", 4).await;
    write_batch(&table, &make_batch((0..32).collect(), (100..132).collect())).await;
    for mode in [ScanMode::All, ScanMode::Delta, ScanMode::Changelog] {
        let all = table
            .new_snapshot_reader()
            .with_mode(mode)
            .read()
            .await
            .unwrap();
        let selected_bucket = all.splits().last().unwrap().bucket();
        let expected_ids: std::collections::HashSet<i32> = rows(
            &table,
            &Plan::new(
                all.splits()
                    .iter()
                    .filter(|split| split.bucket() == selected_bucket)
                    .cloned()
                    .collect(),
            ),
        )
        .await
        .into_iter()
        .map(|(id, _)| id)
        .collect();
        assert!(!expected_ids.is_empty());
        let mut builder = table.new_read_builder();
        builder.with_limit(1);
        let selected = builder
            .new_snapshot_reader()
            .with_mode(mode)
            .with_bucket_filter(move |bucket| Ok(bucket == selected_bucket))
            .read()
            .await
            .unwrap();
        assert_eq!(selected.snapshot_id(), Some(1));
        assert!(!selected.splits().is_empty());
        assert!(selected
            .splits()
            .iter()
            .all(|split| split.bucket() == selected_bucket));
        assert!(rows(&table, &selected)
            .await
            .iter()
            .all(|(id, _)| expected_ids.contains(id)));
        let empty = table
            .new_snapshot_reader()
            .with_mode(mode)
            .with_bucket_filter(|_| Ok(false))
            .read()
            .await
            .unwrap();
        assert_eq!(empty.snapshot_id(), Some(1));
        assert!(empty.splits().is_empty());
    }
}

#[tokio::test]
async fn filter_errors_fail_instead_of_publishing_a_partial_plan() {
    let table = table("callback", "deduplicate", "input", 4).await;
    write_batch(&table, &make_batch((0..32).collect(), (100..132).collect())).await;
    for mode in [ScanMode::All, ScanMode::Delta, ScanMode::Changelog] {
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = calls.clone();
        let error = table
            .new_snapshot_reader()
            .with_mode(mode)
            .with_bucket_filter(move |_| {
                if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    return Ok(true);
                }
                Err(paimon::Error::DataInvalid {
                    message: "bucket selection failed".into(),
                    source: None,
                })
            })
            .read()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("bucket selection failed"));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn replacing_bucket_filter_and_composing_shard_preserves_versions() {
    let table = table("shard", "deduplicate", "input", 4).await;
    write_batch(&table, &make_batch((0..32).collect(), (100..132).collect())).await;
    write_batch(&table, &make_batch((0..32).collect(), (200..232).collect())).await;
    let mut union = Vec::new();
    for index in 0..2 {
        let plan = table
            .new_snapshot_reader()
            .with_bucket_filter(|_| Ok(false))
            .with_bucket_filter(|bucket| Ok(bucket >= 0))
            .with_shard(index, 2)
            .unwrap()
            .with_mode(ScanMode::All)
            .read()
            .await
            .unwrap();
        assert!(plan
            .splits()
            .iter()
            .all(|split| split.bucket() as usize % 2 == index));
        union.extend(rows(&table, &plan).await);
    }
    union.sort_unstable();
    assert_eq!(union, (0..32).map(|id| (id, 200 + id)).collect::<Vec<_>>());
}

#[tokio::test]
async fn read_configuration_and_empty_snapshot_metadata_survive_modes() {
    for engine in ["append", "evolution", "deduplicate"] {
        let table = table(&format!("projection_{engine}"), engine, "none", 1).await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        let mut builder = table.new_read_builder();
        builder.with_projection(&["value"]).unwrap();
        builder.with_filter(
            paimon::spec::PredicateBuilder::new(table.schema().fields())
                .greater_than("id", paimon::spec::Datum::Int(1))
                .unwrap(),
        );
        for mode in [ScanMode::All, ScanMode::Delta] {
            let plan = builder
                .new_snapshot_reader()
                .with_mode(mode)
                .read()
                .await
                .unwrap();
            let batches: Vec<RecordBatch> = builder
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            assert_eq!(batches[0].schema().field(0).name(), "value");
            assert_eq!(
                batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                20
            );
        }
        let empty = builder
            .new_snapshot_reader()
            .with_mode(ScanMode::Changelog)
            .read()
            .await
            .unwrap();
        assert!(empty.splits().is_empty());
        assert_eq!(empty.snapshot_id(), Some(1));
    }
}

#[tokio::test]
async fn absent_and_missing_snapshots_are_distinct_even_with_zero_limit() {
    let table = table("missing", "append", "none", 1).await;
    let mut builder = table.new_read_builder();
    builder.with_limit(0);
    for mode in [ScanMode::All, ScanMode::Delta, ScanMode::Changelog] {
        let reader = builder.new_snapshot_reader().with_mode(mode);
        let empty = reader.read().await.unwrap();
        assert!(empty.splits().is_empty());
        assert_eq!(empty.snapshot_id(), None);
        assert!(matches!(
            reader.with_snapshot(1).unwrap().read().await,
            Err(paimon::Error::SnapshotNotExist { snapshot_id: 1 })
        ));
    }
    assert!(builder.new_snapshot_reader().with_snapshot(0).is_err());
    assert!(builder.new_snapshot_reader().with_snapshot(-1).is_err());
    assert!(builder.new_snapshot_reader().with_shard(0, 0).is_err());
    assert!(builder.new_snapshot_reader().with_shard(2, 2).is_err());
}

#[tokio::test]
async fn all_dv_level_zero_versions_merge_before_value_predicates() {
    for mor in ["true", "false"] {
        let table = table(&format!("dv_{mor}"), "deduplicate", "none", 1)
            .await
            .copy_with_options(HashMap::from([
                ("deletion-vectors.enabled".into(), "true".into()),
                ("deletion-vectors.merge-on-read".into(), mor.into()),
            ]));
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        write_batch(&table, &make_batch(vec![1, 3], vec![11, 30])).await;
        let plan = table.new_snapshot_reader().read().await.unwrap();
        assert_eq!(rows(&table, &plan).await, vec![(1, 11), (2, 20), (3, 30)]);
        let mut builder = table.new_read_builder();
        builder.with_filter(
            paimon::spec::PredicateBuilder::new(table.schema().fields())
                .equal("value", paimon::spec::Datum::Int(10))
                .unwrap(),
        );
        let plan = builder.new_snapshot_reader().read().await.unwrap();
        let batches: Vec<RecordBatch> = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }
}

#[tokio::test]
async fn snapshot_reader_never_bypasses_query_authorization() {
    let table = table("auth", "append", "none", 1)
        .await
        .copy_with_options(HashMap::from([(
            "query-auth.enabled".into(),
            "true".into(),
        )]));
    for mode in [ScanMode::All, ScanMode::Delta, ScanMode::Changelog] {
        for reader in [
            table.new_snapshot_reader().with_mode(mode),
            table
                .new_snapshot_reader()
                .with_mode(mode)
                .with_snapshot(1)
                .unwrap(),
        ] {
            let error = reader
                .with_bucket_filter(|_| Ok(false))
                .read()
                .await
                .unwrap_err();
            assert!(error.to_string().contains("query-auth.enabled"));
            assert!(matches!(error, paimon::Error::Unsupported { .. }));
        }
    }
}

#[tokio::test]
async fn postpone_pending_visibility_is_an_explicit_reader_selection() {
    for producer in ["none", "input"] {
        let table = table(&format!("pending_{producer}"), "deduplicate", producer, -2).await;
        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        for mode in [ScanMode::All, ScanMode::Delta, ScanMode::Changelog] {
            let plan = table
                .new_snapshot_reader()
                .with_mode(mode)
                .read()
                .await
                .unwrap();
            let expected = if mode == ScanMode::Changelog {
                vec![]
            } else {
                vec![(1, 10)]
            };
            assert_eq!(rows(&table, &plan).await, expected, "{producer} {mode:?}");
            assert!(plan.splits().iter().all(|split| split.bucket() == -2));
            let plan = table
                .new_snapshot_reader()
                .with_mode(mode)
                .with_bucket_filter(|_| panic!("unassigned bucket reached callback"))
                .only_read_real_buckets()
                .read()
                .await
                .unwrap();
            assert!(plan.splits().is_empty());
            assert_eq!(plan.snapshot_id(), Some(1));
        }
    }
}
