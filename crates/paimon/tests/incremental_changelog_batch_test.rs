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

use arrow_array::{Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use paimon::spec::{Datum, PredicateBuilder};
use paimon::table::{IncrementalScanMode, Plan, Table};

use common::incremental_helpers::{
    make_batch, make_batch_with_kinds, memory_table, persist_table_schema, pk_schema, setup_dirs,
    write_batch,
};

async fn table(name: &str, bucket: &str, dv: bool, split_size: &str) -> Table {
    let path = format!("memory:/changelog-batch/{name}");
    let (file_io, table) = memory_table(
        &path,
        pk_schema(&[
            ("changelog-producer", "input"),
            ("bucket", bucket),
            (
                "deletion-vectors.enabled",
                if dv { "true" } else { "false" },
            ),
            ("source.split.target-size", split_size),
            ("source.split.open-file-cost", "1b"),
        ]),
    );
    setup_dirs(&file_io, &path).await;
    persist_table_schema(&file_io, &path, table.schema()).await;
    table
}

async fn events(table: &Table, plan: &Plan) -> Vec<(String, i32, i32)> {
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow_with_row_kind(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let kinds = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| {
            (
                kinds.value(row).to_owned(),
                ids.value(row),
                values.value(row),
            )
        }));
    }
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn combined_changelog_packs_overlapping_versions_across_snapshots() {
    let table = table("overlapping", "1", false, "1mb").await;
    for (value, kind) in [(10, 0), (11, 2), (12, 1), (13, 3)] {
        write_batch(
            &table,
            &make_batch_with_kinds(vec![1], vec![value], vec![kind]),
        )
        .await;
    }
    let builder = table.new_read_builder();
    let per_snapshot = builder
        .new_incremental_scan(IncrementalScanMode::Changelog, 0, 4)
        .plan()
        .await
        .unwrap();
    assert_eq!(per_snapshot.splits().len(), 4);
    for mode in [IncrementalScanMode::Changelog, IncrementalScanMode::Auto] {
        let plan = builder
            .new_incremental_scan(mode, 0, 4)
            .plan_combined()
            .await
            .unwrap();
        assert_eq!(plan.snapshot_id(), Some(4));
        assert_eq!(plan.splits().len(), 1, "batch packing must span snapshots");
        let split = &plan.splits()[0];
        assert_eq!(split.snapshot_id(), 4);
        assert!(split.is_streaming());
        assert_eq!(split.data_files().len(), 4);
        assert!(split
            .data_files()
            .iter()
            .all(|file| file.file_name.starts_with("changelog-")));
        assert_eq!(
            events(&table, &plan).await,
            vec![
                ("+I".into(), 1, 10),
                ("+U".into(), 1, 11),
                ("-D".into(), 1, 13),
                ("-U".into(), 1, 12),
            ]
        );
    }
}

#[tokio::test]
async fn combined_changelog_shards_cover_every_physical_event_once() {
    for dv in [false, true] {
        let table = table(&format!("shards-{dv}"), "4", dv, "1mb").await;
        for version in 0..3 {
            write_batch(&table, &make_batch((0..32).collect(), vec![version; 32])).await;
        }
        let builder = table.new_read_builder();
        let full = builder
            .new_incremental_scan(IncrementalScanMode::Changelog, 0, 3)
            .plan_combined()
            .await
            .unwrap();
        let expected = events(&table, &full).await;
        assert_eq!(expected.len(), 96);
        for mode in [IncrementalScanMode::Changelog, IncrementalScanMode::Auto] {
            for count in [2, 5] {
                let mut union = Vec::new();
                let mut file_names = std::collections::HashSet::new();
                for index in 0..count {
                    let plan = builder
                        .new_incremental_scan(mode, 0, 3)
                        .with_shard(index, count)
                        .unwrap()
                        .plan_combined()
                        .await
                        .unwrap();
                    assert_eq!(plan.snapshot_id(), Some(3));
                    for split in plan.splits() {
                        assert!(split.is_streaming());
                        assert_eq!(split.snapshot_id(), 3);
                        if !dv {
                            assert_eq!(split.bucket() as usize % count, index);
                        }
                        for file in split.data_files() {
                            assert!(
                                file_names.insert(file.file_name.clone()),
                                "a file belongs to exactly one shard"
                            );
                        }
                    }
                    union.extend(events(&table, &plan).await);
                }
                union.sort_unstable();
                assert_eq!(union, expected, "dv={dv}, mode={mode:?}, count={count}");
                assert_eq!(
                    file_names.len(),
                    full.splits()
                        .iter()
                        .map(|split| split.data_files().len())
                        .sum::<usize>()
                );
            }
        }
    }
}

#[tokio::test]
async fn combined_changelog_applies_limit_once_to_the_whole_batch() {
    let table = table("limit", "1", false, "1b").await;
    for id in 0..4 {
        write_batch(&table, &make_batch(vec![id], vec![id * 10])).await;
    }
    for limit in [0, 1, 2, 5] {
        let mut builder = table.new_read_builder();
        builder.with_limit(limit);
        let plan = builder
            .new_incremental_scan(IncrementalScanMode::Changelog, 0, 4)
            .plan_combined()
            .await
            .unwrap();
        assert_eq!(plan.snapshot_id(), Some(4));
        assert_eq!(plan.splits().len(), limit.min(4));
        assert_eq!(events(&table, &plan).await.len(), limit.min(4));
    }
    // Value predicates remain residual filters over physical events; early
    // LIMIT must not discard a later matching event.
    let predicate = PredicateBuilder::new(table.schema().fields())
        .greater_than("value", Datum::Int(15))
        .unwrap();
    let mut builder = table.new_read_builder();
    builder.with_filter(predicate).with_limit(1);
    let plan = builder
        .new_incremental_scan(IncrementalScanMode::Auto, 0, 4)
        .plan_combined()
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
    let value = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    assert!(value > 15);
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
async fn combined_changelog_selects_compact_events_but_skips_overwrite_and_missing_lists() {
    let table = table("commit-selection", "1", false, "1mb").await;
    for id in 1..=4 {
        write_batch(&table, &make_batch(vec![id], vec![id * 10])).await;
    }
    patch_snapshot(&table, 2, "commitKind", "COMPACT".into()).await;
    patch_snapshot(&table, 3, "commitKind", "OVERWRITE".into()).await;
    patch_snapshot(&table, 4, "changelogManifestList", serde_json::Value::Null).await;
    let builder = table.new_read_builder();
    for mode in [IncrementalScanMode::Changelog, IncrementalScanMode::Auto] {
        let plan = builder
            .new_incremental_scan(mode, 0, 4)
            .plan_combined()
            .await
            .unwrap();
        assert_eq!(plan.snapshot_id(), Some(4));
        assert!(plan.splits().iter().all(|split| split.snapshot_id() == 4));
        assert_eq!(
            events(&table, &plan).await,
            vec![("+I".into(), 1, 10), ("+I".into(), 2, 20)]
        );
        let last = builder
            .new_incremental_scan(mode, 2, 4)
            .plan_combined()
            .await
            .unwrap();
        assert_eq!(last.snapshot_id(), Some(4));
        assert!(last.splits().is_empty());
    }
    let per_snapshot = builder
        .new_incremental_scan(IncrementalScanMode::Changelog, 0, 4)
        .plan()
        .await
        .unwrap();
    let ids: Vec<_> = per_snapshot
        .splits()
        .iter()
        .map(|split| match split {
            paimon::table::IncrementalSplit::Data(split) => split.snapshot_id(),
            _ => panic!("changelog must not contain diff pairs"),
        })
        .collect();
    assert_eq!(ids, vec![1, 2]);
}

#[tokio::test]
async fn combined_changelog_preserves_empty_boundaries_with_shards() {
    let table = table("empty-boundaries", "1", false, "1mb").await;
    for id in 1..=3 {
        write_batch(&table, &make_batch(vec![id], vec![id * 10])).await;
    }
    let builder = table.new_read_builder();
    for mode in [IncrementalScanMode::Changelog, IncrementalScanMode::Auto] {
        for id in [0, 1, 3] {
            let plan = builder
                .new_incremental_scan(mode, id, id)
                .with_shard(1, 3)
                .unwrap()
                .plan_combined()
                .await
                .unwrap();
            assert_eq!(plan.snapshot_id(), Some(id));
            assert!(plan.splits().is_empty());
        }
        for (start, end) in [(-1, 0), (4, 4), (3, 2)] {
            assert!(matches!(
                builder
                    .new_incremental_scan(mode, start, end)
                    .with_shard(1, 3)
                    .unwrap()
                    .plan_combined()
                    .await,
                Err(paimon::Error::DataInvalid { .. })
            ));
        }
    }
    // Delta retains its existing requirement for an actual ending snapshot.
    assert!(builder
        .new_incremental_scan(IncrementalScanMode::Delta, 0, 0)
        .plan_combined_delta()
        .await
        .is_err());
}

#[tokio::test]
async fn combined_changelog_sharding_keeps_residual_filters_before_reader_limit() {
    let table = table("sharded-residual", "4", false, "1b").await;
    for value in 0..3 {
        write_batch(&table, &make_batch((0..32).collect(), vec![value; 32])).await;
    }
    let predicate = PredicateBuilder::new(table.schema().fields())
        .greater_than("value", Datum::Int(0))
        .unwrap();
    let mut read = table.new_read_builder();
    read.with_filter(predicate);
    read.with_projection(&["value", "id"]).unwrap();
    let mut all_candidates = 0;
    for index in 0..5 {
        let plan = read
            .new_incremental_scan(IncrementalScanMode::Changelog, 0, 3)
            .with_shard(index, 5)
            .unwrap()
            .plan_combined()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = read
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let candidates = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        all_candidates += candidates;
        let mut limited = read.clone();
        limited.with_limit(1);
        let plan = limited
            .new_incremental_scan(IncrementalScanMode::Changelog, 0, 3)
            .with_shard(index, 5)
            .unwrap()
            .plan_combined()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = limited
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            candidates.min(1)
        );
        for batch in batches {
            assert_eq!(batch.schema().fields()[0].name(), "value");
            assert_eq!(batch.schema().fields()[1].name(), "id");
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert!((0..batch.num_rows()).all(|row| values.value(row) > 0));
        }
    }
    assert_eq!(all_candidates, 64);
}

#[tokio::test]
async fn timestamp_changelog_trace_describes_one_packed_batch() {
    let table = table("trace", "1", false, "1mb").await;
    for id in 1..=3 {
        write_batch(&table, &make_batch(vec![1], vec![id])).await;
        patch_snapshot(&table, i64::from(id), "timeMillis", (id * 100).into()).await;
    }
    let table = table.copy_with_options(std::collections::HashMap::from([
        ("scan.mode".into(), "incremental".into()),
        ("incremental-between-timestamp".into(), "0,300".into()),
    ]));
    let (plan, trace) = table
        .new_read_builder()
        .new_scan()
        .plan_with_trace()
        .await
        .unwrap();
    assert_eq!(plan.snapshot_id(), Some(3));
    assert_eq!(trace.snapshot_id, Some(3));
    assert_eq!(trace.base_manifest_files, 0);
    assert_eq!(trace.delta_manifest_files, 3);
    assert_eq!(trace.manifest_entries_read, 3);
    assert_eq!(trace.final_splits, 1);
    assert_eq!(trace.final_files, 3);
    assert_eq!(
        trace.planned_data_file_bytes,
        plan.splits()
            .iter()
            .flat_map(|split| split.data_files())
            .map(|file| file.file_size as u64)
            .sum::<u64>()
    );
    assert_eq!(events(&table, &plan).await.len(), 3);
}
