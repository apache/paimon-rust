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

use arrow_array::{Int32Array, Int64Array, RecordBatch};
use futures::TryStreamExt;
use paimon::spec::{DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema};
use paimon::table::{
    BranchManager, CommitMessage, IncrementalScanMode, Plan, ReadBuilder, RowRange, Table,
    TagManager,
};
use std::collections::HashMap;
use std::sync::Arc;

use common::incremental_helpers::{
    make_batch, memory_table, persist_table_schema, setup_dirs, write_batch,
};

async fn evolution_table(path: &str) -> Table {
    evolution_table_with_options(path, &[]).await
}

async fn evolution_table_with_options(path: &str, options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("source.split.target-size", "1b");
    for &(key, value) in options {
        schema = schema.option(key, value);
    }
    let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

async fn append_ids(table: &Table, start: i32, end: i32) {
    write_batch(
        table,
        &make_batch(
            (start..end).collect(),
            (start..end).map(|id| id * 10).collect(),
        ),
    )
    .await;
}

async fn read_ids(table: &Table, plan: &Plan) -> Vec<i32> {
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    batches
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
        .collect()
}

// Python's native_plan_distribution_test used one six-row file for this
// intersection. Two files expose both manifest- and entry-level early pruning.
async fn assert_positions_precede_range_pruning(combined: bool, shard: bool) {
    let table = evolution_table("memory:/planning_parity/range_pruning").await;
    append_ids(&table, 0, 3).await;
    append_ids(&table, 3, 6).await;
    let mut builder = table.new_read_builder();
    builder.with_row_ranges(vec![RowRange::new(4, 4)]);
    let plan = if combined {
        let scan = builder.new_incremental_scan(IncrementalScanMode::Delta, 0, 2);
        let scan = if shard {
            scan.with_row_position_shard(1, 2)
        } else {
            scan.with_row_position_slice(3, 6)
        }
        .unwrap();
        scan.plan_combined_delta().await.unwrap()
    } else {
        let scan = builder.new_scan();
        let scan = if shard {
            scan.with_row_position_shard(1, 2)
        } else {
            scan.with_row_position_slice(3, 6)
        }
        .unwrap();
        scan.plan().await.unwrap()
    };
    assert_eq!(plan.snapshot_id(), Some(2));
    let ranges: Vec<_> = plan
        .splits()
        .iter()
        .flat_map(|split| split.row_ranges().unwrap())
        .cloned()
        .collect();
    assert_eq!(ranges, vec![RowRange::new(4, 4)]);
    assert_eq!(read_ids(&table, &plan).await, vec![4]);
}

#[tokio::test]
async fn snapshot_slice_assigns_positions_before_row_range_pruning() {
    assert_positions_precede_range_pruning(false, false).await;
}

#[tokio::test]
async fn snapshot_shard_assigns_positions_before_row_range_pruning() {
    assert_positions_precede_range_pruning(false, true).await;
}

#[tokio::test]
async fn combined_slice_assigns_positions_before_row_range_pruning() {
    assert_positions_precede_range_pruning(true, false).await;
}

#[tokio::test]
async fn combined_shard_assigns_positions_before_row_range_pruning() {
    assert_positions_precede_range_pruning(true, true).await;
}

async fn read_column(builder: &ReadBuilder<'_>, plan: &Plan, column: usize) -> Vec<i32> {
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

async fn delete_ids(table: &Table, ids: &[i64]) {
    let builder = table.new_write_builder();
    let mut delete = builder.new_delete().unwrap();
    delete.add_row_ids(ids.iter().copied()).unwrap();
    builder
        .new_commit()
        .commit(delete.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn global_index_ranges_intersect_positions_after_candidate_assignment() {
    for index_type in ["btree", "bitmap"] {
        let table = evolution_table_with_options(
            "memory:/planning_parity/index_positions",
            &[("global-index.enabled", "true")],
        )
        .await;
        append_ids(&table, 0, 3).await;
        append_ids(&table, 3, 6).await;
        assert!(
            table
                .new_btree_global_index_build_builder()
                .with_index_column("id")
                .with_index_type(index_type)
                .execute()
                .await
                .unwrap()
                > 0
        );
        let end = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap()
            .id();
        for search_mode in ["fast", "full", "detail"] {
            let table = table.copy_with_options(HashMap::from([(
                "global-index.search-mode".into(),
                search_mode.into(),
            )]));
            let mut builder = table.new_read_builder();
            builder.with_filter(
                PredicateBuilder::new(table.schema().fields())
                    .equal("id", Datum::Int(4))
                    .unwrap(),
            );
            for combined in [false, true] {
                for shard in [false, true] {
                    let plan = if combined {
                        let scan = builder.new_incremental_scan(IncrementalScanMode::Delta, 0, end);
                        let scan = if shard {
                            scan.with_row_position_shard(1, 2)
                        } else {
                            scan.with_row_position_slice(3, 6)
                        }
                        .unwrap();
                        scan.plan_combined_delta().await.unwrap()
                    } else {
                        let scan = builder.new_scan();
                        let scan = if shard {
                            scan.with_row_position_shard(1, 2)
                        } else {
                            scan.with_row_position_slice(3, 6)
                        }
                        .unwrap();
                        scan.plan().await.unwrap()
                    };
                    assert_eq!(plan.snapshot_id(), Some(end));
                    assert_eq!(read_column(&builder, &plan, 0).await, vec![4],
                        "index={index_type}, mode={search_mode}, combined={combined}, shard={shard}");
                }
            }
            let empty = builder
                .new_scan()
                .with_row_position_slice(0, 3)
                .unwrap()
                .plan()
                .await
                .unwrap();
            assert!(
                empty.splits().is_empty(),
                "index pruning must not shift row 4 into the first slice"
            );
        }
    }
}

#[tokio::test]
async fn shards_cover_candidates_once_and_slices_clip_across_row_id_gaps() {
    let table = evolution_table("memory:/planning_parity/gaps").await;
    for start in [0, 3, 6] {
        append_ids(&table, start, start + 3).await;
    }
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let middle = plan
        .splits()
        .iter()
        .find(|split| split.data_files()[0].first_row_id == Some(3))
        .unwrap();
    let mut removal = CommitMessage::new(
        middle.partition().to_serialized_bytes(),
        middle.bucket(),
        vec![],
    );
    removal.deleted_files = middle.data_files().to_vec();
    table
        .new_write_builder()
        .new_commit()
        .commit(vec![removal])
        .await
        .unwrap();
    let expected = vec![0, 1, 2, 6, 7, 8];
    for count in [1, 2, 4, 9] {
        let mut collected = vec![];
        for index in 0..count {
            let plan = table
                .new_read_builder()
                .new_scan()
                .with_row_position_shard(index, count)
                .unwrap()
                .plan()
                .await
                .unwrap();
            let rows = read_ids(&table, &plan).await;
            let start = index * (6 / count) + index.min(6 % count);
            let end = start + 6 / count + u64::from(index < 6 % count);
            assert_eq!(rows, expected[start as usize..end as usize]);
            assert_eq!(plan.snapshot_id(), Some(4));
            collected.extend(rows);
        }
        assert_eq!(collected, expected);
    }
    for (start, end) in [(0, 1), (2, 5), (4, 100), (10, 12)] {
        let plan = table
            .new_read_builder()
            .new_scan()
            .with_row_position_slice(start, end)
            .unwrap()
            .plan()
            .await
            .unwrap();
        assert_eq!(
            read_ids(&table, &plan).await,
            expected[(start as usize).min(6)..(end as usize).min(6)]
        );
    }
}

#[tokio::test]
async fn position_selection_precedes_deletions_and_preserves_historical_reads() {
    let table = evolution_table_with_options(
        "memory:/planning_parity/dv_shards",
        &[("deletion-vectors.enabled", "true")],
    )
    .await;
    append_ids(&table, 0, 4).await;
    append_ids(&table, 4, 8).await;
    delete_ids(&table, &[1, 2, 7]).await;
    for (index, expected) in [vec![0], vec![3, 4, 5], vec![6]].into_iter().enumerate() {
        let plan = table
            .new_read_builder()
            .new_scan()
            .with_row_position_shard(index as u64, 3)
            .unwrap()
            .plan()
            .await
            .unwrap();
        assert_eq!(plan.snapshot_id(), Some(3));
        assert_eq!(read_ids(&table, &plan).await, expected);
    }
    let historical =
        table.copy_with_options(HashMap::from([("scan.snapshot-id".into(), "2".into())]));
    let plan = historical
        .new_read_builder()
        .new_scan()
        .with_row_position_shard(0, 3)
        .unwrap()
        .plan()
        .await
        .unwrap();
    assert_eq!(plan.snapshot_id(), Some(2));
    assert_eq!(read_ids(&historical, &plan).await, vec![0, 1, 2]);
    // A planning limit is a hint. It cannot drop later groups when earlier
    // selected positions are deleted; the consumer takes its final limit.
    let mut builder = table.new_read_builder();
    builder.with_limit(2);
    let plan = builder
        .new_scan()
        .with_row_position_slice(1, 7)
        .unwrap()
        .plan()
        .await
        .unwrap();
    let rows = read_column(&builder, &plan, 0).await;
    assert_eq!(&rows[..2], &[3, 4]);
}

#[tokio::test]
async fn combined_delta_uses_window_end_deletion_vectors_after_repeated_deletes() {
    for bucket_local in ["false", "true"] {
        let table = evolution_table_with_options(
            "memory:/planning_parity/delta_dv",
            &[
                ("deletion-vectors.enabled", "true"),
                ("index-file-in-data-file-dir", bucket_local),
            ],
        )
        .await;
        append_ids(&table, 0, 3).await;
        append_ids(&table, 3, 6).await;
        delete_ids(&table, &[1, 4]).await;
        delete_ids(&table, &[5]).await;
        for (end, expected) in [(2, vec![3, 4, 5]), (3, vec![3, 5]), (4, vec![3])] {
            let plan = table
                .new_read_builder()
                .new_incremental_scan(IncrementalScanMode::Delta, 1, end)
                .plan_combined_delta()
                .await
                .unwrap();
            assert_eq!(plan.snapshot_id(), Some(end));
            assert!(plan.splits().iter().all(|s| s.snapshot_id() == end));
            assert_eq!(read_ids(&table, &plan).await, expected);
        }
        let mut builder = table.new_read_builder();
        builder.with_limit(1);
        let plan = builder
            .new_incremental_scan(IncrementalScanMode::Delta, 1, 3)
            .with_row_position_slice(1, 3)
            .unwrap()
            .plan_combined_delta()
            .await
            .unwrap();
        assert_eq!(read_column(&builder, &plan, 0).await, vec![5]);
    }
}

#[tokio::test]
async fn projection_and_column_updates_do_not_multiply_positions_or_reorder_groups() {
    let table = evolution_table("memory:/planning_parity/projected_update").await;
    append_ids(&table, 0, 4).await;
    append_ids(&table, 4, 8).await;
    let write = table.new_write_builder();
    let mut update = write.new_update(vec!["value".into()]).unwrap();
    update
        .add_matched_batch(
            RecordBatch::try_from_iter(vec![
                (
                    "_ROW_ID",
                    Arc::new(Int64Array::from(vec![5])) as arrow_array::ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int32Array::from(vec![999])) as arrow_array::ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .unwrap();
    write
        .new_commit()
        .commit(update.prepare_commit().await.unwrap())
        .await
        .unwrap();
    let mut builder = table.new_read_builder();
    builder.with_projection(&["value"]).unwrap().with_limit(1);
    let plan = builder
        .new_scan()
        .with_row_position_slice(2, 6)
        .unwrap()
        .plan()
        .await
        .unwrap();
    assert_eq!(read_column(&builder, &plan, 0).await, vec![20, 30, 40, 999]);
    for (index, expected) in [vec![0, 10, 20, 30], vec![40, 999, 60, 70]]
        .into_iter()
        .enumerate()
    {
        let plan = builder
            .new_scan()
            .with_row_position_shard(index as u64, 2)
            .unwrap()
            .plan()
            .await
            .unwrap();
        assert_eq!(read_column(&builder, &plan, 0).await, expected);
    }
}

#[tokio::test]
async fn branch_snapshot_and_combined_delta_use_independent_snapshot_histories() {
    let table = evolution_table("memory:/planning_parity/branch").await;
    append_ids(&table, 0, 1).await;
    let snapshot = table.snapshot_manager().get_snapshot(1).await.unwrap();
    TagManager::new(table.file_io().clone(), table.location().into())
        .create("base", &snapshot)
        .await
        .unwrap();
    BranchManager::new(table.file_io().clone(), table.location().into())
        .create_branch_from_tag("audit", "base")
        .await
        .unwrap();
    let branch = table.copy_with_branch("audit").await.unwrap();
    append_ids(&table, 10, 11).await;
    let overwrite = table.new_write_builder().with_overwrite();
    let mut writer = overwrite.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![20], vec![200]))
        .await
        .unwrap();
    overwrite
        .new_commit()
        .overwrite(writer.prepare_commit().await.unwrap(), None)
        .await
        .unwrap();
    // Rust intentionally refuses branch writes. Use real data/manifests from
    // another commit to model an independent APPEND at branch snapshot 2.
    let mut metadata =
        serde_json::to_value(table.snapshot_manager().get_snapshot(3).await.unwrap()).unwrap();
    metadata["id"] = serde_json::json!(2);
    metadata["commitKind"] = serde_json::json!("APPEND");
    table
        .file_io()
        .new_output(&branch.snapshot_manager().snapshot_path(2))
        .unwrap()
        .write(bytes::Bytes::from(serde_json::to_vec(&metadata).unwrap()))
        .await
        .unwrap();
    for (table, expected) in [(&table, 10), (&branch, 20)] {
        let plan = table
            .new_read_builder()
            .new_incremental_scan(IncrementalScanMode::Delta, 1, 2)
            .plan_combined_delta()
            .await
            .unwrap();
        assert_eq!(plan.snapshot_id(), Some(2));
        assert_eq!(read_ids(table, &plan).await, vec![expected]);
    }
    let plan = branch.new_read_builder().new_scan().plan().await.unwrap();
    assert_eq!(plan.snapshot_id(), Some(2));
    assert_eq!(read_ids(&branch, &plan).await, vec![20]);
    assert!(branch
        .new_read_builder()
        .new_incremental_scan(IncrementalScanMode::Delta, 1, 3)
        .plan_combined_delta()
        .await
        .is_err());
}

#[tokio::test]
async fn combined_delta_value_filter_does_not_resurrect_old_primary_key_versions() {
    let path = "memory:/planning_parity/pk_filter";
    let (io, table) = memory_table(
        path,
        common::incremental_helpers::pk_schema(&[("source.split.target-size", "1b")]),
    );
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 10])).await;
    write_batch(&table, &make_batch(vec![1], vec![99])).await;
    let mut builder = table.new_read_builder();
    builder.with_filter(
        PredicateBuilder::new(table.schema().fields())
            .equal("value", Datum::Int(10))
            .unwrap(),
    );
    let plan = builder
        .new_incremental_scan(IncrementalScanMode::Delta, 0, 2)
        .plan_combined_delta()
        .await
        .unwrap();
    assert_eq!(read_column(&builder, &plan, 0).await, vec![2]);
}

#[tokio::test]
async fn empty_position_plans_preserve_selected_snapshot() {
    let table = evolution_table("memory:/planning_parity/empty").await;
    let plan = table
        .new_read_builder()
        .new_scan()
        .with_row_position_slice(1, 2)
        .unwrap()
        .plan()
        .await
        .unwrap();
    assert!(plan.splits().is_empty());
    assert_eq!(plan.snapshot_id(), None);
    append_ids(&table, 0, 2).await;
    for shard in [false, true] {
        let builder = table.new_read_builder();
        let scan = builder.new_scan();
        let scan = if shard {
            scan.with_row_position_shard(3, 4)
        } else {
            scan.with_row_position_slice(3, 4)
        }
        .unwrap();
        let empty = scan.plan().await.unwrap();
        assert_eq!(empty.snapshot_id(), Some(1));
        assert!(empty.splits().is_empty());
    }
    let mut builder = table.new_read_builder();
    builder.with_limit(0);
    let plan = builder
        .new_incremental_scan(IncrementalScanMode::Delta, 0, 1)
        .with_row_position_slice(0, 1)
        .unwrap()
        .plan_combined_delta()
        .await
        .unwrap();
    assert_eq!(plan.snapshot_id(), Some(1));
    assert!(plan.splits().is_empty());
}

#[tokio::test]
async fn float_primary_key_plans_merge_signed_zero_versions() {
    use arrow_array::{ArrayRef, Float32Array, Float64Array};
    use paimon::spec::{DoubleType, FloatType};
    for double in [false, true] {
        let path = "memory:/planning_parity/float_keys";
        let schema = Schema::builder()
            .column(
                "f",
                if double {
                    DataType::Double(DoubleType::new())
                } else {
                    DataType::Float(FloatType::new())
                },
            )
            .column("i", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["f", "i"])
            .option("bucket", "1")
            .option("source.split.target-size", "1b")
            .option("source.split.open-file-cost", "1b")
            .build()
            .unwrap();
        let (io, table) = memory_table(path, TableSchema::new(0, &schema));
        setup_dirs(&io, path).await;
        persist_table_schema(&io, path, table.schema()).await;
        for (floats, keys, values) in [
            (
                vec![0.0, -0.0, -0.0, 1.0],
                vec![1, 1, 2, 1],
                vec![10, 20, 30, 50],
            ),
            (vec![-0.0, -0.0, 1.0], vec![1, 2, 1], vec![200, 300, 500]),
        ] {
            let floats: ArrayRef = if double {
                Arc::new(Float64Array::from(floats))
            } else {
                Arc::new(Float32Array::from(
                    floats.into_iter().map(|v| v as f32).collect::<Vec<_>>(),
                ))
            };
            write_batch(
                &table,
                &RecordBatch::try_from_iter(vec![
                    ("f", floats),
                    ("i", Arc::new(Int32Array::from(keys)) as ArrayRef),
                    ("value", Arc::new(Int32Array::from(values)) as ArrayRef),
                ])
                .unwrap(),
            )
            .await;
        }
        for combined in [false, true] {
            let builder = table.new_read_builder();
            let plan = if combined {
                builder
                    .new_incremental_scan(IncrementalScanMode::Delta, 0, 2)
                    .plan_combined_delta()
                    .await
                    .unwrap()
            } else {
                builder.new_scan().plan().await.unwrap()
            };
            let batches: Vec<RecordBatch> = builder
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let mut actual = vec![];
            for batch in batches {
                let keys = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let values = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    let f = if double {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(i)
                    } else {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Float32Array>()
                            .unwrap()
                            .value(i) as f64
                    };
                    let label = if f == 0.0 {
                        if f.is_sign_negative() {
                            "-0".into()
                        } else {
                            "+0".into()
                        }
                    } else {
                        f.to_string()
                    };
                    actual.push((label, keys.value(i), values.value(i)));
                }
            }
            actual.sort();
            assert_eq!(
                actual,
                vec![
                    ("+0".into(), 1, 10),
                    ("-0".into(), 1, 200),
                    ("-0".into(), 2, 300),
                    ("1".into(), 1, 500)
                ],
                "double={double}, combined={combined}"
            );
        }
    }
}

#[tokio::test]
async fn python_dv_manifest_layout_resolves_legacy_canonical_and_external_files() {
    use paimon::spec::IndexManifest;
    for layout in ["legacy", "canonical", "external", "missing-external"] {
        let table = evolution_table_with_options(
            "memory:/planning_parity/python_dv",
            &[
                ("deletion-vectors.enabled", "true"),
                ("index-file-in-data-file-dir", "true"),
            ],
        )
        .await;
        append_ids(&table, 0, 3).await;
        delete_ids(&table, &[1]).await;
        let snapshot = table.snapshot_manager().get_snapshot(2).await.unwrap();
        let manifest_path = format!(
            "{}/manifest/{}",
            table.location(),
            snapshot.index_manifest().unwrap()
        );
        let mut entries = IndexManifest::read(table.file_io(), &manifest_path)
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        let source = plan.splits()[0].data_deletion_files().unwrap()[0]
            .as_ref()
            .unwrap()
            .path()
            .to_string();
        let canonical = format!(
            "{}/{}",
            plan.splits()[0].bucket_path(),
            entries[0].index_file.file_name
        );
        table
            .file_io()
            .copy_file(&source, &canonical)
            .await
            .unwrap();
        let legacy = format!(
            "{}/index/{}",
            table.location(),
            entries[0].index_file.file_name
        );
        let external = format!("{}/external/dv", table.location());
        table
            .file_io()
            .copy_file(&canonical, &legacy)
            .await
            .unwrap();
        let expected_path = match layout {
            "legacy" => {
                table.file_io().delete_file(&canonical).await.unwrap();
                entries[0].index_file.external_path = None;
                legacy
            }
            "canonical" => {
                // The canonical file must win even if a stale legacy file exists.
                table
                    .file_io()
                    .new_output(&legacy)
                    .unwrap()
                    .write(bytes::Bytes::from_static(b"stale legacy DV"))
                    .await
                    .unwrap();
                entries[0].index_file.external_path = None;
                canonical
            }
            _ => {
                if layout == "external" {
                    table
                        .file_io()
                        .copy_file(&canonical, &external)
                        .await
                        .unwrap();
                }
                entries[0].index_file.external_path = Some(external.clone());
                external
            }
        };
        // Rewrite with PyPaimon's non-nullable DV array items, exercising the
        // writer-schema decoder together with path resolution and actual masks.
        let bytes = table
            .file_io()
            .new_input(&manifest_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        let reader = apache_avro::Reader::new(bytes.as_ref()).unwrap();
        let mut schema = serde_json::to_value(reader.writer_schema()).unwrap();
        let fields = schema["fields"].as_array_mut().unwrap();
        let field = fields
            .iter_mut()
            .find(|f| f["name"] == "_DELETIONS_VECTORS_RANGES")
            .unwrap();
        field["type"][1]["items"] = field["type"][1]["items"][1].clone();
        let bytes = paimon::spec::to_avro_bytes(&schema.to_string(), &entries).unwrap();
        table
            .file_io()
            .new_output(&manifest_path)
            .unwrap()
            .write(bytes::Bytes::from(bytes))
            .await
            .unwrap();
        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        assert_eq!(
            plan.splits()[0].data_deletion_files().unwrap()[0]
                .as_ref()
                .unwrap()
                .path(),
            expected_path
        );
        let batches = table
            .new_read_builder()
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await;
        if layout == "missing-external" {
            assert!(
                batches.is_err(),
                "an explicit missing DV must not fall back to a local copy"
            );
        } else {
            assert!(batches.is_ok(), "layout={layout}: {batches:?}");
            assert_eq!(read_ids(&table, &plan).await, vec![0, 2]);
        }
    }
}

#[tokio::test]
async fn multiple_dv_index_files_in_one_bucket_keep_all_deletions() {
    let table = evolution_table_with_options(
        "memory:/planning_parity/multiple_dvs",
        &[("deletion-vectors.enabled", "true")],
    )
    .await;
    append_ids(&table, 0, 3).await;
    append_ids(&table, 3, 6).await;
    let builder = table.new_write_builder();
    let mut messages = vec![];
    for row in [1, 4] {
        let mut delete = builder.new_delete().unwrap();
        delete.add_row_ids([row]).unwrap();
        messages.extend(delete.prepare_commit().await.unwrap());
    }
    builder.new_commit().commit(messages).await.unwrap();
    let snapshot = table.snapshot_manager().get_snapshot(3).await.unwrap();
    let entries = paimon::spec::IndexManifest::read(
        table.file_io(),
        &format!(
            "{}/manifest/{}",
            table.location(),
            snapshot.index_manifest().unwrap()
        ),
    )
    .await
    .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].bucket, entries[1].bucket);
    for combined in [false, true] {
        let builder = table.new_read_builder();
        let plan = if combined {
            builder
                .new_incremental_scan(IncrementalScanMode::Delta, 0, 3)
                .plan_combined_delta()
                .await
                .unwrap()
        } else {
            builder.new_scan().plan().await.unwrap()
        };
        assert_eq!(read_ids(&table, &plan).await, vec![0, 2, 3, 5]);
    }
}
