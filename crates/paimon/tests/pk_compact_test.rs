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

#[path = "pk_compact/merge_engines.rs"]
mod engine_tests;

#[path = "pk_compact/dynamic_bucket.rs"]
mod dynamic_tests;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use common::incremental_helpers::{
    make_batch, make_batch_with_kinds, make_partitioned_batch, memory_table, persist_table_schema,
    pk_schema, setup_dirs, write_batch,
};
use futures::TryStreamExt;
use paimon::spec::{
    CommitKind, DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema, VarCharType,
};
use paimon::table::{CompactPlan, CompactResult, CompactTask, IncrementalScanMode, Table};

async fn table_with_schema(schema: TableSchema) -> Table {
    let path = format!("memory:/pk_compact/{}", uuid::Uuid::new_v4());
    let (io, table) = memory_table(&path, schema);
    setup_dirs(&io, &path).await;
    persist_table_schema(&io, &path, table.schema()).await;
    table
}

async fn table(options: &[(&str, &str)]) -> Table {
    table_with_schema(pk_schema(options)).await
}

async fn read(table: &Table, projection: Option<&[&str]>) -> Vec<RecordBatch> {
    let mut builder = table.new_read_builder();
    if let Some(projection) = projection {
        builder.with_projection(projection).unwrap();
    }
    let plan = builder.new_scan().plan().await.unwrap();
    builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

async fn rows(table: &Table) -> Vec<(i32, i32)> {
    let mut rows = Vec::new();
    for batch in read(table, None).await {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|i| (ids.value(i), values.value(i))));
    }
    rows.sort_unstable();
    rows
}

async fn sequences(table: &Table) -> Vec<(i32, i64)> {
    let mut rows = Vec::new();
    for batch in read(table, Some(&["id", "_SEQUENCE_NUMBER"])).await {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let seqs = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|i| (ids.value(i), seqs.value(i))));
    }
    rows.sort_unstable();
    rows
}

async fn execute(table: &Table, plan: &CompactPlan) -> Vec<CompactResult> {
    let executor = table.new_compact_builder().new_executor().unwrap();
    let mut results = Vec::new();
    for task in plan.tasks() {
        let task: CompactTask = serde_json::from_slice(&serde_json::to_vec(task).unwrap()).unwrap();
        results.push(executor.execute(&task).await.unwrap());
    }
    serde_json::from_slice(&serde_json::to_vec(&results).unwrap()).unwrap()
}

#[tokio::test]
async fn pk_compaction_rejects_concurrent_overlapping_positive_level_files() {
    for (key, conflicts) in [(1, true), (256, false)] {
        let table = table(&[]).await;
        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let results = execute(&table, &plan).await;
        // A Java writer created before our input was committed can compact its
        // own L0 files locally and publish L1 without replacing our L0 input.
        let write = table.new_write_builder();
        let mut writer = write.new_write().unwrap();
        writer
            .write_arrow(&[make_batch(vec![key], vec![20])], None)
            .await
            .unwrap();
        let mut messages = writer.prepare_commit().await.unwrap();
        for file in &mut messages[0].new_files {
            file.level = 1;
        }
        write.new_commit().commit(messages).await.unwrap();
        let result = builder.new_commit().unwrap().commit_compact(results).await;
        if conflicts {
            assert!(result.unwrap_err().to_string().contains("LSM"));
            assert_eq!(rows(&table).await, vec![(1, 20)]);
        } else {
            result.unwrap();
            assert_eq!(rows(&table).await, vec![(1, 10), (256, 20)]);
        }
    }
}

#[tokio::test]
async fn pk_compaction_accepts_disjoint_nan_and_finite_output_ranges() {
    let schema = Schema::builder()
        .column("id", DataType::Double(paimon::spec::DoubleType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("target-file-row-num", "1")
        .build()
        .unwrap();
    let table = table_with_schema(TableSchema::new(0, &schema)).await;
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            std::sync::Arc::new(arrow_array::Float64Array::from(vec![1.0, f64::NAN]))
                as arrow_array::ArrayRef,
        ),
        (
            "value",
            std::sync::Arc::new(Int32Array::from(vec![10, 20])) as arrow_array::ArrayRef,
        ),
    ])
    .unwrap();
    write_batch(&table, &batch).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    assert_eq!(results[0].files().len(), 2);
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    let batches = read(&table, None).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
}

#[tokio::test]
async fn pk_compaction_flushes_small_buffers_without_fragmenting_files() {
    for format in ["parquet", "orc", "avro", "row"] {
        let table = table(&[
            ("file.format", format),
            ("file.compression", "none"),
            ("read.batch-size", "1"),
            ("write-buffer-size", "1b"),
            ("target-file-size", "256mb"),
            ("file-index.bitmap.columns", "id"),
        ])
        .await;
        write_batch(
            &table,
            &make_batch(vec![1, 2, 3, 4, 5], vec![10, 20, 30, 40, 50]),
        )
        .await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        assert_eq!(plan.tasks()[0].files().len(), 1);
        let before = sequences(&table).await;
        let results = execute(&table, &plan).await;
        assert_eq!(
            results[0].files().len(),
            1,
            "{format}: a buffer flush must not close the file"
        );
        let file = &results[0].files()[0];
        for (bytes, expected) in [
            (file.key_stats.min_values(), 1),
            (file.key_stats.max_values(), 5),
            (file.min_key.as_slice(), 1),
            (file.max_key.as_slice(), 5),
        ] {
            let key = paimon::spec::BinaryRow::from_serialized_bytes(bytes).unwrap();
            assert_eq!(key.get_int(0).unwrap(), expected, "{format}");
        }
        assert_eq!(file.key_stats.null_counts(), &[Some(0)]);
        assert!(file.embedded_index.is_some());
        builder
            .new_commit()
            .unwrap()
            .commit_compact(results)
            .await
            .unwrap();
        assert_eq!(
            rows(&table).await,
            vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]
        );
        assert_eq!(sequences(&table).await, before);
        let mut reader = table.new_read_builder();
        reader.with_filter(
            PredicateBuilder::new(table.schema().fields())
                .equal("id", Datum::Int(3))
                .unwrap(),
        );
        let plan = reader.new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = reader
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            1,
            "{format}"
        );
    }
}

#[tokio::test]
async fn pk_compaction_rolls_by_bytes_without_a_row_limit() {
    let table = table(&[("target-file-size", "1b"), ("read.batch-size", "1")]).await;
    write_batch(&table, &make_batch(vec![1, 2, 3], vec![10, 20, 30])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks()[0].files().len(), 1);
    let results = execute(&table, &plan).await;
    assert_eq!(results[0].files().len(), 3);
    assert!(results[0].files().iter().all(|file| file.row_count == 1));
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20), (3, 30)]);
}

#[tokio::test]
async fn pk_compaction_merges_versions_and_preserves_sequences_deletes_and_history() {
    for format in ["parquet", "orc", "avro", "row"] {
        let table = table(&[
            ("file.format", format),
            ("file.compression", "none"),
            ("changelog-producer", "input"),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        write_batch(&table, &make_batch(vec![1, 3], vec![11, 30])).await;
        write_batch(
            &table,
            &make_batch_with_kinds(vec![2, 4], vec![20, 40], vec![3, 0]),
        )
        .await;
        let expected = vec![(1, 11), (3, 30), (4, 40)];
        assert_eq!(rows(&table).await, expected);
        let before_sequences = sequences(&table).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        assert_eq!(plan.tasks().len(), 1);
        assert_eq!(plan.tasks()[0].files().len(), 3);
        let results = execute(&table, &plan).await;
        assert_eq!(results[0].files().len(), 1, "{format}");
        let file = &results[0].files()[0];
        assert_eq!(file.level, 1);
        assert_eq!(file.delete_row_count, Some(1));
        assert_eq!((file.min_sequence_number, file.max_sequence_number), (2, 5));
        assert_eq!(file.file_source, Some(1));
        let committer = builder.new_commit().unwrap();
        let metrics = committer.commit_compact(results.clone()).await.unwrap();
        assert_eq!(
            (
                metrics.files_removed,
                metrics.files_added,
                metrics.rows_written
            ),
            (3, 1, 4)
        );
        assert_eq!(rows(&table).await, expected, "{format}");
        assert_eq!(sequences(&table).await, before_sequences, "{format}");
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.commit_kind(), &CommitKind::COMPACT);
        assert_eq!(snapshot.total_record_count(), Some(4));
        assert_eq!(snapshot.delta_record_count(), Some(-2));
        let old = table.copy_with_pinned_snapshot(Some(
            &table.snapshot_manager().get_snapshot(3).await.unwrap(),
        ));
        assert_eq!(rows(&old).await, expected);
        let reader = table.new_read_builder();
        let events = reader
            .new_incremental_scan(IncrementalScanMode::Delta, 0, 4)
            .plan_combined_delta()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = reader
            .new_read()
            .unwrap()
            .to_arrow(events.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            6
        );
        // Duplicate filtering must precede validation of an expired read snapshot.
        table
            .file_io()
            .delete_file(&table.snapshot_manager().snapshot_path(3))
            .await
            .unwrap();
        committer.commit_compact(results).await.unwrap();
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            Some(4)
        );
        assert!(builder
            .new_planner()
            .unwrap()
            .plan()
            .await
            .unwrap()
            .tasks()
            .is_empty());
        write_batch(&table, &make_batch(vec![1, 5], vec![12, 50])).await;
        assert_eq!(rows(&table).await, vec![(1, 12), (3, 30), (4, 40), (5, 50)]);
        assert!(sequences(&table)
            .await
            .iter()
            .any(|&(id, seq)| id == 1 && seq > 5));
    }
}

#[tokio::test]
async fn pk_compaction_keeps_tombstones_against_later_out_of_order_user_sequences() {
    for (order, initial, deleted, stale, fresh) in [
        ("ascending", 100, 150, 120, 160),
        ("descending", 100, 50, 80, 40),
    ] {
        let table = table(&[
            ("sequence.field", "value"),
            ("sequence.field.sort-order", order),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1], vec![initial])).await;
        write_batch(
            &table,
            &make_batch_with_kinds(vec![1], vec![deleted], vec![3]),
        )
        .await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let results = execute(&table, &plan).await;
        assert_eq!(results[0].files()[0].delete_row_count, Some(1));
        builder
            .new_commit()
            .unwrap()
            .commit_compact(results)
            .await
            .unwrap();
        assert!(rows(&table).await.is_empty());
        write_batch(&table, &make_batch(vec![1], vec![stale])).await;
        assert!(rows(&table).await.is_empty(), "{order}");
        // Compact again with the stale value present; it must still lose.
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        builder
            .new_commit()
            .unwrap()
            .commit_compact(execute(&table, &plan).await)
            .await
            .unwrap();
        write_batch(&table, &make_batch(vec![1], vec![deleted])).await;
        assert_eq!(
            rows(&table).await,
            vec![(1, deleted)],
            "equal user sequence must use the newer system sequence"
        );
        write_batch(&table, &make_batch(vec![1], vec![fresh])).await;
        assert_eq!(rows(&table).await, vec![(1, fresh)], "{order}");
    }
}

#[tokio::test]
async fn pk_compaction_includes_large_files_and_whole_buckets_and_upgrades_single_l0() {
    let table = table(&[("target-file-size", "1b")]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 1);
    assert_eq!(plan.tasks()[0].files().len(), 1);
    builder
        .new_commit()
        .unwrap()
        .commit_compact(execute(&table, &plan).await)
        .await
        .unwrap();
    for value in 11..16 {
        write_batch(&table, &make_batch(vec![1], vec![value])).await;
    }
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 1);
    assert_eq!(plan.tasks()[0].files().len(), 6);
    assert!(plan.tasks()[0]
        .files()
        .iter()
        .all(|file| file.file_size > 1));
    builder
        .new_commit()
        .unwrap()
        .commit_compact(execute(&table, &plan).await)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(1, 15)]);
}

#[tokio::test]
async fn pk_compaction_keeps_writes_committed_after_planning_and_rejects_competing_rewrites() {
    let table = table(&[]).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    write_batch(&table, &make_batch(vec![1, 3], vec![11, 30])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let competitor = table.new_compact_builder();
    let competing = execute(
        &table,
        &competitor.new_planner().unwrap().plan().await.unwrap(),
    )
    .await;
    write_batch(
        &table,
        &make_batch_with_kinds(vec![1, 2], vec![12, 20], vec![0, 3]),
    )
    .await;
    // Execute after the append, using only the captured bucket state.
    let results = execute(&table, &plan).await;
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(1, 12), (3, 30)]);
    assert!(competitor
        .new_commit()
        .unwrap()
        .commit_compact(competing)
        .await
        .is_err());
    assert_eq!(rows(&table).await, vec![(1, 12), (3, 30)]);
}

#[tokio::test]
async fn pk_compaction_rolls_ordered_files_and_rebuilds_indexes_with_level_stats() {
    let table = table(&[
        ("target-file-row-num", "2"),
        ("read.batch-size", "4"),
        ("write-buffer-size", "64 B"),
        ("file-index.bitmap.columns", "id"),
        ("file-index.in-manifest-threshold", "0"),
        ("metadata.stats-mode-per-level", "0:none,1:full"),
    ])
    .await;
    write_batch(
        &table,
        &make_batch(vec![0, 1, 2, 3, 4], vec![0, 10, 20, 30, 40]),
    )
    .await;
    write_batch(
        &table,
        &make_batch_with_kinds(vec![1, 3], vec![11, 30], vec![0, 3]),
    )
    .await;
    let expected = rows(&table).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    assert_eq!(results[0].files().len(), 3);
    assert!(results[0].files().iter().all(|file| file.level == 1
        && file.row_count <= 2
        && (file.embedded_index.is_some() || !file.extra_files.is_empty())
        && !file.value_stats.null_counts().is_empty()));
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, expected);
    // Rolled files at one positive level already form a sorted run. Replanning
    // must not continually rewrite that same run on every maintenance pass.
    assert!(builder
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap()
        .tasks()
        .is_empty());
    for id in 0..6 {
        let mut read = table.new_read_builder();
        read.with_filter(
            PredicateBuilder::new(table.schema().fields())
                .equal("id", Datum::Int(id))
                .unwrap(),
        );
        let plan = read.new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = read
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            usize::from(id < 5 && id != 3)
        );
    }
}

#[tokio::test]
async fn pk_compaction_refuses_partial_buckets_and_unsupported_layouts() {
    for options in [
        vec![
            ("merge-engine", "first-row"),
            ("changelog-producer", "lookup"),
        ],
        vec![("changelog-producer", "full-compaction")],
        vec![
            ("merge-engine", "first-row"),
            ("pk-clustering-override", "true"),
            ("clustering.columns", "value"),
        ],
        vec![
            ("record-level.expire-time", "1 d"),
            ("record-level.time-field", "value"),
        ],
    ] {
        let table = table(&options).await;
        assert!(table.new_compact_builder().new_planner().is_err());
    }
    let table = table(&[]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![2], vec![20])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let mut task = serde_json::to_value(&plan.tasks()[0]).unwrap();
    task["files"].as_array_mut().unwrap().pop();
    let task: CompactTask = serde_json::from_value(task).unwrap();
    assert!(builder
        .new_executor()
        .unwrap()
        .execute(&task)
        .await
        .unwrap_err()
        .to_string()
        .contains("complete captured bucket"));
    let results = execute(&table, &plan).await;
    let mut encoded = serde_json::to_value(&results).unwrap();
    encoded[0]["task"]["files"].as_array_mut().unwrap().pop();
    let partial: Vec<CompactResult> = serde_json::from_value(encoded).unwrap();
    assert!(builder
        .new_commit()
        .unwrap()
        .commit_compact(partial)
        .await
        .is_err());
    let mut encoded = serde_json::to_value(&results).unwrap();
    encoded[0]["files"][0]["_LEVEL"] = serde_json::json!(0);
    let bad: Vec<CompactResult> = serde_json::from_value(encoded).unwrap();
    assert!(builder
        .new_commit()
        .unwrap()
        .commit_compact(bad)
        .await
        .is_err());
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn pk_compaction_groups_composite_keys_by_partition_and_bucket() {
    let schema = Schema::builder()
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["pt", "id"])
        .partition_keys(["pt"])
        .option("bucket", "2")
        .build()
        .unwrap();
    let table = table_with_schema(TableSchema::new(0, &schema)).await;
    for version in 0..2 {
        for pt in ["a", "b"] {
            for bucket in 0..2 {
                let builder = table.new_write_builder();
                let mut writer = builder.new_write().unwrap();
                writer
                    .write_arrow(
                        &[make_partitioned_batch(
                            vec![pt],
                            vec![bucket],
                            vec![version * 10 + bucket],
                        )],
                        Some(bucket),
                    )
                    .await
                    .unwrap();
                builder
                    .new_commit()
                    .commit(writer.prepare_commit().await.unwrap())
                    .await
                    .unwrap();
            }
        }
    }
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 4);
    assert!(plan.tasks().iter().all(|task| task.files().len() == 2));
    let mut results = execute(&table, &plan).await;
    let committer = builder.new_commit().unwrap();
    committer
        .commit_compact(results.drain(..2).collect())
        .await
        .unwrap();
    committer.commit_compact(results).await.unwrap();
    let mut actual = Vec::new();
    for batch in read(&table, None).await {
        let pt = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let id = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let value = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        actual.extend(
            (0..batch.num_rows()).map(|i| (pt.value(i).to_string(), id.value(i), value.value(i))),
        );
    }
    actual.sort();
    assert_eq!(
        actual,
        vec![
            ("a".into(), 0, 10),
            ("a".into(), 1, 11),
            ("b".into(), 0, 10),
            ("b".into(), 1, 11)
        ]
    );
}

#[tokio::test]
async fn pk_compaction_preserves_a_null_winner_instead_of_an_older_non_null_value() {
    let table = table(&[]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    let nullable = RecordBatch::try_new(
        std::sync::Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
            arrow_schema::Field::new("value", arrow_schema::DataType::Int32, true),
        ])),
        vec![
            std::sync::Arc::new(Int32Array::from(vec![1])),
            std::sync::Arc::new(Int32Array::from(vec![None])),
        ],
    )
    .unwrap();
    write_batch(&table, &nullable).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    builder
        .new_commit()
        .unwrap()
        .commit_compact(execute(&table, &plan).await)
        .await
        .unwrap();
    let batches = read(&table, None).await;
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    assert!(batches[0].column_by_name("value").unwrap().is_null(0));
}

#[tokio::test]
async fn pk_compaction_memory_failure_leaves_source_files_and_releases_reservations() {
    let table = table(&[]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![1], vec![11])).await;
    let plan = table
        .new_compact_builder()
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap();
    let resources = paimon::resource::ResourceContext::builder()
        .memory_limit(1)
        .build()
        .unwrap();
    let executor = table
        .new_compact_builder()
        .with_resources(resources.clone())
        .new_executor()
        .unwrap();
    let before = table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .len();
    let error = executor.execute(&plan.tasks()[0]).await.unwrap_err();
    assert!(
        matches!(error, paimon::Error::ResourceExhausted { .. }),
        "{error}"
    );
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    assert_eq!(
        table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len(),
        before
    );
    assert_eq!(rows(&table).await, vec![(1, 11)]);
}

#[tokio::test]
async fn pk_compaction_read_failure_removes_already_flushed_unsubmitted_files() {
    let table = table(&[("write-buffer-size", "1 B"), ("read.batch-size", "1")]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![2], vec![20])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let read = table.new_read_builder().new_scan().plan().await.unwrap();
    let bucket = read.splits()[0].bucket_path();
    let second = plan.tasks()[0]
        .files()
        .iter()
        .find(|file| file.max_sequence_number == 1)
        .unwrap();
    table
        .file_io()
        .delete_file(&format!("{bucket}/{}", second.file_name))
        .await
        .unwrap();
    let before = table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .len();
    assert!(builder
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .is_err());
    assert_eq!(
        table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len(),
        before
    );
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(2)
    );
}
