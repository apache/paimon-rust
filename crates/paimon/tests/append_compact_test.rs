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

use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use common::incremental_helpers::{
    make_batch, make_partitioned_batch, memory_table, persist_table_schema, pk_schema, setup_dirs,
    write_batch,
};
use futures::TryStreamExt;
use paimon::spec::{
    CommitKind, DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema, VarCharType,
};
use paimon::table::{CompactPlan, CompactResult, IncrementalScanMode, Table};
use std::collections::HashMap;

fn schema(options: &[(&str, &str)]) -> TableSchema {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("bucket", "-1")
        .option("file.format", "parquet");
    for (key, value) in options {
        builder = builder.option(*key, *value);
    }
    TableSchema::new(0, &builder.build().unwrap())
}

async fn table(schema: TableSchema) -> Table {
    let path = format!("memory:/compact/{}", uuid::Uuid::new_v4());
    let (io, table) = memory_table(&path, schema);
    setup_dirs(&io, &path).await;
    persist_table_schema(&io, &path, table.schema()).await;
    table
}

async fn batches(table: &Table, snapshot: Option<i64>) -> Vec<RecordBatch> {
    let table = match snapshot {
        Some(id) => table.copy_with_pinned_snapshot(Some(
            &table.snapshot_manager().get_snapshot(id).await.unwrap(),
        )),
        None => table.clone(),
    };
    let builder = table.new_read_builder();
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

async fn rows(table: &Table, snapshot: Option<i64>) -> Vec<(i32, Option<i32>)> {
    let mut rows = Vec::new();
    for batch in batches(table, snapshot).await {
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
        rows.extend(
            (0..batch.num_rows())
                .map(|i| (ids.value(i), (!values.is_null(i)).then(|| values.value(i)))),
        );
    }
    rows.sort_unstable();
    rows
}

async fn append_files(table: &Table, count: i32) {
    for id in 0..count {
        write_batch(table, &make_batch(vec![id, id], vec![id * 10, id * 10])).await;
    }
}

async fn execute(table: &Table, plan: &CompactPlan) -> Vec<CompactResult> {
    // A worker's input limit must not split an already planned task.
    let executor = table
        .new_compact_builder()
        .with_max_files_per_task(2)
        .new_executor()
        .unwrap();
    let mut results = Vec::new();
    for task in plan.tasks() {
        results.push(executor.execute(task).await.unwrap());
    }
    results
}

#[tokio::test]
async fn compact_preserves_duplicates_nulls_old_snapshots_and_incremental_events() {
    for format in ["parquet", "orc", "avro", "row"] {
        let table = table(schema(&[
            ("file.format", format),
            ("file.compression", "none"),
        ]))
        .await;
        append_files(&table, 3).await;
        let batch = make_batch(vec![9], vec![90]);
        let nullable = RecordBatch::try_new(
            std::sync::Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
                arrow_schema::Field::new("value", arrow_schema::DataType::Int32, true),
            ])),
            vec![
                batch.column(0).clone(),
                std::sync::Arc::new(Int32Array::from(vec![None])),
            ],
        )
        .unwrap();
        write_batch(&table, &nullable).await;
        let expected = rows(&table, None).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        assert_eq!(plan.snapshot_id(), Some(4));
        assert_eq!(plan.tasks().len(), 1);
        assert_eq!(plan.tasks()[0].files().len(), 4);

        // Both task and result cross a serialization boundary, as on a remote worker.
        let plan: CompactPlan =
            serde_json::from_slice(&serde_json::to_vec(&plan).unwrap()).unwrap();
        let results = execute(&table, &plan).await;
        let results: Vec<CompactResult> =
            serde_json::from_slice(&serde_json::to_vec(&results).unwrap()).unwrap();
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            Some(4)
        );
        let committer = builder.new_commit().unwrap();
        let metrics = committer.commit_compact(results.clone()).await.unwrap();
        assert_eq!(metrics.files_removed, 4, "{format}");
        assert_eq!(metrics.files_added, 1, "{format}");
        assert_eq!(metrics.rows_written, 7, "{format}");
        assert_eq!(rows(&table, None).await, expected, "{format}");
        assert_eq!(rows(&table, Some(4)).await, expected, "{format}");
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.commit_kind(), &CommitKind::COMPACT);
        assert_eq!(snapshot.total_record_count(), Some(7));
        assert_eq!(snapshot.delta_record_count(), Some(0));
        assert_eq!(snapshot.changelog_record_count(), None);
        let read = table.new_read_builder();
        let latest = read.new_scan().plan().await.unwrap();
        assert_eq!(
            latest.splits().iter().flat_map(|s| s.data_files()).count(),
            1
        );
        assert!(latest
            .splits()
            .iter()
            .flat_map(|s| s.data_files())
            .all(|f| f.file_source == Some(1)));
        let incremental = read
            .new_incremental_scan(IncrementalScanMode::Delta, 0, 5)
            .plan_combined_delta()
            .await
            .unwrap();
        let events: Vec<RecordBatch> = read
            .new_read()
            .unwrap()
            .to_arrow(incremental.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(events.iter().map(|b| b.num_rows()).sum::<usize>(), 7);

        // External retries reuse the same task identity and publish no second snapshot.
        assert_eq!(committer.commit_compact(results).await.unwrap(), metrics);
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            Some(5)
        );
    }
}

#[tokio::test]
async fn compact_allows_concurrent_append_but_rejects_stale_inputs() {
    let table = table(schema(&[])).await;
    append_files(&table, 2).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    let competing = table.new_compact_builder();
    let other = execute(
        &table,
        &competing.new_planner().unwrap().plan().await.unwrap(),
    )
    .await;
    write_batch(&table, &make_batch(vec![99], vec![990])).await;
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    let expected = vec![
        (0, Some(0)),
        (0, Some(0)),
        (1, Some(10)),
        (1, Some(10)),
        (99, Some(990)),
    ];
    assert_eq!(rows(&table, None).await, expected);
    let error = competing
        .new_commit()
        .unwrap()
        .commit_compact(other.clone())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("conflict"), "{error}");
    assert_eq!(rows(&table, None).await, expected);
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(4)
    );
    // A rejected commit retains its prepared output; it never deletes a possible published file.
    let old = table.copy_with_pinned_snapshot(Some(
        &table.snapshot_manager().get_snapshot(2).await.unwrap(),
    ));
    let split = old.new_read_builder().new_scan().plan().await.unwrap();
    let bucket = split.splits()[0].bucket_path();
    assert!(table
        .file_io()
        .exists(&format!("{bucket}/{}", other[0].files()[0].file_name))
        .await
        .unwrap());
}

#[tokio::test]
async fn compact_after_overwrite_does_not_resurrect_old_rows() {
    let table = table(schema(&[])).await;
    append_files(&table, 2).await;
    let compact = table.new_compact_builder();
    let plan = compact.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    let overwrite = table.new_write_builder().with_overwrite();
    let mut writer = overwrite.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![88], vec![880]))
        .await
        .unwrap();
    overwrite
        .new_commit()
        .overwrite(writer.prepare_commit().await.unwrap(), None)
        .await
        .unwrap();
    assert!(compact
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .is_err());
    assert_eq!(rows(&table, None).await, vec![(88, Some(880))]);
}

#[tokio::test]
async fn compact_skips_empty_single_and_target_sized_files_and_bounds_tasks() {
    let table = table(schema(&[])).await;
    let builder = table.new_compact_builder();
    assert!(builder
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap()
        .tasks()
        .is_empty());
    builder
        .new_commit()
        .unwrap()
        .commit_compact(vec![])
        .await
        .unwrap();
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        None
    );
    append_files(&table, 1).await;
    assert!(builder
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap()
        .tasks()
        .is_empty());
    append_files(&table, 4).await;
    let compact = table.new_compact_builder().with_max_files_per_task(2);
    let plan = compact.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 2);
    assert!(plan.tasks().iter().all(|task| task.files().len() == 2));
    let min_size = plan
        .tasks()
        .iter()
        .flat_map(|t| t.files())
        .map(|f| f.file_size)
        .min()
        .unwrap();
    let small = table.copy_with_options(HashMap::from([(
        "target-file-size".into(),
        format!("{min_size}b"),
    )]));
    assert!(small
        .new_compact_builder()
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap()
        .tasks()
        .is_empty());
    let mut results = execute(&table, &plan).await;
    let committer = compact.new_commit().unwrap();
    committer
        .commit_compact(vec![results.remove(0)])
        .await
        .unwrap();
    committer.commit_compact(results).await.unwrap();
    assert_eq!(rows(&table, None).await.len(), 10);
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(7)
    );
}

#[tokio::test]
async fn compact_keeps_partitions_and_buckets_separate_and_honors_partition_filter() {
    let schema = Schema::builder()
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .option("bucket", "2")
        .option("bucket-key", "id")
        .build()
        .unwrap();
    let table = table(TableSchema::new(0, &schema)).await;
    for _ in 0..2 {
        for bucket in 0..2 {
            for pt in ["a", "b"] {
                let write = table.new_write_builder();
                let mut writer = write.new_write().unwrap();
                writer
                    .write_arrow(
                        &[make_partitioned_batch(
                            vec![pt],
                            vec![bucket],
                            vec![bucket * 10],
                        )],
                        Some(bucket),
                    )
                    .await
                    .unwrap();
                write
                    .new_commit()
                    .commit(writer.prepare_commit().await.unwrap())
                    .await
                    .unwrap();
            }
        }
    }
    let predicate = PredicateBuilder::new(table.schema().fields())
        .equal("pt", Datum::String("a".into()))
        .unwrap();
    let builder = table
        .new_compact_builder()
        .with_partition_filter(predicate)
        .unwrap();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 2);
    assert!(plan.tasks().iter().all(|task| task.files().len() == 2));
    assert_ne!(plan.tasks()[0].bucket(), plan.tasks()[1].bucket());
    let expected = rows(&table, None).await;
    let mut results = execute(&table, &plan).await;
    results.reverse();
    let committer = builder.new_commit().unwrap();
    committer.commit_compact(results.clone()).await.unwrap();
    results.reverse();
    committer.commit_compact(results).await.unwrap();
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(9)
    );
    assert_eq!(rows(&table, None).await, expected);
    let mut partition_counts = std::collections::BTreeMap::new();
    for batch in batches(&table, None).await {
        let partitions = batch
            .column_by_name("pt")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            *partition_counts
                .entry(partitions.value(i).to_string())
                .or_insert(0) += 1;
        }
    }
    assert_eq!(
        partition_counts,
        std::collections::BTreeMap::from([("a".to_string(), 4), ("b".to_string(), 4)])
    );
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    assert_eq!(plan.splits().iter().flat_map(|s| s.data_files()).count(), 6);
}

#[tokio::test]
async fn compact_refuses_unsupported_tables_invalid_options_and_foreign_tasks() {
    for options in [
        vec![("row-tracking.enabled", "true")],
        vec![
            ("data-evolution.enabled", "true"),
            ("row-tracking.enabled", "true"),
        ],
        vec![("deletion-vectors.enabled", "true")],
    ] {
        let table = table(schema(&options)).await;
        assert!(table.new_compact_builder().new_planner().is_err());
        assert!(table.new_compact_builder().new_executor().is_err());
    }
    let pk = table(pk_schema(&[("changelog-producer", "full-compaction")])).await;
    assert!(pk.new_compact_builder().new_planner().is_err());
    let table = table(schema(&[])).await;
    let invalid_size =
        table.copy_with_options(HashMap::from([("target-file-size".into(), "0b".into())]));
    assert!(invalid_size.new_compact_builder().new_planner().is_err());
    for max_files_per_task in [0, 1] {
        assert!(table
            .new_compact_builder()
            .with_max_files_per_task(max_files_per_task)
            .new_planner()
            .is_err());
    }
    append_files(&table, 2).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let foreign = self::table(schema(&[])).await;
    assert!(foreign
        .new_compact_builder()
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .is_err());
    let results = execute(&table, &plan).await;
    assert!(foreign
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(results.clone())
        .await
        .is_err());
    assert!(builder
        .new_commit()
        .unwrap()
        .commit_compact(vec![results[0].clone(), results[0].clone()])
        .await
        .is_err());
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(2)
    );
}

#[tokio::test]
async fn compact_rebuilds_embedded_and_sidecar_indexes_for_the_merged_row_positions() {
    for threshold in ["0 B", "1 MB"] {
        let table = table(schema(&[
            ("file-index.bitmap.columns", "id"),
            ("file-index.in-manifest-threshold", threshold),
        ]))
        .await;
        append_files(&table, 3).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let results = execute(&table, &plan).await;
        assert_eq!(results[0].files().len(), 1);
        let file = &results[0].files()[0];
        assert_eq!(file.embedded_index.is_some(), threshold == "1 MB");
        if threshold == "0 B" {
            assert!(file.extra_files.iter().any(|name| name.ends_with(".index")));
        }
        builder
            .new_commit()
            .unwrap()
            .commit_compact(results)
            .await
            .unwrap();
        for id in [0, 1, 2, 9] {
            let mut read = table.new_read_builder();
            let predicate = PredicateBuilder::new(table.schema().fields())
                .equal("id", Datum::Int(id))
                .unwrap();
            read.with_filter(predicate);
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
                if id == 9 { 0 } else { 2 }
            );
        }
    }
}

#[tokio::test]
async fn compact_rejects_mismatched_input_metadata_and_changed_table_schema() {
    let table = table(schema(&[])).await;
    append_files(&table, 2).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    // A result arriving from a worker must retain the source metadata exactly.
    let mut encoded = serde_json::to_value(results).unwrap();
    encoded[0]["task"]["files"][0]["_MIN_SEQUENCE_NUMBER"] = serde_json::json!(99);
    let results: Vec<CompactResult> = serde_json::from_value(encoded).unwrap();
    let error = builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("metadata changed"), "{error}");
    assert_eq!(rows(&table, None).await.len(), 4);

    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    let changed = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .column("extra", DataType::Int(IntType::new()))
        .option("bucket", "-1")
        .build()
        .unwrap();
    persist_table_schema(
        table.file_io(),
        table.location(),
        &TableSchema::new(1, &changed),
    )
    .await;
    let error = builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("schema changed"), "{error}");
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(2)
    );
}

#[tokio::test]
async fn compact_execution_failure_cleans_only_its_unsubmitted_output() {
    let table = table(schema(&[])).await;
    append_files(&table, 2).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let files = plan.tasks()[0].files();
    let source = table.new_read_builder().new_scan().plan().await.unwrap();
    let bucket = source.splits()[0].bucket_path();
    table
        .file_io()
        .delete_file(&format!("{bucket}/{}", files[1].file_name))
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
    assert!(table
        .file_io()
        .exists(&format!("{bucket}/{}", files[0].file_name))
        .await
        .unwrap());
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(2)
    );
}

#[tokio::test]
async fn compact_obeys_the_executor_memory_budget_without_changing_the_table() {
    let table = table(schema(&[])).await;
    append_files(&table, 2).await;
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
    assert_eq!(rows(&table, None).await.len(), 4);
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(2)
    );
}
