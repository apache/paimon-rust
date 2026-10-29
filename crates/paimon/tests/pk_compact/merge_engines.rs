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

use super::*;
use arrow_array::Int8Array;
use std::sync::Arc;

#[path = "merge_engines_edges.rs"]
mod edges;

async fn compact(table: &Table) -> paimon::table::CompactMetrics {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert!(!plan.tasks().is_empty());
    builder
        .new_commit()
        .unwrap()
        .commit_compact(execute(table, &plan).await)
        .await
        .unwrap()
}

async fn all_rows(table: &Table) -> Vec<(i32, i32)> {
    let builder = table.new_read_builder();
    let plan = builder
        .new_scan()
        .with_scan_all_files()
        .plan()
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
    let mut result = Vec::new();
    for batch in batches {
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
        result.extend((0..batch.num_rows()).map(|i| (ids.value(i), values.value(i))));
    }
    result.sort_unstable();
    result
}

async fn partial(options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("version", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .column("other", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "partial-update");
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    table_with_schema(TableSchema::new(0, &schema.build().unwrap())).await
}

fn update(version: Option<i32>, value: Option<i32>, other: Option<i32>, kind: i8) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
            arrow_schema::Field::new("version", arrow_schema::DataType::Int32, true),
            arrow_schema::Field::new("value", arrow_schema::DataType::Int32, true),
            arrow_schema::Field::new("other", arrow_schema::DataType::Int32, true),
            arrow_schema::Field::new("_VALUE_KIND", arrow_schema::DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![version])),
            Arc::new(Int32Array::from(vec![value])),
            Arc::new(Int32Array::from(vec![other])),
            Arc::new(Int8Array::from(vec![kind])),
        ],
    )
    .unwrap()
}

async fn state(table: &Table) -> Vec<(Option<i32>, Option<i32>, Option<i32>)> {
    let mut result = Vec::new();
    for batch in read(table, None).await {
        for row in 0..batch.num_rows() {
            let get = |name| {
                let array = batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                (!array.is_null(row)).then(|| array.value(row))
            };
            result.push((get("version"), get("value"), get("other")));
        }
    }
    result.sort_unstable();
    result
}

#[tokio::test]
async fn first_row_compaction_preserves_earliest_rows_and_future_writes() {
    for format in ["parquet", "orc", "avro", "row"] {
        let table = table(&[
            ("merge-engine", "first-row"),
            ("file.format", format),
            ("file.compression", "none"),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        write_batch(&table, &make_batch(vec![1, 3], vec![11, 30])).await;
        assert_eq!(all_rows(&table).await, vec![(1, 10), (2, 20), (3, 30)]);
        let metrics = compact(&table).await;
        assert_eq!(metrics.rows_written, 3);
        assert_eq!(
            rows(&table).await,
            vec![(1, 10), (2, 20), (3, 30)],
            "{format}"
        );
        write_batch(&table, &make_batch(vec![1, 4], vec![99, 40])).await;
        compact(&table).await;
        assert_eq!(
            rows(&table).await,
            vec![(1, 10), (2, 20), (3, 30), (4, 40)],
            "{format}"
        );
    }
}

#[tokio::test]
async fn partial_update_compaction_combines_sparse_columns_and_keeps_sequence_metadata() {
    for format in ["parquet", "orc", "avro", "row"] {
        let table = partial(&[("file.format", format), ("file.compression", "none")]).await;
        write_batch(&table, &update(Some(1), Some(10), None, 0)).await;
        write_batch(&table, &update(None, None, Some(20), 0)).await;
        let before = sequences(&table).await;
        compact(&table).await;
        assert_eq!(
            state(&table).await,
            vec![(Some(1), Some(10), Some(20))],
            "{format}"
        );
        assert_eq!(sequences(&table).await, before);
        write_batch(&table, &update(Some(2), None, Some(30), 0)).await;
        compact(&table).await;
        assert_eq!(
            state(&table).await,
            vec![(Some(2), Some(10), Some(30))],
            "{format}"
        );
    }
}

#[tokio::test]
async fn partial_update_compaction_keeps_sequence_groups_and_field_aggregate_state() {
    let table = partial(&[
        ("fields.version.sequence-group", "value"),
        ("fields.value.aggregate-function", "sum"),
    ])
    .await;
    write_batch(&table, &update(Some(10), Some(100), Some(1), 0)).await;
    write_batch(&table, &update(Some(9), Some(5), Some(2), 0)).await;
    let expected = state(&table).await;
    assert_eq!(expected, vec![(Some(10), Some(105), Some(2))]);
    compact(&table).await;
    assert_eq!(state(&table).await, expected);
    write_batch(&table, &update(Some(8), Some(7), Some(3), 0)).await;
    assert_eq!(state(&table).await, vec![(Some(10), Some(112), Some(3))]);
    compact(&table).await;
    write_batch(&table, &update(Some(11), Some(2), None, 0)).await;
    assert_eq!(state(&table).await, vec![(Some(11), Some(114), Some(3))]);
}

#[tokio::test]
async fn partial_update_compaction_keeps_sequence_group_delete_and_payload() {
    let table = partial(&[
        ("fields.version.sequence-group", "value,other"),
        ("partial-update.remove-record-on-sequence-group", "version"),
    ])
    .await;
    write_batch(&table, &update(Some(10), Some(100), Some(7), 0)).await;
    write_batch(&table, &update(Some(11), Some(20), None, 3)).await;
    assert!(state(&table).await.is_empty());
    assert_eq!(compact(&table).await.rows_written, 1);
    assert!(state(&table).await.is_empty());
    write_batch(&table, &update(Some(9), Some(999), Some(999), 0)).await;
    assert!(state(&table).await.is_empty());
    compact(&table).await;
    write_batch(&table, &update(Some(12), Some(3), Some(8), 0)).await;
    assert_eq!(state(&table).await, vec![(Some(12), Some(3), Some(8))]);

    let table = partial(&[("partial-update.remove-record-on-delete", "true")]).await;
    write_batch(&table, &update(Some(1), Some(10), Some(7), 0)).await;
    write_batch(&table, &update(None, Some(20), Some(30), 3)).await;
    compact(&table).await;
    assert!(state(&table).await.is_empty());
    write_batch(&table, &update(None, None, Some(40), 0)).await;
    assert_eq!(state(&table).await, vec![(None, Some(20), Some(40))]);
}

#[tokio::test]
async fn aggregation_compaction_keeps_accumulators_for_later_updates() {
    for (function, before, after) in [
        ("sum", 30, 35),
        ("product", 200, 1000),
        ("min", 10, 5),
        ("max", 20, 20),
        ("first_value", 10, 10),
        ("last_value", 20, 5),
    ] {
        let table = table(&[
            ("merge-engine", "aggregation"),
            ("fields.value.aggregate-function", function),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        write_batch(&table, &make_batch(vec![1], vec![20])).await;
        assert_eq!(rows(&table).await, vec![(1, before)]);
        assert_eq!(compact(&table).await.rows_written, 1);
        assert_eq!(rows(&table).await, vec![(1, before)], "{function}");
        write_batch(&table, &make_batch(vec![1], vec![5])).await;
        assert_eq!(rows(&table).await, vec![(1, after)], "{function}");
        compact(&table).await;
        assert_eq!(rows(&table).await, vec![(1, after)], "{function}");
    }
}

#[tokio::test]
async fn aggregation_compaction_keeps_singleton_retract_and_merged_delete_state() {
    let table = table(&[
        ("merge-engine", "aggregation"),
        ("fields.value.aggregate-function", "sum"),
    ])
    .await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(
        &table,
        &make_batch_with_kinds(vec![1, 2], vec![3, 40], vec![3, 3]),
    )
    .await;
    assert_eq!(rows(&table).await, vec![(1, 7)]);
    compact(&table).await;
    assert_eq!(rows(&table).await, vec![(1, 7)]);
    write_batch(&table, &make_batch(vec![1, 2], vec![5, 3])).await;
    assert_eq!(rows(&table).await, vec![(1, 12), (2, -37)]);
    compact(&table).await;
    assert_eq!(rows(&table).await, vec![(1, 12), (2, -37)]);

    let table = super::table(&[
        ("merge-engine", "aggregation"),
        ("fields.value.aggregate-function", "sum"),
        ("aggregation.remove-record-on-delete", "true"),
    ])
    .await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(
        &table,
        &make_batch_with_kinds(vec![1, 2], vec![5, 40], vec![3, 3]),
    )
    .await;
    compact(&table).await;
    assert!(rows(&table).await.is_empty());
    write_batch(&table, &make_batch(vec![1, 2], vec![7, 3])).await;
    assert_eq!(rows(&table).await, vec![(1, 12), (2, 43)]);
}

#[tokio::test]
async fn first_row_legacy_retract_is_rejected_or_ignored_without_leaking_files() {
    for ignore in [false, true] {
        let table = table(&[
            ("merge-engine", "first-row"),
            ("ignore-delete", if ignore { "true" } else { "false" }),
            ("write-buffer-size", "1 B"),
        ])
        .await;
        let legacy = table.copy_with_options(std::collections::HashMap::from([
            ("merge-engine".into(), "deduplicate".into()),
            ("ignore-delete".into(), "false".into()),
        ]));
        write_batch(&legacy, &make_batch(vec![1], vec![10])).await;
        write_batch(&legacy, &make_batch_with_kinds(vec![2], vec![20], vec![3])).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let before = table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len();
        let result = builder
            .new_executor()
            .unwrap()
            .execute(&plan.tasks()[0])
            .await;
        if ignore {
            let result = result.unwrap();
            assert_eq!(result.metrics().rows_written, 1);
            builder
                .new_commit()
                .unwrap()
                .commit_compact(vec![result])
                .await
                .unwrap();
            assert_eq!(rows(&table).await, vec![(1, 10)]);
        } else {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("does not support DELETE"));
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
    }
}
