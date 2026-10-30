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

use super::lookup_changelog_tests::events;
use super::*;
use std::sync::Arc;

fn schema(engine: &str, producer: &str, function: &str, checked: bool) -> TableSchema {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .column("seq", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", if producer == "input" { "-1" } else { "1" })
        .option("merge-engine", engine)
        .option("changelog-producer", producer)
        .option("fields.value.aggregate-function", function);
    if engine == "partial-update" {
        schema = schema.option("fields.seq.sequence-group", "value");
        if producer != "lookup" {
            schema = schema.option("ignore-delete", "true");
        }
    } else {
        schema = schema.option("sequence.field", "seq");
    }
    if checked {
        schema = schema.option("fields.value.sum.fail-on-overflow", "true");
    }
    TableSchema::new(0, &schema.build().unwrap())
}

fn batch(keys: Vec<i32>, value: i32, seq: i32) -> RecordBatch {
    let len = keys.len();
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from(keys)) as arrow_array::ArrayRef,
        ),
        (
            "value",
            Arc::new(Int32Array::from(vec![value; len])) as arrow_array::ArrayRef,
        ),
        (
            "seq",
            Arc::new(Int32Array::from(vec![seq; len])) as arrow_array::ArrayRef,
        ),
    ])
    .unwrap()
}

async fn compact(table: &Table) -> Vec<CompactResult> {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(table, &plan).await;
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results.clone())
        .await
        .unwrap();
    results
}

async fn active_files(table: &Table) -> Vec<paimon::spec::DataFileMeta> {
    table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap()
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .cloned()
        .collect()
}

#[tokio::test]
async fn stateful_lookup_inserts_history_in_sequence_order_and_does_not_apply_it_twice() {
    for engine in ["partial-update", "aggregation"] {
        let table = table_with_schema(schema(engine, "lookup", "first_value", false)).await;
        write_batch(&table, &batch((0..1000).collect(), 20, 20)).await;
        let anchor = compact(&table).await[0].files()[0].clone();
        assert_eq!(anchor.level, 5);
        let start = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
            .unwrap();
        write_batch(&table, &batch(vec![42], 10, 10)).await;
        write_batch(&table, &batch(vec![42], 30, 30)).await;
        let expected = rows(&table).await;
        assert_eq!(expected[42], (42, 10));
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        assert!(plan.tasks()[0].files().iter().all(|file| file.level == 0));
        let results = execute(&table, &plan).await;
        assert_eq!(results[0].metrics().rows_written, 1);
        assert_eq!(results[0].files()[0].level, 4);
        builder
            .new_commit()
            .unwrap()
            .commit_compact(results)
            .await
            .unwrap();
        let end = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            events(&table, start, end).await,
            vec![("+U".into(), 42, 10), ("-U".into(), 42, 20)]
        );
        assert_eq!(rows(&table).await, expected);
        assert!(active_files(&table).await.contains(&anchor));
        write_batch(&table, &batch(vec![42], 25, 25)).await;
        let before_next = rows(&table).await;
        compact(&table).await;
        assert_eq!(rows(&table).await, before_next);
    }
}

#[tokio::test]
async fn stateful_plain_mergeable_fields_leave_history_out_of_the_output_state() {
    for engine in ["partial-update", "aggregation"] {
        for producer in ["none", "input"] {
            for (function, expected_value) in [("sum", 60), ("min", 10), ("max", 30)] {
                let table = table_with_cache(
                    schema(engine, producer, function, false),
                    &[("local-cache.enabled", "false")],
                )
                .await;
                write_batch(&table, &batch((0..1000).collect(), 20, 20)).await;
                let anchor = compact(&table).await[0].files()[0].clone();
                assert_eq!(anchor.level, 5, "{engine}/{producer}/{function}");
                write_batch(&table, &batch(vec![42], 10, 10)).await;
                write_batch(&table, &batch(vec![42], 30, 30)).await;
                let expected = rows(&table).await;
                assert_eq!(expected[42], (42, expected_value));
                let builder = table.new_compact_builder();
                let plan = builder.new_planner().unwrap().plan().await.unwrap();
                assert!(plan.tasks()[0].files().iter().all(|file| file.level == 0));
                let results = execute(&table, &plan).await;
                assert_eq!(results[0].metrics().rows_written, 1);
                builder
                    .new_commit()
                    .unwrap()
                    .commit_compact(results)
                    .await
                    .unwrap();
                assert!(active_files(&table).await.contains(&anchor));
                assert_eq!(
                    rows(&table).await,
                    expected,
                    "{engine}/{producer}/{function}"
                );
                write_batch(&table, &batch(vec![42], 5, 15)).await;
                let next_expected = rows(&table).await;
                compact(&table).await;
                assert_eq!(rows(&table).await, next_expected);
            }
        }
    }
}

#[tokio::test]
async fn stateful_plain_order_sensitive_and_checked_sum_configs_keep_all_history() {
    for engine in ["partial-update", "aggregation"] {
        for producer in ["none", "input"] {
            for (function, checked, initial, earlier, later, expected_value) in [
                ("first_value", false, 20, 10, 30, 10),
                ("sum", true, -i32::MAX, i32::MAX, 1, 1),
            ] {
                let table = table_with_schema(schema(engine, producer, function, checked)).await;
                write_batch(&table, &batch((0..1000).collect(), initial, 20)).await;
                let anchor = compact(&table).await[0].files()[0].clone();
                write_batch(&table, &batch(vec![42], earlier, 10)).await;
                write_batch(&table, &batch(vec![42], later, 30)).await;
                let expected = rows(&table).await;
                assert_eq!(expected[42], (42, expected_value));
                let builder = table.new_compact_builder();
                let plan = builder.new_planner().unwrap().plan().await.unwrap();
                builder
                    .new_commit()
                    .unwrap()
                    .commit_compact(execute(&table, &plan).await)
                    .await
                    .unwrap();
                assert_eq!(rows(&table).await[42], expected[42]);
                assert!(!active_files(&table).await.contains(&anchor));
            }
        }
    }
}

#[tokio::test]
async fn stateful_plain_floating_sum_keeps_history_to_preserve_finite_rounding() {
    for producer in ["none", "input"] {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Double(paimon::spec::DoubleType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", "aggregation")
            .option("changelog-producer", producer)
            .option("fields.value.aggregate-function", "sum")
            .build()
            .unwrap();
        let table = table_with_schema(TableSchema::new(0, &schema)).await;
        let batch = |keys: Vec<i32>, value| {
            let len = keys.len();
            RecordBatch::try_from_iter([
                (
                    "id",
                    Arc::new(Int32Array::from(keys)) as arrow_array::ArrayRef,
                ),
                (
                    "value",
                    Arc::new(arrow_array::Float64Array::from(vec![value; len]))
                        as arrow_array::ArrayRef,
                ),
            ])
            .unwrap()
        };
        write_batch(&table, &batch((0..1000).collect(), 1e20)).await;
        let anchor = compact(&table).await[0].files()[0].clone();
        write_batch(&table, &batch(vec![42], -1e20)).await;
        write_batch(&table, &batch(vec![42], 3.0)).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        builder
            .new_commit()
            .unwrap()
            .commit_compact(execute(&table, &plan).await)
            .await
            .unwrap();
        let batches = read(&table, None).await;
        let actual = batches
            .iter()
            .find_map(|batch| {
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
                    .downcast_ref::<arrow_array::Float64Array>()
                    .unwrap();
                (0..batch.num_rows())
                    .find(|&row| ids.value(row) == 42)
                    .map(|row| values.value(row))
            })
            .unwrap();
        assert_eq!(actual, 3.0);
        assert!(!active_files(&table).await.contains(&anchor));
    }
}

#[tokio::test]
async fn stateful_plain_boolean_fields_are_mergeable_for_both_engines() {
    for engine in ["partial-update", "aggregation"] {
        for producer in ["none", "input"] {
            for (function, initial) in [("bool_and", true), ("bool_or", false)] {
                let mut schema = Schema::builder()
                    .column("id", DataType::Int(IntType::new()))
                    .column("value", DataType::Boolean(paimon::spec::BooleanType::new()))
                    .column("seq", DataType::Int(IntType::new()))
                    .primary_key(["id"])
                    .option("bucket", "1")
                    .option("merge-engine", engine)
                    .option("changelog-producer", producer)
                    .option("fields.value.aggregate-function", function);
                if engine == "partial-update" {
                    schema = schema
                        .option("fields.seq.sequence-group", "value")
                        .option("ignore-delete", "true");
                } else {
                    schema = schema.option("sequence.field", "seq");
                }
                let table = table_with_cache(
                    TableSchema::new(0, &schema.build().unwrap()),
                    &[("local-cache.enabled", "false")],
                )
                .await;
                let batch = |keys: Vec<i32>, value, seq| {
                    let len = keys.len();
                    RecordBatch::try_from_iter([
                        (
                            "id",
                            Arc::new(Int32Array::from(keys)) as arrow_array::ArrayRef,
                        ),
                        (
                            "value",
                            Arc::new(arrow_array::BooleanArray::from(vec![value; len]))
                                as arrow_array::ArrayRef,
                        ),
                        (
                            "seq",
                            Arc::new(Int32Array::from(vec![seq; len])) as arrow_array::ArrayRef,
                        ),
                    ])
                    .unwrap()
                };
                write_batch(&table, &batch((0..1000).collect(), initial, 20)).await;
                let anchor = compact(&table).await[0].files()[0].clone();
                assert_eq!(anchor.level, 5);
                write_batch(&table, &batch(vec![42], !initial, 10)).await;
                write_batch(&table, &batch(vec![42], initial, 30)).await;
                let results = compact(&table).await;
                assert_eq!(results[0].metrics().rows_written, 1);
                assert!(active_files(&table).await.contains(&anchor));
                let mut found = false;
                for batch in read(&table, None).await {
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
                        .downcast_ref::<arrow_array::BooleanArray>()
                        .unwrap();
                    if let Some(row) = (0..batch.num_rows()).find(|&row| ids.value(row) == 42) {
                        assert_eq!(values.value(row), !initial);
                        found = true;
                    }
                }
                assert!(found, "updated key must remain visible");
            }
        }
    }
}

#[tokio::test]
async fn stateful_plain_sparse_overwrite_keeps_interleaved_column_history() {
    for producer in ["none", "input"] {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("other", DataType::Int(IntType::new()))
            .column("seq", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", "partial-update")
            .option("changelog-producer", producer)
            .option("ignore-delete", "true")
            .option("sequence.field", "seq")
            .build()
            .unwrap();
        let table = table_with_schema(TableSchema::new(0, &schema)).await;
        let batch = |keys: Vec<i32>, value: Option<i32>, other: Option<i32>, seq| {
            let len = keys.len();
            RecordBatch::try_from_iter([
                (
                    "id",
                    Arc::new(Int32Array::from(keys)) as arrow_array::ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int32Array::from(vec![value; len])) as arrow_array::ArrayRef,
                ),
                (
                    "other",
                    Arc::new(Int32Array::from(vec![other; len])) as arrow_array::ArrayRef,
                ),
                (
                    "seq",
                    Arc::new(Int32Array::from(vec![seq; len])) as arrow_array::ArrayRef,
                ),
            ])
            .unwrap()
        };
        write_batch(&table, &batch((0..1000).collect(), Some(20), Some(20), 20)).await;
        let anchor = compact(&table).await[0].files()[0].clone();
        write_batch(&table, &batch(vec![42], Some(10), None, 10)).await;
        write_batch(&table, &batch(vec![42], None, Some(30), 30)).await;
        assert_eq!(rows(&table).await[42], (42, 20));
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        builder
            .new_commit()
            .unwrap()
            .commit_compact(execute(&table, &plan).await)
            .await
            .unwrap();
        assert_eq!(rows(&table).await[42], (42, 20));
        assert!(!active_files(&table).await.contains(&anchor));
        let batches = read(&table, None).await;
        assert!(batches.iter().any(|batch| {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let other = batch
                .column_by_name("other")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..batch.num_rows()).any(|row| ids.value(row) == 42 && other.value(row) == 30)
        }));
    }
}

#[tokio::test]
async fn stateful_reset_on_delete_preserves_tombstones_and_only_uses_incremental_lookup() {
    for producer in ["none", "input", "lookup"] {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("seq", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", "aggregation")
            .option("changelog-producer", producer)
            .option("fields.value.aggregate-function", "sum")
            .option("sequence.field", "seq")
            .option("aggregation.remove-record-on-delete", "true")
            .build()
            .unwrap();
        let table = table_with_schema(TableSchema::new(0, &schema)).await;
        write_batch(&table, &batch((0..1000).collect(), 20, 20)).await;
        let anchor = compact(&table).await[0].files()[0].clone();
        let start = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
            .unwrap();
        let before_delete = batch(vec![42], 5, 30);
        let delete = RecordBatch::try_from_iter(
            before_delete
                .schema()
                .fields()
                .iter()
                .zip(before_delete.columns())
                .map(|(field, column)| (field.name().as_str(), column.clone()))
                .chain(std::iter::once((
                    "_VALUE_KIND",
                    Arc::new(arrow_array::Int8Array::from(vec![3])) as arrow_array::ArrayRef,
                ))),
        )
        .unwrap();
        write_batch(&table, &delete).await;
        assert!(!rows(&table).await.iter().any(|(key, _)| *key == 42));
        let deleted = compact(&table).await;
        assert_eq!(
            deleted[0].metrics().rows_written,
            if producer == "lookup" { 1 } else { 1000 }
        );
        assert_eq!(
            active_files(&table).await.contains(&anchor),
            producer == "lookup"
        );
        assert!(!rows(&table).await.iter().any(|(key, _)| *key == 42));
        let end = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
            .unwrap();
        if producer == "lookup" {
            assert_eq!(
                events(&table, start, end).await,
                vec![("-D".into(), 42, 20)]
            );
        }
        write_batch(&table, &batch(vec![42], 7, 40)).await;
        let expected = rows(&table).await;
        assert_eq!(expected[42], (42, 12));
        compact(&table).await;
        assert_eq!(rows(&table).await, expected);
        if producer == "lookup" {
            let final_id = table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                events(&table, end, final_id).await,
                vec![("+I".into(), 42, 12)]
            );
        }
    }
}
