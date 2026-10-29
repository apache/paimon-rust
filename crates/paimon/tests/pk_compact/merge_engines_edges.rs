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

async fn assert_unsafe_aggregate_compaction_rejected(table: &Table) {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let before = table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .len();
    let error = builder
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Compaction cannot preserve"),
        "{error}"
    );
    assert_eq!(
        table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len(),
        before
    );
}

#[tokio::test]
async fn partial_update_retract_only_aggregate_state_refuses_lossy_compaction() {
    for aggregate_option in [
        "fields.value.aggregate-function",
        "fields.default-aggregate-function",
    ] {
        let options = [
            ("fields.version.sequence-group", "value,other"),
            (aggregate_option, "sum"),
        ];
        let control = partial(&options).await;
        let rewritten = partial(&options).await;
        for table in [&control, &rewritten] {
            write_batch(table, &update(Some(10), Some(10), None, 3)).await;
            write_batch(table, &update(Some(11), Some(5), None, 3)).await;
        }
        let builder = rewritten.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let paths_before = rewritten
            .file_io()
            .list_status_recursive(rewritten.location())
            .await
            .unwrap();
        let error = builder
            .new_executor()
            .unwrap()
            .execute(&plan.tasks()[0])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("DELETE aggregate state"));
        assert_eq!(
            rewritten
                .file_io()
                .list_status_recursive(rewritten.location())
                .await
                .unwrap()
                .len(),
            paths_before.len()
        );
        for table in [&control, &rewritten] {
            write_batch(table, &update(Some(12), Some(3), None, 0)).await;
        }
        assert_eq!(state(&control).await, vec![(Some(12), Some(-2), None)]);
        assert_eq!(state(&rewritten).await, state(&control).await);
    }
}

#[tokio::test]
async fn partial_update_aggregate_delete_state_refuses_lossy_compaction() {
    let options = [
        ("fields.version.sequence-group", "value,other"),
        ("partial-update.remove-record-on-sequence-group", "version"),
        ("fields.value.aggregate-function", "first_value"),
    ];
    let control = partial(&options).await;
    let rewritten = partial(&options).await;
    for table in [&control, &rewritten] {
        write_batch(table, &update(Some(1), Some(10), None, 0)).await;
        write_batch(table, &update(Some(2), Some(20), None, 3)).await;
    }
    assert_unsafe_aggregate_compaction_rejected(&rewritten).await;
    for table in [&control, &rewritten] {
        write_batch(table, &update(Some(3), Some(30), None, 0)).await;
    }
    assert_eq!(state(&control).await, vec![(Some(3), Some(20), None)]);
    assert_eq!(state(&rewritten).await, state(&control).await);
}

#[tokio::test]
async fn aggregation_delete_state_refuses_lossy_compaction() {
    let options = [
        ("merge-engine", "aggregation"),
        ("fields.value.aggregate-function", "first_value"),
        ("aggregation.remove-record-on-delete", "true"),
    ];
    let control = table(&options).await;
    let rewritten = table(&options).await;
    for table in [&control, &rewritten] {
        write_batch(table, &make_batch(vec![1], vec![10])).await;
        write_batch(table, &make_batch_with_kinds(vec![1], vec![20], vec![3])).await;
    }
    assert_unsafe_aggregate_compaction_rejected(&rewritten).await;
    for table in [&control, &rewritten] {
        write_batch(table, &make_batch(vec![1], vec![30])).await;
    }
    assert_eq!(rows(&control).await, vec![(1, 20)]);
    assert_eq!(rows(&rewritten).await, rows(&control).await);
}

#[tokio::test]
async fn first_non_null_aggregate_rejects_retract_state_even_when_output_is_insert() {
    for function in ["first_non_null_value", "first_not_null_value"] {
        let table = table(&[
            ("merge-engine", "aggregation"),
            ("fields.value.aggregate-function", function),
            ("aggregation.remove-record-on-delete", "true"),
        ])
        .await;
        write_batch(&table, &make_batch_with_kinds(vec![1], vec![20], vec![3])).await;
        let null_insert = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
            ),
            (
                "value",
                Arc::new(Int32Array::from(vec![None])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        write_batch(&table, &null_insert).await;
        assert_eq!(rows(&table).await, vec![(1, 20)]);
        assert_unsafe_aggregate_compaction_rejected(&table).await;
        write_batch(&table, &make_batch(vec![1], vec![30])).await;
        assert_eq!(rows(&table).await, vec![(1, 30)]);
    }
}

#[tokio::test]
async fn aggregation_replay_guard_uses_effective_functions() {
    for ignored in [
        "fields.default-aggregate-function",
        "fields.id.aggregate-function",
    ] {
        let table = table(&[
            ("merge-engine", "aggregation"),
            (ignored, "first_value"),
            ("fields.value.aggregate-function", "sum"),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        write_batch(&table, &make_batch_with_kinds(vec![1], vec![3], vec![1])).await;
        compact(&table).await;
        assert_eq!(rows(&table).await, vec![(1, 7)]);
        write_batch(&table, &make_batch(vec![1], vec![5])).await;
        assert_eq!(rows(&table).await, vec![(1, 12)]);
    }
}

#[tokio::test]
async fn listagg_rejects_raw_delete_state_that_an_insert_would_normalize() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::VarChar(VarCharType::string_type()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "aggregation")
        .option("fields.value.aggregate-function", "listagg")
        .option("aggregation.remove-record-on-delete", "true")
        .build()
        .unwrap();
    let table = table_with_schema(TableSchema::new(0, &schema)).await;
    let batch = |value: Option<&str>, kind: i8| {
        RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
            ),
            (
                "value",
                Arc::new(StringArray::from(vec![value])) as arrow_array::ArrayRef,
            ),
            (
                "_VALUE_KIND",
                Arc::new(Int8Array::from(vec![kind])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap()
    };
    write_batch(&table, &batch(Some(" "), 3)).await;
    write_batch(&table, &batch(None, 0)).await;
    assert_unsafe_aggregate_compaction_rejected(&table).await;
    write_batch(&table, &batch(None, 0)).await;
    let output = read(&table, None).await;
    assert_eq!(
        output[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        " "
    );
}

#[tokio::test]
async fn partial_update_replay_guard_allows_ignored_legacy_retracts() {
    let table = partial(&[
        ("fields.version.sequence-group", "value,other"),
        ("fields.value.aggregate-function", "first_value"),
        ("ignore-delete", "true"),
    ])
    .await;
    write_batch(&table, &update(Some(1), Some(10), None, 0)).await;
    let legacy = table.copy_with_options(std::collections::HashMap::from([(
        "ignore-delete".into(),
        "false".into(),
    )]));
    write_batch(&legacy, &update(Some(2), Some(20), None, 3)).await;
    compact(&table).await;
    assert_eq!(state(&table).await, vec![(Some(1), Some(10), None)]);
    write_batch(&table, &update(Some(3), Some(30), None, 0)).await;
    assert_eq!(state(&table).await, vec![(Some(3), Some(10), None)]);
}

#[tokio::test]
async fn ignored_legacy_retracts_can_compact_to_empty_and_accept_new_records() {
    for engine in ["first-row", "partial-update"] {
        let table = table(&[("merge-engine", engine), ("ignore-delete", "true")]).await;
        let legacy = table.copy_with_options(std::collections::HashMap::from([
            ("merge-engine".into(), "deduplicate".into()),
            ("ignore-delete".into(), "false".into()),
        ]));
        for kind in [1, 3] {
            write_batch(
                &legacy,
                &make_batch_with_kinds(vec![1], vec![10], vec![kind]),
            )
            .await;
        }
        assert!(all_rows(&table).await.is_empty());
        let metrics = compact(&table).await;
        assert_eq!(
            (
                metrics.files_removed,
                metrics.files_added,
                metrics.rows_written
            ),
            (2, 0, 0),
            "{engine}"
        );
        assert!(rows(&table).await.is_empty());
        write_batch(&table, &make_batch(vec![1], vec![20])).await;
        compact(&table).await;
        assert_eq!(rows(&table).await, vec![(1, 20)], "{engine}");
    }
}

#[tokio::test]
async fn aggregation_compaction_keeps_listagg_state_across_formats() {
    for format in ["parquet", "orc", "avro", "row"] {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", "aggregation")
            .option("fields.value.aggregate-function", "listagg")
            .option("fields.value.list-agg-delimiter", "|")
            .option("file.format", format)
            .option("file.compression", "none")
            .build()
            .unwrap();
        let table = table_with_schema(TableSchema::new(0, &schema)).await;
        let batch = |text: &str| {
            RecordBatch::try_new(
                Arc::new(arrow_schema::Schema::new(vec![
                    arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
                    arrow_schema::Field::new("value", arrow_schema::DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1])),
                    Arc::new(StringArray::from(vec![text])),
                ],
            )
            .unwrap()
        };
        write_batch(&table, &batch("a")).await;
        write_batch(&table, &batch("b")).await;
        compact(&table).await;
        write_batch(&table, &batch("c")).await;
        compact(&table).await;
        let result = read(&table, None).await;
        assert_eq!(
            result.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        assert_eq!(
            result[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a|b|c",
            "{format}"
        );
    }
}

#[tokio::test]
async fn aggregation_unsupported_retract_aborts_already_written_outputs() {
    let table = table(&[
        ("merge-engine", "aggregation"),
        ("fields.value.aggregate-function", "min"),
        ("write-buffer-size", "1 B"),
        ("read.batch-size", "1"),
    ])
    .await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    write_batch(
        &table,
        &make_batch_with_kinds(vec![1, 2], vec![11, 5], vec![0, 3]),
    )
    .await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let before = table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .len();
    let error = builder
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("does not support retract"),
        "{error}"
    );
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
