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

use arrow_array::{Array, ArrayRef, Int32Array, Int8Array, RecordBatch};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, Predicate, PredicateBuilder, Schema, TableSchema};
use paimon::table::{MergeAssignment, MergeSource, Table, WhenMatched, WhenNotMatched};
use std::collections::HashMap;
use std::sync::Arc;

type Row = (Option<i32>, Option<i32>, Option<i32>);

async fn table(name: &str, options: &[(&str, &str)], primary_key: bool) -> Table {
    let mut schema = Schema::builder()
        // Engines may declare keys NOT NULL before applying the table option.
        .column("pt", DataType::Int(IntType::with_nullable(!primary_key)))
        .column("id", DataType::Int(IntType::with_nullable(!primary_key)))
        .column("value", DataType::Int(IntType::new()));
    if primary_key {
        schema = schema
            .primary_key(["pt", "id"])
            .partition_keys(["pt"])
            .option("primary-key.nullable", "true");
    } else {
        schema = schema
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option("deletion-vectors.enabled", "true");
    }
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    if options
        .iter()
        .any(|(key, value)| *key == "merge-engine" && *value == "aggregation")
    {
        schema = schema.option("fields.value.aggregate-function", "sum");
    }
    let path = format!("memory:/nullable/{name}");
    let (io, table) = memory_table(&path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, &path).await;
    persist_table_schema(&io, &path, table.schema()).await;
    table
}

fn batch(rows: &[Row]) -> RecordBatch {
    RecordBatch::try_from_iter(["pt", "id", "value"].into_iter().enumerate().map(
        |(column, name)| {
            let values: Int32Array = rows
                .iter()
                .map(|row| match column {
                    0 => row.0,
                    1 => row.1,
                    _ => row.2,
                })
                .collect();
            (name, Arc::new(values) as ArrayRef)
        },
    ))
    .unwrap()
}

async fn read(table: &Table, predicate: Option<Predicate>) -> Vec<Row> {
    let mut builder = table.new_read_builder();
    if let Some(predicate) = predicate {
        builder.with_filter(predicate);
    }
    // First-row source reads include L0, as Python plan_for_write does.
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
    let mut rows = vec![];
    for batch in batches {
        let columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|array| array.as_any().downcast_ref::<Int32Array>().unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            let get =
                |column: usize| (!columns[column].is_null(row)).then(|| columns[column].value(row));
            rows.push((get(0), get(1), get(2)));
        }
    }
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn nullable_composite_keys_merge_across_writer_restarts_and_bucket_modes() {
    for bucket in ["1", "3", "-1"] {
        for engine in ["deduplicate", "partial-update", "aggregation", "first-row"] {
            let table = table(
                &format!("{bucket}-{engine}"),
                &[("bucket", bucket), ("merge-engine", engine)],
                true,
            )
            .await;
            let first = [
                (None, None, Some(10)),
                (Some(1), None, Some(100)),
                (Some(1), Some(0), Some(1)),
            ];
            write_batch(&table, &batch(&first)).await;
            // A new writer must restore dynamic bucket routing for null keys.
            write_batch(
                &table,
                &batch(&[
                    (None, None, Some(20)),
                    (Some(1), None, Some(200)),
                    (None, Some(0), Some(2)),
                ]),
            )
            .await;
            let values = match engine {
                "aggregation" => (30, 300),
                "first-row" => (10, 100),
                _ => (20, 200),
            };
            let expected = vec![
                (None, None, Some(values.0)),
                (None, Some(0), Some(2)),
                (Some(1), None, Some(values.1)),
                (Some(1), Some(0), Some(1)),
            ];
            assert_eq!(read(&table, None).await, expected, "{bucket}/{engine}");
            let pb = PredicateBuilder::new(table.schema().fields());
            assert_eq!(
                read(&table, Some(pb.is_null("id").unwrap())).await,
                vec![expected[0], expected[2]],
                "{bucket}/{engine}"
            );
            assert_eq!(
                read(&table, Some(pb.is_null("pt").unwrap())).await,
                expected[..2],
                "{bucket}/{engine}"
            );
            let first_view = table.copy_with_options(HashMap::from([
                ("scan.mode".into(), "from-snapshot-full".into()),
                ("scan.snapshot-id".into(), "1".into()),
            ]));
            assert_eq!(read(&first_view, None).await, first);
        }
    }
}

#[tokio::test]
async fn null_key_restores_its_nonzero_dynamic_bucket() {
    let table = table(
        "null-nonzero-bucket",
        &[("bucket", "-1"), ("dynamic-bucket.target-row-num", "1")],
        true,
    )
    .await;
    write_batch(
        &table,
        &batch(&[(None, Some(0), Some(100)), (None, None, Some(10))]),
    )
    .await;
    for value in [20, 30] {
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&batch(&[(None, None, Some(value))]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].bucket, 1);
        builder.new_commit().commit(messages).await.unwrap();
    }
    assert_eq!(
        read(&table, None).await,
        vec![(None, None, Some(30)), (None, Some(0), Some(100))]
    );
}

#[tokio::test]
async fn null_key_retract_does_not_delete_zero_or_other_partition_keys() {
    for bucket in ["3", "-1"] {
        let table = table(&format!("delete-{bucket}"), &[("bucket", bucket)], true).await;
        write_batch(
            &table,
            &batch(&[
                (None, None, Some(10)),
                (None, Some(0), Some(20)),
                (Some(1), None, Some(30)),
            ]),
        )
        .await;
        let mut retract = batch(&[(None, None, Some(10))]);
        let mut fields = retract.schema().fields().to_vec();
        fields.push(Arc::new(arrow_schema::Field::new(
            "_VALUE_KIND",
            arrow_schema::DataType::Int8,
            false,
        )));
        let mut columns = retract.columns().to_vec();
        columns.push(Arc::new(Int8Array::from(vec![3])));
        retract =
            RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns).unwrap();
        write_batch(&table, &retract).await;
        assert_eq!(
            read(&table, None).await,
            vec![(None, Some(0), Some(20)), (Some(1), None, Some(30))]
        );
        write_batch(&table, &batch(&[(None, None, Some(40))])).await;
        assert_eq!(
            read(&table, None).await,
            vec![
                (None, None, Some(40)),
                (None, Some(0), Some(20)),
                (Some(1), None, Some(30)),
            ]
        );
    }
}

#[tokio::test]
async fn nullable_primary_key_merge_source_keeps_sql_null_join_semantics() {
    for engine in ["deduplicate", "partial-update", "aggregation", "first-row"] {
        let source = table(
            &format!("source-{engine}"),
            &[("bucket", "-1"), ("merge-engine", engine)],
            true,
        )
        .await;
        write_batch(
            &source,
            &batch(&[
                (None, None, Some(10)),
                (None, Some(1), Some(11)),
                (Some(1), None, Some(12)),
                (Some(1), Some(1), Some(100)),
            ]),
        )
        .await;
        write_batch(
            &source,
            &batch(&[
                (None, None, Some(20)),
                (None, Some(1), Some(21)),
                (Some(1), None, Some(22)),
                (Some(1), Some(1), Some(200)),
            ]),
        )
        .await;
        let target = table(&format!("target-{engine}"), &[], false).await;
        write_batch(
            &target,
            &batch(&[
                (None, None, Some(-1)),
                (None, Some(1), Some(-3)),
                (Some(1), None, Some(-4)),
                (Some(1), Some(1), Some(-2)),
            ]),
        )
        .await;
        let builder = target.new_write_builder();
        let messages = builder
            .new_update()
            .unwrap()
            .merge_into(
                MergeSource::Table(Arc::new(source.clone())),
                vec![("pt".into(), "pt".into()), ("id".into(), "id".into())],
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
                    assignments: ["pt", "id", "value"]
                        .into_iter()
                        .map(|name| (name.into(), MergeAssignment::SourceColumn(name.into())))
                        .collect(),
                }],
            )
            .await
            .unwrap();
        builder.new_commit().commit(messages).await.unwrap();
        let values = match engine {
            "aggregation" => (30, 32, 34, 300),
            "first-row" => (10, 11, 12, 100),
            _ => (20, 21, 22, 200),
        };
        // PK merging is null-safe, but MERGE's SQL equality must not match nulls.
        assert_eq!(
            read(&target, None).await,
            vec![
                (None, None, Some(-1)),
                (None, None, Some(values.0)),
                (None, Some(1), Some(-3)),
                (None, Some(1), Some(values.1)),
                (Some(1), None, Some(-4)),
                (Some(1), None, Some(values.2)),
                (Some(1), Some(1), Some(values.3)),
            ]
        );
        assert_eq!(
            source
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap()
                .id(),
            2
        );
    }
}
