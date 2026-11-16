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

//! Primary-key reads of a split whose sections each hold one sorted run.

mod common;

use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::spec::{DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema};
use paimon::table::Table;

async fn pk_table() -> Table {
    pk_table_with("memory:/pk_single_run_read", &[]).await
}

async fn pk_table_with(path: &str, options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("v", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1");
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    let schema = schema.build().unwrap();
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

async fn write(table: &Table, ids: std::ops::Range<i32>, offset: i32) {
    let ids: Vec<i32> = ids.collect();
    let values: Vec<i32> = ids.iter().map(|id| id + offset).collect();
    write_batch(
        table,
        &RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
            ("v", Arc::new(Int32Array::from(values)) as ArrayRef),
        ])
        .unwrap(),
    )
    .await;
}

async fn read(
    table: &Table,
    projection: &[&str],
    filter: Option<paimon::spec::Predicate>,
) -> Vec<Vec<i32>> {
    let mut builder = table.new_read_builder();
    builder.with_projection(projection).unwrap();
    if let Some(filter) = filter {
        builder.with_filter(filter);
    }
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows: Vec<Vec<i32>> = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                (0..batch.num_columns())
                    .map(|col| {
                        batch
                            .column(col)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .value(row)
                    })
                    .collect()
            })
        })
        .collect();
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn disjoint_files_read_without_merge_and_overlaps_still_merge() {
    let table = pk_table().await;
    // Three files with disjoint keys land in one split, one run per section.
    write(&table, 0..100, 1000).await;
    write(&table, 100..200, 1000).await;
    write(&table, 200..300, 1000).await;
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    assert_eq!(plan.splits().len(), 1);

    let expected: Vec<Vec<i32>> = (0..300).map(|id| vec![id, id + 1000]).collect();
    assert_eq!(read(&table, &["id", "v"], None).await, expected);
    // Value-only projection and a residual filter on the unmerged path.
    let pb = PredicateBuilder::new(table.schema().fields());
    assert_eq!(
        read(
            &table,
            &["v"],
            Some(pb.greater_or_equal("v", Datum::Int(1250)).unwrap())
        )
        .await,
        (1250..1300).map(|v| vec![v]).collect::<Vec<_>>()
    );

    // Overlapping keys make a two-run section, which still merges.
    write(&table, 150..160, 2000).await;
    let expected: Vec<Vec<i32>> = (0..300)
        .map(|id| vec![id, id + if (150..160).contains(&id) { 2000 } else { 1000 }])
        .collect();
    assert_eq!(read(&table, &["id", "v"], None).await, expected);
}

#[tokio::test]
async fn aggregation_reads_disjoint_files_without_merge_and_overlaps_still_merge() {
    let table = pk_table_with(
        "memory:/pk_single_run_read_aggregation",
        &[
            ("merge-engine", "aggregation"),
            ("fields.v.aggregate-function", "sum"),
        ],
    )
    .await;
    write(&table, 0..100, 1000).await;
    write(&table, 100..200, 1000).await;
    let expected: Vec<Vec<i32>> = (0..200).map(|id| vec![id, id + 1000]).collect();
    assert_eq!(read(&table, &["id", "v"], None).await, expected);

    // An overlapping file makes a two-run section, which still sums.
    write(&table, 150..160, 2000).await;
    let expected: Vec<Vec<i32>> = (0..200)
        .map(|id| {
            let v = id + 1000;
            vec![
                id,
                if (150..160).contains(&id) {
                    v + id + 2000
                } else {
                    v
                },
            ]
        })
        .collect();
    assert_eq!(read(&table, &["id", "v"], None).await, expected);
}

async fn write_nullable(table: &Table, columns: &[(&str, Vec<Option<i32>>)]) {
    write_batch(
        table,
        &RecordBatch::try_from_iter(columns.iter().map(|(name, values)| {
            (
                *name,
                Arc::new(Int32Array::from(values.clone())) as ArrayRef,
            )
        }))
        .unwrap(),
    )
    .await;
}

#[tokio::test]
async fn partial_update_reads_disjoint_files_without_merge_and_overlaps_still_merge() {
    let table = pk_table_with(
        "memory:/pk_single_run_read_partial_update",
        &[("merge-engine", "partial-update")],
    )
    .await;
    write(&table, 0..100, 1000).await;
    write(&table, 100..200, 1000).await;
    let expected: Vec<Vec<i32>> = (0..200).map(|id| vec![id, id + 1000]).collect();
    assert_eq!(read(&table, &["id", "v"], None).await, expected);

    // An overlapping file with a null `v` makes a two-run section, which still
    // merges and keeps the earlier `v`; a non-null `v` replaces it.
    write_nullable(
        &table,
        &[
            ("id", (150..160).map(Some).collect()),
            (
                "v",
                (150..160)
                    .map(|id| (id >= 155).then_some(id + 2000))
                    .collect(),
            ),
        ],
    )
    .await;
    let expected: Vec<Vec<i32>> = (0..200)
        .map(|id| vec![id, id + if (155..160).contains(&id) { 2000 } else { 1000 }])
        .collect();
    assert_eq!(read(&table, &["id", "v"], None).await, expected);
}

#[tokio::test]
async fn partial_update_sequence_group_reads_single_runs_without_its_sequence_field() {
    // `g` orders `v`, so a read of `id, v` also reads `g` and drops it again.
    let path = "memory:/pk_single_run_read_sequence_group";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("v", DataType::Int(IntType::new()))
        .column("g", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "partial-update")
        .option("fields.g.sequence-group", "v")
        .build()
        .unwrap();
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    let rows = |ids: std::ops::Range<i32>, v: i32, g: i32| {
        [
            ("id", ids.clone().map(Some).collect::<Vec<_>>()),
            ("v", ids.clone().map(|id| Some(id + v)).collect()),
            ("g", ids.map(|_| Some(g)).collect()),
        ]
    };
    write_nullable(&table, &rows(0..100, 1000, 5)).await;
    write_nullable(&table, &rows(100..200, 1000, 5)).await;
    let expected: Vec<Vec<i32>> = (0..200).map(|id| vec![id, id + 1000]).collect();
    assert_eq!(read(&table, &["id", "v"], None).await, expected);
    assert_eq!(
        read(&table, &["v", "g"], None).await,
        (0..200).map(|id| vec![id + 1000, 5]).collect::<Vec<_>>()
    );

    // Overlapping files still merge: an older `g` leaves `v` alone, a newer one
    // replaces it.
    write_nullable(&table, &rows(150..155, 2000, 4)).await;
    write_nullable(&table, &rows(155..160, 3000, 6)).await;
    let expected: Vec<Vec<i32>> = (0..200)
        .map(|id| vec![id, id + if (155..160).contains(&id) { 3000 } else { 1000 }])
        .collect();
    assert_eq!(read(&table, &["id", "v"], None).await, expected);
}
