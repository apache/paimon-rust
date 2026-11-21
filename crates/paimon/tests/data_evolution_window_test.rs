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

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use paimon::spec::{DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema};
use std::collections::HashMap;
use std::sync::Arc;

use common::incremental_helpers::{
    make_batch, memory_table, persist_table_schema, setup_dirs, write_batch,
};

fn rows(batches: &[RecordBatch]) -> Vec<(Option<i32>, Option<i32>)> {
    let column = |batch: &RecordBatch, name: &str| {
        let column = batch.column_by_name(name).unwrap();
        column
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone()
    };
    let mut rows = Vec::new();
    for batch in batches {
        let (ids, values) = (column(batch, "id"), column(batch, "value"));
        for row in 0..batch.num_rows() {
            rows.push((
                ids.is_valid(row).then(|| ids.value(row)),
                values.is_valid(row).then(|| values.value(row)),
            ));
        }
    }
    rows.sort();
    rows
}

/// Writes `value` of rows `0..values.len()` as a column slice of their files.
async fn update_values(table: &paimon::table::Table, values: Vec<i32>) {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("_ROW_ID", ArrowDataType::Int64, false),
        Field::new("value", ArrowDataType::Int32, false),
    ]));
    let row_ids = (0..values.len() as i64).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(row_ids)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder
        .new_data_evolution_writer(vec!["value".to_string()])
        .unwrap();
    writer.add_matched_batch(batch).unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();
}

async fn compact(table: &paimon::table::Table) {
    let compaction = table.new_compact_builder();
    let plan = compaction.new_planner().unwrap().plan().await.unwrap();
    let executor = compaction.new_executor().unwrap();
    let mut results = Vec::new();
    for task in plan.tasks() {
        results.push(executor.execute(task).await.unwrap());
    }
    compaction
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
}

/// Reads the table as the changes of the window of every snapshot.
async fn read_window(table: &paimon::table::Table) -> paimon::Result<Vec<RecordBatch>> {
    let options = HashMap::from([(
        "incremental-between-timestamp".to_string(),
        "0,9999999999999".to_string(),
    )]);
    let table = table.copy_with_time_travel(options).await?;
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await?;
    builder
        .new_read()?
        .to_arrow(plan.splits())?
        .try_collect()
        .await
}

fn data_evolution_table(path: &str) -> (paimon::io::FileIO, paimon::table::Table) {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("write-only", "true")
        .option("compaction.min.file-num", "2")
        .build()
        .unwrap();
    memory_table(path, TableSchema::new(0, &schema))
}

/// A column update of rows an insert wrote is a slice of the insert's rows, and
/// the window of both merges them into the updated rows.
#[tokio::test]
async fn window_merges_a_column_update_with_the_insert_it_updates() {
    let path = "memory:/data_evolution_window/slices";
    let (io, table) = data_evolution_table(path);
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    update_values(&table, vec![99, 20]).await;

    let batches = read_window(&table).await.unwrap();
    assert_eq!(
        rows(&batches),
        vec![(Some(1), Some(99)), (Some(2), Some(20))]
    );
}

/// A compaction merges the files of two inserts into one, so an update of every
/// row covers that file and not the rows of either insert. Their files are not
/// column slices of one row range, and the window returns them as they are
/// stored instead of failing the read.
#[tokio::test]
async fn window_reads_files_that_are_not_column_slices_as_stored() {
    let path = "memory:/data_evolution_window/compacted";
    let (io, table) = data_evolution_table(path);
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![1, 2])).await;
    write_batch(&table, &make_batch(vec![3, 4], vec![3, 4])).await;
    compact(&table).await;
    update_values(&table, vec![50; 4]).await;

    let batches = read_window(&table).await.unwrap();
    assert_eq!(
        rows(&batches),
        vec![
            (None, Some(50)),
            (None, Some(50)),
            (None, Some(50)),
            (None, Some(50)),
            (Some(1), Some(1)),
            (Some(2), Some(2)),
            (Some(3), Some(3)),
            (Some(4), Some(4)),
        ]
    );
}

/// Projection pruning must not drop an update-only file from a physical-event
/// group: its absent projected fields are read as NULL like the other columns.
#[tokio::test]
async fn window_reads_physical_events_with_projection() {
    let path = "memory:/data_evolution_window/projection";
    let (io, table) = data_evolution_table(path);
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![1, 2])).await;
    write_batch(&table, &make_batch(vec![3, 4], vec![3, 4])).await;
    compact(&table).await;
    update_values(&table, vec![50; 4]).await;

    let options = HashMap::from([(
        "incremental-between-timestamp".to_string(),
        "0,9999999999999".to_string(),
    )]);
    let table = table.copy_with_time_travel(options).await.unwrap();
    let mut builder = table.new_read_builder();
    builder.with_projection(&["id"]).unwrap();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    ids.sort();
    assert_eq!(
        ids,
        vec![None, None, None, None, Some(1), Some(2), Some(3), Some(4),]
    );
}

/// Merged latest-row statistics must not prune physical events whose stored
/// values satisfy the predicate.
#[tokio::test]
async fn window_filters_physical_events_by_stored_values() {
    let path = "memory:/data_evolution_window/filter";
    let (io, table) = data_evolution_table(path);
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![1, 2])).await;
    write_batch(&table, &make_batch(vec![3, 4], vec![3, 4])).await;
    compact(&table).await;
    update_values(&table, vec![50; 4]).await;

    let options = HashMap::from([(
        "incremental-between-timestamp".to_string(),
        "0,9999999999999".to_string(),
    )]);
    let table = table.copy_with_time_travel(options).await.unwrap();
    let mut builder = table.new_read_builder();
    builder.with_filter(
        PredicateBuilder::new(table.schema().fields())
            .equal("value", Datum::Int(1))
            .unwrap(),
    );
    let plan = builder.new_scan().plan().await.unwrap();
    let batches = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows(&batches), vec![(Some(1), Some(1))]);
}
