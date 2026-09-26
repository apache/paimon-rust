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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, Schema, TableSchema};
use paimon::table::{CommitMessage, Table};

use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};

async fn evolution_table() -> Table {
    let path = "memory:/table_update";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .column("score", DataType::Int(IntType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("deletion-vectors.enabled", "true")
        .build()
        .unwrap();
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

fn batch(columns: &[(&str, Vec<i32>)]) -> RecordBatch {
    RecordBatch::try_from_iter(columns.iter().map(|(name, values)| {
        (
            *name,
            Arc::new(Int32Array::from(values.clone())) as ArrayRef,
        )
    }))
    .unwrap()
}

fn matched(row_ids: Vec<i64>, columns: &[(&str, Vec<i32>)]) -> RecordBatch {
    let input = batch(columns);
    let mut named = vec![("_ROW_ID", Arc::new(Int64Array::from(row_ids)) as ArrayRef)];
    let schema = input.schema();
    named.extend(
        schema
            .fields()
            .iter()
            .zip(input.columns())
            .map(|(field, array)| (field.name().as_str(), array.clone())),
    );
    RecordBatch::try_from_iter(named).unwrap()
}

async fn seed(table: &Table) {
    write_batch(
        table,
        &batch(&[
            ("id", vec![1, 2, 3]),
            ("value", vec![10, 20, 30]),
            ("score", vec![100, 200, 300]),
        ]),
    )
    .await;
}

async fn commit(table: &Table, messages: Vec<CommitMessage>) {
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
}

async fn read_rows(table: &Table) -> Vec<Vec<i32>> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let arrays: Vec<_> = batch
            .columns()
            .iter()
            .map(|array| array.as_any().downcast_ref::<Int32Array>().unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            rows.push(arrays.iter().map(|array| array.value(row)).collect());
        }
    }
    rows.sort();
    rows
}

async fn parquet_files(table: &Table) -> Vec<String> {
    let mut paths: Vec<_> = table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .into_iter()
        .map(|status| status.path)
        .filter(|path| path.ends_with(".parquet"))
        .collect();
    paths.sort();
    paths
}

#[tokio::test]
async fn row_id_update_infers_columns_and_reuses_configuration() {
    let table = evolution_table().await;
    seed(&table).await;
    let mut update = table.new_write_builder().new_update().unwrap();
    let messages = update
        .update_by_arrow_with_row_id(vec![
            matched(vec![0], &[("value", vec![11])]),
            matched(vec![1], &[("value", vec![22])]),
        ])
        .await
        .unwrap();
    commit(&table, messages).await;
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 11, 100], vec![2, 22, 200], vec![3, 30, 300]]
    );

    update
        .with_update_type(vec!["score".into(), "score".into()])
        .unwrap();
    assert!(update
        .with_update_type(vec!["value".into(), "missing".into()])
        .is_err());
    // Invalid configuration must leave the previous selection intact. The
    // low-level factory inherits that selection, ignoring the extra column.
    let mut writer = update.new_update_by_row_id().unwrap();
    writer
        .add_matched_batch(matched(
            vec![0],
            &[("value", vec![999]), ("score", vec![101])],
        ))
        .unwrap();
    commit(&table, writer.prepare_commit().await.unwrap()).await;

    // Selecting the full schema restores per-input inference, like Python.
    update
        .with_update_type(vec!["score".into(), "id".into(), "value".into()])
        .unwrap();
    commit(
        &table,
        update
            .update_by_arrow_with_row_id(vec![matched(vec![2], &[("score", vec![303])])])
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 11, 101], vec![2, 22, 200], vec![3, 30, 303]]
    );
}

#[tokio::test]
async fn grouped_updates_allow_disjoint_columns_on_the_same_file() {
    let table = evolution_table().await;
    seed(&table).await;
    let update = table.new_write_builder().new_update().unwrap();
    let messages = update
        .update_by_arrow_batches_with_row_id(vec![
            vec![
                matched(vec![0], &[("value", vec![11])]),
                matched(vec![1], &[("value", vec![22])]),
            ],
            vec![matched(vec![0, 2], &[("score", vec![101, 303])])],
        ])
        .await
        .unwrap();
    commit(&table, messages).await;
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 11, 101], vec![2, 22, 200], vec![3, 30, 303]]
    );
}

#[tokio::test]
async fn overlapping_groups_abort_previously_prepared_columns() {
    let table = evolution_table().await;
    seed(&table).await;
    let before = parquet_files(&table).await;
    let update = table.new_write_builder().new_update().unwrap();
    // The score writer succeeds before the value writer encounters different
    // logical inputs touching the same file, even though their row IDs differ.
    let error = update
        .update_by_arrow_batches_with_row_id(vec![
            vec![matched(
                vec![0],
                &[("score", vec![101]), ("value", vec![11])],
            )],
            vec![matched(vec![2], &[("value", vec![33])])],
        ])
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("overlapping first_row_ids by column"),
        "{error}"
    );
    assert_eq!(parquet_files(&table).await, before);
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 10, 100], vec![2, 20, 200], vec![3, 30, 300]]
    );
}

#[tokio::test]
async fn row_id_update_rejects_inconsistent_chunks_and_empty_selection() {
    let table = evolution_table().await;
    seed(&table).await;
    let before = parquet_files(&table).await;
    let mut update = table.new_write_builder().new_update().unwrap();
    let error = update
        .update_by_arrow_with_row_id(vec![
            matched(vec![0], &[("value", vec![11])]),
            matched(vec![1], &[("score", vec![202])]),
        ])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("must have the same columns"));
    let error = update
        .update_by_arrow_with_row_id(vec![batch(&[("value", vec![])])])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("_ROW_ID"));
    update.with_update_type(vec![]).unwrap();
    let error = update
        .update_by_arrow_with_row_id(vec![matched(vec![], &[("value", vec![])])])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("column_names cannot be empty"));
    assert!(update
        .update_by_arrow_batches_with_row_id(vec![])
        .await
        .unwrap()
        .is_empty());
    assert_eq!(parquet_files(&table).await, before);
}

#[tokio::test]
async fn grouped_updates_on_an_empty_table_preserve_empty_input_semantics() {
    let table = evolution_table().await;
    let update = table.new_write_builder().new_update().unwrap();
    assert!(update
        .update_by_arrow_batches_with_row_id(vec![
            vec![matched(vec![], &[("value", vec![])])],
            vec![matched(vec![], &[("score", vec![])])],
        ])
        .await
        .unwrap()
        .is_empty());
    let error = update
        .update_by_arrow_batches_with_row_id(vec![
            vec![matched(vec![0], &[("value", vec![11])])],
            vec![matched(vec![0], &[("score", vec![101])])],
        ])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("No files with row tracking"));
    assert!(parquet_files(&table).await.is_empty());
}

#[tokio::test]
async fn configured_upsert_and_delete_are_core_operations() {
    let table = evolution_table().await;
    write_batch(
        &table,
        &batch(&[
            ("id", vec![1, 1, 2]),
            ("value", vec![10, 11, 20]),
            ("score", vec![100, 101, 200]),
        ]),
    )
    .await;
    let builder = table
        .new_write_builder()
        .with_commit_user("core-update")
        .unwrap();
    let mut update = builder.new_update().unwrap();
    update.with_update_type(vec!["value".into()]).unwrap();
    let messages = update
        .upsert_by_arrow_with_key(
            vec![batch(&[
                ("id", vec![1, 3]),
                ("value", vec![12, 30]),
                ("score", vec![999, 300]),
            ])],
            vec!["id".into()],
        )
        .await
        .unwrap();
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        read_rows(&table).await,
        vec![
            vec![1, 12, 100],
            vec![1, 12, 101],
            vec![2, 20, 200],
            vec![3, 30, 300]
        ]
    );
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.commit_user(), "core-update");

    let messages = update.delete_by_row_id(vec![0, 0, 2]).await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 12, 101], vec![3, 30, 300]]
    );
    assert!(update.delete_by_row_id(vec![]).await.unwrap().is_empty());
}

#[tokio::test]
async fn update_builder_rejects_overwrite_and_time_travel() {
    let table = evolution_table().await;
    assert!(table
        .new_write_builder()
        .with_overwrite()
        .new_update()
        .is_err());
    let historical =
        table.copy_with_options(HashMap::from([("scan.snapshot-id".into(), "1".into())]));
    assert!(historical.new_write_builder().new_update().is_err());
}
