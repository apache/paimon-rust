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

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{DataType, Datum, IntType, Schema, TableSchema};
use paimon::table::{BranchManager, CommitMessage, Table};

fn batch(extra: bool, values: &[[i32; 4]]) -> RecordBatch {
    let names = if extra {
        &["id", "pt", "value", "extra"][..]
    } else {
        &["id", "pt", "value"][..]
    };
    RecordBatch::try_from_iter(names.iter().enumerate().map(|(index, name)| {
        (
            *name,
            Arc::new(Int32Array::from(
                values.iter().map(|row| row[index]).collect::<Vec<_>>(),
            )) as ArrayRef,
        )
    }))
    .unwrap()
}

async fn write(table: &Table, data: RecordBatch, overwrite: bool) -> Vec<CommitMessage> {
    let builder = table.new_write_builder();
    let builder = if overwrite {
        builder.with_overwrite()
    } else {
        builder
    };
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&data).await.unwrap();
    writer.prepare_commit().await.unwrap()
}

async fn publish(table: &Table, messages: Vec<CommitMessage>) {
    table
        .new_write_builder()
        .try_new_commit()
        .unwrap()
        .commit(messages)
        .await
        .unwrap();
}

async fn rows(table: &Table) -> Vec<Vec<Option<i32>>> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<_> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        let column = column.as_any().downcast_ref::<Int32Array>().unwrap();
                        column.is_valid(row).then(|| column.value(row))
                    })
                    .collect::<Vec<_>>(),
            );
        }
    }
    rows.sort();
    rows
}

async fn evolved(path: &str, mode: &str) -> (Table, Table, Vec<CommitMessage>) {
    let schema = |extra| {
        let mut builder = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("pt", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .partition_keys(["pt"]);
        if mode == "pk" {
            builder = builder.primary_key(["id", "pt"]).option("bucket", "1");
        } else if mode == "de" {
            builder = builder
                .option("data-evolution.enabled", "true")
                .option("row-tracking.enabled", "true");
        }
        if extra {
            builder = builder.column("extra", DataType::Int(IntType::new()));
        }
        TableSchema::new(i64::from(extra), &builder.build().unwrap())
    };
    let (io, original) = memory_table(path, schema(false));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, original.schema()).await;
    publish(
        &original,
        write(&original, batch(false, &[[1, 1, 10, 0]]), false).await,
    )
    .await;
    let latest_schema = schema(true);
    persist_table_schema(&io, path, &latest_schema).await;
    let latest = original
        .copy_with_resolved_schema(latest_schema, "main")
        .unwrap();
    let mut values = vec![[2, 1, 20, 200], [4, 2, 40, 400]];
    if mode == "pk" {
        values.push([1, 1, 25, 250]);
    }
    let messages = write(&latest, batch(true, &values), false).await;
    publish(&latest, messages.clone()).await;
    let historical = latest
        .copy_with_time_travel(HashMap::from([("scan.snapshot-id".into(), "1".into())]))
        .await
        .unwrap();
    assert_eq!(historical.schema().id(), 0);
    assert_eq!(
        rows(&historical).await,
        vec![vec![Some(1), Some(1), Some(10)]]
    );
    (latest, historical, messages)
}

#[tokio::test]
async fn historical_writers_and_committers_use_latest_publication_state() {
    for mode in ["append", "de", "pk"] {
        for operation in ["append", "commit", "overwrite", "truncate", "partitions"] {
            let (latest, historical, _) = evolved(
                &format!("memory:/historical_write_{mode}_{operation}"),
                mode,
            )
            .await;
            let mut expected = rows(&latest).await;
            if operation == "truncate" {
                historical
                    .new_write_builder()
                    .try_new_commit()
                    .unwrap()
                    .truncate_table()
                    .await
                    .unwrap();
                expected.clear();
            } else if operation == "partitions" {
                historical
                    .new_write_builder()
                    .try_new_commit()
                    .unwrap()
                    .truncate_partitions(vec![HashMap::from([("pt".into(), Some(Datum::Int(1)))])])
                    .await
                    .unwrap();
                expected.retain(|row| row[1] != Some(1));
            } else {
                let current_input = operation == "commit";
                let messages = write(
                    if current_input { &latest } else { &historical },
                    batch(current_input, &[[3, 1, 30, 300]]),
                    operation == "overwrite",
                )
                .await;
                assert!(messages
                    .iter()
                    .flat_map(|message| &message.new_files)
                    .all(|file| file.schema_id == i64::from(current_input)));
                if operation == "overwrite" {
                    historical
                        .new_write_builder()
                        .try_new_commit()
                        .unwrap()
                        .overwrite(
                            messages,
                            Some(HashMap::from([("pt".into(), Some(Datum::Int(1)))])),
                        )
                        .await
                        .unwrap();
                    expected.retain(|row| row[1] != Some(1));
                } else {
                    publish(&historical, messages).await;
                }
                expected.push(vec![
                    Some(3),
                    Some(1),
                    Some(30),
                    current_input.then_some(300),
                ]);
                expected.sort();
            }
            assert_eq!(rows(&latest).await, expected, "{mode} {operation}");
            let snapshot = latest
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap();
            assert_eq!(snapshot.id(), 3);
            assert_eq!(snapshot.schema_id(), 1);
            // Publication does not change the writer handle's selected read view.
            assert_eq!(
                rows(&historical).await,
                vec![vec![Some(1), Some(1), Some(10)]]
            );
        }
    }
}

#[tokio::test]
async fn historical_primary_key_writer_restores_latest_sequence_numbers() {
    let (latest, historical, recent) = evolved("memory:/historical_pk_sequences", "pk").await;
    let latest_sequence = recent
        .iter()
        .flat_map(|message| &message.new_files)
        .filter(|file| file.max_sequence_number >= 0)
        .map(|file| file.max_sequence_number)
        .max()
        .unwrap();
    let messages = write(&historical, batch(false, &[[1, 1, 99, 0]]), false).await;
    assert!(messages
        .iter()
        .flat_map(|message| &message.new_files)
        .all(|file| file.min_sequence_number > latest_sequence));
    publish(&historical, messages).await;
    assert!(rows(&latest)
        .await
        .contains(&vec![Some(1), Some(1), Some(99), None]));
}

#[tokio::test]
async fn captured_empty_writer_keeps_its_empty_read_view() {
    for mode in ["append", "de", "pk"] {
        let (latest, historical, _) =
            evolved(&format!("memory:/captured_empty_write_{mode}"), mode).await;
        let empty = historical.copy_with_pinned_snapshot(None);
        assert!(rows(&empty).await.is_empty());
        let before = rows(&latest).await.len();
        publish(
            &empty,
            write(&empty, batch(false, &[[3, 1, 30, 0]]), false).await,
        )
        .await;
        assert!(rows(&empty).await.is_empty());
        assert_eq!(rows(&latest).await.len(), before + 1);
        assert_eq!(
            latest
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap()
                .schema_id(),
            1
        );
    }
}

#[tokio::test]
async fn historical_branch_publication_keeps_main_isolated() {
    let (main, _, _) = evolved("memory:/historical_write_branch", "de").await;
    let snapshot = main
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    main.tag_manager().create("base", &snapshot).await.unwrap();
    BranchManager::new(main.file_io().clone(), main.location().into())
        .create_branch_from_tag("dev", "base")
        .await
        .unwrap();
    let branch = main.copy_with_branch("dev").await.unwrap();
    let historical = branch
        .copy_with_time_travel(HashMap::from([("scan.snapshot-id".into(), "2".into())]))
        .await
        .unwrap();
    let main_rows = rows(&main).await;
    publish(
        &branch,
        write(&branch, batch(true, &[[5, 1, 50, 500]]), false).await,
    )
    .await;
    publish(
        &historical,
        write(&historical, batch(true, &[[6, 1, 60, 600]]), false).await,
    )
    .await;
    assert_eq!(rows(&main).await, main_rows);
    assert_eq!(rows(&branch).await.len(), 5);
    historical
        .new_write_builder()
        .try_new_commit()
        .unwrap()
        .truncate_table()
        .await
        .unwrap();
    assert!(rows(&branch).await.is_empty());
    assert_eq!(rows(&main).await, main_rows);
}
