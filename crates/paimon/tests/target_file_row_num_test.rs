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

use common::incremental_helpers::{
    make_batch, memory_table, persist_table_schema, pk_schema, setup_dirs,
};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, Schema, TableSchema};
use paimon::table::Table;

async fn table(path: &str, data_evolution: bool) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("target-file-row-num", "2")
        .option("target-file-size", "256mb");
    if data_evolution {
        schema = schema
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true");
    }
    let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

async fn check_roll(data_evolution: bool) {
    let path = if data_evolution {
        "memory:/target_file_row_num/data_evolution"
    } else {
        "memory:/target_file_row_num/append"
    };
    let table = table(path, data_evolution).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    for (start, end) in [(0, 1), (1, 2), (2, 5), (5, 6)] {
        let batch = make_batch(
            (start..end).collect(),
            (start..end).map(|id| id * 10).collect(),
        );
        writer.write_arrow_batch(&batch).await.unwrap();
    }
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0]
            .new_files
            .iter()
            .map(|file| file.row_count)
            .collect::<Vec<_>>(),
        vec![2, 3, 1]
    );
    builder.new_commit().commit(messages).await.unwrap();

    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    if data_evolution {
        let mut files = plan
            .splits()
            .iter()
            .flat_map(|split| split.data_files())
            .map(|file| (file.first_row_id.unwrap(), file.row_count))
            .collect::<Vec<_>>();
        files.sort_unstable();
        assert_eq!(files, vec![(0, 2), (2, 3), (5, 1)]);
    }
    let batches: Vec<arrow_array::RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn append_rolls_after_batches() {
    check_roll(false).await;
}

#[tokio::test]
async fn data_evolution_rolls_after_batches_and_assigns_contiguous_row_ids() {
    check_roll(true).await;
}

#[tokio::test]
async fn primary_key_row_limit_files_commit_and_read_all_keys() {
    let path = "memory:/target_file_row_num/primary_key";
    let (io, table) = memory_table(path, pk_schema(&[("target-file-row-num", "2")]));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;

    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![5, 1, 4, 2, 3], vec![50, 10, 40, 20, 30]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0]
            .new_files
            .iter()
            .map(|file| file.row_count)
            .collect::<Vec<_>>(),
        vec![2, 2, 1]
    );
    builder.new_commit().commit(messages).await.unwrap();

    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let batches: Vec<arrow_array::RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
}

#[test]
fn reject_invalid_row_targets_at_schema_creation() {
    for value in ["0", "-1", "bad"] {
        let result = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .option("target-file-row-num", value)
            .build();
        assert!(result.is_err(), "invalid target {value} was accepted");
    }
}
