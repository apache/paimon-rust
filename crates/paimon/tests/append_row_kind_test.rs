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

use arrow_array::{ArrayRef, Int32Array, Int8Array, RecordBatch, StringArray};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{CharType, DataType, IntType, Schema, TableSchema, VarCharType};
use paimon::table::Table;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;

async fn make_table(evolution: bool, options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::with_nullable(false)))
        .column("op", DataType::VarChar(VarCharType::string_type()))
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .partition_keys(["pt"])
        .option("rowkind.field", "op")
        .option("data-evolution.enabled", evolution.to_string())
        .option("row-tracking.enabled", evolution.to_string());
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    let path = "memory:/append-row-kind";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

fn batch(rows: &[(Option<i32>, &str, &str)]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from_iter(rows.iter().map(|x| x.0))) as ArrayRef,
        ),
        (
            "op",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|x| x.1))) as ArrayRef,
        ),
        (
            "pt",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|x| x.2))) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn ids(table: &Table) -> Vec<i32> {
    let read = table.new_read_builder();
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<_> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    ids.sort();
    ids
}

#[tokio::test]
async fn append_rejects_retracts_and_filters_only_configured_events() {
    for evolution in [false, true] {
        for kind in ["-D", "-U"] {
            let table = make_table(evolution, &[]).await;
            let mut writer = table.new_write_builder().new_write().unwrap();
            let error = writer
                .write_arrow_batch(&batch(&[(Some(1), kind, "bad")]))
                .await
                .unwrap_err();
            assert!(error.to_string().contains("Append"), "{error}");
            assert!(writer.prepare_commit().await.unwrap().is_empty());
            writer.close().await;
        }
        let table = make_table(evolution, &[("ignore-delete", "true")]).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&batch(&[
                (Some(1), "-D", "bad"),
                (Some(2), "+U", "keep"),
                (Some(3), "-U", "bad"),
                (Some(4), "+I", "keep"),
            ]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(
            messages
                .iter()
                .flat_map(|m| &m.new_files)
                .map(|f| f.row_count)
                .sum::<i64>(),
            2
        );
        builder.new_commit().commit(messages).await.unwrap();
        writer.close().await;
        assert_eq!(ids(&table).await, vec![2, 4]);
    }
}

#[tokio::test]
async fn append_checks_required_values_before_filter_and_keeps_ignore_before_distinct() {
    let table = make_table(false, &[("ignore-delete", "true")]).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    let error = writer
        .write_arrow_batch(&batch(&[(None, "-D", "bad")]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("non-null column(id)"), "{error}");
    assert!(writer.prepare_commit().await.unwrap().is_empty());
    writer.close().await;

    let table = make_table(false, &[("ignore-update-before", "true")]).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer
        .write_arrow_batch(&batch(&[(Some(1), "-U", "bad")]))
        .await
        .unwrap();
    let error = writer
        .write_arrow_batch(&batch(&[(Some(2), "-D", "bad")]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Append"), "{error}");
    assert!(writer.prepare_commit().await.unwrap().is_empty());
    writer.close().await;
}

#[tokio::test]
async fn append_selected_write_type_filters_the_same_rows_as_routing() {
    for precomputed in [false, true] {
        for reordered in [false, true] {
            let table = make_table(
                false,
                &[
                    ("ignore-delete", "true"),
                    ("bucket", "2"),
                    ("bucket-key", "id"),
                ],
            )
            .await;
            let builder = table.new_write_builder();
            let mut writer = builder.new_write().unwrap();
            // Full input retains omitted event/routing fields, whereas a
            // reordered input resolves the generator at its current position.
            let names = if reordered {
                vec!["op", "id", "pt"]
            } else {
                vec!["id"]
            };
            writer
                .with_write_type(names.into_iter().map(str::to_owned).collect())
                .unwrap();
            let input = batch(&[(Some(1), "-D", "bad"), (Some(2), "+I", "keep")]);
            let input = if reordered {
                input.project(&[1, 0, 2]).unwrap()
            } else {
                input
            };
            writer
                .write_arrow(&[input], precomputed.then_some(0))
                .await
                .unwrap();
            let messages = writer.prepare_commit().await.unwrap();
            assert_eq!(
                messages
                    .iter()
                    .flat_map(|m| &m.new_files)
                    .map(|f| f.row_count)
                    .sum::<i64>(),
                1
            );
            builder.new_commit().commit(messages).await.unwrap();
            writer.close().await;
            assert_eq!(ids(&table).await, vec![2]);
        }
    }
}

#[tokio::test]
async fn generated_primary_key_kinds_preserve_input_changelog_events() {
    for op_type in [
        DataType::VarChar(VarCharType::string_type()),
        DataType::VarChar(VarCharType::new(2).unwrap()),
        DataType::Char(CharType::new(2).unwrap()),
    ] {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("op", op_type)
            .primary_key(["id"])
            .option("bucket", "1")
            .option("rowkind.field", "op")
            .option("changelog-producer", "input")
            .build()
            .unwrap();
        let path = "memory:/generated-input-changelog";
        let (io, table) = memory_table(path, TableSchema::new(0, &schema));
        setup_dirs(&io, path).await;
        persist_table_schema(&io, path, table.schema()).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        let input = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![1, 1, 2, 2])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(Int32Array::from(vec![10, 20, 25, 30])) as ArrayRef,
            ),
            (
                "op",
                Arc::new(StringArray::from(vec!["+I", "-D", "-U", "+U"])) as ArrayRef,
            ),
        ])
        .unwrap();
        writer.write_arrow_batch(&input).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let logs: Vec<_> = messages
            .iter()
            .flat_map(|m| &m.new_changelog_files)
            .collect();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].row_count, 4);
        assert_eq!(logs[0].delete_row_count, Some(2));
        let bytes = io
            .new_input(&format!("{path}/bucket-0/{}", logs[0].file_name))
            .unwrap()
            .read()
            .await
            .unwrap();
        let batches = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let values: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("value")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        let kinds: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("_VALUE_KIND")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(values, vec![10, 20, 25, 30]);
        assert_eq!(kinds, vec![0, 3, 1, 2]);
        builder.new_commit().commit(messages).await.unwrap();
        assert_eq!(ids(&table).await, vec![2]);
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.changelog_record_count(), Some(4));
        writer.close().await;
    }
}
