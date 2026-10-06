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

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, Schema, TableSchema, VarCharType};
use paimon::table::Table;
use std::sync::Arc;

async fn table(mode: &str, strategy: &str, indexes: bool) -> Table {
    let path = "memory:/external_write";
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("value", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .option("data-file.path-directory", "data/nested")
        .option(
            "data-file.external-paths",
            "memory:/external-a,memory:/external-b",
        )
        .option("data-file.external-paths.strategy", strategy)
        .option("data-file.external-paths.weights", "1,2")
        .option("data-file.external-paths.specific-fs", "memory")
        .option("target-file-row-num", "1");
    if mode == "pk" || mode == "postpone" {
        schema = schema
            .primary_key(["id", "pt"])
            .option("bucket", if mode == "postpone" { "-2" } else { "1" })
            .option(
                "changelog-producer",
                if mode == "postpone" { "none" } else { "input" },
            );
    }
    if mode == "evolution" {
        schema = schema
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("deletion-vectors.enabled", "true");
    }
    if indexes {
        schema = schema
            .option("file-index.bloom-filter.columns", "value")
            .option("file-index.bloom-filter.value.items", "10")
            .option("file-index.in-manifest-threshold", "0 B");
    }
    let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

fn batch(id: i32, value: i32) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
        ("pt", Arc::new(StringArray::from(vec!["a/b"])) as ArrayRef),
        ("value", Arc::new(Int32Array::from(vec![value])) as ArrayRef),
    ])
    .unwrap()
}

async fn values(table: &Table) -> Vec<i32> {
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
    let mut values: Vec<_> = batches
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
    values.sort();
    values
}

#[tokio::test]
async fn external_data_and_sidecars_follow_the_recorded_path() {
    for mode in ["append", "pk", "evolution", "postpone"] {
        for strategy in [
            "round-robin",
            "weight-robin",
            "entropy-inject",
            "specific-fs",
            "none",
        ] {
            let table = table(mode, strategy, true).await;
            let builder = table.new_write_builder();
            let mut write = builder.new_write().unwrap();
            for id in 1..=4 {
                write.write_arrow_batch(&batch(id, id * 10)).await.unwrap();
            }
            let messages = write.prepare_commit().await.unwrap();
            let mut external_roots = std::collections::HashSet::new();
            for message in &messages {
                for file in message.new_files.iter().chain(&message.new_changelog_files) {
                    assert_eq!(
                        file.external_path.is_some(),
                        strategy != "none",
                        "{mode}/{strategy}"
                    );
                    let bucket = format!(
                        "{}/data/nested/pt=a%2Fb/bucket-{}",
                        table.location(),
                        if mode == "postpone" { "postpone" } else { "0" }
                    );
                    for path in file.collect_files(&bucket) {
                        assert!(table.file_io().exists(&path).await.unwrap(), "{path}");
                        if strategy != "none" {
                            assert!(path.starts_with("memory:/external-"), "{path}");
                            assert!(path.contains("/data/nested/pt=a%2Fb/bucket-"));
                            external_roots.insert(path.split('/').nth(1).unwrap().to_string());
                            assert!(!table
                                .file_io()
                                .exists(&format!("{bucket}/{}", file.file_name))
                                .await
                                .unwrap());
                        }
                    }
                    if !file.file_name.starts_with("changelog-") {
                        assert!(!file.extra_files.is_empty());
                    }
                }
            }
            if mode != "postpone"
                && ["round-robin", "entropy-inject", "specific-fs"].contains(&strategy)
            {
                assert_eq!(external_roots.len(), 2);
            }
            builder.new_commit().commit(messages).await.unwrap();
            // Normal scans intentionally skip postpone files until compaction.
            let expected = if mode == "postpone" {
                vec![]
            } else {
                vec![10, 20, 30, 40]
            };
            assert_eq!(values(&table).await, expected, "{mode}/{strategy}");
        }
    }
}

#[tokio::test]
async fn abort_preserves_external_data_changelog_and_sidecars() {
    for mode in ["append", "pk", "evolution", "postpone"] {
        let table = table(mode, "entropy-inject", true).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow_batch(&batch(1, 10)).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let mut paths = Vec::new();
        for message in &messages {
            for file in message.new_files.iter().chain(&message.new_changelog_files) {
                paths.extend(file.collect_files("unused-table-bucket"));
            }
        }
        assert!(!paths.is_empty());
        for path in &paths {
            assert!(path.starts_with("memory:/external-"));
            assert!(table.file_io().exists(path).await.unwrap());
        }
        builder.new_commit().abort(&messages).await.unwrap();
        for path in &paths {
            assert!(table.file_io().exists(path).await.unwrap(), "{path}");
        }
        assert!(table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn updates_read_old_external_files_and_publish_to_new_roots() {
    use std::collections::HashMap;
    let table = table("evolution", "entropy-inject", false).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    for id in 1..=3 {
        writer.write_arrow_batch(&batch(id, id * 10)).await.unwrap();
    }
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    let moved = table.copy_with_options(HashMap::from([
        ("data-file.external-paths".into(), "memory:/new-root".into()),
        (
            "data-file.external-paths.strategy".into(),
            "round-robin".into(),
        ),
    ]));
    let mut update = moved.new_write_builder().new_update().unwrap();
    update.with_update_type(vec!["value".into()]).unwrap();
    let input = RecordBatch::try_from_iter([
        (
            "_ROW_ID",
            Arc::new(arrow_array::Int64Array::from(vec![0])) as ArrayRef,
        ),
        ("value", Arc::new(Int32Array::from(vec![11])) as ArrayRef),
    ])
    .unwrap();
    let messages = update
        .update_by_arrow_with_row_id(vec![input])
        .await
        .unwrap();
    for file in messages.iter().flat_map(|m| &m.new_files) {
        assert!(file
            .external_path
            .as_ref()
            .unwrap()
            .starts_with("memory:/new-root/"));
    }
    moved
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(values(&moved).await, vec![11, 20, 30]);
    let messages = update
        .upsert_by_arrow_with_key(vec![batch(2, 22), batch(4, 40)], vec!["id".into()])
        .await
        .unwrap();
    moved
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(values(&moved).await, vec![11, 22, 30, 40]);
    for (row_id, expected) in [(0, vec![22, 30, 40]), (1, vec![30, 40])] {
        let messages = update.delete_by_row_id(vec![row_id]).await.unwrap();
        moved
            .new_write_builder()
            .new_commit()
            .commit(messages)
            .await
            .unwrap();
        assert_eq!(values(&moved).await, expected);
    }
    let historical =
        moved.copy_with_options(HashMap::from([("scan.snapshot-id".into(), "1".into())]));
    assert_eq!(values(&historical).await, vec![10, 20, 30]);
}

#[tokio::test]
async fn dedicated_blob_fields_share_external_rotation_with_parquet() {
    use arrow_array::LargeBinaryArray;
    use paimon::spec::BlobType;
    let path = "memory:/external_blob";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("first", DataType::Blob(BlobType::new()))
        .column("second", DataType::Blob(BlobType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option(
            "data-file.external-paths",
            "memory:/external-a,memory:/external-b",
        )
        .option("data-file.external-paths.strategy", "entropy-inject")
        .build()
        .unwrap();
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    let input = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        (
            "first",
            Arc::new(LargeBinaryArray::from(vec![
                Some(b"hello".as_slice()),
                None,
                Some(b"".as_slice()),
            ])) as ArrayRef,
        ),
        (
            "second",
            Arc::new(LargeBinaryArray::from(vec![
                None,
                Some(b"world".as_slice()),
                Some(b"!".as_slice()),
            ])) as ArrayRef,
        ),
    ])
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&input).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let files: Vec<_> = messages.iter().flat_map(|m| &m.new_files).collect();
    assert_eq!(files.len(), 3);
    for (column, root) in [
        ("id", "external-b"),
        ("first", "external-a"),
        ("second", "external-b"),
    ] {
        let file = files
            .iter()
            .find(|file| {
                file.write_cols
                    .as_ref()
                    .unwrap()
                    .contains(&column.to_string())
            })
            .unwrap();
        let location = file.external_path.as_ref().unwrap();
        assert!(
            location.starts_with(&format!("memory:/{root}/bucket-0/")),
            "{column}: {location}"
        );
        assert!(io.exists(location).await.unwrap());
    }
    builder.new_commit().commit(messages).await.unwrap();
    let read = table.new_read_builder();
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].columns(), input.columns());
}

#[tokio::test]
async fn copied_pk_blob_options_cannot_enable_external_writes() {
    use paimon::spec::BlobType;
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(BlobType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .build()
        .unwrap();
    let (_, table) = memory_table("memory:/pk-blob", TableSchema::new(0, &schema));
    let table = table.copy_with_options(std::collections::HashMap::from([(
        "data-file.external-paths".into(),
        "memory:/outside".into(),
    )]));
    let error = table
        .new_write_builder()
        .new_write()
        .err()
        .expect("invalid copied schema must fail");
    assert!(error.to_string().contains("external-paths"));
}
