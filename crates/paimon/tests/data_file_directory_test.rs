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
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, Schema, TableSchema, VarCharType};
use paimon::table::Table;
use std::sync::Arc;

async fn table(primary_key: bool, evolution: bool, directory: &str, bucket_indexes: bool) -> Table {
    let path = "memory:/directory_test";
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("value", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .option("data-file.path-directory", directory)
        .option("target-file-row-num", "2");
    if primary_key {
        schema = schema
            .primary_key(["id", "pt"])
            .option("bucket", "1")
            .option("changelog-producer", "input");
    }
    if evolution {
        schema = schema
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("deletion-vectors.enabled", "true")
            .option("index-file-in-data-file-dir", bucket_indexes.to_string());
    }
    let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

fn batch(ids: Vec<i32>, parts: Vec<Option<&str>>, values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
        ("pt", Arc::new(StringArray::from(parts)) as ArrayRef),
        ("value", Arc::new(Int32Array::from(values)) as ArrayRef),
    ])
    .unwrap()
}

async fn values(table: &Table) -> Vec<i32> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    for split in plan.splits() {
        assert!(
            split.bucket_path().contains("/data/nested/pt="),
            "{}",
            split.bucket_path()
        );
        for file in split.data_files() {
            assert!(table
                .file_io()
                .exists(&file.data_file_path(split.bucket_path()))
                .await
                .unwrap());
        }
    }
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut result: Vec<i32> = batches
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
    result.sort();
    result
}

#[tokio::test]
async fn configured_directory_contains_append_and_primary_key_files() {
    for primary_key in [false, true] {
        let table = table(primary_key, false, "data/nested", false).await;
        write_batch(
            &table,
            &batch(
                vec![1, 2, 3],
                vec![
                    Some("a/b"),
                    Some("a/b"),
                    if primary_key { Some("c") } else { None },
                ],
                vec![10, 20, 30],
            ),
        )
        .await;
        assert_eq!(values(&table).await, vec![10, 20, 30]);
        assert!(table
            .file_io()
            .exists("memory:/directory_test/snapshot/snapshot-1")
            .await
            .unwrap());
        assert!(!table
            .file_io()
            .exists("memory:/directory_test/data/nested/snapshot/snapshot-1")
            .await
            .unwrap());
    }
}

#[tokio::test]
async fn relocated_updates_upserts_and_repeated_deletes_share_paths() {
    for bucket_indexes in [false, true] {
        let table = table(false, true, "data/nested", bucket_indexes).await;
        write_batch(
            &table,
            &batch(vec![1, 2, 3], vec![Some("a"); 3], vec![10, 20, 30]),
        )
        .await;
        let mut update = table.new_write_builder().new_update().unwrap();
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
        table
            .new_write_builder()
            .new_commit()
            .commit(messages)
            .await
            .unwrap();
        assert_eq!(values(&table).await, vec![11, 20, 30]);
        let messages = update
            .upsert_by_arrow_with_key(
                vec![batch(vec![2, 4], vec![Some("a"), Some("b")], vec![22, 40])],
                vec!["id".into()],
            )
            .await
            .unwrap();
        table
            .new_write_builder()
            .new_commit()
            .commit(messages)
            .await
            .unwrap();
        assert_eq!(values(&table).await, vec![11, 22, 30, 40]);
        for (row_id, expected) in [(0, vec![22, 30, 40]), (1, vec![30, 40])] {
            let messages = update.delete_by_row_id(vec![row_id]).await.unwrap();
            for message in &messages {
                for index in &message.new_index_files {
                    let directory = if bucket_indexes {
                        "data/nested/pt=a/bucket-0"
                    } else {
                        "index"
                    };
                    assert!(table
                        .file_io()
                        .exists(&format!(
                            "{}/{directory}/{}",
                            table.location(),
                            index.file_name
                        ))
                        .await
                        .unwrap());
                }
            }
            table
                .new_write_builder()
                .new_commit()
                .commit(messages)
                .await
                .unwrap();
            assert_eq!(values(&table).await, expected);
        }
        // Abort preserves both prepared and already committed DV files.
        let messages = update.delete_by_row_id(vec![2]).await.unwrap();
        let paths: Vec<String> = messages
            .iter()
            .flat_map(|message| {
                message.new_index_files.iter().map(|index| {
                    let directory = if bucket_indexes {
                        "data/nested/pt=a/bucket-0"
                    } else {
                        "index"
                    };
                    format!("{}/{directory}/{}", table.location(), index.file_name)
                })
            })
            .collect();
        assert!(!paths.is_empty());
        table
            .new_write_builder()
            .new_commit()
            .abort(&messages)
            .await
            .unwrap();
        for path in paths {
            assert!(table.file_io().exists(&path).await.unwrap());
        }
        assert_eq!(values(&table).await, vec![30, 40]);
    }
}

#[tokio::test]
async fn abort_preserves_relocated_data_and_changelog_files() {
    for primary_key in [false, true] {
        let table = table(primary_key, false, "data/nested", false).await;
        let mut writer = table.new_write_builder().new_write().unwrap();
        writer
            .write_arrow_batch(&batch(vec![1], vec![Some("a")], vec![10]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let mut paths = Vec::new();
        for message in &messages {
            if primary_key {
                assert!(!message.new_changelog_files.is_empty());
            }
            for file in message.new_files.iter().chain(&message.new_changelog_files) {
                let path = format!(
                    "{}/data/nested/pt=a/bucket-{}/{}",
                    table.location(),
                    message.bucket,
                    file.file_name
                );
                assert!(table.file_io().exists(&path).await.unwrap());
                paths.push(path);
            }
        }
        assert!(!paths.is_empty());
        table
            .new_write_builder()
            .new_commit()
            .abort(&messages)
            .await
            .unwrap();
        for path in paths {
            assert!(table.file_io().exists(&path).await.unwrap());
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
async fn managed_blob_packs_and_parquet_share_relocated_bucket() {
    use arrow_array::{Array, LargeBinaryArray};
    use paimon::spec::BlobType;
    for primary_key in [false, true] {
        let path = "memory:/blob_directory";
        let mut builder = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Blob(BlobType::new()))
            .option("data-file.path-directory", "data/nested");
        if primary_key {
            builder = builder.primary_key(["id"]).option("bucket", "1");
        } else {
            builder = builder
                .option("row-tracking.enabled", "true")
                .option("data-evolution.enabled", "true");
        }
        let (io, table) = memory_table(path, TableSchema::new(0, &builder.build().unwrap()));
        setup_dirs(&io, path).await;
        persist_table_schema(&io, path, table.schema()).await;
        let input = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
            (
                "payload",
                Arc::new(LargeBinaryArray::from(vec![
                    Some(b"hello".as_slice()),
                    None,
                    Some(b"".as_slice()),
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        write_batch(&table, &input).await;
        let read = table.new_read_builder();
        let plan = read.new_scan().plan().await.unwrap();
        assert!(plan
            .splits()
            .iter()
            .all(|split| split.bucket_path() == format!("{path}/data/nested/bucket-0")));
        let batches: Vec<RecordBatch> = read
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let payload = batch
                .column_by_name("payload")
                .unwrap()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((
                    ids.value(row),
                    (!payload.is_null(row)).then(|| payload.value(row).to_vec()),
                ));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![(1, Some(b"hello".to_vec())), (2, None), (3, Some(vec![]))]
        );
    }
}

#[tokio::test]
async fn dynamic_bucket_hash_indexes_follow_data_directory() {
    let path = "memory:/dynamic_directory";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("value", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .primary_key(["pt", "id"])
        .option("bucket", "-1")
        .option("dynamic-bucket.target-row-num", "1")
        .option("index-file-in-data-file-dir", "true")
        .option("data-file.path-directory", "data/nested")
        .build()
        .unwrap();
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    for value in [10, 20] {
        let mut writer = table.new_write_builder().new_write().unwrap();
        writer
            .write_arrow_batch(&batch(vec![1, 2, 3], vec![Some("a"); 3], vec![value; 3]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        for message in &messages {
            for index in &message.new_index_files {
                assert_eq!(index.index_type, "HASH");
                assert!(io
                    .exists(&format!(
                        "{path}/data/nested/pt=a/bucket-{}/{}",
                        message.bucket, index.file_name
                    ))
                    .await
                    .unwrap());
            }
        }
        table
            .new_write_builder()
            .new_commit()
            .commit(messages)
            .await
            .unwrap();
        assert_eq!(values(&table).await, vec![value; 3]);
    }
}

#[tokio::test]
async fn referenced_sidecars_use_the_configured_directory() {
    use paimon::table::referenced_files::collect_referenced_files_summary_with_options;
    let table = table(false, false, "data/nested", false)
        .await
        .copy_with_options(std::collections::HashMap::from([
            ("file-index.bloom-filter.columns".into(), "value".into()),
            ("file-index.bloom-filter.value.items".into(), "10".into()),
            ("file-index.in-manifest-threshold".into(), "0 B".into()),
        ]));
    write_batch(&table, &batch(vec![1, 2], vec![Some("a"); 2], vec![10, 20])).await;
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut expected_size = 0;
    let mut expected_count = 0;
    for split in plan.splits() {
        for file in split.data_files() {
            assert!(!file.extra_files.is_empty());
            for path in file.collect_files(split.bucket_path()) {
                expected_size += table
                    .file_io()
                    .new_input(&path)
                    .unwrap()
                    .metadata()
                    .await
                    .unwrap()
                    .size as i64;
                expected_count += 1;
            }
        }
    }
    let summaries = collect_referenced_files_summary_with_options(
        table.file_io(),
        table.location(),
        table.schema().partition_keys(),
        table.schema().fields(),
        &table.schema().core_options(),
    )
    .await
    .unwrap();
    let total = summaries.iter().find(|row| row.source == "total").unwrap();
    assert_eq!(total.data_file_size, expected_size);
    assert_eq!(total.data_file_count, expected_count);
}

#[test]
fn directory_is_immutable_and_cannot_be_empty() {
    use std::collections::HashMap;
    for directory in [None, Some("data/nested")] {
        let mut builder = Schema::builder().column("id", DataType::Int(IntType::new()));
        if let Some(directory) = directory {
            builder = builder.option("data-file.path-directory", directory);
        }
        let schema = TableSchema::new(0, &builder.build().unwrap());
        let copied = schema.copy_with_options(HashMap::from([(
            "data-file.path-directory".into(),
            "other".into(),
        )]));
        assert_eq!(copied.core_options().data_file_path_directory(), directory);
    }
    assert!(Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .option("data-file.path-directory", "")
        .build()
        .is_err());
}
