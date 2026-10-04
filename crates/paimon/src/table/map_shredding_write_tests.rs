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

//! Table-level coverage for MAP physical layouts, rolling and PK changelogs.

use super::table_write::tests::{setup_dirs, test_file_io};
use super::{Table, TableCommit, TableWrite};
use crate::catalog::Identifier;
use crate::spec::{bucket_dir_name, DataType, IntType, MapType, Schema, TableSchema, VarCharType};
use arrow_array::builder::{Int32Builder, MapBuilder, StringBuilder};
use arrow_array::{Array, Int32Array, MapArray, RecordBatch};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::BTreeMap;
use std::sync::Arc;

async fn table(pk: bool, flush_each_batch: bool) -> Table {
    let file_io = test_file_io();
    let path = "memory:/map_shredding_write";
    setup_dirs(&file_io, path).await;
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "tags",
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::string_type())
                    .copy_with_nullable(false)
                    .unwrap(),
                DataType::Int(IntType::new()),
            )),
        )
        .option("file.compression", "zstd")
        .option("target-file-row-num", "1")
        .option("fields.tags.map.storage-layout", "shared-shredding")
        .option("fields.tags.map.shared-shredding.max-columns", "4");
    if pk {
        schema = schema
            .primary_key(["id"])
            .option("bucket", "1")
            .option("changelog-producer", "input");
    }
    if flush_each_batch {
        schema = schema.option("write.parquet-buffer-size", "1b");
    }
    Table::new(
        file_io,
        Identifier::new("default", "maps"),
        path.into(),
        TableSchema::new(0, &schema.build().unwrap()),
        None,
    )
}

fn batch(ids: &[i32], rows: &[Vec<(&str, Option<i32>)>]) -> RecordBatch {
    let mut maps = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    for row in rows {
        for (key, value) in row {
            maps.keys().append_value(key);
            maps.values().append_option(*value);
        }
        maps.append(true).unwrap();
    }
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(ids.to_vec())) as _),
        ("tags", Arc::new(maps.finish()) as _),
    ])
    .unwrap()
}

async fn file_widths(
    table: &Table,
    bucket: i32,
    files: &[crate::spec::DataFileMeta],
) -> Vec<usize> {
    let mut result = Vec::new();
    for file in files {
        let path = format!(
            "{}/{}/{}",
            table.location(),
            bucket_dir_name(bucket),
            file.file_name
        );
        let bytes = table
            .file_io()
            .new_input(&path)
            .unwrap()
            .read()
            .await
            .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let schema = reader.schema();
        let field = schema.field_with_name("tags").unwrap();
        assert_eq!(
            field.metadata()["paimon.map.storage-layout"],
            "shared-shredding"
        );
        result.push(
            field.metadata()["paimon.map.shared-shredding.num-columns"]
                .parse()
                .unwrap(),
        );
        let schema_count = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .filter(|entry| entry.key == "ARROW:schema")
            .count();
        assert_eq!(
            schema_count, 1,
            "Arrow readers must see the completed shredding metadata"
        );
        assert_eq!(
            file.value_stats.null_counts().len(),
            file.value_stats_cols.as_ref().map_or(2, Vec::len),
            "PK system columns are not value stats"
        );
        if let Some(names) = &file.value_stats_cols {
            assert!(names.iter().all(|name| name == "id" || name == "tags"));
        }
    }
    result
}

async fn read_rows(table: &Table) -> BTreeMap<i32, BTreeMap<String, Option<i32>>> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let reader = builder.new_read().unwrap();
    let batches: Vec<RecordBatch> =
        futures::TryStreamExt::try_collect(reader.to_arrow(plan.splits()).unwrap())
            .await
            .unwrap();
    let mut rows = BTreeMap::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let maps = batch
            .column_by_name("tags")
            .unwrap()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        for (index, id) in ids.values().iter().enumerate() {
            let entries = maps.value(index);
            let keys = entries
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let values = entries
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            rows.insert(
                *id,
                (0..entries.len())
                    .map(|i| {
                        (
                            keys.value(i).to_owned(),
                            (!values.is_null(i)).then(|| values.value(i)),
                        )
                    })
                    .collect(),
            );
        }
    }
    rows
}

#[tokio::test]
async fn existing_orc_map_tables_are_rejected_when_opening_append_and_pk_writers() {
    for pk in [false, true] {
        let table = table(pk, false).await;
        for (key, value) in [
            ("file.format", "orc"),
            ("changelog-file.format", "orc"),
            ("file.format.per.level", "0:parquet,1:orc"),
        ] {
            let unsupported = table.copy_with_options(std::collections::HashMap::from([(
                key.to_string(),
                value.to_string(),
            )]));
            let error = TableWrite::new(&unsupported, "map-test".into())
                .err()
                .expect("existing ORC MAP tables must not open a Rust writer");
            assert!(
                error.to_string().contains("only supports parquet"),
                "{error}"
            );
        }
    }
}

#[tokio::test]
async fn append_and_pk_map_files_adapt_and_roundtrip() {
    for pk in [false, true] {
        let table = table(pk, false).await;
        let mut writer = TableWrite::new(&table, "map-test".into()).unwrap();
        let rows = vec![
            vec![("a", Some(1))],
            vec![("b", Some(2)), ("a", None)],
            vec![("c", Some(3)), ("b", Some(4)), ("a", Some(5))],
        ];
        if pk {
            // All rows roll inside one buffer flush.
            writer
                .write_arrow_batch(&batch(&[1, 2, 3], &rows))
                .await
                .unwrap();
        } else {
            // Append rolling checks row targets at batch boundaries.
            for (i, row) in rows.iter().enumerate() {
                writer
                    .write_arrow_batch(&batch(&[i as i32 + 1], std::slice::from_ref(row)))
                    .await
                    .unwrap();
            }
        }
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        let message = &messages[0];
        assert_eq!(
            file_widths(&table, message.bucket, &message.new_files).await,
            [4, 1, 2]
        );
        if pk {
            assert_eq!(
                file_widths(&table, message.bucket, &message.new_changelog_files).await,
                [4, 1, 2]
            );
        }
        TableCommit::new(table.clone(), "map-test".into())
            .commit(messages)
            .await
            .unwrap();
        let expected = rows
            .into_iter()
            .enumerate()
            .map(|(i, entries)| {
                (
                    i as i32 + 1,
                    entries
                        .into_iter()
                        .map(|(k, v)| (k.to_owned(), v))
                        .collect(),
                )
            })
            .collect();
        assert_eq!(read_rows(&table).await, expected);
    }
}

#[tokio::test]
async fn pk_map_history_is_scoped_to_each_flush_and_output_kind() {
    let table = table(true, true).await;
    let mut writer = TableWrite::new(&table, "map-test".into()).unwrap();
    for ids in [[1, 2], [3, 4]] {
        writer
            .write_arrow_batch(&batch(&ids, &[vec![("a", Some(1))], vec![("b", None)]]))
            .await
            .unwrap();
    }
    let messages = writer.prepare_commit().await.unwrap();
    let message = &messages[0];
    assert_eq!(
        file_widths(&table, message.bucket, &message.new_files).await,
        [4, 1, 4, 1]
    );
    assert_eq!(
        file_widths(&table, message.bucket, &message.new_changelog_files).await,
        [4, 1, 4, 1]
    );
    TableCommit::new(table.clone(), "map-test".into())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(read_rows(&table).await.len(), 4);
}
