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

use arrow_array::{
    Array, ArrayRef, Int32Array, Int8Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray,
};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{
    BlobType, DataType, Datum, IntType, Predicate, PredicateBuilder, Schema, TableSchema,
    VarCharType,
};
use paimon::table::Table;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;

async fn table(options: &[(&str, &str)]) -> Table {
    table_with_type(
        options,
        DataType::VarChar(VarCharType::with_nullable(false, VarCharType::MAX_LENGTH).unwrap()),
        false,
    )
    .await
}

async fn table_with_type(
    options: &[(&str, &str)],
    value_type: DataType,
    cross_partition: bool,
) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::with_nullable(false)))
        .column("seq", DataType::Int(IntType::with_nullable(false)))
        .column("value", value_type)
        .column("op", DataType::VarChar(VarCharType::string_type()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("ignore-delete", "false");
    if !cross_partition
        && !options
            .iter()
            .any(|(key, value)| *key == "merge-engine" && *value == "first-row")
    {
        schema = schema.option("sequence.field", "seq");
    }
    if cross_partition {
        schema = schema.partition_keys(["op"]).option("bucket", "-1");
    }
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    let path = "memory:/retained-delete";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

fn batch(
    id: Option<i32>,
    seq: Option<i32>,
    value: Option<&str>,
    kind: &str,
    generated: bool,
    large: bool,
) -> RecordBatch {
    let mut columns = vec![
        ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
        ("seq", Arc::new(Int32Array::from(vec![seq])) as ArrayRef),
        (
            "value",
            if large {
                Arc::new(LargeStringArray::from(vec![value])) as ArrayRef
            } else {
                Arc::new(StringArray::from(vec![value])) as ArrayRef
            },
        ),
        ("op", Arc::new(StringArray::from(vec![kind])) as ArrayRef),
    ];
    if !generated {
        columns.push((
            "_VALUE_KIND",
            Arc::new(Int8Array::from(vec![match kind {
                "-D" => 3,
                "-U" => 1,
                "+U" => 2,
                _ => 0,
            }])) as ArrayRef,
        ));
    }
    RecordBatch::try_from_iter(columns).unwrap()
}

async fn read_ids(table: &Table, predicate: Option<Predicate>) -> Vec<i32> {
    let mut read = table.new_read_builder();
    if let Some(predicate) = predicate {
        read.with_filter(predicate);
    }
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<_> = read
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
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn retained_delete_can_omit_non_key_not_null_values() {
    for generated in [false, true] {
        for large in [false, true] {
            for bucket in [None, Some(0)] {
                let options = if generated {
                    vec![("rowkind.field", "op")]
                } else {
                    vec![("changelog-producer", "input")]
                };
                let table = table(&options).await;
                for (seq, value, kind, expected) in [
                    (10, Some("before"), "+I", vec![1]),
                    (9, None, "-D", vec![1]),
                    (11, None, "-D", vec![]),
                ] {
                    let builder = table.new_write_builder();
                    let mut writer = builder.new_write().unwrap();
                    writer
                        .write_arrow(
                            &[batch(Some(1), Some(seq), value, kind, generated, large)],
                            bucket,
                        )
                        .await
                        .unwrap();
                    let messages = writer.prepare_commit().await.unwrap();
                    for file in messages.iter().flat_map(|message| {
                        message.new_files.iter().chain(&message.new_changelog_files)
                    }) {
                        let path = format!("{}/bucket-0/{}", table.location(), file.file_name);
                        let bytes = table
                            .file_io()
                            .new_input(&path)
                            .unwrap()
                            .read()
                            .await
                            .unwrap();
                        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
                        assert!(reader
                            .schema()
                            .field_with_name("value")
                            .unwrap()
                            .is_nullable());
                        assert!(!reader.schema().field_with_name("id").unwrap().is_nullable());
                        assert!(!reader
                            .schema()
                            .field_with_name("seq")
                            .unwrap()
                            .is_nullable());
                        for data in reader.build().unwrap() {
                            let data = data.unwrap();
                            assert_eq!(
                                data.column_by_name("value").unwrap().null_count(),
                                usize::from(value.is_none())
                            );
                            assert_eq!(
                                data.column_by_name("_VALUE_KIND")
                                    .unwrap()
                                    .as_any()
                                    .downcast_ref::<Int8Array>()
                                    .unwrap()
                                    .value(0),
                                if kind == "-D" { 3 } else { 0 }
                            );
                        }
                    }
                    builder.new_commit().commit(messages).await.unwrap();
                    assert_eq!(read_ids(&table, None).await, expected);
                    let predicate = PredicateBuilder::new(table.schema().fields())
                        .equal("value", Datum::String("before".into()))
                        .unwrap();
                    assert_eq!(read_ids(&table, Some(predicate)).await, expected);
                    assert!(!table.schema().fields()[2].data_type().is_nullable());
                }
            }
        }
    }
}

#[tokio::test]
async fn retained_events_keep_required_logical_fields() {
    for generated in [false, true] {
        let options = if generated {
            vec![("rowkind.field", "op")]
        } else {
            vec![]
        };
        let table = table(&options).await;
        for (id, seq, value, kind, field) in [
            (None, Some(1), None, "-D", "id"),
            (Some(1), None, None, "-D", "seq"),
            (Some(1), Some(1), None, "+I", "value"),
            (Some(1), Some(1), None, "-U", "value"),
            (Some(1), Some(1), None, "+U", "value"),
        ] {
            let mut writer = table.new_write_builder().new_write().unwrap();
            let error = writer
                .write_arrow_batch(&batch(id, seq, value, kind, generated, false))
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains(&format!("non-null column({field})")),
                "{error}"
            );
            assert!(writer.prepare_commit().await.unwrap().is_empty());
        }
    }
    for merge in ["partial-update", "aggregation", "first-row"] {
        let table = table(&[("rowkind.field", "op"), ("merge-engine", merge)]).await;
        let mut writer = table.new_write_builder().new_write().unwrap();
        let error = writer
            .write_arrow_batch(&batch(Some(1), Some(1), None, "-D", true, false))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("non-null column(value)"),
            "{error}"
        );
        assert!(writer.prepare_commit().await.unwrap().is_empty());
    }
    let table = table_with_type(
        &[("rowkind.field", "op")],
        DataType::VarChar(VarCharType::with_nullable(false, VarCharType::MAX_LENGTH).unwrap()),
        true,
    )
    .await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    let error = writer
        .write_arrow_batch(&batch(Some(1), Some(1), None, "-D", true, false))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("non-null column(value)"),
        "{error}"
    );
}

#[tokio::test]
async fn retained_null_blob_delete_does_not_create_payload_files() {
    let table = table_with_type(
        &[("rowkind.field", "op")],
        DataType::Blob(BlobType::with_nullable(false)),
        false,
    )
    .await;
    let mut managed_files = Vec::new();
    for (seq, payload) in [(1, Some(b"managed payload".as_slice())), (2, None)] {
        let kind = if payload.is_some() { "+I" } else { "-D" };
        let input = batch(Some(1), Some(seq), Some("unused"), kind, true, false);
        let mut columns = input.columns().to_vec();
        columns[2] = Arc::new(LargeBinaryArray::from(vec![payload]));
        let mut fields = input.schema().fields().to_vec();
        fields[2] = Arc::new(arrow_schema::Field::new(
            "value",
            arrow_schema::DataType::LargeBinary,
            true,
        ));
        let input =
            RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns).unwrap();
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow_batch(&input).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let files = table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .into_iter()
            .filter(|file| file.path.ends_with(".managed.blob"))
            .map(|file| file.path)
            .collect::<Vec<_>>();
        if payload.is_none() {
            assert_eq!(files, managed_files);
        } else {
            assert!(!files.is_empty());
            managed_files = files;
        }
        builder.new_commit().commit(messages).await.unwrap();
        assert_eq!(
            read_ids(&table, None).await.len(),
            usize::from(payload.is_some())
        );
    }
}
