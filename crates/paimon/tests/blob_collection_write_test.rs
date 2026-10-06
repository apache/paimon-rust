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

use arrow_array::builder::{LargeBinaryBuilder, ListBuilder, MapBuilder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, Int32Array, LargeBinaryArray, ListArray, MapArray, RecordBatch,
};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::io::FileRead;
use paimon::spec::{
    ArrayType, BlobType, DataType, IntType, MapType, Schema, TableSchema, VarCharType,
};
use paimon::table::Table;
use std::sync::Arc;

async fn collection_table(options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "items",
            DataType::Array(ArrayType::new(DataType::Blob(BlobType::new()))),
        )
        .column(
            "attrs",
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::new(100).unwrap()),
                DataType::Blob(BlobType::new()),
            )),
        )
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true");
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    let path = "memory:/collections";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

fn collection_batch(value: &[u8]) -> RecordBatch {
    let mut list = ListBuilder::new(LargeBinaryBuilder::new());
    list.values().append_value(b"ignored-before-slice");
    list.append(true);
    list.values().append_value(value);
    list.values().append_null();
    list.values().append_value(b"");
    list.append(true);
    list.append(false);
    list.append(true);
    let mut map = MapBuilder::new(None, StringBuilder::new(), LargeBinaryBuilder::new());
    map.keys().append_value("ignored");
    map.values().append_value(b"before-slice");
    map.append(true).unwrap();
    for (key, item) in [
        ("first", Some(value)),
        ("", Some(b"".as_slice())),
        ("null", None),
    ] {
        map.keys().append_value(key);
        map.values().append_option(item);
    }
    map.append(true).unwrap();
    map.append(false).unwrap();
    map.append(true).unwrap();
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
        ),
        ("items", Arc::new(list.finish()) as ArrayRef),
        ("attrs", Arc::new(map.finish()) as ArrayRef),
    ])
    .unwrap()
    .slice(1, 3)
}

async fn read_batches(table: &Table, descriptor: bool) -> Vec<RecordBatch> {
    let table = table.copy_with_options(
        [("blob-as-descriptor".into(), descriptor.to_string())]
            .into_iter()
            .collect(),
    );
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

#[tokio::test]
async fn collections_use_java_blob_files_and_preserve_null_empty_and_sliced_values() {
    for descriptors in [false, true] {
        for optimize in [false, true] {
            let table = collection_table(&[(
                "data-evolution.write-cols-optimization.enabled",
                if optimize { "true" } else { "false" },
            )])
            .await;
            let payload = b"payload";
            let source = "memory:/source";
            table
                .file_io()
                .new_output(source)
                .unwrap()
                .write(payload.to_vec().into())
                .await
                .unwrap();
            let descriptor = paimon::spec::BlobDescriptor::new(source.into(), 0, -1).serialize();
            let input = collection_batch(if descriptors { &descriptor } else { payload });
            let builder = table.new_write_builder();
            let mut writer = builder.new_write().unwrap();
            writer.write_arrow_batch(&input).await.unwrap();
            let messages = writer.prepare_commit().await.unwrap();
            let files = &messages[0].new_files;
            assert_eq!(files.len(), 3);
            assert!(files[0].file_name.ends_with(".parquet"));
            assert_eq!(
                files[0].write_cols,
                if optimize {
                    None
                } else {
                    Some(vec!["id".into()])
                }
            );
            for (file, name) in files[1..].iter().zip(["items", "attrs"]) {
                assert!(file.file_name.ends_with(".blob"), "{}", file.file_name);
                assert_eq!(file.write_cols, Some(vec![name.into()]));
                assert_eq!(file.row_count, 3);
            }
            builder.new_commit().commit(messages).await.unwrap();
            for descriptor_mode in [false, true] {
                let batches = read_batches(&table, descriptor_mode).await;
                let batch =
                    arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
                assert_eq!(batch.num_rows(), 3);
                let list = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                assert!(list.is_null(1));
                assert_eq!(list.value_length(2), 0);
                let items = list.value(0);
                let values = items.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                assert!(values.is_null(1));
                let map = batch.column(2).as_any().downcast_ref::<MapArray>().unwrap();
                assert!(map.is_null(1));
                assert_eq!(map.value_length(2), 0);
                let values_map = map
                    .values()
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap();
                assert!(values_map.is_null(2));
                if descriptor_mode {
                    for (bytes, expected) in [
                        (values.value(0), payload.as_slice()),
                        (values.value(2), b"".as_slice()),
                        (values_map.value(0), payload.as_slice()),
                        (values_map.value(1), b"".as_slice()),
                    ] {
                        let desc = paimon::spec::BlobDescriptor::deserialize(bytes).unwrap();
                        assert!(desc.uri().ends_with(".blob"));
                        assert_eq!(desc.length(), expected.len() as i64);
                        let data = table
                            .file_io()
                            .new_input(desc.uri())
                            .unwrap()
                            .reader()
                            .await
                            .unwrap()
                            .read(desc.offset() as u64..(desc.offset() + desc.length()) as u64)
                            .await
                            .unwrap();
                        assert_eq!(data.as_ref(), expected);
                    }
                } else {
                    assert_eq!(values.value(0), payload);
                    assert_eq!(values.value(2), b"");
                    assert_eq!(values_map.value(0), payload);
                    assert_eq!(values_map.value(1), b"");
                }
            }
        }
    }
}

#[tokio::test]
async fn raw_blob_collection_updates_are_rejected_like_java() {
    let table = collection_table(&[]).await;
    for column in ["items", "attrs"] {
        assert!(
            table
                .new_write_builder()
                .new_data_evolution_writer(vec![column.into()])
                .is_err(),
            "raw Blob collection {column} must not be updated"
        );
    }
}

#[test]
fn video_configuration_matches_java_and_does_not_fall_through_to_blob_writes() {
    fn schema(
        video_type: DataType,
        options: &[(&str, &str)],
        primary: bool,
    ) -> paimon::Result<Schema> {
        let mut builder = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("video", video_type)
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option("video-frame-field", "video");
        if primary {
            builder = builder.primary_key(vec!["id"]);
        }
        for (key, value) in options {
            builder = builder.option(*key, *value);
        }
        builder.build()
    }
    let scalar = DataType::Blob(BlobType::new());
    let valid = schema(scalar.clone(), &[], false).unwrap();
    let (_, table) = memory_table("memory:/video", TableSchema::new(0, &valid));
    assert!(
        table.new_write_builder().new_write().is_err(),
        "native video writes must not silently become ordinary .blob files"
    );
    assert!(schema(scalar.clone(), &[("blob-descriptor-field", "video")], false).is_err());
    assert!(schema(scalar.clone(), &[("blob-view-field", "video")], false).is_err());
    assert!(schema(scalar.clone(), &[], true).is_err());
    assert!(schema(DataType::Array(ArrayType::new(scalar)), &[], false).is_err());
    assert!(schema(DataType::Int(IntType::new()), &[], false).is_err());
    let options = [("video-frame-field".into(), " video, video ,other ".into())]
        .into_iter()
        .collect();
    let options = paimon::spec::CoreOptions::new(&options);
    assert_eq!(
        options.video_frame_fields(),
        ["video".to_string(), "other".to_string()]
            .into_iter()
            .collect()
    );
    assert_eq!(options.blob_fields(), options.video_frame_fields());
}
