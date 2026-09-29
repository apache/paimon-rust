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

use arrow_array::{ArrayRef, Int32Array, LargeBinaryArray, RecordBatch};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{BlobDescriptor, BlobType, DataType, IntType, Schema, TableSchema};
use paimon::table::Table;
use std::sync::Arc;

async fn table(options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(BlobType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true");
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    let path = "memory:/blob_write";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

fn batch(ids: Vec<i32>, blobs: Vec<Option<&[u8]>>) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
        (
            "payload",
            Arc::new(LargeBinaryArray::from(blobs)) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn rows(table: &Table) -> Vec<(i32, Option<Vec<u8>>)> {
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
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        rows.extend(
            ids.values()
                .iter()
                .zip(values.iter())
                .map(|(id, value)| (*id, value.map(Vec::from))),
        );
    }
    rows.sort_by_key(|row| row.0);
    rows
}

#[tokio::test]
async fn partial_blob_updates_preserve_unmatched_descriptors_without_resolving() {
    let table = table(&[("blob-descriptor-field", "payload")]).await;
    // None of these references exist. Updating an inline reference must not
    // read the old or new payloads, including rows retained in the same file.
    let old = BlobDescriptor::new("memory:/missing/old".into(), 0, 3).serialize();
    let retained = BlobDescriptor::new("memory:/missing/retained".into(), 7, -1).serialize();
    let updated = BlobDescriptor::new("memory:/missing/updated".into(), 2, 5).serialize();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&batch(
            vec![1, 2, 3],
            vec![Some(&old), Some(&retained), None],
        ))
        .await
        .unwrap();
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();

    let mut update = builder
        .new_data_evolution_writer(vec!["payload".into()])
        .unwrap();
    update
        .add_matched_batch(
            RecordBatch::try_from_iter([
                (
                    "_ROW_ID",
                    Arc::new(arrow_array::Int64Array::from(vec![0])) as ArrayRef,
                ),
                (
                    "payload",
                    Arc::new(LargeBinaryArray::from(vec![Some(updated.as_slice())])) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .unwrap();
    builder
        .new_commit()
        .commit(update.prepare_commit().await.unwrap())
        .await
        .unwrap();

    let descriptor_table = table.copy_with_options(
        [("blob-as-descriptor".to_string(), "true".to_string())]
            .into_iter()
            .collect(),
    );
    assert_eq!(
        rows(&descriptor_table).await,
        vec![(1, Some(updated)), (2, Some(retained)), (3, None)]
    );
}

#[tokio::test]
async fn prepared_sequences_and_optimized_write_columns_match_java() {
    for optimize in [false, true] {
        let table = table(&[(
            "data-evolution.write-cols-optimization.enabled",
            if optimize { "true" } else { "false" },
        )])
        .await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&batch(vec![1, 2, 3], vec![Some(b"one"), None, Some(b"")]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        for file in &messages[0].new_files {
            assert_eq!((file.min_sequence_number, file.max_sequence_number), (0, 2));
            let expected = if file.file_name.ends_with(".blob") {
                Some(vec!["payload".to_string()])
            } else if optimize {
                None
            } else {
                Some(vec!["id".to_string()])
            };
            assert_eq!(file.write_cols, expected);
        }
        builder.new_commit().commit(messages).await.unwrap();
        assert_eq!(
            rows(&table).await,
            vec![(1, Some(b"one".to_vec())), (2, None), (3, Some(vec![]))]
        );
    }
}

#[tokio::test]
async fn normal_rolls_keep_blob_groups_and_row_ids_aligned() {
    let table = table(&[("target-file-row-num", "2")]).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    for (ids, blobs) in [
        (vec![1, 2], vec![Some(b"a".as_slice()), None]),
        (
            vec![3, 4],
            vec![Some(b"b".as_slice()), Some(b"".as_slice())],
        ),
    ] {
        writer.write_arrow_batch(&batch(ids, blobs)).await.unwrap();
    }
    let messages = writer.prepare_commit().await.unwrap();
    let files = &messages[0].new_files;
    assert_eq!(
        files
            .iter()
            .map(|file| file.file_name.ends_with(".blob"))
            .collect::<Vec<_>>(),
        vec![false, true, false, true]
    );
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        rows(&table).await,
        vec![
            (1, Some(b"a".to_vec())),
            (2, None),
            (3, Some(b"b".to_vec())),
            (4, Some(vec![]))
        ]
    );
}

#[tokio::test]
async fn blob_size_rolls_inside_a_batch_using_resolved_payload_bytes() {
    for descriptor in [false, true] {
        let table = table(&[("blob.target-file-size", "32 B")]).await;
        let payload = vec![b'x'; 40];
        let source = "memory:/source/payload";
        table
            .file_io()
            .new_output(source)
            .unwrap()
            .write(payload.clone().into())
            .await
            .unwrap();
        let input = if descriptor {
            BlobDescriptor::new(source.into(), 0, 40).serialize()
        } else {
            payload.clone()
        };
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&batch(
                vec![1, 2, 3],
                vec![Some(&input), None, Some(&input)],
            ))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let blobs: Vec<_> = messages[0]
            .new_files
            .iter()
            .filter(|file| file.file_name.ends_with(".blob"))
            .collect();
        assert_eq!(
            blobs.iter().map(|file| file.row_count).collect::<Vec<_>>(),
            vec![1, 2]
        );
        let prefix = blobs[0].file_name.rsplit_once('-').unwrap().0;
        for file in &blobs {
            assert_eq!(file.file_name.rsplit_once('-').unwrap().0, prefix);
        }
        assert_ne!(blobs[0].file_name, blobs[1].file_name);
        builder.new_commit().commit(messages).await.unwrap();
        assert_eq!(
            rows(&table).await,
            vec![(1, Some(payload.clone())), (2, None), (3, Some(payload))]
        );
    }
}

#[tokio::test]
async fn inline_descriptors_reject_payload_bytes_before_creating_files() {
    let table = table(&[("blob-descriptor-field", "payload")]).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    let err = writer
        .write_arrow_batch(&batch(vec![1], vec![Some(b"raw payload")]))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("blob-descriptor-field"), "{err}");
    assert!(table
        .file_io()
        .list_status_recursive("memory:/blob_write/data")
        .await
        .unwrap()
        .iter()
        .all(|entry| !entry.path.ends_with(".parquet")));
}

#[tokio::test]
async fn multiple_blob_columns_keep_values_across_independent_rolls() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("large", DataType::Blob(BlobType::new()))
        .column("small", DataType::Blob(BlobType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("target-file-row-num", "2")
        .option("blob.target-file-size", "32 B")
        .option("file-index.bloom-filter.columns", "id")
        .option("file-index.bloom-filter.id.items", "10")
        .option("file-index.in-manifest-threshold", "0 B")
        .build()
        .unwrap();
    let path = "memory:/two_blobs";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let mut expected = Vec::new();
    for start in [0, 2, 4] {
        let payload = vec![start as u8; 40];
        let input = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![start, start + 1])) as ArrayRef,
            ),
            (
                "large",
                Arc::new(LargeBinaryArray::from(vec![Some(payload.as_slice()), None])) as ArrayRef,
            ),
            (
                "small",
                Arc::new(LargeBinaryArray::from(vec![
                    Some(b"".as_slice()),
                    Some(b"abc".as_slice()),
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        writer.write_arrow_batch(&input).await.unwrap();
        expected.extend([
            (start, Some(payload), Some(Vec::new())),
            (start + 1, None, Some(b"abc".to_vec())),
        ]);
    }
    let messages = writer.prepare_commit().await.unwrap();
    let files = &messages[0].new_files;
    let prefix = files[0].file_name.rsplit_once('-').unwrap().0;
    let mut suffixes = std::collections::HashSet::new();
    for file in files {
        let (file_prefix, suffix) = file.file_name.rsplit_once('-').unwrap();
        assert_eq!(file_prefix, prefix);
        assert!(suffixes.insert(suffix.split('.').next().unwrap()));
        if file.file_name.ends_with(".parquet") {
            assert_eq!(file.extra_files.len(), 1);
        }
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
    let mut actual = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let large = batch
            .column(1)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        let small = batch
            .column(2)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        actual.extend(
            ids.values()
                .iter()
                .zip(large.iter())
                .zip(small.iter())
                .map(|((id, large), small)| (*id, large.map(Vec::from), small.map(Vec::from))),
        );
    }
    actual.sort_by_key(|row| row.0);
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn inline_descriptors_accept_v1_v2_and_null_but_reject_malformed_values() {
    let table = table(&[("blob-descriptor-field", "payload")]).await;
    let source = "memory:/source/payload";
    table
        .file_io()
        .new_output(source)
        .unwrap()
        .write(b"hello".to_vec().into())
        .await
        .unwrap();
    let v2 = BlobDescriptor::new(source.into(), 0, 5).serialize();
    let mut v1 = vec![1];
    v1.extend_from_slice(&v2[9..]);
    v1.extend_from_slice(b"padding");
    let mut v2 = v2;
    v2.extend_from_slice(b"padding");
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&batch(vec![1, 2, 3], vec![Some(&v1), Some(&v2), None]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages[0].new_files.len(), 1);
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        rows(&table).await,
        vec![
            (1, Some(b"hello".to_vec())),
            (2, Some(b"hello".to_vec())),
            (3, None)
        ]
    );
    let mut bad_version = v1;
    bad_version[0] = 3;
    for invalid in [
        vec![],
        b"payload".to_vec(),
        v2[..v2.len() - 8].to_vec(),
        bad_version,
    ] {
        let mut writer = builder.new_write().unwrap();
        assert!(writer
            .write_arrow_batch(&batch(vec![4], vec![Some(&invalid)]))
            .await
            .is_err());
    }
    assert_eq!(rows(&table).await.len(), 3);
}

#[tokio::test]
async fn primary_key_inline_descriptors_validate_and_resolve_legacy_bytes() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::with_nullable(false)))
        .column("payload", DataType::Blob(BlobType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("blob-descriptor-field", "payload")
        .build()
        .unwrap();
    let path = "memory:/pk_inline_blob";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    let source = "memory:/source/payload";
    io.new_output(source)
        .unwrap()
        .write(b"hello".to_vec().into())
        .await
        .unwrap();
    let v2 = BlobDescriptor::new(source.into(), 0, 5).serialize();
    let mut v1 = vec![1];
    v1.extend_from_slice(&v2[9..]);
    v1.extend_from_slice(b"padding");
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let error = writer
        .write_arrow_batch(&batch(vec![1], vec![Some(b"raw")]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("blob-descriptor-field"));
    writer
        .write_arrow_batch(&batch(vec![1, 2, 3], vec![Some(&v1), Some(&v2), None]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        rows(&table).await,
        vec![
            (1, Some(b"hello".to_vec())),
            (2, Some(b"hello".to_vec())),
            (3, None)
        ]
    );
}

#[tokio::test]
async fn ignored_row_kinds_skip_inline_blob_validation() {
    for generated in [false, true] {
        let mut schema = Schema::builder()
            .column("id", DataType::Int(IntType::with_nullable(false)))
            .column("payload", DataType::Blob(BlobType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("ignore-delete", "true")
            .option("ignore-update-before", "true")
            .option("blob-descriptor-field", "payload");
        if generated {
            schema = schema
                .column(
                    "kind",
                    DataType::VarChar(paimon::spec::VarCharType::string_type()),
                )
                .option("rowkind.field", "kind");
        }
        let path = "memory:/ignored_inline_blob";
        let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
        setup_dirs(&io, path).await;
        persist_table_schema(&io, path, table.schema()).await;
        io.new_output("memory:/payload")
            .unwrap()
            .write(b"hello".to_vec().into())
            .await
            .unwrap();
        let descriptor = BlobDescriptor::new("memory:/payload".into(), 0, 5).serialize();
        let input = batch(
            vec![1, 2, 3],
            vec![Some(&descriptor), Some(b""), Some(b"raw")],
        );
        let mut columns: Vec<(String, ArrayRef)> = input
            .schema()
            .fields()
            .iter()
            .zip(input.columns())
            .map(|(field, column)| (field.name().clone(), column.clone()))
            .collect();
        if generated {
            columns.push((
                "kind".into(),
                Arc::new(arrow_array::StringArray::from(vec!["+I", "-D", "-U"])),
            ));
        } else {
            columns.push((
                "_VALUE_KIND".into(),
                Arc::new(arrow_array::Int8Array::from(vec![0, 3, 1])),
            ));
        }
        let input = RecordBatch::try_from_iter(columns).unwrap();
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        // An all-ignored batch creates no files, even with invalid payloads.
        writer.write_arrow_batch(&input.slice(1, 2)).await.unwrap();
        assert!(writer.prepare_commit().await.unwrap().is_empty());
        writer.write_arrow_batch(&input).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        builder.new_commit().commit(messages).await.unwrap();
        assert_eq!(rows(&table).await, vec![(1, Some(b"hello".to_vec()))]);
        // Filtering must not bypass validation of a retained row.
        let invalid = input.slice(0, 1);
        let mut columns = invalid.columns().to_vec();
        columns[1] = Arc::new(LargeBinaryArray::from(vec![Some(b"raw".as_slice())]));
        let invalid = RecordBatch::try_new(invalid.schema(), columns).unwrap();
        assert!(builder
            .new_write()
            .unwrap()
            .write_arrow_batch(&invalid)
            .await
            .unwrap_err()
            .to_string()
            .contains("blob-descriptor-field"));
    }
}
