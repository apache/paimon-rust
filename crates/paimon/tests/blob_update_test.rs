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
    Array, ArrayRef, Int32Array, Int64Array, LargeBinaryArray, ListArray, MapArray, RecordBatch,
};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{
    ArrayType, BlobDescriptor, BlobType, DataType, IntType, MapType, Schema, TableSchema,
    VarCharType,
};
use paimon::table::Table;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

async fn table(options: &[(&str, &str)]) -> Table {
    typed_table(DataType::Blob(BlobType::new()), options).await
}

async fn typed_table(payload_type: DataType, options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", payload_type)
        .column("value", DataType::Int(IntType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("deletion-vectors.enabled", "true");
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    let path = "memory:/blob_updates";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

fn matched(ids: Vec<i64>, values: Vec<Option<&[u8]>>) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("_ROW_ID", Arc::new(Int64Array::from(ids)) as ArrayRef),
        (
            "payload",
            Arc::new(LargeBinaryArray::from(values)) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn apply(table: &Table, batch: RecordBatch) -> Vec<paimon::table::CommitMessage> {
    table
        .new_write_builder()
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![batch])
        .await
        .unwrap()
}

async fn read_batch(table: &Table) -> RecordBatch {
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
    arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap()
}

#[tokio::test]
async fn short_deltas_keep_prefix_placeholders_and_stop_before_unchanged_tail() {
    let table = table(&[]).await;
    seed(&table).await;
    let builder = table.new_write_builder();
    let messages = apply(&table, matched(vec![1], vec![Some(b"first")])).await;
    assert_eq!(messages[0].new_files[0].row_count, 2);
    builder.new_commit().commit(messages).await.unwrap();
    let messages = apply(&table, matched(vec![2], vec![Some(b"second")])).await;
    assert_eq!(messages[0].new_files[0].row_count, 3);
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        rows(&table)
            .await
            .iter()
            .map(|row| row.1.clone())
            .collect::<Vec<_>>(),
        vec![
            Some(b"old-0".to_vec()),
            Some(b"first".to_vec()),
            Some(b"second".to_vec()),
            Some(b"old-3".to_vec()),
            Some(b"old-4".to_vec()),
        ]
    );
    let historic = table.copy_with_options(
        [("scan.snapshot-id".into(), "1".into())]
            .into_iter()
            .collect(),
    );
    assert_eq!(rows(&historic).await[1].1, None);
    let mut read = table.new_read_builder();
    read.with_projection(&["payload"])
        .unwrap()
        .with_row_ranges(vec![paimon::table::RowRange::new(1, 2)]);
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let batch = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
    let payloads = batch
        .column(0)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    assert_eq!(
        payloads.iter().collect::<Vec<_>>(),
        vec![Some(b"first".as_slice()), Some(b"second".as_slice())]
    );
}

#[tokio::test]
async fn sparse_positions_continue_across_rolled_delta_files() {
    let table = table(&[("blob.target-file-size", "20 B")]).await;
    seed(&table).await;
    let messages = apply(
        &table,
        matched(vec![0, 4], vec![Some(b"first"), Some(b"last")]),
    )
    .await;
    let mut ranges = messages
        .iter()
        .flat_map(|message| &message.new_files)
        .map(|file| file.row_id_range().unwrap())
        .collect::<Vec<_>>();
    ranges.sort_unstable();
    assert_eq!(ranges, vec![(0, 0), (1, 4)]);
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(
        rows(&table)
            .await
            .iter()
            .map(|row| row.1.clone())
            .collect::<Vec<_>>(),
        vec![
            Some(b"first".to_vec()),
            None,
            Some(vec![]),
            Some(b"old-3".to_vec()),
            Some(b"last".to_vec()),
        ]
    );
}

#[tokio::test]
async fn absent_blob_baseline_writes_real_nulls_for_the_entire_normal_range() {
    let table = table(&[]).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .with_write_type(vec!["id".into(), "value".into()])
        .unwrap();
    writer
        .write_arrow_batch(
            &RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef),
                (
                    "value",
                    Arc::new(Int32Array::from(vec![10, 11, 12])) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .await
        .unwrap();
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    let messages = apply(&table, matched(vec![1], vec![Some(b"new")])).await;
    assert_eq!(
        (
            messages[0].new_files[0].first_row_id,
            messages[0].new_files[0].row_count
        ),
        (Some(0), 3)
    );
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        rows(&table).await,
        vec![(0, None, 10), (1, Some(b"new".to_vec()), 11), (2, None, 12)]
    );
}

#[tokio::test]
async fn descriptor_updates_copy_only_matched_payloads() {
    let table = table(&[]).await;
    seed(&table).await;
    let source = "memory:/external";
    table
        .file_io()
        .new_output(source)
        .unwrap()
        .write(b"ignoredreplacementignored".to_vec().into())
        .await
        .unwrap();
    let descriptor = BlobDescriptor::new(source.into(), 7, 11).serialize();
    let messages = apply(&table, matched(vec![3], vec![Some(&descriptor)])).await;
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    table.file_io().delete_file(source).await.unwrap();
    assert_eq!(rows(&table).await[3].1, Some(b"replacement".to_vec()));
}

#[tokio::test]
async fn non_nullable_blob_allows_placeholders_but_rejects_explicit_null() {
    let table = typed_table(
        DataType::Blob(BlobType::new())
            .copy_with_nullable(false)
            .unwrap(),
        &[],
    )
    .await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(
            &RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef),
                (
                    "payload",
                    Arc::new(LargeBinaryArray::from(vec![
                        b"zero".as_slice(),
                        b"one",
                        b"two",
                    ])) as ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int32Array::from(vec![10, 11, 12])) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .await
        .unwrap();
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    let messages = apply(&table, matched(vec![1], vec![Some(b"new")])).await;
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(rows(&table).await[0].1, Some(b"zero".to_vec()));
    let error = builder
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![matched(vec![1], vec![None])])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("NULL"), "{error}");
    assert_eq!(rows(&table).await[1].1, Some(b"new".to_vec()));
}

fn collection_values(map: bool, values: &[Option<Vec<Option<&[u8]>>>]) -> ArrayRef {
    if map {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), LargeBinaryBuilder::new());
        for row in values {
            if let Some(row) = row {
                for (index, value) in row.iter().enumerate() {
                    builder.keys().append_value(index.to_string());
                    builder.values().append_option(*value);
                }
            }
            builder.append(row.is_some()).unwrap();
        }
        Arc::new(builder.finish())
    } else {
        let mut builder = ListBuilder::new(LargeBinaryBuilder::new());
        for row in values {
            if let Some(row) = row {
                for value in row {
                    builder.values().append_option(*value);
                }
            }
            builder.append(row.is_some());
        }
        Arc::new(builder.finish())
    }
}

#[tokio::test]
async fn collection_updates_preserve_parent_null_empty_and_child_null_values() {
    for map in [false, true] {
        for rolling in [false, true] {
            let blob = DataType::Blob(BlobType::new());
            let kind = if map {
                DataType::Map(MapType::new(
                    DataType::VarChar(VarCharType::new(100).unwrap()),
                    blob,
                ))
            } else {
                DataType::Array(ArrayType::new(blob))
            };
            let table = typed_table(
                kind,
                &[(
                    "blob.target-file-size",
                    if rolling { "20 B" } else { "1 MB" },
                )],
            )
            .await;
            let original = collection_values(
                map,
                &[
                    Some(vec![Some(b"zero"), None, Some(b"")]),
                    None,
                    Some(vec![]),
                    Some(vec![Some(b"three")]),
                    Some(vec![Some(b"four")]),
                ],
            );
            let builder = table.new_write_builder();
            let mut writer = builder.new_write().unwrap();
            writer
                .write_arrow_batch(
                    &RecordBatch::try_from_iter([
                        (
                            "id",
                            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef,
                        ),
                        ("payload", original.clone()),
                        (
                            "value",
                            Arc::new(Int32Array::from(vec![10, 11, 12, 13, 14])) as ArrayRef,
                        ),
                    ])
                    .unwrap(),
                )
                .await
                .unwrap();
            builder
                .new_commit()
                .commit(writer.prepare_commit().await.unwrap())
                .await
                .unwrap();
            // Sliced input exercises non-zero list/map offsets as well as
            // unordered row IDs spanning separate input batches.
            let values = collection_values(
                map,
                &[
                    Some(vec![Some(b"unused")]),
                    Some(vec![Some(b"new"), None, Some(b"")]),
                    None,
                ],
            );
            let batch = RecordBatch::try_from_iter([
                (
                    "_ROW_ID",
                    Arc::new(Int64Array::from(vec![-1, 4, 1])) as ArrayRef,
                ),
                ("payload", values),
            ])
            .unwrap()
            .slice(1, 2);
            let messages = apply(&table, batch).await;
            assert!(messages
                .iter()
                .flat_map(|message| &message.new_files)
                .all(|file| file.file_name.ends_with(".blob")));
            builder.new_commit().commit(messages).await.unwrap();
            let batch = read_batch(&table).await;
            let expected = collection_values(
                map,
                &[
                    Some(vec![Some(b"zero"), None, Some(b"")]),
                    None,
                    Some(vec![]),
                    Some(vec![Some(b"three")]),
                    Some(vec![Some(b"new"), None, Some(b"")]),
                ],
            );
            let expected = arrow_cast::cast(&expected, batch.column(1).data_type()).unwrap();
            assert_eq!(batch.column(1).to_data(), expected.to_data());
            let descriptors = table.copy_with_options(
                [("blob-as-descriptor".into(), "true".into())]
                    .into_iter()
                    .collect(),
            );
            let batch = read_batch(&descriptors).await;
            let payload = batch.column(1);
            assert!(payload.is_null(1));
            let (children, empty) = if map {
                let map = payload.as_any().downcast_ref::<MapArray>().unwrap();
                (map.value(4).column(1).clone(), map.value_length(2))
            } else {
                let list = payload.as_any().downcast_ref::<ListArray>().unwrap();
                (list.value(4), list.value_length(2))
            };
            assert_eq!(empty, 0);
            let values = children
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            assert!(values.is_null(1));
            assert_eq!(
                BlobDescriptor::deserialize(values.value(0))
                    .unwrap()
                    .length(),
                3
            );
            assert_eq!(
                BlobDescriptor::deserialize(values.value(2))
                    .unwrap()
                    .length(),
                0
            );
        }
    }
}

async fn seed(table: &Table) {
    seed_chunks(table, 5).await;
}

async fn seed_chunks(table: &Table, chunk_size: usize) {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(LargeBinaryArray::from(vec![
                Some(b"old-0".as_slice()),
                None,
                Some(b"".as_slice()),
                Some(b"old-3".as_slice()),
                Some(b"old-4".as_slice()),
            ])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int32Array::from(vec![10, 11, 12, 13, 14])) as ArrayRef,
        ),
    ])
    .unwrap();
    for offset in (0..batch.num_rows()).step_by(chunk_size) {
        writer
            .write_arrow_batch(&batch.slice(offset, chunk_size.min(batch.num_rows() - offset)))
            .await
            .unwrap();
    }
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn independently_rolled_blob_files_do_not_define_update_ranges() {
    let table = table(&[
        ("blob.target-file-size", "35 B"),
        ("target-file-row-num", "100"),
    ])
    .await;
    seed(&table).await;
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let normal_ranges = plan
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .filter(|file| file.file_name.ends_with(".parquet"))
        .map(|file| file.row_id_range().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(normal_ranges, vec![(0, 4)]);
    let blob_ranges = plan
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .filter(|file| file.file_name.ends_with(".blob"))
        .map(|file| file.row_id_range().unwrap())
        .collect::<Vec<_>>();
    assert!(
        blob_ranges
            .iter()
            .any(|&(first, last)| first < 4 && last == 4),
        "{blob_ranges:?}"
    );
    let builder = table.new_write_builder();
    let mut update = builder.new_update().unwrap();
    update
        .with_update_type(vec!["payload".into(), "value".into()])
        .unwrap();
    let batch = RecordBatch::try_from_iter([
        ("_ROW_ID", Arc::new(Int64Array::from(vec![4])) as ArrayRef),
        (
            "payload",
            Arc::new(LargeBinaryArray::from(vec![Some(b"new-4".as_slice())])) as ArrayRef,
        ),
        ("value", Arc::new(Int32Array::from(vec![99])) as ArrayRef),
    ])
    .unwrap();
    let messages = update
        .update_by_arrow_with_row_id(vec![batch])
        .await
        .unwrap();
    assert!(messages
        .iter()
        .flat_map(|message| &message.new_files)
        .all(|file| file.first_row_id == Some(0) && file.row_count == 5));
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        rows(&table).await,
        vec![
            (0, Some(b"old-0".to_vec()), 10),
            (1, None, 11),
            (2, Some(vec![]), 12),
            (3, Some(b"old-3".to_vec()), 13),
            (4, Some(b"new-4".to_vec()), 99),
        ]
    );
}

async fn rows(table: &Table) -> Vec<(i32, Option<Vec<u8>>, i32)> {
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
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let blobs = batch
            .column(1)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend(
            ids.values()
                .iter()
                .zip(blobs.iter())
                .zip(values.values())
                .map(|((id, blob), value)| (*id, blob.map(Vec::from), *value)),
        );
    }
    rows.sort_by_key(|row| row.0);
    rows
}

#[tokio::test]
async fn raw_blob_update_distinguishes_null_from_java_placeholder() {
    let table = table(&[]).await;
    seed(&table).await;
    let builder = table.new_write_builder();
    let mut update = builder.new_update().unwrap();
    update.with_update_type(vec!["payload".into()]).unwrap();
    let batch = RecordBatch::try_from_iter([
        (
            "_ROW_ID",
            Arc::new(Int64Array::from(vec![2, 4])) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(LargeBinaryArray::from(vec![
                None,
                Some(b"new-4".as_slice()),
            ])) as ArrayRef,
        ),
    ])
    .unwrap();
    let messages = update
        .update_by_arrow_with_row_id(vec![batch])
        .await
        .unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].check_from_snapshot, Some(1));
    assert_eq!(messages[0].new_files.len(), 1);
    let delta = &messages[0].new_files[0];
    assert!(delta.file_name.ends_with(".blob"));
    assert_eq!(delta.write_cols, Some(vec!["payload".into()]));
    assert_eq!((delta.first_row_id, delta.row_count), (Some(0), 5));
    assert_eq!(
        (delta.min_sequence_number, delta.max_sequence_number),
        (0, 0)
    );
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        rows(&table).await,
        vec![
            (0, Some(b"old-0".to_vec()), 10),
            (1, None, 11),
            (2, None, 12),
            (3, Some(b"old-3".to_vec()), 13),
            (4, Some(b"new-4".to_vec()), 14),
        ]
    );
}

async fn physical_files(table: &Table) -> HashSet<String> {
    table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .into_iter()
        .filter(|entry| entry.path.ends_with(".blob") || entry.path.ends_with(".parquet"))
        .map(|entry| entry.path)
        .collect()
}

#[tokio::test]
async fn raw_blob_upsert_matches_keys_before_writing_payloads() {
    let table = table(&[("blob.target-file-size", "20 B")]).await;
    seed(&table).await;
    let builder = table.new_write_builder();
    let mut update = builder.new_update().unwrap();
    update.with_update_type(vec!["payload".into()]).unwrap();
    let source = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1, 5, 5])) as ArrayRef),
        (
            "payload",
            Arc::new(LargeBinaryArray::from(vec![
                Some(b"matched".as_slice()),
                Some(b"shadowed"),
                None,
            ])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int32Array::from(vec![111, 500, 501])) as ArrayRef,
        ),
    ])
    .unwrap();
    let messages = update
        .upsert_by_arrow_with_key(vec![source], vec!["id".into()])
        .await
        .unwrap();
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        rows(&table).await,
        vec![
            (0, Some(b"old-0".to_vec()), 10),
            (1, Some(b"matched".to_vec()), 11),
            (2, Some(vec![]), 12),
            (3, Some(b"old-3".to_vec()), 13),
            (4, Some(b"old-4".to_vec()), 14),
            (5, None, 501),
        ]
    );
}

#[tokio::test]
async fn raw_blob_update_uses_baseline_metadata_without_opening_old_payloads() {
    let table = table(&[]).await;
    seed(&table).await;
    for path in physical_files(&table)
        .await
        .into_iter()
        .filter(|path| path.ends_with(".blob"))
    {
        table.file_io().delete_file(&path).await.unwrap();
    }
    let messages = apply(&table, matched(vec![1], vec![Some(b"new")])).await;
    assert_eq!(messages[0].new_files[0].row_count, 2);
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    let mut builder = table.new_read_builder();
    builder.with_projection(&["id", "value"]).unwrap();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 5);
}

#[tokio::test]
async fn abort_and_commit_conflicts_preserve_prepared_blob_files() {
    for published in [false, true] {
        let table = table(&[]).await;
        seed(&table).await;
        let builder = table.new_write_builder();
        let mut updater = builder
            .new_update()
            .unwrap()
            .new_update_by_row_id()
            .await
            .unwrap();
        let messages = updater
            .update_columns(
                vec![matched(vec![1], vec![Some(b"new")])],
                vec!["payload".into()],
            )
            .await
            .unwrap();
        let files = physical_files(&table).await;
        if published {
            builder.new_commit().commit(messages.clone()).await.unwrap();
        }
        updater.abort().await.unwrap();
        updater.abort().await.unwrap();
        assert_eq!(physical_files(&table).await, files);
        if !published {
            builder.new_commit().commit(messages).await.unwrap();
        }
        assert_eq!(rows(&table).await[1].1, Some(b"new".to_vec()));
    }
    let table = table(&[]).await;
    seed(&table).await;
    let stale = apply(&table, matched(vec![1], vec![Some(b"stale")])).await;
    let latest = apply(&table, matched(vec![1], vec![Some(b"latest")])).await;
    let files = physical_files(&table).await;
    let builder = table.new_write_builder();
    builder.new_commit().commit(latest).await.unwrap();
    let error = builder.new_commit().commit(stale).await.unwrap_err();
    assert!(error.to_string().contains("conflict"), "{error}");
    assert_eq!(physical_files(&table).await, files);
    assert_eq!(rows(&table).await[1].1, Some(b"latest".to_vec()));
}

#[tokio::test]
async fn blob_update_keeps_deleted_rows_deleted_after_partial_rewrite() {
    let table = table(&[]).await;
    seed(&table).await;
    let builder = table.new_write_builder();
    let deletion = builder
        .new_update()
        .unwrap()
        .delete_by_row_id(vec![0, 2])
        .await
        .unwrap();
    builder.new_commit().commit(deletion).await.unwrap();
    let messages = apply(&table, matched(vec![1], vec![Some(b"new")])).await;
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        rows(&table).await,
        vec![
            (1, Some(b"new".to_vec()), 11),
            (3, Some(b"old-3".to_vec()), 13),
            (4, Some(b"old-4".to_vec()), 14),
        ]
    );
}

struct BlobSource {
    opened: AtomicUsize,
    closed: Arc<AtomicUsize>,
}

struct BlobSourceFactory(Arc<BlobSource>);

impl paimon::io::UriReaderFactory for BlobSourceFactory {
    fn create(&self, uri: &str) -> paimon::Result<Arc<dyn paimon::io::UriReader>> {
        assert_eq!(uri, "custom://update");
        Ok(self.0.clone())
    }
}

#[async_trait::async_trait]
impl paimon::io::UriReader for BlobSource {
    async fn new_input_stream(
        &self,
        _uri: &str,
    ) -> paimon::Result<Box<dyn paimon::io::UriInputStream>> {
        self.opened.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(BlobSourceStream {
            position: 0,
            closed: self.closed.clone(),
        }))
    }
}

struct BlobSourceStream {
    position: usize,
    closed: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl paimon::io::UriInputStream for BlobSourceStream {
    async fn read(&mut self, length: usize) -> paimon::Result<bytes::Bytes> {
        assert!(length <= 4096);
        let bytes = bytes::Bytes::from_static(b"streamed value");
        let end = (self.position + length).min(bytes.len());
        let result = bytes.slice(self.position..end);
        self.position = end;
        Ok(result)
    }

    async fn close(&mut self) -> paimon::Result<()> {
        self.closed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn row_id_blob_reader_factory_streams_unknown_lengths_and_closes_sources() {
    let table = table(&[("blob.target-file-size", "20 B")]).await;
    seed(&table).await;
    let reader = Arc::new(BlobSource {
        opened: AtomicUsize::new(0),
        closed: Arc::new(AtomicUsize::new(0)),
    });
    let builder = table.new_write_builder();
    let mut updater = builder
        .new_update()
        .unwrap()
        .new_update_by_row_id()
        .await
        .unwrap();
    updater.with_blob_uri_reader_factory(Some(Arc::new(BlobSourceFactory(reader.clone()))));
    let descriptor = BlobDescriptor::new("custom://update".into(), 0, -1).serialize();
    let messages = updater
        .update_columns(
            vec![matched(
                vec![1, 4],
                vec![Some(&descriptor), Some(&descriptor)],
            )],
            vec!["payload".into()],
        )
        .await
        .unwrap();
    assert_eq!(reader.opened.load(Ordering::Relaxed), 2);
    assert_eq!(reader.closed.load(Ordering::Relaxed), 2);
    builder.new_commit().commit(messages).await.unwrap();
    let rows = rows(&table).await;
    assert_eq!(rows[1].1, Some(b"streamed value".to_vec()));
    assert_eq!(rows[4].1, Some(b"streamed value".to_vec()));
}
