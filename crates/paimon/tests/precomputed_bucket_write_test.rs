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

use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use common::incremental_helpers::{
    make_batch, make_batch_with_kinds, memory_table, persist_table_schema, pk_schema, setup_dirs,
};
use futures::TryStreamExt;
use paimon::spec::{
    batch_hash_codes, DataType, IndexManifest, IntType, Schema, TableSchema, VarCharType,
};
use paimon::table::{CommitMessage, Table};

async fn table(options: &[(&str, &str)]) -> Table {
    let (io, table) = memory_table("memory:/precomputed", pk_schema(options));
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    table
}

fn hashes(table: &Table, batch: &RecordBatch) -> Vec<i32> {
    batch_hash_codes(batch, &[0], table.schema().fields()).unwrap()
}

async fn rows(table: &Table) -> Vec<(i32, i32)> {
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
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| (ids.value(row), values.value(row))));
    }
    rows.sort();
    rows
}

async fn assert_index_hashes(table: &Table, bucket: i32, expected: &[i32]) {
    let manager = table.snapshot_manager();
    let snapshot = manager.get_latest_snapshot().await.unwrap().unwrap();
    let path = format!(
        "{}/{}",
        manager.manifest_dir(),
        snapshot.index_manifest().unwrap()
    );
    let entries = IndexManifest::read(table.file_io(), &path).await.unwrap();
    let entries: Vec<_> = entries
        .iter()
        .filter(|e| e.index_file.index_type == "HASH" && e.bucket == bucket)
        .collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].index_file.row_count, expected.len() as i64);
    let path = entries[0]
        .index_file
        .external_path
        .clone()
        .unwrap_or_else(|| {
            format!(
                "{}/index/{}",
                table.location(),
                entries[0].index_file.file_name
            )
        });
    let bytes = table
        .file_io()
        .new_input(&path)
        .unwrap()
        .read()
        .await
        .unwrap();
    let mut actual = bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|bytes| i32::from_be_bytes(*bytes))
        .collect::<Vec<_>>();
    let mut expected = expected.to_vec();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

async fn commit(table: &Table, messages: Vec<CommitMessage>) {
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
}

#[tokio::test]
async fn fixed_bucket_write_uses_upstream_bucket_and_shared_sequence() {
    let table = table(&[("bucket", "4"), ("changelog-producer", "input")]).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer
        .write_arrow_batch_to_bucket(&make_batch(vec![1], vec![10]), 3, None, None)
        .await
        .unwrap();
    writer
        .write_arrow_batch_to_bucket(&make_batch(vec![1, 2], vec![20, 30]), 3, None, None)
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].bucket, 3);
    // Deduplication discards id=1's first version, while its changelog remains.
    assert_eq!(messages[0].new_files[0].min_sequence_number, 1);
    assert_eq!(messages[0].new_files[0].max_sequence_number, 2);
    assert!(!messages[0].new_changelog_files.is_empty());
    assert!(messages[0].new_index_files.is_empty());
    commit(&table, messages).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer
        .write_arrow_batch_to_bucket(&make_batch(vec![1], vec![40]), 3, None, None)
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages[0].new_files[0].min_sequence_number, 3);
    commit(&table, messages).await;
    assert_eq!(rows(&table).await, vec![(1, 40), (2, 30)]);
}

#[tokio::test]
async fn dynamic_upstream_bucket_survives_mixed_writes_and_checkpoints() {
    let table = table(&[("bucket", "-1"), ("dynamic-bucket.target-row-num", "1")]).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer
        .with_dynamic_bucket_index(false, Some(0))
        .await
        .unwrap();
    let first = make_batch(vec![1], vec![10]);
    let first_hashes = hashes(&table, &first);
    writer
        .write_arrow_batch_to_bucket(&first, 17, None, None)
        .await
        .unwrap();
    // Ordinary assignment must recognize a mapping notified by an upstream group.
    writer
        .write_arrow_batch(&make_batch(vec![1], vec![20]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].bucket, 17);
    commit(&table, messages).await;
    assert_index_hashes(&table, 17, &first_hashes).await;
    let second = make_batch(vec![1, 2], vec![30, 40]);
    let second_hashes = hashes(&table, &second);
    writer
        .write_arrow_batch_to_bucket(&second, 17, Some(&second_hashes), Some(&[false, true]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages[0].new_index_files[0].row_count, 2);
    commit(&table, messages).await;
    assert_index_hashes(&table, 17, &second_hashes).await;
    assert_eq!(rows(&table).await, vec![(1, 30), (2, 40)]);
    let mut restarted = table.new_write_builder().new_write().unwrap();
    restarted
        .write_arrow_batch(&make_batch(vec![1, 2], vec![50, 60]))
        .await
        .unwrap();
    let messages = restarted.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].bucket, 17);
    assert!(messages[0].new_index_files.is_empty());
    commit(&table, messages).await;
    assert_eq!(rows(&table).await, vec![(1, 50), (2, 60)]);
}

#[tokio::test]
async fn repeated_hashes_do_not_rewrite_unchanged_index() {
    let table = table(&[("bucket", "-1")]).await;
    let batch = make_batch(vec![1], vec![10]);
    let key_hashes = hashes(&table, &batch);
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer
        .write_arrow_batch_to_bucket(&batch, 7, Some(&key_hashes), Some(&[true]))
        .await
        .unwrap();
    commit(&table, writer.prepare_commit().await.unwrap()).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer.with_dynamic_bucket_index(false, None).await.unwrap();
    writer
        .write_arrow_batch_to_bucket(&batch, 7, Some(&key_hashes), Some(&[false]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert!(messages[0].new_index_files.is_empty());
    commit(&table, messages).await;
    assert_index_hashes(&table, 7, &key_hashes).await;
}

#[tokio::test]
async fn row_kind_filter_keeps_upstream_hashes_aligned() {
    let table = table(&[
        ("bucket", "-1"),
        ("ignore-delete", "true"),
        ("changelog-producer", "input"),
    ])
    .await;
    // The true new-mapping flags belong to ignored deletes of surviving keys.
    let batch = make_batch_with_kinds(vec![1, 1, 2, 2], vec![0, 10, 0, 20], vec![3, 0, 3, 0]);
    let key_hashes = hashes(&table, &batch);
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer
        .write_arrow_batch_to_bucket(
            &batch,
            9,
            Some(&key_hashes),
            Some(&[true, false, true, false]),
        )
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages[0].new_index_files[0].row_count, 2);
    commit(&table, messages).await;
    assert_index_hashes(&table, 9, &[key_hashes[1], key_hashes[3]]).await;
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn partition_group_and_projected_write_type_are_validated_before_staging() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .partition_keys(["pt"])
        .option("bucket", "4")
        .option("bucket-key", "id")
        .build()
        .unwrap();
    let (io, table) = memory_table("memory:/precomputed_partial", TableSchema::new(0, &schema));
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer
        .with_write_type(vec!["pt".into(), "id".into()])
        .unwrap();
    let batch = RecordBatch::try_from_iter([
        (
            "pt",
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ),
        ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
    ])
    .unwrap();
    let error = writer
        .write_arrow_batch_to_bucket(&batch, 2, None, None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("multiple partitions"));
    // Rejected input did not freeze the write type or stage half a group.
    writer
        .with_write_type(vec!["pt".into(), "id".into()])
        .unwrap();
    writer
        .write_arrow_batch_to_bucket(&batch.slice(0, 1), 2, None, None)
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].bucket, 2);
    assert_eq!(messages[0].new_files[0].row_count, 1);
    assert_eq!(
        messages[0].new_files[0].write_cols,
        Some(vec!["pt".into(), "id".into()])
    );
}

#[tokio::test]
async fn configuration_and_metadata_errors_do_not_stage_data() {
    for bucket in ["1", "4", "-1"] {
        let table = table(&[("bucket", bucket)]).await;
        let mut writer = table.new_write_builder().new_write().unwrap();
        let batch = make_batch(vec![1, 2], vec![10, 20]);
        for invalid in [
            -1,
            if bucket == "-1" {
                32768
            } else {
                bucket.parse().unwrap()
            },
        ] {
            assert!(writer
                .write_arrow_batch_to_bucket(&batch, invalid, None, None)
                .await
                .unwrap_err()
                .to_string()
                .contains("Bucket id"));
        }
        assert!(writer
            .write_arrow_batch_to_bucket(&batch, 0, Some(&[1]), None)
            .await
            .is_err());
        assert!(writer
            .write_arrow_batch_to_bucket(&batch, 0, None, Some(&[true, true]))
            .await
            .is_err());
        assert!(writer
            .write_arrow_batch_to_bucket(&batch, 0, Some(&[1, 2]), Some(&[true]))
            .await
            .is_err());
        if bucket != "-1" {
            assert!(writer
                .with_dynamic_bucket_index(false, None)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("HASH_DYNAMIC"));
        } else {
            assert!(writer
                .with_dynamic_bucket_index(false, Some(-1))
                .await
                .is_err());
            assert!(writer
                .with_dynamic_bucket_index(false, Some(99))
                .await
                .is_err());
            writer
                .with_dynamic_bucket_index(false, Some(0))
                .await
                .unwrap();
        }
        assert!(writer.prepare_commit().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn configuration_freezes_only_after_nonempty_input() {
    let table = table(&[("bucket", "-1")]).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer
        .write_arrow_batch_to_bucket(&make_batch(vec![], vec![]), 0, Some(&[]), Some(&[]))
        .await
        .unwrap();
    writer
        .with_dynamic_bucket_index(false, Some(0))
        .await
        .unwrap();
    writer
        .write_arrow_batch_to_bucket(&make_batch(vec![1], vec![10]), 0, None, None)
        .await
        .unwrap();
    assert!(writer
        .with_dynamic_bucket_index(false, None)
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("before writing"));
    assert_eq!(writer.prepare_commit().await.unwrap().len(), 1);
}

#[tokio::test]
async fn pinned_base_does_not_silently_follow_later_snapshots() {
    let table = table(&[("bucket", "-1")]).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer
        .write_arrow_batch_to_bucket(&make_batch(vec![1], vec![10]), 0, None, None)
        .await
        .unwrap();
    commit(&table, writer.prepare_commit().await.unwrap()).await;
    let mut pinned = table.new_write_builder().new_write().unwrap();
    pinned.with_dynamic_bucket_index(false, None).await.unwrap();
    let second = make_batch(vec![2], vec![20]);
    writer
        .write_arrow_batch_to_bucket(&second, 0, None, None)
        .await
        .unwrap();
    commit(&table, writer.prepare_commit().await.unwrap()).await;
    pinned
        .write_arrow_batch_to_bucket(&make_batch(vec![3], vec![30]), 0, None, None)
        .await
        .unwrap();
    let messages = pinned.prepare_commit().await.unwrap();
    // Index restoration uses snapshot 1, while sequences use current files.
    assert_eq!(messages[0].new_index_files[0].row_count, 2);
    assert_eq!(messages[0].new_files[0].min_sequence_number, 2);
    table
        .new_write_builder()
        .new_commit()
        .abort(&messages)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn first_sequence_restoration_uses_current_files_not_index_base() {
    let table = table(&[("bucket", "-1")]).await;
    let mut first = table.new_write_builder().new_write().unwrap();
    first
        .write_arrow_batch_to_bucket(&make_batch(vec![1], vec![10]), 7, None, None)
        .await
        .unwrap();
    commit(&table, first.prepare_commit().await.unwrap()).await;
    let mut pinned = table.new_write_builder().new_write().unwrap();
    pinned
        .with_dynamic_bucket_index(false, Some(1))
        .await
        .unwrap();
    first
        .write_arrow_batch_to_bucket(&make_batch(vec![1, 1, 1], vec![20, 30, 40]), 7, None, None)
        .await
        .unwrap();
    commit(&table, first.prepare_commit().await.unwrap()).await;
    pinned
        .write_arrow_batch_to_bucket(&make_batch(vec![1], vec![50]), 7, None, None)
        .await
        .unwrap();
    let messages = pinned.prepare_commit().await.unwrap();
    assert_eq!(messages[0].new_files[0].min_sequence_number, 4);
    commit(&table, messages).await;
    assert_eq!(rows(&table).await, vec![(1, 50)]);
}

#[tokio::test]
async fn conflicting_upstream_mapping_never_publishes_a_second_bucket() {
    let table = table(&[("bucket", "-1")]).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    let batch = make_batch(vec![1], vec![10]);
    writer
        .write_arrow_batch_to_bucket(&batch, 7, None, None)
        .await
        .unwrap();
    let error = writer
        .write_arrow_batch_to_bucket(&batch, 8, None, None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("belongs to bucket 7"));
    assert!(writer.prepare_commit().await.is_err());
    assert!(table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn overwrite_ignores_old_hashes_and_sequence_numbers() {
    let table = table(&[("bucket", "-1")]).await;
    let mut first = table.new_write_builder().new_write().unwrap();
    first
        .write_arrow_batch_to_bucket(&make_batch(vec![1, 2], vec![10, 20]), 5, None, None)
        .await
        .unwrap();
    commit(&table, first.prepare_commit().await.unwrap()).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut writer = builder.new_write().unwrap();
    writer
        .with_dynamic_bucket_index(true, Some(1))
        .await
        .unwrap();
    let batch = make_batch(vec![3], vec![30]);
    let key_hashes = hashes(&table, &batch);
    writer
        .write_arrow_batch_to_bucket(&batch, 5, Some(&key_hashes), Some(&[true]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages[0].new_files[0].min_sequence_number, 0);
    assert_eq!(messages[0].new_index_files[0].row_count, 1);
    builder
        .new_commit()
        .overwrite(messages, None)
        .await
        .unwrap();
    assert_index_hashes(&table, 5, &key_hashes).await;
    assert_eq!(rows(&table).await, vec![(3, 30)]);
}

#[tokio::test]
async fn prepared_checkpoints_keep_sequence_progress_before_publication() {
    let table = table(&[("bucket", "-1")]).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .with_dynamic_bucket_index(false, Some(0))
        .await
        .unwrap();
    writer
        .write_arrow_batch_to_bucket(&make_batch(vec![2, 1], vec![20, 10]), 7, None, None)
        .await
        .unwrap();
    let first = writer.prepare_commit().await.unwrap();
    writer
        .write_arrow_batch_to_bucket(&make_batch(vec![1], vec![30]), 7, None, None)
        .await
        .unwrap();
    let second = writer.prepare_commit().await.unwrap();
    assert_eq!(first[0].new_files[0].max_sequence_number, 1);
    assert_eq!(second[0].new_files[0].min_sequence_number, 2);
    builder
        .new_commit()
        .commit_with_identifier(first, 1)
        .await
        .unwrap();
    builder
        .new_commit()
        .commit_with_identifier(second, 2)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(1, 30), (2, 20)]);
}

#[tokio::test]
async fn configured_overwrite_can_use_ordinary_and_direct_assignment() {
    let table = table(&[("bucket", "-1")]).await;
    let mut first = table.new_write_builder().new_write().unwrap();
    first
        .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    commit(&table, first.prepare_commit().await.unwrap()).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut writer = builder.new_write().unwrap();
    writer
        .with_dynamic_bucket_index(true, Some(1))
        .await
        .unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![3], vec![30]))
        .await
        .unwrap();
    writer
        .write_arrow_batch_to_bucket(&make_batch(vec![3, 4], vec![40, 50]), 0, None, None)
        .await
        .unwrap();
    builder
        .new_commit()
        .overwrite(writer.prepare_commit().await.unwrap(), None)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(3, 40), (4, 50)]);
}

#[tokio::test]
async fn precomputed_writer_restores_only_its_bucket() {
    let table = table(&[("bucket", "-1")]).await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    for (id, bucket) in [(1, 7), (2, 8)] {
        writer
            .write_arrow_batch_to_bucket(&make_batch(vec![id], vec![id * 10]), bucket, None, None)
            .await
            .unwrap();
    }
    let messages = writer.prepare_commit().await.unwrap();
    let other_index = messages
        .iter()
        .find(|message| message.bucket == 8)
        .unwrap()
        .new_index_files[0]
        .file_name
        .clone();
    commit(&table, messages).await;
    // An unrelated bucket's payload must never be opened by a direct writer.
    // Ordinary assignment still needs all hashes and detects the corruption.
    table
        .file_io()
        .new_output(&format!("{}/index/{other_index}", table.location()))
        .unwrap()
        .write(bytes::Bytes::from_static(b"bad"))
        .await
        .unwrap();
    let mut direct = table.new_write_builder().new_write().unwrap();
    let batch = make_batch(vec![1, 3], vec![100, 30]);
    direct
        .write_arrow_batch_to_bucket(&batch, 7, None, None)
        .await
        .unwrap();
    let messages = direct.prepare_commit().await.unwrap();
    assert_eq!(messages[0].new_index_files[0].row_count, 2);
    let mut assign = table.new_write_builder().new_write().unwrap();
    assert!(assign
        .write_arrow_batch(&batch)
        .await
        .unwrap_err()
        .to_string()
        .contains("Corrupt HASH index"));
    table
        .new_write_builder()
        .new_commit()
        .abort(&messages)
        .await
        .unwrap();
}

#[tokio::test]
async fn bucket_local_index_paths_survive_precomputed_writer_restart() {
    let table = table(&[
        ("bucket", "-1"),
        ("index-file-in-data-file-dir", "true"),
        ("data-file.path-directory", "data/nested"),
    ])
    .await;
    for ids in [vec![1, 2], vec![2, 3]] {
        let mut writer = table.new_write_builder().new_write().unwrap();
        writer
            .write_arrow_batch_to_bucket(&make_batch(ids.clone(), ids), 19, None, None)
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let meta = &messages[0].new_index_files[0];
        let path = format!(
            "{}/data/nested/bucket-19/{}",
            table.location(),
            meta.file_name
        );
        assert!(table.file_io().exists(&path).await.unwrap());
        commit(&table, messages).await;
    }
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![1, 2, 3], vec![10, 20, 30]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].bucket, 19);
    assert!(messages[0].new_index_files.is_empty());
    commit(&table, messages).await;
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20), (3, 30)]);
}
