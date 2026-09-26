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

use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, Schema, TableSchema, VarCharType};
use paimon::table::{CommitMessage, DataEvolutionWriter, Table};
use std::collections::HashMap;
use std::sync::Arc;

async fn table(options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("p", DataType::VarChar(VarCharType::string_type()))
        .column("q", DataType::Int(IntType::new()))
        .column("id", DataType::Int(IntType::new()))
        .column("v", DataType::Int(IntType::new()))
        .partition_keys(["p", "q"])
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("deletion-vectors.enabled", "true");
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    let schema = schema.build().unwrap();
    let path = "memory:/update_paths";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

fn batch(p: Vec<Option<&str>>, q: Vec<i32>, id: Vec<i32>, v: Vec<i32>) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("p", Arc::new(StringArray::from(p)) as ArrayRef),
        ("q", Arc::new(Int32Array::from(q)) as ArrayRef),
        ("id", Arc::new(Int32Array::from(id)) as ArrayRef),
        ("v", Arc::new(Int32Array::from(v)) as ArrayRef),
    ])
    .unwrap()
}

async fn seed(table: &Table) {
    write_batch(
        table,
        &batch(
            vec![Some("a"), Some("a"), None, None],
            vec![1, 1, 2, 2],
            vec![0, 1, 2, 3],
            vec![10, 11, 12, 13],
        ),
    )
    .await;
}

async fn read(table: &Table) -> Vec<RecordBatch> {
    let mut read = table.new_read_builder();
    read.with_projection(&["p", "q", "id", "v", "_ROW_ID"])
        .unwrap();
    let plan = read.new_scan().plan().await.unwrap();
    read.new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

async fn row_ids(table: &Table) -> HashMap<i32, i64> {
    let mut result = HashMap::new();
    for batch in read(table).await {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let row_ids = batch
            .column_by_name("_ROW_ID")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            result.insert(ids.value(row), row_ids.value(row));
        }
    }
    result
}

async fn values(table: &Table) -> Vec<(Option<String>, i32, i32, i32)> {
    let mut result = Vec::new();
    for batch in read(table).await {
        let p = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let q = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let id = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let v = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            result.push((
                (!p.is_null(row)).then(|| p.value(row).to_string()),
                q.value(row),
                id.value(row),
                v.value(row),
            ));
        }
    }
    result.sort_by_key(|row| row.2);
    result
}

fn matched(ids: Vec<i64>, p: Vec<Option<&str>>, q: Vec<i64>, v: Vec<i32>) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("_ROW_ID", Arc::new(Int64Array::from(ids)) as ArrayRef),
        ("p", Arc::new(StringArray::from(p)) as ArrayRef),
        // The table uses INT: compare after the ordinary row-update coercion.
        ("q", Arc::new(Int64Array::from(q)) as ArrayRef),
        ("v", Arc::new(Int32Array::from(v)) as ArrayRef),
    ])
    .unwrap()
}

async fn commit(table: &Table, messages: Vec<CommitMessage>) {
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
}

async fn files(table: &Table) -> Vec<String> {
    let mut files = table
        .file_io()
        .list_status_recursive("memory:/")
        .await
        .unwrap()
        .into_iter()
        .map(|status| status.path)
        .filter(|path| path.ends_with(".parquet") || path.contains("/index-"))
        .collect::<Vec<_>>();
    files.sort();
    files
}

#[tokio::test]
async fn row_id_updates_carry_unchanged_composite_and_null_partitions() {
    let table = table(&[]).await;
    seed(&table).await;
    let ids = row_ids(&table).await;
    let update = table.new_write_builder().new_update().unwrap();
    let mut writer = update.new_update_by_row_id().await.unwrap();
    let input = matched(
        vec![ids[&3], ids[&0]],
        vec![None, Some("a")],
        vec![2, 1],
        vec![130, 100],
    );
    let messages = writer
        .update_columns(vec![input], vec!["p".into(), "q".into(), "v".into()])
        .await
        .unwrap();
    assert!(messages
        .iter()
        .flat_map(|message| &message.new_files)
        .all(|file| file.write_cols.as_ref().unwrap().contains(&"p".into())));
    let before = files(&table).await;
    let error = writer
        .update_columns(
            vec![matched(vec![ids[&0]], vec![Some("a")], vec![1], vec![0])],
            vec!["p".into()],
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("overlap"), "{error}");
    assert_eq!(files(&table).await, before);
    commit(&table, messages).await;
    assert_eq!(
        values(&table).await,
        vec![
            (Some("a".into()), 1, 0, 100),
            (Some("a".into()), 1, 1, 11),
            (None, 2, 2, 12),
            (None, 2, 3, 130)
        ]
    );

    // SQL assignment entry points retain their partition-column restriction.
    assert!(DataEvolutionWriter::new(&table, vec!["p".into()]).is_err());
    let mut writer = update.new_update_by_row_id().await.unwrap();
    let messages = writer
        .update_columns(
            vec![matched(vec![ids[&0]], vec![Some("a")], vec![1], vec![0])],
            vec!["p".into(), "q".into()],
        )
        .await
        .unwrap();
    commit(&table, messages).await;
    assert_eq!(values(&table).await[0].3, 100);
}

#[tokio::test]
async fn partition_changes_fail_before_writes_and_preserve_prior_messages() {
    let table = table(&[]).await;
    seed(&table).await;
    let ids = row_ids(&table).await;
    let update = table.new_write_builder().new_update().unwrap();
    let mut writer = update.new_update_by_row_id().await.unwrap();
    // Unselected input fields do not constrain the update.
    let saved = writer
        .update_columns(
            vec![matched(
                vec![ids[&0]],
                vec![Some("ignored")],
                vec![999],
                vec![100],
            )],
            vec!["v".into()],
        )
        .await
        .unwrap();
    let before = files(&table).await;
    for (p, q) in [(Some("changed"), 2), (Some("a"), 2), (None, 3)] {
        let input = matched(
            vec![ids[&0], ids[&2]],
            vec![Some("a"), p],
            vec![1, q],
            vec![1, 2],
        );
        let error = writer
            .update_columns(vec![input], vec!["p".into(), "q".into()])
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("Cannot change partition column"),
            "{error}"
        );
        assert_eq!(files(&table).await, before);
        assert_eq!(writer.commit_messages().len(), saved.len());
    }
    let error = writer
        .update_columns(
            vec![matched(vec![ids[&0]], vec![None], vec![1], vec![0])],
            vec!["p".into()],
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Cannot change partition column"));
    commit(&table, saved).await;
    assert_eq!(values(&table).await[0].3, 100);
}

#[tokio::test]
async fn upsert_matches_within_partition_even_when_keys_omit_partition() {
    let table = table(&[]).await;
    write_batch(
        &table,
        &batch(
            vec![Some("a"), Some("b"), None],
            vec![1, 1, 2],
            vec![7, 7, 7],
            vec![10, 20, 30],
        ),
    )
    .await;
    let update = table.new_write_builder().new_update().unwrap();
    let input = batch(
        vec![Some("a"), Some("c"), None, Some("a")],
        vec![1, 1, 2, 1],
        vec![7, 7, 7, 7],
        vec![11, 40, 31, 12],
    );
    let messages = update
        .upsert_by_arrow_with_key(vec![input], vec!["id".into()])
        .await
        .unwrap();
    commit(&table, messages).await;
    let mut rows = values(&table).await;
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (None, 2, 7, 31),
            (Some("a".into()), 1, 7, 12),
            (Some("b".into()), 1, 7, 20),
            (Some("c".into()), 1, 7, 40)
        ]
    );
    // A partition-only key targets all rows in that partition; duplicates use
    // the last source row, just as Python's per-partition matching does.
    let input = batch(vec![Some("a")], vec![1], vec![8], vec![80]);
    commit(
        &table,
        update
            .upsert_by_arrow_with_key(vec![input], vec!["p".into(), "q".into()])
            .await
            .unwrap(),
    )
    .await;
    assert!(values(&table).await.contains(&(Some("a".into()), 1, 8, 80)));
}

async fn delete(table: &Table, ids: Vec<i64>) -> Vec<CommitMessage> {
    let mut writer = table.new_write_builder().new_delete().unwrap();
    writer.add_row_ids(ids).unwrap();
    writer.prepare_commit().await.unwrap()
}

#[tokio::test]
async fn external_deletion_vectors_repeat_time_travel_and_abort() {
    for (in_bucket, strategy) in [
        (true, "round-robin"),
        (true, "weight-robin"),
        (true, "entropy-inject"),
        (true, "specific-fs"),
        (false, "round-robin"),
    ] {
        let table = table(&[
            (
                "index-file-in-data-file-dir",
                if in_bucket { "true" } else { "false" },
            ),
            (
                "data-file.external-paths",
                "memory:/external-data/a,memory:/external-data/b",
            ),
            ("data-file.external-paths.strategy", strategy),
            ("data-file.external-paths.weights", "1,3"),
            ("data-file.external-paths.specific-fs", "memory"),
            ("global-index.external-path", "memory:/external-index"),
        ])
        .await;
        seed(&table).await;
        let ids = row_ids(&table).await;
        let messages = delete(&table, vec![ids[&0], ids[&2]]).await;
        let paths = messages
            .iter()
            .flat_map(|message| &message.new_index_files)
            .map(|file| file.external_path.clone().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(paths.len(), 2);
        for path in &paths {
            assert!(path.starts_with(if in_bucket {
                "memory:/external-data/"
            } else {
                "memory:/external-index/"
            }));
            assert!(table.file_io().exists(path).await.unwrap());
            assert_eq!(path.contains("/bucket-"), in_bucket);
        }
        commit(&table, messages).await;
        assert_eq!(row_ids(&table).await.len(), 2);
        // A changed output root must not redirect existing explicit paths.
        let changed = table.copy_with_options(HashMap::from([
            ("data-file.external-paths".into(), "memory:/new-data".into()),
            (
                "global-index.external-path".into(),
                "memory:/new-index".into(),
            ),
        ]));
        let messages = delete(&changed, vec![ids[&1]]).await;
        let staged = messages[0].new_index_files[0]
            .external_path
            .clone()
            .unwrap();
        assert!(staged.contains("/new-"));
        changed
            .new_write_builder()
            .new_commit()
            .abort(&messages)
            .await
            .unwrap();
        assert!(!table.file_io().exists(&staged).await.unwrap());
        for path in &paths {
            assert!(table.file_io().exists(path).await.unwrap());
        }
        commit(&changed, delete(&changed, vec![ids[&1]]).await).await;
        assert_eq!(
            row_ids(&changed).await.keys().copied().collect::<Vec<_>>(),
            vec![3]
        );
        let historical = table
            .copy_with_time_travel(HashMap::from([("scan.snapshot-id".into(), "2".into())]))
            .await
            .unwrap();
        assert_eq!(row_ids(&historical).await.len(), 2);
    }
}

#[tokio::test]
async fn delete_reads_legacy_index_directory_without_changing_manifest_identity() {
    let table = table(&[("index-file-in-data-file-dir", "true")]).await;
    seed(&table).await;
    let ids = row_ids(&table).await;
    let messages = delete(&table, vec![ids[&0]]).await;
    let old = messages[0].new_index_files[0].clone();
    assert!(old.external_path.is_none());
    let bucket_path = format!("{}/p=a/q=1/bucket-0/{}", table.location(), old.file_name);
    let legacy_path = format!("{}/index/{}", table.location(), old.file_name);
    let bytes = table
        .file_io()
        .new_input(&bucket_path)
        .unwrap()
        .read()
        .await
        .unwrap();
    table
        .file_io()
        .new_output(&legacy_path)
        .unwrap()
        .write(bytes)
        .await
        .unwrap();
    table.file_io().delete_file(&bucket_path).await.unwrap();
    commit(&table, messages).await;
    let messages = delete(&table, vec![ids[&1]]).await;
    assert_eq!(messages[0].deleted_index_files, vec![old]);
    commit(&table, messages).await;
    assert_eq!(row_ids(&table).await.len(), 2);
    assert!(table.file_io().exists(&legacy_path).await.unwrap());
    let historical = table
        .copy_with_time_travel(HashMap::from([("scan.snapshot-id".into(), "2".into())]))
        .await
        .unwrap();
    assert_eq!(row_ids(&historical).await.len(), 3);
}

#[tokio::test]
async fn missing_explicit_deletion_vector_never_falls_back_to_local_decoys() {
    let table = table(&[
        ("index-file-in-data-file-dir", "true"),
        ("data-file.external-paths", "memory:/external"),
        ("data-file.external-paths.strategy", "round-robin"),
    ])
    .await;
    seed(&table).await;
    let ids = row_ids(&table).await;
    let messages = delete(&table, vec![ids[&0]]).await;
    let file = messages[0].new_index_files[0].clone();
    let path = file.external_path.as_ref().unwrap();
    let bytes = table
        .file_io()
        .new_input(path)
        .unwrap()
        .read()
        .await
        .unwrap();
    for dir in ["index", "p=a/q=1/bucket-0"] {
        table
            .file_io()
            .new_output(&format!("{}/{dir}/{}", table.location(), file.file_name))
            .unwrap()
            .write(bytes.clone())
            .await
            .unwrap();
    }
    commit(&table, messages).await;
    table.file_io().delete_file(path).await.unwrap();
    let mut writer = table.new_write_builder().new_delete().unwrap();
    writer.add_row_ids(vec![ids[&1]]).unwrap();
    let before = files(&table).await;
    assert!(writer.prepare_commit().await.is_err());
    assert_eq!(files(&table).await, before);
}

#[tokio::test]
async fn later_bucket_failure_aborts_earlier_external_deletion_vectors() {
    let table = table(&[
        ("index-file-in-data-file-dir", "false"),
        ("global-index.external-path", "memory:/external"),
    ])
    .await;
    seed(&table).await;
    let ids = row_ids(&table).await;
    let messages = delete(&table, vec![ids[&0], ids[&2]]).await;
    // The writer processes bucket keys in order. Removing the last bucket's
    // old DV forces failure after staging a replacement for the first bucket.
    let missing = messages.last().unwrap().new_index_files[0]
        .external_path
        .clone()
        .unwrap();
    commit(&table, messages).await;
    table.file_io().delete_file(&missing).await.unwrap();
    let before = files(&table).await;
    let mut writer = table.new_write_builder().new_delete().unwrap();
    writer.add_row_ids(vec![ids[&1], ids[&3]]).unwrap();
    assert!(writer.prepare_commit().await.is_err());
    assert_eq!(files(&table).await, before);
}
