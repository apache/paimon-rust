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

use super::{native_full_text_options, serialize_index_meta, ShardCursor};
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{
    DataType, IndexManifest, IndexManifestEntry, IntType, Schema, TableSchema, VarCharType,
    ROW_ID_FIELD_NAME,
};
use crate::table::{RowRange, SnapshotManager, Table, TableCommit, TableWrite};
use crate::Error;
use arrow_array::{ArrayRef, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use std::collections::HashMap;
use std::sync::Arc;

fn table_options(rows_per_shard: &str) -> HashMap<String, String> {
    HashMap::from([
        ("row-tracking.enabled".to_string(), "true".to_string()),
        ("data-evolution.enabled".to_string(), "true".to_string()),
        ("global-index.enabled".to_string(), "true".to_string()),
        (
            "global-index.row-count-per-shard".to_string(),
            rows_per_shard.to_string(),
        ),
    ])
}

fn test_table(table_path: &str, options: HashMap<String, String>) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .options(options)
        .build()
        .unwrap();
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "test_table"),
        table_path.to_string(),
        TableSchema::new(0, &schema),
        None,
    )
}

async fn setup_dirs(table: &Table) {
    for dir in ["snapshot", "manifest"] {
        table
            .file_io()
            .mkdirs(&format!("{}/{dir}/", table.location()))
            .await
            .unwrap();
    }
}

fn name_batch(ids: Vec<i32>, names: Vec<Option<&str>>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
        ],
    )
    .unwrap()
}

async fn write_rows(table: &Table, ids: Vec<i32>, names: Vec<Option<&str>>) {
    let mut table_write = TableWrite::new(table, "test-user".to_string()).unwrap();
    table_write
        .write_arrow_batch(&name_batch(ids, names))
        .await
        .unwrap();
    let messages = table_write.prepare_commit().await.unwrap();
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(messages)
        .await
        .unwrap();
}

async fn full_text_entries(table: &Table) -> Vec<IndexManifestEntry> {
    let snapshot = SnapshotManager::new(table.file_io().clone(), table.location().to_string())
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let Some(index_manifest) = snapshot.index_manifest() else {
        return Vec::new();
    };
    let mut entries = IndexManifest::read(
        table.file_io(),
        &format!("{}/manifest/{index_manifest}", table.location()),
    )
    .await
    .unwrap()
    .into_iter()
    .filter(|entry| entry.index_file.index_type == "full-text")
    .collect::<Vec<_>>();
    entries.sort_by_key(|entry| {
        entry
            .index_file
            .global_index_meta
            .as_ref()
            .unwrap()
            .row_range_start
    });
    entries
}

async fn search(table: &Table, query: &str) -> Vec<RowRange> {
    table
        .new_full_text_search_builder()
        .with_text_column("name")
        .with_query_text(query)
        .with_limit(10)
        .execute()
        .await
        .unwrap()
}

fn text_batch(row_ids: Vec<i64>, names: Vec<Option<&str>>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("name", ArrowDataType::Utf8, true),
        ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(Int64Array::from(row_ids)) as ArrayRef,
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_execute_builds_shards_readable_by_full_text_search() {
    let table = test_table("memory:/test_full_text_build_e2e", table_options("2"));
    setup_dirs(&table).await;
    write_rows(
        &table,
        vec![1, 2, 3, 4, 5],
        vec![
            Some("apache paimon lake"),
            Some("rust engine"),
            None,
            Some("paimon rust reader"),
            Some("streaming lake"),
        ],
    )
    .await;
    assert!(search(&table, "paimon").await.is_empty());

    let file_count = table
        .new_full_text_index_build_builder()
        .with_index_column("name")
        .execute()
        .await
        .unwrap();
    assert_eq!(file_count, 3);

    let entries = full_text_entries(&table).await;
    let shards = entries
        .iter()
        .map(|entry| {
            let meta = entry.index_file.global_index_meta.as_ref().unwrap();
            (
                meta.row_range_start,
                meta.row_range_end,
                entry.index_file.row_count,
            )
        })
        .collect::<Vec<_>>();
    // NULL text (row 2) still counts towards the shard's row count, like Java.
    assert_eq!(shards, vec![(0, 1, 2), (2, 3, 2), (4, 4, 1)]);
    for entry in &entries {
        let file = &entry.index_file;
        assert!(file.file_name.starts_with("full-text-global-index-"));
        assert!(file.file_size > 0);
        let meta = file.global_index_meta.as_ref().unwrap();
        assert_eq!(meta.index_field_id, 1);
        assert_eq!(meta.index_meta.as_deref(), Some(b"{}".as_slice()));
        assert!(table
            .file_io()
            .exists(&format!("{}/index/{}", table.location(), file.file_name))
            .await
            .unwrap());
    }

    assert_eq!(
        search(&table, "paimon").await,
        vec![RowRange::new(0, 0), RowRange::new(3, 3)]
    );
    assert_eq!(
        search(&table, "lake").await,
        vec![RowRange::new(0, 0), RowRange::new(4, 4)]
    );
}

#[tokio::test]
async fn test_execute_only_indexes_new_rows() {
    let table = test_table(
        "memory:/test_full_text_build_incremental",
        table_options("100"),
    );
    setup_dirs(&table).await;
    write_rows(&table, vec![1, 2], vec![Some("paimon one"), Some("other")]).await;

    let builder = || {
        let mut builder = table.new_full_text_index_build_builder();
        builder.with_index_column("name");
        builder
    };
    assert_eq!(builder().execute().await.unwrap(), 1);
    // Nothing new to index.
    assert_eq!(builder().execute().await.unwrap(), 0);

    write_rows(&table, vec![3, 4], vec![Some("paimon two"), None]).await;
    assert_eq!(builder().execute().await.unwrap(), 1);

    let ranges = full_text_entries(&table)
        .await
        .iter()
        .map(|entry| {
            let meta = entry.index_file.global_index_meta.as_ref().unwrap();
            (meta.row_range_start, meta.row_range_end)
        })
        .collect::<Vec<_>>();
    assert_eq!(ranges, vec![(0, 1), (2, 3)]);
    assert_eq!(
        search(&table, "paimon").await,
        vec![RowRange::new(0, 0), RowRange::new(2, 2)]
    );
}

#[tokio::test]
async fn test_execute_indexes_all_null_shard() {
    let table = test_table("memory:/test_full_text_build_all_null", table_options("2"));
    setup_dirs(&table).await;
    write_rows(&table, vec![1, 2, 3], vec![None, None, Some("paimon")]).await;

    // Java's writer counts NULL rows, so an all-NULL shard still yields an
    // (empty) index file that marks the range as indexed.
    let file_count = table
        .new_full_text_index_build_builder()
        .with_index_column("name")
        .execute()
        .await
        .unwrap();
    assert_eq!(file_count, 2);
    assert_eq!(search(&table, "paimon").await, vec![RowRange::new(2, 2)]);
}

#[tokio::test]
async fn test_execute_passes_prefixed_options_to_native_writer() {
    let table = test_table("memory:/test_full_text_build_options", table_options("10"));
    setup_dirs(&table).await;
    write_rows(&table, vec![1], vec![Some("Running Paimon")]).await;

    let file_count = table
        .new_full_text_index_build_builder()
        .with_index_column("name")
        .with_options(HashMap::from([
            ("full-text.stem".to_string(), "false".to_string()),
            (
                "global-index.row-count-per-shard".to_string(),
                "1".to_string(),
            ),
        ]))
        .execute()
        .await
        .unwrap();
    assert_eq!(file_count, 1);

    let entries = full_text_entries(&table).await;
    let meta = entries[0].index_file.global_index_meta.as_ref().unwrap();
    assert_eq!(
        meta.index_meta.as_deref(),
        Some(br#"{"stem":"false"}"#.as_slice())
    );
    // Without stemming, "run" no longer matches "Running".
    assert!(search(&table, "run").await.is_empty());
    assert_eq!(search(&table, "running").await, vec![RowRange::new(0, 0)]);
}

#[tokio::test]
async fn test_execute_rejects_invalid_native_options_before_reading() {
    let table = test_table(
        "memory:/test_full_text_build_bad_options",
        table_options("10"),
    );
    setup_dirs(&table).await;

    let err = table
        .new_full_text_index_build_builder()
        .with_index_column("name")
        .with_options(HashMap::from([(
            "full-text.tokenizer".to_string(),
            "no-such-tokenizer".to_string(),
        )]))
        .execute()
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::ConfigInvalid { ref message } if message.contains("Invalid full-text index options")),
        "{err:?}"
    );
}

#[tokio::test]
async fn test_execute_rejects_unsupported_tables_and_columns() {
    let expect_err = |table: Table, column: &'static str| async move {
        table
            .new_full_text_index_build_builder()
            .with_index_column(column)
            .execute()
            .await
            .unwrap_err()
    };

    let mut options = table_options("10");
    // The schema itself refuses data evolution without row tracking.
    options.remove("row-tracking.enabled");
    options.remove("data-evolution.enabled");
    let err = expect_err(test_table("memory:/ft_no_row_tracking", options), "name").await;
    assert!(
        matches!(err, Error::DataInvalid { ref message, .. } if message.contains("row-tracking.enabled")),
        "{err:?}"
    );

    let mut options = table_options("10");
    options.insert("deletion-vectors.enabled".to_string(), "true".to_string());
    let err = expect_err(test_table("memory:/ft_dv", options), "name").await;
    assert!(
        matches!(err, Error::Unsupported { ref message } if message.contains("deletion-vectors")),
        "{err:?}"
    );

    let err = expect_err(
        test_table("memory:/ft_int_column", table_options("10")),
        "id",
    )
    .await;
    assert!(
        matches!(err, Error::Unsupported { ref message } if message.contains("character string column")),
        "{err:?}"
    );

    let err = expect_err(
        test_table("memory:/ft_missing", table_options("10")),
        "nope",
    )
    .await;
    assert!(matches!(err, Error::ColumnNotExist { .. }), "{err:?}");

    let pk_schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .primary_key(["id"])
        .build()
        .unwrap();
    let pk_table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "pk_table"),
        "memory:/ft_pk".to_string(),
        TableSchema::new(0, &pk_schema),
        None,
    );
    let err = expect_err(pk_table, "name").await;
    assert!(
        matches!(err, Error::Unsupported { ref message } if message.contains("primary-key")),
        "{err:?}"
    );
}

#[tokio::test]
async fn test_execute_requires_index_column() {
    let table = test_table("memory:/ft_no_column", table_options("10"));
    let err = table
        .new_full_text_index_build_builder()
        .execute()
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::DataInvalid { ref message, .. } if message.contains("column is required")),
        "{err:?}"
    );
}

#[test]
fn test_cursor_uses_shard_relative_ids_and_counts_nulls() {
    let mut cursor = ShardCursor::new(10, 13);
    let documents = cursor
        .documents(
            &text_batch(vec![10, 11, 12], vec![Some("a"), None, Some("c")]),
            "name",
        )
        .unwrap();
    assert_eq!(documents, vec![(0, "a".to_string()), (2, "c".to_string())]);
    assert_eq!(cursor.row_count, 3);
}

#[test]
fn test_cursor_skips_rows_outside_the_shard() {
    let mut cursor = ShardCursor::new(10, 11);
    let documents = cursor
        .documents(
            &text_batch(
                vec![8, 10, 11, 12],
                vec![Some("x"), Some("a"), Some("b"), Some("y")],
            ),
            "name",
        )
        .unwrap();
    assert_eq!(documents, vec![(0, "a".to_string()), (1, "b".to_string())]);
    assert_eq!(cursor.row_count, 2);
}

#[test]
fn test_cursor_rejects_decreasing_row_ids_across_batches() {
    let mut cursor = ShardCursor::new(0, 10);
    cursor
        .documents(&text_batch(vec![0, 5], vec![Some("a"), Some("b")]), "name")
        .unwrap();
    let err = cursor
        .documents(&text_batch(vec![4], vec![Some("c")]), "name")
        .unwrap_err();
    assert!(
        matches!(err, Error::DataInvalid { ref message, .. } if message.contains("not monotonically increasing")),
        "{err:?}"
    );
}

#[test]
fn test_cursor_rejects_null_row_id() {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("name", ArrowDataType::Utf8, true),
        ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("a")])) as ArrayRef,
            Arc::new(Int64Array::from(vec![None::<i64>])) as ArrayRef,
        ],
    )
    .unwrap();
    let err = ShardCursor::new(0, 10)
        .documents(&batch, "name")
        .unwrap_err();
    assert!(
        matches!(err, Error::DataInvalid { ref message, .. } if message.contains("null _ROW_ID")),
        "{err:?}"
    );
}

#[test]
fn test_cursor_accepts_large_utf8_and_rejects_non_string() {
    let large = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("name", ArrowDataType::LargeUtf8, true),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(LargeStringArray::from(vec![Some("a")])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0])) as ArrayRef,
        ],
    )
    .unwrap();
    assert_eq!(
        ShardCursor::new(0, 0).documents(&large, "name").unwrap(),
        vec![(0, "a".to_string())]
    );

    let ints = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("name", ArrowDataType::Int32, true),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0])) as ArrayRef,
        ],
    )
    .unwrap();
    let err = ShardCursor::new(0, 0).documents(&ints, "name").unwrap_err();
    assert!(
        matches!(err, Error::DataInvalid { ref message, .. } if message.contains("requires a string column")),
        "{err:?}"
    );
}

#[test]
fn test_native_options_strip_prefix_and_meta_is_sorted_flat_json() {
    let native = native_full_text_options(&HashMap::from([
        ("full-text.tokenizer".to_string(), "jieba".to_string()),
        ("full-text.lower-case".to_string(), "false".to_string()),
        (
            "full-text-index.search-mode".to_string(),
            "full".to_string(),
        ),
        ("bucket".to_string(), "-1".to_string()),
    ]));
    assert_eq!(
        native,
        HashMap::from([
            ("tokenizer".to_string(), "jieba".to_string()),
            ("lower-case".to_string(), "false".to_string()),
        ])
    );
    assert_eq!(
        serialize_index_meta(&native).unwrap(),
        br#"{"lower-case":"false","tokenizer":"jieba"}"#.to_vec()
    );
}
