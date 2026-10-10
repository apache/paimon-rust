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

use super::*;
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{IntType, PredicateBuilder, Schema, TableSchema, VarCharType};
use crate::table::TableWrite;
use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use std::sync::Arc;

fn table(partitioned: bool, external: bool) -> Table {
    let location = format!("memory:/index-preparation-{}", uuid::Uuid::new_v4());
    let mut options = HashMap::from([
        ("row-tracking.enabled".into(), "true".into()),
        ("data-evolution.enabled".into(), "true".into()),
        ("global-index.enabled".into(), "true".into()),
        ("global-index.search-mode".into(), "full".into()),
        ("sorted-index.records-per-range".into(), "2".into()),
    ]);
    if external {
        options.insert(
            "global-index.external-path".into(),
            format!("{location}-external"),
        );
    }
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .column("pt", DataType::Int(IntType::new()))
        .options(options);
    if partitioned {
        schema = schema.partition_keys(vec!["pt"]);
    }
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "indexes"),
        location,
        TableSchema::new(0, &schema.build().unwrap()),
        None,
    )
}

async fn append(table: &Table, ids: Vec<i32>, names: Vec<Option<&str>>, pts: Vec<Option<i32>>) {
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowType::Int32, true),
            Field::new("name", ArrowType::Utf8, true),
            Field::new("pt", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int32Array::from(pts)),
        ],
    )
    .unwrap();
    let mut writer = TableWrite::new(table, "data".into()).unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    TableCommit::new(table.clone(), "data".into())
        .commit(messages)
        .await
        .unwrap();
}

async fn read_ids(table: &Table, predicate: Predicate) -> Vec<i32> {
    let mut builder = table.new_read_builder();
    builder
        .with_filter(predicate)
        .with_projection(&["id"])
        .unwrap();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
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

async fn update_name(table: &Table, value: &str) -> Vec<CommitMessage> {
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("_ROW_ID", ArrowType::Int64, false),
            Field::new("name", ArrowType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(StringArray::from(vec![value])),
        ],
    )
    .unwrap();
    table
        .new_write_builder()
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![batch])
        .await
        .unwrap()
}

#[tokio::test]
async fn staged_indexes_reject_target_column_changes_and_preserve_files() {
    for kind in ["btree", "bitmap"] {
        for same_commit in [false, true] {
            let table = table(false, false);
            append(
                &table,
                vec![1, 2],
                vec![Some("a"), Some("b")],
                vec![None; 2],
            )
            .await;
            let mut builder = table.new_sorted_global_index_build_builder();
            builder.with_index_column("name").with_index_type(kind);
            let mut messages = builder.build().await.unwrap();
            let file = messages[0].new_index_files[0].clone();
            let updates = update_name(&table, "changed").await;
            let commit = TableCommit::new(table.clone(), "publish".into());
            if same_commit {
                messages.extend(updates);
            } else {
                commit.commit(updates).await.unwrap();
            }
            let before = table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap()
                .id();
            assert!(commit
                .commit(messages)
                .await
                .unwrap_err()
                .to_string()
                .contains("Global index source conflict"));
            assert_eq!(
                table
                    .snapshot_manager()
                    .get_latest_snapshot()
                    .await
                    .unwrap()
                    .unwrap()
                    .id(),
                before
            );
            assert!(table
                .file_io()
                .exists(&format!("{}/index/{}", table.location(), file.file_name))
                .await
                .unwrap());
        }
    }
}

#[tokio::test]
async fn staged_indexes_reject_deleted_partial_columns_and_removed_source_ranges() {
    for kind in ["btree", "bitmap"] {
        for remove_all in [false, true] {
            let table = table(false, false);
            append(
                &table,
                vec![1, 2],
                vec![Some("a"), Some("b")],
                vec![None; 2],
            )
            .await;
            let updates = update_name(&table, "changed").await;
            let commit = TableCommit::new(table.clone(), "update".into());
            commit.commit(updates.clone()).await.unwrap();
            let mut builder = table.new_sorted_global_index_build_builder();
            builder.with_index_column("name").with_index_type(kind);
            let messages = builder.build().await.unwrap();
            if remove_all {
                commit.overwrite(vec![], None).await.unwrap();
            } else {
                let deletions = updates
                    .into_iter()
                    .map(|update| {
                        let mut message =
                            CommitMessage::new(update.partition, update.bucket, vec![]);
                        message.deleted_files = update.new_files;
                        message
                    })
                    .collect();
                commit.commit(deletions).await.unwrap();
            }
            let error = commit.commit(messages).await.unwrap_err().to_string();
            assert!(
                error.contains(if remove_all {
                    "Global index row ID existence conflict"
                } else {
                    "Global index source conflict"
                }),
                "{error}"
            );
        }
    }
}

#[tokio::test]
async fn staged_index_survives_append_and_unrelated_column_update() {
    let table = table(false, false);
    append(
        &table,
        vec![1, 2],
        vec![Some("a"), Some("b")],
        vec![None; 2],
    )
    .await;
    let mut builder = table.new_sorted_global_index_build_builder();
    builder.with_index_column("name");
    let messages = builder.build().await.unwrap();
    assert_eq!(
        crate::spec::DataEvolutionIndexSourceMeta::deserialize(
            messages[0].new_index_files[0]
                .global_index_meta
                .as_ref()
                .unwrap()
                .source_meta
                .as_ref()
                .unwrap()
        )
        .unwrap()
        .scan_snapshot_id(),
        1
    );
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("_ROW_ID", ArrowType::Int64, false),
            Field::new("id", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int32Array::from(vec![10])),
        ],
    )
    .unwrap();
    let updates = table
        .new_write_builder()
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![batch])
        .await
        .unwrap();
    let commit = TableCommit::new(table.clone(), "data".into());
    commit.commit(updates).await.unwrap();
    append(&table, vec![3], vec![Some("a")], vec![None]).await;
    commit.commit(messages).await.unwrap();
    let predicate = PredicateBuilder::new(table.schema().fields())
        .equal("name", Datum::String("a".into()))
        .unwrap();
    assert_eq!(read_ids(&table, predicate).await, vec![3, 10]);
}

#[tokio::test]
async fn equivalent_empty_partition_encodings_share_source_checks() {
    for partition in [
        vec![],
        vec![0, 0, 0, 0],
        crate::spec::EMPTY_SERIALIZED_ROW.to_vec(),
    ] {
        for changed in [false, true] {
            let table = table(false, false);
            append(&table, vec![1], vec![Some("a")], vec![None]).await;
            let mut builder = table.new_sorted_global_index_build_builder();
            builder.with_index_column("name");
            let mut messages = builder.build().await.unwrap();
            for message in &mut messages {
                message.partition = partition.clone();
            }
            let commit = TableCommit::new(table.clone(), "publish".into());
            if changed {
                commit
                    .commit(update_name(&table, "changed").await)
                    .await
                    .unwrap();
                assert!(commit
                    .commit(messages)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("Global index source conflict"));
            } else {
                commit.commit(messages).await.unwrap();
                let predicate = PredicateBuilder::new(table.schema().fields())
                    .equal("name", Datum::String("a".into()))
                    .unwrap();
                assert_eq!(read_ids(&table, predicate).await, vec![1]);
            }
        }
    }
}

#[tokio::test]
async fn prepared_files_are_invisible_until_explicit_commit() {
    for kind in ["btree", "bitmap"] {
        let table = table(false, false);
        append(
            &table,
            vec![3, 1, 2, 4],
            vec![Some("c"), Some("a"), None, Some("a")],
            vec![None; 4],
        )
        .await;
        let before = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        let mut builder = table.new_sorted_global_index_build_builder();
        builder.with_index_column("name").with_index_type(kind);
        let messages = builder.build().await.unwrap();
        assert_eq!(messages.len(), 2);
        assert!(messages
            .iter()
            .all(|m| m.new_files.is_empty() && m.new_index_files.len() == 1));
        let after_build = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before.id(), after_build.id());
        assert!(after_build.index_manifest().is_none());
        for message in &messages {
            let file = &message.new_index_files[0];
            assert_eq!(file.index_type, kind);
            assert_eq!(file.row_count, 2);
            assert!(table
                .file_io()
                .exists(&format!("{}/index/{}", table.location(), file.file_name))
                .await
                .unwrap());
            assert_eq!(
                CommitMessage::deserialize(14, &message.serialize().unwrap())
                    .unwrap()
                    .new_index_files,
                message.new_index_files
            );
        }
        TableCommit::new(table.clone(), "publish".into())
            .commit(messages)
            .await
            .unwrap();
        let after = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.id(), before.id() + 1);
        assert!(after.index_manifest().is_some());
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("name", Datum::String("a".into()))
            .unwrap();
        assert_eq!(read_ids(&table, predicate).await, vec![1, 4]);
        assert!(builder.build().await.unwrap().is_empty());
        assert_eq!(builder.execute().await.unwrap(), 0);
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap()
                .id(),
            after.id()
        );
    }
}

#[tokio::test]
async fn caller_can_abort_prepared_files_without_changing_snapshot() {
    for kind in ["btree", "bitmap"] {
        let table = table(false, true);
        append(&table, vec![1, 2], vec![Some("a"), None], vec![None; 2]).await;
        let mut builder = table.new_sorted_global_index_build_builder();
        builder.with_index_column("name").with_index_type(kind);
        let messages = builder.build().await.unwrap();
        let path = messages[0].new_index_files[0]
            .external_path
            .as_ref()
            .unwrap();
        assert!(path.starts_with(
            table
                .schema()
                .options()
                .get("global-index.external-path")
                .unwrap()
        ));
        assert!(table.file_io().exists(path).await.unwrap());
        assert!(!table
            .file_io()
            .exists(&format!(
                "{}/index/{}",
                table.location(),
                messages[0].new_index_files[0].file_name
            ))
            .await
            .unwrap());
        TableCommit::new(table.clone(), "discard".into())
            .abort(&messages)
            .await
            .unwrap();
        assert!(!table.file_io().exists(path).await.unwrap());
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap()
                .id(),
            1
        );
        let replacements = builder.build().await.unwrap();
        TableCommit::new(table.clone(), "publish".into())
            .commit(replacements)
            .await
            .unwrap();
        let predicate = PredicateBuilder::new(table.schema().fields())
            .is_null("name")
            .unwrap();
        assert_eq!(read_ids(&table, predicate).await, vec![2]);
    }
}

#[tokio::test]
async fn partition_builds_only_cover_selected_partitions_and_new_rows() {
    for kind in ["btree", "bitmap"] {
        let table = table(true, false);
        append(
            &table,
            vec![1, 2, 3],
            vec![Some("a"), Some("b"), Some("c")],
            vec![Some(0), Some(1), None],
        )
        .await;
        let predicates = PredicateBuilder::new(table.schema().fields());
        let selected = predicates.equal("pt", Datum::Int(1)).unwrap();
        let mut builder = table.new_sorted_global_index_build_builder();
        builder
            .with_index_column("id")
            .with_index_type(kind)
            .with_partition_filter(selected.clone())
            .unwrap();
        let first = builder.build().await.unwrap();
        assert_eq!(
            first
                .iter()
                .map(|m| m.new_index_files.iter().map(|f| f.row_count).sum::<i64>())
                .sum::<i64>(),
            1
        );
        TableCommit::new(table.clone(), "partition-one".into())
            .commit(first)
            .await
            .unwrap();
        assert!(builder.build().await.unwrap().is_empty());
        append(
            &table,
            vec![4, 5],
            vec![Some("d"), Some("e")],
            vec![Some(1), Some(0)],
        )
        .await;
        assert_eq!(builder.execute().await.unwrap(), 1);
        assert!(builder.build().await.unwrap().is_empty());

        let mut all = table.new_sorted_global_index_build_builder();
        all.with_index_column("id").with_index_type(kind);
        let remaining = all.build().await.unwrap();
        assert_eq!(
            remaining
                .iter()
                .flat_map(|m| &m.new_index_files)
                .map(|f| f.row_count)
                .sum::<i64>(),
            3
        );
        TableCommit::new(table.clone(), "all-partitions".into())
            .commit(remaining)
            .await
            .unwrap();
        assert!(all.build().await.unwrap().is_empty());
        assert_eq!(read_ids(&table, selected).await, vec![2, 4]);
    }
}

#[tokio::test]
async fn empty_and_disjoint_partition_builds_publish_nothing() {
    let table = table(true, false);
    let mut builder = table.new_sorted_global_index_build_builder();
    builder.with_index_column("id");
    assert!(builder.build().await.unwrap().is_empty());
    assert_eq!(builder.execute().await.unwrap(), 0);
    assert!(table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .is_none());
    append(&table, vec![1], vec![Some("a")], vec![Some(1)]).await;
    let predicates = PredicateBuilder::new(table.schema().fields());
    for value in [1, 2] {
        builder
            .with_partition_filter(predicates.equal("pt", Datum::Int(value)).unwrap())
            .unwrap();
    }
    assert!(builder.build().await.unwrap().is_empty());
    assert_eq!(builder.execute().await.unwrap(), 0);
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap()
            .id(),
        1
    );
    let data_filter = predicates.equal("id", Datum::Int(1)).unwrap();
    assert!(builder.with_partition_filter(data_filter).is_err());
    let unpartitioned = self::table(false, false);
    let filter = PredicateBuilder::new(unpartitioned.schema().fields())
        .is_null("pt")
        .unwrap();
    assert!(unpartitioned
        .new_sorted_global_index_build_builder()
        .with_partition_filter(filter)
        .is_err());
}
