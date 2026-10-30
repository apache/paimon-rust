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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use common::incremental_helpers::{make_batch, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::io::FileIOBuilder;
use paimon::spec::{CommitKind, DataType, IntType, Schema, Snapshot, TableSchema};
use paimon::table::{CommitCallback, CommitCallbackContext, CommitMessage, Table};

async fn table(primary_key: bool, options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("bucket", if primary_key { "1" } else { "-1" })
        .option("compaction.min.file-num", "2");
    if primary_key {
        schema = schema.primary_key(["id"]);
    }
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    let path = format!("memory:/write_compact/{}", uuid::Uuid::new_v4());
    let io = FileIOBuilder::new("memory").build().unwrap();
    let table = Table::new(
        io.clone(),
        Identifier::new("default", "write_compact"),
        path.clone(),
        TableSchema::new(0, &schema.build().unwrap()),
        None,
    );
    setup_dirs(&io, &path).await;
    persist_table_schema(&io, &path, table.schema()).await;
    table
}

async fn latest(table: &Table) -> Snapshot {
    table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap()
}

async fn rows(table: &Table) -> Vec<(i32, i32)> {
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
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|i| (ids.value(i), values.value(i))));
    }
    rows.sort_unstable();
    rows
}

async fn messages(table: &Table, value: i32) -> Vec<CommitMessage> {
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![1], vec![value]))
        .await
        .unwrap();
    writer.prepare_commit().await.unwrap()
}

#[tokio::test]
async fn append_compacts_by_default_and_explicit_false_but_true_only_writes() {
    for (options, enabled) in [
        (vec![], true),
        (vec![("write-only", "false")], true),
        (vec![("write-only", "true")], false),
        (vec![("write.compaction-skip", "true")], false),
        (vec![("write.compaction-skip", "false")], true),
        (
            vec![("write-only", "FALSE"), ("write.compaction-skip", "true")],
            true,
        ),
    ] {
        let table = table(false, &options).await;
        write_batch(&table, &make_batch(vec![1, 1], vec![10, 10])).await;
        assert_eq!(
            latest(&table).await.id(),
            1,
            "below the file-count threshold"
        );
        write_batch(&table, &make_batch(vec![2], vec![20])).await;
        let snapshot = latest(&table).await;
        assert_eq!(snapshot.id(), if enabled { 3 } else { 2 });
        assert_eq!(
            snapshot.commit_kind(),
            if enabled {
                &CommitKind::COMPACT
            } else {
                &CommitKind::APPEND
            }
        );
        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        assert_eq!(
            plan.splits().iter().flat_map(|s| s.data_files()).count(),
            if enabled { 1 } else { 2 }
        );
        assert_eq!(rows(&table).await, vec![(1, 10), (1, 10), (2, 20)]);
        if enabled {
            assert_eq!(snapshot.delta_record_count(), Some(0));
            let before = table.copy_with_pinned_snapshot(Some(
                &table.snapshot_manager().get_snapshot(2).await.unwrap(),
            ));
            assert_eq!(rows(&before).await, rows(&table).await);
        }
        table
            .new_write_builder()
            .new_commit()
            .commit(vec![])
            .await
            .unwrap();
        assert_eq!(latest(&table).await.id(), snapshot.id());
    }
}

#[tokio::test]
async fn primary_key_compaction_preserves_updates_and_reuses_batch_committer() {
    for bucket in ["1", "-1"] {
        let table = table(
            true,
            &[("bucket", bucket), ("compaction.force-up-level-0", "true")],
        )
        .await;
        let commit = table.new_write_builder().new_commit();
        for value in [10, 20] {
            commit.commit(messages(&table, value).await).await.unwrap();
            assert_eq!(latest(&table).await.commit_kind(), &CommitKind::COMPACT);
            assert_eq!(rows(&table).await, vec![(1, value)]);
        }
        assert_eq!(latest(&table).await.id(), 4);
        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        assert!(plan
            .splits()
            .iter()
            .flat_map(|s| s.data_files())
            .all(|f| f.level > 0));
    }
}

struct FailOnce {
    kind: CommitKind,
    failed: AtomicBool,
}

#[async_trait::async_trait]
impl CommitCallback for FailOnce {
    async fn call(&self, context: &CommitCallbackContext) -> paimon::Result<()> {
        if context.snapshot.commit_kind() == &self.kind && !self.failed.swap(true, Ordering::SeqCst)
        {
            return Err(paimon::Error::UnexpectedError {
                message: "injected callback failure".into(),
                source: None,
            });
        }
        Ok(())
    }
}

#[tokio::test]
async fn retry_finishes_compaction_without_repeating_data_or_compact_snapshots() {
    for (kind, recovery_batch) in [
        (CommitKind::APPEND, false),
        (CommitKind::COMPACT, false),
        (CommitKind::APPEND, true),
        (CommitKind::COMPACT, true),
    ] {
        // Outputs remain eligible: only the recovery identity prevents a
        // second COMPACT snapshot when the original round already committed.
        let table = table(false, &[("target-file-row-num", "1")]).await;
        write_batch(&table, &make_batch(vec![0], vec![0])).await;
        let builder = table.new_write_builder();
        let mut commit = builder.new_commit();
        commit
            .add_commit_callback(Arc::new(FailOnce {
                kind,
                failed: AtomicBool::new(false),
            }))
            .unwrap();
        let messages = messages(&table, 10).await;
        let error = commit
            .commit_with_identifier(messages.clone(), 7)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("injected callback failure"),
            "{error}"
        );
        assert_eq!(rows(&table).await, vec![(0, 0), (1, 10)]);
        if recovery_batch {
            assert_eq!(
                commit
                    .filter_and_commit(vec![(7, messages.clone())])
                    .await
                    .unwrap(),
                0
            );
        } else {
            commit
                .filter_and_commit_with_identifier(messages.clone(), 7)
                .await
                .unwrap();
        }
        assert_eq!(latest(&table).await.id(), 3);
        assert_eq!(latest(&table).await.commit_kind(), &CommitKind::COMPACT);
        commit
            .filter_and_commit_with_identifier(messages, 7)
            .await
            .unwrap();
        assert_eq!(latest(&table).await.id(), 3);
        assert_eq!(rows(&table).await, vec![(0, 0), (1, 10)]);
    }
}

#[tokio::test]
async fn overwrite_compacts_replacement_files_without_resurrecting_old_data() {
    let table = table(false, &[]).await;
    write_batch(&table, &make_batch(vec![99], vec![990])).await;
    let builder = table.new_write_builder().with_overwrite();
    let mut messages = Vec::new();
    for value in [10, 20] {
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![1], vec![value]))
            .await
            .unwrap();
        messages.extend(writer.prepare_commit().await.unwrap());
    }
    let commit = builder.new_commit();
    commit
        .overwrite_with_identifier(messages.clone(), None, 5)
        .await
        .unwrap();
    assert_eq!(latest(&table).await.id(), 3);
    assert_eq!(latest(&table).await.commit_kind(), &CommitKind::COMPACT);
    assert_eq!(rows(&table).await, vec![(1, 10), (1, 20)]);
    commit
        .overwrite_with_identifier(messages, None, 5)
        .await
        .unwrap();
    assert_eq!(latest(&table).await.id(), 3);
    commit.truncate_table().await.unwrap();
    assert_eq!(latest(&table).await.id(), 4);
    assert_eq!(latest(&table).await.commit_kind(), &CommitKind::OVERWRITE);
    assert!(rows(&table).await.is_empty());
}

#[tokio::test]
async fn historical_read_selectors_do_not_hide_newly_committed_files() {
    let table = table(false, &[]).await;
    write_batch(&table, &make_batch(vec![0], vec![0])).await;
    let historical = table.copy_with_pinned_snapshot(Some(&latest(&table).await));
    let evolved = TableSchema::new(
        1,
        &Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("extra", DataType::Int(IntType::new()))
            .option("compaction.min.file-num", "2")
            .build()
            .unwrap(),
    );
    persist_table_schema(table.file_io(), table.location(), &evolved).await;
    let current = table.copy_with_resolved_schema(evolved, "main").unwrap();
    write_batch(&historical, &make_batch(vec![1], vec![10])).await;
    assert_eq!(latest(&table).await.id(), 3);
    assert_eq!(latest(&table).await.commit_kind(), &CommitKind::COMPACT);
    assert_eq!(rows(&table).await, vec![(0, 0), (1, 10)]);
    assert_eq!(rows(&historical).await, vec![(0, 0)]);
    assert_eq!(latest(&current).await.schema_id(), 1);
    let plan = current.new_read_builder().new_scan().plan().await.unwrap();
    assert!(plan
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .all(|file| file.schema_id == 1));
}

#[tokio::test]
async fn invalid_compaction_configuration_fails_before_data_publication() {
    for (primary_key, key, value) in [
        (false, "write-only", "invalid"),
        (false, "compaction.min.file-num", "1"),
        (true, "compaction.force-up-level-0", "invalid"),
    ] {
        let table = table(primary_key, &[(key, value)]).await;
        let error = table
            .new_write_builder()
            .new_commit()
            .commit(messages(&table, 10).await)
            .await
            .unwrap_err();
        assert!(error.to_string().contains(key), "{error}");
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            None
        );
    }
}

#[tokio::test]
async fn modes_without_compaction_retain_write_support() {
    let unsupported = table(false, &[("row-tracking.enabled", "true")]).await;
    for value in [10, 20] {
        write_batch(&unsupported, &make_batch(vec![1], vec![value])).await;
    }
    assert_eq!(latest(&unsupported).await.id(), 2);
    assert_eq!(
        latest(&unsupported).await.commit_kind(),
        &CommitKind::APPEND
    );
    assert_eq!(rows(&unsupported).await, vec![(1, 10), (1, 20)]);

    // Ordinary append compaction cannot preserve global indexes. This is
    // discovered from the snapshot, rather than from the table options.
    let indexed = table(false, &[]).await;
    let mut messages = messages(&indexed, 10).await;
    messages[0]
        .new_index_files
        .push(paimon::spec::IndexFileMeta {
            index_type: "btree".into(),
            file_name: "unused.index".into(),
            file_size: 128,
            row_count: 1,
            deletion_vectors_ranges: None,
            external_path: None,
            global_index_meta: Some(paimon::spec::GlobalIndexMeta {
                row_range_start: 0,
                row_range_end: 0,
                index_field_id: 0,
                extra_field_ids: None,
                source_meta: None,
                index_meta: None,
            }),
        });
    indexed
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    write_batch(&indexed, &make_batch(vec![1], vec![20])).await;
    assert_eq!(latest(&indexed).await.id(), 2);
    assert_eq!(rows(&indexed).await, vec![(1, 10), (1, 20)]);
}

#[tokio::test]
async fn data_evolution_compaction_preserves_assigned_row_ids() {
    let table = table(
        false,
        &[
            ("row-tracking.enabled", "true"),
            ("data-evolution.enabled", "true"),
        ],
    )
    .await;
    for value in [10, 20] {
        write_batch(&table, &make_batch(vec![1], vec![value])).await;
    }
    assert_eq!(latest(&table).await.id(), 3);
    assert_eq!(latest(&table).await.commit_kind(), &CommitKind::COMPACT);
    assert_eq!(rows(&table).await, vec![(1, 10), (1, 20)]);
    let mut read = table.new_read_builder();
    read.with_projection(&["_ROW_ID"]).unwrap();
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
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
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1]);
}
