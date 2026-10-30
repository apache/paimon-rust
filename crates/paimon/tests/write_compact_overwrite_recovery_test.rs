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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use common::incremental_helpers::{
    make_batch, persist_table_schema, pk_schema, setup_dirs, write_batch,
};
use futures::TryStreamExt;
use paimon::catalog::{FileSystemCatalog, Identifier};
use paimon::spec::CommitKind;
use paimon::table::{CommitCallback, CommitCallbackContext, IncrementalScanMode, Table};

async fn table() -> Table {
    let path = format!(
        "memory:/write_compact_overwrite_recovery/{}",
        uuid::Uuid::new_v4()
    );
    let mut options = paimon::Options::new();
    options.set("warehouse", &path);
    options.set("local-cache.enabled", "true");
    let io = FileSystemCatalog::new(options).unwrap().file_io().clone();
    let table = Table::new(
        io.clone(),
        Identifier::new("default", "write_compact_overwrite_recovery"),
        path.clone(),
        pk_schema(&[("write-only", "false"), ("changelog-producer", "lookup")]),
        None,
    );
    setup_dirs(&io, &path).await;
    persist_table_schema(&io, &path, table.schema()).await;
    table
}

struct FailOverwriteOnce(AtomicBool);

#[async_trait::async_trait]
impl CommitCallback for FailOverwriteOnce {
    async fn call(&self, context: &CommitCallbackContext) -> paimon::Result<()> {
        if context.snapshot.commit_kind() == &CommitKind::OVERWRITE
            && !self.0.swap(true, Ordering::SeqCst)
        {
            return Err(paimon::Error::UnexpectedError {
                message: "injected overwrite callback failure".into(),
                source: None,
            });
        }
        Ok(())
    }
}

async fn files(table: &Table) -> Vec<(String, i32)> {
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut files = plan
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .map(|file| (file.file_name.clone(), file.level))
        .collect::<Vec<_>>();
    files.sort_unstable();
    files
}

async fn events(table: &Table, start: i64, end: i64) -> Vec<(String, i32, i32)> {
    let plan = table
        .new_read_builder()
        .new_incremental_scan(IncrementalScanMode::Changelog, start, end)
        .plan_combined()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow_with_row_kind(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut events = Vec::new();
    for batch in batches {
        let kinds = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
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
        events.extend((0..batch.num_rows()).map(|row| {
            (
                kinds.value(row).to_string(),
                ids.value(row),
                values.value(row),
            )
        }));
    }
    events.sort_unstable();
    events
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
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn overwrite_recovery_preserves_later_lookup_append_and_its_changelog() {
    let table = table().await;
    let builder = table
        .new_write_builder()
        .with_commit_user("overwrite-job")
        .unwrap()
        .with_overwrite();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![1], vec![10]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let mut commit = builder.new_commit();
    commit
        .add_commit_callback(Arc::new(FailOverwriteOnce(AtomicBool::new(false))))
        .unwrap();
    let error = commit
        .overwrite_with_identifier(messages.clone(), None, 7)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected overwrite callback failure"),
        "{error}"
    );
    let source = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.id(), 1);
    assert_eq!(source.commit_kind(), &CommitKind::OVERWRITE);

    // Another writer leaves a pending lookup event in the same bucket while
    // the overwrite's data has committed but its automatic round has not run.
    let write_only = table.copy_with_options(HashMap::from([("write-only".into(), "true".into())]));
    let before_append = files(&table).await;
    write_batch(&write_only, &make_batch(vec![2], vec![20])).await;
    let pending = files(&table)
        .await
        .into_iter()
        .find(|file| !before_append.contains(file))
        .unwrap();
    assert_eq!(pending.1, 0);
    assert!(events(&table, source.id(), 2).await.is_empty());

    commit
        .overwrite_with_identifier(messages, None, 7)
        .await
        .unwrap();
    let recovered = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.commit_kind(), &CommitKind::COMPACT);
    assert!(
        files(&table).await.contains(&pending),
        "overwrite recovery must leave the later append's original L0 file pending"
    );
    assert!(events(&table, source.id(), recovered.id()).await.is_empty());

    let compact = table.new_compact_builder();
    let plan = compact.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 1);
    let executor = compact.new_executor().unwrap();
    let result = executor.execute(&plan.tasks()[0]).await.unwrap();
    compact
        .new_commit()
        .unwrap()
        .commit_compact(vec![result])
        .await
        .unwrap();
    let end = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        events(&table, source.id(), end).await,
        vec![("+I".to_string(), 2, 20)],
        "later append must publish its INSERT while overwrite rows stay suppressed"
    );
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn overwrite_recovery_does_not_resurrect_inputs_replaced_by_a_later_overwrite() {
    let table = table().await;
    let builder = table
        .new_write_builder()
        .with_commit_user("original-overwrite-job")
        .unwrap()
        .with_overwrite();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![1], vec![10]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let mut commit = builder.new_commit();
    commit
        .add_commit_callback(Arc::new(FailOverwriteOnce(AtomicBool::new(false))))
        .unwrap();
    let error = commit
        .overwrite_with_identifier(messages.clone(), None, 7)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected overwrite callback failure"),
        "{error}"
    );
    let source = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.id(), 1);
    assert_eq!(source.commit_kind(), &CommitKind::OVERWRITE);

    // A later overwrite replaces the original input before its failed caller
    // retries. The old compact round no longer owns any active source files.
    let write_only = table.copy_with_options(HashMap::from([("write-only".into(), "true".into())]));
    let replacement = write_only.new_write_builder().with_overwrite();
    let mut writer = replacement.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![2], vec![20]))
        .await
        .unwrap();
    let replacement_messages = writer.prepare_commit().await.unwrap();
    replacement
        .new_commit()
        .overwrite_with_identifier(replacement_messages, None, 8)
        .await
        .unwrap();
    let replaced = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replaced.id(), 2);
    assert_eq!(replaced.commit_kind(), &CommitKind::OVERWRITE);
    let replacement_files = files(&table).await;
    assert_eq!(replacement_files.len(), 1);
    assert_eq!(replacement_files[0].1, 0);
    assert_eq!(rows(&table).await, vec![(2, 20)]);

    commit
        .overwrite_with_identifier(messages, None, 7)
        .await
        .unwrap();
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(replaced.id()),
        "retrying obsolete overwrite work must not publish another snapshot"
    );
    assert_eq!(files(&table).await, replacement_files);
    assert_eq!(rows(&table).await, vec![(2, 20)]);
}
