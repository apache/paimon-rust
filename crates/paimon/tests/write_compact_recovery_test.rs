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

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use arrow_array::{Int32Array, RecordBatch};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::io::FileIOBuilder;
use paimon::spec::{BinaryRow, CommitKind, DataType, IntType, Schema, TableSchema};
use paimon::table::{CommitCallback, CommitCallbackContext, Table};

async fn table(partitioned: bool) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("write-only", "false")
        .option("compaction.min.file-num", "2");
    if partitioned {
        schema = schema
            .column("pt", DataType::Int(IntType::new()))
            .partition_keys(["pt"])
            .option("target-file-row-num", "1");
    }
    let path = format!("memory:/write_compact_recovery/{}", uuid::Uuid::new_v4());
    let io = FileIOBuilder::new("memory").build().unwrap();
    let table = Table::new(
        io.clone(),
        Identifier::new("default", "write_compact_recovery"),
        path.clone(),
        TableSchema::new(0, &schema.build().unwrap()),
        None,
    );
    io.mkdirs(&format!("{path}/schema")).await.unwrap();
    io.mkdirs(&format!("{path}/snapshot")).await.unwrap();
    io.new_output(&table.schema_manager().schema_path(0))
        .unwrap()
        .write(serde_json::to_vec(table.schema()).unwrap().into())
        .await
        .unwrap();
    table
}

fn batch(ids: Vec<i32>, partitions: Option<Vec<i32>>) -> RecordBatch {
    let values = ids.iter().map(|id| id * 10).collect::<Vec<_>>();
    let mut columns = vec![
        (
            "id",
            Arc::new(Int32Array::from(ids)) as arrow_array::ArrayRef,
        ),
        (
            "value",
            Arc::new(Int32Array::from(values)) as arrow_array::ArrayRef,
        ),
    ];
    if let Some(partitions) = partitions {
        columns.push((
            "pt",
            Arc::new(Int32Array::from(partitions)) as arrow_array::ArrayRef,
        ));
    }
    RecordBatch::try_from_iter(columns).unwrap()
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
        rows.extend((0..batch.num_rows()).map(|i| (ids.value(i), values.value(i))));
    }
    rows.sort_unstable();
    rows
}

async fn publish(table: &Table, user: &str, identifier: i64, id: i32, recover: bool) {
    let builder = table.new_write_builder().with_commit_user(user).unwrap();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&batch(vec![id], None))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let commit = builder.new_commit();
    if recover {
        commit
            .filter_and_commit_with_identifier(messages, identifier)
            .await
            .unwrap();
    } else {
        commit
            .commit_with_identifier(messages, identifier)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn automatic_compact_identity_does_not_filter_a_legal_writer_name() {
    let table = table(false).await;
    publish(&table, "job", 1, 1, false).await;
    publish(&table, "job", 2, 2, false).await;
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.commit_kind(), &CommitKind::COMPACT);

    // This is a distinct, valid user, rather than recovery of the first job.
    publish(&table, "job-auto-compact", 2, 99, true).await;
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20), (99, 990)]);
}

struct RemoveSecondTaskInput {
    table: Table,
    removed: Mutex<Option<(String, bytes::Bytes)>>,
}

#[async_trait::async_trait]
impl CommitCallback for RemoveSecondTaskInput {
    async fn call(&self, context: &CommitCallbackContext) -> paimon::Result<()> {
        if context.snapshot.commit_kind() != &CommitKind::APPEND {
            return Ok(());
        }
        let plan = self
            .table
            .new_compact_builder()
            .new_planner()?
            .plan()
            .await?;
        assert_eq!(plan.tasks().len(), 2);
        let task = &plan.tasks()[1];
        let partition = BinaryRow::from_serialized_bytes(task.partition())?.get_int(0)?;
        let path = format!(
            "{}/pt={partition}/bucket-{}/{}",
            self.table.location(),
            task.bucket(),
            task.files()[0].file_name
        );
        let bytes = self.table.file_io().new_input(&path)?.read().await?;
        self.table.file_io().delete_file(&path).await?;
        *self.removed.lock().unwrap() = Some((path, bytes));
        Ok(())
    }
}

async fn data_files(table: &Table) -> BTreeSet<String> {
    table
        .file_io()
        .list_status_recursive(&format!("{}/", table.location()))
        .await
        .unwrap()
        .into_iter()
        .map(|file| file.path)
        .filter(|path| path.contains("/bucket-") && path.ends_with(".parquet"))
        .collect()
}

#[tokio::test]
async fn failed_later_compact_task_cleans_unpublished_outputs_and_keeps_data() {
    let table = table(true).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    // Both partitions are touched by this commit and each needs compaction.
    for ids in [vec![1, 3], vec![2, 4]] {
        writer
            .write_arrow_batch(&batch(ids, Some(vec![1, 2])))
            .await
            .unwrap();
    }
    let messages = writer.prepare_commit().await.unwrap();
    let before = data_files(&table).await;
    assert_eq!(before.len(), 4);
    let callback = Arc::new(RemoveSecondTaskInput {
        table: table.clone(),
        removed: Mutex::new(None),
    });
    let mut commit = builder.new_commit();
    commit.add_commit_callback(callback.clone()).unwrap();
    let error = commit
        .commit_with_identifier(messages, 7)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("automatic compaction failed"),
        "{error}"
    );

    // Restore the transiently unavailable input before checking committed data.
    let (path, bytes) = callback.removed.lock().unwrap().take().unwrap();
    table
        .file_io()
        .new_output(&path)
        .unwrap()
        .write(bytes)
        .await
        .unwrap();
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.id(), 1);
    assert_eq!(snapshot.commit_kind(), &CommitKind::APPEND);
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20), (3, 30), (4, 40)]);
    assert_eq!(
        data_files(&table).await,
        before,
        "failed round must not retain outputs of earlier tasks"
    );
}
