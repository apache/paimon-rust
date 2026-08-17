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

use super::postpone_bucket_plan::data_invalid;
use super::postpone_fixed_bucket_router::{
    validate_postpone_fixed_bucket_table, PostponeFixedBucketRouter,
};
use crate::spec::CoreOptions;
use crate::table::{CommitMessage, PostponeBucketPlan, Table, TableCommit, TableWrite};
use crate::Result;
use arrow_array::RecordBatch;

pub struct PostponeFixedBucketTableWrite {
    inner: TableWrite,
    router: PostponeFixedBucketRouter,
    overwrite: bool,
    check_from_snapshot: Option<i64>,
    prepare_started: bool,
}

impl PostponeFixedBucketTableWrite {
    pub(crate) fn new(
        table: &Table,
        commit_user: String,
        plan: PostponeBucketPlan,
        overwrite: bool,
    ) -> Result<Self> {
        validate_postpone_fixed_bucket_write(table)?;
        let inner = TableWrite::new(table, commit_user)?;
        Ok(Self {
            inner: if overwrite {
                inner.with_overwrite()
            } else {
                inner
            },
            router: PostponeFixedBucketRouter::new(table, plan)?,
            overwrite,
            check_from_snapshot: None,
            prepare_started: false,
        })
    }

    pub async fn write_arrow_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.ensure_writable()?;
        let Some(batch) = self.inner.normalize_write_batch(batch)? else {
            return Ok(());
        };
        if self.check_from_snapshot.is_none() {
            self.check_from_snapshot = Some(self.inner.pin_sequence_snapshot().await?);
        }
        for routed in self.router.route(&batch)? {
            self.inner
                .write_partition_bucket_batch(routed.partition, routed.bucket, routed.batch)
                .await?;
        }
        Ok(())
    }

    pub async fn write_arrow(&mut self, batches: &[RecordBatch]) -> Result<()> {
        for batch in batches {
            self.write_arrow_batch(batch).await?;
        }
        Ok(())
    }

    pub async fn prepare_commit(&mut self) -> Result<Vec<CommitMessage>> {
        self.ensure_writable()?;
        self.prepare_started = true;
        let mut messages = self.inner.prepare_commit().await?;
        for message in &mut messages {
            message.total_buckets = self.router.total_buckets(&message.partition);
            if self.overwrite {
                message.mark_fixed_bucket_overwrite();
            }
            message.check_from_snapshot = self.check_from_snapshot;
        }
        Ok(messages)
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.prepare_started {
            return Err(data_invalid("Fixed-bucket postpone TableWrite only supports one prepare_commit call; create a new writer for the next batch"));
        }
        Ok(())
    }
}

pub struct PostponeFixedBucketTableCommit {
    inner: TableCommit,
    overwrite: bool,
}

impl PostponeFixedBucketTableCommit {
    pub(crate) fn new(table: &Table, commit_user: String, overwrite: bool) -> Self {
        Self {
            inner: TableCommit::new(table.clone(), commit_user),
            overwrite,
        }
    }

    pub async fn commit(&self, messages: Vec<CommitMessage>) -> Result<()> {
        if self.overwrite {
            self.inner.overwrite(messages, None).await
        } else {
            self.inner.commit(messages).await
        }
    }

    pub async fn commit_with_identifier(
        &self,
        messages: Vec<CommitMessage>,
        commit_identifier: i64,
    ) -> Result<()> {
        if self.overwrite {
            self.inner
                .overwrite_with_identifier(messages, None, commit_identifier)
                .await
        } else {
            self.inner
                .commit_with_identifier(messages, commit_identifier)
                .await
        }
    }

    pub async fn filter_and_commit_with_identifier(
        &self,
        messages: Vec<CommitMessage>,
        commit_identifier: i64,
    ) -> Result<()> {
        if self.overwrite {
            self.inner
                .overwrite_with_identifier(messages, None, commit_identifier)
                .await
        } else {
            self.inner
                .filter_and_commit_with_identifier(messages, commit_identifier)
                .await
        }
    }

    pub async fn overwrite(&self, messages: Vec<CommitMessage>) -> Result<()> {
        self.inner.overwrite(messages, None).await
    }

    pub async fn overwrite_with_identifier(
        &self,
        messages: Vec<CommitMessage>,
        commit_identifier: i64,
    ) -> Result<()> {
        self.inner
            .overwrite_with_identifier(messages, None, commit_identifier)
            .await
    }

    pub async fn truncate_table(&self) -> Result<()> {
        self.inner.truncate_table().await
    }

    pub async fn truncate_table_with_identifier(&self, commit_identifier: i64) -> Result<()> {
        self.inner
            .truncate_table_with_identifier(commit_identifier)
            .await
    }

    pub async fn abort(&self, messages: &[CommitMessage]) -> Result<()> {
        self.inner.abort(messages).await
    }
}

fn validate_postpone_fixed_bucket_write(table: &Table) -> Result<()> {
    validate_postpone_fixed_bucket_table(table)?;
    if CoreOptions::new(table.schema().options()).deletion_vectors_enabled() {
        return Err(crate::Error::Unsupported {
            message: format!(
                "Table '{}' cannot use postpone fixed-bucket writes with deletion-vectors.enabled=true because deletion-vector scans skip the level-0 files produced by batch writers; use the normal postpone writer or disable deletion vectors",
                table.identifier().full_name()
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::catalog::Identifier;
    use crate::io::FileIO;
    use crate::spec::{DataType, IntType, Schema, TableSchema, VarCharType};
    use crate::table::table_write::tests::{
        make_batch, make_partitioned_batch_3col, read_id_value_rows, setup_dirs, test_file_io,
        test_postpone_partitioned_table, test_postpone_pk_table,
    };
    use crate::table::{
        CommitMessage, PostponeBucketPlan, PostponeFixedBucketWriteBuilder, SnapshotManager, Table,
        TableCommit, TableScan,
    };
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    fn make_partition_bucket_plan_batch(
        partitions: Vec<&str>,
        total_buckets: Vec<i32>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("pt", ArrowDataType::Utf8, false),
                ArrowField::new("total_buckets", ArrowDataType::Int32, false),
            ])),
            vec![
                Arc::new(StringArray::from(partitions)),
                Arc::new(Int32Array::from(total_buckets)),
            ],
        )
        .unwrap()
    }

    fn make_partition_bucket_plan(
        table: &Table,
        partitions: Vec<&str>,
        total_buckets: Vec<i32>,
    ) -> PostponeBucketPlan {
        PostponeBucketPlan::from_arrow(
            table,
            &make_partition_bucket_plan_batch(partitions, total_buckets),
        )
        .unwrap()
    }

    fn make_unpartitioned_bucket_plan(table: &Table, total_buckets: i32) -> PostponeBucketPlan {
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "total_buckets",
                ArrowDataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![total_buckets]))],
        )
        .unwrap();
        PostponeBucketPlan::from_arrow(table, &batch).unwrap()
    }

    fn cross_partition_postpone_table(file_io: &FileIO, table_path: &str) -> Table {
        let schema = Schema::builder()
            .column("pt", DataType::VarChar(VarCharType::string_type()))
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .partition_keys(["pt"])
            .option("bucket", "-2")
            .build()
            .unwrap();
        Table::new(
            file_io.clone(),
            Identifier::new("default", "cross_partition_postpone"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    async fn write_fixed_batch(
        table: &Table,
        commit_user: &str,
        total_buckets: i32,
        batch: &RecordBatch,
    ) -> Vec<CommitMessage> {
        let mut write = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user(commit_user)
            .unwrap()
            .with_bucket_plan(make_unpartitioned_bucket_plan(table, total_buckets))
            .new_write()
            .unwrap();
        write.write_arrow_batch(batch).await.unwrap();
        write.prepare_commit().await.unwrap()
    }

    async fn prepare_partitioned_fixed_batch<'a>(
        table: &'a Table,
        commit_user: &str,
        plan: PostponeBucketPlan,
        partition: &str,
        id: i32,
        value: i32,
    ) -> (PostponeFixedBucketWriteBuilder<'a>, Vec<CommitMessage>) {
        let builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user(commit_user)
            .unwrap()
            .with_bucket_plan(plan);
        let mut write = builder.new_write().unwrap();
        write
            .write_arrow_batch(&make_partitioned_batch_3col(
                vec![partition],
                vec![id],
                vec![value],
            ))
            .await
            .unwrap();
        let messages = write.prepare_commit().await.unwrap();
        (builder, messages)
    }

    #[test]
    fn test_postpone_fixed_bucket_rejects_cross_partition_update() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_fixed_cross_partition";
        let table = cross_partition_postpone_table(&file_io, table_path);
        let error = match table.new_postpone_fixed_bucket_write_builder() {
            Ok(_) => panic!("cross-partition postpone fixed-bucket writes should be rejected"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("do not support cross-partition updates"));
    }

    fn assert_total_buckets(messages: &[CommitMessage], total_buckets: i32) {
        assert!(!messages.is_empty());
        assert!(messages
            .iter()
            .all(|message| message.total_buckets == Some(total_buckets)));
    }

    fn assert_one_shot_error(error: crate::Error) {
        assert!(
            matches!(error, crate::Error::DataInvalid { ref message, .. }
            if message.contains("only supports one prepare_commit call")
                && message.contains("create a new writer"))
        );
    }

    #[tokio::test]
    async fn test_postpone_batch_write_uses_visible_fixed_buckets() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_fixed_bucket_write";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_pk_table(&file_io, table_path);

        let first_builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user("fixed-user-1")
            .unwrap()
            .with_bucket_plan(make_unpartitioned_bucket_plan(&table, 2));
        let mut write = first_builder.new_write().unwrap();
        write
            .write_arrow_batch(&make_batch(vec![1, 2, 3, 4], vec![10, 20, 30, 40]))
            .await
            .unwrap();
        let messages = write.prepare_commit().await.unwrap();
        assert!(messages.iter().all(|message| message.bucket >= 0));
        assert_total_buckets(&messages, 2);

        let stale_messages =
            write_fixed_batch(&table, "stale-user", 1, &make_batch(vec![9], vec![90])).await;
        assert_total_buckets(&stale_messages, 1);
        TableCommit::new(table.clone(), "fixed-user-1".to_string())
            .commit(messages)
            .await
            .unwrap();
        let error = TableCommit::new(table.clone(), "stale-user".to_string())
            .commit(stale_messages)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Fixed-bucket conflict"));
        assert_eq!(
            read_id_value_rows(&table).await,
            vec![(1, 10), (2, 20), (3, 30), (4, 40)]
        );

        let second_builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user("fixed-user-2")
            .unwrap()
            .with_bucket_plan(make_unpartitioned_bucket_plan(&table, 2));
        let mut write = second_builder.new_write().unwrap();
        write
            .write_arrow_batch(&make_batch(vec![5], vec![50]))
            .await
            .unwrap();
        let messages = write.prepare_commit().await.unwrap();
        assert_total_buckets(&messages, 2);
        assert_one_shot_error(
            write
                .write_arrow_batch(&make_batch(vec![6], vec![60]))
                .await
                .unwrap_err(),
        );
        assert_one_shot_error(write.prepare_commit().await.unwrap_err());
        TableCommit::new(table.clone(), "fixed-user-2".to_string())
            .commit(messages)
            .await
            .unwrap();
        assert_eq!(
            read_id_value_rows(&table).await,
            vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]
        );
    }

    #[tokio::test]
    async fn test_postpone_distributed_writers_share_bucket_plan() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_shared_bucket_plan";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_partitioned_table(&file_io, table_path);
        let plan = make_partition_bucket_plan(&table, vec!["p1", "p2"], vec![3, 3]);

        let builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user("shared-plan-user")
            .unwrap()
            .with_bucket_plan(plan);
        let mut first = builder.new_write().unwrap();
        let mut second = builder.new_write().unwrap();
        first
            .write_arrow_batch(&make_partitioned_batch_3col(vec!["p1"], vec![1], vec![10]))
            .await
            .unwrap();
        second
            .write_arrow_batch(&make_partitioned_batch_3col(
                vec!["p2", "p2", "p2", "p2"],
                vec![2, 3, 4, 5],
                vec![20, 30, 40, 50],
            ))
            .await
            .unwrap();

        let mut messages = first.prepare_commit().await.unwrap();
        messages.extend(second.prepare_commit().await.unwrap());
        assert_total_buckets(&messages, 3);
        builder.new_commit().commit(messages).await.unwrap();
    }

    #[tokio::test]
    async fn test_postpone_distributed_writers_reject_overlapping_bucket_ownership() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_overlapping_writer_ownership";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_partitioned_table(&file_io, table_path);
        let builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user("overlapping-writers")
            .unwrap()
            .with_bucket_plan(make_partition_bucket_plan(&table, vec!["p"], vec![1]));

        let mut first = builder.new_write().unwrap();
        let mut second = builder.new_write().unwrap();
        first
            .write_arrow_batch(&make_partitioned_batch_3col(vec!["p"], vec![1], vec![10]))
            .await
            .unwrap();
        second
            .write_arrow_batch(&make_partitioned_batch_3col(vec!["p"], vec![1], vec![20]))
            .await
            .unwrap();

        let mut messages = first.prepare_commit().await.unwrap();
        messages.extend(second.prepare_commit().await.unwrap());
        let error = TableCommit::new(table.clone(), "overlapping-writers".to_string())
            .commit(messages.clone())
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("writer ownership conflict for bucket 0"));

        let error = builder.new_commit().commit(messages).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("writer ownership conflict for bucket 0"));
    }

    #[tokio::test]
    async fn test_postpone_concurrent_commits_reject_overlapping_bucket_ownership() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_concurrent_writer_ownership";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_partitioned_table(&file_io, table_path);
        let plan = make_partition_bucket_plan(&table, vec!["p"], vec![1]);
        let (first_builder, first_messages) = prepare_partitioned_fixed_batch(
            &table,
            "concurrent-writer-1",
            plan.clone(),
            "p",
            1,
            10,
        )
        .await;
        let (second_builder, second_messages) =
            prepare_partitioned_fixed_batch(&table, "concurrent-writer-2", plan, "p", 1, 20).await;
        assert_eq!(first_messages[0].new_files[0].min_sequence_number, 0);
        assert_eq!(second_messages[0].new_files[0].min_sequence_number, 0);
        first_builder
            .new_commit()
            .commit(first_messages)
            .await
            .unwrap();
        let error = second_builder
            .new_commit()
            .commit(second_messages)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("writer ownership conflict for bucket 0"));
    }

    #[tokio::test]
    async fn test_postpone_concurrent_overwrites_reject_overlapping_bucket_ownership() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_concurrent_overwrite_ownership";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_pk_table(&file_io, table_path);
        let plan = make_unpartitioned_bucket_plan(&table, 1);

        let first_builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user("overwrite-writer-1")
            .unwrap()
            .with_bucket_plan(plan.clone())
            .with_overwrite();
        let second_builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user("overwrite-writer-2")
            .unwrap()
            .with_bucket_plan(plan)
            .with_overwrite();
        let mut first_write = first_builder.new_write().unwrap();
        let mut second_write = second_builder.new_write().unwrap();
        first_write
            .write_arrow_batch(&make_batch(vec![1], vec![10]))
            .await
            .unwrap();
        second_write
            .write_arrow_batch(&make_batch(vec![1], vec![20]))
            .await
            .unwrap();
        let first_messages = first_write.prepare_commit().await.unwrap();
        let second_messages = second_write.prepare_commit().await.unwrap();

        first_builder
            .new_commit()
            .commit(first_messages)
            .await
            .unwrap();
        let error = second_builder
            .new_commit()
            .commit(second_messages)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("another commit wrote the same partition"));
    }

    #[tokio::test]
    async fn test_postpone_overwrite_rejects_concurrent_write_to_same_partition() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_overwrite_partition_conflict";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_pk_table(&file_io, table_path);
        let plan = make_unpartitioned_bucket_plan(&table, 2);

        let overwrite_builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user("overwrite-writer")
            .unwrap()
            .with_bucket_plan(plan.clone())
            .with_overwrite();
        let mut overwrite_write = overwrite_builder.new_write().unwrap();
        overwrite_write
            .write_arrow_batch(&make_batch(
                (0..32).collect(),
                (0..32).map(|value| value * 10).collect(),
            ))
            .await
            .unwrap();
        let overwrite_messages = overwrite_write
            .prepare_commit()
            .await
            .unwrap()
            .into_iter()
            .filter(|message| message.bucket == 0)
            .collect::<Vec<_>>();
        assert_eq!(overwrite_messages.len(), 1);

        let append_builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user("append-writer")
            .unwrap()
            .with_bucket_plan(plan);
        let mut append_write = append_builder.new_write().unwrap();
        append_write
            .write_arrow_batch(&make_batch(
                (100..132).collect(),
                (100..132).map(|value| value * 10).collect(),
            ))
            .await
            .unwrap();
        let append_messages = append_write
            .prepare_commit()
            .await
            .unwrap()
            .into_iter()
            .filter(|message| message.bucket == 1)
            .collect::<Vec<_>>();
        assert_eq!(append_messages.len(), 1);
        append_builder
            .new_commit()
            .commit(append_messages)
            .await
            .unwrap();

        let error = overwrite_builder
            .new_commit()
            .commit(overwrite_messages)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("another commit wrote the same partition"));
    }

    #[tokio::test]
    async fn test_postpone_concurrent_commits_allow_disjoint_ownership() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_disjoint_writer_ownership";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_partitioned_table(&file_io, table_path);
        let plan = make_partition_bucket_plan(&table, vec!["p1", "p2"], vec![1, 1]);
        let (first_builder, first_messages) =
            prepare_partitioned_fixed_batch(&table, "disjoint-writer-1", plan.clone(), "p1", 1, 10)
                .await;
        let (second_builder, second_messages) =
            prepare_partitioned_fixed_batch(&table, "disjoint-writer-2", plan, "p2", 2, 20).await;

        first_builder
            .new_commit()
            .commit(first_messages)
            .await
            .unwrap();
        second_builder
            .new_commit()
            .commit(second_messages)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_postpone_rejects_negative_bucket_after_fixed_layout() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_rejects_negative_bucket_after_fixed";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_pk_table(&file_io, table_path);

        let fixed_messages =
            write_fixed_batch(&table, "fixed-writer", 1, &make_batch(vec![1], vec![10])).await;
        TableCommit::new(table.clone(), "fixed-writer".to_string())
            .commit(fixed_messages)
            .await
            .unwrap();

        let normal_builder = table
            .new_write_builder()
            .with_commit_user("normal-writer")
            .unwrap();
        let mut normal_write = normal_builder.new_write().unwrap();
        normal_write
            .write_arrow_batch(&make_batch(vec![1], vec![20]))
            .await
            .unwrap();
        let messages = normal_write.prepare_commit().await.unwrap();
        assert!(messages.iter().all(|message| message.bucket == -2));
        let error = normal_builder
            .new_commit()
            .commit(messages)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Cannot mix bucket=-2 files"));
        assert_eq!(read_id_value_rows(&table).await, vec![(1, 10)]);
    }

    #[tokio::test]
    async fn test_postpone_fixed_append_rejects_existing_negative_bucket() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_fixed_rejects_existing_negative_bucket";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_pk_table(&file_io, table_path);

        let normal_builder = table
            .new_write_builder()
            .with_commit_user("normal-writer")
            .unwrap();
        let mut normal_write = normal_builder.new_write().unwrap();
        normal_write
            .write_arrow_batch(&make_batch(vec![1, 2, 3, 4], vec![10, 20, 30, 40]))
            .await
            .unwrap();
        normal_builder
            .new_commit()
            .commit(normal_write.prepare_commit().await.unwrap())
            .await
            .unwrap();

        let fixed_messages =
            write_fixed_batch(&table, "fixed-append", 1, &make_batch(vec![1], vec![100])).await;
        let error = TableCommit::new(table.clone(), "fixed-append".to_string())
            .commit(fixed_messages)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Cannot mix bucket=-2 files"));

        let overwrite_builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user("fixed-overwrite")
            .unwrap()
            .with_bucket_plan(make_unpartitioned_bucket_plan(&table, 1))
            .with_overwrite();
        let mut overwrite_write = overwrite_builder.new_write().unwrap();
        overwrite_write
            .write_arrow_batch(&make_batch(vec![1], vec![100]))
            .await
            .unwrap();
        overwrite_builder
            .new_commit()
            .commit(overwrite_write.prepare_commit().await.unwrap())
            .await
            .unwrap();
        assert_eq!(read_id_value_rows(&table).await, vec![(1, 100)]);
    }

    #[tokio::test]
    async fn test_postpone_commit_rejects_writer_mode_mismatch() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_writer_mode_mismatch";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_pk_table(&file_io, table_path);
        let plan = make_unpartitioned_bucket_plan(&table, 1);

        let initial_builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_bucket_plan(plan.clone());
        let mut initial_write = initial_builder.new_write().unwrap();
        initial_write
            .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
            .await
            .unwrap();
        initial_builder
            .new_commit()
            .commit(initial_write.prepare_commit().await.unwrap())
            .await
            .unwrap();

        let builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_bucket_plan(plan);
        let append_commit = builder.new_commit();
        let overwrite_builder = builder.with_overwrite();
        let mut overwrite_write = overwrite_builder.new_write().unwrap();
        overwrite_write
            .write_arrow_batch(&make_batch(vec![1], vec![100]))
            .await
            .unwrap();
        let messages = overwrite_write.prepare_commit().await.unwrap();

        let error = append_commit.commit(messages.clone()).await.unwrap_err();
        assert!(error.to_string().contains("submitted as append"));
        assert_eq!(read_id_value_rows(&table).await, vec![(1, 10), (2, 20)]);

        overwrite_builder
            .new_commit()
            .commit(messages)
            .await
            .unwrap();
        assert_eq!(read_id_value_rows(&table).await, vec![(1, 100)]);
    }

    #[tokio::test]
    async fn test_postpone_commit_rejects_invalid_fixed_bucket_range() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_invalid_fixed_bucket_range";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_pk_table(&file_io, table_path);
        let messages =
            write_fixed_batch(&table, "fixed-writer", 1, &make_batch(vec![1], vec![10])).await;

        for (bucket, total_buckets) in [(1, 1), (-1, 1), (0, 0)] {
            let mut invalid = messages.clone();
            invalid[0].bucket = bucket;
            invalid[0].total_buckets = Some(total_buckets);
            let error = TableCommit::new(table.clone(), "fixed-writer".to_string())
                .commit(invalid)
                .await
                .unwrap_err();
            assert!(error.to_string().contains("Invalid fixed bucket"));
        }
    }

    #[tokio::test]
    async fn test_postpone_fixed_commit_rejects_concurrent_negative_bucket() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_fixed_rejects_concurrent_negative_bucket";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_pk_table(&file_io, table_path);

        let fixed_builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user("fixed-writer")
            .unwrap()
            .with_bucket_plan(make_unpartitioned_bucket_plan(&table, 1));
        let mut fixed_write = fixed_builder.new_write().unwrap();
        fixed_write
            .write_arrow_batch(&make_batch(vec![1], vec![10]))
            .await
            .unwrap();
        let fixed_messages = fixed_write.prepare_commit().await.unwrap();

        let normal_builder = table
            .new_write_builder()
            .with_commit_user("normal-writer")
            .unwrap();
        let mut normal_write = normal_builder.new_write().unwrap();
        normal_write
            .write_arrow_batch(&make_batch(vec![1], vec![20]))
            .await
            .unwrap();
        normal_builder
            .new_commit()
            .commit(normal_write.prepare_commit().await.unwrap())
            .await
            .unwrap();

        let error = fixed_builder
            .new_commit()
            .commit(fixed_messages)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Cannot mix bucket=-2 files"));
    }

    #[tokio::test]
    async fn test_postpone_provided_plan_must_cover_input_partitions() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_incomplete_bucket_plan";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_partitioned_table(&file_io, table_path);

        let error = PostponeBucketPlan::from_arrow(
            &table,
            &make_partition_bucket_plan_batch(vec!["p"], vec![0]),
        )
        .unwrap_err();
        assert!(error.to_string().contains("must be positive"));

        let plan = make_partition_bucket_plan(&table, vec!["p"], vec![2]);
        let mut write = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_bucket_plan(plan)
            .new_write()
            .unwrap();

        let error = write
            .write_arrow_batch(&make_partitioned_batch_3col(
                vec!["missing"],
                vec![1],
                vec![10],
            ))
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("does not contain an input partition"));
    }

    #[tokio::test]
    async fn test_postpone_overwrite_allows_bucket_rescale() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_overwrite_bucket_rescale";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_partitioned_table(&file_io, table_path);

        let initial_builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user("initial-layout")
            .unwrap()
            .with_bucket_plan(make_partition_bucket_plan(&table, vec!["p"], vec![1]));
        let mut initial_write = initial_builder.new_write().unwrap();
        initial_write
            .write_arrow_batch(&make_partitioned_batch_3col(vec!["p"], vec![1], vec![10]))
            .await
            .unwrap();
        initial_builder
            .new_commit()
            .commit(initial_write.prepare_commit().await.unwrap())
            .await
            .unwrap();

        let overwrite_builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user("replacement-layout")
            .unwrap()
            .with_bucket_plan(make_partition_bucket_plan(&table, vec!["p"], vec![3]))
            .with_overwrite();
        let mut overwrite_write = overwrite_builder.new_write().unwrap();
        overwrite_write
            .write_arrow_batch(&make_partitioned_batch_3col(vec!["p"], vec![2], vec![20]))
            .await
            .unwrap();
        let messages = overwrite_write.prepare_commit().await.unwrap();
        assert_total_buckets(&messages, 3);
        overwrite_builder
            .new_commit()
            .commit(messages)
            .await
            .unwrap();

        let snapshot = SnapshotManager::new(file_io, table_path.to_string())
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        let entries = TableScan::new(&table, None, vec![], None, None, None)
            .with_scan_all_files()
            .plan_manifest_entries(&snapshot)
            .await
            .unwrap();
        assert!(!entries.is_empty());
        assert!(entries.iter().all(|entry| entry.total_buckets() == 3));
    }

    #[tokio::test]
    async fn test_postpone_fixed_bucket_delete_with_rowkind_field() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_fixed_bucket_rowkind";
        setup_dirs(&file_io, table_path).await;
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("op", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("bucket", "-2")
            .option("rowkind.field", "op")
            .build()
            .unwrap();
        let table = Table::new(
            file_io,
            Identifier::new("default", "test_postpone_fixed_bucket_rowkind"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, false),
                ArrowField::new("value", ArrowDataType::Int32, false),
                ArrowField::new("op", ArrowDataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![10])),
                Arc::new(StringArray::from(vec!["-D"])),
            ],
        )
        .unwrap();

        let messages = write_fixed_batch(&table, "fixed-rowkind", 1, &batch).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].bucket, 0);
        assert_eq!(messages[0].total_buckets, Some(1));
        assert_eq!(messages[0].new_files.len(), 1);
        assert_eq!(messages[0].new_files[0].row_count, 1);
        assert_eq!(messages[0].new_files[0].delete_row_count, Some(1));
    }
}
