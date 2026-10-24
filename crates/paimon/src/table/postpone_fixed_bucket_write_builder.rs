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

use super::postpone_fixed_bucket_router::validate_postpone_fixed_bucket_table;
use crate::resource::ResourceContext;
use crate::spec::CoreOptions;
use crate::table::write_builder::{
    ensure_table_write_allowed, ensure_table_write_authorized, validate_commit_user,
};
use crate::table::{
    PostponeBucketPlan, PostponeFixedBucketTableCommit, PostponeFixedBucketTableWrite, Table,
};
use crate::Result;
use uuid::Uuid;

pub struct PostponeFixedBucketWriteBuilder<'a> {
    table: &'a Table,
    commit_user: String,
    overwrite: bool,
    bucket_plan: Option<PostponeBucketPlan>,
    resources: Option<ResourceContext>,
}

impl<'a> PostponeFixedBucketWriteBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Result<Self> {
        validate_postpone_fixed_bucket_table(table)?;
        Ok(Self {
            table,
            commit_user: Uuid::new_v4().to_string(),
            overwrite: false,
            bucket_plan: None,
            resources: None,
        })
    }

    pub fn commit_user(&self) -> &str {
        &self.commit_user
    }

    pub fn with_commit_user(mut self, commit_user: impl Into<String>) -> Result<Self> {
        let commit_user = commit_user.into();
        validate_commit_user(&commit_user)?;
        self.commit_user = commit_user;
        Ok(self)
    }

    pub fn with_overwrite(mut self) -> Self {
        self.overwrite = true;
        self
    }

    pub fn with_bucket_plan(mut self, bucket_plan: PostponeBucketPlan) -> Self {
        self.bucket_plan = Some(bucket_plan);
        self
    }

    pub fn with_resources(mut self, resources: ResourceContext) -> Self {
        self.resources = Some(resources);
        self
    }

    /// Resolve routing metadata before distributed input is materialized.
    ///
    /// Existing partitions are read from the latest write baseline, independently
    /// of historical read selectors. This uses writer metadata access and does
    /// not require a SELECT grant. Overwrite starts with no existing layout;
    /// an explicitly supplied plan takes precedence over that default.
    /// Bucket counts for new input partitions are not inferred by this method.
    pub async fn resolve_bucket_plan(&self) -> Result<PostponeBucketPlan> {
        ensure_table_write_authorized(self.table)?;
        if let Some(plan) = &self.bucket_plan {
            return Ok(plan.clone());
        }
        if self.overwrite {
            return Ok(PostponeBucketPlan::default());
        }
        let mut plan = PostponeBucketPlan::default();
        if let Some(snapshot) = self.table.snapshot_manager().get_latest_snapshot().await? {
            plan.load_existing_buckets_from_snapshot(self.table, &snapshot)
                .await?;
        }
        Ok(plan)
    }

    pub fn new_commit(&self) -> PostponeFixedBucketTableCommit {
        PostponeFixedBucketTableCommit::new(self.table, self.commit_user.clone(), self.overwrite)
    }

    pub fn try_new_commit(&self) -> Result<PostponeFixedBucketTableCommit> {
        ensure_table_write_allowed(self.table)?;
        Ok(self.new_commit())
    }

    pub fn new_write(&self) -> Result<PostponeFixedBucketTableWrite> {
        ensure_table_write_allowed(self.table)?;
        let load_existing_buckets = self.bucket_plan.is_none() && !self.overwrite;
        let plan = match &self.bucket_plan {
            Some(plan) => Some(plan.clone()),
            None => CoreOptions::new(self.table.schema().options())
                .postpone_default_bucket_num()?
                .map(PostponeBucketPlan::with_default_bucket_num),
        };
        PostponeFixedBucketTableWrite::new(
            self.table,
            self.commit_user.clone(),
            plan,
            load_existing_buckets,
            self.overwrite,
            self.resources.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::table::table_write::tests::{
        make_batch, setup_dirs, test_file_io, test_postpone_pk_table,
    };
    use crate::table::{PostponeBucketPlan, Table};
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn bucket_plan(table: &Table, total_buckets: i32) -> PostponeBucketPlan {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "total_buckets",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![total_buckets]))],
        )
        .unwrap();
        PostponeBucketPlan::from_arrow(table.schema(), &batch).unwrap()
    }

    #[tokio::test]
    async fn resolve_bucket_plan_obeys_explicit_and_overwrite_precedence_without_default_rows() {
        let io = test_file_io();
        let path = "memory:/builder_bucket_plan_modes";
        setup_dirs(&io, path).await;
        let table =
            test_postpone_pk_table(&io, path).copy_with_options(std::collections::HashMap::from([
                ("postpone.default-bucket-num".into(), "7".into()),
            ]));
        let builder = table.new_postpone_fixed_bucket_write_builder().unwrap();
        assert_eq!(
            builder
                .resolve_bucket_plan()
                .await
                .unwrap()
                .to_arrow(table.schema())
                .unwrap()
                .num_rows(),
            0
        );
        let explicit = bucket_plan(&table, 3);
        let builder = builder.with_overwrite().with_bucket_plan(explicit);
        let exported = builder
            .resolve_bucket_plan()
            .await
            .unwrap()
            .to_arrow(table.schema())
            .unwrap();
        assert_eq!(exported.num_rows(), 1);
        assert_eq!(
            exported
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            3
        );
        assert_eq!(
            table
                .new_postpone_fixed_bucket_write_builder()
                .unwrap()
                .with_overwrite()
                .resolve_bucket_plan()
                .await
                .unwrap()
                .to_arrow(table.schema())
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[tokio::test]
    async fn resolve_bucket_plan_uses_latest_write_baseline_and_ignores_pending_files() {
        let io = test_file_io();
        let path = "memory:/builder_bucket_plan_latest";
        setup_dirs(&io, path).await;
        let table = test_postpone_pk_table(&io, path);
        let pending = table.new_write_builder();
        let mut writer = pending.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![1], vec![10]))
            .await
            .unwrap();
        pending
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        assert_eq!(
            table
                .new_postpone_fixed_bucket_write_builder()
                .unwrap()
                .resolve_bucket_plan()
                .await
                .unwrap()
                .to_arrow(table.schema())
                .unwrap()
                .num_rows(),
            0
        );
        let fixed = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_bucket_plan(bucket_plan(&table, 3));
        let mut writer = fixed.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![2], vec![20]))
            .await
            .unwrap();
        fixed
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        let historical = table.copy_with_options(std::collections::HashMap::from([(
            "scan.snapshot-id".into(),
            "1".into(),
        )]));
        let exported = historical
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .resolve_bucket_plan()
            .await
            .unwrap()
            .to_arrow(historical.schema())
            .unwrap();
        assert_eq!(exported.num_rows(), 1);
        assert_eq!(
            exported
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            3
        );
        assert_eq!(
            historical
                .new_postpone_fixed_bucket_write_builder()
                .unwrap()
                .with_overwrite()
                .resolve_bucket_plan()
                .await
                .unwrap()
                .to_arrow(historical.schema())
                .unwrap()
                .num_rows(),
            0
        );
    }

    #[tokio::test]
    async fn resolve_bucket_plan_rejects_conflicting_entries_in_one_bucket() {
        use crate::spec::{Manifest, ManifestEntry, ManifestList};
        let io = test_file_io();
        let path = "memory:/builder_bucket_plan_conflict";
        setup_dirs(&io, path).await;
        let table = test_postpone_pk_table(&io, path);
        let fixed = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_bucket_plan(bucket_plan(&table, 1));
        let mut writer = fixed.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![1], vec![10]))
            .await
            .unwrap();
        fixed
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        let manager = table.snapshot_manager();
        let snapshot = manager.get_snapshot(1).await.unwrap();
        let list_path = manager.manifest_path(snapshot.delta_manifest_list());
        let mut metas = ManifestList::read(&io, &list_path).await.unwrap();
        let manifest_path = manager.manifest_path(metas[0].file_name());
        let mut entries = Manifest::read(&io, &manifest_path).await.unwrap();
        let original = &entries[0];
        let mut file = original.file().clone();
        file.file_name = "different-bucket-count.parquet".into();
        entries.push(ManifestEntry::new(
            *original.kind(),
            original.partition().to_vec(),
            original.bucket(),
            2,
            file,
            2,
        ));
        Manifest::write(&io, &manifest_path, &entries)
            .await
            .unwrap();
        let mut meta = serde_json::to_value(&metas[0]).unwrap();
        meta["_NUM_ADDED_FILES"] = serde_json::json!(entries.len());
        meta["_TOTAL_BUCKETS"] = serde_json::Value::Null;
        metas[0] = serde_json::from_value(meta).unwrap();
        ManifestList::write(&io, &list_path, &metas).await.unwrap();
        let error = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .resolve_bucket_plan()
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("conflicting total bucket counts"));
    }

    #[tokio::test]
    async fn test_postpone_fixed_bucket_builder_modes() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_fixed_bucket_builder";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_pk_table(&file_io, table_path);

        table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .new_write()
            .unwrap();

        let builder = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user("explicit-fixed-user")
            .unwrap()
            .with_bucket_plan(bucket_plan(&table, 1));
        let mut write = builder.new_write().unwrap();
        write
            .write_arrow_batch(&make_batch(vec![4], vec![40]))
            .await
            .unwrap();
        let messages = write.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].bucket, 0);
        assert_eq!(messages[0].total_buckets, Some(1));

        let dv_table = table.copy_with_options(std::collections::HashMap::from([(
            "deletion-vectors.enabled".to_string(),
            "true".to_string(),
        )]));
        let mut write = dv_table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_bucket_plan(bucket_plan(&dv_table, 1))
            .new_write()
            .unwrap();
        write
            .write_arrow_batch(&make_batch(vec![5], vec![50]))
            .await
            .unwrap();
        let messages = write.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].bucket, 0);
        assert_eq!(messages[0].total_buckets, Some(1));
        assert_eq!(messages[0].new_files[0].level, 0);
        assert_eq!(messages[0].new_files[0].row_count, 1);
        write.close().await;
    }

    #[tokio::test]
    async fn default_bucket_count_routes_without_an_explicit_plan() {
        let file_io = test_file_io();
        let table_path = "memory:/default_postpone_bucket_count";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_pk_table(&file_io, table_path).copy_with_options(
            std::collections::HashMap::from([("postpone.default-bucket-num".into(), "3".into())]),
        );
        let builder = table.new_postpone_fixed_bucket_write_builder().unwrap();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![1, 2, 3, 4], vec![10, 20, 30, 40]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert!(!messages.is_empty());
        assert!(messages
            .iter()
            .all(|message| message.total_buckets == Some(3) && (0..3).contains(&message.bucket)));
        builder.new_commit().commit(messages).await.unwrap();
    }

    #[tokio::test]
    async fn automatic_bucket_count_uses_the_whole_input() {
        let io = test_file_io();
        let path = "memory:/automatic_postpone_count";
        setup_dirs(&io, path).await;
        let table =
            test_postpone_pk_table(&io, path).copy_with_options(std::collections::HashMap::from([
                ("postpone.target-row-num-per-bucket".into(), "2".into()),
                (
                    "postpone.batch-write-fixed-bucket.max-parallelism".into(),
                    "8".into(),
                ),
            ]));
        let builder = table.new_postpone_fixed_bucket_write_builder().unwrap();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
            .await
            .unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![3, 4, 5], vec![30, 40, 50]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert!(!messages.is_empty());
        assert!(messages.iter().all(|m| m.total_buckets == Some(4)));
        builder.new_commit().commit(messages).await.unwrap();
        assert_eq!(
            super::super::table_write::tests::read_id_value_rows(&table).await,
            vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]
        );
    }
}
