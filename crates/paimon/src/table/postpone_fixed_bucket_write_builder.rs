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
use super::postpone_fixed_bucket_router::validate_postpone_fixed_bucket_table;
use crate::table::write_builder::{ensure_table_write_allowed, validate_commit_user};
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
}

impl<'a> PostponeFixedBucketWriteBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Result<Self> {
        validate_postpone_fixed_bucket_table(table)?;
        Ok(Self {
            table,
            commit_user: Uuid::new_v4().to_string(),
            overwrite: false,
            bucket_plan: None,
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

    pub fn new_commit(&self) -> PostponeFixedBucketTableCommit {
        PostponeFixedBucketTableCommit::new(self.table, self.commit_user.clone(), self.overwrite)
    }

    pub fn try_new_commit(&self) -> Result<PostponeFixedBucketTableCommit> {
        self.table.ensure_not_branch_reference_for_write()?;
        Ok(self.new_commit())
    }

    pub fn new_write(&self) -> Result<PostponeFixedBucketTableWrite> {
        ensure_table_write_allowed(self.table)?;
        let plan = self
            .bucket_plan
            .clone()
            .ok_or_else(|| data_invalid("A resolved postpone bucket plan is required"))?;
        PostponeFixedBucketTableWrite::new(
            self.table,
            self.commit_user.clone(),
            plan,
            self.overwrite,
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
        PostponeBucketPlan::from_arrow(table, &batch).unwrap()
    }

    #[tokio::test]
    async fn test_postpone_fixed_bucket_builder_modes() {
        let file_io = test_file_io();
        let table_path = "memory:/test_postpone_fixed_bucket_builder";
        setup_dirs(&file_io, table_path).await;
        let table = test_postpone_pk_table(&file_io, table_path);

        let error = table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .new_write()
            .err()
            .unwrap();
        assert!(error.to_string().contains("bucket plan is required"));

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
        let error = dv_table
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_bucket_plan(bucket_plan(&dv_table, 1))
            .new_write()
            .err()
            .unwrap();
        assert!(matches!(error, crate::Error::Unsupported { ref message }
                if message.contains("postpone fixed-bucket writes")
                    && message.contains("deletion-vectors.enabled=true")));
    }
}
