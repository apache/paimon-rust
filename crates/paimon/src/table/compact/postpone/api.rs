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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::super::deletion_vectors::DeletionVectorUpdate;
use super::super::{
    ensure_supported, invalid, validate_dv_schemas, validate_file, validate_snapshot_indexes,
    CompactBuilder, CompactContext, CompactMetrics,
};
use crate::resource::ResourceContext;
use crate::spec::{DataFileMeta, Predicate, POSTPONE_BUCKET};
use crate::table::partition_filter::PartitionFilter;
use crate::table::snapshot_manifest::read_snapshot_entries_in_manifest_order;
use crate::table::{Table, TableCommit};
use crate::{Error, Result};

/// Configures partition-wide routing and rewriting for `bucket=-2` PK tables.
/// Unlike ordinary bucket compaction, one task may consume and produce many buckets.
pub struct PostponeCompactBuilder<'a> {
    inner: CompactBuilder<'a>,
}

impl<'a> PostponeCompactBuilder<'a> {
    pub(in crate::table) fn new(table: &'a Table) -> Self {
        Self {
            inner: CompactBuilder::new(table),
        }
    }

    /// Restrict planning to partitions. Repeated calls combine filters with AND.
    pub fn with_partition_filter(mut self, filter: Predicate) -> Result<Self> {
        self.inner = self.inner.with_partition_filter(filter)?;
        Ok(self)
    }

    pub fn with_resources(mut self, resources: ResourceContext) -> Self {
        self.inner = self.inner.with_resources(resources);
        self
    }

    pub fn with_commit_user(mut self, commit_user: impl Into<String>) -> Result<Self> {
        self.inner = self.inner.with_commit_user(commit_user)?;
        Ok(self)
    }

    pub fn new_planner(&self) -> Result<PostponeCompactPlanner> {
        ensure_postpone_supported(self.inner.table)?;
        Ok(PostponeCompactPlanner {
            table: self.inner.table.clone(),
            partition_filter: self.inner.partition_filter.clone(),
        })
    }

    pub fn new_executor(&self) -> Result<PostponeCompactExecutor> {
        ensure_postpone_supported(self.inner.table)?;
        // Reuse local execution configuration validation, including actual
        // FileIO local-cache requirements for DV and lookup modes.
        let executor = self.inner.create_executor()?;
        Ok(PostponeCompactExecutor {
            table: executor.table,
            resources: executor.resources,
        })
    }

    pub fn new_commit(&self) -> Result<TableCommit> {
        ensure_postpone_supported(self.inner.table)?;
        Ok(TableCommit::new(
            self.inner.table.clone(),
            self.inner.commit_user.clone(),
        ))
    }
}

pub(super) fn ensure_postpone_supported(table: &Table) -> Result<()> {
    ensure_supported(table)?;
    if table.schema().core_options().bucket() != POSTPONE_BUCKET {
        return Err(Error::Unsupported {
            message: "Postpone compaction requires a bucket=-2 table".into(),
        });
    }
    if table.schema().core_options().target_file_size() <= 0 {
        return Err(invalid(
            "Postpone compaction requires a positive target-file-size",
        ));
    }
    Ok(())
}

/// Captures the complete input state of each eligible postpone partition.
pub struct PostponeCompactPlanner {
    table: Table,
    partition_filter: Option<Predicate>,
}

impl PostponeCompactPlanner {
    pub async fn plan(&self) -> Result<PostponeCompactPlan> {
        ensure_postpone_supported(&self.table)?;
        let mut reader = self.table.new_read_builder();
        if let Some(filter) = &self.partition_filter {
            reader.with_filter(filter.clone());
        }
        let source = reader.new_scan().with_scan_all_files().plan_all().await?;
        let Some(snapshot_id) = source.snapshot_id() else {
            return Ok(PostponeCompactPlan {
                snapshot_id: None,
                tasks: vec![],
            });
        };
        let snapshot = self
            .table
            .snapshot_manager()
            .get_snapshot(snapshot_id)
            .await?;
        validate_snapshot_indexes(&self.table, &snapshot).await?;
        validate_dv_schemas(
            &self.table,
            source.splits().iter().flat_map(|split| split.data_files()),
        )
        .await?;
        let context = CompactContext {
            plan_id: uuid::Uuid::new_v4().to_string(),
            location: self.table.location().to_string(),
            identifier: self.table.identifier().full_name(),
            branch: self.table.branch().to_string(),
            schema: self.table.schema().clone(),
            snapshot,
        };
        let filter = self
            .partition_filter
            .as_ref()
            .map(|predicate| {
                let mapping = crate::spec::field_idx_to_partition_idx(
                    self.table.schema().fields(),
                    self.table.schema().partition_keys(),
                );
                predicate
                    .remap_field_index(&mapping)
                    .map(|predicate| {
                        PartitionFilter::from_predicate(
                            predicate,
                            &self.table.schema().partition_fields(),
                        )
                    })
                    .ok_or_else(|| invalid("Invalid postpone partition filter"))
            })
            .transpose()?;
        let entries = read_snapshot_entries_in_manifest_order(
            &self.table.snapshot_manager(),
            &context.snapshot,
            filter.as_ref(),
        )
        .await?;
        Ok(PostponeCompactPlan {
            snapshot_id: Some(snapshot_id),
            tasks: super::plan_tasks(&entries, &context)?,
        })
    }
}

/// Serializable, independently executable partition rewrites from one snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostponeCompactPlan {
    snapshot_id: Option<i64>,
    tasks: Vec<PostponeCompactTask>,
}

impl PostponeCompactPlan {
    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }
    pub fn tasks(&self) -> &[PostponeCompactTask] {
        &self.tasks
    }
}

/// One input file and its source bucket. Encounter order is significant for replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PostponeCompactInput {
    pub(super) bucket: i32,
    pub(super) total_buckets: i32,
    pub(super) file: DataFileMeta,
}

impl PostponeCompactInput {
    pub fn bucket(&self) -> i32 {
        self.bucket
    }
    pub fn total_buckets(&self) -> i32 {
        self.total_buckets
    }
    pub fn file(&self) -> &DataFileMeta {
        &self.file
    }
}

/// Complete captured partition state, routed to `total_buckets` real buckets.
/// There is no single task bucket: inputs may include pending (-2) and real buckets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostponeCompactTask {
    pub(super) context: CompactContext,
    pub(super) task_id: usize,
    pub(super) partition: Vec<u8>,
    pub(super) total_buckets: i32,
    pub(super) inputs: Vec<PostponeCompactInput>,
    pub(super) output_level: i32,
}

impl PostponeCompactTask {
    pub fn snapshot_id(&self) -> i64 {
        self.context.snapshot.id()
    }
    pub fn partition(&self) -> &[u8] {
        &self.partition
    }
    pub fn total_buckets(&self) -> i32 {
        self.total_buckets
    }
    pub fn inputs(&self) -> &[PostponeCompactInput] {
        &self.inputs
    }

    pub(super) fn validate_target(&self, table: &Table) -> Result<()> {
        ensure_postpone_supported(table)?;
        self.context.validate_target(table)?;
        super::validate_task(self)?;
        let mut names = HashSet::new();
        for input in &self.inputs {
            validate_file(&input.file, true)?;
            if !names.insert((input.bucket, &input.file.file_name)) {
                return Err(invalid("Duplicate postpone compaction input file"));
            }
        }
        Ok(())
    }
}

/// Streams routing and per-bucket merging without publishing a snapshot.
pub struct PostponeCompactExecutor {
    table: Table,
    resources: Option<ResourceContext>,
}

impl PostponeCompactExecutor {
    pub async fn execute(&self, task: &PostponeCompactTask) -> Result<PostponeCompactResult> {
        task.validate_target(&self.table)?;
        let manager = self.table.snapshot_manager();
        let snapshot = manager.get_snapshot(task.snapshot_id()).await?;
        if !task.context.snapshot.has_same_read_metadata(&snapshot) {
            return Err(invalid("Postpone compaction snapshot was replaced"));
        }
        validate_snapshot_indexes(&self.table, &snapshot).await?;
        let active = read_snapshot_entries_in_manifest_order(&manager, &snapshot, None).await?;
        validate_dv_schemas(&self.table, active.iter().map(|entry| entry.file())).await?;
        super::validate_inputs(task, &active)?;
        let table = self
            .table
            .copy_with_resolved_schema(task.context.schema.clone(), &task.context.branch)?
            .copy_with_pinned_snapshot(Some(&snapshot));
        super::execute(&table, task, self.resources.clone()).await
    }
}

/// Data and lookup changelog outputs belonging to one real target bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostponeCompactBucketResult {
    pub(super) bucket: i32,
    pub(super) files: Vec<DataFileMeta>,
    pub(super) changelog_files: Vec<DataFileMeta>,
}

impl PostponeCompactBucketResult {
    pub fn bucket(&self) -> i32 {
        self.bucket
    }
    pub fn files(&self) -> &[DataFileMeta] {
        &self.files
    }
    pub fn changelog_files(&self) -> &[DataFileMeta] {
        &self.changelog_files
    }
}

/// Prepared partition outputs, published together by `TableCommit::commit_postpone_compact`.
/// Retain these files after a failed or uncertain commit so publication can be retried.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostponeCompactResult {
    pub(super) task: PostponeCompactTask,
    pub(super) buckets: Vec<PostponeCompactBucketResult>,
    #[serde(default)]
    pub(super) deletion_vector_updates: Option<Vec<DeletionVectorUpdate>>,
}

impl PostponeCompactResult {
    pub fn buckets(&self) -> &[PostponeCompactBucketResult] {
        &self.buckets
    }
    pub fn files(&self) -> impl Iterator<Item = &DataFileMeta> {
        self.buckets.iter().flat_map(|bucket| &bucket.files)
    }
    pub fn changelog_files(&self) -> impl Iterator<Item = &DataFileMeta> {
        self.buckets
            .iter()
            .flat_map(|bucket| &bucket.changelog_files)
    }
    pub fn metrics(&self) -> CompactMetrics {
        CompactMetrics {
            files_removed: self.task.inputs.len(),
            files_added: self.files().count(),
            rows_written: self.files().map(|file| file.row_count as u64).sum(),
            bytes_written: self.files().map(|file| file.file_size as u64).sum(),
            changelog_files_added: self.changelog_files().count(),
            changelog_rows_written: self
                .changelog_files()
                .map(|file| file.row_count as u64)
                .sum(),
            changelog_bytes_written: self
                .changelog_files()
                .map(|file| file.file_size as u64)
                .sum(),
        }
    }
}
