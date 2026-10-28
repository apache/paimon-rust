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

//! Snapshot-bound append compaction and full-bucket primary-key compaction.

mod primary_key;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::data_file_index_writer::FileIndexOptions;
use super::data_file_writer::DataFileWriter;
use super::partition_filter::validate_partition_filter;
use super::snapshot_manifest::read_snapshot_entries;
use super::write_builder::{ensure_table_write_authorized, validate_commit_user};
use super::{DataSplit, Table, TableCommit};
use crate::resource::ResourceContext;
use crate::spec::{
    bucket_path_under, BinaryRow, ChangelogProducer, DataFileMeta, FileKind, IndexManifest,
    ManifestEntry, MergeEngine, PartitionComputer, Predicate, Snapshot, TableSchema,
};
use crate::{Error, Result};

/// Options for merging small files. File sizes are targets, not hard limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactOptions {
    /// Output size and small-file threshold in bytes. Defaults to `target-file-size`.
    pub target_file_size: Option<i64>,
    /// Maximum append input files per task. Defaults to 32.
    /// PK tasks include a complete bucket and are not split by this limit.
    pub max_files_per_task: usize,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            target_file_size: None,
            max_files_per_task: 32,
        }
    }
}

/// Creates planners, executors and a committer with one shared configuration.
/// Supports ordinary append tables and fixed or dynamic bucket PK tables,
/// without row tracking, deletion vectors, data evolution, query authorization
/// or dedicated blob/vector storage.
pub struct CompactBuilder<'a> {
    table: &'a Table,
    options: CompactOptions,
    partition_filter: Option<Predicate>,
    resources: Option<ResourceContext>,
    commit_user: String,
}

impl<'a> CompactBuilder<'a> {
    pub(super) fn new(table: &'a Table) -> Self {
        Self {
            table,
            options: CompactOptions::default(),
            partition_filter: None,
            resources: None,
            commit_user: uuid::Uuid::new_v4().to_string(),
        }
    }

    pub fn with_options(mut self, options: CompactOptions) -> Self {
        self.options = options;
        self
    }

    /// Restrict planning to partitions. Repeated calls combine filters with AND.
    pub fn with_partition_filter(mut self, filter: Predicate) -> Result<Self> {
        validate_partition_filter(self.table, &filter)?;
        self.partition_filter = Some(match self.partition_filter {
            Some(previous) => Predicate::and(vec![previous, filter]),
            None => filter,
        });
        Ok(self)
    }

    pub fn with_resources(mut self, resources: ResourceContext) -> Self {
        self.resources = Some(resources);
        self
    }

    pub fn with_commit_user(mut self, commit_user: impl Into<String>) -> Result<Self> {
        let commit_user = commit_user.into();
        validate_commit_user(&commit_user)?;
        self.commit_user = commit_user;
        Ok(self)
    }

    pub fn new_planner(&self) -> Result<CompactPlanner> {
        ensure_supported(self.table)?;
        let target_file_size = self
            .options
            .target_file_size
            .unwrap_or_else(|| self.table.schema().core_options().target_file_size());
        if target_file_size <= 0 || self.options.max_files_per_task < 2 {
            return Err(Error::ConfigInvalid {
                message:
                    "Compaction requires a positive target_file_size and max_files_per_task >= 2"
                        .into(),
            });
        }
        Ok(CompactPlanner {
            table: self.table.clone(),
            target_file_size,
            max_files_per_task: self.options.max_files_per_task,
            partition_filter: self.partition_filter.clone(),
        })
    }

    /// Executor resources are local; input files and output settings come from each task.
    pub fn new_executor(&self) -> Result<CompactExecutor> {
        ensure_supported(self.table)?;
        Ok(CompactExecutor {
            table: self.table.clone(),
            resources: self.resources.clone(),
        })
    }

    pub fn new_commit(&self) -> Result<TableCommit> {
        ensure_supported(self.table)?;
        Ok(TableCommit::new(
            self.table.clone(),
            self.commit_user.clone(),
        ))
    }
}

/// Selects append small files or whole PK buckets from each captured snapshot.
pub struct CompactPlanner {
    table: Table,
    target_file_size: i64,
    max_files_per_task: usize,
    partition_filter: Option<Predicate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactContext {
    plan_id: String,
    location: String,
    identifier: String,
    branch: String,
    schema: TableSchema,
    snapshot: Snapshot,
    target_file_size: i64,
}

/// An immutable, serializable set of independently executable compaction tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactPlan {
    snapshot_id: Option<i64>,
    tasks: Vec<CompactTask>,
}

impl CompactPlan {
    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    pub fn tasks(&self) -> &[CompactTask] {
        &self.tasks
    }
}

/// Complete metadata for rewriting files in one partition and bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactTask {
    context: CompactContext,
    task_id: usize,
    partition: Vec<u8>,
    bucket: i32,
    total_buckets: i32,
    files: Vec<DataFileMeta>,
}

impl CompactTask {
    pub fn snapshot_id(&self) -> i64 {
        self.context.snapshot.id()
    }

    pub fn partition(&self) -> &[u8] {
        &self.partition
    }

    pub fn bucket(&self) -> i32 {
        self.bucket
    }

    pub fn files(&self) -> &[DataFileMeta] {
        &self.files
    }
}

impl CompactPlanner {
    /// Capture a snapshot without writing data or publishing metadata.
    pub async fn plan(&self) -> Result<CompactPlan> {
        ensure_supported(&self.table)?;
        let mut reader = self.table.new_read_builder();
        if let Some(filter) = &self.partition_filter {
            reader.with_filter(filter.clone());
        }
        let source = reader.new_scan().plan_all().await?;
        let Some(snapshot_id) = source.snapshot_id() else {
            return Ok(CompactPlan {
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
        let context = CompactContext {
            plan_id: uuid::Uuid::new_v4().to_string(),
            location: self.table.location().to_string(),
            identifier: self.table.identifier().full_name(),
            branch: self.table.branch().to_string(),
            schema: self.table.schema().clone(),
            snapshot,
            target_file_size: self.target_file_size,
        };
        let mut groups = BTreeMap::<_, Vec<DataFileMeta>>::new();
        let primary_key = !self.table.schema().primary_keys().is_empty();
        for split in source.splits() {
            let group = groups
                .entry((
                    split.partition().to_serialized_bytes(),
                    split.bucket(),
                    split.total_buckets(),
                ))
                .or_default();
            for file in split.data_files() {
                validate_file(file, primary_key)?;
                if primary_key || file.file_size < self.target_file_size {
                    group.push(file.clone());
                }
            }
        }
        let mut tasks = Vec::new();
        for ((partition, bucket, total_buckets), files) in groups {
            if primary_key {
                // L0 requires upgrading; multiple levels require merging. Files
                // at one positive level already form a non-overlapping sorted
                // run, including when the previous compaction rolled files.
                let first_level = files.first().map(|file| file.level);
                if files
                    .iter()
                    .any(|file| file.level == 0 || Some(file.level) != first_level)
                {
                    tasks.push(CompactTask {
                        context: context.clone(),
                        task_id: tasks.len(),
                        partition,
                        bucket,
                        total_buckets,
                        files,
                    });
                }
                continue;
            }
            let mut pending = Vec::new();
            let mut bytes = 0i64;
            for file in files {
                bytes = bytes.saturating_add(file.file_size);
                pending.push(file);
                if pending.len() >= 2
                    && (bytes >= self.target_file_size || pending.len() >= self.max_files_per_task)
                {
                    tasks.push(CompactTask {
                        context: context.clone(),
                        task_id: tasks.len(),
                        partition: partition.clone(),
                        bucket,
                        total_buckets,
                        files: std::mem::take(&mut pending),
                    });
                    bytes = 0;
                }
            }
            if pending.len() >= 2 {
                tasks.push(CompactTask {
                    context: context.clone(),
                    task_id: tasks.len(),
                    partition,
                    bucket,
                    total_buckets,
                    files: pending,
                });
            }
        }
        Ok(CompactPlan {
            snapshot_id: Some(snapshot_id),
            tasks,
        })
    }
}

/// Streams task inputs into new data files without publishing a snapshot.
pub struct CompactExecutor {
    table: Table,
    resources: Option<ResourceContext>,
}

/// Prepared files owned by the caller until submitted to `TableCommit::commit_compact`.
/// A failed or uncertain commit must retain these files for retry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactResult {
    task: CompactTask,
    files: Vec<DataFileMeta>,
}

impl CompactResult {
    pub fn files(&self) -> &[DataFileMeta] {
        &self.files
    }

    pub fn metrics(&self) -> CompactMetrics {
        CompactMetrics {
            files_removed: self.task.files.len(),
            files_added: self.files.len(),
            rows_written: self.files.iter().map(|file| file.row_count as u64).sum(),
            bytes_written: self.files.iter().map(|file| file.file_size as u64).sum(),
        }
    }
}

/// Counts for the submitted compaction results; removed files remain for time travel.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactMetrics {
    pub files_removed: usize,
    pub files_added: usize,
    pub rows_written: u64,
    pub bytes_written: u64,
}

impl CompactExecutor {
    pub async fn execute(&self, task: &CompactTask) -> Result<CompactResult> {
        ensure_supported(&self.table)?;
        task.validate_target(&self.table)?;
        let manager = self.table.snapshot_manager();
        let snapshot = manager.get_snapshot(task.snapshot_id()).await?;
        if !task.context.snapshot.has_same_read_metadata(&snapshot) {
            return Err(invalid("Compaction snapshot was replaced"));
        }
        validate_snapshot_indexes(&self.table, &snapshot).await?;
        let active = read_snapshot_entries(&manager, &snapshot, None).await?;
        task.validate_inputs(&active)?;
        let table = self
            .table
            .copy_with_resolved_schema(task.context.schema.clone(), &task.context.branch)?
            .copy_with_pinned_snapshot(Some(&snapshot));
        let options = table.schema().core_options();
        let partition = BinaryRow::from_serialized_bytes(&task.partition)?;
        let computer = PartitionComputer::new(
            table.schema().partition_keys(),
            table.schema().fields(),
            options.partition_default_name(),
            options.legacy_partition_name(),
        )?;
        let partition_path = if table.schema().partition_keys().is_empty() {
            String::new()
        } else {
            computer.generate_partition_path(&partition)?
        };
        let bucket_path =
            bucket_path_under(&table.data_file_location(), &partition_path, task.bucket);
        let split = DataSplit::builder()
            .with_snapshot(snapshot.id())
            .with_partition(partition)
            .with_bucket(task.bucket)
            .with_total_buckets(task.total_buckets)
            .with_bucket_path(bucket_path)
            .with_data_files(task.files.clone())
            .build()?;
        if task.is_primary_key() {
            let files = primary_key::execute(&table, task, &split, self.resources.clone()).await?;
            return Ok(CompactResult {
                task: task.clone(),
                files,
            });
        }
        let mut read = table.new_read_builder().new_read()?.with_ordered(true);
        if let Some(resources) = &self.resources {
            read = read.with_resources(resources.clone());
        }
        let mut batches = read.to_arrow(&[split])?;
        let index = FileIndexOptions::parse(table.schema().options(), table.schema().fields())?;
        let mut writer = DataFileWriter::new(
            table.file_io().clone(),
            table.location().to_string(),
            partition_path,
            task.bucket,
            table.schema().id(),
            task.context.target_file_size,
            options.file_compression().to_string(),
            options.file_compression_zstd_level(),
            options.write_buffer_size()? as i64,
            options.file_format(),
            table.schema().fields().to_vec(),
            table.schema().options().clone(),
            Some(1), // FileSource.COMPACT
            None,
            None,
        )?
        .with_file_index(index.map(Arc::new))
        .with_target_file_row_num(options.target_file_row_num()?)
        .with_resources(self.resources.clone());
        let written = async {
            let mut rows = 0u64;
            while let Some(batch) = batches.try_next().await? {
                rows += batch.num_rows() as u64;
                writer.write(&batch).await?;
            }
            if rows
                != task
                    .files
                    .iter()
                    .map(|file| file.row_count as u64)
                    .sum::<u64>()
            {
                return Err(invalid("Compaction input row count changed"));
            }
            writer.prepare_commit().await
        }
        .await;
        match written {
            Ok(files) => Ok(CompactResult {
                task: task.clone(),
                files,
            }),
            Err(error) => {
                writer.abort().await;
                Err(error)
            }
        }
    }
}

impl CompactTask {
    fn is_primary_key(&self) -> bool {
        !self.context.schema.primary_keys().is_empty()
    }

    fn output_level(&self) -> i32 {
        if self.is_primary_key() {
            self.files
                .iter()
                .map(|file| file.level)
                .max()
                .unwrap_or(0)
                .max(1)
        } else {
            0
        }
    }

    fn validate_inputs(&self, active: &[ManifestEntry]) -> Result<()> {
        let bucket_files = active
            .iter()
            .filter(|entry| {
                entry.partition() == self.partition
                    && entry.bucket() == self.bucket
                    && entry.total_buckets() == self.total_buckets
            })
            .map(|entry| (entry.file().file_name.as_str(), entry))
            .collect::<HashMap<_, _>>();
        if self.is_primary_key() && bucket_files.len() != self.files.len() {
            return Err(invalid(
                "PK compaction must include the complete captured bucket",
            ));
        }
        for file in &self.files {
            if !bucket_files
                .get(file.file_name.as_str())
                .is_some_and(|entry| entry.file() == file)
            {
                return Err(invalid(
                    "Compaction input does not match the captured snapshot",
                ));
            }
        }
        Ok(())
    }

    fn validate_target(&self, table: &Table) -> Result<()> {
        if table.location() != self.context.location
            || table.identifier().full_name() != self.context.identifier
            || table.branch() != self.context.branch
            || table.schema() != &self.context.schema
        {
            return Err(invalid(
                "Compaction task belongs to a different table, branch or schema",
            ));
        }
        ensure_snapshot_supported(table, &self.context.snapshot)?;
        let minimum_files = if self.is_primary_key() { 1 } else { 2 };
        if self.context.target_file_size <= 0 || self.files.len() < minimum_files || self.bucket < 0
        {
            return Err(invalid("Invalid compaction task"));
        }
        let mut names = HashSet::new();
        for file in &self.files {
            validate_file(file, self.is_primary_key())?;
            if !names.insert(&file.file_name) {
                return Err(invalid("Duplicate compaction input file"));
            }
        }
        Ok(())
    }
}

/// Validated inputs for the existing atomic snapshot publication machinery.
pub(super) struct PreparedCompact {
    pub entries: Vec<ManifestEntry>,
    pub schema_id: i64,
    pub read_snapshot: Option<Box<Snapshot>>,
    pub commit_suffix: String,
    pub metrics: CompactMetrics,
}

pub(super) fn prepare_results(table: &Table, results: &[CompactResult]) -> Result<PreparedCompact> {
    ensure_supported(table)?;
    let first = results
        .first()
        .ok_or_else(|| invalid("No compaction results"))?;
    let mut ids = Vec::new();
    let mut inputs = HashSet::new();
    let mut outputs = HashSet::new();
    let mut entries = Vec::new();
    let mut metrics = CompactMetrics::default();
    for result in results {
        let task = &result.task;
        task.validate_target(table)?;
        if task.context.plan_id != first.task.context.plan_id
            || !task
                .context
                .snapshot
                .has_same_read_metadata(&first.task.context.snapshot)
        {
            return Err(invalid("Compaction results must belong to one plan"));
        }
        ids.push(task.task_id);
        for file in &task.files {
            if !inputs.insert((task.partition.clone(), task.bucket, file.file_name.clone())) {
                return Err(invalid("Overlapping compaction results"));
            }
            entries.push(ManifestEntry::new(
                FileKind::Delete,
                task.partition.clone(),
                task.bucket,
                task.total_buckets,
                file.clone(),
                2,
            ));
        }
        let input_rows = task
            .files
            .iter()
            .map(|file| file.row_count as u64)
            .sum::<u64>();
        let mut output_rows = 0u64;
        for file in &result.files {
            validate_file(file, task.is_primary_key())?;
            if file.schema_id != task.context.schema.id()
                || file.file_source != Some(1)
                || file.level != task.output_level()
                || !outputs.insert(file.file_name.clone())
                || task.files.iter().any(|old| old.file_name == file.file_name)
            {
                return Err(invalid("Invalid compaction output file"));
            }
            output_rows += file.row_count as u64;
            entries.push(ManifestEntry::new(
                FileKind::Add,
                task.partition.clone(),
                task.bucket,
                task.total_buckets,
                file.clone(),
                2,
            ));
        }
        if !task.is_primary_key() && input_rows != output_rows {
            return Err(invalid("Compaction must preserve the input row count"));
        }
        let options = task.context.schema.core_options();
        let ignores_retracts = task.is_primary_key()
            && options.ignore_delete()
            && matches!(
                options.merge_engine()?,
                MergeEngine::FirstRow | MergeEngine::PartialUpdate
            );
        if task.is_primary_key()
            && (output_rows > input_rows
                || (input_rows > 0 && output_rows == 0 && !ignores_retracts))
        {
            return Err(invalid(
                "PK compaction must retain each key's merged state unless retracts are ignored",
            ));
        }
        let part = result.metrics();
        metrics.files_removed += part.files_removed;
        metrics.files_added += part.files_added;
        metrics.rows_written += part.rows_written;
        metrics.bytes_written += part.bytes_written;
    }
    ids.sort_unstable();
    if ids.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid("Duplicate compaction task"));
    }
    // Stable across result ordering and task retries; distinct subsets may commit separately.
    let mut digest = Sha256::new();
    digest.update(first.task.context.plan_id.as_bytes());
    for id in ids {
        digest.update((id as u64).to_le_bytes());
    }
    Ok(PreparedCompact {
        entries,
        schema_id: first.task.context.schema.id(),
        read_snapshot: first
            .task
            .is_primary_key()
            .then(|| Box::new(first.task.context.snapshot.clone())),
        commit_suffix: hex::encode(digest.finalize()),
        metrics,
    })
}

pub(super) fn ensure_supported(table: &Table) -> Result<()> {
    ensure_table_write_authorized(table)?;
    table.ensure_not_branch_reference_for_write()?;
    let options = table.schema().core_options();
    if table.is_format_table()
        || options.row_tracking_enabled()
        || options.data_evolution_enabled()
        || options.deletion_vectors_enabled()
        || options.query_auth_enabled()
        || !options.blob_fields().is_empty()
        || table
            .schema()
            .fields()
            .iter()
            .any(|field| field.data_type().is_blob_file_field())
        || options.vector_file_format().is_some()
        || options.rowkind_field().is_some()
        || options.bucket() < -1
    {
        return Err(Error::Unsupported {
            message: "Compaction does not support row tracking, data evolution, deletion vectors, query authorization, rowkind fields, postpone buckets or dedicated blob/vector storage".into(),
        });
    }
    if !table.schema().primary_keys().is_empty()
        && (options.bucket() == 0
            || !matches!(
                options.try_changelog_producer()?,
                ChangelogProducer::None | ChangelogProducer::Input
            )
            || (options.merge_engine()? == MergeEngine::FirstRow
                && options.try_changelog_producer()? != ChangelogProducer::None))
    {
        return Err(Error::Unsupported {
            message: "PK compaction requires fixed or dynamic buckets and changelog-producer=none or input; first-row requires changelog-producer=none".into(),
        });
    }
    options.validate_data_file_path_directory()
}

fn ensure_snapshot_supported(table: &Table, snapshot: &Snapshot) -> Result<()> {
    if snapshot.index_manifest().is_some()
        && (table.schema().primary_keys().is_empty()
            || table.schema().core_options().bucket() != -1)
    {
        return Err(Error::Unsupported {
            message: "Compaction only supports dynamic-bucket HASH index manifests".into(),
        });
    }
    Ok(())
}

/// HASH indexes contain key-to-bucket assignments, independent of data files.
/// Preserve their manifest verbatim, including hashes of deleted keys. Other
/// index types may address rewritten files or row positions and cannot be reused.
pub(super) async fn validate_snapshot_indexes(
    table: &Table,
    snapshot: &Snapshot,
) -> Result<Option<String>> {
    ensure_snapshot_supported(table, snapshot)?;
    let Some(name) = snapshot.index_manifest() else {
        return Ok(None);
    };
    let path = table.snapshot_manager().manifest_path(name);
    let entries = IndexManifest::read(table.file_io(), &path).await?;
    if entries.iter().any(|entry| {
        entry.index_file.index_type != "HASH"
            || entry.index_file.deletion_vectors_ranges.is_some()
            || entry.index_file.global_index_meta.is_some()
    }) {
        return Err(Error::Unsupported {
            message: "Compaction only supports dynamic-bucket HASH index manifests".into(),
        });
    }
    Ok(Some(name.to_string()))
}

fn validate_file(file: &DataFileMeta, primary_key: bool) -> Result<()> {
    if file.row_count < 0
        || file.file_size < 0
        || file.first_row_id.is_some()
        || file.write_cols.is_some()
        || file.level < 0
        || file
            .delete_row_count
            .is_some_and(|count| count < 0 || (!primary_key && count != 0))
        || file.file_name.contains(".vector.")
        || super::dedicated_format_file_writer::is_blob_or_video_file_name(&file.file_name)
    {
        return Err(Error::Unsupported {
            message: "Compaction requires complete data files with known row counts".into(),
        });
    }
    Ok(())
}

fn invalid(message: &str) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}
