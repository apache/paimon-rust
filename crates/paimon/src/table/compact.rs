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

//! Snapshot-bound append, data-evolution and primary-key compaction.

mod append;
mod automatic;
pub(super) use automatic::execute_auto_plan;
pub(super) mod data_evolution;
pub(super) mod deletion_vectors;
pub(super) mod primary_key;
use deletion_vectors::DeletionVectorUpdate;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::data_file_index_writer::FileIndexOptions;
use super::data_file_writer::DataFileWriter;
use super::partition_filter::{validate_partition_filter, PartitionFilter};
use super::snapshot_manifest::read_snapshot_entries;
use super::write_builder::{ensure_table_write_authorized, validate_commit_user};
use super::{DataSplit, Plan, Table, TableCommit, TableScan};
use crate::resource::ResourceContext;
use crate::spec::{
    bucket_path_under, BinaryRow, ChangelogProducer, DataFileMeta, FileKind, IndexManifest,
    IndexManifestEntry, ManifestEntry, MergeEngine, PartitionComputer, Predicate, Snapshot,
    TableSchema,
};
use crate::{Error, Result};

/// Creates planners, executors and a committer with one shared configuration.
/// Output size and append task selection use the captured table options.
/// Supports ordinary append, data-evolution normal files, and fixed or dynamic
/// bucket PK tables. Data evolution preserves row IDs and logical deletions.
pub struct CompactBuilder<'a> {
    table: &'a Table,
    partition_filter: Option<Predicate>,
    resources: Option<ResourceContext>,
    commit_user: String,
}

impl<'a> CompactBuilder<'a> {
    pub(super) fn new(table: &'a Table) -> Self {
        Self {
            table,
            partition_filter: None,
            resources: None,
            commit_user: uuid::Uuid::new_v4().to_string(),
        }
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
        let options = self.table.schema().core_options();
        if options.target_file_size() <= 0 {
            return Err(Error::ConfigInvalid {
                message: "Compaction requires a positive target-file-size".into(),
            });
        }
        let kind = if options.data_evolution_enabled() {
            CompactPlannerKind::DataEvolution(data_evolution::DataEvolutionCompactPlanner::new(
                &options,
            )?)
        } else if self.table.schema().primary_keys().is_empty() {
            CompactPlannerKind::Append(append::AppendCompactPlanner::new(&options)?)
        } else {
            CompactPlannerKind::PrimaryKey(primary_key::PrimaryKeyCompactPlanner {
                automatic: false,
            })
        };
        Ok(CompactPlanner {
            table: self.table.clone(),
            kind,
            partition_filter: self.partition_filter.clone(),
            buckets: None,
        })
    }

    /// Executor resources are local; input files and output settings come from each task.
    pub fn new_executor(&self) -> Result<CompactExecutor> {
        self.new_executor_impl(false)
    }

    pub(super) fn new_auto_executor(&self) -> Result<CompactExecutor> {
        self.new_executor_impl(true)
    }

    fn new_executor_impl(&self, automatic: bool) -> Result<CompactExecutor> {
        ensure_supported(self.table)?;
        let options = self.table.schema().core_options();
        let lookup = options.try_changelog_producer()? == ChangelogProducer::Lookup
            || (!self.table.schema().primary_keys().is_empty()
                && options.merge_engine()? == MergeEngine::FirstRow)
            || (options.deletion_vectors_enabled()
                && !self.table.schema().primary_keys().is_empty());
        let local_cache = self.table.file_io().local_cache();
        if lookup && !automatic && local_cache.is_none() {
            return Err(Error::ConfigInvalid {
                message:
                    "Lookup, first-row or deletion-vector compaction requires local-cache.enabled=true on the catalog/FileIO"
                        .into(),
            });
        }
        Ok(CompactExecutor {
            table: self.table.clone(),
            resources: self.resources.clone(),
            lookup_cache: if lookup {
                Some(Arc::new(if automatic {
                    primary_key::lookup_file::LookupFileCache::automatic(
                        local_cache,
                        self.table.schema().options(),
                    )?
                } else {
                    primary_key::lookup_file::LookupFileCache::new(
                        local_cache.expect("lookup cache validated above"),
                        self.table.schema().options(),
                    )?
                }))
            } else {
                None
            },
            emit_changelog: true,
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

/// Captures a shared snapshot boundary and delegates to the table-mode planner.
pub struct CompactPlanner {
    table: Table,
    kind: CompactPlannerKind,
    partition_filter: Option<Predicate>,
    buckets: Option<CompactBuckets>,
}

pub(super) type CompactBuckets = HashSet<(Vec<u8>, i32, i32)>;

enum CompactPlannerKind {
    Append(append::AppendCompactPlanner),
    PrimaryKey(primary_key::PrimaryKeyCompactPlanner),
    DataEvolution(data_evolution::DataEvolutionCompactPlanner),
}

type BucketFiles = BTreeMap<(Vec<u8>, i32, i32), Vec<DataFileMeta>>;

/// Append and PK planners share the split-to-bucket metadata representation.
/// Eligibility and task selection remain with each table-mode planner.
fn split_files_by_bucket(source: &Plan) -> BucketFiles {
    let mut groups = BucketFiles::new();
    for split in source.splits() {
        groups
            .entry((
                split.partition().to_serialized_bytes(),
                split.bucket(),
                split.total_buckets(),
            ))
            .or_default()
            .extend_from_slice(split.data_files());
    }
    groups
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CompactContext {
    plan_id: String,
    location: String,
    identifier: String,
    branch: String,
    schema: TableSchema,
    snapshot: Snapshot,
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
    #[serde(default)]
    output_level: Option<i32>,
}

impl CompactTask {
    fn needs_captured_inputs(&self) -> bool {
        self.is_primary_key() || self.is_data_evolution()
    }

    pub fn snapshot_id(&self) -> i64 {
        self.context.snapshot.id()
    }

    pub fn partition(&self) -> &[u8] {
        &self.partition
    }

    pub fn bucket(&self) -> i32 {
        self.bucket
    }

    pub(super) fn total_buckets(&self) -> i32 {
        self.total_buckets
    }

    pub fn files(&self) -> &[DataFileMeta] {
        &self.files
    }
}

impl CompactPlanner {
    /// Restrict automatic work to the buckets owned by one committed write.
    pub(super) fn for_write(mut self, buckets: CompactBuckets) -> Self {
        self.buckets = Some(buckets);
        if let CompactPlannerKind::PrimaryKey(planner) = &mut self.kind {
            planner.automatic = true;
        }
        self
    }

    /// Capture a snapshot without writing data or publishing metadata.
    pub async fn plan(&self) -> Result<CompactPlan> {
        ensure_supported(&self.table)?;
        let mut reader = self.table.new_read_builder();
        if let Some(filter) = &self.partition_filter {
            reader.with_filter(filter.clone());
        }
        let scan = if let Some(buckets) = &self.buckets {
            let fields = self.table.schema().partition_fields();
            let filter = if fields.is_empty() {
                None
            } else {
                Some(PartitionFilter::from_partition_set(
                    buckets
                        .iter()
                        .map(|(partition, _, _)| partition.clone())
                        .collect(),
                    &fields,
                )?)
            };
            TableScan::new(&self.table, filter, Vec::new(), None, None, None)
        } else {
            reader.new_scan()
        }
        .with_scan_all_files();
        let source = scan.plan_all().await?.map_splits(|splits| {
            Ok(splits
                .into_iter()
                .filter(|split| {
                    self.buckets.as_ref().is_none_or(|buckets| {
                        buckets.contains(&(
                            split.partition().to_serialized_bytes(),
                            split.bucket(),
                            split.total_buckets(),
                        ))
                    })
                })
                .collect())
        })?;
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
        let tasks = match &self.kind {
            CompactPlannerKind::Append(planner) => planner.plan_tasks(&source, &context)?,
            CompactPlannerKind::PrimaryKey(planner) => planner.plan_tasks(&source, &context)?,
            CompactPlannerKind::DataEvolution(planner) => {
                planner
                    .plan_tasks(&self.table, &scan, &context, self.buckets.as_ref())
                    .await?
            }
        };
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
    lookup_cache: Option<Arc<primary_key::lookup_file::LookupFileCache>>,
    emit_changelog: bool,
}

// Valid only for one immutable automatic round. Publication still reads and
// validates current metadata; manual execution captures fresh inputs each call.
struct CompactInputs {
    snapshot: Snapshot,
    data: HashMap<(Vec<u8>, i32), Vec<ManifestEntry>>,
    indexes: HashMap<(Vec<u8>, i32), Vec<IndexManifestEntry>>,
}

/// Prepared data, changelog and index files owned by the caller until submitted
/// to `TableCommit::commit_compact`.
/// A failed or uncertain commit must retain these files for retry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactResult {
    task: CompactTask,
    files: Vec<DataFileMeta>,
    #[serde(default)]
    changelog_files: Vec<DataFileMeta>,
    #[serde(default)]
    deletion_vector_updates: Option<Vec<DeletionVectorUpdate>>,
    #[serde(default)]
    index_entries: Option<Vec<crate::spec::IndexManifestEntry>>,
}

impl CompactResult {
    /// Atomic index additions and retirements prepared with these data files.
    pub fn index_entries(&self) -> Option<&[crate::spec::IndexManifestEntry]> {
        self.index_entries.as_deref()
    }

    pub fn files(&self) -> &[DataFileMeta] {
        &self.files
    }

    pub fn changelog_files(&self) -> &[DataFileMeta] {
        &self.changelog_files
    }

    pub fn metrics(&self) -> CompactMetrics {
        CompactMetrics {
            files_removed: self.task.files.len(),
            files_added: self.files.len(),
            rows_written: self.files.iter().map(|file| file.row_count as u64).sum(),
            bytes_written: self.files.iter().map(|file| file.file_size as u64).sum(),
            changelog_files_added: self.changelog_files.len(),
            changelog_rows_written: self
                .changelog_files
                .iter()
                .map(|file| file.row_count as u64)
                .sum(),
            changelog_bytes_written: self
                .changelog_files
                .iter()
                .map(|file| file.file_size as u64)
                .sum(),
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
    #[serde(default)]
    pub changelog_files_added: usize,
    #[serde(default)]
    pub changelog_rows_written: u64,
    #[serde(default)]
    pub changelog_bytes_written: u64,
}

impl CompactExecutor {
    pub(super) fn without_changelog(mut self) -> Self {
        self.emit_changelog = false;
        self
    }

    pub async fn execute(&self, task: &CompactTask) -> Result<CompactResult> {
        let inputs = self.capture_inputs(task).await?;
        self.execute_captured(task, &inputs).await
    }

    async fn capture_inputs(&self, task: &CompactTask) -> Result<CompactInputs> {
        ensure_supported(&self.table)?;
        task.validate_target(&self.table)?;
        let manager = self.table.snapshot_manager();
        let snapshot = manager.get_snapshot(task.snapshot_id()).await?;
        if !task.context.snapshot.has_same_read_metadata(&snapshot) {
            return Err(invalid("Compaction snapshot was replaced"));
        }
        let indexes = read_snapshot_indexes(&self.table, &snapshot).await?;
        if !self.table.schema().primary_keys().is_empty()
            && self
                .table
                .schema()
                .core_options()
                .deletion_vectors_enabled()
        {
            primary_key::deletion_vectors::validate_index_references(&indexes)?;
        }
        let active = read_snapshot_entries(&manager, &snapshot, None).await?;
        validate_dv_schemas(&self.table, active.iter().map(|entry| entry.file())).await?;
        let mut data = HashMap::<_, Vec<_>>::new();
        for entry in active {
            data.entry((entry.partition().to_vec(), entry.bucket()))
                .or_default()
                .push(entry);
        }
        let mut by_bucket = HashMap::<_, Vec<_>>::new();
        for entry in indexes {
            by_bucket
                .entry((entry.partition.clone(), entry.bucket))
                .or_default()
                .push(entry);
        }
        Ok(CompactInputs {
            snapshot,
            data,
            indexes: by_bucket,
        })
    }

    async fn execute_captured(
        &self,
        task: &CompactTask,
        inputs: &CompactInputs,
    ) -> Result<CompactResult> {
        task.validate_target(&self.table)?;
        let snapshot = &inputs.snapshot;
        if !task.context.snapshot.has_same_read_metadata(snapshot) {
            return Err(invalid("Compaction snapshot was replaced"));
        }
        let key = (task.partition.clone(), task.bucket);
        let active = inputs.data.get(&key).map(Vec::as_slice).unwrap_or_default();
        let indexes = inputs
            .indexes
            .get(&key)
            .map(Vec::as_slice)
            .unwrap_or_default();
        task.validate_inputs(active)?;
        // validate_target proved the complete schema and branch are identical.
        // Preserve already resolved catalog schemas in the shared manager;
        // rebuilding it here would require REST-only schemas to exist on disk.
        let table = self.table.copy_with_pinned_snapshot(Some(snapshot));
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
        let split = if options.deletion_vectors_enabled() && task.is_primary_key() {
            primary_key::deletion_vectors::with_deletion_entries(&table, &split, indexes)?
        } else {
            split
        };
        if task.is_data_evolution() {
            let files = data_evolution::execute(
                &table,
                task,
                &split,
                partition_path,
                self.resources.clone(),
            )
            .await?;
            return Ok(CompactResult {
                task: task.clone(),
                files,
                changelog_files: vec![],
                deletion_vector_updates: None,
                index_entries: None,
            });
        }
        if task.is_primary_key() {
            let mut deletions = None;
            let lookup = if let Some(cache) = &self.lookup_cache {
                let history = active
                    .iter()
                    .filter(|entry| {
                        entry.partition() == task.partition
                            && entry.bucket() == task.bucket
                            && entry.total_buckets() == task.total_buckets
                            && entry.file().level > task.output_level()
                    })
                    .map(|entry| entry.file().clone())
                    .collect::<Vec<_>>();
                if history.is_empty() {
                    None
                } else {
                    let (provider, collected) = primary_key::lookup_store::provider(
                        &table,
                        &split,
                        history,
                        cache.clone(),
                        self.resources.clone(),
                        indexes,
                    )
                    .await?;
                    deletions = collected;
                    Some(provider)
                }
            } else {
                None
            };
            let (files, changelog_files) = primary_key::execute(
                &table,
                task,
                &split,
                self.resources.clone(),
                lookup,
                self.emit_changelog,
            )
            .await?;
            let mut deletion_vector_updates = Vec::new();
            if let Some(deletions) = deletions {
                for (name, bitmap) in deletions.lock().unwrap().iter() {
                    let mut bytes = Vec::new();
                    bitmap
                        .serialize_into(&mut bytes)
                        .map_err(|error| invalid(&error.to_string()))?;
                    deletion_vector_updates.push(DeletionVectorUpdate {
                        partition: task.partition.clone(),
                        bucket: task.bucket,
                        file_name: name.clone(),
                        bitmap: bytes,
                    });
                }
            }
            let index_entries = match primary_key::indexes::prepare(
                &table,
                task,
                &split,
                indexes,
                active,
                &files,
                self.resources.clone(),
            )
            .await
            {
                Ok(entries) => entries,
                Err(error) => {
                    let mut message =
                        super::CommitMessage::new(task.partition.clone(), task.bucket, files);
                    message.new_changelog_files = changelog_files;
                    let _ = super::TableCommit::new(table.clone(), "compact-cleanup".into())
                        .abort(&[message])
                        .await;
                    return Err(error);
                }
            };
            return Ok(CompactResult {
                task: task.clone(),
                files,
                changelog_files,
                index_entries: Some(index_entries),
                deletion_vector_updates: options
                    .deletion_vectors_enabled()
                    .then_some(deletion_vector_updates),
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
            options.target_file_size(),
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
                changelog_files: vec![],
                deletion_vector_updates: None,
                index_entries: None,
            }),
            Err(error) => {
                writer.abort().await;
                Err(error)
            }
        }
    }
}

impl CompactTask {
    fn is_data_evolution(&self) -> bool {
        self.context.schema.core_options().data_evolution_enabled()
    }

    fn is_primary_key(&self) -> bool {
        !self.context.schema.primary_keys().is_empty()
    }

    fn output_level(&self) -> i32 {
        if let Some(output_level) = self.output_level {
            return output_level;
        }
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
        if self.is_data_evolution() {
            data_evolution::validate_inputs(&self.partition, self.bucket, &self.files, active)?;
        }
        let bucket_files = active
            .iter()
            .filter(|entry| {
                entry.partition() == self.partition
                    && entry.bucket() == self.bucket
                    && entry.total_buckets() == self.total_buckets
            })
            .map(|entry| (entry.file().file_name.as_str(), entry))
            .collect::<HashMap<_, _>>();
        if self.is_primary_key() && primary_key::planner::enabled(&self.context.schema)? {
            let selected = primary_key::planner::select(
                bucket_files
                    .values()
                    .map(|entry| entry.file().clone())
                    .collect(),
                self.context.schema.options(),
            )?
            .ok_or_else(|| invalid("No incremental PK compaction is eligible"))?;
            if selected.0 != self.files || Some(selected.1) != self.output_level {
                return Err(invalid(
                    "PK compaction inputs do not match incremental run selection",
                ));
            }
        } else if self.is_primary_key() && bucket_files.len() != self.files.len() {
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
        let incremental =
            self.is_primary_key() && primary_key::planner::enabled(&self.context.schema)?;
        // Legacy results may be recovered as already committed before their
        // captured inputs are validated. Uncommitted tasks still need the
        // exact incremental selection in validate_inputs.
        if self
            .output_level
            .is_some_and(|output_level| !incremental || output_level <= 0)
        {
            return Err(invalid("Invalid incremental PK compaction selection"));
        }
        let minimum_files = if self.is_primary_key() || self.is_data_evolution() {
            1
        } else {
            2
        };
        if table.schema().core_options().target_file_size() <= 0
            || self.files.len() < minimum_files
            || self.bucket < 0
        {
            return Err(invalid("Invalid compaction task"));
        }
        let mut names = HashSet::new();
        for file in &self.files {
            if self.is_data_evolution() {
                data_evolution::validate_file(file)?;
            } else {
                validate_file(file, self.is_primary_key())?;
            }
            if !names.insert(&file.file_name) {
                return Err(invalid("Duplicate compaction input file"));
            }
        }
        if self.is_data_evolution() {
            data_evolution::row_range(&self.files)?;
        }
        Ok(())
    }
}

/// Validate published PK file changes without depending on executor task metadata.
pub(super) fn validate_primary_key_changes(
    schema: &TableSchema,
    changes: &[ManifestEntry],
    changelog: &[ManifestEntry],
    captured: &[ManifestEntry],
) -> Result<()> {
    fn group(entry: &ManifestEntry) -> (&[u8], i32, i32) {
        (entry.partition(), entry.bucket(), entry.total_buckets())
    }
    let mut inputs = HashMap::<_, Vec<&ManifestEntry>>::new();
    for entry in changes
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Delete)
    {
        inputs.entry(group(entry)).or_default().push(entry);
    }
    let mut active = HashMap::<_, Vec<&DataFileMeta>>::new();
    for entry in captured {
        let key = group(entry);
        if inputs.contains_key(&key) {
            active.entry(key).or_default().push(entry.file());
        }
    }
    let incremental = primary_key::planner::enabled(schema)?;
    let mut output_levels = HashMap::new();
    for (key, removed) in inputs {
        let files = active
            .remove(&key)
            .unwrap_or_default()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let (selected, output_level) = if incremental {
            primary_key::planner::select(files, schema.options())?
                .ok_or_else(|| invalid("No incremental PK compaction is eligible"))?
        } else {
            let level = files
                .iter()
                .map(|file| file.level)
                .max()
                .unwrap_or(0)
                .max(1);
            (files, level)
        };
        let expected = selected
            .iter()
            .map(|file| (file.file_name.as_str(), file))
            .collect::<HashMap<_, _>>();
        if expected.len() != removed.len()
            || removed.iter().any(|entry| {
                expected
                    .get(entry.file().file_name.as_str())
                    .is_none_or(|file| **file != *entry.file())
            })
        {
            return Err(invalid(
                "PK compaction inputs do not match captured run selection",
            ));
        }
        output_levels.insert(key, output_level);
    }
    if changes
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Add)
        .chain(changelog.iter())
        .any(|entry| output_levels.get(&group(entry)) != Some(&entry.file().level))
    {
        return Err(invalid(
            "PK compaction output level does not match captured run selection",
        ));
    }
    Ok(())
}

/// Validated inputs for the existing atomic snapshot publication machinery.
pub(super) struct PreparedCompact {
    pub entries: Vec<ManifestEntry>,
    pub changelog_entries: Vec<ManifestEntry>,
    pub deletion_vector_updates: Option<Vec<DeletionVectorUpdate>>,
    pub index_entries: Option<Vec<crate::spec::IndexManifestEntry>>,
    pub schema_id: i64,
    pub snapshot_id: i64,
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
    let mut changelog_entries = Vec::new();
    let mut deletion_vector_updates = Some(Vec::new());
    let mut index_entries = Some(Vec::new());
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
        if task.is_primary_key() {
            match (&mut index_entries, &result.index_entries) {
                (Some(all), Some(entries)) => {
                    if entries
                        .iter()
                        .any(|e| e.partition != task.partition || e.bucket != task.bucket)
                    {
                        return Err(invalid("PK index output belongs to a different bucket"));
                    }
                    all.extend(entries.iter().cloned());
                }
                (_, None) => index_entries = None,
                _ => {}
            }
        } else if result
            .index_entries
            .as_ref()
            .is_some_and(|entries| !entries.is_empty())
        {
            return Err(invalid(
                "Only PK compaction can prepare source-backed indexes",
            ));
        }
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
        // Missing PK task metadata must not block valid prepared file changes;
        // their output level is checked against the captured selection at commit.
        let output_level = if task.is_primary_key() {
            task.output_level
        } else {
            Some(0)
        };
        let mut output_rows = 0u64;
        for file in &result.files {
            if task.is_data_evolution() {
                data_evolution::validate_file(file)?;
            } else {
                validate_file(file, task.is_primary_key())?;
            }
            if file.schema_id != task.context.schema.id()
                || file.file_source != Some(1)
                || output_level.is_some_and(|level| file.level != level)
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
        if task.is_data_evolution() {
            data_evolution::validate_outputs(task, &result.files)?;
        } else if !task.is_primary_key() && input_rows != output_rows {
            return Err(invalid("Compaction must preserve the input row count"));
        }
        let options = task.context.schema.core_options();
        let dv = task.is_primary_key() && options.deletion_vectors_enabled();
        // First-row can leave every accepted key in retained history, even
        // with ignore-delete=false, and therefore produce no data files.
        let can_omit_keys = dv
            || (task.is_primary_key()
                && (options.merge_engine()? == MergeEngine::FirstRow
                    || (options.ignore_delete()
                        && options.merge_engine()? == MergeEngine::PartialUpdate)));
        if dv
            && result.files.iter().any(|file| {
                file.delete_row_count != Some(0) || file.row_count > i64::from(u32::MAX) + 1
            })
        {
            return Err(invalid(
                "Deletion-vector compaction data must contain only add records",
            ));
        }
        let updates = result
            .deletion_vector_updates
            .as_deref()
            .unwrap_or_default();
        if dv && result.deletion_vector_updates.is_none() {
            // Require this only after already-committed results are recovered.
            deletion_vector_updates = None;
        }
        if !dv && !updates.is_empty() {
            return Err(invalid(
                "Only deletion-vector compaction may publish row deletions",
            ));
        }
        if let Some(collected) = &mut deletion_vector_updates {
            collected.extend(updates.iter().cloned());
        }
        if task.is_primary_key()
            && (output_rows > input_rows || (input_rows > 0 && output_rows == 0 && !can_omit_keys))
        {
            return Err(invalid(
                "PK compaction must retain merged keys unless its merge mode can omit them",
            ));
        }
        let lookup = options.try_changelog_producer()? == ChangelogProducer::Lookup;
        if !lookup && !result.changelog_files.is_empty() {
            return Err(invalid(
                "Only lookup compaction may produce changelog files",
            ));
        }
        let mut changelog_rows = 0u64;
        let changelog_level = output_level.or_else(|| result.files.first().map(|file| file.level));
        for file in &result.changelog_files {
            validate_file(file, true)?;
            if file.schema_id != task.context.schema.id()
                || file.file_source != Some(1)
                || changelog_level.is_some_and(|level| file.level != level)
                || file
                    .delete_row_count
                    .is_none_or(|count| count > file.row_count)
                || !outputs.insert(file.file_name.clone())
                || task.files.iter().any(|old| old.file_name == file.file_name)
            {
                return Err(invalid("Invalid compaction changelog file"));
            }
            changelog_rows += file.row_count as u64;
            changelog_entries.push(ManifestEntry::new(
                FileKind::Add,
                task.partition.clone(),
                task.bucket,
                task.total_buckets,
                file.clone(),
                2,
            ));
        }
        let l0_rows = task
            .files
            .iter()
            .filter(|file| file.level == 0)
            .map(|file| file.row_count as u64)
            .sum::<u64>();
        if changelog_rows > l0_rows.saturating_mul(2) {
            return Err(invalid(
                "Lookup changelogs must be generated by level-0 records",
            ));
        }
        let part = result.metrics();
        metrics.files_removed += part.files_removed;
        metrics.files_added += part.files_added;
        metrics.rows_written += part.rows_written;
        metrics.bytes_written += part.bytes_written;
        metrics.changelog_files_added += part.changelog_files_added;
        metrics.changelog_rows_written += part.changelog_rows_written;
        metrics.changelog_bytes_written += part.changelog_bytes_written;
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
        changelog_entries,
        deletion_vector_updates,
        index_entries,
        schema_id: first.task.context.schema.id(),
        snapshot_id: first.task.snapshot_id(),
        read_snapshot: first
            .task
            .needs_captured_inputs()
            .then(|| Box::new(first.task.context.snapshot.clone())),
        commit_suffix: hex::encode(digest.finalize()),
        metrics,
    })
}

pub(super) fn ensure_supported(table: &Table) -> Result<()> {
    ensure_table_write_authorized(table)?;
    table.ensure_not_branch_reference_for_write()?;
    let options = table.schema().core_options();
    let properties = table.schema().options();
    if !table.schema().primary_keys().is_empty() {
        super::pk_index_options::definitions(table.schema()).map_err(|error| {
            Error::ConfigInvalid {
                message: format!("Invalid primary-key index declaration: {error}"),
            }
        })?;
    }
    if options.data_evolution_enabled() {
        data_evolution::ensure_supported(table)?;
        return options.validate_data_file_path_directory();
    }
    if table.schema().primary_keys().is_empty()
        && options.bucket() > 0
        && !properties
            .get("bucket-append-ordered")
            .is_some_and(|value| value.eq_ignore_ascii_case("false"))
    {
        // Ordered append needs contiguous sequence windows and rewrite counters.
        // Ordinary Rust append writers do not yet restore those counters.
        return Err(Error::Unsupported {
            message: "Compaction of ordered bucketed append tables is not supported; bucket-append-ordered=false is required".into(),
        });
    }
    if properties
        .get("pk-clustering-override")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        || properties.contains_key("record-level.expire-time")
        || (table.schema().primary_keys().is_empty() && options.clustering_incremental_enabled())
    {
        // Clustered KV files are not necessarily primary-key ordered; record
        // expiration must filter versions before merging, as Java does.
        return Err(Error::Unsupported {
            message:
                "Compaction does not support pk-clustering-override, append incremental clustering or record-level.expire-time"
                    .into(),
        });
    }
    if table.is_format_table()
        || options.row_tracking_enabled()
        || options.data_evolution_enabled()
        || (options.deletion_vectors_enabled() && table.schema().primary_keys().is_empty())
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
                ChangelogProducer::None | ChangelogProducer::Input | ChangelogProducer::Lookup
            )
            || (options.merge_engine()? == MergeEngine::FirstRow
                && !matches!(
                    options.try_changelog_producer()?,
                    ChangelogProducer::None | ChangelogProducer::Lookup
                )))
    {
        return Err(Error::Unsupported {
            message: "PK compaction requires fixed or dynamic buckets and changelog-producer=none, input or lookup; first-row requires changelog-producer=none or lookup".into(),
        });
    }
    if options.deletion_vectors_enabled() && options.merge_engine()? != MergeEngine::Deduplicate {
        return Err(Error::Unsupported {
            message: "Primary-key deletion-vector compaction requires deduplicate".into(),
        });
    }
    if options.try_changelog_producer()? == ChangelogProducer::Lookup {
        primary_key::lookup::validate_options(properties)?;
    }
    options.validate_data_file_path_directory()
}

/// Dropping DV tombstones relies on every historical version having used DV.
/// Switching an existing non-DV table requires a migration Rust does not offer.
pub(super) async fn validate_dv_schemas<'a>(
    table: &Table,
    files: impl Iterator<Item = &'a DataFileMeta>,
) -> Result<()> {
    if table.schema().primary_keys().is_empty()
        || !table.schema().core_options().deletion_vectors_enabled()
    {
        return Ok(());
    }
    let mut schemas = HashSet::new();
    for file in files {
        if file.row_count > i64::from(u32::MAX) + 1 {
            return Err(invalid(
                "Deletion-vector compaction file exceeds u32 physical positions",
            ));
        }
        if schemas.insert(file.schema_id)
            && !table
                .schema_manager()
                .schema(file.schema_id)
                .await?
                .core_options()
                .deletion_vectors_enabled()
        {
            return Err(Error::Unsupported {
                message: "Deletion-vector compaction cannot migrate files written without deletion vectors".into(),
            });
        }
    }
    Ok(())
}

fn ensure_snapshot_supported(table: &Table, snapshot: &Snapshot) -> Result<()> {
    if snapshot.index_manifest().is_some()
        && !table.schema().core_options().data_evolution_enabled()
        && !table.schema().core_options().deletion_vectors_enabled()
        && table.schema().primary_keys().is_empty()
    {
        return Err(Error::Unsupported {
            message: "Compaction only supports dynamic-bucket HASH index manifests".into(),
        });
    }
    Ok(())
}

/// HASH indexes contain key-to-bucket assignments, independent of data files.
/// Preserve their manifest verbatim, including hashes of deleted keys. PK source
/// indexes and deletion vectors are validated here and maintained at commit.
pub(super) async fn validate_snapshot_indexes(
    table: &Table,
    snapshot: &Snapshot,
) -> Result<Option<String>> {
    read_snapshot_indexes(table, snapshot).await?;
    Ok(snapshot.index_manifest().map(str::to_owned))
}

async fn read_snapshot_indexes(
    table: &Table,
    snapshot: &Snapshot,
) -> Result<Vec<IndexManifestEntry>> {
    ensure_snapshot_supported(table, snapshot)?;
    let Some(name) = snapshot.index_manifest() else {
        return Ok(vec![]);
    };
    let path = table.snapshot_manager().manifest_path(name);
    let entries = IndexManifest::read(table.file_io(), &path).await?;
    if table.schema().core_options().data_evolution_enabled() {
        data_evolution::validate_indexes(&entries)?;
        return Ok(entries);
    }
    for entry in &entries {
        if !table.schema().primary_keys().is_empty()
            && super::pk_index_maintenance::is_source_index(entry)
        {
            super::pk_index_maintenance::source(entry)?;
            continue;
        }
        if !(entry.index_file.index_type == "HASH"
            || (table.schema().core_options().deletion_vectors_enabled()
                && entry.index_file.index_type == "DELETION_VECTORS"))
            || (entry.index_file.index_type == "HASH"
                && entry.index_file.deletion_vectors_ranges.is_some())
            || entry.index_file.global_index_meta.is_some()
        {
            return Err(Error::Unsupported {
                message: "Compaction requires HASH, deletion-vector or source-backed PK indexes"
                    .into(),
            });
        }
    }
    Ok(entries)
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
