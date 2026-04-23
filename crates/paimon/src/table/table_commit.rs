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

//! Table commit logic for Paimon write operations.
//!
//! Reference: [org.apache.paimon.operation.FileStoreCommitImpl](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java)
//! and [pypaimon table_commit.py / file_store_commit.py](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/write/)

use crate::io::FileIO;
use crate::spec::stats::BinaryTableStats;
use crate::spec::FileKind;
use crate::spec::{
    datums_to_binary_row, extract_datum, BinaryRow, CommitKind, CoreOptions, DataType, Datum,
    IndexManifest, IndexManifestEntry, Manifest, ManifestEntry, ManifestFileMeta, ManifestList,
    PartitionStatistics, Snapshot,
};
use crate::table::commit_message::CommitMessage;
use crate::table::partition_filter::PartitionFilter;
use crate::table::snapshot_commit::SnapshotCommit;
use crate::table::{SnapshotManager, Table, TableScan};
use crate::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Batch commit identifier (i64::MAX), same as Python's BATCH_COMMIT_IDENTIFIER.
const BATCH_COMMIT_IDENTIFIER: i64 = i64::MAX;

/// Table commit logic for Paimon write operations.
///
/// Provides atomic commit functionality including append, overwrite and truncate
pub struct TableCommit {
    table: Table,
    snapshot_manager: SnapshotManager,
    snapshot_commit: Arc<dyn SnapshotCommit>,
    commit_user: String,
    total_buckets: i32,
    // commit config
    commit_max_retries: u32,
    commit_timeout_ms: u64,
    commit_min_retry_wait_ms: u64,
    commit_max_retry_wait_ms: u64,
    row_tracking_enabled: bool,
    partition_default_name: String,
}

impl TableCommit {
    pub fn new(table: Table, commit_user: String) -> Self {
        let snapshot_manager = SnapshotManager::new(table.file_io.clone(), table.location.clone());
        let snapshot_commit = if let Some(env) = &table.rest_env {
            env.snapshot_commit()
        } else {
            Arc::new(crate::table::snapshot_commit::RenamingSnapshotCommit::new(
                snapshot_manager.clone(),
            ))
        };
        let core_options = CoreOptions::new(table.schema().options());
        let total_buckets = core_options.bucket();
        let commit_max_retries = core_options.commit_max_retries();
        let commit_timeout_ms = core_options.commit_timeout_ms();
        let commit_min_retry_wait_ms = core_options.commit_min_retry_wait_ms();
        let commit_max_retry_wait_ms = core_options.commit_max_retry_wait_ms();
        let row_tracking_enabled = core_options.row_tracking_enabled();
        let partition_default_name = core_options.partition_default_name().to_string();
        Self {
            table,
            snapshot_manager,
            snapshot_commit,
            commit_user,
            total_buckets,
            commit_max_retries,
            commit_timeout_ms,
            commit_min_retry_wait_ms,
            commit_max_retry_wait_ms,
            row_tracking_enabled,
            partition_default_name,
        }
    }

    /// Commit new files in APPEND mode.
    pub async fn commit(&self, commit_messages: Vec<CommitMessage>) -> Result<()> {
        if commit_messages.is_empty() {
            return Ok(());
        }

        let entries = self.messages_to_entries(&commit_messages);
        let new_index_entries = self.messages_to_index_entries(&commit_messages);
        self.try_commit(CommitEntriesPlan::Direct {
            entries,
            new_index_entries,
        })
        .await
    }

    /// Overwrite partitions with new data.
    ///
    /// When `static_partitions` is `None`, extracts the set of partitions
    /// touched by `commit_messages` and overwrites only those (dynamic partition overwrite).
    /// When `static_partitions` is `Some`, uses the caller-provided partition spec
    /// to determine which partitions to replace (static partition overwrite).
    /// A partial spec (not all partition keys specified) uses predicate-based filtering
    /// so that all matching partitions are overwritten.
    /// For unpartitioned tables this is a full table overwrite.
    ///
    /// When `static_partitions` is `Some` but `commit_messages` is empty,
    /// the specified partitions are truncated (all existing data removed, nothing written).
    pub async fn overwrite(
        &self,
        commit_messages: Vec<CommitMessage>,
        static_partitions: Option<HashMap<String, Option<Datum>>>,
    ) -> Result<()> {
        if commit_messages.is_empty() && static_partitions.is_none() {
            return Ok(());
        }

        let new_entries = self.messages_to_entries(&commit_messages);
        let new_index_entries = self.messages_to_index_entries(&commit_messages);

        let partition_filter = if let Some(sp) = static_partitions {
            let partition_keys = self.table.schema().partition_keys();
            let partition_fields = self.table.schema().partition_fields();
            let is_full_spec = partition_keys.iter().all(|k| sp.contains_key(k));

            if is_full_spec {
                let bytes = self.partitions_to_bytes(&[sp]);
                Some(PartitionFilter::from_partition_set(
                    bytes,
                    &partition_fields,
                )?)
            } else {
                Some(self.build_static_partition_predicate(&sp, &partition_fields)?)
            }
        } else {
            self.build_dynamic_partition_filter(&commit_messages)?
        };

        self.try_commit(CommitEntriesPlan::Overwrite {
            partition_filter,
            new_entries,
            new_index_entries,
        })
        .await
    }

    /// Build a predicate-based partition filter from a partial static partition spec.
    fn build_static_partition_predicate(
        &self,
        static_partitions: &HashMap<String, Option<Datum>>,
        partition_fields: &[crate::spec::DataField],
    ) -> Result<PartitionFilter> {
        use crate::spec::PredicateBuilder;
        let pb = PredicateBuilder::new(partition_fields);
        let mut predicates = Vec::new();
        for (key, value) in static_partitions {
            // Currently all values from parse_static_partitions are Some;
            // None would represent an explicit NULL partition value.
            let pred = match value {
                Some(datum) => pb.equal(key, datum.clone())?,
                None => pb.is_null(key)?,
            };
            predicates.push(pred);
        }
        let combined = if predicates.len() == 1 {
            predicates.into_iter().next().unwrap()
        } else {
            crate::spec::Predicate::and(predicates)
        };
        Ok(PartitionFilter::from_predicate(combined, partition_fields))
    }

    /// Build a dynamic partition filter from the partitions present in commit messages.
    ///
    /// Returns `None` for unpartitioned tables (full table overwrite).
    /// Uses `PartitionSet` for O(1) byte-level matching.
    fn build_dynamic_partition_filter(
        &self,
        commit_messages: &[CommitMessage],
    ) -> Result<Option<PartitionFilter>> {
        let partition_fields = self.table.schema().partition_fields();
        if partition_fields.is_empty() {
            return Ok(None);
        }

        let mut partition_bytes_set: HashSet<Vec<u8>> = HashSet::new();
        for msg in commit_messages {
            partition_bytes_set.insert(msg.partition.clone());
        }

        Ok(Some(PartitionFilter::from_partition_set(
            partition_bytes_set,
            &partition_fields,
        )?))
    }

    /// Build a partition filter from manifest entries for scan pushdown.
    ///
    /// Returns `None` for unpartitioned tables (scan everything).
    /// Uses `PartitionSet` for O(1) byte-level matching.
    fn build_entries_partition_filter(
        &self,
        entries: &[&ManifestEntry],
    ) -> Result<Option<PartitionFilter>> {
        let partition_fields = self.table.schema().partition_fields();
        if partition_fields.is_empty() {
            return Ok(None);
        }

        let mut partition_bytes_set: HashSet<Vec<u8>> = HashSet::new();
        for entry in entries {
            partition_bytes_set.insert(entry.partition().to_vec());
        }

        Ok(Some(PartitionFilter::from_partition_set(
            partition_bytes_set,
            &partition_fields,
        )?))
    }

    /// Drop specific partitions (OVERWRITE with only deletes).
    pub async fn truncate_partitions(
        &self,
        partitions: Vec<HashMap<String, Option<Datum>>>,
    ) -> Result<()> {
        if partitions.is_empty() {
            return Ok(());
        }

        let partition_fields = self.table.schema().partition_fields();
        let partition_filter = PartitionFilter::from_partition_set(
            self.partitions_to_bytes(&partitions),
            &partition_fields,
        )?;

        self.try_commit(CommitEntriesPlan::Overwrite {
            partition_filter: Some(partition_filter),
            new_entries: vec![],
            new_index_entries: vec![],
        })
        .await
    }

    fn partitions_to_bytes(
        &self,
        partitions: &[HashMap<String, Option<Datum>>],
    ) -> HashSet<Vec<u8>> {
        let partition_fields = self.table.schema().partition_fields();
        let partition_keys = self.table.schema().partition_keys();
        partitions
            .iter()
            .map(|p| {
                let owned_datums: Vec<(Option<Datum>, DataType)> = partition_keys
                    .iter()
                    .enumerate()
                    .map(|(i, key)| {
                        let datum = p.get(key).cloned().flatten();
                        let dt = partition_fields[i].data_type().clone();
                        (datum, dt)
                    })
                    .collect();
                let refs: Vec<(&Option<Datum>, &DataType)> =
                    owned_datums.iter().map(|(d, t)| (d, t)).collect();
                datums_to_binary_row(&refs)
            })
            .collect()
    }

    /// Truncate the entire table (OVERWRITE with no filter, only deletes).
    pub async fn truncate_table(&self) -> Result<()> {
        self.try_commit(CommitEntriesPlan::Overwrite {
            partition_filter: None,
            new_entries: vec![],
            new_index_entries: vec![],
        })
        .await
    }

    /// Try to commit with retries.
    async fn try_commit(&self, plan: CommitEntriesPlan) -> Result<()> {
        let mut retry_count = 0u32;
        let mut last_snapshot_for_dup_check: Option<Snapshot> = None;
        let start_time_ms = current_time_millis();

        loop {
            let latest_snapshot = self.snapshot_manager.get_latest_snapshot().await?;
            let resolved = self.resolve_commit(&plan, &latest_snapshot).await?;

            if resolved.entries.is_empty() {
                break;
            }

            // Check for duplicate commit (idempotency on retry)
            if self
                .is_duplicate_commit(
                    &last_snapshot_for_dup_check,
                    &latest_snapshot,
                    &resolved.kind,
                )
                .await
            {
                break;
            }

            let result = self.try_commit_once(resolved, &latest_snapshot).await?;

            match result {
                true => break,
                false => {
                    last_snapshot_for_dup_check = latest_snapshot;
                }
            }

            let elapsed_ms = current_time_millis() - start_time_ms;
            if elapsed_ms > self.commit_timeout_ms || retry_count >= self.commit_max_retries {
                let snap_id = last_snapshot_for_dup_check
                    .as_ref()
                    .map(|s| s.id() + 1)
                    .unwrap_or(1);
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Commit failed for snapshot {} after {} millis with {} retries, \
                         there may exist commit conflicts between multiple jobs.",
                        snap_id, elapsed_ms, retry_count
                    ),
                    source: None,
                });
            }

            self.commit_retry_wait(retry_count).await;
            retry_count += 1;
        }

        Ok(())
    }

    /// Single commit attempt.
    async fn try_commit_once(
        &self,
        mut resolved: ResolvedCommit,
        latest_snapshot: &Option<Snapshot>,
    ) -> Result<bool> {
        let new_snapshot_id = latest_snapshot.as_ref().map(|s| s.id() + 1).unwrap_or(1);

        // Row tracking
        let mut next_row_id: Option<i64> = None;
        if self.row_tracking_enabled {
            let first_row_id_start = latest_snapshot
                .as_ref()
                .and_then(|s| s.next_row_id())
                .unwrap_or(0);
            let (assigned, nrid) = self.assign_row_tracking_meta(
                new_snapshot_id,
                first_row_id_start,
                resolved.entries,
            );
            resolved.entries = assigned;
            next_row_id = Some(nrid);
        }

        let file_io = self.snapshot_manager.file_io();
        let manifest_dir = self.snapshot_manager.manifest_dir();

        let unique_id = uuid::Uuid::new_v4();
        let base_manifest_list_name = format!("manifest-list-{unique_id}-0");
        let delta_manifest_list_name = format!("manifest-list-{unique_id}-1");
        let new_manifest_name = format!("manifest-{}-0", uuid::Uuid::new_v4());

        let base_manifest_list_path = format!("{manifest_dir}/{base_manifest_list_name}");
        let delta_manifest_list_path = format!("{manifest_dir}/{delta_manifest_list_name}");
        let new_manifest_path = format!("{manifest_dir}/{new_manifest_name}");

        // Write manifest file
        let new_manifest_file_meta = self
            .write_manifest_file(
                file_io,
                &new_manifest_path,
                &new_manifest_name,
                &resolved.entries,
            )
            .await?;

        // Write delta manifest list
        ManifestList::write(
            file_io,
            &delta_manifest_list_path,
            &[new_manifest_file_meta],
        )
        .await?;

        // Read existing manifests (base + delta from previous snapshot) and write base manifest list
        let mut total_record_count: i64 = 0;
        let existing_manifest_files = if let Some(snap) = latest_snapshot {
            let base_path = format!("{manifest_dir}/{}", snap.base_manifest_list());
            let delta_path = format!("{manifest_dir}/{}", snap.delta_manifest_list());
            let base_files = ManifestList::read(file_io, &base_path).await?;
            let delta_files = ManifestList::read(file_io, &delta_path).await?;
            if let Some(prev) = snap.total_record_count() {
                total_record_count += prev;
            }
            let mut all = base_files;
            all.extend(delta_files);
            all
        } else {
            vec![]
        };

        ManifestList::write(file_io, &base_manifest_list_path, &existing_manifest_files).await?;

        // Calculate delta record count
        let mut delta_record_count: i64 = 0;
        for entry in &resolved.entries {
            match entry.kind() {
                FileKind::Add => delta_record_count += entry.file().row_count,
                FileKind::Delete => delta_record_count -= entry.file().row_count,
            }
        }
        total_record_count += delta_record_count;

        let snapshot = Snapshot::builder()
            .version(3)
            .id(new_snapshot_id)
            .schema_id(self.table.schema().id())
            .base_manifest_list(base_manifest_list_name)
            .delta_manifest_list(delta_manifest_list_name)
            .commit_user(self.commit_user.clone())
            .commit_identifier(BATCH_COMMIT_IDENTIFIER)
            .commit_kind(resolved.kind)
            .time_millis(current_time_millis())
            .total_record_count(Some(total_record_count))
            .delta_record_count(Some(delta_record_count))
            .next_row_id(next_row_id)
            .index_manifest(resolved.index_manifest_name)
            .build();

        let statistics = self.generate_partition_statistics(&resolved.entries)?;

        self.snapshot_commit.commit(&snapshot, &statistics).await
    }

    /// Write an index manifest file from already-merged entries.
    ///
    /// Returns `None` if `merged_index_entries` is empty.
    async fn write_index_manifest(
        file_io: &FileIO,
        manifest_dir: &str,
        merged_index_entries: &[IndexManifestEntry],
    ) -> Result<Option<String>> {
        if merged_index_entries.is_empty() {
            return Ok(None);
        }
        let name = format!("index-manifest-{}-0", uuid::Uuid::new_v4());
        let path = format!("{manifest_dir}/{name}");
        IndexManifest::write(file_io, &path, merged_index_entries).await?;
        Ok(Some(name))
    }

    /// Write a manifest file and return its metadata.
    async fn write_manifest_file(
        &self,
        file_io: &FileIO,
        path: &str,
        file_name: &str,
        entries: &[ManifestEntry],
    ) -> Result<ManifestFileMeta> {
        Manifest::write(file_io, path, entries).await?;

        let mut added_file_count: i64 = 0;
        let mut deleted_file_count: i64 = 0;
        for entry in entries {
            match entry.kind() {
                FileKind::Add => added_file_count += 1,
                FileKind::Delete => deleted_file_count += 1,
            }
        }

        // Get file size
        let status = file_io.get_status(path).await?;

        let partition_stats = self.compute_partition_stats(entries)?;

        Ok(ManifestFileMeta::new(
            file_name.to_string(),
            status.size as i64,
            added_file_count,
            deleted_file_count,
            partition_stats,
            self.table.schema().id(),
        ))
    }

    /// Check if this commit was already completed (idempotency).
    async fn is_duplicate_commit(
        &self,
        last_snapshot_for_dup_check: &Option<Snapshot>,
        latest_snapshot: &Option<Snapshot>,
        commit_kind: &CommitKind,
    ) -> bool {
        if let (Some(prev_snap), Some(latest)) = (last_snapshot_for_dup_check, latest_snapshot) {
            let start_id = prev_snap.id() + 1;
            for snapshot_id in start_id..=latest.id() {
                if let Ok(snap) = self.snapshot_manager.get_snapshot(snapshot_id).await {
                    if snap.commit_user() == self.commit_user && snap.commit_kind() == commit_kind {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Resolve commit entries and merge index entries based on the plan type.
    async fn resolve_commit(
        &self,
        plan: &CommitEntriesPlan,
        latest_snapshot: &Option<Snapshot>,
    ) -> Result<ResolvedCommit> {
        let file_io = self.snapshot_manager.file_io();
        let manifest_dir = self.snapshot_manager.manifest_dir();

        match plan {
            CommitEntriesPlan::Direct {
                entries,
                new_index_entries,
            } => {
                if self.row_tracking_enabled {
                    self.validate_row_id_alignment(entries, latest_snapshot)
                        .await?;
                }
                self.validate_deleted_files(entries, latest_snapshot)
                    .await?;
                // Auto-promote to OVERWRITE when CoW rewrites produce Delete entries.
                // This ensures the snapshot correctly reflects file replacements.
                let has_delete = entries.iter().any(|e| *e.kind() == FileKind::Delete);
                let kind = if has_delete {
                    CommitKind::OVERWRITE
                } else {
                    CommitKind::APPEND
                };

                let index_manifest_name = if new_index_entries.is_empty() {
                    latest_snapshot
                        .as_ref()
                        .and_then(|s| s.index_manifest().map(|s| s.to_string()))
                } else {
                    let mut all =
                        Self::read_prev_index_entries(file_io, &manifest_dir, latest_snapshot)
                            .await?;
                    let new_keys: HashSet<(Vec<u8>, i32)> = new_index_entries
                        .iter()
                        .filter(|e| e.index_file.index_type == "HASH")
                        .map(|e| (e.partition.clone(), e.bucket))
                        .collect();
                    all.retain(|e| {
                        if e.index_file.index_type == "HASH" {
                            !new_keys.contains(&(e.partition.clone(), e.bucket))
                        } else {
                            true
                        }
                    });
                    all.extend_from_slice(new_index_entries);
                    Self::write_index_manifest(file_io, &manifest_dir, &all).await?
                };

                Ok(ResolvedCommit {
                    entries: entries.clone(),
                    kind,
                    index_manifest_name,
                })
            }
            CommitEntriesPlan::Overwrite {
                partition_filter,
                new_entries,
                new_index_entries,
            } => {
                let entries = self
                    .generate_overwrite_entries(
                        latest_snapshot,
                        partition_filter.as_ref(),
                        new_entries,
                    )
                    .await?;

                let mut all =
                    Self::read_prev_index_entries(file_io, &manifest_dir, latest_snapshot).await?;
                match partition_filter.as_ref() {
                    None => all.clear(),
                    Some(filter) => {
                        let mut retained = Vec::new();
                        for e in all {
                            if !filter.matches_entry(&e.partition)? {
                                retained.push(e);
                            }
                        }
                        all = retained;
                    }
                }
                all.extend_from_slice(new_index_entries);
                let index_manifest_name =
                    Self::write_index_manifest(file_io, &manifest_dir, &all).await?;

                Ok(ResolvedCommit {
                    entries,
                    kind: CommitKind::OVERWRITE,
                    index_manifest_name,
                })
            }
        }
    }

    /// Read index entries from the previous snapshot's index manifest.
    async fn read_prev_index_entries(
        file_io: &FileIO,
        manifest_dir: &str,
        latest_snapshot: &Option<Snapshot>,
    ) -> Result<Vec<IndexManifestEntry>> {
        if let Some(snap) = latest_snapshot {
            if let Some(prev_index_manifest) = snap.index_manifest() {
                let prev_path = format!("{manifest_dir}/{prev_index_manifest}");
                return IndexManifest::read(file_io, &prev_path).await;
            }
        }
        Ok(vec![])
    }

    /// Generate overwrite entries: DELETE existing + ADD new.
    async fn generate_overwrite_entries(
        &self,
        latest_snapshot: &Option<Snapshot>,
        partition_filter: Option<&PartitionFilter>,
        new_entries: &[ManifestEntry],
    ) -> Result<Vec<ManifestEntry>> {
        let mut entries = Vec::new();

        if let Some(snap) = latest_snapshot {
            let scan = TableScan::new(
                &self.table,
                partition_filter.cloned(),
                vec![],
                None,
                None,
                None,
            )
            .with_scan_all_files();
            let current_entries = scan.plan_manifest_entries(snap).await?;
            for entry in current_entries {
                entries.push(entry.with_kind(FileKind::Delete));
            }
        }

        entries.extend(new_entries.iter().cloned());
        Ok(entries)
    }

    /// Assign row tracking metadata: snapshot ID as sequence number, and
    /// first_row_id for new APPEND files that don't already have one.
    /// Normal files advance the main counter. Blob files (identified by file name)
    /// use per-column counters starting from the same base, since each blob column
    /// rolls independently.
    fn assign_row_tracking_meta(
        &self,
        snapshot_id: i64,
        first_row_id_start: i64,
        entries: Vec<ManifestEntry>,
    ) -> (Vec<ManifestEntry>, i64) {
        let mut result = Vec::with_capacity(entries.len());
        let mut start = first_row_id_start;
        // Per blob column (write_cols key) counter, each starts from first_row_id_start.
        let mut blob_starts: HashMap<Vec<String>, i64> = HashMap::new();

        for entry in entries {
            let mut entry = entry.with_sequence_number(snapshot_id, snapshot_id);
            if *entry.kind() == FileKind::Add
                && entry.file().file_source == Some(0) // APPEND
                && entry.file().first_row_id.is_none()
            {
                let is_blob_file =
                    crate::table::blob_file_writer::is_blob_file_name(&entry.file().file_name);
                if is_blob_file {
                    let key = entry.file().write_cols.clone().unwrap_or_default();
                    let blob_start = blob_starts.entry(key).or_insert(first_row_id_start);
                    entry = entry.with_first_row_id(*blob_start);
                    *blob_start += entry.file().row_count;
                } else {
                    entry = entry.with_first_row_id(start);
                    start += entry.file().row_count;
                }
            }
            result.push(entry);
        }

        (result, start)
    }

    /// Validate that files with pre-assigned `first_row_id` (e.g. partial-column
    /// files from MERGE INTO) still match existing files in the current snapshot.
    ///
    /// When MERGE INTO and COMPACT run concurrently, compaction may rewrite the
    /// original files that partial-column files reference. If the original file's
    /// row ID range no longer exists, the partial-column files become invalid and
    /// the commit must be rejected.
    async fn validate_row_id_alignment(
        &self,
        commit_entries: &[ManifestEntry],
        latest_snapshot: &Option<Snapshot>,
    ) -> Result<()> {
        // Collect files that already have first_row_id assigned (pre-set by writer).
        let files_to_check: Vec<_> = commit_entries
            .iter()
            .filter(|e| *e.kind() == FileKind::Add && e.file().first_row_id.is_some())
            .collect();

        if files_to_check.is_empty() {
            return Ok(());
        }

        let snap = match latest_snapshot {
            Some(s) => s,
            None => {
                // No existing snapshot means no existing files — any pre-assigned
                // first_row_id cannot match anything.
                let entry = &files_to_check[0];
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Row ID conflict: file '{}' has pre-assigned first_row_id={} \
                         but no snapshot exists. The referenced files may have been removed \
                         by a concurrent compaction.",
                        entry.file().file_name,
                        entry.file().first_row_id.unwrap(),
                    ),
                    source: None,
                });
            }
        };

        // Read current files from the latest snapshot, filtered by partitions.
        let partition_filter = self.build_entries_partition_filter(&files_to_check)?;
        let scan = TableScan::new(&self.table, partition_filter, vec![], None, None, None)
            .with_scan_all_files();
        let existing_entries = scan.plan_manifest_entries(snap).await?;

        // Build index: (partition, bucket, first_row_id, row_count)
        let existing_index: HashSet<(&[u8], i32, i64, i64)> = existing_entries
            .iter()
            .filter_map(|e| {
                e.file()
                    .first_row_id
                    .map(|fid| (e.partition(), e.bucket(), fid, e.file().row_count))
            })
            .collect();

        for entry in &files_to_check {
            let fid = entry.file().first_row_id.unwrap();
            let key = (
                entry.partition(),
                entry.bucket(),
                fid,
                entry.file().row_count,
            );
            if !existing_index.contains(&key) {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Row ID conflict: file '{}' references first_row_id={}, row_count={} \
                         in partition/bucket ({}, {}), but no matching file exists in the \
                         current snapshot. The referenced file may have been rewritten by a \
                         concurrent compaction.",
                        entry.file().file_name,
                        fid,
                        entry.file().row_count,
                        entry.bucket(),
                        entry.file().row_count,
                    ),
                    source: None,
                });
            }
        }

        Ok(())
    }

    /// Validate that files marked for deletion actually exist in the current snapshot.
    ///
    /// For CoW UPDATE/DELETE, the commit contains `FileKind::Delete` entries for
    /// files being replaced. If a concurrent commit has already removed or rewritten
    /// those files, the delete entries become stale and the commit must be rejected.
    async fn validate_deleted_files(
        &self,
        commit_entries: &[ManifestEntry],
        latest_snapshot: &Option<Snapshot>,
    ) -> Result<()> {
        let delete_entries: Vec<_> = commit_entries
            .iter()
            .filter(|e| *e.kind() == FileKind::Delete)
            .collect();

        if delete_entries.is_empty() {
            return Ok(());
        }

        let snap = match latest_snapshot {
            Some(s) => s,
            None => {
                let entry = &delete_entries[0];
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Delete conflict: file '{}' is marked for deletion but no snapshot exists.",
                        entry.file().file_name,
                    ),
                    source: None,
                });
            }
        };

        let partition_filter = self.build_entries_partition_filter(&delete_entries)?;
        let scan = TableScan::new(&self.table, partition_filter, vec![], None, None, None)
            .with_scan_all_files();
        let existing_entries = scan.plan_manifest_entries(snap).await?;

        let existing_files: HashSet<(&[u8], i32, &str)> = existing_entries
            .iter()
            .map(|e| (e.partition(), e.bucket(), e.file().file_name.as_str()))
            .collect();

        for entry in &delete_entries {
            let key = (
                entry.partition(),
                entry.bucket(),
                entry.file().file_name.as_str(),
            );
            if !existing_files.contains(&key) {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Delete conflict: file '{}' in partition/bucket ({}) \
                         does not exist in the current snapshot. \
                         It may have been removed by a concurrent operation.",
                        entry.file().file_name,
                        entry.bucket(),
                    ),
                    source: None,
                });
            }
        }

        Ok(())
    }

    /// Exponential backoff with jitter.
    async fn commit_retry_wait(&self, retry_count: u32) {
        let base_wait = self
            .commit_min_retry_wait_ms
            .saturating_mul(2u64.saturating_pow(retry_count));
        let wait = base_wait.min(self.commit_max_retry_wait_ms);
        // Simple jitter: add up to 20% of wait time
        let jitter = (wait as f64 * 0.2 * rand_f64()) as u64;
        let total_wait = wait + jitter;
        tokio::time::sleep(std::time::Duration::from_millis(total_wait)).await;
    }

    /// Compute partition stats (min/max/null_counts) across all entries.
    fn compute_partition_stats(&self, entries: &[ManifestEntry]) -> Result<BinaryTableStats> {
        let partition_fields = self.table.schema().partition_fields();
        let num_fields = partition_fields.len();

        if num_fields == 0 || entries.is_empty() {
            return Ok(BinaryTableStats::new(vec![], vec![], vec![]));
        }

        let data_types: Vec<_> = partition_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let mut mins: Vec<Option<Datum>> = vec![None; num_fields];
        let mut maxs: Vec<Option<Datum>> = vec![None; num_fields];
        let mut null_counts: Vec<i64> = vec![0; num_fields];

        for entry in entries {
            let partition_bytes = entry.partition();
            if partition_bytes.is_empty() {
                continue;
            }
            let row = BinaryRow::from_serialized_bytes(partition_bytes)?;
            for i in 0..num_fields {
                match extract_datum(&row, i, &data_types[i])? {
                    Some(datum) => {
                        mins[i] = Some(match mins[i].take() {
                            Some(cur) if cur <= datum => cur,
                            Some(_) => datum.clone(),
                            None => datum.clone(),
                        });
                        maxs[i] = Some(match maxs[i].take() {
                            Some(cur) if cur >= datum => cur,
                            Some(_) => datum,
                            None => datum,
                        });
                    }
                    None => {
                        null_counts[i] += 1;
                    }
                }
            }
        }

        let min_datums: Vec<_> = mins.iter().zip(data_types.iter()).collect();
        let max_datums: Vec<_> = maxs.iter().zip(data_types.iter()).collect();

        let min_bytes = datums_to_binary_row(&min_datums);
        let max_bytes = datums_to_binary_row(&max_datums);
        let null_counts = null_counts.into_iter().map(Some).collect();

        Ok(BinaryTableStats::new(min_bytes, max_bytes, null_counts))
    }

    /// Generate per-partition statistics from commit entries.
    ///
    /// Reference: [pypaimon FileStoreCommit._generate_partition_statistics](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/write/file_store_commit.py)
    fn generate_partition_statistics(
        &self,
        entries: &[ManifestEntry],
    ) -> Result<Vec<PartitionStatistics>> {
        let partition_fields = self.table.schema().partition_fields();
        let data_types: Vec<_> = partition_fields
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let partition_keys: Vec<_> = self
            .table
            .schema()
            .partition_keys()
            .iter()
            .map(|s| s.to_string())
            .collect();

        let mut stats_map: HashMap<Vec<u8>, PartitionStatistics> = HashMap::new();

        for entry in entries {
            let partition_bytes = entry.partition().to_vec();
            let is_add = *entry.kind() == FileKind::Add;
            let sign: i64 = if is_add { 1 } else { -1 };

            let file = entry.file();
            let file_creation_time = file
                .creation_time
                .map(|t| t.timestamp_millis() as u64)
                .unwrap_or_else(current_time_millis);

            let stats = stats_map.entry(partition_bytes.clone()).or_insert_with(|| {
                // Parse partition spec from BinaryRow
                let spec = self
                    .parse_partition_spec(&partition_bytes, &partition_keys, &data_types)
                    .unwrap_or_default();
                PartitionStatistics {
                    spec,
                    record_count: 0,
                    file_size_in_bytes: 0,
                    file_count: 0,
                    last_file_creation_time: 0,
                    total_buckets: entry.total_buckets(),
                }
            });

            stats.record_count += sign * file.row_count;
            stats.file_size_in_bytes += sign * file.file_size;
            stats.file_count += sign;
            stats.last_file_creation_time = stats.last_file_creation_time.max(file_creation_time);
        }

        Ok(stats_map.into_values().collect())
    }

    /// Parse partition BinaryRow bytes into a HashMap<String, String>.
    fn parse_partition_spec(
        &self,
        partition_bytes: &[u8],
        partition_keys: &[String],
        data_types: &[crate::spec::DataType],
    ) -> Result<HashMap<String, String>> {
        let mut spec = HashMap::new();
        if partition_bytes.is_empty() || partition_keys.is_empty() {
            return Ok(spec);
        }
        let row = BinaryRow::from_serialized_bytes(partition_bytes)?;
        for (i, key) in partition_keys.iter().enumerate() {
            let value = match extract_datum(&row, i, &data_types[i])? {
                Some(datum) => datum.to_string(),
                None => self.partition_default_name.clone(),
            };
            spec.insert(key.clone(), value);
        }
        Ok(spec)
    }

    /// Convert commit messages to manifest entries (ADD/DELETE kind).
    fn messages_to_entries(&self, messages: &[CommitMessage]) -> Vec<ManifestEntry> {
        messages
            .iter()
            .flat_map(|msg| {
                let adds = msg.new_files.iter().map(|file| {
                    ManifestEntry::new(
                        FileKind::Add,
                        msg.partition.clone(),
                        msg.bucket,
                        self.total_buckets,
                        file.clone(),
                        2,
                    )
                });
                let deletes = msg.deleted_files.iter().map(|file| {
                    ManifestEntry::new(
                        FileKind::Delete,
                        msg.partition.clone(),
                        msg.bucket,
                        self.total_buckets,
                        file.clone(),
                        2,
                    )
                });
                adds.chain(deletes)
            })
            .collect()
    }

    /// Convert commit messages to index manifest entries (ADD kind).
    fn messages_to_index_entries(&self, messages: &[CommitMessage]) -> Vec<IndexManifestEntry> {
        messages
            .iter()
            .flat_map(|msg| {
                msg.new_index_files
                    .iter()
                    .map(move |index_file| IndexManifestEntry {
                        kind: FileKind::Add,
                        partition: msg.partition.clone(),
                        bucket: msg.bucket,
                        index_file: index_file.clone(),
                        version: 1,
                    })
            })
            .collect()
    }
}

/// Plan for resolving commit entries.
enum CommitEntriesPlan {
    /// Caller-provided entries. May contain `FileKind::Delete` entries from CoW
    /// rewrites, in which case `resolve_commit` auto-promotes to `CommitKind::OVERWRITE`.
    Direct {
        entries: Vec<ManifestEntry>,
        new_index_entries: Vec<IndexManifestEntry>,
    },
    /// Overwrite with optional partition filter.
    Overwrite {
        partition_filter: Option<PartitionFilter>,
        new_entries: Vec<ManifestEntry>,
        new_index_entries: Vec<IndexManifestEntry>,
    },
}

/// Fully resolved commit ready for writing.
struct ResolvedCommit {
    entries: Vec<ManifestEntry>,
    kind: CommitKind,
    index_manifest_name: Option<String>,
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Random f64 in [0, 1) using RandomState for per-process entropy.
fn rand_f64() -> f64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64,
    );
    (hasher.finish() as f64) / (u64::MAX as f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{BinaryRowBuilder, DataFileMeta, ManifestList, TableSchema};
    use chrono::{DateTime, Utc};

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn test_schema() -> TableSchema {
        use crate::spec::{DataType, IntType, Schema, VarCharType};
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_partitioned_schema() -> TableSchema {
        use crate::spec::{DataType, IntType, Schema, VarCharType};
        let schema = Schema::builder()
            .column("pt", DataType::VarChar(VarCharType::string_type()))
            .column("id", DataType::Int(IntType::new()))
            .partition_keys(["pt"])
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_table"),
            table_path.to_string(),
            test_schema(),
            None,
        )
    }

    fn test_partitioned_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_table"),
            table_path.to_string(),
            test_partitioned_schema(),
            None,
        )
    }

    fn test_data_file(name: &str, row_count: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 1024,
            row_count,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::new(vec![], vec![], vec![]),
            value_stats: BinaryTableStats::new(vec![], vec![], vec![]),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: Some(
                "2024-09-06T07:45:55.039+00:00"
                    .parse::<DateTime<Utc>>()
                    .unwrap(),
            ),
            delete_row_count: Some(0),
            embedded_index: None,
            first_row_id: None,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
        }
    }

    fn setup_commit(file_io: &FileIO, table_path: &str) -> TableCommit {
        let table = test_table(file_io, table_path);
        TableCommit::new(table, "test-user".to_string())
    }

    fn setup_partitioned_commit(file_io: &FileIO, table_path: &str) -> TableCommit {
        let table = test_partitioned_table(file_io, table_path);
        TableCommit::new(table, "test-user".to_string())
    }

    fn partition_bytes(pt: &str) -> Vec<u8> {
        let mut builder = BinaryRowBuilder::new(1);
        if pt.len() <= 7 {
            builder.write_string_inline(0, pt);
        } else {
            builder.write_string(0, pt);
        }
        builder.build_serialized()
    }

    async fn setup_dirs(file_io: &FileIO, table_path: &str) {
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_append_commit() {
        let file_io = test_file_io();
        let table_path = "memory:/test_append_commit";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);

        let messages = vec![CommitMessage::new(
            vec![],
            0,
            vec![test_data_file("data-0.parquet", 100)],
        )];

        commit.commit(messages).await.unwrap();

        // Verify snapshot was created
        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 1);
        assert_eq!(snapshot.commit_identifier(), BATCH_COMMIT_IDENTIFIER);
        assert_eq!(snapshot.total_record_count(), Some(100));
        assert_eq!(snapshot.delta_record_count(), Some(100));

        // Verify manifest list was written
        let manifest_dir = format!("{table_path}/manifest");
        let delta_path = format!("{manifest_dir}/{}", snapshot.delta_manifest_list());
        let delta_metas = ManifestList::read(&file_io, &delta_path).await.unwrap();
        assert_eq!(delta_metas.len(), 1);
        assert_eq!(delta_metas[0].num_added_files(), 1);

        // Verify manifest entries
        let manifest_path = format!("{manifest_dir}/{}", delta_metas[0].file_name());
        let entries = Manifest::read(&file_io, &manifest_path).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(*entries[0].kind(), FileKind::Add);
        assert_eq!(entries[0].file().file_name, "data-0.parquet");
    }

    #[tokio::test]
    async fn test_multiple_appends() {
        let file_io = test_file_io();
        let table_path = "memory:/test_multiple_appends";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);

        // First commit
        commit
            .commit(vec![CommitMessage::new(
                vec![],
                0,
                vec![test_data_file("data-0.parquet", 100)],
            )])
            .await
            .unwrap();

        // Second commit
        commit
            .commit(vec![CommitMessage::new(
                vec![],
                0,
                vec![test_data_file("data-1.parquet", 200)],
            )])
            .await
            .unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 2);
        assert_eq!(snapshot.total_record_count(), Some(300));
        assert_eq!(snapshot.delta_record_count(), Some(200));
    }

    #[tokio::test]
    async fn test_empty_commit_is_noop() {
        let file_io = test_file_io();
        let table_path = "memory:/test_empty_commit";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);
        commit.commit(vec![]).await.unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap();
        assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn test_truncate_table() {
        let file_io = test_file_io();
        let table_path = "memory:/test_truncate";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);

        // Append some data first
        commit
            .commit(vec![CommitMessage::new(
                vec![],
                0,
                vec![test_data_file("data-0.parquet", 100)],
            )])
            .await
            .unwrap();

        // Truncate
        commit.truncate_table().await.unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 2);
        assert_eq!(snapshot.commit_kind(), &CommitKind::OVERWRITE);
        assert_eq!(snapshot.total_record_count(), Some(0));
        assert_eq!(snapshot.delta_record_count(), Some(-100));
    }

    #[tokio::test]
    async fn test_overwrite_partition() {
        let file_io = test_file_io();
        let table_path = "memory:/test_overwrite_partition";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_partitioned_commit(&file_io, table_path);

        // Append data for partition "a" and "b"
        commit
            .commit(vec![
                CommitMessage::new(
                    partition_bytes("a"),
                    0,
                    vec![test_data_file("data-a.parquet", 100)],
                ),
                CommitMessage::new(
                    partition_bytes("b"),
                    0,
                    vec![test_data_file("data-b.parquet", 200)],
                ),
            ])
            .await
            .unwrap();

        // Overwrite partition "a" with new data (dynamic partition overwrite)
        commit
            .overwrite(
                vec![CommitMessage::new(
                    partition_bytes("a"),
                    0,
                    vec![test_data_file("data-a2.parquet", 50)],
                )],
                None,
            )
            .await
            .unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 2);
        assert_eq!(snapshot.commit_kind(), &CommitKind::OVERWRITE);
        // 300 - 100 (delete a) + 50 (add a2) = 250
        assert_eq!(snapshot.total_record_count(), Some(250));
    }

    #[tokio::test]
    async fn test_drop_partitions() {
        let file_io = test_file_io();
        let table_path = "memory:/test_drop_partitions";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_partitioned_commit(&file_io, table_path);

        // Append data for partitions "a", "b", "c"
        commit
            .commit(vec![
                CommitMessage::new(
                    partition_bytes("a"),
                    0,
                    vec![test_data_file("data-a.parquet", 100)],
                ),
                CommitMessage::new(
                    partition_bytes("b"),
                    0,
                    vec![test_data_file("data-b.parquet", 200)],
                ),
                CommitMessage::new(
                    partition_bytes("c"),
                    0,
                    vec![test_data_file("data-c.parquet", 300)],
                ),
            ])
            .await
            .unwrap();

        // Drop partitions "a" and "c"
        let partitions = vec![
            HashMap::from([("pt".to_string(), Some(Datum::String("a".to_string())))]),
            HashMap::from([("pt".to_string(), Some(Datum::String("c".to_string())))]),
        ];
        commit.truncate_partitions(partitions).await.unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 2);
        assert_eq!(snapshot.commit_kind(), &CommitKind::OVERWRITE);
        // 600 - 100 (a) - 300 (c) = 200
        assert_eq!(snapshot.total_record_count(), Some(200));
    }

    fn null_partition_bytes() -> Vec<u8> {
        let mut builder = BinaryRowBuilder::new(1);
        builder.set_null_at(0);
        builder.build_serialized()
    }

    fn test_row_tracking_schema() -> TableSchema {
        use crate::spec::{DataType, IntType, Schema, VarCharType};
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_row_tracking_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_table"),
            table_path.to_string(),
            test_row_tracking_schema(),
            None,
        )
    }

    fn setup_row_tracking_commit(file_io: &FileIO, table_path: &str) -> TableCommit {
        let table = test_row_tracking_table(file_io, table_path);
        TableCommit::new(table, "test-user".to_string())
    }

    #[tokio::test]
    async fn test_row_id_conflict_rejects_stale_partial_file() {
        // Simulate: initial commit creates a file with row IDs 0-99,
        // then a "partial-column" commit references row IDs 0-49 (wrong range)
        // which should be rejected.
        let file_io = test_file_io();
        let table_path = "memory:/test_row_id_conflict";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_row_tracking_commit(&file_io, table_path);

        // Step 1: Commit an initial file (row_count=100, first_row_id will be assigned as 0)
        let mut initial_file = test_data_file("data-0.parquet", 100);
        initial_file.file_source = Some(0); // APPEND
        commit
            .commit(vec![CommitMessage::new(
                vec![0, 0, 0, 0],
                0,
                vec![initial_file],
            )])
            .await
            .unwrap();

        // Verify snapshot has next_row_id = 100
        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.next_row_id(), Some(100));

        // Step 2: Try to commit a partial-column file referencing row IDs 0-49
        // (wrong row_count — original file has 100 rows, not 50)
        let mut partial_file = test_data_file("partial-0.parquet", 50);
        partial_file.first_row_id = Some(0);
        partial_file.file_source = Some(0);
        partial_file.write_cols = Some(vec!["name".to_string()]);

        let result = commit
            .commit(vec![CommitMessage::new(
                vec![0, 0, 0, 0],
                0,
                vec![partial_file],
            )])
            .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Row ID conflict"),
            "Expected 'Row ID conflict' error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_row_id_conflict_accepts_matching_partial_file() {
        // Partial-column file with matching (first_row_id, row_count) should succeed.
        let file_io = test_file_io();
        let table_path = "memory:/test_row_id_match";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_row_tracking_commit(&file_io, table_path);

        // Step 1: Commit initial file (100 rows, will get first_row_id=0)
        let mut initial_file = test_data_file("data-0.parquet", 100);
        initial_file.file_source = Some(0);
        commit
            .commit(vec![CommitMessage::new(
                vec![0, 0, 0, 0],
                0,
                vec![initial_file],
            )])
            .await
            .unwrap();

        // Step 2: Commit a partial-column file with matching range (0, 100)
        let mut partial_file = test_data_file("partial-0.parquet", 100);
        partial_file.first_row_id = Some(0);
        partial_file.file_source = Some(0);
        partial_file.write_cols = Some(vec!["name".to_string()]);

        commit
            .commit(vec![CommitMessage::new(
                vec![0, 0, 0, 0],
                0,
                vec![partial_file],
            )])
            .await
            .unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 2);
    }

    #[tokio::test]
    async fn test_row_id_conflict_no_snapshot_rejects() {
        // Committing a file with pre-assigned first_row_id when no snapshot exists
        // should be rejected.
        let file_io = test_file_io();
        let table_path = "memory:/test_row_id_no_snap";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_row_tracking_commit(&file_io, table_path);

        let mut partial_file = test_data_file("partial-0.parquet", 100);
        partial_file.first_row_id = Some(0);
        partial_file.file_source = Some(0);
        partial_file.write_cols = Some(vec!["name".to_string()]);

        let result = commit
            .commit(vec![CommitMessage::new(
                vec![0, 0, 0, 0],
                0,
                vec![partial_file],
            )])
            .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Row ID conflict"),
            "Expected 'Row ID conflict' error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_overwrite_null_partition() {
        let file_io = test_file_io();
        let table_path = "memory:/test_overwrite_null_partition";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_partitioned_commit(&file_io, table_path);

        // Append data for partition "a", "b", and NULL
        commit
            .commit(vec![
                CommitMessage::new(
                    partition_bytes("a"),
                    0,
                    vec![test_data_file("data-a.parquet", 100)],
                ),
                CommitMessage::new(
                    partition_bytes("b"),
                    0,
                    vec![test_data_file("data-b.parquet", 200)],
                ),
                CommitMessage::new(
                    null_partition_bytes(),
                    0,
                    vec![test_data_file("data-null.parquet", 300)],
                ),
            ])
            .await
            .unwrap();

        // Overwrite NULL partition only — should NOT affect "a" or "b"
        commit
            .overwrite(
                vec![CommitMessage::new(
                    null_partition_bytes(),
                    0,
                    vec![test_data_file("data-null2.parquet", 50)],
                )],
                None,
            )
            .await
            .unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 2);
        assert_eq!(snapshot.commit_kind(), &CommitKind::OVERWRITE);
        // 600 - 300 (delete null) + 50 (add null2) = 350
        assert_eq!(snapshot.total_record_count(), Some(350));
    }

    #[tokio::test]
    async fn test_delete_conflict_rejects_missing_file() {
        let file_io = test_file_io();
        let table_path = "memory:/test_delete_conflict_missing";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);

        commit
            .commit(vec![CommitMessage::new(
                vec![],
                0,
                vec![test_data_file("data-0.parquet", 100)],
            )])
            .await
            .unwrap();

        let mut msg = CommitMessage::new(
            vec![0, 0, 0, 0],
            0,
            vec![test_data_file("data-new.parquet", 80)],
        );
        msg.deleted_files = vec![test_data_file("nonexistent.parquet", 100)];

        let result = commit.commit(vec![msg]).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Delete conflict"),
            "Expected 'Delete conflict' error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_delete_conflict_accepts_existing_file() {
        let file_io = test_file_io();
        let table_path = "memory:/test_delete_conflict_ok";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);

        commit
            .commit(vec![CommitMessage::new(
                vec![],
                0,
                vec![test_data_file("data-0.parquet", 100)],
            )])
            .await
            .unwrap();

        let mut msg = CommitMessage::new(
            vec![0, 0, 0, 0],
            0,
            vec![test_data_file("data-new.parquet", 80)],
        );
        msg.deleted_files = vec![test_data_file("data-0.parquet", 100)];

        commit.commit(vec![msg]).await.unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 2);
        assert_eq!(snapshot.commit_kind(), &CommitKind::OVERWRITE);
        // 100 - 100 (delete) + 80 (add) = 80
        assert_eq!(snapshot.total_record_count(), Some(80));
    }

    #[tokio::test]
    async fn test_delete_conflict_no_snapshot_rejects() {
        let file_io = test_file_io();
        let table_path = "memory:/test_delete_conflict_no_snap";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);

        let mut msg = CommitMessage::new(vec![0, 0, 0, 0], 0, vec![]);
        msg.deleted_files = vec![test_data_file("data-0.parquet", 100)];

        let result = commit.commit(vec![msg]).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Delete conflict"),
            "Expected 'Delete conflict' error, got: {err_msg}"
        );
    }
}
