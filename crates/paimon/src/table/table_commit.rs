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
    bucket_dir_name, extract_datum, merge_active_entries, BinaryRow, BinaryRowBuilder, CommitKind,
    CoreOptions, DataFileMeta, DataType, Datum, GlobalIndexColumnUpdateAction, IndexManifest,
    IndexManifestEntry, Manifest, ManifestEntry, ManifestFileMeta, ManifestList, PartitionComputer,
    PartitionStatistics, Predicate, Snapshot, EMPTY_SERIALIZED_ROW, MANIFEST_ENTRY_SCHEMA,
};
use crate::table::commit_message::CommitMessage;
use crate::table::partition_filter::PartitionFilter;
use crate::table::snapshot_commit::SnapshotCommit;
use crate::table::{SnapshotManager, Table, TableScan};
use crate::Result;
use apache_avro::{to_value, Schema};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Batch commit identifier (i64::MAX), same as Python's BATCH_COMMIT_IDENTIFIER.
const BATCH_COMMIT_IDENTIFIER: i64 = i64::MAX;
/// Java RollingFileWriter.CHECK_ROLLING_RECORD_CNT.
const CHECK_ROLLING_RECORD_COUNT: usize = 1000;
const DELETION_VECTORS_INDEX_TYPE: &str = "DELETION_VECTORS";

type PartitionBucketKey = (Vec<u8>, i32);
type RowIdRange = (i64, i64);
type ExistingRowIdRanges = HashMap<PartitionBucketKey, Vec<RowIdRange>>;

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
    manifest_compression: String,
    manifest_target_size: i64,
    manifest_merge_min_count: usize,
    row_tracking_enabled: bool,
    data_evolution_enabled: bool,
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
        let manifest_compression = core_options.manifest_compression().to_string();
        let manifest_target_size = core_options.manifest_target_size();
        let manifest_merge_min_count = core_options.manifest_merge_min_count();
        let row_tracking_enabled = core_options.row_tracking_enabled();
        let data_evolution_enabled = core_options.data_evolution_enabled();
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
            manifest_compression,
            manifest_target_size,
            manifest_merge_min_count,
            row_tracking_enabled,
            data_evolution_enabled,
            partition_default_name,
        }
    }

    /// Commit new files in APPEND mode.
    pub async fn commit(&self, commit_messages: Vec<CommitMessage>) -> Result<()> {
        self.commit_with_identifier(commit_messages, BATCH_COMMIT_IDENTIFIER)
            .await
    }

    /// Commit new files with a caller-provided commit identifier.
    ///
    /// The identifier participates in retry idempotency, matching Python
    /// `FileStoreCommit.commit(commit_messages, commit_identifier)`.
    pub async fn commit_with_identifier(
        &self,
        commit_messages: Vec<CommitMessage>,
        commit_identifier: i64,
    ) -> Result<()> {
        if commit_messages.is_empty() {
            return Ok(());
        }

        let entries = self.messages_to_entries(&commit_messages);
        let changelog_entries = self.messages_to_changelog_entries(&commit_messages);
        let new_index_entries = self.messages_to_index_entries(&commit_messages);
        let check_from_snapshot = Self::min_check_from_snapshot(&commit_messages);
        self.try_commit(
            CommitEntriesPlan::Direct {
                entries,
                changelog_entries,
                new_index_entries,
                check_from_snapshot,
            },
            None,
            commit_identifier,
        )
        .await
    }

    pub(crate) async fn commit_if_latest_snapshot(
        &self,
        commit_messages: Vec<CommitMessage>,
        expected_snapshot_id: i64,
    ) -> Result<()> {
        self.commit_if_latest_snapshot_with_identifier(
            commit_messages,
            expected_snapshot_id,
            BATCH_COMMIT_IDENTIFIER,
        )
        .await
    }

    pub(crate) async fn commit_if_latest_snapshot_with_identifier(
        &self,
        commit_messages: Vec<CommitMessage>,
        expected_snapshot_id: i64,
        commit_identifier: i64,
    ) -> Result<()> {
        if commit_messages.is_empty() {
            return Ok(());
        }

        let entries = self.messages_to_entries(&commit_messages);
        let changelog_entries = self.messages_to_changelog_entries(&commit_messages);
        let new_index_entries = self.messages_to_index_entries(&commit_messages);
        let check_from_snapshot = Self::min_check_from_snapshot(&commit_messages);
        self.try_commit(
            CommitEntriesPlan::Direct {
                entries,
                changelog_entries,
                new_index_entries,
                check_from_snapshot,
            },
            Some(expected_snapshot_id),
            commit_identifier,
        )
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
        self.overwrite_with_identifier(commit_messages, static_partitions, BATCH_COMMIT_IDENTIFIER)
            .await
    }

    /// Overwrite partitions with a caller-provided commit identifier.
    pub async fn overwrite_with_identifier(
        &self,
        commit_messages: Vec<CommitMessage>,
        static_partitions: Option<HashMap<String, Option<Datum>>>,
        commit_identifier: i64,
    ) -> Result<()> {
        if commit_messages.is_empty() && static_partitions.is_none() {
            return Ok(());
        }

        let new_entries = self.messages_to_entries(&commit_messages);
        let new_index_entries = self.messages_to_index_entries(&commit_messages);
        let has_new_data_entries = new_entries
            .iter()
            .any(|entry| *entry.kind() == FileKind::Add);
        let has_static_partitions = static_partitions.is_some();

        let partition_filter = if let Some(sp) = static_partitions {
            self.validate_partition_spec_keys(&sp)?;
            let partition_fields = self.table.schema().partition_fields();
            Some(self.build_static_partition_predicate(&sp, &partition_fields)?)
        } else if !self.table.schema().partition_fields().is_empty() && !has_new_data_entries {
            return Ok(());
        } else {
            self.build_dynamic_partition_filter(&new_entries)?
        };

        if has_static_partitions {
            if let Some(filter) = partition_filter.as_ref() {
                self.validate_static_overwrite_entries(filter, &new_entries)?;
            }
        }

        self.try_commit(
            CommitEntriesPlan::Overwrite {
                partition_filter,
                new_entries,
                new_index_entries,
                cached_snapshot: None,
                cached_entries: Vec::new(),
                full_scan_count: 0,
                delta_probe_count: 0,
            },
            None,
            commit_identifier,
        )
        .await
    }

    /// Build a predicate-based partition filter from a partial static partition spec.
    fn build_static_partition_predicate(
        &self,
        static_partitions: &HashMap<String, Option<Datum>>,
        partition_fields: &[crate::spec::DataField],
    ) -> Result<PartitionFilter> {
        use crate::spec::PredicateBuilder;
        if static_partitions.is_empty() {
            return Ok(PartitionFilter::from_predicate(
                Predicate::AlwaysTrue,
                partition_fields,
            ));
        }
        let pb = PredicateBuilder::new(partition_fields);
        let combined = self.partition_spec_predicate(&pb, static_partitions)?;
        Ok(PartitionFilter::from_predicate(combined, partition_fields))
    }

    /// Build a dynamic partition filter from the partitions present in new data entries.
    ///
    /// Returns `None` for unpartitioned tables (full table overwrite).
    /// Uses `PartitionSet` for O(1) byte-level matching.
    fn build_dynamic_partition_filter(
        &self,
        entries: &[ManifestEntry],
    ) -> Result<Option<PartitionFilter>> {
        let partition_fields = self.table.schema().partition_fields();
        if partition_fields.is_empty() {
            return Ok(None);
        }

        let mut partition_bytes_set: HashSet<Vec<u8>> = HashSet::new();
        for entry in entries {
            if *entry.kind() == FileKind::Add {
                partition_bytes_set.insert(entry.partition().to_vec());
            }
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

    fn build_partition_filter_from_specs(
        &self,
        partitions: &[HashMap<String, Option<Datum>>],
    ) -> Result<PartitionFilter> {
        let partition_fields = self.table.schema().partition_fields();
        if partition_fields.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Cannot drop partitions from an unpartitioned table.".to_string(),
                source: None,
            });
        }

        use crate::spec::PredicateBuilder;
        let pb = PredicateBuilder::new(&partition_fields);
        let mut predicates = Vec::new();
        for partition in partitions {
            self.validate_partition_spec_keys(partition)?;
            if !partition.is_empty() {
                predicates.push(self.partition_spec_predicate(&pb, partition)?);
            }
        }

        if predicates.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Failed to build partition filter for drop_partitions.".to_string(),
                source: None,
            });
        }

        Ok(PartitionFilter::from_predicate(
            Predicate::or(predicates),
            &partition_fields,
        ))
    }

    fn partition_spec_predicate(
        &self,
        pb: &crate::spec::PredicateBuilder,
        partition: &HashMap<String, Option<Datum>>,
    ) -> Result<Predicate> {
        let predicates = partition
            .iter()
            .map(|(key, value)| self.partition_value_predicate(pb, key, value))
            .collect::<Result<Vec<_>>>()?;
        Ok(Predicate::and(predicates))
    }

    fn partition_value_predicate(
        &self,
        pb: &crate::spec::PredicateBuilder,
        key: &str,
        value: &Option<Datum>,
    ) -> Result<Predicate> {
        match value {
            None => pb.is_null(key),
            Some(Datum::String(value)) if value == &self.partition_default_name => pb.is_null(key),
            Some(datum) => pb.equal(key, datum.clone()),
        }
    }

    fn validate_partition_spec_keys(
        &self,
        partition: &HashMap<String, Option<Datum>>,
    ) -> Result<()> {
        let partition_keys: HashSet<&str> = self
            .table
            .schema()
            .partition_keys()
            .iter()
            .map(String::as_str)
            .collect();
        for key in partition.keys() {
            if !partition_keys.contains(key.as_str()) {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Partition spec key '{key}' is not a partition column. Partition keys are: {:?}.",
                        self.table.schema().partition_keys()
                    ),
                    source: None,
                });
            }
        }
        Ok(())
    }

    fn validate_static_overwrite_entries(
        &self,
        partition_filter: &PartitionFilter,
        new_entries: &[ManifestEntry],
    ) -> Result<()> {
        for entry in new_entries {
            if *entry.kind() == FileKind::Add
                && !partition_filter.matches_entry(entry.partition())?
            {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Trying to overwrite static partition, but file '{}' in bucket {} does not belong to this partition.",
                        entry.file().file_name,
                        entry.bucket(),
                    ),
                    source: None,
                });
            }
        }
        Ok(())
    }

    /// Drop specific partitions (OVERWRITE with only deletes).
    pub async fn truncate_partitions(
        &self,
        partitions: Vec<HashMap<String, Option<Datum>>>,
    ) -> Result<()> {
        self.truncate_partitions_with_identifier(partitions, BATCH_COMMIT_IDENTIFIER)
            .await
    }

    /// Drop specific partitions with a caller-provided commit identifier.
    pub async fn truncate_partitions_with_identifier(
        &self,
        partitions: Vec<HashMap<String, Option<Datum>>>,
        commit_identifier: i64,
    ) -> Result<()> {
        if partitions.is_empty() {
            return Ok(());
        }

        let partition_filter = self.build_partition_filter_from_specs(&partitions)?;

        self.try_commit(
            CommitEntriesPlan::Overwrite {
                partition_filter: Some(partition_filter),
                new_entries: vec![],
                new_index_entries: vec![],
                cached_snapshot: None,
                cached_entries: Vec::new(),
                full_scan_count: 0,
                delta_probe_count: 0,
            },
            None,
            commit_identifier,
        )
        .await
    }

    /// Python-compatible alias for dropping partitions.
    pub async fn drop_partitions(
        &self,
        partitions: Vec<HashMap<String, Option<Datum>>>,
    ) -> Result<()> {
        self.drop_partitions_with_identifier(partitions, BATCH_COMMIT_IDENTIFIER)
            .await
    }

    /// Python-compatible alias for dropping partitions with a caller-provided
    /// commit identifier. Unlike `truncate_partitions`, an empty partition list
    /// is rejected just like `FileStoreCommit.drop_partitions`.
    pub async fn drop_partitions_with_identifier(
        &self,
        partitions: Vec<HashMap<String, Option<Datum>>>,
        commit_identifier: i64,
    ) -> Result<()> {
        if partitions.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Partitions list cannot be empty.".to_string(),
                source: None,
            });
        }
        self.truncate_partitions_with_identifier(partitions, commit_identifier)
            .await
    }

    /// Truncate the entire table (OVERWRITE with no filter, only deletes).
    pub async fn truncate_table(&self) -> Result<()> {
        self.truncate_table_with_identifier(BATCH_COMMIT_IDENTIFIER)
            .await
    }

    /// Truncate the entire table with a caller-provided commit identifier.
    pub async fn truncate_table_with_identifier(&self, commit_identifier: i64) -> Result<()> {
        self.try_commit(
            CommitEntriesPlan::Overwrite {
                partition_filter: None,
                new_entries: vec![],
                new_index_entries: vec![],
                cached_snapshot: None,
                cached_entries: Vec::new(),
                full_scan_count: 0,
                delta_probe_count: 0,
            },
            None,
            commit_identifier,
        )
        .await
    }

    /// Abort a prepared commit by deleting newly written data and changelog files.
    ///
    /// Deletion is best-effort and mirrors Python `FileStoreCommit.abort`: missing
    /// files or storage errors are ignored so abort cleanup never masks the
    /// original write failure.
    pub async fn abort(&self, commit_messages: &[CommitMessage]) -> Result<()> {
        for message in commit_messages {
            let bucket_path = self.bucket_path(&message.partition, message.bucket)?;
            for file in message
                .new_files
                .iter()
                .chain(message.new_changelog_files.iter())
            {
                for path in file.collect_files(&bucket_path) {
                    let _ = self.table.file_io().delete_file(&path).await;
                }
            }
        }
        Ok(())
    }

    fn bucket_path(&self, partition: &[u8], bucket: i32) -> Result<String> {
        let base = self.table.location().trim_end_matches('/');
        let partition_keys = self.table.schema().partition_keys();
        if partition_keys.is_empty() {
            return Ok(format!("{base}/{}", bucket_dir_name(bucket)));
        }

        let partition_row = BinaryRow::from_serialized_bytes(partition)?;
        let core_options = CoreOptions::new(self.table.schema().options());
        let computer = PartitionComputer::new(
            partition_keys,
            self.table.schema().fields(),
            core_options.partition_default_name(),
            core_options.legacy_partition_name(),
        )?;
        Ok(format!(
            "{base}/{}{}",
            computer.generate_partition_path(&partition_row)?,
            bucket_dir_name(bucket)
        ))
    }

    /// Try to commit with retries.
    async fn try_commit(
        &self,
        mut plan: CommitEntriesPlan,
        expected_snapshot_id: Option<i64>,
        commit_identifier: i64,
    ) -> Result<()> {
        let mut retry_count = 0u32;
        let mut duplicate_check_start_snapshot_id: Option<i64> = None;
        let mut retry_state: Option<Box<RetryState>> = None;
        let start_time_ms = current_time_millis();

        loop {
            let latest_snapshot = self.snapshot_manager.get_latest_snapshot().await?;
            if let Some(start_snapshot_id) = duplicate_check_start_snapshot_id {
                if self
                    .is_duplicate_commit(
                        start_snapshot_id,
                        &latest_snapshot,
                        commit_identifier,
                        &plan.commit_kind_hint(),
                    )
                    .await
                {
                    break;
                }
            }
            validate_expected_latest_snapshot(expected_snapshot_id, &latest_snapshot)?;
            let resolved = self
                .resolve_commit(&mut plan, &latest_snapshot, retry_state.as_deref())
                .await?;

            if resolved.entries.is_empty()
                && resolved.changelog_entries.is_empty()
                && !resolved.index_manifest_changed
            {
                break;
            }

            let result = self
                .try_commit_once(resolved, &latest_snapshot, commit_identifier)
                .await?;

            match result {
                CommitAttemptResult::Success => break,
                CommitAttemptResult::Retry(state) => {
                    duplicate_check_start_snapshot_id.get_or_insert_with(|| {
                        latest_snapshot.as_ref().map(|s| s.id() + 1).unwrap_or(1)
                    });
                    retry_state = Some(state);
                }
            }

            let elapsed_ms = current_time_millis() - start_time_ms;
            if elapsed_ms > self.commit_timeout_ms || retry_count >= self.commit_max_retries {
                let snap_id = duplicate_check_start_snapshot_id.unwrap_or(1);
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
        commit_identifier: i64,
    ) -> Result<CommitAttemptResult> {
        let new_snapshot_id = latest_snapshot.as_ref().map(|s| s.id() + 1).unwrap_or(1);

        // Row tracking
        let mut next_row_id: Option<i64> = None;
        if self.row_tracking_enabled {
            let first_row_id_start = latest_snapshot
                .as_ref()
                .and_then(|s| s.next_row_id())
                .unwrap_or(0);
            if resolved.entries.is_empty() {
                next_row_id = latest_snapshot
                    .as_ref()
                    .and_then(|s| s.next_row_id())
                    .or(Some(first_row_id_start));
            } else {
                let (assigned, nrid) = self.assign_row_tracking_meta(
                    new_snapshot_id,
                    first_row_id_start,
                    resolved.entries,
                )?;
                resolved.entries = assigned;
                next_row_id = Some(nrid);
            }
        }

        let file_io = self.snapshot_manager.file_io();
        let manifest_dir = self.snapshot_manager.manifest_dir();

        let unique_id = uuid::Uuid::new_v4();
        let base_manifest_list_name = format!("manifest-list-{unique_id}-0");
        let delta_manifest_list_name = format!("manifest-list-{unique_id}-1");
        let changelog_manifest_list_name = format!("manifest-list-{unique_id}-2");
        let new_manifest_prefix = format!("manifest-{}", uuid::Uuid::new_v4());
        let changelog_manifest_prefix = format!("manifest-{}-changelog", uuid::Uuid::new_v4());

        let base_manifest_list_path = format!("{manifest_dir}/{base_manifest_list_name}");
        let delta_manifest_list_path = format!("{manifest_dir}/{delta_manifest_list_name}");
        let changelog_manifest_list_path = format!("{manifest_dir}/{changelog_manifest_list_name}");

        // Write delta manifest files, rolling by target size.
        let new_manifest_file_metas = self
            .write_manifest_files(
                file_io,
                &manifest_dir,
                &new_manifest_prefix,
                &resolved.entries,
            )
            .await?;

        // Write delta manifest list
        ManifestList::write_with_compression(
            file_io,
            &delta_manifest_list_path,
            &new_manifest_file_metas,
            &self.manifest_compression,
        )
        .await?;

        let (changelog_record_count, changelog_manifest_list_size) =
            if resolved.changelog_entries.is_empty() {
                (None, None)
            } else {
                let changelog_manifest_file_metas = self
                    .write_manifest_files(
                        file_io,
                        &manifest_dir,
                        &changelog_manifest_prefix,
                        &resolved.changelog_entries,
                    )
                    .await?;
                ManifestList::write_with_compression(
                    file_io,
                    &changelog_manifest_list_path,
                    &changelog_manifest_file_metas,
                    &self.manifest_compression,
                )
                .await?;
                let status = file_io.get_status(&changelog_manifest_list_path).await?;
                (
                    Some(
                        resolved
                            .changelog_entries
                            .iter()
                            .map(|entry| entry.file().row_count)
                            .sum(),
                    ),
                    Some(status.size as i64),
                )
            };

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

        let (base_manifest_files, _merge_new_files) = self
            .merge_manifest_files(file_io, &manifest_dir, existing_manifest_files)
            .await?;

        ManifestList::write_with_compression(
            file_io,
            &base_manifest_list_path,
            &base_manifest_files,
            &self.manifest_compression,
        )
        .await?;

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
            .commit_identifier(commit_identifier)
            .commit_kind(resolved.kind)
            .time_millis(current_time_millis())
            .total_record_count(Some(total_record_count))
            .delta_record_count(Some(delta_record_count))
            .changelog_manifest_list(changelog_record_count.map(|_| changelog_manifest_list_name))
            .changelog_manifest_list_size(changelog_manifest_list_size)
            .changelog_record_count(changelog_record_count)
            .next_row_id(next_row_id)
            .index_manifest(resolved.index_manifest_name)
            .build();

        let statistics = self.generate_partition_statistics(&resolved.entries)?;

        if self.snapshot_commit.commit(&snapshot, &statistics).await? {
            Ok(CommitAttemptResult::Success)
        } else {
            Ok(CommitAttemptResult::Retry(Box::new(RetryState {
                latest_snapshot: latest_snapshot.clone(),
                base_data_files: resolved.base_data_files.take(),
            })))
        }
    }

    /// Write an index manifest file from already-merged entries.
    ///
    /// Returns `None` if `merged_index_entries` is empty.
    async fn write_index_manifest(
        &self,
        file_io: &FileIO,
        manifest_dir: &str,
        merged_index_entries: &[IndexManifestEntry],
    ) -> Result<Option<String>> {
        if merged_index_entries.is_empty() {
            return Ok(None);
        }
        let name = format!("index-manifest-{}-0", uuid::Uuid::new_v4());
        let path = format!("{manifest_dir}/{name}");
        IndexManifest::write_with_compression(
            file_io,
            &path,
            merged_index_entries,
            &self.manifest_compression,
        )
        .await?;
        Ok(Some(name))
    }

    /// Write manifest files, rolling by configured target size, and return their metadata.
    async fn write_manifest_files(
        &self,
        file_io: &FileIO,
        manifest_dir: &str,
        name_prefix: &str,
        entries: &[ManifestEntry],
    ) -> Result<Vec<ManifestFileMeta>> {
        if entries.is_empty() {
            return Ok(vec![]);
        }

        let target_size = self.manifest_target_size.max(1) as usize;
        let mut result = Vec::new();
        let mut chunk_start = 0usize;
        let schema = Schema::parse_str(MANIFEST_ENTRY_SCHEMA)?;
        let mut writer = crate::spec::new_avro_writer(
            &schema,
            &self.manifest_compression,
            crate::spec::DEFAULT_AVRO_BLOCK_SIZE,
        )?;

        for (idx, entry) in entries.iter().enumerate() {
            let value = to_value(entry).and_then(|value| value.resolve(&schema))?;
            writer.append(value)?;
            let record_count = idx + 1;
            if record_count % CHECK_ROLLING_RECORD_COUNT == 0
                && writer.get_ref().len() >= target_size
            {
                let chunk_end = idx + 1;
                let file_name = format!("{name_prefix}-{}", result.len());
                let path = format!("{manifest_dir}/{file_name}");
                let bytes = writer.into_inner()?;
                let meta = self
                    .write_manifest_file_bytes(
                        file_io,
                        &path,
                        &file_name,
                        &entries[chunk_start..chunk_end],
                        bytes,
                    )
                    .await?;
                result.push(meta);
                chunk_start = chunk_end;
                writer = crate::spec::new_avro_writer(
                    &schema,
                    &self.manifest_compression,
                    crate::spec::DEFAULT_AVRO_BLOCK_SIZE,
                )?;
            }
        }

        if chunk_start < entries.len() {
            let file_name = format!("{name_prefix}-{}", result.len());
            let path = format!("{manifest_dir}/{file_name}");
            let bytes = writer.into_inner()?;
            let meta = self
                .write_manifest_file_bytes(
                    file_io,
                    &path,
                    &file_name,
                    &entries[chunk_start..],
                    bytes,
                )
                .await?;
            result.push(meta);
        }

        Ok(result)
    }

    /// Minor-compact existing manifest files before writing the base manifest list.
    async fn merge_manifest_files(
        &self,
        file_io: &FileIO,
        manifest_dir: &str,
        manifest_files: Vec<ManifestFileMeta>,
    ) -> Result<(Vec<ManifestFileMeta>, Vec<ManifestFileMeta>)> {
        if manifest_files.is_empty() {
            return Ok((vec![], vec![]));
        }

        let target_size = self.manifest_target_size.max(1);
        let mut result = Vec::new();
        let mut new_files = Vec::new();
        let mut candidates = Vec::new();
        let mut total_size = 0i64;

        for manifest in manifest_files {
            total_size += manifest.file_size();
            candidates.push(manifest);
            if total_size >= target_size {
                self.merge_manifest_candidates(
                    file_io,
                    manifest_dir,
                    &mut candidates,
                    &mut result,
                    &mut new_files,
                )
                .await?;
                total_size = 0;
            }
        }

        if candidates.len() >= self.manifest_merge_min_count {
            self.merge_manifest_candidates(
                file_io,
                manifest_dir,
                &mut candidates,
                &mut result,
                &mut new_files,
            )
            .await?;
        } else {
            result.append(&mut candidates);
        }

        Ok((result, new_files))
    }

    async fn merge_manifest_candidates(
        &self,
        file_io: &FileIO,
        manifest_dir: &str,
        candidates: &mut Vec<ManifestFileMeta>,
        result: &mut Vec<ManifestFileMeta>,
        new_files: &mut Vec<ManifestFileMeta>,
    ) -> Result<()> {
        if candidates.is_empty() {
            return Ok(());
        }
        if candidates.len() == 1 {
            result.append(candidates);
            return Ok(());
        }

        let mut entries = Vec::new();
        for manifest in candidates.drain(..) {
            let path = format!("{manifest_dir}/{}", manifest.file_name());
            entries.extend(Manifest::read(file_io, &path).await?);
        }

        let merged_entries = merge_active_entries(entries);
        if merged_entries.is_empty() {
            return Ok(());
        }

        let manifest_prefix = format!("manifest-{}", uuid::Uuid::new_v4());
        let merged_metas = self
            .write_manifest_files(file_io, manifest_dir, &manifest_prefix, &merged_entries)
            .await?;
        result.extend(merged_metas.clone());
        new_files.extend(merged_metas);
        Ok(())
    }

    /// Write already-encoded manifest bytes and return metadata for the corresponding entries.
    async fn write_manifest_file_bytes(
        &self,
        file_io: &FileIO,
        path: &str,
        file_name: &str,
        entries: &[ManifestEntry],
        bytes: Vec<u8>,
    ) -> Result<ManifestFileMeta> {
        let file_size = bytes.len() as i64;
        let output = file_io.new_output(path)?;
        output.write(bytes::Bytes::from(bytes)).await?;

        let mut added_file_count: i64 = 0;
        let mut deleted_file_count: i64 = 0;
        // Bucket / level pruning stats; left as None when entries is empty so back-compat
        // readers (Java < apache/paimon#5345 or older Rust writers) see the same shape
        // they would for a pre-feature manifest.
        let mut min_bucket: Option<i32> = None;
        let mut max_bucket: Option<i32> = None;
        let mut min_level: Option<i32> = None;
        let mut max_level: Option<i32> = None;
        let mut min_row_id: Option<i64> = None;
        let mut max_row_id: Option<i64> = None;
        let mut all_entries_have_row_id = !entries.is_empty();
        let mut schema_id = self.table.schema().id();
        for entry in entries {
            match entry.kind() {
                FileKind::Add => added_file_count += 1,
                FileKind::Delete => deleted_file_count += 1,
            }
            schema_id = schema_id.max(entry.file().schema_id);
            let b = entry.bucket();
            min_bucket = Some(min_bucket.map_or(b, |cur| cur.min(b)));
            max_bucket = Some(max_bucket.map_or(b, |cur| cur.max(b)));
            let l = entry.file().level;
            min_level = Some(min_level.map_or(l, |cur| cur.min(l)));
            max_level = Some(max_level.map_or(l, |cur| cur.max(l)));
            if let Some((start, end)) = entry.file().row_id_range() {
                min_row_id = Some(min_row_id.map_or(start, |cur| cur.min(start)));
                max_row_id = Some(max_row_id.map_or(end, |cur| cur.max(end)));
            } else {
                all_entries_have_row_id = false;
            }
        }
        if !all_entries_have_row_id {
            min_row_id = None;
            max_row_id = None;
        }

        let partition_stats = self.compute_partition_stats(entries)?;

        Ok(ManifestFileMeta::new(
            file_name.to_string(),
            file_size,
            added_file_count,
            deleted_file_count,
            partition_stats,
            schema_id,
        )
        .with_bucket_level_stats(min_bucket, max_bucket, min_level, max_level)
        .with_row_id_stats(min_row_id, max_row_id))
    }

    /// Check if this commit was already completed (idempotency).
    async fn is_duplicate_commit(
        &self,
        start_snapshot_id: i64,
        latest_snapshot: &Option<Snapshot>,
        commit_identifier: i64,
        commit_kind: &CommitKind,
    ) -> bool {
        if let Some(latest) = latest_snapshot {
            for snapshot_id in start_snapshot_id..=latest.id() {
                if let Ok(snap) = self.snapshot_manager.get_snapshot(snapshot_id).await {
                    if snap.commit_user() == self.commit_user
                        && snap.commit_identifier() == commit_identifier
                        && snap.commit_kind() == commit_kind
                    {
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
        plan: &mut CommitEntriesPlan,
        latest_snapshot: &Option<Snapshot>,
        retry_state: Option<&RetryState>,
    ) -> Result<ResolvedCommit> {
        let file_io = self.snapshot_manager.file_io();
        let manifest_dir = self.snapshot_manager.manifest_dir();

        match plan {
            CommitEntriesPlan::Direct {
                entries,
                changelog_entries,
                new_index_entries,
                check_from_snapshot,
            } => {
                // Auto-promote to OVERWRITE when CoW rewrites produce Delete entries.
                // This ensures the snapshot correctly reflects file replacements.
                let has_delete = entries.iter().any(|e| *e.kind() == FileKind::Delete);
                let kind = if has_delete {
                    CommitKind::OVERWRITE
                } else {
                    CommitKind::APPEND
                };
                let detect_conflicts = has_delete || check_from_snapshot.is_some();
                let base_data_files = if detect_conflicts {
                    self.check_deletion_vector_index_only_conflict(
                        latest_snapshot.as_ref(),
                        entries,
                        new_index_entries,
                        *check_from_snapshot,
                    )?;
                    self.detect_commit_conflicts(
                        latest_snapshot,
                        retry_state,
                        entries,
                        &kind,
                        *check_from_snapshot,
                    )
                    .await?
                } else {
                    if self.row_tracking_enabled {
                        self.validate_row_id_alignment(entries, latest_snapshot)
                            .await?;
                    }
                    self.validate_deleted_files(entries, latest_snapshot)
                        .await?;
                    None
                };

                let previous =
                    Self::read_prev_index_entries(file_io, &manifest_dir, latest_snapshot).await?;
                let mut index_entries = new_index_entries.clone();
                index_entries.extend(self.global_index_update_entries(
                    &previous,
                    entries,
                    new_index_entries,
                )?);
                let all = Self::merge_index_entries(&previous, &index_entries, false)?;
                let index_manifest_changed = all != previous;
                let index_manifest_name = if index_manifest_changed {
                    self.write_index_manifest(file_io, &manifest_dir, &all)
                        .await?
                } else {
                    latest_snapshot
                        .as_ref()
                        .and_then(|s| s.index_manifest().map(|s| s.to_string()))
                };

                Ok(ResolvedCommit {
                    entries: entries.clone(),
                    changelog_entries: changelog_entries.clone(),
                    kind,
                    index_manifest_name,
                    index_manifest_changed,
                    base_data_files,
                })
            }
            CommitEntriesPlan::Overwrite { .. } => {
                let entries = self
                    .provide_overwrite_entries(plan, latest_snapshot)
                    .await?;
                let (partition_filter, new_index_entries) = match plan {
                    CommitEntriesPlan::Overwrite {
                        partition_filter,
                        new_index_entries,
                        ..
                    } => (partition_filter.clone(), new_index_entries.clone()),
                    CommitEntriesPlan::Direct { .. } => unreachable!(),
                };
                let base_data_files = self
                    .detect_commit_conflicts(
                        latest_snapshot,
                        retry_state,
                        &entries,
                        &CommitKind::OVERWRITE,
                        None,
                    )
                    .await?;

                let previous =
                    Self::read_prev_index_entries(file_io, &manifest_dir, latest_snapshot).await?;
                let mut all = previous.clone();
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
                let all = Self::merge_index_entries(&all, &new_index_entries, false)?;
                let index_manifest_changed = all != previous;
                let index_manifest_name = if index_manifest_changed {
                    self.write_index_manifest(file_io, &manifest_dir, &all)
                        .await?
                } else {
                    latest_snapshot
                        .as_ref()
                        .and_then(|s| s.index_manifest().map(|s| s.to_string()))
                };

                Ok(ResolvedCommit {
                    entries,
                    changelog_entries: vec![],
                    kind: CommitKind::OVERWRITE,
                    index_manifest_name,
                    index_manifest_changed,
                    base_data_files,
                })
            }
        }
    }

    fn merge_index_entries(
        previous_entries: &[IndexManifestEntry],
        new_index_entries: &[IndexManifestEntry],
        drop_previous_global_indexes: bool,
    ) -> Result<Vec<IndexManifestEntry>> {
        let mut all = if drop_previous_global_indexes {
            previous_entries
                .iter()
                .filter(|entry| entry.index_file.global_index_meta.is_none())
                .cloned()
                .collect::<Vec<_>>()
        } else {
            previous_entries.to_vec()
        };
        let deletions = new_index_entries
            .iter()
            .filter(|entry| entry.kind == FileKind::Delete)
            .collect::<Vec<_>>();
        if !deletions.is_empty() {
            all.retain(|entry| {
                !deletions
                    .iter()
                    .any(|delete| same_index_file_entry(entry, delete))
            });
        }

        let additions = new_index_entries
            .iter()
            .filter(|entry| entry.kind == FileKind::Add)
            .cloned()
            .collect::<Vec<_>>();
        let new_hash_keys: HashSet<(Vec<u8>, i32)> = additions
            .iter()
            .filter(|e| e.index_file.index_type == "HASH")
            .map(|e| (e.partition.clone(), e.bucket))
            .collect();
        all.retain(|e| {
            if e.index_file.index_type == "HASH" {
                !new_hash_keys.contains(&(e.partition.clone(), e.bucket))
            } else {
                true
            }
        });
        Self::validate_global_index_overlap(&all, &additions)?;
        Self::validate_added_global_index_overlap(&additions)?;
        all.extend(additions);
        Ok(all)
    }

    fn global_index_update_entries(
        &self,
        previous_entries: &[IndexManifestEntry],
        commit_entries: &[ManifestEntry],
        new_index_entries: &[IndexManifestEntry],
    ) -> Result<Vec<IndexManifestEntry>> {
        if new_index_entries
            .iter()
            .any(|entry| entry.kind == FileKind::Delete)
        {
            return Ok(vec![]);
        }

        let mut updated_cols = HashSet::new();
        let mut written_partitions: Vec<Vec<u8>> = Vec::new();
        for entry in commit_entries
            .iter()
            .filter(|entry| *entry.kind() == FileKind::Add)
        {
            let Some(write_cols) = entry.file().write_cols.as_ref() else {
                continue;
            };
            for col in write_cols {
                if !is_system_field(col) {
                    updated_cols.insert(col.clone());
                }
            }
            if !written_partitions
                .iter()
                .any(|partition| same_index_partition(partition, entry.partition()))
            {
                written_partitions.push(entry.partition().to_vec());
            }
        }
        if updated_cols.is_empty() || written_partitions.is_empty() {
            return Ok(vec![]);
        }

        let field_by_id = self
            .table
            .schema()
            .fields()
            .iter()
            .map(|field| (field.id(), field.name().to_string()))
            .collect::<HashMap<_, _>>();

        let mut affected = Vec::new();
        let mut conflicted_cols = HashSet::new();
        for entry in previous_entries {
            if entry.kind != FileKind::Add
                || !written_partitions
                    .iter()
                    .any(|partition| same_index_partition(partition, &entry.partition))
            {
                continue;
            }
            let Some(global_meta) = entry.index_file.global_index_meta.as_ref() else {
                continue;
            };
            let mut indexed_field_ids = vec![global_meta.index_field_id];
            if let Some(extra_field_ids) = global_meta.extra_field_ids.as_ref() {
                indexed_field_ids.extend(extra_field_ids.iter().copied());
            }
            let matched = indexed_field_ids
                .iter()
                .filter_map(|field_id| field_by_id.get(field_id))
                .filter(|field_name| updated_cols.contains(*field_name))
                .cloned()
                .collect::<Vec<_>>();
            if !matched.is_empty() {
                conflicted_cols.extend(matched);
                affected.push(entry.clone());
            }
        }
        if affected.is_empty() {
            return Ok(vec![]);
        }

        match CoreOptions::new(self.table.schema().options()).global_index_column_update_action()? {
            GlobalIndexColumnUpdateAction::DropPartitionIndex => Ok(affected
                .into_iter()
                .map(|entry| IndexManifestEntry {
                    kind: FileKind::Delete,
                    partition: entry.partition,
                    bucket: entry.bucket,
                    index_file: entry.index_file,
                    version: entry.version,
                })
                .collect()),
            GlobalIndexColumnUpdateAction::ThrowError => {
                let mut updated = updated_cols.into_iter().collect::<Vec<_>>();
                updated.sort();
                let mut conflicted = conflicted_cols.into_iter().collect::<Vec<_>>();
                conflicted.sort();
                Err(crate::Error::DataInvalid {
                    message: format!(
                        "Update columns contain globally indexed columns, not supported now. Updated columns: {:?}. Conflicted columns: {:?}.",
                        updated, conflicted
                    ),
                    source: None,
                })
            }
        }
    }

    fn validate_global_index_overlap(
        retained_entries: &[IndexManifestEntry],
        added_entries: &[IndexManifestEntry],
    ) -> Result<()> {
        for retained in retained_entries {
            if retained.kind == FileKind::Delete {
                continue;
            }
            let Some(retained_meta) = retained.index_file.global_index_meta.as_ref() else {
                continue;
            };
            for added in added_entries {
                if added.kind == FileKind::Delete {
                    continue;
                }
                let Some(added_meta) = added.index_file.global_index_meta.as_ref() else {
                    continue;
                };
                if retained_meta.index_field_id == added_meta.index_field_id
                    && ranges_overlap(
                        retained_meta.row_range_start,
                        retained_meta.row_range_end,
                        added_meta.row_range_start,
                        added_meta.row_range_end,
                    )
                {
                    return Err(global_index_overlap_error(
                        retained,
                        retained_meta,
                        added,
                        added_meta,
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_added_global_index_overlap(added_entries: &[IndexManifestEntry]) -> Result<()> {
        for (left_index, left) in added_entries.iter().enumerate() {
            if left.kind == FileKind::Delete {
                continue;
            }
            let Some(left_meta) = left.index_file.global_index_meta.as_ref() else {
                continue;
            };
            for right in added_entries.iter().skip(left_index + 1) {
                if right.kind == FileKind::Delete {
                    continue;
                }
                let Some(right_meta) = right.index_file.global_index_meta.as_ref() else {
                    continue;
                };
                if left_meta.index_field_id == right_meta.index_field_id
                    && ranges_overlap(
                        left_meta.row_range_start,
                        left_meta.row_range_end,
                        right_meta.row_range_start,
                        right_meta.row_range_end,
                    )
                {
                    return Err(global_index_overlap_error(
                        left, left_meta, right, right_meta,
                    ));
                }
            }
        }
        Ok(())
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
                return Ok(normalize_index_entries(
                    IndexManifest::read(file_io, &prev_path).await?,
                ));
            }
        }
        Ok(vec![])
    }

    /// Stateful overwrite provider mirroring Python `OverwriteChangesProvider`.
    async fn provide_overwrite_entries(
        &self,
        plan: &mut CommitEntriesPlan,
        latest_snapshot: &Option<Snapshot>,
    ) -> Result<Vec<ManifestEntry>> {
        let CommitEntriesPlan::Overwrite {
            partition_filter,
            new_entries,
            cached_snapshot,
            cached_entries,
            full_scan_count,
            delta_probe_count,
            ..
        } = plan
        else {
            unreachable!("provide_overwrite_entries only accepts overwrite plans");
        };

        let Some(latest) = latest_snapshot else {
            return Ok(Self::build_overwrite_result(&[], new_entries));
        };

        let rebuild_cache = match cached_snapshot.as_ref() {
            None => true,
            Some(cached) if cached.id() > latest.id() => {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Cached snapshot id {} is greater than latest snapshot id {}.",
                        cached.id(),
                        latest.id()
                    ),
                    source: None,
                });
            }
            Some(cached) if cached.id() < latest.id() => {
                !self
                    .can_use_overwrite_cache(
                        cached,
                        latest,
                        partition_filter.as_ref(),
                        delta_probe_count,
                    )
                    .await?
            }
            Some(_) => false,
        };

        if rebuild_cache {
            *cached_entries = self
                .scan_snapshot_entries(latest_snapshot, partition_filter.as_ref())
                .await?;
            *full_scan_count += 1;
        }
        *cached_snapshot = Some(Box::new(latest.clone()));

        Ok(Self::build_overwrite_result(cached_entries, new_entries))
    }

    async fn can_use_overwrite_cache(
        &self,
        cached_snapshot: &Snapshot,
        latest_snapshot: &Snapshot,
        partition_filter: Option<&PartitionFilter>,
        delta_probe_count: &mut usize,
    ) -> Result<bool> {
        let Some(partition_filter) = partition_filter else {
            return Ok(false);
        };

        for snapshot_id in cached_snapshot.id() + 1..=latest_snapshot.id() {
            *delta_probe_count += 1;
            let snapshot = match self.snapshot_manager.get_snapshot(snapshot_id).await {
                Ok(snapshot) => snapshot,
                Err(_) => return Ok(false),
            };
            if snapshot.commit_kind() != &CommitKind::APPEND {
                return Ok(false);
            }
            let delta_entries = self
                .read_delta_entries(Some(partition_filter), &snapshot)
                .await?;
            if !delta_entries.is_empty() {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn build_overwrite_result(
        existing_entries: &[ManifestEntry],
        new_entries: &[ManifestEntry],
    ) -> Vec<ManifestEntry> {
        let mut entries = existing_entries
            .iter()
            .cloned()
            .map(|entry| entry.with_kind(FileKind::Delete))
            .collect::<Vec<_>>();
        entries.extend(new_entries.iter().cloned());
        entries
    }

    async fn scan_snapshot_entries(
        &self,
        snapshot: &Option<Snapshot>,
        partition_filter: Option<&PartitionFilter>,
    ) -> Result<Vec<ManifestEntry>> {
        let Some(snap) = snapshot else {
            return Ok(vec![]);
        };
        let file_io = self.snapshot_manager.file_io();
        let manifest_dir = self.snapshot_manager.manifest_dir();
        let mut entries = Vec::new();
        for manifest_list in [snap.base_manifest_list(), snap.delta_manifest_list()] {
            let manifest_list_path = format!("{manifest_dir}/{manifest_list}");
            for manifest_file in ManifestList::read(file_io, &manifest_list_path).await? {
                let manifest_path = format!("{manifest_dir}/{}", manifest_file.file_name());
                for entry in Manifest::read(file_io, &manifest_path).await? {
                    if let Some(filter) = partition_filter {
                        if !filter.matches_entry(entry.partition())? {
                            continue;
                        }
                    }
                    entries.push(entry);
                }
            }
        }
        Ok(merge_active_entries(entries))
    }

    async fn scan_changed_partition_entries(
        &self,
        snapshot: &Option<Snapshot>,
        commit_entries: &[ManifestEntry],
    ) -> Result<Vec<ManifestEntry>> {
        let entry_refs = commit_entries.iter().collect::<Vec<_>>();
        let partition_filter = self.build_entries_partition_filter(&entry_refs)?;
        self.scan_snapshot_entries(snapshot, partition_filter.as_ref())
            .await
    }

    async fn read_delta_entries(
        &self,
        partition_filter: Option<&PartitionFilter>,
        snapshot: &Snapshot,
    ) -> Result<Vec<ManifestEntry>> {
        let file_io = self.snapshot_manager.file_io();
        let manifest_dir = self.snapshot_manager.manifest_dir();
        let delta_path = format!("{manifest_dir}/{}", snapshot.delta_manifest_list());
        let manifest_files = ManifestList::read(file_io, &delta_path).await?;
        let mut entries = Vec::new();
        for manifest in manifest_files {
            let path = format!("{manifest_dir}/{}", manifest.file_name());
            for entry in Manifest::read(file_io, &path).await? {
                if let Some(filter) = partition_filter {
                    if !filter.matches_entry(entry.partition())? {
                        continue;
                    }
                }
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    async fn read_incremental_changes(
        &self,
        from_snapshot: &Snapshot,
        to_snapshot: &Snapshot,
        commit_entries: &[ManifestEntry],
    ) -> Result<Option<Vec<ManifestEntry>>> {
        let entry_refs = commit_entries.iter().collect::<Vec<_>>();
        let partition_filter = self.build_entries_partition_filter(&entry_refs)?;
        let mut entries = Vec::new();
        for snapshot_id in from_snapshot.id() + 1..=to_snapshot.id() {
            let snapshot = match self.snapshot_manager.get_snapshot(snapshot_id).await {
                Ok(snapshot) => snapshot,
                Err(_) => return Ok(None),
            };
            entries.extend(
                self.read_delta_entries(partition_filter.as_ref(), &snapshot)
                    .await?,
            );
        }
        Ok(Some(entries))
    }

    async fn detect_commit_conflicts(
        &self,
        latest_snapshot: &Option<Snapshot>,
        retry_state: Option<&RetryState>,
        commit_entries: &[ManifestEntry],
        commit_kind: &CommitKind,
        check_from_snapshot: Option<i64>,
    ) -> Result<Option<Vec<ManifestEntry>>> {
        let base_data_files = self
            .resolve_conflict_base_entries(latest_snapshot, retry_state, commit_entries)
            .await?;
        self.check_commit_conflicts(
            latest_snapshot.as_ref(),
            &base_data_files,
            commit_entries,
            commit_kind,
            check_from_snapshot,
        )
        .await?;
        Ok(Some(base_data_files))
    }

    async fn resolve_conflict_base_entries(
        &self,
        latest_snapshot: &Option<Snapshot>,
        retry_state: Option<&RetryState>,
        commit_entries: &[ManifestEntry],
    ) -> Result<Vec<ManifestEntry>> {
        let Some(latest) = latest_snapshot else {
            return Ok(vec![]);
        };

        if let Some(RetryState {
            latest_snapshot: Some(previous_snapshot),
            base_data_files: Some(previous_base),
        }) = retry_state
        {
            if let Some(incremental) = self
                .read_incremental_changes(previous_snapshot, latest, commit_entries)
                .await?
            {
                let mut base = previous_base.clone();
                base.extend(incremental);
                return Ok(merge_active_entries(base));
            }
        }

        self.scan_changed_partition_entries(latest_snapshot, commit_entries)
            .await
    }

    async fn check_commit_conflicts(
        &self,
        latest_snapshot: Option<&Snapshot>,
        base_entries: &[ManifestEntry],
        delta_entries: &[ManifestEntry],
        commit_kind: &CommitKind,
        check_from_snapshot: Option<i64>,
    ) -> Result<()> {
        self.check_delete_entries_against_base(base_entries, delta_entries)?;

        if !self.data_evolution_enabled {
            return Ok(());
        }

        let next_row_id = latest_snapshot.and_then(Snapshot::next_row_id);
        self.check_row_id_existence(base_entries, delta_entries, next_row_id)?;

        let mut all_entries = base_entries.to_vec();
        all_entries.extend(delta_entries.iter().cloned());
        let merged_entries = merge_active_entries(all_entries);
        self.check_row_id_range_conflicts(commit_kind, check_from_snapshot, &merged_entries)?;
        self.check_row_id_from_snapshot(latest_snapshot, delta_entries, check_from_snapshot)
            .await
    }

    fn check_deletion_vector_index_only_conflict(
        &self,
        latest_snapshot: Option<&Snapshot>,
        data_entries: &[ManifestEntry],
        index_entries: &[IndexManifestEntry],
        check_from_snapshot: Option<i64>,
    ) -> Result<()> {
        if !self.data_evolution_enabled || !data_entries.is_empty() {
            return Ok(());
        }
        let Some(check_from_snapshot) = check_from_snapshot else {
            return Ok(());
        };
        let has_deletion_vector_index_change = index_entries
            .iter()
            .any(|entry| entry.index_file.index_type == DELETION_VECTORS_INDEX_TYPE);
        if !has_deletion_vector_index_change {
            return Ok(());
        }
        let Some(latest_snapshot) = latest_snapshot else {
            return Ok(());
        };
        if latest_snapshot.id() <= check_from_snapshot {
            return Ok(());
        }

        Err(crate::Error::DataInvalid {
            message: format!(
                "Row ID conflict: deletion-vector DELETE was prepared from snapshot \
                 {check_from_snapshot}, but latest snapshot is {}. Retry with the latest \
                 deletion vectors.",
                latest_snapshot.id()
            ),
            source: None,
        })
    }

    fn check_delete_entries_against_base(
        &self,
        base_entries: &[ManifestEntry],
        delta_entries: &[ManifestEntry],
    ) -> Result<()> {
        let base_identifiers = base_entries
            .iter()
            .map(ManifestEntry::identifier)
            .collect::<HashSet<_>>();
        for entry in delta_entries
            .iter()
            .filter(|entry| *entry.kind() == FileKind::Delete)
        {
            if !base_identifiers.contains(&entry.identifier()) {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Delete conflict: file '{}' in bucket {} does not exist in the current snapshot.",
                        entry.file().file_name,
                        entry.bucket(),
                    ),
                    source: None,
                });
            }
        }
        Ok(())
    }

    fn check_row_id_existence(
        &self,
        base_entries: &[ManifestEntry],
        delta_entries: &[ManifestEntry],
        next_row_id: Option<i64>,
    ) -> Result<()> {
        let Some(next_row_id) = next_row_id else {
            return Ok(());
        };

        let files_to_check = delta_entries
            .iter()
            .filter(|entry| {
                *entry.kind() == FileKind::Add
                    && entry
                        .file()
                        .first_row_id
                        .is_some_and(|first_row_id| first_row_id < next_row_id)
            })
            .collect::<Vec<_>>();
        if files_to_check.is_empty() {
            return Ok(());
        }

        let mut existing_index: HashSet<(Vec<u8>, i32, i64, i64)> = HashSet::new();
        let mut existing_ranges: ExistingRowIdRanges = HashMap::new();
        for base in base_entries {
            if let Some(first_row_id) = base.file().first_row_id {
                existing_index.insert((
                    base.partition().to_vec(),
                    base.bucket(),
                    first_row_id,
                    base.file().row_count,
                ));
                if !is_dedicated_storage_file(base.file()) {
                    existing_ranges
                        .entry((base.partition().to_vec(), base.bucket()))
                        .or_default()
                        .push((first_row_id, first_row_id + base.file().row_count - 1));
                }
            }
        }

        for entry in files_to_check {
            let first_row_id = entry.file().first_row_id.unwrap();
            if is_dedicated_storage_file(entry.file()) {
                if let Some((start, end)) = entry.file().row_id_range() {
                    let overlaps_existing = existing_ranges
                        .get(&(entry.partition().to_vec(), entry.bucket()))
                        .is_some_and(|ranges| {
                            ranges.iter().any(|&(base_start, base_end)| {
                                ranges_overlap(start, end, base_start, base_end)
                            })
                        });
                    if overlaps_existing {
                        continue;
                    }
                }
            }

            let key = (
                entry.partition().to_vec(),
                entry.bucket(),
                first_row_id,
                entry.file().row_count,
            );
            if !existing_index.contains(&key) {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Row ID existence conflict: file '{}' references first_row_id={}, row_count={} in bucket {}, but no matching file exists in the current snapshot.",
                        entry.file().file_name,
                        first_row_id,
                        entry.file().row_count,
                        entry.bucket(),
                    ),
                    source: None,
                });
            }
        }
        Ok(())
    }

    fn check_row_id_range_conflicts(
        &self,
        commit_kind: &CommitKind,
        check_from_snapshot: Option<i64>,
        commit_entries: &[ManifestEntry],
    ) -> Result<()> {
        if check_from_snapshot.is_none() && commit_kind != &CommitKind::COMPACT {
            return Ok(());
        }

        let entries = commit_entries
            .iter()
            .filter(|entry| {
                entry.file().first_row_id.is_some() && !is_dedicated_storage_file(entry.file())
            })
            .collect::<Vec<_>>();
        for (idx, left) in entries.iter().enumerate() {
            let Some((left_start, left_end)) = left.file().row_id_range() else {
                continue;
            };
            for right in entries.iter().skip(idx + 1) {
                let Some((right_start, right_end)) = right.file().row_id_range() else {
                    continue;
                };
                if ranges_overlap(left_start, left_end, right_start, right_end)
                    && (left_start, left_end) != (right_start, right_end)
                {
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "For Data Evolution table, multiple operations have row-id range conflicts: {} [{}, {}] and {} [{}, {}].",
                            left.file().file_name,
                            left_start,
                            left_end,
                            right.file().file_name,
                            right_start,
                            right_end,
                        ),
                        source: None,
                    });
                }
            }
        }
        Ok(())
    }

    async fn check_row_id_from_snapshot(
        &self,
        latest_snapshot: Option<&Snapshot>,
        delta_entries: &[ManifestEntry],
        check_from_snapshot: Option<i64>,
    ) -> Result<()> {
        let Some(check_from_snapshot) = check_from_snapshot else {
            return Ok(());
        };
        let Some(latest_snapshot) = latest_snapshot else {
            return Ok(());
        };

        let source_snapshot = self
            .snapshot_manager
            .get_snapshot(check_from_snapshot)
            .await?;
        let check_next_row_id =
            source_snapshot
                .next_row_id()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!(
                        "Next row id cannot be null for snapshot {check_from_snapshot}."
                    ),
                    source: None,
                })?;

        let write_ranges = self.build_row_id_write_ranges(delta_entries).await?;
        if write_ranges.is_empty() {
            return Ok(());
        }

        let delta_entry_refs = delta_entries.iter().collect::<Vec<_>>();
        let partition_filter = self.build_entries_partition_filter(&delta_entry_refs)?;
        for snapshot_id in check_from_snapshot + 1..=latest_snapshot.id() {
            let snapshot = self.snapshot_manager.get_snapshot(snapshot_id).await?;
            if snapshot.commit_kind() == &CommitKind::COMPACT {
                continue;
            }
            for entry in self
                .read_delta_entries(partition_filter.as_ref(), &snapshot)
                .await?
                .into_iter()
                .filter(|entry| *entry.kind() == FileKind::Add)
            {
                let Some((start, end)) = entry.file().row_id_range() else {
                    continue;
                };
                if start >= check_next_row_id {
                    continue;
                }
                let committed_field_ids = self.write_field_ids(entry.file()).await?;
                if write_ranges.iter().any(|range| {
                    ranges_overlap(range.start, range.end, start, end)
                        && range
                            .field_ids
                            .iter()
                            .any(|field_id| committed_field_ids.contains(field_id))
                }) {
                    return Err(crate::Error::DataInvalid {
                        message: "For Data Evolution table, multiple MERGE INTO operations have encountered conflicts, updating the same file, which can render some updates ineffective.".to_string(),
                        source: None,
                    });
                }
            }
        }
        Ok(())
    }

    async fn build_row_id_write_ranges(
        &self,
        delta_entries: &[ManifestEntry],
    ) -> Result<Vec<RowIdWriteRange>> {
        let mut ranges = Vec::new();
        for entry in delta_entries
            .iter()
            .filter(|entry| *entry.kind() == FileKind::Add)
        {
            let Some((start, end)) = entry.file().row_id_range() else {
                continue;
            };
            let field_ids = self.write_field_ids(entry.file()).await?;
            if !field_ids.is_empty() {
                ranges.push(RowIdWriteRange {
                    start,
                    end,
                    field_ids,
                });
            }
        }
        Ok(ranges)
    }

    async fn write_field_ids(&self, file: &DataFileMeta) -> Result<HashSet<i32>> {
        let fields = if file.schema_id == self.table.schema().id() {
            self.table.schema().fields().to_vec()
        } else {
            self.table
                .schema_manager()
                .schema(file.schema_id)
                .await?
                .fields()
                .to_vec()
        };
        let field_id_by_name = fields
            .iter()
            .map(|field| (field.name().to_string(), field.id()))
            .collect::<HashMap<_, _>>();

        let mut field_ids = HashSet::new();
        match file.write_cols.as_ref() {
            None => {
                field_ids.extend(
                    fields
                        .iter()
                        .filter(|field| !is_system_field(field.name()))
                        .map(|field| field.id()),
                );
            }
            Some(write_cols) => {
                for col in write_cols {
                    if is_system_field(col) {
                        continue;
                    }
                    let Some(field_id) = field_id_by_name.get(col) else {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "Cannot find write column '{}' in schema {}.",
                                col, file.schema_id
                            ),
                            source: None,
                        });
                    };
                    field_ids.insert(*field_id);
                }
            }
        }
        Ok(field_ids)
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
    ) -> Result<(Vec<ManifestEntry>, i64)> {
        let mut result = Vec::with_capacity(entries.len());
        let mut start = first_row_id_start;
        let mut blob_start_default = first_row_id_start;
        let mut blob_starts: HashMap<String, i64> = HashMap::new();
        let mut vector_store_start = first_row_id_start;

        for entry in entries {
            let mut entry = entry.with_sequence_number(snapshot_id, snapshot_id);
            if entry.file().file_source.is_none() {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "file_source must be present for row-tracking table, file={}",
                        entry.file().file_name
                    ),
                    source: None,
                });
            }
            let contains_row_id =
                entry.file().write_cols.as_ref().is_some_and(|cols| {
                    cols.iter().any(|col| col == crate::spec::ROW_ID_FIELD_NAME)
                });
            if *entry.kind() == FileKind::Add
                && entry.file().file_source == Some(0) // APPEND
                && entry.file().first_row_id.is_none()
                && !contains_row_id
            {
                if is_blob_data_file(entry.file()) {
                    let blob_field_name = entry
                        .file()
                        .write_cols
                        .as_ref()
                        .and_then(|cols| cols.first())
                        .cloned()
                        .ok_or_else(|| crate::Error::DataInvalid {
                            message: format!(
                                "Blob file '{}' must have write_cols for row-tracking assignment.",
                                entry.file().file_name
                            ),
                            source: None,
                        })?;
                    let blob_start = blob_starts
                        .entry(blob_field_name)
                        .or_insert(blob_start_default);
                    if *blob_start >= start {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "This is a bug, blobStart {} should be less than start {} when assigning a blob entry file.",
                                *blob_start, start
                            ),
                            source: None,
                        });
                    }
                    entry = entry.with_first_row_id(*blob_start);
                    *blob_start += entry.file().row_count;
                } else if is_vector_store_file(entry.file()) {
                    if vector_store_start >= start {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "This is a bug, vectorStoreStart {} should be less than start {} when assigning a vector-store entry file.",
                                vector_store_start, start
                            ),
                            source: None,
                        });
                    }
                    entry = entry.with_first_row_id(vector_store_start);
                    vector_store_start += entry.file().row_count;
                } else {
                    entry = entry.with_first_row_id(start);
                    blob_start_default = start;
                    blob_starts.clear();
                    start += entry.file().row_count;
                }
            }
            result.push(entry);
        }

        Ok((result, start))
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
            return Ok(BinaryTableStats::empty());
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

        let min_bytes = build_partition_stats_row(&mins, &data_types);
        let max_bytes = build_partition_stats_row(&maxs, &data_types);
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

    /// Earliest source snapshot requested by row-id conflict checks.
    fn min_check_from_snapshot(messages: &[CommitMessage]) -> Option<i64> {
        messages
            .iter()
            .filter_map(|message| message.check_from_snapshot)
            .min()
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

    /// Convert commit messages to changelog manifest entries (ADD kind only).
    fn messages_to_changelog_entries(&self, messages: &[CommitMessage]) -> Vec<ManifestEntry> {
        messages
            .iter()
            .flat_map(|msg| {
                msg.new_changelog_files.iter().map(|file| {
                    ManifestEntry::new(
                        FileKind::Add,
                        msg.partition.clone(),
                        msg.bucket,
                        self.total_buckets,
                        file.clone(),
                        0,
                    )
                })
            })
            .collect()
    }

    /// Convert commit messages to index manifest entries (ADD kind).
    fn messages_to_index_entries(&self, messages: &[CommitMessage]) -> Vec<IndexManifestEntry> {
        messages
            .iter()
            .flat_map(|msg| {
                let adds = msg
                    .new_index_files
                    .iter()
                    .map(move |index_file| IndexManifestEntry {
                        kind: FileKind::Add,
                        partition: msg.partition.clone(),
                        bucket: msg.bucket,
                        index_file: index_file.clone(),
                        version: 1,
                    });
                let deletes =
                    msg.deleted_index_files
                        .iter()
                        .map(move |index_file| IndexManifestEntry {
                            kind: FileKind::Delete,
                            partition: msg.partition.clone(),
                            bucket: msg.bucket,
                            index_file: index_file.clone(),
                            version: 1,
                        });
                adds.chain(deletes)
            })
            .collect()
    }
}

/// Serialized BinaryRow for partition stats; unlike `datums_to_binary_row`, returns a
/// valid arity-N row even when every datum is `None` (the all-null case must still
/// decode on the Java side).
fn build_partition_stats_row(datums: &[Option<Datum>], data_types: &[DataType]) -> Vec<u8> {
    let mut builder = BinaryRowBuilder::new(datums.len() as i32);
    for (pos, (datum_opt, data_type)) in datums.iter().zip(data_types.iter()).enumerate() {
        match datum_opt {
            Some(d) => builder.write_datum(pos, d, data_type),
            None => builder.set_null_at(pos),
        }
    }
    builder.build_serialized()
}

/// Plan for resolving commit entries.
enum CommitEntriesPlan {
    /// Caller-provided entries. May contain `FileKind::Delete` entries from CoW
    /// rewrites, in which case `resolve_commit` auto-promotes to `CommitKind::OVERWRITE`.
    Direct {
        entries: Vec<ManifestEntry>,
        changelog_entries: Vec<ManifestEntry>,
        new_index_entries: Vec<IndexManifestEntry>,
        check_from_snapshot: Option<i64>,
    },
    /// Overwrite with optional partition filter.
    Overwrite {
        partition_filter: Option<PartitionFilter>,
        new_entries: Vec<ManifestEntry>,
        new_index_entries: Vec<IndexManifestEntry>,
        cached_snapshot: Option<Box<Snapshot>>,
        cached_entries: Vec<ManifestEntry>,
        full_scan_count: usize,
        delta_probe_count: usize,
    },
}

impl CommitEntriesPlan {
    fn commit_kind_hint(&self) -> CommitKind {
        match self {
            CommitEntriesPlan::Direct { entries, .. } => {
                if entries
                    .iter()
                    .any(|entry| *entry.kind() == FileKind::Delete)
                {
                    CommitKind::OVERWRITE
                } else {
                    CommitKind::APPEND
                }
            }
            CommitEntriesPlan::Overwrite { .. } => CommitKind::OVERWRITE,
        }
    }
}

/// Fully resolved commit ready for writing.
struct ResolvedCommit {
    entries: Vec<ManifestEntry>,
    changelog_entries: Vec<ManifestEntry>,
    kind: CommitKind,
    index_manifest_name: Option<String>,
    index_manifest_changed: bool,
    base_data_files: Option<Vec<ManifestEntry>>,
}

enum CommitAttemptResult {
    Success,
    Retry(Box<RetryState>),
}

struct RetryState {
    latest_snapshot: Option<Snapshot>,
    base_data_files: Option<Vec<ManifestEntry>>,
}

struct RowIdWriteRange {
    start: i64,
    end: i64,
    field_ids: HashSet<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileStorageKind {
    Normal,
    Blob,
    Vector,
}

fn ranges_overlap(left_start: i64, left_end: i64, right_start: i64, right_end: i64) -> bool {
    left_start <= right_end && right_start <= left_end
}

fn is_blob_data_file(file: &DataFileMeta) -> bool {
    crate::table::blob_file_writer::is_blob_file_name(&file.file_name)
}

fn is_vector_store_file(file: &DataFileMeta) -> bool {
    file.file_name.contains(".vector.")
}

fn is_dedicated_storage_file(file: &DataFileMeta) -> bool {
    !matches!(file_storage_kind(file), FileStorageKind::Normal)
}

fn file_storage_kind(file: &DataFileMeta) -> FileStorageKind {
    if is_blob_data_file(file) {
        FileStorageKind::Blob
    } else if is_vector_store_file(file) {
        FileStorageKind::Vector
    } else {
        FileStorageKind::Normal
    }
}

fn is_system_field(name: &str) -> bool {
    matches!(
        name,
        crate::spec::ROW_ID_FIELD_NAME
            | crate::spec::SEQUENCE_NUMBER_FIELD_NAME
            | crate::spec::VALUE_KIND_FIELD_NAME
    )
}

fn global_index_overlap_error(
    retained: &IndexManifestEntry,
    retained_meta: &crate::spec::GlobalIndexMeta,
    added: &IndexManifestEntry,
    added_meta: &crate::spec::GlobalIndexMeta,
) -> crate::Error {
    crate::Error::DataInvalid {
        message: format!(
            "Trying to add global index file {} of type {} for index field {} with row range \
             [{}, {}], but previous file {} still exists with overlapping row range [{}, {}]. \
             Remove the previous file first.",
            added.index_file.file_name,
            added.index_file.index_type,
            added_meta.index_field_id,
            added_meta.row_range_start,
            added_meta.row_range_end,
            retained.index_file.file_name,
            retained_meta.row_range_start,
            retained_meta.row_range_end,
        ),
        source: None,
    }
}

fn normalize_index_entries(entries: Vec<IndexManifestEntry>) -> Vec<IndexManifestEntry> {
    let mut active = Vec::new();
    for entry in entries {
        match entry.kind {
            FileKind::Add => {
                active.retain(|current| !same_index_file_entry(current, &entry));
                active.push(entry);
            }
            FileKind::Delete => {
                active.retain(|current| !same_index_file_entry(current, &entry));
            }
        }
    }
    active
}

fn same_index_file_entry(left: &IndexManifestEntry, right: &IndexManifestEntry) -> bool {
    same_index_partition(&left.partition, &right.partition)
        && left.bucket == right.bucket
        && left.index_file.index_type == right.index_file.index_type
        && left.index_file.file_name == right.index_file.file_name
}

fn same_index_partition(left: &[u8], right: &[u8]) -> bool {
    left == right || (is_empty_partition(left) && is_empty_partition(right))
}

fn is_empty_partition(partition: &[u8]) -> bool {
    partition.is_empty()
        || partition == EMPTY_SERIALIZED_ROW.as_slice()
        || partition == [0, 0, 0, 0]
}

fn validate_expected_latest_snapshot(
    expected_snapshot_id: Option<i64>,
    latest_snapshot: &Option<Snapshot>,
) -> Result<()> {
    let Some(expected_snapshot_id) = expected_snapshot_id else {
        return Ok(());
    };
    let actual_snapshot_id = latest_snapshot.as_ref().map(Snapshot::id);
    if actual_snapshot_id == Some(expected_snapshot_id) {
        return Ok(());
    }
    Err(crate::Error::DataInvalid {
        message: format!(
            "Snapshot changed while committing index files: expected latest snapshot {}, got {}",
            expected_snapshot_id,
            actual_snapshot_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "none".to_string())
        ),
        source: None,
    })
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
    use crate::spec::{
        BinaryRowBuilder, DataFileMeta, DeletionVectorMeta, GlobalIndexMeta, IndexFileMeta,
        ManifestList, TableSchema,
    };
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

    fn test_table_with_options(
        file_io: &FileIO,
        table_path: &str,
        options: HashMap<String, String>,
    ) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_table"),
            table_path.to_string(),
            test_schema().copy_with_options(options),
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
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
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

    fn test_global_index_file(
        name: &str,
        index_field_id: i32,
        row_range_start: i64,
        row_range_end: i64,
    ) -> IndexFileMeta {
        IndexFileMeta {
            index_type: "lumina".to_string(),
            file_name: name.to_string(),
            file_size: 128,
            row_count: (row_range_end - row_range_start + 1) as i32,
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start,
                row_range_end,
                index_field_id,
                extra_field_ids: None,
                index_meta: None,
            }),
        }
    }

    fn test_global_index_file_with_extra_fields(
        name: &str,
        index_field_id: i32,
        extra_field_ids: Vec<i32>,
        row_range_start: i64,
        row_range_end: i64,
    ) -> IndexFileMeta {
        let mut file = test_global_index_file(name, index_field_id, row_range_start, row_range_end);
        file.global_index_meta
            .as_mut()
            .expect("global index meta")
            .extra_field_ids = Some(extra_field_ids);
        file
    }

    fn test_deletion_vector_index_file(name: &str, data_file_name: &str) -> IndexFileMeta {
        IndexFileMeta {
            index_type: DELETION_VECTORS_INDEX_TYPE.to_string(),
            file_name: name.to_string(),
            file_size: 128,
            row_count: 1,
            deletion_vectors_ranges: Some(indexmap::IndexMap::from([(
                data_file_name.to_string(),
                DeletionVectorMeta {
                    offset: 1,
                    length: 16,
                    cardinality: Some(1),
                },
            )])),
            global_index_meta: None,
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

    fn partition_filter_for(commit: &TableCommit, partitions: Vec<Vec<u8>>) -> PartitionFilter {
        PartitionFilter::from_partition_set(
            partitions.into_iter().collect(),
            &commit.table.schema().partition_fields(),
        )
        .unwrap()
    }

    fn overwrite_plan_counts(plan: &CommitEntriesPlan) -> (usize, usize) {
        match plan {
            CommitEntriesPlan::Overwrite {
                full_scan_count,
                delta_probe_count,
                ..
            } => (*full_scan_count, *delta_probe_count),
            CommitEntriesPlan::Direct { .. } => unreachable!(),
        }
    }

    fn overwrite_plan(
        partition_filter: Option<PartitionFilter>,
        new_entries: Vec<ManifestEntry>,
    ) -> CommitEntriesPlan {
        CommitEntriesPlan::Overwrite {
            partition_filter,
            new_entries,
            new_index_entries: vec![],
            cached_snapshot: None,
            cached_entries: Vec::new(),
            full_scan_count: 0,
            delta_probe_count: 0,
        }
    }

    async fn latest_snapshot(file_io: &FileIO, table_path: &str) -> Option<Snapshot> {
        SnapshotManager::new(file_io.clone(), table_path.to_string())
            .get_latest_snapshot()
            .await
            .unwrap()
    }

    async fn active_entries(
        file_io: &FileIO,
        table_path: &str,
        snapshot: &Snapshot,
    ) -> Vec<ManifestEntry> {
        let manifest_dir = format!("{table_path}/manifest");
        let mut entries = Vec::new();
        for list in [
            snapshot.base_manifest_list(),
            snapshot.delta_manifest_list(),
        ] {
            let list_path = format!("{manifest_dir}/{list}");
            for meta in ManifestList::read(file_io, &list_path).await.unwrap() {
                entries.extend(
                    Manifest::read(file_io, &format!("{manifest_dir}/{}", meta.file_name()))
                        .await
                        .unwrap(),
                );
            }
        }
        merge_active_entries(entries)
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
    async fn test_commit_with_identifier_writes_snapshot_identifier() {
        let file_io = test_file_io();
        let table_path = "memory:/test_commit_with_identifier";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);
        commit
            .commit_with_identifier(
                vec![CommitMessage::new(
                    vec![],
                    0,
                    vec![test_data_file("data-0.parquet", 100)],
                )],
                42,
            )
            .await
            .unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.commit_identifier(), 42);
    }

    #[tokio::test]
    async fn test_duplicate_commit_requires_same_identifier() {
        let file_io = test_file_io();
        let table_path = "memory:/test_duplicate_commit_identifier";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);
        commit
            .commit_with_identifier(
                vec![CommitMessage::new(
                    vec![],
                    0,
                    vec![test_data_file("data-0.parquet", 100)],
                )],
                7,
            )
            .await
            .unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let latest = snap_manager.get_latest_snapshot().await.unwrap();
        assert!(
            commit
                .is_duplicate_commit(1, &latest, 7, &CommitKind::APPEND)
                .await
        );
        assert!(
            !commit
                .is_duplicate_commit(1, &latest, 8, &CommitKind::APPEND)
                .await
        );
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
    async fn test_index_only_commit_creates_snapshot() {
        let file_io = test_file_io();
        let table_path = "memory:/test_index_only_commit";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_row_tracking_commit(&file_io, table_path);
        let mut data_file = test_data_file("data-0.parquet", 10);
        data_file.file_source = Some(0);
        commit
            .commit(vec![CommitMessage::new(vec![], 0, vec![data_file])])
            .await
            .unwrap();

        let mut message = CommitMessage::new(vec![], 0, vec![]);
        message.new_index_files = vec![test_global_index_file("lumina-0.index", 0, 0, 9)];
        commit
            .commit_if_latest_snapshot(vec![message], 1)
            .await
            .unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 2);
        assert_eq!(snapshot.total_record_count(), Some(10));
        assert_eq!(snapshot.delta_record_count(), Some(0));
        assert_eq!(snapshot.next_row_id(), Some(10));

        let index_manifest = snapshot.index_manifest().expect("index manifest");
        let manifest_dir = format!("{table_path}/manifest");
        let index_entries =
            IndexManifest::read(&file_io, &format!("{manifest_dir}/{index_manifest}"))
                .await
                .unwrap();
        assert_eq!(index_entries.len(), 1);
        assert_eq!(index_entries[0].index_file.file_name, "lumina-0.index");
    }

    #[tokio::test]
    async fn test_index_only_commit_rejects_stale_snapshot_guard() {
        let file_io = test_file_io();
        let table_path = "memory:/test_index_only_commit_snapshot_guard";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_row_tracking_commit(&file_io, table_path);
        let mut data_file = test_data_file("data-0.parquet", 10);
        data_file.file_source = Some(0);
        commit
            .commit(vec![CommitMessage::new(vec![], 0, vec![data_file])])
            .await
            .unwrap();

        let mut message = CommitMessage::new(vec![], 0, vec![]);
        message.new_index_files = vec![test_global_index_file("lumina-0.index", 0, 0, 9)];
        let result = commit.commit_if_latest_snapshot(vec![message], 0).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Snapshot changed while committing index files"),
            "expected snapshot guard error, got: {err_msg}"
        );

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 1);
        assert!(snapshot.index_manifest().is_none());
    }

    #[tokio::test]
    async fn test_deletion_vector_index_only_commit_rejects_stale_snapshot() {
        let file_io = test_file_io();
        let table_path = "memory:/test_dv_index_only_stale_snapshot";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_data_evolution_commit(&file_io, table_path);
        let partition = EMPTY_SERIALIZED_ROW.clone();
        let mut data_file = test_data_file("data-0.parquet", 10);
        data_file.file_source = Some(0);
        commit
            .commit(vec![CommitMessage::new(
                partition.clone(),
                0,
                vec![data_file],
            )])
            .await
            .unwrap();

        let mut first_delete = CommitMessage::new(partition.clone(), 0, vec![]);
        first_delete.check_from_snapshot = Some(1);
        first_delete.new_index_files = vec![test_deletion_vector_index_file(
            "dv-0.index",
            "data-0.parquet",
        )];
        commit.commit(vec![first_delete]).await.unwrap();

        let mut stale_delete = CommitMessage::new(partition, 0, vec![]);
        stale_delete.check_from_snapshot = Some(1);
        stale_delete.new_index_files = vec![test_deletion_vector_index_file(
            "dv-1.index",
            "data-0.parquet",
        )];
        let result = commit.commit(vec![stale_delete]).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Row ID conflict"),
            "expected row-id conflict for stale DV commit, got: {err_msg}"
        );

        let snapshot = latest_snapshot(&file_io, table_path).await.unwrap();
        assert_eq!(snapshot.id(), 2);
    }

    #[tokio::test]
    async fn test_global_index_overlap_rejected_on_commit() {
        let file_io = test_file_io();
        let table_path = "memory:/test_global_index_overlap";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);
        let mut first = CommitMessage::new(vec![], 0, vec![]);
        first.new_index_files = vec![test_global_index_file("lumina-0.index", 0, 0, 9)];
        commit.commit(vec![first]).await.unwrap();

        let mut second = CommitMessage::new(vec![], 0, vec![]);
        second.new_index_files = vec![test_global_index_file("lumina-1.index", 0, 5, 14)];

        let result = commit.commit(vec![second]).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("overlapping row range"),
            "expected overlap error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_global_index_overlap_rejected_within_same_commit() {
        let file_io = test_file_io();
        let table_path = "memory:/test_global_index_overlap_same_commit";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);
        let mut message = CommitMessage::new(vec![], 0, vec![]);
        message.new_index_files = vec![
            test_global_index_file("lumina-0.index", 0, 0, 9),
            test_global_index_file("lumina-1.index", 0, 5, 14),
        ];

        let result = commit.commit(vec![message]).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("overlapping row range"),
            "expected overlap error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_global_index_non_overlap_allowed_on_commit() {
        let file_io = test_file_io();
        let table_path = "memory:/test_global_index_non_overlap";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);
        let mut first = CommitMessage::new(vec![], 0, vec![]);
        first.new_index_files = vec![test_global_index_file("lumina-0.index", 0, 0, 9)];
        commit.commit(vec![first]).await.unwrap();

        let mut second = CommitMessage::new(vec![], 0, vec![]);
        second.new_index_files = vec![test_global_index_file("lumina-1.index", 0, 10, 19)];
        commit.commit(vec![second]).await.unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        let index_manifest = snapshot.index_manifest().expect("index manifest");
        let index_entries =
            IndexManifest::read(&file_io, &format!("{table_path}/manifest/{index_manifest}"))
                .await
                .unwrap();
        assert_eq!(index_entries.len(), 2);
    }

    #[tokio::test]
    async fn test_index_delete_removes_previous_index_manifest_entry() {
        let file_io = test_file_io();
        let table_path = "memory:/test_index_delete";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);
        let index_file = test_global_index_file("lumina-0.index", 0, 0, 9);
        let mut first = CommitMessage::new(vec![], 0, vec![]);
        first.new_index_files = vec![index_file.clone()];
        commit.commit(vec![first]).await.unwrap();

        let mut second = CommitMessage::new(vec![], 0, vec![]);
        second.deleted_index_files = vec![index_file];
        commit
            .commit_with_identifier(vec![second], 2)
            .await
            .unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 2);
        assert!(snapshot.index_manifest().is_none());
    }

    #[tokio::test]
    async fn test_append_data_preserves_previous_global_index() {
        let file_io = test_file_io();
        let table_path = "memory:/test_append_data_preserves_previous_global_index";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);
        let mut first = CommitMessage::new(vec![], 0, vec![]);
        first.new_index_files = vec![test_global_index_file("lumina-0.index", 0, 0, 9)];
        commit.commit(vec![first]).await.unwrap();

        commit
            .commit(vec![CommitMessage::new(
                vec![],
                0,
                vec![test_data_file("data-0.parquet", 10)],
            )])
            .await
            .unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 2);
        assert!(snapshot.index_manifest().is_some());
        let index_manifest = snapshot.index_manifest().expect("index manifest");
        let index_entries =
            IndexManifest::read(&file_io, &format!("{table_path}/manifest/{index_manifest}"))
                .await
                .unwrap();
        assert_eq!(index_entries.len(), 1);
        assert_eq!(index_entries[0].index_file.file_name, "lumina-0.index");
    }

    #[tokio::test]
    async fn test_partial_update_indexed_column_rejects_by_default() {
        let file_io = test_file_io();
        let table_path = "memory:/test_partial_update_indexed_column_rejects";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);
        let mut first = CommitMessage::new(vec![], 0, vec![]);
        first.new_index_files = vec![test_global_index_file("lumina-0.index", 0, 0, 9)];
        commit.commit(vec![first]).await.unwrap();

        let mut data_file = test_data_file("data-update-id.parquet", 10);
        data_file.write_cols = Some(vec!["id".to_string()]);
        let result = commit
            .commit(vec![CommitMessage::new(vec![], 0, vec![data_file])])
            .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("globally indexed columns"),
            "expected global index update error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_partial_update_indexed_column_drops_partition_index_when_configured() {
        let file_io = test_file_io();
        let table_path = "memory:/test_partial_update_indexed_column_drops_index";
        setup_dirs(&file_io, table_path).await;

        let table = Table::new(
            file_io.clone(),
            Identifier::new("default", "test_table"),
            table_path.to_string(),
            test_schema().copy_with_options(HashMap::from([(
                "global-index.column-update-action".to_string(),
                "DROP_PARTITION_INDEX".to_string(),
            )])),
            None,
        );
        let commit = TableCommit::new(table, "test-user".to_string());

        let mut first = CommitMessage::new(vec![], 0, vec![]);
        first.new_index_files = vec![test_global_index_file("lumina-0.index", 0, 0, 9)];
        commit.commit(vec![first]).await.unwrap();

        let mut data_file = test_data_file("data-update-id.parquet", 10);
        data_file.write_cols = Some(vec!["id".to_string()]);
        commit
            .commit(vec![CommitMessage::new(vec![], 0, vec![data_file])])
            .await
            .unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 2);
        assert!(snapshot.index_manifest().is_none());
    }

    #[tokio::test]
    async fn test_partial_update_extra_indexed_column_rejects_by_default() {
        let file_io = test_file_io();
        let table_path = "memory:/test_partial_update_extra_indexed_column_rejects";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);
        let mut first = CommitMessage::new(vec![], 0, vec![]);
        first.new_index_files = vec![test_global_index_file_with_extra_fields(
            "lumina-id-name.index",
            0,
            vec![1],
            0,
            9,
        )];
        commit.commit(vec![first]).await.unwrap();

        let mut data_file = test_data_file("data-update-name.parquet", 10);
        data_file.write_cols = Some(vec!["name".to_string()]);
        let result = commit
            .commit(vec![CommitMessage::new(vec![], 0, vec![data_file])])
            .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Conflicted columns")
                && err_msg.contains("name")
                && err_msg.contains("globally indexed columns"),
            "expected extra-field global index update error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_partial_update_extra_indexed_column_drops_partition_index_when_configured() {
        let file_io = test_file_io();
        let table_path = "memory:/test_partial_update_extra_indexed_column_drops_index";
        setup_dirs(&file_io, table_path).await;

        let table = test_table_with_options(
            &file_io,
            table_path,
            HashMap::from([(
                "global-index.column-update-action".to_string(),
                "DROP_PARTITION_INDEX".to_string(),
            )]),
        );
        let commit = TableCommit::new(table, "test-user".to_string());

        let mut first = CommitMessage::new(vec![], 0, vec![]);
        first.new_index_files = vec![test_global_index_file_with_extra_fields(
            "lumina-id-name.index",
            0,
            vec![1],
            0,
            9,
        )];
        commit.commit(vec![first]).await.unwrap();

        let mut data_file = test_data_file("data-update-name.parquet", 10);
        data_file.write_cols = Some(vec!["name".to_string()]);
        commit
            .commit(vec![CommitMessage::new(vec![], 0, vec![data_file])])
            .await
            .unwrap();

        let snapshot = latest_snapshot(&file_io, table_path).await.unwrap();
        assert_eq!(snapshot.id(), 2);
        assert!(snapshot.index_manifest().is_none());
    }

    #[tokio::test]
    async fn test_partial_update_non_indexed_column_preserves_global_index() {
        let file_io = test_file_io();
        let table_path = "memory:/test_partial_update_non_indexed_column_preserves_index";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);
        let mut first = CommitMessage::new(vec![], 0, vec![]);
        first.new_index_files = vec![test_global_index_file("lumina-0.index", 0, 0, 9)];
        commit.commit(vec![first]).await.unwrap();

        let mut data_file = test_data_file("data-update-name.parquet", 10);
        data_file.write_cols = Some(vec!["name".to_string()]);
        commit
            .commit(vec![CommitMessage::new(vec![], 0, vec![data_file])])
            .await
            .unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 2);
        assert!(snapshot.index_manifest().is_some());
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
    async fn test_overwrite_cache_reuses_when_append_misses_target_partition() {
        let file_io = test_file_io();
        let table_path = "memory:/test_overwrite_cache_reuse";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_partitioned_commit(&file_io, table_path);
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

        let new_entries = commit.messages_to_entries(&[CommitMessage::new(
            partition_bytes("a"),
            0,
            vec![test_data_file("data-a2.parquet", 50)],
        )]);
        let mut plan = overwrite_plan(
            Some(partition_filter_for(&commit, vec![partition_bytes("a")])),
            new_entries,
        );

        let snapshot1 = latest_snapshot(&file_io, table_path).await;
        let first = commit
            .provide_overwrite_entries(&mut plan, &snapshot1)
            .await
            .unwrap();
        assert_eq!(overwrite_plan_counts(&plan), (1, 0));
        assert!(first.iter().any(|entry| {
            *entry.kind() == FileKind::Delete && entry.file().file_name == "data-a.parquet"
        }));

        commit
            .commit(vec![CommitMessage::new(
                partition_bytes("z"),
                0,
                vec![test_data_file("data-z.parquet", 10)],
            )])
            .await
            .unwrap();

        let snapshot2 = latest_snapshot(&file_io, table_path).await;
        let second = commit
            .provide_overwrite_entries(&mut plan, &snapshot2)
            .await
            .unwrap();
        assert_eq!(
            overwrite_plan_counts(&plan),
            (1, 1),
            "unrelated APPEND should reuse the cached target-partition scan"
        );
        assert!(second.iter().any(|entry| {
            *entry.kind() == FileKind::Delete && entry.file().file_name == "data-a.parquet"
        }));
        assert!(!second
            .iter()
            .any(|entry| entry.file().file_name == "data-z.parquet"));
    }

    #[tokio::test]
    async fn test_overwrite_cache_rebuilds_when_append_hits_target_partition() {
        let file_io = test_file_io();
        let table_path = "memory:/test_overwrite_cache_rebuild_target_append";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_partitioned_commit(&file_io, table_path);
        commit
            .commit(vec![CommitMessage::new(
                partition_bytes("a"),
                0,
                vec![test_data_file("data-a.parquet", 100)],
            )])
            .await
            .unwrap();

        let new_entries = commit.messages_to_entries(&[CommitMessage::new(
            partition_bytes("a"),
            0,
            vec![test_data_file("data-a2.parquet", 50)],
        )]);
        let mut plan = overwrite_plan(
            Some(partition_filter_for(&commit, vec![partition_bytes("a")])),
            new_entries,
        );

        let snapshot1 = latest_snapshot(&file_io, table_path).await;
        commit
            .provide_overwrite_entries(&mut plan, &snapshot1)
            .await
            .unwrap();

        commit
            .commit(vec![CommitMessage::new(
                partition_bytes("a"),
                0,
                vec![test_data_file("data-a3.parquet", 10)],
            )])
            .await
            .unwrap();

        let snapshot2 = latest_snapshot(&file_io, table_path).await;
        let second = commit
            .provide_overwrite_entries(&mut plan, &snapshot2)
            .await
            .unwrap();
        assert_eq!(
            overwrite_plan_counts(&plan),
            (2, 1),
            "target-partition APPEND must force a full scan rebuild"
        );
        let deleted = second
            .iter()
            .filter(|entry| *entry.kind() == FileKind::Delete)
            .map(|entry| entry.file().file_name.as_str())
            .collect::<HashSet<_>>();
        assert!(deleted.contains("data-a.parquet"));
        assert!(
            deleted.contains("data-a3.parquet"),
            "rebuilt overwrite scan must delete the concurrent target append too"
        );
    }

    #[tokio::test]
    async fn test_overwrite_cache_rebuilds_on_non_append_snapshot() {
        let file_io = test_file_io();
        let table_path = "memory:/test_overwrite_cache_rebuild_non_append";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_partitioned_commit(&file_io, table_path);
        commit
            .commit(vec![CommitMessage::new(
                partition_bytes("a"),
                0,
                vec![test_data_file("data-a.parquet", 100)],
            )])
            .await
            .unwrap();

        let new_entries = commit.messages_to_entries(&[CommitMessage::new(
            partition_bytes("a"),
            0,
            vec![test_data_file("data-a2.parquet", 50)],
        )]);
        let mut plan = overwrite_plan(
            Some(partition_filter_for(&commit, vec![partition_bytes("a")])),
            new_entries,
        );

        let snapshot1 = latest_snapshot(&file_io, table_path).await;
        commit
            .provide_overwrite_entries(&mut plan, &snapshot1)
            .await
            .unwrap();

        commit
            .overwrite(
                vec![CommitMessage::new(
                    partition_bytes("z"),
                    0,
                    vec![test_data_file("data-z.parquet", 10)],
                )],
                None,
            )
            .await
            .unwrap();

        let snapshot2 = latest_snapshot(&file_io, table_path).await;
        commit
            .provide_overwrite_entries(&mut plan, &snapshot2)
            .await
            .unwrap();
        assert_eq!(
            overwrite_plan_counts(&plan),
            (2, 1),
            "non-APPEND snapshots between retries cannot reuse overwrite cache"
        );
    }

    #[tokio::test]
    async fn test_whole_table_overwrite_never_uses_delta_probe_cache() {
        let file_io = test_file_io();
        let table_path = "memory:/test_whole_table_overwrite_cache";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_partitioned_commit(&file_io, table_path);
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

        let new_entries = commit.messages_to_entries(&[CommitMessage::new(
            partition_bytes("a"),
            0,
            vec![test_data_file("data-a2.parquet", 50)],
        )]);
        let mut plan = overwrite_plan(None, new_entries);

        let snapshot1 = latest_snapshot(&file_io, table_path).await;
        commit
            .provide_overwrite_entries(&mut plan, &snapshot1)
            .await
            .unwrap();

        commit
            .commit(vec![CommitMessage::new(
                partition_bytes("z"),
                0,
                vec![test_data_file("data-z.parquet", 10)],
            )])
            .await
            .unwrap();

        let snapshot2 = latest_snapshot(&file_io, table_path).await;
        let second = commit
            .provide_overwrite_entries(&mut plan, &snapshot2)
            .await
            .unwrap();
        assert_eq!(
            overwrite_plan_counts(&plan),
            (2, 0),
            "whole-table overwrite has no target predicate, so it must full-scan each retry"
        );
        assert!(second.iter().any(|entry| {
            *entry.kind() == FileKind::Delete && entry.file().file_name == "data-z.parquet"
        }));
    }

    #[tokio::test]
    async fn test_dynamic_overwrite_ignores_changelog_only_message() {
        let file_io = test_file_io();
        let table_path = "memory:/test_dynamic_overwrite_changelog_only";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_partitioned_commit(&file_io, table_path);
        commit
            .commit(vec![CommitMessage::new(
                partition_bytes("a"),
                0,
                vec![test_data_file("data-a.parquet", 100)],
            )])
            .await
            .unwrap();

        let mut message = CommitMessage::new(partition_bytes("a"), 0, vec![]);
        message.new_changelog_files = vec![test_data_file("changelog-a.parquet", 1)];

        commit.overwrite(vec![message], None).await.unwrap();

        let snap_manager = SnapshotManager::new(file_io, table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 1);
        assert_eq!(snapshot.commit_kind(), &CommitKind::APPEND);
        assert_eq!(snapshot.total_record_count(), Some(100));
        assert_eq!(snapshot.changelog_manifest_list(), None);
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

    #[tokio::test]
    async fn test_drop_partitions_empty_list_rejected() {
        let file_io = test_file_io();
        let table_path = "memory:/test_drop_partitions_empty";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_partitioned_commit(&file_io, table_path);
        let result = commit.drop_partitions(vec![]).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Partitions list cannot be empty"));
    }

    #[tokio::test]
    async fn test_truncate_missing_partition_is_noop() {
        let file_io = test_file_io();
        let table_path = "memory:/test_truncate_missing_partition";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_partitioned_commit(&file_io, table_path);
        commit
            .commit(vec![CommitMessage::new(
                partition_bytes("a"),
                0,
                vec![test_data_file("data-a.parquet", 100)],
            )])
            .await
            .unwrap();

        commit
            .truncate_partitions(vec![HashMap::from([(
                "pt".to_string(),
                Some(Datum::String("missing".to_string())),
            )])])
            .await
            .unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 1);
        assert_eq!(snapshot.total_record_count(), Some(100));
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

    fn test_data_evolution_schema() -> TableSchema {
        use crate::spec::{DataType, IntType, Schema, VarCharType};
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
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

    fn test_data_evolution_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_table"),
            table_path.to_string(),
            test_data_evolution_schema(),
            None,
        )
    }

    fn setup_row_tracking_commit(file_io: &FileIO, table_path: &str) -> TableCommit {
        let table = test_row_tracking_table(file_io, table_path);
        TableCommit::new(table, "test-user".to_string())
    }

    fn setup_data_evolution_commit(file_io: &FileIO, table_path: &str) -> TableCommit {
        let table = test_data_evolution_table(file_io, table_path);
        TableCommit::new(table, "test-user".to_string())
    }

    #[tokio::test]
    async fn test_row_tracking_rejects_missing_file_source() {
        let file_io = test_file_io();
        let table_path = "memory:/test_row_tracking_missing_file_source";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_row_tracking_commit(&file_io, table_path);
        let file = test_data_file("data-0.parquet", 10);

        let result = commit
            .commit(vec![CommitMessage::new(vec![], 0, vec![file])])
            .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("file_source must be present"),
            "expected file_source error, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_row_tracking_assigns_vector_store_files_from_current_data_start() {
        let file_io = test_file_io();
        let table_path = "memory:/test_row_tracking_vector_store";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_row_tracking_commit(&file_io, table_path);
        let mut data_file = test_data_file("data-0.parquet", 10);
        data_file.file_source = Some(0);
        let mut vector_file = test_data_file("data-0.vector.vortex", 10);
        vector_file.file_source = Some(0);
        vector_file.write_cols = Some(vec!["name".to_string()]);

        commit
            .commit(vec![CommitMessage::new(
                vec![],
                0,
                vec![data_file, vector_file],
            )])
            .await
            .unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.next_row_id(), Some(10));

        let delta_metas = ManifestList::read(
            &file_io,
            &format!("{table_path}/manifest/{}", snapshot.delta_manifest_list()),
        )
        .await
        .unwrap();
        assert_eq!(delta_metas[0].min_row_id(), Some(0));
        assert_eq!(delta_metas[0].max_row_id(), Some(9));
        let entries = Manifest::read(
            &file_io,
            &format!("{table_path}/manifest/{}", delta_metas[0].file_name()),
        )
        .await
        .unwrap();
        let data = entries
            .iter()
            .find(|entry| entry.file().file_name == "data-0.parquet")
            .unwrap();
        let vector = entries
            .iter()
            .find(|entry| entry.file().file_name == "data-0.vector.vortex")
            .unwrap();
        assert_eq!(data.file().first_row_id, Some(0));
        assert_eq!(vector.file().first_row_id, Some(0));
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
    async fn test_check_from_snapshot_rejects_concurrent_same_column_update() {
        let file_io = test_file_io();
        let table_path = "memory:/test_check_from_snapshot_same_column";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_data_evolution_commit(&file_io, table_path);
        let partition = EMPTY_SERIALIZED_ROW.clone();
        let mut initial_file = test_data_file("data-0.parquet", 100);
        initial_file.file_source = Some(0);
        commit
            .commit(vec![CommitMessage::new(
                partition.clone(),
                0,
                vec![initial_file],
            )])
            .await
            .unwrap();
        let snapshot = latest_snapshot(&file_io, table_path).await.unwrap();
        let entries = active_entries(&file_io, table_path, &snapshot).await;
        assert_eq!(entries[0].file().first_row_id, Some(0));

        let mut first_partial = test_data_file("partial-name-a.parquet", 100);
        first_partial.first_row_id = Some(0);
        first_partial.file_source = Some(0);
        first_partial.write_cols = Some(vec!["name".to_string()]);
        let mut first_message = CommitMessage::new(partition.clone(), 0, vec![first_partial]);
        first_message.check_from_snapshot = Some(1);
        commit.commit(vec![first_message]).await.unwrap();

        let mut second_partial = test_data_file("partial-name-b.parquet", 100);
        second_partial.first_row_id = Some(0);
        second_partial.file_source = Some(0);
        second_partial.write_cols = Some(vec!["name".to_string()]);
        let mut second_message = CommitMessage::new(partition, 0, vec![second_partial]);
        second_message.check_from_snapshot = Some(1);

        let result = commit.commit(vec![second_message]).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("multiple MERGE INTO operations have encountered conflicts"),
            "expected row-id/column conflict, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_check_from_snapshot_allows_concurrent_different_column_update() {
        let file_io = test_file_io();
        let table_path = "memory:/test_check_from_snapshot_different_column";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_data_evolution_commit(&file_io, table_path);
        let partition = EMPTY_SERIALIZED_ROW.clone();
        let mut initial_file = test_data_file("data-0.parquet", 100);
        initial_file.file_source = Some(0);
        commit
            .commit(vec![CommitMessage::new(
                partition.clone(),
                0,
                vec![initial_file],
            )])
            .await
            .unwrap();
        let snapshot = latest_snapshot(&file_io, table_path).await.unwrap();
        let entries = active_entries(&file_io, table_path, &snapshot).await;
        assert_eq!(entries[0].file().first_row_id, Some(0));

        let mut name_partial = test_data_file("partial-name.parquet", 100);
        name_partial.first_row_id = Some(0);
        name_partial.file_source = Some(0);
        name_partial.write_cols = Some(vec!["name".to_string()]);
        let mut name_message = CommitMessage::new(partition.clone(), 0, vec![name_partial]);
        name_message.check_from_snapshot = Some(1);
        commit.commit(vec![name_message]).await.unwrap();

        let mut id_partial = test_data_file("partial-id.parquet", 100);
        id_partial.first_row_id = Some(0);
        id_partial.file_source = Some(0);
        id_partial.write_cols = Some(vec!["id".to_string()]);
        let mut id_message = CommitMessage::new(partition, 0, vec![id_partial]);
        id_message.check_from_snapshot = Some(1);

        commit.commit(vec![id_message]).await.unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.id(), 3);
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
    async fn test_static_overwrite_default_partition_name_treated_as_null() {
        let file_io = test_file_io();
        let table_path = "memory:/test_static_overwrite_default_partition";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_partitioned_commit(&file_io, table_path);
        commit
            .commit(vec![
                CommitMessage::new(
                    partition_bytes("a"),
                    0,
                    vec![test_data_file("data-a.parquet", 100)],
                ),
                CommitMessage::new(
                    null_partition_bytes(),
                    0,
                    vec![test_data_file("data-null.parquet", 300)],
                ),
            ])
            .await
            .unwrap();

        commit
            .overwrite(
                vec![CommitMessage::new(
                    null_partition_bytes(),
                    0,
                    vec![test_data_file("data-null2.parquet", 50)],
                )],
                Some(HashMap::from([(
                    "pt".to_string(),
                    Some(Datum::String("__DEFAULT_PARTITION__".to_string())),
                )])),
            )
            .await
            .unwrap();

        let snapshot = latest_snapshot(&file_io, table_path).await.unwrap();
        assert_eq!(snapshot.id(), 2);
        assert_eq!(snapshot.total_record_count(), Some(150));
        let active_file_names = active_entries(&file_io, table_path, &snapshot)
            .await
            .into_iter()
            .map(|entry| entry.file().file_name.clone())
            .collect::<HashSet<_>>();
        assert_eq!(
            active_file_names,
            HashSet::from([
                "data-a.parquet".to_string(),
                "data-null2.parquet".to_string()
            ])
        );
    }

    #[tokio::test]
    async fn test_static_overwrite_rejects_mismatched_message_partition() {
        let file_io = test_file_io();
        let table_path = "memory:/test_static_overwrite_mismatch";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_partitioned_commit(&file_io, table_path);
        let result = commit
            .overwrite(
                vec![CommitMessage::new(
                    partition_bytes("b"),
                    0,
                    vec![test_data_file("data-b.parquet", 100)],
                )],
                Some(HashMap::from([(
                    "pt".to_string(),
                    Some(Datum::String("a".to_string())),
                )])),
            )
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("does not belong to this partition"));
    }

    #[tokio::test]
    async fn test_overwrite_ignores_changelog_files() {
        let file_io = test_file_io();
        let table_path = "memory:/test_overwrite_changelog_files";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);
        let mut message = CommitMessage::new(vec![], 0, vec![test_data_file("data.parquet", 1)]);
        message.new_changelog_files = vec![test_data_file("changelog.parquet", 1)];

        commit.overwrite(vec![message], None).await.unwrap();

        let snap_manager = SnapshotManager::new(file_io, table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.commit_kind(), &CommitKind::OVERWRITE);
        assert_eq!(snapshot.total_record_count(), Some(1));
        assert_eq!(snapshot.changelog_record_count(), None);
        assert_eq!(snapshot.changelog_manifest_list(), None);
    }

    #[tokio::test]
    async fn test_commit_writes_changelog_manifest_list_size() {
        let file_io = test_file_io();
        let table_path = "memory:/test_changelog_manifest_list_size";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);
        let mut message = CommitMessage::new(vec![], 0, vec![test_data_file("data.parquet", 10)]);
        message.new_changelog_files = vec![test_data_file("changelog.parquet", 3)];
        commit.commit(vec![message]).await.unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.changelog_record_count(), Some(3));
        assert!(snapshot.changelog_manifest_list().is_some());
        assert!(snapshot.changelog_manifest_list_size().unwrap() > 0);
    }

    #[tokio::test]
    async fn test_abort_deletes_new_data_and_changelog_files() {
        let file_io = test_file_io();
        let table_path = "memory:/test_abort_cleanup";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);
        let bucket_dir = format!("{table_path}/bucket-0");
        file_io.mkdirs(&format!("{bucket_dir}/")).await.unwrap();

        let mut data_file = test_data_file("data.parquet", 10);
        data_file.extra_files = vec!["data.parquet.index".to_string()];
        let changelog_file = test_data_file("changelog.parquet", 3);
        for name in ["data.parquet", "data.parquet.index", "changelog.parquet"] {
            file_io
                .new_output(&format!("{bucket_dir}/{name}"))
                .unwrap()
                .write(bytes::Bytes::from_static(b"x"))
                .await
                .unwrap();
        }

        let mut message = CommitMessage::new(vec![], 0, vec![data_file]);
        message.new_changelog_files = vec![changelog_file];
        commit.abort(&[message]).await.unwrap();

        assert!(!file_io
            .exists(&format!("{bucket_dir}/data.parquet"))
            .await
            .unwrap());
        assert!(!file_io
            .exists(&format!("{bucket_dir}/data.parquet.index"))
            .await
            .unwrap());
        assert!(!file_io
            .exists(&format!("{bucket_dir}/changelog.parquet"))
            .await
            .unwrap());
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

    /// Regression: a non-partitioned table (e.g. `CREATE TABLE test_pk (... PRIMARY KEY ...)`)
    /// must still emit `_PARTITION_STATS._MIN_VALUES`/`_MAX_VALUES` carrying the 4-byte BE
    /// arity prefix; otherwise Java readers like Spark/Flink hit
    /// `BufferUnderflowException` inside `SerializationUtils.deserializeBinaryRow`.
    #[test]
    fn compute_partition_stats_no_partition_fields_returns_decodable_empty() {
        let file_io = test_file_io();
        let commit = setup_commit(&file_io, "memory:/test_no_partition_stats");

        let entry = ManifestEntry::new(
            FileKind::Add,
            vec![],
            0,
            1,
            test_data_file("data-0.parquet", 1),
            2,
        );

        let stats = commit.compute_partition_stats(&[entry]).unwrap();
        BinaryRow::from_serialized_bytes(stats.min_values())
            .expect("min_values must decode via the same protocol as Java's deserializeBinaryRow");
        BinaryRow::from_serialized_bytes(stats.max_values())
            .expect("max_values must decode via the same protocol as Java's deserializeBinaryRow");
        assert!(stats.null_counts().is_empty());
    }

    /// Regression: when there are no entries at all, the empty stats we return must also
    /// satisfy the protocol — same Java reader path runs on it.
    #[test]
    fn compute_partition_stats_empty_entries_returns_decodable_empty() {
        let file_io = test_file_io();
        let commit = setup_partitioned_commit(&file_io, "memory:/test_no_entries_stats");

        let stats = commit.compute_partition_stats(&[]).unwrap();
        BinaryRow::from_serialized_bytes(stats.min_values()).unwrap();
        BinaryRow::from_serialized_bytes(stats.max_values()).unwrap();
        assert!(stats.null_counts().is_empty());
    }

    /// Regression: partitioned table with an all-null partition row must still emit
    /// decodable min/max bytes (otherwise Java hits `BufferUnderflowException`).
    #[test]
    fn compute_partition_stats_all_null_partition_values_returns_decodable_bytes() {
        let file_io = test_file_io();
        let commit = setup_partitioned_commit(&file_io, "memory:/test_all_null_partition_stats");

        let mut builder = BinaryRowBuilder::new(1);
        builder.set_null_at(0);
        let null_partition = builder.build_serialized();

        let entry = ManifestEntry::new(
            FileKind::Add,
            null_partition,
            0,
            1,
            test_data_file("data-null-pt.parquet", 1),
            2,
        );

        let stats = commit.compute_partition_stats(&[entry]).unwrap();
        let min_row = BinaryRow::from_serialized_bytes(stats.min_values()).unwrap();
        let max_row = BinaryRow::from_serialized_bytes(stats.max_values()).unwrap();
        assert_eq!(min_row.arity(), 1);
        assert_eq!(max_row.arity(), 1);
        assert!(min_row.is_null_at(0));
        assert!(max_row.is_null_at(0));
        assert_eq!(stats.null_counts(), &vec![Some(1)]);
    }

    #[tokio::test]
    async fn test_manifest_files_roll_by_target_size_and_preserve_entries() {
        let file_io = test_file_io();
        let table_path = "memory:/test_manifest_rolling";
        setup_dirs(&file_io, table_path).await;

        let table = test_table_with_options(
            &file_io,
            table_path,
            HashMap::from([("manifest.target-file-size".to_string(), "1 kb".to_string())]),
        );
        let commit = TableCommit::new(table, "test-user".to_string());

        let messages = (0..2500)
            .map(|i| {
                let mut file = test_data_file(&format!("data-{i:04}.parquet"), 1);
                file.extra_files = (0..8).map(|j| format!("data-{i:04}-{j}.idx")).collect();
                CommitMessage::new(vec![], 0, vec![file])
            })
            .collect::<Vec<_>>();
        commit.commit(messages).await.unwrap();

        let snapshot = latest_snapshot(&file_io, table_path).await.unwrap();
        let manifest_dir = format!("{table_path}/manifest");
        let delta_manifest_list_path = format!("{manifest_dir}/{}", snapshot.delta_manifest_list());
        let delta_manifest_list_bytes = file_io
            .new_input(&delta_manifest_list_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        assert!(
            contains_bytes(&delta_manifest_list_bytes, b"zstandard"),
            "manifest lists should use the default zstd Avro codec"
        );
        let delta_metas = ManifestList::read(&file_io, &delta_manifest_list_path)
            .await
            .unwrap();
        assert!(
            delta_metas.len() > 1,
            "small manifest target should roll into multiple manifest files"
        );
        assert_eq!(
            delta_metas
                .iter()
                .map(|meta| meta.num_added_files() + meta.num_deleted_files())
                .sum::<i64>(),
            2500
        );
        for meta in &delta_metas[..delta_metas.len() - 1] {
            assert!(
                meta.file_size() >= 1024,
                "rolled manifest files should not be smaller than target"
            );
        }

        let mut file_names = HashSet::new();
        for meta in &delta_metas {
            let manifest_path = format!("{manifest_dir}/{}", meta.file_name());
            let manifest_bytes = file_io
                .new_input(&manifest_path)
                .unwrap()
                .read()
                .await
                .unwrap();
            assert!(
                contains_bytes(&manifest_bytes, b"zstandard"),
                "manifest files should use the default zstd Avro codec"
            );
            assert_eq!(
                file_io.get_status(&manifest_path).await.unwrap().size as i64,
                meta.file_size()
            );
            let entries = Manifest::read(&file_io, &manifest_path).await.unwrap();
            assert_eq!(
                entries.len() as i64,
                meta.num_added_files() + meta.num_deleted_files()
            );
            for entry in entries {
                file_names.insert(entry.file().file_name.clone());
            }
        }
        assert_eq!(file_names.len(), 2500);
        assert!(file_names.contains("data-0000.parquet"));
        assert!(file_names.contains("data-2499.parquet"));
    }

    #[tokio::test]
    async fn test_manifest_rolling_waits_for_java_check_cadence() {
        let file_io = test_file_io();
        let table_path = "memory:/test_manifest_rolling_cadence";
        setup_dirs(&file_io, table_path).await;

        let table = test_table_with_options(
            &file_io,
            table_path,
            HashMap::from([("manifest.target-file-size".to_string(), "1 kb".to_string())]),
        );
        let commit = TableCommit::new(table, "test-user".to_string());

        let messages = (0..80)
            .map(|i| {
                let mut file = test_data_file(&format!("data-{i:03}.parquet"), 1);
                file.extra_files = (0..8).map(|j| format!("data-{i:03}-{j}.idx")).collect();
                CommitMessage::new(vec![], 0, vec![file])
            })
            .collect::<Vec<_>>();
        commit.commit(messages).await.unwrap();

        let snapshot = latest_snapshot(&file_io, table_path).await.unwrap();
        let manifest_dir = format!("{table_path}/manifest");
        let delta_metas = ManifestList::read(
            &file_io,
            &format!("{manifest_dir}/{}", snapshot.delta_manifest_list()),
        )
        .await
        .unwrap();
        assert_eq!(
            delta_metas.len(),
            1,
            "manifest rolling should not check before Java's 1000-record cadence"
        );
    }

    fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    #[tokio::test]
    async fn test_minor_compaction_nets_add_delete_manifest_entries() {
        let file_io = test_file_io();
        let table_path = "memory:/test_minor_manifest_compaction";
        setup_dirs(&file_io, table_path).await;

        let table = test_table_with_options(
            &file_io,
            table_path,
            HashMap::from([("manifest.merge-min-count".to_string(), "2".to_string())]),
        );
        let commit = TableCommit::new(table, "test-user".to_string());

        commit
            .commit(vec![CommitMessage::new(
                vec![],
                0,
                vec![test_data_file("data-0.parquet", 100)],
            )])
            .await
            .unwrap();

        commit
            .overwrite(
                vec![CommitMessage::new(
                    vec![],
                    0,
                    vec![test_data_file("data-1.parquet", 50)],
                )],
                None,
            )
            .await
            .unwrap();

        commit
            .commit(vec![CommitMessage::new(
                vec![],
                0,
                vec![test_data_file("data-2.parquet", 25)],
            )])
            .await
            .unwrap();

        let snapshot = latest_snapshot(&file_io, table_path).await.unwrap();
        assert_eq!(snapshot.id(), 3);
        let manifest_dir = format!("{table_path}/manifest");
        let base_metas = ManifestList::read(
            &file_io,
            &format!("{manifest_dir}/{}", snapshot.base_manifest_list()),
        )
        .await
        .unwrap();
        assert_eq!(
            base_metas.len(),
            1,
            "two previous manifest files should be minor-compacted into one base manifest"
        );

        let base_entries = Manifest::read(
            &file_io,
            &format!("{manifest_dir}/{}", base_metas[0].file_name()),
        )
        .await
        .unwrap();
        assert_eq!(base_entries.len(), 1);
        assert_eq!(*base_entries[0].kind(), FileKind::Add);
        assert_eq!(base_entries[0].file().file_name, "data-1.parquet");

        let active_file_names = active_entries(&file_io, table_path, &snapshot)
            .await
            .into_iter()
            .map(|entry| entry.file().file_name.clone())
            .collect::<HashSet<_>>();
        assert_eq!(
            active_file_names,
            HashSet::from(["data-1.parquet".to_string(), "data-2.parquet".to_string()])
        );
    }

    /// `write_manifest_file` must aggregate min/max bucket and level across entries so the
    /// Java reader can prune manifests by bucket / level (see apache/paimon#5345). This
    /// drives a real commit so all the call-site plumbing is exercised end to end.
    #[tokio::test]
    async fn test_commit_writes_bucket_and_level_stats_into_manifest_list() {
        let file_io = test_file_io();
        let table_path = "memory:/test_commit_bucket_level_stats";
        setup_dirs(&file_io, table_path).await;

        let commit = setup_commit(&file_io, table_path);

        fn data_file_at_level(name: &str, level: i32) -> DataFileMeta {
            let mut f = test_data_file(name, 1);
            f.level = level;
            f
        }

        // Two commit messages on different buckets, each carrying a file at a different
        // level. Expected aggregate: bucket [0, 3], level [0, 2].
        let messages = vec![
            CommitMessage::new(vec![], 0, vec![data_file_at_level("data-b0.parquet", 0)]),
            CommitMessage::new(vec![], 3, vec![data_file_at_level("data-b3.parquet", 2)]),
        ];
        commit.commit(messages).await.unwrap();

        let snap_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snap_manager.get_latest_snapshot().await.unwrap().unwrap();
        let delta_path = format!("{table_path}/manifest/{}", snapshot.delta_manifest_list());
        let metas = ManifestList::read(&file_io, &delta_path).await.unwrap();
        assert_eq!(
            metas.len(),
            1,
            "expected a single manifest covering both entries"
        );
        assert_eq!(metas[0].min_bucket(), Some(0));
        assert_eq!(metas[0].max_bucket(), Some(3));
        assert_eq!(metas[0].min_level(), Some(0));
        assert_eq!(metas[0].max_level(), Some(2));
    }
}
