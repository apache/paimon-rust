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

//! Orphan file cleanup.
//!
//! Reference: Java `OrphanFilesClean` and `LocalOrphanFilesClean`.
//!
//! A file is an orphan when it sits in a directory Paimon writes to (the
//! manifest, index and statistics directories, bucket directories and data
//! file external paths), is older than `older_than`, and no snapshot, tag or
//! long-lived changelog of any branch references it. Temporary files left in
//! snapshot and changelog directories by interrupted commits are removed too.
//!
//! `older_than` defaults to one day ago and must be in the past, so files that
//! an in-flight write has created but not yet committed are never candidates.
//! Unlike Java, which reads a missing manifest as empty, a missing manifest of
//! a snapshot that still exists aborts the run: its data files would otherwise
//! look unreferenced and be deleted.

use crate::io::{FileIO, FileStatus};
use crate::spec::{IndexManifest, Manifest, ManifestList, Snapshot};
use crate::table::snapshot_deletion::reassign_plan_file;
use crate::table::{BranchManager, SnapshotManager, Table};
use crate::{Error, Result};
use futures::{stream, StreamExt, TryStreamExt};
use std::collections::{BTreeMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_OLDER_THAN_MS: i64 = 24 * 60 * 60 * 1000;
const FILE_OPERATION_CONCURRENCY: usize = 16;
const MAIN_BRANCH: &str = "main";
const SNAPSHOT_PREFIX: &str = "snapshot-";
const CHANGELOG_PREFIX: &str = "changelog-";
const EARLIEST: &str = "EARLIEST";
const LATEST: &str = "LATEST";
const BUCKET_PREFIX: &str = "bucket-";
/// Java `ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX`: never cleaned here.
const MANAGED_BLOB_SUFFIX: &str = ".managed.blob";
const DATA_FILE_EXTERNAL_PATHS_OPTION: &str = "data-file.external-paths";

/// Files removed (or, in a dry run, that would be removed) by a cleanup.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrphanFilesCleanResult {
    pub deleted_file_count: u64,
    pub deleted_file_total_bytes: u64,
    /// Full paths, sorted.
    pub deleted_files: Vec<String>,
}

/// Remove files that no snapshot, tag or changelog of the table references.
pub struct RemoveOrphanFiles<'a> {
    table: &'a Table,
    older_than_millis: Option<i64>,
    dry_run: bool,
    parallelism: usize,
    current_time_millis: Option<i64>,
}

impl<'a> RemoveOrphanFiles<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            older_than_millis: None,
            dry_run: false,
            parallelism: FILE_OPERATION_CONCURRENCY,
            current_time_millis: None,
        }
    }

    /// Maximum number of concurrent manifest reads and file deletions.
    pub fn with_parallelism(&mut self, parallelism: usize) -> &mut Self {
        self.parallelism = parallelism.max(1);
        self
    }

    /// Only files last modified before this epoch-millisecond timestamp are
    /// candidates. Defaults to one day ago; must be in the past.
    pub fn with_older_than_millis(&mut self, older_than_millis: i64) -> &mut Self {
        self.older_than_millis = Some(older_than_millis);
        self
    }

    /// Report the orphan files without deleting them.
    pub fn with_dry_run(&mut self, dry_run: bool) -> &mut Self {
        self.dry_run = dry_run;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_current_time_millis(&mut self, current_time_millis: i64) -> &mut Self {
        self.current_time_millis = Some(current_time_millis);
        self
    }

    pub async fn execute(&self) -> Result<OrphanFilesCleanResult> {
        self.table.ensure_not_branch_reference_for_write()?;
        let now = self.current_time_millis.unwrap_or_else(current_time_millis);
        let older_than = match self.older_than_millis {
            None => now - DEFAULT_OLDER_THAN_MS,
            Some(value) if value < now => value,
            Some(_) => {
                return Err(Error::DataInvalid {
                    message: "older_than must be earlier than now; files being written and not \
                              yet referenced by a snapshot would be deleted"
                        .to_string(),
                    source: None,
                })
            }
        };
        let clean = Clean {
            file_io: self.table.file_io().clone(),
            table_location: self.table.location().trim_end_matches('/').to_string(),
            older_than,
        };

        let branches = self.valid_branches().await?;
        let mut deleted = BTreeMap::new();
        for branch in &branches {
            deleted.extend(clean.non_snapshot_files(&self.branch_root(branch)).await?);
        }

        let candidates = clean
            .candidate_files(
                self.table.schema().partition_keys().len(),
                self.table
                    .schema()
                    .options()
                    .get(DATA_FILE_EXTERNAL_PATHS_OPTION),
            )
            .await?;
        if !candidates.is_empty() {
            let mut used = HashSet::new();
            for branch in &branches {
                self.collect_used_files(&clean, branch, &mut used).await?;
            }
            for (path, size) in candidates {
                if !used.contains(file_name(&path)) {
                    deleted.insert(path, size);
                }
            }
        }

        if !self.dry_run {
            let deletes = deleted
                .keys()
                .map(|path| clean.delete_quietly(path))
                .collect::<Vec<_>>();
            stream::iter(deletes)
                .buffer_unordered(self.parallelism)
                .collect::<Vec<_>>()
                .await;
        }
        Ok(OrphanFilesCleanResult {
            deleted_file_count: deleted.len() as u64,
            deleted_file_total_bytes: deleted.values().sum(),
            deleted_files: deleted.into_keys().collect(),
        })
    }

    /// Every branch plus main. A branch without a schema aborts the run, as in
    /// Java: its files cannot be accounted for.
    async fn valid_branches(&self) -> Result<Vec<String>> {
        let branch_manager = BranchManager::new(
            self.table.file_io().clone(),
            self.table.location().to_string(),
        );
        let branches = branch_manager.list_all().await?;
        let mut abnormal = Vec::new();
        for branch in &branches {
            let schema_manager = self.table.schema_manager().with_branch(branch);
            if schema_manager.latest().await?.is_none() {
                abnormal.push(branch.clone());
            }
        }
        if !abnormal.is_empty() {
            return Err(Error::DataInvalid {
                message: format!(
                    "Branches {abnormal:?} have no schemas. Orphan files cleaning aborted. \
                     Please check these branches manually."
                ),
                source: None,
            });
        }
        let mut all = branches;
        all.push(MAIN_BRANCH.to_string());
        Ok(all)
    }

    fn branch_root(&self, branch: &str) -> String {
        if branch == MAIN_BRANCH {
            self.table.location().trim_end_matches('/').to_string()
        } else {
            BranchManager::new(
                self.table.file_io().clone(),
                self.table.location().to_string(),
            )
            .branch_path(branch)
        }
    }

    /// Names of every file a snapshot, tag or long-lived changelog of `branch`
    /// references. Java `LocalOrphanFilesClean#getUsedFiles`.
    async fn collect_used_files(
        &self,
        clean: &Clean,
        branch: &str,
        used: &mut HashSet<String>,
    ) -> Result<()> {
        let snapshot_manager = self.table.snapshot_manager().with_branch(branch);
        let mut owners = Vec::new();
        for id in snapshot_manager.list_all_ids().await? {
            if let Some(snapshot) = snapshot_manager.try_get_snapshot(id).await? {
                owners.push(Owner {
                    file: Some(snapshot_manager.snapshot_path(id)),
                    snapshot,
                });
            }
        }
        let tag_manager = if branch == MAIN_BRANCH {
            self.table.tag_manager()
        } else {
            self.table.tag_manager().with_branch(branch)
        };
        for (_, snapshot) in tag_manager.list_all().await? {
            owners.push(Owner {
                file: None,
                snapshot,
            });
        }
        owners.extend(clean.changelogs(&self.branch_root(branch)).await?);

        let mut manifests = HashSet::new();
        for owner in &owners {
            if let Some(files) = clean.metadata_files(&snapshot_manager, owner).await? {
                used.extend(files.names);
                manifests.extend(files.manifests);
            }
        }
        let paths = manifests
            .iter()
            .map(|name| snapshot_manager.manifest_path(name))
            .collect::<Vec<_>>();
        let reads = paths
            .iter()
            .map(|path| Manifest::read(&clean.file_io, path))
            .collect::<Vec<_>>();
        let entries = stream::iter(reads)
            .buffer_unordered(self.parallelism)
            .try_collect::<Vec<_>>()
            .await?;
        for entry in entries.into_iter().flatten() {
            used.insert(entry.file().file_name.clone());
            used.extend(entry.file().extra_files.iter().cloned());
        }
        Ok(())
    }
}

/// A snapshot-like object that references files, and the file that makes it
/// live (`None` for a tag, whose manifests must always be readable).
struct Owner {
    file: Option<String>,
    snapshot: Snapshot,
}

struct MetadataFiles {
    names: Vec<String>,
    manifests: Vec<String>,
}

struct Clean {
    file_io: FileIO,
    table_location: String,
    older_than: i64,
}

impl Clean {
    fn old_enough(&self, status: &FileStatus) -> bool {
        // A store that reports no modification time never exposes a file to deletion.
        status
            .last_modified
            .is_some_and(|modified| modified.timestamp_millis() < self.older_than)
    }

    /// Files in `dir`, or nothing when it does not exist.
    async fn list(&self, dir: &str) -> Result<Vec<FileStatus>> {
        if !self.file_io.exists_dir(dir).await? {
            return Ok(Vec::new());
        }
        self.file_io.list_status(dir).await
    }

    /// Old non-snapshot files in the snapshot and changelog directories, such
    /// as temporary files of interrupted commits. Java `cleanBranchSnapshotDir`.
    async fn non_snapshot_files(&self, branch_root: &str) -> Result<Vec<(String, u64)>> {
        let mut files = Vec::new();
        for (dir, prefix) in [
            ("snapshot", SNAPSHOT_PREFIX),
            ("changelog", CHANGELOG_PREFIX),
        ] {
            for status in self.list(&format!("{branch_root}/{dir}")).await? {
                let name = file_name(&status.path);
                if !status.is_dir
                    && !name.starts_with(prefix)
                    && name != EARLIEST
                    && name != LATEST
                    && self.old_enough(&status)
                {
                    files.push((status.path, status.size));
                }
            }
        }
        Ok(files)
    }

    /// Old files in the directories Paimon writes data and metadata to, keyed
    /// by full path. Java `getCandidateDeletingFiles`.
    async fn candidate_files(
        &self,
        partition_depth: usize,
        external_paths: Option<&String>,
    ) -> Result<BTreeMap<String, u64>> {
        let mut dirs = ["manifest", "index", "statistics"]
            .map(|dir| format!("{}/{dir}", self.table_location))
            .to_vec();
        dirs.extend(
            self.bucket_dirs(&self.table_location, partition_depth)
                .await?,
        );
        for external in external_paths
            .into_iter()
            .flat_map(|paths| paths.split(','))
            .map(str::trim)
            .filter(|path| !path.is_empty())
        {
            dirs.extend(
                self.bucket_dirs(external.trim_end_matches('/'), partition_depth)
                    .await?,
            );
        }

        let mut candidates = BTreeMap::new();
        for dir in dirs {
            for status in self.list(&dir).await? {
                if !status.is_dir
                    && !status.path.ends_with(MANAGED_BLOB_SUFFIX)
                    && self.old_enough(&status)
                {
                    candidates.insert(status.path, status.size);
                }
            }
        }
        Ok(candidates)
    }

    /// Bucket directories under `root`, descending through `partition_depth`
    /// levels of `key=value` partition directories. Java `listFileDirs`.
    async fn bucket_dirs(&self, root: &str, partition_depth: usize) -> Result<Vec<String>> {
        let mut level = vec![root.to_string()];
        for _ in 0..partition_depth {
            let mut next = Vec::new();
            for dir in &level {
                for status in self.list(dir).await? {
                    if status.is_dir && file_name(&status.path).contains('=') {
                        next.push(status.path.trim_end_matches('/').to_string());
                    }
                }
            }
            level = next;
        }
        let mut buckets = Vec::new();
        for dir in &level {
            for status in self.list(dir).await? {
                if status.is_dir && file_name(&status.path).starts_with(BUCKET_PREFIX) {
                    buckets.push(status.path.trim_end_matches('/').to_string());
                }
            }
        }
        Ok(buckets)
    }

    /// Long-lived changelogs, which Java keeps in `changelog/changelog-<id>`
    /// with the snapshot JSON format.
    async fn changelogs(&self, branch_root: &str) -> Result<Vec<Owner>> {
        let mut owners = Vec::new();
        for status in self.list(&format!("{branch_root}/changelog")).await? {
            let name = file_name(&status.path);
            if status.is_dir
                || !name.starts_with(CHANGELOG_PREFIX)
                || name[CHANGELOG_PREFIX.len()..].parse::<i64>().is_err()
            {
                continue;
            }
            let bytes = match self.file_io.new_input(&status.path)?.read().await {
                Ok(bytes) => bytes,
                Err(error) if is_not_found(&error) => continue,
                Err(error) => return Err(error),
            };
            let snapshot =
                serde_json::from_slice::<Snapshot>(&bytes).map_err(|e| Error::DataInvalid {
                    message: format!("changelog {} JSON invalid: {e}", status.path),
                    source: Some(Box::new(e)),
                })?;
            owners.push(Owner {
                file: Some(status.path),
                snapshot,
            });
        }
        Ok(owners)
    }

    /// Manifest-type files `owner` references, or `None` when it vanished
    /// while being read (a concurrently expired snapshot or changelog).
    /// Java `collectWithoutDataFileWithManifestFlag`.
    async fn metadata_files(
        &self,
        snapshot_manager: &SnapshotManager,
        owner: &Owner,
    ) -> Result<Option<MetadataFiles>> {
        let snapshot = &owner.snapshot;
        let mut names = Vec::new();
        let mut manifests = Vec::new();
        let mut lists = vec![
            snapshot.base_manifest_list().to_string(),
            snapshot.delta_manifest_list().to_string(),
        ];
        lists.extend(snapshot.changelog_manifest_list().map(str::to_string));
        for list in lists {
            let path = snapshot_manager.manifest_path(&list);
            let metas = match ManifestList::read(&self.file_io, &path).await {
                Ok(metas) => metas,
                Err(error) if is_not_found(&error) => {
                    return self.missing(owner, &path).await;
                }
                Err(error) => return Err(error),
            };
            for meta in metas {
                manifests.push(meta.file_name().to_string());
                names.extend(meta.extra_files().unwrap_or_default().iter().cloned());
            }
            names.push(list);
        }
        if let Some(index_manifest) = snapshot.index_manifest() {
            let path = snapshot_manager.manifest_path(index_manifest);
            match IndexManifest::read(&self.file_io, &path).await {
                Ok(entries) => {
                    names.extend(entries.into_iter().map(|entry| entry.index_file.file_name));
                    names.push(index_manifest.to_string());
                }
                Err(error) if is_not_found(&error) => return self.missing(owner, &path).await,
                Err(error) => return Err(error),
            }
        }
        if let Some(plan) = reassign_plan_file(snapshot) {
            names.push(plan.to_string());
        }
        if let Some(statistics) = snapshot.statistics() {
            names.push(statistics.to_string());
        }
        names.extend(manifests.iter().cloned());
        Ok(Some(MetadataFiles { names, manifests }))
    }

    /// A referenced metadata file is missing. Fine if its owner is gone too;
    /// otherwise the table is inconsistent and nothing can be deleted safely.
    async fn missing(&self, owner: &Owner, path: &str) -> Result<Option<MetadataFiles>> {
        if let Some(file) = &owner.file {
            if !self.file_io.exists(file).await? {
                return Ok(None);
            }
        }
        Err(Error::DataInvalid {
            message: format!(
                "Snapshot {} references missing file {path}; orphan files cleaning aborted",
                owner.snapshot.id()
            ),
            source: None,
        })
    }

    async fn delete_quietly(&self, path: &str) {
        if let Err(error) = self.file_io.delete_file(path).await {
            log::warn!("Failed to delete orphan file {path}: {error}");
        }
    }
}

fn file_name(path: &str) -> &str {
    path.trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(path)
}

fn is_not_found(error: &Error) -> bool {
    matches!(error, Error::IoUnexpected { source, .. } if source.kind() == opendal::ErrorKind::NotFound)
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
