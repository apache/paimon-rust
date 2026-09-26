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

//! Planning and deleting the files of expired snapshots.
//!
//! Reference: Java `FileDeletionBase` and `SnapshotDeletion`.
//!
//! Every read failure makes the plan delete less, never more: an unreadable
//! delta manifest cancels that snapshot's data-file deletion, and a skipping
//! set that cannot be built cancels manifest deletion. A file left behind is
//! an orphan that orphan-file cleanup can remove later; a referenced file
//! deleted by mistake is data loss.

use crate::io::FileIO;
use crate::spec::{
    bucket_path, BinaryRow, CoreOptions, FileKind, IndexManifest, Manifest, ManifestEntry,
    ManifestFileMeta, ManifestList, PartitionComputer, Snapshot,
};
use crate::table::index_file_path::committed_index_file_path;
use crate::table::{SnapshotManager, Table};
use crate::Result;
use futures::{stream, StreamExt, TryStreamExt};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

const FILE_OPERATION_CONCURRENCY: usize = 16;
const STATISTICS_DIR: &str = "statistics";
/// Java `SerializationAssignment.PLAN_FILE_PROPERTY`.
const REASSIGN_PLAN_FILE_PROPERTY: &str = "row-id-reassign.plan";
/// Java `SerializationAssignment.REASSIGN_SNAPSHOT_ID`.
const REASSIGN_SNAPSHOT_ID_PROPERTY: &str = "reassign-snapshot-id";

/// A data file within a bucket: `(partition, bucket, file name)`. File names
/// are unique within a bucket, so this is how Java's `containsDataFile`
/// matches entries across snapshots.
pub(crate) type DataFileKey = (Vec<u8>, i32, String);

/// Data files that a snapshot's delta manifests delete, with the physical
/// paths (data file plus extra files) to remove for each.
#[derive(Debug, Default)]
pub(crate) struct DataFileDeletionPlan {
    candidates: Vec<(DataFileKey, Vec<String>)>,
}

impl DataFileDeletionPlan {
    /// Paths of candidates that `retained` does not protect.
    pub(crate) fn paths_to_delete(&self, retained: Option<&HashSet<DataFileKey>>) -> Vec<String> {
        self.candidates
            .iter()
            .filter(|(key, _)| retained.is_none_or(|retained| !retained.contains(key)))
            .flat_map(|(_, paths)| paths.iter().cloned())
            .collect()
    }
}

pub(crate) struct SnapshotDeletion {
    file_io: FileIO,
    table_location: String,
    snapshot_manager: SnapshotManager,
    partition_computer: Option<PartitionComputer>,
    index_file_in_data_file_dir: bool,
}

impl SnapshotDeletion {
    pub(crate) fn new(table: &Table) -> Result<Self> {
        let schema = table.schema();
        let core_options = CoreOptions::new(schema.options());
        let partition_computer = if schema.partition_keys().is_empty() {
            None
        } else {
            Some(PartitionComputer::new(
                schema.partition_keys(),
                schema.fields(),
                core_options.partition_default_name(),
                core_options.legacy_partition_name(),
            )?)
        };
        Ok(Self {
            file_io: table.file_io().clone(),
            table_location: table.location().trim_end_matches('/').to_string(),
            snapshot_manager: table.snapshot_manager(),
            partition_computer,
            index_file_in_data_file_dir: core_options.index_file_in_data_file_dir(),
        })
    }

    fn bucket_path(&self, partition: &[u8], bucket: i32) -> Result<String> {
        let partition = match self.partition_computer {
            // An unpartitioned table never decodes its (empty) partition blob.
            None => BinaryRow::new(0),
            Some(_) => BinaryRow::from_serialized_bytes(partition)?,
        };
        bucket_path(
            &self.table_location,
            self.partition_computer.as_ref(),
            &partition,
            bucket,
        )
    }

    fn manifest_path(&self, name: &str) -> String {
        self.snapshot_manager.manifest_path(name)
    }

    async fn read_manifest_list(&self, name: &str) -> Result<Vec<ManifestFileMeta>> {
        ManifestList::read(&self.file_io, &self.manifest_path(name)).await
    }

    /// Entries of every manifest in `manifests`, in manifest order.
    async fn read_entries(&self, manifests: &[ManifestFileMeta]) -> Result<Vec<ManifestEntry>> {
        let paths = manifests
            .iter()
            .map(|manifest| self.manifest_path(manifest.file_name()))
            .collect::<Vec<_>>();
        let reads = paths
            .iter()
            .map(|path| Manifest::read(&self.file_io, path))
            .collect::<Vec<_>>();
        let entries = stream::iter(reads)
            .buffered(FILE_OPERATION_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(entries.into_iter().flatten().collect())
    }

    /// Data files deleted by `snapshot`'s delta manifests. Java
    /// `FileDeletionBase#planDeletedInDeltaManifest`.
    ///
    /// A file that the same delta also adds (a level upgrade) stays. Unlike
    /// Java, which relies on DELETE preceding ADD in the manifest, the check
    /// here does not depend on entry order. Any read failure yields an empty
    /// plan, cancelling this snapshot's data-file deletion.
    pub(crate) async fn plan_deleted_in_delta_manifest(
        &self,
        snapshot: &Snapshot,
    ) -> DataFileDeletionPlan {
        match self.try_plan_deleted_in_delta_manifest(snapshot).await {
            Ok(plan) => plan,
            Err(error) => {
                log::warn!(
                    "Failed to read the delta manifests of snapshot {}; skip deleting its data files: {error}",
                    snapshot.id()
                );
                DataFileDeletionPlan::default()
            }
        }
    }

    async fn try_plan_deleted_in_delta_manifest(
        &self,
        snapshot: &Snapshot,
    ) -> Result<DataFileDeletionPlan> {
        let manifests = self
            .read_manifest_list(snapshot.delta_manifest_list())
            .await?;
        let entries = self.read_entries(&manifests).await?;

        let mut added = HashSet::new();
        let mut deleted = HashMap::new();
        for entry in &entries {
            let key = data_file_key(entry);
            match entry.kind() {
                FileKind::Add => {
                    added.insert(key);
                }
                FileKind::Delete => {
                    if let Entry::Vacant(slot) = deleted.entry(key) {
                        let bucket_path = self.bucket_path(entry.partition(), entry.bucket())?;
                        slot.insert(entry.file().collect_files(&bucket_path));
                    }
                }
            }
        }
        let mut candidates = deleted
            .into_iter()
            .filter(|(key, _)| !added.contains(key))
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(DataFileDeletionPlan { candidates })
    }

    /// Data files live in `tag`, used to keep the files a tag still reads.
    /// Mirrors Java `createDataFileSkipperForTag`, which merges the tag's base
    /// and delta manifests like a scan does.
    pub(crate) async fn tag_data_files(&self, tag: &Snapshot) -> Result<HashSet<DataFileKey>> {
        let mut manifests = self.read_manifest_list(tag.base_manifest_list()).await?;
        manifests.extend(self.read_manifest_list(tag.delta_manifest_list()).await?);
        let entries = self.read_entries(&manifests).await?;

        // Java `FileEntry.mergeEntries`: the identifier includes the level so
        // that a level upgrade (DELETE at one level, ADD at another) never
        // cancels the surviving entry. A DELETE with no matching ADD stays in
        // the merged result, which only protects more files.
        let mut merged: HashMap<(Vec<u8>, i32, i32, String), FileKind> = HashMap::new();
        for entry in &entries {
            let identifier = (
                entry.partition().to_vec(),
                entry.bucket(),
                entry.file().level,
                entry.file().file_name.clone(),
            );
            match entry.kind() {
                FileKind::Add => {
                    merged.insert(identifier, FileKind::Add);
                }
                FileKind::Delete => {
                    if merged.remove(&identifier).is_none() {
                        merged.insert(identifier, FileKind::Delete);
                    }
                }
            }
        }
        Ok(merged
            .into_keys()
            .map(|(partition, bucket, _, file_name)| (partition, bucket, file_name))
            .collect())
    }

    /// Changelog files added by `snapshot`. Java
    /// `planAddedInChangelogManifest`: unreadable manifests are skipped, since
    /// these files are only ever deleted.
    pub(crate) async fn changelog_files(&self, snapshot: &Snapshot) -> Vec<String> {
        let Some(list) = snapshot.changelog_manifest_list() else {
            return Vec::new();
        };
        let manifests = match self.read_manifest_list(list).await {
            Ok(manifests) => manifests,
            Err(error) => {
                log::warn!("Failed to read changelog manifest list {list}: {error}");
                return Vec::new();
            }
        };
        let mut paths = Vec::new();
        for manifest in &manifests {
            let path = self.manifest_path(manifest.file_name());
            let entries = match Manifest::read(&self.file_io, &path).await {
                Ok(entries) => entries,
                Err(error) => {
                    log::info!("Failed to read changelog manifest {path}; ignore it: {error}");
                    continue;
                }
            };
            for entry in entries {
                if *entry.kind() != FileKind::Add {
                    continue;
                }
                match self.bucket_path(entry.partition(), entry.bucket()) {
                    Ok(bucket_path) => paths.push(entry.file().data_file_path(&bucket_path)),
                    Err(error) => log::warn!(
                        "Failed to resolve the bucket of changelog file {}: {error}",
                        entry.file().file_name
                    ),
                }
            }
        }
        paths
    }

    /// Names of every manifest-type file that `snapshots` still reference:
    /// manifest lists, manifests and their extra files, the index manifest and
    /// its index files, the statistics file, and the reassign plan. Java
    /// `FileDeletionBase#manifestSkippingSet`.
    pub(crate) async fn manifest_skipping_set(
        &self,
        snapshots: &[&Snapshot],
    ) -> Result<HashSet<String>> {
        let mut skipping = HashSet::new();
        for snapshot in snapshots {
            for list in [
                snapshot.base_manifest_list(),
                snapshot.delta_manifest_list(),
            ] {
                skipping.insert(list.to_string());
                for manifest in self.read_manifest_list(list).await? {
                    skipping.insert(manifest.file_name().to_string());
                    skipping.extend(manifest.extra_files().unwrap_or_default().iter().cloned());
                }
            }
            if let Some(index_manifest) = snapshot.index_manifest() {
                skipping.insert(index_manifest.to_string());
                let entries =
                    IndexManifest::read(&self.file_io, &self.manifest_path(index_manifest)).await?;
                skipping.extend(entries.into_iter().map(|entry| entry.index_file.file_name));
            }
            if let Some(plan_file) = reassign_plan_file(snapshot) {
                skipping.insert(plan_file.to_string());
            }
            if let Some(statistics) = snapshot.statistics() {
                skipping.insert(statistics.to_string());
            }
        }
        Ok(skipping)
    }

    /// Paths of `snapshot`'s manifest-type files that `skipping` does not
    /// protect. Every returned name is added to `skipping`, so a file shared by
    /// several expiring snapshots is returned once. Java
    /// `FileDeletionBase#planManifestsCleaner`.
    pub(crate) async fn unused_manifest_paths(
        &self,
        snapshot: &Snapshot,
        skipping: &mut HashSet<String>,
    ) -> Result<Vec<String>> {
        let mut paths = Vec::new();
        self.collect_unused_manifest_list(snapshot.base_manifest_list(), skipping, &mut paths)
            .await;
        self.collect_unused_manifest_list(snapshot.delta_manifest_list(), skipping, &mut paths)
            .await;
        if let Some(changelog) = snapshot.changelog_manifest_list() {
            self.collect_unused_manifest_list(changelog, skipping, &mut paths)
                .await;
        }

        if let Some(index_manifest) = snapshot.index_manifest() {
            match IndexManifest::read(&self.file_io, &self.manifest_path(index_manifest)).await {
                Ok(entries) => {
                    for entry in entries {
                        if skipping.insert(entry.index_file.file_name.clone()) {
                            let bucket_path = self.bucket_path(&entry.partition, entry.bucket)?;
                            paths.push(committed_index_file_path(
                                &self.table_location,
                                &bucket_path,
                                self.index_file_in_data_file_dir,
                                &entry.index_file,
                            ));
                        }
                    }
                    if skipping.insert(index_manifest.to_string()) {
                        paths.push(self.manifest_path(index_manifest));
                    }
                }
                // Already removed while expiring another snapshot.
                Err(crate::Error::IoUnexpected { ref source, .. })
                    if source.kind() == opendal::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }

        if let Some(statistics) = snapshot.statistics() {
            if skipping.insert(statistics.to_string()) {
                paths.push(format!(
                    "{}/{STATISTICS_DIR}/{statistics}",
                    self.table_location
                ));
            }
        }
        if let Some(plan_file) = reassign_plan_file(snapshot) {
            if skipping.insert(plan_file.to_string()) {
                paths.push(self.manifest_path(plan_file));
            }
        }
        Ok(paths)
    }

    /// Java `collectUnusedManifestList`: an unreadable list still has its own
    /// file deleted, leaving any manifests it named for orphan cleanup.
    async fn collect_unused_manifest_list(
        &self,
        list: &str,
        skipping: &mut HashSet<String>,
        paths: &mut Vec<String>,
    ) {
        let manifests = match self.read_manifest_list(list).await {
            Ok(manifests) => manifests,
            Err(error) => {
                log::warn!("Failed to read manifest list {list}: {error}");
                Vec::new()
            }
        };
        for manifest in &manifests {
            if skipping.insert(manifest.file_name().to_string()) {
                paths.push(self.manifest_path(manifest.file_name()));
                for extra in manifest.extra_files().unwrap_or_default() {
                    if skipping.insert(extra.clone()) {
                        paths.push(self.manifest_path(extra));
                    }
                }
            }
        }
        if skipping.insert(list.to_string()) {
            paths.push(self.manifest_path(list));
        }
    }

    /// Delete `paths`, ignoring failures like Java `FileIO#deleteQuietly`.
    pub(crate) async fn delete_quietly(&self, paths: Vec<String>) {
        let file_io = &self.file_io;
        let deletes = paths
            .into_iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .map(|path| async move {
                if let Err(error) = file_io.delete_file(&path).await {
                    log::warn!("Failed to delete {path}: {error}");
                }
            })
            .collect::<Vec<_>>();
        stream::iter(deletes)
            .buffer_unordered(FILE_OPERATION_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
    }
}

fn data_file_key(entry: &ManifestEntry) -> DataFileKey {
    (
        entry.partition().to_vec(),
        entry.bucket(),
        entry.file().file_name.clone(),
    )
}

/// Java `SerializationAssignment.planFile`: the plan file of a row-id
/// reassignment, owned by the snapshot that performed it.
fn reassign_plan_file(snapshot: &Snapshot) -> Option<&str> {
    let properties = snapshot.properties()?;
    if properties.get(REASSIGN_SNAPSHOT_ID_PROPERTY)? != &snapshot.id().to_string() {
        return None;
    }
    properties
        .get(REASSIGN_PLAN_FILE_PROPERTY)
        .map(String::as_str)
}
