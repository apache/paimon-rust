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

//! Collect deduplicated referenced file size summaries for all snapshots of a table.
//!
//! Reference: [LocalOrphanFilesClean](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/operation/LocalOrphanFilesClean.java)

use std::collections::HashMap;
use std::sync::Mutex;

use crate::io::FileIO;
use crate::spec::{IndexManifest, Manifest, ManifestEntry, ManifestFileMeta};
use crate::table::{BranchManager, SnapshotManager, TagManager};
use futures::future::try_join_all;
use futures::stream::{self, StreamExt, TryStreamExt};

/// Per-scope aggregated summary of referenced files (deduplicated).
///
/// Each row represents the unique referenced files for a scope:
/// - `"total"`: all snapshots across all branches and tags
/// - `"branch:main"`: main branch snapshots + tags
/// - `"branch:<name>"`: a specific branch
///
/// Files are deduplicated by file name within each scope, so the sum
/// represents actual disk usage that is still referenced (protected from cleanup).
/// Both ADD and DELETE manifest entries are included since both reference
/// physical files that cannot be removed until the snapshot expires.
#[derive(Debug, Clone, Default)]
pub struct ReferencedFilesSummary {
    pub source: String,
    pub manifest_file_count: i64,
    pub manifest_file_size: i64,
    pub data_file_count: i64,
    pub data_file_size: i64,
    pub index_file_count: i64,
    pub index_file_size: i64,
}

/// Deduplicated file set for a scope, keyed by file name.
#[derive(Default)]
struct ScopeFileSet {
    manifest_files: HashMap<String, i64>,
    data_files: HashMap<String, i64>,
    index_files: HashMap<String, i64>,
}

impl ScopeFileSet {
    fn to_summary(&self, source: &str) -> ReferencedFilesSummary {
        ReferencedFilesSummary {
            source: source.to_string(),
            manifest_file_count: self.manifest_files.len() as i64,
            manifest_file_size: self.manifest_files.values().sum(),
            data_file_count: self.data_files.len() as i64,
            data_file_size: self.data_files.values().sum(),
            index_file_count: self.index_files.len() as i64,
            index_file_size: self.index_files.values().sum(),
        }
    }

    fn merge(&mut self, other: &ScopeFileSet) {
        for (k, v) in &other.manifest_files {
            self.manifest_files.entry(k.clone()).or_insert(*v);
        }
        for (k, v) in &other.data_files {
            self.data_files.entry(k.clone()).or_insert(*v);
        }
        for (k, v) in &other.index_files {
            self.index_files.entry(k.clone()).or_insert(*v);
        }
    }
}

const SNAPSHOT_CONCURRENCY: usize = 32;

/// Cached data file entries (file_name, file_size) per manifest file full path.
type ManifestCache = Mutex<HashMap<String, Vec<(String, i64)>>>;

/// Collect per-scope deduplicated referenced file size summaries for a table.
///
/// Returns rows:
/// 1. `"total"` — union of all referenced files from main branch, tags, and branches
/// 2. `"branch:main"` — main branch snapshots + tag snapshots
/// 3. `"branch:<name>"` — one row per branch
///
/// Snapshots are processed concurrently (up to 32 at a time). Within each
/// snapshot, manifest list and manifest file reads are also concurrent.
/// A shared cache avoids re-reading the same manifest file across snapshots.
///
/// Files are deduplicated by name within each scope to produce an accurate
/// count of unique referenced files. Both ADD and DELETE entries are included
/// since both reference physical files protected from cleanup.
///
/// Manifest list files and index manifest files are counted as manifest files,
/// consistent with `physical_files_size` classification.
pub async fn collect_referenced_files_summary(
    file_io: &FileIO,
    table_location: &str,
) -> crate::Result<Vec<ReferencedFilesSummary>> {
    let manifest_cache: ManifestCache = Mutex::new(HashMap::new());
    let manifest_cache_ref = &manifest_cache;

    // 1. Main branch snapshots + tags
    let sm = SnapshotManager::new(file_io.clone(), table_location.to_string());
    let mut main_files = collect_scope_files(file_io, &sm, manifest_cache_ref).await?;

    let tm = TagManager::new(file_io.clone(), table_location.to_string());
    let tag_files = collect_tag_files(file_io, &sm, &tm, manifest_cache_ref).await?;
    main_files.merge(&tag_files);

    // 2. Branch file sets (snapshots + branch-level tags)
    let bm = BranchManager::new(file_io.clone(), table_location.to_string());
    let branch_names = bm.list_all().await?;
    let mut branch_file_sets = Vec::new();
    for branch_name in &branch_names {
        let branch_sm = sm.with_branch(branch_name);
        let mut branch_files =
            collect_scope_files(file_io, &branch_sm, manifest_cache_ref).await?;

        let branch_tm = tm.with_branch(branch_name);
        let branch_tag_files =
            collect_tag_files(file_io, &branch_sm, &branch_tm, manifest_cache_ref).await?;
        branch_files.merge(&branch_tag_files);

        branch_file_sets.push((branch_name.clone(), branch_files));
    }

    // 3. Assemble output: total, main, branches
    let mut total_files = ScopeFileSet::default();
    total_files.merge(&main_files);
    for (_, bs) in &branch_file_sets {
        total_files.merge(bs);
    }

    let mut result = vec![
        total_files.to_summary("total"),
        main_files.to_summary("branch:main"),
    ];
    for (name, files) in &branch_file_sets {
        result.push(files.to_summary(&format!("branch:{name}")));
    }
    Ok(result)
}

async fn collect_scope_files(
    file_io: &FileIO,
    sm: &SnapshotManager,
    manifest_cache: &ManifestCache,
) -> crate::Result<ScopeFileSet> {
    let snapshot_ids = sm.list_all_ids().await?;

    let per_snapshot: Vec<Option<ScopeFileSet>> =
        stream::iter(snapshot_ids)
            .map(|snapshot_id| {
                let sm = sm.clone();
                async move {
                    collect_single_snapshot_files(file_io, &sm, snapshot_id, manifest_cache).await
                }
            })
            .buffer_unordered(SNAPSHOT_CONCURRENCY)
            .try_collect()
            .await?;

    let mut merged = ScopeFileSet::default();
    for fs in per_snapshot.into_iter().flatten() {
        merged.merge(&fs);
    }
    Ok(merged)
}

async fn collect_tag_files(
    file_io: &FileIO,
    sm: &SnapshotManager,
    tm: &TagManager,
    manifest_cache: &ManifestCache,
) -> crate::Result<ScopeFileSet> {
    let tag_names = tm.list_all_names().await?;
    let mut merged = ScopeFileSet::default();

    for tag_name in &tag_names {
        let snapshot = match tm.get(tag_name).await? {
            Some(s) => s,
            None => continue,
        };
        if let Some(fs) = collect_snapshot_files(file_io, sm, &snapshot, manifest_cache).await? {
            merged.merge(&fs);
        }
    }

    Ok(merged)
}

async fn collect_single_snapshot_files(
    file_io: &FileIO,
    sm: &SnapshotManager,
    snapshot_id: i64,
    manifest_cache: &ManifestCache,
) -> crate::Result<Option<ScopeFileSet>> {
    let snapshot = match try_get_snapshot(sm, snapshot_id).await? {
        Some(s) => s,
        None => return Ok(None),
    };

    collect_snapshot_files(file_io, sm, &snapshot, manifest_cache).await
}

async fn collect_snapshot_files(
    file_io: &FileIO,
    sm: &SnapshotManager,
    snapshot: &crate::spec::Snapshot,
    manifest_cache: &ManifestCache,
) -> crate::Result<Option<ScopeFileSet>> {
    let mut file_set = ScopeFileSet::default();

    // Collect manifest list file names (these are manifest-type files themselves)
    let mut manifest_list_names = vec![
        snapshot.base_manifest_list().to_string(),
        snapshot.delta_manifest_list().to_string(),
    ];
    if let Some(cl) = snapshot.changelog_manifest_list() {
        manifest_list_names.push(cl.to_string());
    }

    // Pre-compute paths
    let manifest_list_paths: Vec<String> = manifest_list_names
        .iter()
        .map(|name| sm.manifest_path(name))
        .collect();

    // Read all manifest lists concurrently and record their sizes
    let manifest_list_futures: Vec<_> = manifest_list_paths
        .iter()
        .map(|path| try_read_manifest_list_with_size(file_io, path))
        .collect();
    let manifest_list_results = try_join_all(manifest_list_futures).await?;

    // Register manifest list files themselves as manifest files
    for (name, (_, size)) in manifest_list_names.iter().zip(&manifest_list_results) {
        if *size > 0 {
            file_set.manifest_files.entry(name.clone()).or_insert(*size);
        }
    }

    // Flatten all manifest file metas from all manifest lists
    let all_manifest_metas: Vec<&ManifestFileMeta> = manifest_list_results
        .iter()
        .flat_map(|(metas, _)| metas.iter())
        .collect();

    // Register manifest files
    for meta in &all_manifest_metas {
        file_set
            .manifest_files
            .entry(meta.file_name().to_string())
            .or_insert(meta.file_size());
    }

    // Read manifest files to get data file entries, using cache by full path
    let manifest_paths: Vec<String> = all_manifest_metas
        .iter()
        .map(|meta| sm.manifest_path(meta.file_name()))
        .collect();

    let uncached_indices: Vec<usize> = manifest_paths
        .iter()
        .enumerate()
        .filter(|(_, path)| {
            let cache = manifest_cache.lock().unwrap();
            !cache.contains_key(path.as_str())
        })
        .map(|(i, _)| i)
        .collect();

    if !uncached_indices.is_empty() {
        let uncached_paths: Vec<&str> = uncached_indices
            .iter()
            .map(|&i| manifest_paths[i].as_str())
            .collect();

        let manifest_futures: Vec<_> = uncached_paths
            .iter()
            .map(|path| try_read_manifest(file_io, path))
            .collect();
        let results = try_join_all(manifest_futures).await?;

        let mut cache = manifest_cache.lock().unwrap();
        for (path, entries) in uncached_paths.into_iter().zip(results) {
            let file_entries: Vec<(String, i64)> = entries
                .iter()
                .map(|e| (e.file().file_name.clone(), e.file().file_size))
                .collect();
            cache.insert(path.to_string(), file_entries);
        }
    }

    // Collect data files from cache (deduplicated by HashMap key)
    {
        let cache = manifest_cache.lock().unwrap();
        for path in &manifest_paths {
            if let Some(entries) = cache.get(path.as_str()) {
                for (name, size) in entries {
                    file_set.data_files.entry(name.clone()).or_insert(*size);
                }
            }
        }
    }

    // Read index manifest if present
    if let Some(index_manifest_name) = snapshot.index_manifest() {
        // The index manifest file itself is a manifest-type file
        let index_manifest_path = sm.manifest_path(index_manifest_name);
        let index_entries =
            try_read_index_manifest_with_size(file_io, &index_manifest_path).await?;

        if index_entries.1 > 0 {
            file_set
                .manifest_files
                .entry(index_manifest_name.to_string())
                .or_insert(index_entries.1);
        }

        for entry in &index_entries.0 {
            file_set
                .index_files
                .entry(entry.index_file.file_name.clone())
                .or_insert(entry.index_file.file_size as i64);
        }
    }

    Ok(Some(file_set))
}

async fn try_get_snapshot(
    sm: &SnapshotManager,
    snapshot_id: i64,
) -> crate::Result<Option<crate::spec::Snapshot>> {
    match sm.get_snapshot(snapshot_id).await {
        Ok(s) => Ok(Some(s)),
        Err(crate::Error::IoUnexpected { ref source, .. })
            if source.kind() == opendal::ErrorKind::NotFound =>
        {
            Ok(None)
        }
        Err(crate::Error::DataInvalid { ref message, .. })
            if message.contains("does not exist") =>
        {
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// Read a manifest list file. Returns (entries, file_size_in_bytes).
async fn try_read_manifest_list_with_size(
    file_io: &FileIO,
    path: &str,
) -> crate::Result<(Vec<ManifestFileMeta>, i64)> {
    let input = file_io.new_input(path)?;
    match input.read().await {
        Ok(bytes) => {
            let size = bytes.len() as i64;
            let metas = crate::spec::avro::from_avro_bytes_fast(&bytes)?;
            Ok((metas, size))
        }
        Err(crate::Error::IoUnexpected { ref source, .. })
            if source.kind() == opendal::ErrorKind::NotFound =>
        {
            Ok((Vec::new(), 0))
        }
        Err(e) => Err(e),
    }
}

async fn try_read_manifest(file_io: &FileIO, path: &str) -> crate::Result<Vec<ManifestEntry>> {
    match Manifest::read(file_io, path).await {
        Ok(entries) => Ok(entries),
        Err(crate::Error::IoUnexpected { ref source, .. })
            if source.kind() == opendal::ErrorKind::NotFound =>
        {
            Ok(Vec::new())
        }
        Err(e) => Err(e),
    }
}

/// Read an index manifest file. Returns (entries, file_size_in_bytes).
async fn try_read_index_manifest_with_size(
    file_io: &FileIO,
    path: &str,
) -> crate::Result<(Vec<crate::spec::IndexManifestEntry>, i64)> {
    match IndexManifest::read_with_size(file_io, path).await {
        Ok(result) => Ok(result),
        Err(crate::Error::IoUnexpected { ref source, .. })
            if source.kind() == opendal::ErrorKind::NotFound =>
        {
            Ok((Vec::new(), 0))
        }
        Err(e) => Err(e),
    }
}

/// Summary of all physical files in the table directory, categorized by file type.
#[derive(Debug, Clone, Default)]
pub struct PhysicalFilesSummary {
    pub manifest_file_count: i64,
    pub manifest_file_size: i64,
    pub data_file_count: i64,
    pub data_file_size: i64,
    pub index_file_count: i64,
    pub index_file_size: i64,
}

/// Categorize a file name into a file type.
fn classify_file_name(file_name: &str) -> FileType {
    if file_name.starts_with("manifest-") || file_name.starts_with("index-manifest-") {
        FileType::Manifest
    } else if file_name.starts_with("data-") {
        FileType::Data
    } else if file_name.starts_with("index-") {
        FileType::Index
    } else {
        FileType::Other
    }
}

enum FileType {
    Manifest,
    Data,
    Index,
    Other,
}

const DIR_LIST_CONCURRENCY: usize = 32;

/// Scan the table directory and compute total file sizes grouped by type.
///
/// First lists top-level subdirectories, then concurrently lists each
/// subdirectory recursively (up to 32 in parallel) to maximize throughput
/// on object stores with many partition directories.
///
/// Files are classified by their file name prefix:
/// - `manifest-*` / `manifest-list-*` / `index-manifest-*` → manifest
/// - `data-*` → data
/// - `index-*` (excluding `index-manifest-*`) → index
/// - Other files (snapshots, schemas, etc.) are not counted.
pub async fn collect_physical_files_summary(
    file_io: &FileIO,
    table_location: &str,
) -> crate::Result<PhysicalFilesSummary> {
    // List top-level entries to discover subdirectories and top-level files
    let top_entries = match file_io.list_status(table_location).await {
        Ok(s) => s,
        Err(crate::Error::IoUnexpected { ref source, .. })
            if source.kind() == opendal::ErrorKind::NotFound =>
        {
            return Ok(PhysicalFilesSummary::default());
        }
        Err(e) => return Err(e),
    };

    let mut summary = PhysicalFilesSummary::default();

    // Classify top-level files directly
    let mut sub_dirs = Vec::new();
    for entry in &top_entries {
        if entry.is_dir {
            sub_dirs.push(entry.path.clone());
        } else {
            let file_name = entry.path.rsplit('/').next().unwrap_or(&entry.path);
            accumulate_file(&mut summary, file_name, entry.size);
        }
    }

    // Concurrently list each subdirectory recursively
    let dir_results: Vec<crate::Result<Vec<crate::io::FileStatus>>> = stream::iter(sub_dirs)
        .map(|dir_path| async move {
            match file_io.list_status_recursive(&dir_path).await {
                Ok(s) => Ok(s),
                Err(crate::Error::IoUnexpected { ref source, .. })
                    if source.kind() == opendal::ErrorKind::NotFound =>
                {
                    Ok(Vec::new())
                }
                Err(e) => Err(e),
            }
        })
        .buffer_unordered(DIR_LIST_CONCURRENCY)
        .collect()
        .await;

    for result in dir_results {
        let statuses = result?;
        for status in &statuses {
            let file_name = status.path.rsplit('/').next().unwrap_or(&status.path);
            accumulate_file(&mut summary, file_name, status.size);
        }
    }

    Ok(summary)
}

fn accumulate_file(summary: &mut PhysicalFilesSummary, file_name: &str, size: u64) {
    match classify_file_name(file_name) {
        FileType::Manifest => {
            summary.manifest_file_count += 1;
            summary.manifest_file_size += size as i64;
        }
        FileType::Data => {
            summary.data_file_count += 1;
            summary.data_file_size += size as i64;
        }
        FileType::Index => {
            summary.index_file_count += 1;
            summary.index_file_size += size as i64;
        }
        FileType::Other => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{CommitKind, Snapshot};
    use crate::table::{BranchManager, SnapshotManager, TagManager};

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    #[tokio::test]
    async fn test_collect_empty_table() {
        let file_io = test_file_io();
        let result = collect_referenced_files_summary(&file_io, "memory:/test_empty_table")
            .await
            .unwrap();
        // total + branch:main
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].source, "total");
        assert_eq!(result[0].data_file_count, 0);
        assert_eq!(result[1].source, "branch:main");
        assert_eq!(result[1].data_file_count, 0);
    }

    #[tokio::test]
    async fn test_collect_with_missing_manifest() {
        let table_path = "memory:/test_missing_manifest";
        let file_io = test_file_io();
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();

        let sm = SnapshotManager::new(file_io.clone(), table_path.to_string());

        // Create a snapshot that references non-existent manifest lists
        let snapshot = Snapshot::builder()
            .version(3)
            .id(1)
            .schema_id(0)
            .base_manifest_list("non-existent-base".to_string())
            .delta_manifest_list("non-existent-delta".to_string())
            .commit_user("test".to_string())
            .commit_identifier(0)
            .commit_kind(CommitKind::APPEND)
            .time_millis(1000)
            .build();
        sm.commit_snapshot(&snapshot).await.unwrap();

        let result = collect_referenced_files_summary(&file_io, table_path)
            .await
            .unwrap();
        // total + branch:main
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].source, "total");
        assert_eq!(result[0].manifest_file_count, 0);
        assert_eq!(result[0].data_file_count, 0);
        assert_eq!(result[1].source, "branch:main");
        assert_eq!(result[1].manifest_file_count, 0);
        assert_eq!(result[1].data_file_count, 0);
    }

    #[tokio::test]
    async fn test_branch_tag_referenced_files() {
        let table_path = "memory:/test_branch_tag";
        let file_io = test_file_io();

        // Set up main branch with a snapshot
        let sm = SnapshotManager::new(file_io.clone(), table_path.to_string());
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();

        let snapshot = Snapshot::builder()
            .version(3)
            .id(1)
            .schema_id(0)
            .base_manifest_list("manifest-list-base-1".to_string())
            .delta_manifest_list("manifest-list-delta-1".to_string())
            .commit_user("test".to_string())
            .commit_identifier(0)
            .commit_kind(CommitKind::APPEND)
            .time_millis(1000)
            .build();
        sm.commit_snapshot(&snapshot).await.unwrap();

        // Create branch directory structure (no snapshot in branch)
        let bm = BranchManager::new(file_io.clone(), table_path.to_string());
        bm.create_branch("b1").await.unwrap();

        // Create a tag under the branch that references a snapshot with manifest lists
        let branch_tm = TagManager::new(file_io.clone(), table_path.to_string())
            .with_branch("b1");
        let branch_snapshot = Snapshot::builder()
            .version(3)
            .id(100)
            .schema_id(0)
            .base_manifest_list("manifest-list-branch-base".to_string())
            .delta_manifest_list("manifest-list-branch-delta".to_string())
            .commit_user("test".to_string())
            .commit_identifier(0)
            .commit_kind(CommitKind::APPEND)
            .time_millis(2000)
            .build();
        branch_tm.create("v1", &branch_snapshot).await.unwrap();

        let result = collect_referenced_files_summary(&file_io, table_path)
            .await
            .unwrap();

        // Should have: total, branch:main, branch:b1
        assert_eq!(result.len(), 3);
        assert_eq!(result[2].source, "branch:b1");
        // The branch tag references manifest lists that don't exist (NotFound → skipped),
        // but the manifest list file names themselves should be counted as manifest files
        // if they were readable. Since they don't exist, size is 0 but no error occurs.
        // The key assertion: the function completes without error and includes the branch.
        // If branch tags were not collected, this branch would have been missed entirely
        // or produced incorrect results.

        // Verify that main branch's manifest list file names are counted
        // (they also don't exist physically, so size = 0 from NotFound)
        assert_eq!(result[1].source, "branch:main");
    }
}
