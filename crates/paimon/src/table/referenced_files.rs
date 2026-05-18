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

//! Collect per-snapshot file size summaries for all snapshots of a table.
//!
//! Reference: [LocalOrphanFilesClean](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/operation/LocalOrphanFilesClean.java)

use std::collections::HashMap;
use std::sync::Mutex;

use crate::io::FileIO;
use crate::spec::{IndexManifest, Manifest, ManifestEntry, ManifestFileMeta};
use crate::table::{BranchManager, SnapshotManager, TagManager};
use futures::future::try_join_all;
use futures::stream::{self, StreamExt, TryStreamExt};

/// Per-scope aggregated summary of referenced files.
///
/// Each row represents the total referenced files for a scope:
/// - `"total"`: all snapshots across all branches and tags
/// - `"main"`: main branch snapshots + tags
/// - `"branch:<name>"`: a specific branch
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

impl ReferencedFilesSummary {
    fn accumulate(&mut self, other: &ReferencedFilesSummary) {
        self.manifest_file_count += other.manifest_file_count;
        self.manifest_file_size += other.manifest_file_size;
        self.data_file_count += other.data_file_count;
        self.data_file_size += other.data_file_size;
        self.index_file_count += other.index_file_count;
        self.index_file_size += other.index_file_size;
    }
}

const SNAPSHOT_CONCURRENCY: usize = 32;

/// Cached (data_file_count, data_file_size) per manifest file full path.
type ManifestCache = Mutex<HashMap<String, (i64, i64)>>;

/// Collect per-scope referenced file size summaries for a table.
///
/// Returns rows:
/// 1. `"total"` — union of all snapshots from main branch, tags, and branches
/// 2. `"main"` — main branch snapshots + tag snapshots
/// 3. `"branch:<name>"` — one row per branch
///
/// Snapshots are processed concurrently (up to 32 at a time). Within each
/// snapshot, manifest list and manifest file reads are also concurrent.
/// A shared cache avoids re-reading the same manifest file across snapshots.
///
/// Manifest files that have been deleted by concurrent cleanup are gracefully
/// skipped (treated as contributing 0 files/bytes).
pub async fn collect_referenced_files_summary(
    file_io: &FileIO,
    table_location: &str,
) -> crate::Result<Vec<ReferencedFilesSummary>> {
    let manifest_cache: ManifestCache = Mutex::new(HashMap::new());
    let manifest_cache_ref = &manifest_cache;

    // 1. Main branch snapshots + tags
    let sm = SnapshotManager::new(file_io.clone(), table_location.to_string());
    let mut main_summary =
        collect_scope_summary(file_io, &sm, "branch:main", manifest_cache_ref).await?;

    let tm = TagManager::new(file_io.clone(), table_location.to_string());
    let tag_summary = collect_tag_scope_summary(file_io, &sm, &tm, manifest_cache_ref).await?;
    main_summary.accumulate(&tag_summary);

    // 2. Branch summaries
    let bm = BranchManager::new(file_io.clone(), table_location.to_string());
    let branch_names = bm.list_all().await?;
    let mut branch_summaries = Vec::new();
    for branch_name in &branch_names {
        let branch_sm = sm.with_branch(branch_name);
        let branch_summary = collect_scope_summary(
            file_io,
            &branch_sm,
            &format!("branch:{branch_name}"),
            manifest_cache_ref,
        )
        .await?;
        branch_summaries.push(branch_summary);
    }

    // 3. Assemble output: total, main, branches
    let mut total = ReferencedFilesSummary {
        source: "total".to_string(),
        ..Default::default()
    };
    total.accumulate(&main_summary);
    for bs in &branch_summaries {
        total.accumulate(bs);
    }

    let mut result = vec![total, main_summary];
    result.extend(branch_summaries);
    Ok(result)
}

async fn collect_scope_summary(
    file_io: &FileIO,
    sm: &SnapshotManager,
    source: &str,
    manifest_cache: &ManifestCache,
) -> crate::Result<ReferencedFilesSummary> {
    let snapshot_ids = sm.list_all_ids().await?;

    let per_snapshot: Vec<Option<ReferencedFilesSummary>> = stream::iter(snapshot_ids)
        .map(|snapshot_id| {
            let sm = sm.clone();
            async move {
                collect_single_snapshot_summary(file_io, &sm, snapshot_id, manifest_cache).await
            }
        })
        .buffer_unordered(SNAPSHOT_CONCURRENCY)
        .try_collect()
        .await?;

    let mut summary = ReferencedFilesSummary {
        source: source.to_string(),
        ..Default::default()
    };
    for s in per_snapshot.into_iter().flatten() {
        summary.accumulate(&s);
    }
    Ok(summary)
}

async fn collect_tag_scope_summary(
    file_io: &FileIO,
    sm: &SnapshotManager,
    tm: &TagManager,
    manifest_cache: &ManifestCache,
) -> crate::Result<ReferencedFilesSummary> {
    let tag_names = tm.list_all_names().await?;
    let mut summary = ReferencedFilesSummary::default();

    for tag_name in &tag_names {
        let snapshot = match tm.get(tag_name).await? {
            Some(s) => s,
            None => continue,
        };
        if let Some(s) = collect_snapshot_summary(file_io, sm, &snapshot, manifest_cache).await? {
            summary.accumulate(&s);
        }
    }

    Ok(summary)
}

async fn collect_single_snapshot_summary(
    file_io: &FileIO,
    sm: &SnapshotManager,
    snapshot_id: i64,
    manifest_cache: &ManifestCache,
) -> crate::Result<Option<ReferencedFilesSummary>> {
    let snapshot = match try_get_snapshot(sm, snapshot_id).await? {
        Some(s) => s,
        None => return Ok(None),
    };

    collect_snapshot_summary(file_io, sm, &snapshot, manifest_cache).await
}

async fn collect_snapshot_summary(
    file_io: &FileIO,
    sm: &SnapshotManager,
    snapshot: &crate::spec::Snapshot,
    manifest_cache: &ManifestCache,
) -> crate::Result<Option<ReferencedFilesSummary>> {
    let mut summary = ReferencedFilesSummary::default();

    // Collect manifest list file names
    let mut manifest_list_names = vec![
        snapshot.base_manifest_list().to_string(),
        snapshot.delta_manifest_list().to_string(),
    ];
    if let Some(cl) = snapshot.changelog_manifest_list() {
        manifest_list_names.push(cl.to_string());
    }

    // Pre-compute paths so futures can borrow them
    let manifest_list_paths: Vec<String> = manifest_list_names
        .iter()
        .map(|name| sm.manifest_path(name))
        .collect();

    // Read all manifest lists concurrently
    let manifest_list_futures: Vec<_> = manifest_list_paths
        .iter()
        .map(|path| try_read_manifest_list(file_io, path))
        .collect();
    let manifest_lists = try_join_all(manifest_list_futures).await?;

    // Flatten all manifest file metas from all manifest lists
    let all_manifest_metas: Vec<&ManifestFileMeta> =
        manifest_lists.iter().flat_map(|ml| ml.iter()).collect();

    summary.manifest_file_count = all_manifest_metas.len() as i64;
    summary.manifest_file_size = all_manifest_metas.iter().map(|m| m.file_size()).sum();

    // Read manifest files to get data file stats, using cache by full path
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

    // Only read manifests not yet in cache
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

        // Store results in cache
        let mut cache = manifest_cache.lock().unwrap();
        for (path, entries) in uncached_paths.into_iter().zip(results) {
            let count = entries.len() as i64;
            let size: i64 = entries.iter().map(|e| e.file().file_size).sum();
            cache.insert(path.to_string(), (count, size));
        }
    }

    // Aggregate from cache
    {
        let cache = manifest_cache.lock().unwrap();
        for path in &manifest_paths {
            if let Some(&(count, size)) = cache.get(path.as_str()) {
                summary.data_file_count += count;
                summary.data_file_size += size;
            }
        }
    }

    // Read index manifest if present
    if let Some(index_manifest_name) = snapshot.index_manifest() {
        let index_path = sm.manifest_path(index_manifest_name);
        let index_entries = try_read_index_manifest(file_io, &index_path).await?;
        for entry in &index_entries {
            summary.index_file_count += 1;
            summary.index_file_size += entry.index_file.file_size as i64;
        }
    }

    Ok(Some(summary))
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

async fn try_read_manifest_list(
    file_io: &FileIO,
    path: &str,
) -> crate::Result<Vec<ManifestFileMeta>> {
    let input = file_io.new_input(path)?;
    match input.read().await {
        Ok(bytes) => crate::spec::avro::from_avro_bytes_fast(&bytes),
        Err(crate::Error::IoUnexpected { ref source, .. })
            if source.kind() == opendal::ErrorKind::NotFound =>
        {
            Ok(Vec::new())
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

async fn try_read_index_manifest(
    file_io: &FileIO,
    path: &str,
) -> crate::Result<Vec<crate::spec::IndexManifestEntry>> {
    match IndexManifest::read(file_io, path).await {
        Ok(entries) => Ok(entries),
        Err(crate::Error::IoUnexpected { ref source, .. })
            if source.kind() == opendal::ErrorKind::NotFound =>
        {
            Ok(Vec::new())
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
    use crate::table::SnapshotManager;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    #[tokio::test]
    async fn test_collect_empty_table() {
        let file_io = test_file_io();
        let result = collect_referenced_files_summary(&file_io, "memory:/test_empty_table")
            .await
            .unwrap();
        // total + main
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
        // total + main
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].source, "total");
        assert_eq!(result[0].manifest_file_count, 0);
        assert_eq!(result[0].data_file_count, 0);
        assert_eq!(result[1].source, "branch:main");
        assert_eq!(result[1].manifest_file_count, 0);
        assert_eq!(result[1].data_file_count, 0);
    }
}
