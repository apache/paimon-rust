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
use crate::spec::{
    bucket_dir_name, BinaryRow, DataField, DataFileMeta, IndexManifest, Manifest, ManifestEntry,
    ManifestFileMeta, PartitionComputer,
};
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

/// Resolves extra file paths for stat-ing their real sizes.
struct ExtraFileResolver {
    table_location: String,
    partition_computer: Option<PartitionComputer>,
}

impl ExtraFileResolver {
    fn new(table_location: &str, partition_keys: &[String], schema_fields: &[DataField]) -> Self {
        let partition_computer = if partition_keys.is_empty() {
            None
        } else {
            PartitionComputer::new(
                partition_keys,
                schema_fields,
                "__DEFAULT_PARTITION__",
                false,
            )
            .ok()
        };
        Self {
            table_location: table_location.to_string(),
            partition_computer,
        }
    }

    fn resolve_bucket_path(&self, partition_bytes: &[u8], bucket: i32) -> Option<String> {
        let partition_path = if let Some(ref computer) = self.partition_computer {
            let row = BinaryRow::from_serialized_bytes(partition_bytes).ok()?;
            computer.generate_partition_path(&row).ok()?
        } else {
            String::new()
        };
        let bucket_dir = bucket_dir_name(bucket);
        Some(format!(
            "{}/{}{}",
            self.table_location, partition_path, bucket_dir
        ))
    }

    fn resolve_extra_file_path(
        &self,
        partition_bytes: &[u8],
        bucket: i32,
        data_file: &DataFileMeta,
        extra_file_name: &str,
    ) -> Option<String> {
        let bucket_path = self.resolve_bucket_path(partition_bytes, bucket)?;
        Some(data_file.aligned_file_path(&bucket_path, extra_file_name))
    }
}

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
///
/// Extra files referenced by data file entries are stat-ed to obtain their
/// real sizes, using partition/bucket info to construct full paths.
pub async fn collect_referenced_files_summary(
    file_io: &FileIO,
    table_location: &str,
    partition_keys: &[String],
    schema_fields: &[DataField],
) -> crate::Result<Vec<ReferencedFilesSummary>> {
    let manifest_cache: ManifestCache = Mutex::new(HashMap::new());
    let manifest_cache_ref = &manifest_cache;
    let extra_resolver = ExtraFileResolver::new(table_location, partition_keys, schema_fields);
    let extra_resolver_ref = &extra_resolver;

    let sm = SnapshotManager::new(file_io.clone(), table_location.to_string());
    let tm = TagManager::new(file_io.clone(), table_location.to_string());

    // 1. Main branch snapshots + tags (concurrently)
    // For main branch, snapshot reading and manifest resolution both use root SM.
    let (main_files, tag_files) = tokio::try_join!(
        collect_scope_files(file_io, &sm, &sm, manifest_cache_ref, extra_resolver_ref),
        collect_tag_files(
            file_io,
            &sm,
            &sm,
            &tm,
            manifest_cache_ref,
            extra_resolver_ref
        ),
    )?;
    let mut main_files = main_files;
    main_files.merge(&tag_files);

    // 2. Branch file sets (all branches concurrently)
    let bm = BranchManager::new(file_io.clone(), table_location.to_string());
    let branch_names = bm.list_all().await?;

    let sm_ref = &sm;
    let branch_futures: Vec<_> = branch_names
        .iter()
        .map(|branch_name| {
            let branch_sm = sm.with_branch(branch_name);
            let branch_tm = tm.with_branch(branch_name);
            async move {
                // Branch SM reads snapshot/tag files from branch path,
                // but manifest paths are always resolved from the table root.
                let (mut branch_files, branch_tag_files) = tokio::try_join!(
                    collect_scope_files(
                        file_io,
                        &branch_sm,
                        sm_ref,
                        manifest_cache_ref,
                        extra_resolver_ref
                    ),
                    collect_tag_files(
                        file_io,
                        &branch_sm,
                        sm_ref,
                        &branch_tm,
                        manifest_cache_ref,
                        extra_resolver_ref
                    ),
                )?;
                branch_files.merge(&branch_tag_files);
                Ok::<_, crate::Error>(branch_files)
            }
        })
        .collect();
    let branch_results = try_join_all(branch_futures).await?;

    // 3. Assemble output: total, main, branches
    let mut total_files = ScopeFileSet::default();
    total_files.merge(&main_files);
    for bs in &branch_results {
        total_files.merge(bs);
    }

    let mut result = vec![
        total_files.to_summary("total"),
        main_files.to_summary("branch:main"),
    ];
    for (name, files) in branch_names.iter().zip(&branch_results) {
        result.push(files.to_summary(&format!("branch:{name}")));
    }
    Ok(result)
}

async fn collect_scope_files(
    file_io: &FileIO,
    sm: &SnapshotManager,
    manifest_sm: &SnapshotManager,
    manifest_cache: &ManifestCache,
    extra_resolver: &ExtraFileResolver,
) -> crate::Result<ScopeFileSet> {
    let snapshot_ids = sm.list_all_ids().await?;

    let per_snapshot: Vec<Option<ScopeFileSet>> = stream::iter(snapshot_ids)
        .map(|snapshot_id| {
            let sm = sm.clone();
            async move {
                collect_single_snapshot_files(
                    file_io,
                    &sm,
                    manifest_sm,
                    snapshot_id,
                    manifest_cache,
                    extra_resolver,
                )
                .await
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
    _sm: &SnapshotManager,
    manifest_sm: &SnapshotManager,
    tm: &TagManager,
    manifest_cache: &ManifestCache,
    extra_resolver: &ExtraFileResolver,
) -> crate::Result<ScopeFileSet> {
    let tag_names = tm.list_all_names().await?;

    let tag_futures: Vec<_> = tag_names
        .iter()
        .map(|tag_name| async move {
            let snapshot = match tm.get(tag_name).await? {
                Some(s) => s,
                None => return Ok(None),
            };
            collect_snapshot_files(
                file_io,
                manifest_sm,
                &snapshot,
                manifest_cache,
                extra_resolver,
            )
            .await
        })
        .collect();
    let tag_results = try_join_all(tag_futures).await?;

    let mut merged = ScopeFileSet::default();
    for fs in tag_results.into_iter().flatten() {
        merged.merge(&fs);
    }
    Ok(merged)
}

async fn collect_single_snapshot_files(
    file_io: &FileIO,
    sm: &SnapshotManager,
    manifest_sm: &SnapshotManager,
    snapshot_id: i64,
    manifest_cache: &ManifestCache,
    extra_resolver: &ExtraFileResolver,
) -> crate::Result<Option<ScopeFileSet>> {
    let snapshot = match try_get_snapshot(sm, snapshot_id).await? {
        Some(s) => s,
        None => return Ok(None),
    };

    collect_snapshot_files(
        file_io,
        manifest_sm,
        &snapshot,
        manifest_cache,
        extra_resolver,
    )
    .await
}

async fn collect_snapshot_files(
    file_io: &FileIO,
    manifest_sm: &SnapshotManager,
    snapshot: &crate::spec::Snapshot,
    manifest_cache: &ManifestCache,
    extra_resolver: &ExtraFileResolver,
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

    // Pre-compute paths (always resolved from table root)
    let manifest_list_paths: Vec<String> = manifest_list_names
        .iter()
        .map(|name| manifest_sm.manifest_path(name))
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
        .map(|meta| manifest_sm.manifest_path(meta.file_name()))
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

        // Collect extra files that need stat-ing
        let mut extra_file_stat_tasks: Vec<(usize, usize, String)> = Vec::new();
        let mut all_file_entries: Vec<Vec<(String, i64)>> = Vec::with_capacity(results.len());

        for (manifest_idx, entries) in results.iter().enumerate() {
            let mut file_entries: Vec<(String, i64)> = Vec::new();
            for e in entries {
                file_entries.push((e.file().file_name.clone(), e.file().file_size));
                for extra in &e.file().extra_files {
                    let entry_idx = file_entries.len();
                    let full_path = extra_resolver.resolve_extra_file_path(
                        e.partition(),
                        e.bucket(),
                        e.file(),
                        extra,
                    );
                    if let Some(path) = full_path {
                        extra_file_stat_tasks.push((manifest_idx, entry_idx, path));
                    }
                    file_entries.push((extra.clone(), 0));
                }
            }
            all_file_entries.push(file_entries);
        }

        // Batch stat extra files concurrently
        if !extra_file_stat_tasks.is_empty() {
            let stat_futures: Vec<_> = extra_file_stat_tasks
                .iter()
                .map(|(_, _, path)| try_stat_file_size(file_io, path))
                .collect();
            let stat_results = try_join_all(stat_futures).await?;

            for ((manifest_idx, entry_idx, _), size) in
                extra_file_stat_tasks.iter().zip(stat_results)
            {
                if size > 0 {
                    all_file_entries[*manifest_idx][*entry_idx].1 = size;
                }
            }
        }

        let mut cache = manifest_cache.lock().unwrap();
        for (path, file_entries) in uncached_paths.into_iter().zip(all_file_entries) {
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
        let index_manifest_path = manifest_sm.manifest_path(index_manifest_name);
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

    // Collect statistics file if present
    if let Some(statistics_name) = snapshot.statistics() {
        let statistics_path = format!(
            "{}/statistics/{}",
            extra_resolver.table_location, statistics_name
        );
        let size = try_stat_file_size(file_io, &statistics_path).await?;
        if size > 0 {
            file_set
                .manifest_files
                .entry(statistics_name.to_string())
                .or_insert(size);
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

/// Stat a file to get its size. Returns 0 if the file is not found.
async fn try_stat_file_size(file_io: &FileIO, path: &str) -> crate::Result<i64> {
    let input = file_io.new_input(path)?;
    match input.metadata().await {
        Ok(status) => Ok(status.size as i64),
        Err(crate::Error::IoUnexpected { ref source, .. })
            if source.kind() == opendal::ErrorKind::NotFound =>
        {
            Ok(0)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PhysicalFileKind {
    Manifest,
    Statistics,
    Data,
    Index,
    Other,
}

fn table_relative_path<'a>(table_location: &str, path: &'a str) -> Option<&'a str> {
    let table_location = table_location.trim_end_matches('/');
    if path == table_location {
        Some("")
    } else {
        path.strip_prefix(table_location)
            .and_then(|rest| rest.strip_prefix('/'))
    }
}

fn is_manifest_file_name(file_name: &str) -> bool {
    file_name.starts_with("manifest-")
        || file_name.starts_with("manifest-list-")
        || file_name.starts_with("index-manifest-")
}

fn is_bucket_dir_name(segment: &str) -> bool {
    segment == "bucket-postpone"
        || segment
            .strip_prefix("bucket-")
            .is_some_and(|bucket| !bucket.is_empty() && bucket.chars().all(|c| c.is_ascii_digit()))
}

fn is_partition_segment(segment: &str) -> bool {
    let Some((key, _value)) = segment.split_once('=') else {
        return false;
    };
    !key.is_empty()
}

fn is_data_file_in_bucket(segments: &[&str], partition_depth: usize) -> bool {
    if segments.len() != partition_depth + 2 {
        return false;
    }

    segments[..partition_depth]
        .iter()
        .all(|segment| is_partition_segment(segment))
        && is_bucket_dir_name(segments[partition_depth])
        && !segments[partition_depth + 1].starts_with("index-")
}

fn is_data_file_in_data_dir(
    relative_path: &str,
    data_dir_relative_path: &str,
    partition_depth: usize,
) -> bool {
    let data_dir_relative_path = data_dir_relative_path.trim_matches('/');
    let data_relative_path = if data_dir_relative_path.is_empty() {
        relative_path
    } else {
        let Some(rest) = relative_path
            .strip_prefix(data_dir_relative_path)
            .and_then(|rest| rest.strip_prefix('/'))
        else {
            return false;
        };
        rest
    };
    let segments = data_relative_path.split('/').collect::<Vec<_>>();
    is_data_file_in_bucket(&segments, partition_depth)
}

fn classify_physical_path(
    table_location: &str,
    path: &str,
    partition_depth: usize,
    data_file_path_directory: Option<&str>,
) -> PhysicalFileKind {
    let Some(relative_path) = table_relative_path(table_location, path) else {
        return PhysicalFileKind::Other;
    };
    let relative_path = relative_path.trim_matches('/');
    if relative_path.is_empty() {
        return PhysicalFileKind::Other;
    }

    let segments = relative_path.split('/').collect::<Vec<_>>();

    match segments.as_slice() {
        ["manifest", name] if is_manifest_file_name(name) => PhysicalFileKind::Manifest,
        ["statistics", _] => PhysicalFileKind::Statistics,
        ["index", _] => PhysicalFileKind::Index,
        _ => {
            if let Some(data_dir) = data_file_path_directory {
                let data_dir = table_relative_path(table_location, data_dir).unwrap_or(data_dir);
                if is_data_file_in_data_dir(relative_path, data_dir, partition_depth) {
                    PhysicalFileKind::Data
                } else {
                    PhysicalFileKind::Other
                }
            } else if is_data_file_in_bucket(&segments, partition_depth) {
                PhysicalFileKind::Data
            } else {
                PhysicalFileKind::Other
            }
        }
    }
}

const DIR_LIST_CONCURRENCY: usize = 32;

/// Scan the table directory and compute total file sizes grouped by type.
///
/// First lists top-level subdirectories, then concurrently lists each
/// subdirectory recursively (up to 32 in parallel) to maximize throughput
/// on object stores with many partition directories.
///
/// Files are classified by their table-relative path. Only recognized Paimon
/// metadata directories and partition/bucket data paths are counted; unknown
/// files are ignored by this summary.
pub async fn collect_physical_files_summary(
    file_io: &FileIO,
    table_location: &str,
    partition_depth: usize,
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
            accumulate_file(
                &mut summary,
                table_location,
                &entry.path,
                partition_depth,
                entry.size,
            );
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
            accumulate_file(
                &mut summary,
                table_location,
                &status.path,
                partition_depth,
                status.size,
            );
        }
    }

    Ok(summary)
}

fn accumulate_file(
    summary: &mut PhysicalFilesSummary,
    table_location: &str,
    path: &str,
    partition_depth: usize,
    size: u64,
) {
    match classify_physical_path(table_location, path, partition_depth, None) {
        PhysicalFileKind::Manifest | PhysicalFileKind::Statistics => {
            summary.manifest_file_count += 1;
            summary.manifest_file_size += size as i64;
        }
        PhysicalFileKind::Data => {
            summary.data_file_count += 1;
            summary.data_file_size += size as i64;
        }
        PhysicalFileKind::Index => {
            summary.index_file_count += 1;
            summary.index_file_size += size as i64;
        }
        PhysicalFileKind::Other => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{CommitKind, Snapshot};
    use crate::table::{BranchManager, SnapshotManager, TagManager};
    use bytes::Bytes;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    async fn write_test_file(file_io: &FileIO, path: &str, content: &str) {
        file_io
            .new_output(path)
            .unwrap()
            .write(Bytes::from(content.to_string()))
            .await
            .unwrap();
    }

    #[test]
    fn test_extra_file_resolver_uses_external_path_parent() {
        use crate::spec::stats::BinaryTableStats;

        let resolver = ExtraFileResolver::new("s3://warehouse/table", &[], &[]);
        let stats = BinaryTableStats::empty();
        let file = DataFileMeta {
            file_name: "data-0.row".to_string(),
            file_size: 1,
            row_count: 1,
            min_key: vec![],
            max_key: vec![],
            key_stats: stats.clone(),
            value_stats: stats,
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: vec!["data-0.row.index".to_string()],
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: Some("s3://bucket/external/data-0.row".to_string()),
            first_row_id: None,
            write_cols: None,
        };

        assert_eq!(
            resolver.resolve_extra_file_path(&[], 0, &file, "data-0.row.index"),
            Some("s3://bucket/external/data-0.row.index".to_string())
        );
    }

    #[tokio::test]
    async fn test_referenced_files_stats_external_sidecar_from_parent() {
        use crate::spec::stats::BinaryTableStats;
        use crate::spec::{DataFileMeta, FileKind, Manifest, ManifestFileMeta, ManifestList};

        let table_path = "memory:/test_external_sidecar_references";
        let external_dir = "memory:/external_sidecar_references";
        let sidecar_name = "data-0.row.index";
        let sidecar_content = "sidecar-bytes";
        let file_io = test_file_io();

        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();
        write_test_file(
            &file_io,
            &format!("{external_dir}/{sidecar_name}"),
            sidecar_content,
        )
        .await;

        let manifest_name = "manifest-external-sidecar-0";
        let manifest_path = format!("{table_path}/manifest/{manifest_name}");
        let data_file = DataFileMeta {
            file_name: "data-0.row".to_string(),
            file_size: 100,
            row_count: 10,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: vec![sidecar_name.to_string()],
            creation_time: None,
            delete_row_count: Some(0),
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: Some(format!("{external_dir}/data-0.row")),
            first_row_id: None,
            write_cols: None,
        };
        let entry = ManifestEntry::new(FileKind::Add, vec![0u8; 12], 0, 1, data_file, 0);
        Manifest::write(&file_io, &manifest_path, &[entry])
            .await
            .unwrap();

        let manifest_list_name = "manifest-list-external-sidecar";
        let manifest_list_path = format!("{table_path}/manifest/{manifest_list_name}");
        let manifest_meta = ManifestFileMeta::new(
            manifest_name.to_string(),
            512,
            1,
            0,
            BinaryTableStats::empty(),
            0,
        );
        ManifestList::write(&file_io, &manifest_list_path, &[manifest_meta])
            .await
            .unwrap();

        let delta_list_name = "manifest-list-external-sidecar-delta";
        let delta_list_path = format!("{table_path}/manifest/{delta_list_name}");
        ManifestList::write(&file_io, &delta_list_path, &[])
            .await
            .unwrap();

        let sm = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = Snapshot::builder()
            .version(3)
            .id(1)
            .schema_id(0)
            .base_manifest_list(manifest_list_name.to_string())
            .delta_manifest_list(delta_list_name.to_string())
            .commit_user("test".to_string())
            .commit_identifier(0)
            .commit_kind(CommitKind::APPEND)
            .time_millis(1000)
            .build();
        sm.commit_snapshot(&snapshot).await.unwrap();

        let result = collect_referenced_files_summary(&file_io, table_path, &[], &[])
            .await
            .unwrap();
        let total = result.iter().find(|r| r.source == "total").unwrap();
        assert_eq!(total.data_file_count, 2);
        assert_eq!(total.data_file_size, 100 + sidecar_content.len() as i64);
    }

    #[tokio::test]
    async fn test_collect_empty_table() {
        let file_io = test_file_io();
        let result =
            collect_referenced_files_summary(&file_io, "memory:/test_empty_table", &[], &[])
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

        let result = collect_referenced_files_summary(&file_io, table_path, &[], &[])
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
    async fn test_physical_files_summary_uses_path_context_for_unpartitioned_table() {
        let table_path = "memory:/test_physical_files_summary_path_context";
        let file_io = test_file_io();

        write_test_file(
            &file_io,
            &format!("{table_path}/manifest/manifest-list-0"),
            "manifest list",
        )
        .await;
        write_test_file(
            &file_io,
            &format!("{table_path}/manifest/manifest-0"),
            "manifest",
        )
        .await;
        write_test_file(
            &file_io,
            &format!("{table_path}/manifest/index-manifest-0"),
            "index manifest",
        )
        .await;
        write_test_file(&file_io, &format!("{table_path}/index/index-0"), "index").await;
        write_test_file(&file_io, &format!("{table_path}/bucket-0/data-0"), "data").await;
        write_test_file(
            &file_io,
            &format!("{table_path}/bucket-0/part-0.parquet"),
            "data without prefix",
        )
        .await;
        write_test_file(
            &file_io,
            &format!("{table_path}/bucket-0/index-should-not-be-data"),
            "bucket index",
        )
        .await;
        write_test_file(
            &file_io,
            &format!("{table_path}/bucket-postpone/data-u-0"),
            "postpone data",
        )
        .await;
        write_test_file(
            &file_io,
            &format!("{table_path}/nested/bucket-0/data-too-deep"),
            "not a data bucket",
        )
        .await;
        write_test_file(
            &file_io,
            &format!("{table_path}/data-root-file.parquet"),
            "root data prefix",
        )
        .await;
        write_test_file(
            &file_io,
            &format!("{table_path}/snapshot/snapshot-1"),
            "snapshot",
        )
        .await;
        write_test_file(&file_io, &format!("{table_path}/schema/schema-0"), "schema").await;
        write_test_file(&file_io, &format!("{table_path}/tag/tag-v1"), "tag").await;
        write_test_file(&file_io, &format!("{table_path}/_SUCCESS"), "success").await;
        write_test_file(&file_io, &format!("{table_path}/random-file"), "random").await;
        write_test_file(
            &file_io,
            &format!("{table_path}/statistics/stat-0"),
            "statistics",
        )
        .await;
        write_test_file(
            &file_io,
            &format!("{table_path}/manifest/stat-0"),
            "not classified by statistics prefix",
        )
        .await;

        let result = collect_physical_files_summary(&file_io, table_path, 0)
            .await
            .unwrap();

        assert_eq!(result.manifest_file_count, 4);
        assert_eq!(result.index_file_count, 1);
        assert_eq!(result.data_file_count, 3);
    }

    #[tokio::test]
    async fn test_physical_files_summary_uses_partition_depth() {
        let table_path = "memory:/test_physical_files_summary_partitioned";
        let file_io = test_file_io();

        write_test_file(
            &file_io,
            &format!("{table_path}/dt=2026-05-21/bucket-0/part-0.parquet"),
            "partition data",
        )
        .await;
        write_test_file(
            &file_io,
            &format!("{table_path}/dt=2026-05-21/bucket-0/index-0"),
            "partition bucket index",
        )
        .await;
        write_test_file(
            &file_io,
            &format!("{table_path}/dt=2026-05-21/not-bucket/data-0"),
            "not bucket",
        )
        .await;
        write_test_file(
            &file_io,
            &format!("{table_path}/not_partition/bucket-0/data-0"),
            "not partition",
        )
        .await;
        write_test_file(
            &file_io,
            &format!("{table_path}/bucket-0/root-bucket-file"),
            "wrong depth",
        )
        .await;

        let result = collect_physical_files_summary(&file_io, table_path, 1)
            .await
            .unwrap();

        assert_eq!(result.data_file_count, 1);
        assert_eq!(result.index_file_count, 0);
    }

    #[tokio::test]
    async fn test_branch_tag_referenced_files() {
        use crate::spec::stats::BinaryTableStats;
        use crate::spec::{DataFileMeta, FileKind, Manifest, ManifestFileMeta, ManifestList};

        let table_path = "memory:/test_branch_tag";
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
        let empty_stats = BinaryTableStats::new(vec![0u8; 8], vec![0u8; 8], vec![Some(0)]);

        // Write a manifest file (referenced by branch tag only) at the TABLE ROOT
        let manifest_name = "manifest-branch-only-1";
        let manifest_path = format!("{table_path}/manifest/{manifest_name}");
        let data_file = DataFileMeta {
            file_name: "data-branch-tag-file-1.parquet".to_string(),
            file_size: 4096,
            row_count: 100,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: Some(0),
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
        };
        let entry = ManifestEntry::new(FileKind::Add, vec![0u8; 12], 0, 1, data_file, 2);
        Manifest::write(&file_io, &manifest_path, &[entry])
            .await
            .unwrap();

        // Write a manifest list that references the above manifest (at the table root)
        let manifest_list_name = "manifest-list-branch-tag-base";
        let manifest_list_path = format!("{table_path}/manifest/{manifest_list_name}");
        let manifest_meta =
            ManifestFileMeta::new(manifest_name.to_string(), 512, 1, 0, empty_stats.clone(), 0);
        ManifestList::write(&file_io, &manifest_list_path, &[manifest_meta])
            .await
            .unwrap();

        // Write an empty delta manifest list at the table root
        let delta_list_name = "manifest-list-branch-tag-delta";
        let delta_list_path = format!("{table_path}/manifest/{delta_list_name}");
        ManifestList::write(&file_io, &delta_list_path, &[])
            .await
            .unwrap();

        // Create a main branch snapshot (with non-existent manifest lists)
        let main_snapshot = Snapshot::builder()
            .version(3)
            .id(1)
            .schema_id(0)
            .base_manifest_list("manifest-list-main-base".to_string())
            .delta_manifest_list("manifest-list-main-delta".to_string())
            .commit_user("test".to_string())
            .commit_identifier(0)
            .commit_kind(CommitKind::APPEND)
            .time_millis(1000)
            .build();
        sm.commit_snapshot(&main_snapshot).await.unwrap();

        // Create branch b1 with NO snapshots
        let bm = BranchManager::new(file_io.clone(), table_path.to_string());
        bm.create_branch("b1").await.unwrap();

        // Create a tag under branch b1 that references the readable manifest lists
        let branch_tm = TagManager::new(file_io.clone(), table_path.to_string()).with_branch("b1");
        let branch_tag_snapshot = Snapshot::builder()
            .version(3)
            .id(100)
            .schema_id(0)
            .base_manifest_list(manifest_list_name.to_string())
            .delta_manifest_list(delta_list_name.to_string())
            .commit_user("test".to_string())
            .commit_identifier(0)
            .commit_kind(CommitKind::APPEND)
            .time_millis(2000)
            .build();
        branch_tm.create("v1", &branch_tag_snapshot).await.unwrap();

        let result = collect_referenced_files_summary(&file_io, table_path, &[], &[])
            .await
            .unwrap();

        // Should have: total, branch:main, branch:b1
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].source, "total");
        assert_eq!(result[1].source, "branch:main");
        assert_eq!(result[2].source, "branch:b1");

        // branch:b1 must have non-zero counts from the branch tag's readable manifests.
        // The manifest list + manifest file + delta manifest list = 3 manifest files.
        assert!(
            result[2].manifest_file_count > 0,
            "branch:b1 must have manifest files from branch tag, got {}",
            result[2].manifest_file_count
        );
        assert!(
            result[2].manifest_file_size > 0,
            "branch:b1 must have non-zero manifest file size, got {}",
            result[2].manifest_file_size
        );
        // The manifest references one data file
        assert_eq!(result[2].data_file_count, 1);
        assert_eq!(result[2].data_file_size, 4096);

        // total should include branch:b1's files
        assert!(result[0].data_file_count >= 1);
        assert!(result[0].data_file_size >= 4096);
    }
}
