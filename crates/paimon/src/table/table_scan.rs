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

//! TableScan for full table scan.
//!
//! Reference: [pypaimon.read.table_scan.TableScan](https://github.com/apache/paimon/blob/release-1.3/paimon-python/pypaimon/read/table_scan.py)
//! and [FullStartingScanner](https://github.com/apache/paimon/blob/release-1.3/paimon-python/pypaimon/read/scanner/full_starting_scanner.py).

use super::Table;
use crate::io::FileIO;
use crate::spec::{BinaryRow, CoreOptions, FileKind, IndexManifest, ManifestEntry, Snapshot};
use crate::table::bin_pack::split_for_batch;
use crate::table::source::{DataSplitBuilder, DeletionFile, PartitionBucket, Plan};
use crate::table::SnapshotManager;
use crate::Error;
use std::collections::{HashMap, HashSet};

/// Path segment for manifest directory under table.
const MANIFEST_DIR: &str = "manifest";
/// Path segment for index directory under table.
const INDEX_DIR: &str = "index";

/// Reads a manifest list file (Avro) and returns manifest file metas.
async fn read_manifest_list(
    file_io: &FileIO,
    table_path: &str,
    list_name: &str,
) -> crate::Result<Vec<crate::spec::ManifestFileMeta>> {
    let path = format!(
        "{}/{}/{}",
        table_path.trim_end_matches('/'),
        MANIFEST_DIR,
        list_name
    );
    let input = file_io.new_input(&path)?;
    if !input.exists().await? {
        return Ok(Vec::new());
    }
    let bytes = input.read().await?;
    crate::spec::from_avro_bytes::<crate::spec::ManifestFileMeta>(&bytes)
}

/// Reads all manifest entries for a snapshot (base + delta manifest lists, then each manifest file).
async fn read_all_manifest_entries(
    file_io: &FileIO,
    table_path: &str,
    snapshot: &Snapshot,
) -> crate::Result<Vec<ManifestEntry>> {
    let mut manifest_files =
        read_manifest_list(file_io, table_path, snapshot.base_manifest_list()).await?;
    let delta = read_manifest_list(file_io, table_path, snapshot.delta_manifest_list()).await?;
    manifest_files.extend(delta);

    let manifest_path_prefix = format!("{}/{}", table_path.trim_end_matches('/'), MANIFEST_DIR);
    let mut all_entries = Vec::new();
    // todo: consider use multiple-threads read manifest
    for meta in manifest_files {
        let path = format!("{}/{}", manifest_path_prefix, meta.file_name());
        let entries = crate::spec::Manifest::read(file_io, &path).await?;
        all_entries.extend(entries);
    }
    Ok(all_entries)
}

fn filter_manifest_entries(
    entries: Vec<ManifestEntry>,
    deletion_vectors_enabled: bool,
) -> Vec<ManifestEntry> {
    entries
        .into_iter()
        .filter(|entry| !(deletion_vectors_enabled && entry.file().level == 0))
        .collect()
}

/// Builds a map from (partition, bucket) to (data_file_name -> DeletionFile) from index manifest entries.
/// Only considers ADD entries with index_type "DELETION_VECTORS" and their deletion_vectors_ranges.
fn build_deletion_files_map(
    index_entries: &[crate::spec::IndexManifestEntry],
    table_path: &str,
) -> HashMap<PartitionBucket, HashMap<String, DeletionFile>> {
    use crate::spec::FileKind;
    let table_path = table_path.trim_end_matches('/');
    let index_path_prefix = format!("{table_path}/{INDEX_DIR}");
    let mut map: HashMap<PartitionBucket, HashMap<String, DeletionFile>> = HashMap::new();
    for entry in index_entries {
        if entry.kind != FileKind::Add {
            continue;
        }
        if entry.index_file.index_type != "DELETION_VECTORS" {
            continue;
        }
        let ranges = match &entry.index_file.deletion_vectors_ranges {
            Some(r) if !r.is_empty() => r,
            _ => continue,
        };
        let key = PartitionBucket::new(entry.partition.clone(), entry.bucket);
        let dv_path = format!("{}/{}", index_path_prefix, entry.index_file.file_name);
        let per_bucket = map.entry(key).or_default();
        for (data_file_name, (offset, length)) in ranges {
            per_bucket.insert(
                data_file_name.clone(),
                DeletionFile::new(
                    dv_path.clone(),
                    *offset as i64,
                    *length as i64,
                    // todo: consider cardinality
                    None,
                ),
            );
        }
    }
    map
}

/// Merges add/delete manifest entries following pypaimon's `adds - deletes` behavior.
///
/// The identifier must be rich enough to match Paimon's file identity, otherwise a delete
/// for one file version can incorrectly remove another with the same file name.
fn merge_manifest_entries(entries: Vec<ManifestEntry>) -> Vec<ManifestEntry> {
    let mut deleted_entry_keys = HashSet::new();
    let mut added_entries = Vec::new();

    for entry in entries {
        match entry.kind() {
            FileKind::Add => added_entries.push(entry),
            FileKind::Delete => {
                deleted_entry_keys.insert(entry.identifier());
            }
        }
    }

    added_entries
        .into_iter()
        .filter(|entry| !deleted_entry_keys.contains(&entry.identifier()))
        .collect()
}

/// TableScan for full table scan (no incremental, no predicate).
///
/// Reference: [pypaimon.read.table_scan.TableScan](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/read/table_scan.py)
#[derive(Debug, Clone)]
pub struct TableScan<'a> {
    table: &'a Table,
}

impl<'a> TableScan<'a> {
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Plan the full scan: read latest snapshot, manifest list, manifest entries, then build DataSplits using bin packing.
    pub async fn plan(&self) -> crate::Result<Plan> {
        let file_io = self.table.file_io();
        let table_path = self.table.location();
        let snapshot_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());

        let snapshot = match snapshot_manager.get_latest_snapshot().await? {
            Some(s) => s,
            None => return Ok(Plan::new(Vec::new())),
        };
        let core_options = CoreOptions::new(self.table.schema().options());
        let target_split_size = core_options.source_split_target_size();
        let open_file_cost = core_options.source_split_open_file_cost();
        let deletion_vectors_enabled = core_options.deletion_vectors_enabled();
        Self::plan_snapshot(
            snapshot,
            file_io,
            table_path,
            target_split_size,
            open_file_cost,
            deletion_vectors_enabled,
        )
        .await
    }

    async fn plan_snapshot(
        snapshot: Snapshot,
        file_io: &FileIO,
        table_path: &str,
        target_split_size: i64,
        open_file_cost: i64,
        deletion_vectors_enabled: bool,
    ) -> crate::Result<Plan> {
        let entries = read_all_manifest_entries(file_io, table_path, &snapshot).await?;
        let entries = filter_manifest_entries(entries, deletion_vectors_enabled);
        let entries = merge_manifest_entries(entries);
        if entries.is_empty() {
            return Ok(Plan::new(Vec::new()));
        }

        // Group by (partition, bucket). Key = (partition_bytes, bucket).
        let mut groups: HashMap<(Vec<u8>, i32), Vec<ManifestEntry>> = HashMap::new();
        for e in entries {
            let key = (e.partition().to_vec(), e.bucket());
            groups.entry(key).or_default().push(e);
        }

        let snapshot_id = snapshot.id();
        let base_path = table_path;
        let mut splits = Vec::new();

        // Read deletion vector index manifest once (like Java generateSplits / scanDvIndex).
        let deletion_files_map = if let Some(index_manifest_name) = snapshot.index_manifest() {
            let index_manifest_path =
                format!("{}/{}", base_path.trim_end_matches('/'), MANIFEST_DIR);
            let path = format!("{index_manifest_path}/{index_manifest_name}");
            let index_entries = IndexManifest::read(file_io, &path).await?;
            Some(build_deletion_files_map(&index_entries, base_path))
        } else {
            None
        };

        for ((partition, bucket), group_entries) in groups {
            let total_buckets = group_entries
                .first()
                .map(|e| e.total_buckets())
                .ok_or_else(|| Error::UnexpectedError {
                    message: format!("Manifest entry group for bucket {bucket} is empty, cannot determine total_buckets"),
                    source: None,
                })?;
            let data_files: Vec<_> = group_entries
                .into_iter()
                .map(|e| {
                    let ManifestEntry { file, .. } = e;
                    file
                })
                .collect();

            // todo: consider partitioned table
            let bucket_path = format!("{base_path}/bucket-{bucket}");

            // Get the per-bucket deletion file map for looking up by file name after bin packing.
            let per_bucket_deletion_map = deletion_files_map
                .as_ref()
                .and_then(|map| map.get(&PartitionBucket::new(partition, bucket)));

            let file_groups = split_for_batch(data_files, target_split_size, open_file_cost);
            for file_group in file_groups {
                // Build deletion files list for this specific file group (by file name lookup),
                // matching Python's _get_deletion_files_for_split.
                let data_deletion_files = per_bucket_deletion_map.map(|per_bucket| {
                    file_group
                        .iter()
                        .map(|f| per_bucket.get(&f.file_name).cloned())
                        .collect::<Vec<Option<DeletionFile>>>()
                });

                let mut builder = DataSplitBuilder::new()
                    .with_snapshot(snapshot_id)
                    // todo: consider pass real partition
                    .with_partition(BinaryRow::new(0))
                    .with_bucket(bucket)
                    .with_bucket_path(bucket_path.clone())
                    .with_total_buckets(total_buckets)
                    .with_data_files(file_group);
                if let Some(files) = data_deletion_files {
                    builder = builder.with_data_deletion_files(files);
                }
                splits.push(builder.build()?);
            }
        }
        Ok(Plan::new(splits))
    }
}
