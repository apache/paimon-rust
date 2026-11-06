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

//! Commit-time manifest merging, ported from Java `ManifestFileLegacyMerger`.
//!
//! A DELETE entry leaves the manifest set only together with the ADD it
//! cancels, so merged manifests resolve to the same live files as their input.

use super::*;
use futures::{Stream, StreamExt, TryStreamExt};

/// Commit-time merge settings, read from table options on every merge.
struct MergeConfig {
    target_size: i64,
    min_count: usize,
    read_parallelism: usize,
}

impl TableCommit {
    /// Merge the previous snapshot's manifests before they become the new base list.
    ///
    /// New manifests are recorded in `created_files`. Input manifests are
    /// neither modified nor deleted.
    pub(super) async fn merge_manifests(
        &self,
        input: Vec<ManifestFileMeta>,
        created_files: &mut Vec<String>,
    ) -> Result<Vec<ManifestFileMeta>> {
        if input.is_empty() {
            return Ok(input);
        }
        let options = self.table.schema().core_options();
        let config = MergeConfig {
            target_size: self.manifest_target_size.max(1),
            min_count: options.manifest_merge_min_count()?,
            read_parallelism: options.scan_manifest_parallelism()?,
        };
        self.compact_minor(input, &config, created_files).await
    }

    /// Java `compactMinor`: merge consecutive manifests once their total size
    /// reaches the target, and the trailing group only with `min_count` files.
    async fn compact_minor(
        &self,
        input: Vec<ManifestFileMeta>,
        config: &MergeConfig,
        created_files: &mut Vec<String>,
    ) -> Result<Vec<ManifestFileMeta>> {
        let mut result = Vec::new();
        let mut candidates = Vec::new();
        let mut total_size = 0i64;
        for manifest in input {
            total_size += manifest.file_size();
            candidates.push(manifest);
            if total_size >= config.target_size {
                let group = std::mem::take(&mut candidates);
                result.extend(self.merge_candidates(group, config, created_files).await?);
                total_size = 0;
            }
        }
        if candidates.len() >= config.min_count {
            result.extend(
                self.merge_candidates(candidates, config, created_files)
                    .await?,
            );
        } else {
            result.extend(candidates);
        }
        Ok(result)
    }

    async fn merge_candidates(
        &self,
        candidates: Vec<ManifestFileMeta>,
        config: &MergeConfig,
        created_files: &mut Vec<String>,
    ) -> Result<Vec<ManifestFileMeta>> {
        if candidates.len() <= 1 {
            return Ok(candidates);
        }
        let manifest_dir = self.snapshot_manager.manifest_dir();
        let groups: Vec<Vec<ManifestEntry>> = read_manifests(
            self.snapshot_manager.file_io(),
            &manifest_dir,
            &candidates,
            config.read_parallelism,
        )
        .try_collect()
        .await?;
        let merged = merge_group_entries(groups.into_iter().flatten().collect())?;
        self.write_merged_manifests(&merged, created_files).await
    }

    async fn write_merged_manifests(
        &self,
        entries: &[ManifestEntry],
        created_files: &mut Vec<String>,
    ) -> Result<Vec<ManifestFileMeta>> {
        let manifest_dir = self.snapshot_manager.manifest_dir();
        let prefix = format!("manifest-{}", uuid::Uuid::new_v4());
        self.write_manifest_files(
            self.snapshot_manager.file_io(),
            &manifest_dir,
            &prefix,
            entries,
            created_files,
        )
        .await
    }
}

/// Read manifests with bounded concurrency, yielding their entries in input order.
fn read_manifests(
    file_io: &FileIO,
    manifest_dir: &str,
    manifests: &[ManifestFileMeta],
    parallelism: usize,
) -> impl Stream<Item = Result<Vec<ManifestEntry>>> + Send {
    let file_io = file_io.clone();
    let paths = manifests
        .iter()
        .map(|manifest| format!("{manifest_dir}/{}", manifest.file_name()))
        .collect::<Vec<_>>();
    // Each read owns its inputs so the commit future stays `Send`.
    futures::stream::iter(paths)
        .map(move |path| {
            let file_io = file_io.clone();
            async move { Manifest::read(&file_io, &path).await }
        })
        .buffered(parallelism.max(1))
}

/// Merge one group of manifest entries. For well-formed input this matches
/// Java `FileEntry.mergeEntries`, including the entry order.
///
/// An ADD and a DELETE of the same file inside the group cancel. A DELETE whose
/// ADD is in another group is kept; dropping it would make the file live again.
fn merge_group_entries(entries: Vec<ManifestEntry>) -> Result<Vec<ManifestEntry>> {
    let mut added = HashSet::new();
    let mut deleted = HashSet::new();
    for entry in &entries {
        match entry.kind() {
            FileKind::Add => {
                if !added.insert(entry.identifier()) {
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "Trying to add file '{}' in bucket {} which is already in the merged manifests",
                            entry.file().file_name,
                            entry.bucket(),
                        ),
                        source: None,
                    });
                }
            }
            FileKind::Delete => {
                deleted.insert(entry.identifier());
            }
        }
    }
    let mut emitted_deletes = HashSet::new();
    Ok(entries
        .into_iter()
        .filter(|entry| {
            let identifier = entry.identifier();
            match entry.kind() {
                FileKind::Add => !deleted.contains(&identifier),
                // A repeated DELETE removes nothing more; keep one.
                FileKind::Delete => {
                    !added.contains(&identifier) && emitted_deletes.insert(identifier)
                }
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(kind: FileKind, name: &str, level: i32) -> ManifestEntry {
        let file = DataFileMeta {
            file_name: name.to_string(),
            file_size: 100,
            row_count: 1,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
            column_max_sequence_numbers: None,
        };
        ManifestEntry::new(kind, vec![], 0, 1, file, 2)
    }

    fn keys(entries: &[ManifestEntry]) -> Vec<(FileKind, &str, i32)> {
        entries
            .iter()
            .map(|e| (*e.kind(), e.file().file_name.as_str(), e.file().level))
            .collect()
    }

    #[test]
    fn add_and_delete_in_one_group_cancel() {
        let merged = merge_group_entries(vec![
            entry(FileKind::Add, "a", 0),
            entry(FileKind::Delete, "a", 0),
            entry(FileKind::Add, "b", 0),
        ])
        .unwrap();
        assert_eq!(keys(&merged), [(FileKind::Add, "b", 0)]);
    }

    #[test]
    fn delete_whose_add_is_outside_the_group_is_kept() {
        // #899: the ADD of "old" lives in an earlier group that is not merged.
        let merged = merge_group_entries(vec![
            entry(FileKind::Delete, "old", 0),
            entry(FileKind::Add, "new", 0),
        ])
        .unwrap();
        assert_eq!(
            keys(&merged),
            [(FileKind::Delete, "old", 0), (FileKind::Add, "new", 0)]
        );
    }

    #[test]
    fn level_is_part_of_file_identity() {
        // An in-place upgrade deletes f@0 and adds f@1 under the same name.
        let merged = merge_group_entries(vec![
            entry(FileKind::Add, "f", 0),
            entry(FileKind::Delete, "f", 0),
            entry(FileKind::Add, "f", 1),
        ])
        .unwrap();
        assert_eq!(keys(&merged), [(FileKind::Add, "f", 1)]);
    }

    #[test]
    fn repeated_delete_is_kept_once() {
        let merged = merge_group_entries(vec![
            entry(FileKind::Delete, "x", 0),
            entry(FileKind::Delete, "x", 0),
        ])
        .unwrap();
        assert_eq!(keys(&merged), [(FileKind::Delete, "x", 0)]);
    }

    #[test]
    fn duplicate_add_is_rejected() {
        let error = merge_group_entries(vec![
            entry(FileKind::Add, "a", 0),
            entry(FileKind::Add, "a", 0),
        ])
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("already in the merged manifests"),
            "{error}"
        );
    }
}
