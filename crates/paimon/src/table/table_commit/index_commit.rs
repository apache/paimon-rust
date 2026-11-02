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

//! Resolve index deltas for compaction, ordinary writes and overwrites.
//!
//! Source validation belongs to each layout. Every validated delta uses the
//! same manifest merge and the caller's snapshot publication/recovery protocol.

use super::*;
use std::borrow::Cow;

#[derive(Clone, Copy)]
pub(super) enum IndexChanges<'a> {
    Compact {
        captured: Option<&'a Snapshot>,
        active: &'a [ManifestEntry],
        prepared: Option<&'a [IndexManifestEntry]>,
    },
    Direct(&'a [IndexManifestEntry]),
    Overwrite {
        entries: &'a [IndexManifestEntry],
        partition_filter: Option<&'a PartitionFilter>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum IndexDomain {
    GlobalRowIds,
    PrimaryKeyLevel,
}

pub(super) struct ResolvedIndexes {
    pub(super) manifest_name: Option<String>,
    pub(super) entries: Option<Vec<IndexManifestEntry>>,
}

impl TableCommit {
    pub(super) async fn resolve_indexes(
        &self,
        latest_snapshot: &Option<Snapshot>,
        data_entries: &[ManifestEntry],
        changes: IndexChanges<'_>,
    ) -> Result<ResolvedIndexes> {
        let manifest_name = latest_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.index_manifest().map(str::to_owned));
        // DE compaction preserves global row IDs. Its index maintenance runs
        // through Direct after data publication; compaction retains the manifest.
        if matches!(changes, IndexChanges::Compact { .. })
            && self.table.schema().primary_keys().is_empty()
        {
            return Ok(ResolvedIndexes {
                manifest_name,
                entries: None,
            });
        }
        let file_io = self.snapshot_manager.file_io();
        let manifest_dir = self.snapshot_manager.manifest_dir();
        let previous = if matches!(changes, IndexChanges::Compact { .. }) {
            Self::read_index_entries(file_io, &manifest_dir, latest_snapshot).await?
        } else {
            Self::read_prev_index_entries(file_io, &manifest_dir, latest_snapshot).await?
        };
        if let IndexChanges::Compact {
            captured,
            active,
            prepared,
        } = changes
        {
            super::super::pk_index_maintenance::validate(
                &self.table,
                captured,
                &previous,
                active,
                data_entries,
                prepared,
            )
            .await?;
            let prepared = prepared.unwrap_or_default();
            let entries = if prepared.is_empty() {
                None
            } else {
                Some(Self::merge_index_entries(
                    previous,
                    prepared,
                    IndexDomain::PrimaryKeyLevel,
                )?)
            };
            return Ok(ResolvedIndexes {
                manifest_name,
                entries,
            });
        }
        let (explicit, overwrite, partition_filter) = match changes {
            IndexChanges::Direct(entries) => (entries, false, None),
            IndexChanges::Overwrite {
                entries,
                partition_filter,
            } => (entries, true, partition_filter),
            IndexChanges::Compact { .. } => unreachable!("compaction handled above"),
        };
        self.check_global_index_sources(latest_snapshot, data_entries, explicit)
            .await?;
        let mut updates = Cow::Borrowed(explicit);
        if !overwrite {
            let invalidated = self
                .global_index_update_entries(&previous, data_entries, explicit)
                .await?;
            if !invalidated.is_empty() {
                updates.to_mut().extend(invalidated);
            }
        }
        let mut retained = previous.clone();
        if overwrite {
            match partition_filter {
                None => retained.clear(),
                Some(filter) => {
                    let mut matching = Vec::new();
                    for entry in retained {
                        if !filter.matches_entry(&entry.partition)? {
                            matching.push(entry);
                        }
                    }
                    retained = matching;
                }
            }
        }
        let merged = Self::merge_index_entries(retained, &updates, IndexDomain::GlobalRowIds)?;
        self.check_deletion_vector_references(latest_snapshot, data_entries, &updates, &merged)
            .await?;
        Ok(ResolvedIndexes {
            manifest_name,
            entries: (merged != previous).then_some(merged),
        })
    }

    async fn read_index_entries(
        file_io: &FileIO,
        manifest_dir: &str,
        snapshot: &Option<Snapshot>,
    ) -> Result<Vec<IndexManifestEntry>> {
        if let Some(name) = snapshot.as_ref().and_then(Snapshot::index_manifest) {
            return IndexManifest::read(file_io, &format!("{manifest_dir}/{name}")).await;
        }
        Ok(vec![])
    }

    /// Global index commits consume active entries; PK validation also needs
    /// the original manifest membership before applying any changes.
    pub(super) async fn read_prev_index_entries(
        file_io: &FileIO,
        manifest_dir: &str,
        snapshot: &Option<Snapshot>,
    ) -> Result<Vec<IndexManifestEntry>> {
        Ok(normalize_index_entries(
            Self::read_index_entries(file_io, manifest_dir, snapshot).await?,
        ))
    }

    /// Apply a validated index delta. PK ordinals belong to a source level;
    /// global row IDs share a table-wide coordinate space.
    pub(super) fn merge_index_entries(
        mut all: Vec<IndexManifestEntry>,
        new_index_entries: &[IndexManifestEntry],
        domain: IndexDomain,
    ) -> Result<Vec<IndexManifestEntry>> {
        let deletions = new_index_entries
            .iter()
            .filter(|entry| entry.kind == FileKind::Delete)
            .collect::<Vec<_>>();
        for deletion in deletions {
            let global_index = !matches!(
                deletion.index_file.index_type.as_str(),
                "HASH" | DELETION_VECTORS_INDEX_TYPE
            );
            // Java combines global indexes by type and file name, then checks
            // the complete current metadata before removing an entry. A stale
            // removal must not discard metadata-only row-id reassignment.
            let position = all.iter().position(|entry| {
                if global_index {
                    entry.index_file.index_type == deletion.index_file.index_type
                        && entry.index_file.file_name == deletion.index_file.file_name
                } else {
                    same_index_file_entry(entry, deletion)
                }
            });
            if let Some(position) = position {
                let previous = &all[position];
                if global_index
                    && (!same_index_partition(&previous.partition, &deletion.partition)
                        || previous.bucket != deletion.bucket
                        || previous.index_file != deletion.index_file)
                {
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "Index file {} metadata does not match the current entry.",
                            deletion.index_file.file_name
                        ),
                        source: None,
                    });
                }
                all.remove(position);
            } else if global_index {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Trying to delete global index file {} which does not exist.",
                        deletion.index_file.file_name
                    ),
                    source: None,
                });
            } else if deletion.index_file.index_type == DELETION_VECTORS_INDEX_TYPE {
                return Err(crate::Error::DataInvalid {
                    message: format!("Cannot replace missing deletion vector index '{}'; prepare the DELETE again from the latest snapshot", deletion.index_file.file_name),
                    source: None,
                });
            }
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
        if domain == IndexDomain::GlobalRowIds {
            Self::validate_global_index_overlap(&all, &additions)?;
            Self::validate_added_global_index_overlap(&additions)?;
        }
        let mut dv_files = HashSet::new();
        let mut dv_names = HashSet::new();
        for entry in all
            .iter()
            .chain(&additions)
            .filter(|entry| entry.index_file.index_type == DELETION_VECTORS_INDEX_TYPE)
        {
            let partition = if is_empty_partition(&entry.partition) {
                &[][..]
            } else {
                entry.partition.as_slice()
            };
            if !dv_names.insert((partition, entry.bucket, entry.index_file.file_name.as_str())) {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Duplicate deletion vector index '{}'",
                        entry.index_file.file_name
                    ),
                    source: None,
                });
            }
            if let Some(ranges) = &entry.index_file.deletion_vectors_ranges {
                for file in ranges.keys() {
                    if !dv_files.insert((partition, entry.bucket, file.as_str())) {
                        return Err(crate::Error::DataInvalid {
                            message: format!("Conflicting deletion vectors for data file '{file}'; prepare the DELETE again from the latest snapshot"),
                            source: None,
                        });
                    }
                }
            }
        }
        all.extend(additions);
        Ok(all)
    }

    pub(super) fn validate_global_index_overlap(
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
                    && retained.index_file.index_type == added.index_file.index_type
                    && same_extra_field_ids(
                        retained_meta.extra_field_ids.as_deref(),
                        added_meta.extra_field_ids.as_deref(),
                    )
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

    pub(super) fn validate_added_global_index_overlap(
        added_entries: &[IndexManifestEntry],
    ) -> Result<()> {
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
                    && left.index_file.index_type == right.index_file.index_type
                    && same_extra_field_ids(
                        left_meta.extra_field_ids.as_deref(),
                        right_meta.extra_field_ids.as_deref(),
                    )
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
}

#[cfg(test)]
mod tests;
