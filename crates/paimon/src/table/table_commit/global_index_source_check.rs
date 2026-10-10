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

//! Validate unpublished global indexes against their source data on every commit attempt.

use super::*;
use crate::spec::DataEvolutionIndexSourceMeta;
use crate::table::{merge_row_ranges, RowRange};

impl TableCommit {
    pub(super) async fn check_global_index_sources(
        &self,
        latest_snapshot: &Option<Snapshot>,
        data_entries: &[ManifestEntry],
        index_entries: &[IndexManifestEntry],
    ) -> Result<()> {
        if !self.data_evolution_enabled {
            return Ok(());
        }
        let additions = index_entries
            .iter()
            .filter(|entry| {
                entry.kind == FileKind::Add && entry.index_file.global_index_meta.is_some()
            })
            .cloned()
            .collect::<Vec<_>>();
        if additions.is_empty() {
            return Ok(());
        }
        let partition_fields = self.table.schema().partition_fields();
        let filter = if partition_fields.is_empty() {
            None
        } else {
            Some(PartitionFilter::from_partition_set(
                additions
                    .iter()
                    .map(|entry| entry.partition.clone())
                    .collect(),
                &partition_fields,
            )?)
        };
        let mut current = self
            .scan_snapshot_entries(latest_snapshot, filter.as_ref())
            .await?;
        current.extend_from_slice(data_entries);
        let current = merge_active_entries(current);
        let mut ranges: HashMap<PartitionBucketKey, Vec<RowRange>> = HashMap::new();
        for entry in current {
            if let Some((start, end)) = entry.file().row_id_range() {
                ranges
                    .entry((partition_key(entry.partition()), entry.bucket()))
                    .or_default()
                    .push(RowRange::new(start, end));
            }
        }
        let ranges = ranges
            .into_iter()
            .map(|(key, ranges)| (key, merge_row_ranges(ranges)))
            .collect::<HashMap<_, _>>();
        let mut sources = Vec::new();
        for entry in &additions {
            let meta = entry.index_file.global_index_meta.as_ref().unwrap();
            if !ranges
                .get(&(partition_key(&entry.partition), entry.bucket))
                .is_some_and(|ranges| {
                    ranges.iter().any(|range| {
                        range.from() <= meta.row_range_start && meta.row_range_end <= range.to()
                    })
                })
            {
                return Err(index_conflict(format!(
                    "Global index row ID existence conflict: index file '{}' is not fully covered by current data files.",
                    entry.index_file.file_name
                )));
            }
            if DataEvolutionIndexSourceMeta::is_data_evolution_meta(meta.source_meta.as_deref()) {
                let source = DataEvolutionIndexSourceMeta::deserialize(
                    meta.source_meta.as_deref().unwrap(),
                )?;
                let source_id = source.scan_snapshot_id();
                let latest = latest_snapshot.as_ref().ok_or_else(|| {
                    index_conflict("Global index source conflict: no snapshot exists.")
                })?;
                if source_id > latest.id() {
                    return Err(index_conflict("Global index source conflict: source snapshot is newer than the latest snapshot."));
                }
                // Missing/expired snapshots cannot prove that the indexed values are unchanged.
                if source_id != latest.id() {
                    self.snapshot_manager.get_snapshot(source_id).await?;
                }
                sources.push((source_id, entry.clone()));
            }
        }
        if sources.is_empty() {
            return Ok(());
        }
        self.check_index_column_changes(&sources, data_entries, None)
            .await?;
        let earliest = sources.iter().map(|(id, _)| *id).min().unwrap();
        let latest = latest_snapshot.as_ref().unwrap();
        for id in earliest + 1..=latest.id() {
            let snapshot = if id == latest.id() {
                latest.clone()
            } else {
                self.snapshot_manager.get_snapshot(id).await?
            };
            if snapshot.commit_kind() == &CommitKind::COMPACT {
                continue;
            }
            let changes = self.read_delta_entries(filter.as_ref(), &snapshot).await?;
            self.check_index_column_changes(&sources, &changes, Some(id))
                .await?;
        }
        Ok(())
    }

    async fn check_index_column_changes(
        &self,
        sources: &[(i64, IndexManifestEntry)],
        changes: &[ManifestEntry],
        snapshot_id: Option<i64>,
    ) -> Result<()> {
        for change in changes {
            let Some((start, end)) = change.file().row_id_range() else {
                continue;
            };
            let overlapping = sources
                .iter()
                .filter(|(source_id, index)| {
                    let meta = index.index_file.global_index_meta.as_ref().unwrap();
                    snapshot_id.is_none_or(|id| id > *source_id)
                        && same_index_partition(&index.partition, change.partition())
                        && index.bucket == change.bucket()
                        && ranges_overlap(start, end, meta.row_range_start, meta.row_range_end)
                })
                .map(|(_, index)| index)
                .collect::<Vec<_>>();
            if overlapping.is_empty() {
                continue;
            }
            let written_fields = self.write_field_ids(change.file()).await?;
            for index in overlapping {
                let meta = index.index_file.global_index_meta.as_ref().unwrap();
                if std::iter::once(&meta.index_field_id)
                    .chain(meta.extra_field_ids.iter().flatten())
                    .any(|id| written_fields.contains(id))
                {
                    return Err(index_conflict(format!(
                        "Global index source conflict: indexed values changed after building index file '{}'.",
                        index.index_file.file_name
                    )));
                }
            }
        }
        Ok(())
    }
}

fn index_conflict(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

fn partition_key(partition: &[u8]) -> Vec<u8> {
    if is_empty_partition(partition) {
        Vec::new()
    } else {
        partition.to_vec()
    }
}
