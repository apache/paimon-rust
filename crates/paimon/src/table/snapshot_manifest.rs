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

use super::partition_filter::PartitionFilter;
use super::SnapshotManager;
use crate::spec::{
    merge_active_entries, FileKind, Manifest, ManifestEntry, ManifestList, Snapshot,
};

/// Read active metadata without read-side bucket, level, column, or index pruning.
/// Partition mutations must also see pending files and dedicated column files.
pub(super) async fn read_snapshot_entries(
    manager: &SnapshotManager,
    snapshot: &Snapshot,
    partition_filter: Option<&PartitionFilter>,
) -> crate::Result<Vec<ManifestEntry>> {
    Ok(merge_active_entries(
        read_snapshot_log(manager, snapshot, partition_filter).await?,
    ))
}

/// Java postpone replay preserves manifest encounter order for equal timestamps.
/// Ordinary scans keep their existing unordered metadata representation.
pub(super) async fn read_snapshot_entries_in_manifest_order(
    manager: &SnapshotManager,
    snapshot: &Snapshot,
    partition_filter: Option<&PartitionFilter>,
) -> crate::Result<Vec<ManifestEntry>> {
    let mut positions = std::collections::HashMap::new();
    let mut active = Vec::new();
    for entry in read_snapshot_log(manager, snapshot, partition_filter).await? {
        let id = entry.identifier();
        match entry.kind() {
            FileKind::Add => {
                if let Some(&index) = positions.get(&id) {
                    active[index] = Some(entry);
                } else {
                    positions.insert(id, active.len());
                    active.push(Some(entry));
                }
            }
            FileKind::Delete => {
                if let Some(index) = positions.remove(&id) {
                    active[index] = None;
                }
            }
        }
    }
    Ok(active.into_iter().flatten().collect())
}

async fn read_snapshot_log(
    manager: &SnapshotManager,
    snapshot: &Snapshot,
    partition_filter: Option<&PartitionFilter>,
) -> crate::Result<Vec<ManifestEntry>> {
    let file_io = manager.file_io();
    let mut entries = Vec::new();
    for list in [
        snapshot.base_manifest_list(),
        snapshot.delta_manifest_list(),
    ] {
        for manifest in ManifestList::read(file_io, &manager.manifest_path(list)).await? {
            for entry in
                Manifest::read(file_io, &manager.manifest_path(manifest.file_name())).await?
            {
                if let Some(filter) = partition_filter {
                    if !filter.matches_entry(entry.partition())? {
                        continue;
                    }
                }
                entries.push(entry);
            }
        }
    }
    Ok(entries)
}
