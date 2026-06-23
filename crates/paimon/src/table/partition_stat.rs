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

//! Per-partition statistics computed by scanning the latest snapshot's manifest entries.
//!
//! Mirrors what Java Paimon exposes via the `$partitions` system table for runtime introspection.

use std::collections::HashMap;

use crate::io::FileIO;
use crate::spec::{
    avro::from_avro_bytes_fast, BinaryRow, CoreOptions, FileKind, ManifestEntry, ManifestFileMeta,
    PartitionComputer, Snapshot,
};
use crate::table::SnapshotManager;
use crate::table::Table;

const MANIFEST_DIR: &str = "manifest";

/// Per-partition aggregated statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionStat {
    /// Partition key/value mapping (e.g. `{"dt": "2024-01-01", "hr": "10"}`).
    pub partition: HashMap<String, String>,
    /// Net record count (added rows minus deleted rows) across all live data files.
    pub record_count: i64,
    /// Net data file count (additions minus deletions).
    pub file_count: u64,
    /// Net total bytes for live data files.
    pub total_size_bytes: u64,
}

#[derive(Default)]
struct Accum {
    record_count: i64,
    file_count: i64,
    total_size_bytes: i64,
}

impl Table {
    /// Compute per-partition statistics from the latest snapshot.
    ///
    /// **Warning:** This method reads all manifest lists and entries from the latest snapshot.
    /// For tables with a large number of manifests, this operation can be expensive.
    ///
    /// Returns an empty Vec when the table has no snapshots yet.
    pub async fn partition_stats(&self) -> crate::Result<Vec<PartitionStat>> {
        let sm = SnapshotManager::new(self.file_io().clone(), self.location().to_string());
        let snapshot = match sm.get_latest_snapshot().await? {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        let entries = read_all_manifest_entries(self.file_io(), self.location(), &snapshot).await?;

        let schema = self.schema();
        let core = CoreOptions::new(schema.options());
        let computer = PartitionComputer::new(
            schema.partition_keys(),
            schema.fields(),
            core.partition_default_name(),
            core.legacy_partition_name(),
        )?;

        aggregate_partition_stats(&entries, &computer)
    }

    /// List all partition values present in the latest snapshot.
    ///
    /// **Warning:** This method computes partition statistics which reads all manifest lists
    /// and entries. For large tables, this operation can be expensive.
    ///
    /// Returns an empty Vec when the table has no snapshots yet.
    pub async fn list_partitions(&self) -> crate::Result<Vec<HashMap<String, String>>> {
        Ok(self
            .partition_stats()
            .await?
            .into_iter()
            .map(|s| s.partition)
            .collect())
    }
}

async fn read_manifest_list(
    file_io: &FileIO,
    table_path: &str,
    list_name: &str,
) -> crate::Result<Vec<ManifestFileMeta>> {
    if list_name.is_empty() {
        return Ok(Vec::new());
    }
    let path = format!(
        "{}/{}/{}",
        table_path.trim_end_matches('/'),
        MANIFEST_DIR,
        list_name
    );
    let input = file_io.new_input(&path)?;
    let bytes = input.read().await?;
    from_avro_bytes_fast::<ManifestFileMeta>(&bytes)
}

async fn read_all_manifest_entries(
    file_io: &FileIO,
    table_path: &str,
    snapshot: &Snapshot,
) -> crate::Result<Vec<ManifestEntry>> {
    let mut metas = read_manifest_list(file_io, table_path, snapshot.base_manifest_list()).await?;
    metas.extend(read_manifest_list(file_io, table_path, snapshot.delta_manifest_list()).await?);

    let manifest_dir = format!("{}/{}", table_path.trim_end_matches('/'), MANIFEST_DIR);
    let mut all_entries = Vec::new();
    for meta in metas {
        let path = format!("{}/{}", manifest_dir, meta.file_name());
        let input = file_io.new_input(&path)?;
        let bytes = input.read().await?;
        let entries = from_avro_bytes_fast::<ManifestEntry>(&bytes)?;
        all_entries.extend(entries);
    }
    Ok(all_entries)
}

fn aggregate_partition_stats(
    entries: &[ManifestEntry],
    computer: &PartitionComputer,
) -> crate::Result<Vec<PartitionStat>> {
    let mut grouped: HashMap<Vec<u8>, Accum> = HashMap::new();
    for entry in entries {
        let bucket = grouped.entry(entry.partition().to_vec()).or_default();
        let file = entry.file();
        let sign: i64 = match entry.kind() {
            FileKind::Add => 1,
            FileKind::Delete => -1,
        };
        bucket.record_count += sign * file.row_count;
        bucket.file_count += sign;
        bucket.total_size_bytes += sign * file.file_size;
    }

    let mut out = Vec::with_capacity(grouped.len());
    for (partition_bytes, accum) in grouped {
        if accum.file_count <= 0 {
            // Partition has been fully deleted in this snapshot.
            continue;
        }
        let partition = if partition_bytes.is_empty() {
            HashMap::new()
        } else {
            let row = BinaryRow::from_serialized_bytes(&partition_bytes)?;
            computer.generate_part_values(&row)?.into_iter().collect()
        };
        out.push(PartitionStat {
            partition,
            record_count: accum.record_count.max(0),
            file_count: accum.file_count.max(0) as u64,
            total_size_bytes: accum.total_size_bytes.max(0) as u64,
        });
    }
    Ok(out)
}
