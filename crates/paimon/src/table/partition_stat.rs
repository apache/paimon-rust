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

use std::collections::{BTreeMap, HashMap};

use crate::io::FileIO;
use crate::spec::{
    avro::from_avro_bytes_fast, merge_active_entries, BinaryRow, CoreOptions, ManifestEntry,
    ManifestFileMeta, PartitionComputer, Snapshot,
};
use crate::table::SnapshotManager;
use crate::table::Table;

const MANIFEST_DIR: &str = "manifest";

/// Per-partition aggregated statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionStat {
    /// Partition key/value mapping (e.g. `{"dt": "2024-01-01", "hr": "10"}`).
    pub partition: HashMap<String, String>,
    /// Total record count across all live data files.
    pub record_count: i64,
    /// Live data file count.
    pub file_count: u64,
    /// Total bytes for live data files.
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
    /// Mirrors [`catalog::list_partitions_from_file_system`]: calls
    /// `merge_active_entries` first to deduplicate entries that appear in both
    /// the base and delta manifests, then aggregates the remaining live ADD
    /// files in a `BTreeMap` for a deterministic result order.
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

        aggregate_partition_stats(entries, &computer)
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

/// Aggregate live manifest entries into per-partition statistics.
///
/// Mirrors `catalog::list_partitions_from_file_system`:
/// 1. Call `merge_active_entries` to collapse ADD/DELETE pairs and remove
///    entries shadowed by a later DELETE, including duplicate ADD entries
///    that may appear in both base and delta manifests after compaction.
/// 2. Accumulate the remaining live ADD entries in a `BTreeMap` keyed by
///    raw partition bytes for a deterministic, sorted result order.
fn aggregate_partition_stats(
    entries: Vec<ManifestEntry>,
    computer: &PartitionComputer,
) -> crate::Result<Vec<PartitionStat>> {
    // Step 1: deduplicate — collapse ADD/DELETE pairs and drop files that have
    // been deleted. After this point every remaining entry is a live ADD.
    let live_entries = merge_active_entries(entries);

    // Step 2: accumulate into a BTreeMap for deterministic partition ordering.
    let mut grouped: BTreeMap<Vec<u8>, Accum> = BTreeMap::new();
    for entry in &live_entries {
        let file = entry.file();
        let accum = grouped.entry(entry.partition().to_vec()).or_default();
        accum.record_count += file.row_count;
        accum.file_count += 1;
        accum.total_size_bytes += file.file_size;
    }

    let mut out = Vec::with_capacity(grouped.len());
    for (partition_bytes, accum) in grouped {
        let partition = if partition_bytes.is_empty() {
            HashMap::new()
        } else {
            let row = BinaryRow::from_serialized_bytes(&partition_bytes)?;
            computer.generate_part_values(&row)?.into_iter().collect()
        };
        out.push(PartitionStat {
            partition,
            record_count: accum.record_count,
            file_count: accum.file_count as u64,
            total_size_bytes: accum.total_size_bytes as u64,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{DataFileMeta, FileKind, ManifestEntry};

    /// Build a minimal synthetic ManifestEntry for unit testing.
    /// Mirrors the helper used in `spec::manifest` tests.
    fn make_entry(
        kind: FileKind,
        partition: Vec<u8>,
        file_name: &str,
        row_count: i64,
    ) -> ManifestEntry {
        let stats = BinaryTableStats::empty();
        let file = DataFileMeta {
            file_name: file_name.to_string(),
            file_size: row_count * 100,
            row_count,
            min_key: vec![],
            max_key: vec![],
            key_stats: stats.clone(),
            value_stats: stats,
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
        };
        ManifestEntry::new(kind, partition, 0, 1, file, 2)
    }

    /// Duplicate ADD entries (same file appearing in both base and delta
    /// manifests after compaction) must not be double-counted.
    /// `merge_active_entries` collapses identical identifiers to one.
    #[test]
    fn test_duplicate_add_entries_are_not_double_counted() {
        let computer =
            PartitionComputer::new(&[] as &[String], &[], "__DEFAULT_PT__", false).unwrap();

        // Same file_name / level appears twice (base manifest + delta manifest).
        let entries = vec![
            make_entry(FileKind::Add, vec![], "file-001.parquet", 10),
            make_entry(FileKind::Add, vec![], "file-001.parquet", 10), // duplicate
        ];

        let stats = aggregate_partition_stats(entries, &computer).unwrap();

        // Must produce exactly 1 entry with record_count == 10, not 20.
        assert_eq!(stats.len(), 1, "expected 1 partition entry");
        assert_eq!(
            stats[0].record_count, 10,
            "duplicate ADD must be collapsed to a single count, not double-counted"
        );
        assert_eq!(stats[0].file_count, 1);
    }

    /// A DELETE entry that follows an ADD for the same file must cancel it
    /// out completely; the partition must not appear in the result.
    #[test]
    fn test_add_then_delete_removes_partition() {
        let computer =
            PartitionComputer::new(&[] as &[String], &[], "__DEFAULT_PT__", false).unwrap();

        let entries = vec![
            make_entry(FileKind::Add, vec![], "file-002.parquet", 5),
            make_entry(FileKind::Delete, vec![], "file-002.parquet", 5),
        ];

        let stats = aggregate_partition_stats(entries, &computer).unwrap();
        assert!(
            stats.is_empty(),
            "partition should be gone after its only file is deleted"
        );
    }
}
