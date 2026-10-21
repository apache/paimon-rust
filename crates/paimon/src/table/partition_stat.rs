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

//! Per-partition statistics computed by scanning the selected snapshot's manifest entries.
//!
//! Mirrors what Java Paimon exposes via the `$partitions` system table for runtime introspection.

use std::collections::HashMap;

#[cfg(test)]
use crate::catalog::aggregate_partition_entries;
use crate::catalog::{list_partition_entries, FileSystemPartitionEntry};
#[cfg(test)]
use crate::spec::ManifestEntry;
use crate::spec::{CoreOptions, PartitionComputer};
use crate::table::Table;

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

impl Table {
    /// Compute per-partition statistics from the selected snapshot.
    ///
    /// Mirrors [`catalog::list_partitions_from_file_system`]: calls
    /// `merge_active_entries` first to deduplicate entries that appear in both
    /// the base and delta manifests, then aggregates the remaining live ADD
    /// files in a `BTreeMap` for a deterministic result order.
    ///
    /// **Warning:** This method reads all manifest lists and entries from the selected snapshot.
    /// For tables with a large number of manifests, this operation can be expensive.
    ///
    /// Returns an empty Vec when the table has no snapshots yet.
    pub async fn partition_stats(&self) -> crate::Result<Vec<PartitionStat>> {
        let schema = self.schema();
        let core = CoreOptions::new(schema.options());
        let computer = PartitionComputer::new(
            schema.partition_keys(),
            schema.fields(),
            core.partition_default_name(),
            core.legacy_partition_name(),
        )?;

        partition_stats_from_entries(list_partition_entries(self).await?, &computer)
    }

    /// List all partition values present in the selected snapshot.
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

fn partition_stats_from_entries(
    entries: Vec<FileSystemPartitionEntry>,
    computer: &PartitionComputer,
) -> crate::Result<Vec<PartitionStat>> {
    entries
        .into_iter()
        .map(|entry| {
            Ok(PartitionStat {
                partition: computer
                    .generate_part_values(&entry.partition)?
                    .into_iter()
                    .collect(),
                record_count: entry.record_count,
                file_count: entry.file_count as u64,
                total_size_bytes: entry.file_size_in_bytes as u64,
            })
        })
        .collect()
}

#[cfg(test)]
fn aggregate_partition_stats(
    entries: Vec<ManifestEntry>,
    computer: &PartitionComputer,
) -> crate::Result<Vec<PartitionStat>> {
    partition_stats_from_entries(aggregate_partition_entries(entries)?, computer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{DataFileMeta, FileKind, ManifestEntry};

    #[tokio::test]
    async fn test_query_auth_table_refuses_partition_stats_and_listing() {
        let table = crate::table::query_auth_table();
        for (err, what) in [
            (
                table.partition_stats().await.unwrap_err(),
                "partition stats",
            ),
            (
                table.list_partitions().await.unwrap_err(),
                "partition listing",
            ),
        ] {
            assert!(
                matches!(err, crate::Error::Unsupported { ref message }
                    if message.contains("query-auth.enabled")),
                "{what} on a query-auth.enabled table must fail closed, got {err:?}"
            );
        }
    }

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
            column_max_sequence_numbers: None,
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
