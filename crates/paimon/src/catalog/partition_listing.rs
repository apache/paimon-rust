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

//! Mirrors Java [CatalogUtils.listPartitionsFromFileSystem](https://github.com/apache/paimon/blob/release-1.4/paimon-core/src/main/java/org/apache/paimon/catalog/CatalogUtils.java).
//!
//! Used as the catalog-side fallback when a backend doesn't track partitions
//! (e.g. `FileSystemCatalog`, or `RESTCatalog` against a non-metastore server).

use std::collections::BTreeMap;

use crate::spec::{
    merge_active_entries, BinaryRow, CoreOptions, Manifest, ManifestEntry, ManifestList, Partition,
    PartitionComputer,
};
use crate::table::Table;
use crate::Result;

/// Scan a table's manifest entries and aggregate them into [`Partition`] rows,
/// matching the shape catalogs would otherwise return from a metastore.
pub async fn list_partitions_from_file_system(table: &Table) -> Result<Vec<Partition>> {
    let schema = table.schema();
    let core = CoreOptions::new(schema.options());
    let computer = PartitionComputer::new(
        schema.partition_keys(),
        schema.fields(),
        core.partition_default_name(),
        core.legacy_partition_name(),
    )?;
    list_partition_entries(table)
        .await?
        .into_iter()
        .map(|entry| {
            let spec = computer
                .generate_part_values(&entry.partition)?
                .into_iter()
                .collect();
            Ok(Partition {
                spec,
                record_count: entry.record_count,
                file_size_in_bytes: entry.file_size_in_bytes,
                file_count: entry.file_count,
                last_file_creation_time: entry.last_file_creation_time.unwrap_or(0),
                total_buckets: entry.total_buckets,
                done: false,
                created_at: None,
                created_by: None,
                updated_at: None,
                updated_by: None,
                options: None,
            })
        })
        .collect()
}

/// Typed file statistics shared by filesystem catalog listings and metadata reads.
#[derive(Debug)]
pub(crate) struct FileSystemPartitionEntry {
    pub partition: BinaryRow,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub file_count: i64,
    // Epoch instant, matching Java DataFileMeta.creationTimeEpochMillis.
    pub last_file_creation_time: Option<i64>,
    pub total_buckets: i32,
}

impl Default for FileSystemPartitionEntry {
    fn default() -> Self {
        Self {
            partition: BinaryRow::from_datums(&[]),
            record_count: 0,
            file_size_in_bytes: 0,
            file_count: 0,
            last_file_creation_time: None,
            total_buckets: 0,
        }
    }
}

pub(crate) async fn list_partition_entries(table: &Table) -> Result<Vec<FileSystemPartitionEntry>> {
    CoreOptions::new(table.schema().options()).ensure_read_authorized()?;
    let Some(snapshot) = table.resolve_read_snapshot().await? else {
        return Ok(Vec::new());
    };
    let file_io = table.file_io();
    let manager = table.snapshot_manager();
    let base_path = manager.manifest_path(snapshot.base_manifest_list());
    let delta_path = manager.manifest_path(snapshot.delta_manifest_list());
    let (base, delta) = futures::try_join!(
        ManifestList::read(file_io, &base_path),
        ManifestList::read(file_io, &delta_path),
    )?;
    let mut entries = Vec::new();
    for meta in base.into_iter().chain(delta) {
        entries.extend(Manifest::read(file_io, &manager.manifest_path(meta.file_name())).await?);
    }
    aggregate_partition_entries(entries)
}

pub(crate) fn aggregate_partition_entries(
    entries: Vec<ManifestEntry>,
) -> Result<Vec<FileSystemPartitionEntry>> {
    aggregate_partition_entries_in_zone(entries, &jiff::tz::TimeZone::system())
}

fn aggregate_partition_entries_in_zone(
    entries: Vec<ManifestEntry>,
    zone: &jiff::tz::TimeZone,
) -> Result<Vec<FileSystemPartitionEntry>> {
    let mut grouped: BTreeMap<Vec<u8>, FileSystemPartitionEntry> = BTreeMap::new();
    for entry in merge_active_entries(entries) {
        let file = entry.file();
        let agg = grouped.entry(entry.partition().to_vec()).or_default();
        agg.record_count = agg.record_count.wrapping_add(file.row_count);
        agg.file_size_in_bytes = agg.file_size_in_bytes.wrapping_add(file.file_size);
        agg.file_count += 1;
        agg.total_buckets = entry.total_buckets();
        if let Some(time) = file.creation_time_epoch_millis_in_zone(zone)? {
            agg.last_file_creation_time = Some(
                agg.last_file_creation_time
                    .map_or(time, |old| old.max(time)),
            );
        }
    }
    grouped
        .into_iter()
        .map(|(bytes, mut entry)| {
            entry.partition = if bytes.is_empty() {
                BinaryRow::from_datums(&[])
            } else {
                BinaryRow::from_serialized_bytes(&bytes)?
            };
            Ok(entry)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creation_times_are_compared_as_instants_across_dst_gaps() {
        use crate::spec::{stats::BinaryTableStats, DataFileMeta, FileKind};
        let entries = ["2024-03-10T02:55:00Z", "2024-03-10T03:10:00Z"]
            .into_iter()
            .map(|time| {
                let file = DataFileMeta {
                    file_name: time.into(),
                    file_size: 10,
                    row_count: 1,
                    min_key: vec![],
                    max_key: vec![],
                    key_stats: BinaryTableStats::empty(),
                    value_stats: BinaryTableStats::empty(),
                    min_sequence_number: 0,
                    max_sequence_number: 0,
                    schema_id: 0,
                    level: 0,
                    extra_files: vec![],
                    creation_time: Some(time.parse().unwrap()),
                    delete_row_count: None,
                    embedded_index: None,
                    file_source: None,
                    value_stats_cols: None,
                    external_path: None,
                    first_row_id: None,
                    write_cols: None,
                    column_max_sequence_numbers: None,
                };
                ManifestEntry::new(
                    FileKind::Add,
                    BinaryRow::from_datums(&[]).to_serialized_bytes(),
                    0,
                    8,
                    file,
                    2,
                )
            })
            .collect();
        let rows = aggregate_partition_entries_in_zone(
            entries,
            &jiff::tz::TimeZone::get("America/New_York").unwrap(),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].record_count, 2);
        assert_eq!(rows[0].total_buckets, 8);
        // 02:55 is shifted through the gap to 03:55 EDT (07:55 UTC), later than 03:10 EDT.
        assert_eq!(rows[0].last_file_creation_time, Some(1_710_057_300_000));
    }

    #[tokio::test]
    async fn test_query_auth_table_refuses_partition_listing() {
        let table = crate::table::query_auth_table();
        let err = list_partitions_from_file_system(&table).await.unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message }
                if message.contains("query-auth.enabled")),
            "listing partitions on a query-auth.enabled table must fail closed, got {err:?}"
        );
    }
}
