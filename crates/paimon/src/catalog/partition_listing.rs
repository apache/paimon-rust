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

use std::collections::{BTreeMap, HashMap};

use crate::spec::{BinaryRow, CoreOptions, Manifest, ManifestList, Partition, PartitionComputer};
use crate::table::{SnapshotManager, Table};
use crate::Result;

/// Scan a table's manifest entries and aggregate them into [`Partition`] rows,
/// matching the shape catalogs would otherwise return from a metastore.
pub async fn list_partitions_from_file_system(table: &Table) -> Result<Vec<Partition>> {
    let file_io = table.file_io();
    let sm = SnapshotManager::new(file_io.clone(), table.location().to_string());
    let snapshot = match sm.get_latest_snapshot().await? {
        Some(s) => s,
        None => return Ok(Vec::new()),
    };

    let base_path = sm.manifest_path(snapshot.base_manifest_list());
    let delta_path = sm.manifest_path(snapshot.delta_manifest_list());
    let (base_metas, delta_metas) = futures::try_join!(
        ManifestList::read(file_io, &base_path),
        ManifestList::read(file_io, &delta_path),
    )?;

    let mut all_entries = Vec::new();
    for meta in base_metas.into_iter().chain(delta_metas) {
        let manifest_path = sm.manifest_path(meta.file_name());
        let entries = Manifest::read(file_io, &manifest_path).await?;
        all_entries.extend(entries);
    }

    let schema = table.schema();
    let core = CoreOptions::new(schema.options());
    let computer = PartitionComputer::new(
        schema.partition_keys(),
        schema.fields(),
        core.partition_default_name(),
        core.legacy_partition_name(),
    )?;

    #[derive(Default)]
    struct Agg {
        record_count: i64,
        file_size: i64,
        file_count: i64,
        last_file_creation_time: i64,
    }
    let mut buckets: BTreeMap<Vec<u8>, Agg> = BTreeMap::new();
    for entry in &all_entries {
        let file = entry.file();
        let agg = buckets.entry(entry.partition().to_vec()).or_default();
        agg.record_count += file.row_count;
        agg.file_size += file.file_size;
        agg.file_count += 1;
        if let Some(ct) = file.creation_time {
            agg.last_file_creation_time = agg.last_file_creation_time.max(ct.timestamp_millis());
        }
    }

    let mut result = Vec::with_capacity(buckets.len());
    for (bytes, agg) in buckets {
        let spec = if bytes.is_empty() {
            HashMap::new()
        } else {
            let row = BinaryRow::from_serialized_bytes(&bytes)?;
            computer.generate_part_values(&row)?.into_iter().collect()
        };
        result.push(Partition {
            spec,
            record_count: agg.record_count,
            file_size_in_bytes: agg.file_size,
            file_count: agg.file_count,
            last_file_creation_time: agg.last_file_creation_time,
            total_buckets: 0,
            done: false,
            created_at: None,
            created_by: None,
            updated_at: None,
            updated_by: None,
            options: None,
        });
    }
    Ok(result)
}
