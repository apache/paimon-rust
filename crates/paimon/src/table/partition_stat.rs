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
    avro::from_avro_bytes_fast, BinaryRow, DataField, DataType, FileKind, ManifestEntry,
    ManifestFileMeta, Snapshot,
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
        let partition_keys = self.schema().partition_keys();
        let partition_fields = self.schema().partition_fields();

        aggregate_partition_stats(&entries, partition_keys, &partition_fields)
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
    partition_keys: &[String],
    partition_fields: &[DataField],
) -> crate::Result<Vec<PartitionStat>> {
    let mut grouped: HashMap<Vec<u8>, Accum> = HashMap::new();
    for entry in entries {
        let bucket = grouped.entry(entry.partition().to_vec()).or_default();
        let file = entry.file();
        let live_rows = file.row_count - file.delete_row_count.unwrap_or(0);
        let sign: i64 = match entry.kind() {
            FileKind::Add => 1,
            FileKind::Delete => -1,
        };
        bucket.record_count += sign * live_rows;
        bucket.file_count += sign;
        bucket.total_size_bytes += sign * file.file_size;
    }

    let mut out = Vec::with_capacity(grouped.len());
    for (partition_bytes, accum) in grouped {
        if accum.file_count <= 0 {
            // Partition has been fully deleted in this snapshot.
            continue;
        }
        let partition = decode_partition(&partition_bytes, partition_keys, partition_fields)?;
        out.push(PartitionStat {
            partition,
            record_count: accum.record_count.max(0),
            file_count: accum.file_count.max(0) as u64,
            total_size_bytes: accum.total_size_bytes.max(0) as u64,
        });
    }
    Ok(out)
}

fn decode_partition(
    bytes: &[u8],
    keys: &[String],
    fields: &[DataField],
) -> crate::Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    if keys.is_empty() {
        return Ok(map);
    }
    let row = BinaryRow::from_serialized_bytes(bytes)?;
    for (i, key) in keys.iter().enumerate() {
        let dt = fields
            .iter()
            .find(|f| f.name() == key)
            .map(|f| f.data_type());
        let value = if row.is_null_at(i) {
            "null".to_string()
        } else {
            partition_value_to_string(&row, i, dt)
        };
        map.insert(key.clone(), value);
    }
    Ok(map)
}

fn partition_value_to_string(row: &BinaryRow, pos: usize, dt: Option<&DataType>) -> String {
    let pos_i = pos;
    match dt {
        Some(DataType::TinyInt(_)) => row.get_byte(pos_i).map(|v| v.to_string()),
        Some(DataType::SmallInt(_)) => row.get_short(pos_i).map(|v| v.to_string()),
        Some(DataType::Int(_)) | Some(DataType::Date(_)) => {
            row.get_int(pos_i).map(|v| v.to_string())
        }
        Some(DataType::BigInt(_)) => row.get_long(pos_i).map(|v| v.to_string()),
        Some(DataType::Boolean(_)) => row.get_boolean(pos_i).map(|v| v.to_string()),
        Some(DataType::Float(_)) => row.get_float(pos_i).map(|v| v.to_string()),
        Some(DataType::Double(_)) => row.get_double(pos_i).map(|v| v.to_string()),
        Some(DataType::Char(_)) | Some(DataType::VarChar(_)) => {
            row.get_string(pos_i).map(|v| v.to_string())
        }
        Some(DataType::Binary(_)) | Some(DataType::VarBinary(_)) => {
            row.get_binary(pos_i).map(hex::encode)
        }
        _ => row.get_string(pos_i).map(|v| v.to_string()),
    }
    .unwrap_or_else(|_| "?".to_string())
}
