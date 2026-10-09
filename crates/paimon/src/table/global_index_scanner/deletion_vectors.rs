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

//! Resolution of deletion-vector positions into global row ranges.

use super::DELETION_VECTORS_INDEX_TYPE;
use crate::deletion_vector::DeletionVectorFactory;
use crate::spec::{FileKind, IndexManifestEntry};
use crate::table::index_file_path::IndexFileLocation;
use crate::table::{merge_row_ranges, DeletionFile, RowRange, Table};
use crate::Result;
use std::collections::HashMap;

/// Resolve live deletion-vector index entries into global row-id ranges.
///
/// Data-evolution DV entries are keyed by the normal anchor data file. The DV
/// bitmap positions are local to that anchor file's `first_row_id`, so this
/// helper joins index metadata with live data-file metadata before converting
/// deleted positions to global row IDs.
pub(crate) async fn deleted_row_ranges_for_data_evolution_dvs(
    table: &Table,
    index_entries: &[IndexManifestEntry],
) -> Result<Vec<RowRange>> {
    if !index_entries.iter().any(|entry| {
        entry.kind == FileKind::Add && entry.index_file.index_type == DELETION_VECTORS_INDEX_TYPE
    }) {
        return Ok(Vec::new());
    }

    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await?;

    let mut first_row_ids: HashMap<(Vec<u8>, i32, String), i64> = HashMap::new();
    // A deletion vector is an index file, so it may live beside its bucket's data
    // files. Capture each bucket's directory from the plan rather than rebuilding
    // it, so custom data directories are honored.
    let mut bucket_paths: HashMap<(Vec<u8>, i32), String> = HashMap::new();
    for split in plan.splits() {
        let partition = split.partition().to_serialized_bytes();
        let bucket = split.bucket();
        bucket_paths
            .entry((partition.clone(), bucket))
            .or_insert_with(|| split.bucket_path().to_string());
        for file in split.data_files() {
            if let Some(first_row_id) = file.first_row_id {
                first_row_ids.insert(
                    (partition.clone(), bucket, file.file_name.clone()),
                    first_row_id,
                );
            }
        }
    }

    let mut ranges = Vec::new();
    let table_path = table.location().trim_end_matches('/');
    let index_file_in_data_file_dir = table.schema().core_options().index_file_in_data_file_dir();
    for entry in index_entries {
        if entry.kind != FileKind::Add || entry.index_file.index_type != DELETION_VECTORS_INDEX_TYPE
        {
            continue;
        }
        let Some(dv_ranges) = entry.index_file.deletion_vectors_ranges.as_ref() else {
            continue;
        };
        // A deletion vector is resolved against the bucket that owns it; a bucket
        // with no captured directory has no live split, and the row-id join below
        // rejects every data file in the entry before any path is needed.
        let bucket_path = bucket_paths.get(&(entry.partition.clone(), entry.bucket));
        for (data_file_name, meta) in dv_ranges {
            let key = (
                entry.partition.clone(),
                entry.bucket,
                data_file_name.clone(),
            );
            let first_row_id = first_row_ids.get(&key).copied().ok_or_else(|| {
                crate::Error::DataInvalid {
                    message: format!(
                        "Deletion vector references data file '{}' but no live row-tracked file was found",
                        data_file_name
                    ),
                    source: None,
                }
            })?;
            // The join above found a live row-tracked file in this bucket, so the
            // loop over the plan captured its directory.
            let bucket_path = bucket_path.ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "no bucket directory captured for deletion vector '{}'",
                    entry.index_file.file_name
                ),
                source: None,
            })?;
            let index_path = IndexFileLocation::BucketLocal {
                table_path,
                bucket_path,
                index_file_in_data_file_dir,
            }
            .resolve(
                &entry.index_file.file_name,
                entry.index_file.external_path.as_deref(),
            );
            let deletion_file = DeletionFile::new(
                index_path,
                meta.offset as i64,
                meta.length as i64,
                meta.cardinality,
            );
            let deletion_vector =
                DeletionVectorFactory::read(table.file_io(), &deletion_file).await?;
            for deleted in deletion_vector.iter() {
                let deleted = i64::try_from(deleted).map_err(|_| crate::Error::DataInvalid {
                    message: format!(
                        "Deleted position {deleted} for data file '{}' exceeds i64::MAX",
                        data_file_name
                    ),
                    source: None,
                })?;
                let row_id =
                    first_row_id
                        .checked_add(deleted)
                        .ok_or_else(|| crate::Error::DataInvalid {
                            message: format!(
                                "Deleted row id overflows i64 for data file '{}'",
                                data_file_name
                            ),
                            source: None,
                        })?;
                ranges.push(RowRange::new(row_id, row_id));
            }
        }
    }

    Ok(merge_row_ranges(ranges))
}
