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

use super::{DELETION_VECTORS_INDEX_TYPE, INDEX_DIR};
use crate::deletion_vector::DeletionVectorFactory;
use crate::spec::{FileKind, IndexManifestEntry};
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
    for split in plan.splits() {
        let partition = split.partition().to_serialized_bytes();
        let bucket = split.bucket();
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
    for entry in index_entries {
        if entry.kind != FileKind::Add || entry.index_file.index_type != DELETION_VECTORS_INDEX_TYPE
        {
            continue;
        }
        let Some(dv_ranges) = entry.index_file.deletion_vectors_ranges.as_ref() else {
            continue;
        };
        let index_path = format!("{table_path}/{INDEX_DIR}/{}", entry.index_file.file_name);
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
            let deletion_file = DeletionFile::new(
                index_path.clone(),
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
