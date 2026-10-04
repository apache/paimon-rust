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

//! Row tracking metadata assignment, following Java RowTrackingCommitUtils.

use std::collections::HashMap;

use super::{is_blob_data_file, is_vector_store_file};
use crate::spec::{FileKind, ManifestEntry};
use crate::Result;

/// Group partitions without changing the file order within a partition. This
/// keeps each partition's new row IDs contiguous and preserves Blob alignment.
pub(super) fn group_by_partition(entries: Vec<ManifestEntry>) -> Vec<ManifestEntry> {
    let mut partitions = indexmap::IndexMap::<Vec<u8>, Vec<ManifestEntry>>::new();
    for entry in entries {
        partitions
            .entry(entry.partition().to_vec())
            .or_default()
            .push(entry);
    }
    partitions.into_values().flatten().collect()
}

/// Zero is the pending-snapshot sentinel. A rewritten file can contain both
/// unmodified and modified records, so its minimum must survive the commit.
fn assign_snapshot_id(snapshot_id: i64, entry: ManifestEntry) -> ManifestEntry {
    let min = entry.file().min_sequence_number;
    let max = entry.file().max_sequence_number;
    if min == 0 {
        entry.with_sequence_number(snapshot_id, snapshot_id)
    } else if max == 0 {
        entry.with_sequence_number(min, snapshot_id)
    } else {
        entry
    }
}

/// Assign row tracking metadata: snapshot ID as sequence number, and
/// first_row_id for new APPEND files that don't already have one.
/// Normal files advance the main counter. Blob files (identified by file name)
/// use per-column counters starting from the same base, since each blob column
/// rolls independently.
pub(super) fn assign_row_tracking(
    snapshot_id: i64,
    first_row_id_start: i64,
    entries: Vec<ManifestEntry>,
) -> Result<(Vec<ManifestEntry>, i64)> {
    let mut result = Vec::with_capacity(entries.len());
    let mut start = first_row_id_start;
    let mut blob_start_default = first_row_id_start;
    let mut blob_starts: HashMap<String, i64> = HashMap::new();
    let mut vector_store_start = first_row_id_start;

    for entry in entries {
        let mut entry = assign_snapshot_id(snapshot_id, entry);
        if entry.file().file_source.is_none() {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "file_source must be present for row-tracking table, file={}",
                    entry.file().file_name
                ),
                source: None,
            });
        }
        let contains_row_id = entry
            .file()
            .write_cols
            .as_ref()
            .is_some_and(|cols| cols.iter().any(|col| col == crate::spec::ROW_ID_FIELD_NAME));
        if *entry.kind() == FileKind::Add
            && entry.file().file_source == Some(0) // APPEND
            && entry.file().first_row_id.is_none()
            && !contains_row_id
        {
            if is_blob_data_file(entry.file()) {
                let blob_field_name = entry
                    .file()
                    .write_cols
                    .as_ref()
                    .and_then(|cols| cols.first())
                    .cloned()
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: format!(
                            "Blob file '{}' must have write_cols for row-tracking assignment.",
                            entry.file().file_name
                        ),
                        source: None,
                    })?;
                let blob_start = blob_starts
                    .entry(blob_field_name)
                    .or_insert(blob_start_default);
                if *blob_start >= start {
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "This is a bug, blobStart {} should be less than start {} when assigning a blob entry file.",
                            *blob_start, start
                        ),
                        source: None,
                    });
                }
                entry = entry.with_first_row_id(*blob_start);
                *blob_start += entry.file().row_count;
            } else if is_vector_store_file(entry.file()) {
                if vector_store_start >= start {
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "This is a bug, vectorStoreStart {} should be less than start {} when assigning a vector-store entry file.",
                            vector_store_start, start
                        ),
                        source: None,
                    });
                }
                entry = entry.with_first_row_id(vector_store_start);
                vector_store_start += entry.file().row_count;
            } else {
                entry = entry.with_first_row_id(start);
                blob_start_default = start;
                blob_starts.clear();
                start += entry.file().row_count;
            }
        }
        result.push(entry);
    }

    Ok((result, start))
}
