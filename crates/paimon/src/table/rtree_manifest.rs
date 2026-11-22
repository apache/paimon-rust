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

//! Validate the V1 manifest header before file pruning or overlap checks.
use crate::rtree::{Metadata, RTreeFileMeta};
use crate::spec::{FileKind, IndexFileMeta, IndexManifestEntry};
use crate::{Error, Result};
use std::collections::HashMap;

pub(super) fn file_metadata(file: &IndexFileMeta) -> Result<(RTreeFileMeta, Metadata)> {
    let invalid = || Error::DataInvalid {
        message: format!("Invalid R-tree manifest for {}", file.file_name),
        source: None,
    };
    let shard = file.global_index_meta.as_ref().ok_or_else(invalid)?;
    let rows = shard
        .row_range_end
        .checked_sub(shard.row_range_start)
        .and_then(|n| n.checked_add(1))
        .filter(|n| *n > 0 && shard.row_range_start >= 0)
        .ok_or_else(invalid)? as u64;
    if shard
        .extra_field_ids
        .as_ref()
        .is_some_and(|ids| !ids.is_empty())
    {
        return Err(invalid());
    }
    let header = shard
        .index_meta
        .as_deref()
        .ok_or_else(invalid)?
        .try_into()
        .map_err(|_| invalid())?;
    let parsed = Metadata::from_header(&header, rows).map_err(|e| Error::DataInvalid {
        message: format!("Invalid R-tree header for {}", file.file_name),
        source: Some(Box::new(e)),
    })?;
    if u64::try_from(file.file_size).ok() != Some(parsed.file_size())
        || (file.row_count >= 0 && file.row_count != parsed.row_count() as i64)
    {
        return Err(invalid());
    }
    Ok((
        RTreeFileMeta {
            name: file.file_name.clone(),
            file_size: parsed.file_size(),
            row_count: parsed.row_count(),
            header,
        },
        parsed,
    ))
}

/// Each R-tree file stores IDs relative to the same logical shard. Validate its
/// physical parts once and hand one representative to the regular overlap check.
/// This only groups new additions; a concurrent, already committed shard still conflicts.
pub(super) fn logical_additions(
    entries: &[IndexManifestEntry],
) -> Result<Vec<&IndexManifestEntry>> {
    let mut additions = Vec::new();
    let mut shards = HashMap::new();
    for entry in entries.iter().filter(|entry| entry.kind == FileKind::Add) {
        let file = &entry.index_file;
        if file.index_type != "rtree" {
            if file.global_index_meta.is_some() {
                additions.push(entry);
            }
            continue;
        }
        let (_, header) = file_metadata(file)?;
        let meta = file
            .global_index_meta
            .as_ref()
            .expect("validated R-tree manifest");
        let (first, parts) = shards
            .entry((
                meta.index_field_id,
                meta.row_range_start,
                meta.row_range_end,
            ))
            .or_insert_with(|| {
                additions.push(entry);
                (entry, Vec::new())
            });
        if entry.partition != first.partition
            || entry.bucket != first.bucket
            || first
                .index_file
                .global_index_meta
                .as_ref()
                .unwrap()
                .source_meta
                != meta.source_meta
        {
            return Err(Error::DataInvalid {
                message: format!("Conflicting R-tree source shards for {}", file.file_name),
                source: None,
            });
        }
        parts.push((header.first, header.last));
    }
    for (first, mut parts) in shards.into_values() {
        parts.sort_unstable();
        if parts.windows(2).any(|pair| pair[0].1 >= pair[1].0) {
            return Err(Error::DataInvalid {
                message: format!(
                    "Overlapping physical R-tree files in shard of {}",
                    first.index_file.file_name
                ),
                source: None,
            });
        }
    }
    Ok(additions)
}
