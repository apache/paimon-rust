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

//! Shared helpers for the global-index build builders (btree, lumina, vindex).
//!
//! Mirrors Java `GlobalIndexBuilderUtils`, which exposes a single
//! `indexedRowRanges` used by both the sorted and vector index builders. The
//! gap computation (what is already indexed for a given field, so a build only
//! covers the new rows) is identical across index types apart from the
//! `index_type` string, so it lives here once rather than being copied into
//! each builder.

use crate::spec::{FileKind, IndexManifest};
use crate::table::{merge_row_ranges, RowRange, Table};
use crate::{Error, Result};

pub(crate) mod vector;

/// Java `sameExtraFieldIds`: null/empty are equal; otherwise exact ordered equality.
pub(crate) fn same_extra_field_ids(a: Option<&[i32]>, b: Option<&[i32]>) -> bool {
    let a = a.unwrap_or(&[]);
    let b = b.unwrap_or(&[]);
    a == b
}

/// Inclusive `[start, end]` range overlap.
fn ranges_overlap(a_start: i64, a_end: i64, b_start: i64, b_end: i64) -> bool {
    a_start <= b_end && b_start <= a_end
}

/// Row ranges already covered by `index_type` global-index files for
/// `index_field_id` (and matching `extra_field_ids`). Mirrors Java
/// `GlobalIndexBuilderUtils.indexedRowRanges`.
pub(crate) async fn indexed_row_ranges(
    table: &Table,
    index_manifest_name: Option<&str>,
    index_type: &str,
    index_field_id: i32,
    extra_field_ids: Option<&[i32]>,
) -> Result<Vec<RowRange>> {
    let Some(index_manifest_name) = index_manifest_name else {
        return Ok(Vec::new());
    };
    let path = format!(
        "{}/manifest/{}",
        table.location().trim_end_matches('/'),
        index_manifest_name
    );
    let entries = IndexManifest::read(table.file_io(), &path).await?;
    let mut ranges = Vec::new();
    for entry in entries {
        if entry.kind != FileKind::Add || entry.index_file.index_type != index_type {
            continue;
        }
        let Some(meta) = entry.index_file.global_index_meta else {
            continue;
        };
        if meta.index_field_id != index_field_id
            || !same_extra_field_ids(meta.extra_field_ids.as_deref(), extra_field_ids)
        {
            continue;
        }
        ranges.push(RowRange::new(meta.row_range_start, meta.row_range_end));
    }
    Ok(merge_row_ranges(ranges))
}

/// Guard against building a global index over a row range that an existing index
/// file of the SAME identity already covers. Identity is the full
/// `(index_type, index_field_id, extra_field_ids)` tuple -- matching
/// `indexed_row_ranges` and Java `GlobalIndexIdentifier` -- so a different index
/// type (or different `extra_field_ids`) on the same field coexists. Two files
/// of the same identity with overlapping ranges are still rejected.
pub(crate) async fn validate_existing_index_overlap(
    table: &Table,
    index_manifest_name: Option<&str>,
    index_type: &str,
    index_field_id: i32,
    extra_field_ids: Option<&[i32]>,
    planned: &[RowRange],
) -> Result<()> {
    let Some(index_manifest_name) = index_manifest_name else {
        return Ok(());
    };
    let path = format!(
        "{}/manifest/{}",
        table.location().trim_end_matches('/'),
        index_manifest_name
    );
    let entries = IndexManifest::read(table.file_io(), &path).await?;
    for entry in entries {
        if entry.kind != FileKind::Add || entry.index_file.index_type != index_type {
            continue;
        }
        let Some(meta) = entry.index_file.global_index_meta else {
            continue;
        };
        if meta.index_field_id != index_field_id
            || !same_extra_field_ids(meta.extra_field_ids.as_deref(), extra_field_ids)
        {
            continue;
        }
        if planned
            .iter()
            .any(|r| ranges_overlap(meta.row_range_start, meta.row_range_end, r.from(), r.to()))
        {
            return Err(Error::DataInvalid {
                message: format!(
                    "Existing global index file '{}' overlaps requested row range for field {}",
                    entry.index_file.file_name, index_field_id
                ),
                source: None,
            });
        }
    }
    Ok(())
}
