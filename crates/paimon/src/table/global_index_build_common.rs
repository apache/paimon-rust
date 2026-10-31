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

//! Shared helpers for the global-index build builders (btree, full-text,
//! vindex).
//!
//! Mirrors Java `GlobalIndexBuilderUtils`, which exposes a single
//! `indexedRowRanges` used by both the sorted and vector index builders. The
//! gap computation (what is already indexed for a given field, so a build only
//! covers the new rows) is identical across index types apart from the
//! `index_type` string, so it lives here once rather than being copied into
//! each builder.

use crate::spec::{DataField, FileKind, IndexManifest, IndexManifestEntry, Snapshot};
use crate::table::{merge_row_ranges, RowRange, Table};
use crate::{Error, Result};
use std::sync::Arc;

pub(crate) mod preparation;
pub(crate) mod vector;

/// One source snapshot and its effective index coverage. Builders use this same
/// immutable view for both gap planning and overlap validation.
#[derive(Clone, Default)]
pub(crate) struct GlobalIndexBuildView {
    snapshot: Option<Snapshot>,
    index_entries: Arc<[IndexManifestEntry]>,
}

impl GlobalIndexBuildView {
    pub(crate) async fn capture(table: &Table, snapshot: Option<Snapshot>) -> Result<Self> {
        let Some(snapshot) = snapshot else {
            return Ok(Self::default());
        };
        let entries = match snapshot.index_manifest() {
            Some(name) => {
                IndexManifest::read(
                    table.file_io(),
                    &table.snapshot_manager().manifest_path(name),
                )
                .await?
            }
            None => vec![],
        };
        Ok(Self {
            snapshot: Some(snapshot),
            index_entries: entries.into(),
        })
    }

    /// An empty captured view validates configuration without reading metadata.
    pub(super) async fn resolve(table: &Table, captured: Option<&Self>) -> Result<Self> {
        match captured {
            Some(view) => Ok(view.clone()),
            None => {
                Self::capture(table, table.snapshot_manager().get_latest_snapshot().await?).await
            }
        }
    }

    pub(crate) fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    pub(crate) fn index_entries(&self) -> &[IndexManifestEntry] {
        &self.index_entries
    }

    pub(crate) fn filter_indexes(&self, mut keep: impl FnMut(&IndexManifestEntry) -> bool) -> Self {
        Self {
            snapshot: self.snapshot.clone(),
            index_entries: self
                .index_entries
                .iter()
                .filter(|entry| keep(entry))
                .cloned()
                .collect(),
        }
    }
}

/// Validate the table that owns global row IDs. Procedure options only tune
/// index generation; they cannot enable row tracking or change bucket layout.
/// Data evolution and `global-index.enabled` govern reads, not index creation.
pub(crate) fn validate_build_table(table: &Table) -> Result<()> {
    let options = table.schema().core_options();
    if !table.schema().primary_keys().is_empty() {
        return Err(Error::Unsupported {
            message: "Global index build does not support primary-key tables; use primary-key file indexes instead".into(),
        });
    }
    if !options.row_tracking_enabled() {
        return Err(Error::DataInvalid {
            message: "Global index build requires 'row-tracking.enabled' = 'true'".into(),
            source: None,
        });
    }
    if options.bucket() != -1 {
        return Err(Error::Unsupported {
            message: format!(
                "Global index build only supports unaware-bucket tables (bucket = -1), got {}",
                options.bucket()
            ),
        });
    }
    Ok(())
}

pub(crate) enum IndexColumns {
    Column(String),
    Columns(Vec<String>),
}

/// Resolve an ordered definition, shared by build and drop. The leading field
/// and ordered extra IDs together identify a composite definition.
pub(crate) fn resolve_index_fields<'a>(
    table: &'a Table,
    columns: &IndexColumns,
    index_type: &str,
) -> Result<Vec<&'a DataField>> {
    let columns = match columns {
        IndexColumns::Column(column) => {
            if table
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == column)
            {
                vec![column.as_str()]
            } else {
                column.split(',').map(str::trim).collect()
            }
        }
        IndexColumns::Columns(columns) => columns.iter().map(String::as_str).collect(),
    };
    if columns.is_empty() {
        return Err(Error::DataInvalid {
            message: "Global index column list is empty".into(),
            source: None,
        });
    }
    let mut fields = Vec::new();
    for column in columns {
        if column.is_empty() {
            return Err(Error::DataInvalid {
                message: "Global index column list contains an empty column".into(),
                source: None,
            });
        }
        let field = table
            .schema()
            .fields()
            .iter()
            .find(|field| field.name() == column)
            .ok_or_else(|| Error::ColumnNotExist {
                full_name: table.identifier().full_name(),
                column: column.to_string(),
            })?;
        if fields.contains(&field) {
            return Err(Error::DataInvalid {
                message: format!("Duplicate global index column '{column}'"),
                source: None,
            });
        }
        fields.push(field);
    }
    if fields.len() > 1 && index_type != super::global_index_types::BTREE_GLOBAL_INDEX_TYPE {
        return Err(Error::Unsupported {
            message: "Only BTree global indexes support multiple index columns".into(),
        });
    }
    Ok(fields)
}

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
pub(crate) fn indexed_row_ranges(
    entries: &[IndexManifestEntry],
    index_type: &str,
    index_field_id: i32,
    extra_field_ids: Option<&[i32]>,
) -> Vec<RowRange> {
    let mut ranges = Vec::new();
    for entry in entries {
        if entry.kind != FileKind::Add || entry.index_file.index_type != index_type {
            continue;
        }
        let Some(meta) = &entry.index_file.global_index_meta else {
            continue;
        };
        if meta.index_field_id != index_field_id
            || !same_extra_field_ids(meta.extra_field_ids.as_deref(), extra_field_ids)
        {
            continue;
        }
        ranges.push(RowRange::new(meta.row_range_start, meta.row_range_end));
    }
    merge_row_ranges(ranges)
}

/// Guard against building a global index over a row range that an existing index
/// file of the SAME identity already covers. Identity is the full
/// `(index_type, index_field_id, extra_field_ids)` tuple -- matching
/// `indexed_row_ranges` and Java `GlobalIndexIdentifier` -- so a different index
/// type (or different `extra_field_ids`) on the same field coexists. Two files
/// of the same identity with overlapping ranges are still rejected.
pub(crate) fn validate_existing_index_overlap(
    entries: &[IndexManifestEntry],
    index_type: &str,
    index_field_id: i32,
    extra_field_ids: Option<&[i32]>,
    planned: &[RowRange],
) -> Result<()> {
    for entry in entries {
        if entry.kind != FileKind::Add || entry.index_file.index_type != index_type {
            continue;
        }
        let Some(meta) = &entry.index_file.global_index_meta else {
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
