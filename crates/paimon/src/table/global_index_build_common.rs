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

use crate::io::OutputFile;
use crate::spec::{DataField, FileKind, IndexManifest, IndexManifestEntry, Snapshot};
use crate::table::{merge_row_ranges, RowRange, Table};
use crate::{Error, Result};
use bytes::Bytes;
use std::collections::HashSet;
use std::path::Path;
use tokio::io::AsyncReadExt;

pub(crate) mod preparation;
pub(crate) mod vector;

const COPY_BUFFER_SIZE: usize = 1024 * 1024;

pub(crate) type IndexFileKey = (Vec<u8>, i32, String);

pub(crate) fn index_file_key(entry: &IndexManifestEntry) -> IndexFileKey {
    (
        entry.partition.clone(),
        entry.bucket,
        entry.index_file.file_name.clone(),
    )
}

/// An automatic round fixes one source snapshot for every index definition.
/// Capturing an empty view also validates builder configuration without I/O.
pub(super) async fn build_snapshot(
    table: &Table,
    captured: Option<&Option<Snapshot>>,
) -> Result<Option<Snapshot>> {
    match captured {
        Some(snapshot) => Ok(snapshot.clone()),
        None => table.snapshot_manager().get_latest_snapshot().await,
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

/// Stream a locally built full-text index to its table location. The native
/// writer serializes to a temporary file because it needs synchronous I/O.
pub(crate) async fn copy_local_file_to_output(
    source_path: &Path,
    output: OutputFile,
) -> Result<()> {
    let mut source =
        tokio::fs::File::open(source_path)
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to open temporary index file: {e}"),
                source: None,
            })?;
    let mut writer = output.writer().await?;
    let mut buffer = vec![0u8; COPY_BUFFER_SIZE];

    loop {
        let len = source
            .read(&mut buffer)
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to read temporary index file: {e}"),
                source: None,
            })?;
        if len == 0 {
            break;
        }
        writer.write(Bytes::copy_from_slice(&buffer[..len])).await?;
    }
    writer.close().await
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
pub(crate) async fn indexed_row_ranges(
    table: &Table,
    index_manifest_name: Option<&str>,
    index_type: &str,
    index_field_id: i32,
    extra_field_ids: Option<&[i32]>,
    replaced: &HashSet<IndexFileKey>,
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
        if entry.kind != FileKind::Add
            || entry.index_file.index_type != index_type
            || replaced.contains(&index_file_key(&entry))
        {
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
    replaced: &HashSet<IndexFileKey>,
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
        if entry.kind != FileKind::Add
            || entry.index_file.index_type != index_type
            || replaced.contains(&index_file_key(&entry))
        {
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

/// Resolve and create the table's global index directory, including external placement.
pub(crate) async fn prepare_index_file_path(
    table: &Table,
    file_name: &str,
) -> Result<(String, Option<String>)> {
    let external_path = super::external_path::new_index_external_path(
        table.schema().options(),
        false,
        "",
        file_name,
    )?;
    let path = external_path.clone().unwrap_or_else(|| {
        format!(
            "{}/index/{file_name}",
            table.location().trim_end_matches('/')
        )
    });
    if let Some((directory, _)) = path.rsplit_once('/') {
        table.file_io().mkdirs(&format!("{directory}/")).await?;
    }
    Ok((path, external_path))
}
