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

//! Read-only source work retained by a vector or full-text search plan.

use super::bucket_filter::split_partition_and_data_predicates;
use super::global_index_scanner::unindexed_ranges_for_global_index_entries;
use super::global_index_types::normalize_queryable_global_index_type;
use super::row_id_predicate::intersect_sorted_ranges;
use super::vector_search_common::configured_vector_index_type;
use super::{find_field_id_by_name, merge_row_ranges, DataSplit, RowRange, Table};
use crate::spec::{
    CoreOptions, FileKind, GlobalIndexSearchMode, IndexFileMeta, IndexManifestEntry, Predicate,
};
use crate::vindex::is_vindex_index_type;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

/// Search source metadata, without live readers or filesystem handles.
/// The global row-ID and primary-key physical address spaces remain distinct.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SearchScanSplit {
    Index {
        column: String,
        row_range: RowRange,
        index_files: Vec<IndexFileMeta>,
        scalar_index_files: Vec<IndexFileMeta>,
    },
    Raw {
        row_ranges: Vec<RowRange>,
        scalar_index_files: Vec<IndexFileMeta>,
        index_type: Option<String>,
    },
    PrimaryKey {
        data_split: DataSplit,
        payloads: Vec<IndexFileMeta>,
        uncovered_data_files: Vec<String>,
        row_ranges_by_file: HashMap<String, Vec<RowRange>>,
    },
}

pub(super) async fn global_search_splits(
    table: &Table,
    entries: &[IndexManifestEntry],
    column: &str,
    filter: Option<&Predicate>,
    options: &HashMap<String, String>,
    full_text: bool,
) -> crate::Result<Vec<SearchScanSplit>> {
    let field_id = find_field_id_by_name(table.schema().fields(), column).ok_or_else(|| {
        crate::Error::ConfigInvalid {
            message: format!("Search column '{column}' does not exist"),
        }
    })?;
    let mut effective_options = table.schema().options().clone();
    effective_options.extend(options.clone());
    let core = CoreOptions::new(&effective_options);
    let mode = if full_text {
        core.full_text_index_search_mode()?
    } else {
        core.vector_index_search_mode()?
    };
    let belongs = |file: &IndexFileMeta| {
        file.global_index_meta
            .as_ref()
            .is_some_and(|meta| meta.index_field_id == field_id)
            && if full_text {
                file.index_type == "full-text"
            } else {
                is_vindex_index_type(&file.index_type)
            }
    };
    let selected = entries
        .iter()
        .filter(|entry| entry.kind == FileKind::Add && belongs(&entry.index_file))
        .collect::<Vec<_>>();
    // Full-text admits raw fallback only after finding its index definition.
    if full_text && selected.is_empty() {
        return Ok(vec![]);
    }
    let row_filter = filter.and_then(|filter| {
        let (_, data) = split_partition_and_data_predicates(
            filter.clone(),
            table.schema().fields(),
            table.schema().partition_keys(),
        );
        (!data.is_empty()).then(|| Predicate::and(data))
    });
    let scalar_files = entries
        .iter()
        .filter(|entry| entry.kind == FileKind::Add)
        .map(|entry| &entry.index_file)
        .filter(|file| {
            normalize_queryable_global_index_type(&file.index_type).is_some()
                && file.global_index_meta.is_some()
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut grouped = BTreeMap::<(i64, i64), Vec<IndexFileMeta>>::new();
    for entry in selected {
        let meta = entry.index_file.global_index_meta.as_ref().unwrap();
        grouped
            .entry((meta.row_range_start, meta.row_range_end))
            .or_default()
            .push(entry.index_file.clone());
    }
    let intersecting_scalar = |ranges: &[RowRange]| {
        scalar_files
            .iter()
            .filter(|file| {
                let meta = file.global_index_meta.as_ref().unwrap();
                ranges
                    .iter()
                    .any(|range| range.overlaps_inclusive(meta.row_range_start, meta.row_range_end))
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    let mut result = grouped
        .iter()
        .map(|(&(from, to), files)| {
            let row_range = RowRange::new(from, to);
            SearchScanSplit::Index {
                column: column.into(),
                scalar_index_files: intersecting_scalar(std::slice::from_ref(&row_range)),
                row_range,
                index_files: files.clone(),
            }
        })
        .collect::<Vec<_>>();
    let scalar_mode = row_filter
        .as_ref()
        .map(|_| core.scalar_index_search_mode())
        .transpose()?;
    let detail_ranges = if mode == GlobalIndexSearchMode::Detail
        || scalar_mode == Some(GlobalIndexSearchMode::Detail)
    {
        // These readers use this same helper against the plan's pinned table.
        super::de_vector_read::detail_data_ranges_for_table(table).await?
    } else {
        vec![]
    };
    let next_row_id = table
        .travel_snapshot()
        .and_then(|snapshot| snapshot.next_row_id());
    let mut raw_ranges = unindexed_ranges_for_global_index_entries(
        entries,
        &HashSet::from([field_id]),
        mode,
        next_row_id,
        &detail_ranges,
        belongs,
    );
    if !full_text {
        if let (Some(filter), Some(scalar_mode)) = (&row_filter, scalar_mode) {
            let mut scalar_ranges = super::de_vector_read::scalar_unindexed_ranges(
                super::de_vector_read::VectorSearchEvaluation {
                    table: Some(table),
                    file_io: table.file_io(),
                    table_path: table.location(),
                    table_options: table.schema().options(),
                    schema_fields: table.schema().fields(),
                    next_row_id,
                    filter: Some(filter),
                },
                entries,
                filter,
                scalar_mode,
                &detail_ranges,
            );
            if mode == GlobalIndexSearchMode::Fast {
                let indexed = merge_row_ranges(
                    grouped
                        .keys()
                        .map(|&(from, to)| RowRange::new(from, to))
                        .collect(),
                );
                scalar_ranges = intersect_sorted_ranges(&scalar_ranges, &indexed);
            }
            raw_ranges.extend(scalar_ranges);
        }
    }
    let raw_ranges = merge_row_ranges(raw_ranges);
    if !raw_ranges.is_empty() {
        let index_type = grouped
            .values()
            .flat_map(|files| files.iter())
            .next()
            .map(|file| file.index_type.clone())
            .or_else(|| configured_vector_index_type(table.schema().options(), options, column));
        result.push(SearchScanSplit::Raw {
            scalar_index_files: intersecting_scalar(&raw_ranges),
            row_ranges: raw_ranges,
            index_type,
        });
    }
    Ok(result)
}

fn invalid_source(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Restore only the index payloads explicitly named by a source plan. The
/// partition/bucket fields are irrelevant to global payload addressing; scalar
/// and vector readers use the persisted global metadata instead.
pub(super) fn global_entries_from_splits(
    splits: &[SearchScanSplit],
) -> crate::Result<Vec<IndexManifestEntry>> {
    let mut entries = Vec::new();
    let mut scalar_files = Vec::<IndexFileMeta>::new();
    for split in splits {
        let scalar = match split {
            SearchScanSplit::Index {
                row_range,
                index_files,
                scalar_index_files,
                ..
            } => {
                if row_range.from() < 0 || row_range.to() < row_range.from() {
                    return Err(invalid_source("Invalid global search index split"));
                }
                for file in index_files {
                    let meta = file.global_index_meta.as_ref().ok_or_else(|| {
                        invalid_source(format!(
                            "Search index '{}' has no global metadata",
                            file.file_name
                        ))
                    })?;
                    if meta.row_range_start != row_range.from()
                        || meta.row_range_end != row_range.to()
                    {
                        return Err(invalid_source(
                            "Search index split range disagrees with its payload",
                        ));
                    }
                    entries.push(IndexManifestEntry {
                        version: 1,
                        kind: FileKind::Add,
                        partition: crate::spec::BinaryRow::new(0).to_serialized_bytes(),
                        bucket: -1,
                        index_file: file.clone(),
                    });
                }
                scalar_index_files
            }
            SearchScanSplit::Raw {
                row_ranges,
                scalar_index_files,
                ..
            } => {
                if row_ranges
                    .iter()
                    .any(|range| range.from() < 0 || range.to() < range.from())
                {
                    return Err(invalid_source("Invalid global search raw row range"));
                }
                scalar_index_files
            }
            SearchScanSplit::PrimaryKey { .. } => {
                return Err(invalid_source(
                    "Global search cannot consume primary-key physical source splits",
                ))
            }
        };
        for file in scalar {
            if normalize_queryable_global_index_type(&file.index_type).is_none() {
                return Err(invalid_source(format!(
                    "Scalar search source '{}' has unsupported index type '{}'",
                    file.file_name, file.index_type
                )));
            }
            let meta = file.global_index_meta.as_ref().ok_or_else(|| {
                invalid_source(format!(
                    "Scalar search index '{}' has no global metadata",
                    file.file_name
                ))
            })?;
            if meta.row_range_start < 0 || meta.row_range_end < meta.row_range_start {
                return Err(invalid_source("Invalid scalar search index range"));
            }
            if !scalar_files.contains(file) {
                scalar_files.push(file.clone());
            }
        }
    }
    entries.extend(
        scalar_files
            .into_iter()
            .map(|index_file| IndexManifestEntry {
                version: 1,
                kind: FileKind::Add,
                partition: crate::spec::BinaryRow::new(0).to_serialized_bytes(),
                bucket: -1,
                index_file,
            }),
    );
    Ok(entries)
}

/// Read deletion vectors from the data scan of the supplied pinned snapshot.
/// A transported source plan need not copy bucket-local DV index entries into
/// every search split. Data-source planning resolves those vectors without
/// selecting a newer snapshot or extending the plan's search sources.
pub(super) async fn deleted_row_ranges_for_search_sources(
    table: &Table,
) -> crate::Result<Vec<RowRange>> {
    if !table.schema().core_options().deletion_vectors_enabled() {
        return Ok(vec![]);
    }
    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await?;
    let mut ranges = Vec::new();
    for split in plan.splits() {
        let Some(dvs) = split.data_deletion_files() else {
            continue;
        };
        for (file, deletion) in split.data_files().iter().zip(dvs) {
            let Some(deletion) = deletion else {
                continue;
            };
            let first = file.first_row_id.ok_or_else(|| {
                invalid_source(format!(
                    "Deletion vector anchor '{}' has no first row id",
                    file.file_name
                ))
            })?;
            for deleted in
                crate::deletion_vector::DeletionVectorFactory::read(table.file_io(), deletion)
                    .await?
                    .iter()
            {
                let deleted = i64::try_from(deleted)
                    .map_err(|_| invalid_source("Deleted position exceeds i64::MAX"))?;
                let row_id = first
                    .checked_add(deleted)
                    .ok_or_else(|| invalid_source("Deleted row id exceeds i64::MAX"))?;
                ranges.push(RowRange::new(row_id, row_id));
            }
        }
    }
    Ok(merge_row_ranges(ranges))
}

/// Resolve partition membership in the global row-ID address space from the
/// pinned data-file metadata. Partition predicates are constant per split;
/// reading Arrow rows would add work and couple this to read projections.
pub(super) async fn partition_row_ids_for_search_sources(
    table: &Table,
    partition: &Predicate,
    source_ranges: Vec<RowRange>,
) -> crate::Result<roaring::RoaringTreemap> {
    let source_ranges = merge_row_ranges(source_ranges);
    let mut read = table.new_read_builder();
    read.with_filter(partition.clone())
        .with_row_ranges(source_ranges.clone());
    let plan = read.new_scan().plan().await?;
    let mut file_ranges = Vec::new();
    for split in plan.splits() {
        for file in split.data_files() {
            if file.row_count <= 0 {
                continue;
            }
            let first = file.first_row_id.ok_or_else(|| {
                invalid_source(format!(
                    "Partition search source '{}' has no first row id",
                    file.file_name
                ))
            })?;
            let last = first.checked_add(file.row_count - 1).ok_or_else(|| {
                invalid_source("Partition search source row range exceeds i64::MAX")
            })?;
            if first < 0 {
                return Err(invalid_source("Partition search source row ID is negative"));
            }
            file_ranges.push(RowRange::new(first, last));
        }
    }
    let mut selected = roaring::RoaringTreemap::new();
    for range in intersect_sorted_ranges(&merge_row_ranges(file_ranges), &source_ranges) {
        selected.insert_range(range.from() as u64..=range.to() as u64);
    }
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{CommitKind, GlobalIndexMeta, Snapshot};
    use crate::table::VectorScanPlan;

    fn snapshot() -> Snapshot {
        Snapshot::builder()
            .version(3)
            .id(1)
            .schema_id(0)
            .base_manifest_list("base".into())
            .delta_manifest_list("delta".into())
            .commit_user("test".into())
            .commit_identifier(1)
            .commit_kind(CommitKind::APPEND)
            .time_millis(0)
            .build()
    }

    fn raw_source_with_scalar(index_type: &str) -> SearchScanSplit {
        SearchScanSplit::Raw {
            row_ranges: vec![RowRange::new(1, 1)],
            scalar_index_files: vec![IndexFileMeta {
                index_type: index_type.into(),
                file_name: "scalar-index".into(),
                file_size: 1,
                row_count: 3,
                deletion_vectors_ranges: None,
                external_path: None,
                global_index_meta: Some(GlobalIndexMeta {
                    row_range_start: 0,
                    row_range_end: 2,
                    index_field_id: 1,
                    extra_field_ids: None,
                    index_meta: None,
                    source_meta: None,
                }),
            }],
            index_type: None,
        }
    }

    #[test]
    fn raw_source_scalar_metadata_cannot_add_search_payloads() {
        // A search payload hidden in scalar metadata would otherwise be searched
        // over [0, 2], extending the explicitly selected raw source [1, 1].
        for index_type in ["ivf-flat", "diskann", "full-text"] {
            let source = raw_source_with_scalar(index_type);
            let error = VectorScanPlan::new(Some(snapshot()), vec![source.clone()]).unwrap_err();
            assert!(error.to_string().contains("unsupported index type"));
            let wire = serde_json::json!({
                "snapshot": snapshot(),
                "splits": [source.clone()],
            });
            assert!(serde_json::from_value::<VectorScanPlan>(wire.clone()).is_err());
            assert!(crate::table::FullTextScanPlan::new(Some(snapshot()), vec![source]).is_err());
            assert!(serde_json::from_value::<crate::table::FullTextScanPlan>(wire).is_err());
        }
    }

    #[test]
    fn raw_source_accepts_queryable_scalar_metadata() {
        for index_type in [
            "bitmap",
            "Bitmap",
            "btree",
            "BTREE",
            "multivalue",
            "MultiValue",
            "fm",
            "FM",
        ] {
            let source = raw_source_with_scalar(index_type);
            let plan = VectorScanPlan::new(Some(snapshot()), vec![source.clone()]).unwrap();
            let wire = serde_json::to_value(plan).unwrap();
            assert!(serde_json::from_value::<VectorScanPlan>(wire.clone()).is_ok());
            assert!(crate::table::FullTextScanPlan::new(Some(snapshot()), vec![source]).is_ok());
            assert!(serde_json::from_value::<crate::table::FullTextScanPlan>(wire).is_ok());
        }
    }
}
