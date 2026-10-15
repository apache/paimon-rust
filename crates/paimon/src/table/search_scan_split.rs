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
use super::vector_search_common::{configured_vector_index_type, VectorIndexBackend};
use super::{find_field_id_by_name, merge_row_ranges, DataSplit, RowRange, Table};
use crate::spec::{
    CoreOptions, FileKind, GlobalIndexSearchMode, IndexFileMeta, IndexManifestEntry, Predicate,
};
use std::collections::{BTreeMap, HashMap, HashSet};

/// Search source metadata, without live readers or filesystem handles.
/// The global row-ID and primary-key physical address spaces remain distinct.
#[derive(Clone, Debug)]
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
                VectorIndexBackend::from_index_type(&file.index_type).is_some()
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
    let mut filter_fields = Vec::new();
    let row_filter = filter.and_then(|filter| {
        let (_, data) = split_partition_and_data_predicates(
            filter.clone(),
            table.schema().fields(),
            table.schema().partition_keys(),
        );
        (!data.is_empty()).then(|| Predicate::and(data))
    });
    if let Some(filter) = &row_filter {
        crate::arrow::residual::collect_predicate_leaf_refs(filter, &mut filter_fields);
    }
    let filter_ids = filter_fields
        .into_iter()
        .filter_map(|(name, _)| find_field_id_by_name(table.schema().fields(), name))
        .collect::<HashSet<_>>();
    let scalar_files = entries
        .iter()
        .filter(|entry| entry.kind == FileKind::Add)
        .map(|entry| &entry.index_file)
        .filter(|file| {
            normalize_queryable_global_index_type(&file.index_type).is_some()
                && file.global_index_meta.as_ref().is_some_and(|meta| {
                    filter_ids.contains(&meta.index_field_id)
                        || meta
                            .extra_field_ids
                            .as_ref()
                            .is_some_and(|ids| ids.iter().any(|id| filter_ids.contains(id)))
                })
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
