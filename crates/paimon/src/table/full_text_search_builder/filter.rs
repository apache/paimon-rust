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

//! Java full-text scalar coverage and exact candidate refinement.

use super::*;
use crate::table::global_index_scanner::GlobalIndexScanner;
use crate::table::global_index_search_filter::matching_row_ids_for_filter;
use crate::table::global_index_types::normalize_queryable_global_index_type;

pub(super) fn intersect_include(
    include: Option<&RoaringTreemap>,
    mut rows: RoaringTreemap,
) -> RoaringTreemap {
    if let Some(include) = include {
        rows &= include;
    }
    rows
}

fn row_ids(ranges: &[RowRange]) -> crate::Result<RoaringTreemap> {
    let mut ids = RoaringTreemap::new();
    for range in ranges {
        let from = u64::try_from(range.from()).map_err(|_| crate::Error::DataInvalid {
            message: "Negative full-text candidate row ID".into(),
            source: None,
        })?;
        let to = u64::try_from(range.to()).map_err(|_| crate::Error::DataInvalid {
            message: "Negative full-text candidate row ID".into(),
            source: None,
        })?;
        ids.insert_range(from..=to);
    }
    Ok(ids)
}

fn ranges(ids: &RoaringTreemap) -> crate::Result<Vec<RowRange>> {
    let mut ranges: Vec<RowRange> = Vec::new();
    for id in ids.iter() {
        let id = i64::try_from(id).map_err(|_| crate::Error::DataInvalid {
            message: "Full-text candidate row ID exceeds i64".into(),
            source: None,
        })?;
        match ranges.last_mut() {
            Some(last) if last.to().checked_add(1) == Some(id) => {
                *last = RowRange::new(last.from(), id);
            }
            _ => ranges.push(RowRange::new(id, id)),
        }
    }
    Ok(ranges)
}

fn field_ids(predicate: &Predicate, fields: &[DataField], ids: &mut HashSet<i32>) {
    match predicate {
        Predicate::Leaf { column, .. } => {
            if let Some(id) = find_field_id_by_name(fields, column) {
                ids.insert(id);
            }
        }
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                field_ids(child, fields, ids);
            }
        }
        Predicate::Not(child) => field_ids(child, fields, ids),
        Predicate::AlwaysTrue | Predicate::AlwaysFalse => {}
    }
}

pub(super) async fn matching_rows(
    evaluation: &FullTextSearchEvaluation<'_>,
    filter: &Predicate,
    ranges: Vec<RowRange>,
) -> crate::Result<RoaringTreemap> {
    if ranges.is_empty() {
        return Ok(RoaringTreemap::new());
    }
    let table = evaluation.table.ok_or_else(|| crate::Error::DataInvalid {
        message: "Full-text row filtering requires table context".into(),
        source: None,
    })?;
    // The caller has already selected which ranges Java permits reading. A
    // candidate refinement must not lose rows to scalar FAST pruning again.
    let table = table.copy_with_options(HashMap::from([(
        "scalar-index.search-mode".into(),
        "full".into(),
    )]));
    let filter = match evaluation.partition_filter {
        Some(partition) => Predicate::and(vec![partition.clone(), filter.clone()]),
        None => filter.clone(),
    };
    matching_row_ids_for_filter(&table, &filter, Some(ranges)).await
}

pub(super) async fn matching_indexed_rows(
    evaluation: &FullTextSearchEvaluation<'_>,
    entries: &[IndexManifestEntry],
    full_text_entries: &[&IndexManifestEntry],
    filter: &Predicate,
) -> crate::Result<RoaringTreemap> {
    let core = CoreOptions::new(evaluation.table_options);
    let mode = core.scalar_index_search_mode()?;
    let covered_ranges = full_text_entries
        .iter()
        .map(|entry| {
            let meta = entry.index_file.global_index_meta.as_ref().unwrap();
            candidate_limit(meta.row_range_start, meta.row_range_end)?;
            Ok(RowRange::new(meta.row_range_start, meta.row_range_end))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    let covered = row_ids(&covered_ranges)?;
    let scalar_entries = entries
        .iter()
        .filter(|entry| {
            normalize_queryable_global_index_type(&entry.index_file.index_type).is_some()
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut fields = HashSet::new();
    field_ids(filter, evaluation.schema_fields, &mut fields);
    let detail = if mode == GlobalIndexSearchMode::Detail {
        let table = evaluation.table.ok_or_else(|| crate::Error::DataInvalid {
            message: "Full-text DETAIL filtering requires table context".into(),
            source: None,
        })?;
        detail_data_ranges_for_table(table, evaluation.partition_filter).await?
    } else {
        Vec::new()
    };
    let unindexed_ranges = unindexed_ranges_for_global_index_entries(
        &scalar_entries,
        &fields,
        mode,
        evaluation.next_row_id,
        &detail,
        |file| normalize_queryable_global_index_type(&file.index_type).is_some(),
    );
    let mut unindexed = row_ids(&unindexed_ranges)? & &covered;
    let decided = &covered - &unindexed;
    let mut matched = RoaringTreemap::new();
    if !decided.is_empty() {
        // Java createForScalarFilters excludes multi-field definitions: search
        // pre-filters evaluate individual fields rather than composite keys.
        let scalar_entries = scalar_entries
            .into_iter()
            .filter(|entry| {
                entry
                    .index_file
                    .global_index_meta
                    .as_ref()
                    .is_some_and(|meta| meta.extra_field_ids.as_ref().is_none_or(Vec::is_empty))
            })
            .collect::<Vec<_>>();
        let scanner = GlobalIndexScanner::create_with_fm_options(
            evaluation.file_io,
            evaluation.table_path,
            core.global_index_thread_num()?,
            core.btree_index_fallback_scan_max_size()?,
            core.btree_index_data_block_cache_size()?,
            core.bitmap_index_fallback_scan_max_size()?,
            &scalar_entries,
            evaluation.schema_fields,
            if scalar_entries
                .iter()
                .any(|entry| entry.index_file.index_type.eq_ignore_ascii_case("fm"))
            {
                crate::fm_index::FMOptions::from_options(evaluation.table_options)?.read
            } else {
                crate::fm_index::FMReadOptions::default()
            },
        )?;
        let answer = match scanner {
            Some(scanner) => scanner.matching_ranges_with_exactness(filter).await?,
            None => None,
        };
        match answer {
            Some((answer, exact)) => {
                let candidates = row_ids(&answer)? & &decided;
                if exact {
                    matched |= candidates;
                } else if core.global_index_filter_refine_from_data() {
                    matched |= matching_rows(evaluation, filter, ranges(&candidates)?).await?;
                }
            }
            None if mode != GlobalIndexSearchMode::Fast => unindexed |= decided,
            None => {}
        }
    }
    matched |= matching_rows(evaluation, filter, ranges(&unindexed)?).await?;
    Ok(matched)
}
