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

//! Predicate orchestration and index-query planning.

use super::entry::{
    bitmap_meta_may_match, bitmap_meta_may_match_between, multivalue_meta_may_match,
    sorted_entry_meta, GlobalIndexEntry, GlobalIndexFileKind,
};
use super::predicates::{
    entries_support_predicate, is_multivalue_predicate, is_sorted_global_index_supported_op,
    select_entries_for_predicates,
};
use super::query_plan::{
    fallback_plan_evaluates_entry, requires_fallback_scan, EntryQueryPlan, FallbackScanPlan,
};
#[cfg(test)]
use super::row_ranges::unindexed_ranges_for_coverage;
use super::row_ranges::{bitmap_to_ranges, intersect_sorted_ranges};
use super::GlobalIndexScanner;
use crate::btree::query::extract_between;
use crate::btree::{make_key_comparator, serialize_datum};
#[cfg(test)]
use crate::spec::GlobalIndexSearchMode;
use crate::spec::{DataType, Datum, Predicate, PredicateOperator};
use crate::table::bitmap_global_index_format::{
    make_bitmap_key_comparator, serialize_bitmap_datum,
};
use crate::table::RowRange;
use crate::{Error, Result};
use futures::{StreamExt, TryStreamExt};
use roaring::RoaringTreemap;
use std::collections::HashSet;
use std::future::Future;

type EvaluateFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Option<GlobalIndexScanResult>>> + Send + 'a>,
>;

type PredicateTuple<'a> = (PredicateOperator, &'a [Datum], &'a DataType);

pub(super) struct GlobalIndexScanResult {
    pub(super) row_ranges: Vec<RowRange>,
    pub(super) evaluated_field_ids: HashSet<i32>,
    pub(super) indexed_coverage: Vec<RowRange>,
}

pub(super) async fn try_fold_bounded<T, Fut, Acc, Fold>(
    futures: impl IntoIterator<Item = Fut>,
    max_concurrency: usize,
    mut accumulator: Acc,
    mut fold: Fold,
) -> Result<Acc>
where
    Fut: Future<Output = Result<T>>,
    Fold: FnMut(&mut Acc, T),
{
    debug_assert!(max_concurrency > 0);
    let stream = futures::stream::iter(futures).buffer_unordered(max_concurrency);
    futures::pin_mut!(stream);
    while let Some(value) = stream.try_next().await? {
        fold(&mut accumulator, value);
    }
    Ok(accumulator)
}

impl GlobalIndexScanner {
    /// Evaluate a predicate against the global indexes and return matching row ranges.
    /// Returns `None` if the predicate cannot be evaluated by the global index.
    pub(super) fn evaluate<'a>(&'a self, predicate: &'a Predicate) -> EvaluateFuture<'a> {
        Box::pin(async move {
            match predicate {
                Predicate::Leaf {
                    column,
                    op,
                    literals,
                    data_type,
                    ..
                } => {
                    if !is_sorted_global_index_supported_op(*op) {
                        return Ok(None);
                    }
                    let field_id = self.find_field_id_by_name(column)?;
                    let field_id = match field_id {
                        Some(id) => id,
                        None => return Ok(None),
                    };
                    let entries = match self.entries_for_field(field_id) {
                        Some(e) => e,
                        None => return Ok(None),
                    };
                    if !entries_support_predicate(entries, *op, literals) {
                        return Ok(None);
                    }
                    let predicates = [(*op, literals.as_slice(), data_type)];
                    let selected_entries = select_entries_for_predicates(entries, &predicates);
                    self.evaluate_leaf(&selected_entries, &predicates)
                        .await
                        .map(|result| {
                            result.map(|(row_ranges, indexed_coverage)| GlobalIndexScanResult {
                                row_ranges,
                                evaluated_field_ids: HashSet::from([field_id]),
                                indexed_coverage,
                            })
                        })
                }
                Predicate::And(children) => {
                    // Group leaf predicates by field_id to reuse readers
                    let mut leaf_groups: std::collections::HashMap<i32, Vec<PredicateTuple<'_>>> =
                        std::collections::HashMap::new();
                    let mut non_leaf_children = Vec::new();

                    for child in children {
                        if let Predicate::Leaf {
                            column,
                            op,
                            literals,
                            data_type,
                            ..
                        } = child
                        {
                            if is_sorted_global_index_supported_op(*op) {
                                if let Some(field_id) = self.find_field_id_by_name(column)? {
                                    if self.entries_for_field(field_id).is_some_and(|entries| {
                                        entries_support_predicate(entries, *op, literals)
                                    }) {
                                        leaf_groups.entry(field_id).or_default().push((
                                            *op,
                                            literals.as_slice(),
                                            data_type,
                                        ));
                                        continue;
                                    }
                                }
                            }
                        }
                        non_leaf_children.push(child);
                    }

                    // Evaluate independent fields concurrently while keeping predicates for the
                    // same field together so each index file is opened only once.
                    let mut leaf_futures = Vec::with_capacity(leaf_groups.len());
                    for (field_id, predicates) in &leaf_groups {
                        if let Some(entries) = self.entries_for_field(*field_id) {
                            let field_id = *field_id;
                            let mut selected_predicates = predicates.clone();
                            let mut selected_entries =
                                select_entries_for_predicates(entries, &selected_predicates);
                            if selected_entries.is_empty() {
                                for predicate in predicates {
                                    let candidate_predicates = vec![*predicate];
                                    let candidate_entries = select_entries_for_predicates(
                                        entries,
                                        &candidate_predicates,
                                    );
                                    if !candidate_entries.is_empty() {
                                        selected_predicates = candidate_predicates;
                                        selected_entries = candidate_entries;
                                        break;
                                    }
                                }
                            }
                            leaf_futures.push(async move {
                                let result = self
                                    .evaluate_leaf(&selected_entries, &selected_predicates)
                                    .await?;
                                Ok((field_id, result))
                            });
                        }
                    }
                    let leaf_group_count = leaf_futures.len();
                    let (mut row_ranges, mut indexed_coverage, mut evaluated_field_ids) =
                        try_fold_bounded(
                            leaf_futures,
                            leaf_group_count.max(1),
                            (None::<Vec<RowRange>>, None::<Vec<RowRange>>, HashSet::new()),
                            |(row_ranges, indexed_coverage, evaluated_field_ids),
                             (field_id, result)| {
                                if let Some((ranges, coverage)) = result {
                                    *row_ranges = Some(match row_ranges.take() {
                                        None => ranges,
                                        Some(existing) => {
                                            intersect_sorted_ranges(&existing, &ranges)
                                        }
                                    });
                                    *indexed_coverage = Some(match indexed_coverage.take() {
                                        None => coverage,
                                        Some(existing) => {
                                            intersect_sorted_ranges(&existing, &coverage)
                                        }
                                    });
                                    evaluated_field_ids.insert(field_id);
                                }
                            },
                        )
                        .await?;

                    // Evaluate non-leaf children recursively
                    for child in non_leaf_children {
                        if let Some(child_result) = self.evaluate(child).await? {
                            row_ranges = Some(match row_ranges {
                                None => child_result.row_ranges,
                                Some(existing) => {
                                    intersect_sorted_ranges(&existing, &child_result.row_ranges)
                                }
                            });
                            evaluated_field_ids.extend(child_result.evaluated_field_ids);
                            indexed_coverage = Some(match indexed_coverage {
                                None => child_result.indexed_coverage,
                                Some(existing) => intersect_sorted_ranges(
                                    &existing,
                                    &child_result.indexed_coverage,
                                ),
                            });
                        }
                    }

                    Ok(row_ranges.map(|row_ranges| GlobalIndexScanResult {
                        row_ranges,
                        evaluated_field_ids,
                        indexed_coverage: indexed_coverage.unwrap_or_default(),
                    }))
                }
                Predicate::Or(children) => {
                    let mut all_ranges: Vec<RowRange> = Vec::new();
                    let mut evaluated_field_ids = HashSet::new();
                    let mut indexed_coverage: Option<Vec<RowRange>> = None;
                    for child in children {
                        match self.evaluate(child).await? {
                            Some(child_result) => {
                                all_ranges.extend(child_result.row_ranges);
                                evaluated_field_ids.extend(child_result.evaluated_field_ids);
                                indexed_coverage = Some(match indexed_coverage {
                                    None => child_result.indexed_coverage,
                                    Some(existing) => intersect_sorted_ranges(
                                        &existing,
                                        &child_result.indexed_coverage,
                                    ),
                                });
                            }
                            None => return Ok(None),
                        }
                    }
                    let row_ranges = if all_ranges.is_empty() {
                        Vec::new()
                    } else {
                        crate::table::merge_row_ranges(all_ranges)
                    };
                    Ok(Some(GlobalIndexScanResult {
                        row_ranges,
                        evaluated_field_ids,
                        indexed_coverage: indexed_coverage.unwrap_or_default(),
                    }))
                }
                _ => Ok(None),
            }
        })
    }

    /// Evaluate multiple predicates against the same set of index entries.
    /// Opens each file once and evaluates all predicates, intersecting results.
    /// Detects between patterns (GtEq/Gt + LtEq/Lt) and merges them into a single range query.
    async fn evaluate_leaf(
        &self,
        entries: &[&GlobalIndexEntry],
        predicates: &[(PredicateOperator, &[Datum], &DataType)],
    ) -> Result<Option<(Vec<RowRange>, Vec<RowRange>)>> {
        let normalized_predicates = predicates
            .iter()
            .map(|(op, literals, data_type)| {
                let key_type = if is_multivalue_predicate(*op) {
                    let DataType::Array(array) = data_type else {
                        return Err(Error::DataInvalid {
                            message: format!(
                                "Array global-index predicate {op} requires an ARRAY field type"
                            ),
                            source: None,
                        });
                    };
                    array.element_type()
                } else {
                    *data_type
                };
                Ok((*op, *literals, key_type))
            })
            .collect::<Result<Vec<_>>>()?;
        let predicates = normalized_predicates.as_slice();

        // Try to detect between pattern and split into (between, remaining)
        let (between, remaining) = extract_between(predicates);

        let effective_predicates = if between.is_some() {
            &remaining
        } else {
            predicates
        };

        // Pre-compute comparators and serialized keys for file-level pruning per predicate
        let pruning_info: Vec<_> = effective_predicates
            .iter()
            .map(|(op, literals, data_type)| {
                let btree_cmp = make_key_comparator(data_type);
                let btree_serialized = literals
                    .iter()
                    .map(|l| serialize_datum(l, data_type))
                    .collect::<Vec<_>>();
                let bitmap_cmp = make_bitmap_key_comparator(data_type);
                let bitmap_serialized = literals
                    .iter()
                    .map(|l| serialize_bitmap_datum(l, data_type))
                    .collect::<Vec<_>>();
                (
                    *op,
                    *data_type,
                    btree_cmp,
                    btree_serialized,
                    bitmap_cmp,
                    bitmap_serialized,
                )
            })
            .collect();

        let predicate_matches: Vec<Vec<bool>> = pruning_info
            .iter()
            .map(
                |(op, data_type, btree_cmp, btree_serialized, bitmap_cmp, bitmap_serialized)| {
                    entries
                        .iter()
                        .map(|entry| match entry.index_type {
                            GlobalIndexFileKind::BTree => {
                                sorted_entry_meta(entry).may_match(*op, btree_serialized, btree_cmp)
                            }
                            GlobalIndexFileKind::Bitmap => bitmap_meta_may_match(
                                sorted_entry_meta(entry),
                                *op,
                                data_type,
                                bitmap_serialized,
                                bitmap_cmp.as_ref(),
                            ),
                            GlobalIndexFileKind::Multivalue => multivalue_meta_may_match(
                                sorted_entry_meta(entry),
                                *op,
                                bitmap_serialized,
                                bitmap_cmp.as_ref(),
                            ),
                            GlobalIndexFileKind::FM => true,
                        })
                        .collect()
                },
            )
            .collect();
        let predicate_fallback_plans: Vec<Option<FallbackScanPlan>> = effective_predicates
            .iter()
            .enumerate()
            .map(|(i, (op, _, _))| {
                requires_fallback_scan(*op)
                    .then(|| self.fallback_scan_plan(entries, &predicate_matches[i]))
            })
            .collect();

        let between_matches_by_entry: Vec<bool> =
            match between.as_ref() {
                Some(b) => {
                    let btree_cmp = make_key_comparator(b.data_type);
                    let btree_from = serialize_datum(b.from, b.data_type);
                    let btree_to = serialize_datum(b.to, b.data_type);
                    let bitmap_cmp = make_bitmap_key_comparator(b.data_type);
                    let bitmap_from = serialize_bitmap_datum(b.from, b.data_type);
                    let bitmap_to = serialize_bitmap_datum(b.to, b.data_type);
                    entries
                        .iter()
                        .map(|entry| match entry.index_type {
                            GlobalIndexFileKind::BTree => sorted_entry_meta(entry)
                                .may_match_between(&btree_from, &btree_to, &btree_cmp),
                            GlobalIndexFileKind::Bitmap => bitmap_meta_may_match_between(
                                sorted_entry_meta(entry),
                                b.data_type,
                                &bitmap_from,
                                &bitmap_to,
                                bitmap_cmp.as_ref(),
                            ),
                            GlobalIndexFileKind::Multivalue | GlobalIndexFileKind::FM => false,
                        })
                        .collect()
                }
                None => Vec::new(),
            };
        let between_fallback_plan = between
            .as_ref()
            .map(|_| self.fallback_scan_plan(entries, &between_matches_by_entry));

        let mut query_plans = Vec::with_capacity(entries.len());
        for (entry_idx, entry) in entries.iter().enumerate() {
            // Also check if between range may match
            let between_matches = between
                .as_ref()
                .is_some_and(|_| between_matches_by_entry[entry_idx]);
            let between_evaluated_for_entry = between_fallback_plan.is_some_and(|plan| {
                fallback_plan_evaluates_entry(plan, entry.index_type, between_matches)
            });

            // When a Between conjunct exists but the file does not overlap its
            // range, the whole AND cannot match — drop the file regardless of
            // how the remaining predicates evaluate. Without this guard, a file
            // outside the Between range but matched by some remaining predicate
            // (e.g. `BETWEEN 10 AND 20 AND id >= 0` on a file [30, 40]) would
            // be retained because `file_result` is initialized from the
            // remaining bitmap, silently dropping the Between conjunct.
            if between_evaluated_for_entry && !between_matches {
                continue;
            }

            let mut file_evaluated = between_evaluated_for_entry;
            let mut file_cannot_match = false;
            let mut file_has_unsupported_match =
                between_matches && !between_evaluated_for_entry && between_fallback_plan.is_some();
            let matching_predicates: Vec<usize> = (0..effective_predicates.len())
                .filter(|&i| {
                    let predicate_matches_entry = predicate_matches[i][entry_idx];
                    let predicate_evaluated_for_entry =
                        predicate_fallback_plans[i].is_none_or(|plan| {
                            fallback_plan_evaluates_entry(
                                plan,
                                entry.index_type,
                                predicate_matches_entry,
                            )
                        });
                    if !predicate_evaluated_for_entry {
                        file_has_unsupported_match |= predicate_matches_entry;
                        return false;
                    }
                    file_evaluated = true;
                    if !predicate_matches_entry {
                        file_cannot_match = true;
                        return false;
                    }
                    true
                })
                .collect();
            if file_cannot_match {
                continue;
            }
            if !file_evaluated {
                if file_has_unsupported_match {
                    return Ok(None);
                }
                continue;
            }

            query_plans.push(EntryQueryPlan {
                entry_idx,
                between_matches,
                between_evaluated: between_evaluated_for_entry,
                matching_predicates,
            });
        }

        // Complete all pruning and fallback decisions before starting shard I/O.
        // A later unsupported shard must fall back to the normal scan without an
        // earlier shard racing it with an I/O or query error.
        let data_type = between
            .as_ref()
            .map(|b| b.data_type)
            .or_else(|| effective_predicates.first().map(|p| p.2))
            .unwrap_or(predicates[0].2);
        let between = between.as_ref();
        let futures =
            query_plans.into_iter().map(|plan| async move {
                let entry = &entries[plan.entry_idx];
                let _permit = self.query_semaphore.acquire().await.map_err(|error| {
                    Error::UnexpectedError {
                        message: "global-index query concurrency budget was closed".to_string(),
                        source: Some(Box::new(error)),
                    }
                })?;
                #[cfg(test)]
                let _query_io_probe_guard = match &self.query_io_probe {
                    Some(probe) => Some(probe.enter().await),
                    None => None,
                };
                let result = self
                    .query_entry(entry, data_type, between, &plan, effective_predicates)
                    .await?;
                Ok((entry.row_range_start, result))
            });
        let (all_row_ids, declined) = try_fold_bounded(
            futures,
            self.global_index_thread_num,
            (RoaringTreemap::new(), false),
            |(all_row_ids, declined), (row_range_start, file_result)| {
                if file_result.declined {
                    *declined = true;
                    return;
                }
                if let Some(bitmap) = file_result.bitmap {
                    for row_id in bitmap.iter() {
                        all_row_ids.insert(row_id + row_range_start as u64);
                    }
                }
            },
        )
        .await?;

        if declined {
            return Ok(None);
        }

        let coverage = crate::table::merge_row_ranges(
            entries
                .iter()
                .map(|entry| RowRange::new(entry.row_range_start, entry.row_range_end))
                .collect(),
        );
        Ok(Some((bitmap_to_ranges(&all_row_ids), coverage)))
    }

    fn find_field_id_by_name(&self, column: &str) -> Result<Option<i32>> {
        Ok(crate::table::find_field_id_by_name(
            &self.schema_fields,
            column,
        ))
    }

    fn entries_for_field(&self, field_id: i32) -> Option<&[GlobalIndexEntry]> {
        self.entries_by_field
            .iter()
            .find(|(id, _)| *id == field_id)
            .map(|(_, entries)| entries.as_slice())
    }

    /// Return row ranges not covered by global indexes for this predicate.
    ///
    /// `full` uses `[0, snapshot.next_row_id - 1]`; `detail` uses actual
    /// data-file row ranges collected by the scan. The caller unions these
    /// ranges with indexed matches, and the normal read filter evaluates the
    /// predicate on the raw rows.
    #[cfg(test)]
    pub(super) fn unindexed_ranges(
        &self,
        predicate: &Predicate,
        search_mode: GlobalIndexSearchMode,
        next_row_id: Option<i64>,
        data_ranges: &[RowRange],
    ) -> Result<Vec<RowRange>> {
        let field_ids = self.collect_field_ids(predicate)?;
        Ok(self.unindexed_ranges_for_field_ids(&field_ids, search_mode, next_row_id, data_ranges))
    }

    #[cfg(test)]
    fn unindexed_ranges_for_field_ids(
        &self,
        field_ids: &HashSet<i32>,
        search_mode: GlobalIndexSearchMode,
        next_row_id: Option<i64>,
        data_ranges: &[RowRange],
    ) -> Vec<RowRange> {
        unindexed_ranges_for_coverage(
            &self.coverage_by_field,
            field_ids,
            search_mode,
            next_row_id,
            data_ranges,
        )
    }

    #[cfg(test)]
    fn collect_field_ids(&self, predicate: &Predicate) -> Result<HashSet<i32>> {
        let mut field_ids = HashSet::new();
        self.collect_field_ids_inner(predicate, &mut field_ids)?;
        Ok(field_ids)
    }

    #[cfg(test)]
    fn collect_field_ids_inner(
        &self,
        predicate: &Predicate,
        field_ids: &mut HashSet<i32>,
    ) -> Result<()> {
        match predicate {
            Predicate::Leaf { column, .. } => {
                if let Some(field_id) = self.find_field_id_by_name(column)? {
                    field_ids.insert(field_id);
                }
            }
            Predicate::And(children) | Predicate::Or(children) => {
                for child in children {
                    self.collect_field_ids_inner(child, field_ids)?;
                }
            }
            Predicate::Not(inner) => self.collect_field_ids_inner(inner, field_ids)?,
            Predicate::AlwaysTrue | Predicate::AlwaysFalse => {}
        }
        Ok(())
    }
}
