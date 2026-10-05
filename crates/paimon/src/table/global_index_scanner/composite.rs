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

//! Composite definition selection, pruning, and bounded shard queries.

use super::entry::{sorted_entry_meta, GlobalIndexEntry, GlobalIndexFileKind};
use super::evaluator::{try_fold_bounded, GlobalIndexScanResult};
use super::reader::OpenedGlobalIndexReader;
use super::row_ranges::{bitmap_to_ranges, intersect_sorted_ranges};
use super::GlobalIndexScanner;
use crate::btree::key_serde::is_key_comparison_failure;
use crate::btree::CompositePlan;
use crate::spec::{DataType, Predicate, RowType};
use crate::table::{merge_row_ranges, RowRange};
use crate::{Error, Result};
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

fn coverage(entries: &[GlobalIndexEntry]) -> Vec<RowRange> {
    merge_row_ranges(
        entries
            .iter()
            .map(|entry| RowRange::new(entry.row_range_start, entry.row_range_end))
            .collect(),
    )
}

pub(super) struct CompositeIndexScanResult {
    pub(super) result: GlobalIndexScanResult,
    pub(super) key_field_ids: HashSet<i32>,
}

impl GlobalIndexScanner {
    pub(super) async fn evaluate_composite(
        &self,
        predicate: &Predicate,
    ) -> Result<Option<CompositeIndexScanResult>> {
        let mut candidates = Vec::new();
        for (ids, entries) in &self.composite_entries {
            let fields = ids
                .iter()
                .map(|id| {
                    self.schema_fields
                        .iter()
                        .find(|field| field.id() == *id)
                        .cloned()
                })
                .collect::<Option<Vec<_>>>();
            let Some(fields) = fields else {
                continue;
            };
            if ids.iter().collect::<HashSet<_>>().len() != ids.len() {
                continue;
            }
            // An unsupported type or literal layout declines this index.
            let Ok(Some(plan)) = CompositePlan::plan(&fields, predicate) else {
                continue;
            };
            let indexed_coverage = coverage(entries);
            if plan.bound_columns == 1 && self.single_column_covers(ids[0], &indexed_coverage) {
                continue;
            }
            let selected = entries
                .iter()
                .filter_map(|entry| {
                    let meta = sorted_entry_meta(entry);
                    match plan.may_match(meta.first_key.as_deref(), meta.last_key.as_deref()) {
                        Ok(true) => Some(Ok(entry)),
                        Ok(false) => None,
                        Err(error) => Some(Err(error)),
                    }
                })
                .collect::<Result<Vec<_>>>();
            let Ok(selected) = selected else {
                continue;
            };
            if !plan.is_point_lookup() && !selected.is_empty() {
                let mut sizes = HashMap::new();
                let valid = selected.iter().all(|entry| {
                    let size = sizes
                        .entry((entry.row_range_start, entry.row_range_end))
                        .or_insert(0i64);
                    if entry.file_size < 0 {
                        return false;
                    }
                    if let Some(total) = size.checked_add(entry.file_size) {
                        *size = total;
                        true
                    } else {
                        false
                    }
                });
                if self.btree_fallback_scan_max_size <= 0
                    || !valid
                    || sizes
                        .values()
                        .any(|size| *size > self.btree_fallback_scan_max_size)
                {
                    continue;
                }
            }
            candidates.push((plan, selected, indexed_coverage));
        }
        // Stable tie breaking avoids HashMap iteration deciding which coverage
        // domain fast mode uses when equivalent definitions coexist.
        candidates.sort_by_key(|(plan, _, _)| {
            (
                Reverse(plan.bound_columns),
                Reverse(plan.equal_columns),
                plan.fields.len(),
                plan.fields
                    .iter()
                    .map(|field| field.id())
                    .collect::<Vec<_>>(),
            )
        });
        for (plan, selected, indexed_coverage) in candidates {
            let key_type = DataType::Row(RowType::new(plan.fields.clone()));
            let mut queries = Vec::with_capacity(selected.len());
            for entry in selected {
                let plan = &plan;
                let key_type = &key_type;
                queries.push(async move {
                    let _guard = self.btree_file_lock(entry).lock_owned().await;
                    let _permit = self.query_semaphore.acquire().await.map_err(|error| {
                        Error::UnexpectedError {
                            message: "Global-index query budget was closed".into(),
                            source: Some(Box::new(error)),
                        }
                    })?;
                    let OpenedGlobalIndexReader::BTree(reader) = self
                        .get_or_open_reader(entry, sorted_entry_meta(entry), key_type)
                        .await?
                    else {
                        unreachable!()
                    };
                    let queried = reader.query_composite(plan).await;
                    self.return_reader(entry.resolved_path(&self.table_path), reader);
                    match queried {
                        Ok(bitmap) => Ok(Some(
                            bitmap_to_ranges(&bitmap)
                                .into_iter()
                                .map(|range| {
                                    RowRange::new(
                                        range.from() + entry.row_range_start,
                                        range.to() + entry.row_range_start,
                                    )
                                })
                                .collect::<Vec<_>>(),
                        )),
                        Err(error) if is_key_comparison_failure(&error) => Ok(None),
                        Err(error) => Err(Self::query_error(entry, error)),
                    }
                });
            }
            let (ranges, declined) = try_fold_bounded(
                queries,
                self.global_index_thread_num,
                (Vec::new(), false),
                |(ranges, declined), result| {
                    if let Some(result) = result {
                        ranges.extend(result);
                    } else {
                        *declined = true;
                    }
                },
            )
            .await?;
            if declined {
                continue;
            }
            return Ok(Some(CompositeIndexScanResult {
                key_field_ids: plan.fields.iter().map(|field| field.id()).collect(),
                result: GlobalIndexScanResult {
                    row_ranges: merge_row_ranges(ranges),
                    // Remaining predicates are always retained by the table reader.
                    evaluated_field_ids: plan
                        .fields
                        .iter()
                        .take(plan.bound_columns)
                        .map(|field| field.id())
                        .collect(),
                    indexed_coverage,
                },
            }));
        }
        Ok(None)
    }

    fn single_column_covers(&self, id: i32, composite_coverage: &[RowRange]) -> bool {
        let Some((_, entries)) = self.entries_by_field.iter().find(|(field, _)| *field == id)
        else {
            return false;
        };
        let entries = entries
            .iter()
            .filter(|entry| {
                matches!(
                    entry.index_type,
                    GlobalIndexFileKind::BTree | GlobalIndexFileKind::Bitmap
                )
            })
            .collect::<Vec<_>>();
        let single_coverage = merge_row_ranges(
            entries
                .iter()
                .map(|entry| RowRange::new(entry.row_range_start, entry.row_range_end))
                .collect(),
        );
        !entries.is_empty()
            && intersect_sorted_ranges(&single_coverage, composite_coverage) == composite_coverage
    }
}
