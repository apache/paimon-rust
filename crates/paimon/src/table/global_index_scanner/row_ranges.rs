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

//! Row-range conversion, coverage accounting, and lookup utilities.

use crate::spec::{FileKind, GlobalIndexSearchMode, IndexFileMeta, IndexManifestEntry};
use crate::table::{merge_row_ranges, source, RowRange};
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};

/// Convert a RoaringTreemap to merged RowRanges (already sorted and deduplicated).
pub(super) fn bitmap_to_ranges(bitmap: &RoaringTreemap) -> Vec<RowRange> {
    if bitmap.is_empty() {
        return Vec::new();
    }
    let mut ranges = Vec::new();
    let mut iter = bitmap.iter();
    let first = iter.next().unwrap();
    let mut start = first as i64;
    let mut end = start;

    for id in iter {
        let id = id as i64;
        if id == end + 1 {
            end = id;
        } else {
            ranges.push(RowRange::new(start, end));
            start = id;
            end = id;
        }
    }
    ranges.push(RowRange::new(start, end));
    ranges
}

/// Intersect two sorted range lists using RowRangeIndex for efficient binary search.
pub(super) fn intersect_sorted_ranges(a: &[RowRange], b: &[RowRange]) -> Vec<RowRange> {
    let idx = RowRangeIndex::create(a.to_vec());
    let mut result = Vec::new();
    for r in b {
        result.extend(idx.intersected_ranges(r.from(), r.to()));
    }
    result
}

pub(super) fn data_ranges_for_search_mode(
    search_mode: GlobalIndexSearchMode,
    next_row_id: Option<i64>,
    data_ranges: &[RowRange],
) -> Option<Vec<RowRange>> {
    match search_mode {
        GlobalIndexSearchMode::Fast => None,
        GlobalIndexSearchMode::Full => match next_row_id {
            Some(next_row_id) if next_row_id > 0 => Some(vec![RowRange::new(0, next_row_id - 1)]),
            _ => None,
        },
        GlobalIndexSearchMode::Detail => {
            if data_ranges.is_empty() {
                None
            } else {
                Some(data_ranges.to_vec())
            }
        }
    }
}

pub(super) fn indexed_ranges_from_coverage(
    coverage_by_field: &HashMap<i32, Vec<RowRange>>,
    field_ids: &HashSet<i32>,
) -> Vec<RowRange> {
    let mut ranges: Option<Vec<RowRange>> = None;
    for field_id in field_ids {
        let Some(field_ranges) = coverage_by_field.get(field_id) else {
            return Vec::new();
        };
        if field_ranges.is_empty() {
            return Vec::new();
        }
        let field_ranges = merge_row_ranges(field_ranges.clone());
        ranges = Some(match ranges {
            None => field_ranges,
            Some(existing) => intersect_sorted_ranges(&existing, &field_ranges),
        });
    }
    ranges.map(merge_row_ranges).unwrap_or_default()
}

pub(super) fn unindexed_ranges_for_coverage(
    coverage_by_field: &HashMap<i32, Vec<RowRange>>,
    field_ids: &HashSet<i32>,
    search_mode: GlobalIndexSearchMode,
    next_row_id: Option<i64>,
    data_ranges: &[RowRange],
) -> Vec<RowRange> {
    let Some(data_ranges) = data_ranges_for_search_mode(search_mode, next_row_id, data_ranges)
    else {
        return Vec::new();
    };
    let indexed_ranges = indexed_ranges_from_coverage(coverage_by_field, field_ids);
    source::exclude_row_ranges(&data_ranges, &indexed_ranges)
}

pub(super) fn unindexed_ranges_for_indexed_coverage(
    indexed_ranges: &[RowRange],
    search_mode: GlobalIndexSearchMode,
    next_row_id: Option<i64>,
    data_ranges: &[RowRange],
) -> Vec<RowRange> {
    let Some(data_ranges) = data_ranges_for_search_mode(search_mode, next_row_id, data_ranges)
    else {
        return Vec::new();
    };
    source::exclude_row_ranges(&data_ranges, &merge_row_ranges(indexed_ranges.to_vec()))
}

/// Compute row ranges not covered by a family of global index files.
///
/// This mirrors Java `GlobalIndexCoverage`: `full` compares index coverage
/// against `[0, snapshot.next_row_id - 1]`, while `detail` compares against
/// exact data-file row ranges supplied by the caller.
pub(crate) fn unindexed_ranges_for_global_index_entries(
    index_entries: &[IndexManifestEntry],
    field_ids: &HashSet<i32>,
    search_mode: GlobalIndexSearchMode,
    next_row_id: Option<i64>,
    data_ranges: &[RowRange],
    index_file_filter: impl Fn(&IndexFileMeta) -> bool,
) -> Vec<RowRange> {
    let mut coverage_by_field: HashMap<i32, Vec<RowRange>> = HashMap::new();
    for entry in index_entries {
        if entry.kind != FileKind::Add || !index_file_filter(&entry.index_file) {
            continue;
        }
        let Some(global_meta) = entry.index_file.global_index_meta.as_ref() else {
            continue;
        };
        let row_range = RowRange::new(global_meta.row_range_start, global_meta.row_range_end);
        coverage_by_field
            .entry(global_meta.index_field_id)
            .or_default()
            .push(row_range.clone());
        if let Some(extra_field_ids) = global_meta.extra_field_ids.as_ref() {
            for extra_field_id in extra_field_ids {
                coverage_by_field
                    .entry(*extra_field_id)
                    .or_default()
                    .push(row_range.clone());
            }
        }
    }
    unindexed_ranges_for_coverage(
        &coverage_by_field,
        field_ids,
        search_mode,
        next_row_id,
        data_ranges,
    )
}

/// Index for row ranges. Stores sorted, non-overlapping ranges and supports
/// efficient intersection queries via binary search.
///
/// Reference: [org.apache.paimon.utils.RowRangeIndex](https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/utils/RowRangeIndex.java)
#[derive(Debug, Clone)]
pub(crate) struct RowRangeIndex {
    ranges: Vec<RowRange>,
    starts: Vec<i64>,
    ends: Vec<i64>,
}

impl RowRangeIndex {
    /// Create a new `RowRangeIndex` from a list of ranges.
    /// Ranges are sorted and merged to eliminate overlaps.
    pub fn create(ranges: Vec<RowRange>) -> Self {
        let ranges = merge_row_ranges(ranges);
        let starts: Vec<i64> = ranges.iter().map(|r| r.from()).collect();
        let ends: Vec<i64> = ranges.iter().map(|r| r.to()).collect();
        Self {
            ranges,
            starts,
            ends,
        }
    }

    /// Returns the underlying ranges.
    #[cfg(test)]
    pub fn ranges(&self) -> &[RowRange] {
        &self.ranges
    }

    /// Returns true if the index has any range that intersects `[start, end]`.
    pub fn intersects(&self, start: i64, end: i64) -> bool {
        let candidate = lower_bound(&self.ends, start);
        candidate < self.starts.len() && self.starts[candidate] <= end
    }

    /// Counts rows in this index that intersect `[start, end]`.
    pub fn intersection_row_count(&self, start: i64, end: i64) -> usize {
        if start > end {
            return 0;
        }
        self.intersected_ranges(start, end)
            .into_iter()
            .fold(0usize, |total, range| {
                let len = range.to().saturating_sub(range.from()).saturating_add(1);
                total.saturating_add(usize::try_from(len).unwrap_or(usize::MAX))
            })
    }

    /// Returns the sub-ranges of this index that intersect `[start, end]`,
    /// clipped to the intersection boundaries.
    pub fn intersected_ranges(&self, start: i64, end: i64) -> Vec<RowRange> {
        let left = lower_bound(&self.ends, start);
        if left >= self.ranges.len() || self.starts[left] > end {
            return Vec::new();
        }

        let mut right = lower_bound(&self.ends, end);
        if right >= self.ranges.len() {
            right = self.ranges.len() - 1;
        }

        let mut result = Vec::new();

        // First range: clip from the left
        let first = &self.ranges[left];
        result.push(RowRange::new(start.max(first.from()), end.min(first.to())));

        // Middle ranges: fully included
        if right > left + 1 {
            for r in &self.ranges[left + 1..right] {
                result.push(r.clone());
            }
        }

        // Last range (if different from first): clip from the right
        if right != left {
            let last = &self.ranges[right];
            if last.from() <= end {
                result.push(RowRange::new(start.max(last.from()), end.min(last.to())));
            }
        }

        result
    }
}

pub(crate) fn search_limit_with_deleted_rows(
    limit: usize,
    row_range_start: i64,
    row_range_end: i64,
    deleted_rows: Option<&RowRangeIndex>,
) -> usize {
    let Some(range_len) = row_range_end
        .checked_sub(row_range_start)
        .and_then(|len| len.checked_add(1))
        .and_then(|len| usize::try_from(len).ok())
    else {
        return limit;
    };

    let deleted_count = deleted_rows
        .map(|index| index.intersection_row_count(row_range_start, row_range_end))
        .unwrap_or(0)
        .min(range_len);
    limit.saturating_add(deleted_count).min(range_len)
}

/// Binary search: find the first index where `sorted[index] >= target`.
fn lower_bound(sorted: &[i64], target: i64) -> usize {
    let mut left = 0;
    let mut right = sorted.len();
    while left < right {
        let mid = left + (right - left) / 2;
        if sorted[mid] < target {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}
