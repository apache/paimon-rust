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

//! Global index scanner: evaluates predicates against BTree global indexes
//! to produce row ID ranges for data evolution tables.
//!
//! Reference: [org.apache.paimon.index.GlobalIndexScanner](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/index/GlobalIndexScanner.java)

use crate::btree::query::{extract_between, IndexQuery};
use crate::btree::{make_key_comparator, serialize_datum, BTreeIndexMeta, BTreeIndexReader};
use crate::io::FileIO;
use crate::spec::{
    DataField, DataType, Datum, FileKind, GlobalIndexSearchMode, IndexFileMeta, IndexManifestEntry,
    Predicate, PredicateOperator,
};
use crate::table::RowRange;
use crate::Result;
use roaring::RoaringTreemap;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

type BoxedCmp = Box<dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync>;

type EvaluateFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<Vec<RowRange>>>> + Send + 'a>>;

type PredicateTuple<'a> = (PredicateOperator, &'a [Datum], &'a DataType);

const BTREE_INDEX_TYPE: &str = "btree";
const INDEX_DIR: &str = "index";

/// Evaluates global index predicates and returns matching row ranges.
///
/// The scanner filters index manifest entries for global index files,
/// uses BTreeIndexMeta for file-level pruning, then reads matching
/// BTree files to evaluate predicates and collect row IDs.
/// Opened BTreeIndexReaders are cached for reuse across evaluations.
pub(crate) struct GlobalIndexScanner {
    file_io: FileIO,
    table_path: String,
    /// Global index entries grouped by field_id.
    entries_by_field: Vec<(i32, Vec<GlobalIndexEntry>)>,
    /// Indexed row-id coverage grouped by field_id.
    coverage_by_field: HashMap<i32, Vec<RowRange>>,
    /// Schema fields for field_id lookup.
    schema_fields: Vec<DataField>,
    /// Cache of opened BTree readers, keyed by file name.
    reader_cache: Mutex<HashMap<String, BTreeIndexReader<BoxedCmp>>>,
}

/// A resolved global index entry with parsed metadata.
struct GlobalIndexEntry {
    file_name: String,
    row_range_start: i64,
    meta: BTreeIndexMeta,
}

impl GlobalIndexScanner {
    /// Create a scanner from index manifest entries.
    /// Returns `None` if there are no global index entries.
    pub(crate) fn create(
        file_io: &FileIO,
        table_path: &str,
        index_entries: &[IndexManifestEntry],
        schema_fields: &[DataField],
    ) -> Option<Self> {
        let mut entries_by_field: std::collections::HashMap<i32, Vec<GlobalIndexEntry>> =
            std::collections::HashMap::new();
        let mut coverage_by_field: HashMap<i32, Vec<RowRange>> = HashMap::new();

        for entry in index_entries {
            if entry.kind != FileKind::Add {
                continue;
            }
            if entry.index_file.index_type != BTREE_INDEX_TYPE {
                continue;
            }
            let global_meta = match &entry.index_file.global_index_meta {
                Some(m) => m,
                None => continue,
            };

            let btree_meta = global_meta
                .index_meta
                .as_ref()
                .and_then(|bytes| BTreeIndexMeta::deserialize(bytes).ok())
                .unwrap_or_else(|| BTreeIndexMeta::new(None, None, false));

            let resolved = GlobalIndexEntry {
                file_name: entry.index_file.file_name.clone(),
                row_range_start: global_meta.row_range_start,
                meta: btree_meta,
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

            entries_by_field
                .entry(global_meta.index_field_id)
                .or_default()
                .push(resolved);
        }

        if entries_by_field.is_empty() {
            return None;
        }

        Some(Self {
            file_io: file_io.clone(),
            table_path: table_path.trim_end_matches('/').to_string(),
            entries_by_field: entries_by_field.into_iter().collect(),
            coverage_by_field,
            schema_fields: schema_fields.to_vec(),
            reader_cache: Mutex::new(HashMap::new()),
        })
    }

    /// Evaluate a predicate against the global indexes and return matching row ranges.
    /// Returns `None` if the predicate cannot be evaluated by the global index.
    pub(crate) fn evaluate<'a>(&'a self, predicate: &'a Predicate) -> EvaluateFuture<'a> {
        Box::pin(async move {
            match predicate {
                Predicate::Leaf {
                    column,
                    op,
                    literals,
                    data_type,
                    ..
                } => {
                    if !is_btree_supported_op(*op) {
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
                    self.evaluate_leaf(entries, &[(*op, literals.as_slice(), data_type)])
                        .await
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
                            if is_btree_supported_op(*op) {
                                if let Some(field_id) = self.find_field_id_by_name(column)? {
                                    if self.entries_for_field(field_id).is_some() {
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

                    let mut result: Option<Vec<RowRange>> = None;

                    // Evaluate grouped leaves (one open per file)
                    for (field_id, predicates) in &leaf_groups {
                        if let Some(entries) = self.entries_for_field(*field_id) {
                            if let Some(ranges) = self.evaluate_leaf(entries, predicates).await? {
                                result = Some(match result {
                                    None => ranges,
                                    Some(existing) => intersect_sorted_ranges(&existing, &ranges),
                                });
                            }
                        }
                    }

                    // Evaluate non-leaf children recursively
                    for child in non_leaf_children {
                        if let Some(ranges) = self.evaluate(child).await? {
                            result = Some(match result {
                                None => ranges,
                                Some(existing) => intersect_sorted_ranges(&existing, &ranges),
                            });
                        }
                    }

                    Ok(result)
                }
                Predicate::Or(children) => {
                    let mut all_ranges: Vec<RowRange> = Vec::new();
                    for child in children {
                        match self.evaluate(child).await? {
                            Some(ranges) => all_ranges.extend(ranges),
                            None => return Ok(None),
                        }
                    }
                    if all_ranges.is_empty() {
                        Ok(Some(Vec::new()))
                    } else {
                        Ok(Some(super::merge_row_ranges(all_ranges)))
                    }
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
        entries: &[GlobalIndexEntry],
        predicates: &[(PredicateOperator, &[Datum], &DataType)],
    ) -> Result<Option<Vec<RowRange>>> {
        // Try to detect between pattern and split into (between, remaining)
        let (between, remaining) = extract_between(predicates);

        let effective_predicates = if between.is_some() {
            &remaining
        } else {
            predicates
        };

        let mut all_row_ids = RoaringTreemap::new();

        // Pre-compute comparators and serialized keys for file-level pruning per predicate
        let pruning_info: Vec<_> = effective_predicates
            .iter()
            .map(|(op, literals, data_type)| {
                let cmp = make_key_comparator(data_type);
                let serialized: Vec<Vec<u8>> = literals
                    .iter()
                    .map(|l| serialize_datum(l, data_type))
                    .collect();
                (*op, cmp, serialized)
            })
            .collect();

        for entry in entries {
            // Check if any predicate may match this file (use effective_predicates for pruning)
            let matching_predicates: Vec<usize> = (0..effective_predicates.len())
                .filter(|&i| {
                    entry
                        .meta
                        .may_match(pruning_info[i].0, &pruning_info[i].2, &pruning_info[i].1)
                })
                .collect();

            // Also check if between range may match
            let between_matches = between.as_ref().is_some_and(|b| {
                let cmp = make_key_comparator(b.data_type);
                let from_key = serialize_datum(b.from, b.data_type);
                let to_key = serialize_datum(b.to, b.data_type);
                entry.meta.may_match_between(&from_key, &to_key, &cmp)
            });

            // When a Between conjunct exists but the file does not overlap its
            // range, the whole AND cannot match — drop the file regardless of
            // how the remaining predicates evaluate. Without this guard, a file
            // outside the Between range but matched by some remaining predicate
            // (e.g. `BETWEEN 10 AND 20 AND id >= 0` on a file [30, 40]) would
            // be retained because `file_result` is initialized from the
            // remaining bitmap, silently dropping the Between conjunct.
            if between.is_some() && !between_matches {
                continue;
            }

            if matching_predicates.is_empty() && !between_matches {
                continue;
            }

            let data_type = between
                .as_ref()
                .map(|b| b.data_type)
                .or_else(|| effective_predicates.first().map(|p| p.2))
                .unwrap_or(predicates[0].2);
            let reader = self
                .get_or_open_reader(&entry.file_name, &entry.meta, data_type)
                .await?;

            let mut file_result: Option<RoaringTreemap> = None;

            // Execute between query first if applicable
            if between_matches {
                if let Some(b) = &between {
                    let from_key = serialize_datum(b.from, b.data_type);
                    let to_key = serialize_datum(b.to, b.data_type);
                    let bitmap = reader
                        .range_query(&from_key, &to_key, b.from_inclusive, b.to_inclusive)
                        .await
                        .map_err(|e| crate::Error::DataInvalid {
                            message: "BTree query failed".to_string(),
                            source: Some(Box::new(e)),
                        })?;
                    file_result = Some(bitmap);
                }
            }

            // Evaluate remaining predicates
            for &idx in &matching_predicates {
                let (op, literals, dt) = &effective_predicates[idx];
                let bitmap = reader.query(*op, literals, dt).await.map_err(|e| {
                    crate::Error::DataInvalid {
                        message: "BTree query failed".to_string(),
                        source: Some(Box::new(e)),
                    }
                })?;
                file_result = Some(match file_result {
                    None => bitmap,
                    Some(mut existing) => {
                        existing &= bitmap;
                        existing
                    }
                });
            }

            // Return reader to cache
            self.return_reader(entry.file_name.clone(), reader);

            if let Some(bitmap) = file_result {
                for rid in bitmap.iter() {
                    all_row_ids.insert(rid + entry.row_range_start as u64);
                }
            }
        }

        Ok(Some(bitmap_to_ranges(&all_row_ids)))
    }

    /// Get a cached reader or open a new one for the given file.
    async fn get_or_open_reader(
        &self,
        file_name: &str,
        meta: &BTreeIndexMeta,
        data_type: &DataType,
    ) -> Result<BTreeIndexReader<BoxedCmp>> {
        // Try to take from cache
        {
            let mut cache = self.reader_cache.lock().unwrap();
            if let Some(reader) = cache.remove(file_name) {
                return Ok(reader);
            }
        }

        // Open new reader
        let path = format!("{}/{INDEX_DIR}/{}", self.table_path, file_name);
        let input = self.file_io.new_input(&path)?;
        let file_size = input.metadata().await?.size;
        let file_reader = input.reader().await?;

        let cmp = make_key_comparator(data_type);
        BTreeIndexReader::open(Box::new(file_reader), file_size, meta, cmp)
            .await
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to open BTree index file: {file_name}"),
                source: Some(Box::new(e)),
            })
    }

    /// Return a reader to the cache for future reuse.
    fn return_reader(&self, file_name: String, reader: BTreeIndexReader<BoxedCmp>) {
        let mut cache = self.reader_cache.lock().unwrap();
        cache.insert(file_name, reader);
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
    fn unindexed_ranges(
        &self,
        predicate: &Predicate,
        search_mode: GlobalIndexSearchMode,
        next_row_id: Option<i64>,
        data_ranges: &[RowRange],
    ) -> Result<Vec<RowRange>> {
        let field_ids = self.collect_field_ids(predicate)?;
        Ok(unindexed_ranges_for_coverage(
            &self.coverage_by_field,
            &field_ids,
            search_mode,
            next_row_id,
            data_ranges,
        ))
    }

    fn collect_field_ids(&self, predicate: &Predicate) -> Result<HashSet<i32>> {
        let mut field_ids = HashSet::new();
        self.collect_field_ids_inner(predicate, &mut field_ids)?;
        Ok(field_ids)
    }

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

/// Whether the b-tree global index can evaluate this operator directly.
/// Operators that fall outside this set bypass the index and are evaluated
/// later in the read pipeline (stats prune + parquet row filter).
fn is_btree_supported_op(op: PredicateOperator) -> bool {
    matches!(
        op,
        PredicateOperator::Eq
            | PredicateOperator::NotEq
            | PredicateOperator::Lt
            | PredicateOperator::LtEq
            | PredicateOperator::Gt
            | PredicateOperator::GtEq
            | PredicateOperator::In
            | PredicateOperator::NotIn
            | PredicateOperator::IsNull
            | PredicateOperator::IsNotNull
            | PredicateOperator::Between
            | PredicateOperator::NotBetween
    )
}

/// Convert a RoaringTreemap to merged RowRanges (already sorted and deduplicated).
fn bitmap_to_ranges(bitmap: &RoaringTreemap) -> Vec<RowRange> {
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
fn intersect_sorted_ranges(a: &[RowRange], b: &[RowRange]) -> Vec<RowRange> {
    let idx = RowRangeIndex::create(a.to_vec());
    let mut result = Vec::new();
    for r in b {
        result.extend(idx.intersected_ranges(r.from(), r.to()));
    }
    result
}

fn exclude_row_ranges(data_ranges: &[RowRange], indexed_ranges: &[RowRange]) -> Vec<RowRange> {
    let data_ranges = super::merge_row_ranges(data_ranges.to_vec());
    if data_ranges.is_empty() {
        return Vec::new();
    }
    let indexed_ranges = super::merge_row_ranges(indexed_ranges.to_vec());
    if indexed_ranges.is_empty() {
        return data_ranges;
    }

    let mut result = Vec::new();
    for data_range in data_ranges {
        let mut cursor = data_range.from();
        let mut exhausted = false;
        for indexed_range in &indexed_ranges {
            if indexed_range.to() < cursor {
                continue;
            }
            if indexed_range.from() > data_range.to() {
                break;
            }
            if indexed_range.from() > cursor {
                result.push(RowRange::new(cursor, indexed_range.from() - 1));
            }
            if indexed_range.to() >= data_range.to() {
                exhausted = true;
                break;
            }
            cursor = cursor.max(indexed_range.to() + 1);
        }
        if !exhausted && cursor <= data_range.to() {
            result.push(RowRange::new(cursor, data_range.to()));
        }
    }
    super::merge_row_ranges(result)
}

fn data_ranges_for_search_mode(
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

fn indexed_ranges_from_coverage(
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
        let field_ranges = super::merge_row_ranges(field_ranges.clone());
        ranges = Some(match ranges {
            None => field_ranges,
            Some(existing) => intersect_sorted_ranges(&existing, &field_ranges),
        });
    }
    ranges.map(super::merge_row_ranges).unwrap_or_default()
}

fn unindexed_ranges_for_coverage(
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
    exclude_row_ranges(&data_ranges, &indexed_ranges)
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
        let ranges = super::merge_row_ranges(ranges);
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
    #[cfg(test)]
    pub fn intersects(&self, start: i64, end: i64) -> bool {
        let candidate = lower_bound(&self.ends, start);
        candidate < self.starts.len() && self.starts[candidate] <= end
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

/// Create a GlobalIndexScanner and evaluate predicates, returning row ranges.
/// This is the main entry point for the table scan integration.
///
/// Returns `None` if global index is not available or predicates can't be evaluated.
pub(crate) struct GlobalIndexEvaluation<'a> {
    pub(crate) file_io: &'a FileIO,
    pub(crate) table_path: &'a str,
    pub(crate) index_entries: &'a [IndexManifestEntry],
    pub(crate) predicates: &'a [Predicate],
    pub(crate) schema_fields: &'a [DataField],
    pub(crate) search_mode: GlobalIndexSearchMode,
    pub(crate) next_row_id: Option<i64>,
    pub(crate) data_ranges: &'a [RowRange],
}

pub(crate) async fn evaluate_global_index(
    evaluation: GlobalIndexEvaluation<'_>,
) -> Result<Option<Vec<RowRange>>> {
    let scanner = match GlobalIndexScanner::create(
        evaluation.file_io,
        evaluation.table_path,
        evaluation.index_entries,
        evaluation.schema_fields,
    ) {
        Some(s) => s,
        None => return Ok(None),
    };

    let combined = Predicate::and(evaluation.predicates.to_vec());

    let mut row_ranges = match scanner.evaluate(&combined).await? {
        Some(row_ranges) => row_ranges,
        None => return Ok(None),
    };
    row_ranges.extend(scanner.unindexed_ranges(
        &combined,
        evaluation.search_mode,
        evaluation.next_row_id,
        evaluation.data_ranges,
    )?);
    Ok(Some(super::merge_row_ranges(row_ranges)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_to_ranges() {
        assert_eq!(
            bitmap_to_ranges(&RoaringTreemap::new()),
            Vec::<RowRange>::new()
        );

        let mut bm = RoaringTreemap::new();
        bm.insert(5);
        assert_eq!(bitmap_to_ranges(&bm), vec![RowRange::new(5, 5)]);

        let mut bm = RoaringTreemap::new();
        for id in [1, 2, 3, 5, 6, 10] {
            bm.insert(id);
        }
        assert_eq!(
            bitmap_to_ranges(&bm),
            vec![
                RowRange::new(1, 3),
                RowRange::new(5, 6),
                RowRange::new(10, 10),
            ]
        );
    }

    #[test]
    fn test_intersect_sorted_ranges() {
        let a = vec![RowRange::new(0, 10), RowRange::new(20, 30)];
        let b = vec![RowRange::new(5, 25)];
        let result = intersect_sorted_ranges(&a, &b);
        assert_eq!(result, vec![RowRange::new(5, 10), RowRange::new(20, 25)]);
    }

    #[test]
    fn test_intersect_no_overlap() {
        let a = vec![RowRange::new(0, 5)];
        let b = vec![RowRange::new(10, 20)];
        assert!(intersect_sorted_ranges(&a, &b).is_empty());
    }

    #[test]
    fn test_serialize_datum_int() {
        let key = serialize_datum(&Datum::Int(42), &DataType::Int(crate::spec::IntType::new()));
        assert_eq!(key, 42i32.to_le_bytes().to_vec());
    }

    #[test]
    fn test_serialize_datum_string() {
        let key = serialize_datum(
            &Datum::String("hello".to_string()),
            &DataType::VarChar(crate::spec::VarCharType::new(100).unwrap()),
        );
        assert_eq!(key, b"hello".to_vec());
    }

    #[test]
    fn test_row_range_index_merges_overlapping() {
        let idx = RowRangeIndex::create(vec![
            RowRange::new(0, 5),
            RowRange::new(3, 10),
            RowRange::new(20, 30),
        ]);
        assert_eq!(idx.ranges().len(), 2);
        assert_eq!(idx.ranges()[0], RowRange::new(0, 10));
        assert_eq!(idx.ranges()[1], RowRange::new(20, 30));
    }

    #[test]
    fn test_row_range_index_merges_adjacent() {
        let idx = RowRangeIndex::create(vec![RowRange::new(0, 5), RowRange::new(6, 10)]);
        assert_eq!(idx.ranges().len(), 1);
        assert_eq!(idx.ranges()[0], RowRange::new(0, 10));
    }

    #[test]
    fn test_row_range_index_intersects() {
        let idx = RowRangeIndex::create(vec![RowRange::new(10, 20), RowRange::new(30, 40)]);
        assert!(idx.intersects(15, 25));
        assert!(idx.intersects(5, 10));
        assert!(idx.intersects(20, 30));
        assert!(!idx.intersects(0, 9));
        assert!(!idx.intersects(21, 29));
        assert!(!idx.intersects(41, 50));
    }

    #[test]
    fn test_row_range_index_intersected_ranges() {
        let idx = RowRangeIndex::create(vec![
            RowRange::new(10, 20),
            RowRange::new(30, 40),
            RowRange::new(50, 60),
        ]);
        let result = idx.intersected_ranges(15, 55);
        assert_eq!(
            result,
            vec![
                RowRange::new(15, 20),
                RowRange::new(30, 40),
                RowRange::new(50, 55),
            ]
        );
    }

    #[test]
    fn test_row_range_index_empty() {
        let idx = RowRangeIndex::create(Vec::new());
        assert!(!idx.intersects(0, 100));
        assert!(idx.intersected_ranges(0, 100).is_empty());
    }

    fn le_int_key(v: i32) -> Vec<u8> {
        v.to_le_bytes().to_vec()
    }

    /// Set up a temp dir with `index/{file_name}` containing the btree testdata file,
    /// and return (FileIO, table_path, file_name, _tmp_dir).
    fn setup_testdata_table(testdata_name: &str) -> (FileIO, String, String, tempfile::TempDir) {
        let src = format!(
            "{}/testdata/btree/{testdata_name}",
            env!("CARGO_MANIFEST_DIR")
        );
        let tmp = tempfile::tempdir().unwrap();
        let index_dir = tmp.path().join("index");
        std::fs::create_dir_all(&index_dir).unwrap();
        std::fs::copy(&src, index_dir.join(testdata_name)).unwrap();

        let table_path = format!("file://{}", tmp.path().display());
        let file_io = crate::io::FileIOBuilder::new("file").build().unwrap();
        (file_io, table_path, testdata_name.to_string(), tmp)
    }

    fn make_global_index_entry(
        file_name: &str,
        field_id: i32,
        row_range_start: i64,
        row_range_end: i64,
        meta: &BTreeIndexMeta,
    ) -> crate::spec::IndexManifestEntry {
        use crate::spec::{GlobalIndexMeta, IndexFileMeta};
        IndexManifestEntry {
            version: 1,
            kind: FileKind::Add,
            partition: vec![],
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: BTREE_INDEX_TYPE.to_string(),
                file_name: file_name.to_string(),
                file_size: 0,
                row_count: 0,
                deletion_vectors_ranges: None,
                global_index_meta: Some(GlobalIndexMeta {
                    row_range_start,
                    row_range_end,
                    index_field_id: field_id,
                    extra_field_ids: None,
                    index_meta: Some(meta.serialize()),
                }),
            },
        }
    }

    fn int_schema_fields() -> Vec<DataField> {
        vec![DataField::new(
            1,
            "id".to_string(),
            DataType::Int(crate::spec::IntType::new()),
        )]
    }

    async fn evaluate_global_index_fast(
        file_io: &FileIO,
        table_path: &str,
        entries: &[IndexManifestEntry],
        predicates: &[Predicate],
        fields: &[DataField],
    ) -> Result<Option<Vec<RowRange>>> {
        super::evaluate_global_index(super::GlobalIndexEvaluation {
            file_io,
            table_path,
            index_entries: entries,
            predicates,
            schema_fields: fields,
            search_mode: GlobalIndexSearchMode::Fast,
            next_row_id: None,
            data_ranges: &[],
        })
        .await
    }

    fn two_field_schema_fields() -> Vec<DataField> {
        vec![
            DataField::new(
                1,
                "id".to_string(),
                DataType::Int(crate::spec::IntType::new()),
            ),
            DataField::new(
                2,
                "value".to_string(),
                DataType::Int(crate::spec::IntType::new()),
            ),
        ]
    }

    fn int_eq(column: &str, index: usize, value: i32) -> Predicate {
        Predicate::Leaf {
            column: column.to_string(),
            index,
            data_type: DataType::Int(crate::spec::IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(value)],
        }
    }

    #[test]
    fn test_unindexed_ranges_fast_mode_empty() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let meta = BTreeIndexMeta::new(None, None, false);
        let entries = vec![make_global_index_entry("idx", 1, 0, 49, &meta)];
        let fields = int_schema_fields();
        let scanner =
            GlobalIndexScanner::create(&file_io, "memory:/t", &entries, &fields).expect("scanner");

        let ranges = scanner
            .unindexed_ranges(
                &int_eq("id", 0, 7),
                GlobalIndexSearchMode::Fast,
                Some(100),
                &[RowRange::new(50, 99)],
            )
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_unindexed_ranges_full_uses_snapshot_next_row_id() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let meta = BTreeIndexMeta::new(None, None, false);
        let entries = vec![make_global_index_entry("idx", 1, 0, 49, &meta)];
        let fields = int_schema_fields();
        let scanner =
            GlobalIndexScanner::create(&file_io, "memory:/t", &entries, &fields).expect("scanner");

        let ranges = scanner
            .unindexed_ranges(
                &int_eq("id", 0, 7),
                GlobalIndexSearchMode::Full,
                Some(100),
                &[],
            )
            .unwrap();
        assert_eq!(ranges, vec![RowRange::new(50, 99)]);
    }

    #[test]
    fn test_unindexed_ranges_detail_uses_data_file_ranges() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let meta = BTreeIndexMeta::new(None, None, false);
        let entries = vec![make_global_index_entry("idx", 1, 0, 49, &meta)];
        let fields = int_schema_fields();
        let scanner =
            GlobalIndexScanner::create(&file_io, "memory:/t", &entries, &fields).expect("scanner");

        let ranges = scanner
            .unindexed_ranges(
                &int_eq("id", 0, 7),
                GlobalIndexSearchMode::Detail,
                Some(100),
                &[
                    RowRange::new(0, 10),
                    RowRange::new(40, 60),
                    RowRange::new(80, 90),
                ],
            )
            .unwrap();
        assert_eq!(ranges, vec![RowRange::new(50, 60), RowRange::new(80, 90)]);
    }

    #[test]
    fn test_unindexed_ranges_uses_all_predicate_field_coverage() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let meta = BTreeIndexMeta::new(None, None, false);
        let entries = vec![
            make_global_index_entry("idx_id", 1, 0, 49, &meta),
            make_global_index_entry("idx_value", 2, 0, 99, &meta),
        ];
        let fields = two_field_schema_fields();
        let scanner =
            GlobalIndexScanner::create(&file_io, "memory:/t", &entries, &fields).expect("scanner");
        let predicate = Predicate::and(vec![int_eq("id", 0, 7), int_eq("value", 1, 8)]);

        let ranges = scanner
            .unindexed_ranges(&predicate, GlobalIndexSearchMode::Full, Some(100), &[])
            .unwrap();
        assert_eq!(ranges, vec![RowRange::new(50, 99)]);
    }

    #[test]
    fn test_unindexed_ranges_missing_field_coverage_reads_all_data_ranges() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let meta = BTreeIndexMeta::new(None, None, false);
        let entries = vec![make_global_index_entry("idx_id", 1, 0, 49, &meta)];
        let fields = two_field_schema_fields();
        let scanner =
            GlobalIndexScanner::create(&file_io, "memory:/t", &entries, &fields).expect("scanner");
        let predicate = Predicate::and(vec![int_eq("id", 0, 7), int_eq("value", 1, 8)]);

        let ranges = scanner
            .unindexed_ranges(&predicate, GlobalIndexSearchMode::Full, Some(100), &[])
            .unwrap();
        assert_eq!(ranges, vec![RowRange::new(0, 99)]);
    }

    #[test]
    fn test_unindexed_ranges_counts_extra_field_coverage() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let meta = BTreeIndexMeta::new(None, None, false);
        let mut entry = make_global_index_entry("idx_id_value", 1, 0, 99, &meta);
        entry
            .index_file
            .global_index_meta
            .as_mut()
            .unwrap()
            .extra_field_ids = Some(vec![2]);
        let fields = two_field_schema_fields();
        let scanner =
            GlobalIndexScanner::create(&file_io, "memory:/t", &[entry], &fields).expect("scanner");

        let ranges = scanner
            .unindexed_ranges(
                &int_eq("value", 1, 8),
                GlobalIndexSearchMode::Full,
                Some(100),
                &[],
            )
            .unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn test_evaluate_global_index_eq() {
        let (file_io, table_path, file_name, _tmp) =
            setup_testdata_table("btree_int_100_no_compress.bin");
        let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
        let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
        let fields = int_schema_fields();

        // key=50 -> row_id=25, offset by row_range_start=0 -> global row_id=25
        let predicates = vec![Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(crate::spec::IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(50)],
        }];

        let result =
            evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
                .await
                .unwrap();
        let ranges = result.unwrap();
        assert_eq!(ranges, vec![RowRange::new(25, 25)]);
    }

    #[tokio::test]
    async fn test_evaluate_global_index_full_mode_includes_unindexed_tail() {
        let (file_io, table_path, file_name, _tmp) =
            setup_testdata_table("btree_int_100_no_compress.bin");
        let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
        let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
        let fields = int_schema_fields();
        let predicates = vec![int_eq("id", 0, 50)];

        let result = super::evaluate_global_index(super::GlobalIndexEvaluation {
            file_io: &file_io,
            table_path: &table_path,
            index_entries: &entries,
            predicates: &predicates,
            schema_fields: &fields,
            search_mode: GlobalIndexSearchMode::Full,
            next_row_id: Some(150),
            data_ranges: &[],
        })
        .await
        .unwrap();

        assert_eq!(
            result.unwrap(),
            vec![RowRange::new(25, 25), RowRange::new(100, 149)]
        );
    }

    #[tokio::test]
    async fn test_evaluate_global_index_detail_mode_uses_data_ranges() {
        let (file_io, table_path, file_name, _tmp) =
            setup_testdata_table("btree_int_100_no_compress.bin");
        let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
        let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
        let fields = int_schema_fields();
        let predicates = vec![int_eq("id", 0, 50)];

        let data_ranges = [RowRange::new(90, 120), RowRange::new(140, 145)];
        let result = super::evaluate_global_index(super::GlobalIndexEvaluation {
            file_io: &file_io,
            table_path: &table_path,
            index_entries: &entries,
            predicates: &predicates,
            schema_fields: &fields,
            search_mode: GlobalIndexSearchMode::Detail,
            next_row_id: Some(150),
            data_ranges: &data_ranges,
        })
        .await
        .unwrap();

        assert_eq!(
            result.unwrap(),
            vec![
                RowRange::new(25, 25),
                RowRange::new(100, 120),
                RowRange::new(140, 145),
            ]
        );
    }

    #[tokio::test]
    async fn test_evaluate_global_index_range() {
        let (file_io, table_path, file_name, _tmp) =
            setup_testdata_table("btree_int_100_no_compress.bin");
        let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
        let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
        let fields = int_schema_fields();

        // keys 10..=20 -> keys 10,12,14,16,18,20 -> row_ids 5,6,7,8,9,10
        let predicates = vec![
            Predicate::Leaf {
                column: "id".to_string(),
                index: 0,
                data_type: DataType::Int(crate::spec::IntType::new()),
                op: PredicateOperator::GtEq,
                literals: vec![Datum::Int(10)],
            },
            Predicate::Leaf {
                column: "id".to_string(),
                index: 0,
                data_type: DataType::Int(crate::spec::IntType::new()),
                op: PredicateOperator::LtEq,
                literals: vec![Datum::Int(20)],
            },
        ];

        let result =
            evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
                .await
                .unwrap();
        let ranges = result.unwrap();
        assert_eq!(ranges, vec![RowRange::new(5, 10)]);
    }

    #[tokio::test]
    async fn test_evaluate_global_index_in() {
        let (file_io, table_path, file_name, _tmp) =
            setup_testdata_table("btree_int_100_no_compress.bin");
        let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
        let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
        let fields = int_schema_fields();

        // IN(0, 50, 198) -> row_ids 0, 25, 99
        let predicates = vec![Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(crate::spec::IntType::new()),
            op: PredicateOperator::In,
            literals: vec![Datum::Int(0), Datum::Int(50), Datum::Int(198)],
        }];

        let result =
            evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
                .await
                .unwrap();
        let ranges = result.unwrap();
        assert_eq!(
            ranges,
            vec![
                RowRange::new(0, 0),
                RowRange::new(25, 25),
                RowRange::new(99, 99)
            ]
        );
    }

    #[tokio::test]
    async fn test_evaluate_global_index_no_match() {
        let (file_io, table_path, file_name, _tmp) =
            setup_testdata_table("btree_int_100_no_compress.bin");
        let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
        let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
        let fields = int_schema_fields();

        // key=999 doesn't exist
        let predicates = vec![Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(crate::spec::IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(999)],
        }];

        let result =
            evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
                .await
                .unwrap();
        let ranges = result.unwrap();
        assert!(ranges.is_empty());
    }

    #[tokio::test]
    async fn test_evaluate_global_index_with_row_offset() {
        let (file_io, table_path, file_name, _tmp) =
            setup_testdata_table("btree_int_100_no_compress.bin");
        let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
        // row_range_start=1000 simulates an offset
        let entries = vec![make_global_index_entry(&file_name, 1, 1000, 1099, &meta)];
        let fields = int_schema_fields();

        // key=50 -> local row_id=25, offset -> global row_id=1025
        let predicates = vec![Predicate::Leaf {
            column: "id".to_string(),
            index: 0,
            data_type: DataType::Int(crate::spec::IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(50)],
        }];

        let result =
            evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
                .await
                .unwrap();
        let ranges = result.unwrap();
        assert_eq!(ranges, vec![RowRange::new(1025, 1025)]);
    }

    #[tokio::test]
    async fn test_evaluate_global_index_unknown_column() {
        let (file_io, table_path, file_name, _tmp) =
            setup_testdata_table("btree_int_100_no_compress.bin");
        let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
        let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
        let fields = int_schema_fields();

        // Column "unknown" not in schema -> None (can't evaluate)
        let predicates = vec![Predicate::Leaf {
            column: "unknown".to_string(),
            index: 0,
            data_type: DataType::Int(crate::spec::IntType::new()),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(50)],
        }];

        let result =
            evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
                .await
                .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_evaluate_global_index_multi_field_and() {
        // Two fields, each with its own btree index file (same data, different field_id).
        // btree_int_100_no_compress.bin: keys 0,2,4,...,198 -> row_ids 0,1,...,99
        let src = format!(
            "{}/testdata/btree/btree_int_100_no_compress.bin",
            env!("CARGO_MANIFEST_DIR")
        );
        let tmp = tempfile::tempdir().unwrap();
        let index_dir = tmp.path().join("index");
        std::fs::create_dir_all(&index_dir).unwrap();
        std::fs::copy(&src, index_dir.join("index_field1.bin")).unwrap();
        std::fs::copy(&src, index_dir.join("index_field2.bin")).unwrap();

        let table_path = format!("file://{}", tmp.path().display());
        let file_io = crate::io::FileIOBuilder::new("file").build().unwrap();

        let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);

        let fields = vec![
            DataField::new(
                1,
                "id".to_string(),
                DataType::Int(crate::spec::IntType::new()),
            ),
            DataField::new(
                2,
                "value".to_string(),
                DataType::Int(crate::spec::IntType::new()),
            ),
        ];

        let entries = vec![
            make_global_index_entry("index_field1.bin", 1, 0, 99, &meta),
            make_global_index_entry("index_field2.bin", 2, 0, 99, &meta),
        ];

        // id >= 40 AND id <= 60 → keys 40,42,...,60 → row_ids 20..30
        // value >= 44 AND value <= 52 → keys 44,46,48,50,52 → row_ids 22..26
        // AND → intersect [20..30] and [22..26] = [22..26]
        let predicates = vec![
            Predicate::Leaf {
                column: "id".to_string(),
                index: 0,
                data_type: DataType::Int(crate::spec::IntType::new()),
                op: PredicateOperator::GtEq,
                literals: vec![Datum::Int(40)],
            },
            Predicate::Leaf {
                column: "id".to_string(),
                index: 0,
                data_type: DataType::Int(crate::spec::IntType::new()),
                op: PredicateOperator::LtEq,
                literals: vec![Datum::Int(60)],
            },
            Predicate::Leaf {
                column: "value".to_string(),
                index: 1,
                data_type: DataType::Int(crate::spec::IntType::new()),
                op: PredicateOperator::GtEq,
                literals: vec![Datum::Int(44)],
            },
            Predicate::Leaf {
                column: "value".to_string(),
                index: 1,
                data_type: DataType::Int(crate::spec::IntType::new()),
                op: PredicateOperator::LtEq,
                literals: vec![Datum::Int(52)],
            },
        ];

        let result =
            evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
                .await
                .unwrap();
        let ranges = result.unwrap();
        assert_eq!(ranges, vec![RowRange::new(22, 26)]);
    }

    /// Regression for the Between+remaining bug in `evaluate_leaf`. When a
    /// native `Between` leaf is paired with another conjunct (e.g. `id >= 0`),
    /// and the file's b-tree key range falls **outside** the Between range
    /// but is still matched by the remaining predicate, the whole AND must
    /// produce zero rows. Before the fix, `file_result` was initialized from
    /// the remaining predicate's bitmap and the Between conjunct was silently
    /// dropped — the test would observe the file's full row id set instead of
    /// the empty set.
    #[tokio::test]
    async fn test_between_unmatched_file_drops_remaining_match() {
        let (file_io, table_path, file_name, _tmp) =
            setup_testdata_table("btree_int_100_no_compress.bin");
        // File covers keys [0, 198] (row_ids 0..99). Pick a Between range
        // entirely below 0 so `may_match_between` is false, and a `>= 0`
        // conjunct that would otherwise scoop up every row in the file.
        let meta = BTreeIndexMeta::new(Some(le_int_key(0)), Some(le_int_key(198)), false);
        let entries = vec![make_global_index_entry(&file_name, 1, 0, 99, &meta)];
        let fields = int_schema_fields();

        let predicates = vec![Predicate::and(vec![
            Predicate::Leaf {
                column: "id".to_string(),
                index: 0,
                data_type: DataType::Int(crate::spec::IntType::new()),
                op: PredicateOperator::Between,
                literals: vec![Datum::Int(-100), Datum::Int(-50)],
            },
            Predicate::Leaf {
                column: "id".to_string(),
                index: 0,
                data_type: DataType::Int(crate::spec::IntType::new()),
                op: PredicateOperator::GtEq,
                literals: vec![Datum::Int(0)],
            },
        ])];

        let result =
            evaluate_global_index_fast(&file_io, &table_path, &entries, &predicates, &fields)
                .await
                .unwrap();
        let ranges = result.unwrap();
        assert!(
            ranges.is_empty(),
            "Between(-100..-50) AND id>=0 must produce zero rows on a file \
             whose key range is [0, 198] — got {ranges:?}"
        );
    }
}
