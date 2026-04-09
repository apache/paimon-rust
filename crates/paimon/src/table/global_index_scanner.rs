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
    DataField, DataType, Datum, FileKind, IndexManifestEntry, Predicate, PredicateOperator,
};
use crate::table::RowRange;
use crate::Result;
use roaring::RoaringTreemap;
use std::cmp::Ordering;
use std::collections::HashMap;
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
        for field in &self.schema_fields {
            if field.name() == column {
                return Ok(Some(field.id()));
            }
        }
        Ok(None)
    }

    fn entries_for_field(&self, field_id: i32) -> Option<&[GlobalIndexEntry]> {
        self.entries_by_field
            .iter()
            .find(|(id, _)| *id == field_id)
            .map(|(_, entries)| entries.as_slice())
    }
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
pub(crate) async fn evaluate_global_index(
    file_io: &FileIO,
    table_path: &str,
    index_entries: &[IndexManifestEntry],
    predicates: &[Predicate],
    schema_fields: &[DataField],
) -> Result<Option<Vec<RowRange>>> {
    let scanner =
        match GlobalIndexScanner::create(file_io, table_path, index_entries, schema_fields) {
            Some(s) => s,
            None => return Ok(None),
        };

    let combined = Predicate::and(predicates.to_vec());

    scanner.evaluate(&combined).await
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

        let result = evaluate_global_index(&file_io, &table_path, &entries, &predicates, &fields)
            .await
            .unwrap();
        let ranges = result.unwrap();
        assert_eq!(ranges, vec![RowRange::new(25, 25)]);
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

        let result = evaluate_global_index(&file_io, &table_path, &entries, &predicates, &fields)
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

        let result = evaluate_global_index(&file_io, &table_path, &entries, &predicates, &fields)
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

        let result = evaluate_global_index(&file_io, &table_path, &entries, &predicates, &fields)
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

        let result = evaluate_global_index(&file_io, &table_path, &entries, &predicates, &fields)
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

        let result = evaluate_global_index(&file_io, &table_path, &entries, &predicates, &fields)
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

        let result = evaluate_global_index(&file_io, &table_path, &entries, &predicates, &fields)
            .await
            .unwrap();
        let ranges = result.unwrap();
        assert_eq!(ranges, vec![RowRange::new(22, 26)]);
    }
}
