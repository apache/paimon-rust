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

//! Table source types: DataSplit, Plan, DeletionFile, and related structs.
//!
//! Reference: [org.apache.paimon.table.source](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/).

use crate::spec::{BinaryRow, DataFileMeta};
use crate::table::stats_filter::group_by_overlapping_row_id;
use serde::{Deserialize, Serialize};

fn is_vector_store_file_name(file_name: &str) -> bool {
    file_name.to_ascii_lowercase().contains(".vector.")
}

pub(crate) fn is_data_evolution_normal_file(file: &DataFileMeta) -> bool {
    !crate::table::dedicated_format_file_writer::is_blob_file_name(&file.file_name)
        && !is_vector_store_file_name(&file.file_name)
}

pub(crate) fn data_evolution_anchor_file(files: &[DataFileMeta]) -> crate::Result<&DataFileMeta> {
    files
        .iter()
        .filter(|file| is_data_evolution_normal_file(file))
        .min_by_key(|file| (file.max_sequence_number, file.file_name.as_str()))
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "Data-evolution deletion vectors require a normal anchor file in each row range group.".to_string(),
            source: None,
        })
}
// ======================= RowRange ===============================

/// An inclusive row ID range `[from, to]` for filtering reads in data evolution mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowRange {
    from: i64,
    to: i64,
}

impl RowRange {
    pub fn new(from: i64, to: i64) -> Self {
        assert!(from <= to, "RowRange from ({from}) must be <= to ({to})");
        Self { from, to }
    }

    pub fn from(&self) -> i64 {
        self.from
    }

    pub fn to(&self) -> i64 {
        self.to
    }

    pub fn count(&self) -> i64 {
        self.to - self.from + 1
    }

    /// Check overlap with an inclusive file range `[file_start, file_end]`.
    pub fn overlaps_inclusive(&self, file_start: i64, file_end_inclusive: i64) -> bool {
        self.from <= file_end_inclusive && self.to >= file_start
    }

    /// Intersect with an inclusive file range `[file_start, file_end]`.
    pub fn intersect_inclusive(
        &self,
        file_start: i64,
        file_end_inclusive: i64,
    ) -> Option<RowRange> {
        let from = self.from.max(file_start);
        let to = self.to.min(file_end_inclusive);
        if from <= to {
            Some(RowRange::new(from, to))
        } else {
            None
        }
    }
}

/// Returns `true` if the file has no `first_row_id`.
pub fn any_range_overlaps_file(ranges: &[RowRange], file: &DataFileMeta) -> bool {
    match file.row_id_range() {
        None => true,
        Some((file_start, file_end)) => ranges
            .iter()
            .any(|r| r.overlaps_inclusive(file_start, file_end)),
    }
}

pub fn intersect_ranges_with_file(ranges: &[RowRange], file: &DataFileMeta) -> Vec<RowRange> {
    match file.row_id_range() {
        None => Vec::new(),
        Some((file_start, file_end)) => ranges
            .iter()
            .filter_map(|r| r.intersect_inclusive(file_start, file_end))
            .collect(),
    }
}

pub fn merge_row_ranges(mut ranges: Vec<RowRange>) -> Vec<RowRange> {
    if ranges.len() <= 1 {
        return ranges;
    }
    ranges.sort_by_key(|r| r.from);
    let mut merged: Vec<RowRange> = Vec::with_capacity(ranges.len());
    let mut iter = ranges.into_iter();
    let mut current = iter.next().unwrap();
    for r in iter {
        if r.from <= current.to.saturating_add(1) {
            current.to = current.to.max(r.to);
        } else {
            merged.push(current);
            current = r;
        }
    }
    merged.push(current);
    merged
}

/// Subtract `subtract` from `base`, returning the remaining sorted, non-overlapping
/// inclusive ranges. Mirrors Java `Range.exclude`. Inputs need not be sorted or
/// merged; the result is normalized (sorted, merged-adjacent, non-empty ranges).
pub(crate) fn exclude_row_ranges(base: &[RowRange], subtract: &[RowRange]) -> Vec<RowRange> {
    let base = merge_row_ranges(base.to_vec());
    let subtract = merge_row_ranges(subtract.to_vec());
    let mut result = Vec::new();
    for seg in base {
        // Current uncovered cursor within [seg.from, seg.to].
        let mut cursor = seg.from();
        // Set once a cut covers through seg.to(); the trailing suffix must then
        // be suppressed. Distinguishing this from `cursor > seg.to()` is what
        // keeps the i64::MAX edge correct: at i64::MAX, `saturating_add(1)`
        // cannot push the cursor past seg.to(), so we rely on this flag instead.
        let mut exhausted = false;
        for cut in &subtract {
            if cut.to() < cursor || cut.from() > seg.to() {
                continue; // cut entirely before the cursor or beyond this segment
            }
            if cut.from() > cursor {
                // Emit the gap before this cut.
                result.push(RowRange::new(cursor, cut.from() - 1));
            }
            if cut.to() >= seg.to() {
                // The cut reaches or passes the segment end; nothing remains.
                exhausted = true;
                break;
            }
            // Advance the cursor past the cut (never move it backwards).
            cursor = cursor.max(cut.to() + 1);
        }
        if !exhausted && cursor <= seg.to() {
            result.push(RowRange::new(cursor, seg.to()));
        }
    }
    result
}

#[cfg(test)]
mod row_range_tests {
    use super::*;

    fn file_meta_with_row_id(first_row_id: Option<i64>, row_count: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: "test.parquet".into(),
            file_size: 128,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: crate::spec::stats::BinaryTableStats::new(
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
            value_stats: crate::spec::stats::BinaryTableStats::new(
                Vec::new(),
                Vec::new(),
                Vec::new(),
            ),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: Vec::new(),
            creation_time: Some(chrono::Utc::now()),
            delete_row_count: None,
            embedded_index: None,
            first_row_id,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
        }
    }

    #[test]
    fn test_row_range_overlaps_inclusive_touching() {
        // [5, 10] overlaps [10, 15] because row 10 is in both
        let r = RowRange::new(5, 10);
        assert!(r.overlaps_inclusive(10, 15));
    }

    #[test]
    fn test_row_range_overlaps_inclusive_adjacent_no_overlap() {
        // [5, 9] does NOT overlap [10, 15]
        let r = RowRange::new(5, 9);
        assert!(!r.overlaps_inclusive(10, 15));
    }

    #[test]
    fn test_row_range_overlaps_inclusive_disjoint_before() {
        let r = RowRange::new(5, 8);
        assert!(!r.overlaps_inclusive(10, 15));
    }

    #[test]
    fn test_row_range_overlaps_inclusive_disjoint_after() {
        let r = RowRange::new(20, 30);
        assert!(!r.overlaps_inclusive(10, 15));
    }

    #[test]
    fn test_row_range_overlaps_inclusive_subset() {
        assert!(RowRange::new(12, 14).overlaps_inclusive(10, 15));
    }

    #[test]
    fn test_row_range_overlaps_inclusive_superset() {
        assert!(RowRange::new(5, 20).overlaps_inclusive(10, 15));
    }

    #[test]
    fn test_row_range_overlaps_inclusive_partial_left() {
        assert!(RowRange::new(8, 12).overlaps_inclusive(10, 15));
    }

    #[test]
    fn test_row_range_overlaps_inclusive_partial_right() {
        assert!(RowRange::new(14, 20).overlaps_inclusive(10, 15));
    }

    #[test]
    fn test_row_range_intersect_inclusive_no_overlap() {
        assert_eq!(RowRange::new(0, 5).intersect_inclusive(10, 15), None);
    }

    #[test]
    fn test_row_range_intersect_inclusive_partial() {
        assert_eq!(
            RowRange::new(8, 12).intersect_inclusive(10, 15),
            Some(RowRange::new(10, 12))
        );
    }

    #[test]
    fn test_row_range_intersect_inclusive_subset() {
        assert_eq!(
            RowRange::new(11, 14).intersect_inclusive(10, 15),
            Some(RowRange::new(11, 14))
        );
    }

    #[test]
    fn test_row_range_intersect_inclusive_superset() {
        assert_eq!(
            RowRange::new(5, 20).intersect_inclusive(10, 15),
            Some(RowRange::new(10, 15))
        );
    }

    #[test]
    fn test_row_range_intersect_inclusive_touching_end() {
        assert_eq!(
            RowRange::new(5, 10).intersect_inclusive(10, 15),
            Some(RowRange::new(10, 10))
        );
    }

    #[test]
    fn test_merge_row_ranges_non_overlapping() {
        let merged = merge_row_ranges(vec![RowRange::new(0, 4), RowRange::new(10, 15)]);
        assert_eq!(merged, vec![RowRange::new(0, 4), RowRange::new(10, 15)]);
    }

    #[test]
    fn test_merge_row_ranges_overlapping() {
        let merged = merge_row_ranges(vec![RowRange::new(0, 10), RowRange::new(5, 15)]);
        assert_eq!(merged, vec![RowRange::new(0, 15)]);
    }

    #[test]
    fn test_merge_row_ranges_adjacent() {
        // [0,5] and [6,10] are adjacent and should merge to [0,10]
        let merged = merge_row_ranges(vec![RowRange::new(0, 5), RowRange::new(6, 10)]);
        assert_eq!(merged, vec![RowRange::new(0, 10)]);
    }

    #[test]
    fn test_merge_row_ranges_unsorted() {
        let merged = merge_row_ranges(vec![
            RowRange::new(10, 20),
            RowRange::new(0, 5),
            RowRange::new(3, 12),
        ]);
        assert_eq!(merged, vec![RowRange::new(0, 20)]);
    }

    #[test]
    fn test_merge_row_ranges_single() {
        assert_eq!(
            merge_row_ranges(vec![RowRange::new(5, 10)]),
            vec![RowRange::new(5, 10)]
        );
    }

    #[test]
    fn test_merge_row_ranges_empty() {
        assert!(merge_row_ranges(Vec::new()).is_empty());
    }

    #[test]
    fn test_any_range_overlaps_file_with_overlap() {
        // file row_id_range = [10, 14]
        let file = file_meta_with_row_id(Some(10), 5);
        assert!(any_range_overlaps_file(
            &[RowRange::new(0, 5), RowRange::new(12, 20)],
            &file
        ));
    }

    #[test]
    fn test_any_range_overlaps_file_no_overlap() {
        // file row_id_range = [10, 14]
        let file = file_meta_with_row_id(Some(10), 5);
        assert!(!any_range_overlaps_file(
            &[RowRange::new(0, 5), RowRange::new(20, 30)],
            &file
        ));
    }

    #[test]
    fn test_any_range_overlaps_file_no_first_row_id() {
        let file = file_meta_with_row_id(None, 5);
        assert!(any_range_overlaps_file(&[RowRange::new(0, 5)], &file));
    }

    #[test]
    fn test_intersect_ranges_with_file_partial_overlap() {
        // file row_id_range = [10, 19]
        let file = file_meta_with_row_id(Some(10), 10);
        let result =
            intersect_ranges_with_file(&[RowRange::new(5, 14), RowRange::new(18, 25)], &file);
        assert_eq!(result, vec![RowRange::new(10, 14), RowRange::new(18, 19)]);
    }

    #[test]
    fn test_intersect_ranges_with_file_no_overlap() {
        // file row_id_range = [10, 14]
        let file = file_meta_with_row_id(Some(10), 5);
        assert!(
            intersect_ranges_with_file(&[RowRange::new(0, 5), RowRange::new(20, 30)], &file)
                .is_empty()
        );
    }

    #[test]
    fn test_intersect_ranges_with_file_full_overlap() {
        // file row_id_range = [10, 14]
        let file = file_meta_with_row_id(Some(10), 5);
        assert_eq!(
            intersect_ranges_with_file(&[RowRange::new(0, 100)], &file),
            vec![RowRange::new(10, 14)]
        );
    }

    #[test]
    fn test_intersect_ranges_with_file_no_first_row_id() {
        let file = file_meta_with_row_id(None, 5);
        assert!(intersect_ranges_with_file(&[RowRange::new(0, 100)], &file).is_empty());
    }

    #[test]
    fn test_row_range_count_and_empty() {
        let r = RowRange::new(5, 10);
        assert_eq!(r.count(), 6); // rows 5,6,7,8,9,10
    }
}

// ======================= DeletionFile ===============================

/// Deletion file for a data file: describes a region in a file that stores deletion vector bitmap.
///
/// Format of the region (first 4 bytes length, then magic, then RoaringBitmap content):
/// - First 4 bytes: length (should equal [Self::length]).
/// - Next 4 bytes: magic number (1581511376).
/// - Remaining: serialized RoaringBitmap.
///
/// Reference: [org.apache.paimon.table.source.DeletionFile](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/source/DeletionFile.java)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeletionFile {
    /// Path of the file containing the deletion vector (e.g. index file path).
    path: String,
    /// Starting offset of the deletion vector data in the file.
    offset: i64,
    /// Length in bytes of the deletion vector data.
    length: i64,
    /// Number of deleted rows (cardinality of the bitmap), if known.
    cardinality: Option<i64>,
}

impl DeletionFile {
    pub fn new(path: String, offset: i64, length: i64, cardinality: Option<i64>) -> Self {
        Self {
            path,
            offset,
            length,
            cardinality,
        }
    }

    /// Path of the file.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Starting offset of data in the file.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Length of data in the file.
    pub fn length(&self) -> i64 {
        self.length
    }

    /// Number of deleted rows, if known.
    pub fn cardinality(&self) -> Option<i64> {
        self.cardinality
    }
}

// ======================= PartitionBucket ===============================

/// Key for grouping splits by partition and bucket: (partition bytes, bucket id).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionBucket {
    pub partition: Vec<u8>,
    pub bucket: i32,
}

impl PartitionBucket {
    pub fn new(partition: Vec<u8>, bucket: i32) -> Self {
        Self { partition, bucket }
    }
}

// ======================= DataSplit ===============================

/// Input split for reading: partition + bucket + list of data files and optional deletion files.
///
/// Reference: [org.apache.paimon.table.source.DataSplit](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/source/DataSplit.java)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataSplit {
    snapshot_id: i64,
    partition: BinaryRow,
    bucket: i32,
    bucket_path: String,
    total_buckets: i32,
    data_files: Vec<DataFileMeta>,
    /// Deletion file for each data file, same order as `data_files`.
    /// `None` at index `i` means no deletion file for `data_files[i]` (matches Java getDeletionFiles() / List<DeletionFile> with null elements).
    data_deletion_files: Option<Vec<Option<DeletionFile>>>,
    row_ranges: Option<Vec<RowRange>>,
    /// Whether the split can be read raw, without the merge reader: its
    /// physical rows are exactly its logical rows (modulo deletion files).
    /// Mirrors Java `DataSplit#rawConvertible`.
    raw_convertible: bool,
}

impl DataSplit {
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }
    pub fn partition(&self) -> &BinaryRow {
        &self.partition
    }
    pub fn bucket(&self) -> i32 {
        self.bucket
    }
    pub fn bucket_path(&self) -> &str {
        &self.bucket_path
    }
    pub fn total_buckets(&self) -> i32 {
        self.total_buckets
    }

    pub fn data_files(&self) -> &[DataFileMeta] {
        &self.data_files
    }

    /// Deletion files for each data file (same order as `data_files`); `None` = no deletion file for that data file.
    pub fn data_deletion_files(&self) -> Option<&[Option<DeletionFile>]> {
        self.data_deletion_files.as_deref()
    }

    pub fn row_ranges(&self) -> Option<&[RowRange]> {
        self.row_ranges.as_deref()
    }

    /// Whether this split can be read raw (no sort-merge needed); see the
    /// field doc. Mirrors Java `DataSplit#rawConvertible`.
    pub fn raw_convertible(&self) -> bool {
        self.raw_convertible
    }

    /// Returns the deletion file for the data file at the given index, if any. `None` at that index means no deletion file.
    pub fn deletion_file_for_data_file_index(&self, index: usize) -> Option<&DeletionFile> {
        self.data_deletion_files
            .as_deref()?
            .get(index)
            .and_then(Option::as_ref)
    }

    /// Returns the deletion file for the given data file (by file name), if any.
    pub fn deletion_file_for_data_file(&self, file: &DataFileMeta) -> Option<&DeletionFile> {
        let index = self
            .data_files
            .iter()
            .position(|f| f.file_name == file.file_name)?;
        self.deletion_file_for_data_file_index(index)
    }

    /// Full path for a single data file in this split, respecting `_EXTERNAL_PATH`.
    pub fn data_file_path(&self, file: &DataFileMeta) -> String {
        file.data_file_path(&self.bucket_path)
    }

    /// Total row count of all data files in this split.
    pub fn row_count(&self) -> i64 {
        self.data_files.iter().map(|f| f.row_count).sum()
    }

    /// Returns the merged row count if it can be computed.
    ///
    /// Two paths, checked in the same order as Java:
    /// 1. Raw convertible splits (with all deletion-file cardinalities known):
    ///    physical row counts equal logical row counts, so sum `row_count`
    ///    minus deleted rows. Splits that need the sort-merge reader may
    ///    collapse multiple versions of a key into one row, so their physical
    ///    counts are only an upper bound and are never reported.
    /// 2. If all files have `first_row_id` (data evolution mode): merge
    ///    overlapping row ID ranges and take max row count per group.
    ///
    /// Returns `None` otherwise.
    ///
    /// Reference: [DataSplit.mergedRowCount()](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/source/DataSplit.java#L133)
    pub fn merged_row_count(&self) -> Option<i64> {
        if let Some(count) = self.raw_merged_row_count() {
            return Some(count);
        }
        self.data_evolution_merged_row_count()
    }

    /// Physical row count minus deletions, valid only for raw convertible
    /// splits with all deletion-file cardinalities known.
    ///
    /// Mirrors Java `rawMergedRowCountAvailable` + `rawMergedRowCount`.
    fn raw_merged_row_count(&self) -> Option<i64> {
        if !self.raw_convertible {
            return None;
        }
        match &self.data_deletion_files {
            None => Some(self.row_count()),
            Some(deletion_files) => {
                let mut total = 0i64;
                for (i, file) in self.data_files.iter().enumerate() {
                    let deleted_count = match deletion_files.get(i).and_then(|df| df.as_ref()) {
                        None => 0,
                        Some(df) => df.cardinality()?,
                    };
                    total += file.row_count - deleted_count;
                }
                Some(total)
            }
        }
    }

    /// Check if data evolution merged row count is available and compute it.
    ///
    /// Available when all files have `first_row_id` set. This is used for
    /// data evolution mode where files may have overlapping row ID ranges.
    ///
    /// The algorithm merges overlapping ranges and takes the max row count
    /// from each group (since overlapping files share some rows).
    ///
    /// Reference: [DataSplit.dataEvolutionMergedRowCount()](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/source/DataSplit.java#L174)
    fn data_evolution_merged_row_count(&self) -> Option<i64> {
        // Check all files have first_row_id
        if self.data_files.iter().any(|f| f.first_row_id.is_none()) {
            return None;
        }

        if self.data_files.is_empty() {
            return Some(0);
        }

        // Merge overlapping row ID ranges and compute max row_count per group
        let groups = group_by_overlapping_row_id(self.data_files.to_vec());
        let mut sum: i64 = groups
            .iter()
            .map(|group| group.iter().map(|f| f.row_count).max().unwrap_or(0))
            .sum();
        if let Some(deletion_files) = &self.data_deletion_files {
            for deletion_file in deletion_files.iter().flatten() {
                sum -= deletion_file.cardinality()?;
            }
        }

        Some(sum)
    }

    pub fn builder() -> DataSplitBuilder {
        DataSplitBuilder::new()
    }

    /// Serialize the DataSplit fields to Java `DataSplit#serialize` (version 8) binary.
    /// Byte-compatible with `compatibility/datasplit-v8`. Row ranges are not part of the v8
    /// format; `serialize_split_v1` wraps a row-range split as an `IndexedSplit` instead.
    pub fn serialize(&self) -> crate::Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(&SPLIT_MAGIC.to_be_bytes());
        out.extend_from_slice(&SPLIT_VERSION.to_be_bytes());
        out.extend_from_slice(&self.snapshot_id.to_be_bytes());
        let p = self.partition.to_serialized_bytes(); // serializeBinaryRow: writeInt(len) + [arity+data]
        out.extend_from_slice(&(p.len() as i32).to_be_bytes());
        out.extend_from_slice(&p);
        out.extend_from_slice(&self.bucket.to_be_bytes());
        write_java_utf(&mut out, &self.bucket_path)?;
        out.push(1); // totalBuckets present (version >= 6)
        out.extend_from_slice(&self.total_buckets.to_be_bytes());
        out.extend_from_slice(&0i32.to_be_bytes()); // deprecated beforeFiles count
        out.push(0); // beforeDeletionFiles = null list
        out.extend_from_slice(&(self.data_files.len() as i32).to_be_bytes());
        for f in &self.data_files {
            let d = f.to_serialized_row_data()?;
            out.extend_from_slice(&(d.len() as i32).to_be_bytes());
            out.extend_from_slice(&d);
        }
        write_deletion_list(&mut out, self.data_deletion_files.as_deref())?;
        out.push(0); // isStreaming = false
        out.push(u8::from(self.raw_convertible));
        Ok(out)
    }

    /// Reverse of [`DataSplit::serialize`]: parse a raw v8 `DataSplit#serialize` body.
    /// Consumes the entire buffer; trailing bytes are an error.
    pub fn deserialize(data: &[u8]) -> crate::Result<DataSplit> {
        let mut cur = data;
        let split = Self::read_v8_body(&mut cur)?;
        if !cur.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: format!("{} trailing bytes after DataSplit", cur.len()),
                source: None,
            });
        }
        Ok(split)
    }

    /// Read a v8 `DataSplit` body from the cursor, leaving it positioned after the body
    /// (used both by `deserialize` and the SPLIT_V1 frame reader).
    fn read_v8_body(cur: &mut &[u8]) -> crate::Result<DataSplit> {
        let magic = read_i64(cur)?;
        if magic != SPLIT_MAGIC {
            return Err(crate::Error::DataInvalid {
                message: format!("invalid DataSplit magic: {magic:#018x}"),
                source: None,
            });
        }
        match read_i32(cur)? {
            SPLIT_VERSION => Self::read_v8_body_after_version(cur),
            version => Err(crate::Error::Unsupported {
                message: format!(
                    "DataSplit version {version} not supported (only v{SPLIT_VERSION})"
                ),
            }),
        }
    }

    /// Read the fields following the magic + version header of a v8 `DataSplit` body.
    fn read_v8_body_after_version(cur: &mut &[u8]) -> crate::Result<DataSplit> {
        let snapshot_id = read_i64(cur)?;

        let part_len = read_i32(cur)?;
        if part_len < 0 {
            return Err(crate::Error::DataInvalid {
                message: "negative partition length".into(),
                source: None,
            });
        }
        let part_bytes = take(cur, part_len as usize)?;
        let partition = BinaryRow::from_serialized_bytes(part_bytes)?;

        let bucket = read_i32(cur)?;
        let bucket_path = read_java_utf(cur)?;

        let total_buckets = if read_u8(cur)? == 0 {
            None
        } else {
            Some(read_i32(cur)?)
        };

        let before_files_n = read_i32(cur)?; // deprecated, always 0 on write
        if before_files_n != 0 {
            return Err(crate::Error::Unsupported {
                message: format!("non-empty beforeFiles ({before_files_n}) not supported"),
            });
        }
        // `beforeDeletionFiles` is always a null list on write; a non-null list
        // is rejected rather than silently dropped (Java treats such a split as
        // invalid), matching the `beforeFiles` handling above.
        if read_deletion_list(cur)?.is_some() {
            return Err(crate::Error::Unsupported {
                message: "non-null beforeDeletionFiles not supported".to_string(),
            });
        }

        let data_files_n = read_i32(cur)?;
        if data_files_n < 0 {
            return Err(crate::Error::DataInvalid {
                message: "negative data_files count".into(),
                source: None,
            });
        }
        // Bound the eager reservation by the remaining bytes: each entry is at least a
        // 4-byte length prefix, so a crafted huge count cannot reserve more than the
        // buffer could ever hold. Underrun is still caught per-element below.
        let mut data_files = Vec::with_capacity((data_files_n as usize).min(cur.len() / 4));
        for _ in 0..data_files_n {
            let len = read_i32(cur)?;
            if len < 0 {
                return Err(crate::Error::DataInvalid {
                    message: "negative DataFileMeta length".into(),
                    source: None,
                });
            }
            let row = take(cur, len as usize)?;
            data_files.push(DataFileMeta::from_serialized_row_data(row)?);
        }

        let data_deletion_files = read_deletion_list(cur)?;
        // Rust only produces and serves batch splits (isStreaming = false); a
        // streaming split carries a semantic bit that Java readers branch on, so
        // reject it rather than silently dropping it.
        if read_u8(cur)? != 0 {
            return Err(crate::Error::Unsupported {
                message: "streaming DataSplit (isStreaming = true) not supported".to_string(),
            });
        }
        let raw_convertible = read_u8(cur)? != 0;

        let mut builder = DataSplitBuilder::new()
            .with_snapshot(snapshot_id)
            .with_partition(partition)
            .with_bucket(bucket)
            .with_bucket_path(bucket_path)
            .with_total_buckets(total_buckets.unwrap_or(1))
            .with_data_files(data_files)
            .with_raw_convertible(raw_convertible);
        if let Some(dels) = data_deletion_files {
            builder = builder.with_data_deletion_files(dels);
        }
        builder.build()
    }

    /// Java `SplitSerializer#serialize` frame: magic + version + type id + body. The cross-language
    /// entry point. A plain split serializes as `DataSplit` (type 1); a split carrying row ranges as
    /// `IndexedSplit` (type 3) wrapping the DataSplit body plus the ranges. Byte-compatible with
    /// `compatibility/split-v1-data` / `split-v1-indexed`.
    pub fn serialize_split_v1(&self) -> crate::Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(&SPLIT_SER_MAGIC.to_be_bytes());
        out.extend_from_slice(&SPLIT_SER_VERSION.to_be_bytes());
        match &self.row_ranges {
            None => {
                out.extend_from_slice(&SPLIT_SER_TYPE_DATA_SPLIT.to_be_bytes());
                out.extend_from_slice(&self.serialize()?);
            }
            Some(ranges) => {
                out.extend_from_slice(&SPLIT_SER_TYPE_INDEXED_SPLIT.to_be_bytes());
                // IndexedSplit#serialize: magic + version + DataSplit body + ranges + scores.
                out.extend_from_slice(&INDEXED_SPLIT_MAGIC.to_be_bytes());
                out.extend_from_slice(&INDEXED_SPLIT_VERSION.to_be_bytes());
                out.extend_from_slice(&self.serialize()?);
                out.extend_from_slice(&(ranges.len() as i32).to_be_bytes());
                for r in ranges {
                    out.extend_from_slice(&r.from().to_be_bytes());
                    out.extend_from_slice(&r.to().to_be_bytes());
                }
                out.push(0); // scores = null (vector scores are not modeled on the Rust split)
            }
        }
        Ok(out)
    }

    /// Reverse of [`DataSplit::serialize_split_v1`]: parse a Java `SplitSerializer` frame into a
    /// `DataSplit` (type 1) or an `IndexedSplit` (type 3, which carries `row_ranges`). Other split
    /// type ids, and IndexedSplit vector scores, are unsupported. Consumes the entire buffer.
    pub fn deserialize_split_v1(data: &[u8]) -> crate::Result<DataSplit> {
        let mut cur = data;
        let magic = read_i64(&mut cur)?;
        if magic != SPLIT_SER_MAGIC {
            return Err(crate::Error::DataInvalid {
                message: format!("invalid SPLIT_V1 magic: {magic:#018x}"),
                source: None,
            });
        }
        let version = read_i32(&mut cur)?;
        if version != SPLIT_SER_VERSION {
            return Err(crate::Error::Unsupported {
                message: format!("SplitSerializer version {version} not supported"),
            });
        }
        let type_id = read_i32(&mut cur)?;
        let split = match type_id {
            SPLIT_SER_TYPE_DATA_SPLIT => Self::read_v8_body(&mut cur)?,
            SPLIT_SER_TYPE_INDEXED_SPLIT => {
                let im = read_i64(&mut cur)?;
                if im != INDEXED_SPLIT_MAGIC {
                    return Err(crate::Error::DataInvalid {
                        message: format!("invalid IndexedSplit magic: {im:#018x}"),
                        source: None,
                    });
                }
                let iv = read_i32(&mut cur)?;
                if iv != INDEXED_SPLIT_VERSION {
                    return Err(crate::Error::Unsupported {
                        message: format!("IndexedSplit version {iv} not supported"),
                    });
                }
                let body = Self::read_v8_body(&mut cur)?;
                let ranges_n = read_i32(&mut cur)?;
                if ranges_n < 0 {
                    return Err(crate::Error::DataInvalid {
                        message: "negative row_ranges count".into(),
                        source: None,
                    });
                }
                // Bound the eager reservation by the remaining bytes: each RowRange is
                // 16 bytes (two i64), so a crafted huge count cannot over-reserve.
                let mut ranges = Vec::with_capacity((ranges_n as usize).min(cur.len() / 16));
                for _ in 0..ranges_n {
                    let from = read_i64(&mut cur)?;
                    let to = read_i64(&mut cur)?;
                    if from > to {
                        return Err(crate::Error::DataInvalid {
                            message: format!("row range from {from} > to {to}"),
                            source: None,
                        });
                    }
                    ranges.push(RowRange::new(from, to));
                }
                let scores_flag = read_u8(&mut cur)?;
                if scores_flag != 0 {
                    return Err(crate::Error::Unsupported {
                        message: "IndexedSplit scores are not supported".into(),
                    });
                }
                let mut body = body;
                body.row_ranges = Some(ranges);
                body
            }
            other => {
                return Err(crate::Error::Unsupported {
                    message: format!("unsupported split type id: {other}"),
                });
            }
        };
        if !cur.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: format!("{} trailing bytes after SPLIT_V1 frame", cur.len()),
                source: None,
            });
        }
        Ok(split)
    }
}

/// Java `DataSplit#MAGIC` / `VERSION` for the serialize format.
const SPLIT_MAGIC: i64 = -2394839472490812314;
const SPLIT_VERSION: i32 = 8;

/// Java `SplitSerializer` frame: magic "SPLIT_V1", version, and the `DataSplit` type id.
const SPLIT_SER_MAGIC: i64 = 0x53504C49545F5631; // "SPLIT_V1"
const SPLIT_SER_VERSION: i32 = 1;
const SPLIT_SER_TYPE_DATA_SPLIT: i32 = 1;
const SPLIT_SER_TYPE_INDEXED_SPLIT: i32 = 3;

/// Java `IndexedSplit#MAGIC` / `VERSION`.
const INDEXED_SPLIT_MAGIC: i64 = -938472394838495695;
const INDEXED_SPLIT_VERSION: i32 = 1;

/// Java `DataOutput#writeUTF`: modified UTF-8 over UTF-16 code units, prefixed by the u16
/// byte length. NUL and chars >= U+0800 (incl. surrogates for supplementary chars) take 2-3
/// bytes. Errors if the encoded form exceeds 65535 bytes, as Java throws UTFDataFormatException.
fn write_java_utf(out: &mut Vec<u8>, s: &str) -> crate::Result<()> {
    let byte_len: usize = s
        .encode_utf16()
        .map(|c| {
            if (0x0001..=0x007F).contains(&c) {
                1
            } else if c > 0x07FF {
                3
            } else {
                2
            }
        })
        .sum();
    if byte_len > 0xFFFF {
        return Err(crate::Error::DataInvalid {
            message: format!("string too long for writeUTF: {byte_len} bytes (max 65535)"),
            source: None,
        });
    }
    out.extend_from_slice(&(byte_len as u16).to_be_bytes());
    for c in s.encode_utf16() {
        if (0x0001..=0x007F).contains(&c) {
            out.push(c as u8);
        } else if c > 0x07FF {
            out.push(0xE0 | (c >> 12) as u8);
            out.push(0x80 | ((c >> 6) & 0x3F) as u8);
            out.push(0x80 | (c & 0x3F) as u8);
        } else {
            out.push(0xC0 | (c >> 6) as u8);
            out.push(0x80 | (c & 0x3F) as u8);
        }
    }
    Ok(())
}

/// Java `DeletionFile#serializeList`: `0` = null list; else `1` + count + per entry
/// (`0` = null, or `1` + path + offset + length + cardinality, -1 when absent).
fn write_deletion_list(
    out: &mut Vec<u8>,
    files: Option<&[Option<DeletionFile>]>,
) -> crate::Result<()> {
    match files {
        None => out.push(0),
        Some(list) => {
            out.push(1);
            out.extend_from_slice(&(list.len() as i32).to_be_bytes());
            for f in list {
                match f {
                    None => out.push(0),
                    Some(df) => {
                        out.push(1);
                        write_java_utf(out, df.path())?;
                        out.extend_from_slice(&df.offset().to_be_bytes());
                        out.extend_from_slice(&df.length().to_be_bytes());
                        out.extend_from_slice(&df.cardinality().unwrap_or(-1).to_be_bytes());
                    }
                }
            }
        }
    }
    Ok(())
}

/// Advances `cur` by `n` bytes, returning the consumed slice. Errors on underrun.
fn take<'a>(cur: &mut &'a [u8], n: usize) -> crate::Result<&'a [u8]> {
    if cur.len() < n {
        return Err(crate::Error::DataInvalid {
            message: format!("split buffer underrun: need {n}, have {}", cur.len()),
            source: None,
        });
    }
    let (head, tail) = cur.split_at(n);
    *cur = tail;
    Ok(head)
}

fn read_u8(cur: &mut &[u8]) -> crate::Result<u8> {
    Ok(take(cur, 1)?[0])
}

fn read_i16(cur: &mut &[u8]) -> crate::Result<i16> {
    Ok(i16::from_be_bytes(take(cur, 2)?.try_into().unwrap()))
}

fn read_i32(cur: &mut &[u8]) -> crate::Result<i32> {
    Ok(i32::from_be_bytes(take(cur, 4)?.try_into().unwrap()))
}

fn read_i64(cur: &mut &[u8]) -> crate::Result<i64> {
    Ok(i64::from_be_bytes(take(cur, 8)?.try_into().unwrap()))
}

fn utf_err() -> crate::Error {
    crate::Error::DataInvalid {
        message: "invalid modified UTF-8 in split".into(),
        source: None,
    }
}

/// Reverse of [`write_java_utf`]: `u16` byte-length prefix + modified UTF-8. Each UTF-16 code
/// unit is encoded independently, so a supplementary char arrives as two 3-byte surrogate units;
/// collect the raw `u16` units and let [`String::from_utf16`] pair the surrogates.
fn read_java_utf(cur: &mut &[u8]) -> crate::Result<String> {
    let len = read_i16(cur)? as u16 as usize;
    let bytes = take(cur, len)?;
    let mut units: Vec<u16> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        let (unit, adv) = if b & 0x80 == 0 {
            // 1-byte: 0xxxxxxx (includes raw 0x00)
            (b as u16, 1)
        } else if b & 0xE0 == 0xC0 {
            // 2-byte: 110xxxxx 10xxxxxx (0xC0 0x80 is Java's encoding of '\0')
            if i + 1 >= bytes.len() || bytes[i + 1] & 0xC0 != 0x80 {
                return Err(utf_err());
            }
            ((((b & 0x1F) as u16) << 6) | (bytes[i + 1] & 0x3F) as u16, 2)
        } else if b & 0xF0 == 0xE0 {
            // 3-byte: 1110xxxx 10xxxxxx 10xxxxxx
            if i + 2 >= bytes.len() || bytes[i + 1] & 0xC0 != 0x80 || bytes[i + 2] & 0xC0 != 0x80 {
                return Err(utf_err());
            }
            (
                (((b & 0x0F) as u16) << 12)
                    | (((bytes[i + 1] & 0x3F) as u16) << 6)
                    | (bytes[i + 2] & 0x3F) as u16,
                3,
            )
        } else {
            return Err(utf_err());
        };
        units.push(unit);
        i += adv;
    }
    String::from_utf16(&units).map_err(|_| utf_err())
}

/// Reverse of [`write_deletion_list`]: `0` = null list; else `1` + count + per entry
/// (`0` = null, or `1` + path + offset + length + cardinality, where `-1` decodes to `None`).
fn read_deletion_list(cur: &mut &[u8]) -> crate::Result<Option<Vec<Option<DeletionFile>>>> {
    if read_u8(cur)? == 0 {
        return Ok(None);
    }
    let count = read_i32(cur)?;
    if count < 0 {
        return Err(crate::Error::DataInvalid {
            message: format!("deletion list count {count} < 0"),
            source: None,
        });
    }
    // Bound the eager reservation by the remaining bytes: each entry is at least a
    // 1-byte null flag, so a crafted huge count cannot over-reserve.
    let mut out = Vec::with_capacity((count as usize).min(cur.len()));
    for _ in 0..count {
        if read_u8(cur)? == 0 {
            out.push(None);
        } else {
            let path = read_java_utf(cur)?;
            let offset = read_i64(cur)?;
            let length = read_i64(cur)?;
            let cardinality = read_i64(cur)?;
            let cardinality = if cardinality == -1 {
                None
            } else {
                Some(cardinality)
            };
            out.push(Some(DeletionFile::new(path, offset, length, cardinality)));
        }
    }
    Ok(Some(out))
}

/// Builder for [DataSplit].
///
/// Reference: [DataSplit.Builder](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/source/DataSplit.java)
#[derive(Debug)]
pub struct DataSplitBuilder {
    snapshot_id: i64,
    partition: Option<BinaryRow>,
    bucket: i32,
    bucket_path: Option<String>,
    total_buckets: i32,
    data_files: Option<Vec<DataFileMeta>>,
    /// Same length as data_files; `None` at index i = no deletion file for data_files[i].
    data_deletion_files: Option<Vec<Option<DeletionFile>>>,
    row_ranges: Option<Vec<RowRange>>,
    raw_convertible: bool,
}

impl DataSplitBuilder {
    pub fn new() -> Self {
        Self {
            snapshot_id: -1,
            partition: None,
            bucket: -1,
            bucket_path: None,
            total_buckets: -1,
            data_files: None,
            data_deletion_files: None,
            row_ranges: None,
            // Splits with no merge semantics (append tables, single-file
            // utility splits) are raw by nature; the merge-tree and
            // data-evolution scan paths set this explicitly per split group.
            raw_convertible: true,
        }
    }

    pub fn with_snapshot(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = snapshot_id;
        self
    }
    pub fn with_partition(mut self, partition: BinaryRow) -> Self {
        self.partition = Some(partition);
        self
    }
    pub fn with_bucket(mut self, bucket: i32) -> Self {
        self.bucket = bucket;
        self
    }
    pub fn with_bucket_path(mut self, bucket_path: String) -> Self {
        self.bucket_path = Some(bucket_path);
        self
    }
    pub fn with_total_buckets(mut self, total_buckets: i32) -> Self {
        self.total_buckets = total_buckets;
        self
    }
    pub fn with_data_files(mut self, data_files: Vec<DataFileMeta>) -> Self {
        self.data_files = Some(data_files);
        self
    }

    /// Sets deletion files; length must match data_files. Use `None` at index i when data_files[i] has no deletion file.
    pub fn with_data_deletion_files(
        mut self,
        data_deletion_files: Vec<Option<DeletionFile>>,
    ) -> Self {
        self.data_deletion_files = Some(data_deletion_files);
        self
    }

    pub fn with_row_ranges(mut self, row_ranges: Vec<RowRange>) -> Self {
        self.row_ranges = Some(row_ranges);
        self
    }

    /// Mark whether the split can be read raw; see [`DataSplit::raw_convertible`].
    pub fn with_raw_convertible(mut self, raw_convertible: bool) -> Self {
        self.raw_convertible = raw_convertible;
        self
    }

    pub fn build(self) -> crate::Result<DataSplit> {
        if self.snapshot_id == -1 {
            return Err(crate::Error::UnexpectedError {
                message: "DataSplit requires snapshot_id != -1".to_string(),
                source: None,
            });
        }
        let partition = self
            .partition
            .ok_or_else(|| crate::Error::UnexpectedError {
                message: "DataSplit requires partition".to_string(),
                source: None,
            })?;
        let bucket_path = self
            .bucket_path
            .ok_or_else(|| crate::Error::UnexpectedError {
                message: "DataSplit requires bucket_path".to_string(),
                source: None,
            })?;
        let data_files = self
            .data_files
            .ok_or_else(|| crate::Error::UnexpectedError {
                message: "DataSplit requires data_files".to_string(),
                source: None,
            })?;
        if self.bucket == -1 {
            return Err(crate::Error::UnexpectedError {
                message: "DataSplit requires bucket != -1".to_string(),
                source: None,
            });
        }
        if let Some(ref data_deletion_files) = self.data_deletion_files {
            if data_deletion_files.len() != data_files.len() {
                return Err(crate::Error::UnexpectedError {
                    message: format!(
                        "DataSplit deletion files length {} must match data_files length {}",
                        data_deletion_files.len(),
                        data_files.len()
                    ),
                    source: None,
                });
            }
        }
        Ok(DataSplit {
            snapshot_id: self.snapshot_id,
            partition,
            bucket: self.bucket,
            bucket_path,
            total_buckets: self.total_buckets,
            data_files,
            data_deletion_files: self.data_deletion_files,
            row_ranges: self.row_ranges,
            raw_convertible: self.raw_convertible,
        })
    }
}

impl Default for DataSplitBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ======================= Plan ===============================

/// Read plan: list of splits.
///
/// Reference: [org.apache.paimon.table.source.PlanImpl](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/source/PlanImpl.java)
#[derive(Debug)]
pub struct Plan {
    splits: Vec<DataSplit>,
}

impl Plan {
    pub fn new(splits: Vec<DataSplit>) -> Self {
        Self { splits }
    }
    pub fn splits(&self) -> &[DataSplit] {
        &self.splits
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;

    fn file(name: &str, row_count: i64, first_row_id: Option<i64>) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 128,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            value_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 1,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            first_row_id,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
        }
    }

    fn split(files: Vec<DataFileMeta>, raw_convertible: bool) -> DataSplit {
        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(files)
            .with_raw_convertible(raw_convertible)
            .build()
            .unwrap()
    }

    #[test]
    fn data_split_serde_json_round_trip() {
        let split = DataSplit::builder()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![])
            .build()
            .unwrap();

        let bytes = serde_json::to_vec(&split).expect("serialize");
        let restored: DataSplit = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(restored.snapshot_id(), split.snapshot_id());
        assert_eq!(restored.bucket(), split.bucket());
        assert_eq!(restored.bucket_path(), split.bucket_path());
    }

    /// Raw convertible split without deletion files: physical sum is exact.
    #[test]
    fn test_merged_row_count_raw_convertible_sums_physical_rows() {
        let s = split(vec![file("a", 10, None), file("b", 5, None)], true);
        assert_eq!(s.merged_row_count(), Some(15));
    }

    #[test]
    fn test_data_file_path_prefers_external_path() {
        let mut f = file("data-0.parquet", 10, None);
        f.external_path = Some("s3://bucket/table-external/data-0.parquet".to_string());
        let s = split(vec![f.clone()], true);
        assert_eq!(
            s.data_file_path(&f),
            "s3://bucket/table-external/data-0.parquet"
        );
    }

    /// Merge-needed split (multiple versions of a key may collapse): the
    /// physical sum is only an upper bound, so the count is unknown.
    /// Mirrors Java `rawMergedRowCountAvailable`.
    #[test]
    fn test_merged_row_count_unknown_for_merge_splits() {
        let s = split(vec![file("a", 10, None), file("b", 5, None)], false);
        assert_eq!(s.merged_row_count(), None);
    }

    /// Non-raw-convertible split where all files carry `first_row_id`: the
    /// data-evolution branch still applies (overlapping row-id groups count
    /// the max row count per group).
    #[test]
    fn test_merged_row_count_data_evolution_branch() {
        let mut a = file("a", 10, Some(0));
        a.row_count = 10;
        let mut b = file("b", 4, Some(0));
        b.row_count = 4;
        let c = file("c", 7, Some(100));
        let s = split(vec![a, b, c], false);
        // a and b share row ids [0, ..): max(10, 4) = 10; c adds 7.
        assert_eq!(s.merged_row_count(), Some(17));
    }

    /// Raw convertible split with a deletion file of known cardinality:
    /// deleted rows are subtracted; unknown cardinality makes the raw branch
    /// unavailable.
    #[test]
    fn test_merged_row_count_with_deletion_files() {
        let s = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![file("a", 10, None)])
            .with_data_deletion_files(vec![Some(DeletionFile::new(
                "file:/tmp/a.dv".to_string(),
                0,
                0,
                Some(3),
            ))])
            .with_raw_convertible(true)
            .build()
            .unwrap();
        assert_eq!(s.merged_row_count(), Some(7));

        let unknown = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![file("a", 10, None)])
            .with_data_deletion_files(vec![Some(DeletionFile::new(
                "file:/tmp/a.dv".to_string(),
                0,
                0,
                None,
            ))])
            .with_raw_convertible(true)
            .build()
            .unwrap();
        assert_eq!(unknown.merged_row_count(), None);
    }

    fn r(from: i64, to: i64) -> RowRange {
        RowRange::new(from, to)
    }

    fn pairs(ranges: &[RowRange]) -> Vec<(i64, i64)> {
        ranges.iter().map(|x| (x.from(), x.to())).collect()
    }

    #[test]
    fn exclude_empty_subtract_returns_base_merged() {
        // No subtraction -> base, normalized (sorted, merged-adjacent).
        let base = vec![r(0, 4), r(5, 9)];
        assert_eq!(pairs(&exclude_row_ranges(&base, &[])), vec![(0, 9)]);
    }

    #[test]
    fn exclude_empty_base_returns_empty() {
        assert!(exclude_row_ranges(&[], &[r(0, 9)]).is_empty());
    }

    #[test]
    fn exclude_no_overlap_returns_base() {
        let base = vec![r(10, 19)];
        let sub = vec![r(0, 9), r(20, 29)];
        assert_eq!(pairs(&exclude_row_ranges(&base, &sub)), vec![(10, 19)]);
    }

    #[test]
    fn exclude_prefix_leaves_suffix() {
        // The core incremental case: indexed [0,2], data [0,9] -> build [3,9].
        let base = vec![r(0, 9)];
        let sub = vec![r(0, 2)];
        assert_eq!(pairs(&exclude_row_ranges(&base, &sub)), vec![(3, 9)]);
    }

    #[test]
    fn exclude_middle_hole_splits_range() {
        let base = vec![r(0, 9)];
        let sub = vec![r(3, 5)];
        assert_eq!(
            pairs(&exclude_row_ranges(&base, &sub)),
            vec![(0, 2), (6, 9)]
        );
    }

    #[test]
    fn exclude_full_cover_returns_empty() {
        let base = vec![r(0, 9)];
        let sub = vec![r(0, 9)];
        assert!(exclude_row_ranges(&base, &sub).is_empty());
    }

    #[test]
    fn exclude_superset_cover_returns_empty() {
        let base = vec![r(2, 7)];
        let sub = vec![r(0, 100)];
        assert!(exclude_row_ranges(&base, &sub).is_empty());
    }

    #[test]
    fn exclude_multiple_subtract_ranges_and_boundaries() {
        // Adjacent subtract ranges must not leave a spurious 1-row gap.
        let base = vec![r(0, 20)];
        let sub = vec![r(0, 4), r(5, 9), r(15, 15)];
        assert_eq!(
            pairs(&exclude_row_ranges(&base, &sub)),
            vec![(10, 14), (16, 20)]
        );
    }

    #[test]
    fn exclude_multi_base_segments() {
        let base = vec![r(0, 4), r(10, 14)];
        let sub = vec![r(2, 11)];
        assert_eq!(
            pairs(&exclude_row_ranges(&base, &sub)),
            vec![(0, 1), (12, 14)]
        );
    }

    #[test]
    fn exclude_saturates_at_i64_max_boundary() {
        // A cut covering through i64::MAX must exhaust the segment, not emit a
        // spurious (i64::MAX, i64::MAX). Guards the shared primitive against the
        // i64::MAX saturation edge (read + write paths both rely on this fn).
        let base = vec![RowRange::new(5, i64::MAX)];
        let sub = vec![RowRange::new(5, i64::MAX)];
        assert!(exclude_row_ranges(&base, &sub).is_empty());

        // Partial cut up to i64::MAX from the middle leaves the prefix only.
        let base = vec![RowRange::new(0, i64::MAX)];
        let sub = vec![RowRange::new(10, i64::MAX)];
        assert_eq!(
            exclude_row_ranges(&base, &sub)
                .iter()
                .map(|r| (r.from(), r.to()))
                .collect::<Vec<_>>(),
            vec![(0, 9)]
        );
    }

    fn single_col(s: &str) -> Vec<u8> {
        let mut b = crate::spec::BinaryRowBuilder::new(1);
        b.write_bytes(0, s.as_bytes());
        b.build_serialized()
    }

    #[test]
    fn serialize_matches_datasplit_v8() {
        // Golden generated by Java (paimon-core DataSplitCompatibleTest) for cross-language parity.
        let expected = include_bytes!("goldens/datasplit_v8.bin");
        let split = sample_v8_split();
        assert_eq!(split.serialize().unwrap().as_slice(), &expected[..]);
    }

    /// The fixture split whose Java-generated bytes live in `goldens/datasplit_v8.bin`.
    /// Shared by the serialize and deserialize golden tests.
    fn sample_v8_split() -> DataSplit {
        use chrono::DateTime;

        let mut pb = crate::spec::BinaryRowBuilder::new(1);
        pb.write_bytes(0, b"aaaaa");

        let file = DataFileMeta {
            file_name: "my_file".to_string(),
            file_size: 1024 * 1024,
            row_count: 1024,
            min_key: single_col("min_key"),
            max_key: single_col("max_key"),
            key_stats: BinaryTableStats::new(
                single_col("min_key"),
                single_col("max_key"),
                vec![Some(0)],
            ),
            value_stats: BinaryTableStats::new(
                single_col("min_value"),
                single_col("max_value"),
                vec![Some(0)],
            ),
            min_sequence_number: 15,
            max_sequence_number: 200,
            schema_id: 5,
            level: 3,
            extra_files: vec!["extra1".to_string(), "extra2".to_string()],
            creation_time: DateTime::from_timestamp_millis(1646252412000),
            delete_row_count: Some(11),
            embedded_index: Some(vec![1, 2, 4]),
            first_row_id: Some(12),
            write_cols: Some(["a", "b", "c", "f"].iter().map(|s| s.to_string()).collect()),
            external_path: Some("hdfs:///path/to/warehouse".to_string()),
            file_source: Some(1),
            value_stats_cols: Some(
                ["field1", "field2", "field3"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            ),
        };

        DataSplitBuilder::new()
            .with_snapshot(18)
            .with_partition(pb.build())
            .with_bucket(20)
            .with_total_buckets(32)
            .with_bucket_path("my path".to_string())
            .with_data_files(vec![file])
            .with_data_deletion_files(vec![Some(DeletionFile::new(
                "deletion_file".to_string(),
                100,
                22,
                Some(33),
            ))])
            .with_raw_convertible(false)
            .build()
            .unwrap()
    }

    #[test]
    fn deserialize_matches_datasplit_v8_golden() {
        let golden = include_bytes!("goldens/datasplit_v8.bin");
        let split = DataSplit::deserialize(golden).unwrap();
        assert_eq!(split, sample_v8_split());
    }

    #[test]
    fn deserialize_round_trips_serialize() {
        let split = sample_v8_split();
        assert_eq!(
            DataSplit::deserialize(&split.serialize().unwrap()).unwrap(),
            split
        );
    }

    #[test]
    fn deserialize_rejects_trailing_bytes() {
        let mut bytes = sample_v8_split().serialize().unwrap();
        bytes.push(0xFF);
        assert!(DataSplit::deserialize(&bytes).is_err());
    }

    #[test]
    fn deserialize_rejects_bad_magic() {
        let bytes = [0u8; 12];
        assert!(DataSplit::deserialize(&bytes).is_err());
    }

    // A non-null `beforeDeletionFiles` list is rejected rather than silently
    // discarded (Java treats such a split as invalid). Walk the wire layout with
    // the same readers to locate the null-list flag, then flip it to a non-null
    // (empty) list and confirm deserialize fails loudly.
    #[test]
    fn deserialize_rejects_non_null_before_deletion_files() {
        let split = sample_v8_split();
        let bytes = split.serialize().unwrap();
        assert!(DataSplit::deserialize(&bytes).is_ok());

        let mut cur = bytes.as_slice();
        read_i64(&mut cur).unwrap(); // magic
        read_i32(&mut cur).unwrap(); // version
        read_i64(&mut cur).unwrap(); // snapshot_id
        let part_len = read_i32(&mut cur).unwrap() as usize;
        take(&mut cur, part_len).unwrap(); // partition bytes
        read_i32(&mut cur).unwrap(); // bucket
        read_java_utf(&mut cur).unwrap(); // bucket_path
        if read_u8(&mut cur).unwrap() != 0 {
            read_i32(&mut cur).unwrap(); // total_buckets
        }
        read_i32(&mut cur).unwrap(); // beforeFiles count (0)
                                     // `cur` now points at the beforeDeletionFiles null-list flag byte.
        let offset = bytes.len() - cur.len();

        let mut patched = bytes.clone();
        // null-list marker (0x00) -> non-null empty list (0x01 + i32 count 0).
        patched.splice(offset..offset + 1, [1u8, 0, 0, 0, 0]);
        match DataSplit::deserialize(&patched) {
            Err(crate::Error::Unsupported { .. }) => {}
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    // A streaming split (isStreaming = true) is rejected rather than silently
    // dropping the bit: Rust only writes/serves batch splits (isStreaming =
    // false), and Java readers branch on this flag. The isStreaming byte is the
    // second-to-last byte on the wire (isStreaming, then raw_convertible).
    #[test]
    fn deserialize_rejects_streaming_split() {
        let bytes = sample_v8_split().serialize().unwrap();
        assert!(DataSplit::deserialize(&bytes).is_ok());

        let mut patched = bytes.clone();
        let pos = patched.len() - 2; // isStreaming flag
        assert_eq!(
            patched[pos], 0,
            "fixture must serialize isStreaming = false"
        );
        patched[pos] = 1;
        match DataSplit::deserialize(&patched) {
            Err(crate::Error::Unsupported { .. }) => {}
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_rejects_huge_data_files_count_without_aborting() {
        // Build a well-formed split with an empty data-file list so the trailing layout
        // after `data_files_n` is fixed: deletion-null (1) + isStreaming (1) +
        // raw_convertible (1) = 3 bytes. Thus `data_files_n` (i32) sits at `len - 7`.
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRowBuilder::new(0).build())
            .with_bucket(0)
            .with_bucket_path("p".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![])
            .with_raw_convertible(false)
            .build()
            .unwrap();
        let mut bytes = split.serialize().unwrap();
        // Sanity: unpatched buffer round-trips.
        assert!(DataSplit::deserialize(&bytes).is_ok());

        let pos = bytes.len() - 7;
        bytes[pos..pos + 4].copy_from_slice(&i32::MAX.to_be_bytes());
        match DataSplit::deserialize(&bytes) {
            Err(crate::Error::DataInvalid { .. }) => {}
            other => panic!("expected DataInvalid, got {other:?}"),
        }
    }

    // Same hardening for the IndexedSplit row-ranges count in the SPLIT_V1 frame.
    #[test]
    fn deserialize_split_v1_rejects_huge_ranges_count_without_aborting() {
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRowBuilder::new(0).build())
            .with_bucket(0)
            .with_bucket_path("p".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![])
            .with_row_ranges(vec![RowRange::new(0, 5)])
            .with_raw_convertible(false)
            .build()
            .unwrap();
        let mut bytes = split.serialize_split_v1().unwrap();
        assert!(DataSplit::deserialize_split_v1(&bytes).is_ok());

        // Trailing after `ranges_n` (i32): one RowRange (16) + scores flag (1) = 17 bytes.
        let pos = bytes.len() - 21;
        bytes[pos..pos + 4].copy_from_slice(&i32::MAX.to_be_bytes());
        match DataSplit::deserialize_split_v1(&bytes) {
            Err(crate::Error::DataInvalid { .. }) => {}
            other => panic!("expected DataInvalid, got {other:?}"),
        }
    }

    #[test]
    fn write_java_utf_matches_java_modified_utf8() {
        let enc = |s: &str| {
            let mut b = Vec::new();
            write_java_utf(&mut b, s).unwrap();
            b
        };
        // ASCII: u16 length + raw bytes.
        assert_eq!(enc("ab"), vec![0, 2, b'a', b'b']);
        // NUL -> C0 80 (2 bytes), not 0x00.
        assert_eq!(enc("\u{0}"), vec![0, 2, 0xC0, 0x80]);
        // U+00E9 'é' -> 2 bytes C3 A9.
        assert_eq!(enc("\u{00E9}"), vec![0, 2, 0xC3, 0xA9]);
        // U+4E2D '中' -> 3 bytes E4 B8 AD.
        assert_eq!(enc("\u{4E2D}"), vec![0, 3, 0xE4, 0xB8, 0xAD]);
        // U+1F600 -> surrogate pair D83D DE00, each surrogate 3 bytes -> 6 bytes.
        assert_eq!(
            enc("\u{1F600}"),
            vec![0, 6, 0xED, 0xA0, 0xBD, 0xED, 0xB8, 0x80]
        );
        // Overlong -> error (like Java UTFDataFormatException).
        let mut sink = Vec::new();
        assert!(write_java_utf(&mut sink, &"a".repeat(70000)).is_err());
    }

    #[test]
    fn java_utf_round_trips() {
        for s in [
            "",
            "ascii",
            "caf\u{e9}",
            "\0",
            "\u{800}",
            "\u{1F600}emoji",
            &"x".repeat(1000),
        ] {
            let mut out = Vec::new();
            write_java_utf(&mut out, s).unwrap();
            let mut cur = out.as_slice();
            assert_eq!(read_java_utf(&mut cur).unwrap(), s);
            assert!(cur.is_empty(), "should consume exactly the encoded bytes");
        }
    }

    #[test]
    fn java_utf_rejects_truncated() {
        let mut out = Vec::new();
        write_java_utf(&mut out, "hello").unwrap();
        out.truncate(out.len() - 2); // drop tail bytes
        let mut cur = out.as_slice();
        assert!(read_java_utf(&mut cur).is_err());
    }

    #[test]
    fn java_utf_rejects_malformed_continuation() {
        // len=2, lead 0xC0 (2-byte form) followed by 0x41 ('A'), which is NOT a
        // 10xxxxxx continuation byte. Must error, not silently decode to "\u{1}".
        let bytes = [0x00u8, 0x02, 0xC0, 0x41];
        let mut cur = bytes.as_slice();
        assert!(read_java_utf(&mut cur).is_err());
    }

    #[test]
    fn deletion_list_round_trips() {
        for list in [
            None,
            Some(vec![None]),
            Some(vec![
                Some(DeletionFile::new("f.idx".into(), 1, 2, Some(3))),
                None,
            ]),
        ] {
            let mut out = Vec::new();
            write_deletion_list(&mut out, list.as_deref()).unwrap();
            let mut cur = out.as_slice();
            assert_eq!(read_deletion_list(&mut cur).unwrap(), list);
            assert!(cur.is_empty());
        }
    }

    #[test]
    fn deletion_list_cardinality_sentinel_maps_to_none() {
        // A -1 cardinality on the wire must decode back to `None`, matching the
        // forward `unwrap_or(-1)`.
        let list = Some(vec![Some(DeletionFile::new("f.idx".into(), 4, 5, None))]);
        let mut out = Vec::new();
        write_deletion_list(&mut out, list.as_deref()).unwrap();
        let mut cur = out.as_slice();
        let back = read_deletion_list(&mut cur).unwrap().unwrap();
        assert_eq!(back[0].as_ref().unwrap().cardinality(), None);
        assert!(cur.is_empty());
    }

    #[test]
    fn binary_array_str_var_element() {
        // Element > 7 bytes takes the offset+length pointer branch, which datasplit-v8
        // (all elements <= 7, inline) never exercises. Check the pointer resolves to the value.
        let out = crate::spec::serialize_binary_array_str(&["abcdefgh".to_string()]);
        assert_eq!(&out[0..4], &1i32.to_le_bytes()); // element count
        let slot = u64::from_le_bytes(out[8..16].try_into().unwrap());
        let off = (slot >> 32) as usize;
        let len = (slot & 0xFFFF_FFFF) as usize;
        assert_eq!(len, 8);
        assert_eq!(&out[off..off + len], b"abcdefgh");
    }

    #[test]
    fn binary_array_long_multiword_null_bitset() {
        // n > 32 spans a second 4-byte null word; a None at index >= 32 must set the right bit.
        let mut vals: Vec<Option<i64>> = (0..40).map(|i| Some(i as i64)).collect();
        vals[35] = None;
        let out = crate::spec::serialize_binary_array_long(&vals);
        assert_eq!(&out[0..4], &40i32.to_le_bytes()); // element count
        let header = 4 + 40usize.div_ceil(32) * 4; // 12
        assert_eq!(out[4 + 35 / 8] & (1 << (35 % 8)), 1 << (35 % 8)); // null bit in 2nd word
        assert_eq!(out[4] & 1, 0); // element 0 not null
        assert_eq!(
            i64::from_le_bytes(out[header..header + 8].try_into().unwrap()),
            0
        );
    }

    fn int_binary_row(vals: &[i32]) -> BinaryRow {
        let mut b = crate::spec::BinaryRowBuilder::new(vals.len() as i32);
        for (i, v) in vals.iter().enumerate() {
            b.write_int(i, *v);
        }
        b.build()
    }

    // Mirror SplitSerializerTest.dataFile(name, level, minKey, maxKey, maxSequence).
    fn v1_data_file(
        name: &str,
        level: i32,
        min_key: i32,
        max_key: i32,
        max_seq: i64,
    ) -> DataFileMeta {
        let size = (max_key - min_key + 1) as i64;
        DataFileMeta {
            file_name: name.to_string(),
            file_size: size,
            row_count: size,
            min_key: int_binary_row(&[min_key]).to_serialized_bytes(),
            max_key: int_binary_row(&[max_key]).to_serialized_bytes(),
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: max_seq,
            schema_id: 0,
            level,
            extra_files: Vec::new(),
            creation_time: chrono::DateTime::from_timestamp_millis(100),
            delete_row_count: Some(0),
            embedded_index: None,
            file_source: Some(0), // FileSource.APPEND
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
        }
    }

    // Mirrors SplitSerializerTest.dataSplit(): two files, EMPTY_STATS, null-padded deletion list.
    fn v1_data_split_builder() -> DataSplitBuilder {
        DataSplitBuilder::new()
            .with_snapshot(42)
            .with_partition(int_binary_row(&[2026, 7]))
            .with_bucket(3)
            .with_total_buckets(8)
            .with_bucket_path("dt=20260706/bucket-3".to_string())
            .with_data_files(vec![
                v1_data_file("file-a", 0, 1, 10, 100),
                v1_data_file("file-b", 1, 11, 20, 200),
            ])
            .with_data_deletion_files(vec![
                None,
                Some(DeletionFile::new("dv/file-b".to_string(), 2, 10, Some(3))),
            ])
            .with_raw_convertible(true)
    }

    #[test]
    fn serialize_split_v1_matches_golden() {
        // Golden generated by Java (paimon-core) for byte-for-byte cross-language compatibility.
        let expected = include_bytes!("goldens/split_v1_data.bin");
        let split = v1_data_split_builder().build().unwrap();
        assert_eq!(
            split.serialize_split_v1().unwrap().as_slice(),
            &expected[..]
        );
    }

    #[test]
    fn serialize_indexed_split_v1_matches_golden() {
        // Row ranges -> IndexedSplit (type 3): same DataSplit as split-v1-data + ranges [1,4],[11,13].
        // The Java golden ends with vector scores, which the Rust split has none of, so compare against
        // the golden up to its scores tail plus a `false` scores flag.
        let golden = include_bytes!("goldens/split_v1_indexed.bin");
        let scores_len = 1 + 4 + 3 * 4; // writeBoolean(true) + count + 3 floats
        let mut expected = golden[..golden.len() - scores_len].to_vec();
        expected.push(0); // scores = false
        let split = v1_data_split_builder()
            .with_row_ranges(vec![RowRange::new(1, 4), RowRange::new(11, 13)])
            .build()
            .unwrap();
        assert_eq!(split.serialize_split_v1().unwrap(), expected);
    }

    // The scores-stripped IndexedSplit frame the Rust serializer emits (see
    // `serialize_indexed_split_v1_matches_golden`): the Java golden's trailing vector scores are
    // replaced by a `false` scores flag, which is the exact byte form `deserialize_split_v1` supports.
    fn indexed_split_v1_no_scores() -> Vec<u8> {
        let golden = include_bytes!("goldens/split_v1_indexed.bin");
        let scores_len = 1 + 4 + 3 * 4; // writeBoolean(true) + count + 3 floats
        let mut bytes = golden[..golden.len() - scores_len].to_vec();
        bytes.push(0); // scores = false
        bytes
    }

    #[test]
    fn deserialize_split_v1_data_golden() {
        // `split_v1_data.bin` is the SPLIT_V1 (type 1) frame produced by `v1_data_split_builder`.
        let golden = include_bytes!("goldens/split_v1_data.bin");
        assert_eq!(
            DataSplit::deserialize_split_v1(golden).unwrap(),
            v1_data_split_builder().build().unwrap()
        );
        // Round-trip gate: bytes -> split -> bytes.
        assert_eq!(
            DataSplit::deserialize_split_v1(golden)
                .unwrap()
                .serialize_split_v1()
                .unwrap()
                .as_slice(),
            &golden[..]
        );
    }

    #[test]
    fn deserialize_split_v1_indexed_golden() {
        // IndexedSplit (type 3) without vector scores, carrying row_ranges.
        let frame = indexed_split_v1_no_scores();
        let split = DataSplit::deserialize_split_v1(&frame).unwrap();
        assert_eq!(
            split.row_ranges(),
            Some([RowRange::new(1, 4), RowRange::new(11, 13)].as_slice())
        );
        // Round-trips back to the same bytes.
        assert_eq!(split.serialize_split_v1().unwrap(), frame);
    }

    #[test]
    fn deserialize_split_v1_rejects_indexed_scores() {
        // The raw Java golden carries vector scores, which the Rust split cannot model.
        let golden = include_bytes!("goldens/split_v1_indexed.bin");
        assert!(matches!(
            DataSplit::deserialize_split_v1(golden),
            Err(crate::Error::Unsupported { .. })
        ));
    }

    #[test]
    fn deserialize_split_v1_rejects_unsupported_type() {
        // hand-build a frame with type id 2
        let mut b = Vec::new();
        b.extend_from_slice(&SPLIT_SER_MAGIC.to_be_bytes());
        b.extend_from_slice(&SPLIT_SER_VERSION.to_be_bytes());
        b.extend_from_slice(&2i32.to_be_bytes());
        assert!(matches!(
            DataSplit::deserialize_split_v1(&b),
            Err(crate::Error::Unsupported { .. })
        ));
    }

    #[test]
    fn deserialize_split_v1_rejects_trailing_bytes() {
        let golden = include_bytes!("goldens/split_v1_data.bin");
        let mut b = golden.to_vec();
        b.push(0xff); // one trailing byte after a complete frame
        assert!(matches!(
            DataSplit::deserialize_split_v1(&b),
            Err(crate::Error::DataInvalid { .. })
        ));
    }

    #[test]
    fn deserialize_split_v1_rejects_inverted_row_range() {
        // Take a valid IndexedSplit frame (first range [1,4]) and flip its bounds to from=5,to=2.
        // `RowRange::new` asserts from <= to, so the loop must reject this before constructing it.
        let mut frame = indexed_split_v1_no_scores();
        let needle: Vec<u8> = 1i64
            .to_be_bytes()
            .iter()
            .chain(4i64.to_be_bytes().iter())
            .copied()
            .collect();
        let pos = frame
            .windows(needle.len())
            .position(|w| w == needle.as_slice())
            .expect("range [1,4] present in indexed frame");
        let mut replacement = 5i64.to_be_bytes().to_vec();
        replacement.extend_from_slice(&2i64.to_be_bytes());
        frame[pos..pos + needle.len()].copy_from_slice(&replacement);
        assert!(matches!(
            DataSplit::deserialize_split_v1(&frame),
            Err(crate::Error::DataInvalid { .. })
        ));
    }
}
