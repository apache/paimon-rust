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
#[derive(Debug, Clone)]
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

    /// Full path for a single data file in this split (bucket_path + file_name).
    pub fn data_file_path(&self, file: &DataFileMeta) -> String {
        let base = self.bucket_path.trim_end_matches('/');
        format!("{}/{}", base, file.file_name)
    }

    /// Total row count of all data files in this split.
    pub fn row_count(&self) -> i64 {
        self.data_files.iter().map(|f| f.row_count).sum()
    }

    /// Returns the merged row count if it can be computed.
    ///
    /// Two paths:
    /// 1. If all files have `first_row_id` (data evolution mode): merge overlapping
    ///    row ID ranges and take max row count per group.
    /// 2. Otherwise: row_count() minus deleted rows (if deletion file cardinality is known).
    ///
    /// Returns `None` if deletion files exist but cardinality is unknown.
    ///
    /// Reference: [DataSplit.mergedRowCount()](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/source/DataSplit.java#L133)
    pub fn merged_row_count(&self) -> Option<i64> {
        // Try data evolution merged row count first (all files have first_row_id)
        if let Some(count) = self.data_evolution_merged_row_count() {
            return Some(count);
        }

        // Fallback: row_count - deleted rows
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
        let sum: i64 = groups
            .iter()
            .map(|group| group.iter().map(|f| f.row_count).max().unwrap_or(0))
            .sum();
        Some(sum)
    }

    pub fn builder() -> DataSplitBuilder {
        DataSplitBuilder::new()
    }
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
