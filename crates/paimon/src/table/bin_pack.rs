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

//! Bin packing utilities for splitting data files into reasonably-sized groups.
//!
//! Reference: [BinPacking](https://github.com/apache/paimon/blob/release-1.3/paimon-common/src/main/java/org/apache/paimon/utils/BinPacking.java)

use crate::spec::DataFileMeta;
use std::cmp;

/// Greedy order-preserving bin packing.
///
/// Groups items into bins where each bin's total weight does not exceed `target_weight`.
/// Items are never reordered across bins. A single item larger than `target_weight`
/// will become its own bin.
///
/// Reference: [BinPacking.packForOrdered](https://github.com/apache/paimon/blob/release-1.3/paimon-common/src/main/java/org/apache/paimon/utils/BinPacking.java)
pub fn pack_for_ordered<T>(
    items: Vec<T>,
    weight_func: impl Fn(&T) -> i64,
    target_weight: i64,
) -> Vec<Vec<T>> {
    let mut packed: Vec<Vec<T>> = Vec::new();
    let mut bin_items: Vec<T> = Vec::new();
    let mut bin_weight: i64 = 0;

    for item in items {
        let weight = weight_func(&item);
        if bin_weight + weight > target_weight && !bin_items.is_empty() {
            packed.push(bin_items);
            bin_items = Vec::new();
            bin_weight = 0;
        }
        bin_weight += weight;
        bin_items.push(item);
    }

    if !bin_items.is_empty() {
        packed.push(bin_items);
    }

    packed
}

/// Split data files for batch reading using bin packing.
///
/// Files are sorted by `min_sequence_number`, then packed into groups where each group's
/// total weight (using `max(file_size, open_file_cost)` as per-file weight) does not
/// exceed `target_split_size`.
///
/// Reference: [AppendOnlySplitGenerator.splitForBatch](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/AppendOnlySplitGenerator.java)
pub fn split_for_batch(
    mut files: Vec<DataFileMeta>,
    target_split_size: i64,
    open_file_cost: i64,
) -> Vec<Vec<DataFileMeta>> {
    files.sort_by_key(|f| f.min_sequence_number);
    let weight_func = move |f: &DataFileMeta| cmp::max(f.file_size, open_file_cost);
    pack_for_ordered(files, weight_func, target_split_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use chrono::{DateTime, Utc};

    fn make_file(file_name: &str, file_size: i64, min_seq: i64, max_seq: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: file_name.to_string(),
            file_size,
            row_count: 100,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            value_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            min_sequence_number: min_seq,
            max_sequence_number: max_seq,
            schema_id: 0,
            level: 0,
            extra_files: Vec::new(),
            creation_time: DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
            delete_row_count: None,
            embedded_index: None,
            first_row_id: None,
            write_cols: None,
            external_path: None,
        }
    }

    #[test]
    fn test_pack_for_ordered_empty() {
        let result: Vec<Vec<i32>> = pack_for_ordered(vec![], |_| 1, 10);
        assert!(result.is_empty());
    }

    #[test]
    fn test_pack_for_ordered_single_oversized() {
        let result = pack_for_ordered(vec![100], |x| *x, 10);
        assert_eq!(result, vec![vec![100]]);
    }

    #[test]
    fn test_pack_for_ordered_all_fit_in_one_bin() {
        let result = pack_for_ordered(vec![1, 2, 3], |x| *x, 100);
        assert_eq!(result, vec![vec![1, 2, 3]]);
    }

    /// Matches Java SplitGeneratorTest with targetSplitSize=40, openFileCost=2.
    #[test]
    fn test_split_for_batch_low_open_cost() {
        let files = vec![
            make_file("1", 11, 0, 20),
            make_file("2", 13, 21, 30),
            make_file("3", 46, 31, 40),
            make_file("4", 23, 41, 50),
            make_file("5", 4, 51, 60),
            make_file("6", 101, 61, 100),
        ];

        let groups = split_for_batch(files, 40, 2);
        let names: Vec<Vec<&str>> = groups
            .iter()
            .map(|g| g.iter().map(|f| f.file_name.as_str()).collect())
            .collect();
        assert_eq!(
            names,
            vec![vec!["1", "2"], vec!["3"], vec!["4", "5"], vec!["6"]]
        );
    }

    /// Matches Java SplitGeneratorTest with targetSplitSize=40, openFileCost=20.
    #[test]
    fn test_split_for_batch_high_open_cost() {
        let files = vec![
            make_file("1", 11, 0, 20),
            make_file("2", 13, 21, 30),
            make_file("3", 46, 31, 40),
            make_file("4", 23, 41, 50),
            make_file("5", 4, 51, 60),
            make_file("6", 101, 61, 100),
        ];

        let groups = split_for_batch(files, 40, 20);
        let names: Vec<Vec<&str>> = groups
            .iter()
            .map(|g| g.iter().map(|f| f.file_name.as_str()).collect())
            .collect();
        assert_eq!(
            names,
            vec![vec!["1", "2"], vec!["3"], vec!["4"], vec!["5"], vec!["6"]]
        );
    }

    /// Verify files are sorted by min_sequence_number before packing.
    #[test]
    fn test_split_for_batch_sorts_by_sequence() {
        let files = vec![
            make_file("c", 10, 30, 40),
            make_file("a", 10, 10, 20),
            make_file("b", 10, 20, 30),
        ];

        let groups = split_for_batch(files, 1000, 0);
        assert_eq!(groups.len(), 1);
        let names: Vec<&str> = groups[0].iter().map(|f| f.file_name.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_split_for_batch_empty() {
        let groups = split_for_batch(vec![], 128, 4);
        assert!(groups.is_empty());
    }
}
