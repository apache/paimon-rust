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

//! Split generation for primary-key (merge-tree) tables.
//!
//! Files whose primary-key ranges overlap must be read by the same
//! sort-merge reader, so they have to stay in the same split. This module
//! groups a bucket's files into key-range "sections" and then bin-packs
//! whole sections into splits, mirroring the Java implementation.
//!
//! References:
//! [MergeTreeSplitGenerator](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/MergeTreeSplitGenerator.java),
//! [IntervalPartition](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/mergetree/compact/IntervalPartition.java)

use super::bin_pack::pack_for_ordered;
use crate::spec::{datum_cmp, BinaryRow, DataFileMeta, DataType, Datum, TableSchema};
use std::cmp::{self, Ordering};

/// Compares serialized `BinaryRow` keys field-by-field using the trimmed
/// primary-key data types.
///
/// BinaryRow stores fields little-endian, so raw byte comparison would order
/// e.g. int 256 (`[00 01 00 00]`) before int 1 (`[01 00 00 00]`); keys must
/// be decoded before comparing.
pub(crate) struct KeyComparator {
    key_types: Vec<DataType>,
}

/// A decoded key: one `Option<Datum>` per trimmed primary-key field
/// (`None` = SQL NULL).
type DecodedKey = Vec<Option<Datum>>;

impl KeyComparator {
    pub(crate) fn new(key_types: Vec<DataType>) -> Self {
        Self { key_types }
    }

    /// Build a comparator over a table's trimmed primary keys, matching the
    /// key layout the kv writer uses for min/max keys. Returns `None` for
    /// tables without primary keys.
    pub(crate) fn from_table_schema(schema: &TableSchema) -> Option<Self> {
        let trimmed_pks = schema.trimmed_primary_keys();
        if trimmed_pks.is_empty() {
            return None;
        }
        let fields = schema.fields();
        let key_types: Vec<DataType> = trimmed_pks
            .iter()
            .filter_map(|name| {
                fields
                    .iter()
                    .find(|f| f.name() == name)
                    .map(|f| f.data_type().clone())
            })
            .collect();
        // A PK name missing from the fields (should not happen) leaves the
        // arity short; decode then fails and callers degrade safely.
        Some(Self::new(key_types))
    }

    /// Decode a serialized min/max key. Returns `None` when the key is empty,
    /// malformed, or contains NaN, letting callers degrade to the safe "treat
    /// everything as overlapping" path instead of failing the scan. NaN must
    /// not compare equal to finite values: for composite keys that would let
    /// later fields separate ranges which actually overlap.
    fn decode(&self, key: &[u8]) -> Option<DecodedKey> {
        if key.is_empty() {
            return None;
        }
        let row = BinaryRow::from_serialized_bytes(key).ok()?;
        if (row.arity() as usize) != self.key_types.len() {
            return None;
        }
        self.key_types
            .iter()
            .enumerate()
            .map(|(pos, dt)| {
                let datum = row.get_datum(pos, dt).ok()?;
                match datum {
                    Some(Datum::Float(value)) if value.is_nan() => None,
                    Some(Datum::Double(value)) if value.is_nan() => None,
                    _ => Some(datum),
                }
            })
            .collect()
    }
}

/// Compare decoded keys field-by-field. NULL sorts first; floating-point
/// values preserve the Java key ordering of -0 before +0. NaN keys are rejected
/// during decoding so unordered values never reach this comparator.
/// Binary keys use unsigned lexicographic order, matching the generated Java
/// key comparator and the on-disk row order.
fn compare_decoded(a: &DecodedKey, b: &DecodedKey) -> Ordering {
    for (fa, fb) in a.iter().zip(b.iter()) {
        let ord = match (fa, fb) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(Datum::Float(a)), Some(Datum::Float(b))) => a.total_cmp(b),
            (Some(Datum::Double(a)), Some(Datum::Double(b))) => a.total_cmp(b),
            (Some(da), Some(db)) => datum_cmp(da, db).unwrap_or(Ordering::Equal),
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// A file paired with its decoded min/max keys.
struct KeyedFile {
    file: DataFileMeta,
    min: DecodedKey,
    max: DecodedKey,
}

/// Decode every file's key range up front. Returns the original files as `Err`
/// if any range is missing, undecodable, or inverted, in which case callers
/// must assume full overlap.
fn decode_all(
    files: Vec<DataFileMeta>,
    comparator: &KeyComparator,
) -> Result<Vec<KeyedFile>, Vec<DataFileMeta>> {
    let mut keyed = Vec::with_capacity(files.len());
    let mut undecodable = false;
    for file in &files {
        match (
            comparator.decode(&file.min_key),
            comparator.decode(&file.max_key),
        ) {
            (Some(min), Some(max))
                if !undecodable && compare_decoded(&min, &max) != Ordering::Greater =>
            {
                keyed.push(KeyedFile {
                    file: file.clone(),
                    min,
                    max,
                })
            }
            _ => undecodable = true,
        }
    }
    if undecodable {
        Err(files)
    } else {
        Ok(keyed)
    }
}

/// Group files into sections by primary-key range overlap.
///
/// Files are sorted by `(min_key, max_key)`; a running upper bound tracks the
/// max key seen in the current section, and a file whose min key exceeds the
/// bound starts a new section. Sections never overlap each other, while files
/// inside one section all transitively overlap and must be merged together.
///
/// Files with empty, undecodable, or inverted key ranges collapse everything
/// into one section: no parallelism, but never a missed merge.
pub(crate) fn interval_partition(
    files: Vec<DataFileMeta>,
    comparator: &KeyComparator,
) -> Vec<Vec<DataFileMeta>> {
    if files.len() <= 1 {
        return if files.is_empty() {
            Vec::new()
        } else {
            vec![files]
        };
    }

    let mut keyed = match decode_all(files, comparator) {
        Ok(keyed) => keyed,
        Err(files) => return vec![files],
    };
    keyed.sort_by(|a, b| {
        compare_decoded(&a.min, &b.min).then_with(|| compare_decoded(&a.max, &b.max))
    });

    let mut sections: Vec<Vec<DataFileMeta>> = Vec::new();
    let mut current: Vec<DataFileMeta> = Vec::new();
    let mut bound: Option<DecodedKey> = None;

    for kf in keyed {
        if let Some(ref b) = bound {
            if compare_decoded(&kf.min, b) == Ordering::Greater {
                sections.push(std::mem::take(&mut current));
                bound = None;
            }
        }
        match bound {
            Some(ref b) if compare_decoded(&kf.max, b) != Ordering::Greater => {}
            _ => bound = Some(kf.max),
        }
        current.push(kf.file);
    }
    if !current.is_empty() {
        sections.push(current);
    }
    sections
}

/// Pack files into sorted runs. Files within a run have strictly disjoint key
/// ranges and can therefore be read by concatenation. The number of runs equals
/// the maximum key-range overlap depth, even when the input spans multiple
/// non-overlapping sections. Undecodable or inconsistent manifest keys safely
/// degrade to one run per file, preserving the previous merge fan-in and
/// correctness.
pub(crate) fn pack_sorted_runs(
    files: Vec<DataFileMeta>,
    comparator: &KeyComparator,
) -> Vec<Vec<DataFileMeta>> {
    pack_sorted_runs_by(files, comparator, |file| file)
}

/// Pack arbitrary payloads into key-sorted runs using `file_meta` to select the
/// [`DataFileMeta`] that defines each payload's key range.
///
/// Files are appended to a run only when the previous maximum key is strictly
/// less than the next minimum key, so concatenating that run remains monotonic.
/// Missing, undecodable, or inverted ranges degrade to one item per run. A final
/// independent range check verifies the concatenation precondition and applies
/// the same fallback if the constructed runs are not sound.
pub(crate) fn pack_sorted_runs_by<T, F>(
    items: Vec<T>,
    comparator: &KeyComparator,
    file_meta: F,
) -> Vec<Vec<T>>
where
    F: Fn(&T) -> &DataFileMeta + Copy,
{
    if items.len() <= 1 {
        return if items.is_empty() {
            Vec::new()
        } else {
            vec![items]
        };
    }

    let mut decoded_ranges = Vec::with_capacity(items.len());
    for item in &items {
        let file = file_meta(item);
        match (
            comparator.decode(&file.min_key),
            comparator.decode(&file.max_key),
        ) {
            (Some(min), Some(max)) if compare_decoded(&min, &max) != Ordering::Greater => {
                decoded_ranges.push((min, max));
            }
            _ => return items.into_iter().map(|item| vec![item]).collect(),
        }
    }

    let mut keyed = items
        .into_iter()
        .zip(decoded_ranges)
        .map(|(item, (min, max))| (item, min, max))
        .collect::<Vec<_>>();
    keyed.sort_by(|a, b| compare_decoded(&a.1, &b.1).then_with(|| compare_decoded(&a.2, &b.2)));

    let mut runs: Vec<Vec<T>> = Vec::new();
    let mut run_ends: Vec<DecodedKey> = Vec::new();
    for (item, min, max) in keyed {
        let mut best_run = None;
        for (index, end) in run_ends.iter().enumerate() {
            if compare_decoded(end, &min) != Ordering::Less {
                continue;
            }
            match best_run {
                Some(best) if compare_decoded(&run_ends[best], end) != Ordering::Less => {}
                _ => best_run = Some(index),
            }
        }

        match best_run {
            Some(index) => {
                runs[index].push(item);
                run_ends[index] = max;
            }
            None => {
                runs.push(vec![item]);
                run_ends.push(max);
            }
        }
    }

    let sound = runs.iter().all(|run| {
        run.windows(2).all(|pair| {
            let previous = file_meta(&pair[0]);
            let next = file_meta(&pair[1]);
            match (
                comparator.decode(&previous.max_key),
                comparator.decode(&next.min_key),
            ) {
                (Some(previous_max), Some(next_min)) => {
                    compare_decoded(&previous_max, &next_min) == Ordering::Less
                }
                _ => false,
            }
        })
    });
    if sound {
        runs
    } else {
        runs.into_iter().flatten().map(|item| vec![item]).collect()
    }
}

/// Bin-pack whole sections into splits. A section is atomic: its files
/// overlap on primary key and must never be separated, even when the section
/// alone exceeds `target_split_size`.
///
/// Mirrors Java `MergeTreeSplitGenerator#packSplits`: a section's weight is
/// `max(total file size, open_file_cost)` — the open-file cost is charged
/// once per section, not per file.
pub(crate) fn pack_sections(
    sections: Vec<Vec<DataFileMeta>>,
    target_split_size: i64,
    open_file_cost: i64,
) -> Vec<Vec<DataFileMeta>> {
    pack_for_ordered(
        sections,
        |section| {
            cmp::max(
                section.iter().map(|f| f.file_size).sum::<i64>(),
                open_file_cost,
            )
        },
        target_split_size,
    )
    .into_iter()
    .map(|sections| sections.into_iter().flatten().collect())
    .collect()
}

/// A group of files forming one split, plus whether the split can be read
/// raw — without the sort-merge reader — so its physical row count equals
/// its logical row count.
///
/// Mirrors Java `SplitGenerator.SplitGroup`.
#[derive(Debug)]
pub(crate) struct SplitGroup {
    pub(crate) files: Vec<DataFileMeta>,
    pub(crate) raw_convertible: bool,
}

/// Whether a file is known to contain no DELETE rows.
///
/// Mirrors Java `MergeTreeSplitGenerator#withoutDeleteRow`: a missing
/// `delete_row_count` is treated as "no deletes" for compatibility with files
/// written by old versions.
fn without_delete_row(file: &DataFileMeta) -> bool {
    file.delete_row_count.is_none_or(|count| count == 0)
}

/// Generate batch splits for a merge-tree (primary-key) bucket.
///
/// Mirrors Java `MergeTreeSplitGenerator#splitForBatch` for the merging read
/// path (deletion-vector and first-row tables are routed to plain size-based
/// packing before reaching this function, matching Java's
/// `alwaysRawConvertible` fast path):
///
/// * If every file is compacted (level != 0), has no delete rows, and all
///   files sit on a single level, no two files can overlap on key range, so
///   the files are bin-packed individually and every group is raw
///   convertible.
/// * Otherwise files are sectioned by key-range overlap and whole sections
///   are bin-packed; a group is raw convertible only when it holds exactly
///   one file without delete rows.
///
/// `file_keys_unique` is a deliberate deviation from Java: raw convertibility
/// additionally assumes a file never holds two rows of one key. Java's
/// `MergeTreeWriter#flushWriteBuffer` runs the merge function before flushing,
/// so that holds for every engine; the Rust writer only deduplicates at flush
/// for deduplicate/first-row, while partial-update keeps all rows for
/// read-side field-wise merge (`kv_file_writer.rs`, `select_flush_indices`).
/// Callers pass `false` for engines without that write-time guarantee, forcing
/// every group non-raw-convertible. Can be relaxed once the writer merges on
/// flush like Java.
pub(crate) fn merge_tree_split_for_batch(
    files: Vec<DataFileMeta>,
    comparator: &KeyComparator,
    target_split_size: i64,
    open_file_cost: i64,
    file_keys_unique: bool,
) -> Vec<SplitGroup> {
    let raw_convertible = files.iter().all(|f| f.level != 0 && without_delete_row(f));
    let one_level = {
        let mut levels: Vec<i32> = files.iter().map(|f| f.level).collect();
        levels.sort_unstable();
        levels.dedup();
        levels.len() == 1
    };

    if raw_convertible && one_level {
        return pack_for_ordered(
            files,
            |f| cmp::max(f.file_size, open_file_cost),
            target_split_size,
        )
        .into_iter()
        .map(|files| SplitGroup {
            files,
            raw_convertible: file_keys_unique,
        })
        .collect();
    }

    pack_sections(
        interval_partition(files, comparator),
        target_split_size,
        open_file_cost,
    )
    .into_iter()
    .map(|files| {
        let raw_convertible = file_keys_unique && files.len() == 1 && without_delete_row(&files[0]);
        SplitGroup {
            files,
            raw_convertible,
        }
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{BinaryRowBuilder, IntType};
    use chrono::{DateTime, Utc};

    fn int_key(value: i32) -> Vec<u8> {
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_int(0, value);
        builder.build_serialized()
    }

    fn keyed_file(name: &str, min: i32, max: i32, file_size: i64, level: i32) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size,
            row_count: 100,
            min_key: int_key(min),
            max_key: int_key(max),
            key_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            value_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level,
            extra_files: Vec::new(),
            creation_time: DateTime::<Utc>::from_timestamp(0, 0),
            delete_row_count: None,
            embedded_index: None,
            first_row_id: None,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
            column_max_sequence_numbers: None,
        }
    }

    fn int_comparator() -> KeyComparator {
        KeyComparator::new(vec![DataType::Int(IntType::new())])
    }

    fn section_names(sections: &[Vec<DataFileMeta>]) -> Vec<Vec<&str>> {
        sections
            .iter()
            .map(|s| s.iter().map(|f| f.file_name.as_str()).collect())
            .collect()
    }

    /// Int keys must be ordered numerically, not by little-endian bytes
    /// (byte-wise, 256 = [00 01 00 00] would sort before 1 = [01 00 00 00]).
    #[test]
    fn key_comparator_orders_ints_numerically() {
        let comparator = int_comparator();
        let one = comparator.decode(&int_key(1)).unwrap();
        let two = comparator.decode(&int_key(2)).unwrap();
        let big = comparator.decode(&int_key(256)).unwrap();
        assert_eq!(compare_decoded(&one, &two), Ordering::Less);
        assert_eq!(compare_decoded(&two, &big), Ordering::Less);
        assert_eq!(compare_decoded(&big, &one), Ordering::Greater);
    }

    #[test]
    fn pack_sorted_runs_collapses_shallow_overlap() {
        let files = vec![
            keyed_file("a", 1, 10, 100, 0),
            keyed_file("b", 5, 15, 100, 0),
            keyed_file("c", 20, 30, 100, 0),
            keyed_file("d", 25, 35, 100, 0),
            keyed_file("e", 40, 50, 100, 0),
            keyed_file("f", 45, 55, 100, 0),
        ];
        let runs = pack_sorted_runs(files, &int_comparator());
        assert_eq!(runs.len(), 2);
        assert_eq!(runs.iter().map(Vec::len).sum::<usize>(), 6);
    }

    #[test]
    fn pack_sorted_runs_chains_disjoint_files_in_key_order() {
        let files = vec![
            keyed_file("c", 21, 30, 100, 0),
            keyed_file("a", 1, 10, 100, 0),
            keyed_file("b", 11, 20, 100, 0),
        ];
        let runs = pack_sorted_runs(files, &int_comparator());
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0]
                .iter()
                .map(|file| file.file_name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn pack_sorted_runs_treats_touching_ranges_as_overlapping() {
        let files = vec![
            keyed_file("a", 1, 10, 100, 0),
            keyed_file("b", 10, 20, 100, 0),
        ];
        assert_eq!(pack_sorted_runs(files, &int_comparator()).len(), 2);
    }

    #[test]
    fn pack_sorted_runs_degrades_when_keys_are_undecodable() {
        let mut undecodable = keyed_file("a", 1, 10, 100, 0);
        undecodable.min_key.clear();
        undecodable.max_key.clear();
        let files = vec![undecodable, keyed_file("b", 5, 15, 100, 0)];
        assert_eq!(pack_sorted_runs(files, &int_comparator()).len(), 2);
    }

    #[test]
    fn pack_sorted_runs_degrades_when_a_file_range_is_inverted() {
        let files = vec![
            keyed_file("invalid", 10, 5, 100, 0),
            keyed_file("valid", 6, 9, 100, 0),
        ];
        assert_eq!(pack_sorted_runs(files, &int_comparator()).len(), 2);
    }

    #[test]
    fn pack_sorted_runs_orders_binary_keys_unsigned() {
        fn bytes_key(value: u8) -> Vec<u8> {
            let mut builder = BinaryRowBuilder::new(1);
            builder.write_binary(0, &[value]);
            builder.build_serialized()
        }

        fn binary_file(name: &str, min: u8, max: u8) -> DataFileMeta {
            let mut file = keyed_file(name, 0, 0, 100, 0);
            file.min_key = bytes_key(min);
            file.max_key = bytes_key(max);
            file
        }

        let comparator = KeyComparator::new(vec![DataType::VarBinary(
            crate::spec::VarBinaryType::new(16).unwrap(),
        )]);
        let runs = pack_sorted_runs(
            vec![
                binary_file("high", 0x80, 0xFE),
                binary_file("low", 0x01, 0x7F),
            ],
            &comparator,
        );
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0]
                .iter()
                .map(|file| file.file_name.as_str())
                .collect::<Vec<_>>(),
            vec!["low", "high"]
        );
    }

    #[test]
    fn pack_sorted_runs_handles_multi_column_keys() {
        fn key(first: i32, second: &str) -> Vec<u8> {
            let mut builder = BinaryRowBuilder::new(2);
            builder.write_int(0, first);
            builder.write_string(1, second);
            builder.build_serialized()
        }

        fn file(name: &str, min: (i32, &str), max: (i32, &str)) -> DataFileMeta {
            let mut file = keyed_file(name, 0, 0, 100, 0);
            file.min_key = key(min.0, min.1);
            file.max_key = key(max.0, max.1);
            file
        }

        let comparator = KeyComparator::new(vec![
            DataType::Int(IntType::new()),
            DataType::VarChar(crate::spec::VarCharType::new(16).unwrap()),
        ]);

        let overlapping = pack_sorted_runs(
            vec![file("a", (1, "a"), (2, "a")), file("b", (1, "b"), (2, "b"))],
            &comparator,
        );
        assert_eq!(
            overlapping.len(),
            2,
            "second-key overlap must keep files in separate runs"
        );

        let disjoint = pack_sorted_runs(
            vec![file("d", (3, "a"), (4, "a")), file("c", (1, "a"), (2, "a"))],
            &comparator,
        );
        assert_eq!(disjoint.len(), 1);
        assert_eq!(
            disjoint[0]
                .iter()
                .map(|file| file.file_name.as_str())
                .collect::<Vec<_>>(),
            vec!["c", "d"]
        );
    }

    #[test]
    fn nan_composite_keys_keep_overlapping_files_in_one_split() {
        // Java orders NaN after finite values, so the broad range contains
        // (2, 100). Treating its first field as equal instead compares 100 to
        // 10 and incorrectly separates two versions of that key.
        for first_type in [
            DataType::Float(crate::spec::FloatType::new()),
            DataType::Double(crate::spec::DoubleType::new()),
        ] {
            let key = |first: f64, second: i32| {
                let mut builder = BinaryRowBuilder::new(2);
                match first_type {
                    DataType::Float(_) => builder.write_float(0, first as f32),
                    DataType::Double(_) => builder.write_double(0, first),
                    _ => unreachable!(),
                }
                builder.write_int(1, second);
                builder.build_serialized()
            };
            let mut broad = keyed_file("broad", 0, 0, 100, 0);
            broad.min_key = key(1.0, 0);
            broad.max_key = key(f64::NAN, 10);
            let mut point = keyed_file("point", 0, 0, 100, 0);
            point.min_key = key(2.0, 100);
            point.max_key = point.min_key.clone();
            let comparator = KeyComparator::new(vec![first_type, DataType::Int(IntType::new())]);
            let files = vec![broad, point];

            let splits = merge_tree_split_for_batch(files.clone(), &comparator, 1, 1, true);
            assert_eq!(splits.len(), 1, "overlapping versions must share a split");
            assert_eq!(splits[0].files.len(), 2);
            assert!(!splits[0].raw_convertible);

            let runs = pack_sorted_runs(files, &comparator);
            assert_eq!(runs.len(), 2, "overlapping files cannot be concatenated");
        }
    }

    #[test]
    fn signed_zero_composite_keys_keep_overlapping_files_in_one_split() {
        for first_type in [
            DataType::Float(crate::spec::FloatType::new()),
            DataType::Double(crate::spec::DoubleType::new()),
        ] {
            let key = |first: f64, second: i32| {
                let mut builder = BinaryRowBuilder::new(2);
                match first_type {
                    DataType::Float(_) => builder.write_float(0, first as f32),
                    DataType::Double(_) => builder.write_double(0, first),
                    _ => unreachable!(),
                }
                builder.write_int(1, second);
                builder.build_serialized()
            };
            // -0 sorts before +0, so (-0, 100) is below (+0, 10)
            // regardless of the second field. Numeric equality of the first
            // field would incorrectly put these files in separate splits.
            let mut broad = keyed_file("broad", 0, 0, 100, 0);
            broad.min_key = key(-0.0, 0);
            broad.max_key = key(0.0, 10);
            let mut point = keyed_file("point", 0, 0, 100, 0);
            point.min_key = key(-0.0, 100);
            point.max_key = point.min_key.clone();
            let comparator = KeyComparator::new(vec![first_type, DataType::Int(IntType::new())]);
            let files = vec![broad, point];

            let splits = merge_tree_split_for_batch(files.clone(), &comparator, 1, 1, true);
            assert_eq!(splits.len(), 1, "overlapping versions must share a split");
            assert_eq!(splits[0].files.len(), 2);
            assert!(!splits[0].raw_convertible);
            assert_eq!(pack_sorted_runs(files, &comparator).len(), 2);
        }
    }

    #[test]
    fn interval_partition_groups_overlapping_files() {
        let files = vec![
            keyed_file("a", 1, 10, 100, 0),
            keyed_file("b", 5, 15, 100, 0),
            keyed_file("c", 20, 30, 100, 0),
            keyed_file("d", 25, 28, 100, 0),
        ];
        let sections = interval_partition(files, &int_comparator());
        assert_eq!(
            section_names(&sections),
            vec![vec!["a", "b"], vec!["c", "d"]]
        );
    }

    #[test]
    fn interval_partition_keeps_disjoint_files_separate() {
        let files = vec![
            keyed_file("b", 3, 4, 100, 0),
            keyed_file("a", 1, 2, 100, 0),
            keyed_file("c", 5, 6, 100, 0),
        ];
        let sections = interval_partition(files, &int_comparator());
        assert_eq!(
            section_names(&sections),
            vec![vec!["a"], vec!["b"], vec!["c"]]
        );
    }

    /// A later file can extend the section bound past an earlier file's max:
    /// [1,100] chains [50,60] and [90,110] into one section with [105,120].
    #[test]
    fn interval_partition_tracks_running_bound() {
        let files = vec![
            keyed_file("a", 1, 100, 100, 0),
            keyed_file("b", 50, 60, 100, 0),
            keyed_file("c", 90, 110, 100, 0),
            keyed_file("d", 105, 120, 100, 0),
            keyed_file("e", 121, 130, 100, 0),
        ];
        let sections = interval_partition(files, &int_comparator());
        assert_eq!(
            section_names(&sections),
            vec![vec!["a", "b", "c", "d"], vec!["e"]]
        );
    }

    #[test]
    fn interval_partition_empty_key_degrades_to_single_section() {
        let mut no_key = keyed_file("a", 1, 2, 100, 0);
        no_key.min_key = Vec::new();
        no_key.max_key = Vec::new();
        let files = vec![no_key, keyed_file("b", 10, 20, 100, 0)];
        let sections = interval_partition(files, &int_comparator());
        assert_eq!(section_names(&sections), vec![vec!["a", "b"]]);
    }

    #[test]
    fn interval_partition_inverted_range_degrades_to_single_section() {
        let files = vec![
            keyed_file("invalid", 10, 5, 100, 0),
            keyed_file("valid", 6, 9, 100, 0),
        ];
        let sections = interval_partition(files, &int_comparator());
        assert_eq!(section_names(&sections), vec![vec!["invalid", "valid"]]);
    }

    #[test]
    fn pack_sections_respects_target_size() {
        let sections = vec![
            vec![keyed_file("a", 1, 2, 100, 0)],
            vec![keyed_file("b", 3, 4, 100, 0)],
            vec![keyed_file("c", 5, 6, 100, 0)],
        ];
        let splits = pack_sections(sections, 250, 1);
        assert_eq!(section_names(&splits), vec![vec!["a", "b"], vec!["c"]]);
    }

    #[test]
    fn pack_sections_never_splits_a_section() {
        let sections = vec![vec![
            keyed_file("a", 1, 10, 100, 0),
            keyed_file("b", 5, 15, 100, 0),
        ]];
        let splits = pack_sections(sections, 50, 1);
        assert_eq!(section_names(&splits), vec![vec!["a", "b"]]);
    }

    #[test]
    fn pack_sections_applies_open_file_cost() {
        let sections = vec![
            vec![keyed_file("a", 1, 1, 2, 0)],
            vec![keyed_file("b", 2, 2, 2, 0)],
            vec![keyed_file("c", 3, 3, 2, 0)],
        ];
        // Weight per section is max(total file size=2, open_file_cost=100) = 100.
        let splits = pack_sections(sections, 150, 100);
        assert_eq!(
            section_names(&splits),
            vec![vec!["a"], vec!["b"], vec!["c"]]
        );
    }

    /// The open-file cost is charged once per section, not per file (Java
    /// `packSplits`): two 3-file sections weigh max(6, 100) = 100 each and
    /// share one split under a 250 target, where a per-file charge (3 × 100)
    /// would split them apart.
    #[test]
    fn pack_sections_charges_open_file_cost_per_section() {
        let sections = vec![
            vec![
                keyed_file("a1", 1, 2, 2, 0),
                keyed_file("a2", 1, 2, 2, 0),
                keyed_file("a3", 1, 2, 2, 0),
            ],
            vec![
                keyed_file("b1", 3, 4, 2, 0),
                keyed_file("b2", 3, 4, 2, 0),
                keyed_file("b3", 3, 4, 2, 0),
            ],
        ];
        let splits = pack_sections(sections, 250, 100);
        assert_eq!(
            section_names(&splits),
            vec![vec!["a1", "a2", "a3", "b1", "b2", "b3"]]
        );
    }

    fn group_names(groups: &[SplitGroup]) -> Vec<Vec<&str>> {
        groups
            .iter()
            .map(|g| g.files.iter().map(|f| f.file_name.as_str()).collect())
            .collect()
    }

    /// All files compacted on one level: the fast path bin-packs files
    /// individually and every group is raw convertible, even multi-file ones
    /// (same-level files never overlap).
    #[test]
    fn split_for_batch_one_level_fast_path_is_raw_convertible() {
        let comparator = int_comparator();
        let files = vec![
            keyed_file("a", 1, 10, 100, 5),
            keyed_file("b", 11, 20, 100, 5),
            keyed_file("c", 21, 30, 100, 5),
        ];
        let groups = merge_tree_split_for_batch(files, &comparator, 250, 1, true);
        assert_eq!(group_names(&groups), vec![vec!["a", "b"], vec!["c"]]);
        assert!(groups.iter().all(|g| g.raw_convertible));
    }

    /// A delete-row file disables the fast path; after sectioning, only
    /// single-file groups without delete rows stay raw convertible.
    #[test]
    fn split_for_batch_delete_rows_disable_raw_conversion() {
        let comparator = int_comparator();
        let mut with_deletes = keyed_file("del", 1, 10, 100, 5);
        with_deletes.delete_row_count = Some(3);
        let files = vec![with_deletes, keyed_file("clean", 11, 20, 100, 5)];
        // Large target size packs both disjoint sections into one split.
        let groups = merge_tree_split_for_batch(files, &comparator, 1000, 1, true);
        assert_eq!(group_names(&groups), vec![vec!["del", "clean"]]);
        assert!(!groups[0].raw_convertible, "multi-file group is never raw");

        // Tiny target size keeps each section alone; the delete-row file is
        // still not raw convertible, the clean one is.
        let mut with_deletes = keyed_file("del", 1, 10, 100, 5);
        with_deletes.delete_row_count = Some(3);
        let files = vec![with_deletes, keyed_file("clean", 11, 20, 100, 5)];
        let groups = merge_tree_split_for_batch(files, &comparator, 1, 1, true);
        assert_eq!(group_names(&groups), vec![vec!["del"], vec!["clean"]]);
        assert!(!groups[0].raw_convertible);
        assert!(groups[1].raw_convertible);
    }

    /// Engines whose writer does not deduplicate at flush (partial-update
    /// keeps all rows of a key in one file) cannot prove file-internal key
    /// uniqueness: every group must stay non-raw-convertible on both the fast
    /// path and the sectioned path, so physical row counts are never reported
    /// as merged row counts.
    #[test]
    fn split_for_batch_without_unique_keys_never_raw_convertible() {
        let comparator = int_comparator();

        // Fast-path shape: all compacted, one level, no delete rows.
        let files = vec![
            keyed_file("a", 1, 10, 100, 5),
            keyed_file("b", 11, 20, 100, 5),
        ];
        let groups = merge_tree_split_for_batch(files, &comparator, 250, 1, false);
        assert_eq!(group_names(&groups), vec![vec!["a", "b"]]);
        assert!(groups.iter().all(|g| !g.raw_convertible));

        // Sectioned shape: a disjoint single compacted file would be raw for
        // deduplicate, but not without the write-time uniqueness guarantee.
        let files = vec![
            keyed_file("l0", 1, 50, 100, 0),
            keyed_file("solo", 100, 120, 100, 2),
        ];
        let groups = merge_tree_split_for_batch(files, &comparator, 1, 1, false);
        assert_eq!(group_names(&groups), vec![vec!["l0"], vec!["solo"]]);
        assert!(groups.iter().all(|g| !g.raw_convertible));
    }

    /// Level-0 or cross-level files take the sectioning path; overlapping
    /// files share a non-raw-convertible group while a disjoint single file
    /// stays raw convertible. Missing delete_row_count counts as "no deletes"
    /// (old-version files).
    #[test]
    fn split_for_batch_sections_overlapping_files() {
        let comparator = int_comparator();
        let files = vec![
            keyed_file("l0", 1, 50, 100, 0),
            keyed_file("l1", 40, 90, 100, 1),
            keyed_file("solo", 100, 120, 100, 2),
        ];
        let groups = merge_tree_split_for_batch(files, &comparator, 1, 1, true);
        assert_eq!(group_names(&groups), vec![vec!["l0", "l1"], vec!["solo"]]);
        assert!(
            !groups[0].raw_convertible,
            "overlapping versions must merge"
        );
        assert!(groups[1].raw_convertible, "disjoint single compacted file");
    }
}
