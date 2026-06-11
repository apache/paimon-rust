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

    /// Decode a serialized min/max key. Returns `None` when the key is empty
    /// or malformed, letting callers degrade to the safe "treat everything as
    /// overlapping" path instead of failing the scan.
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
            .map(|(pos, dt)| row.get_datum(pos, dt).ok())
            .collect()
    }
}

/// Compare decoded keys field-by-field. NULL sorts first; fields that
/// `datum_cmp` cannot order (e.g. float NaN) compare as equal, which forces
/// the files into the same section — conservative but never incorrect.
fn compare_decoded(a: &DecodedKey, b: &DecodedKey) -> Ordering {
    for (fa, fb) in a.iter().zip(b.iter()) {
        let ord = match (fa, fb) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
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

/// Decode every file's key range up front. Returns `None` if any file lacks
/// a usable key range, in which case callers must assume full overlap.
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
            (Some(min), Some(max)) if !undecodable => keyed.push(KeyedFile {
                file: file.clone(),
                min,
                max,
            }),
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
/// Files with empty or undecodable key ranges collapse everything into one
/// section: no parallelism, but never a missed merge.
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
