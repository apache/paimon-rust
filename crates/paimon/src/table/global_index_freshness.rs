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

//! Verify that active global indexes still describe the indexed column values.

use std::collections::{HashMap, HashSet};

use super::global_index_types::normalize_queryable_global_index_type;
use super::key_normalization::canonical_partition_key;
use super::{DataSplit, Table};
use crate::spec::{
    BinaryRow, DataEvolutionIndexSourceMeta, DataField, DataFileMeta,
    GlobalIndexColumnUpdateAction, IndexManifest, Snapshot,
};

/// Under IGNORE, prove that indexed columns have not changed since the index's
/// source snapshot. Other current update policies bypass this check.
/// `partitions` contains canonical logical partition keys.
pub(super) async fn indexed_columns_unchanged(
    table: &Table,
    snapshot: &Snapshot,
    columns: &[String],
    partitions: &HashSet<Vec<u8>>,
) -> crate::Result<bool> {
    if table
        .schema()
        .core_options()
        .global_index_column_update_action()?
        != GlobalIndexColumnUpdateAction::Ignore
    {
        return Ok(true);
    }
    let Some(manifest) = snapshot.index_manifest() else {
        return Ok(true);
    };
    let path = table.snapshot_manager().manifest_path(manifest);
    let entries = IndexManifest::read(table.file_io(), &path).await?;
    let fields = table.schema().fields();
    let partition_fields = table.schema().partition_fields();
    let mut indexes = Vec::new();
    for entry in &entries {
        if entry.kind != crate::spec::FileKind::Add
            || normalize_queryable_global_index_type(&entry.index_file.index_type).is_none()
        {
            continue;
        }
        let Some(meta) = &entry.index_file.global_index_meta else {
            continue;
        };
        let index_partition = if partition_fields.is_empty() {
            Vec::new()
        } else {
            canonical_partition_key(
                &BinaryRow::from_serialized_bytes(&entry.partition)?,
                &partition_fields,
            )?
        };
        if !partition_fields.is_empty() && !partitions.contains(&index_partition) {
            continue;
        }
        if meta.row_range_start < 0 || meta.row_range_end < meta.row_range_start {
            return Ok(false);
        }
        let keys = fields
            .iter()
            .filter(|field| {
                columns.iter().any(|column| column == field.name())
                    && (field.id() == meta.index_field_id
                        || meta
                            .extra_field_ids
                            .as_ref()
                            .is_some_and(|ids| ids.contains(&field.id())))
            })
            .collect::<Vec<_>>();
        if keys.is_empty() {
            continue;
        }
        let Some(source_id) = meta
            .source_meta
            .as_deref()
            .and_then(|bytes| DataEvolutionIndexSourceMeta::deserialize(bytes).ok())
            .map(|meta| meta.scan_snapshot_id())
            .filter(|id| *id <= snapshot.id())
        else {
            return Ok(false);
        };
        indexes.push((entry, index_partition, keys, source_id));
    }
    if indexes.is_empty() {
        return Ok(true);
    }
    // Inspect every active column file before key predicates or indexes can
    // prune away evidence of historical updates. This reads metadata only.
    let unindexed = table
        .copy_with_pinned_snapshot(Some(snapshot))
        .copy_with_options(HashMap::from([(
            "global-index.enabled".into(),
            "false".into(),
        )]));
    let plan = unindexed.new_read_builder().new_scan().plan_all().await?;
    let index_groups = indexes
        .iter()
        .map(|(entry, partition, _, _)| (partition.clone(), entry.bucket))
        .collect();
    let files_by_group = file_range_lookups(plan.splits(), &partition_fields, &index_groups)?;
    for (entry, index_partition, keys, source_id) in indexes {
        let meta = entry.index_file.global_index_meta.as_ref().unwrap();
        let Some(files) = files_by_group.get(&(index_partition, entry.bucket)) else {
            continue;
        };
        if !files.all_overlapping_unchanged(
            meta.row_range_start,
            meta.row_range_end,
            table.schema(),
            &keys,
            source_id,
        ) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn file_range_lookups<'a>(
    splits: &'a [DataSplit],
    partition_fields: &[DataField],
    index_groups: &HashSet<(Vec<u8>, i32)>,
) -> crate::Result<HashMap<(Vec<u8>, i32), FileRangeLookup<'a>>> {
    let buckets = index_groups
        .iter()
        .map(|(_, bucket)| *bucket)
        .collect::<HashSet<_>>();
    let mut groups: HashMap<_, Vec<_>> = HashMap::new();
    for split in splits {
        if !buckets.contains(&split.bucket()) {
            continue;
        }
        let partition = if partition_fields.is_empty() {
            Vec::new()
        } else {
            canonical_partition_key(split.partition(), partition_fields)?
        };
        if !index_groups.contains(&(partition.clone(), split.bucket())) {
            continue;
        }
        groups
            .entry((partition, split.bucket()))
            .or_default()
            .extend(split.data_files());
    }
    Ok(groups
        .into_iter()
        .map(|(group, files)| (group, FileRangeLookup::new(files)))
        .collect())
}

struct FileRangeLookup<'a> {
    ranges: Option<Box<FileRangeTree<'a>>>,
    unknown_ranges: Vec<&'a DataFileMeta>,
}

impl<'a> FileRangeLookup<'a> {
    fn new(files: Vec<&'a DataFileMeta>) -> Self {
        let mut ranges = Vec::new();
        let mut unknown_ranges = Vec::new();
        for file in files {
            if let Some((start, end)) = file.row_id_range() {
                ranges.push(FileRange { start, end, file });
            } else {
                unknown_ranges.push(file);
            }
        }
        ranges.sort_unstable_by_key(|range| (range.start, range.end));
        Self {
            ranges: FileRangeTree::from_sorted(ranges),
            unknown_ranges,
        }
    }

    fn all_overlapping_unchanged(
        &self,
        start: i64,
        end: i64,
        schema: &crate::spec::TableSchema,
        keys: &[&DataField],
        source_id: i64,
    ) -> bool {
        // A missing range cannot prove disjointness. Retain the existing
        // column-level check, which can still accept a payload-only file.
        self.unknown_ranges.iter().all(|file| {
            file.schema_id == schema.id()
                && file_columns_unchanged(file, schema, keys, source_id, false)
        }) && self.ranges.as_ref().is_none_or(|ranges| {
            ranges.all_overlapping(start, end, &mut |file| {
                file.schema_id == schema.id()
                    && file_columns_unchanged(file, schema, keys, source_id, true)
            })
        })
    }
}

struct FileRange<'a> {
    start: i64,
    end: i64,
    file: &'a DataFileMeta,
}

/// A centered interval tree reports overlaps in O(log(files) + overlaps).
/// A sorted prefix alone would rescan every preceding file when even one
/// long-lived column file spans many disjoint index shards.
struct FileRangeTree<'a> {
    center: i64,
    by_start: Vec<FileRange<'a>>,
    by_end: Vec<usize>,
    left: Option<Box<Self>>,
    right: Option<Box<Self>>,
    #[cfg(test)]
    inspected: std::cell::Cell<usize>,
}

impl<'a> FileRangeTree<'a> {
    fn from_sorted(ranges: Vec<FileRange<'a>>) -> Option<Box<Self>> {
        if ranges.is_empty() {
            return None;
        }
        let center = ranges[ranges.len() / 2].start;
        let (mut left, mut right, mut by_start) = (Vec::new(), Vec::new(), Vec::new());
        for range in ranges {
            if range.end < center {
                left.push(range);
            } else if range.start > center {
                right.push(range);
            } else {
                by_start.push(range);
            }
        }
        let mut by_end = (0..by_start.len()).collect::<Vec<_>>();
        by_end.sort_unstable_by_key(|index| std::cmp::Reverse(by_start[*index].end));
        Some(Box::new(Self {
            center,
            by_start,
            by_end,
            left: Self::from_sorted(left),
            right: Self::from_sorted(right),
            #[cfg(test)]
            inspected: std::cell::Cell::new(0),
        }))
    }

    fn all_overlapping(
        &self,
        start: i64,
        end: i64,
        check: &mut impl FnMut(&DataFileMeta) -> bool,
    ) -> bool {
        if end < self.center {
            for range in &self.by_start {
                #[cfg(test)]
                self.inspected.set(self.inspected.get() + 1);
                if range.start > end {
                    break;
                }
                if !check(range.file) {
                    return false;
                }
            }
        } else if start > self.center {
            for index in &self.by_end {
                #[cfg(test)]
                self.inspected.set(self.inspected.get() + 1);
                let range = &self.by_start[*index];
                if range.end < start {
                    break;
                }
                if !check(range.file) {
                    return false;
                }
            }
        } else {
            for range in &self.by_start {
                #[cfg(test)]
                self.inspected.set(self.inspected.get() + 1);
                if !check(range.file) {
                    return false;
                }
            }
        }
        (start > self.center
            || self
                .left
                .as_ref()
                .is_none_or(|left| left.all_overlapping(start, end, check)))
            && (end < self.center
                || self
                    .right
                    .as_ref()
                    .is_none_or(|right| right.all_overlapping(start, end, check)))
    }

    #[cfg(test)]
    fn inspections(&self) -> usize {
        self.inspected.get()
            + self.left.as_ref().map_or(0, |tree| tree.inspections())
            + self.right.as_ref().map_or(0, |tree| tree.inspections())
    }
}

fn file_columns_unchanged(
    file: &DataFileMeta,
    schema: &crate::spec::TableSchema,
    keys: &[&DataField],
    source_id: i64,
    has_row_range: bool,
) -> bool {
    // Java aligns column sequences with the physical schema: omitted write
    // columns and row-tracking metadata do not occupy a sequence slot.
    let fields = schema.data_file_fields(file.write_cols.as_deref());
    if fields
        .iter()
        .map(DataField::id)
        .collect::<HashSet<_>>()
        .len()
        != fields.len()
        || file.write_cols.as_ref().is_some_and(|columns| {
            columns.iter().any(|name| {
                !crate::spec::is_row_tracking_column(name)
                    && name != crate::spec::VALUE_KIND_FIELD_NAME
                    && !schema.fields().iter().any(|field| field.name() == name)
            })
        })
    {
        return false;
    }
    keys.iter().all(|key| {
        let Some(position) = fields.iter().position(|field| field.id() == key.id()) else {
            return true;
        };
        let sequence = file
            .column_max_sequence_numbers
            .as_ref()
            .filter(|sequences| sequences.len() == fields.len())
            .and_then(|sequences| sequences.get(position))
            .copied()
            .filter(|sequence| *sequence > 0 && *sequence <= file.max_sequence_number)
            .unwrap_or(file.max_sequence_number);
        has_row_range && sequence > 0 && sequence <= source_id
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        BinaryRowBuilder, DataFileMeta, DataType as PaimonType, IntType, Schema as PaimonSchema,
        TableSchema,
    };

    fn test_schema() -> TableSchema {
        let schema = PaimonSchema::builder()
            .column("key", PaimonType::Int(IntType::new()))
            .column("value", PaimonType::Int(IntType::new()))
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_file(name: &str, start: i64, end: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: name.into(),
            file_size: 128,
            row_count: end - start + 1,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 1,
            max_sequence_number: 1,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            first_row_id: Some(start),
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
            column_max_sequence_numbers: None,
        }
    }

    #[test]
    fn file_range_lookup_preserves_inclusive_nested_and_duplicate_overlaps() {
        let intervals = [
            (20, 29),
            (0, 100),
            (10, 19),
            (10, 19),
            (5, 9),
            (30, 30),
            (0, 0),
            (20, 20),
            (29, 40),
        ];
        let files = intervals
            .iter()
            .enumerate()
            .map(|(id, (start, end))| test_file(&id.to_string(), *start, *end))
            .collect::<Vec<_>>();
        let lookup = FileRangeLookup::new(files.iter().collect());
        let tree = lookup.ranges.as_ref().unwrap();
        for (start, end) in [
            (0, 0),
            (4, 5),
            (9, 10),
            (19, 20),
            (29, 30),
            (31, 39),
            (101, 102),
            (i64::MIN, -1),
        ] {
            let mut expected = files
                .iter()
                .filter(|file| {
                    let (file_start, file_end) = file.row_id_range().unwrap();
                    file_start <= end && file_end >= start
                })
                .map(|file| file.file_name.clone())
                .collect::<Vec<_>>();
            let mut actual = Vec::new();
            assert!(tree.all_overlapping(start, end, &mut |file| {
                actual.push(file.file_name.clone());
                true
            }));
            actual.sort();
            expected.sort();
            assert_eq!(actual, expected, "query=[{start}, {end}]");
            if !expected.is_empty() {
                let mut checks = 0;
                assert!(!tree.all_overlapping(start, end, &mut |_| {
                    checks += 1;
                    false
                }));
                assert_eq!(checks, 1, "stop at the first stale overlap");
            }
        }
        assert!(FileRangeLookup::new(Vec::new()).ranges.is_none());
    }

    #[test]
    fn file_range_lookup_checks_only_relevant_columns_ranges_and_sources() {
        let schema = test_schema();
        let key = &schema.fields()[0];
        let value = &schema.fields()[1];
        let mut updated = test_file("updated", 10, 19);
        updated.max_sequence_number = 3;
        updated.column_max_sequence_numbers = Some(vec![1, 3]);
        let mut malformed = test_file("malformed", 30, 39);
        malformed.schema_id = 99;
        let lookup = FileRangeLookup::new(vec![&updated, &malformed]);
        assert!(lookup.all_overlapping_unchanged(0, 9, &schema, &[key], 1));
        assert!(lookup.all_overlapping_unchanged(10, 19, &schema, &[key], 1));
        assert!(!lookup.all_overlapping_unchanged(10, 19, &schema, &[value], 2));
        assert!(lookup.all_overlapping_unchanged(10, 19, &schema, &[value], 3));
        assert!(!lookup.all_overlapping_unchanged(29, 30, &schema, &[key], 3));

        let mut unknown = test_file("unknown", 0, 9);
        unknown.first_row_id = None;
        unknown.write_cols = Some(vec!["value".into()]);
        unknown.max_sequence_number = 3;
        assert!(
            FileRangeLookup::new(vec![&unknown]).all_overlapping_unchanged(
                100,
                109,
                &schema,
                &[key],
                1
            )
        );
        assert!(
            !FileRangeLookup::new(vec![&unknown]).all_overlapping_unchanged(
                100,
                109,
                &schema,
                &[value],
                3
            )
        );
        unknown.write_cols = None;
        assert!(
            !FileRangeLookup::new(vec![&unknown]).all_overlapping_unchanged(
                100,
                109,
                &schema,
                &[key],
                3
            )
        );
        for (first_row_id, row_count) in [(Some(i64::MAX), 2), (Some(0), 0)] {
            unknown.first_row_id = first_row_id;
            unknown.row_count = row_count;
            assert!(
                !FileRangeLookup::new(vec![&unknown]).all_overlapping_unchanged(
                    100,
                    109,
                    &schema,
                    &[key],
                    3
                )
            );
        }
    }

    #[test]
    fn file_range_lookup_keeps_partition_bucket_groups_separate() {
        let field = DataField::new(0, "part".into(), PaimonType::Int(IntType::new()));
        let partition = |value| {
            let mut builder = BinaryRowBuilder::new(1);
            builder.write_int(0, value);
            builder.build()
        };
        let split = |part, bucket, file| {
            DataSplit::builder()
                .with_snapshot(1)
                .with_partition(partition(part))
                .with_bucket(bucket)
                .with_bucket_path(format!("memory:/part={part}/bucket-{bucket}"))
                .with_data_files(vec![file])
                .build()
                .unwrap()
        };
        let good = test_file("good", 0, 9);
        let mut unknown = test_file("unknown", 0, 9);
        unknown.first_row_id = None;
        let splits = vec![
            split(1, 0, good.clone()),
            split(1, 0, good),
            split(1, 1, unknown.clone()),
            split(2, 0, unknown),
        ];
        let index_groups = HashSet::from([
            (partition(1).to_serialized_bytes(), 0),
            (partition(1).to_serialized_bytes(), 1),
            (partition(2).to_serialized_bytes(), 0),
        ]);
        let groups = file_range_lookups(&splits, &[field], &index_groups).unwrap();
        let schema = test_schema();
        let keys = [&schema.fields()[0]];
        assert_eq!(groups.len(), 3);
        assert!(groups[&(partition(1).to_serialized_bytes(), 0)]
            .all_overlapping_unchanged(0, 9, &schema, &keys, 1));
        for group in [
            (partition(1).to_serialized_bytes(), 1),
            (partition(2).to_serialized_bytes(), 0),
        ] {
            assert!(!groups[&group].all_overlapping_unchanged(0, 9, &schema, &keys, 1));
        }
        let wanted = HashSet::from([(partition(1).to_serialized_bytes(), 0)]);
        let groups = file_range_lookups(
            &splits,
            &[DataField::new(
                0,
                "part".into(),
                PaimonType::Int(IntType::new()),
            )],
            &wanted,
        )
        .unwrap();
        assert_eq!(groups.len(), 1);
        assert!(groups[&(partition(1).to_serialized_bytes(), 0)]
            .all_overlapping_unchanged(0, 9, &schema, &keys, 1));
    }

    #[test]
    fn file_range_lookup_avoids_quadratic_work_with_one_covering_file() {
        const SHARDS: usize = 5_000;
        let mut files = (0..SHARDS)
            .rev()
            .map(|id| test_file(&id.to_string(), (id * 4) as i64, (id * 4 + 1) as i64))
            .collect::<Vec<_>>();
        files.push(test_file("covering", 0, (SHARDS * 4) as i64));
        let lookup = FileRangeLookup::new(files.iter().collect());
        let tree = lookup.ranges.as_ref().unwrap();
        let schema = test_schema();
        let mut overlaps = 0;
        for id in 0..SHARDS {
            let start = (id * 4) as i64;
            assert!(lookup.all_overlapping_unchanged(
                start,
                start + 1,
                &schema,
                &[&schema.fields()[0]],
                1,
            ));
            assert!(tree.all_overlapping(start, start + 1, &mut |_| {
                overlaps += 1;
                true
            }));
        }
        assert_eq!(overlaps, SHARDS * 2);
        assert!(
            tree.inspections() < SHARDS * 2 * 32,
            "{} range inspections for {SHARDS} shards must stay below quadratic work",
            tree.inspections()
        );
        // Many ranges crossing one center also need bounded scans at either
        // edge; checking all crossing ranges would reintroduce quadratic work.
        for left_edge in [true, false] {
            let files = (0..SHARDS)
                .map(|id| {
                    let (start, end) = if left_edge {
                        ((id * 4) as i64, (SHARDS * 4) as i64)
                    } else {
                        (0, (id * 4 + 1) as i64)
                    };
                    test_file(&id.to_string(), start, end)
                })
                .collect::<Vec<_>>();
            let lookup = FileRangeLookup::new(files.iter().collect());
            let tree = lookup.ranges.as_ref().unwrap();
            let query = if left_edge {
                0
            } else {
                (SHARDS * 4 - 3) as i64
            };
            let mut overlaps = 0;
            for _ in 0..SHARDS {
                assert!(tree.all_overlapping(query, query, &mut |_| {
                    overlaps += 1;
                    true
                }));
            }
            assert_eq!(overlaps, SHARDS);
            assert!(tree.inspections() <= SHARDS * 2, "left_edge={left_edge}");
        }
    }

    #[test]
    fn indexed_columns_use_compacted_column_sequences() {
        let schema = PaimonSchema::builder()
            .column("key", PaimonType::Int(IntType::new()))
            .column("value", PaimonType::Int(IntType::new()))
            .column("keep", PaimonType::Int(IntType::new()))
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        let mut file = DataFileMeta {
            file_name: "data.parquet".into(),
            file_size: 128,
            row_count: 3,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 1,
            max_sequence_number: 1,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            first_row_id: Some(0),
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
            column_max_sequence_numbers: None,
        };
        file.write_cols = Some(vec!["key".into(), "value".into()]);
        file.column_max_sequence_numbers = Some(vec![1, 3]);
        file.max_sequence_number = 3;
        file.file_source = Some(1);
        let key = &schema.fields()[0];
        assert!(file_columns_unchanged(&file, &schema, &[key], 2, true));
        file.column_max_sequence_numbers = Some(vec![3, 3]);
        assert!(!file_columns_unchanged(&file, &schema, &[key], 2, true));
        for sequences in [None, Some(vec![1]), Some(vec![0, 3]), Some(vec![4, 3])] {
            file.column_max_sequence_numbers = sequences;
            assert!(!file_columns_unchanged(&file, &schema, &[key], 2, true));
        }
        file.max_sequence_number = 1;
        file.column_max_sequence_numbers = None;
        assert!(file_columns_unchanged(&file, &schema, &[key], 2, true));
        file.max_sequence_number = 3;
        file.write_cols = None;
        file.column_max_sequence_numbers = Some(vec![1, 3, 1]);
        assert!(file_columns_unchanged(&file, &schema, &[key], 2, true));
        file.write_cols = Some(vec!["_ROW_ID".into(), "value".into(), "key".into()]);
        file.column_max_sequence_numbers = Some(vec![3, 1]);
        assert!(file_columns_unchanged(&file, &schema, &[key], 2, true));
        for columns in [
            vec!["key".into(), "key".into()],
            vec!["key".into(), "unknown".into()],
        ] {
            file.write_cols = Some(columns);
            file.column_max_sequence_numbers = Some(vec![1, 1]);
            assert!(!file_columns_unchanged(&file, &schema, &[key], 2, true));
        }

        let schema = PaimonSchema::builder()
            .column("blob", PaimonType::Blob(crate::spec::BlobType::new()))
            .column("key", PaimonType::Int(IntType::new()))
            .column("value", PaimonType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option("data-evolution.write-cols-optimization.enabled", "true")
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        let key = &schema.fields()[1];
        file.write_cols = None;
        file.column_max_sequence_numbers = Some(vec![1, 3]);
        assert!(file_columns_unchanged(&file, &schema, &[key], 2, true));
        file.write_cols = Some(vec!["blob".into()]);
        file.column_max_sequence_numbers = Some(vec![3]);
        assert!(file_columns_unchanged(&file, &schema, &[key], 2, true));
    }
}
