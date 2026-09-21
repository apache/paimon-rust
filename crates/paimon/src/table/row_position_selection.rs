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

//! Select balanced row-position slices for data-evolution and append scans.

use super::source::{merge_row_ranges, DataSplit, DataSplitBuilder, RowRange};
use crate::spec::ManifestEntry;
use crate::{Error, Result};

#[derive(Debug, Clone, Copy)]
pub(crate) enum RowPositionSelection {
    Slice { start: u64, end: u64 },
    Shard { index: u64, count: u64 },
}

impl RowPositionSelection {
    pub(crate) fn slice(start: u64, end: u64) -> Result<Self> {
        if start >= end {
            return Err(Error::DataInvalid {
                message: "row-position slice start must be less than end".into(),
                source: None,
            });
        }
        Ok(Self::Slice { start, end })
    }

    pub(crate) fn shard(index: u64, count: u64) -> Result<Self> {
        if count == 0 || index >= count {
            return Err(Error::DataInvalid {
                message: "row-position shard count must be positive and index less than count"
                    .into(),
                source: None,
            });
        }
        Ok(Self::Shard { index, count })
    }

    pub(crate) fn is_slice(self) -> bool {
        matches!(self, Self::Slice { .. })
    }

    fn bounds(self, total: u64) -> (u64, u64) {
        match self {
            Self::Slice { start, end } => (start, end.min(total)),
            Self::Shard { index, count } => {
                let size = total / count;
                let remainder = total % count;
                let start = index * size + index.min(remainder);
                (start, start + size + u64::from(index < remainder))
            }
        }
    }

    /// Positions count the union of complete candidate file ranges. Updates
    /// and blob column files share row IDs and must not multiply that count;
    /// deleted rows still occupy positions until their files leave the snapshot.
    pub(crate) fn select(
        self,
        entries: &[ManifestEntry],
        selected_ranges: Option<&[RowRange]>,
    ) -> Result<Vec<RowRange>> {
        let ranges = entries
            .iter()
            .map(|entry| {
                let file = entry.file();
                match file.row_id_range() {
                    Some((from, to)) if from >= 0 && to >= from => Ok(RowRange::new(from, to)),
                    _ => Err(Error::DataInvalid {
                        message: format!(
                            "Row-position selection requires a valid row-id range for '{}'",
                            file.file_name
                        ),
                        source: None,
                    }),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(self.select_from_ranges(ranges, selected_ranges))
    }

    fn select_from_ranges(
        self,
        ranges: Vec<RowRange>,
        selected_ranges: Option<&[RowRange]>,
    ) -> Vec<RowRange> {
        let ranges = merge_row_ranges(ranges);
        // Nonnegative i64 row IDs span at most 2^63 positions. Use unsigned
        // counts so a range ending at i64::MAX cannot overflow RowRange::count.
        let count = |range: &RowRange| range.to() as u64 - range.from() as u64 + 1;
        let total: u64 = ranges.iter().map(count).sum();
        let (start, end) = self.bounds(total);
        if start >= end {
            return Vec::new();
        }
        let mut position = 0;
        let mut result = Vec::new();
        for range in ranges {
            let next = position + count(&range);
            let begin = start.max(position);
            let finish = end.min(next);
            if begin < finish {
                result.push(RowRange::new(
                    range.from() + (begin - position) as i64,
                    range.from() + (finish - position - 1) as i64,
                ));
            }
            position = next;
            if position >= end {
                break;
            }
        }
        if let Some(selected) = selected_ranges {
            let selected = merge_row_ranges(selected.to_vec());
            let mut intersection = Vec::new();
            let (mut left, mut right) = (0, 0);
            while left < result.len() && right < selected.len() {
                if let Some(range) =
                    result[left].intersect_inclusive(selected[right].from(), selected[right].to())
                {
                    intersection.push(range);
                }
                if result[left].to() <= selected[right].to() {
                    left += 1;
                } else {
                    right += 1;
                }
            }
            result = intersection;
        }
        merge_row_ranges(result)
    }

    /// Select physical append rows after stats pruning and split packing.
    ///
    /// Append positions follow the final split/file order. Row-tracked tables
    /// carry stable global row IDs to the reader; tables without row tracking
    /// carry positions local to the filtered output split. Filtering files here
    /// avoids opening files which contain no selected rows.
    pub(crate) fn select_append_splits(
        self,
        splits: Vec<DataSplit>,
        row_tracking_enabled: bool,
    ) -> Result<Vec<DataSplit>> {
        let mut total = 0u64;
        for split in &splits {
            for file in split.data_files() {
                let count = u64::try_from(file.row_count).map_err(|_| Error::DataInvalid {
                    message: format!(
                        "Row-position selection requires a valid row count for '{}'",
                        file.file_name
                    ),
                    source: None,
                })?;
                total = total.checked_add(count).ok_or_else(|| Error::DataInvalid {
                    message: "Row-position selection row count overflow".to_string(),
                    source: None,
                })?;
            }
        }
        let (start, end) = self.bounds(total);
        if start >= end {
            return Ok(Vec::new());
        }

        let mut position = 0u64;
        let mut selected_splits = Vec::new();
        for split in splits {
            let mut files = Vec::new();
            let mut deletion_files = split.data_deletion_files().map(|_| Vec::new());
            let mut ranges = Vec::new();
            let mut kept_position = 0u64;
            let mut needs_ranges = split.row_ranges().is_some();

            for (file_index, file) in split.data_files().iter().enumerate() {
                let count = file.row_count as u64;
                let next = position + count;
                let selected_start = start.max(position);
                let selected_end = end.min(next);
                if selected_start < selected_end {
                    let from = selected_start - position;
                    let to = selected_end - position;
                    needs_ranges |= from != 0 || to != count;
                    files.push(file.clone());
                    if let (Some(source), Some(selected)) =
                        (split.data_deletion_files(), deletion_files.as_mut())
                    {
                        selected.push(source[file_index].clone());
                    }

                    let range = if row_tracking_enabled {
                        let first_row_id = file.first_row_id.ok_or_else(|| Error::DataInvalid {
                            message: format!(
                                "Row-position selection requires a valid first row id for '{}'",
                                file.file_name
                            ),
                            source: None,
                        })?;
                        let from = first_row_id.checked_add(from as i64).ok_or_else(|| {
                            Error::DataInvalid {
                                message: "Row-position selection row id overflow".to_string(),
                                source: None,
                            }
                        })?;
                        let to = first_row_id.checked_add(to as i64 - 1).ok_or_else(|| {
                            Error::DataInvalid {
                                message: "Row-position selection row id overflow".to_string(),
                                source: None,
                            }
                        })?;
                        RowRange::new(from, to)
                    } else {
                        let from =
                            kept_position
                                .checked_add(from)
                                .ok_or_else(|| Error::DataInvalid {
                                    message: "Row-position selection split offset overflow"
                                        .to_string(),
                                    source: None,
                                })?;
                        let to = kept_position.checked_add(to - 1).ok_or_else(|| {
                            Error::DataInvalid {
                                message: "Row-position selection split offset overflow".to_string(),
                                source: None,
                            }
                        })?;
                        RowRange::new(
                            i64::try_from(from).map_err(|_| Error::DataInvalid {
                                message: "Row-position selection split offset exceeds i64"
                                    .to_string(),
                                source: None,
                            })?,
                            i64::try_from(to).map_err(|_| Error::DataInvalid {
                                message: "Row-position selection split offset exceeds i64"
                                    .to_string(),
                                source: None,
                            })?,
                        )
                    };
                    ranges.push(range);
                    kept_position += count;
                }
                position = next;
            }

            if files.is_empty() {
                if position >= end {
                    break;
                }
                continue;
            }

            let ranges = if let Some(existing) = split.row_ranges() {
                intersect_ranges(&ranges, existing)
            } else {
                merge_row_ranges(ranges)
            };
            if ranges.is_empty() {
                if position >= end {
                    break;
                }
                continue;
            }

            let mut builder = DataSplitBuilder::new()
                .with_snapshot(split.snapshot_id())
                .with_partition(split.partition().clone())
                .with_bucket(split.bucket())
                .with_bucket_path(split.bucket_path().to_string())
                .with_total_buckets(split.total_buckets())
                .with_data_files(files)
                .with_raw_convertible(split.raw_convertible())
                .with_streaming(split.is_streaming());
            if let Some(deletion_files) = deletion_files {
                builder = builder.with_data_deletion_files(deletion_files);
            }
            if needs_ranges {
                builder = builder.with_row_ranges(ranges);
            }
            selected_splits.push(builder.build()?);
            if position >= end {
                break;
            }
        }
        Ok(selected_splits)
    }
}

fn intersect_ranges(left: &[RowRange], right: &[RowRange]) -> Vec<RowRange> {
    let left = merge_row_ranges(left.to_vec());
    let right = merge_row_ranges(right.to_vec());
    let mut result = Vec::new();
    let (mut left_index, mut right_index) = (0, 0);
    while left_index < left.len() && right_index < right.len() {
        if let Some(range) =
            left[left_index].intersect_inclusive(right[right_index].from(), right[right_index].to())
        {
            result.push(range);
        }
        if left[left_index].to() <= right[right_index].to() {
            left_index += 1;
        } else {
            right_index += 1;
        }
    }
    merge_row_ranges(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{BinaryRow, DataFileMeta};
    use crate::table::source::DeletionFile;

    fn ranges(pairs: &[(i64, i64)]) -> Vec<RowRange> {
        pairs
            .iter()
            .map(|&(start, end)| RowRange::new(start, end))
            .collect()
    }

    fn file(name: &str, row_count: i64, first_row_id: Option<i64>) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 100,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            value_stats: BinaryTableStats::new(Vec::new(), Vec::new(), Vec::new()),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            first_row_id,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
            column_max_sequence_numbers: None,
        }
    }

    fn split(
        snapshot: i64,
        files: Vec<DataFileMeta>,
        deletion_files: Option<Vec<Option<DeletionFile>>>,
        row_ranges: Option<Vec<RowRange>>,
    ) -> DataSplit {
        let mut builder = DataSplitBuilder::new()
            .with_snapshot(snapshot)
            .with_partition(BinaryRow::new(0))
            .with_bucket(2)
            .with_bucket_path("memory:/append/bucket-2".to_string())
            .with_total_buckets(4)
            .with_data_files(files)
            .with_raw_convertible(true)
            .with_streaming(true);
        if let Some(deletion_files) = deletion_files {
            builder = builder.with_data_deletion_files(deletion_files);
        }
        if let Some(row_ranges) = row_ranges {
            builder = builder.with_row_ranges(row_ranges);
        }
        builder.build().unwrap()
    }

    fn pairs(ranges: Option<&[RowRange]>) -> Option<Vec<(i64, i64)>> {
        ranges.map(|ranges| {
            ranges
                .iter()
                .map(|range| (range.from(), range.to()))
                .collect()
        })
    }

    #[test]
    fn append_slice_filters_files_and_rebases_local_ranges() {
        let deletion = DeletionFile::new("b.dv".to_string(), 0, 10, Some(1));
        let input = split(
            7,
            vec![file("a.parquet", 3, None), file("b.parquet", 4, None)],
            Some(vec![None, Some(deletion.clone())]),
            None,
        );

        let selected = RowPositionSelection::slice(4, 6)
            .unwrap()
            .select_append_splits(vec![input], false)
            .unwrap();

        assert_eq!(selected.len(), 1);
        let selected = &selected[0];
        assert_eq!(
            selected
                .data_files()
                .iter()
                .map(|file| file.file_name.as_str())
                .collect::<Vec<_>>(),
            vec!["b.parquet"]
        );
        assert_eq!(pairs(selected.row_ranges()), Some(vec![(1, 2)]));
        assert_eq!(
            selected.data_deletion_files(),
            Some([Some(deletion)].as_slice())
        );
        assert_eq!(selected.snapshot_id(), 7);
        assert_eq!(selected.bucket(), 2);
        assert_eq!(selected.total_buckets(), 4);
        assert!(selected.is_streaming());
        assert!(selected.raw_convertible());
    }

    #[test]
    fn append_slice_uses_global_ids_when_row_tracking_is_enabled() {
        let input = split(
            1,
            vec![
                file("a.parquet", 3, Some(100)),
                file("b.parquet", 4, Some(200)),
            ],
            None,
            None,
        );

        let selected = RowPositionSelection::slice(2, 5)
            .unwrap()
            .select_append_splits(vec![input], true)
            .unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(
            pairs(selected[0].row_ranges()),
            Some(vec![(102, 102), (200, 201)])
        );

        let restored =
            DataSplit::deserialize_split_v1(&selected[0].serialize_split_v1().unwrap()).unwrap();
        assert_eq!(
            pairs(restored.row_ranges()),
            pairs(selected[0].row_ranges())
        );
    }

    #[test]
    fn append_shards_cover_physical_rows_once_across_splits() {
        let inputs = vec![
            split(
                1,
                vec![file("a.parquet", 3, None), file("b.parquet", 4, None)],
                None,
                None,
            ),
            split(1, vec![file("c.parquet", 2, None)], None, None),
        ];
        let mut counts = Vec::new();
        for index in 0..4 {
            let selected = RowPositionSelection::shard(index, 4)
                .unwrap()
                .select_append_splits(inputs.clone(), false)
                .unwrap();
            counts.push(selected.iter().map(DataSplit::row_count).sum::<i64>());
        }
        assert_eq!(counts, vec![3, 2, 2, 2]);
        assert_eq!(counts.iter().sum::<i64>(), 9);
    }

    #[test]
    fn append_selection_intersects_existing_global_ranges() {
        let input = split(
            1,
            vec![
                file("a.parquet", 3, Some(100)),
                file("b.parquet", 4, Some(200)),
            ],
            None,
            Some(ranges(&[(201, 203)])),
        );
        let selected = RowPositionSelection::slice(2, 5)
            .unwrap()
            .select_append_splits(vec![input], true)
            .unwrap();
        assert_eq!(pairs(selected[0].row_ranges()), Some(vec![(201, 201)]));
    }

    #[test]
    fn append_selection_rejects_unknown_counts_and_missing_row_ids() {
        let unknown = split(
            1,
            vec![file(
                "unknown.parquet",
                DataFileMeta::ROW_COUNT_UNKNOWN,
                None,
            )],
            None,
            None,
        );
        assert!(RowPositionSelection::slice(0, 1)
            .unwrap()
            .select_append_splits(vec![unknown], false)
            .is_err());

        let missing_id = split(1, vec![file("missing.parquet", 1, None)], None, None);
        assert!(RowPositionSelection::slice(0, 1)
            .unwrap()
            .select_append_splits(vec![missing_id], true)
            .is_err());
    }

    #[test]
    fn slice_counts_shared_row_ids_once_and_skips_gaps() {
        let candidates = ranges(&[(10, 12), (0, 3), (1, 2), (11, 12), (20, 21)]);
        assert_eq!(
            RowPositionSelection::slice(2, 7)
                .unwrap()
                .select_from_ranges(candidates, None),
            ranges(&[(2, 3), (10, 12)])
        );
    }

    #[test]
    fn shards_are_balanced_disjoint_and_cover_all_candidates() {
        let candidates = ranges(&[(0, 3), (10, 12), (20, 21), (0, 3)]);
        let expected = [
            ranges(&[(0, 2)]),
            ranges(&[(3, 3), (10, 11)]),
            ranges(&[(12, 12), (20, 21)]),
        ];
        for (index, expected) in expected.into_iter().enumerate() {
            assert_eq!(
                RowPositionSelection::shard(index as u64, 3)
                    .unwrap()
                    .select_from_ranges(candidates.clone(), None),
                expected
            );
        }
        for index in 9..12 {
            assert!(RowPositionSelection::shard(index, 12)
                .unwrap()
                .select_from_ranges(candidates.clone(), None)
                .is_empty());
        }
    }

    #[test]
    fn explicit_ranges_intersect_after_positions_are_assigned() {
        let selection = RowPositionSelection::shard(0, 2).unwrap();
        let candidates = ranges(&[(0, 5)]);
        assert!(selection
            .select_from_ranges(candidates.clone(), Some(&ranges(&[(4, 4)])))
            .is_empty());
        assert_eq!(
            selection.select_from_ranges(candidates.clone(), Some(&ranges(&[(2, 4)]))),
            ranges(&[(2, 2)])
        );
        assert!(selection
            .select_from_ranges(candidates, Some(&[]))
            .is_empty());
    }

    #[test]
    fn slice_clips_out_of_bounds_and_handles_maximum_row_id() {
        let candidates = ranges(&[(0, i64::MAX)]);
        assert_eq!(
            RowPositionSelection::slice(i64::MAX as u64, u64::MAX)
                .unwrap()
                .select_from_ranges(candidates.clone(), None),
            ranges(&[(i64::MAX, i64::MAX)])
        );
        assert_eq!(
            RowPositionSelection::shard(1, 2)
                .unwrap()
                .select_from_ranges(candidates, None),
            ranges(&[(1 << 62, i64::MAX)])
        );
        assert!(RowPositionSelection::slice(5, 10)
            .unwrap()
            .select_from_ranges(ranges(&[(0, 2)]), None)
            .is_empty());
        assert!(RowPositionSelection::shard(0, 1)
            .unwrap()
            .select_from_ranges(vec![], None)
            .is_empty());
    }

    #[test]
    fn invalid_distribution_parameters_are_rejected() {
        assert!(RowPositionSelection::slice(0, 0).is_err());
        assert!(RowPositionSelection::slice(2, 1).is_err());
        assert!(RowPositionSelection::shard(0, 0).is_err());
        assert!(RowPositionSelection::shard(2, 2).is_err());
    }

    #[test]
    fn positions_match_an_independent_row_enumeration() {
        // Enumerating a small row-id space supplies an oracle independent of
        // the range-merging and offset arithmetic used by the implementation.
        for mask in 0u16..256 {
            let ids: Vec<i64> = (0..8).filter(|id| mask & (1 << id) != 0).collect();
            let mut candidates: Vec<_> = ids.iter().map(|&id| RowRange::new(id, id)).collect();
            candidates.extend(candidates.clone()); // duplicate column/update files
            candidates.reverse(); // manifest order must not assign positions
            for explicit in [
                None,
                Some(ranges(&[])),
                Some(ranges(&[(1, 2), (5, 6), (2, 2)])),
            ] {
                let expected = |start: usize, end: usize| {
                    ids.iter()
                        .skip(start)
                        .take(end.saturating_sub(start))
                        .copied()
                        .filter(|id| {
                            explicit.as_ref().is_none_or(|ranges| {
                                ranges
                                    .iter()
                                    .any(|range| range.from() <= *id && *id <= range.to())
                            })
                        })
                        .collect::<Vec<_>>()
                };
                let expand = |ranges: Vec<RowRange>| {
                    ranges
                        .into_iter()
                        .flat_map(|range| range.from()..=range.to())
                        .collect::<Vec<_>>()
                };
                for start in 0..10 {
                    for end in start + 1..=10 {
                        assert_eq!(
                            expand(
                                RowPositionSelection::slice(start as u64, end as u64)
                                    .unwrap()
                                    .select_from_ranges(candidates.clone(), explicit.as_deref())
                            ),
                            expected(start, end),
                            "mask={mask}, slice=({start},{end}), explicit={explicit:?}"
                        );
                    }
                }
                for count in 1..=10 {
                    let mut all = vec![];
                    for index in 0..count {
                        let start = index * (ids.len() / count) + index.min(ids.len() % count);
                        let end =
                            start + ids.len() / count + usize::from(index < ids.len() % count);
                        let actual = expand(
                            RowPositionSelection::shard(index as u64, count as u64)
                                .unwrap()
                                .select_from_ranges(candidates.clone(), explicit.as_deref()),
                        );
                        assert_eq!(
                            actual,
                            expected(start, end),
                            "mask={mask}, shard=({index},{count})"
                        );
                        all.extend(actual);
                    }
                    assert_eq!(
                        all,
                        expected(0, ids.len()),
                        "shards must cover each selected row exactly once"
                    );
                }
            }
        }
    }
}
