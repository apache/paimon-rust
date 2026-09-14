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

//! Map row positions to data-evolution row IDs before group pruning.

use super::source::{merge_row_ranges, RowRange};
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
        let (start, end) = match self {
            Self::Slice { start, end } => (start, end.min(total)),
            Self::Shard { index, count } => {
                let size = total / count;
                let remainder = total % count;
                let start = index * size + index.min(remainder);
                (start, start + size + u64::from(index < remainder))
            }
        };
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranges(pairs: &[(i64, i64)]) -> Vec<RowRange> {
        pairs
            .iter()
            .map(|&(start, end)| RowRange::new(start, end))
            .collect()
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
}
