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

//! Global-index selections shared by searches and table scans.

use super::{merge_row_ranges, DataSplit, Plan, RowRange, Table};
use crate::spec::BinaryRow;
use crate::table::pk_vector_indexed_split_read::PkVectorIndexedSplit;
use crate::table::pk_vector_orchestrator::{
    build_indexed_splits, PkVectorCandidate, PkVectorSearchSplit,
};
use crate::vector_search::ScoredRowIds;
use crate::vindex::pkvector::metric::VectorSearchMetric;
use std::sync::Arc;

mod read;
pub use read::GlobalIndexResultReadBuilder;

/// One scored physical row in a primary-key table, in relevance order.
#[derive(Debug, Clone)]
pub struct PrimaryKeySearchPosition {
    pub partition: BinaryRow,
    pub bucket: i32,
    pub data_file_name: String,
    pub row_position: i64,
    pub score: f32,
}

#[derive(Debug, Clone)]
enum GlobalIndexSelection {
    RowRanges(Vec<RowRange>),
    ScoredRowIds(ScoredRowIds),
    Splits(Vec<DataSplit>),
}

/// Context retained only by table searches, for ranked reads without re-searching.
#[derive(Debug, Clone)]
struct IndexReadContext {
    table: Arc<Table>,
    vector_column: Option<String>,
    positions: Option<Vec<PrimaryKeySearchPosition>>,
}

/// An evaluated global-index selection, accepted directly by table scans.
///
/// Data Evolution results address stable global row IDs. Primary-key results
/// retain physical splits and their source snapshot, as in Java's
/// `GlobalIndexSplitResult`. Table searches also retain the context needed by
/// [`new_read_builder`](Self::new_read_builder) to read rows in relevance order.
#[derive(Debug, Clone)]
pub struct GlobalIndexResult {
    selection: GlobalIndexSelection,
    snapshot_id: Option<i64>,
    read_context: Option<IndexReadContext>,
}

impl GlobalIndexResult {
    pub fn from_row_ranges(ranges: Vec<RowRange>) -> Self {
        Self {
            selection: GlobalIndexSelection::RowRanges(merge_row_ranges(ranges)),
            snapshot_id: None,
            read_context: None,
        }
    }

    pub fn from_scored_row_ids(hits: ScoredRowIds) -> Self {
        Self {
            selection: GlobalIndexSelection::ScoredRowIds(hits),
            snapshot_id: None,
            read_context: None,
        }
    }

    pub fn from_splits(snapshot_id: Option<i64>, splits: Vec<DataSplit>) -> Self {
        Self {
            selection: GlobalIndexSelection::Splits(splits),
            snapshot_id,
            read_context: None,
        }
    }

    pub(super) fn from_row_ids(
        table: Arc<Table>,
        vector_column: String,
        hits: ScoredRowIds,
    ) -> Self {
        let snapshot_id = table.travel_snapshot().map(|snapshot| snapshot.id());
        Self {
            snapshot_id,
            selection: GlobalIndexSelection::ScoredRowIds(hits),
            read_context: Some(IndexReadContext {
                table,
                vector_column: Some(vector_column),
                positions: None,
            }),
        }
    }

    pub(super) fn from_global_search(table: Arc<Table>, hits: ScoredRowIds) -> Self {
        let snapshot_id = table.travel_snapshot().map(|snapshot| snapshot.id());
        Self {
            snapshot_id,
            selection: GlobalIndexSelection::ScoredRowIds(hits),
            read_context: Some(IndexReadContext {
                table,
                vector_column: None,
                positions: None,
            }),
        }
    }

    #[cfg(feature = "fulltext")]
    pub(super) fn from_full_text_primary_key(
        table: Arc<Table>,
        snapshot_id: i64,
        candidates: Vec<super::pk_full_text_read::PrimaryKeyFullTextCandidate>,
        source_splits: &[super::pk_full_text_scan::PrimaryKeyFullTextSearchSplit],
    ) -> crate::Result<Self> {
        let positions = candidates
            .iter()
            .map(|c| PrimaryKeySearchPosition {
                partition: c.partition.clone(),
                bucket: c.bucket,
                data_file_name: c.data_file_name.clone(),
                row_position: c.row_position,
                score: c.score,
            })
            .collect();
        let splits =
            super::pk_full_text_read::build_full_text_indexed_splits(candidates, source_splits)?;
        Self::from_indexed_splits(table, snapshot_id, positions, splits)
    }

    pub(super) fn from_primary_key(
        table: Arc<Table>,
        snapshot_id: i64,
        candidates: Vec<PkVectorCandidate>,
        source_splits: &[PkVectorSearchSplit],
        metric: VectorSearchMetric,
    ) -> crate::Result<Self> {
        let positions = candidates
            .iter()
            .map(|c| PrimaryKeySearchPosition {
                partition: c.partition.clone(),
                bucket: c.bucket,
                data_file_name: c.data_file_name.clone(),
                row_position: c.row_position,
                score: metric.distance_to_score(c.distance),
            })
            .collect();
        let splits = build_indexed_splits(candidates, source_splits, metric)?;
        Self::from_indexed_splits(table, snapshot_id, positions, splits)
    }

    fn from_indexed_splits(
        table: Arc<Table>,
        snapshot_id: i64,
        positions: Vec<PrimaryKeySearchPosition>,
        splits: Vec<PkVectorIndexedSplit>,
    ) -> crate::Result<Self> {
        let splits = splits
            .into_iter()
            .map(|selected| {
                let split = selected.split.with_selected_row_ranges(selected.row_ranges);
                match selected.scores {
                    Some(scores) => split.with_scores(scores),
                    None => Ok(split),
                }
            })
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(Self {
            selection: GlobalIndexSelection::Splits(splits),
            snapshot_id: (snapshot_id != 0).then_some(snapshot_id),
            read_context: Some(IndexReadContext {
                table,
                vector_column: None,
                positions: Some(positions),
            }),
        })
    }

    /// Source snapshot, or `None` when no snapshot was supplied or resolved.
    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    /// Source table retained by a search. External selections have no read context.
    pub fn table(&self) -> Option<&Table> {
        self.read_context
            .as_ref()
            .map(|context| context.table.as_ref())
    }

    /// Number of selected rows (a lower bound for splits with unknown row counts).
    pub fn len(&self) -> usize {
        match &self.selection {
            GlobalIndexSelection::RowRanges(ranges) => {
                ranges.iter().fold(0usize, |count, range| {
                    count.saturating_add(
                        usize::try_from(i128::from(range.to()) - i128::from(range.from()) + 1)
                            .unwrap_or(usize::MAX),
                    )
                })
            }
            GlobalIndexSelection::ScoredRowIds(hits) => hits.len(),
            GlobalIndexSelection::Splits(splits) => splits.iter().fold(0usize, |count, split| {
                count.saturating_add(usize::try_from(split.row_count()).unwrap_or(usize::MAX))
            }),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Scored global row IDs, in relevance order; rejects physical PK selections.
    pub fn row_ids(&self) -> crate::Result<&ScoredRowIds> {
        match &self.selection {
            GlobalIndexSelection::ScoredRowIds(hits) => Ok(hits),
            _ => Err(no_scored_row_ids()),
        }
    }

    pub fn into_row_ids(self) -> crate::Result<ScoredRowIds> {
        match self.selection {
            GlobalIndexSelection::ScoredRowIds(hits) => Ok(hits),
            _ => Err(no_scored_row_ids()),
        }
    }

    /// Scored physical positions in relevance order, available on PK search results.
    pub fn positions(&self) -> crate::Result<&[PrimaryKeySearchPosition]> {
        if !self.is_split_result() {
            return Err(crate::Error::Unsupported {
                message: "Global row-ID results use global row IDs, not primary-key file positions"
                    .into(),
            });
        }
        self.read_context
            .as_ref()
            .and_then(|context| context.positions.as_deref())
            .ok_or_else(|| crate::Error::Unsupported {
                message:
                    "Ranked physical positions are only available on primary-key search results"
                        .into(),
            })
    }

    /// Snapshot-scoped PK selections, including ranges and aligned scores.
    pub fn splits(&self) -> crate::Result<&[DataSplit]> {
        match &self.selection {
            GlobalIndexSelection::Splits(splits) => Ok(splits),
            _ => Err(crate::Error::Unsupported {
                message: "Global row-ID results do not carry primary-key indexed splits".into(),
            }),
        }
    }

    /// Snapshot-scoped PK selections in Java SplitSerializer v1 format.
    pub fn serialize_primary_key_splits(&self) -> crate::Result<Vec<Vec<u8>>> {
        self.splits()?
            .iter()
            .map(DataSplit::serialize_split_v1)
            .collect()
    }

    pub(super) fn is_split_result(&self) -> bool {
        matches!(self.selection, GlobalIndexSelection::Splits(_))
    }

    fn indexed_splits(&self) -> crate::Result<Vec<PkVectorIndexedSplit>> {
        self.splits()?
            .iter()
            .map(|split| {
                Ok(PkVectorIndexedSplit {
                    split: split.clone(),
                    row_ranges: split
                        .row_ranges()
                        .ok_or_else(|| crate::Error::DataInvalid {
                            message: "Ranked primary-key splits must carry physical row ranges"
                                .into(),
                            source: None,
                        })?
                        .to_vec(),
                    scores: split.scores().map(<[f32]>::to_vec),
                })
            })
            .collect()
    }

    /// Read selected user columns and scores in relevance order. External selections
    /// must be read through a table scan because they do not retain a source table.
    pub fn new_read_builder(&self) -> GlobalIndexResultReadBuilder<'_> {
        GlobalIndexResultReadBuilder::new(self)
    }

    pub(crate) fn row_ranges(&self) -> crate::Result<Option<Vec<RowRange>>> {
        match &self.selection {
            GlobalIndexSelection::RowRanges(ranges) => Ok(Some(merge_row_ranges(ranges.clone()))),
            GlobalIndexSelection::Splits(_) => Ok(None),
            GlobalIndexSelection::ScoredRowIds(hits) => {
                if hits.row_ids.len() != hits.scores.len() {
                    return Err(crate::Error::DataInvalid {
                        message: "Scored row IDs and scores must have equal lengths".into(),
                        source: None,
                    });
                }
                let mut ranges = hits
                    .row_ids
                    .iter()
                    .map(|id| {
                        let id = i64::try_from(*id).map_err(|_| crate::Error::DataInvalid {
                            message: "Global row ID exceeds the signed row-range address space"
                                .into(),
                            source: None,
                        })?;
                        Ok(RowRange::new(id, id))
                    })
                    .collect::<crate::Result<Vec<_>>>()?;
                ranges.sort_unstable_by_key(RowRange::from);
                if ranges.windows(2).any(|r| r[0].from() == r[1].from()) {
                    return Err(crate::Error::DataInvalid {
                        message: "Scored global row IDs must be unique".into(),
                        source: None,
                    });
                }
                Ok(Some(merge_row_ranges(ranges)))
            }
        }
    }

    fn score_map(&self) -> crate::Result<Option<std::collections::HashMap<i64, f32>>> {
        let GlobalIndexSelection::ScoredRowIds(hits) = &self.selection else {
            return Ok(None);
        };
        if hits.row_ids.len() != hits.scores.len() {
            return Err(crate::Error::DataInvalid {
                message: "Scored row IDs and scores must have equal lengths".into(),
                source: None,
            });
        }
        let mut scores = std::collections::HashMap::with_capacity(hits.len());
        for (id, score) in hits.row_ids.iter().zip(&hits.scores) {
            let id = i64::try_from(*id).map_err(|_| crate::Error::DataInvalid {
                message: "Global row ID exceeds the signed row-range address space".into(),
                source: None,
            })?;
            if scores.insert(id, *score).is_some() {
                return Err(crate::Error::DataInvalid {
                    message: "Scored global row IDs must be unique".into(),
                    source: None,
                });
            }
        }
        Ok(Some(scores))
    }

    pub(crate) fn attach_scores(&self, plan: Plan) -> crate::Result<Plan> {
        let Some(scores) = self.score_map()? else {
            return Ok(plan);
        };
        plan.map_splits(|splits| {
            splits
                .into_iter()
                .map(|split| {
                    let selected = split
                        .row_ranges()
                        .ok_or_else(|| crate::Error::DataInvalid {
                            message: "Scored global-index splits must carry row ranges".into(),
                            source: None,
                        })?;
                    let mut values = Vec::new();
                    for range in selected {
                        for id in range.from()..=range.to() {
                            values.push(*scores.get(&id).ok_or_else(|| {
                                crate::Error::DataInvalid {
                                    message: format!(
                                        "Missing score for selected global row ID {id}"
                                    ),
                                    source: None,
                                }
                            })?);
                        }
                    }
                    split.with_scores(values)
                })
                .collect::<crate::Result<Vec<_>>>()
        })
    }
}

fn no_scored_row_ids() -> crate::Error {
    crate::Error::Unsupported {
        message: "Scored global row IDs are unavailable for primary-key physical file positions or unscored row ranges".into(),
    }
}

#[cfg(test)]
mod search_tests;
#[cfg(test)]
mod tests;
