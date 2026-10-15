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

//! External global-index selections used by ordinary batch scans.

use super::{merge_row_ranges, DataSplit, Plan, RowRange};

/// Externally evaluated index selections. Row ranges address stable global row
/// IDs in Data Evolution tables. Primary-key selections retain their physical
/// splits and source snapshot, as in Java's GlobalIndexSplitResult.
#[derive(Debug, Clone)]
pub enum GlobalIndexResult {
    RowRanges(Vec<RowRange>),
    ScoredRowIds(crate::vector_search::ScoredRowIds),
    Splits {
        snapshot_id: Option<i64>,
        splits: Vec<DataSplit>,
    },
}

impl GlobalIndexResult {
    pub(crate) fn row_ranges(&self) -> crate::Result<Option<Vec<RowRange>>> {
        match self {
            Self::RowRanges(ranges) => Ok(Some(merge_row_ranges(ranges.clone()))),
            Self::Splits { .. } => Ok(None),
            Self::ScoredRowIds(hits) => {
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
        let Self::ScoredRowIds(hits) = self else {
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
        let snapshot_id = plan.snapshot_id();
        let splits = plan
            .into_splits()
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
                        values.push(*scores.get(&id).ok_or_else(|| crate::Error::DataInvalid {
                            message: format!("Missing score for selected global row ID {id}"),
                            source: None,
                        })?);
                    }
                }
                split.with_scores(values)
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let plan = Plan::new(splits);
        Ok(match snapshot_id {
            Some(id) => plan.with_snapshot_id(id),
            None => plan,
        })
    }
}

#[cfg(test)]
mod tests;
