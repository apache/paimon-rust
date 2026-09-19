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

//! Snapshot-scoped search results for global row IDs and primary-key file positions.

use crate::spec::{BinaryRow, CoreOptions};
use crate::table::data_file_reader::DataFileReader;
use crate::table::de_vector_read::{materialize_row_ids, DeVectorRead};
use crate::table::pk_vector_indexed_split_read::PkVectorIndexedSplit;
use crate::table::pk_vector_orchestrator::{
    build_indexed_splits, PkVectorCandidate, PkVectorSearchSplit,
};
use crate::table::pk_vector_read::materialize_positions;
use crate::table::vector_search_common::resolve_materialize_read_type;
use crate::table::{ArrowRecordBatchStream, Table};
use crate::vector_search::ScoredRowIds;
use crate::vindex::pkvector::metric::VectorSearchMetric;
use std::sync::Arc;

/// One scored physical row in a primary-key table. Positions are local to the
/// named file; neither the primary key nor a global row ID is synthesized.
#[derive(Debug, Clone)]
pub struct PrimaryKeySearchPosition {
    pub partition: BinaryRow,
    pub bucket: i32,
    pub data_file_name: String,
    pub row_position: i64,
    pub score: f32,
}

#[derive(Debug, Clone)]
enum SearchHits {
    DataEvolution {
        vector_column: String,
        hits: ScoredRowIds,
    },
    PrimaryKey {
        positions: Vec<PrimaryKeySearchPosition>,
        splits: Vec<PkVectorIndexedSplit>,
    },
}

/// Ranked vector-search hits together with the source snapshot needed to read them.
///
/// Both single and batch searches return this type. DE hits use global row IDs;
/// PK hits use physical file positions, as in Java's `PrimaryKeyScoredResult`.
/// Searching does not materialize projected user columns. Use
/// [`new_read_builder`](Self::new_read_builder) when rows are needed.
#[derive(Debug, Clone)]
pub struct SearchResult {
    table: Arc<Table>,
    snapshot_id: Option<i64>,
    hits: SearchHits,
}

impl SearchResult {
    pub(super) fn from_row_ids(
        table: Arc<Table>,
        vector_column: String,
        hits: ScoredRowIds,
    ) -> Self {
        let snapshot_id = table.travel_snapshot.as_ref().map(|snapshot| snapshot.id());
        Self {
            table,
            snapshot_id,
            hits: SearchHits::DataEvolution {
                vector_column,
                hits,
            },
        }
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
        Ok(Self {
            table,
            snapshot_id: (snapshot_id != 0).then_some(snapshot_id),
            hits: SearchHits::PrimaryKey { positions, splits },
        })
    }

    /// Source snapshot, or `None` when the table had no resolved snapshot.
    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    /// The source table retained by the search; DE scans are pinned to this snapshot.
    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn len(&self) -> usize {
        match &self.hits {
            SearchHits::DataEvolution { hits, .. } => hits.len(),
            SearchHits::PrimaryKey { positions, .. } => positions.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Scored global row IDs, in relevance order. PK positions cannot be converted
    /// into this address space, including when a PK search has no hits.
    pub fn row_ids(&self) -> crate::Result<&ScoredRowIds> {
        match &self.hits {
            SearchHits::DataEvolution { hits, .. } => Ok(hits),
            SearchHits::PrimaryKey { .. } => Err(no_global_row_ids()),
        }
    }

    pub fn into_row_ids(self) -> crate::Result<ScoredRowIds> {
        match self.hits {
            SearchHits::DataEvolution { hits, .. } => Ok(hits),
            SearchHits::PrimaryKey { .. } => Err(no_global_row_ids()),
        }
    }

    /// Scored physical positions in relevance order, scoped to this result's snapshot.
    pub fn positions(&self) -> crate::Result<&[PrimaryKeySearchPosition]> {
        match &self.hits {
            SearchHits::PrimaryKey { positions, .. } => Ok(positions),
            SearchHits::DataEvolution { .. } => Err(crate::Error::Unsupported {
                message: "data-evolution search results use global row IDs, not primary-key file positions".to_string(),
            }),
        }
    }

    /// Snapshot-scoped file metadata and selections, reused when hybrid fusion
    /// builds its final selections without scanning the source table again.
    pub(super) fn indexed_splits(&self) -> crate::Result<&[PkVectorIndexedSplit]> {
        match &self.hits {
            SearchHits::PrimaryKey { splits, .. } => Ok(splits),
            SearchHits::DataEvolution { .. } => Err(crate::Error::Unsupported {
                message: "data-evolution search results do not carry primary-key indexed splits"
                    .to_string(),
            }),
        }
    }

    pub fn new_read_builder(&self) -> SearchResultReadBuilder<'_> {
        SearchResultReadBuilder {
            result: self,
            projection: None,
        }
    }
}

fn no_global_row_ids() -> crate::Error {
    crate::Error::Unsupported {
        message: "primary-key search results use physical file positions, not global row IDs"
            .to_string(),
    }
}

/// Reads selected user columns and `__paimon_search_score`, in relevance order.
/// Internal global row IDs and PK positions are not output. Reading a result
/// never reruns vector search or replans its primary-key source files.
#[derive(Debug, Clone)]
pub struct SearchResultReadBuilder<'a> {
    result: &'a SearchResult,
    projection: Option<Vec<String>>,
}

impl SearchResultReadBuilder<'_> {
    /// Select user columns in this order. Defaults to every user table column.
    pub fn with_projection(&mut self, columns: &[&str]) -> &mut Self {
        self.projection = Some(columns.iter().map(|name| name.to_string()).collect());
        self
    }

    pub async fn read(&self) -> crate::Result<ArrowRecordBatchStream> {
        let table = &self.result.table;
        let core_options = CoreOptions::new(table.schema().options());
        core_options.ensure_read_authorized()?;
        match &self.result.hits {
            SearchHits::DataEvolution {
                vector_column,
                hits,
            } => {
                let read_type =
                    DeVectorRead::read_type(table, vector_column, self.projection.as_deref())?;
                materialize_row_ids(table, hits, read_type).await
            }
            SearchHits::PrimaryKey { positions, splits } => {
                let read_type = resolve_materialize_read_type(table, self.projection.as_deref())?;
                let reader = DataFileReader::new(
                    table.file_io().clone(),
                    table.schema_manager().clone(),
                    table.schema().id(),
                    table.schema().fields().to_vec(),
                    read_type,
                    Vec::new(),
                )
                .with_table_options(table.schema().options().clone());
                materialize_positions(positions, splits, &reader).await
            }
        }
    }
}

#[cfg(test)]
mod tests;
