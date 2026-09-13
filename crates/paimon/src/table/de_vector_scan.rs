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

//! Plans global-index vector searches against one snapshot and resolves scalar pre-filters.

use crate::spec::{CoreOptions, IndexManifest, IndexManifestEntry, Predicate, ROW_ID_FIELD_NAME};
use crate::table::vector_scan::Scan;
use crate::table::Table;
use crate::vindex::vector_search_timing_enabled;
use arrow_array::{Array, Int64Array};
use futures::TryStreamExt;
use roaring::RoaringTreemap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A scalar vector pre-filter resolved once against one pinned snapshot.
///
/// Reusing this value avoids repeating the same scalar-index/table read for
/// every input batch of a lateral vector query.
#[derive(Debug, Clone)]
pub struct PreparedVectorSearchFilter {
    table: Table,
    include_row_ids: Arc<RoaringTreemap>,
}

impl PreparedVectorSearchFilter {
    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn include_row_ids(&self) -> &Arc<RoaringTreemap> {
        &self.include_row_ids
    }
}

fn same_vector_search_table(left: &Table, right: &Table) -> bool {
    left.location().trim_end_matches('/') == right.location().trim_end_matches('/')
        && left.branch() == right.branch()
}

async fn matching_row_ids_for_filter(
    table: &Table,
    filter: &Predicate,
) -> crate::Result<RoaringTreemap> {
    let mut read_builder = table.new_read_builder();
    read_builder
        .with_projection(&[ROW_ID_FIELD_NAME])?
        .with_filter(filter.clone());
    let plan = read_builder.new_scan().plan().await?;
    let read = read_builder.new_read()?;
    let mut stream = read.to_arrow(plan.splits())?;
    let mut row_ids = RoaringTreemap::new();
    while let Some(batch) = stream.try_next().await? {
        let index =
            batch
                .schema()
                .index_of(ROW_ID_FIELD_NAME)
                .map_err(|_| crate::Error::DataInvalid {
                    message: format!(
                        "scalar vector pre-filter read is missing {ROW_ID_FIELD_NAME}"
                    ),
                    source: None,
                })?;
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "scalar vector pre-filter {ROW_ID_FIELD_NAME} column is not Int64"
                ),
                source: None,
            })?;
        for row in 0..values.len() {
            if values.is_null(row) {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "scalar vector pre-filter produced a null {ROW_ID_FIELD_NAME}"
                    ),
                    source: None,
                });
            }
            let row_id = values.value(row);
            let row_id = u64::try_from(row_id).map_err(|_| crate::Error::DataInvalid {
                message: format!(
                    "scalar vector pre-filter produced a negative {ROW_ID_FIELD_NAME}: {row_id}"
                ),
                source: None,
            })?;
            row_ids.insert(row_id);
        }
    }
    Ok(row_ids)
}

impl Table {
    /// Resolve a scalar predicate once and pin all later vector-search/read
    /// stages to the same snapshot.
    pub async fn prepare_vector_search_filter(
        &self,
        filter: Predicate,
    ) -> crate::Result<PreparedVectorSearchFilter> {
        CoreOptions::new(self.schema().options()).ensure_read_authorized()?;
        let Some(snapshot) = crate::table::time_travel::resolve_snapshot(self).await? else {
            return Ok(PreparedVectorSearchFilter {
                table: self.clone(),
                include_row_ids: Arc::new(RoaringTreemap::new()),
            });
        };
        let table = self.copy_with_resolved_snapshot(&snapshot).await?;
        let include_row_ids = matching_row_ids_for_filter(&table, &filter).await?;
        Ok(PreparedVectorSearchFilter {
            table,
            include_row_ids: Arc::new(include_row_ids),
        })
    }
}

pub(super) struct DeVectorScan {
    table: Table,
    filter: Option<Predicate>,
    include_row_ids: Option<Arc<RoaringTreemap>>,
    prepared_filter: Option<PreparedVectorSearchFilter>,
}

impl DeVectorScan {
    pub(super) fn new(
        table: &Table,
        filter: Option<&Predicate>,
        include_row_ids: Option<&Arc<RoaringTreemap>>,
        prepared_filter: Option<&PreparedVectorSearchFilter>,
    ) -> Self {
        Self {
            table: table.clone(),
            filter: filter.cloned(),
            include_row_ids: include_row_ids.cloned(),
            prepared_filter: prepared_filter.cloned(),
        }
    }
}

/// One pinned snapshot, its index manifest and resolved row-ID filter.
/// Query vectors and search options belong to the reader, not to this plan.
#[derive(Clone)]
pub(super) struct DeVectorScanPlan {
    pub(super) table: Table,
    pub(super) index_entries: Vec<IndexManifestEntry>,
    pub(super) include_row_ids: Option<Arc<RoaringTreemap>>,
    pub(super) timing: Option<DeVectorScanTiming>,
    pub(super) next_row_id: Option<i64>,
    pub(super) skip_search: bool,
}

impl DeVectorScanPlan {
    fn empty(table: Table, timing: Option<DeVectorScanTiming>) -> Self {
        Self {
            table,
            skip_search: true,
            index_entries: Vec::new(),
            include_row_ids: None,
            next_row_id: None,
            timing,
        }
    }
}

#[derive(Clone)]
pub(super) struct DeVectorScanTiming {
    pub(super) total_start: Instant,
    pub(super) setup: Duration,
    pub(super) snapshot: Duration,
    pub(super) manifest: Duration,
}

impl Scan for DeVectorScan {
    type Plan = DeVectorScanPlan;
    async fn plan(&self) -> crate::Result<DeVectorScanPlan> {
        let timing_enabled = vector_search_timing_enabled();
        let total_start = timing_enabled.then(Instant::now);
        // The builder target is authoritative for current auth/type policy.
        // A prepared filter only pins a snapshot and may carry older options.
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        if let Some(prepared) = self.prepared_filter.as_ref() {
            if !same_vector_search_table(&self.table, prepared.table()) {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Prepared vector search filter belongs to a different table: builder target is '{}@{}', prepared filter target is '{}@{}'",
                        self.table.location(),
                        self.table.branch(),
                        prepared.table().location(),
                        prepared.table().branch(),
                    ),
                    source: None,
                });
            }
        }
        // Check the pinned execution view as defense in depth before any fast
        // path returns data-derived row ids/scores outside TableScan/TableRead.
        let execution_table = self
            .prepared_filter
            .as_ref()
            .map(PreparedVectorSearchFilter::table)
            .unwrap_or(&self.table);
        let core = CoreOptions::new(execution_table.schema().options());
        core.ensure_read_authorized()?;
        let mut plan = DeVectorScanPlan::empty(
            execution_table.clone(),
            total_start.map(|total_start| DeVectorScanTiming {
                total_start,
                setup: total_start.elapsed(),
                snapshot: Duration::ZERO,
                manifest: Duration::ZERO,
            }),
        );

        if self
            .prepared_filter
            .as_ref()
            .is_some_and(|prepared| prepared.include_row_ids().is_empty())
        {
            return Ok(plan);
        }

        let snapshot_manager = execution_table.snapshot_manager();
        let snapshot_start = timing_enabled.then(Instant::now);
        let snapshot = crate::table::time_travel::resolve_snapshot(execution_table).await?;
        if let Some(timing) = &mut plan.timing {
            timing.snapshot = snapshot_start.map_or(Duration::ZERO, |start| start.elapsed());
        }
        let Some(snapshot) = snapshot else {
            return Ok(plan);
        };
        plan.table = match self.prepared_filter.as_ref() {
            Some(prepared) => prepared.table().clone(),
            None => {
                execution_table
                    .copy_with_resolved_snapshot(&snapshot)
                    .await?
            }
        };
        plan.next_row_id = snapshot.next_row_id();

        plan.include_row_ids = if let Some(prepared) = self.prepared_filter.as_ref() {
            Some(Arc::clone(prepared.include_row_ids()))
        } else if let Some(include_row_ids) = self.include_row_ids.as_ref() {
            Some(Arc::clone(include_row_ids))
        } else if let Some(filter) = self.filter.as_ref() {
            Some(Arc::new(
                matching_row_ids_for_filter(&plan.table, filter).await?,
            ))
        } else {
            None
        };
        if plan
            .include_row_ids
            .as_ref()
            .is_some_and(|ids| ids.is_empty())
        {
            return Ok(plan);
        }

        let manifest_start = timing_enabled.then(Instant::now);
        plan.index_entries = match snapshot.index_manifest() {
            Some(index_manifest_name) => {
                let manifest_path = snapshot_manager.manifest_path(index_manifest_name);
                IndexManifest::read(execution_table.file_io(), &manifest_path).await?
            }
            None => Vec::new(),
        };
        if let Some(timing) = &mut plan.timing {
            timing.manifest = manifest_start.map_or(Duration::ZERO, |start| start.elapsed());
        }
        plan.skip_search = false;
        Ok(plan)
    }
}

#[cfg(test)]
mod tests;
