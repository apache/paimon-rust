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

//! Vector search planning contract, shared by DE and primary-key scans.

use std::future::Future;
use std::sync::Arc;

use roaring::RoaringTreemap;

use crate::spec::{CoreOptions, Predicate};
use crate::table::de_vector_scan::{DeVectorScan, DeVectorScanPlan, PreparedVectorSearchFilter};
use crate::table::pk_vector_scan::{PkVectorScan, PkVectorScanPlan};
use crate::table::vector_search_common::targets_primary_key_column;
use crate::table::{find_field_id_by_name, BucketVectorSearchSplit, Table};

/// Resolve one snapshot and the work a vector reader will consume.
///
/// The associated plan keeps global row IDs and physical bucket positions in
/// separate types. A reader cannot accidentally consume another route's plan.
pub(super) trait Scan: Sync {
    type Plan;

    fn plan(&self) -> impl Future<Output = crate::Result<Self::Plan>> + Send;
}

/// Snapshot and search work shared by the single-query and batch readers.
/// Plans own their source context and remain usable after the scan is dropped.
#[derive(Clone)]
pub struct VectorScanPlan {
    pub(super) context: PlanContext,
    pub(super) work: VectorScanWork,
}

#[derive(Clone)]
pub(super) enum VectorScanWork {
    DataEvolution(DeVectorScanPlan),
    PrimaryKey(PkVectorScanPlan),
}

impl VectorScanPlan {
    pub fn snapshot_id(&self) -> Option<i64> {
        match &self.work {
            VectorScanWork::DataEvolution(plan) => plan.table.travel_snapshot().map(|s| s.id()),
            VectorScanWork::PrimaryKey(plan) => (plan.snapshot_id != 0).then_some(plan.snapshot_id),
        }
    }
}

/// A plan must use the reader's table, column and pre-filter. Query vectors,
/// limits and index search options may differ when a plan is reused.
#[derive(Clone, PartialEq)]
pub(super) struct PlanContext {
    location: String,
    branch: String,
    column: String,
    filter: Option<Predicate>,
    include_row_ids: Option<Arc<RoaringTreemap>>,
}

impl PlanContext {
    pub(super) fn new(
        table: &Table,
        column: &str,
        filter: Option<&Predicate>,
        include_row_ids: Option<&Arc<RoaringTreemap>>,
        prepared: Option<&PreparedVectorSearchFilter>,
    ) -> crate::Result<Self> {
        let core = CoreOptions::new(table.schema().options());
        core.ensure_read_authorized()?;
        if let Some(prepared) = prepared {
            if table.location().trim_end_matches('/')
                != prepared.table().location().trim_end_matches('/')
                || table.branch() != prepared.table().branch()
            {
                return Err(crate::Error::DataInvalid {
                    message: "Prepared vector search filter belongs to a different table"
                        .to_string(),
                    source: None,
                });
            }
            CoreOptions::new(prepared.table().schema().options()).ensure_read_authorized()?;
        }
        if column.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Vector column must be set via with_vector_column()".to_string(),
            });
        }
        if targets_primary_key_column(&core, column)
            && (include_row_ids.is_some() || prepared.is_some())
        {
            return Err(crate::Error::DataInvalid {
                message: "global row-ID filters cannot be applied to primary-key file positions; use with_filter()".to_string(), source: None,
            });
        }
        Ok(Self {
            location: table.location().trim_end_matches('/').to_string(),
            branch: table.branch().to_string(),
            column: column.to_string(),
            filter: filter.cloned(),
            include_row_ids: prepared
                .map(PreparedVectorSearchFilter::include_row_ids)
                .or(include_row_ids)
                .cloned(),
        })
    }
}

/// Creates query-independent plans for DE or primary-key vector search.
pub struct VectorScan {
    context: PlanContext,
    scan: VectorScanKind,
}

enum VectorScanKind {
    DataEvolution(DeVectorScan),
    PrimaryKey(PkVectorScan),
}

impl VectorScan {
    pub(super) fn new(
        table: &Table,
        column: &str,
        filter: Option<&Predicate>,
        include_row_ids: Option<&Arc<RoaringTreemap>>,
        prepared: Option<&PreparedVectorSearchFilter>,
    ) -> crate::Result<Self> {
        let context = PlanContext::new(table, column, filter, include_row_ids, prepared)?;
        let core = CoreOptions::new(table.schema().options());
        let scan = if targets_primary_key_column(&core, column) {
            let column = core.primary_key_vector_index_column()?;
            let field_id =
                find_field_id_by_name(table.schema().fields(), &column).ok_or_else(|| {
                    crate::Error::DataInvalid {
                        message: format!("PK-vector column '{column}' not found in schema"),
                        source: None,
                    }
                })?;
            VectorScanKind::PrimaryKey(PkVectorScan::new(
                table,
                field_id,
                core.primary_key_vector_index_type(&column)?,
                filter.cloned(),
            ))
        } else {
            VectorScanKind::DataEvolution(DeVectorScan::new(
                table,
                filter,
                include_row_ids,
                prepared,
            ))
        };
        Ok(Self { context, scan })
    }

    pub async fn plan(&self) -> crate::Result<VectorScanPlan> {
        let work = match &self.scan {
            VectorScanKind::DataEvolution(scan) => {
                VectorScanWork::DataEvolution(scan.plan().await?)
            }
            VectorScanKind::PrimaryKey(scan) => VectorScanWork::PrimaryKey(scan.plan().await?),
        };
        Ok(VectorScanPlan {
            context: self.context.clone(),
            work,
        })
    }

    /// Adapt already-decoded Java PK bucket work into a common read plan.
    /// The supplied snapshot, files and physical row ranges are authoritative;
    /// this does not consult the table's snapshot or index manifests.
    pub fn plan_from_bucket_splits(
        &self,
        splits: Vec<BucketVectorSearchSplit>,
    ) -> crate::Result<VectorScanPlan> {
        let VectorScanKind::PrimaryKey(scan) = &self.scan else {
            return Err(crate::Error::DataInvalid {
                message: "bucket splits require a primary-key vector scan".to_string(),
                source: None,
            });
        };
        Ok(VectorScanPlan {
            context: self.context.clone(),
            work: VectorScanWork::PrimaryKey(scan.plan_for_bucket_vector_splits(splits)?),
        })
    }
}
