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

//! Vector index execution contract and the shared scan -> plan -> read pipeline.

use crate::spec::{CoreOptions, Predicate};
use crate::table::de_vector_read::DeVectorRead;
use crate::table::de_vector_scan::PreparedVectorSearchFilter;
use crate::table::pk_vector_read::PkVectorRead;
use crate::table::pk_vector_search_params::PkVectorSearchParams;
use crate::table::vector_scan::{PlanContext, VectorScanPlan, VectorScanWork};
use crate::table::vector_search_common::{take_only_result, targets_primary_key_column};
use crate::table::Table;
use crate::vector_search::SearchResult;
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

/// Execute a resolved plan without resolving another snapshot or manifest.
///
/// Both implementations return snapshot-scoped search results. The associated
/// plan type prevents passing a DE plan to a PK reader or vice versa.
pub(super) trait Read: Sync {
    type Plan;

    fn read(
        &self,
        plan: Self::Plan,
    ) -> impl Future<Output = crate::Result<Vec<SearchResult>>> + Send;
}

/// Searches a common plan with one query, without replanning or materializing rows.
pub struct VectorRead {
    pub(super) batch: BatchVectorRead,
}

impl VectorRead {
    pub async fn read(&self, plan: VectorScanPlan) -> crate::Result<SearchResult> {
        take_only_result(self.batch.read(plan).await?, "vector search")
    }
}

/// Searches a common plan with multiple queries, preserving input order and arity.
/// Owns its configuration so it can outlive the builder that created it.
pub struct BatchVectorRead {
    context: PlanContext,
    reader: VectorReadKind,
}

enum VectorReadKind {
    DataEvolution(DeVectorRead),
    PrimaryKey(PkVectorRead),
}

impl BatchVectorRead {
    pub(super) fn new(
        table: &Table,
        column: &str,
        queries: &[&[f32]],
        limit: usize,
        options: &HashMap<String, String>,
        filter: Option<&Predicate>,
        include_row_ids: Option<&Arc<RoaringTreemap>>,
        prepared: Option<&PreparedVectorSearchFilter>,
    ) -> crate::Result<Self> {
        let context = PlanContext::new(table, column, filter, include_row_ids, prepared)?;
        let core = CoreOptions::new(table.schema().options());
        let reader = if targets_primary_key_column(&core, column) {
            let pk_col = core.primary_key_vector_index_column()?;
            let params =
                PkVectorSearchParams::resolve(table, options, filter, &pk_col, queries, limit)?;
            VectorReadKind::PrimaryKey(PkVectorRead::new(
                table, options, filter, &pk_col, queries, limit, params,
            ))
        } else {
            VectorReadKind::DataEvolution(DeVectorRead::new(column, queries, limit, options)?)
        };
        Ok(Self { context, reader })
    }

    pub async fn read(&self, plan: VectorScanPlan) -> crate::Result<Vec<SearchResult>> {
        if self.context != plan.context {
            return Err(crate::Error::DataInvalid {
                message: "vector plan and reader must use the same table, column and pre-filter"
                    .to_string(),
                source: None,
            });
        }
        match (&self.reader, plan.work) {
            (VectorReadKind::DataEvolution(reader), VectorScanWork::DataEvolution(plan)) => {
                reader.read(plan).await
            }
            (VectorReadKind::PrimaryKey(reader), VectorScanWork::PrimaryKey(plan)) => {
                reader.read(plan).await
            }
            _ => Err(crate::Error::DataInvalid {
                message: "vector plan and reader use different DE/PK address spaces".to_string(),
                source: None,
            }),
        }
    }
}
