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
use crate::table::pk_vector_read::PkVectorRead;
use crate::table::pk_vector_search_params::PkVectorSearchParams;
use crate::table::vector_scan::{PlanContext, VectorScanPlan};
use crate::table::vector_search_common::{take_only_result, targets_primary_key_column};
use crate::table::{SearchTaskExecutor, Table};
use crate::vector_search::GlobalIndexResult;
use std::collections::HashMap;
use std::future::Future;

/// Execute a source plan without resolving another snapshot or discovering
/// additional search sources.
///
/// Both implementations return snapshot-scoped search results. The associated
/// plan type prevents passing a DE plan to a PK reader or vice versa.
pub(super) trait Read: Sync {
    type Plan;

    fn read(
        &self,
        plan: Self::Plan,
    ) -> impl Future<Output = crate::Result<Vec<GlobalIndexResult>>> + Send;
}

/// Searches a common plan with one query, without replanning or materializing rows.
pub struct VectorRead {
    pub(super) batch: BatchVectorRead,
}

impl VectorRead {
    pub async fn read(&self, plan: VectorScanPlan) -> crate::Result<GlobalIndexResult> {
        take_only_result(self.batch.read(plan).await?, "vector search")
    }
    pub(crate) async fn read_with_executor(
        &self,
        plan: VectorScanPlan,
        executor: &dyn SearchTaskExecutor,
    ) -> crate::Result<GlobalIndexResult> {
        take_only_result(
            self.batch.read_with_executor(plan, executor).await?,
            "vector search",
        )
    }
}

/// Searches a common plan with multiple queries, preserving input order and arity.
/// Owns its configuration so it can outlive the builder that created it.
pub struct BatchVectorRead {
    context: PlanContext,
    table: Table,
    reader: VectorReadKind,
}

enum VectorReadKind {
    DataEvolution(DeVectorRead),
    PrimaryKey(Box<PkVectorRead>),
}

impl BatchVectorRead {
    pub(super) fn new(
        table: &Table,
        column: &str,
        queries: &[&[f32]],
        limit: usize,
        options: &HashMap<String, String>,
        filter: Option<&Predicate>,
        context: PlanContext,
    ) -> crate::Result<Self> {
        let core = CoreOptions::new(table.schema().options());
        let reader = if targets_primary_key_column(&core, column) {
            let pk_col = core.primary_key_vector_index_column()?;
            let params =
                PkVectorSearchParams::resolve(table, options, filter, &pk_col, queries, limit)?;
            VectorReadKind::PrimaryKey(Box::new(PkVectorRead::new(
                table, options, filter, &pk_col, queries, limit, params,
            )))
        } else {
            VectorReadKind::DataEvolution(DeVectorRead::new(column, queries, limit, options)?)
        };
        Ok(Self {
            context,
            table: table.clone(),
            reader,
        })
    }

    pub async fn read(&self, plan: VectorScanPlan) -> crate::Result<Vec<GlobalIndexResult>> {
        self.read_impl(plan, None).await
    }
    pub(crate) async fn read_with_executor(
        &self,
        plan: VectorScanPlan,
        executor: &dyn SearchTaskExecutor,
    ) -> crate::Result<Vec<GlobalIndexResult>> {
        self.read_impl(plan, Some(executor)).await
    }
    async fn read_impl(
        &self,
        plan: VectorScanPlan,
        executor: Option<&dyn SearchTaskExecutor>,
    ) -> crate::Result<Vec<GlobalIndexResult>> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        if let Some(snapshot) = &self.context.prepared_snapshot {
            if snapshot.as_ref() != plan.snapshot() {
                return Err(crate::Error::DataInvalid {
                    message:
                        "Prepared vector filter and source plan must use the same captured snapshot"
                            .into(),
                    source: None,
                });
            }
        }
        let table = self.table.copy_with_pinned_snapshot(plan.snapshot());
        match &self.reader {
            VectorReadKind::DataEvolution(reader) => {
                for source in plan.splits() {
                    if let crate::table::SearchScanSplit::Index { index_files, .. } = source {
                        let field_id = crate::table::find_field_id_by_name(
                            table.schema().fields(),
                            &self.context.column,
                        );
                        if index_files.iter().any(|file| {
                            super::vector_search_common::VectorIndexBackend::from_index_type(
                                &file.index_type,
                            )
                            .is_none()
                                || file
                                    .global_index_meta
                                    .as_ref()
                                    .map(|meta| meta.index_field_id)
                                    != field_id
                        }) {
                            return Err(crate::Error::DataInvalid {
                                message:
                                    "Vector source index does not match the reader's vector column"
                                        .into(),
                                source: None,
                            });
                        }
                    }
                }
                let index_entries =
                    super::search_scan_split::global_entries_from_splits(plan.splits())?;
                let work = super::de_vector_scan::DeVectorScanPlan {
                    next_row_id: plan.snapshot().and_then(|snapshot| snapshot.next_row_id()),
                    skip_search: plan.snapshot().is_none() || plan.splits().is_empty(),
                    table,
                    index_entries,
                    filter: self.context.filter.clone(),
                    include_row_ids: self.context.include_row_ids.clone(),
                    timing: None,
                    source_splits: Some(plan.splits().to_vec()),
                };
                reader.read_with_executor(work, executor).await
            }
            VectorReadKind::PrimaryKey(reader) => {
                if executor.is_some() {
                    return Err(crate::Error::Unsupported {
                        message: "Distributed primary-key vector search is not supported".into(),
                    });
                }
                reader
                    .read(super::pk_vector_scan::plan_from_search_splits(
                        &table,
                        &self.context.column,
                        self.context.filter.as_ref(),
                        plan.snapshot().cloned(),
                        plan.splits(),
                    )?)
                    .await
            }
        }
    }
}
