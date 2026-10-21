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

//! Executes full-text queries over a snapshot-scoped plan.

use super::scan::PlanContext;
use super::*;

/// An owned reader can execute queries over plans from the same table, column
/// and filter. Changing a query or limit does not require another scan.
pub struct FullTextRead {
    table: Table,
    context: PlanContext,
    search: FullTextSearch,
}

impl FullTextRead {
    pub(super) fn new(builder: &FullTextSearchBuilder<'_>) -> crate::Result<Self> {
        CoreOptions::new(builder.table.schema().options()).ensure_read_authorized()?;
        let limit = builder.limit.filter(|limit| *limit > 0).ok_or_else(|| {
            crate::Error::ConfigInvalid {
                message: "Limit must be positive, set via with_limit()".into(),
            }
        })?;
        let context = PlanContext::new(builder)?;
        let query = builder
            .query_text
            .as_deref()
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: "Query text must be set via with_query() or with_query_text()".into(),
            })?;
        if context.primary_key_field_id.is_some() {
            if builder
                .table
                .schema()
                .core_options()
                .full_text_index_search_mode()?
                != GlobalIndexSearchMode::Fast
            {
                return Err(crate::Error::DataInvalid { message: "primary-key full-text search supports only the FAST global-index search mode".into(), source: None });
            }
            if let Some(filter) = &context.filter {
                let (_, data) = super::super::bucket_filter::split_partition_and_data_predicates(
                    filter.clone(),
                    builder.table.schema().fields(),
                    builder.table.schema().partition_keys(),
                );
                if !data.is_empty() {
                    return Err(crate::Error::Unsupported {
                        message:
                            "Primary-key full-text search does not support non-partition filters"
                                .into(),
                    });
                }
            }
        }
        let query = if builder.query_is_dsl || context.primary_key_field_id.is_some() {
            query.to_string()
        } else {
            normalize_query_text(query, &context.column)?
        };
        let search = FullTextSearch::new(query, limit, context.column.clone())?;
        Ok(Self {
            table: builder.table.clone(),
            context,
            search,
        })
    }

    pub async fn read(
        &self,
        plan: FullTextScanPlan,
    ) -> crate::Result<crate::table::GlobalIndexResult> {
        self.read_internal(plan, None).await
    }

    /// Execute shard work through a scheduler while retaining the plan's
    /// snapshot, filtering, and final global ranking in core.
    pub async fn read_with_executor(
        &self,
        plan: FullTextScanPlan,
        executor: &dyn crate::table::SearchTaskExecutor,
    ) -> crate::Result<crate::table::GlobalIndexResult> {
        self.read_internal(plan, Some(executor)).await
    }

    async fn read_internal(
        &self,
        plan: FullTextScanPlan,
        executor: Option<&dyn crate::table::SearchTaskExecutor>,
    ) -> crate::Result<crate::table::GlobalIndexResult> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        CoreOptions::new(plan.table.schema().options()).ensure_read_authorized()?;
        if self.context != plan.context {
            return Err(crate::Error::DataInvalid {
                message: "Full-text plan belongs to a different table, column or filter".into(),
                source: None,
            });
        }
        if executor.is_some() && !self.table.schema().core_options().data_evolution_enabled() {
            return Err(crate::Error::Unsupported {
                message: "Scheduled full-text search requires a data-evolution table".into(),
            });
        }
        let table = std::sync::Arc::new(plan.table.clone());
        if self.context.primary_key_field_id.is_some() {
            let empty = super::super::pk_full_text_scan::PrimaryKeyFullTextScanPlan {
                snapshot_id: 0,
                splits: Vec::new(),
            };
            let source = plan.primary_key_plan.as_ref().unwrap_or(&empty);
            let materialize = DataFileReader::new(
                plan.table.file_io().clone(),
                plan.table.schema_manager().clone(),
                plan.table.schema().id(),
                plan.table.schema().fields().to_vec(),
                plan.table.data_fields_extractor(),
                plan.table.schema().fields().to_vec(),
                Vec::new(),
            )
            .with_table_options(plan.table.schema().options().clone());
            let read = PrimaryKeyFullTextRead::new(
                plan.table.file_io().clone(),
                materialize,
                plan.table.location().trim_end_matches('/').to_string(),
                plan.table
                    .schema()
                    .core_options()
                    .index_file_in_data_file_dir(),
            );
            let result = read
                .search_route(source, &self.search.query_text, self.search.limit)
                .await?;
            return crate::table::GlobalIndexResult::from_full_text_primary_key(
                table,
                result.snapshot_id,
                result.candidates,
                result.splits,
            );
        }
        let mut search = self.search.clone();
        search.include_row_ids = self.context.include_row_ids.clone();
        let hits = evaluate_full_text_search_with_executor(
            FullTextSearchEvaluation {
                table: Some(&plan.table),
                file_io: plan.table.file_io(),
                table_path: plan.table.location(),
                table_options: self.table.schema().options(),
                schema_fields: self.table.schema().fields(),
                next_row_id: plan.next_row_id,
                partition_filter: plan.partition_filter.as_ref(),
                row_filter: plan.row_filter.as_ref(),
            },
            &plan.entries,
            &search,
            executor,
        )
        .await?;
        Ok(crate::table::GlobalIndexResult::from_global_search(
            table,
            crate::vector_search::ScoredRowIds::new(hits.row_ids, hits.scores),
        ))
    }
}
