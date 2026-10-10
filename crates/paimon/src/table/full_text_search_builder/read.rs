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
        let query = if builder.query_is_dsl {
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

    pub async fn read(&self, plan: FullTextScanPlan) -> crate::Result<SearchResult> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        CoreOptions::new(plan.table.schema().options()).ensure_read_authorized()?;
        if self.context != plan.context {
            return Err(crate::Error::DataInvalid {
                message: "Full-text plan belongs to a different table, column or filter".into(),
                source: None,
            });
        }
        if plan.snapshot_id.is_none() {
            return Ok(SearchResult::empty());
        }
        let mut search = self.search.clone();
        search.include_row_ids = self.context.include_row_ids.clone();
        evaluate_full_text_search(
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
        )
        .await
    }
}
