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

use std::cell::Cell;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{plan_datafusion_err, Result};
use datafusion::datasource::{TableProvider, TableType, ViewTable};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use paimon::catalog::Catalog;

tokio::task_local! {
    // Logical view expansion awaits table lookups in the current task. Tracking
    // the actual metadata reads also finds dependencies through nested views,
    // without treating ordinary temporary views as late-bound SQL definitions.
    static METADATA_READ: Cell<bool>;
    static VIEW_PATH: Vec<usize>;
}

pub(super) fn record_metadata_read() {
    let _ = METADATA_READ.try_with(|read| read.set(true));
}

pub(crate) async fn track_metadata_reads<T>(
    future: impl Future<Output = Result<T>>,
) -> Result<(T, bool)> {
    METADATA_READ
        .scope(Cell::new(false), async {
            let result = future.await?;
            Ok((result, METADATA_READ.with(Cell::get)))
        })
        .await
}

/// Stored only for SQL temporary views whose planning read partition metadata.
/// The schema provider resolves these back to ordinary ViewTables before SQL
/// planning, so optimizers and DML snapshot pinning still see the source scans.
pub(crate) struct MetadataView {
    query: String,
    catalog: String,
    database: String,
    normalize_identifiers: bool,
    schema: SchemaRef,
    catalogs: HashMap<String, Arc<dyn Catalog>>,
}

impl std::fmt::Debug for MetadataView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataView")
            .field("query", &self.query)
            .field("catalog", &self.catalog)
            .field("database", &self.database)
            .finish()
    }
}

impl MetadataView {
    pub(crate) fn new(
        plan: &LogicalPlan,
        query: String,
        state: &SessionState,
        catalogs: HashMap<String, Arc<dyn Catalog>>,
    ) -> Self {
        Self {
            query,
            catalog: state.config_options().catalog.default_catalog.clone(),
            database: state.config_options().catalog.default_schema.clone(),
            normalize_identifiers: state.config_options().sql_parser.enable_ident_normalization,
            schema: Arc::clone(plan.schema().inner()),
            catalogs,
        }
    }

    pub(crate) async fn resolve(&self, state: &SessionState) -> Result<ViewTable> {
        let identity = self as *const Self as usize;
        let mut path = VIEW_PATH.try_with(Clone::clone).unwrap_or_default();
        if path.contains(&identity) || path.len() >= 64 {
            return Err(plan_datafusion_err!(
                "recursive metadata-dependent temporary view or view expansion limit exceeded"
            ));
        }
        path.push(identity);
        VIEW_PATH
            .scope(path, async {
                let mut state = state.clone();
                let options = state.config_mut().options_mut();
                options.catalog.default_catalog = self.catalog.clone();
                options.catalog.default_schema = self.database.clone();
                options.sql_parser.enable_ident_normalization = self.normalize_identifiers;
                let query = crate::sql_function::expand_sql(
                    &state,
                    &self.query,
                    &self.catalogs,
                    &self.catalog,
                    &self.database,
                )
                .await?;
                let plan = crate::sql_projection::sql(
                    &datafusion::prelude::SessionContext::new_with_state(state),
                    &query,
                    datafusion::execution::context::SQLOptions::new(),
                )
                .await?
                .into_unoptimized_plan();
                if plan.schema().as_arrow() != self.schema.as_ref() {
                    return Err(plan_datafusion_err!(
                        "metadata-dependent temporary view schema changed since creation"
                    ));
                }
                Ok(ViewTable::new(plan, Some(self.query.clone())))
            })
            .await
    }
}

#[async_trait]
impl TableProvider for MetadataView {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn get_table_definition(&self) -> Option<&str> {
        Some(&self.query)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let state = state
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| {
                plan_datafusion_err!("temporary view requires a DataFusion SessionState")
            })?;
        self.resolve(state)
            .await?
            .scan(state, projection, filters, limit)
            .await
    }
}
