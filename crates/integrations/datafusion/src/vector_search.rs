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

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::project_schema;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use paimon::catalog::Catalog;

use crate::error::to_datafusion_error;
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::table::{PaimonScanBuilder, PaimonTableProvider};
use crate::table_function_args::{
    extract_int_literal, extract_string_literal, parse_table_identifier,
};

const FUNCTION_NAME: &str = "vector_search";

pub fn register_vector_search(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    default_database: &str,
) {
    ctx.register_udtf(
        "vector_search",
        Arc::new(VectorSearchFunction::new(catalog, default_database)),
    );
}

pub struct VectorSearchFunction {
    catalog: Arc<dyn Catalog>,
    default_database: String,
}

impl Debug for VectorSearchFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorSearchFunction")
            .field("default_database", &self.default_database)
            .finish()
    }
}

impl VectorSearchFunction {
    pub fn new(catalog: Arc<dyn Catalog>, default_database: &str) -> Self {
        Self {
            catalog,
            default_database: default_database.to_string(),
        }
    }
}

impl TableFunctionImpl for VectorSearchFunction {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 4 {
            return Err(datafusion::error::DataFusionError::Plan(
                "vector_search requires 4 arguments: (table_name, column_name, query_vector_json, limit)".to_string(),
            ));
        }

        let table_name = extract_string_literal(FUNCTION_NAME, &args[0], "table_name")?;
        let column_name = extract_string_literal(FUNCTION_NAME, &args[1], "column_name")?;
        let limit = extract_int_literal(FUNCTION_NAME, &args[3], "limit")?;

        if limit <= 0 {
            return Err(DataFusionError::Plan(
                "vector_search: limit must be positive".to_string(),
            ));
        }

        let identifier =
            parse_table_identifier(FUNCTION_NAME, &table_name, &self.default_database)?;

        let catalog = Arc::clone(&self.catalog);
        let table = block_on_with_runtime(
            async move { catalog.get_table(&identifier).await },
            "vector_search: catalog access thread panicked",
        )
        .map_err(to_datafusion_error)?;

        let inner = PaimonTableProvider::try_new(table)?;
        let query_vector_json =
            match extract_string_literal(FUNCTION_NAME, &args[2], "query_vector_json") {
                Ok(value) => value,
                Err(_) if matches!(args[2], Expr::Column(_)) => {
                    return Ok(Arc::new(LateralVectorSearchTableProvider {
                        inner,
                        column_name,
                        query_vector_expr: args[2].clone(),
                        limit: limit as usize,
                    }));
                }
                Err(err) => return Err(err),
            };

        let query_vector: Vec<f32> = serde_json::from_str(&query_vector_json).map_err(|e| {
            DataFusionError::Plan(format!(
                "vector_search: query_vector_json must be a JSON array of floats, got '{}': {}",
                query_vector_json, e
            ))
        })?;

        if query_vector.is_empty() {
            return Err(DataFusionError::Plan(
                "vector_search: query vector cannot be empty".to_string(),
            ));
        }

        Ok(Arc::new(VectorSearchTableProvider {
            inner,
            column_name,
            query_vector,
            limit: limit as usize,
        }))
    }
}

#[derive(Debug)]
pub(crate) struct LateralVectorSearchTableProvider {
    inner: PaimonTableProvider,
    column_name: String,
    query_vector_expr: Expr,
    limit: usize,
}

impl LateralVectorSearchTableProvider {
    pub(crate) fn inner(&self) -> &PaimonTableProvider {
        &self.inner
    }

    pub(crate) fn column_name(&self) -> &str {
        &self.column_name
    }

    pub(crate) fn query_vector_expr(&self) -> &Expr {
        &self.query_vector_expr
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit
    }
}

#[async_trait]
impl TableProvider for LateralVectorSearchTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "lateral vector_search must be planned through a lateral join".to_string(),
        ))
    }
}

#[derive(Debug)]
struct VectorSearchTableProvider {
    inner: PaimonTableProvider,
    column_name: String,
    query_vector: Vec<f32>,
    limit: usize,
}

#[async_trait]
impl TableProvider for VectorSearchTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table = self.inner.table();

        let row_ranges = await_with_runtime(async {
            let mut builder = table.new_vector_search_builder();
            builder
                .with_vector_column(&self.column_name)
                .with_query_vector(self.query_vector.clone())
                .with_limit(self.limit);
            builder.execute().await.map_err(to_datafusion_error)
        })
        .await?;

        if row_ranges.is_empty() {
            let schema = project_schema(&self.schema(), projection)?;
            return Ok(Arc::new(EmptyExec::new(schema)));
        }

        let mut read_builder = table.new_read_builder();
        if let Some(limit) = limit {
            read_builder.with_limit(limit);
        }
        let scan = read_builder.new_scan().with_row_ranges(row_ranges);
        let plan = await_with_runtime(scan.plan())
            .await
            .map_err(to_datafusion_error)?;

        let target = state.config_options().execution.target_partitions;
        PaimonScanBuilder {
            table,
            schema: &self.schema(),
            plan: &plan,
            scan_trace: None,
            projection,
            pushed_predicate: None,
            limit,
            target_partitions: target,
            filter_exact: false,
        }
        .build()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}
