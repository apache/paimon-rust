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

//! `full_text_search` table-valued function for DataFusion.
//!
//! Usage:
//! ```sql
//! SELECT * FROM full_text_search('table_name', 'column_name', 'query text', 10)
//! ```
//!
//! Reference: [PaimonTableValuedFunctions.scala](https://github.com/apache/paimon/blob/master/paimon-spark/paimon-spark-common/src/main/scala/org/apache/paimon/spark/PaimonTableValuedFunctions.scala)

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use paimon::catalog::{Catalog, Identifier};

use crate::error::to_datafusion_error;
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::table::{build_paimon_scan, PaimonTableProvider};

/// Register the `full_text_search` table-valued function on a [`SessionContext`].
pub fn register_full_text_search(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    default_database: &str,
) {
    ctx.register_udtf(
        "full_text_search",
        Arc::new(FullTextSearchFunction::new(catalog, default_database)),
    );
}

/// Table function that performs full-text search on a Paimon table.
///
/// Arguments: `(table_name STRING, column_name STRING, query_text STRING, limit INT)`
pub struct FullTextSearchFunction {
    catalog: Arc<dyn Catalog>,
    default_database: String,
}

impl Debug for FullTextSearchFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FullTextSearchFunction")
            .field("default_database", &self.default_database)
            .finish()
    }
}

impl FullTextSearchFunction {
    pub fn new(catalog: Arc<dyn Catalog>, default_database: &str) -> Self {
        Self {
            catalog,
            default_database: default_database.to_string(),
        }
    }
}

impl TableFunctionImpl for FullTextSearchFunction {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 4 {
            return Err(datafusion::error::DataFusionError::Plan(
                "full_text_search requires 4 arguments: (table_name, column_name, query_text, limit)".to_string(),
            ));
        }

        let table_name = extract_string_literal(&args[0], "table_name")?;
        let column_name = extract_string_literal(&args[1], "column_name")?;
        let query_text = extract_string_literal(&args[2], "query_text")?;
        let limit = extract_int_literal(&args[3], "limit")?;

        if limit <= 0 {
            return Err(datafusion::error::DataFusionError::Plan(
                "full_text_search: limit must be positive".to_string(),
            ));
        }

        let identifier = parse_table_identifier(&table_name, &self.default_database)?;

        let catalog = Arc::clone(&self.catalog);
        let table = block_on_with_runtime(
            async move { catalog.get_table(&identifier).await },
            "full_text_search: catalog access thread panicked",
        )
        .map_err(to_datafusion_error)?;

        let inner = PaimonTableProvider::try_new(table)?;

        Ok(Arc::new(FullTextSearchTableProvider {
            inner,
            column_name,
            query_text,
            limit: limit as usize,
        }))
    }
}

/// A wrapper around [`PaimonTableProvider`] that injects full-text search
/// row filtering into the scan path.
#[derive(Debug)]
struct FullTextSearchTableProvider {
    inner: PaimonTableProvider,
    column_name: String,
    query_text: String,
    limit: usize,
}

#[async_trait]
impl TableProvider for FullTextSearchTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

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

        // Use FullTextSearchBuilder to execute the search.
        let row_ranges = await_with_runtime(async {
            let mut builder = table.new_full_text_search_builder();
            builder
                .with_text_column(&self.column_name)
                .with_query_text(&self.query_text)
                .with_limit(self.limit);
            builder.execute().await.map_err(to_datafusion_error)
        })
        .await?;

        // Convert search results to row ranges and inject into the scan.
        let mut read_builder = table.new_read_builder();
        if let Some(limit) = limit {
            read_builder.with_limit(limit);
        }
        let scan = if row_ranges.is_empty() {
            read_builder.new_scan()
        } else {
            read_builder.new_scan().with_row_ranges(row_ranges)
        };
        let plan = await_with_runtime(scan.plan())
            .await
            .map_err(to_datafusion_error)?;

        let target = state.config_options().execution.target_partitions;
        build_paimon_scan(
            table,
            &self.schema(),
            &plan,
            projection,
            None,
            limit,
            target,
        )
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

fn extract_string_literal(expr: &Expr, name: &str) -> DFResult<String> {
    match expr {
        Expr::Literal(scalar, _) => {
            let s = scalar.try_as_str().flatten().ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "full_text_search: {name} must be a string literal, got: {expr}"
                ))
            })?;
            Ok(s.to_string())
        }
        _ => Err(datafusion::error::DataFusionError::Plan(format!(
            "full_text_search: {name} must be a literal, got: {expr}"
        ))),
    }
}

fn extract_int_literal(expr: &Expr, name: &str) -> DFResult<i64> {
    use datafusion::common::ScalarValue;
    match expr {
        Expr::Literal(scalar, _) => match scalar {
            ScalarValue::Int8(Some(v)) => Ok(*v as i64),
            ScalarValue::Int16(Some(v)) => Ok(*v as i64),
            ScalarValue::Int32(Some(v)) => Ok(*v as i64),
            ScalarValue::Int64(Some(v)) => Ok(*v),
            ScalarValue::UInt8(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt16(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt32(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt64(Some(v)) => i64::try_from(*v).map_err(|_| {
                datafusion::error::DataFusionError::Plan(format!(
                    "full_text_search: {name} value {v} exceeds i64 range"
                ))
            }),
            _ => Err(datafusion::error::DataFusionError::Plan(format!(
                "full_text_search: {name} must be an integer literal, got: {expr}"
            ))),
        },
        _ => Err(datafusion::error::DataFusionError::Plan(format!(
            "full_text_search: {name} must be a literal, got: {expr}"
        ))),
    }
}

fn parse_table_identifier(name: &str, default_database: &str) -> DFResult<Identifier> {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.len() {
        1 => Ok(Identifier::new(default_database, parts[0])),
        2 => Ok(Identifier::new(parts[0], parts[1])),
        // 3-part name: catalog.database.table — ignore catalog prefix
        3 => Ok(Identifier::new(parts[1], parts[2])),
        _ => Err(datafusion::error::DataFusionError::Plan(format!(
            "full_text_search: invalid table name '{name}', expected 'table', 'database.table', or 'catalog.database.table'"
        ))),
    }
}
