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

//! `paimon_incremental_query` table-valued function for DataFusion.
//!
//! Usage:
//! ```sql
//! SELECT * FROM paimon_incremental_query('table_name', start_snapshot_exclusive, end_snapshot_inclusive);
//! SELECT * FROM paimon_incremental_query('table_name$audit_log', 0, 2, 'diff');
//! ```
//!
//! This module only parses SQL arguments and adapts the resulting
//! [`IncrementalPlan`](paimon::table::IncrementalPlan) into a DataFusion
//! execution plan. Snapshot planning lives in paimon-core
//! (`IncrementalScan` / `AuditLogTable`).

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::project_schema;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use paimon::catalog::Catalog;
use paimon::table::{AuditLogTable, IncrementalScanMode, Table};

use crate::error::to_datafusion_error;
use crate::physical_plan::PaimonIncrementalScan;
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::table::PaimonTableProvider;
use crate::table_function_args::{
    extract_snapshot_bound_literal, extract_string_literal, parse_incremental_scan_mode,
    parse_incremental_table_ref,
};

const FUNCTION_NAME: &str = "paimon_incremental_query";

#[derive(Clone)]
struct CatalogBinding {
    catalog: Arc<dyn Catalog>,
    default_database: String,
}

#[derive(Clone, Default)]
pub(crate) struct IncrementalQueryCatalogs {
    catalogs: Arc<RwLock<HashMap<String, CatalogBinding>>>,
    current_catalog: Arc<RwLock<Option<String>>>,
    current_database: Arc<RwLock<Option<String>>>,
}

impl IncrementalQueryCatalogs {
    pub(crate) fn register(&self, name: String, catalog: Arc<dyn Catalog>, default_database: &str) {
        self.catalogs.write().unwrap().insert(
            name,
            CatalogBinding {
                catalog,
                default_database: default_database.to_string(),
            },
        );
    }

    pub(crate) fn set_current(&self, name: String) {
        *self.current_catalog.write().unwrap() = Some(name);
    }

    pub(crate) fn set_current_database(&self, name: String) {
        *self.current_database.write().unwrap() = Some(name);
    }

    fn resolve(&self, name: Option<&str>) -> DFResult<CatalogBinding> {
        let name = match name {
            Some(name) => name.to_string(),
            None => self
                .current_catalog
                .read()
                .unwrap()
                .clone()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(
                        "paimon_incremental_query: no current catalog is configured".to_string(),
                    )
                })?,
        };
        let mut binding = self
            .catalogs
            .read()
            .unwrap()
            .get(&name)
            .cloned()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "paimon_incremental_query: unknown catalog '{name}'"
                ))
            })?;
        if let Some(current_database) = self.current_database.read().unwrap().as_ref() {
            binding.default_database.clone_from(current_database);
        }
        Ok(binding)
    }
}

/// Register the `paimon_incremental_query` table-valued function on a [`SessionContext`].
pub fn register_incremental_query(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    default_database: &str,
) {
    ctx.register_udtf(
        FUNCTION_NAME,
        Arc::new(IncrementalQueryFunction::new(catalog, default_database)),
    );
}

pub(crate) fn register_incremental_query_catalogs(
    ctx: &SessionContext,
    catalogs: IncrementalQueryCatalogs,
) {
    ctx.register_udtf(
        FUNCTION_NAME,
        Arc::new(IncrementalQueryFunction::with_catalogs(catalogs)),
    );
}

/// Table function that performs batch incremental reads between snapshot IDs.
///
/// Arguments:
/// `(table_name STRING, start_snapshot_exclusive BIGINT, end_snapshot_inclusive BIGINT [, scan_mode STRING])`
pub struct IncrementalQueryFunction {
    source: CatalogSource,
}

#[derive(Clone)]
enum CatalogSource {
    Fixed(CatalogBinding),
    Registered(IncrementalQueryCatalogs),
}

impl Debug for IncrementalQueryFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalQueryFunction").finish()
    }
}

impl IncrementalQueryFunction {
    pub fn new(catalog: Arc<dyn Catalog>, default_database: &str) -> Self {
        Self {
            source: CatalogSource::Fixed(CatalogBinding {
                catalog,
                default_database: default_database.to_string(),
            }),
        }
    }

    fn with_catalogs(catalogs: IncrementalQueryCatalogs) -> Self {
        Self {
            source: CatalogSource::Registered(catalogs),
        }
    }

    fn resolve_catalog(&self, name: Option<&str>) -> DFResult<CatalogBinding> {
        match &self.source {
            CatalogSource::Fixed(binding) => Ok(binding.clone()),
            CatalogSource::Registered(catalogs) => catalogs.resolve(name),
        }
    }
}

impl TableFunctionImpl for IncrementalQueryFunction {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 3 && args.len() != 4 {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "{FUNCTION_NAME} requires 3 or 4 arguments: \
                 (table_name, start_snapshot_exclusive, end_snapshot_inclusive [, scan_mode])"
            )));
        }

        let table_name = extract_string_literal(FUNCTION_NAME, &args[0], "table_name")?;
        let start_exclusive =
            extract_snapshot_bound_literal(FUNCTION_NAME, &args[1], "start_snapshot_exclusive")?;
        let end_inclusive =
            extract_snapshot_bound_literal(FUNCTION_NAME, &args[2], "end_snapshot_inclusive")?;
        let scan_mode = if args.len() == 4 {
            parse_incremental_scan_mode(FUNCTION_NAME, &args[3])?
        } else {
            IncrementalScanMode::Auto
        };

        let table_ref = parse_incremental_table_ref(FUNCTION_NAME, &table_name)?;
        let binding = self.resolve_catalog(table_ref.catalog.as_deref())?;

        let catalog = Arc::clone(&binding.catalog);
        let identifier = table_ref.identifier(&binding.default_database);
        let table = block_on_with_runtime(
            async move { catalog.get_table(&identifier).await },
            "paimon_incremental_query: catalog access thread panicked",
        )
        .map_err(to_datafusion_error)?;

        let schema = if table_ref.audit_log {
            let fields = AuditLogTable::new(table.clone())
                .fields()
                .map_err(to_datafusion_error)?;
            paimon::arrow::build_target_arrow_schema(&fields).map_err(to_datafusion_error)?
        } else {
            PaimonTableProvider::try_new(table.clone())?.schema()
        };

        Ok(Arc::new(IncrementalQueryTableProvider {
            table,
            schema,
            start_exclusive,
            end_inclusive,
            scan_mode,
            audit_log: table_ref.audit_log,
        }))
    }
}

#[derive(Debug)]
struct IncrementalQueryTableProvider {
    table: Table,
    schema: ArrowSchemaRef,
    start_exclusive: i64,
    end_inclusive: i64,
    scan_mode: IncrementalScanMode,
    audit_log: bool,
}

#[async_trait]
impl TableProvider for IncrementalQueryTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table = self.table.clone();
        let start_exclusive = self.start_exclusive;
        let end_inclusive = self.end_inclusive;
        let scan_mode = self.scan_mode;

        // Planning is delegated entirely to paimon-core IncrementalScan.
        let incremental_plan = await_with_runtime(async move {
            table
                .new_read_builder()
                .new_incremental_scan(scan_mode, start_exclusive, end_inclusive)
                .plan()
                .await
                .map_err(to_datafusion_error)
        })
        .await?;

        let projected_schema = project_schema(&self.schema, projection)?;
        let projected_columns = projection.map(|indices| {
            indices
                .iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect::<Vec<_>>()
        });

        Ok(Arc::new(PaimonIncrementalScan::new(
            projected_schema,
            self.table.clone(),
            incremental_plan,
            self.audit_log,
            projected_columns,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Residual filter evaluation is performed by DataFusion above this plan.
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}
