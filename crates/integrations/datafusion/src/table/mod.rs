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

//! Paimon table provider for DataFusion (read-only).

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::table::Table;

use crate::physical_plan::PaimonTableScan;
use crate::schema::paimon_schema_to_arrow;

/// Read-only table provider for a Paimon table.
///
/// Supports full table scan only (no write, no subset/reordered projection, no predicate
/// pushdown).
#[derive(Debug, Clone)]
pub struct PaimonTableProvider {
    table: Table,
    schema: ArrowSchemaRef,
}

impl PaimonTableProvider {
    /// Create a table provider from a Paimon table.
    ///
    /// Loads the table schema and converts it to Arrow for DataFusion.
    pub fn try_new(table: Table) -> DFResult<Self> {
        let fields = table.schema().fields();
        let schema = paimon_schema_to_arrow(fields)?;
        Ok(Self { table, schema })
    }

    pub fn table(&self) -> &Table {
        &self.table
    }
}

#[async_trait]
impl TableProvider for PaimonTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        if let Some(projection) = projection {
            let is_full_schema_projection = projection.len() == self.schema.fields().len()
                && projection.iter().copied().eq(0..self.schema.fields().len());

            if !is_full_schema_projection {
                return Err(DataFusionError::NotImplemented(
                    "Paimon DataFusion integration does not yet support subset or reordered projections; use SELECT * until apache/paimon-rust#146 is implemented".to_string(),
                ));
            }
        }

        Ok(Arc::new(PaimonTableScan::new(
            self.schema.clone(),
            self.table.clone(),
        )))
    }
}
