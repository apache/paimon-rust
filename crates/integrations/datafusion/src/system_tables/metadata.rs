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

//! DataFusion adaptation of core read-only system tables.

use crate::error::to_datafusion_error;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use paimon::table::{SystemRead, Table};
use std::sync::Arc;

pub(super) fn build(table: Table, name: &str) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(MetadataTableProvider {
        read: table.new_system_read(name).map_err(to_datafusion_error)?,
    }))
}

#[derive(Debug)]
pub(super) struct MetadataTableProvider {
    read: SystemRead,
}

#[async_trait]
impl TableProvider for MetadataTableProvider {
    fn schema(&self) -> SchemaRef {
        self.read.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut read = self.read.clone();
        if let Some(indices) = projection {
            let schema = self.read.schema();
            let columns: Vec<_> = indices
                .iter()
                .map(|&index| schema.field(index).name().as_str())
                .collect();
            read.with_projection(&columns)
                .map_err(to_datafusion_error)?;
        }
        if let Some(predicate) = crate::filter_pushdown::analyze_filters(
            filters,
            self.read.full_row_type().fields(),
            true,
        )
        .pushed_predicate
        {
            read.with_filter(predicate).map_err(to_datafusion_error)?;
        }
        let schema = read.schema();
        let batch = crate::runtime::await_with_runtime(read.read())
            .await
            .map_err(to_datafusion_error)?;
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            None,
        )?)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let fields = self.read.full_row_type();
        Ok(filters
            .iter()
            .map(|filter| {
                if crate::filter_pushdown::analyze_filters(
                    &[(*filter).clone()],
                    fields.fields(),
                    true,
                )
                .pushed_predicate
                .is_some()
                {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}
