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

//! Mirrors Java [AuditLogTable](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/system/AuditLogTable.java).

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use paimon::spec::DataField;
use paimon::table::{AuditLogTable as PaimonAuditLogTable, Table};

use crate::error::to_datafusion_error;
use crate::filter_pushdown::{analyze_filters, classify_filter_pushdown};
use crate::runtime::await_with_runtime;
use crate::table::{datafusion_arrow_schema, PaimonScanBuilder};

pub(super) fn build(table: Table) -> DFResult<Arc<dyn TableProvider>> {
    let fields = PaimonAuditLogTable::new(table.clone())
        .fields()
        .map_err(to_datafusion_error)?;
    let schema = datafusion_arrow_schema(&fields, true)?;
    Ok(Arc::new(AuditLogTable {
        table,
        fields,
        schema,
    }))
}

#[derive(Debug)]
struct AuditLogTable {
    table: Table,
    fields: Vec<DataField>,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for AuditLogTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let filter_analysis = analyze_filters(filters, self.table.schema().fields(), true);
        let pushed_limit = limit.filter(|_| !filter_analysis.requires_residual);
        let mut read_builder = self.table.new_read_builder();
        if let Some(indices) = projection {
            read_builder.with_read_type(
                indices
                    .iter()
                    .map(|&index| self.fields[index].clone())
                    .filter(|field| {
                        !matches!(
                            field.id(),
                            paimon::spec::ROW_KIND_FIELD_ID
                                | paimon::spec::SEQUENCE_NUMBER_FIELD_ID
                        )
                    })
                    .collect(),
            );
        }
        if let Some(predicate) = filter_analysis.pushed_predicate.clone() {
            read_builder.with_filter(predicate);
        }
        if let Some(limit) = pushed_limit {
            read_builder.with_limit(limit);
        }
        let (plan, trace) = await_with_runtime(read_builder.new_audit_scan().plan_with_trace())
            .await
            .map_err(to_datafusion_error)?;

        PaimonScanBuilder {
            table: &self.table,
            schema: &self.schema,
            plan,
            scan_trace: Some(trace),
            projection,
            pushed_predicate: filter_analysis.pushed_predicate,
            limit: pushed_limit,
            target_partitions: state.config_options().execution.target_partitions,
            filter_exact: false,
            case_sensitive: true,
        }
        .build_audit_log(self.fields.clone())
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        let read_builder = self.table.new_read_builder();
        Ok(filters
            .iter()
            .map(|filter| {
                classify_filter_pushdown(filter, self.table.schema().fields(), true, |predicate| {
                    read_builder.is_exact_filter_pushdown(predicate)
                })
            })
            .collect())
    }
}
