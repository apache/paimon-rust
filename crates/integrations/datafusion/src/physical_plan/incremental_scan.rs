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

//! Physical plan for batch incremental reads via paimon-core
//! [`IncrementalScan`](paimon::table::IncrementalScan) /
//! [`AuditLogTable`](paimon::table::AuditLogTable).

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use futures::{StreamExt, TryStreamExt};
use paimon::table::{AuditLogTable, IncrementalPlan, Table};

use crate::error::to_datafusion_error;

pub(crate) const PLAN_NAME: &str = "PaimonIncrementalScan";

/// Execution plan that streams incremental snapshot-range rows from a Paimon table.
///
/// Planning of snapshot ranges is performed eagerly in
/// [`crate::incremental_query::IncrementalQueryFunction`]; this plan only executes
/// the precomputed [`IncrementalPlan`].
#[derive(Debug)]
pub(crate) struct PaimonIncrementalScan {
    table: Table,
    incremental_plan: IncrementalPlan,
    audit_log: bool,
    projected_columns: Option<Vec<String>>,
    schema: ArrowSchemaRef,
    plan_properties: Arc<PlanProperties>,
}

impl PaimonIncrementalScan {
    pub(crate) fn new(
        schema: ArrowSchemaRef,
        table: Table,
        incremental_plan: IncrementalPlan,
        audit_log: bool,
        projected_columns: Option<Vec<String>>,
    ) -> Self {
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            table,
            incremental_plan,
            audit_log,
            projected_columns,
            schema,
            plan_properties,
        }
    }
}

impl ExecutionPlan for PaimonIncrementalScan {
    fn name(&self) -> &str {
        PLAN_NAME
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let table = self.table.clone();
        let plan = self.incremental_plan.clone();
        let audit_log = self.audit_log;
        let projected_columns = self.projected_columns.clone();
        let schema = self.schema.clone();

        let fut = async move {
            let stream = if audit_log {
                // AuditLogTable always materialises the full audit schema
                // (rowkind + table fields). Column projection is applied below.
                AuditLogTable::new(table)
                    .to_arrow(&plan)
                    .map_err(to_datafusion_error)?
            } else {
                let mut read_builder = table.new_read_builder();
                if let Some(ref columns) = projected_columns {
                    let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                    read_builder
                        .with_projection(&col_refs)
                        .map_err(to_datafusion_error)?;
                }
                let read = read_builder.new_read().map_err(to_datafusion_error)?;
                read.to_incremental_arrow(&plan)
                    .map_err(to_datafusion_error)?
            };

            let stream = stream.map(move |result| {
                let batch = result.map_err(to_datafusion_error)?;
                if audit_log {
                    if let Some(ref columns) = projected_columns {
                        let indices = columns
                            .iter()
                            .map(|name| {
                                batch.schema().index_of(name).map_err(|e| {
                                    datafusion::error::DataFusionError::Internal(format!(
                                        "PaimonIncrementalScan: projection column '{name}': {e}"
                                    ))
                                })
                            })
                            .collect::<DFResult<Vec<_>>>()?;
                        return batch.project(&indices).map_err(|e| {
                            datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                        });
                    }
                }
                Ok(batch)
            });

            Ok::<_, datafusion::error::DataFusionError>(RecordBatchStreamAdapter::new(
                schema,
                Box::pin(stream),
            ))
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::once(fut).try_flatten(),
        )))
    }
}

impl DisplayAs for PaimonIncrementalScan {
    fn fmt_as(
        &self,
        _format: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "PaimonIncrementalScan(audit_log={}, splits={})",
            self.audit_log,
            self.incremental_plan.splits().len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incremental_scan_plan_name_constant() {
        assert_eq!(PLAN_NAME, "PaimonIncrementalScan");
    }
}
