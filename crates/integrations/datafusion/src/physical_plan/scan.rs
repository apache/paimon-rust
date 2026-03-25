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

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use futures::{StreamExt, TryStreamExt};
use paimon::table::Table;

use crate::error::to_datafusion_error;

/// Execution plan that scans a Paimon table (read-only, no projection, no predicate, no limit).
#[derive(Debug)]
pub struct PaimonTableScan {
    table: Table,
    plan_properties: PlanProperties,
}

impl PaimonTableScan {
    pub(crate) fn new(schema: ArrowSchemaRef, table: Table) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            // TODO: Currently all Paimon splits are read in a single DataFusion partition,
            // which means we lose DataFusion parallelism. A follow-up should expose one
            // execution partition per Paimon split so that DataFusion can schedule them
            // across threads.
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            table,
            plan_properties,
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }
}

impl ExecutionPlan for PaimonTableScan {
    fn name(&self) -> &str {
        "PaimonTableScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
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
        let schema = self.schema();

        let fut = async move {
            let read_builder = table.new_read_builder();
            let scan = read_builder.new_scan();
            let plan = scan.plan().await.map_err(to_datafusion_error)?;
            let read = read_builder.new_read().map_err(to_datafusion_error)?;
            let stream = read.to_arrow(plan.splits()).map_err(to_datafusion_error)?;
            let stream = stream.map(|r| r.map_err(to_datafusion_error));

            Ok::<_, datafusion::error::DataFusionError>(RecordBatchStreamAdapter::new(
                schema,
                Box::pin(stream),
            ))
        };

        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for PaimonTableScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "PaimonTableScan")
    }
}
