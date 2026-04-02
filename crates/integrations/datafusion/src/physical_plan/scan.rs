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
use paimon::DataSplit;

use crate::error::to_datafusion_error;

/// Execution plan that scans a Paimon table with optional column projection.
///
/// Planning is performed eagerly in [`super::super::table::PaimonTableProvider::scan`],
/// and the resulting splits are distributed across DataFusion execution partitions
/// so that DataFusion can schedule them in parallel.
#[derive(Debug)]
pub struct PaimonTableScan {
    table: Table,
    /// Projected column names (if None, reads all columns).
    projected_columns: Option<Vec<String>>,
    /// Pre-planned partition assignments: `planned_partitions[i]` contains the
    /// Paimon splits that DataFusion partition `i` will read.
    /// Wrapped in `Arc` to avoid deep-cloning `DataSplit` metadata in `execute()`.
    planned_partitions: Vec<Arc<[DataSplit]>>,
    plan_properties: PlanProperties,
}

impl PaimonTableScan {
    pub(crate) fn new(
        schema: ArrowSchemaRef,
        table: Table,
        projected_columns: Option<Vec<String>>,
        planned_partitions: Vec<Arc<[DataSplit]>>,
    ) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(planned_partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            table,
            projected_columns,
            planned_partitions,
            plan_properties,
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    #[cfg(test)]
    pub(crate) fn planned_partitions(&self) -> &[Arc<[DataSplit]>] {
        &self.planned_partitions
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
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let splits = Arc::clone(self.planned_partitions.get(partition).ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(format!(
                "PaimonTableScan: partition index {partition} out of range (total {})",
                self.planned_partitions.len()
            ))
        })?);

        let table = self.table.clone();
        let schema = self.schema();
        let projected_columns = self.projected_columns.clone();

        let fut = async move {
            let mut read_builder = table.new_read_builder();

            if let Some(ref columns) = projected_columns {
                let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                read_builder.with_projection(&col_refs);
            }

            let read = read_builder.new_read().map_err(to_datafusion_error)?;
            let stream = read.to_arrow(&splits).map_err(to_datafusion_error)?;
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
        write!(
            f,
            "PaimonTableScan: partitions={}",
            self.planned_partitions.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use datafusion::physical_plan::ExecutionPlan;

    fn test_schema() -> ArrowSchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]))
    }

    #[test]
    fn test_partition_count_empty_plan() {
        let schema = test_schema();
        let scan = PaimonTableScan::new(schema, dummy_table(), None, vec![Arc::from(Vec::new())]);
        assert_eq!(scan.properties().output_partitioning().partition_count(), 1);
    }

    #[test]
    fn test_partition_count_multiple_partitions() {
        let schema = test_schema();
        let planned_partitions = vec![
            Arc::from(Vec::new()),
            Arc::from(Vec::new()),
            Arc::from(Vec::new()),
        ];
        let scan = PaimonTableScan::new(schema, dummy_table(), None, planned_partitions);
        assert_eq!(scan.properties().output_partitioning().partition_count(), 3);
    }

    /// Constructs a minimal Table for testing (no real files needed since we
    /// only test PlanProperties, not actual reads).
    fn dummy_table() -> Table {
        use paimon::catalog::Identifier;
        use paimon::io::FileIOBuilder;
        use paimon::spec::{Schema, TableSchema};

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let schema = Schema::builder().build().unwrap();
        let table_schema = TableSchema::new(0, &schema);
        Table::new(
            file_io,
            Identifier::new("test_db", "test_table"),
            "/tmp/test-table".to_string(),
            table_schema,
        )
    }
}
