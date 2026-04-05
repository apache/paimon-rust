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
use paimon::spec::Predicate;
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
    /// Filter translated from DataFusion expressions and reused during execute()
    /// so reader-side pruning reaches the actual read path.
    pushed_predicate: Option<Predicate>,
    /// Pre-planned partition assignments: `planned_partitions[i]` contains the
    /// Paimon splits that DataFusion partition `i` will read.
    /// Wrapped in `Arc` to avoid deep-cloning `DataSplit` metadata in `execute()`.
    planned_partitions: Vec<Arc<[DataSplit]>>,
    plan_properties: PlanProperties,
    /// Optional limit on the number of rows to return.
    limit: Option<usize>,
}

impl PaimonTableScan {
    pub(crate) fn new(
        schema: ArrowSchemaRef,
        table: Table,
        projected_columns: Option<Vec<String>>,
        pushed_predicate: Option<Predicate>,
        planned_partitions: Vec<Arc<[DataSplit]>>,
        limit: Option<usize>,
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
            pushed_predicate,
            planned_partitions,
            plan_properties,
            limit,
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    #[cfg(test)]
    pub(crate) fn planned_partitions(&self) -> &[Arc<[DataSplit]>] {
        &self.planned_partitions
    }

    #[cfg(test)]
    pub(crate) fn pushed_predicate(&self) -> Option<&Predicate> {
        self.pushed_predicate.as_ref()
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
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
        let pushed_predicate = self.pushed_predicate.clone();

        let fut = async move {
            let mut read_builder = table.new_read_builder();

            if let Some(ref columns) = projected_columns {
                let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                read_builder.with_projection(&col_refs);
            }
            if let Some(filter) = pushed_predicate {
                read_builder.with_filter(filter);
            }

            let read = read_builder.new_read().map_err(to_datafusion_error)?;
            let stream = read.to_arrow(&splits).map_err(to_datafusion_error)?;
            let stream = stream.map(|r| r.map_err(to_datafusion_error));

            Ok::<_, datafusion::error::DataFusionError>(RecordBatchStreamAdapter::new(
                schema,
                Box::pin(stream),
            ))
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(fut).try_flatten(),
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
        )?;
        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    mod test_utils {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../test_utils.rs"));
    }

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;
    use paimon::catalog::Identifier;
    use paimon::io::FileIOBuilder;
    use paimon::spec::{
        BinaryRow, DataType, Datum, IntType, PredicateBuilder, Schema as PaimonSchema, TableSchema,
    };
    use std::fs;
    use tempfile::tempdir;
    use test_utils::{local_file_path, test_data_file, write_int_parquet_file};

    fn test_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]))
    }

    #[test]
    fn test_partition_count_empty_plan() {
        let schema = test_schema();
        let scan = PaimonTableScan::new(
            schema,
            dummy_table(),
            None,
            None,
            vec![Arc::from(Vec::new())],
            None,
        );
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
        let scan =
            PaimonTableScan::new(schema, dummy_table(), None, None, planned_partitions, None);
        assert_eq!(scan.properties().output_partitioning().partition_count(), 3);
    }

    /// Constructs a minimal Table for testing (no real files needed since we
    /// only test PlanProperties, not actual reads).
    fn dummy_table() -> Table {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let schema = PaimonSchema::builder().build().unwrap();
        let table_schema = TableSchema::new(0, &schema);
        Table::new(
            file_io,
            Identifier::new("test_db", "test_table"),
            "/tmp/test-table".to_string(),
            table_schema,
        )
    }

    #[tokio::test]
    async fn test_execute_applies_pushed_filter_during_read() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        write_int_parquet_file(
            &bucket_dir.join("data.parquet"),
            vec![("id", vec![1, 2, 3, 4]), ("value", vec![5, 20, 30, 40])],
            Some(2),
        );

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &paimon::spec::Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            table_path,
            table_schema,
        );

        let split = paimon::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 4)])
            .with_raw_convertible(true)
            .build()
            .unwrap();

        let pushed_predicate = PredicateBuilder::new(table.schema().fields())
            .greater_or_equal("value", Datum::Int(10))
            .unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let scan = PaimonTableScan::new(
            schema,
            table,
            Some(vec!["id".to_string()]),
            Some(pushed_predicate),
            vec![Arc::from(vec![split])],
            None,
        );

        let ctx = SessionContext::new();
        let stream = scan
            .execute(0, ctx.task_ctx())
            .expect("execute should succeed");
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();

        let actual_ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id column should be Int32Array");
                (0..ids.len()).map(|idx| ids.value(idx)).collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(actual_ids, vec![2, 3, 4]);
    }
}
