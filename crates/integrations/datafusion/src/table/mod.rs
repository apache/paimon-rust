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

//! Paimon table provider for DataFusion.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef as ArrowSchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use paimon::table::Table;

use crate::physical_plan::PaimonDataSink;

use crate::error::to_datafusion_error;
use crate::filter_pushdown::{build_pushed_predicate, classify_filter_pushdown};
use crate::physical_plan::PaimonTableScan;
use crate::runtime::await_with_runtime;

/// Read-only table provider for a Paimon table.
///
/// Supports full table scan, column projection, and predicate pushdown for
/// planning. Partition predicates prune splits eagerly, while supported
/// non-partition data predicates may also be reused by the Parquet read path
/// for row-group pruning and partial decode-time filtering.
///
/// DataFusion still treats pushed filters as inexact because unsupported
/// predicates and non-Parquet reads remain residual filters.
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
        let mut fields = table.schema().fields().to_vec();
        let core_options = paimon::spec::CoreOptions::new(table.schema().options());
        if core_options.data_evolution_enabled() {
            fields.push(paimon::spec::DataField::new(
                paimon::spec::ROW_ID_FIELD_ID,
                paimon::spec::ROW_ID_FIELD_NAME.to_string(),
                paimon::spec::DataType::BigInt(paimon::spec::BigIntType::with_nullable(true)),
            ));
        }
        let schema =
            paimon::arrow::build_target_arrow_schema(&fields).map_err(to_datafusion_error)?;
        Ok(Self { table, schema })
    }

    pub fn table(&self) -> &Table {
        &self.table
    }
}

/// Distribute `items` into `num_buckets` groups using round-robin assignment.
pub(crate) fn bucket_round_robin<T>(items: Vec<T>, num_buckets: usize) -> Vec<Vec<T>> {
    let mut buckets: Vec<Vec<T>> = (0..num_buckets).map(|_| Vec::new()).collect();
    for (i, item) in items.into_iter().enumerate() {
        buckets[i % num_buckets].push(item);
    }
    buckets
}

/// Build a [`PaimonTableScan`] from a planned [`paimon::table::Plan`].
///
/// Shared by [`PaimonTableProvider`] and the full-text search UDTF to avoid
/// duplicating projection, partition distribution, and scan construction.
pub(crate) fn build_paimon_scan(
    table: &Table,
    schema: &ArrowSchemaRef,
    plan: &paimon::table::Plan,
    projection: Option<&Vec<usize>>,
    pushed_predicate: Option<paimon::spec::Predicate>,
    limit: Option<usize>,
    target_partitions: usize,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let (projected_schema, projected_columns) = if let Some(indices) = projection {
        let fields: Vec<Field> = indices.iter().map(|&i| schema.field(i).clone()).collect();
        let column_names: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();
        (Arc::new(Schema::new(fields)), Some(column_names))
    } else {
        let column_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        (schema.clone(), Some(column_names))
    };

    let splits = plan.splits().to_vec();
    let planned_partitions: Vec<Arc<[_]>> = if splits.is_empty() {
        vec![Arc::from(Vec::new())]
    } else {
        let num_partitions = splits.len().min(target_partitions.max(1));
        bucket_round_robin(splits, num_partitions)
            .into_iter()
            .map(Arc::from)
            .collect()
    };

    Ok(Arc::new(PaimonTableScan::new(
        projected_schema,
        table.clone(),
        projected_columns,
        pushed_predicate,
        planned_partitions,
        limit,
    )))
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Plan splits eagerly so we know partition count upfront.
        let pushed_predicate = build_pushed_predicate(filters, self.table.schema().fields());
        let mut read_builder = self.table.new_read_builder();
        if let Some(filter) = pushed_predicate.clone() {
            read_builder.with_filter(filter);
        }
        // Push the limit hint to paimon-core planning to reduce splits when possible.
        // DataFusion still enforces the final LIMIT semantics.
        if let Some(limit) = limit {
            read_builder.with_limit(limit);
        }
        let scan = read_builder.new_scan();
        // DataFusion's Python FFI may poll `TableProvider::scan()` without an active
        // Tokio runtime. `scan.plan()` can reach OpenDAL/Tokio filesystem calls while
        // reading Paimon metadata, so we must provide a runtime here instead of
        // assuming the caller already entered one.
        let plan = await_with_runtime(scan.plan())
            .await
            .map_err(to_datafusion_error)?;

        let target = state.config_options().execution.target_partitions;
        build_paimon_scan(
            &self.table,
            &self.schema,
            &plan,
            projection,
            pushed_predicate,
            limit,
            target,
        )
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let overwrite = match insert_op {
            InsertOp::Append => false,
            InsertOp::Overwrite => true,
            other => {
                return Err(datafusion::error::DataFusionError::NotImplemented(format!(
                    "{other} is not supported for Paimon tables"
                )));
            }
        };
        let sink = PaimonDataSink::new(self.table.clone(), self.schema.clone(), overwrite);
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        let fields = self.table.schema().fields();
        let partition_keys = self.table.schema().partition_keys();

        Ok(filters
            .iter()
            .map(|filter| classify_filter_pushdown(filter, fields, partition_keys))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::{col, lit, Expr};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use paimon::catalog::Identifier;
    use paimon::{Catalog, CatalogOptions, DataSplit, FileSystemCatalog, Options};

    use crate::physical_plan::PaimonTableScan;

    #[test]
    fn test_bucket_round_robin_distributes_evenly() {
        let result = bucket_round_robin(vec![0, 1, 2, 3, 4], 3);
        assert_eq!(result, vec![vec![0, 3], vec![1, 4], vec![2]]);
    }

    #[test]
    fn test_bucket_round_robin_fewer_items_than_buckets() {
        let result = bucket_round_robin(vec![10, 20], 2);
        assert_eq!(result, vec![vec![10], vec![20]]);
    }

    #[test]
    fn test_bucket_round_robin_single_bucket() {
        let result = bucket_round_robin(vec![1, 2, 3], 1);
        assert_eq!(result, vec![vec![1, 2, 3]]);
    }

    fn get_test_warehouse() -> String {
        std::env::var("PAIMON_TEST_WAREHOUSE")
            .unwrap_or_else(|_| "/tmp/paimon-warehouse".to_string())
    }

    fn create_catalog() -> FileSystemCatalog {
        let warehouse = get_test_warehouse();
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse);
        FileSystemCatalog::new(options).expect("Failed to create catalog")
    }

    async fn create_provider(table_name: &str) -> PaimonTableProvider {
        let catalog = create_catalog();
        let identifier = Identifier::new("default", table_name);
        let table = catalog
            .get_table(&identifier)
            .await
            .expect("Failed to get table");

        PaimonTableProvider::try_new(table).expect("Failed to create table provider")
    }

    async fn plan_partitions(
        provider: &PaimonTableProvider,
        filters: Vec<Expr>,
    ) -> Vec<Arc<[DataSplit]>> {
        let config = SessionConfig::new().with_target_partitions(8);
        let ctx = SessionContext::new_with_config(config);
        let state = ctx.state();
        let plan = provider
            .scan(&state, None, &filters, None)
            .await
            .expect("scan() should succeed");
        let scan = plan
            .as_any()
            .downcast_ref::<PaimonTableScan>()
            .expect("Expected PaimonTableScan");

        scan.planned_partitions().to_vec()
    }

    fn extract_dt_partition_set(planned_partitions: &[Arc<[DataSplit]>]) -> BTreeSet<String> {
        planned_partitions
            .iter()
            .flat_map(|splits| splits.iter())
            .map(|split| {
                split
                    .partition()
                    .get_string(0)
                    .expect("Failed to decode dt")
                    .to_string()
            })
            .collect()
    }

    fn extract_dt_hr_partition_set(
        planned_partitions: &[Arc<[DataSplit]>],
    ) -> BTreeSet<(String, i32)> {
        planned_partitions
            .iter()
            .flat_map(|splits| splits.iter())
            .map(|split| {
                let partition = split.partition();
                (
                    partition
                        .get_string(0)
                        .expect("Failed to decode dt")
                        .to_string(),
                    partition.get_int(1).expect("Failed to decode hr"),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn test_scan_partition_filter_plans_matching_partition_set() {
        let provider = create_provider("partitioned_log_table").await;
        let planned_partitions =
            plan_partitions(&provider, vec![col("dt").eq(lit("2024-01-01"))]).await;

        assert_eq!(
            extract_dt_partition_set(&planned_partitions),
            BTreeSet::from(["2024-01-01".to_string()]),
        );
    }

    #[tokio::test]
    async fn test_scan_mixed_and_filter_keeps_partition_pruning() {
        let provider = create_provider("partitioned_log_table").await;
        let planned_partitions = plan_partitions(
            &provider,
            vec![col("dt").eq(lit("2024-01-01")).and(col("id").gt(lit(1)))],
        )
        .await;

        assert_eq!(
            extract_dt_partition_set(&planned_partitions),
            BTreeSet::from(["2024-01-01".to_string()]),
        );
    }

    #[tokio::test]
    async fn test_scan_multi_partition_filter_plans_exact_partition_set() {
        let provider = create_provider("multi_partitioned_log_table").await;

        let dt_only_partitions =
            plan_partitions(&provider, vec![col("dt").eq(lit("2024-01-01"))]).await;
        let dt_hr_partitions = plan_partitions(
            &provider,
            vec![col("dt").eq(lit("2024-01-01")).and(col("hr").eq(lit(10)))],
        )
        .await;

        assert_eq!(
            extract_dt_hr_partition_set(&dt_only_partitions),
            BTreeSet::from([
                ("2024-01-01".to_string(), 10),
                ("2024-01-01".to_string(), 20),
            ]),
        );
        assert_eq!(
            extract_dt_hr_partition_set(&dt_hr_partitions),
            BTreeSet::from([("2024-01-01".to_string(), 10)]),
        );
    }

    #[tokio::test]
    async fn test_scan_keeps_pushed_predicate_for_execute() {
        let provider = create_provider("partitioned_log_table").await;
        let filter = col("id").gt(lit(1));

        let config = SessionConfig::new().with_target_partitions(8);
        let ctx = SessionContext::new_with_config(config);
        let state = ctx.state();
        let plan = provider
            .scan(&state, None, std::slice::from_ref(&filter), None)
            .await
            .expect("scan() should succeed");
        let scan = plan
            .as_any()
            .downcast_ref::<PaimonTableScan>()
            .expect("Expected PaimonTableScan");

        let expected = build_pushed_predicate(&[filter], provider.table().schema().fields())
            .expect("data filter should translate");

        assert_eq!(scan.pushed_predicate(), Some(&expected));
    }

    #[tokio::test]
    async fn test_insert_into_and_read_back() {
        use paimon::io::FileIOBuilder;
        use paimon::spec::{DataType, IntType, Schema as PaimonSchema, TableSchema};

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/test_df_insert_into";
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();

        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let table_schema = TableSchema::new(0, &schema);
        let table = paimon::table::Table::new(
            file_io,
            Identifier::new("default", "test_insert"),
            table_path.to_string(),
            table_schema,
            None,
        );

        let provider = PaimonTableProvider::try_new(table).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        // INSERT INTO
        let result = ctx
            .sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Verify count output
        let count_array = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(count_array.value(0), 3);

        // Read back
        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut rows = Vec::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap();
            let vals = batch
                .column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((ids.value(i), vals.value(i)));
            }
        }
        assert_eq!(rows, vec![(1, 10), (2, 20), (3, 30)]);
    }

    #[tokio::test]
    async fn test_insert_overwrite() {
        use paimon::io::FileIOBuilder;
        use paimon::spec::{DataType, IntType, Schema as PaimonSchema, TableSchema, VarCharType};

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/test_df_insert_overwrite";
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();

        let schema = PaimonSchema::builder()
            .column("pt", DataType::VarChar(VarCharType::string_type()))
            .column("id", DataType::Int(IntType::new()))
            .partition_keys(["pt"])
            .build()
            .unwrap();
        let table_schema = TableSchema::new(0, &schema);
        let table = paimon::table::Table::new(
            file_io,
            Identifier::new("default", "test_overwrite"),
            table_path.to_string(),
            table_schema,
            None,
        );

        let provider = PaimonTableProvider::try_new(table).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        // Initial INSERT: partition "a" and "b"
        ctx.sql("INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // INSERT OVERWRITE with only partition "a" data
        // Should overwrite partition "a" but leave partition "b" intact
        ctx.sql("INSERT OVERWRITE t VALUES ('a', 10), ('a', 20)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Read back
        let batches = ctx
            .sql("SELECT pt, id FROM t ORDER BY pt, id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut rows = Vec::new();
        for batch in &batches {
            let pts = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            let ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((pts.value(i).to_string(), ids.value(i)));
            }
        }
        // Partition "a" overwritten with new data, partition "b" untouched
        assert_eq!(
            rows,
            vec![
                ("a".to_string(), 10),
                ("a".to_string(), 20),
                ("b".to_string(), 3),
                ("b".to_string(), 4),
            ]
        );
    }

    #[tokio::test]
    async fn test_insert_overwrite_unpartitioned() {
        use paimon::io::FileIOBuilder;
        use paimon::spec::{DataType, IntType, Schema as PaimonSchema, TableSchema};

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/test_df_insert_overwrite_unpart";
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();

        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let table_schema = TableSchema::new(0, &schema);
        let table = paimon::table::Table::new(
            file_io,
            Identifier::new("default", "test_overwrite_unpart"),
            table_path.to_string(),
            table_schema,
            None,
        );

        let provider = PaimonTableProvider::try_new(table).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        // Initial INSERT
        ctx.sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // INSERT OVERWRITE on unpartitioned table — full table overwrite
        ctx.sql("INSERT OVERWRITE t VALUES (4, 40), (5, 50)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let batches = ctx
            .sql("SELECT id, value FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut rows = Vec::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap();
            let vals = batch
                .column(1)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((ids.value(i), vals.value(i)));
            }
        }
        // Old data fully replaced
        assert_eq!(rows, vec![(4, 40), (5, 50)]);
    }
}
