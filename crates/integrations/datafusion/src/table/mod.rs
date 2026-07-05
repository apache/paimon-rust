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
use crate::BlobReaderRegistry;

use crate::error::to_datafusion_error;
#[cfg(test)]
use crate::filter_pushdown::build_pushed_predicate;
use crate::filter_pushdown::{analyze_filters, classify_filter_pushdown};
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

    pub fn try_new_with_blob_reader_registry(
        table: Table,
        blob_reader_registry: BlobReaderRegistry,
    ) -> DFResult<Self> {
        blob_reader_registry
            .register_if_absent(table.location().to_string(), table.file_io().clone());
        Self::try_new(table)
    }

    pub fn table(&self) -> &Table {
        &self.table
    }
}

/// Distribute `items` into `num_buckets` groups using round-robin assignment.
pub(crate) fn bucket_round_robin<T>(items: Vec<T>, num_buckets: usize) -> Vec<Vec<T>> {
    let mut buckets: Vec<Vec<T>> = (0..num_buckets).map(|_| Vec::new()).collect();
    for (index, item) in items.into_iter().enumerate() {
        buckets[index % num_buckets].push(item);
    }
    buckets
}

/// Build parameters for [`PaimonTableScan`].
pub(crate) struct PaimonScanBuilder<'a> {
    pub(crate) table: &'a Table,
    pub(crate) schema: &'a ArrowSchemaRef,
    pub(crate) plan: &'a paimon::table::Plan,
    pub(crate) scan_trace: Option<paimon::table::ScanTrace>,
    pub(crate) projection: Option<&'a Vec<usize>>,
    pub(crate) pushed_predicate: Option<paimon::spec::Predicate>,
    pub(crate) limit: Option<usize>,
    pub(crate) target_partitions: usize,
    pub(crate) filter_exact: bool,
}

impl PaimonScanBuilder<'_> {
    /// Build a [`PaimonTableScan`] from the configured parameters.
    pub(crate) fn build(self) -> DFResult<Arc<dyn ExecutionPlan>> {
        let (projected_schema, projected_columns) = if let Some(indices) = self.projection {
            let fields: Vec<Field> = indices
                .iter()
                .map(|&i| self.schema.field(i).clone())
                .collect();
            let column_names: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();
            (Arc::new(Schema::new(fields)), Some(column_names))
        } else {
            let column_names: Vec<String> = self
                .schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            (self.schema.clone(), Some(column_names))
        };

        let splits = self.plan.splits().to_vec();
        let planned_partitions: Vec<Arc<[_]>> = if splits.is_empty() {
            vec![Arc::from(Vec::new())]
        } else {
            let num_partitions = splits.len().min(self.target_partitions.max(1));
            bucket_round_robin(splits, num_partitions)
                .into_iter()
                .map(Arc::from)
                .collect()
        };

        Ok(Arc::new(PaimonTableScan::new(
            projected_schema,
            self.table.clone(),
            projected_columns,
            self.pushed_predicate,
            planned_partitions,
            self.limit,
            self.filter_exact,
            self.scan_trace,
        )))
    }
}

#[async_trait]
impl TableProvider for PaimonTableProvider {
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
        let filter_analysis = analyze_filters(filters, self.table.schema().fields());
        let mut read_builder = self.table.new_read_builder();
        let projected_columns = projection.map(|indices| {
            indices
                .iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect::<Vec<_>>()
        });
        if let Some(ref columns) = projected_columns {
            let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            read_builder.with_projection(&col_refs);
        }
        if let Some(filter) = filter_analysis.pushed_predicate.clone() {
            read_builder.with_filter(filter);
        }
        let pushed_limit = limit.filter(|_| !filter_analysis.has_untranslated_residual);
        if let Some(limit) = pushed_limit {
            read_builder.with_limit(limit);
        }
        let scan = read_builder.new_scan();
        // DataFusion's Python FFI may poll `TableProvider::scan()` without an active
        // Tokio runtime. `scan.plan()` can reach OpenDAL/Tokio filesystem calls while
        // reading Paimon metadata, so we must provide a runtime here instead of
        // assuming the caller already entered one.
        let (plan, scan_trace) = await_with_runtime(scan.plan_with_trace())
            .await
            .map_err(to_datafusion_error)?;

        let target = state.config_options().execution.target_partitions;
        let filter_exact = !filter_analysis.has_untranslated_residual
            && filter_analysis
                .pushed_predicate
                .as_ref()
                .is_none_or(|p| read_builder.is_exact_filter_pushdown(p));
        PaimonScanBuilder {
            table: &self.table,
            schema: &self.schema,
            plan: &plan,
            scan_trace: Some(scan_trace),
            projection,
            pushed_predicate: filter_analysis.pushed_predicate,
            limit: pushed_limit,
            target_partitions: target,
            filter_exact,
        }
        .build()
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
        let read_builder = self.table.new_read_builder();

        Ok(filters
            .iter()
            .map(|filter| {
                classify_filter_pushdown(filter, fields, |predicate| {
                    read_builder.is_exact_filter_pushdown(predicate)
                })
            })
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
        limit: Option<usize>,
    ) -> Vec<Arc<[DataSplit]>> {
        let plan = plan_scan(provider, filters, limit).await;
        let scan = plan
            .downcast_ref::<PaimonTableScan>()
            .expect("Expected PaimonTableScan");

        scan.planned_partitions().to_vec()
    }

    async fn plan_scan(
        provider: &PaimonTableProvider,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        let config = SessionConfig::new().with_target_partitions(8);
        let ctx = SessionContext::new_with_config(config);
        let state = ctx.state();
        provider
            .scan(&state, None, &filters, limit)
            .await
            .expect("scan() should succeed")
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

    fn empty_binary_stats_json() -> serde_json::Value {
        let row = paimon::spec::EMPTY_SERIALIZED_ROW.as_slice().to_vec();
        serde_json::json!({
            "_MIN_VALUES": row,
            "_MAX_VALUES": row,
            "_NULL_COUNTS": [],
        })
    }

    fn data_evolution_file(
        file_name: &str,
        file_size: i64,
        row_count: i64,
        first_row_id: i64,
        write_cols: &[&str],
    ) -> paimon::spec::DataFileMeta {
        serde_json::from_value(serde_json::json!({
            "_FILE_NAME": file_name,
            "_FILE_SIZE": file_size,
            "_ROW_COUNT": row_count,
            "_MIN_KEY": [],
            "_MAX_KEY": [],
            "_KEY_STATS": empty_binary_stats_json(),
            "_VALUE_STATS": empty_binary_stats_json(),
            "_MIN_SEQUENCE_NUMBER": 0,
            "_MAX_SEQUENCE_NUMBER": 0,
            "_SCHEMA_ID": 0,
            "_LEVEL": 1,
            "_EXTRA_FILES": [],
            "_CREATION_TIME": null,
            "_DELETE_ROW_COUNT": null,
            "_EMBEDDED_FILE_INDEX": null,
            "_FILE_SOURCE": null,
            "_VALUE_STATS_COLS": null,
            "_FIRST_ROW_ID": first_row_id,
            "_WRITE_COLS": write_cols,
            "_EXTERNAL_PATH": null,
        }))
        .expect("test data file should deserialize")
    }

    fn manifest_file_meta(
        file_name: &str,
        file_size: i64,
        num_added_files: i64,
    ) -> paimon::spec::ManifestFileMeta {
        serde_json::from_value(serde_json::json!({
            "_VERSION": 2,
            "_FILE_NAME": file_name,
            "_FILE_SIZE": file_size,
            "_NUM_ADDED_FILES": num_added_files,
            "_NUM_DELETED_FILES": 0,
            "_PARTITION_STATS": empty_binary_stats_json(),
            "_SCHEMA_ID": 0,
        }))
        .expect("test manifest file meta should deserialize")
    }

    async fn data_evolution_projection_pruning_provider() -> PaimonTableProvider {
        use paimon::io::FileIOBuilder;
        use paimon::spec::{
            CommitKind, DataType, FileKind, IntType, Manifest, ManifestEntry, ManifestList,
            Schema as PaimonSchema, Snapshot, TableSchema,
        };
        use paimon::table::{SnapshotManager, Table};

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = format!("memory:/df_de_projection_pruning_{}", uuid::Uuid::new_v4());
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
            .column("name", DataType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap();
        let table_schema = TableSchema::new(0, &schema);
        let table = Table::new(
            file_io.clone(),
            Identifier::new("default", "df_de_projection_pruning"),
            table_path.clone(),
            table_schema,
            None,
        );

        let partition = paimon::spec::EMPTY_SERIALIZED_ROW.as_slice().to_vec();
        let entries = vec![
            ManifestEntry::new(
                FileKind::Add,
                partition.clone(),
                0,
                1,
                data_evolution_file("id.parquet", 11, 10, 0, &["id"]),
                2,
            ),
            ManifestEntry::new(
                FileKind::Add,
                partition,
                0,
                1,
                data_evolution_file("name.parquet", 13, 10, 0, &["name"]),
                2,
            ),
        ];

        let manifest_name = "manifest-de-projection-0";
        let manifest_path = format!("{table_path}/manifest/{manifest_name}");
        Manifest::write(&file_io, &manifest_path, &entries)
            .await
            .unwrap();
        let manifest_size = file_io
            .new_input(&manifest_path)
            .unwrap()
            .metadata()
            .await
            .unwrap()
            .size;

        let base_list_name = "base-list-de-projection";
        let delta_list_name = "delta-list-de-projection";
        ManifestList::write(
            &file_io,
            &format!("{table_path}/manifest/{base_list_name}"),
            &[manifest_file_meta(
                manifest_name,
                manifest_size as i64,
                entries.len() as i64,
            )],
        )
        .await
        .unwrap();
        ManifestList::write(
            &file_io,
            &format!("{table_path}/manifest/{delta_list_name}"),
            &[],
        )
        .await
        .unwrap();

        let snapshot = Snapshot::builder()
            .version(3)
            .id(1)
            .schema_id(0)
            .base_manifest_list(base_list_name.to_string())
            .delta_manifest_list(delta_list_name.to_string())
            .commit_user("test-user".to_string())
            .commit_identifier(1)
            .commit_kind(CommitKind::APPEND)
            .time_millis(1)
            .total_record_count(Some(10))
            .delta_record_count(Some(10))
            .build();
        let snapshot_manager = SnapshotManager::new(file_io, table_path);
        assert!(snapshot_manager.commit_snapshot(&snapshot).await.unwrap());

        PaimonTableProvider::try_new(table).expect("provider should be created")
    }

    fn planned_file_names(scan: &PaimonTableScan) -> Vec<String> {
        let mut names = scan
            .planned_partitions()
            .iter()
            .flat_map(|partition| partition.iter())
            .flat_map(|split| split.data_files().iter())
            .map(|file| file.file_name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[tokio::test]
    async fn test_scan_partition_filter_plans_matching_partition_set() {
        let provider = create_provider("partitioned_log_table").await;
        let planned_partitions =
            plan_partitions(&provider, vec![col("dt").eq(lit("2024-01-01"))], None).await;

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
            None,
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
            plan_partitions(&provider, vec![col("dt").eq(lit("2024-01-01"))], None).await;
        let dt_hr_partitions = plan_partitions(
            &provider,
            vec![col("dt").eq(lit("2024-01-01")).and(col("hr").eq(lit(10)))],
            None,
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
    async fn test_scan_partially_translated_filter_keeps_partition_pruning_but_skips_limit_hint() {
        let provider = create_provider("multi_partitioned_log_table").await;
        let filter = col("dt")
            .eq(lit("2024-01-01"))
            .and(Expr::Not(Box::new(col("hr").eq(lit(10)))));
        let full_plan = plan_partitions(&provider, vec![filter.clone()], None).await;
        let plan = plan_scan(&provider, vec![filter], Some(1)).await;
        let scan = plan
            .downcast_ref::<PaimonTableScan>()
            .expect("Expected PaimonTableScan");

        assert_eq!(scan.limit(), None);
        assert_eq!(
            extract_dt_hr_partition_set(scan.planned_partitions()),
            BTreeSet::from([
                ("2024-01-01".to_string(), 10),
                ("2024-01-01".to_string(), 20),
            ]),
        );
        assert_eq!(
            scan.planned_partitions()
                .iter()
                .map(|partition| partition.len())
                .sum::<usize>(),
            full_plan
                .iter()
                .map(|partition| partition.len())
                .sum::<usize>()
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
            .downcast_ref::<PaimonTableScan>()
            .expect("Expected PaimonTableScan");

        let expected = build_pushed_predicate(&[filter], provider.table().schema().fields())
            .expect("data filter should translate");

        assert_eq!(scan.pushed_predicate(), Some(&expected));
    }

    #[tokio::test]
    async fn test_scan_applies_projection_to_data_evolution_planning() {
        let provider = data_evolution_projection_pruning_provider().await;
        let config = SessionConfig::new().with_target_partitions(8);
        let ctx = SessionContext::new_with_config(config);
        let state = ctx.state();

        let full_plan = provider
            .scan(&state, None, &[], None)
            .await
            .expect("full scan should succeed");
        let full_scan = full_plan
            .downcast_ref::<PaimonTableScan>()
            .expect("Expected PaimonTableScan");
        assert_eq!(
            planned_file_names(full_scan),
            vec!["id.parquet".to_string(), "name.parquet".to_string()]
        );

        let projection = vec![1];
        let projected_plan = provider
            .scan(&state, Some(&projection), &[], None)
            .await
            .expect("projected scan should succeed");
        let projected_scan = projected_plan
            .downcast_ref::<PaimonTableScan>()
            .expect("Expected PaimonTableScan");

        assert_eq!(
            planned_file_names(projected_scan),
            vec!["name.parquet".to_string()]
        );
    }

    #[tokio::test]
    async fn test_scan_applies_limit_hint_only_when_safe() {
        let provider = create_provider("partitioned_log_table").await;
        let full_plan = plan_partitions(&provider, vec![], None).await;
        let plan = plan_scan(&provider, vec![], Some(1)).await;
        let scan = plan
            .downcast_ref::<PaimonTableScan>()
            .expect("Expected PaimonTableScan");

        assert_eq!(scan.limit(), Some(1));
        assert!(
            scan.planned_partitions()
                .iter()
                .map(|partition| partition.len())
                .sum::<usize>()
                < full_plan
                    .iter()
                    .map(|partition| partition.len())
                    .sum::<usize>()
        );
    }

    #[tokio::test]
    async fn test_scan_keeps_limit_but_skips_limit_pruning_for_data_filters() {
        let provider = create_provider("partitioned_log_table").await;
        let filter = col("id").gt(lit(1));
        let full_plan = plan_partitions(&provider, vec![filter.clone()], None).await;
        let plan = plan_scan(&provider, vec![filter], Some(1)).await;
        let scan = plan
            .downcast_ref::<PaimonTableScan>()
            .expect("Expected PaimonTableScan");

        assert_eq!(scan.limit(), Some(1));
        assert_eq!(
            scan.planned_partitions()
                .iter()
                .map(|partition| partition.len())
                .sum::<usize>(),
            full_plan
                .iter()
                .map(|partition| partition.len())
                .sum::<usize>()
        );
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
