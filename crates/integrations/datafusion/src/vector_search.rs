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

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, RecordBatchOptions, UInt32Array,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::stats::Precision;
use datafusion::common::{internal_err, project_schema, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::SessionContext;
use futures::{stream, TryStreamExt};
use paimon::catalog::Catalog;
use paimon::spec::{
    BigIntType, CoreOptions, DataField, DataType, Predicate, ROW_ID_FIELD_ID, ROW_ID_FIELD_NAME,
};
use paimon::table::Table;

use crate::error::to_datafusion_error;
use crate::filter_pushdown::analyze_filters;
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::table::{datafusion_read_fields, PaimonTableProvider};
use crate::table_function_args::{
    extract_int_literal, extract_string_literal, parse_table_identifier,
};
use crate::table_loader::load_data_table_for_read;
use crate::DynamicOptions;

const FUNCTION_NAME: &str = "vector_search";

pub fn register_vector_search(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    default_database: &str,
) {
    register_vector_search_with_dynamic_options(ctx, catalog, default_database, Default::default());
}

pub(crate) fn register_vector_search_with_dynamic_options(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    default_database: &str,
    dynamic_options: DynamicOptions,
) {
    ctx.register_udtf(
        "vector_search",
        Arc::new(VectorSearchFunction::new_with_dynamic_options(
            catalog,
            default_database,
            dynamic_options,
        )),
    );
}

pub struct VectorSearchFunction {
    catalog: Arc<dyn Catalog>,
    default_database: String,
    dynamic_options: DynamicOptions,
}

impl Debug for VectorSearchFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorSearchFunction")
            .field("default_database", &self.default_database)
            .finish()
    }
}

impl VectorSearchFunction {
    pub fn new(catalog: Arc<dyn Catalog>, default_database: &str) -> Self {
        Self::new_with_dynamic_options(catalog, default_database, Default::default())
    }

    pub(crate) fn new_with_dynamic_options(
        catalog: Arc<dyn Catalog>,
        default_database: &str,
        dynamic_options: DynamicOptions,
    ) -> Self {
        Self {
            catalog,
            default_database: default_database.to_string(),
            dynamic_options,
        }
    }
}

impl TableFunctionImpl for VectorSearchFunction {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 4 {
            return Err(datafusion::error::DataFusionError::Plan(
                "vector_search requires 4 arguments: (table_name, column_name, query_vector_json, limit)".to_string(),
            ));
        }

        let table_name = extract_string_literal(FUNCTION_NAME, &args[0], "table_name")?;
        let column_name = extract_string_literal(FUNCTION_NAME, &args[1], "column_name")?;
        let limit = extract_int_literal(FUNCTION_NAME, &args[3], "limit")?;

        if limit <= 0 {
            return Err(DataFusionError::Plan(
                "vector_search: limit must be positive".to_string(),
            ));
        }

        let identifier =
            parse_table_identifier(FUNCTION_NAME, &table_name, &self.default_database)?;

        let catalog = Arc::clone(&self.catalog);
        let dynamic_options = self.dynamic_options.read().unwrap().clone();
        let table = block_on_with_runtime(
            async move {
                let table = load_data_table_for_read(&catalog, &identifier, FUNCTION_NAME).await?;
                let table = if dynamic_options.is_empty() {
                    table
                } else {
                    table
                        .copy_with_time_travel(dynamic_options)
                        .await
                        .map_err(to_datafusion_error)?
                };
                Ok::<_, DataFusionError>(table)
            },
            "vector_search: catalog access thread panicked",
        )?;

        let inner = PaimonTableProvider::try_new(table)?;
        let query_vector_json =
            match extract_string_literal(FUNCTION_NAME, &args[2], "query_vector_json") {
                Ok(value) => value,
                Err(_) if matches!(args[2], Expr::Column(_)) => {
                    return Ok(Arc::new(LateralVectorSearchTableProvider {
                        inner,
                        column_name,
                        query_vector_expr: args[2].clone(),
                        limit: limit as usize,
                    }));
                }
                Err(err) => return Err(err),
            };

        let query_vector: Vec<f32> = serde_json::from_str(&query_vector_json).map_err(|e| {
            DataFusionError::Plan(format!(
                "vector_search: query_vector_json must be a JSON array of floats, got '{}': {}",
                query_vector_json, e
            ))
        })?;

        if query_vector.is_empty() {
            return Err(DataFusionError::Plan(
                "vector_search: query vector cannot be empty".to_string(),
            ));
        }

        Ok(Arc::new(VectorSearchTableProvider {
            inner,
            column_name,
            query_vector,
            limit: limit as usize,
        }))
    }
}

#[derive(Debug)]
pub(crate) struct LateralVectorSearchTableProvider {
    inner: PaimonTableProvider,
    column_name: String,
    query_vector_expr: Expr,
    limit: usize,
}

impl LateralVectorSearchTableProvider {
    pub(crate) fn inner(&self) -> &PaimonTableProvider {
        &self.inner
    }

    pub(crate) fn column_name(&self) -> &str {
        &self.column_name
    }

    pub(crate) fn query_vector_expr(&self) -> &Expr {
        &self.query_vector_expr
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit
    }
}

#[async_trait]
impl TableProvider for LateralVectorSearchTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "lateral vector_search must be planned through a lateral join".to_string(),
        ))
    }
}

#[derive(Debug)]
struct VectorSearchTableProvider {
    inner: PaimonTableProvider,
    column_name: String,
    query_vector: Vec<f32>,
    limit: usize,
}

#[async_trait]
impl TableProvider for VectorSearchTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema(), projection)?;
        let filter_analysis = analyze_filters(filters, self.inner.table().schema().fields(), true);
        if filter_analysis.requires_residual {
            return Err(DataFusionError::Plan(
                "vector_search cannot apply a partially translated scalar pre-filter".to_string(),
            ));
        }
        let pushed_predicate = filter_analysis.pushed_predicate;

        // An outer `LIMIT 0` needs no rows.
        if limit == Some(0) {
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        // The search runs with the table function's own top-k (`self.limit`) so the ANN
        // recall/search width is unchanged; the outer DataFusion `limit` only truncates
        // the already-ranked result before any rows are read (so a large top-k with a
        // small outer LIMIT doesn't read/materialize everything). All of this — search,
        // read and rank-order gather — runs at execution time in the exec's stream, so
        // planning / EXPLAIN stays cheap and the work is driven by the TaskContext.
        let mut exec = VectorSearchExec::new(
            self.inner.table().clone(),
            self.column_name.clone(),
            self.query_vector.clone(),
            self.limit,
            limit,
            projection.cloned(),
            projected_schema,
        );
        if let Some(filter) = pushed_predicate {
            exec = exec.with_filter(filter);
        }
        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        let fields = self.inner.table().schema().fields();
        Ok(filters
            .iter()
            .map(|filter| {
                let analysis = analyze_filters(std::slice::from_ref(*filter), fields, true);
                if analysis.pushed_predicate.is_some() && !analysis.requires_residual {
                    // Keep DataFusion's residual filter as a correctness backstop
                    // while using the same predicate before vector Top-K.
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

/// Execution-time plan for `vector_search`: runs the ANN search, reads the matching
/// rows, and gathers them into best-first relevance order when its stream is polled,
/// so planning (and `EXPLAIN`) stays cheap and the work runs under DataFusion's
/// `TaskContext`.
#[derive(Debug, Clone)]
struct VectorSearchExec {
    table: Table,
    column_name: String,
    query_vector: Vec<f32>,
    /// The table function's own top-k — drives the ANN search width; never reduced.
    search_limit: usize,
    /// The outer DataFusion `LIMIT`, applied by truncating the ranked result.
    output_limit: Option<usize>,
    projection: Option<Vec<usize>>,
    output_schema: ArrowSchemaRef,
    filter: Option<Predicate>,
    plan_properties: Arc<PlanProperties>,
}

impl VectorSearchExec {
    fn new(
        table: Table,
        column_name: String,
        query_vector: Vec<f32>,
        search_limit: usize,
        output_limit: Option<usize>,
        projection: Option<Vec<usize>>,
        output_schema: ArrowSchemaRef,
    ) -> Self {
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            table,
            column_name,
            query_vector,
            search_limit,
            output_limit,
            projection,
            output_schema,
            filter: None,
            plan_properties,
        }
    }

    fn with_filter(mut self, filter: Predicate) -> Self {
        self.filter = Some(filter);
        self
    }

    async fn compute_batch(&self) -> DFResult<RecordBatch> {
        let prepared_filter = match &self.filter {
            Some(filter) => Some(
                self.table
                    .prepare_vector_search_filter(filter.clone())
                    .await
                    .map_err(to_datafusion_error)?,
            ),
            None => None,
        };
        let search_table = prepared_filter
            .as_ref()
            .map(|prepared| prepared.table())
            .unwrap_or(&self.table);

        // Best-first row-ids from the index, searched at the full top-k so the ANN
        // recall is unchanged (data-evolution / global-index path; PK-vector tables are
        // unsupported here, as before).
        let mut search_result = await_with_runtime(async {
            let mut builder = self.table.new_batch_vector_search_builder();
            builder
                .with_vector_column(&self.column_name)
                .with_query_vectors(vec![self.query_vector.clone()])
                .with_limit(self.search_limit);
            if let Some(prepared) = &prepared_filter {
                builder.with_prepared_filter(prepared.clone());
            }
            let mut results = builder.execute().await.map_err(to_datafusion_error)?;
            Ok::<_, DataFusionError>(results.remove(0))
        })
        .await?;

        if search_result.is_empty() {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }

        // Apply the outer LIMIT by truncating the ranked result *before* reading, so a
        // large top-k with a small outer LIMIT only reads/materializes the rows it can
        // return (the search itself is unaffected).
        if let Some(n) = self.output_limit {
            if search_result.row_ids.len() > n {
                search_result.row_ids.truncate(n);
                search_result.scores.truncate(n);
            }
        }

        // Read the projected columns (+ internal `_ROW_ID`); the row-range scan yields
        // file order, realigned to relevance rank below.
        let read_fields = projected_read_fields(search_table, self.projection.as_ref())?;
        let row_ranges = search_result.to_row_ranges().map_err(to_datafusion_error)?;
        let batches = await_with_runtime(async {
            let mut read_builder = search_table.new_read_builder();
            read_builder
                .with_read_type(read_fields)
                .with_row_ranges(row_ranges);
            let scan = read_builder.new_scan();
            let plan = scan.plan().await.map_err(to_datafusion_error)?;
            let table_read = read_builder.new_read().map_err(to_datafusion_error)?;
            let mut stream = table_read
                .to_arrow(plan.splits())
                .map_err(to_datafusion_error)?;
            let mut batches: Vec<RecordBatch> = Vec::new();
            while let Some(batch) = stream.try_next().await.map_err(to_datafusion_error)? {
                batches.push(batch);
            }
            Ok::<_, DataFusionError>(batches)
        })
        .await?;

        // Realign file-order rows to best-first rank and drop `_ROW_ID`.
        gather_rows_by_rank(&batches, &search_result.row_ids, &self.output_schema)
    }
}

impl DisplayAs for VectorSearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "VectorSearchExec: column={}, search_limit={}, output_limit={:?}, filter={:?}",
            self.column_name, self.search_limit, self.output_limit, self.filter
        )
    }
}

impl ExecutionPlan for VectorSearchExec {
    fn name(&self) -> &str {
        "VectorSearchExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("VectorSearchExec is a leaf and takes no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "VectorSearchExec has a single partition, got partition {partition}"
            );
        }
        let exec = self.clone();
        let stream = stream::once(async move { exec.compute_batch().await });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            Box::pin(stream),
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Arc<Statistics>> {
        Ok(Arc::new(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(&self.output_schema),
        }))
    }
}

/// Projected user columns (+ internal `_ROW_ID`, needed to realign rows to rank).
/// Errors if the table has no row tracking, since results then can't be ordered.
fn projected_read_fields(
    table: &paimon::table::Table,
    projection: Option<&Vec<usize>>,
) -> DFResult<Vec<DataField>> {
    let base_fields = datafusion_read_fields(table);
    let mut read_fields: Vec<DataField> = match projection {
        Some(indices) => indices.iter().map(|&i| base_fields[i].clone()).collect(),
        None => base_fields,
    };
    if !read_fields
        .iter()
        .any(|field| field.name() == ROW_ID_FIELD_NAME)
    {
        if !CoreOptions::new(table.schema().options()).row_tracking_enabled() {
            return Err(DataFusionError::Plan(
                "vector_search: cannot order results by relevance because _ROW_ID is not available"
                    .to_string(),
            ));
        }
        read_fields.push(DataField::new(
            ROW_ID_FIELD_ID,
            ROW_ID_FIELD_NAME.to_string(),
            DataType::BigInt(BigIntType::with_nullable(true)),
        ));
    }
    Ok(read_fields)
}

/// Gather the file-order `batches` into `ranked_row_ids` order (rank == slice index),
/// producing `output_schema` (which excludes `_ROW_ID`). A permutation driven by the
/// index's existing ranking, not a re-sort.
fn gather_rows_by_rank(
    batches: &[RecordBatch],
    ranked_row_ids: &[u64],
    output_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    let input_schema = batches.first().map(|batch| batch.schema()).ok_or_else(|| {
        DataFusionError::Internal("vector_search: no rows materialized".to_string())
    })?;
    let combined = arrow_select::concat::concat_batches(&input_schema, batches)
        .map_err(DataFusionError::from)?;

    let row_id_index = combined.schema().index_of(ROW_ID_FIELD_NAME).map_err(|_| {
        DataFusionError::Internal(format!(
            "vector_search: materialized rows are missing the {ROW_ID_FIELD_NAME} column"
        ))
    })?;
    let row_ids = combined
        .column(row_id_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!("vector_search: {ROW_ID_FIELD_NAME} must be Int64"))
        })?;

    // Map global row id -> physical position in the materialized batch.
    let mut position_of: HashMap<i64, u32> = HashMap::with_capacity(combined.num_rows());
    for row in 0..combined.num_rows() {
        if !row_ids.is_null(row) {
            position_of.insert(row_ids.value(row), row as u32);
        }
    }

    // Emit in rank order; extra scanned rows are ignored, but a missing scored id
    // fails loud rather than silently shrinking the top-k.
    let mut take_indices: Vec<u32> = Vec::with_capacity(ranked_row_ids.len());
    for &row_id in ranked_row_ids {
        let position = position_of.get(&(row_id as i64)).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "vector_search: scored row id {row_id} was not materialized; \
                 cannot return the requested top-k"
            ))
        })?;
        take_indices.push(*position);
    }
    let take_indices = UInt32Array::from(take_indices);
    let row_count = take_indices.len();

    let columns = output_schema
        .fields()
        .iter()
        .map(|field| -> DFResult<ArrayRef> {
            let index = combined.schema().index_of(field.name()).map_err(|_| {
                DataFusionError::Internal(format!(
                    "vector_search: materialized rows are missing expected column '{}'",
                    field.name()
                ))
            })?;
            let taken =
                arrow_select::take::take(combined.column(index).as_ref(), &take_indices, None)
                    .map_err(DataFusionError::from)?;
            // The Paimon read keeps its own arrow types (e.g. `Utf8`), but the provider
            // schema may differ (e.g. DataFusion's `Utf8View`); cast to match, as the
            // normal scan path does via `to_datafusion_batch`.
            if taken.data_type() == field.data_type() {
                Ok(taken)
            } else {
                cast(taken.as_ref(), field.data_type()).map_err(DataFusionError::from)
            }
        })
        .collect::<DFResult<Vec<_>>>()?;

    // Preserve the row count for a zero-column projection (e.g. `COUNT(*)`).
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    RecordBatch::try_new_with_options(Arc::clone(output_schema), columns, &options)
        .map_err(DataFusionError::from)
}

#[cfg(test)]
mod tests {
    use datafusion::catalog::TableFunctionArgs;
    use datafusion::logical_expr::lit;
    use paimon::spec::SCAN_VERSION_OPTION;
    use paimon::{CatalogOptions, FileSystemCatalog, Options};

    use super::*;
    use crate::SQLContext;

    #[tokio::test]
    async fn test_vector_search_applies_supported_session_dynamic_options() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut catalog_options = Options::new();
        catalog_options.set(
            CatalogOptions::WAREHOUSE,
            format!("file://{}", temp_dir.path().display()),
        );
        let catalog = Arc::new(FileSystemCatalog::new(catalog_options).unwrap());

        let mut sql_context = SQLContext::new();
        sql_context
            .register_catalog("paimon", catalog)
            .await
            .unwrap();
        sql_context
            .sql(
                "CREATE TABLE paimon.default.vector_blob (\
                    id INT, \
                    embedding ARRAY<FLOAT>, \
                    picture BLOB\
                ) WITH (\
                    'data-evolution.enabled' = 'true', \
                    'row-tracking.enabled' = 'true'\
                )",
            )
            .await
            .unwrap();
        sql_context
            .sql("INSERT INTO paimon.default.vector_blob (id) VALUES (1)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        sql_context
            .sql("SET 'paimon.blob-as-descriptor' = 'true'")
            .await
            .unwrap();
        sql_context
            .sql("SET 'paimon.scan.version' = '1'")
            .await
            .unwrap();

        let state = sql_context.ctx().state();
        let table_function = state
            .table_functions()
            .get(FUNCTION_NAME)
            .expect("vector_search should be registered");
        let args = [
            lit("paimon.default.vector_blob"),
            lit("embedding"),
            lit("[1.0]"),
            lit(1_i64),
        ];
        let provider = table_function
            .create_table_provider_with_args(TableFunctionArgs::new(&args, &state))
            .unwrap();
        let provider = provider
            .downcast_ref::<VectorSearchTableProvider>()
            .expect("vector_search should return its table provider");

        assert!(
            CoreOptions::new(provider.inner.table().schema().options()).blob_as_descriptor(),
            "vector_search should apply session dynamic options to the loaded table"
        );
        assert!(
            provider
                .inner
                .table()
                .schema()
                .options()
                .contains_key(SCAN_VERSION_OPTION),
            "vector_search should keep session time-travel options"
        );
        assert_eq!(
            provider
                .inner
                .table()
                .travel_snapshot()
                .map(|snapshot| snapshot.id()),
            Some(1),
            "vector_search should resolve the session time-travel snapshot"
        );
    }
}
