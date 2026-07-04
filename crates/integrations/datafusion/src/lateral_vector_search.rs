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
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    new_empty_array, Array, ArrayRef, FixedSizeListArray, Float32Array, Int64Array, ListArray,
    RecordBatch, UInt32Array,
};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::default_table_source::source_as_provider;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{
    internal_err, DFSchemaRef, DataFusionError, JoinType, Result as DFResult, Statistics,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, TableScan, UserDefinedLogicalNode};
use datafusion::optimizer::{ApplyOrder, Optimizer, OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::SessionConfig;
use futures::{StreamExt, TryStreamExt};
use paimon::spec::ROW_ID_FIELD_NAME;
use paimon::table::{RowRange, Table};
use paimon::vector_search::SearchResult;

use crate::error::to_datafusion_error;
use crate::vector_search::LateralVectorSearchTableProvider;

#[derive(Debug)]
pub(crate) struct PaimonQueryPlanner;

impl PaimonQueryPlanner {
    pub(crate) fn new() -> Self {
        Self
    }
}

#[async_trait]
impl QueryPlanner for PaimonQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            LateralVectorSearchExtensionPlanner,
        )]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

#[derive(Debug)]
pub(crate) struct RewriteLateralVectorSearch;

impl RewriteLateralVectorSearch {
    pub(crate) fn new() -> Self {
        Self
    }
}

pub(crate) fn optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> =
        vec![Arc::new(RewriteLateralVectorSearch::new())];
    rules.extend(Optimizer::default().rules);
    rules
}

impl OptimizerRule for RewriteLateralVectorSearch {
    fn name(&self) -> &str {
        "rewrite_lateral_vector_search"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Join(join) = plan else {
            return Ok(Transformed::no(plan));
        };

        if join.join_type != JoinType::Inner || !join.on.is_empty() || join.filter.is_some() {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }

        let Some(spec) = find_lateral_vector_search_provider(&join.right)? else {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        };

        let node = LateralVectorSearchNode::new(
            Arc::clone(&join.left),
            spec.target_table,
            spec.target_schema,
            spec.target_column,
            spec.query_vector_expr,
            spec.limit,
            Arc::clone(&join.schema),
        );
        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        })))
    }
}

fn find_lateral_vector_search_provider(
    plan: &LogicalPlan,
) -> DFResult<Option<LateralVectorSearchSpec>> {
    match plan {
        LogicalPlan::TableScan(TableScan { source, .. }) => {
            let provider = source_as_provider(source)?;
            let Some(provider) = provider
                .as_any()
                .downcast_ref::<LateralVectorSearchTableProvider>()
            else {
                return Ok(None);
            };
            Ok(Some(LateralVectorSearchSpec {
                target_table: provider.inner().table().clone(),
                target_schema: provider.inner().schema(),
                target_column: provider.column_name().to_string(),
                query_vector_expr: provider.query_vector_expr().clone(),
                limit: provider.limit(),
            }))
        }
        LogicalPlan::Subquery(subquery) => find_lateral_vector_search_provider(&subquery.subquery),
        LogicalPlan::SubqueryAlias(alias) => find_lateral_vector_search_provider(&alias.input),
        _ => Ok(None),
    }
}

struct LateralVectorSearchSpec {
    target_table: Table,
    target_schema: ArrowSchemaRef,
    target_column: String,
    query_vector_expr: Expr,
    limit: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct LateralVectorSearchNode {
    input: Arc<LogicalPlan>,
    target_table: Table,
    target_schema: ArrowSchemaRef,
    target_column: String,
    query_vector_expr: Expr,
    limit: usize,
    schema: DFSchemaRef,
}

impl LateralVectorSearchNode {
    fn new(
        input: Arc<LogicalPlan>,
        target_table: Table,
        target_schema: ArrowSchemaRef,
        target_column: String,
        query_vector_expr: Expr,
        limit: usize,
        schema: DFSchemaRef,
    ) -> Self {
        Self {
            input,
            target_table,
            target_schema,
            target_column,
            query_vector_expr,
            limit,
            schema,
        }
    }

    fn target_table(&self) -> &Table {
        &self.target_table
    }

    fn target_schema(&self) -> &ArrowSchemaRef {
        &self.target_schema
    }

    fn target_column(&self) -> &str {
        &self.target_column
    }

    fn query_vector_expr(&self) -> &Expr {
        &self.query_vector_expr
    }

    fn limit(&self) -> usize {
        self.limit
    }
}

impl UserDefinedLogicalNode for LateralVectorSearchNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "LateralVectorSearch"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn check_invariants(&self, _check: datafusion::logical_expr::InvariantLevel) -> DFResult<()> {
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.query_vector_expr.clone()]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LateralVectorSearch: column={}, limit={}",
            self.target_column, self.limit
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Arc<dyn UserDefinedLogicalNode>> {
        if exprs.len() != 1 || inputs.len() != 1 {
            return internal_err!("LateralVectorSearch expects one expression and one input");
        }
        Ok(Arc::new(Self {
            input: Arc::new(inputs.into_iter().next().unwrap()),
            target_table: self.target_table.clone(),
            target_schema: Arc::clone(&self.target_schema),
            target_column: self.target_column.clone(),
            query_vector_expr: exprs.into_iter().next().unwrap(),
            limit: self.limit,
            schema: Arc::clone(&self.schema),
        }))
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.name().hash(&mut state);
        self.input.hash(&mut state);
        self.target_table.location().hash(&mut state);
        self.target_column.hash(&mut state);
        self.query_vector_expr.hash(&mut state);
        self.limit.hash(&mut state);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other.as_any().downcast_ref::<Self>().is_some_and(|other| {
            self.input == other.input
                && self.target_table.location() == other.target_table.location()
                && self.target_column == other.target_column
                && self.query_vector_expr == other.query_vector_expr
                && self.limit == other.limit
        })
    }

    fn dyn_ord(&self, other: &dyn UserDefinedLogicalNode) -> Option<Ordering> {
        let other = other.as_any().downcast_ref::<Self>()?;
        if self.dyn_eq(other) {
            Some(Ordering::Equal)
        } else {
            Some(format!("{self:?}").cmp(&format!("{other:?}")))
        }
    }
}

#[derive(Debug)]
struct LateralVectorSearchExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for LateralVectorSearchExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<LateralVectorSearchNode>() else {
            return Ok(None);
        };
        if logical_inputs.len() != 1 || physical_inputs.len() != 1 {
            return internal_err!("LateralVectorSearch physical planning expects one input");
        }

        let query_vector_expr = planner.create_physical_expr(
            node.query_vector_expr(),
            logical_inputs[0].schema(),
            session_state,
        )?;
        Ok(Some(Arc::new(LateralVectorSearchExec::new(
            Arc::clone(&physical_inputs[0]),
            node.target_table().clone(),
            Arc::clone(node.target_schema()),
            node.target_column().to_string(),
            query_vector_expr,
            node.limit(),
            Arc::new(node.schema().as_arrow().clone()),
        ))))
    }
}

#[derive(Debug, Clone)]
struct LateralVectorSearchExec {
    input: Arc<dyn ExecutionPlan>,
    target_table: Table,
    target_schema: ArrowSchemaRef,
    target_column: String,
    query_vector_expr: Arc<dyn PhysicalExpr>,
    limit: usize,
    output_schema: ArrowSchemaRef,
    plan_properties: Arc<PlanProperties>,
}

impl LateralVectorSearchExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        target_table: Table,
        target_schema: ArrowSchemaRef,
        target_column: String,
        query_vector_expr: Arc<dyn PhysicalExpr>,
        limit: usize,
        output_schema: ArrowSchemaRef,
    ) -> Self {
        let partition_count = input.output_partitioning().partition_count();
        let plan_properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            input,
            target_table,
            target_schema,
            target_column,
            query_vector_expr,
            limit,
            output_schema,
            plan_properties,
        }
    }

    async fn process_batch(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        if batch.num_rows() == 0 {
            return empty_batch(self.output_schema.clone());
        }

        let vector_array = self
            .query_vector_expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())?;
        let (query_vectors, left_query_rows) = collect_query_vectors(&vector_array)?;
        if query_vectors.is_empty() {
            return empty_batch(self.output_schema.clone());
        }

        let mut builder = self.target_table.new_batch_vector_search_builder();
        let results = builder
            .with_vector_column(&self.target_column)
            .with_query_vectors(query_vectors)
            .with_limit(self.limit)
            .execute()
            .await
            .map_err(to_datafusion_error)?;

        let (target_batch, target_row_id_to_index) =
            read_target_rows(&self.target_table, &self.target_schema, &results).await?;

        let mut left_indices = Vec::new();
        let mut right_indices = Vec::new();
        for (query_index, result) in results.iter().enumerate() {
            let left_row = left_query_rows[query_index] as u32;
            for row_id in &result.row_ids {
                if let Some(&right_row) = target_row_id_to_index.get(row_id) {
                    left_indices.push(left_row);
                    right_indices.push(right_row);
                }
            }
        }

        if left_indices.is_empty() {
            return empty_batch(self.output_schema.clone());
        }

        let left_indices = UInt32Array::from(left_indices);
        let right_indices = UInt32Array::from(right_indices);
        let mut columns = Vec::with_capacity(batch.num_columns() + target_batch.num_columns());
        for column in batch.columns() {
            columns.push(arrow_select::take::take(
                column.as_ref(),
                &left_indices,
                None,
            )?);
        }
        for column in target_batch.columns() {
            columns.push(arrow_select::take::take(
                column.as_ref(),
                &right_indices,
                None,
            )?);
        }

        RecordBatch::try_new(self.output_schema.clone(), columns).map_err(DataFusionError::from)
    }
}

impl DisplayAs for LateralVectorSearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LateralVectorSearchExec: column={}, limit={}",
            self.target_column, self.limit
        )
    }
}

impl ExecutionPlan for LateralVectorSearchExec {
    fn name(&self) -> &str {
        "LateralVectorSearchExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("LateralVectorSearchExec expects one child");
        }
        Ok(Arc::new(Self::new(
            children.remove(0),
            self.target_table.clone(),
            Arc::clone(&self.target_schema),
            self.target_column.clone(),
            Arc::clone(&self.query_vector_expr),
            self.limit,
            Arc::clone(&self.output_schema),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let exec = self.clone();
        let stream = input.then(move |batch| {
            let exec = exec.clone();
            async move {
                let batch = batch?;
                exec.process_batch(batch).await
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            Box::pin(stream),
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Statistics> {
        Ok(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(&self.output_schema),
        })
    }
}

fn collect_query_vectors(array: &ArrayRef) -> DFResult<(Vec<Vec<f32>>, Vec<usize>)> {
    enum VectorLayout<'a> {
        List(&'a ListArray),
        Fixed(&'a FixedSizeListArray),
    }
    let layout = if let Some(array) = array.as_any().downcast_ref::<ListArray>() {
        VectorLayout::List(array)
    } else if let Some(array) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        VectorLayout::Fixed(array)
    } else {
        return Err(DataFusionError::Plan(
            "lateral vector_search query vector must be List<Float32> or FixedSizeList<Float32>"
                .to_string(),
        ));
    };
    let values = match layout {
        VectorLayout::List(array) => array.values(),
        VectorLayout::Fixed(array) => array.values(),
    }
    .as_any()
    .downcast_ref::<Float32Array>()
    .ok_or_else(|| {
        DataFusionError::Plan(
            "lateral vector_search query vector elements must be Float32".to_string(),
        )
    })?;

    let row_count = match layout {
        VectorLayout::List(array) => array.len(),
        VectorLayout::Fixed(array) => array.len(),
    };
    let mut vectors = Vec::new();
    let mut rows = Vec::new();
    for row in 0..row_count {
        let is_null = match layout {
            VectorLayout::List(array) => array.is_null(row),
            VectorLayout::Fixed(array) => array.is_null(row),
        };
        if is_null {
            continue;
        }

        let (start, end) = match layout {
            VectorLayout::List(array) => {
                let offsets = array.value_offsets();
                (offsets[row] as usize, offsets[row + 1] as usize)
            }
            VectorLayout::Fixed(array) => {
                let len = array.value_length() as usize;
                (row * len, (row + 1) * len)
            }
        };
        let mut vector = Vec::with_capacity(end - start);
        for value_index in start..end {
            if values.is_null(value_index) {
                return Err(DataFusionError::Plan(
                    "lateral vector_search query vector cannot contain null elements".to_string(),
                ));
            }
            vector.push(values.value(value_index));
        }
        vectors.push(vector);
        rows.push(row);
    }
    Ok((vectors, rows))
}

async fn read_target_rows(
    table: &Table,
    target_schema: &ArrowSchemaRef,
    results: &[SearchResult],
) -> DFResult<(RecordBatch, HashMap<u64, u32>)> {
    let mut row_ids = results
        .iter()
        .flat_map(|result| result.row_ids.iter().copied())
        .collect::<Vec<_>>();
    row_ids.sort_unstable();
    row_ids.dedup();
    if row_ids.is_empty() {
        return Ok((empty_batch(target_schema.clone())?, HashMap::new()));
    }

    let row_ranges = row_ranges_from_row_ids(&row_ids)?;
    let mut projection = target_schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    if !projection.iter().any(|column| column == ROW_ID_FIELD_NAME) {
        projection.push(ROW_ID_FIELD_NAME.to_string());
    }
    let projection_refs = projection.iter().map(String::as_str).collect::<Vec<_>>();

    let mut read_builder = table.new_read_builder();
    read_builder
        .with_projection(&projection_refs)
        .with_row_ranges(row_ranges);
    let plan = read_builder
        .new_scan()
        .plan()
        .await
        .map_err(to_datafusion_error)?;
    if plan.splits().is_empty() {
        return Ok((empty_batch(target_schema.clone())?, HashMap::new()));
    }

    let read = read_builder.new_read().map_err(to_datafusion_error)?;
    let mut stream = read.to_arrow(plan.splits()).map_err(to_datafusion_error)?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.try_next().await.map_err(to_datafusion_error)? {
        batches.push(batch);
    }
    if batches.is_empty() {
        return Ok((empty_batch(target_schema.clone())?, HashMap::new()));
    }

    let read_schema = batches[0].schema();
    let batch = arrow_select::concat::concat_batches(&read_schema, &batches)
        .map_err(DataFusionError::from)?;
    let row_id_index = batch
        .schema()
        .index_of(ROW_ID_FIELD_NAME)
        .map_err(DataFusionError::from)?;
    let row_id_array = batch
        .column(row_id_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "_ROW_ID must be Int64 in vector search target read".to_string(),
            )
        })?;

    let mut row_id_to_index = HashMap::new();
    for row in 0..batch.num_rows() {
        if row_id_array.is_null(row) {
            continue;
        }
        let row_id = u64::try_from(row_id_array.value(row)).map_err(|_| {
            DataFusionError::Internal(format!(
                "negative _ROW_ID {} in vector search target read",
                row_id_array.value(row)
            ))
        })?;
        row_id_to_index.insert(row_id, row as u32);
    }

    let target_columns = (0..target_schema.fields().len())
        .map(|index| Arc::clone(batch.column(index)))
        .collect::<Vec<_>>();
    let target_batch = RecordBatch::try_new(target_schema.clone(), target_columns)
        .map_err(DataFusionError::from)?;
    Ok((target_batch, row_id_to_index))
}

fn row_ranges_from_row_ids(row_ids: &[u64]) -> DFResult<Vec<RowRange>> {
    let scores = vec![0.0; row_ids.len()];
    SearchResult::new(row_ids.to_vec(), scores)
        .to_row_ranges()
        .map_err(to_datafusion_error)
}

fn empty_batch(schema: ArrowSchemaRef) -> DFResult<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| new_empty_array(field.data_type()))
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema, columns).map_err(DataFusionError::from)
}

pub(crate) fn session_config() -> SessionConfig {
    SessionConfig::new().with_information_schema(true)
}
