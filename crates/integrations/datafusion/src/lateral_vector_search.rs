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
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

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
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::logical_expr::{
    Expr, Extension, Filter, LogicalPlan, Projection, TableScan, UserDefinedLogicalNode,
};
use datafusion::optimizer::{ApplyOrder, Optimizer, OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, RecordBatchStream,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::SessionConfig;
use futures::{Stream, StreamExt, TryStreamExt};
use paimon::spec::{Predicate, ROW_ID_FIELD_NAME};
use paimon::table::{PreparedVectorSearchFilter, RowRange, Table};
use paimon::vector_search::SearchResult;
use tokio::sync::OnceCell;

use crate::error::to_datafusion_error;
use crate::filter_pushdown::analyze_filters;
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
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(LateralVectorSearchExtensionPlanner),
            Arc::new(crate::variant_pushdown::VariantExtractionExtensionPlanner),
        ]);
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
    let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
        Arc::new(crate::variant_pushdown::RewriteVariantExtractions::new()),
        Arc::new(RewriteLateralVectorSearch::new()),
    ];
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
        if let LogicalPlan::Filter(filter) = plan {
            let (extension, projection) = match filter.input.as_ref() {
                LogicalPlan::Extension(extension) => (extension, None),
                LogicalPlan::Projection(projection)
                    if projection
                        .expr
                        .iter()
                        .all(|expr| matches!(expr, Expr::Column(_))) =>
                {
                    let LogicalPlan::Extension(extension) = projection.input.as_ref() else {
                        return Ok(Transformed::no(LogicalPlan::Filter(filter)));
                    };
                    (extension, Some(projection))
                }
                _ => return Ok(Transformed::no(LogicalPlan::Filter(filter))),
            };
            let Some(node) = extension
                .node
                .as_any()
                .downcast_ref::<LateralVectorSearchNode>()
            else {
                return Ok(Transformed::no(LogicalPlan::Filter(filter)));
            };
            let mut target_predicates = Vec::new();
            let mut residual_predicates = Vec::new();
            for conjunct in split_conjunction(&filter.predicate) {
                if conjunct
                    .column_refs()
                    .iter()
                    .any(|column| node.input.schema().index_of_column(column).is_ok())
                {
                    residual_predicates.push(conjunct.clone());
                    continue;
                }
                let analysis = analyze_filters(
                    std::slice::from_ref(conjunct),
                    node.target_table.schema().fields(),
                    true,
                );
                match analysis.pushed_predicate {
                    Some(predicate) if !analysis.requires_residual => {
                        target_predicates.push(predicate);
                    }
                    _ => {
                        residual_predicates.push(conjunct.clone());
                    }
                }
            }
            if target_predicates.is_empty() {
                return Ok(Transformed::no(LogicalPlan::Filter(filter)));
            }
            let predicate = Predicate::and(target_predicates);
            let predicate = match &node.filter {
                Some(existing) => Predicate::and(vec![existing.clone(), predicate]),
                None => predicate,
            };
            let extension = LogicalPlan::Extension(Extension {
                node: Arc::new(node.with_filter(predicate)),
            });
            let rewritten = match projection {
                Some(projection) => LogicalPlan::Projection(Projection::try_new_with_schema(
                    projection.expr.clone(),
                    Arc::new(extension),
                    Arc::clone(&projection.schema),
                )?),
                None => extension,
            };
            let rewritten = match conjunction(residual_predicates) {
                Some(predicate) => {
                    LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(rewritten))?)
                }
                None => rewritten,
            };
            return Ok(Transformed::yes(rewritten));
        }

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
            let Some(provider) = provider.downcast_ref::<LateralVectorSearchTableProvider>() else {
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
    filter: Option<Predicate>,
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
            filter: None,
        }
    }

    fn with_filter(&self, filter: Predicate) -> Self {
        let mut node = self.clone();
        node.filter = Some(filter);
        node
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
            "LateralVectorSearch: column={}, limit={}, filter={:?}",
            self.target_column, self.limit, self.filter
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
            filter: self.filter.clone(),
        }))
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.name().hash(&mut state);
        self.input.hash(&mut state);
        self.target_table.location().hash(&mut state);
        self.target_column.hash(&mut state);
        self.query_vector_expr.hash(&mut state);
        self.limit.hash(&mut state);
        format!("{:?}", self.filter).hash(&mut state);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other.as_any().downcast_ref::<Self>().is_some_and(|other| {
            self.input == other.input
                && self.target_table.location() == other.target_table.location()
                && self.target_column == other.target_column
                && self.query_vector_expr == other.query_vector_expr
                && self.limit == other.limit
                && self.filter == other.filter
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
        let mut exec = LateralVectorSearchExec::new(
            Arc::clone(&physical_inputs[0]),
            node.target_table().clone(),
            Arc::clone(node.target_schema()),
            node.target_column().to_string(),
            query_vector_expr,
            node.limit(),
            Arc::new(node.schema().as_arrow().clone()),
        );
        if let Some(filter) = &node.filter {
            exec = exec.with_filter(filter.clone());
        }
        Ok(Some(Arc::new(exec)))
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
    filter: Option<Predicate>,
    prepared_filter_cache: Arc<ExecutionPreparedFilterCache>,
    plan_properties: Arc<PlanProperties>,
}

#[derive(Debug)]
struct ExecutionPreparedFilterEntry {
    context: Weak<TaskContext>,
    prepared_filter: Arc<OnceCell<PreparedVectorSearchFilter>>,
    partition_count: usize,
    completed_partitions: HashSet<usize>,
    active_partition_leases: HashMap<usize, usize>,
}

#[derive(Debug, Default)]
struct ExecutionPreparedFilterCache {
    // DataFusion passes the same TaskContext Arc to every partition of one
    // execution. Keep the prepared filter alive for that TaskContext so
    // sequential partitions resolve the same target snapshot. DataFusion 54
    // exposes no TaskContext drop hook, so one cache-level reaper observes the
    // weak contexts and removes subset executions after their context dies.
    entries: Mutex<Vec<ExecutionPreparedFilterEntry>>,
    reaper_running: AtomicBool,
}

#[derive(Clone)]
struct ExecutionPreparedFilterLease {
    prepared_filter: Arc<OnceCell<PreparedVectorSearchFilter>>,
    _completion: Arc<ExecutionPartitionCompletion>,
}

impl ExecutionPreparedFilterLease {
    fn new(
        cache: &Arc<ExecutionPreparedFilterCache>,
        context: Arc<TaskContext>,
        prepared_filter: Arc<OnceCell<PreparedVectorSearchFilter>>,
        partition: usize,
    ) -> Self {
        Self {
            prepared_filter: Arc::clone(&prepared_filter),
            _completion: Arc::new(ExecutionPartitionCompletion {
                cache: Arc::downgrade(cache),
                context,
                prepared_filter,
                partition,
            }),
        }
    }
}

struct ExecutionPartitionCompletion {
    cache: Weak<ExecutionPreparedFilterCache>,
    context: Arc<TaskContext>,
    prepared_filter: Arc<OnceCell<PreparedVectorSearchFilter>>,
    partition: usize,
}

impl Drop for ExecutionPartitionCompletion {
    fn drop(&mut self) {
        if let Some(cache) = self.cache.upgrade() {
            cache.finish_partition(&self.context, &self.prepared_filter, self.partition);
        }
    }
}

impl ExecutionPreparedFilterCache {
    fn for_execution(
        self: &Arc<Self>,
        context: &Arc<TaskContext>,
        partition: usize,
        partition_count: usize,
    ) -> ExecutionPreparedFilterLease {
        debug_assert!(partition < partition_count);
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.retain(|entry| entry.context.strong_count() > 0);
        for entry in entries.iter_mut() {
            let Some(entry_context) = entry.context.upgrade() else {
                continue;
            };
            if Arc::ptr_eq(&entry_context, context) && entry.partition_count == partition_count {
                entry.completed_partitions.remove(&partition);
                *entry.active_partition_leases.entry(partition).or_default() += 1;
                let lease = ExecutionPreparedFilterLease::new(
                    self,
                    Arc::clone(context),
                    Arc::clone(&entry.prepared_filter),
                    partition,
                );
                drop(entries);
                self.ensure_context_reaper();
                return lease;
            }
        }

        let prepared_filter = Arc::new(OnceCell::new());
        let mut active_partition_leases = HashMap::new();
        active_partition_leases.insert(partition, 1);
        entries.push(ExecutionPreparedFilterEntry {
            context: Arc::downgrade(context),
            prepared_filter: Arc::clone(&prepared_filter),
            partition_count,
            completed_partitions: HashSet::new(),
            active_partition_leases,
        });
        let lease = ExecutionPreparedFilterLease::new(
            self,
            Arc::clone(context),
            prepared_filter,
            partition,
        );
        drop(entries);
        self.ensure_context_reaper();
        lease
    }

    fn ensure_context_reaper(self: &Arc<Self>) {
        if self.reaper_running.swap(true, AtomicOrdering::AcqRel) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.reaper_running.store(false, AtomicOrdering::Release);
            return;
        };
        let cache = Arc::downgrade(self);
        runtime.spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let Some(cache) = cache.upgrade() else {
                    return;
                };
                if cache.prune_dead_contexts() {
                    cache.reaper_running.store(false, AtomicOrdering::Release);
                    let has_entries = !cache
                        .entries
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .is_empty();
                    if has_entries {
                        cache.ensure_context_reaper();
                    }
                    return;
                }
            }
        });
    }

    fn prune_dead_contexts(&self) -> bool {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        entries.retain(|entry| entry.context.strong_count() > 0);
        entries.is_empty()
    }

    fn finish_partition(
        &self,
        context: &Arc<TaskContext>,
        prepared_filter: &Arc<OnceCell<PreparedVectorSearchFilter>>,
        partition: usize,
    ) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(entry_index) = entries
            .iter()
            .position(|entry| Arc::ptr_eq(&entry.prepared_filter, prepared_filter))
        else {
            return;
        };
        let entry = &mut entries[entry_index];
        let Some(active_leases) = entry.active_partition_leases.get_mut(&partition) else {
            return;
        };
        *active_leases -= 1;
        if *active_leases == 0 {
            entry.active_partition_leases.remove(&partition);
            entry.completed_partitions.insert(partition);
        }
        if entry.active_partition_leases.is_empty()
            && (Arc::strong_count(context) == 1
                || entry.completed_partitions.len() == entry.partition_count)
        {
            entries.remove(entry_index);
        }
    }
}

struct ExecutionScopedStream {
    schema: ArrowSchemaRef,
    inner: Option<SendableRecordBatchStream>,
    lease: Option<ExecutionPreparedFilterLease>,
}

impl ExecutionScopedStream {
    fn new(
        schema: ArrowSchemaRef,
        inner: SendableRecordBatchStream,
        lease: ExecutionPreparedFilterLease,
    ) -> Self {
        Self {
            schema,
            inner: Some(inner),
            lease: Some(lease),
        }
    }

    fn finish(&mut self) {
        drop(self.inner.take());
        drop(self.lease.take());
    }
}

impl Stream for ExecutionScopedStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(inner) = &mut this.inner else {
            return Poll::Ready(None);
        };
        let result = inner.as_mut().poll_next(cx);
        if matches!(&result, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            this.finish();
        }
        result
    }
}

impl RecordBatchStream for ExecutionScopedStream {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for ExecutionScopedStream {
    fn drop(&mut self) {
        self.finish();
    }
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
            filter: None,
            prepared_filter_cache: Arc::new(ExecutionPreparedFilterCache::default()),
            plan_properties,
        }
    }

    fn with_filter(mut self, filter: Predicate) -> Self {
        self.filter = Some(filter);
        self
    }

    async fn process_batch(
        &self,
        batch: RecordBatch,
        prepared_filter: Option<&OnceCell<PreparedVectorSearchFilter>>,
    ) -> DFResult<RecordBatch> {
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

        let prepared_filter = match &self.filter {
            Some(filter) => {
                let prepared_filter = prepared_filter.ok_or_else(|| {
                    DataFusionError::Internal(
                        "filtered lateral vector search is missing execution state".to_string(),
                    )
                })?;
                let prepared = prepared_filter
                    .get_or_try_init(|| {
                        self.target_table
                            .prepare_vector_search_filter(filter.clone())
                    })
                    .await
                    .map_err(to_datafusion_error)?;
                Some(prepared)
            }
            None => None,
        };
        let target_table = prepared_filter
            .map(PreparedVectorSearchFilter::table)
            .unwrap_or(&self.target_table);
        let mut builder = target_table.new_batch_vector_search_builder();
        builder
            .with_vector_column(&self.target_column)
            .with_query_vectors(query_vectors)
            .with_limit(self.limit);
        if let Some(prepared_filter) = prepared_filter {
            builder.with_prepared_filter(prepared_filter.clone());
        }
        let results = builder.execute().await.map_err(to_datafusion_error)?;

        let (target_batch, target_row_id_to_index) =
            read_target_rows(target_table, &self.target_schema, &results).await?;

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
            "LateralVectorSearchExec: column={}, limit={}, filter={:?}",
            self.target_column, self.limit, self.filter
        )
    }
}

impl ExecutionPlan for LateralVectorSearchExec {
    fn name(&self) -> &str {
        "LateralVectorSearchExec"
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
        let mut exec = Self::new(
            children.remove(0),
            self.target_table.clone(),
            Arc::clone(&self.target_schema),
            self.target_column.clone(),
            Arc::clone(&self.query_vector_expr),
            self.limit,
            Arc::clone(&self.output_schema),
        );
        if let Some(filter) = &self.filter {
            exec = exec.with_filter(filter.clone());
        }
        exec.prepared_filter_cache = Arc::clone(&self.prepared_filter_cache);
        Ok(Arc::new(exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let prepared_filter = self.filter.as_ref().map(|_| {
            self.prepared_filter_cache.for_execution(
                &context,
                partition,
                self.input.output_partitioning().partition_count(),
            )
        });
        let prepared_filter_cell = prepared_filter
            .as_ref()
            .map(|lease| Arc::clone(&lease.prepared_filter));
        let exec = self.clone();
        let stream = input.then(move |batch| {
            let exec = exec.clone();
            let prepared_filter_cell = prepared_filter_cell.clone();
            async move {
                let batch = batch?;
                exec.process_batch(batch, prepared_filter_cell.as_deref())
                    .await
            }
        });
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            Box::pin(stream),
        ));
        Ok(match prepared_filter {
            Some(lease) => Box::pin(ExecutionScopedStream::new(
                self.output_schema.clone(),
                stream,
                lease,
            )),
            None => stream,
        })
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Arc<Statistics>> {
        Ok(Arc::new(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(&self.output_schema),
        }))
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
        .map_err(to_datafusion_error)?
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

#[cfg(test)]
mod tests {
    use super::*;

    struct ContextHoldingStream {
        schema: ArrowSchemaRef,
        _context: Arc<TaskContext>,
    }

    impl Stream for ContextHoldingStream {
        type Item = DFResult<RecordBatch>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl RecordBatchStream for ContextHoldingStream {
        fn schema(&self) -> ArrowSchemaRef {
            Arc::clone(&self.schema)
        }
    }

    #[test]
    fn prepared_filter_cache_releases_completed_execution() {
        let cache = Arc::new(ExecutionPreparedFilterCache::default());
        let context = Arc::new(TaskContext::default());
        let first = cache.for_execution(&context, 0, 2);
        let prepared_filter = Arc::downgrade(&first.prepared_filter);

        drop(first);
        let second = cache.for_execution(&context, 1, 2);
        assert!(Arc::ptr_eq(
            &prepared_filter
                .upgrade()
                .expect("an unfinished execution should retain its prepared filter"),
            &second.prepared_filter
        ));

        drop(second);
        assert!(
            prepared_filter.upgrade().is_none(),
            "finishing every partition should release the prepared filter even while the task context remains alive"
        );
        assert!(cache
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());
    }

    #[test]
    fn prepared_filter_cache_releases_subset_execution_after_context_drop() {
        let cache = Arc::new(ExecutionPreparedFilterCache::default());
        let context = Arc::new(TaskContext::default());
        let lease = cache.for_execution(&context, 0, 4);
        let prepared_filter = Arc::downgrade(&lease.prepared_filter);

        drop(context);
        drop(lease);

        assert!(
            prepared_filter.upgrade().is_none(),
            "unstarted declared partitions must not retain a completed execution"
        );
        assert!(cache
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());
    }

    #[tokio::test]
    async fn prepared_filter_cache_releases_when_context_drops_after_stream() {
        let cache = Arc::new(ExecutionPreparedFilterCache::default());
        let context = Arc::new(TaskContext::default());
        let lease = cache.for_execution(&context, 0, 4);
        let prepared_filter = Arc::downgrade(&lease.prepared_filter);

        drop(lease);
        assert!(
            prepared_filter.upgrade().is_some(),
            "the retained context must keep sequential partition reuse possible"
        );
        drop(context);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while prepared_filter.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping the execution context must trigger cache cleanup");
        assert!(cache
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());
    }

    #[test]
    fn execution_stream_cancellation_drops_inner_before_cache_lease() {
        let cache = Arc::new(ExecutionPreparedFilterCache::default());
        let context = Arc::new(TaskContext::default());
        let lease = cache.for_execution(&context, 0, 4);
        let prepared_filter = Arc::downgrade(&lease.prepared_filter);
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let inner: SendableRecordBatchStream = Box::pin(ContextHoldingStream {
            schema: Arc::clone(&schema),
            _context: Arc::clone(&context),
        });
        let stream = ExecutionScopedStream::new(schema, inner, lease);

        drop(context);
        drop(stream);

        assert!(
            prepared_filter.upgrade().is_none(),
            "cancellation must drop the child stream's context before releasing the cache lease"
        );
    }

    #[test]
    fn prepared_filter_cache_evicts_only_the_completed_execution() {
        let cache = Arc::new(ExecutionPreparedFilterCache::default());
        let first_context = Arc::new(TaskContext::default());
        let second_context = Arc::new(TaskContext::default());
        let first = cache.for_execution(&first_context, 0, 1);
        let second = cache.for_execution(&second_context, 0, 1);
        let first_filter = Arc::downgrade(&first.prepared_filter);
        let second_filter = Arc::downgrade(&second.prepared_filter);

        drop(first);

        assert!(first_filter.upgrade().is_none());
        assert!(
            second_filter.upgrade().is_some(),
            "completing one execution must not evict another execution's filter"
        );
        assert_eq!(
            cache
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            1
        );

        drop(second);
        assert!(second_filter.upgrade().is_none());
    }

    #[test]
    fn prepared_filter_cache_waits_for_all_lease_clones() {
        let cache = Arc::new(ExecutionPreparedFilterCache::default());
        let context = Arc::new(TaskContext::default());
        let lease = cache.for_execution(&context, 0, 1);
        let lease_clone = lease.clone();
        let prepared_filter = Arc::downgrade(&lease.prepared_filter);

        drop(lease);
        assert!(
            prepared_filter.upgrade().is_some(),
            "in-flight batch futures may retain a cloned lease"
        );

        drop(lease_clone);
        assert!(prepared_filter.upgrade().is_none());
    }
}
