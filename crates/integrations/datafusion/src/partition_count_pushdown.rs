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

//! Answers `SELECT <partition cols>, COUNT(*) ... GROUP BY <partition cols>` from
//! manifests.
//!
//! DataFusion's `aggregate_statistics` only folds an ungrouped `COUNT(*)`, and it
//! still needs the scan planned first — every live file's metadata and column
//! statistics held as splits, which is what runs out of memory on very large
//! tables. A grouped count additionally opens every data file.
//!
//! Whenever the grouping keys are partition columns and the filter is decided by
//! partition values alone, the answer is a function of the manifests. This rule
//! rewrites
//!
//! ```text
//! Aggregate: groupBy=[[t.dt]], aggr=[[count(1)]]
//!   TableScan: t, full_filters=[t.region = 'eu']
//! ```
//!
//! into a `SUM` over [`Table::partition_row_counts_with_filter`], which streams
//! manifests in bounded memory and counts data-evolution row ranges once:
//!
//! ```text
//! Projection: t.dt, coalesce(sum(row_count), 0) AS count(1)
//!   Aggregate: groupBy=[[t.dt]], aggr=[[sum(row_count)]]
//!     TableScan: t (PartitionRowCountProvider)
//! ```
//!
//! The provider returns a lazy [`PartitionRowCountExec`], so physical planning and
//! `EXPLAIN` do not read manifests; metadata I/O starts when execution polls it.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{
    internal_err, project_schema, Column, DataFusionError, Result as DFResult, ScalarValue,
};
use datafusion::datasource::{provider_as_source, source_as_provider, TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::expr_fn::{count, sum};
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{
    col, lit, Aggregate, Expr, LogicalPlan, LogicalPlanBuilder, TableScan, TableSource,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use datafusion::sql::TableReference;
use futures::{stream, StreamExt, TryStreamExt};
use paimon::spec::{CoreOptions, DataField, Datum, Predicate};
use paimon::table::Table;

use crate::error::to_datafusion_error;
use crate::filter_pushdown::analyze_filters;
use crate::physical_plan::scan::datum_to_scalar;
use crate::table::PaimonTableProvider;

const ROW_COUNT_COLUMN: &str = "__paimon_partition_row_count";

#[derive(Debug)]
pub(crate) struct PushDownPartitionCount;

impl OptimizerRule for PushDownPartitionCount {
    fn name(&self) -> &str {
        "paimon_push_down_partition_count"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(aggregate) = &plan else {
            return Ok(Transformed::no(plan));
        };
        match rewrite_aggregate(aggregate)? {
            Some(rewritten) => Ok(Transformed::yes(rewritten)),
            None => Ok(Transformed::no(plan)),
        }
    }
}

fn rewrite_aggregate(aggregate: &Aggregate) -> DFResult<Option<LogicalPlan>> {
    let LogicalPlan::TableScan(scan) = aggregate.input.as_ref() else {
        return Ok(None);
    };
    if scan.fetch.is_some() || !aggregate.aggr_expr.iter().all(is_count_star) {
        return Ok(None);
    }
    let Ok(provider) = source_as_provider(&scan.source) else {
        return Ok(None);
    };
    let Some(paimon) = provider.downcast_ref::<PaimonTableProvider>() else {
        return Ok(None);
    };
    let table = paimon.table();
    let table_schema = table.schema();
    // Manifest row counts of a primary-key table are physical: several versions
    // of a key count separately until they are merged at read time.
    if CoreOptions::new(table_schema.options()).is_format_table()
        || !table_schema.primary_keys().is_empty()
    {
        return Ok(None);
    }

    let partition_keys = table_schema.partition_keys();
    let groups_by_partition_columns = aggregate
        .group_expr
        .iter()
        .all(|expr| matches!(expr, Expr::Column(column) if partition_keys.contains(&column.name)));
    if !groups_by_partition_columns {
        return Ok(None);
    }

    // Every filter must be decided by partition values alone, with nothing left
    // for DataFusion to re-check on rows.
    let analysis = analyze_filters(&scan.filters, table_schema.fields(), true);
    if analysis.requires_residual {
        return Ok(None);
    }
    match &analysis.pushed_predicate {
        Some(predicate) => {
            if !table.new_read_builder().is_exact_filter_pushdown(predicate) {
                return Ok(None);
            }
        }
        None if !scan.filters.is_empty() => return Ok(None),
        None => {}
    }

    let partition_fields = table_schema.partition_fields();
    let arrow_schema = paimon.schema();
    let mut fields = Vec::with_capacity(partition_fields.len() + 1);
    for field in &partition_fields {
        let Ok(arrow_field) = arrow_schema.field_with_name(field.name()) else {
            return Ok(None);
        };
        fields.push(arrow_field.clone());
    }
    fields.push(Field::new(ROW_COUNT_COLUMN, DataType::Int64, false));

    let counts = PartitionRowCountProvider {
        table: table.clone(),
        partition_fields,
        predicate: analysis.pushed_predicate,
        schema: Arc::new(Schema::new(fields)),
        source: Arc::clone(&scan.source),
        table_name: scan.table_name.clone(),
        filters: scan.filters.clone(),
    };
    let counts_scan = LogicalPlan::TableScan(TableScan::try_new(
        scan.table_name.clone(),
        provider_as_source(Arc::new(counts)),
        None,
        vec![],
        None,
    )?);

    let row_count = Expr::Column(Column::new(Some(scan.table_name.clone()), ROW_COUNT_COLUMN));
    let summed = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(counts_scan),
        aggregate.group_expr.clone(),
        vec![sum(row_count)],
    )?);

    // Reproduce the original output columns exactly: grouping keys keep their
    // names, and each COUNT(*) reads the one SUM. An ungrouped aggregate over no
    // partitions sums to NULL where COUNT(*) is 0.
    let group_len = aggregate.group_expr.len();
    let summed_column = Expr::Column(Column::from(summed.schema().qualified_field(group_len)));
    let mut projection = Vec::with_capacity(aggregate.schema.fields().len());
    for index in 0..aggregate.schema.fields().len() {
        let (qualifier, field) = aggregate.schema.qualified_field(index);
        if index < group_len {
            projection.push(Expr::Column(Column::new(qualifier.cloned(), field.name())));
        } else {
            projection.push(
                coalesce(vec![summed_column.clone(), lit(0i64)])
                    .alias_qualified(qualifier.cloned(), field.name()),
            );
        }
    }
    LogicalPlanBuilder::from(summed)
        .project(projection)?
        .build()
        .map(Some)
}

/// `COUNT(*)` / `COUNT(<non-null literal>)` with no DISTINCT, FILTER or ORDER BY.
fn is_count_star(expr: &Expr) -> bool {
    let expr = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };
    let Expr::AggregateFunction(AggregateFunction { func, params }) = expr else {
        return false;
    };
    func == &count_udaf()
        && !params.distinct
        && params.filter.is_none()
        && params.order_by.is_empty()
        && matches!(params.args.as_slice(), [Expr::Literal(value, _)] if !value.is_null())
}

/// One row per live partition: its typed partition values and its real row count.
/// Planning only constructs a lazy [`PartitionRowCountExec`].
struct PartitionRowCountProvider {
    table: Table,
    partition_fields: Vec<DataField>,
    predicate: Option<Predicate>,
    schema: SchemaRef,
    // The scan this provider replaced, kept for partitions whose count the
    // manifests cannot give exactly.
    source: Arc<dyn TableSource>,
    table_name: TableReference,
    filters: Vec<Expr>,
}

impl std::fmt::Debug for PartitionRowCountProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionRowCountProvider")
            .field("table", &self.table.identifier())
            .field("predicate", &self.predicate)
            .finish()
    }
}

#[async_trait]
impl TableProvider for PartitionRowCountProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let state = state
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "partition count execution requires a SessionState".to_string(),
                )
            })?
            .clone();
        Ok(Arc::new(PartitionRowCountExec::new(
            self,
            projection.cloned(),
            state,
        )?))
    }
}

#[derive(Clone)]
struct PartitionRowCountExec {
    table: Table,
    partition_fields: Vec<DataField>,
    predicate: Option<Predicate>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    output_schema: SchemaRef,
    source: Arc<dyn TableSource>,
    table_name: TableReference,
    filters: Vec<Expr>,
    state: SessionState,
    plan_properties: Arc<PlanProperties>,
}

impl PartitionRowCountExec {
    fn new(
        provider: &PartitionRowCountProvider,
        projection: Option<Vec<usize>>,
        state: SessionState,
    ) -> DFResult<Self> {
        let output_schema = project_schema(&provider.schema, projection.as_ref())?;
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            table: provider.table.clone(),
            partition_fields: provider.partition_fields.clone(),
            predicate: provider.predicate.clone(),
            schema: Arc::clone(&provider.schema),
            projection,
            output_schema,
            source: Arc::clone(&provider.source),
            table_name: provider.table_name.clone(),
            filters: provider.filters.clone(),
            state,
            plan_properties,
        })
    }

    async fn execute_stream(
        &self,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let table = self.table.clone();
        let predicate = self.predicate.clone();
        let counts = crate::runtime::await_with_runtime(async move {
            table.partition_row_counts_with_filter(predicate).await
        })
        .await
        .map_err(to_datafusion_error)?;

        // A deletion vector without a recorded cardinality leaves a partition's
        // count unknown to the manifests; only reading can answer then.
        if counts.iter().any(|count| count.record_count.is_none()) {
            let plan = crate::runtime::await_with_runtime(self.scan_by_reading()).await?;
            if plan.schema() != self.output_schema {
                return internal_err!(
                    "partition count fallback schema mismatch: expected {:?}, got {:?}",
                    self.output_schema,
                    plan.schema()
                );
            }
            let streams = (0..plan.output_partitioning().partition_count())
                .map(|partition| plan.execute(partition, Arc::clone(&context)))
                .collect::<DFResult<Vec<_>>>()?;
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.output_schema),
                Box::pin(stream::iter(streams).flatten()),
            )));
        }

        let batch = self.counts_to_batch(&counts)?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            Box::pin(stream::iter([Ok(batch)])),
        )))
    }

    fn counts_to_batch(
        &self,
        counts: &[paimon::table::PartitionRowCount],
    ) -> DFResult<RecordBatch> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for (index, field) in self.partition_fields.iter().enumerate() {
            let arrow_type = self.schema.field(index).data_type();
            let mut values = Vec::with_capacity(counts.len());
            for count in counts {
                let datum = count
                    .partition_row
                    .get_datum(index, field.data_type())
                    .map_err(to_datafusion_error)?;
                values.push(match datum {
                    None => ScalarValue::try_from(arrow_type)?,
                    Some(datum) => {
                        partition_datum_to_scalar(datum, arrow_type).ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "cannot represent partition column '{}' as {arrow_type}",
                                field.name()
                            ))
                        })?
                    }
                });
            }
            columns.push(if values.is_empty() {
                datafusion::arrow::array::new_empty_array(arrow_type)
            } else {
                ScalarValue::iter_to_array(values)?
            });
        }
        columns.push(Arc::new(Int64Array::from_iter_values(
            counts.iter().filter_map(|count| count.record_count),
        )));

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        match &self.projection {
            Some(projection) => Ok(batch.project(projection)?),
            None => Ok(batch),
        }
    }

    /// The same rows, computed the ordinary way: count the original scan per partition.
    async fn scan_by_reading(&self) -> DFResult<Arc<dyn ExecutionPlan>> {
        let source_schema = self.source.schema();
        let partition_indices = self
            .partition_fields
            .iter()
            .map(|field| source_schema.index_of(field.name()))
            .collect::<Result<Vec<_>, _>>()?;
        let scan = LogicalPlan::TableScan(TableScan::try_new(
            self.table_name.clone(),
            Arc::clone(&self.source),
            Some(partition_indices),
            self.filters.clone(),
            None,
        )?);

        let group_by: Vec<Expr> = self
            .partition_fields
            .iter()
            .map(|field| col(Column::new(Some(self.table_name.clone()), field.name())))
            .collect();
        let mut output = group_by.clone();
        output.push(col(Column::from_name(ROW_COUNT_COLUMN)));
        let selected = match &self.projection {
            Some(indices) => indices.iter().map(|&index| output[index].clone()).collect(),
            None => output.clone(),
        };
        let counted = LogicalPlanBuilder::from(scan)
            .aggregate(group_by, vec![count(lit(1i64)).alias(ROW_COUNT_COLUMN)])?
            .project(selected)?
            .build()?;

        // Planned directly, not through the optimizer: this rule must not
        // rewrite its own fallback.
        self.state
            .query_planner()
            .create_physical_plan(&counted, &self.state)
            .await
    }
}

impl std::fmt::Debug for PartitionRowCountExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionRowCountExec")
            .field("table", &self.table.identifier())
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl DisplayAs for PartitionRowCountExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "PartitionRowCountExec: table={}",
            self.table.identifier()
        )
    }
}

impl ExecutionPlan for PartitionRowCountExec {
    fn name(&self) -> &str {
        "PartitionRowCountExec"
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
            return internal_err!("PartitionRowCountExec is a leaf and takes no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "PartitionRowCountExec has a single partition, got partition {partition}"
            );
        }
        let exec = self.clone();
        let stream = stream::once(async move { exec.execute_stream(context).await }).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            Box::pin(stream),
        )))
    }
}

fn partition_datum_to_scalar(value: Datum, data_type: &DataType) -> Option<ScalarValue> {
    match (value, data_type) {
        (Datum::Bytes(value), DataType::Binary) => Some(ScalarValue::Binary(Some(value))),
        (value, data_type) => datum_to_scalar(value, data_type),
    }
}
