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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::compute::{cast, filter_record_batch};
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, ScalarValue, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::parquet::can_expr_be_pushed_down_with_schemas;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, InListExpr, Literal,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use paimon::spec::{DataField, Datum, MergeEngine, Predicate, PredicateBuilder};
use paimon::table::{ScanTrace, Table};
use paimon::DataSplit;

use crate::config::PaimonConfig;
use crate::error::to_datafusion_error;
use crate::filter_pushdown::scalar_to_datum;

const RUNTIME_FILTER_WAIT_MIN_ROWS: usize = 250_000;
const RUNTIME_FILTER_WAIT_TIMEOUT: Duration = Duration::from_secs(1);

fn scan_applies_row_filter(config: &ConfigOptions) -> bool {
    config
        .extensions
        .get::<PaimonConfig>()
        .is_some_and(|config| config.read.row_filter)
}

fn to_datafusion_batch(batch: RecordBatch, schema: &ArrowSchemaRef) -> DFResult<RecordBatch> {
    if batch.num_columns() != schema.fields().len() {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "Paimon reader returned {} columns for DataFusion schema with {} fields",
            batch.num_columns(),
            schema.fields().len()
        )));
    }

    let row_count = batch.num_rows();
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column.as_ref(), field.data_type()).map_err(Into::into)
            }
        })
        .collect::<DFResult<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));

    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(Into::into)
}

async fn runtime_pruning_predicate(
    filters: &[Arc<dyn PhysicalExpr>],
    fields: &[DataField],
    case_sensitive: bool,
    wait_timeout: Duration,
) -> Option<Predicate> {
    let mut pending = filters.to_vec();
    let mut dynamic_filters = Vec::new();
    while let Some(expr) = pending.pop() {
        pending.extend(expr.children().into_iter().cloned());
        if expr.downcast_ref::<DynamicFilterPhysicalExpr>().is_some() {
            dynamic_filters.push(expr);
        }
    }

    let mut waiters = FuturesUnordered::new();
    for expr in dynamic_filters {
        waiters.push(async move {
            let dynamic = expr
                .downcast_ref::<DynamicFilterPhysicalExpr>()
                .expect("only dynamic filters are queued");
            dynamic.wait_complete().await;
            dynamic.expression_id()
        });
    }

    let deadline = tokio::time::Instant::now() + wait_timeout;
    let mut completed_dynamic_filters = HashSet::new();
    while !waiters.is_empty() {
        match tokio::time::timeout_at(deadline, waiters.next()).await {
            Ok(Some(Some(expression_id))) => {
                completed_dynamic_filters.insert(expression_id);
            }
            Ok(Some(None)) => {}
            Ok(None) | Err(_) => break,
        }
    }

    let predicate_builder = PredicateBuilder::new_with_case_sensitive(fields, case_sensitive);
    let mut predicates = Vec::new();
    for filter in filters {
        collect_runtime_pruning_predicates(
            filter.as_ref(),
            fields,
            &predicate_builder,
            case_sensitive,
            &completed_dynamic_filters,
            &mut predicates,
        );
    }
    (!predicates.is_empty()).then(|| Predicate::and(predicates))
}

fn runtime_filter_wait_timeout(estimated_rows: usize) -> Duration {
    if estimated_rows > RUNTIME_FILTER_WAIT_MIN_ROWS {
        RUNTIME_FILTER_WAIT_TIMEOUT
    } else {
        Duration::ZERO
    }
}

fn collect_runtime_pruning_predicates(
    expr: &dyn PhysicalExpr,
    fields: &[DataField],
    predicate_builder: &PredicateBuilder,
    case_sensitive: bool,
    completed_dynamic_filters: &HashSet<u64>,
    predicates: &mut Vec<Predicate>,
) {
    if let Some(dynamic) = expr.downcast_ref::<DynamicFilterPhysicalExpr>() {
        if dynamic
            .expression_id()
            .is_none_or(|id| !completed_dynamic_filters.contains(&id))
        {
            return;
        }
        if let Ok(current) = dynamic.current() {
            collect_runtime_pruning_predicates(
                current.as_ref(),
                fields,
                predicate_builder,
                case_sensitive,
                completed_dynamic_filters,
                predicates,
            );
        }
        return;
    }

    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        if binary.op() == &Operator::And {
            collect_runtime_pruning_predicates(
                binary.left().as_ref(),
                fields,
                predicate_builder,
                case_sensitive,
                completed_dynamic_filters,
                predicates,
            );
            collect_runtime_pruning_predicates(
                binary.right().as_ref(),
                fields,
                predicate_builder,
                case_sensitive,
                completed_dynamic_filters,
                predicates,
            );
        } else if let Some(predicate) =
            translate_runtime_comparison(binary, fields, predicate_builder, case_sensitive)
        {
            predicates.push(predicate);
        }
        return;
    }

    if let Some(in_list) = expr.downcast_ref::<InListExpr>() {
        if let Some(predicate) =
            translate_runtime_in_list(in_list, fields, predicate_builder, case_sensitive)
        {
            predicates.push(predicate);
        }
    }
}

fn translate_runtime_comparison(
    binary: &BinaryExpr,
    fields: &[DataField],
    predicate_builder: &PredicateBuilder,
    case_sensitive: bool,
) -> Option<Predicate> {
    let direct = runtime_column_literal(
        binary.left().as_ref(),
        binary.right().as_ref(),
        fields,
        case_sensitive,
    )
    .map(|(field, datum)| (*binary.op(), field, datum));
    let comparison = direct.or_else(|| {
        runtime_column_literal(
            binary.right().as_ref(),
            binary.left().as_ref(),
            fields,
            case_sensitive,
        )
        .and_then(|(field, datum)| {
            reverse_runtime_comparison(*binary.op()).map(|op| (op, field, datum))
        })
    })?;

    let (op, field, datum) = comparison;
    if matches!(
        field.data_type(),
        paimon::spec::DataType::Binary(_) | paimon::spec::DataType::VarBinary(_)
    ) && matches!(
        op,
        Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    ) {
        // Arrow compares binary values as unsigned bytes, while Paimon follows
        // Java's signed-byte ordering. Range predicates could therefore prune
        // rows that DataFusion would keep; equality predicates remain safe.
        return None;
    }
    match op {
        Operator::Eq => predicate_builder.equal(field.name(), datum).ok(),
        Operator::NotEq => predicate_builder.not_equal(field.name(), datum).ok(),
        Operator::Lt => predicate_builder.less_than(field.name(), datum).ok(),
        Operator::LtEq => predicate_builder.less_or_equal(field.name(), datum).ok(),
        Operator::Gt => predicate_builder.greater_than(field.name(), datum).ok(),
        Operator::GtEq => predicate_builder.greater_or_equal(field.name(), datum).ok(),
        _ => None,
    }
}

fn translate_runtime_in_list(
    in_list: &InListExpr,
    fields: &[DataField],
    predicate_builder: &PredicateBuilder,
    case_sensitive: bool,
) -> Option<Predicate> {
    let column = in_list.expr().downcast_ref::<Column>()?;
    let field = resolve_runtime_field(column.name(), fields, case_sensitive)?;
    let literals = in_list
        .list()
        .iter()
        .map(|expr| {
            let literal = expr.downcast_ref::<Literal>()?;
            if literal.value().is_null() {
                return None;
            }
            scalar_to_datum(literal.value(), field.data_type())
        })
        .collect::<Option<Vec<_>>>()?;

    if in_list.negated() {
        predicate_builder.is_not_in(field.name(), literals).ok()
    } else {
        predicate_builder.is_in(field.name(), literals).ok()
    }
}

fn runtime_column_literal<'a>(
    column: &dyn PhysicalExpr,
    literal: &dyn PhysicalExpr,
    fields: &'a [DataField],
    case_sensitive: bool,
) -> Option<(&'a DataField, Datum)> {
    let column = column.downcast_ref::<Column>()?;
    let literal = literal.downcast_ref::<Literal>()?;
    if literal.value().is_null() {
        return None;
    }
    let field = resolve_runtime_field(column.name(), fields, case_sensitive)?;
    let datum = scalar_to_datum(literal.value(), field.data_type())?;
    Some((field, datum))
}

fn resolve_runtime_field<'a>(
    name: &str,
    fields: &'a [DataField],
    case_sensitive: bool,
) -> Option<&'a DataField> {
    if case_sensitive {
        fields.iter().find(|field| field.name() == name)
    } else {
        let mut matches = fields
            .iter()
            .filter(|field| field.name().eq_ignore_ascii_case(name));
        let field = matches.next()?;
        matches.next().is_none().then_some(field)
    }
}

fn reverse_runtime_comparison(op: Operator) -> Option<Operator> {
    match op {
        Operator::Eq => Some(Operator::Eq),
        Operator::NotEq => Some(Operator::NotEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        _ => None,
    }
}

#[derive(Debug)]
struct ColumnStatsAccumulator {
    min_value: Option<Datum>,
    max_value: Option<Datum>,
    null_count: usize,
    min_valid: bool,
    max_valid: bool,
    null_valid: bool,
}

impl Default for ColumnStatsAccumulator {
    fn default() -> Self {
        Self {
            min_value: None,
            max_value: None,
            null_count: 0,
            min_valid: true,
            max_valid: true,
            null_valid: true,
        }
    }
}

impl ColumnStatsAccumulator {
    fn add_file(&mut self, file: &paimon::spec::DataFileMeta, field: &DataField, table: &Table) {
        if file.row_count == 0 {
            return;
        }
        let Some(stats) =
            file.value_stats_for_field(table.schema().id(), table.schema().fields(), field)
        else {
            self.min_valid = false;
            self.max_valid = false;
            self.null_valid = false;
            return;
        };

        let all_null = stats.null_count == Some(file.row_count);
        match stats.min_value {
            Some(value) => match &self.min_value {
                Some(current) => match value.partial_cmp(current) {
                    Some(std::cmp::Ordering::Less) => self.min_value = Some(value),
                    Some(_) => {}
                    None => self.min_valid = false,
                },
                None => self.min_value = Some(value),
            },
            None if !all_null => self.min_valid = false,
            None => {}
        }
        match stats.max_value {
            Some(value) => match &self.max_value {
                Some(current) => match value.partial_cmp(current) {
                    Some(std::cmp::Ordering::Greater) => self.max_value = Some(value),
                    Some(_) => {}
                    None => self.max_valid = false,
                },
                None => self.max_value = Some(value),
            },
            None if !all_null => self.max_valid = false,
            None => {}
        }
        match stats
            .null_count
            .and_then(|count| usize::try_from(count).ok())
            .and_then(|count| self.null_count.checked_add(count))
        {
            Some(count) => self.null_count = count,
            None => self.null_valid = false,
        }
    }

    fn finish(self, data_type: &ArrowDataType, exact_null_count: bool) -> ColumnStatistics {
        let min_value = if self.min_valid {
            self.min_value
                .and_then(|value| datum_to_scalar(value, data_type))
                .map(Precision::Inexact)
                .unwrap_or(Precision::Absent)
        } else {
            Precision::Absent
        };
        let max_value = if self.max_valid {
            self.max_value
                .and_then(|value| datum_to_scalar(value, data_type))
                .map(Precision::Inexact)
                .unwrap_or(Precision::Absent)
        } else {
            Precision::Absent
        };
        let null_count = if exact_null_count && self.null_valid {
            Precision::Exact(self.null_count)
        } else {
            Precision::Absent
        };

        ColumnStatistics {
            null_count,
            min_value,
            max_value,
            distinct_count: Precision::Absent,
            sum_value: Precision::Absent,
            byte_size: Precision::Absent,
        }
    }
}

fn datum_to_scalar(value: Datum, data_type: &ArrowDataType) -> Option<ScalarValue> {
    match (value, data_type) {
        (Datum::Bool(value), ArrowDataType::Boolean) => Some(ScalarValue::Boolean(Some(value))),
        (Datum::TinyInt(value), ArrowDataType::Int8) => Some(ScalarValue::Int8(Some(value))),
        (Datum::SmallInt(value), ArrowDataType::Int16) => Some(ScalarValue::Int16(Some(value))),
        (Datum::Int(value), ArrowDataType::Int32) => Some(ScalarValue::Int32(Some(value))),
        (Datum::Long(value), ArrowDataType::Int64) => Some(ScalarValue::Int64(Some(value))),
        (Datum::Float(value), ArrowDataType::Float32) => Some(ScalarValue::Float32(Some(value))),
        (Datum::Double(value), ArrowDataType::Float64) => Some(ScalarValue::Float64(Some(value))),
        (Datum::String(value), ArrowDataType::Utf8) => Some(ScalarValue::Utf8(Some(value))),
        (Datum::String(value), ArrowDataType::Utf8View) => Some(ScalarValue::Utf8View(Some(value))),
        (Datum::String(value), ArrowDataType::LargeUtf8) => {
            Some(ScalarValue::LargeUtf8(Some(value)))
        }
        (Datum::Date(value), ArrowDataType::Date32) => Some(ScalarValue::Date32(Some(value))),
        (Datum::Time(value), ArrowDataType::Time32(TimeUnit::Millisecond)) => {
            Some(ScalarValue::Time32Millisecond(Some(value)))
        }
        (Datum::Timestamp { millis, nanos }, ArrowDataType::Timestamp(unit, timezone))
        | (
            Datum::LocalZonedTimestamp { millis, nanos },
            ArrowDataType::Timestamp(unit, timezone),
        ) => {
            let value = match unit {
                TimeUnit::Second => millis.checked_div(1_000)?,
                TimeUnit::Millisecond => millis,
                TimeUnit::Microsecond => millis
                    .checked_mul(1_000)?
                    .checked_add(i64::from(nanos / 1_000))?,
                TimeUnit::Nanosecond => millis
                    .checked_mul(1_000_000)?
                    .checked_add(i64::from(nanos))?,
            };
            match unit {
                TimeUnit::Second => {
                    Some(ScalarValue::TimestampSecond(Some(value), timezone.clone()))
                }
                TimeUnit::Millisecond => Some(ScalarValue::TimestampMillisecond(
                    Some(value),
                    timezone.clone(),
                )),
                TimeUnit::Microsecond => Some(ScalarValue::TimestampMicrosecond(
                    Some(value),
                    timezone.clone(),
                )),
                TimeUnit::Nanosecond => Some(ScalarValue::TimestampNanosecond(
                    Some(value),
                    timezone.clone(),
                )),
            }
        }
        (
            Datum::Decimal {
                unscaled,
                precision,
                scale,
            },
            ArrowDataType::Decimal128(_, _),
        ) => Some(ScalarValue::Decimal128(
            Some(unscaled),
            u8::try_from(precision).ok()?,
            i8::try_from(scale).ok()?,
        )),
        // Paimon compares bytes using Java's signed-byte ordering, while Arrow compares
        // binary values using unsigned lexicographic ordering. Publishing the manifest
        // bounds would therefore be unsound for values crossing 0x7f/0x80.
        (
            Datum::Bytes(_),
            ArrowDataType::Binary | ArrowDataType::BinaryView | ArrowDataType::LargeBinary,
        ) => None,
        _ => None,
    }
}

/// Execution plan that scans a Paimon table with optional column projection.
///
/// Planning is performed eagerly in [`super::super::table::PaimonTableProvider::scan`],
/// and the resulting splits are distributed across DataFusion execution partitions
/// so that DataFusion can schedule them in parallel.
#[derive(Debug, Clone)]
pub struct PaimonTableScan {
    table: Table,
    /// Full Paimon read type for nested or connector-defined projections.
    read_type: Vec<DataField>,
    /// Filter translated from DataFusion expressions and reused during execute()
    /// so reader-side pruning reaches the actual read path.
    pushed_predicate: Option<Predicate>,
    /// Pre-planned partition assignments: `planned_partitions[i]` contains the
    /// Paimon splits that DataFusion partition `i` will read.
    /// Wrapped in `Arc` to avoid deep-cloning `DataSplit` metadata in `execute()`.
    planned_partitions: Vec<Arc<[DataSplit]>>,
    plan_properties: Arc<PlanProperties>,
    /// Optional limit hint pushed to paimon-core planning.
    limit: Option<usize>,
    /// Whether the pushed predicate is exact (no residual filtering needed).
    /// When true and all splits have known merged_row_count, statistics can be exact.
    filter_exact: bool,
    /// Metadata-pruning trace captured during eager scan planning.
    scan_trace: Option<ScanTrace>,
    /// Human-readable Variant extraction summary for explain output.
    pushed_variants: Option<String>,
    /// Column-name case sensitivity carried from planning to execution so the
    /// read path resolves names the same way the scan was planned.
    case_sensitive: bool,
    /// Physical filters retained from DataFusion's runtime filter-pushdown pass.
    /// They are always available for conservative reader pruning and are
    /// evaluated exactly only when Paimon row filtering is enabled.
    runtime_filters: Vec<Arc<dyn PhysicalExpr>>,
    /// Planning-time decision that this scan evaluates `runtime_filters`
    /// exactly. Stored on the plan so execution cannot observe a different
    /// session setting after the parent FilterExec has been removed.
    apply_row_filter: bool,
}

impl PaimonTableScan {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        schema: ArrowSchemaRef,
        table: Table,
        read_type: Vec<DataField>,
        pushed_predicate: Option<Predicate>,
        planned_partitions: Vec<Arc<[DataSplit]>>,
        limit: Option<usize>,
        filter_exact: bool,
        scan_trace: Option<ScanTrace>,
        pushed_variants: Option<String>,
        case_sensitive: bool,
    ) -> Self {
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(planned_partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            table,
            read_type,
            pushed_predicate,
            planned_partitions,
            plan_properties,
            limit,
            filter_exact,
            scan_trace,
            pushed_variants,
            case_sensitive,
            runtime_filters: Vec::new(),
            apply_row_filter: false,
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

    #[cfg(test)]
    pub(crate) fn filter_exact(&self) -> bool {
        self.filter_exact
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    fn manifest_column_statistics(&self, partitions: &[Arc<[DataSplit]>]) -> Vec<ColumnStatistics> {
        if self.read_type.len() != self.schema().fields().len() {
            return Statistics::unknown_column(&self.schema());
        }

        let Ok(merge_engine) = self.table.schema().core_options().merge_engine() else {
            return Statistics::unknown_column(&self.schema());
        };
        if merge_engine == MergeEngine::Aggregation {
            // Aggregate functions such as SUM can produce logical values outside every
            // physical file's min/max bounds.
            return Statistics::unknown_column(&self.schema());
        }

        let exact_null_counts = self.runtime_filters.is_empty()
            && (self.table.schema().primary_keys().is_empty()
                || merge_engine == MergeEngine::Deduplicate)
            && self.pushed_predicate.is_none()
            && self.limit.is_none()
            && partitions
                .iter()
                .flat_map(|splits| splits.iter())
                .all(|split| {
                    split.raw_convertible()
                        && split.row_ranges().is_none()
                        && split
                            .data_deletion_files()
                            .is_none_or(|files| files.iter().all(Option::is_none))
                });
        let mut accumulators = (0..self.read_type.len())
            .map(|_| ColumnStatsAccumulator::default())
            .collect::<Vec<_>>();

        for file in partitions
            .iter()
            .flat_map(|splits| splits.iter())
            .flat_map(|split| split.data_files())
        {
            for (accumulator, field) in accumulators.iter_mut().zip(&self.read_type) {
                accumulator.add_file(file, field, &self.table);
            }
        }

        accumulators
            .into_iter()
            .zip(self.schema().fields())
            .map(|(accumulator, field)| accumulator.finish(field.data_type(), exact_null_counts))
            .collect()
    }
}

impl ExecutionPlan for PaimonTableScan {
    fn name(&self) -> &str {
        "PaimonTableScan"
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

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let filters = child_pushdown_result
            .parent_filters
            .into_iter()
            .map(|result| result.filter)
            .collect::<Vec<_>>();
        if filters.is_empty() {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                Vec::new(),
            ));
        }

        let schema = self.schema();
        let apply_row_filter = scan_applies_row_filter(config);
        let mut accepted = Vec::new();
        let parent_filter_handled = filters
            .into_iter()
            .map(|filter| {
                if can_expr_be_pushed_down_with_schemas(&filter, schema.as_ref()) {
                    accepted.push(filter);
                    // `PushedDown` reports whether this scan evaluates the predicate
                    // exactly so the parent FilterExec can be removed. The predicate is
                    // retained above for pruning regardless of this result.
                    if apply_row_filter {
                        PushedDown::Yes
                    } else {
                        PushedDown::No
                    }
                } else {
                    PushedDown::No
                }
            })
            .collect::<Vec<_>>();
        if accepted.is_empty() {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                parent_filter_handled,
            ));
        }

        let mut scan = self.clone();
        scan.runtime_filters.extend(accepted);
        scan.apply_row_filter = apply_row_filter;
        Ok(
            FilterPushdownPropagation::with_parent_pushdown_result(parent_filter_handled)
                .with_updated_node(Arc::new(scan)),
        )
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
        let read_type = self.read_type.clone();
        let pushed_predicate = self.pushed_predicate.clone();
        let case_sensitive = self.case_sensitive;
        let runtime_filters = self.runtime_filters.clone();
        let apply_row_filter = self.apply_row_filter;

        let fut = async move {
            let mut read_builder = table.new_read_builder();

            read_builder.with_case_sensitive(case_sensitive);
            read_builder.with_read_type(read_type);
            let estimated_rows = splits
                .iter()
                .filter_map(|split| usize::try_from(split.row_count()).ok())
                .fold(0usize, usize::saturating_add);
            let runtime_pruning = runtime_pruning_predicate(
                &runtime_filters,
                table.schema().fields(),
                case_sensitive,
                runtime_filter_wait_timeout(estimated_rows),
            )
            .await;
            let read_predicate = match (pushed_predicate, runtime_pruning) {
                (Some(pushed), Some(runtime)) => Some(Predicate::and(vec![pushed, runtime])),
                (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
                (None, None) => None,
            };
            if let Some(filter) = read_predicate {
                read_builder.with_filter(filter);
            }

            let read = read_builder
                .new_read()
                .map_err(to_datafusion_error)?
                .with_row_filter(apply_row_filter);
            let stream = read.to_arrow(&splits).map_err(to_datafusion_error)?;
            let batch_schema = Arc::clone(&schema);
            let stream = stream.map(move |result| {
                let mut batch = result
                    .map_err(to_datafusion_error)
                    .and_then(|batch| to_datafusion_batch(batch, &batch_schema))?;
                if apply_row_filter {
                    for filter in &runtime_filters {
                        let predicate = filter.evaluate(&batch)?.into_array(batch.num_rows())?;
                        let predicate = predicate
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| {
                                datafusion::error::DataFusionError::Execution(format!(
                                    "Paimon runtime filter must return Boolean, got {}",
                                    predicate.data_type()
                                ))
                            })?;
                        batch = filter_record_batch(&batch, predicate)?;
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
            self.schema(),
            futures::stream::once(fut).try_flatten(),
        )))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Arc<Statistics>> {
        let partitions: &[Arc<[DataSplit]>] = match partition {
            Some(idx) => std::slice::from_ref(&self.planned_partitions[idx]),
            None => &self.planned_partitions,
        };

        let mut total_rows: usize = 0;
        let mut all_row_counts_known = true;
        for splits in partitions {
            for split in splits.iter() {
                if let Some(row_count) = split.merged_row_count() {
                    total_rows += row_count as usize;
                } else {
                    all_row_counts_known = false;
                    total_rows += split.row_count() as usize;
                }
            }
        }

        // Return exact statistics when:
        // 1. All splits have known merged_row_count (no deletion files with unknown cardinality)
        // 2. No limit is applied (limit would make row count inexact)
        // 3. Filter is exact (no residual filtering needed above the scan)
        let num_rows_precision = if all_row_counts_known
            && self.limit.is_none()
            && self.filter_exact
            && self.runtime_filters.is_empty()
        {
            Precision::Exact(total_rows)
        } else {
            Precision::Inexact(total_rows)
        };

        Ok(Arc::new(Statistics {
            num_rows: num_rows_precision,
            total_byte_size: Precision::Absent,
            column_statistics: self.manifest_column_statistics(partitions),
        }))
    }
}

impl DisplayAs for PaimonTableScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "PaimonTableScan: table={}", self.table.identifier())?;

        let total_splits: usize = self.planned_partitions.iter().map(|p| p.len()).sum();
        let total_files: usize = self
            .planned_partitions
            .iter()
            .flat_map(|p| p.iter())
            .map(|s| s.data_files().len())
            .sum();
        write!(
            f,
            ", partitions={}, splits={total_splits}, files={total_files}",
            self.planned_partitions.len()
        )?;

        let columns = self
            .read_type
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>();
        write!(f, ", projection=[{}]", columns.join(", "))?;
        if let Some(ref predicate) = self.pushed_predicate {
            write!(f, ", predicate={predicate}")?;
        }
        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }
        if let Some(ref trace) = self.scan_trace {
            write!(f, ", trace={trace}")?;
        }
        if let Some(ref pushed_variants) = self.pushed_variants {
            write!(f, ", PushedVariants=[{pushed_variants}]")?;
        }
        if !self.runtime_filters.is_empty() {
            let filters = self
                .runtime_filters
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            write!(f, ", runtime_filters=[{}]", filters.join(" AND "))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PaimonConfig;
    mod test_utils {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../test_utils.rs"));
    }

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{
        lit, BinaryExpr, Column, DynamicFilterPhysicalExpr, InListExpr,
    };
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::filter_pushdown::{
        ChildFilterPushdownResult, ChildPushdownResult,
    };
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;
    use paimon::catalog::Identifier;
    use paimon::io::FileIOBuilder;
    use paimon::spec::{
        BinaryRow, BinaryType, DataFileMeta, DataType, Datum, IntType, PredicateBuilder,
        Schema as PaimonSchema, TableSchema, VarBinaryType,
    };
    use paimon::table::{DeletionFile, RowRange, Table};
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

    fn test_read_type() -> Vec<DataField> {
        vec![DataField::new(
            0,
            "id".to_string(),
            DataType::Int(IntType::new()),
        )]
    }

    #[test]
    fn test_binary_manifest_bounds_are_not_exposed() {
        for data_type in [
            ArrowDataType::Binary,
            ArrowDataType::BinaryView,
            ArrowDataType::LargeBinary,
        ] {
            assert_eq!(
                datum_to_scalar(Datum::Bytes(vec![0x7f, 0x80]), &data_type),
                None
            );
        }
    }

    #[test]
    fn test_binary_runtime_ranges_are_not_translated() {
        for data_type in [
            DataType::Binary(BinaryType::new(1).unwrap()),
            DataType::VarBinary(VarBinaryType::new(1).unwrap()),
        ] {
            let fields = vec![DataField::new(0, "bytes".to_string(), data_type)];
            let predicate_builder = PredicateBuilder::new(&fields);
            for op in [Operator::Lt, Operator::LtEq, Operator::Gt, Operator::GtEq] {
                let expression = BinaryExpr::new(
                    Arc::new(Column::new("bytes", 0)),
                    op,
                    Arc::new(Literal::new(ScalarValue::Binary(Some(vec![0xff])))),
                );

                assert!(
                    translate_runtime_comparison(&expression, &fields, &predicate_builder, true,)
                        .is_none(),
                    "binary {op} must remain with DataFusion"
                );
            }

            let equality = BinaryExpr::new(
                Arc::new(Column::new("bytes", 0)),
                Operator::Eq,
                Arc::new(Literal::new(ScalarValue::Binary(Some(vec![0xff])))),
            );
            assert!(
                translate_runtime_comparison(&equality, &fields, &predicate_builder, true)
                    .is_some(),
                "binary equality is ordering-independent"
            );
        }
    }

    #[test]
    fn test_partition_count_empty_plan() {
        let schema = test_schema();
        let scan = PaimonTableScan::new(
            schema,
            dummy_table(),
            test_read_type(),
            None,
            vec![Arc::from(Vec::new())],
            None,
            false,
            None,
            None,
            true,
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
        let scan = PaimonTableScan::new(
            schema,
            dummy_table(),
            test_read_type(),
            None,
            planned_partitions,
            None,
            false,
            None,
            None,
            true,
        );
        assert_eq!(scan.properties().output_partitioning().partition_count(), 3);
    }

    /// Constructs a minimal Table for testing (no real files needed since we
    /// only test PlanProperties, not actual reads).
    fn dummy_table() -> Table {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let table_schema = TableSchema::new(0, &schema);
        Table::new(
            file_io,
            Identifier::new("test_db", "test_table"),
            "/tmp/test-table".to_string(),
            table_schema,
            None,
        )
    }

    fn data_file_with_int_stats(
        file_name: &str,
        row_count: i64,
        min: i32,
        max: i32,
        null_count: i64,
    ) -> DataFileMeta {
        let data_type = DataType::Int(IntType::new());
        let min = Datum::Int(min);
        let max = Datum::Int(max);
        let mut file =
            serde_json::to_value(test_data_file::<DataFileMeta>(file_name, row_count, 100))
                .unwrap();
        file["_VALUE_STATS"] = serde_json::json!({
            "_MIN_VALUES": BinaryRow::from_datums(&[(Some(&min), &data_type)]).to_serialized_bytes(),
            "_MAX_VALUES": BinaryRow::from_datums(&[(Some(&max), &data_type)]).to_serialized_bytes(),
            "_NULL_COUNTS": [null_count],
        });
        serde_json::from_value(file).unwrap()
    }

    fn split_with_int_stats(
        row_ranges: Option<Vec<RowRange>>,
        deletion_file: Option<DeletionFile>,
    ) -> DataSplit {
        let mut builder = paimon::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/test-table/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![data_file_with_int_stats("data.parquet", 4, 1, 4, 1)]);
        if let Some(row_ranges) = row_ranges {
            builder = builder.with_row_ranges(row_ranges);
        }
        if let Some(deletion_file) = deletion_file {
            builder = builder.with_data_deletion_files(vec![Some(deletion_file)]);
        }
        builder.build().unwrap()
    }

    #[test]
    fn test_partition_statistics_include_manifest_column_bounds() {
        let split = paimon::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/test-table/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![
                data_file_with_int_stats("a.parquet", 4, 2, 8, 1),
                data_file_with_int_stats("b.parquet", 6, 1, 10, 2),
            ])
            .build()
            .unwrap();
        let scan = PaimonTableScan::new(
            test_schema(),
            dummy_table(),
            test_read_type(),
            None,
            vec![Arc::from(vec![split])],
            None,
            true,
            None,
            None,
            true,
        );

        let statistics = scan.partition_statistics(None).unwrap();
        let id = &statistics.column_statistics[0];

        assert_eq!(
            id.min_value,
            Precision::Inexact(ScalarValue::Int32(Some(1)))
        );
        assert_eq!(
            id.max_value,
            Precision::Inexact(ScalarValue::Int32(Some(10)))
        );
        assert_eq!(id.null_count, Precision::Exact(3));
    }

    #[test]
    fn test_partition_statistics_omit_compressed_file_size() {
        let split = paimon::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/test-table/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![data_file_with_int_stats("data.parquet", 4, 1, 4, 0)])
            .build()
            .unwrap();
        let scan = PaimonTableScan::new(
            test_schema(),
            dummy_table(),
            test_read_type(),
            None,
            vec![Arc::from(vec![split])],
            None,
            true,
            None,
            None,
            true,
        );

        let statistics = scan.partition_statistics(None).unwrap();

        assert_eq!(statistics.num_rows, Precision::Exact(4));
        assert_eq!(statistics.total_byte_size, Precision::Absent);
    }

    #[test]
    fn test_partition_statistics_downgrade_unsafe_null_counts() {
        let table = dummy_table();
        let predicate = PredicateBuilder::new(table.schema().fields())
            .greater_than("id", Datum::Int(1))
            .unwrap();
        let cases = vec![
            (split_with_int_stats(None, None), Some(predicate), None),
            (split_with_int_stats(None, None), None, Some(2)),
            (
                split_with_int_stats(Some(vec![RowRange::new(1, 2)]), None),
                None,
                None,
            ),
            (
                split_with_int_stats(
                    None,
                    Some(DeletionFile::new("dv.bin".to_string(), 0, 16, Some(1))),
                ),
                None,
                None,
            ),
        ];

        for (split, predicate, limit) in cases {
            let scan = PaimonTableScan::new(
                test_schema(),
                table.clone(),
                test_read_type(),
                predicate,
                vec![Arc::from(vec![split])],
                limit,
                true,
                None,
                None,
                true,
            );
            assert_eq!(
                scan.partition_statistics(None).unwrap().column_statistics[0].null_count,
                Precision::Absent
            );
        }
    }

    #[test]
    fn test_partition_statistics_hide_aggregation_value_bounds() {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &PaimonSchema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "aggregation")
                .option("fields.value.aggregate-function", "sum")
                .build()
                .unwrap(),
        );
        let value_field = table_schema.fields()[1].clone();
        let table = Table::new(
            file_io,
            Identifier::new("test_db", "aggregation_table"),
            "/tmp/aggregation-table".to_string(),
            table_schema,
            None,
        );
        let mut file = data_file_with_int_stats("data.parquet", 2, 1, 2, 0);
        file.value_stats_cols = Some(vec!["value".to_string()]);
        let split = paimon::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/aggregation-table/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![file])
            .build()
            .unwrap();
        let scan = PaimonTableScan::new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "value",
                ArrowDataType::Int32,
                true,
            )])),
            table,
            vec![value_field],
            None,
            vec![Arc::from(vec![split])],
            None,
            true,
            None,
            None,
            true,
        );

        let value_stats = &scan.partition_statistics(None).unwrap().column_statistics[0];
        assert_eq!(value_stats.min_value, Precision::Absent);
        assert_eq!(value_stats.max_value, Precision::Absent);
        assert_eq!(value_stats.null_count, Precision::Absent);
    }

    #[test]
    fn test_scan_retains_dynamic_filter_for_runtime_evaluation() {
        let scan = PaimonTableScan::new(
            test_schema(),
            dummy_table(),
            test_read_type(),
            None,
            vec![Arc::from(Vec::<DataSplit>::new())],
            None,
            true,
            None,
            None,
            true,
        );
        let dynamic_filter: Arc<dyn PhysicalExpr> =
            Arc::new(DynamicFilterPhysicalExpr::new(Vec::new(), lit(true)));
        let result = scan
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                ChildPushdownResult {
                    parent_filters: vec![ChildFilterPushdownResult {
                        filter: dynamic_filter,
                        child_results: Vec::new(),
                    }],
                    self_filters: Vec::new(),
                },
                &ConfigOptions::default(),
            )
            .unwrap();

        let updated = result
            .updated_node
            .expect("the scan must retain physical filters so dynamic join filters remain active");
        let statistics = updated.partition_statistics(None).unwrap();
        assert_eq!(statistics.num_rows, Precision::Inexact(0));
        assert_eq!(
            statistics.column_statistics[0].null_count,
            Precision::Absent,
            "runtime-filtered scans must not expose unfiltered exact null counts"
        );
    }

    #[test]
    fn test_scan_reports_filter_handled_when_row_filter_is_enabled() {
        let scan = PaimonTableScan::new(
            test_schema(),
            dummy_table(),
            test_read_type(),
            None,
            vec![Arc::from(Vec::<DataSplit>::new())],
            None,
            true,
            None,
            None,
            true,
        );
        let filter: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 0)),
            Operator::Gt,
            lit(1_i32),
        ));
        let mut paimon_config = PaimonConfig::default();
        paimon_config.read.row_filter = true;
        let mut config = ConfigOptions::default();
        config.extensions.insert(paimon_config);

        let result = scan
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                ChildPushdownResult {
                    parent_filters: vec![ChildFilterPushdownResult {
                        filter,
                        child_results: Vec::new(),
                    }],
                    self_filters: Vec::new(),
                },
                &config,
            )
            .unwrap();

        assert!(matches!(result.filters.as_slice(), [PushedDown::Yes]));
        assert!(result.updated_node.is_some());
    }

    #[test]
    fn test_scan_rejects_filter_outside_output_schema() {
        let scan = PaimonTableScan::new(
            test_schema(),
            dummy_table(),
            test_read_type(),
            None,
            vec![Arc::from(Vec::<DataSplit>::new())],
            None,
            true,
            None,
            None,
            true,
        );
        let filter: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("missing", 1)),
            Operator::Gt,
            lit(1_i32),
        ));
        let mut paimon_config = PaimonConfig::default();
        paimon_config.read.row_filter = true;
        let mut config = ConfigOptions::default();
        config.extensions.insert(paimon_config);

        let result = scan
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                ChildPushdownResult {
                    parent_filters: vec![ChildFilterPushdownResult {
                        filter,
                        child_results: Vec::new(),
                    }],
                    self_filters: Vec::new(),
                },
                &config,
            )
            .unwrap();

        assert!(matches!(result.filters.as_slice(), [PushedDown::No]));
        assert!(result.updated_node.is_none());
    }

    #[tokio::test]
    async fn test_scan_uses_retained_runtime_filter_for_pruning_only() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();
        write_int_parquet_file(
            &bucket_dir.join("data.parquet"),
            vec![("id", vec![1, 2, 3, 4, 5])],
            None,
        );
        let file_size = fs::metadata(bucket_dir.join("data.parquet")).unwrap().len() as i64;
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &PaimonSchema::builder()
                .column("id", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            table_path,
            table_schema,
            None,
        );
        let split = paimon::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 5, file_size)])
            .build()
            .unwrap();
        let scan = PaimonTableScan::new(
            test_schema(),
            table,
            test_read_type(),
            None,
            vec![Arc::from(vec![split])],
            None,
            false,
            None,
            None,
            true,
        );
        let filter: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 0)),
            Operator::Gt,
            lit(2_i32),
        ));
        let pruning_result = scan
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                ChildPushdownResult {
                    parent_filters: vec![ChildFilterPushdownResult {
                        filter: Arc::clone(&filter),
                        child_results: Vec::new(),
                    }],
                    self_filters: Vec::new(),
                },
                &ConfigOptions::default(),
            )
            .unwrap();
        assert!(matches!(
            pruning_result.filters.as_slice(),
            [PushedDown::No]
        ));
        let pruning_scan = pruning_result.updated_node.unwrap();
        let ctx = SessionContext::new();
        let batches = pruning_scan
            .execute(0, ctx.task_ctx())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let ids = collect_ids(&batches);

        assert_eq!(
            ids,
            vec![1, 2, 3, 4, 5],
            "unsupported physical filters may prune row groups but must not remove rows"
        );

        let mut paimon_config = PaimonConfig::default();
        paimon_config.read.row_filter = true;
        let mut exact_config = ConfigOptions::default();
        exact_config.extensions.insert(paimon_config);
        let exact_result = scan
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                ChildPushdownResult {
                    parent_filters: vec![ChildFilterPushdownResult {
                        filter,
                        child_results: Vec::new(),
                    }],
                    self_filters: Vec::new(),
                },
                &exact_config,
            )
            .unwrap();
        assert!(matches!(exact_result.filters.as_slice(), [PushedDown::Yes]));

        // Execution deliberately uses the default context. The scan must honor
        // the planning-time decision that allowed the parent FilterExec to be removed.
        let exact_scan = exact_result.updated_node.unwrap();
        let exact_batches = exact_scan
            .execute(0, SessionContext::new().task_ctx())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(collect_ids(&exact_batches), vec![3, 4, 5]);
    }

    fn collect_ids(batches: &[RecordBatch]) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect()
    }

    #[tokio::test]
    async fn test_dynamic_filter_builds_reader_pruning_predicate() {
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&column)],
            lit(true),
        ));
        dynamic_filter
            .update(Arc::new(BinaryExpr::new(
                column,
                Operator::GtEq,
                lit(3_i32),
            )))
            .unwrap();
        dynamic_filter.mark_complete();
        let filter: Arc<dyn PhysicalExpr> = dynamic_filter;

        let predicate =
            runtime_pruning_predicate(&[filter], &test_read_type(), true, Duration::ZERO)
                .await
                .expect("a completed column/literal dynamic filter should prune the reader");

        assert_eq!(predicate.to_string(), "id >= 3");
    }

    #[tokio::test]
    async fn test_runtime_in_list_builds_reader_pruning_predicate() {
        let filter: Arc<dyn PhysicalExpr> = Arc::new(
            InListExpr::try_new(
                Arc::new(Column::new("id", 0)),
                vec![lit(1_i32), lit(3_i32)],
                false,
                test_schema().as_ref(),
            )
            .unwrap(),
        );

        let predicate =
            runtime_pruning_predicate(&[filter], &test_read_type(), true, Duration::ZERO)
                .await
                .expect("a literal IN list should prune the reader");

        assert_eq!(predicate.to_string(), "id IN (1, 3)");
    }

    #[test]
    fn test_runtime_filter_wait_is_cost_aware() {
        assert_eq!(runtime_filter_wait_timeout(204_000), Duration::ZERO);
        assert_eq!(runtime_filter_wait_timeout(250_000), Duration::ZERO);
        assert_eq!(runtime_filter_wait_timeout(250_001), Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_incomplete_dynamic_filter_does_not_materially_delay_scan_startup() {
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
        let dynamic_filter: Arc<dyn PhysicalExpr> =
            Arc::new(DynamicFilterPhysicalExpr::new(vec![column], lit(true)));

        let predicate = tokio::time::timeout(
            Duration::from_millis(100),
            runtime_pruning_predicate(&[dynamic_filter], &test_read_type(), true, Duration::ZERO),
        )
        .await
        .expect("an incomplete dynamic filter must not delay scan startup");

        assert!(predicate.is_none());
    }

    #[tokio::test]
    async fn test_completed_dynamic_filter_still_prunes_when_another_times_out() {
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
        let completed = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&column)],
            lit(true),
        ));
        completed
            .update(Arc::new(BinaryExpr::new(
                Arc::clone(&column),
                Operator::GtEq,
                lit(3_i32),
            )))
            .unwrap();
        completed.mark_complete();
        let incomplete: Arc<dyn PhysicalExpr> =
            Arc::new(DynamicFilterPhysicalExpr::new(vec![column], lit(true)));
        let completed: Arc<dyn PhysicalExpr> = completed;

        let predicate = runtime_pruning_predicate(
            &[completed, incomplete],
            &test_read_type(),
            true,
            Duration::ZERO,
        )
        .await
        .expect("completed filters should survive another filter timing out");

        assert_eq!(predicate.to_string(), "id >= 3");
    }

    #[tokio::test]
    async fn test_execute_uses_pushed_filter_only_for_pruning_by_default() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        write_int_parquet_file(
            &bucket_dir.join("data.parquet"),
            vec![("id", vec![1, 2, 3, 4]), ("value", vec![5, 20, 30, 40])],
            Some(2),
        );
        let file_size = fs::metadata(bucket_dir.join("data.parquet")).unwrap().len() as i64;

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
            None,
        );

        let split = paimon::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 4, file_size)])
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
            vec![DataField::new(
                0,
                "id".to_string(),
                DataType::Int(IntType::new()),
            )],
            Some(pushed_predicate),
            vec![Arc::from(vec![split])],
            None,
            false,
            None,
            None,
            true,
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

        assert_eq!(actual_ids, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_execute_uses_read_batch_size_option() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        write_int_parquet_file(
            &bucket_dir.join("data.parquet"),
            vec![("id", vec![1, 2, 3, 4, 5])],
            None,
        );
        let file_size = fs::metadata(bucket_dir.join("data.parquet")).unwrap().len() as i64;

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &PaimonSchema::builder()
                .column("id", DataType::Int(IntType::new()))
                .option("read.batch-size", "2")
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            table_path,
            table_schema,
            None,
        );
        let split = paimon::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 5, file_size)])
            .build()
            .unwrap();
        let scan = PaimonTableScan::new(
            test_schema(),
            table,
            test_read_type(),
            None,
            vec![Arc::from(vec![split])],
            None,
            false,
            None,
            None,
            true,
        );

        let ctx = SessionContext::new();
        let batches = scan
            .execute(0, ctx.task_ctx())
            .expect("execute should succeed")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
    }
}
