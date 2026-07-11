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
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::default_table_source::source_as_provider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{
    internal_err, plan_err, Column, DFSchema, DFSchemaRef, Result as DFResult, ScalarValue,
};
use datafusion::execution::context::SessionState;
use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::expr::{InList, ScalarFunction};
use datafusion::logical_expr::{
    Between, BinaryExpr, Case, Cast, Expr, Extension, Filter, Like, LogicalPlan, Projection,
    TableScan, TryCast, UserDefinedLogicalNode,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use paimon::spec::{
    variant_extraction_row, BigIntType, BooleanType, DataField, DataType, DecimalType, DoubleType,
    FloatType, IntType, SmallIntType, TinyIntType, VarCharType,
};
use paimon::table::Table;

use crate::error::to_datafusion_error;
use crate::filter_pushdown::analyze_filters;
use crate::physical_plan::PaimonTableScan;
use crate::runtime::await_with_runtime;
use crate::table::{bucket_round_robin, datafusion_read_fields, PaimonTableProvider};

#[derive(Debug)]
pub(crate) struct RewriteVariantExtractions;

impl RewriteVariantExtractions {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl OptimizerRule for RewriteVariantExtractions {
    fn name(&self) -> &str {
        "rewrite_variant_extractions"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(projection) = plan else {
            return Ok(Transformed::no(plan));
        };
        let rewrite = match projection.input.as_ref() {
            LogicalPlan::TableScan(scan) => build_projection_rewrite(&projection, scan, None)?,
            LogicalPlan::Filter(filter) => match filter.input.as_ref() {
                LogicalPlan::TableScan(scan) => {
                    build_projection_rewrite(&projection, scan, Some(filter))?
                }
                _ => None,
            },
            _ => None,
        };
        let Some(rewrite) = rewrite else {
            return Ok(Transformed::no(LogicalPlan::Projection(projection)));
        };
        Ok(Transformed::yes(LogicalPlan::Projection(
            Projection::try_new_with_schema(
                rewrite.exprs,
                Arc::new(rewrite.input),
                projection.schema,
            )?,
        )))
    }
}

#[derive(Debug)]
struct ProjectionRewrite {
    input: LogicalPlan,
    exprs: Vec<Expr>,
}

#[derive(Debug, Clone)]
struct VariantExtractionScanNode {
    table: Table,
    schema: DFSchemaRef,
    arrow_schema: ArrowSchemaRef,
    read_type: Vec<DataField>,
    filters: Vec<Expr>,
    fetch: Option<usize>,
    pushed_variants: String,
}

impl UserDefinedLogicalNode for VariantExtractionScanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "PaimonVariantExtractionScan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn check_invariants(&self, _check: datafusion::logical_expr::InvariantLevel) -> DFResult<()> {
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PaimonVariantExtractionScan: PushedVariants=[{}]",
            self.pushed_variants
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Arc<dyn UserDefinedLogicalNode>> {
        if !exprs.is_empty() || !inputs.is_empty() {
            return internal_err!("PaimonVariantExtractionScan expects no expressions or inputs");
        }
        Ok(Arc::new(self.clone()))
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        self.name().hash(&mut state);
        self.table.location().hash(&mut state);
        self.read_type.hash(&mut state);
        self.filters.hash(&mut state);
        self.fetch.hash(&mut state);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other.as_any().downcast_ref::<Self>().is_some_and(|other| {
            self.table.location() == other.table.location()
                && self.read_type == other.read_type
                && self.filters == other.filters
                && self.fetch == other.fetch
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
pub(crate) struct VariantExtractionExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for VariantExtractionExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<VariantExtractionScanNode>() else {
            return Ok(None);
        };
        if !logical_inputs.is_empty() || !physical_inputs.is_empty() {
            return internal_err!("PaimonVariantExtractionScan physical planning expects no input");
        }

        // Column-name matching is case-sensitive on the DataFusion path, mirroring
        // `TableProvider::scan`: DataFusion resolves columns against the provider
        // schema before this planner runs, so a genuine case mismatch fails at
        // planning and never reaches here. Case-insensitive matching is offered
        // only through the direct ReadBuilder API (core / C / Python), not via SQL.
        let case_sensitive = true;
        let filter_analysis =
            analyze_filters(&node.filters, node.table.schema().fields(), case_sensitive);
        let mut read_builder = node.table.new_read_builder();
        read_builder.with_case_sensitive(case_sensitive);
        read_builder.with_read_type(node.read_type.clone());
        if let Some(filter) = filter_analysis.pushed_predicate.clone() {
            read_builder.with_filter(filter);
        }
        let pushed_limit = node.fetch.filter(|_| !filter_analysis.requires_residual);
        if let Some(limit) = pushed_limit {
            read_builder.with_limit(limit);
        }
        let scan = read_builder.new_scan();
        let (plan, scan_trace) = await_with_runtime(scan.plan_with_trace())
            .await
            .map_err(to_datafusion_error)?;

        let splits = plan.splits().to_vec();
        let target = session_state.config_options().execution.target_partitions;
        let planned_partitions: Vec<Arc<[_]>> = if splits.is_empty() {
            vec![Arc::from(Vec::new())]
        } else {
            let num_partitions = splits.len().min(target.max(1));
            bucket_round_robin(splits, num_partitions)
                .into_iter()
                .map(Arc::from)
                .collect()
        };
        let filter_exact = !filter_analysis.requires_residual
            && filter_analysis
                .pushed_predicate
                .as_ref()
                .is_none_or(|p| read_builder.is_exact_filter_pushdown(p));

        Ok(Some(Arc::new(PaimonTableScan::new(
            Arc::clone(&node.arrow_schema),
            node.table.clone(),
            node.read_type.clone(),
            filter_analysis.pushed_predicate,
            planned_partitions,
            pushed_limit,
            filter_exact,
            Some(scan_trace),
            Some(node.pushed_variants.clone()),
            case_sensitive,
        ))))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VariantGetCall {
    column: Column,
    path: String,
    type_name: String,
    data_type: DataType,
    fail_on_error: bool,
}

#[derive(Debug, Clone)]
struct AcceptedExtraction {
    call: VariantGetCall,
    field_name: String,
}

fn build_projection_rewrite(
    projection: &Projection,
    scan: &TableScan,
    filter: Option<&Filter>,
) -> DFResult<Option<ProjectionRewrite>> {
    let provider = source_as_provider(&scan.source)?;
    let Some(provider) = provider.downcast_ref::<PaimonTableProvider>() else {
        return Ok(None);
    };

    let mut calls = Vec::new();
    let mut full_columns = HashSet::new();
    for expr in &projection.expr {
        collect_variant_usage(expr, &mut calls, &mut full_columns)?;
    }
    for expr in &scan.filters {
        collect_variant_usage(expr, &mut calls, &mut full_columns)?;
    }
    if let Some(filter) = filter {
        collect_variant_usage(&filter.predicate, &mut calls, &mut full_columns)?;
    }
    if calls.is_empty() {
        return Ok(None);
    }

    let read_fields = datafusion_read_fields(provider.table());
    let mut by_column: HashMap<String, Vec<VariantGetCall>> = HashMap::new();
    for call in calls {
        if full_columns.contains(&call.column.name) {
            continue;
        }
        let Some(table_field) = read_fields
            .iter()
            .find(|field| field.name() == call.column.name)
        else {
            continue;
        };
        if !matches!(table_field.data_type(), DataType::Variant(_)) {
            continue;
        }
        let entries = by_column.entry(call.column.name.clone()).or_default();
        if !entries.iter().any(|existing| existing == &call) {
            entries.push(call);
        }
    }
    if by_column.is_empty() {
        return Ok(None);
    }

    let read_indices = scan
        .projection
        .clone()
        .unwrap_or_else(|| (0..read_fields.len()).collect());
    let mut read_type = Vec::with_capacity(read_indices.len());
    for idx in read_indices {
        let Some(field) = read_fields.get(idx).cloned() else {
            return plan_err!("Paimon TableScan projection index is out of bounds");
        };
        if let Some(extractions) = by_column.get(field.name()) {
            let extraction_row = variant_extraction_row(
                field.data_type().is_nullable(),
                extractions.iter().map(|call| {
                    (
                        call.data_type.clone(),
                        call.path.clone(),
                        call.fail_on_error,
                        "UTC".to_string(),
                    )
                }),
            );
            read_type.push(
                DataField::new(
                    field.id(),
                    field.name().to_string(),
                    DataType::Row(extraction_row),
                )
                .with_description(field.description().map(ToString::to_string)),
            );
        } else {
            read_type.push(field);
        }
    }

    let accepted = by_column
        .values()
        .flat_map(|calls| {
            calls
                .iter()
                .enumerate()
                .map(|(idx, call)| AcceptedExtraction {
                    call: call.clone(),
                    field_name: idx.to_string(),
                })
        })
        .collect::<Vec<_>>();
    let exprs = projection
        .expr
        .iter()
        .cloned()
        .map(|expr| rewrite_variant_gets(expr, &accepted))
        .collect::<DFResult<Vec<_>>>()?;
    let filter_predicate = filter
        .map(|filter| rewrite_variant_gets(filter.predicate.clone(), &accepted))
        .transpose()?;

    let arrow_schema =
        paimon::arrow::build_target_arrow_schema(&read_type).map_err(to_datafusion_error)?;
    let schema = Arc::new(DFSchema::try_from_qualified_schema(
        scan.table_name.clone(),
        arrow_schema.as_ref(),
    )?);
    let pushed_variants = describe_pushed_variants(&by_column);
    let mut filters = scan.filters.clone();
    if let Some(filter) = filter {
        filters.push(filter.predicate.clone());
    }

    let scan_input = LogicalPlan::Extension(Extension {
        node: Arc::new(VariantExtractionScanNode {
            table: provider.table().clone(),
            schema,
            arrow_schema,
            read_type,
            filters,
            fetch: scan.fetch,
            pushed_variants,
        }),
    });
    let input = if let Some(predicate) = filter_predicate {
        LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(scan_input))?)
    } else {
        scan_input
    };

    Ok(Some(ProjectionRewrite { input, exprs }))
}

fn collect_variant_usage(
    expr: &Expr,
    calls: &mut Vec<VariantGetCall>,
    full_columns: &mut HashSet<String>,
) -> DFResult<()> {
    match parse_variant_get(expr)? {
        VariantGetParse::Scalar(call) => {
            calls.push(call);
            return Ok(());
        }
        VariantGetParse::FullVariant(column) => {
            full_columns.insert(column.name);
            return Ok(());
        }
        VariantGetParse::NotVariantGet => {}
    }

    match expr {
        Expr::Alias(alias) => collect_variant_usage(alias.expr.as_ref(), calls, full_columns),
        Expr::Column(column) => {
            full_columns.insert(column.name.clone());
            Ok(())
        }
        Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
            collect_variant_usage(left, calls, full_columns)?;
            collect_variant_usage(right, calls, full_columns)
        }
        Expr::Like(Like { expr, pattern, .. }) | Expr::SimilarTo(Like { expr, pattern, .. }) => {
            collect_variant_usage(expr, calls, full_columns)?;
            collect_variant_usage(pattern, calls, full_columns)
        }
        Expr::Not(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsNotUnknown(inner)
        | Expr::Negative(inner) => collect_variant_usage(inner, calls, full_columns),
        Expr::Between(Between {
            expr, low, high, ..
        }) => {
            collect_variant_usage(expr, calls, full_columns)?;
            collect_variant_usage(low, calls, full_columns)?;
            collect_variant_usage(high, calls, full_columns)
        }
        Expr::Case(Case {
            expr,
            when_then_expr,
            else_expr,
        }) => {
            if let Some(expr) = expr {
                collect_variant_usage(expr, calls, full_columns)?;
            }
            for (when, then) in when_then_expr {
                collect_variant_usage(when, calls, full_columns)?;
                collect_variant_usage(then, calls, full_columns)?;
            }
            if let Some(expr) = else_expr {
                collect_variant_usage(expr, calls, full_columns)?;
            }
            Ok(())
        }
        Expr::Cast(Cast { expr, .. }) | Expr::TryCast(TryCast { expr, .. }) => {
            collect_variant_usage(expr, calls, full_columns)
        }
        Expr::ScalarFunction(ScalarFunction { args, .. }) => {
            for arg in args {
                collect_variant_usage(arg, calls, full_columns)?;
            }
            Ok(())
        }
        Expr::InList(InList { expr, list, .. }) => {
            collect_variant_usage(expr, calls, full_columns)?;
            for expr in list {
                collect_variant_usage(expr, calls, full_columns)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn rewrite_variant_gets(expr: Expr, accepted: &[AcceptedExtraction]) -> DFResult<Expr> {
    expr.transform(|expr| {
        if let VariantGetParse::Scalar(call) = parse_variant_get(&expr)? {
            if let Some(accepted) = accepted.iter().find(|accepted| accepted.call == call) {
                return Ok(Transformed::yes(get_field(
                    Expr::Column(call.column),
                    accepted.field_name.as_str(),
                )));
            }
        }
        Ok(Transformed::no(expr))
    })
    .map(|transformed| transformed.data)
}

enum VariantGetParse {
    Scalar(VariantGetCall),
    FullVariant(Column),
    NotVariantGet,
}

fn parse_variant_get(expr: &Expr) -> DFResult<VariantGetParse> {
    let Expr::ScalarFunction(func) = expr else {
        return Ok(VariantGetParse::NotVariantGet);
    };
    let name = func.name();
    if name != "variant_get" && name != "try_variant_get" {
        return Ok(VariantGetParse::NotVariantGet);
    }
    if func.args.len() != 2 && func.args.len() != 3 {
        return Ok(VariantGetParse::NotVariantGet);
    }
    let Expr::Column(column) = &func.args[0] else {
        return Ok(VariantGetParse::NotVariantGet);
    };
    let Some(path) = string_literal(&func.args[1]) else {
        return Ok(VariantGetParse::NotVariantGet);
    };
    let Some(type_name) = func.args.get(2).and_then(string_literal) else {
        return Ok(VariantGetParse::FullVariant(column.clone()));
    };
    let Some(data_type) = paimon_type_for_variant_get(&type_name)? else {
        return Ok(VariantGetParse::FullVariant(column.clone()));
    };
    Ok(VariantGetParse::Scalar(VariantGetCall {
        column: column.clone(),
        path,
        type_name: type_name.trim().to_ascii_lowercase(),
        data_type,
        fail_on_error: name == "variant_get",
    }))
}

fn string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _) => Some(value.clone()),
        _ => None,
    }
}

fn paimon_type_for_variant_get(type_name: &str) -> DFResult<Option<DataType>> {
    let normalized = type_name.trim().to_ascii_lowercase();
    Ok(match normalized.as_str() {
        "variant" => None,
        "boolean" | "bool" => Some(DataType::Boolean(BooleanType::new())),
        "byte" | "tinyint" => Some(DataType::TinyInt(TinyIntType::new())),
        "short" | "smallint" => Some(DataType::SmallInt(SmallIntType::new())),
        "int" | "integer" => Some(DataType::Int(IntType::new())),
        "long" | "bigint" => Some(DataType::BigInt(BigIntType::new())),
        "float" | "real" => Some(DataType::Float(FloatType::new())),
        "double" => Some(DataType::Double(DoubleType::new())),
        "string" | "varchar" | "text" => Some(DataType::VarChar(VarCharType::string_type())),
        "decimal" => Some(DataType::Decimal(
            DecimalType::new(10, 0).map_err(to_datafusion_error)?,
        )),
        _ if normalized.starts_with("decimal(") && normalized.ends_with(')') => {
            let inner = &normalized["decimal(".len()..normalized.len() - 1];
            let Some((precision, scale)) = inner.split_once(',') else {
                return plan_err!("Invalid decimal type for variant_get: {type_name}");
            };
            let precision = precision.trim().parse::<u32>().map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!("Invalid decimal precision: {e}"))
            })?;
            let scale = scale.trim().parse::<u32>().map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!("Invalid decimal scale: {e}"))
            })?;
            Some(DataType::Decimal(
                DecimalType::new(precision, scale).map_err(to_datafusion_error)?,
            ))
        }
        _ => return plan_err!("Unsupported variant_get type for pushdown: {type_name}"),
    })
}

fn describe_pushed_variants(by_column: &HashMap<String, Vec<VariantGetCall>>) -> String {
    let mut columns = by_column.keys().cloned().collect::<Vec<_>>();
    columns.sort();
    columns
        .into_iter()
        .map(|column| {
            let paths = by_column[&column]
                .iter()
                .map(|call| call.path.as_str())
                .collect::<Vec<_>>()
                .join(",");
            format!("{column}=[{paths}]")
        })
        .collect::<Vec<_>>()
        .join(", ")
}
