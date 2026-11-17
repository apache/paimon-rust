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

//! Resolve Paimon's max_pt to a string literal before DataFusion plans partition filters.

mod temporary_view;

pub(crate) use temporary_view::{track_metadata_reads, MetadataView};

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::execution::SessionState;
use datafusion::sql::planner::{object_name_to_table_reference, IdentNormalizer};
use datafusion::sql::sqlparser::ast::{
    visit_expressions, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    Select, SelectItem, Statement, Value, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::Token;
use paimon::catalog::Identifier;
use paimon::spec::Datum;

use crate::error::to_datafusion_error;

#[derive(Clone, PartialEq, Eq, Hash)]
struct Call {
    catalog: Option<String>,
    table: String,
}

fn function_catalog(function: &Function, state: &SessionState) -> Option<Option<String>> {
    let normalizer =
        IdentNormalizer::new(state.config_options().sql_parser.enable_ident_normalization);
    let parts = function
        .name
        .0
        .iter()
        .map(|part| part.as_ident().cloned().map(|id| normalizer.normalize(id)))
        .collect::<Option<Vec<_>>>()?;
    let name = parts.join(".");
    if state.scalar_functions().contains_key(&name)
        || state.aggregate_functions().contains_key(&name)
        || state.window_functions().contains_key(&name)
        || state.higher_order_functions().contains_key(&name)
    {
        return None;
    }
    match parts.as_slice() {
        [name] if name == "max_pt" => Some(None),
        [schema, name] if schema == "sys" && name == "max_pt" => Some(None),
        [catalog, schema, name] if schema == "sys" && name == "max_pt" => {
            Some(Some(catalog.clone()))
        }
        _ => None,
    }
}

pub(crate) fn is_max_pt(function: &Function, state: &SessionState) -> bool {
    function_catalog(function, state).is_some()
}

fn parse_call(function: &Function, state: &SessionState) -> Result<Option<Call>> {
    let Some(catalog) = function_catalog(function, state) else {
        return Ok(None);
    };
    if function.filter.is_some()
        || function.over.is_some()
        || function.null_treatment.is_some()
        || !function.within_group.is_empty()
        || !matches!(function.parameters, FunctionArguments::None)
        || function.uses_odbc_syntax
    {
        return Err(error(
            "max_pt requires an ordinary call with one string literal",
        ));
    }
    if let FunctionArguments::List(args) = &function.args {
        if args.duplicate_treatment.is_none() && args.clauses.is_empty() {
            if let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value)))] =
                args.args.as_slice()
            {
                if let Value::SingleQuotedString(table) = &value.value {
                    return Ok(Some(Call {
                        catalog,
                        table: table.clone(),
                    }));
                }
            }
        }
    }
    Err(error(
        "max_pt expects exactly one non-null string literal table name",
    ))
}

pub(crate) async fn rewrite(
    statement: &mut Statement,
    state: &SessionState,
    current_catalog: &str,
    current_database: &str,
) -> Result<bool> {
    let mut calls = HashSet::new();
    let flow = visit_expressions(statement, |expr| {
        if let Expr::Function(function) = expr {
            match parse_call(function, state) {
                Ok(Some(call)) => {
                    calls.insert(call);
                }
                Ok(None) => {}
                Err(error) => return ControlFlow::Break(error),
            }
        }
        ControlFlow::Continue(())
    });
    if let ControlFlow::Break(error) = flow {
        return Err(error);
    }
    if calls.is_empty() {
        return Ok(false);
    }
    temporary_view::record_metadata_read();

    // Resolve each distinct call once per statement; retain no cross-query cache.
    let mut values = HashMap::new();
    for call in calls {
        // Loading/scanning a table has a large future. Keep it off callers' stacks,
        // including catalog view planning, which can recursively expand SQL.
        let value = Box::pin(max_partition(
            &call,
            state,
            current_catalog,
            current_database,
        ))
        .await?;
        values.insert(call, Expr::Value(Value::SingleQuotedString(value).into()));
    }
    let flow = statement.visit(&mut Replacer {
        state,
        values: &values,
        projections: Vec::new(),
    });
    if let ControlFlow::Break(error) = flow {
        return Err(error);
    }
    Ok(true)
}

struct Replacer<'a> {
    state: &'a SessionState,
    values: &'a HashMap<Call, Expr>,
    projections: Vec<Vec<(usize, Expr)>>,
}

impl VisitorMut for Replacer<'_> {
    type Break = DataFusionError;

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
        self.projections.push(
            select
                .projection
                .iter()
                .enumerate()
                .filter_map(|(i, item)| {
                    if let SelectItem::UnnamedExpr(expr) = item {
                        Some((i, expr.clone()))
                    } else {
                        None
                    }
                })
                .collect(),
        );
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
        for (index, original) in self.projections.pop().unwrap() {
            if let SelectItem::UnnamedExpr(current) = &select.projection[index] {
                if current != &original {
                    select.projection[index] = SelectItem::ExprWithAlias {
                        expr: current.clone(),
                        alias: Ident::with_quote('"', original.to_string()),
                    };
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(function) = expr {
            match parse_call(function, self.state) {
                Ok(Some(call)) => {
                    if let Some(value) = self.values.get(&call) {
                        *expr = value.clone();
                    }
                }
                Ok(None) => {}
                Err(error) => return ControlFlow::Break(error),
            }
        }
        ControlFlow::Continue(())
    }
}

async fn max_partition(
    call: &Call,
    state: &SessionState,
    current_catalog: &str,
    current_database: &str,
) -> Result<String> {
    if let Some(catalog) = &call.catalog {
        if state.catalog_list().catalog(catalog).is_none() {
            return Err(error(format!("Unknown catalog '{catalog}'")));
        }
    }
    let name = {
        let mut parser = Parser::new(&GenericDialect {}).try_with_sql(&call.table)?;
        let name = parser.parse_object_name(false)?;
        if parser.peek_token().token != Token::EOF {
            return Err(error("max_pt: invalid table name"));
        }
        name
    };
    let reference = object_name_to_table_reference(
        name,
        state.config_options().sql_parser.enable_ident_normalization,
    )?;
    // Paimon resolves the table argument in the session's namespace even when
    // the function itself is qualified through a different catalog.
    let resolved = reference.resolve(current_catalog, current_database);
    let catalog = state
        .catalog_list()
        .catalog(resolved.catalog.as_ref())
        .ok_or_else(|| error(format!("Unknown catalog '{}'", resolved.catalog)))?;
    let identifier = Identifier::new(resolved.schema.as_ref(), resolved.table.as_ref());
    if identifier
        .parsed_object_name()
        .map_err(to_datafusion_error)?
        .system_table()
        .is_some()
    {
        return Err(error("max_pt requires a data table"));
    }
    // Resolve exactly as a SELECT does, including session scan options, branches,
    // temporary shadows, and the catalog's authorization checks.
    let database = catalog
        .schema(resolved.schema.as_ref())
        .ok_or_else(|| error(format!("Unknown database '{}'", resolved.schema)))?;
    let provider =
        if let Some(paimon) = database.downcast_ref::<crate::catalog::PaimonSchemaProvider>() {
            paimon
                .table_without_view_fallback(resolved.table.as_ref())
                .await?
        } else {
            database.table(resolved.table.as_ref()).await?
        }
        .ok_or_else(|| error(format!("Table '{identifier}' does not exist")))?;
    let provider = provider
        .downcast_ref::<crate::table::PaimonTableProvider>()
        .ok_or_else(|| error("max_pt requires a Paimon data table"))?;
    let table = provider.table();
    let schema = table.schema();
    let key = schema
        .partition_keys()
        .first()
        .ok_or_else(|| error(format!("{identifier} is not a partitioned table")))?;
    let field = schema
        .fields()
        .iter()
        .find(|field| field.name() == key)
        .ok_or_else(|| error(format!("max_pt: missing partition field '{key}'")))?;
    let data_type = field.data_type();
    // Scan metadata, not table rows. This also skips empty catalog partitions and
    // uses the selected branch/snapshot and the table's read authorization checks.
    let plan = table
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .map_err(to_datafusion_error)?;
    // Planning authorizes the scan, but row filters and masks are applied only
    // when reading rows. Split partitions still contain raw metadata values.
    if plan
        .splits()
        .iter()
        .any(|split| split.has_query_auth_rules())
    {
        return Err(error(
            "max_pt requires unrestricted table metadata; query authorization returned row filters or column masks",
        ));
    }
    let mut maximum: Option<Datum> = None;
    for split in plan.splits() {
        if split.data_files().is_empty() {
            continue;
        }
        let Some(value) = split
            .partition()
            .get_datum(0, data_type)
            .map_err(to_datafusion_error)?
        else {
            continue;
        };
        if let Some(previous) = &maximum {
            // Paimon follows Java's total float order: NaN is greatest and
            // negative zero precedes positive zero.
            let ordering = match (&value, previous) {
                (Datum::Float(a), Datum::Float(b)) => {
                    let canonical = |v: f32| if v.is_nan() { f32::NAN } else { v };
                    Some(canonical(*a).total_cmp(&canonical(*b)))
                }
                (Datum::Double(a), Datum::Double(b)) => {
                    let canonical = |v: f64| if v.is_nan() { f64::NAN } else { v };
                    Some(canonical(*a).total_cmp(&canonical(*b)))
                }
                _ => value.partial_cmp(previous),
            }
            .ok_or_else(|| error("max_pt: partition values cannot be compared"))?;
            if !ordering.is_gt() {
                continue;
            }
        }
        maximum = Some(value);
    }
    let maximum = maximum.ok_or_else(|| {
        error(format!(
            "{identifier} has no non-null partitions containing data files"
        ))
    })?;
    match &maximum {
        Datum::Bytes(bytes) => return Ok(paimon::spec::decode_java_utf8(bytes)),
        Datum::Float(value) => return Ok(paimon::spec::format_java_float(*value)),
        Datum::Double(value) => return Ok(paimon::spec::format_java_double(*value)),
        Datum::Timestamp { millis, nanos } => return timestamp_string(*millis, *nanos, None),
        Datum::LocalZonedTimestamp { millis, nanos } => {
            return timestamp_string(
                *millis,
                *nanos,
                state.config_options().execution.time_zone.as_deref(),
            )
        }
        _ => {}
    }
    let arrow_type = paimon::arrow::paimon_type_to_arrow(data_type).map_err(to_datafusion_error)?;
    let scalar = crate::physical_plan::scan::datum_to_scalar(maximum, &arrow_type)
        .ok_or_else(|| error(format!("max_pt: unsupported partition type {data_type:?}")))?;
    match scalar.cast_to(&DataType::Utf8)? {
        ScalarValue::Utf8(Some(value)) => Ok(value),
        _ => Err(error("max_pt: could not format partition value")),
    }
}

fn timestamp_string(millis: i64, nanos: i32, timezone: Option<&str>) -> Result<String> {
    let nanos = u32::try_from(millis.rem_euclid(1000) * 1_000_000 + i64::from(nanos))
        .map_err(|_| error("max_pt: invalid timestamp partition"))?;
    let timestamp = chrono::DateTime::from_timestamp(millis.div_euclid(1000), nanos)
        .ok_or_else(|| error("max_pt: timestamp partition out of range"))?;
    let value = if let Some(timezone) = timezone {
        let zone: Tz = timezone
            .parse()
            .map_err(|e| error(format!("max_pt: invalid timezone: {e}")))?;
        timestamp
            .with_timezone(&zone)
            .format("%Y-%m-%d %H:%M:%S%.f")
            .to_string()
    } else {
        timestamp.format("%Y-%m-%d %H:%M:%S%.f").to_string()
    };
    Ok(if value.contains('.') {
        value
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    } else {
        value
    })
}

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(message.into())
}
