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

use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::logical_expr::Volatility;
use datafusion::sql::sqlparser::ast::{
    visit_expressions, DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentClause, FunctionArguments, Ident, NullTreatment, ObjectName, Select,
    SelectItem, Statement, Value, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
use std::ops::ControlFlow;

pub(crate) fn is_aggregate(name: &str) -> bool {
    matches!(
        name,
        "count_if"
            | "any"
            | "any_value"
            | "first"
            | "approx_count_distinct"
            | "max_by"
            | "min_by"
            | "arg_max"
            | "listagg"
            | "group_concat"
    )
}
fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(message.into())
}
fn expression(sql: &str) -> Result<Expr> {
    Parser::new(&GenericDialect {})
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_expr())
        .map_err(|e| error(e.to_string()))
}
fn arguments(call: &Function) -> Result<Vec<Expr>> {
    if !matches!(call.parameters, FunctionArguments::None) || call.uses_odbc_syntax {
        return Err(error(
            "Compatibility aggregates require ordinary positional arguments",
        ));
    }
    let FunctionArguments::List(args) = &call.args else {
        return Err(error("Expected positional arguments"));
    };
    args.args
        .iter()
        .map(|a| match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Ok(e.clone()),
            _ => Err(error("Expected positional expression arguments")),
        })
        .collect()
}
fn set_args(call: &mut Function, args: Vec<Expr>) {
    if let FunctionArguments::List(list) = &mut call.args {
        list.args = args
            .into_iter()
            .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
            .collect();
    }
}
fn lower_aggregate(call: &Function, name: &str, state: &SessionState) -> Result<Expr> {
    if name == "group_concat" {
        return lower_group_concat(call, state);
    }
    let args = arguments(call)?;
    let mut lowered = call.clone();
    if !call.within_group.is_empty() && name != "listagg" {
        return Err(error(format!("{name} does not accept WITHIN GROUP")));
    }
    let list = match &call.args {
        FunctionArguments::List(list) => list,
        _ => unreachable!(),
    };
    if list.clauses.iter().any(|clause| {
        !matches!(
            clause,
            FunctionArgumentClause::OrderBy(_) | FunctionArgumentClause::IgnoreOrRespectNulls(_)
        )
    }) {
        return Err(error(format!("Unsupported {name} argument modifier")));
    }
    let arity = match name {
        "first" | "any_value" | "approx_count_distinct" | "listagg" => 1..=2,
        "max_by" | "min_by" | "arg_max" => 2..=2,
        _ => 1..=1,
    };
    if !arity.contains(&args.len()) {
        return Err(error(format!(
            "{name} expects {} to {} arguments",
            arity.start(),
            arity.end()
        )));
    }
    match name {
        "count_if" => {
            lowered.name = ObjectName::from(vec![Ident::new("count")]);
            set_args(
                &mut lowered,
                vec![expression(&format!(
                    "CASE WHEN ({}) THEN 1 ELSE NULL END",
                    args[0]
                ))?],
            );
        }
        "any" => {
            lowered.name = ObjectName::from(vec![Ident::new("bool_or")]);
        }
        "first" | "any_value" => {
            lowered.name = ObjectName::from(vec![Ident::new("first_value")]);
            if let Some(flag) = args.get(1) {
                let Expr::Value(value) = flag else {
                    return Err(error(format!(
                        "{name} ignoreNulls must be a boolean literal"
                    )));
                };
                let Value::Boolean(ignore) = value.value else {
                    return Err(error(format!(
                        "{name} ignoreNulls must be a boolean literal"
                    )));
                };
                if lowered.null_treatment.is_some()
                    || list
                        .clauses
                        .iter()
                        .any(|c| matches!(c, FunctionArgumentClause::IgnoreOrRespectNulls(_)))
                {
                    return Err(error("Specify NULL handling only once"));
                }
                lowered.null_treatment = Some(if ignore {
                    NullTreatment::IgnoreNulls
                } else {
                    NullTreatment::RespectNulls
                });
            }
            set_args(&mut lowered, vec![args[0].clone()]);
        }
        "approx_count_distinct" => {
            if let Some(rsd) = args.get(1) {
                let Expr::Value(value) = rsd else {
                    return Err(error("relativeSD must be a numeric literal"));
                };
                let Value::Number(value, _) = &value.value else {
                    return Err(error("relativeSD must be a numeric literal"));
                };
                let rsd = value
                    .parse::<f64>()
                    .map_err(|_| error("Invalid relativeSD"))?;
                // Native HLL uses 2^14 registers, with relative standard error 1.04/sqrt(m).
                if !rsd.is_finite() || !(0.008125..=0.39).contains(&rsd) {
                    return Err(error(
                        "relativeSD must be between 0.008125 and 0.39 for the native HLL precision",
                    ));
                }
            }
            lowered.name = ObjectName::from(vec![Ident::new("approx_distinct")]);
            set_args(&mut lowered, vec![args[0].clone()]);
            return expression(&format!("CAST({} AS BIGINT)", Expr::Function(lowered)));
        }
        "max_by" | "min_by" | "arg_max" => {
            if call.over.is_some()
                || call.null_treatment.is_some()
                || !list.clauses.is_empty()
                || list.duplicate_treatment == Some(DuplicateTreatment::Distinct)
            {
                return Err(error(format!(
                    "{name} does not support OVER, DISTINCT or additional ordering/NULL modifiers"
                )));
            }
            lowered.name = ObjectName::from(vec![Ident::new(format!("__paimon_{name}"))]);
        }
        "listagg" => {
            lowered.name = ObjectName::from(vec![Ident::new("string_agg")]);
            if args.len() == 1 {
                set_args(&mut lowered, vec![args[0].clone(), expression("''")?]);
            }
            if !lowered.within_group.is_empty() {
                if list
                    .clauses
                    .iter()
                    .any(|c| matches!(c, FunctionArgumentClause::OrderBy(_)))
                {
                    return Err(error("Specify LISTAGG ordering only once"));
                }
                let mut order = std::mem::take(&mut lowered.within_group);
                // Ordered-set literals are values, not SELECT-list positions.
                for key in &mut order {
                    key.expr = Expr::Nested(Box::new(key.expr.clone()));
                }
                if let FunctionArguments::List(list) = &mut lowered.args {
                    list.clauses.push(FunctionArgumentClause::OrderBy(order));
                }
            }
        }
        _ => unreachable!(),
    }
    Ok(Expr::Function(lowered))
}

fn contains_volatile_function(expr: &Expr, state: &SessionState) -> bool {
    matches!(
        visit_expressions(expr, |expr| {
            if let Expr::Function(call) = expr {
                let name = if call.name.0.len() == 1 {
                    let Some(ident) = call.name.0[0].as_ident() else {
                        return ControlFlow::Continue(());
                    };
                    if ident.quote_style.is_some() {
                        ident.value.clone()
                    } else {
                        ident.value.to_ascii_lowercase()
                    }
                } else {
                    call.name.to_string()
                };
                let volatile = state.scalar_functions().get(&name).is_some_and(|function| {
                    function.signature().volatility == Volatility::Volatile
                }) || state.higher_order_functions().get(&name).is_some_and(
                    |function| function.signature().volatility == Volatility::Volatile,
                ) || state.aggregate_functions().get(&name).is_some_and(
                    |function| function.signature().volatility == Volatility::Volatile,
                ) || state.window_functions().get(&name).is_some_and(|function| {
                    function.signature().volatility == Volatility::Volatile
                });
                if volatile {
                    return ControlFlow::Break(());
                }
            }
            ControlFlow::Continue(())
        }),
        ControlFlow::Break(())
    )
}

fn lower_group_concat(call: &Function, state: &SessionState) -> Result<Expr> {
    let args = arguments(call)?;
    if args.is_empty() || !call.within_group.is_empty() || call.null_treatment.is_some() {
        return Err(error(
            "GROUP_CONCAT requires expressions and does not accept WITHIN GROUP or NULL modifiers",
        ));
    }
    let mut aggregate = call.clone();
    aggregate.name = ObjectName::from(vec![Ident::new("array_agg")]);
    aggregate.null_treatment = Some(NullTreatment::IgnoreNulls);
    let FunctionArguments::List(list) = &mut aggregate.args else {
        unreachable!()
    };
    if args.len() > 1 && list.duplicate_treatment == Some(DuplicateTreatment::Distinct) {
        return Err(error("GROUP_CONCAT DISTINCT supports one expression"));
    }
    let mut separator = None;
    let mut clauses = Vec::new();
    for clause in &list.clauses {
        match clause {
            FunctionArgumentClause::Separator(value) => {
                if separator.is_some() || !matches!(value.value, Value::SingleQuotedString(_)) {
                    return Err(error("GROUP_CONCAT SEPARATOR must be one string literal"));
                }
                separator = Some(Expr::Value(value.clone()));
            }
            FunctionArgumentClause::OrderBy(ordering) => {
                let mut ordering = ordering.clone();
                for key in &mut ordering {
                    if let Expr::Value(value) = &key.expr {
                        if let Value::Number(number, _) = &value.value {
                            if number.chars().all(|ch| ch.is_ascii_digit()) {
                                let index = number
                                    .parse::<usize>()
                                    .ok()
                                    .and_then(|index| index.checked_sub(1))
                                    .filter(|index| *index < args.len())
                                    .ok_or_else(|| {
                                        error(format!(
                                        "GROUP_CONCAT ORDER BY position must be between 1 and {}",
                                        args.len()
                                    ))
                                    })?;
                                if contains_volatile_function(&args[index], state) {
                                    return Err(error("GROUP_CONCAT ORDER BY position cannot reference a volatile expression; compute the expression in a subquery first"));
                                }
                                key.expr = Expr::Nested(Box::new(args[index].clone()));
                            } else {
                                // DataFusion treats a bare numeric aggregate sort key
                                // as an input-column index. Non-integer literals here
                                // are ordinary constants, not argument positions.
                                key.expr = Expr::Nested(Box::new(key.expr.clone()));
                            }
                        }
                    }
                }
                clauses.push(FunctionArgumentClause::OrderBy(ordering));
            }
            _ => return Err(error("Unsupported GROUP_CONCAT argument modifier")),
        }
    }
    list.clauses = clauses;
    let value = if args.len() == 1 {
        args[0].clone()
    } else {
        // Evaluate every input once. A CASE-based NULL guard would evaluate
        // volatile arguments again when concatenating the non-NULL row.
        let strings = args
            .iter()
            .map(|arg| format!("CAST(({arg}) AS STRING)"))
            .collect::<Vec<_>>()
            .join(", ");
        expression(&format!("__paimon_group_concat_row({strings})"))?
    };
    set_args(&mut aggregate, vec![value]);
    let separator = separator.unwrap_or(expression("','")?);
    // Aggregate before converting to text so DISTINCT and numeric ORDER BY
    // retain their original types. Empty/all-NULL groups return NULL, but a
    // group containing an empty string still returns an empty string. Convert
    // elements with Arrow's STRING cast after aggregation so all argument
    // counts share the same formatting, including integral floating values.
    expression(&format!(
        "CASE WHEN cardinality({aggregate}) > 0 THEN array_to_string(arrow_cast({aggregate}, 'List(Utf8)'), {separator}) ELSE CAST(NULL AS STRING) END"
    ))
}

struct Rewriter<'a> {
    state: &'a SessionState,
    changed: bool,
    projections: Vec<Vec<(usize, Expr)>>,
}
impl VisitorMut for Rewriter<'_> {
    type Break = DataFusionError;
    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
        self.projections.push(
            select
                .projection
                .iter()
                .enumerate()
                .filter_map(|(i, item)| match item {
                    SelectItem::UnnamedExpr(expr) => Some((i, expr.clone())),
                    _ => None,
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
    fn post_visit_expr(&mut self, node: &mut Expr) -> ControlFlow<Self::Break> {
        let replacement = (|| -> Result<Option<Expr>> {
            if let Expr::Function(call) = node {
                if call.name.0.len() == 1 {
                    if let Some(id) = call.name.0[0].as_ident() {
                        let name = if id.quote_style.is_some() {
                            id.value.clone()
                        } else {
                            id.value.to_ascii_lowercase()
                        };
                        let registered = self.state.scalar_functions().contains_key(&name)
                            || self.state.aggregate_functions().contains_key(&name)
                            || self.state.window_functions().contains_key(&name)
                            || self.state.higher_order_functions().contains_key(&name);
                        if is_aggregate(&name) && !registered {
                            return lower_aggregate(call, &name, self.state).map(Some);
                        }
                    }
                }
            }
            let (negated, any, input, pattern, escape, insensitive) = match node {
                Expr::Like {
                    negated,
                    any,
                    expr,
                    pattern,
                    escape_char: Some(escape),
                } => (*negated, *any, expr, pattern, escape, false),
                Expr::ILike {
                    negated,
                    any,
                    expr,
                    pattern,
                    escape_char: Some(escape),
                } => (*negated, *any, expr, pattern, escape, true),
                _ => return Ok(None),
            };
            let Value::SingleQuotedString(escape) = &escape.value else {
                return Err(error(
                    "LIKE ESCAPE must be a single-character string literal",
                ));
            };
            if escape.chars().count() != 1 {
                return Err(error("LIKE ESCAPE must contain exactly one character"));
            }
            if escape == "\\" {
                return Ok(None);
            }
            if any {
                return Err(error("LIKE ANY with a custom escape is not supported"));
            }
            let mut replacement =
                expression(&format!("__paimon_like(NULL,NULL,NULL,{insensitive})"))?;
            if let Expr::Function(call) = &mut replacement {
                set_args(
                    call,
                    vec![
                        (**input).clone(),
                        (**pattern).clone(),
                        expression(&format!("'{}'", escape.replace('\'', "''")))?,
                        expression(if insensitive { "TRUE" } else { "FALSE" })?,
                    ],
                );
            }
            if negated {
                replacement = expression(&format!("NOT ({replacement})"))?;
            }
            Ok(Some(replacement))
        })();
        match replacement {
            Ok(Some(replacement)) => {
                *node = replacement;
                self.changed = true;
                ControlFlow::Continue(())
            }
            Ok(None) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    }
}
pub(crate) fn rewrite(statement: &mut Statement, state: &SessionState) -> Result<bool> {
    let mut rewriter = Rewriter {
        state,
        changed: false,
        projections: Vec::new(),
    };
    match statement.visit(&mut rewriter) {
        ControlFlow::Break(error) => Err(error),
        _ => Ok(rewriter.changed),
    }
}
