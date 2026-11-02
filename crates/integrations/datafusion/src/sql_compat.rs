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
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr,
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
            | "collect_list"
            | "listagg"
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
fn filter(call: &mut Function, predicate: Expr) {
    call.filter = Some(Box::new(match call.filter.take() {
        Some(original) => Expr::BinaryOp {
            left: Box::new(Expr::Nested(original)),
            op: BinaryOperator::And,
            right: Box::new(Expr::Nested(Box::new(predicate))),
        },
        None => predicate,
    }));
}
fn lower_aggregate(call: &Function, name: &str) -> Result<Expr> {
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
            let direction = if name == "min_by" { "ASC" } else { "DESC" };
            let Expr::Function(mut ordered) = expression(&format!(
                "first_value({} ORDER BY ({}) {direction} NULLS LAST)",
                args[0], args[1]
            ))?
            else {
                unreachable!()
            };
            ordered.filter = lowered.filter.take();
            filter(
                &mut ordered,
                Expr::IsNotNull(Box::new(Expr::Nested(Box::new(args[1].clone())))),
            );
            if name == "arg_max" {
                filter(
                    &mut ordered,
                    Expr::IsNotNull(Box::new(Expr::Nested(Box::new(args[0].clone())))),
                );
            }
            return Ok(Expr::Function(ordered));
        }
        "collect_list" => {
            lowered.name = ObjectName::from(vec![Ident::new("array_agg")]);
            filter(
                &mut lowered,
                Expr::IsNotNull(Box::new(Expr::Nested(Box::new(args[0].clone())))),
            );
            let Expr::Function(mut coalesce) = expression("coalesce(NULL, ARRAY[])")? else {
                unreachable!()
            };
            set_args(
                &mut coalesce,
                vec![Expr::Function(lowered), expression("ARRAY[]")?],
            );
            return Ok(Expr::Function(coalesce));
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
                            return lower_aggregate(call, &name).map(Some);
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
