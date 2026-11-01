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

use datafusion::arrow::array::{
    new_empty_array, Array, ArrayRef, BooleanArray, FixedSizeListArray, Int32Array, LargeListArray,
    ListArray, MapArray, StructArray, UInt64Array,
};
use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::{
    ast::{
        self, visit_expressions_mut, Expr, FunctionArg, FunctionArgExpr, FunctionArguments,
        SetExpr, Statement, VisitMut, VisitorMut,
    },
    dialect::GenericDialect,
    parser::Parser,
};
use std::ops::ControlFlow;
use std::sync::Arc;

fn plan_error(message: &str) -> DataFusionError {
    DataFusionError::Plan(message.into())
}
fn decoded(mut ty: &DataType) -> &DataType {
    while let DataType::Dictionary(_, v) = ty {
        ty = v;
    }
    ty
}
fn output_fields(ty: &DataType) -> Result<Fields> {
    let mut fields = vec![Field::new("pos", DataType::Int32, true)];
    match decoded(ty) {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            fields.push(Field::new("col", f.data_type().clone(), true))
        }
        DataType::Map(f, _) => {
            let DataType::Struct(entries) = f.data_type() else {
                return Err(plan_error("Invalid map entries"));
            };
            fields.push(Field::new("key", entries[0].data_type().clone(), true));
            fields.push(Field::new("value", entries[1].data_type().clone(), true));
        }
        DataType::Null => fields.push(Field::new("col", DataType::Null, true)),
        _ => return Err(plan_error("explode expects an array or map")),
    }
    Ok(fields.into())
}
#[derive(Debug, PartialEq, Eq, Hash)]
struct Explode {
    signature: Signature,
}
pub(crate) fn register(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(Explode {
        signature: Signature::user_defined(Volatility::Immutable),
    }));
}
impl ScalarUDFImpl for Explode {
    fn name(&self) -> &str {
        "__paimon_explode"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let [ty, flag] = types else {
            return Err(plan_error("explode expects collection and outer flag"));
        };
        output_fields(ty)?;
        if !matches!(flag, DataType::Boolean | DataType::Null) {
            return Err(plan_error("explode outer flag must be BOOLEAN"));
        }
        Ok(vec![decoded(ty).clone(), DataType::Boolean])
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(output_fields(&types[0])?),
            true,
        ))))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = &arrays[0];
        let outer = arrays[1].as_any().downcast_ref::<BooleanArray>().unwrap();
        let fields = output_fields(input.data_type())?;
        let values: ArrayRef = match input.data_type() {
            DataType::List(_) => input
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .values()
                .clone(),
            DataType::LargeList(_) => input
                .as_any()
                .downcast_ref::<LargeListArray>()
                .unwrap()
                .values()
                .clone(),
            DataType::FixedSizeList(_, _) => input
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap()
                .values()
                .clone(),
            DataType::Map(_, _) => Arc::new(
                input
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .unwrap()
                    .entries()
                    .clone(),
            ),
            _ => new_empty_array(&DataType::Null),
        };
        let mut offsets = vec![0i32];
        let mut indices = Vec::new();
        let mut positions = Vec::new();
        for row in 0..input.len() {
            if outer.is_null(row) {
                return Err(plan_error("explode outer flag must be non-null"));
            }
            let (start, len) = if input.is_null(row) {
                (0, 0)
            } else {
                match input.data_type() {
                    DataType::List(_) => {
                        let a = input.as_any().downcast_ref::<ListArray>().unwrap();
                        (
                            a.value_offsets()[row] as usize,
                            a.value_length(row) as usize,
                        )
                    }
                    DataType::LargeList(_) => {
                        let a = input.as_any().downcast_ref::<LargeListArray>().unwrap();
                        (
                            a.value_offsets()[row] as usize,
                            a.value_length(row) as usize,
                        )
                    }
                    DataType::FixedSizeList(_, _) => {
                        let a = input.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                        (a.value_offset(row) as usize, a.value_length() as usize)
                    }
                    DataType::Map(_, _) => {
                        let a = input.as_any().downcast_ref::<MapArray>().unwrap();
                        (
                            a.value_offsets()[row] as usize,
                            a.value_length(row) as usize,
                        )
                    }
                    _ => (0, 0),
                }
            };
            if len == 0 && outer.value(row) {
                indices.push(None);
                positions.push(None);
            }
            for pos in 0..len {
                positions
                    .push(Some(i32::try_from(pos).map_err(|_| {
                        plan_error("explode position exceeds INT range")
                    })?));
                indices.push(Some((start + pos) as u64));
            }
            offsets.push(
                i32::try_from(indices.len())
                    .map_err(|_| plan_error("explode batch exceeds Arrow list offset range"))?,
            );
        }
        let selected = take(values.as_ref(), &UInt64Array::from(indices), None)?;
        let mut columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(positions))];
        if let DataType::Map(_, _) = input.data_type() {
            let entries = selected.as_any().downcast_ref::<StructArray>().unwrap();
            columns.extend(entries.columns().iter().cloned());
        } else {
            columns.push(selected);
        }
        let entries = Arc::new(StructArray::try_new(fields.clone(), columns, None)?);
        let output = Arc::new(ListArray::try_new(
            Arc::new(Field::new("item", DataType::Struct(fields), true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            entries,
            None,
        )?);
        if args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)))
        {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                output.as_ref(),
                0,
            )?))
        } else {
            Ok(ColumnarValue::Array(output))
        }
    }
}

#[derive(Debug)]
pub(crate) struct ExplodeRelationPlanner;
impl datafusion::logical_expr::planner::RelationPlanner for ExplodeRelationPlanner {
    fn plan_relation(
        &self,
        relation: ast::TableFactor,
        context: &mut dyn datafusion::logical_expr::planner::RelationPlannerContext,
    ) -> Result<datafusion::logical_expr::planner::RelationPlanning> {
        use datafusion::logical_expr::planner::{PlannedRelation, RelationPlanning};
        use datafusion::logical_expr::{Expr as LogicalExpr, ExprSchemable, LogicalPlanBuilder};
        let ast::TableFactor::NestedJoin {
            table_with_joins,
            alias: wrapper_alias,
        } = &relation
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };
        let Some(join) = table_with_joins.joins.last() else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };
        let ast::TableFactor::Function {
            name,
            args,
            alias: Some(alias),
            lateral: true,
            with_ordinality: false,
        } = &join.relation
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };
        if name.to_string() != "__paimon_lateral_explode"
            || !matches!(
                join.join_operator,
                ast::JoinOperator::CrossJoin(ast::JoinConstraint::None)
            )
        {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        }
        let [FunctionArg::Unnamed(FunctionArgExpr::Expr(input)), FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(outer))), FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(positional)))] =
            args.as_slice()
        else {
            return Err(plan_error("Invalid lateral explode arguments"));
        };
        let (ast::Value::Boolean(outer), ast::Value::Boolean(positional)) =
            (&outer.value, &positional.value)
        else {
            return Err(plan_error("Invalid lateral explode flags"));
        };
        let mut parent = table_with_joins.as_ref().clone();
        parent.joins.pop();
        let left = context.plan(ast::TableFactor::NestedJoin {
            table_with_joins: Box::new(parent),
            alias: None,
        })?;
        let input = context.sql_to_expr(input.clone(), left.schema())?;
        let fields = output_fields(&input.get_type(left.schema())?)?;
        let fields = fields
            .iter()
            .skip(if *positional { 0 } else { 1 })
            .collect::<Vec<_>>();
        if !alias.columns.is_empty() && alias.columns.len() != fields.len() {
            return Err(plan_error("Wrong number of generator column aliases"));
        }
        let internal = format!("__paimon_explode_{}", uuid::Uuid::new_v4().as_simple());
        let mut columns = left
            .schema()
            .columns()
            .into_iter()
            .map(LogicalExpr::Column)
            .collect::<Vec<_>>();
        let mut expressions = columns.clone();
        expressions.push(
            ScalarUDF::from(Explode {
                signature: Signature::user_defined(Volatility::Immutable),
            })
            .call(vec![input, datafusion::logical_expr::lit(*outer)])
            .alias(&internal),
        );
        let expanded = LogicalPlanBuilder::from(left)
            .project(expressions)?
            .unnest_column(internal.as_str())?
            .unnest_column(internal.as_str())?
            .build()?;
        for (index, field) in fields.iter().enumerate() {
            let name = alias
                .columns
                .get(index)
                .map(|c| context.normalize_ident(c.name.clone()))
                .unwrap_or_else(|| field.name().clone());
            columns.push(
                LogicalExpr::Column(datafusion::common::Column::from_name(format!(
                    "{internal}.{}",
                    field.name()
                )))
                .alias_qualified(
                    Some(datafusion::common::TableReference::bare(
                        context.normalize_ident(alias.name.clone()),
                    )),
                    name,
                ),
            );
        }
        let plan = LogicalPlanBuilder::from(expanded)
            .project(columns)?
            .build()?;
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan,
            wrapper_alias.clone(),
        ))))
    }
}

pub(crate) fn rewrite(statement: &mut Statement) -> Result<bool> {
    struct Lower {
        changed: bool,
    }
    impl VisitorMut for Lower {
        type Break = DataFusionError;
        fn post_visit_select(&mut self, select: &mut ast::Select) -> ControlFlow<Self::Break> {
            if select.lateral_views.is_empty() {
                return ControlFlow::Continue(());
            }
            if select.from.len() != 1 {
                return ControlFlow::Break(plan_error(
                    "LATERAL VIEW requires one FROM relation; use explicit joins",
                ));
            }
            for view in std::mem::take(&mut select.lateral_views) {
                let Expr::Function(function) = view.lateral_view else {
                    return ControlFlow::Break(plan_error("Unsupported LATERAL VIEW expression"));
                };
                let name = function.name.to_string().to_ascii_lowercase();
                if function.name.0.len() != 1
                    || !matches!(
                        name.as_str(),
                        "explode" | "explode_outer" | "posexplode" | "posexplode_outer"
                    )
                {
                    return ControlFlow::Break(plan_error(
                        "LATERAL VIEW supports explode and posexplode, including outer variants",
                    ));
                }
                let FunctionArguments::List(args) = function.args else {
                    return ControlFlow::Break(plan_error("explode requires one argument"));
                };
                if args.duplicate_treatment.is_some()
                    || !args.clauses.is_empty()
                    || function.filter.is_some()
                    || function.over.is_some()
                    || !function.within_group.is_empty()
                {
                    return ControlFlow::Break(plan_error("Generator modifiers are not supported"));
                }
                let [FunctionArg::Unnamed(FunctionArgExpr::Expr(input))] = args.args.as_slice()
                else {
                    return ControlFlow::Break(plan_error(
                        "explode requires one positional expression",
                    ));
                };
                if view.lateral_view_name.0.len() != 1 {
                    return ControlFlow::Break(plan_error(
                        "LATERAL VIEW alias must be a simple identifier",
                    ));
                }
                let positional = name.starts_with("pos");
                let outer = view.outer || name.ends_with("_outer");
                let aliases = if view.lateral_col_alias.is_empty() {
                    String::new()
                } else {
                    format!(
                        "({})",
                        view.lateral_col_alias
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                };
                let sql=format!("SELECT * FROM parent CROSS JOIN LATERAL __paimon_lateral_explode(NULL, {outer}, {positional}) AS {}{aliases}",view.lateral_view_name);
                let mut statement = match Parser::parse_sql(&GenericDialect {}, &sql) {
                    Ok(mut s) => s.remove(0),
                    Err(e) => return ControlFlow::Break(plan_error(&e.to_string())),
                };
                let _: ControlFlow<()> = visit_expressions_mut(&mut statement, |expr| {
                    if matches!(expr,Expr::Value(v) if v.value==ast::Value::Null) {
                        *expr = input.clone();
                    }
                    ControlFlow::Continue(())
                });
                if let Statement::Query(q) = statement {
                    if let SetExpr::Select(mut s) = *q.body {
                        let mut parent = select.from.remove(0);
                        parent.joins.push(s.from[0].joins.remove(0));
                        select.from.push(ast::TableWithJoins {
                            relation: ast::TableFactor::NestedJoin {
                                table_with_joins: Box::new(parent),
                                alias: None,
                            },
                            joins: vec![],
                        });
                    }
                }
                self.changed = true;
            }
            ControlFlow::Continue(())
        }
    }
    let mut lower = Lower { changed: false };
    match statement.visit(&mut lower) {
        ControlFlow::Break(e) => Err(e),
        _ => Ok(lower.changed),
    }
}

/// sqlparser 0.62 mistakes query-ending clauses for an optional generator alias.
/// A tautological WHERE terminates the alias list without changing query results.
pub(crate) fn parseable_sql(sql: &str) -> Result<std::borrow::Cow<'_, str>> {
    use datafusion::sql::sqlparser::{
        keywords::Keyword as K,
        tokenizer::{Token, Tokenizer},
    };
    let tokens = Tokenizer::new(&GenericDialect {}, sql)
        .tokenize_with_location()
        .map_err(|e| plan_error(&format!("SQL parse error: {e}")))?;
    let significant = tokens
        .iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .collect::<Vec<_>>();
    let mut depth = 0i32;
    let mut active = std::collections::HashSet::new();
    let mut positions = Vec::new();
    for (index, token) in significant.iter().enumerate() {
        if token.token == Token::RParen {
            active.remove(&depth);
            depth -= 1;
        }
        if matches!(&token.token,Token::Word(w) if w.keyword==K::VIEW)
            && index > 0
            && matches!(&significant[index-1].token,Token::Word(w) if w.keyword==K::LATERAL)
        {
            active.insert(depth);
        }
        if active.contains(&depth) {
            if let Token::Word(word) = &token.token {
                let next = significant.get(index + 1).map(|t| &t.token);
                let ending = match word.keyword {
                    K::ORDER | K::SORT | K::DISTRIBUTE => {
                        matches!(next,Some(Token::Word(w)) if w.keyword==K::BY)
                    }
                    K::LIMIT | K::OFFSET | K::QUALIFY => true,
                    K::FETCH => {
                        matches!(next,Some(Token::Word(w)) if matches!(w.keyword,K::FIRST|K::NEXT))
                    }
                    K::UNION | K::INTERSECT | K::EXCEPT => {
                        matches!(next, Some(Token::LParen))
                            || matches!(next,Some(Token::Word(w)) if matches!(w.keyword,K::ALL|K::DISTINCT|K::SELECT))
                    }
                    K::WINDOW => {
                        matches!(significant.get(index+2).map(|t|&t.token),Some(Token::Word(w)) if w.keyword==K::AS)
                    }
                    _ => false,
                };
                if ending {
                    let mut offset = 0;
                    for (line_index, line) in sql.split_inclusive('\n').enumerate() {
                        if line_index + 1 == token.span.start.line as usize {
                            offset += line
                                .char_indices()
                                .nth(token.span.start.column as usize - 1)
                                .map(|(i, _)| i)
                                .unwrap_or(line.len());
                            break;
                        }
                        offset += line.len();
                    }
                    positions.push(offset);
                    active.remove(&depth);
                } else if matches!(
                    word.keyword,
                    K::AS | K::WHERE | K::GROUP | K::CLUSTER | K::HAVING | K::LATERAL
                ) {
                    active.remove(&depth);
                }
            }
        }
        if token.token == Token::LParen {
            depth += 1;
        }
    }
    if positions.is_empty() || Parser::parse_sql(&GenericDialect {}, sql).is_ok() {
        return Ok(std::borrow::Cow::Borrowed(sql));
    }
    let mut output = sql.to_string();
    for position in positions.into_iter().rev() {
        output.insert_str(position, " WHERE TRUE ");
    }
    Ok(std::borrow::Cow::Owned(output))
}
