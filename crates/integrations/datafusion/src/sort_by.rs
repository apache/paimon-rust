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

use std::cmp::Ordering;
use std::fmt;
use std::ops::ControlFlow;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::compute::SortOptions;
use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{internal_err, plan_err, DFSchemaRef, DataFusionError, Result};
use datafusion::logical_expr::logical_plan::UserDefinedLogicalNodeCore;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, Sort, UserDefinedLogicalNode};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::sql::sqlparser::ast::{
    self, FunctionArg, FunctionArgExpr, LimitClause, Offset, OffsetRows, OrderBy, OrderByKind,
    SetExpr, Statement, TableFactor, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

/// sqlparser stores a set query's trailing SORT BY on the final SELECT,
/// whereas Spark/Hive apply that clause to the complete set expression.
/// A parenthesized query owns its own ordering and is a scope boundary.
fn trailing_select(body: &mut SetExpr) -> Option<&mut ast::Select> {
    match body {
        SetExpr::Select(select) => Some(select.as_mut()),
        SetExpr::SetOperation { right, .. } => trailing_select(right.as_mut()),
        _ => None,
    }
}

/// Let DataFusion resolve sort expressions, aliases, ordinals and hidden columns,
/// then replace only this query's sort before optimization can make it global.
pub(crate) fn rewrite(statement: &mut Statement) -> Result<bool> {
    struct Rewriter(bool);
    impl VisitorMut for Rewriter {
        type Break = DataFusionError;

        fn post_visit_query(&mut self, query: &mut ast::Query) -> ControlFlow<Self::Break> {
            let direct_select = matches!(query.body.as_ref(), SetExpr::Select(_));
            let Some(select) = trailing_select(query.body.as_mut()) else {
                return ControlFlow::Continue(());
            };
            if select.sort_by.is_empty() {
                return ControlFlow::Continue(());
            }
            if query.order_by.is_some() {
                return ControlFlow::Break(DataFusionError::Plan(
                    "SORT BY cannot be combined with ORDER BY in the same query".into(),
                ));
            }
            if direct_select && matches!(&select.distinct, Some(ast::Distinct::On(_))) {
                return ControlFlow::Break(DataFusionError::Plan(
                    "SORT BY with DISTINCT ON is not supported".into(),
                ));
            }
            for sort in &mut select.sort_by {
                // Spark/Hive SORT BY defaults: ASC NULLS FIRST, DESC NULLS LAST.
                sort.options
                    .nulls_first
                    .get_or_insert(sort.options.asc.unwrap_or(true));
            }
            query.order_by = Some(OrderBy {
                kind: OrderByKind::Expressions(std::mem::take(&mut select.sort_by)),
                interpolate: None,
            });
            let mut wrapper = match Parser::parse_sql(
                &GenericDialect {},
                "SELECT * FROM LATERAL __paimon_sort_by((SELECT 1))",
            ) {
                Ok(mut statements) => match statements.remove(0) {
                    Statement::Query(query) => query,
                    _ => unreachable!(),
                },
                Err(error) => return ControlFlow::Break(DataFusionError::Plan(error.to_string())),
            };
            let SetExpr::Select(wrapper_select) = wrapper.body.as_mut() else {
                unreachable!()
            };
            let TableFactor::Function { args, .. } = &mut wrapper_select.from[0].relation else {
                unreachable!()
            };
            // Pipe operators run after this query's sort/limit. Keep them
            // outside the derived relation so subquery planning cannot remove
            // their ordering or mistake a pipe LIMIT for our temporary OFFSET.
            let pipe_operators = std::mem::take(&mut query.pipe_operators);
            // WITH is shared by the sorted query and its following pipes.
            let with = query.with.take();
            args[0] = FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Subquery(Box::new(
                query.clone(),
            ))));
            wrapper.with = with;
            wrapper.pipe_operators = pipe_operators;
            *query = *wrapper;
            self.0 = true;
            ControlFlow::Continue(())
        }
    }
    let mut rewriter = Rewriter(false);
    match statement.visit(&mut rewriter) {
        ControlFlow::Break(error) => Err(error),
        _ => Ok(rewriter.0),
    }
}

#[derive(Debug)]
pub(crate) struct SortByRelationPlanner;

impl RelationPlanner for SortByRelationPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        let TableFactor::Function {
            name,
            args,
            alias,
            lateral: true,
            with_ordinality: false,
        } = &relation
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };
        if name.to_string() != "__paimon_sort_by" {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        }
        let [FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Subquery(query)))] =
            args.as_slice()
        else {
            return plan_err!("Invalid SORT BY query");
        };
        let mut query = query.clone();
        // DataFusion removes derived-table ORDER BY during SQL planning unless
        // a limit is present. OFFSET 0 keeps the sort until we can replace it;
        // remove this artificial limit immediately, before physical planning.
        let artificial_limit = query.limit_clause.is_none();
        if artificial_limit {
            query.limit_clause = Some(LimitClause::LimitOffset {
                limit: None,
                offset: Some(Offset {
                    value: ast::Expr::Value(ast::Value::Number("0".into(), false).into()),
                    rows: OffsetRows::None,
                }),
                limit_by: vec![],
            });
        }
        let plan = context.plan(TableFactor::Derived {
            lateral: false,
            subquery: query,
            alias: None,
            sample: None,
        })?;
        let plan = if artificial_limit {
            let LogicalPlan::Limit(limit) = plan else {
                return internal_err!("SORT BY query has no temporary limit plan");
            };
            Arc::unwrap_or_clone(limit.input)
        } else {
            plan
        };
        let mut replaced = false;
        let plan = plan
            .transform_down(|plan| {
                if !replaced {
                    if let LogicalPlan::Sort(sort) = plan {
                        replaced = true;
                        return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                            node: Arc::new(LocalSort { sort }),
                        })));
                    }
                }
                Ok(Transformed::no(plan))
            })?
            .data;
        if !replaced {
            return internal_err!("SORT BY query has no sort plan");
        }
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan,
            alias.clone(),
        ))))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LocalSort {
    sort: Sort,
}

impl PartialOrd for LocalSort {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(format!("{self:?}").cmp(&format!("{other:?}")))
    }
}

impl UserDefinedLogicalNodeCore for LocalSort {
    fn name(&self) -> &str {
        "SortBy"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.sort.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        self.sort.input.schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        self.sort
            .expr
            .iter()
            .map(|sort| sort.expr.clone())
            .collect()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SortBy: {:?}", self.sort.expr)
    }
    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != 1 || exprs.len() != self.sort.expr.len() {
            return internal_err!("SortBy expects one input and its sort expressions");
        }
        let mut sort = self.sort.clone();
        sort.input = Arc::new(inputs.remove(0));
        for (sort, expr) in sort.expr.iter_mut().zip(exprs) {
            sort.expr = expr;
        }
        Ok(Self { sort })
    }
}

#[derive(Debug)]
pub(crate) struct SortByExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for SortByExtensionPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        state: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<LocalSort>() else {
            return Ok(None);
        };
        let mut expressions = Vec::with_capacity(node.sort.expr.len());
        for sort in &node.sort.expr {
            expressions.push(PhysicalSortExpr {
                expr: planner.create_physical_expr(
                    &sort.expr,
                    logical_inputs[0].schema(),
                    state,
                    planning_ctx,
                )?,
                options: SortOptions {
                    descending: !sort.asc,
                    nulls_first: sort.nulls_first,
                },
            });
        }
        let input = Arc::clone(&physical_inputs[0]);
        let Some(ordering) = LexOrdering::new(expressions) else {
            return Ok(Some(input));
        };
        Ok(Some(Arc::new(
            SortExec::new(ordering, input)
                .with_preserve_partitioning(true)
                .with_fetch(node.sort.fetch),
        )))
    }
}
