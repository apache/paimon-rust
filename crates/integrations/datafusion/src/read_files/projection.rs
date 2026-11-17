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

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::common::ExprSchema;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SQLOptions;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Cte, Query, Select, SelectItem, TableFactor, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
use datafusion::sql::unparser::Unparser;

use super::{error, is_read_files, GENERATED_METADATA};

/// The SQL planner has no hidden-column facility. Expand wildcards against the
/// FROM schema before planning the real projection, excluding only generated
/// metadata on direct read_files sources. Explicit exports through a subquery,
/// CTE or view are ordinary visible columns.
pub(crate) fn sql<'a>(
    ctx: &'a SessionContext,
    sql: &'a str,
    options: SQLOptions,
) -> futures::future::BoxFuture<'a, Result<DataFrame>> {
    // Keep this additional planning layer off SQLContext's already large future.
    Box::pin(async move {
        // The AST check below is authoritative; this avoids reparsing ordinary SQL.
        if !sql.to_ascii_lowercase().contains("read_files") {
            return ctx.sql_with_options(sql, options).await;
        }
        let mut statements = Parser::parse_sql(&GenericDialect, sql)?;
        let [statement] = statements.as_mut_slice() else {
            return ctx.sql_with_options(sql, options).await;
        };
        struct FindSource;
        impl VisitorMut for FindSource {
            type Break = ();
            fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
                let name = match factor {
                    TableFactor::Table {
                        name,
                        args: Some(_),
                        ..
                    }
                    | TableFactor::Function { name, .. } => name,
                    _ => return ControlFlow::Continue(()),
                };
                if is_read_files(name) {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            }
        }
        if statement.visit(&mut FindSource).is_continue() {
            return ctx.sql_with_options(sql, options).await;
        }
        let mut state = ctx.state();
        let Some(function) = state.table_functions().get("read_files").cloned() else {
            return ctx.sql_with_options(sql, options).await;
        };
        // Bind each source once for both schema planning and execution. The cache
        // belongs to this statement's cloned state, never to the shared session.
        state.register_udtf(
            "read_files",
            Arc::new(CachedSource {
                function: function.function().clone(),
                providers: Mutex::new(HashMap::new()),
            }),
        );
        let mut rewriter = Wildcards {
            state: state.clone(),
            scopes: Vec::new(),
            originals: Vec::new(),
            rewritten: HashMap::new(),
        };
        if let ControlFlow::Break(error) = statement.visit(&mut rewriter) {
            return Err(*error);
        }
        SessionContext::new_with_state(state)
            .sql_with_options(&statement.to_string(), options)
            .await
    })
}

#[derive(Debug)]
struct CachedSource {
    function: Arc<dyn TableFunctionImpl>,
    providers: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
}

impl TableFunctionImpl for CachedSource {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let key = format!("{:?}", args.exprs());
        let mut providers = self
            .providers
            .lock()
            .map_err(|_| error("source cache poisoned"))?;
        if let Some(provider) = providers.get(&key) {
            return Ok(provider.clone());
        }
        let provider = self.function.call_with_args(args)?;
        providers.insert(key, provider.clone());
        Ok(provider)
    }
}

struct Wildcards {
    state: SessionState,
    scopes: Vec<Vec<ScopedCte>>,
    originals: Vec<usize>,
    rewritten: HashMap<usize, Query>,
}

#[derive(Clone)]
struct ScopedCte {
    cte: Cte,
    // Identity of the original boxed AST node during this visit, never dereferenced.
    identity: usize,
    recursive: bool,
}

fn direct_sources(factor: &TableFactor, state: &SessionState, sources: &mut Vec<String>) {
    match factor {
        TableFactor::Table {
            name,
            args: Some(_),
            alias,
            ..
        }
        | TableFactor::Function { name, alias, .. }
            if is_read_files(name) =>
        {
            sources.push(alias.as_ref().map_or_else(
                || "read_files()".to_string(),
                |alias| {
                    if alias.name.quote_style.is_none()
                        && state.config_options().sql_parser.enable_ident_normalization
                    {
                        alias.name.value.to_ascii_lowercase()
                    } else {
                        alias.name.value.clone()
                    }
                },
            ));
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            direct_sources(&table_with_joins.relation, state, sources);
            for join in &table_with_joins.joins {
                direct_sources(&join.relation, state, sources);
            }
        }
        _ => {}
    }
}

fn aliased_source_join(factor: &TableFactor, state: &SessionState) -> bool {
    let TableFactor::NestedJoin {
        table_with_joins,
        alias,
    } = factor
    else {
        return false;
    };
    let mut sources = Vec::new();
    direct_sources(factor, state, &mut sources);
    (!sources.is_empty() && alias.is_some())
        || aliased_source_join(&table_with_joins.relation, state)
        || table_with_joins
            .joins
            .iter()
            .any(|join| aliased_source_join(&join.relation, state))
}

impl Wildcards {
    fn expand(&self, select: &mut Select) -> Result<()> {
        if !select.projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
            )
        }) {
            return Ok(());
        }
        let mut sources = Vec::new();
        for from in &select.from {
            direct_sources(&from.relation, &self.state, &mut sources);
            for join in &from.joins {
                direct_sources(&join.relation, &self.state, &mut sources);
            }
        }
        if sources.is_empty() {
            return Ok(());
        }
        // DataFusion collapses all qualifiers under a parenthesized join alias
        // before projecting. At that point generated and explicitly exported
        // metadata cannot be distinguished reliably. Reject this unsupported
        // wildcard shape instead of silently exposing or dropping a column.
        if select.from.iter().any(|from| {
            aliased_source_join(&from.relation, &self.state)
                || from
                    .joins
                    .iter()
                    .any(|join| aliased_source_join(&join.relation, &self.state))
        }) {
            return Err(error("wildcards on aliased parenthesized joins containing read_files are not supported; alias the individual sources instead"));
        }
        let ctes = self
            .scopes
            .last()
            .unwrap()
            .iter()
            .map(|scoped| {
                let mut cte = scoped.cte.clone();
                if let Some(query) = self.rewritten.get(&scoped.identity) {
                    *cte.query = query.clone();
                }
                cte.to_string()
            })
            .collect::<Vec<_>>();
        let prefix = if ctes.is_empty() {
            String::new()
        } else {
            let recursive = if self.scopes.last().unwrap().iter().any(|cte| cte.recursive) {
                "RECURSIVE "
            } else {
                ""
            };
            format!("WITH {recursive}{} ", ctes.join(", "))
        };
        let from = select
            .from
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let mut projection = Vec::new();
        for item in &select.projection {
            if !matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
            ) {
                projection.push(item.clone());
                continue;
            }
            let sql = format!("{prefix}SELECT {item} FROM {from}");
            let state = self.state.clone();
            let plan = crate::runtime::block_on_with_runtime(
                async move { state.create_logical_plan(&sql).await },
                "read_files: wildcard planning thread panicked",
            )?;
            let LogicalPlan::Projection(plan) = plan else {
                return Err(error("expected a wildcard projection"));
            };
            for expr in plan.expr {
                if let Expr::Column(column) = &expr {
                    let field = plan.input.schema().field_from_column(column)?;
                    if field.metadata().contains_key(GENERATED_METADATA)
                        && column
                            .relation
                            .as_ref()
                            .is_some_and(|name| sources.iter().any(|source| source == name.table()))
                    {
                        continue;
                    }
                }
                projection.push(SelectItem::UnnamedExpr(
                    Unparser::default().expr_to_sql(&expr)?,
                ));
            }
        }
        select.projection = projection;
        Ok(())
    }
}

impl VisitorMut for Wildcards {
    type Break = Box<DataFusionError>;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        let original = query as *const Query as usize;
        let mut scope = self.scopes.last().cloned().unwrap_or_default();
        // A nonrecursive CTE can refer only to preceding CTEs. In particular,
        // do not plan its own original wildcard while expanding its body.
        if let Some(index) = scope.iter().position(|cte| cte.identity == original) {
            scope.truncate(index + usize::from(scope[index].recursive));
        }
        self.originals.push(original);
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let normalizer = datafusion::sql::planner::IdentNormalizer::new(
                    self.state
                        .config_options()
                        .sql_parser
                        .enable_ident_normalization,
                );
                scope.retain(|outer| {
                    normalizer.normalize(outer.cte.alias.name.clone())
                        != normalizer.normalize(cte.alias.name.clone())
                });
                scope.push(ScopedCte {
                    cte: cte.clone(),
                    identity: cte.query.as_ref() as *const Query as usize,
                    recursive: with.recursive,
                });
            }
        }
        self.scopes.push(scope);
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, select: &mut Select) -> ControlFlow<Self::Break> {
        match self.expand(select) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(Box::new(error)),
        }
    }

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        self.scopes.pop();
        self.rewritten
            .insert(self.originals.pop().unwrap(), query.clone());
        ControlFlow::Continue(())
    }
}
