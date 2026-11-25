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

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::common::{Column, DFSchema, ResolvedTableReference, SchemaError, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SQLOptions;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{CopyToSource, Statement};
use datafusion::sql::planner::{object_name_to_table_reference, IdentNormalizer};
use datafusion::sql::sqlparser::ast::helpers::attached_token::AttachedToken;
use datafusion::sql::sqlparser::ast::{
    Cte, ExcludeSelectItem, Ident, Insert, JoinConstraint, JoinOperator, ObjectName, Query, Select,
    SelectItem, SelectItemQualifiedWildcardKind, SetExpr, SetOperator, Statement as SqlStatement,
    TableFactor, TableObject, TableWithJoins, VisitMut, VisitorMut, WildcardAdditionalOptions,
    With,
};
use datafusion::sql::unparser::Unparser;
use paimon::spec::ROW_ID_FIELD_NAME;

use crate::catalog::PlanningTables;
use crate::read_files::{error as read_files_error, is_read_files, GENERATED_METADATA};
use crate::table::{PaimonTableProvider, VIRTUAL_ROW_ID_METADATA};

/// The SQL planner has no hidden-column facility. Expand wildcards against the
/// FROM schema before planning the real projection. Explicit exports through a
/// subquery, CTE or view are ordinary visible columns.
pub(crate) fn sql<'a>(
    ctx: &'a SessionContext,
    sql: &'a str,
    options: SQLOptions,
) -> futures::future::BoxFuture<'a, Result<DataFrame>> {
    sql_with_row_id_visibility(ctx, sql, options, true)
}

pub(crate) async fn view_plan(
    ctx: &SessionContext,
    query: &str,
    hide_row_id: bool,
) -> Result<LogicalPlan> {
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false);
    sql_with_row_id_visibility(ctx, query, options, hide_row_id)
        .await
        .map(DataFrame::into_unoptimized_plan)
}

fn sql_with_row_id_visibility<'a>(
    ctx: &'a SessionContext,
    sql: &'a str,
    options: SQLOptions,
    hide_row_id: bool,
) -> futures::future::BoxFuture<'a, Result<DataFrame>> {
    if !(hide_row_id && sql.contains('*')) && !sql.to_ascii_lowercase().contains("read_files") {
        return Box::pin(ctx.sql_with_options(sql, options));
    }
    // Keep this additional planning layer off SQLContext's already large future.
    Box::pin(async move {
        let mut state = ctx.state();
        let mut statement =
            state.sql_to_statement(sql, &state.config_options().sql_parser.dialect)?;
        let mut sources = FindSources::default();
        let _ = visit_statement(&mut statement, &mut sources);
        let mut cached_read_files = false;
        if sources.read_files {
            if let Some(function) = state.table_functions().get("read_files").cloned() {
                // Bind each source once across schema planning and execution.
                state.register_udtf(
                    "read_files",
                    Arc::new(CachedSource {
                        function: function.function().clone(),
                        providers: Mutex::new(HashMap::new()),
                    }),
                );
                cached_read_files = true;
            }
        }
        let bindings = PlanningTables::default();
        let plan = bindings
            .scope(async {
                let row_id_tables = if hide_row_id && sources.table_wildcards {
                    row_id_tables(&state, &statement).await?
                } else {
                    HashMap::new()
                };
                let positional_layout =
                    insert_data_columns(&state, &mut statement, &row_id_tables)?
                        .map(|columns| (columns, statement.clone()));
                if cached_read_files || !row_id_tables.is_empty() {
                    rewrite_wildcards(&state, &bindings, &mut statement, &row_id_tables)?;
                }
                if let Some((columns, original)) = positional_layout {
                    plan_positional_insert(
                        &state,
                        &bindings,
                        statement,
                        original,
                        columns,
                        cached_read_files,
                    )
                    .await
                } else {
                    state.statement_to_plan(statement).await
                }
            })
            .await?;
        options.verify_plan(&plan)?;
        if cached_read_files {
            SessionContext::new_with_state(state)
                .execute_logical_plan(plan)
                .await
        } else {
            ctx.execute_logical_plan(plan).await
        }
    })
}

fn rewrite_wildcards(
    state: &SessionState,
    bindings: &PlanningTables,
    statement: &mut Statement,
    row_id_tables: &HashMap<ResolvedTableReference, Vec<ObjectName>>,
) -> Result<()> {
    let mut rewriter = Wildcards {
        state: state.clone(),
        bindings: bindings.clone(),
        row_id_tables,
        scopes: Vec::new(),
        originals: Vec::new(),
        rewritten: HashMap::new(),
    };
    match visit_statement(statement, &mut rewriter) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(*error),
    }
}

fn visit_statement<V: VisitorMut>(
    statement: &mut Statement,
    visitor: &mut V,
) -> ControlFlow<V::Break> {
    match statement {
        Statement::Statement(statement) => statement.visit(visitor),
        Statement::Explain(explain) => visit_statement(&mut explain.statement, visitor),
        Statement::CopyTo(copy) => match &mut copy.source {
            CopyToSource::Query(query) => query.visit(visitor),
            CopyToSource::Relation(_) => ControlFlow::Continue(()),
        },
        _ => ControlFlow::Continue(()),
    }
}

#[derive(Default)]
struct FindSources {
    read_files: bool,
    table_wildcards: bool,
}

impl VisitorMut for FindSources {
    type Break = ();

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table {
            name,
            args: Some(_),
            ..
        }
        | TableFactor::Function { name, .. } = factor
        {
            self.read_files |= is_read_files(name);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<()> {
        self.table_wildcards |= select.projection.iter().any(is_wildcard)
            && select.from.iter().any(|from| {
                contains_base_table(&from.relation)
                    || from
                        .joins
                        .iter()
                        .any(|join| contains_base_table(&join.relation))
            });
        ControlFlow::Continue(())
    }
}

fn contains_base_table(factor: &TableFactor) -> bool {
    match factor {
        TableFactor::Table { args: None, .. } => true,
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            contains_base_table(&table_with_joins.relation)
                || table_with_joins
                    .joins
                    .iter()
                    .any(|join| contains_base_table(&join.relation))
        }
        _ => false,
    }
}

async fn row_id_tables(
    state: &SessionState,
    statement: &Statement,
) -> Result<HashMap<ResolvedTableReference, Vec<ObjectName>>> {
    let mut tables = HashMap::new();
    let defaults = &state.config_options().catalog;
    let references: HashSet<_> = state
        .resolve_table_references(statement)?
        .into_iter()
        .map(|table| table.resolve(&defaults.default_catalog, &defaults.default_schema))
        .collect();
    for reference in references {
        if let Ok(schema) = state.schema_for_ref(reference.clone()) {
            if let Some(provider) = schema.table(&reference.table).await? {
                if let Some(provider) = provider.downcast_ref::<PaimonTableProvider>() {
                    if provider.schema().field_with_name(ROW_ID_FIELD_NAME).is_ok() {
                        let columns = provider
                            .table()
                            .schema()
                            .fields()
                            .iter()
                            .map(|field| vec![Ident::with_quote('"', field.name())].into())
                            .collect();
                        tables.insert(reference, columns);
                    }
                }
            }
        }
    }
    Ok(tables)
}

fn insert_data_columns<'a>(
    state: &SessionState,
    statement: &mut Statement,
    row_id_tables: &'a HashMap<ResolvedTableReference, Vec<ObjectName>>,
) -> Result<Option<&'a [ObjectName]>> {
    let Some(insert) = positional_insert(statement) else {
        return Ok(None);
    };
    let TableObject::TableName(name) = &insert.table else {
        return Ok(None);
    };
    let config = state.config_options();
    let reference =
        object_name_to_table_reference(name.clone(), config.sql_parser.enable_ident_normalization)?
            .resolve(
                &config.catalog.default_catalog,
                &config.catalog.default_schema,
            );
    Ok(row_id_tables.get(&reference).map(Vec::as_slice))
}

async fn plan_positional_insert(
    state: &SessionState,
    bindings: &PlanningTables,
    statement: Statement,
    mut original: Statement,
    columns: &[ObjectName],
    cached_read_files: bool,
) -> Result<LogicalPlan> {
    let mut business_columns = statement.clone();
    let Some(insert) = positional_insert(&mut business_columns) else {
        return state.statement_to_plan(statement).await;
    };
    insert.columns = columns.to_vec();
    let plan = state.statement_to_plan(business_columns).await;
    if plan.is_ok() {
        return plan;
    }

    // Keep full-width inserts compatible when they already planned before
    // hiding the virtual column. A newly matching input could otherwise map
    // an extra business column to _ROW_ID, where the sink would discard it.
    if cached_read_files
        && rewrite_wildcards(state, bindings, &mut original, &HashMap::new()).is_err()
    {
        return plan;
    }
    if state.statement_to_plan(original).await.is_ok() {
        return state.statement_to_plan(statement).await.or(plan);
    }
    plan
}

fn positional_insert(statement: &mut Statement) -> Option<&mut Insert> {
    let mut statement = statement;
    while let Statement::Explain(explain) = statement {
        statement = &mut explain.statement;
    }
    let Statement::Statement(statement) = statement else {
        return None;
    };
    let mut statement = statement.as_mut();
    while let SqlStatement::Explain {
        statement: inner, ..
    }
    | SqlStatement::Prepare {
        statement: inner, ..
    } = statement
    {
        statement = inner;
    }
    match statement {
        SqlStatement::Insert(insert) if insert.columns.is_empty() => Some(insert),
        _ => None,
    }
}

fn is_wildcard(item: &SelectItem) -> bool {
    matches!(
        item,
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
    )
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
            .map_err(|_| read_files_error("source cache poisoned"))?;
        if let Some(provider) = providers.get(&key) {
            return Ok(provider.clone());
        }
        let provider = self.function.call_with_args(args)?;
        providers.insert(key, provider.clone());
        Ok(provider)
    }
}

struct Wildcards<'a> {
    state: SessionState,
    bindings: PlanningTables,
    row_id_tables: &'a HashMap<ResolvedTableReference, Vec<ObjectName>>,
    scopes: Vec<Vec<ScopedCte>>,
    originals: Vec<usize>,
    rewritten: HashMap<usize, Query>,
}

#[derive(Clone)]
struct ScopedCte {
    cte: Cte,
    // Identity of the original boxed AST node during this visit, never dereferenced.
    identity: usize,
    with_owner: usize,
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

struct RowIdSource {
    qualifier: TableReference,
    origin: RowIdOrigin,
}

enum RowIdOrigin {
    Column(String),
    Join(Vec<RowIdSource>),
}

impl Wildcards<'_> {
    fn row_id_sources(&self, factor: &TableFactor, sources: &mut Vec<RowIdSource>) {
        let normalize = self
            .state
            .config_options()
            .sql_parser
            .enable_ident_normalization;
        let alias_reference = |ident: &Ident| {
            TableReference::bare(IdentNormalizer::new(normalize).normalize(ident.clone()))
        };
        match factor {
            TableFactor::Table {
                name,
                args: None,
                alias,
                ..
            } => {
                let Ok(reference) = object_name_to_table_reference(name.clone(), normalize) else {
                    return;
                };
                let shadowed = matches!(reference, TableReference::Bare { .. })
                    && self
                        .scopes
                        .last()
                        .into_iter()
                        .flatten()
                        .any(|cte| alias_reference(&cte.cte.alias.name) == reference);
                let defaults = &self.state.config_options().catalog;
                if !shadowed
                    && self.row_id_tables.contains_key(
                        &reference
                            .clone()
                            .resolve(&defaults.default_catalog, &defaults.default_schema),
                    )
                {
                    let name = alias
                        .as_ref()
                        .and_then(|alias| alias.columns.last())
                        .map_or_else(
                            || ROW_ID_FIELD_NAME.to_string(),
                            |column| IdentNormalizer::new(normalize).normalize(column.name.clone()),
                        );
                    sources.push(RowIdSource {
                        qualifier: alias
                            .as_ref()
                            .map_or(reference, |alias| alias_reference(&alias.name)),
                        origin: RowIdOrigin::Column(name),
                    });
                }
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => {
                let start = sources.len();
                self.row_id_sources_in_join(table_with_joins, sources);
                if let Some(alias) = alias.as_ref().filter(|_| sources.len() > start) {
                    let children = sources.split_off(start);
                    sources.push(RowIdSource {
                        qualifier: alias_reference(&alias.name),
                        origin: RowIdOrigin::Join(children),
                    });
                }
            }
            _ => {}
        }
    }

    fn row_id_sources_in_join(&self, from: &TableWithJoins, sources: &mut Vec<RowIdSource>) {
        let start = sources.len();
        self.row_id_sources(&from.relation, sources);
        for join in &from.joins {
            match join.join_operator {
                JoinOperator::Semi(_)
                | JoinOperator::LeftSemi(_)
                | JoinOperator::Anti(_)
                | JoinOperator::LeftAnti(_) => continue,
                JoinOperator::RightSemi(_) | JoinOperator::RightAnti(_) => sources.truncate(start),
                _ => {}
            }
            self.row_id_sources(&join.relation, sources);
        }
    }

    fn expand(&self, select: &mut Select) -> Result<()> {
        if !select.projection.iter().any(is_wildcard) {
            return Ok(());
        }
        let mut file_sources = Vec::new();
        let mut row_id_sources = Vec::new();
        for from in &select.from {
            direct_sources(&from.relation, &self.state, &mut file_sources);
            self.row_id_sources_in_join(from, &mut row_id_sources);
            for join in &from.joins {
                direct_sources(&join.relation, &self.state, &mut file_sources);
            }
        }
        if file_sources.is_empty() && row_id_sources.is_empty() {
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
            return Err(read_files_error("wildcards on aliased parenthesized joins containing read_files are not supported; alias the individual sources instead"));
        }
        let ctes = self
            .scopes
            .last()
            .unwrap()
            .iter()
            .cloned()
            .map(|mut scoped| {
                if let Some(query) = self.rewritten.get(&scoped.identity) {
                    *scoped.cte.query = query.clone();
                }
                scoped
            })
            .collect::<Vec<_>>();
        let single_row_id = match row_id_sources.as_slice() {
            [RowIdSource {
                origin: RowIdOrigin::Column(name),
                ..
            }] if file_sources.is_empty() => Some(name.as_str()),
            _ => None,
        };
        let single_relation = matches!(select.from.as_slice(), [from] if from.joins.is_empty());
        let normalize = self
            .state
            .config_options()
            .sql_parser
            .enable_ident_normalization;
        let mut projection = Vec::new();
        for item in &select.projection {
            let (unqualified, mut item) = match item {
                SelectItem::Wildcard(_) => (true, item.clone()),
                SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::ObjectName(name),
                    _,
                ) => {
                    if let Ok(qualifier) = object_name_to_table_reference(name.clone(), normalize) {
                        if let Some(source) = row_id_sources
                            .iter()
                            .find(|source| source.qualifier == qualifier)
                        {
                            if let RowIdOrigin::Column(name) = &source.origin {
                                let mut wildcard = item.clone();
                                exclude_from_wildcard(&mut wildcard, name);
                                projection.push(wildcard);
                                continue;
                            }
                        } else if !file_sources
                            .iter()
                            .any(|source| source == qualifier.table())
                        {
                            projection.push(item.clone());
                            continue;
                        }
                    }
                    (false, item.clone())
                }
                _ => {
                    projection.push(item.clone());
                    continue;
                }
            };
            if unqualified && single_relation {
                if let Some(name) = single_row_id {
                    exclude_from_wildcard(&mut item, name);
                    projection.push(item);
                    continue;
                }
            }

            // REPLACE expressions must be planned with the original WHERE,
            // grouping and window scope. Only expand the wildcard's shape here.
            let mut shape_item = item.clone();
            let replacements =
                wildcard_options(&mut shape_item).and_then(|options| options.opt_replace.take());
            let plan = match self.probe(&shape_item, &select.from, &ctes) {
                Err(error) if !row_id_sources.is_empty() && missing_field(&error) => {
                    // The probe binds relations outside the original query. Correlated
                    // predicates cannot be resolved there and do not change FROM fields.
                    let mut shape = select.from.clone();
                    for relation in &mut shape {
                        strip_join_conditions(relation);
                    }
                    let _ = shape.visit(&mut StripConditions);
                    let mut cte_shapes = ctes.clone();
                    for scoped in &mut cte_shapes {
                        let _ = scoped.cte.visit(&mut StripConditions);
                    }
                    let shape_probe = self.probe(&shape_item, &shape, &cte_shapes);
                    match (shape_probe, single_row_id) {
                        (Ok(plan), _) => plan,
                        (Err(shape_error), Some(name))
                            if unqualified && missing_field(&shape_error) =>
                        {
                            exclude_from_wildcard(&mut item, name);
                            projection.push(item);
                            continue;
                        }
                        (Err(_), _) => return Err(error),
                    }
                }
                other => other?,
            };
            let LogicalPlan::Projection(plan) = plan else {
                return Err(DataFusionError::Plan(
                    "expected a wildcard projection".to_string(),
                ));
            };
            let mut hidden = hidden_row_id_columns(&plan.input, &row_id_sources);
            hidden.extend(plan.input.schema().iter().filter_map(|(qualifier, field)| {
                let qualifier = qualifier?;
                (field.metadata().contains_key(GENERATED_METADATA)
                    && file_sources
                        .iter()
                        .any(|source| source == qualifier.table()))
                .then(|| Column::new(Some(qualifier.clone()), field.name()))
            }));
            let using = if unqualified && !hidden.is_empty() {
                plan.input.using_columns()?
            } else {
                Vec::new()
            };
            let mut projected: HashSet<_> = if using.is_empty() {
                HashSet::new()
            } else {
                plan.expr
                    .iter()
                    .filter_map(|expr| match expr {
                        Expr::Column(column) => Some(column.clone()),
                        _ => None,
                    })
                    .collect()
            };
            for mut expr in plan.expr {
                if let Expr::Column(column) = &expr {
                    let replacement = replacements.as_ref().and_then(|replace| {
                        replace
                            .items
                            .iter()
                            .find(|item| item.column_name.value == column.name)
                    });
                    if let Some(replacement) = replacement {
                        projection.push(SelectItem::ExprWithAlias {
                            expr: replacement.expr.clone(),
                            alias: Ident::with_quote('"', column.name.clone()),
                        });
                        continue;
                    }
                    if hidden.contains(column) {
                        // USING has already picked one representative for the group.
                        // A default-hidden source must not suppress another source's
                        // explicitly exported field.
                        let replacement = visible_using_column(
                            column,
                            &using,
                            &hidden,
                            &projected,
                            plan.input.schema(),
                        );
                        let Some(replacement) = replacement else {
                            continue;
                        };
                        projected.insert(replacement.clone());
                        expr = Expr::Column(replacement);
                    }
                }
                projection.push(match expr {
                    Expr::Alias(alias) => SelectItem::ExprWithAlias {
                        expr: Unparser::default().expr_to_sql(&alias.expr)?,
                        alias: Ident::with_quote('"', alias.name),
                    },
                    expr => SelectItem::UnnamedExpr(Unparser::default().expr_to_sql(&expr)?),
                });
            }
        }
        if projection.is_empty() {
            return Err(DataFusionError::Plan(
                "SELECT list is empty after resolving * expressions".to_string(),
            ));
        }
        select.projection = projection;
        Ok(())
    }

    fn probe(
        &self,
        item: &SelectItem,
        from: &[TableWithJoins],
        ctes: &[ScopedCte],
    ) -> Result<LogicalPlan> {
        let state = self.state.clone();
        let bindings = self.bindings.clone();
        let mut query = probe_query(&state)?;
        let SetExpr::Select(select) = query.body.as_mut() else {
            return Err(DataFusionError::Internal(
                "expected a probe select".to_string(),
            ));
        };
        select.projection = vec![item.clone()];
        select.from = from.to_vec();
        let statement = |query| Statement::Statement(Box::new(SqlStatement::Query(query)));
        if !ctes.is_empty() {
            let normalize =
                IdentNormalizer::new(state.config_options().sql_parser.enable_ident_normalization);
            let mut references: HashSet<_> = state
                .resolve_table_references(&statement(query.clone()))?
                .into_iter()
                .collect();
            let mut dependencies = Vec::new();
            // CTE definitions can use preceding siblings. Select definitions
            // backwards so an unfinished recursive definition is kept out of
            // its own anchor's probe, which cannot reference the work table.
            for scoped in ctes.iter().rev() {
                let cte = &scoped.cte;
                let name = TableReference::bare(normalize.normalize(cte.alias.name.clone()));
                if references.remove(&name) {
                    references
                        .extend(state.resolve_table_references(&statement(cte.query.clone()))?);
                    dependencies.push(scoped);
                }
            }
            // Keep each WITH block in its original scope. Flattening the
            // blocks can turn an ordinary UNION CTE into a recursive CTE.
            for block in dependencies.chunk_by(|left, right| left.with_owner == right.with_owner) {
                if query.with.is_some() {
                    let mut outer = probe_query(&state)?;
                    outer.body = Box::new(SetExpr::Query(query));
                    query = outer;
                }
                query.with = Some(With {
                    with_token: AttachedToken::empty(),
                    recursive: block[0].recursive,
                    cte_tables: block
                        .iter()
                        .rev()
                        .map(|scoped| scoped.cte.clone())
                        .collect(),
                });
            }
        }
        let statement = statement(query);
        crate::runtime::block_on_with_runtime(
            async move { bindings.scope(state.statement_to_plan(statement)).await },
            "wildcard planning thread panicked",
        )
    }
}

fn probe_query(state: &SessionState) -> Result<Box<Query>> {
    let dialect = &state.config_options().sql_parser.dialect;
    if let Statement::Statement(statement) = state.sql_to_statement("SELECT *", dialect)? {
        if let SqlStatement::Query(query) = *statement {
            return Ok(query);
        }
    }
    Err(DataFusionError::Internal(
        "expected a probe query".to_string(),
    ))
}

fn missing_field(error: &DataFusionError) -> bool {
    matches!(error.find_root(), DataFusionError::SchemaError(error, _)
        if matches!(error.as_ref(), SchemaError::FieldNotFound { .. }))
}

struct StripConditions;

impl VisitorMut for StripConditions {
    type Break = ();

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<()> {
        select.selection = None;
        for relation in &mut select.from {
            strip_join_conditions(relation);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        if let TableFactor::NestedJoin {
            table_with_joins, ..
        } = factor
        {
            strip_join_conditions(table_with_joins);
        }
        ControlFlow::Continue(())
    }
}

fn strip_join_conditions(relation: &mut TableWithJoins) {
    for join in &mut relation.joins {
        let constraint = match &mut join.join_operator {
            JoinOperator::Join(constraint)
            | JoinOperator::Inner(constraint)
            | JoinOperator::Left(constraint)
            | JoinOperator::LeftOuter(constraint)
            | JoinOperator::Right(constraint)
            | JoinOperator::RightOuter(constraint)
            | JoinOperator::FullOuter(constraint)
            | JoinOperator::LeftSemi(constraint)
            | JoinOperator::RightSemi(constraint)
            | JoinOperator::LeftAnti(constraint)
            | JoinOperator::RightAnti(constraint) => constraint,
            _ => continue,
        };
        if matches!(constraint, JoinConstraint::On(_)) {
            *constraint = JoinConstraint::None;
        }
    }
}

fn hidden_row_id_columns(plan: &LogicalPlan, sources: &[RowIdSource]) -> HashSet<Column> {
    if sources.is_empty() {
        return HashSet::new();
    }
    let origin_for = |qualifier: &TableReference| {
        sources
            .iter()
            .find(|source| &source.qualifier == qualifier)
            .map(|source| &source.origin)
    };
    match plan {
        LogicalPlan::TableScan(scan) => match origin_for(&scan.table_name) {
            Some(RowIdOrigin::Column(name)) => columns_with_row_id_metadata(plan.schema(), name),
            _ => HashSet::new(),
        },
        LogicalPlan::SubqueryAlias(alias) => match origin_for(&alias.alias) {
            Some(RowIdOrigin::Column(name)) => columns_with_row_id_metadata(plan.schema(), name),
            Some(RowIdOrigin::Join(children)) => {
                let hidden = hidden_join_projection(&alias.input, children);
                alias
                    .input
                    .schema()
                    .columns()
                    .into_iter()
                    .zip(plan.schema().columns())
                    .filter_map(|(input, output)| hidden.contains(&input).then_some(output))
                    .collect()
            }
            None => HashSet::new(),
        },
        LogicalPlan::Join(join) => hidden_row_id_columns(&join.left, sources)
            .into_iter()
            .chain(hidden_row_id_columns(&join.right, sources))
            .filter(|column| plan.schema().has_column(column))
            .collect(),
        _ => HashSet::new(),
    }
}

fn columns_with_row_id_metadata(schema: &DFSchema, name: &str) -> HashSet<Column> {
    schema
        .iter()
        .filter(|(_, field)| {
            field.name() == name && field.metadata().contains_key(VIRTUAL_ROW_ID_METADATA)
        })
        .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()))
        .collect()
}

fn hidden_join_projection(plan: &LogicalPlan, sources: &[RowIdSource]) -> HashSet<Column> {
    // DataFusion inserts positional projections underneath a JOIN alias to
    // apply explicit column names and to disambiguate duplicate names.
    let LogicalPlan::Projection(projection) = plan else {
        return hidden_row_id_columns(plan, sources);
    };
    let hidden = hidden_join_projection(&projection.input, sources);
    projection
        .expr
        .iter()
        .zip(plan.schema().columns())
        .filter_map(|(expr, output)| {
            let expr = match expr {
                Expr::Alias(alias) => alias.expr.as_ref(),
                expr => expr,
            };
            matches!(expr, Expr::Column(column) if hidden.contains(column)).then_some(output)
        })
        .collect()
}

fn visible_using_column(
    original: &Column,
    groups: &[HashSet<Column>],
    hidden: &HashSet<Column>,
    projected: &HashSet<Column>,
    schema: &DFSchema,
) -> Option<Column> {
    if groups.is_empty() {
        return None;
    }
    let mut related = HashSet::from([original]);
    loop {
        let previous_count = related.len();
        for group in groups {
            if group.iter().any(|column| related.contains(column)) {
                related.extend(
                    group
                        .iter()
                        .filter(|column| column.name == original.name && schema.has_column(column)),
                );
            }
        }
        if related.len() == previous_count {
            break;
        }
    }
    related
        .into_iter()
        .filter(|column| !hidden.contains(*column) && !projected.contains(*column))
        .min()
        .cloned()
}

fn exclude_from_wildcard(item: &mut SelectItem, name: &str) {
    let Some(options) = wildcard_options(item) else {
        return;
    };
    if explicit_wildcard_column(options, name) {
        return;
    }
    let name = vec![Ident::with_quote('"', name)].into();
    let exclude = match options.opt_exclude.take() {
        None => ExcludeSelectItem::Single(name),
        Some(ExcludeSelectItem::Single(column)) => ExcludeSelectItem::Multiple(vec![column, name]),
        Some(ExcludeSelectItem::Multiple(mut columns)) => {
            columns.push(name);
            ExcludeSelectItem::Multiple(columns)
        }
    };
    options.opt_exclude = Some(exclude);
}

fn wildcard_options(item: &mut SelectItem) -> Option<&mut WildcardAdditionalOptions> {
    match item {
        SelectItem::Wildcard(options) | SelectItem::QualifiedWildcard(_, options) => Some(options),
        _ => None,
    }
}

fn explicit_wildcard_column(options: &WildcardAdditionalOptions, name: &str) -> bool {
    fn is_name(column: &ObjectName, name: &str) -> bool {
        column.0.len() == 1
            && column.0[0]
                .as_ident()
                .is_some_and(|ident| ident.value == name)
    }
    options.opt_replace.as_ref().is_some_and(|replace| {
        replace
            .items
            .iter()
            .any(|item| item.column_name.value == name)
    }) || options.opt_except.as_ref().is_some_and(|except| {
        std::iter::once(&except.first_element)
            .chain(&except.additional_elements)
            .any(|ident| ident.value == name)
    }) || options
        .opt_exclude
        .as_ref()
        .is_some_and(|exclude| match exclude {
            ExcludeSelectItem::Single(column) => is_name(column, name),
            ExcludeSelectItem::Multiple(columns) => {
                columns.iter().any(|column| is_name(column, name))
            }
        })
}

impl VisitorMut for Wildcards<'_> {
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
                scope.push(ScopedCte {
                    cte: cte.clone(),
                    identity: cte.query.as_ref() as *const Query as usize,
                    with_owner: original,
                    recursive: with.recursive,
                });
            }
        }
        self.scopes.push(scope);
        // DataFusion binds the work table from the static term, with the
        // declared aliases, before planning the recursive term. Give probes
        // in the recursive term the same shape without planning that term
        // recursively while it is still being rewritten.
        if let SetExpr::SetOperation {
            op: SetOperator::Union,
            left,
            ..
        } = query.body.as_mut()
        {
            let scope = self.scopes.last_mut().unwrap();
            if let Some(index) = scope
                .iter()
                .position(|cte| cte.recursive && cte.identity == original)
            {
                let mut work_table = scope.remove(index);
                left.visit(self)?;
                let mut shape = match probe_query(&self.state) {
                    Ok(query) => query,
                    Err(error) => return ControlFlow::Break(Box::new(error)),
                };
                let mut anchor = shape.clone();
                anchor.body = left.clone();
                // Wrap a static term that is itself a UNION so DataFusion
                // treats the probe definition as a nonrecursive query.
                shape.body = Box::new(SetExpr::Query(anchor));
                work_table.cte.query = shape;
                self.scopes.last_mut().unwrap().insert(index, work_table);
            }
        }
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
