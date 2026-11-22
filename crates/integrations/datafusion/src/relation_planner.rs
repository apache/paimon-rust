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

//! Time-travel planning for Paimon tables and registered engines.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::catalog::default_table_source::{provider_as_source, source_as_provider};
use datafusion::common::config::CatalogOptions;
use datafusion::common::plan_datafusion_err;
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::builder::LogicalPlanBuilder;
use datafusion::logical_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion::logical_expr::{Expr, LogicalPlan, Statement};
use datafusion::sql::sqlparser::ast::{self, TableFactor, TableVersion, VisitMut, VisitorMut};
use paimon::spec::{SCAN_TIMESTAMP_MILLIS_OPTION, SCAN_VERSION_OPTION};

use crate::catalog::{
    parse_engine_snapshot_id, EngineTimeTravel, ReadOnlyTableProvider,
    UnavailableEngineTableProvider,
};
use crate::table::{ObjectTableProvider, PaimonTableProvider};

/// Resolves statement-local TABLE bindings to snapshot scans.
#[derive(Debug)]
pub(crate) struct SnapshotRelationPlanner {
    bindings: HashMap<String, (TableReference, Arc<dyn TableProvider>)>,
    display_qualifiers: HashMap<String, String>,
    cte_names: HashSet<String>,
}

impl SnapshotRelationPlanner {
    pub(crate) fn try_new(
        mut bindings: HashMap<String, (TableReference, Arc<dyn TableProvider>)>,
        statement: &mut ast::Statement,
        defaults: &CatalogOptions,
        normalize: bool,
    ) -> DFResult<Self> {
        let mut qualifiers = SnapshotQualifiers {
            bindings: &bindings,
            defaults,
            normalize,
            private_bindings: HashSet::new(),
            cte_names: HashSet::new(),
            physical_tables: physical_table_references(statement, normalize)?,
            scopes: Vec::new(),
            query_has_select: Vec::new(),
            select_sources: Vec::new(),
            ambiguous_qualifier: None,
        };
        let _ = statement.visit(&mut qualifiers);
        if let Some(qualifier) = qualifiers.ambiguous_qualifier {
            return Err(plan_datafusion_err!(
                "Ambiguous snapshot qualifier '{qualifier}'. Use distinct aliases for the snapshot scans"
            ));
        }
        let cte_names = qualifiers.cte_names;
        let mut display_qualifiers = HashMap::new();
        for name in qualifiers.private_bindings {
            let (table, _) = bindings.get_mut(&name).unwrap();
            display_qualifiers.insert(name.clone(), table.to_string());
            *table = TableReference::bare(name);
        }
        Ok(Self {
            bindings,
            display_qualifiers,
            cte_names,
        })
    }

    /// DataFusion includes qualifiers in default expression names. Restore
    /// their SQL spelling before a caller persists the schema or prepares it.
    pub(crate) fn restore_output_names(&self, plan: LogicalPlan) -> DFResult<LogicalPlan> {
        if self.display_qualifiers.is_empty() {
            return Ok(plan);
        }
        if let LogicalPlan::Statement(Statement::Prepare(mut prepare)) = plan {
            prepare.input =
                Arc::new(self.restore_output_names(Arc::unwrap_or_clone(prepare.input))?);
            return Ok(LogicalPlan::Statement(Statement::Prepare(prepare)));
        }
        let mut renamed = false;
        let projection = plan
            .schema()
            .columns()
            .into_iter()
            .map(|column| {
                let mut name = column.name.clone();
                for (binding, original) in &self.display_qualifiers {
                    if name.contains(binding) {
                        name = name.replace(binding, original);
                    }
                }
                if name == column.name {
                    Expr::Column(column)
                } else {
                    renamed = true;
                    let qualifier = column.relation.clone();
                    Expr::Column(column).alias_qualified(qualifier, name)
                }
            })
            .collect::<Vec<_>>();
        if renamed {
            LogicalPlanBuilder::from(plan).project(projection)?.build()
        } else {
            Ok(plan)
        }
    }
}

enum ScopedQualifier<'a> {
    Original,
    Snapshots(Vec<&'a str>),
}

fn physical_table_references(
    statement: &ast::Statement,
    normalize: bool,
) -> DFResult<HashSet<*const ast::ObjectName>> {
    struct TableVisitor {
        normalize: bool,
        query_depth: usize,
        names: HashSet<*const ast::ObjectName>,
    }

    impl ast::Visitor for TableVisitor {
        type Break = DataFusionError;

        fn pre_visit_query(&mut self, query: &ast::Query) -> ControlFlow<Self::Break> {
            self.query_depth += 1;
            if self.query_depth != 1 {
                return ControlFlow::Continue(());
            }
            match crate::sql_function::visit_view_table_dependencies(
                query,
                self.normalize,
                &mut |name| {
                    self.names.insert(std::ptr::from_ref(name));
                    Ok(())
                },
            ) {
                Ok(()) => ControlFlow::Continue(()),
                Err(error) => ControlFlow::Break(error),
            }
        }

        fn post_visit_query(&mut self, _: &ast::Query) -> ControlFlow<Self::Break> {
            self.query_depth -= 1;
            ControlFlow::Continue(())
        }
    }

    let mut visitor = TableVisitor {
        normalize,
        query_depth: 0,
        names: HashSet::new(),
    };
    match ast::Visit::visit(statement, &mut visitor) {
        ControlFlow::Continue(()) => Ok(visitor.names),
        ControlFlow::Break(error) => Err(error),
    }
}

/// DataFusion matches a bare qualifier against the last component of a full
/// table name. Bind colliding snapshots privately and resolve SQL qualifiers
/// within their original scope before DataFusion plans the join.
struct SnapshotQualifiers<'a> {
    bindings: &'a HashMap<String, (TableReference, Arc<dyn TableProvider>)>,
    defaults: &'a CatalogOptions,
    normalize: bool,
    private_bindings: HashSet<String>,
    cte_names: HashSet<String>,
    // Node addresses distinguish tables from same-named CTEs. Moving a FROM
    // vector for traversal leaves its elements at their original addresses.
    physical_tables: HashSet<*const ast::ObjectName>,
    scopes: Vec<HashMap<TableReference, ScopedQualifier<'a>>>,
    query_has_select: Vec<bool>,
    select_sources: Vec<Vec<ast::TableWithJoins>>,
    ambiguous_qualifier: Option<TableReference>,
}

impl<'binding> SnapshotQualifiers<'binding> {
    fn factors(group: &ast::TableWithJoins) -> impl DoubleEndedIterator<Item = &TableFactor> {
        std::iter::once(&group.relation).chain(group.joins.iter().map(|join| &join.relation))
    }

    fn visible_factors<'a>(
        relation: &'a TableFactor,
        joins: &'a [ast::Join],
    ) -> impl DoubleEndedIterator<Item = &'a TableFactor> {
        use ast::JoinOperator::{LeftAnti, LeftSemi, RightAnti, RightSemi};

        let (relation, joins) = match joins
            .iter()
            .rposition(|join| matches!(join.join_operator, RightSemi(_) | RightAnti(_)))
        {
            Some(index) => (&joins[index].relation, &joins[index + 1..]),
            None => (relation, joins),
        };
        std::iter::once(relation).chain(joins.iter().filter_map(|join| {
            (!matches!(join.join_operator, LeftSemi(_) | LeftAnti(_))).then_some(&join.relation)
        }))
    }

    fn table_qualifiers(&self, table: &TableReference) -> [TableReference; 3] {
        let resolved = table.clone().resolve(
            &self.defaults.default_catalog,
            &self.defaults.default_schema,
        );
        [
            TableReference::bare(Arc::clone(&resolved.table)),
            TableReference::partial(Arc::clone(&resolved.schema), Arc::clone(&resolved.table)),
            TableReference::full(resolved.catalog, resolved.schema, resolved.table),
        ]
    }

    fn relation_scope<'relation>(
        &mut self,
        factors: impl DoubleEndedIterator<Item = &'relation TableFactor>,
        visible_only: bool,
    ) -> HashMap<TableReference, ScopedQualifier<'binding>> {
        let mut sources = Vec::new();
        let mut snapshots = Vec::new();
        let mut scope = HashMap::new();
        let mut pending: Vec<_> = factors.rev().collect();
        let normalizer = datafusion::sql::planner::IdentNormalizer::new(self.normalize);
        while let Some(factor) = pending.pop() {
            let alias = match factor {
                TableFactor::TableFunction {
                    expr: ast::Expr::Identifier(binding),
                    alias: None,
                } => {
                    if let Some((binding, (table, _))) = self.bindings.get_key_value(&binding.value)
                    {
                        snapshots.push((table.clone(), Some(binding.as_str())));
                    }
                    continue;
                }
                TableFactor::Table {
                    name,
                    args: None,
                    alias: None,
                    ..
                } => {
                    if let Ok(table) = datafusion::sql::planner::object_name_to_table_reference(
                        name.clone(),
                        self.normalize,
                    ) {
                        if self.physical_tables.contains(&std::ptr::from_ref(name)) {
                            scope.extend(
                                self.table_qualifiers(&table)
                                    .into_iter()
                                    .map(|qualifier| (qualifier, ScopedQualifier::Original)),
                            );
                        }
                        sources.push((table, None));
                    }
                    continue;
                }
                TableFactor::NestedJoin {
                    table_with_joins,
                    alias: None,
                } => {
                    if visible_only {
                        pending.extend(
                            Self::visible_factors(
                                &table_with_joins.relation,
                                &table_with_joins.joins,
                            )
                            .rev(),
                        );
                    } else {
                        pending.extend(Self::factors(table_with_joins).rev());
                    }
                    continue;
                }
                TableFactor::NestedJoin { alias, .. }
                | TableFactor::Table { alias, .. }
                | TableFactor::Derived { alias, .. }
                | TableFactor::Function { alias, .. }
                | TableFactor::UNNEST { alias, .. }
                | TableFactor::TableFunction { alias, .. } => alias,
                _ => continue,
            };
            if let Some(alias) = alias {
                sources.push((
                    TableReference::bare(normalizer.normalize(alias.name.clone())),
                    None,
                ));
            }
        }

        // Live relations reserve their qualifiers before any implicit snapshot names.
        sources.extend(snapshots);
        let mut reserved = HashMap::<&str, Vec<&TableReference>>::new();
        for (table, binding) in &sources {
            let names = reserved.entry(table.table()).or_default();
            if let Some(binding) = *binding {
                let qualifiers = self.table_qualifiers(table);
                // Private snapshot bindings protect outer references during decorrelation.
                let overlaps_existing = qualifiers.iter().any(|qualifier| {
                    matches!(scope.get(qualifier), Some(ScopedQualifier::Original))
                        || self
                            .scopes
                            .iter()
                            .any(|scope| scope.contains_key(qualifier))
                });
                if overlaps_existing || names.iter().any(|name| name.resolved_eq(table)) {
                    self.private_bindings.insert(binding.to_owned());
                }
                for qualifier in qualifiers {
                    if let ScopedQualifier::Snapshots(bindings) = scope
                        .entry(qualifier)
                        .or_insert_with(|| ScopedQualifier::Snapshots(Vec::new()))
                    {
                        bindings.push(binding);
                    }
                }
            } else {
                scope.insert(table.clone(), ScopedQualifier::Original);
            }
            names.push(table);
        }
        scope
    }

    fn visit_joins(&mut self, group: &mut ast::TableWithJoins, preceding: &[&TableFactor]) {
        self.visit_factor(&mut group.relation, preceding.iter().copied());
        for index in 0..group.joins.len() {
            let (previous, rest) = group.joins.split_at_mut(index);
            let join = &mut rest[0];
            let left = || Self::visible_factors(&group.relation, previous);
            self.visit_factor(&mut join.relation, preceding.iter().copied().chain(left()));
            // The ON clause can see its join inputs and enclosing queries.
            let join_scope =
                self.relation_scope(left().chain(std::iter::once(&join.relation)), true);
            self.scopes.push(join_scope);
            let _ = join.join_operator.visit(self);
            self.scopes.pop();
        }
    }

    fn visit_factor<'relation>(
        &mut self,
        factor: &mut TableFactor,
        preceding: impl DoubleEndedIterator<Item = &'relation TableFactor>,
    ) {
        match factor {
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.relation_scope(Self::factors(table_with_joins), false);
                self.visit_joins(table_with_joins, &preceding.collect::<Vec<_>>());
            }
            TableFactor::Derived { lateral: true, .. }
            | TableFactor::Function { lateral: true, .. }
            | TableFactor::UNNEST { .. } => {
                let scope = self.relation_scope(preceding, true);
                self.scopes.push(scope);
                let _ = factor.visit(self);
                self.scopes.pop();
            }
            _ => {
                let _ = factor.visit(self);
            }
        }
    }

    fn rewrite_prefix(
        &mut self,
        idents: &[ast::Ident],
        has_column: bool,
    ) -> Option<(usize, ast::Ident)> {
        let prefix_len = if has_column {
            idents.len().saturating_sub(1)
        } else {
            idents.len()
        };
        let normalizer = datafusion::sql::planner::IdentNormalizer::new(self.normalize);
        let candidates: Vec<_> = (1..=prefix_len.min(3))
            .rev()
            .filter_map(|len| {
                let name = ast::ObjectName::from(idents[..len].to_vec());
                let table =
                    datafusion::sql::planner::object_name_to_table_reference(name, self.normalize)
                        .ok()?;
                let column = has_column.then(|| normalizer.normalize(idents[len].clone()));
                Some((len, table, column))
            })
            .collect();
        for scope in self.scopes.iter().rev() {
            let mut matched_snapshot = false;
            for (len, table, column) in &candidates {
                match scope.get(table) {
                    Some(ScopedQualifier::Original) => return None,
                    Some(ScopedQualifier::Snapshots(bindings)) => {
                        matched_snapshot = true;
                        if column.as_ref().is_some_and(|column| {
                            !bindings.iter().any(|binding| {
                                self.bindings[*binding]
                                    .1
                                    .schema()
                                    .column_with_name(column)
                                    .is_some()
                            })
                        }) {
                            continue;
                        }
                        let [binding] = bindings.as_slice() else {
                            self.ambiguous_qualifier
                                .get_or_insert_with(|| table.clone());
                            return None;
                        };
                        return self
                            .private_bindings
                            .contains(*binding)
                            .then(|| (*len, ast::Ident::new(*binding)));
                    }
                    None => {}
                }
            }
            if matched_snapshot {
                // Let DataFusion resolve an unqualified nested field in this scope.
                return None;
            }
        }
        None
    }

    fn rewrite_wildcard(&mut self, name: &mut ast::ObjectName) {
        let idents: Vec<_> = name
            .0
            .iter()
            .map_while(|part| part.as_ident().cloned())
            .collect();
        if let Some((len, binding)) = self.rewrite_prefix(&idents, false) {
            name.0
                .splice(..len, [ast::ObjectNamePart::Identifier(binding)]);
        }
    }
}

impl VisitorMut for SnapshotQualifiers<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut ast::Query) -> ControlFlow<()> {
        if let Some(with) = &query.with {
            let normalizer = datafusion::sql::planner::IdentNormalizer::new(self.normalize);
            self.cte_names.extend(
                with.cte_tables
                    .iter()
                    .map(|cte| normalizer.normalize(cte.alias.name.clone())),
            );
        }
        self.query_has_select.push(query.body.as_select().is_some());
        self.scopes.push(HashMap::new());
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &mut ast::Query) -> ControlFlow<()> {
        self.scopes.pop();
        self.query_has_select.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &mut ast::Select) -> ControlFlow<()> {
        let mut sources = std::mem::take(&mut select.from);
        // Reserve private bindings before visiting inputs in SQL visibility order.
        self.relation_scope(sources.iter().flat_map(Self::factors), false);
        for index in 0..sources.len() {
            let (previous, rest) = sources.split_at_mut(index);
            let preceding = previous
                .iter()
                .flat_map(|group| Self::visible_factors(&group.relation, &group.joins))
                .collect::<Vec<_>>();
            self.visit_joins(&mut rest[0], &preceding);
        }
        let scope = self.relation_scope(
            sources
                .iter()
                .flat_map(|group| Self::visible_factors(&group.relation, &group.joins)),
            true,
        );
        self.scopes.push(scope);
        self.select_sources.push(sources);
        for item in &mut select.projection {
            if let ast::SelectItem::QualifiedWildcard(
                ast::SelectItemQualifiedWildcardKind::ObjectName(name),
                _,
            ) = item
            {
                self.rewrite_wildcard(name);
            }
        }
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, select: &mut ast::Select) -> ControlFlow<()> {
        select.from = self.select_sources.pop().unwrap();
        let scope = self.scopes.pop().unwrap();
        if self.query_has_select.last() == Some(&true) {
            // ORDER BY belongs to Query and is visited after its SELECT.
            *self.scopes.last_mut().unwrap() = scope;
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<()> {
        match expr {
            ast::Expr::CompoundIdentifier(idents) => {
                if let Some((len, binding)) = self.rewrite_prefix(idents, true) {
                    idents.splice(..len, [binding]);
                }
            }
            ast::Expr::QualifiedWildcard(name, _) => self.rewrite_wildcard(name),
            ast::Expr::Function(function) => {
                if let ast::FunctionArguments::List(args) = &mut function.args {
                    for arg in &mut args.args {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::QualifiedWildcard(
                            name,
                        )) = arg
                        {
                            self.rewrite_wildcard(name);
                        }
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl RelationPlanner for SnapshotRelationPlanner {
    fn plan_relation(
        &self,
        mut relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> DFResult<RelationPlanning> {
        if let TableFactor::TableFunction {
            expr: ast::Expr::Identifier(binding),
            alias,
        } = &relation
        {
            if let Some((table, provider)) = self.bindings.get(&binding.value) {
                return plan_scan(alias.clone(), table.clone(), provider.clone());
            }
        }
        if let Some(marker) = self.display_qualifiers.keys().next() {
            let alias = match &mut relation {
                TableFactor::Derived { alias, .. } => Some(alias),
                TableFactor::Table {
                    name,
                    alias,
                    args: None,
                    ..
                } if context
                    .object_name_to_table_reference(name.clone())
                    .is_ok_and(|table| self.cte_names.contains(&table.to_string())) =>
                {
                    Some(alias)
                }
                _ => None,
            };
            if let Some(alias) = alias {
                // context.plan reenters the relation planners. The statement-local
                // marker delegates that call before we restore the inner field names.
                if alias.as_ref().is_some_and(|a| a.name.value == *marker) {
                    *alias = None;
                } else {
                    let original = alias.replace(ast::TableAlias {
                        name: ast::Ident::new(marker),
                        explicit: true,
                        columns: Vec::new(),
                        at: None,
                    });
                    let plan = self.restore_output_names(context.plan(relation)?)?;
                    return Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
                        plan, original,
                    ))));
                }
            }
        }
        Ok(RelationPlanning::Original(Box::new(relation)))
    }
}

/// Resolves Paimon time travel, and registered engine snapshot IDs and timestamps.
#[derive(Debug)]
pub struct PaimonRelationPlanner;

impl PaimonRelationPlanner {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PaimonRelationPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl RelationPlanner for PaimonRelationPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> DFResult<RelationPlanning> {
        let TableFactor::Table {
            ref name,
            ref version,
            ref alias,
            ..
        } = relation
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };

        let Some(version) = version else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };

        let table_ref = object_name_to_table_reference(name, context)?;
        let source = context
            .context_provider()
            .get_table_source(table_ref.clone())?;
        let provider = source_as_provider(&source)?;

        if let Some(unavailable) = provider.downcast_ref::<UnavailableEngineTableProvider>() {
            return Err(unavailable.unavailable_error());
        }

        if let Some(routed) = provider.downcast_ref::<ReadOnlyTableProvider>() {
            let travel = match version {
                TableVersion::VersionAsOf(expr) => {
                    let options = resolve_version_as_of(expr)?;
                    EngineTimeTravel::Snapshot(parse_engine_snapshot_id(
                        &options[SCAN_VERSION_OPTION],
                    )?)
                }
                TableVersion::TimestampAsOf(expr) => {
                    EngineTimeTravel::TimestampMillis(timestamp_as_of_millis(expr)?)
                }
                _ => return Err(routed.unsupported_time_travel()),
            };
            let routed = routed.clone();
            let new_provider = crate::runtime::block_on_with_runtime(
                async move { routed.at_time_travel(travel).await },
                "engine time travel resolution thread panicked",
            )?;
            return plan_scan(alias.clone(), table_ref, new_provider);
        }

        let Some(paimon_provider) = provider.downcast_ref::<PaimonTableProvider>() else {
            // Reject unsupported Paimon providers before the default planner drops the clause.
            // Leave foreign providers to their own planner.
            if provider.is::<ObjectTableProvider>()
                || crate::system_tables::is_system_table_provider(provider.as_ref())
            {
                return Err(plan_datafusion_err!(
                    "time travel is not supported for '{table_ref}'"
                ));
            }
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };

        let extra_options = match version {
            TableVersion::VersionAsOf(expr) => resolve_version_as_of(expr)?,
            TableVersion::TimestampAsOf(expr) | TableVersion::ForSystemTimeAsOf(expr) => {
                resolve_timestamp_as_of(expr)?
            }
            // Never let unsupported version clauses fall through to current data.
            _ => {
                return Err(plan_datafusion_err!(
                    "this time-travel syntax is not supported for Paimon tables; \
                     use VERSION AS OF, TIMESTAMP AS OF or FOR SYSTEM_TIME AS OF"
                ))
            }
        };

        // Snapshot schema resolution needs async I/O inside this synchronous hook.
        let table = paimon_provider.table().clone();
        let new_table = crate::runtime::block_on_with_runtime(
            async move { table.copy_with_time_travel(extra_options).await },
            "paimon time travel resolution thread panicked",
        )
        .map_err(crate::to_datafusion_error)?;
        let new_provider = PaimonTableProvider::try_new(new_table)?;
        plan_scan(alias.clone(), table_ref, Arc::new(new_provider))
    }
}

fn plan_scan(
    alias: Option<ast::TableAlias>,
    table_ref: TableReference,
    provider: Arc<dyn TableProvider>,
) -> DFResult<RelationPlanning> {
    let plan = LogicalPlanBuilder::scan(table_ref, provider_as_source(provider), None)?.build()?;
    Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
        plan, alias,
    ))))
}

/// Convert a sqlparser `ObjectName` to a DataFusion `TableReference`.
fn object_name_to_table_reference(
    name: &ast::ObjectName,
    context: &mut dyn RelationPlannerContext,
) -> DFResult<TableReference> {
    let idents: Vec<String> = name
        .0
        .iter()
        .map(|part| {
            let ident = part.as_ident().ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Expected simple identifier in table reference, got: {part}"
                ))
            })?;
            Ok(context.normalize_ident(ident.clone()))
        })
        .collect::<DFResult<_>>()?;
    match idents.len() {
        1 => Ok(TableReference::bare(idents[0].clone())),
        2 => Ok(TableReference::partial(
            idents[0].clone(),
            idents[1].clone(),
        )),
        3 => Ok(TableReference::full(
            idents[0].clone(),
            idents[1].clone(),
            idents[2].clone(),
        )),
        _ => Err(datafusion::error::DataFusionError::Plan(format!(
            "Unsupported table reference: {name}"
        ))),
    }
}

/// Resolve `VERSION AS OF <expr>` into `scan.version` option.
///
/// The raw value (integer or string) is passed through as-is.
/// Resolution (tag vs watermark vs snapshot id) happens at scan time in `TableScan`.
fn resolve_version_as_of(expr: &ast::Expr) -> DFResult<HashMap<String, String>> {
    let version = match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => n.clone(),
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => s.clone(),
            _ => {
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "Unsupported VERSION AS OF expression: {expr}"
                )))
            }
        },
        _ => {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "Unsupported VERSION AS OF expression: {expr}. Expected an integer snapshot id, a tag name, or a quoted 'watermark-<value>'."
            )))
        }
    };
    Ok(HashMap::from([(SCAN_VERSION_OPTION.to_string(), version)]))
}

/// Resolve `TIMESTAMP AS OF <expr>` into `scan.timestamp-millis` option.
fn resolve_timestamp_as_of(expr: &ast::Expr) -> DFResult<HashMap<String, String>> {
    let millis = timestamp_as_of_millis(expr)?;
    Ok(HashMap::from([(
        SCAN_TIMESTAMP_MILLIS_OPTION.to_string(),
        millis.to_string(),
    )]))
}

/// Parse the timestamp string of `TIMESTAMP AS OF <expr>` to epoch millis.
fn timestamp_as_of_millis(expr: &ast::Expr) -> DFResult<i64> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                parse_timestamp_to_millis(s)
            }
            _ => Err(datafusion::error::DataFusionError::Plan(format!(
                "Unsupported TIMESTAMP AS OF expression: {expr}. Expected a timestamp string."
            ))),
        },
        _ => Err(datafusion::error::DataFusionError::Plan(format!(
            "Unsupported TIMESTAMP AS OF expression: {expr}. Expected a timestamp string."
        ))),
    }
}

/// Parse a timestamp string to milliseconds since epoch (using local timezone).
///
/// Matches Java Paimon's behavior which uses `TimeZone.getDefault()`.
pub(crate) fn parse_timestamp_to_millis(ts: &str) -> DFResult<i64> {
    use chrono::{Local, NaiveDateTime, TimeZone};

    let naive = NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S").map_err(|e| {
        datafusion::error::DataFusionError::Plan(format!(
            "Cannot parse time travel timestamp '{ts}': {e}. Expected format: YYYY-MM-DD HH:MM:SS"
        ))
    })?;
    let local = Local.from_local_datetime(&naive).single().ok_or_else(|| {
        datafusion::error::DataFusionError::Plan(format!("Ambiguous or invalid local time: '{ts}'"))
    })?;
    Ok(local.timestamp_millis())
}
