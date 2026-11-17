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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::sqlparser::ast::{
    visit_expressions, visit_expressions_mut, Expr as SqlExpr, FunctionArg, FunctionArgExpr,
    FunctionArguments, Ident, ObjectName, Query, SetExpr, SetOperator, Statement, TableFactor,
    VisitMut, Visitor, VisitorMut,
};
use paimon::catalog::{Catalog, Function, Identifier};

const MAX_EXPANSION_DEPTH: usize = 32;
const MAX_FUNCTION_REFERENCES: usize = 1024;
const MAX_EXPANDED_CALLS: usize = 1024;
type FunctionReference = (String, Identifier);

/// Bind persistent catalog dependencies in a view's stored SQL to
/// the namespace used to create its logical plan. Keep CTEs, table functions,
/// and native function calls in their original scopes.
pub(crate) fn bind_view_query(
    state: &SessionState,
    sql: &str,
    current_catalog: &str,
    current_database: &str,
) -> DFResult<String> {
    let regex_sql = crate::regex_operators::parseable_sql(sql)?;
    let parseable = crate::lateral_views::parseable_sql(regex_sql.as_ref())?;
    let mut statements = crate::vector_type::parse_sql(parseable.as_ref())
        .map_err(|error| DataFusionError::Plan(format!("Invalid view SQL: {error}")))?;
    if statements.len() != 1 || !matches!(statements.first(), Some(Statement::Query(_))) {
        return Err(DataFusionError::Plan(
            "View SQL must contain one query".into(),
        ));
    }
    struct Binder<'a> {
        state: &'a SessionState,
        catalog: &'a str,
        database: &'a str,
        scopes: crate::catalog::ViewQueryScopes,
        table_functions: HashSet<usize>,
    }
    impl VisitorMut for Binder<'_> {
        type Break = ();

        fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
            self.scopes.enter(
                query,
                self.state
                    .config_options()
                    .sql_parser
                    .enable_ident_normalization,
            );
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &mut Query) -> ControlFlow<Self::Break> {
            self.scopes.leave();
            ControlFlow::Continue(())
        }

        fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<Self::Break> {
            if let TableFactor::TableFunction {
                expr: SqlExpr::Function(call),
                ..
            } = factor
            {
                self.table_functions.insert(call as *const _ as usize);
                return ControlFlow::Continue(());
            }
            let TableFactor::Table {
                name, args: None, ..
            } = factor
            else {
                return ControlFlow::Continue(());
            };
            let normalize = self
                .state
                .config_options()
                .sql_parser
                .enable_ident_normalization;
            if self.scopes.is_cte(name, normalize) {
                return ControlFlow::Continue(());
            }
            let parts = name
                .0
                .iter()
                .map(|part| {
                    part.as_ident()
                        .cloned()
                        .map(|identifier| IdentNormalizer::new(normalize).normalize(identifier))
                })
                .collect::<Option<Vec<_>>>();
            if let Some(parts) = parts {
                let names = match parts.as_slice() {
                    [table] => vec![self.catalog, self.database, table.as_str()],
                    [database, table] => vec![self.catalog, database.as_str(), table.as_str()],
                    _ => return ControlFlow::Continue(()),
                };
                *name = quoted_name(&names);
            }
            ControlFlow::Continue(())
        }

        fn post_visit_expr(&mut self, expr: &mut SqlExpr) -> ControlFlow<Self::Break> {
            if let SqlExpr::Function(function) = expr {
                if self
                    .table_functions
                    .remove(&(function as *const _ as usize))
                {
                    return ControlFlow::Continue(());
                }
                if let Some((catalog, identifier)) =
                    function_reference(function, self.catalog, self.database, self.state)
                {
                    function.name =
                        quoted_name(&[&catalog, identifier.database(), identifier.object()]);
                }
            }
            ControlFlow::Continue(())
        }
    }
    let mut binder = Binder {
        state,
        catalog: current_catalog,
        database: current_database,
        scopes: crate::catalog::ViewQueryScopes::default(),
        table_functions: HashSet::new(),
    };
    let _: ControlFlow<()> = statements[0].visit(&mut binder);
    Ok(statements.remove(0).to_string())
}

fn quoted_name(parts: &[&str]) -> ObjectName {
    ObjectName::from(
        parts
            .iter()
            .map(|part| Ident::with_quote('"', *part))
            .collect::<Vec<_>>(),
    )
}

/// A native provider's stored SQL may lack its creation namespace, even though
/// its logical plan still holds the original table providers. Only export SQL
/// whose physical table references are independent of the session defaults.
pub(crate) fn validate_bound_view_query(query: &Query, normalize: bool) -> DFResult<()> {
    struct Validator<'a> {
        visible_ctes: &'a HashSet<String>,
        normalize: bool,
        skip_root_query: bool,
        nested_queries: usize,
    }
    impl Visitor for Validator<'_> {
        type Break = DataFusionError;

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
            if self.skip_root_query {
                self.skip_root_query = false;
                return ControlFlow::Continue(());
            }
            if self.nested_queries == 0 {
                if let Err(error) = validate_query(query, self.visible_ctes, self.normalize) {
                    return ControlFlow::Break(error);
                }
            }
            self.nested_queries += 1;
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
            self.nested_queries = self.nested_queries.saturating_sub(1);
            ControlFlow::Continue(())
        }

        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
            // Nested queries have already been checked in their own CTE scope.
            if self.nested_queries != 0 {
                return ControlFlow::Continue(());
            }
            if let TableFactor::Table {
                name, args: None, ..
            } = factor
            {
                let is_cte = match name.0.as_slice() {
                    [part] => part.as_ident().is_some_and(|identifier| {
                        self.visible_ctes.contains(
                            &IdentNormalizer::new(self.normalize).normalize(identifier.clone()),
                        )
                    }),
                    _ => false,
                };
                if name.0.len() < 3 && !is_cte {
                    return ControlFlow::Break(DataFusionError::Plan(format!(
                        "Cannot export view with unqualified dependency '{name}': its creation \
                         namespace is unavailable; use fully qualified catalog.schema.table \
                         names in the view definition"
                    )));
                }
            }
            ControlFlow::Continue(())
        }
    }

    fn validate_query(
        query: &Query,
        inherited_ctes: &HashSet<String>,
        normalize: bool,
    ) -> DFResult<()> {
        let mut visible_ctes = inherited_ctes.clone();
        if let Some(with) = &query.with {
            // DataFusion plans CTEs sequentially even with WITH RECURSIVE.
            for cte in &with.cte_tables {
                let name = IdentNormalizer::new(normalize).normalize(cte.alias.name.clone());
                if let (
                    true,
                    SetExpr::SetOperation {
                        op: SetOperator::Union,
                        left,
                        right,
                        ..
                    },
                ) = (with.recursive, cte.query.body.as_ref())
                {
                    // recursive_cte plans these operands directly. The static
                    // left operand cannot see the work table; only the right can.
                    for (operand, recursive) in [(left, false), (right, true)] {
                        let mut branch_ctes = visible_ctes.clone();
                        if recursive {
                            branch_ctes.insert(name.clone());
                        }
                        let mut validator = Validator {
                            visible_ctes: &branch_ctes,
                            normalize,
                            skip_root_query: false,
                            nested_queries: 0,
                        };
                        if let ControlFlow::Break(error) =
                            datafusion::sql::sqlparser::ast::Visit::visit(operand, &mut validator)
                        {
                            return Err(error);
                        }
                    }
                } else {
                    validate_query(&cte.query, &visible_ctes, normalize)?;
                }
                visible_ctes.insert(name);
            }
        }

        // Check the rest of the query once its WITH definitions are processed.
        // Delegate nested queries to validate_query instead of inheriting all
        // names from their parent's WITH block indiscriminately.
        let mut body = query.clone();
        body.with = None;
        let mut validator = Validator {
            visible_ctes: &visible_ctes,
            normalize,
            skip_root_query: true,
            nested_queries: 0,
        };
        match datafusion::sql::sqlparser::ast::Visit::visit(&body, &mut validator) {
            ControlFlow::Break(error) => Err(error),
            ControlFlow::Continue(()) => Ok(()),
        }
    }

    validate_query(query, &HashSet::new(), normalize)
}

pub(crate) async fn expand_sql(
    state: &SessionState,
    sql: &str,
    catalogs: &HashMap<String, Arc<dyn Catalog>>,
    current_catalog: &str,
    current_database: &str,
) -> DFResult<String> {
    expand_sql_internal(
        state,
        sql,
        catalogs,
        current_catalog,
        current_database,
        None,
    )
    .await
}

async fn expand_sql_internal(
    state: &SessionState,
    sql: &str,
    catalogs: &HashMap<String, Arc<dyn Catalog>>,
    current_catalog: &str,
    current_database: &str,
    candidate: Option<&Function>,
) -> DFResult<String> {
    let regex_sql = crate::regex_operators::parseable_sql(sql)?;
    let parseable = crate::lateral_views::parseable_sql(regex_sql.as_ref())?;
    let mut statements = crate::vector_type::parse_sql(parseable.as_ref())
        .map_err(|error| DataFusionError::Plan(format!("Invalid REST SQL: {error}")))?;
    if statements.len() != 1 {
        return Err(DataFusionError::Plan(format!(
            "REST SQL must contain exactly one statement, found {}",
            statements.len()
        )));
    }
    let statement = expand_statement_internal(
        state,
        statements.remove(0),
        catalogs,
        current_catalog,
        current_database,
        MAX_EXPANDED_CALLS,
        candidate,
    )
    .await?;
    Ok(statement.to_string())
}

pub(crate) async fn expand_sql_with_candidate(
    state: &SessionState,
    sql: &str,
    catalogs: &HashMap<String, Arc<dyn Catalog>>,
    current_catalog: &str,
    current_database: &str,
    candidate: &Function,
) -> DFResult<String> {
    expand_sql_internal(
        state,
        sql,
        catalogs,
        current_catalog,
        current_database,
        Some(candidate),
    )
    .await
}

pub(crate) async fn expand_statement(
    state: &SessionState,
    statement: Statement,
    catalogs: &HashMap<String, Arc<dyn Catalog>>,
    current_catalog: &str,
    current_database: &str,
) -> DFResult<Statement> {
    expand_statement_with_budget(
        state,
        statement,
        catalogs,
        current_catalog,
        current_database,
        MAX_EXPANDED_CALLS,
    )
    .await
}

pub(crate) async fn expand_statement_with_budget(
    state: &SessionState,
    statement: Statement,
    catalogs: &HashMap<String, Arc<dyn Catalog>>,
    current_catalog: &str,
    current_database: &str,
    max_expanded_calls: usize,
) -> DFResult<Statement> {
    expand_statement_internal(
        state,
        statement,
        catalogs,
        current_catalog,
        current_database,
        max_expanded_calls,
        None,
    )
    .await
}

async fn expand_statement_internal(
    state: &SessionState,
    mut statement: Statement,
    catalogs: &HashMap<String, Arc<dyn Catalog>>,
    current_catalog: &str,
    current_database: &str,
    max_expanded_calls: usize,
    candidate: Option<&Function>,
) -> DFResult<Statement> {
    crate::lateral_views::rewrite(&mut statement)?;
    let candidate_reference =
        candidate.map(|function| (current_catalog.to_string(), function.identifier().clone()));
    let mut functions = HashMap::new();
    let mut dependencies = HashMap::new();
    let mut total_expanded_calls = 0;
    for _ in 0..MAX_EXPANSION_DEPTH {
        let mut references = BTreeMap::new();
        let _: ControlFlow<()> = visit_expressions(&statement, |expr| {
            if let SqlExpr::Function(function) = expr {
                if let Some(reference) =
                    function_reference(function, current_catalog, current_database, state)
                {
                    references.insert(reference, ());
                }
            }
            ControlFlow::Continue(())
        });

        let mut pending = references.into_keys().collect::<VecDeque<_>>();
        while let Some(reference) = pending.pop_front() {
            if functions.contains_key(&reference) {
                continue;
            }
            if functions.len() >= MAX_FUNCTION_REFERENCES {
                return Err(DataFusionError::Plan(format!(
                    "REST SQL function expansion exceeded the reference limit of {MAX_FUNCTION_REFERENCES}"
                )));
            }
            let Some(catalog) = catalogs.get(&reference.0) else {
                functions.insert(reference, None);
                continue;
            };
            let function = if candidate_reference.as_ref() == Some(&reference) {
                candidate.cloned()
            } else {
                match catalog.get_function(&reference.1).await {
                    Ok(function) => Some(function),
                    Err(paimon::Error::FunctionNotExist { .. })
                    | Err(paimon::Error::Unsupported { .. }) => None,
                    Err(error) => return Err(crate::to_datafusion_error(error)),
                }
            };
            if let Some(function) = &function {
                let nested =
                    function_dependencies(function, &reference.0, reference.1.database(), state);
                pending.extend(nested.iter().cloned());
                dependencies.insert(reference.clone(), nested);
            }
            functions.insert(reference, function);
        }

        if let Some(cycle) = find_dependency_cycle(&dependencies) {
            let path = cycle
                .iter()
                .map(|(catalog, identifier)| format!("{catalog}.{}", identifier.full_name()))
                .collect::<Vec<_>>()
                .join(" -> ");
            return Err(DataFusionError::Plan(format!(
                "recursive REST SQL function dependency detected: {path}"
            )));
        }

        let mut expanded_count = 0;
        let flow = visit_expressions_mut(&mut statement, |expr| {
            let SqlExpr::Function(call) = expr else {
                return ControlFlow::Continue(());
            };
            let Some(reference) =
                function_reference(call, current_catalog, current_database, state)
            else {
                return ControlFlow::Continue(());
            };
            let Some(Some(function)) = functions.get(&reference) else {
                return ControlFlow::Continue(());
            };
            if total_expanded_calls >= max_expanded_calls {
                return ControlFlow::Break(DataFusionError::Plan(format!(
                    "REST SQL function expansion budget of {max_expanded_calls} calls exceeded"
                )));
            }
            total_expanded_calls += 1;

            match expand_call(function, call, &reference.0, &functions, state) {
                Ok(expanded) => {
                    *expr = expanded;
                    expanded_count += 1;
                    ControlFlow::Continue(())
                }
                Err(error) => ControlFlow::Break(error),
            }
        });
        if let ControlFlow::Break(error) = flow {
            return Err(error);
        }
        if expanded_count == 0 {
            crate::expression_compat::rewrite(&mut statement, state)?;
            crate::binary_cast::rewrite(&mut statement)?;
            crate::regex_operators::rewrite(&mut statement);
            return Ok(statement);
        }
    }

    Err(DataFusionError::Plan(format!(
        "REST SQL function expansion exceeded maximum depth of {MAX_EXPANSION_DEPTH}"
    )))
}

fn function_dependencies(
    function: &Function,
    current_catalog: &str,
    current_database: &str,
    state: &SessionState,
) -> Vec<FunctionReference> {
    let Some(definition) = function
        .definition("datafusion")
        .and_then(paimon::catalog::FunctionDefinition::sql)
    else {
        return Vec::new();
    };
    let Ok(parseable) = crate::regex_operators::parseable_sql(definition) else {
        return Vec::new();
    };
    let Ok(body) = crate::vector_type::parse_expr(parseable.as_ref()) else {
        return Vec::new();
    };
    let mut dependencies = Vec::new();
    let _: ControlFlow<()> = visit_expressions(&body, |expr| {
        if let SqlExpr::Function(call) = expr {
            // A definition's self-reference is recursive even if it shares a
            // name with a local function. Preserve this validation before CREATE.
            if bare_function_name(call)
                .as_ref()
                .map(normalize_identifier)
                .as_deref()
                == Some(function.name())
            {
                dependencies.push((current_catalog.to_string(), function.identifier().clone()));
                return ControlFlow::Continue(());
            }
            if let Some(reference) =
                function_reference(call, current_catalog, current_database, state)
            {
                dependencies.push(reference);
            }
        }
        ControlFlow::Continue(())
    });
    dependencies
}

fn find_dependency_cycle(
    dependencies: &HashMap<FunctionReference, Vec<FunctionReference>>,
) -> Option<Vec<FunctionReference>> {
    fn visit(
        reference: &FunctionReference,
        dependencies: &HashMap<FunctionReference, Vec<FunctionReference>>,
        finished: &mut HashSet<FunctionReference>,
        path: &mut Vec<FunctionReference>,
    ) -> Option<Vec<FunctionReference>> {
        if let Some(start) = path.iter().position(|entry| entry == reference) {
            let mut cycle = path[start..].to_vec();
            cycle.push(reference.clone());
            return Some(cycle);
        }
        if finished.contains(reference) {
            return None;
        }

        path.push(reference.clone());
        if let Some(next_references) = dependencies.get(reference) {
            for next in next_references {
                if let Some(cycle) = visit(next, dependencies, finished, path) {
                    return Some(cycle);
                }
            }
        }
        path.pop();
        finished.insert(reference.clone());
        None
    }

    let mut finished = HashSet::new();
    for reference in dependencies.keys() {
        if let Some(cycle) = visit(reference, dependencies, &mut finished, &mut Vec::new()) {
            return Some(cycle);
        }
    }
    None
}

fn function_reference(
    function: &datafusion::sql::sqlparser::ast::Function,
    current_catalog: &str,
    current_database: &str,
    state: &SessionState,
) -> Option<(String, Identifier)> {
    let identifiers = function
        .name
        .0
        .iter()
        .map(|part| part.as_ident().map(normalize_identifier))
        .collect::<Option<Vec<_>>>()?;
    match identifiers.as_slice() {
        // Local registrations (including aliases) follow DataFusion resolution first.
        // An explicitly qualified call still requests the catalog function, even
        // when its name collides with a local scalar, aggregate or window function.
        [function] if is_registered_function(state, function) => None,
        [function] => Some((
            current_catalog.to_string(),
            Identifier::new(current_database, function),
        )),
        [catalog, database, function] => {
            Some((catalog.clone(), Identifier::new(database, function)))
        }
        _ => None,
    }
}

fn normalize_identifier(identifier: &Ident) -> String {
    IdentNormalizer::default().normalize(identifier.clone())
}

fn is_registered_function(state: &SessionState, name: &str) -> bool {
    crate::expression_compat::is_aggregate(name)
        || state.scalar_functions().contains_key(name)
        || state.higher_order_functions().contains_key(name)
        || state.aggregate_functions().contains_key(name)
        || state.window_functions().contains_key(name)
}

fn expand_call(
    function: &Function,
    call: &datafusion::sql::sqlparser::ast::Function,
    owner_catalog: &str,
    functions: &HashMap<FunctionReference, Option<Function>>,
    state: &SessionState,
) -> DFResult<SqlExpr> {
    if !function.is_deterministic() {
        return Err(DataFusionError::Plan(format!(
            "REST SQL function '{}' is non-deterministic and is not supported",
            function.full_name()
        )));
    }
    if function
        .return_params()
        .map(<[paimon::spec::DataField]>::len)
        != Some(1)
    {
        return Err(DataFusionError::Plan(format!(
            "REST SQL function '{}' must declare exactly one return parameter",
            function.full_name()
        )));
    }
    let input_params = function.input_params().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "REST SQL function '{}' has no input parameters",
            function.full_name()
        ))
    })?;
    let definition = function
        .definition("datafusion")
        .and_then(paimon::catalog::FunctionDefinition::sql)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "REST SQL function '{}' has no datafusion SQL definition",
                function.full_name()
            ))
        })?;

    let FunctionArguments::List(args) = &call.args else {
        return Err(DataFusionError::Plan(format!(
            "REST SQL function '{}' requires positional arguments",
            function.full_name()
        )));
    };
    let values = args
        .args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr.clone()),
            _ => Err(DataFusionError::Plan(format!(
                "REST SQL function '{}' requires positional expression arguments",
                function.full_name()
            ))),
        })
        .collect::<DFResult<Vec<_>>>()?;
    if values.len() != input_params.len() {
        return Err(DataFusionError::Plan(format!(
            "REST SQL function '{}' expects {} arguments but received {}",
            function.full_name(),
            input_params.len(),
            values.len()
        )));
    }

    let parseable = crate::regex_operators::parseable_sql(definition)?;
    let mut body = crate::vector_type::parse_expr(parseable.as_ref()).map_err(|error| {
        DataFusionError::Plan(format!("Invalid SQL function definition: {error}"))
    })?;
    let replacements: HashMap<String, SqlExpr> = input_params
        .iter()
        .zip(values)
        .map(|(field, value)| (field.name().to_string(), value))
        .collect();
    let validation = visit_expressions(&body, |expr| match expr {
        SqlExpr::Identifier(identifier)
            if !replacements.contains_key(&normalize_identifier(identifier)) =>
        {
            ControlFlow::Break(identifier.value.clone())
        }
        SqlExpr::CompoundIdentifier(identifiers) => ControlFlow::Break(
            identifiers
                .iter()
                .map(|identifier| identifier.value.as_str())
                .collect::<Vec<_>>()
                .join("."),
        ),
        _ => ControlFlow::Continue(()),
    });
    if let ControlFlow::Break(identifier) = validation {
        return Err(DataFusionError::Plan(format!(
            "REST SQL function '{}' references undeclared identifier '{identifier}'",
            function.full_name()
        )));
    }
    let _: ControlFlow<()> = visit_expressions_mut(&mut body, |expr| {
        let SqlExpr::Function(call) = expr else {
            return ControlFlow::Continue(());
        };
        let Some(function_name) = bare_function_name(call) else {
            return ControlFlow::Continue(());
        };
        let function_name = normalize_identifier(&function_name);
        if is_registered_function(state, &function_name) {
            return ControlFlow::Continue(());
        }
        let reference = (
            owner_catalog.to_string(),
            Identifier::new(function.identifier().database(), &function_name),
        );
        if matches!(functions.get(&reference), Some(Some(_))) {
            call.name = ObjectName::from(vec![
                Ident::with_quote('"', owner_catalog),
                Ident::with_quote('"', function.identifier().database()),
                Ident::with_quote('"', function_name),
            ]);
        }
        ControlFlow::Continue(())
    });
    let _: ControlFlow<()> = visit_expressions_mut(&mut body, |expr| {
        if let SqlExpr::Identifier(identifier) = expr {
            if let Some(replacement) = replacements.get(&normalize_identifier(identifier)) {
                *expr = replacement.clone();
            }
        }
        ControlFlow::Continue(())
    });

    let return_type = function.return_params().expect("validated above")[0].data_type();
    let sql_type = crate::table::data_type_to_sql(return_type).map_err(|error| {
        DataFusionError::Plan(format!(
            "Invalid return type for REST SQL function '{}': {error}",
            function.full_name()
        ))
    })?;
    crate::vector_type::parse_expr(&format!("CAST(({body}) AS {sql_type})")).map_err(|error| {
        DataFusionError::Plan(format!(
            "Invalid return type for REST SQL function '{}': {error}",
            function.full_name()
        ))
    })
}

fn bare_function_name(function: &datafusion::sql::sqlparser::ast::Function) -> Option<Ident> {
    match function.name.0.as_slice() {
        [part] => part.as_ident().cloned(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{bind_view_query, expand_sql};

    #[test]
    fn view_binding_preserves_sequential_and_recursive_cte_scopes() {
        let state = datafusion::prelude::SessionContext::new().state();
        let bound = bind_view_query(
            &state,
            "WITH t AS (SELECT id FROM t), u AS (SELECT id FROM t) SELECT id FROM u",
            "lake",
            "db",
        )
        .unwrap();
        assert_eq!(bound, "WITH t AS (SELECT id FROM \"lake\".\"db\".\"t\"), u AS (SELECT id FROM t) SELECT id FROM u");
        let recursive = "WITH RECURSIVE t AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM t WHERE id < 2) SELECT id FROM t";
        assert_eq!(
            bind_view_query(&state, recursive, "lake", "db").unwrap(),
            recursive
        );
    }

    #[test]
    fn view_binding_keeps_table_functions_and_qualifies_catalog_scalar_functions() {
        let state = datafusion::prelude::SessionContext::new().state();
        assert_eq!(
            bind_view_query(
                &state,
                "SELECT upper(v), custom_fn(v) FROM other.source",
                "lake",
                "db"
            )
            .unwrap(),
            "SELECT upper(v), \"lake\".\"db\".\"custom_fn\"(v) FROM \"lake\".\"other\".\"source\""
        );
        for sql in [
            "SELECT * FROM generate_series(1, 2)",
            "SELECT * FROM TABLE(generate_series(1, 2))",
        ] {
            assert_eq!(bind_view_query(&state, sql, "lake", "db").unwrap(), sql);
        }
    }

    #[test]
    fn view_binding_uses_session_identifier_normalization() {
        let mut state = datafusion::prelude::SessionContext::new().state();
        state
            .config_mut()
            .options_mut()
            .sql_parser
            .enable_ident_normalization = false;
        assert_eq!(
            bind_view_query(
                &state,
                "WITH \"Source\" AS (SELECT id FROM \"Source\") SELECT id FROM Source",
                "lake",
                "db"
            )
            .unwrap(),
            "WITH \"Source\" AS (SELECT id FROM \"lake\".\"db\".\"Source\") SELECT id FROM Source"
        );
    }

    #[test]
    fn vector_cast_preserves_function_dependency_detection() {
        use paimon::catalog::{Function, FunctionDefinition, Identifier};

        let identifier = Identifier::new("db", "cycle");
        let function = Function::new(
            identifier.clone(),
            None,
            None,
            true,
            HashMap::from([(
                "datafusion".to_string(),
                FunctionDefinition::Sql {
                    definition: "CAST(cycle(x) AS VECTOR<FLOAT, 2>)".to_string(),
                },
            )]),
            None,
            HashMap::new(),
        );
        let state = datafusion::prelude::SessionContext::new().state();
        assert_eq!(
            super::function_dependencies(&function, "paimon", "db", &state),
            vec![("paimon".to_string(), identifier)]
        );
    }

    #[tokio::test]
    async fn leaves_bare_functions_for_datafusion_without_a_paimon_catalog() {
        let sql = "SELECT vector_from_json('[1, 2.5]')";

        let state = datafusion::prelude::SessionContext::new().state();
        let expanded = expand_sql(&state, sql, &HashMap::new(), "datafusion", "public")
            .await
            .unwrap();

        assert_eq!(expanded, sql);
    }
}
