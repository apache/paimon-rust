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
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::sqlparser::ast::{
    visit_expressions, visit_expressions_mut, Expr as SqlExpr, FunctionArg, FunctionArgExpr,
    FunctionArguments, Ident, ObjectName, Statement,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use paimon::catalog::{Catalog, Function, Identifier};

const MAX_EXPANSION_DEPTH: usize = 32;
const MAX_FUNCTION_REFERENCES: usize = 1024;
const MAX_EXPANDED_CALLS: usize = 1024;
type FunctionReference = (String, Identifier);

pub(crate) async fn expand_sql(
    sql: &str,
    catalogs: &HashMap<String, Arc<dyn Catalog>>,
    current_catalog: &str,
    current_database: &str,
) -> DFResult<String> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|error| DataFusionError::Plan(format!("Invalid REST SQL: {error}")))?;
    if statements.len() != 1 {
        return Err(DataFusionError::Plan(format!(
            "REST SQL must contain exactly one statement, found {}",
            statements.len()
        )));
    }
    let statement = expand_statement(
        statements.remove(0),
        catalogs,
        current_catalog,
        current_database,
    )
    .await?;
    Ok(statement.to_string())
}

pub(crate) async fn expand_statement(
    statement: Statement,
    catalogs: &HashMap<String, Arc<dyn Catalog>>,
    current_catalog: &str,
    current_database: &str,
) -> DFResult<Statement> {
    expand_statement_with_budget(
        statement,
        catalogs,
        current_catalog,
        current_database,
        MAX_EXPANDED_CALLS,
    )
    .await
}

pub(crate) async fn expand_statement_with_budget(
    mut statement: Statement,
    catalogs: &HashMap<String, Arc<dyn Catalog>>,
    current_catalog: &str,
    current_database: &str,
    max_expanded_calls: usize,
) -> DFResult<Statement> {
    let mut functions = HashMap::new();
    let mut dependencies = HashMap::new();
    let mut total_expanded_calls = 0;
    for _ in 0..MAX_EXPANSION_DEPTH {
        let mut references = BTreeMap::new();
        let _: ControlFlow<()> = visit_expressions(&statement, |expr| {
            if let SqlExpr::Function(function) = expr {
                if let Some(reference) =
                    function_reference(function, current_catalog, current_database)
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
            let function = match catalog.get_function(&reference.1).await {
                Ok(function) => Some(function),
                Err(paimon::Error::FunctionNotExist { .. })
                | Err(paimon::Error::Unsupported { .. }) => None,
                Err(error) => return Err(crate::to_datafusion_error(error)),
            };
            if let Some(function) = &function {
                let nested = function_dependencies(function, &reference.0, reference.1.database());
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
            let Some(reference) = function_reference(call, current_catalog, current_database)
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

            match expand_call(function, call, &reference.0, &functions) {
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
) -> Vec<FunctionReference> {
    let Some(definition) = function
        .definition("datafusion")
        .and_then(paimon::catalog::FunctionDefinition::sql)
    else {
        return Vec::new();
    };
    let Ok(mut parser) = Parser::new(&GenericDialect {}).try_with_sql(definition) else {
        return Vec::new();
    };
    let Ok(body) = parser.parse_expr() else {
        return Vec::new();
    };
    let mut dependencies = Vec::new();
    let _: ControlFlow<()> = visit_expressions(&body, |expr| {
        if let SqlExpr::Function(call) = expr {
            if let Some(reference) = function_reference(call, current_catalog, current_database) {
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
) -> Option<(String, Identifier)> {
    let identifiers = function
        .name
        .0
        .iter()
        .map(|part| part.as_ident().map(normalize_identifier))
        .collect::<Option<Vec<_>>>()?;
    match identifiers.as_slice() {
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

fn expand_call(
    function: &Function,
    call: &datafusion::sql::sqlparser::ast::Function,
    owner_catalog: &str,
    functions: &HashMap<FunctionReference, Option<Function>>,
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

    let mut body = Parser::new(&GenericDialect {})
        .try_with_sql(definition)
        .map_err(|error| {
            DataFusionError::Plan(format!("Invalid SQL function definition: {error}"))
        })?
        .parse_expr()
        .map_err(|error| {
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
    let serialized_type = serde_json::to_value(return_type).map_err(|error| {
        DataFusionError::Plan(format!(
            "Invalid return type for REST SQL function '{}': {error}",
            function.full_name()
        ))
    })?;
    let sql_type = serialized_type.as_str().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "REST SQL function '{}' has a return type that cannot be represented in SQL",
            function.full_name()
        ))
    })?;
    let sql_type = sql_type.strip_suffix(" NOT NULL").unwrap_or(sql_type);
    Parser::new(&GenericDialect {})
        .try_with_sql(&format!("CAST(({body}) AS {sql_type})"))
        .map_err(|error| {
            DataFusionError::Plan(format!(
                "Invalid return type for REST SQL function '{}': {error}",
                function.full_name()
            ))
        })?
        .parse_expr()
        .map_err(|error| {
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

    use super::expand_sql;

    #[tokio::test]
    async fn leaves_bare_functions_for_datafusion_without_a_paimon_catalog() {
        let sql = "SELECT vector_from_json('[1, 2.5]')";

        let expanded = expand_sql(sql, &HashMap::new(), "datafusion", "public")
            .await
            .unwrap();

        assert_eq!(expanded, sql);
    }
}
