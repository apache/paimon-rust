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

use datafusion::common::config::CatalogOptions;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use paimon::catalog::Identifier;

pub(crate) fn extract_string_literal(
    function_name: &str,
    expr: &Expr,
    name: &str,
) -> DFResult<String> {
    match expr {
        Expr::Literal(scalar, _) => {
            let s = scalar.try_as_str().flatten().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "{function_name}: {name} must be a string literal, got: {expr}"
                ))
            })?;
            Ok(s.to_string())
        }
        _ => Err(DataFusionError::Plan(format!(
            "{function_name}: {name} must be a literal, got: {expr}"
        ))),
    }
}

pub(crate) fn extract_int_literal(function_name: &str, expr: &Expr, name: &str) -> DFResult<i64> {
    match expr {
        Expr::Literal(scalar, _) => match scalar {
            ScalarValue::Int8(Some(v)) => Ok(*v as i64),
            ScalarValue::Int16(Some(v)) => Ok(*v as i64),
            ScalarValue::Int32(Some(v)) => Ok(*v as i64),
            ScalarValue::Int64(Some(v)) => Ok(*v),
            ScalarValue::UInt8(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt16(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt32(Some(v)) => Ok(*v as i64),
            ScalarValue::UInt64(Some(v)) => i64::try_from(*v).map_err(|_| {
                DataFusionError::Plan(format!(
                    "{function_name}: {name} value {v} exceeds i64 range"
                ))
            }),
            _ => Err(DataFusionError::Plan(format!(
                "{function_name}: {name} must be an integer literal, got: {expr}"
            ))),
        },
        _ => Err(DataFusionError::Plan(format!(
            "{function_name}: {name} must be a literal, got: {expr}"
        ))),
    }
}

pub(crate) fn parse_table_identifier(
    function_name: &str,
    name: &str,
    default_database: &str,
) -> DFResult<Identifier> {
    let (_, database, table) = split_table_name(function_name, name)?;
    Ok(Identifier::new(database.unwrap_or(default_database), table))
}

/// Resolves a table-name argument the way a `FROM` clause resolves a table
/// name: `table` and `database.table` take the session's current catalog (and
/// database), and `catalog.database.table` names the catalog. Returns the
/// catalog's name and the table's identifier in that catalog.
pub(crate) fn resolve_table_argument(
    function_name: &str,
    name: &str,
    session: &CatalogOptions,
) -> DFResult<(String, Identifier)> {
    let (catalog, database, table) = split_table_name(function_name, name)?;
    Ok((
        catalog.unwrap_or(&session.default_catalog).to_string(),
        Identifier::new(database.unwrap_or(&session.default_schema), table),
    ))
}

/// Splits `table`, `database.table` or `catalog.database.table`.
fn split_table_name<'a>(
    function_name: &str,
    name: &'a str,
) -> DFResult<(Option<&'a str>, Option<&'a str>, &'a str)> {
    match name.split('.').collect::<Vec<_>>().as_slice() {
        [table] => Ok((None, None, *table)),
        [database, table] => Ok((None, Some(*database), *table)),
        [catalog, database, table] => Ok((Some(*catalog), Some(*database), *table)),
        _ => Err(DataFusionError::Plan(format!(
            "{function_name}: invalid table name '{name}', expected 'table', 'database.table', or 'catalog.database.table'"
        ))),
    }
}
