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

//! Custom [`RelationPlanner`] for Paimon time travel via `VERSION AS OF` and `TIMESTAMP AS OF`.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::catalog::default_table_source::{provider_as_source, source_as_provider};
use datafusion::common::TableReference;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::builder::LogicalPlanBuilder;
use datafusion::logical_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion::sql::sqlparser::ast::{self, TableFactor, TableVersion};
use paimon::spec::{SCAN_TIMESTAMP_MILLIS_OPTION, SCAN_VERSION_OPTION};

use crate::table::PaimonTableProvider;

/// A [`RelationPlanner`] that intercepts `VERSION AS OF` and `TIMESTAMP AS OF`
/// clauses on Paimon tables and resolves them to time travel options.
///
/// - `VERSION AS OF <integer or string>` → sets `scan.version` option on the table.
///   At scan time, the version is resolved: tag name (if exists) → snapshot id → error.
/// - `TIMESTAMP AS OF <timestamp string>` → parsed as a timestamp, sets `scan.timestamp-millis`.
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
        // Only handle Table factors with a version clause.
        let TableFactor::Table {
            ref name,
            ref version,
            ..
        } = relation
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };

        let extra_options = match version {
            Some(TableVersion::VersionAsOf(expr)) => resolve_version_as_of(expr)?,
            Some(TableVersion::TimestampAsOf(expr)) => resolve_timestamp_as_of(expr)?,
            _ => return Ok(RelationPlanning::Original(Box::new(relation))),
        };

        // Resolve the table reference.
        let table_ref = object_name_to_table_reference(name, context)?;
        let source = context
            .context_provider()
            .get_table_source(table_ref.clone())?;
        let provider = source_as_provider(&source)?;

        // Check if this is a Paimon table.
        let Some(paimon_provider) = provider.as_any().downcast_ref::<PaimonTableProvider>() else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };

        let new_table = paimon_provider.table().copy_with_options(extra_options);
        let new_provider = PaimonTableProvider::try_new(new_table)?;
        let new_source = provider_as_source(Arc::new(new_provider));

        // Destructure to get alias.
        let TableFactor::Table { alias, .. } = relation else {
            unreachable!()
        };

        let plan = LogicalPlanBuilder::scan(table_ref, new_source, None)?.build()?;
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan, alias,
        ))))
    }
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
/// Resolution (tag vs snapshot id) happens at scan time in `TableScan`.
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
                "Unsupported VERSION AS OF expression: {expr}. Expected an integer snapshot id or a tag name."
            )))
        }
    };
    Ok(HashMap::from([(SCAN_VERSION_OPTION.to_string(), version)]))
}

/// Resolve `TIMESTAMP AS OF <expr>` into `scan.timestamp-millis` option.
fn resolve_timestamp_as_of(expr: &ast::Expr) -> DFResult<HashMap<String, String>> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                let millis = parse_timestamp_to_millis(s)?;
                Ok(HashMap::from([(
                    SCAN_TIMESTAMP_MILLIS_OPTION.to_string(),
                    millis.to_string(),
                )]))
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
fn parse_timestamp_to_millis(ts: &str) -> DFResult<i64> {
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
