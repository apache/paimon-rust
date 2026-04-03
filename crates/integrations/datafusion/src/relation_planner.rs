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

//! Custom [`RelationPlanner`] for Paimon time travel via `FOR SYSTEM_TIME AS OF`.

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
use paimon::spec::{SCAN_SNAPSHOT_ID_OPTION, SCAN_TAG_NAME_OPTION, SCAN_TIMESTAMP_MILLIS_OPTION};

use crate::table::PaimonTableProvider;

/// A [`RelationPlanner`] that intercepts `FOR SYSTEM_TIME AS OF` clauses
/// on Paimon tables and resolves them to time travel options.
///
/// - Integer literal → sets `scan.snapshot-id` option on the table.
/// - String literal (timestamp) → parsed as a timestamp, sets `scan.timestamp-millis` option.
/// - String literal (other) → sets `scan.tag-name` option on the table.
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
            return Ok(RelationPlanning::Original(relation));
        };

        let version_expr = match version {
            Some(TableVersion::ForSystemTimeAsOf(expr)) => expr.clone(),
            _ => return Ok(RelationPlanning::Original(relation)),
        };

        // Resolve the table reference.
        let table_ref = object_name_to_table_reference(name, context)?;
        let source = context
            .context_provider()
            .get_table_source(table_ref.clone())?;
        let provider = source_as_provider(&source)?;

        // Check if this is a Paimon table.
        let Some(paimon_provider) = provider.as_any().downcast_ref::<PaimonTableProvider>() else {
            return Ok(RelationPlanning::Original(relation));
        };

        let extra_options = resolve_time_travel_options(&version_expr)?;
        let new_table = paimon_provider.table().copy_with_options(extra_options);
        let new_provider = PaimonTableProvider::try_new(new_table)?;
        let new_source = provider_as_source(Arc::new(new_provider));

        // Destructure to get alias.
        let TableFactor::Table { alias, .. } = relation else {
            unreachable!()
        };

        let plan = LogicalPlanBuilder::scan(table_ref, new_source, None)?.build()?;
        Ok(RelationPlanning::Planned(PlannedRelation::new(plan, alias)))
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

/// Resolve `FOR SYSTEM_TIME AS OF <expr>` into table options.
///
/// - Integer literal → `{"scan.snapshot-id": "N"}`
/// - String literal (timestamp `YYYY-MM-DD HH:MM:SS`) → `{"scan.timestamp-millis": "M"}`
/// - String literal (other) → `{"scan.tag-name": "S"}`
fn resolve_time_travel_options(expr: &ast::Expr) -> DFResult<HashMap<String, String>> {
    match expr {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::Number(n, _) => {
                // Validate it's a valid integer
                n.parse::<i64>().map_err(|e| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Invalid snapshot id '{n}': {e}"
                    ))
                })?;
                Ok(HashMap::from([(
                    SCAN_SNAPSHOT_ID_OPTION.to_string(),
                    n.clone(),
                )]))
            }
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                // Try parsing as timestamp first; fall back to tag name.
                match parse_timestamp_to_millis(s) {
                    Ok(timestamp_millis) => Ok(HashMap::from([(
                        SCAN_TIMESTAMP_MILLIS_OPTION.to_string(),
                        timestamp_millis.to_string(),
                    )])),
                    Err(_) => Ok(HashMap::from([(
                        SCAN_TAG_NAME_OPTION.to_string(),
                        s.clone(),
                    )])),
                }
            }
            _ => Err(datafusion::error::DataFusionError::Plan(format!(
                "Unsupported time travel expression: {expr}"
            ))),
        },
        _ => Err(datafusion::error::DataFusionError::Plan(format!(
            "Unsupported time travel expression: {expr}. Expected an integer snapshot id, a timestamp string, or a tag name."
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
