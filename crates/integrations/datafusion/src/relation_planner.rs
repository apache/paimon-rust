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

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::catalog::default_table_source::{provider_as_source, source_as_provider};
use datafusion::common::plan_datafusion_err;
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::builder::LogicalPlanBuilder;
use datafusion::logical_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion::sql::sqlparser::ast::{self, TableFactor, TableVersion};
use paimon::spec::{SCAN_TIMESTAMP_MILLIS_OPTION, SCAN_VERSION_OPTION};

use crate::catalog::{
    parse_engine_snapshot_id, ReadOnlyTableProvider, UnavailableEngineTableProvider,
};
use crate::table::{ObjectTableProvider, PaimonTableProvider};

/// Resolves Paimon time travel and registered engine snapshot IDs.
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
            let TableVersion::VersionAsOf(expr) = version else {
                return Err(routed.unsupported_time_travel());
            };
            let options = resolve_version_as_of(expr)?;
            let snapshot_id = parse_engine_snapshot_id(&options[SCAN_VERSION_OPTION])?;
            let routed = routed.clone();
            let new_provider = crate::runtime::block_on_with_runtime(
                async move { routed.at_snapshot(snapshot_id).await },
                "engine snapshot resolution thread panicked",
            )?;
            return plan_scan(relation, table_ref, new_provider);
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
        plan_scan(relation, table_ref, Arc::new(new_provider))
    }
}

fn plan_scan(
    relation: TableFactor,
    table_ref: TableReference,
    provider: Arc<dyn TableProvider>,
) -> DFResult<RelationPlanning> {
    let TableFactor::Table { alias, .. } = relation else {
        unreachable!()
    };
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
