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

//! CALL procedure support for Paimon tables.
//!
//! Supported procedures:
//! - `CALL sys.create_tag(table => '...', tag => '...', snapshot_id => ..., time_retained => '...')`
//! - `CALL sys.replace_tag(table => '...', tag => '...', snapshot_id => ..., time_retained => '...')`
//! - `CALL sys.delete_tag(table => '...', tag => '...')`
//! - `CALL sys.rename_tag(table => '...', tag => '...', target_tag => '...')`
//! - `CALL sys.reset_consumer(table => '...', consumer_id => '...'[, next_snapshot_id => ...])`
//! - `CALL sys.clear_consumers(table => '...'[, including_consumers => '...', excluding_consumers => '...'])`
//! - `CALL sys.rollback_to(table => '...', snapshot_id => ... | tag => '...')`
//! - `CALL sys.rollback_to_timestamp(table => '...', timestamp => ...)`
//! - `CALL sys.create_tag_from_timestamp(table => '...', tag => '...', timestamp => ...)`
//! - `CALL sys.create_tag_from_watermark(table => '...', tag => '...', watermark => ..., time_retained => '...')`
//! - `CALL sys.create_global_index(table => '...', index_column => '...', index_type => 'btree')`
//! - `CALL sys.create_global_index(table => '...', index_column => '...', index_type => 'bitmap')`
//! - `CALL sys.create_global_index(table => '...', index_column => '...', index_type => 'ivf-pq')`
//! - `CALL sys.drop_global_index(table => '...', index_column => '...', index_type => 'btree')` (also 'bitmap', 'multivalue', 'fm', 'lumina', or a vindex type such as 'ivf-pq')
//! - `CALL sys.create_lumina_index(table => '...', index_column => '...')`
//!
//! REST management procedures (REST catalogs only, mirroring Java's
//! `RESTCatalog.permissionManagement()` / `policyManagement()`):
//! - `CALL sys.grant_permission(resource_type => '...', access => '...', principal => '...'[, database, table, function, view, expire_time, column_names, excluded_column_names])`
//! - `CALL sys.revoke_permission(resource_type => '...', access => '...', principal => '...'[, database, table, function, view])`
//! - `CALL sys.list_permissions(resource_type => '...'[, database, table, function, view, principal, access, max_results, page_token])`
//! - `CALL sys.create_policy(database => '...', table => '...', policy_type => '...', principal => '...'[, predicate_json, on_column, transform_json])`
//! - `CALL sys.drop_policy(database => '...', table => '...', policy_type => '...', principal => '...'[, column, if_exists])`
//! - `CALL sys.list_policies(database => '...', table => '...'[, policy_type, principal, column, max_results, page_token])`
//!
//! The `index_type` argument of the three global index procedures is
//! case-insensitive and surrounding whitespace is ignored.
//! `drop_global_index` accepts partition alternatives such as
//! `partitions => 'dt=2026-10-10,hr=1;dt=2026-10-11,hr=2'`; omitted keys are unrestricted.
//! Explicit empty or invalid partition specifications fail before deleting any index.
//! Timezone-less LTZ partition values use local time for non-legacy names and UTC for
//! legacy names. Ambiguous or nonexistent local times require an explicit offset.
//!
//! `column_names` and `excluded_column_names` are comma-separated lists, not SQL arrays:
//! `column_names => 'id, region'`. Java's Spark procedures take `ARRAY<STRING>` there, but a
//! DataFusion CALL argument is always a scalar, so this crate uses the same comma convention as
//! `delete_tag`'s `tag` argument.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgOperator,
    FunctionArguments, ObjectName, Value as SqlValue,
};
use paimon::api::{
    ColumnMask, DataPolicy, ListPermissionsRequest, ListPoliciesRequest, PermissionAssignment,
    PermissionColumns, PermissionResource, PolicyType, ResourceType, RowFilter,
};
use paimon::catalog::{Catalog, Identifier, RESTCatalog};
use paimon::lumina::{is_lumina_index_type, LUMINA_IDENTIFIER};
use paimon::spec::{CoreOptions, DataType, Datum, Predicate, PredicateBuilder, Snapshot};
use paimon::table::{
    normalize_global_index_type, BranchManager, ConsumerManager, SnapshotManager, Table,
    TagManager, SUPPORTED_GLOBAL_INDEX_TYPES_FOR_DROP,
};

use crate::error::to_datafusion_error;

/// Default `index_type` for the global index procedures when the argument is
/// omitted, matching Java's `CreateGlobalIndexProcedure`.
const DEFAULT_GLOBAL_INDEX_TYPE: &str = "btree";

/// Resolve a snapshot by id: try live snapshot file first, then fall back to tag metadata.
async fn resolve_snapshot_by_id(
    sm: &SnapshotManager,
    tm: &TagManager,
    snapshot_id: i64,
) -> DFResult<Snapshot> {
    if let Ok(snap) = sm.get_snapshot(snapshot_id).await {
        return Ok(snap);
    }
    let tags = tm.list_all().await.map_err(to_datafusion_error)?;
    for (_, snap) in &tags {
        if snap.id() == snapshot_id {
            return Ok(snap.clone());
        }
    }
    Err(DataFusionError::Plan(format!(
        "Snapshot '{snapshot_id}' does not exist in live files or tag metadata"
    )))
}

/// Find the earliest snapshot with commit time >= timestamp_millis,
/// considering both live snapshots and tag-retained snapshots.
async fn later_or_equal_from_all(
    sm: &SnapshotManager,
    tm: &TagManager,
    timestamp_millis: i64,
) -> DFResult<Option<Snapshot>> {
    let live = sm
        .later_or_equal_time_millis(timestamp_millis)
        .await
        .map_err(to_datafusion_error)?;
    let tags = tm.list_all().await.map_err(to_datafusion_error)?;
    let tag_candidate = tags
        .into_iter()
        .map(|(_, snap)| snap)
        .filter(|s| (s.time_millis() as i64) >= timestamp_millis)
        .min_by_key(|s| s.time_millis());
    match (live, tag_candidate) {
        (Some(a), Some(b)) => {
            if a.time_millis() <= b.time_millis() {
                Ok(Some(a))
            } else {
                Ok(Some(b))
            }
        }
        (Some(a), None) => Ok(Some(a)),
        (None, Some(b)) => Ok(Some(b)),
        (None, None) => Ok(None),
    }
}

/// Find a snapshot with watermark >= the requested value, including tag-retained
/// snapshots. Matches Java's Spark procedure: compare the first eligible live and
/// tagged snapshots by watermark, then by snapshot ID when watermarks are equal.
async fn later_or_equal_watermark_from_all(
    sm: &SnapshotManager,
    tm: &TagManager,
    watermark: i64,
) -> DFResult<Option<Snapshot>> {
    let live = sm
        .later_or_equal_watermark(watermark)
        .await
        .map_err(to_datafusion_error)?;
    let tags = tm.list_all().await.map_err(to_datafusion_error)?;
    let tag_candidate = tags
        .into_iter()
        .map(|(_, snapshot)| snapshot)
        .filter(|snapshot| {
            snapshot
                .watermark()
                .is_some_and(|value| value != i64::MIN && value >= watermark)
        })
        // Rust lists tags by name; Java's tag candidates are ordered by snapshot ID.
        .min_by_key(Snapshot::id);
    Ok(live
        .into_iter()
        .chain(tag_candidate)
        .min_by_key(|snapshot| (snapshot.watermark(), snapshot.id())))
}

/// Find the latest snapshot with commit time <= timestamp_millis,
/// considering both live snapshots and tag-retained snapshots.
async fn earlier_or_equal_from_all(
    sm: &SnapshotManager,
    tm: &TagManager,
    timestamp_millis: i64,
) -> DFResult<Option<Snapshot>> {
    let live = sm
        .earlier_or_equal_time_millis(timestamp_millis)
        .await
        .map_err(to_datafusion_error)?;
    let tags = tm.list_all().await.map_err(to_datafusion_error)?;
    let tag_candidate = tags
        .into_iter()
        .map(|(_, snap)| snap)
        .filter(|s| (s.time_millis() as i64) <= timestamp_millis)
        .max_by_key(|s| s.time_millis());
    match (live, tag_candidate) {
        (Some(a), Some(b)) => {
            if a.time_millis() >= b.time_millis() {
                Ok(Some(a))
            } else {
                Ok(Some(b))
            }
        }
        (Some(a), None) => Ok(Some(a)),
        (None, Some(b)) => Ok(Some(b)),
        (None, None) => Ok(None),
    }
}

/// The parameter names each `sys.*` procedure declares, in Java's order. Java rejects a
/// CALL argument that no parameter declares, so a typo cannot be dropped in silence: a
/// misspelled `snapshot_id` on `create_tag` would otherwise tag the latest snapshot, and
/// a misspelled `index_type` on `create_global_index` would build the default index.
fn declared_parameters(proc_name: &str) -> Option<&'static [&'static str]> {
    Some(match proc_name {
        "create_tag" => &["table", "tag", "snapshot_id", "time_retained"],
        "replace_tag" => &["table", "tag", "snapshot_id", "time_retained"],
        "delete_tag" => &["table", "tag"],
        "rename_tag" => &["table", "tag", "target_tag"],
        "reset_consumer" => &["table", "consumer_id", "next_snapshot_id"],
        "clear_consumers" => &["table", "including_consumers", "excluding_consumers"],
        "rollback_to" => &["table", "snapshot_id", "tag"],
        "rollback_to_timestamp" => &["table", "timestamp"],
        "create_tag_from_timestamp" => &["table", "tag", "timestamp"],
        "create_tag_from_watermark" => &["table", "tag", "watermark", "time_retained"],
        "rename_branch" => &["table", "from_branch", "to_branch"],
        "delete_branch" => &["table", "branch"],
        "create_global_index" => &["table", "index_column", "index_type", "options"],
        "drop_global_index" => &[
            "table",
            "index_column",
            "index_type",
            "partitions",
            "dry_run",
        ],
        "create_lumina_index" => &["table", "index_column", "index_type", "options"],
        "grant_permission" => &[
            "resource_type",
            "access",
            "principal",
            "database",
            "table",
            "function",
            "view",
            "expire_time",
            "column_names",
            "excluded_column_names",
        ],
        "revoke_permission" => &[
            "resource_type",
            "access",
            "principal",
            "database",
            "table",
            "function",
            "view",
        ],
        "list_permissions" => &[
            "resource_type",
            "database",
            "table",
            "function",
            "view",
            "principal",
            "access",
            "max_results",
            "page_token",
        ],
        "create_policy" => &[
            "database",
            "table",
            "policy_type",
            "principal",
            "predicate_json",
            "on_column",
            "transform_json",
        ],
        "drop_policy" => &[
            "database",
            "table",
            "policy_type",
            "principal",
            "column",
            "if_exists",
        ],
        "list_policies" => &[
            "database",
            "table",
            "policy_type",
            "principal",
            "column",
            "max_results",
            "page_token",
        ],
        _ => return None,
    })
}

/// Reject a CALL argument that no parameter of `proc_name` declares, matching Java's
/// `ProcedureBase` argument binding. Applies to every procedure with a declared parameter
/// set; a procedure absent from [`declared_parameters`] is left unchecked.
fn reject_unknown_args(proc_name: &str, args: &HashMap<String, String>) -> DFResult<()> {
    if let Some(declared) = declared_parameters(proc_name) {
        if let Some(unknown) = args.keys().find(|key| !declared.contains(&key.as_str())) {
            return Err(DataFusionError::Plan(format!(
                "Argument {unknown} is unknown. Expected one of [{}].",
                declared.join(", ")
            )));
        }
    }
    Ok(())
}

pub async fn execute_call(
    ctx: &SessionContext,
    catalogs: &HashMap<String, Arc<dyn Catalog>>,
    current_catalog: &str,
    func: &Function,
) -> DFResult<DataFrame> {
    let (explicit_catalog, proc_name) = extract_procedure_name(&func.name)?;
    let catalog_name = explicit_catalog.as_deref().unwrap_or(current_catalog);
    let catalog = catalogs
        .get(catalog_name)
        .ok_or_else(|| DataFusionError::Plan(format!("Unknown catalog '{catalog_name}'")))?;
    let args = extract_named_args(&func.args)?;

    reject_unknown_args(&proc_name, &args)?;

    match proc_name.as_str() {
        "create_tag" => proc_create_tag(ctx, catalog, catalog_name, &args).await,
        "replace_tag" => proc_replace_tag(ctx, catalog, catalog_name, &args).await,
        "delete_tag" => proc_delete_tag(ctx, catalog, catalog_name, &args).await,
        "rename_tag" => proc_rename_tag(ctx, catalog, catalog_name, &args).await,
        "reset_consumer" => proc_reset_consumer(ctx, catalog, catalog_name, &args).await,
        "clear_consumers" => proc_clear_consumers(ctx, catalog, catalog_name, &args).await,
        "rollback_to" => proc_rollback_to(ctx, catalog, catalog_name, &args).await,
        "rollback_to_timestamp" => {
            proc_rollback_to_timestamp(ctx, catalog, catalog_name, &args).await
        }
        "create_tag_from_timestamp" => {
            proc_create_tag_from_timestamp(ctx, catalog, catalog_name, &args).await
        }
        "create_tag_from_watermark" => {
            proc_create_tag_from_watermark(ctx, catalog, catalog_name, &args).await
        }
        "rename_branch" => proc_rename_branch(ctx, catalog, catalog_name, &args).await,
        "delete_branch" => proc_delete_branch(ctx, catalog, catalog_name, &args).await,
        "create_global_index" => proc_create_global_index(ctx, catalog, catalog_name, &args).await,
        "drop_global_index" => proc_drop_global_index(ctx, catalog, catalog_name, &args).await,
        "create_lumina_index" => proc_create_lumina_index(ctx, catalog, catalog_name, &args).await,
        "grant_permission" => proc_grant_permission(ctx, catalog, catalog_name, &args).await,
        "revoke_permission" => proc_revoke_permission(ctx, catalog, catalog_name, &args).await,
        "list_permissions" => proc_list_permissions(ctx, catalog, catalog_name, &args).await,
        "create_policy" => proc_create_policy(ctx, catalog, catalog_name, &args).await,
        "drop_policy" => proc_drop_policy(ctx, catalog, catalog_name, &args).await,
        "list_policies" => proc_list_policies(ctx, catalog, catalog_name, &args).await,
        _ => Err(DataFusionError::Plan(format!(
            "Unknown procedure: {proc_name}"
        ))),
    }
}

/// Returns (optional_catalog_name, procedure_name).
fn extract_procedure_name(name: &ObjectName) -> DFResult<(Option<String>, String)> {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident().map(|id| id.value.clone()))
        .collect();
    match parts.len() {
        1 => Ok((None, parts[0].clone())),
        2 => Ok((None, parts[1].clone())),
        3 => Ok((Some(parts[0].clone()), parts[2].clone())),
        _ => Err(DataFusionError::Plan(format!(
            "Invalid procedure name: {name}. Expected procedure_name, sys.procedure_name, or catalog.sys.procedure_name"
        ))),
    }
}

fn extract_named_args(args: &FunctionArguments) -> DFResult<HashMap<String, String>> {
    let arg_list = match args {
        FunctionArguments::List(list) => &list.args,
        FunctionArguments::None => return Ok(HashMap::new()),
        _ => {
            return Err(DataFusionError::Plan(
                "Unsupported argument format for CALL".to_string(),
            ))
        }
    };

    let mut map = HashMap::new();
    for arg in arg_list {
        match arg {
            FunctionArg::Named {
                name,
                arg: FunctionArgExpr::Expr(expr),
                operator: FunctionArgOperator::RightArrow,
            } => {
                let value = expr_to_string(expr)?;
                let name = name.value.to_lowercase();
                // Java `PaimonProcedureResolver.buildNameToArgumentMap` rejects a repeat
                // rather than letting the last one win.
                if map.insert(name.clone(), value).is_some() {
                    return Err(DataFusionError::Plan(format!(
                        "Procedure argument {name} is duplicated."
                    )));
                }
            }
            _ => return Err(DataFusionError::Plan(
                "CALL procedures require named arguments with '=>' syntax, e.g. table => 'db.t'"
                    .to_string(),
            )),
        }
    }
    Ok(map)
}

fn expr_to_string(expr: &SqlExpr) -> DFResult<String> {
    match expr {
        SqlExpr::Value(v) => match &v.value {
            SqlValue::SingleQuotedString(s) => Ok(s.clone()),
            SqlValue::Number(n, _) => Ok(n.clone()),
            SqlValue::Boolean(b) => Ok(b.to_string()),
            _ => Err(DataFusionError::Plan(format!(
                "Unsupported argument value: {v}"
            ))),
        },
        SqlExpr::UnaryOp {
            op: datafusion::sql::sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => {
            let inner = expr_to_string(expr)?;
            Ok(format!("-{inner}"))
        }
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported argument expression: {expr}"
        ))),
    }
}

fn require_arg<'a>(args: &'a HashMap<String, String>, name: &str) -> DFResult<&'a str> {
    args.get(name)
        .map(|s| s.as_str())
        .ok_or_else(|| DataFusionError::Plan(format!("Missing required argument: '{name}'")))
}

fn resolve_table_identifier(table_str: &str, catalog_name: &str) -> DFResult<Identifier> {
    let parts: Vec<&str> = table_str.split('.').collect();
    match parts.len() {
        2 => Ok(Identifier::new(parts[0], parts[1])),
        3 => {
            if parts[0] != catalog_name {
                return Err(DataFusionError::Plan(format!(
                    "Catalog name mismatch: expected '{catalog_name}', got '{}'",
                    parts[0]
                )));
            }
            Ok(Identifier::new(parts[1], parts[2]))
        }
        _ => Err(DataFusionError::Plan(format!(
            "Invalid table identifier: '{table_str}'. Expected 'database.table' or 'catalog.database.table'"
        ))),
    }
}

async fn get_table(
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<Table> {
    let table_str = require_arg(args, "table")?;
    let identifier = resolve_table_identifier(table_str, catalog_name)?;
    crate::table_loader::get_paimon_table(catalog, &identifier).await
}

fn managers(table: &Table) -> (SnapshotManager, TagManager) {
    let sm = SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let tm = TagManager::new(table.file_io().clone(), table.location().to_string());
    (sm, tm)
}

async fn proc_reset_consumer(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let consumer_id = require_arg(args, "consumer_id")?;
    ConsumerManager::validate_consumer_id(consumer_id).map_err(to_datafusion_error)?;
    let next_snapshot_id = args
        .get("next_snapshot_id")
        .map(|value| {
            value
                .parse::<i64>()
                .ok()
                .filter(|id| *id > 0)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Invalid next_snapshot_id: '{value}'. Expected a positive BIGINT"
                    ))
                })
        })
        .transpose()?;
    let table = get_table(catalog, catalog_name, args).await?;
    if table.rest_env().is_some() {
        return Err(DataFusionError::NotImplemented(
            "reset_consumer is not supported for tables managed by a REST catalog".to_string(),
        ));
    }
    if table.schema().core_options().is_format_table() {
        return Err(DataFusionError::NotImplemented(
            "reset_consumer requires a FileStoreTable; format-table is not supported".to_string(),
        ));
    }
    let manager = table.consumer_manager();
    if let Some(id) = next_snapshot_id {
        // Stream recovery reads live snapshot files, not tag-retained metadata.
        table
            .snapshot_manager()
            .get_snapshot(id)
            .await
            .map_err(to_datafusion_error)?;
        manager
            .reset(consumer_id, id)
            .await
            .map_err(to_datafusion_error)?;
    } else {
        manager
            .delete(consumer_id)
            .await
            .map_err(to_datafusion_error)?;
    }
    ok_result(ctx)
}

async fn proc_clear_consumers(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let table = get_table(catalog, catalog_name, args).await?;
    if table.rest_env().is_some() {
        return Err(DataFusionError::NotImplemented(
            "clear_consumers is not supported for tables managed by a REST catalog".to_string(),
        ));
    }
    if table.schema().core_options().is_format_table() {
        return Err(DataFusionError::NotImplemented(
            "clear_consumers requires a FileStoreTable; format-table is not supported".to_string(),
        ));
    }
    table
        .consumer_manager()
        .clear(
            args.get("including_consumers").map(String::as_str),
            args.get("excluding_consumers").map(String::as_str),
        )
        .await
        .map_err(to_datafusion_error)?;
    ok_result(ctx)
}

async fn proc_create_tag(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let table = get_table(catalog, catalog_name, args).await?;
    let tag_name = require_arg(args, "tag")?;
    let snapshot_id: Option<i64> = args
        .get("snapshot_id")
        .map(|s| {
            s.parse()
                .map_err(|_| DataFusionError::Plan(format!("Invalid snapshot_id: '{s}'")))
        })
        .transpose()?;
    let time_retained = args.get("time_retained").map(String::as_str);

    let (sm, tm) = managers(&table);
    if tm.tag_exists(tag_name).await.map_err(to_datafusion_error)? {
        return Err(DataFusionError::Plan(format!(
            "Tag '{tag_name}' already exists"
        )));
    }
    let snapshot = if let Some(id) = snapshot_id {
        resolve_snapshot_by_id(&sm, &tm, id).await?
    } else {
        sm.get_latest_snapshot()
            .await
            .map_err(to_datafusion_error)?
            .ok_or_else(|| DataFusionError::Plan("No snapshots exist".to_string()))?
    };
    tm.create_with_retention(tag_name, &snapshot, time_retained)
        .await
        .map_err(to_datafusion_error)?;
    ok_result(ctx)
}

async fn proc_replace_tag(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let table = get_table(catalog, catalog_name, args).await?;
    let tag_name = require_arg(args, "tag")?;
    let snapshot_id = args
        .get("snapshot_id")
        .map(|value| {
            value
                .parse::<i64>()
                .ok()
                .filter(|id| *id > 0)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Invalid snapshot_id: '{value}'. Expected a positive BIGINT"
                    ))
                })
        })
        .transpose()?;
    let time_retained = args.get("time_retained").map(String::as_str);

    if table.rest_env().is_some() {
        return Err(DataFusionError::NotImplemented(
            "replace_tag is not supported for tables managed by a REST catalog".to_string(),
        ));
    }
    if table.schema().core_options().is_format_table() {
        return Err(DataFusionError::NotImplemented(
            "replace_tag requires a FileStoreTable; format-table is not supported".to_string(),
        ));
    }

    let sm = table.snapshot_manager();
    let tm = table.tag_manager();
    if !tm.tag_exists(tag_name).await.map_err(to_datafusion_error)? {
        return Err(to_datafusion_error(paimon::Error::TagNotExist {
            tag_name: tag_name.to_string(),
        }));
    }
    let snapshot = if let Some(id) = snapshot_id {
        match sm.get_snapshot(id).await {
            Ok(snapshot) => snapshot,
            Err(paimon::Error::SnapshotNotExist { .. }) => tm
                .list_all()
                .await
                .map_err(to_datafusion_error)?
                .into_iter()
                .map(|(_, snapshot)| snapshot)
                .find(|snapshot| snapshot.id() == id)
                .ok_or_else(|| {
                    to_datafusion_error(paimon::Error::SnapshotNotExist { snapshot_id: id })
                })?,
            Err(error) => return Err(to_datafusion_error(error)),
        }
    } else {
        sm.get_latest_snapshot()
            .await
            .map_err(to_datafusion_error)?
            .ok_or_else(|| DataFusionError::Plan("No snapshots exist".to_string()))?
    };
    tm.replace(tag_name, &snapshot, time_retained)
        .await
        .map_err(to_datafusion_error)?;
    ok_result(ctx)
}

async fn proc_rename_branch(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let table = get_table(catalog, catalog_name, args).await?;
    let from_branch = require_arg(args, "from_branch")?;
    let to_branch = require_arg(args, "to_branch")?;

    // A REST catalog owns the authoritative snapshot/branch metadata and exposes
    // no branch-rename endpoint (Java `RESTCatalog.renameBranch` throws
    // `UnsupportedOperationException`). Renaming here would move the physical
    // branch directory while the catalog kept pointing at the old name, so later
    // reads of both the old and new name would fail. Refuse the rename until a
    // catalog-aware path exists, instead of silently desyncing the table.
    if table.rest_env().is_some() {
        return Err(DataFusionError::NotImplemented(
            "rename_branch is not supported for tables managed by a REST catalog".to_string(),
        ));
    }

    let bm = BranchManager::new(table.file_io().clone(), table.location().to_string());
    bm.rename_branch(from_branch, to_branch)
        .await
        .map_err(to_datafusion_error)?;
    ok_result(ctx)
}

async fn proc_delete_branch(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let table = get_table(catalog, catalog_name, args).await?;
    let branch_str = require_arg(args, "branch")?;

    // A REST catalog owns branch metadata and the Rust REST catalog has no
    // branch-drop endpoint yet (Java `RESTCatalog.dropBranch` DELETEs through the
    // catalog). Dropping the physical branch directory here would leave the
    // catalog still listing the branch while its data is gone. Refuse before
    // deleting anything, until a catalog-aware path exists.
    if table.rest_env().is_some() {
        return Err(DataFusionError::NotImplemented(
            "delete_branch is not yet supported for tables managed by a REST catalog".to_string(),
        ));
    }

    let bm = BranchManager::new(table.file_io().clone(), table.location().to_string());
    let options = table.schema().options();
    // Java `Table.deleteBranches` splits on comma and passes each token through
    // unchanged. Branch identities keep leading/trailing spaces (the shared
    // validator and readers treat `" b1"` and `"b1"` as distinct), so the
    // delete_tag trim convention must not apply here — trimming could delete a
    // different branch than the one requested.
    for branch_name in branch_str.split(',') {
        if branch_name.is_empty() {
            continue;
        }
        // Validate the logical name before any existence check or deletion: a
        // separator-bearing name like `prod/schema` would otherwise slip past the
        // configured-branch guard (it is not literally `prod`) and recursively
        // delete the inner directory of a protected branch.
        BranchManager::validate_branch_name(branch_name).map_err(to_datafusion_error)?;
        BranchManager::ensure_branch_deletable(options, branch_name)
            .map_err(to_datafusion_error)?;
        if !bm
            .branch_exists(branch_name)
            .await
            .map_err(to_datafusion_error)?
        {
            continue;
        }
        bm.drop_branch(branch_name)
            .await
            .map_err(to_datafusion_error)?;
    }
    ok_result(ctx)
}

async fn proc_delete_tag(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let table = get_table(catalog, catalog_name, args).await?;
    let tag_str = require_arg(args, "tag")?;

    let (_, tm) = managers(&table);
    for tag_name in tag_str.split(',') {
        let tag_name = tag_name.trim();
        if tag_name.is_empty() {
            continue;
        }
        if !tm.tag_exists(tag_name).await.map_err(to_datafusion_error)? {
            continue;
        }
        tm.delete(tag_name).await.map_err(to_datafusion_error)?;
    }
    ok_result(ctx)
}

async fn proc_rename_tag(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let table = get_table(catalog, catalog_name, args).await?;
    let tag_name = require_arg(args, "tag")?;
    let target_tag_name = require_arg(args, "target_tag")?;

    let (_, tm) = managers(&table);
    tm.rename(tag_name, target_tag_name)
        .await
        .map_err(to_datafusion_error)?;
    ok_result(ctx)
}

async fn clean_larger_than(
    sm: &SnapshotManager,
    tm: &TagManager,
    retained_snapshot_id: i64,
) -> DFResult<()> {
    // 1. Update LATEST hint
    sm.write_latest_hint(retained_snapshot_id)
        .await
        .map_err(to_datafusion_error)?;

    // 2. Delete snapshots newer than the target
    let all_ids = sm.list_all_ids().await.map_err(to_datafusion_error)?;
    for &id in all_ids.iter().rev() {
        if id <= retained_snapshot_id {
            break;
        }
        sm.delete_snapshot(id).await.map_err(to_datafusion_error)?;
    }

    // TODO: clean long-lived changelogs newer than retained_snapshot_id
    // Java's RollbackHelper.cleanLargerThan also calls cleanLongLivedChangelogs here.
    // Implement once ChangelogManager is available.

    // 3. Delete tags that reference snapshots newer than the target
    let tags = tm.list_all().await.map_err(to_datafusion_error)?;
    for (name, snap) in tags.iter().rev() {
        if snap.id() <= retained_snapshot_id {
            continue;
        }
        tm.delete(name).await.map_err(to_datafusion_error)?;
    }

    Ok(())
}

async fn proc_rollback_to(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let table = get_table(catalog, catalog_name, args).await?;

    if let Some(rest_env) = table.rest_env() {
        if let Some(id_str) = args.get("snapshot_id") {
            let id: i64 = id_str
                .parse()
                .map_err(|_| DataFusionError::Plan(format!("Invalid snapshot_id: '{id_str}'")))?;
            rest_env
                .api()
                .rollback_to(
                    rest_env.identifier(),
                    &paimon::table::Instant::snapshot(id),
                    None,
                    Some(rest_env.uuid()),
                )
                .await
                .map_err(to_datafusion_error)?;
        } else if let Some(tag_name) = args.get("tag") {
            rest_env
                .api()
                .rollback_to(
                    rest_env.identifier(),
                    &paimon::table::Instant::tag(tag_name),
                    None,
                    Some(rest_env.uuid()),
                )
                .await
                .map_err(to_datafusion_error)?;
        } else {
            return Err(DataFusionError::Plan(
                "rollback_to requires either 'snapshot_id' or 'tag' argument".to_string(),
            ));
        }
    } else {
        let (sm, tm) = managers(&table);
        if let Some(id_str) = args.get("snapshot_id") {
            let id: i64 = id_str
                .parse()
                .map_err(|_| DataFusionError::Plan(format!("Invalid snapshot_id: '{id_str}'")))?;
            let snapshot = resolve_snapshot_by_id(&sm, &tm, id).await?;
            clean_larger_than(&sm, &tm, id).await?;
            if !sm
                .file_io()
                .exists(&sm.snapshot_path(id))
                .await
                .map_err(to_datafusion_error)?
            {
                sm.commit_snapshot(&snapshot)
                    .await
                    .map_err(to_datafusion_error)?;
                sm.write_earliest_hint(id)
                    .await
                    .map_err(to_datafusion_error)?;
            }
        } else if let Some(tag_name) = args.get("tag") {
            let snapshot = tm
                .get(tag_name)
                .await
                .map_err(to_datafusion_error)?
                .ok_or_else(|| DataFusionError::Plan(format!("Tag '{tag_name}' does not exist")))?;
            let snapshot_id = snapshot.id();
            clean_larger_than(&sm, &tm, snapshot_id).await?;
            if !sm
                .file_io()
                .exists(&sm.snapshot_path(snapshot_id))
                .await
                .map_err(to_datafusion_error)?
            {
                sm.commit_snapshot(&snapshot)
                    .await
                    .map_err(to_datafusion_error)?;
                sm.write_earliest_hint(snapshot_id)
                    .await
                    .map_err(to_datafusion_error)?;
            }
        } else {
            return Err(DataFusionError::Plan(
                "rollback_to requires either 'snapshot_id' or 'tag' argument".to_string(),
            ));
        }
    }

    ok_result(ctx)
}

async fn proc_rollback_to_timestamp(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let table = get_table(catalog, catalog_name, args).await?;
    let ts_str = require_arg(args, "timestamp")?;
    let timestamp: i64 = ts_str
        .parse()
        .map_err(|_| DataFusionError::Plan(format!("Invalid timestamp: '{ts_str}'")))?;

    let (sm, tm) = managers(&table);
    let snapshot = earlier_or_equal_from_all(&sm, &tm, timestamp)
        .await?
        .ok_or_else(|| {
            DataFusionError::Plan(format!("No snapshot found with commit time <= {timestamp}"))
        })?;

    if let Some(rest_env) = table.rest_env() {
        rest_env
            .api()
            .rollback_to(
                rest_env.identifier(),
                &paimon::table::Instant::snapshot(snapshot.id()),
                None,
                Some(rest_env.uuid()),
            )
            .await
            .map_err(to_datafusion_error)?;
    } else {
        clean_larger_than(&sm, &tm, snapshot.id()).await?;
    }
    ok_result(ctx)
}

async fn proc_create_tag_from_timestamp(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let table = get_table(catalog, catalog_name, args).await?;
    let tag_name = require_arg(args, "tag")?;
    let ts_str = require_arg(args, "timestamp")?;
    let timestamp: i64 = ts_str
        .parse()
        .map_err(|_| DataFusionError::Plan(format!("Invalid timestamp: '{ts_str}'")))?;

    let (sm, tm) = managers(&table);
    let snapshot = later_or_equal_from_all(&sm, &tm, timestamp)
        .await?
        .ok_or_else(|| {
            DataFusionError::Plan(format!("No snapshot found with commit time >= {timestamp}"))
        })?;

    if tm.tag_exists(tag_name).await.map_err(to_datafusion_error)? {
        return Err(DataFusionError::Plan(format!(
            "Tag '{tag_name}' already exists"
        )));
    }
    tm.create(tag_name, &snapshot)
        .await
        .map_err(to_datafusion_error)?;
    ok_result(ctx)
}

async fn proc_create_tag_from_watermark(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let table = get_table(catalog, catalog_name, args).await?;
    let tag_name = require_arg(args, "tag")?;
    let watermark_str = require_arg(args, "watermark")?;
    let watermark: i64 = watermark_str
        .parse()
        .map_err(|_| DataFusionError::Plan(format!("Invalid watermark: '{watermark_str}'")))?;
    let time_retained = args.get("time_retained").map(String::as_str);

    // REST snapshots may exist only in the catalog; storage listings cannot
    // enumerate the authoritative history needed for watermark selection.
    if table.rest_env().is_some() {
        return Err(DataFusionError::NotImplemented(
            "create_tag_from_watermark is not supported for tables managed by a REST catalog"
                .to_string(),
        ));
    }

    let (sm, tm) = managers(&table);
    if tm.tag_exists(tag_name).await.map_err(to_datafusion_error)? {
        return Err(DataFusionError::Plan(format!(
            "Tag '{tag_name}' already exists"
        )));
    }
    let snapshot = later_or_equal_watermark_from_all(&sm, &tm, watermark)
        .await?
        .ok_or_else(|| {
            DataFusionError::Plan(format!("No snapshot found with watermark >= {watermark}"))
        })?;
    tm.create_with_retention(tag_name, &snapshot, time_retained)
        .await
        .map_err(to_datafusion_error)?;
    ok_result(ctx)
}

async fn proc_create_lumina_index(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let table = get_table(catalog, catalog_name, args).await?;
    let index_column = require_arg(args, "index_column")?;
    let mut builder = table.new_global_index_build_builder();
    builder.with_index_column(index_column);
    let index_type = normalize_index_type(
        args.get("index_type")
            .map(String::as_str)
            .unwrap_or(LUMINA_IDENTIFIER),
    );
    if !is_lumina_index_type(&index_type) {
        return Err(DataFusionError::NotImplemented(format!(
            "Unsupported Lumina index type: {index_type}"
        )));
    }
    builder.with_index_type(&index_type);
    if let Some(options) = args.get("options") {
        builder.with_options(parse_key_value_options(options)?);
    }
    builder.execute().await.map_err(to_datafusion_error)?;
    ok_result(ctx)
}

async fn proc_create_global_index(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let table = get_table(catalog, catalog_name, args).await?;
    let index_column = require_arg(args, "index_column")?;
    let index_type_arg = args
        .get("index_type")
        .map(String::as_str)
        .unwrap_or(DEFAULT_GLOBAL_INDEX_TYPE);
    let mut builder = table.new_global_index_build_builder();
    builder
        .with_index_column(index_column)
        .with_index_type(index_type_arg);
    if let Some(options) = args.get("options") {
        builder.with_options(parse_key_value_options(options)?);
    }
    builder.execute().await.map_err(to_datafusion_error)?;
    ok_result(ctx)
}

async fn proc_drop_global_index(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let table = get_table(catalog, catalog_name, args).await?;
    let index_column = require_arg(args, "index_column")?;
    let index_type_arg = args
        .get("index_type")
        .map(String::as_str)
        .unwrap_or(DEFAULT_GLOBAL_INDEX_TYPE);
    let index_type = normalize_index_type(index_type_arg);
    let index_type = index_type.as_str();
    if normalize_global_index_type(index_type).is_none() {
        // Echo the raw argument, not the normalized one, so a typo stays visible.
        return Err(DataFusionError::NotImplemented(format!(
            "unsupported global index type '{index_type_arg}'; supported: {SUPPORTED_GLOBAL_INDEX_TYPES_FOR_DROP}"
        )));
    }
    let dry_run = bool_arg(args, "dry_run")?;

    let mut builder = table.new_global_index_drop_builder();
    builder.with_index_column(index_column);
    builder.with_index_type(index_type);
    builder.with_dry_run(dry_run);
    if let Some(partitions) = args.get("partitions") {
        builder
            .with_partition_filter(parse_global_index_partitions(&table, partitions)?)
            .map_err(to_datafusion_error)?;
    }
    let dropped = builder.execute().await.map_err(to_datafusion_error)?;
    if dry_run {
        return utf8_result(
            ctx,
            &[("result", false)],
            vec![vec![Some(format!(
                "Would drop {dropped} global index file(s)"
            ))]],
        );
    }
    ok_result(ctx)
}

/// Semicolon-separated alternatives, each containing comma-separated key=value constraints.
fn parse_global_index_partitions(table: &Table, partitions: &str) -> DFResult<Predicate> {
    let partition_fields = table.schema().partition_fields();
    if partition_fields.is_empty() {
        return Err(DataFusionError::Plan(
            "Cannot filter global index partitions on an unpartitioned table".to_string(),
        ));
    }
    let pb = PredicateBuilder::new(table.schema().fields());
    let options = CoreOptions::new(table.schema().options());
    let mut alternatives = Vec::new();
    for partition in partitions.split(';') {
        let mut constraints = Vec::new();
        let mut keys = std::collections::HashSet::new();
        for entry in partition.split(',') {
            let (key, value) = entry.split_once('=').ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Invalid partitions entry '{entry}'. Expected key=value"
                ))
            })?;
            // Java ParameterUtils uses String.trim(), not Unicode whitespace trimming.
            let key = key.trim_matches(|ch| ch <= ' ');
            let value = value.trim_matches(|ch| ch <= ' ');
            let field = partition_fields
                .iter()
                .find(|field| field.name() == key)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Column '{key}' is not a partition column"))
                })?;
            if !keys.insert(key) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate partition column '{key}'"
                )));
            }
            if value.is_empty() {
                return Err(DataFusionError::Plan(format!(
                    "Empty partitions value for column '{key}'"
                )));
            }
            let predicate = if value == options.partition_default_name() {
                pb.is_null(key)
            } else {
                let datum = parse_global_index_partition_value(
                    value,
                    field.data_type(),
                    options.legacy_partition_name(),
                )
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Invalid partitions value '{value}' for column '{key}' with type {:?}",
                        field.data_type()
                    ))
                })?;
                pb.equal(key, datum)
            };
            constraints.push(predicate.map_err(to_datafusion_error)?);
        }
        alternatives.push(Predicate::and(constraints));
    }
    Ok(Predicate::or(alternatives))
}

fn parse_global_index_partition_value(
    value: &str,
    data_type: &DataType,
    legacy_partition_name: bool,
) -> Option<Datum> {
    match data_type {
        DataType::Char(typ) => {
            (value.chars().count() <= typ.length()).then(|| Datum::String(value.to_string()))
        }
        DataType::VarChar(typ) => (value.chars().count() <= typ.length() as usize)
            .then(|| Datum::String(value.to_string())),
        DataType::Binary(_) => Some(Datum::Bytes(value.as_bytes().to_vec())),
        DataType::VarBinary(typ) => {
            (value.len() <= typ.length() as usize).then(|| Datum::Bytes(value.as_bytes().to_vec()))
        }
        DataType::Float(_) => {
            // Match TypeUtils.castFromString's check for FLOAT precision loss.
            let double = value.parse::<f64>().ok()?;
            let float = value.parse::<f32>().ok()?;
            (f64::from(float) == double
                || (float.is_nan() && double.is_nan())
                || paimon::spec::format_java_float(float).parse::<f64>().ok()? == double)
                .then_some(Datum::Float(float))
        }
        DataType::Double(_) => value.parse().ok().map(Datum::Double),
        DataType::Decimal(typ) => {
            // Java Decimal.fromBigDecimal rounds HALF_UP; Arrow's string cast truncates.
            if value.contains('_') {
                return None;
            }
            let decimal = value
                .parse::<bigdecimal::BigDecimal>()
                .ok()?
                .with_scale_round(i64::from(typ.scale()), bigdecimal::RoundingMode::HalfUp);
            let unscaled = decimal
                .as_bigint_and_exponent()
                .0
                .to_string()
                .parse::<i128>()
                .ok()?;
            if unscaled
                .unsigned_abs()
                .checked_ilog10()
                .map_or(1, |log| log + 1)
                > typ.precision()
            {
                return None;
            }
            Some(Datum::Decimal {
                unscaled,
                precision: typ.precision(),
                scale: typ.scale(),
            })
        }
        DataType::Time(_) => {
            paimon::table::parse_format_partition_value(value, data_type).or_else(|| {
                let arrow_type = paimon::arrow::paimon_type_to_arrow(data_type).ok()?;
                let scalar = ScalarValue::try_from_string(value.to_string(), &arrow_type).ok()?;
                crate::filter_pushdown::scalar_to_datum(&scalar, data_type)
            })
        }
        DataType::Timestamp(typ) => {
            parse_global_index_partition_timestamp(value, data_type, typ.precision(), false)
        }
        DataType::LocalZonedTimestamp(typ) => parse_global_index_partition_timestamp(
            value,
            data_type,
            typ.precision(),
            !legacy_partition_name,
        ),
        _ => paimon::table::parse_format_partition_value(value, data_type),
    }
}

fn parse_global_index_partition_timestamp(
    value: &str,
    data_type: &DataType,
    precision: u32,
    local_timezone: bool,
) -> Option<Datum> {
    if value.bytes().all(|byte| byte.is_ascii_digit()) {
        // Java only accepts numeric timestamps at these precisions.
        if !matches!(precision, 0 | 3 | 6 | 9) {
            return None;
        }
        let arrow_type = paimon::arrow::paimon_type_to_arrow(data_type).ok()?;
        let scalar = ScalarValue::Int64(Some(value.parse().ok()?))
            .cast_to(&arrow_type)
            .ok()?;
        return crate::filter_pushdown::scalar_to_datum(&scalar, data_type);
    }
    // Arrow accepts extra fractional digits by truncating them; reject any loss of identity.
    if value.split_once('.').is_some_and(|(_, fraction)| {
        fraction
            .bytes()
            .take_while(u8::is_ascii_digit)
            .skip(9)
            .any(|byte| byte != b'0')
    }) {
        return None;
    }
    // Legacy partition names omit seconds when both seconds and fractions are zero.
    let value = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M")
        .map(|value| value.format("%Y-%m-%dT%H:%M:%S").to_string())
        .unwrap_or_else(|_| value.to_string());
    let timestamp = if local_timezone {
        // Non-legacy LTZ names use the system-local timezone. Preserve explicit offsets
        // and fail on ambiguous/nonexistent local times instead of falling back to UTC.
        datafusion::arrow::compute::kernels::cast_utils::string_to_datetime(&chrono::Local, &value)
            .ok()?
            .with_timezone(&chrono::Utc)
    } else {
        datafusion::arrow::compute::kernels::cast_utils::string_to_datetime(&chrono::Utc, &value)
            .ok()?
    };
    // Stored partitions can retain fractions beyond the declared precision or Arrow unit.
    let millis = timestamp.timestamp_millis();
    let nanos = (timestamp.timestamp_subsec_nanos() % 1_000_000) as i32;
    match data_type {
        DataType::Timestamp(_) => Some(Datum::Timestamp { millis, nanos }),
        DataType::LocalZonedTimestamp(_) => Some(Datum::LocalZonedTimestamp { millis, nanos }),
        _ => None,
    }
}

/// Canonicalize a procedure's `index_type` argument: trim, then lowercase.
/// Mirrors `indexType.toLowerCase(Locale.ROOT).trim()` in Java's Flink and Spark
/// `CreateGlobalIndexProcedure` / `DropGlobalIndexProcedure`. Normalizing at this
/// boundary and the core builder both accept procedure-style identifiers.
fn normalize_index_type(index_type: &str) -> String {
    index_type.trim().to_ascii_lowercase()
}

fn parse_key_value_options(options: &str) -> DFResult<HashMap<String, String>> {
    let mut parsed = HashMap::new();
    for entry in options.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (key, value) = entry.split_once('=').ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Invalid options entry '{entry}'. Expected comma-separated key=value pairs"
            ))
        })?;
        let key = key.trim();
        if key.is_empty() {
            return Err(DataFusionError::Plan(
                "Invalid options entry with empty key".to_string(),
            ));
        }
        parsed.insert(key.to_string(), value.trim().to_string());
    }
    Ok(parsed)
}

// ==================== REST management procedures (Java #9410) ====================
//
// Parameter names follow Java's; divergences are called out where they occur.

/// The REST catalog behind `catalog`, which is where permission and policy management lives.
/// Java does the same check with `DelegateCatalog.rootCatalog(...) instanceof RESTCatalog`.
fn rest_catalog<'a>(
    catalog: &'a Arc<dyn Catalog>,
    catalog_name: &str,
) -> DFResult<&'a RESTCatalog> {
    catalog
        .as_any()
        .and_then(|any| any.downcast_ref::<RESTCatalog>())
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Catalog '{catalog_name}' does not support permission or policy management."
            ))
        })
}

/// Java's blankness, which is `String.trim()`: only `<= U+0020` counts. `str::trim` would also
/// strip the rest of Unicode whitespace and disagree on a non-breaking space. (`listagg.rs` has
/// a third variant using `Character.isWhitespace`; none of the three are interchangeable.)
fn is_blank(value: &str) -> bool {
    value.chars().all(|ch| ch <= ' ')
}

/// Java `emptyToNull`: a blank value is absent.
fn opt_arg<'a>(args: &'a HashMap<String, String>, name: &str) -> Option<&'a str> {
    args.get(name)
        .map(String::as_str)
        .filter(|value| !is_blank(value))
}

/// Java `BasePermissionProcedure.enumValue`.
/// The values Java prints in `Invalid <arg> '<value>'. Expected one of <values>.`
trait ProcedureEnum: std::str::FromStr<Err = paimon::Error> + Sized {
    fn allowed() -> Vec<&'static str>;
}

impl ProcedureEnum for ResourceType {
    fn allowed() -> Vec<&'static str> {
        ResourceType::VALUES
            .iter()
            .map(ResourceType::as_str)
            .collect()
    }
}

impl ProcedureEnum for PolicyType {
    fn allowed() -> Vec<&'static str> {
        vec![
            PolicyType::RowFilter.as_str(),
            PolicyType::ColumnMasking.as_str(),
        ]
    }
}

fn enum_arg<T>(args: &HashMap<String, String>, name: &str) -> DFResult<T>
where
    T: ProcedureEnum,
{
    let value = require_arg(args, name)?;
    // Java neither trims nor accepts blank, so ' TABLE ' must stay an error here too.
    if is_blank(value) {
        return Err(DataFusionError::Plan(format!("{name} cannot be empty.")));
    }
    value.parse().map_err(|_| {
        DataFusionError::Plan(format!(
            "Invalid {name} '{value}'. Expected one of [{}].",
            T::allowed().join(", ")
        ))
    })
}

/// Same as [`enum_arg`], but the argument may be absent or blank.
fn opt_enum_arg<T>(args: &HashMap<String, String>, name: &str) -> DFResult<Option<T>>
where
    T: ProcedureEnum,
{
    match opt_arg(args, name) {
        None => Ok(None),
        Some(_) => enum_arg(args, name).map(Some),
    }
}

/// Comma-separated, like `delete_tag`'s `tag`. Java declares `ARRAY<STRING>`, but a
/// DataFusion CALL argument is always a scalar.
fn comma_list(args: &HashMap<String, String>, name: &str) -> Option<Vec<String>> {
    args.get(name).map(|raw| {
        raw.split(',')
            // Java's trim, as everywhere else here: a non-breaking space is part of the name.
            .map(|value| value.trim_matches(|ch| ch <= ' '))
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .collect()
    })
}

fn bool_arg(args: &HashMap<String, String>, name: &str) -> DFResult<bool> {
    // Java's `ProcedureParameter.optional(..., BooleanType)` leaves a missing value as false.
    match args.get(name) {
        None => Ok(false),
        Some(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(DataFusionError::Plan(format!(
                "Invalid {name} '{value}'. Expected 'true' or 'false'"
            ))),
        },
    }
}

fn max_results_arg(args: &HashMap<String, String>) -> DFResult<Option<u32>> {
    args.get("max_results")
        .map(|value| {
            value
                .trim()
                .parse()
                .map_err(|_| DataFusionError::Plan(format!("Invalid max_results: '{value}'")))
        })
        .transpose()
}

/// `resource_type` plus whichever locators it needs (Java `BasePermissionProcedure.resource`).
fn permission_resource(args: &HashMap<String, String>) -> DFResult<PermissionResource> {
    PermissionResource::new(
        enum_arg::<ResourceType>(args, "resource_type")?,
        opt_arg(args, "database"),
        opt_arg(args, "table"),
        opt_arg(args, "function"),
        opt_arg(args, "view"),
    )
    .map_err(to_datafusion_error)
}

/// The `TABLE` resource a policy hangs off (Java `BasePolicyProcedure.tableResource`).
fn policy_table_resource(args: &HashMap<String, String>) -> DFResult<PermissionResource> {
    PermissionResource::new(
        ResourceType::Table,
        Some(require_arg(args, "database")?),
        Some(require_arg(args, "table")?),
        None,
        None,
    )
    .map_err(to_datafusion_error)
}

/// Java hands both lists to `PermissionColumns` and lets it reject having both; each Rust
/// constructor takes one list, so that case is rejected here instead.
fn permission_columns(args: &HashMap<String, String>) -> DFResult<Option<PermissionColumns>> {
    match (
        comma_list(args, "column_names"),
        comma_list(args, "excluded_column_names"),
    ) {
        (None, None) => Ok(None),
        (Some(names), None) => PermissionColumns::names(names)
            .map(Some)
            .map_err(to_datafusion_error),
        (None, Some(excluded)) => PermissionColumns::excluded(excluded)
            .map(Some)
            .map_err(to_datafusion_error),
        (Some(_), Some(_)) => Err(DataFusionError::Plan(
            "columns must contain exactly one of column_names or excluded_column_names."
                .to_string(),
        )),
    }
}

fn utf8_result(
    ctx: &SessionContext,
    fields: &[(&str, bool)],
    rows: Vec<Vec<Option<String>>>,
) -> DFResult<DataFrame> {
    debug_assert!(
        rows.iter().all(|row| row.len() == fields.len()),
        "every row must have one cell per declared column"
    );
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, nullable)| Field::new(*name, ArrowDataType::Utf8, *nullable))
            .collect::<Vec<_>>(),
    ));
    let columns = (0..fields.len())
        .map(|column| {
            Arc::new(
                rows.iter()
                    .map(|row| row[column].clone())
                    .collect::<StringArray>(),
            ) as ArrayRef
        })
        .collect::<Vec<_>>();
    ctx.read_batch(RecordBatch::try_new(schema, columns)?)
}

fn joined(values: Option<&[String]>) -> Option<String> {
    values.map(|values| values.join(","))
}

async fn proc_grant_permission(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let rest = rest_catalog(catalog, catalog_name)?;
    let assignment = PermissionAssignment::new(
        permission_resource(args)?,
        require_arg(args, "access")?,
        require_arg(args, "principal")?,
        permission_columns(args)?,
        opt_arg(args, "expire_time"),
    )
    .map_err(to_datafusion_error)?;
    rest.grant_permission(&assignment)
        .await
        .map_err(to_datafusion_error)?;
    // Java returns boolean `true`; every write procedure here answers `ok_result` instead.
    ok_result(ctx)
}

async fn proc_revoke_permission(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let rest = rest_catalog(catalog, catalog_name)?;
    rest.revoke_permission(
        &permission_resource(args)?,
        require_arg(args, "access")?,
        require_arg(args, "principal")?,
    )
    .await
    .map_err(to_datafusion_error)?;
    ok_result(ctx)
}

async fn proc_list_permissions(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let rest = rest_catalog(catalog, catalog_name)?;
    let request = ListPermissionsRequest {
        resource: permission_resource(args)?,
        principal: opt_arg(args, "principal").map(str::to_string),
        access: opt_arg(args, "access").map(str::to_string),
        max_results: max_results_arg(args)?,
        page_token: opt_arg(args, "page_token").map(str::to_string),
    };
    let page = rest
        .list_permissions_paged(&request)
        .await
        .map_err(to_datafusion_error)?;

    // Java parity, quirk included: `next_page_token` repeats on every row, so an empty page
    // returns zero rows and loses it. See `ListPermissionsProcedure.call`.
    let rows = page
        .elements
        .iter()
        .map(|assignment| {
            let resource = assignment.resource();
            let columns = assignment.columns();
            vec![
                Some(resource.resource_type().to_string()),
                resource.database_name().map(str::to_string),
                resource.table_name().map(str::to_string),
                resource.function_name().map(str::to_string),
                resource.view_name().map(str::to_string),
                Some(assignment.access().to_string()),
                Some(assignment.principal().to_string()),
                // Comma-joined, matching how `column_names` is passed in.
                joined(columns.and_then(PermissionColumns::column_names)),
                joined(columns.and_then(PermissionColumns::excluded_column_names)),
                assignment.expire_time().map(str::to_string),
                page.next_page_token.clone(),
            ]
        })
        .collect();

    utf8_result(
        ctx,
        &[
            ("resource_type", false),
            ("database", true),
            ("table", true),
            ("function", true),
            ("view", true),
            ("access", false),
            ("principal", false),
            ("column_names", true),
            ("excluded_column_names", true),
            ("expire_time", true),
            ("next_page_token", true),
        ],
        rows,
    )
}

async fn proc_create_policy(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let rest = rest_catalog(catalog, catalog_name)?;
    let resource = policy_table_resource(args)?;
    let principal = require_arg(args, "principal")?;
    let predicate = opt_arg(args, "predicate_json");
    let on_column = opt_arg(args, "on_column");
    let transform = opt_arg(args, "transform_json");

    // Java `BasePolicyProcedure.policy`: each policy type rejects the other's fields.
    let policy = match enum_arg::<PolicyType>(args, "policy_type")? {
        PolicyType::RowFilter => {
            for (value, name) in [(on_column, "on_column"), (transform, "transform_json")] {
                if value.is_some() {
                    return Err(DataFusionError::Plan(format!(
                        "ROW_FILTER policy cannot specify {name}."
                    )));
                }
            }
            let row_filter =
                RowFilter::new(predicate.unwrap_or_default()).map_err(to_datafusion_error)?;
            DataPolicy::new_row_filter(resource, row_filter, principal)
        }
        PolicyType::ColumnMasking => {
            if predicate.is_some() {
                return Err(DataFusionError::Plan(
                    "COLUMN_MASKING policy cannot specify predicate_json.".to_string(),
                ));
            }
            let column_mask =
                ColumnMask::new(on_column.unwrap_or_default(), transform.unwrap_or_default())
                    .map_err(to_datafusion_error)?;
            DataPolicy::new_column_mask(resource, column_mask, principal)
        }
    }
    .map_err(to_datafusion_error)?;

    rest.create_policy(&policy)
        .await
        .map_err(to_datafusion_error)?;
    ok_result(ctx)
}

async fn proc_drop_policy(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let rest = rest_catalog(catalog, catalog_name)?;
    rest.drop_policy(
        &policy_table_resource(args)?,
        enum_arg::<PolicyType>(args, "policy_type")?,
        require_arg(args, "principal")?,
        opt_arg(args, "column"),
        bool_arg(args, "if_exists")?,
    )
    .await
    .map_err(to_datafusion_error)?;
    ok_result(ctx)
}

async fn proc_list_policies(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let rest = rest_catalog(catalog, catalog_name)?;
    let request = ListPoliciesRequest {
        resource: policy_table_resource(args)?,
        policy_type: opt_enum_arg(args, "policy_type")?,
        principal: opt_arg(args, "principal").map(str::to_string),
        column: opt_arg(args, "column").map(str::to_string),
        max_results: max_results_arg(args)?,
        page_token: opt_arg(args, "page_token").map(str::to_string),
    };
    let page = rest
        .list_policies_paged(&request)
        .await
        .map_err(to_datafusion_error)?;

    // Same Java pagination quirk as `list_permissions`.
    let rows = page
        .elements
        .iter()
        .map(|policy| {
            let resource = policy.resource();
            let column_mask = policy.column_mask();
            vec![
                resource.database_name().map(str::to_string),
                resource.table_name().map(str::to_string),
                Some(policy.policy_type().to_string()),
                Some(policy.principal().to_string()),
                policy
                    .row_filter()
                    .map(|filter| filter.predicate().to_string()),
                column_mask.map(|mask| mask.on_column().to_string()),
                column_mask.map(|mask| mask.transform().to_string()),
                page.next_page_token.clone(),
            ]
        })
        .collect();

    utf8_result(
        ctx,
        &[
            ("database", false),
            ("table", false),
            ("policy_type", false),
            ("principal", false),
            ("predicate_json", true),
            ("on_column", true),
            ("transform_json", true),
            ("next_page_token", true),
        ],
        rows,
    )
}

fn ok_result(ctx: &SessionContext) -> DFResult<DataFrame> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "result",
        ArrowDataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["OK"]))],
    )?;
    ctx.read_batch(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use paimon::io::FileIOBuilder;
    use paimon::spec::CommitKind;

    #[test]
    fn test_global_index_partition_values_are_typed_and_strict() {
        use paimon::spec::{
            CharType, DateType, DecimalType, FloatType, IntType, TimeType, TimestampType,
            VarCharType,
        };
        assert_eq!(
            parse_global_index_partition_value("01", &DataType::Int(IntType::new()), true),
            Some(Datum::Int(1))
        );
        for text in ["1.5", "2147483648", "bad"] {
            assert!(
                parse_global_index_partition_value(text, &DataType::Int(IntType::new()), true)
                    .is_none()
            );
        }
        assert!(parse_global_index_partition_value(
            "2026-02-30",
            &DataType::Date(DateType::new()),
            true
        )
        .is_none());
        assert_eq!(
            parse_global_index_partition_value(
                "1970-01-02",
                &DataType::Date(DateType::new()),
                true
            ),
            Some(Datum::Date(1))
        );
        assert_eq!(
            parse_global_index_partition_value(
                "a=b",
                &DataType::VarChar(VarCharType::new(3).unwrap()),
                true,
            ),
            Some(Datum::String("a=b".into()))
        );
        assert!(parse_global_index_partition_value(
            "abcd",
            &DataType::Char(CharType::new(3).unwrap()),
            true,
        )
        .is_none());
        assert_eq!(
            parse_global_index_partition_value(
                "12:00:00.123",
                &DataType::Time(TimeType::new(3).unwrap()),
                true,
            ),
            Some(Datum::Time(43_200_123))
        );
        let decimal = DataType::Decimal(DecimalType::new(4, 2).unwrap());
        for (text, unscaled) in [("1.235", 124), ("-1.235", -124), ("1.235e1", 1235)] {
            assert_eq!(
                parse_global_index_partition_value(text, &decimal, true),
                Some(Datum::Decimal {
                    unscaled,
                    precision: 4,
                    scale: 2
                })
            );
        }
        for text in ["99.995", "bad", "1_2.34"] {
            assert!(parse_global_index_partition_value(text, &decimal, true).is_none());
        }
        let timestamp = DataType::Timestamp(TimestampType::new(1).unwrap());
        assert_eq!(
            parse_global_index_partition_value("1970-01-01 00:00:00.199", &timestamp, true),
            Some(Datum::Timestamp {
                millis: 199,
                nanos: 0
            })
        );
        assert!(parse_global_index_partition_value("123", &timestamp, true).is_none());
        assert!(parse_global_index_partition_value("bad", &timestamp, true).is_none());
        let float = DataType::Float(FloatType::new());
        assert_eq!(
            parse_global_index_partition_value("1.1", &float, true),
            Some(Datum::Float(1.1))
        );
        for text in ["1e100", "1e-100", "1.234567891"] {
            assert!(parse_global_index_partition_value(text, &float, true).is_none());
        }
    }

    #[test]
    fn test_global_index_timestamp_partition_values_preserve_identity() {
        use paimon::spec::{LocalZonedTimestampType, TimestampType};

        for precision in 0..=9 {
            for zoned in [false, true] {
                let data_type = if zoned {
                    DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(precision).unwrap())
                } else {
                    DataType::Timestamp(TimestampType::new(precision).unwrap())
                };
                for (text, millis, nanos) in [
                    ("1970-01-01T00:00:00.123456789Z", 123, 456_789),
                    ("1969-12-31T23:59:59.999999999Z", -1, 999_999),
                    ("2500-01-01T00:00:00Z", 16_725_225_600_000, 0),
                ] {
                    let expected = if zoned {
                        Datum::LocalZonedTimestamp { millis, nanos }
                    } else {
                        Datum::Timestamp { millis, nanos }
                    };
                    assert_eq!(
                        parse_global_index_partition_value(text, &data_type, true),
                        Some(expected),
                        "{data_type:?}: {text}"
                    );
                }
                assert!(parse_global_index_partition_value(
                    "1970-01-01T00:00:00.1234567891Z",
                    &data_type,
                    true,
                )
                .is_none());
            }
        }
    }

    fn test_file_io() -> paimon::io::FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn test_snapshot(id: i64, time_millis: u64) -> Snapshot {
        Snapshot::builder()
            .version(3)
            .id(id)
            .schema_id(0)
            .base_manifest_list("base-list".to_string())
            .delta_manifest_list("delta-list".to_string())
            .commit_user("test-user".to_string())
            .commit_identifier(0)
            .commit_kind(CommitKind::APPEND)
            .time_millis(time_millis)
            .build()
    }

    async fn setup(table_path: &str) -> (paimon::io::FileIO, SnapshotManager, TagManager) {
        let file_io = test_file_io();
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io.mkdirs(&format!("{table_path}/tag/")).await.unwrap();
        let sm = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let tm = TagManager::new(file_io.clone(), table_path.to_string());
        (file_io, sm, tm)
    }

    #[tokio::test]
    async fn test_resolve_snapshot_by_id_live() {
        let (_, sm, tm) = setup("memory:/test_resolve_live").await;
        let snap = test_snapshot(1, 1000);
        sm.commit_snapshot(&snap).await.unwrap();

        let result = resolve_snapshot_by_id(&sm, &tm, 1).await.unwrap();
        assert_eq!(result.id(), 1);
    }

    #[tokio::test]
    async fn test_resolve_snapshot_by_id_tag_fallback() {
        let (_, sm, tm) = setup("memory:/test_resolve_tag").await;
        let snap = test_snapshot(1, 1000);
        tm.create("v1", &snap).await.unwrap();

        let result = resolve_snapshot_by_id(&sm, &tm, 1).await.unwrap();
        assert_eq!(result.id(), 1);
    }

    #[tokio::test]
    async fn test_resolve_snapshot_by_id_not_found() {
        let (_, sm, tm) = setup("memory:/test_resolve_none").await;
        let result = resolve_snapshot_by_id(&sm, &tm, 99).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_later_or_equal_exact_live() {
        let (_, sm, tm) = setup("memory:/test_later_exact").await;
        sm.commit_snapshot(&test_snapshot(1, 1000)).await.unwrap();
        sm.commit_snapshot(&test_snapshot(2, 2000)).await.unwrap();

        let result = later_or_equal_from_all(&sm, &tm, 1000).await.unwrap();
        assert_eq!(result.unwrap().id(), 1);
    }

    #[tokio::test]
    async fn test_later_or_equal_tag_better() {
        let (_, sm, tm) = setup("memory:/test_later_tag_better").await;
        sm.commit_snapshot(&test_snapshot(3, 3000)).await.unwrap();
        tm.create("v2", &test_snapshot(2, 2000)).await.unwrap();

        let result = later_or_equal_from_all(&sm, &tm, 1500).await.unwrap();
        assert_eq!(result.unwrap().id(), 2);
    }

    #[tokio::test]
    async fn test_later_or_equal_only_tag() {
        let (_, sm, tm) = setup("memory:/test_later_only_tag").await;
        tm.create("v1", &test_snapshot(1, 1000)).await.unwrap();

        let result = later_or_equal_from_all(&sm, &tm, 500).await.unwrap();
        assert_eq!(result.unwrap().id(), 1);
    }

    #[tokio::test]
    async fn test_earlier_or_equal_exact_live() {
        let (_, sm, tm) = setup("memory:/test_earlier_exact").await;
        sm.commit_snapshot(&test_snapshot(1, 1000)).await.unwrap();
        sm.commit_snapshot(&test_snapshot(2, 2000)).await.unwrap();

        let result = earlier_or_equal_from_all(&sm, &tm, 2000).await.unwrap();
        assert_eq!(result.unwrap().id(), 2);
    }

    #[tokio::test]
    async fn test_earlier_or_equal_tag_better() {
        let (_, sm, tm) = setup("memory:/test_earlier_tag_better").await;
        sm.commit_snapshot(&test_snapshot(1, 1000)).await.unwrap();
        tm.create("v2", &test_snapshot(2, 2000)).await.unwrap();

        let result = earlier_or_equal_from_all(&sm, &tm, 2500).await.unwrap();
        assert_eq!(result.unwrap().id(), 2);
    }

    #[tokio::test]
    async fn test_earlier_or_equal_only_tag() {
        let (_, sm, tm) = setup("memory:/test_earlier_only_tag").await;
        tm.create("v1", &test_snapshot(1, 1000)).await.unwrap();

        let result = earlier_or_equal_from_all(&sm, &tm, 1500).await.unwrap();
        assert_eq!(result.unwrap().id(), 1);
    }

    #[test]
    fn test_normalize_index_type() {
        // Casing and surrounding whitespace are both absorbed, matching Java's
        // `indexType.toLowerCase(Locale.ROOT).trim()`.
        assert_eq!(normalize_index_type("BTREE"), "btree");
        assert_eq!(normalize_index_type(" btree "), "btree");
        assert_eq!(normalize_index_type(" BitMap\t"), "bitmap");
        assert_eq!(normalize_index_type("IVF-FLAT"), "ivf-flat");
        assert_eq!(normalize_index_type("Ivf-Pq"), "ivf-pq");
        assert_eq!(
            normalize_index_type("Lumina-Vector-Ann"),
            "lumina-vector-ann"
        );
        // Already canonical values are returned unchanged, and an unknown type
        // is normalized but not rewritten -- the caller still rejects it.
        assert_eq!(normalize_index_type("ivf-flat"), "ivf-flat");
        assert_eq!(normalize_index_type(" Full-Text "), "full-text");
    }

    #[test]
    fn test_reject_unknown_args_covers_tag_and_index_procedures() {
        // A typo'd optional arg on a tag/index procedure used to be silently dropped:
        // `create_tag(..., snapshotid => 5)` tagged the latest snapshot, not snapshot 5.
        let typo = HashMap::from([
            ("table".to_string(), "db.t".to_string()),
            ("snapshotid".to_string(), "5".to_string()),
        ]);
        let err = reject_unknown_args("create_tag", &typo).unwrap_err();
        assert!(err.to_string().contains("snapshotid"), "{err}");

        // The correctly spelled argument is accepted.
        let ok = HashMap::from([
            ("table".to_string(), "db.t".to_string()),
            ("snapshot_id".to_string(), "5".to_string()),
        ]);
        reject_unknown_args("create_tag", &ok).unwrap();

        // `create_global_index` index_type typo is rejected (was: silent default btree).
        let idx_typo = HashMap::from([
            ("table".to_string(), "db.t".to_string()),
            ("index_column".to_string(), "id".to_string()),
            ("index_typ".to_string(), "bitmap".to_string()),
        ]);
        assert!(reject_unknown_args("create_global_index", &idx_typo).is_err());

        // A management procedure is still checked, unchanged.
        let mgmt_typo = HashMap::from([("resourcetype".to_string(), "TABLE".to_string())]);
        assert!(reject_unknown_args("grant_permission", &mgmt_typo).is_err());

        // A name with no declared parameter set is left unchecked -- no false positive.
        let other = HashMap::from([("anything".to_string(), "x".to_string())]);
        reject_unknown_args("not_a_procedure", &other).unwrap();
    }
}
