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
//! - `CALL sys.create_tag(table => '...', tag => '...', snapshot_id => ...)`
//! - `CALL sys.delete_tag(table => '...', tag => '...')`
//! - `CALL sys.rollback_to(table => '...', snapshot_id => ... | tag => '...')`
//! - `CALL sys.rollback_to_timestamp(table => '...', timestamp => ...)`
//! - `CALL sys.create_tag_from_timestamp(table => '...', tag => '...', timestamp => ...)`

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgOperator,
    FunctionArguments, ObjectName, Value as SqlValue,
};
use paimon::catalog::{Catalog, Identifier};
use paimon::table::{SnapshotManager, Table, TagManager};

use crate::error::to_datafusion_error;

pub async fn execute_call(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    func: &Function,
) -> DFResult<DataFrame> {
    let proc_name = extract_procedure_name(&func.name)?;
    let args = extract_named_args(&func.args)?;

    match proc_name.as_str() {
        "create_tag" => proc_create_tag(ctx, catalog, catalog_name, &args).await,
        "delete_tag" => proc_delete_tag(ctx, catalog, catalog_name, &args).await,
        "rollback_to" => proc_rollback_to(ctx, catalog, catalog_name, &args).await,
        "rollback_to_timestamp" => {
            proc_rollback_to_timestamp(ctx, catalog, catalog_name, &args).await
        }
        "create_tag_from_timestamp" => {
            proc_create_tag_from_timestamp(ctx, catalog, catalog_name, &args).await
        }
        _ => Err(DataFusionError::Plan(format!(
            "Unknown procedure: {proc_name}"
        ))),
    }
}

fn extract_procedure_name(name: &ObjectName) -> DFResult<String> {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident().map(|id| id.value.clone()))
        .collect();
    match parts.len() {
        1 => Ok(parts[0].clone()),
        2 => Ok(parts[1].clone()),
        _ => Err(DataFusionError::Plan(format!(
            "Invalid procedure name: {name}. Expected sys.procedure_name or procedure_name"
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
                map.insert(name.value.to_lowercase(), value);
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
    catalog
        .get_table(&identifier)
        .await
        .map_err(to_datafusion_error)
}

fn managers(table: &Table) -> (SnapshotManager, TagManager) {
    let sm = SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let tm = TagManager::new(table.file_io().clone(), table.location().to_string());
    (sm, tm)
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

    let (sm, tm) = managers(&table);
    if tm.tag_exists(tag_name).await.map_err(to_datafusion_error)? {
        return Err(DataFusionError::Plan(format!(
            "Tag '{tag_name}' already exists"
        )));
    }
    let snapshot = if let Some(id) = snapshot_id {
        sm.get_snapshot(id).await.map_err(to_datafusion_error)?
    } else {
        sm.get_latest_snapshot()
            .await
            .map_err(to_datafusion_error)?
            .ok_or_else(|| DataFusionError::Plan("No snapshots exist".to_string()))?
    };
    tm.create(tag_name, &snapshot)
        .await
        .map_err(to_datafusion_error)?;
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
        if !tm.tag_exists(tag_name).await.map_err(to_datafusion_error)? {
            return Err(DataFusionError::Plan(format!(
                "Tag '{tag_name}' does not exist"
            )));
        }
        tm.delete(tag_name).await.map_err(to_datafusion_error)?;
    }
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
                .rollback_to_snapshot(rest_env.identifier(), id)
                .await
                .map_err(to_datafusion_error)?;
        } else if let Some(tag_name) = args.get("tag") {
            rest_env
                .api()
                .rollback_to_tag(rest_env.identifier(), tag_name)
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
            sm.get_snapshot(id).await.map_err(to_datafusion_error)?;
            clean_larger_than(&sm, &tm, id).await?;
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
    let snapshot = sm
        .earlier_or_equal_time_millis(timestamp)
        .await
        .map_err(to_datafusion_error)?
        .ok_or_else(|| {
            DataFusionError::Plan(format!("No snapshot found with commit time <= {timestamp}"))
        })?;

    if let Some(rest_env) = table.rest_env() {
        rest_env
            .api()
            .rollback_to_snapshot(rest_env.identifier(), snapshot.id())
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
    let snapshot = sm
        .later_or_equal_time_millis(timestamp)
        .await
        .map_err(to_datafusion_error)?
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
