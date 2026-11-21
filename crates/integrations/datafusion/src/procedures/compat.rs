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
use std::sync::Arc;

use datafusion::arrow::array::{BooleanArray, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::lit;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::planner::object_name_to_table_reference;
use datafusion::sql::sqlparser::ast::{Ident, ObjectName};
use datafusion::sql::sqlparser::{
    dialect::GenericDialect,
    parser::Parser,
    tokenizer::{Token, Tokenizer, Whitespace},
};
use paimon::api::RestError;
use paimon::catalog::{Catalog, Function, Identifier};
use paimon::spec::DataField;
use paimon::table::{Instant, RESTEnv, Table};
use serde_json::Value;

use super::{
    parse_key_value_options, parse_partition_specs, require_arg, resolve_rollback_target,
    resolve_table_identifier, rollback_table, rollback_to_snapshot, to_datafusion_error,
};
use crate::full_text_search::FullTextSearchFunction;
use crate::sql_context::ok_result;
use crate::vector_search::VectorSearchFunction;
use crate::SQLContext;

async fn validate_write_target(
    sql_ctx: Option<&SQLContext>,
    catalog: &str,
    identifier: &Identifier,
    operation: &str,
) -> DFResult<()> {
    let name = ObjectName::from(vec![
        Ident::with_quote('"', catalog),
        Ident::with_quote('"', identifier.database()),
        Ident::with_quote('"', identifier.object()),
    ]);
    SQLContext::ensure_main_branch_base_table(&name, operation)?;
    if let Some(sql_ctx) = sql_ctx {
        sql_ctx.ensure_no_time_travel_for_write(operation)?;
        sql_ctx
            .reject_temporary_write_target(&name, operation)
            .await?;
    }
    Ok(())
}

pub(super) async fn rollback(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
    sql_ctx: Option<&SQLContext>,
) -> DFResult<DataFrame> {
    let version = args.get("version").filter(|value| !value.trim().is_empty());
    let target = match (version, args.get("snapshot"), args.get("tag")) {
        (Some(version), None, None) if version.bytes().all(|byte| byte.is_ascii_digit()) => {
            Instant::snapshot(parse_snapshot_id(version)?)
        }
        (Some(tag), None, None) | (None, None, Some(tag)) if !tag.trim().is_empty() => {
            Instant::tag(tag)
        }
        (None, Some(id), None) => Instant::snapshot(parse_snapshot_id(id)?),
        _ => {
            return Err(DataFusionError::Plan(
                "rollback requires exactly one of 'version', 'snapshot', or 'tag'".to_string(),
            ))
        }
    };

    let identifier = resolve_table_identifier(require_arg(args, "table")?, catalog_name)?;
    validate_write_target(sql_ctx, catalog_name, &identifier, "CALL rollback").await?;
    let table = crate::table_loader::get_paimon_table(catalog, &identifier).await?;
    let snapshots = table.snapshot_manager();
    let previous = snapshots
        .get_latest_snapshot()
        .await
        .map_err(to_datafusion_error)?
        .ok_or_else(|| DataFusionError::Plan("Cannot roll back a table without a snapshot".into()))?
        .id();
    let current = match (table.rest_env(), &target) {
        (Some(rest), Instant::Tag { .. }) => {
            rollback_rest_tag(&table, rest, &target, previous).await?
        }
        (Some(_), Instant::Snapshot { snapshot_id }) => {
            rollback_table(&table, &target, Some(previous)).await?;
            *snapshot_id
        }
        (None, _) => {
            let snapshot = resolve_rollback_target(&table, &target).await?;
            rollback_to_snapshot(&snapshots, &table.tag_manager(), &snapshot).await?;
            snapshot.id()
        }
    };

    let schema = Arc::new(Schema::new(vec![
        Field::new("previous_snapshot_id", DataType::Int64, false),
        Field::new("current_snapshot_id", DataType::Int64, false),
    ]));
    ctx.read_batch(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![previous])),
            Arc::new(Int64Array::from(vec![current])),
        ],
    )?)
}

async fn rollback_rest_tag(
    table: &Table,
    rest: &RESTEnv,
    tag: &Instant,
    previous: i64,
) -> DFResult<i64> {
    let expected = resolve_rollback_target(table, tag).await?;
    let apply = |instant: Instant| async move {
        rest.api()
            .rollback_to(
                rest.identifier(),
                &instant,
                Some(previous),
                Some(rest.uuid()),
            )
            .await
    };
    match apply(Instant::snapshot(expected.id())).await {
        Ok(()) => return Ok(expected.id()),
        Err(paimon::Error::RestApi {
            source: RestError::NoSuchResource { resource_type, .. },
        }) if resource_type.as_deref() == Some("SNAPSHOT") => {}
        Err(error) => return Err(to_datafusion_error(error)),
    }

    // Some catalogs can restore a tag-retained snapshot only by name. The
    // REST protocol has no tag-target guard, so verify the outcome as well.
    if resolve_rollback_target(table, tag).await? != expected {
        return Err(DataFusionError::Execution(
            "Rollback tag changed after resolution. No rollback was applied.".into(),
        ));
    }
    apply(tag.clone()).await.map_err(to_datafusion_error)?;
    let latest = table.snapshot_manager().get_latest_snapshot().await.map_err(|error| {
        DataFusionError::Context(
            "REST rollback was accepted but its result could not be verified. Inspect the table head before retrying.".into(),
            Box::new(to_datafusion_error(error)),
        )
    })?;
    if latest.as_ref() != Some(&expected) {
        return Err(DataFusionError::Execution(format!(
            "REST rollback was accepted but could not verify expected snapshot {} (observed head {:?}). Inspect the table head before retrying.",
            expected.id(),
            latest.as_ref().map(|snapshot| snapshot.id()),
        )));
    }
    Ok(expected.id())
}

fn parse_snapshot_id(value: &str) -> DFResult<i64> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Invalid snapshot '{value}'. Expected a positive BIGINT"
            ))
        })
}

pub(super) async fn drop_partition(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
    positional_partitions: &[String],
    sql_ctx: Option<&SQLContext>,
) -> DFResult<DataFrame> {
    let identifier = resolve_table_identifier(require_arg(args, "table")?, catalog_name)?;
    let named_partitions = positional_partitions
        .is_empty()
        .then(|| require_arg(args, "partitions"))
        .transpose()?;
    validate_write_target(sql_ctx, catalog_name, &identifier, "CALL drop_partition").await?;
    let table = crate::table_loader::get_paimon_table(catalog, &identifier).await?;
    // Preserve literal whitespace so dropping a partition cannot target its trimmed value.
    let specs = match named_partitions {
        Some(specs) => parse_partition_specs(&table, specs.split(';'), true),
        None => parse_partition_specs(
            &table,
            positional_partitions.iter().map(String::as_str),
            true,
        ),
    }?
    .into_iter()
    .map(|spec| {
        if spec.len() != table.schema().partition_keys().len() {
            return Err(DataFusionError::Plan(
                "Incomplete partition spec. All partition columns must be specified.".into(),
            ));
        }
        Ok(spec
            .into_iter()
            .map(|(key, datum)| (key.to_string(), datum))
            .collect())
    })
    .collect::<DFResult<Vec<_>>>()?;
    table
        .new_write_builder()
        .try_new_commit()
        .map_err(to_datafusion_error)?
        .truncate_partitions(specs)
        .await
        .map_err(to_datafusion_error)?;
    ok_result(ctx)
}

fn function_identifier(
    ctx: &SessionContext,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<Identifier> {
    let name = require_arg(args, "function")?;
    let object_name = if name.contains(['"', '`']) {
        let dialect = GenericDialect {};
        let tokens = Tokenizer::new(&dialect, name).tokenize().map_err(|error| {
            DataFusionError::Plan(format!("Invalid function identifier: {error}"))
        })?;
        if tokens.iter().any(|token| {
            matches!(
                token,
                Token::Whitespace(
                    Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment(_)
                )
            )
        }) {
            return Err(DataFusionError::Plan(
                "Comments are not allowed in a quoted function identifier".into(),
            ));
        }
        let mut parser = Parser::new(&dialect).with_tokens(tokens);
        let object_name = parser.parse_object_name(false)?;
        parser.expect_token(&Token::EOF)?;
        object_name
    } else {
        // Keep exact literal names accepted before the quoted-name syntax.
        ObjectName::from(name.split('.').map(Ident::new).collect::<Vec<_>>())
    };
    let state = ctx.state();
    let options = state.config_options();
    // The argument is a string containing exact catalog names, as in the
    // other procedures. Quoting delimits a component without changing its case.
    let reference = object_name_to_table_reference(object_name, false)?;
    let resolved = reference.resolve(catalog_name, options.catalog.default_schema.as_str());
    if resolved.catalog.as_ref() != catalog_name
        || resolved.schema.is_empty()
        || resolved.table.is_empty()
    {
        return Err(DataFusionError::Plan(format!(
            "Invalid function identifier '{name}' for catalog '{catalog_name}'"
        )));
    }
    Ok(Identifier::new(
        resolved.schema.as_ref(),
        resolved.table.as_ref(),
    ))
}

pub(super) async fn create_function(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let identifier = function_identifier(ctx, catalog_name, args)?;
    let input = parse_function_parameters(require_arg(args, "inputparams")?, "inputParams")?;
    let output = parse_function_parameters(require_arg(args, "returnparams")?, "returnParams")?;
    let deterministic = args
        .get("deterministic")
        .map(|value| {
            value
                .parse::<bool>()
                .map_err(|_| DataFusionError::Plan("deterministic must be true or false".into()))
        })
        .transpose()?
        .unwrap_or(true);
    let options = args
        .get("options")
        .map(|value| parse_key_value_options(value))
        .transpose()?
        .unwrap_or_default();

    let function = Function::new(
        identifier,
        Some(input),
        Some(output),
        deterministic,
        HashMap::new(),
        args.get("comment").cloned(),
        options,
    );
    catalog
        .create_function(&function, false)
        .await
        .map_err(to_datafusion_error)?;
    function_result(ctx)
}

fn parse_function_parameters(json: &str, name: &str) -> DFResult<Vec<DataField>> {
    let invalid = |error| DataFusionError::Plan(format!("Invalid {name} fields: {error}"));
    let mut fields = serde_json::from_str::<Vec<Value>>(json).map_err(invalid)?;
    let assign_ids = fields
        .first()
        .is_some_and(|field| field.get("id").is_none());
    let mut next_id = 0;
    normalize_parameter_ids(&mut fields, assign_ids, &mut next_id, name)?;
    let mut ids = HashSet::new();
    let mut names = HashSet::new();
    fields
        .into_iter()
        .map(|field| {
            let field: DataField = serde_json::from_value(field).map_err(invalid)?;
            if !ids.insert(field.id()) || !names.insert(field.name().to_string()) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate field ID or name in {name}"
                )));
            }
            Ok(field)
        })
        .collect()
}

fn normalize_parameter_ids(
    fields: &mut [Value],
    assign_ids: bool,
    next_id: &mut usize,
    name: &str,
) -> DFResult<()> {
    for field in fields.iter_mut().filter_map(Value::as_object_mut) {
        if field.contains_key("id") == assign_ids {
            return Err(DataFusionError::Plan(format!(
                "{name} cannot mix explicit field IDs and omitted field IDs"
            )));
        }
        if assign_ids {
            field.insert("id".into(), (*next_id).into());
            *next_id += 1;
        }
        if let Some(data_type) = field.get_mut("type") {
            normalize_parameter_type_ids(data_type, assign_ids, next_id, name)?;
        }
    }
    Ok(())
}

fn normalize_parameter_type_ids(
    data_type: &mut Value,
    assign_ids: bool,
    next_id: &mut usize,
    name: &str,
) -> DFResult<()> {
    let Some(data_type) = data_type.as_object_mut() else {
        return Ok(());
    };
    if let Some(fields) = data_type.get_mut("fields").and_then(Value::as_array_mut) {
        normalize_parameter_ids(fields, assign_ids, next_id, name)?;
    }
    for key in ["element", "key", "value"] {
        if let Some(child) = data_type.get_mut(key) {
            normalize_parameter_type_ids(child, assign_ids, next_id, name)?;
        }
    }
    Ok(())
}

pub(super) async fn drop_function(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    args: &HashMap<String, String>,
) -> DFResult<DataFrame> {
    let identifier = function_identifier(ctx, catalog_name, args)?;
    catalog
        .drop_function(&identifier, false)
        .await
        .map_err(to_datafusion_error)?;
    function_result(ctx)
}

fn function_result(ctx: &SessionContext) -> DFResult<DataFrame> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Boolean,
        false,
    )]));
    ctx.read_batch(RecordBatch::try_new(
        schema,
        vec![Arc::new(BooleanArray::from(vec![true]))],
    )?)
}

pub(super) fn search(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    procedure: &str,
    args: &HashMap<String, String>,
    sql_ctx: Option<&SQLContext>,
) -> DFResult<DataFrame> {
    let table = require_arg(args, "table")?;
    let default_schema = ctx.state().config_options().catalog.default_schema.clone();
    // Bind through the selected catalog directly, including when this is a
    // qualified CALL and a different catalog registered the session's TVF.
    if table.contains('.') {
        resolve_table_identifier(table, catalog_name)?;
    }
    let top_k = require_arg(args, "top_k")?
        .parse::<i64>()
        .ok()
        .filter(|value| (1..=10_000).contains(value))
        .ok_or_else(|| DataFusionError::Plan("top_k must be an integer from 1 to 10000".into()))?;
    let (column, query) = if procedure == "vector_search" {
        let column = require_arg(args, "vector_column")?;
        let query = require_arg(args, "query_vector")?;
        (column, vector_json(query)?)
    } else {
        (
            require_arg(args, "column")?,
            require_arg(args, "query")?.to_string(),
        )
    };
    if table.trim().is_empty() || column.trim().is_empty() || query.trim().is_empty() {
        return Err(DataFusionError::Plan(
            "Search requires a non-blank table, column, and query".into(),
        ));
    }

    let parameters = [lit(table), lit(column), lit(query), lit(top_k)];
    let state = ctx.state();
    let call = TableFunctionArgs::new(&parameters, &state);
    let dynamic_options = sql_ctx
        .map(|context| context.dynamic_options().clone())
        .unwrap_or_default();
    let provider = if procedure == "vector_search" {
        VectorSearchFunction::new_with_dynamic_options(
            Arc::clone(catalog),
            &default_schema,
            dynamic_options,
        )
        .call_with_args(call)?
    } else {
        FullTextSearchFunction::new_with_dynamic_options(
            Arc::clone(catalog),
            &default_schema,
            dynamic_options,
        )
        .call_with_args(call)?
    };
    let df = ctx.read_table(provider)?;
    match args.get("projection").map(|value| value.trim()) {
        None | Some("" | "*") => Ok(df),
        Some(projection) => {
            df.select_columns(&projection.split(',').map(str::trim).collect::<Vec<_>>())
        }
    }
}

fn vector_json(query: &str) -> DFResult<String> {
    let query = query.trim();
    let invalid = || {
        DataFusionError::Plan(
        "query_vector must be a non-empty JSON array or comma-separated list of finite FLOAT values"
            .into(),
    )
    };
    let values: Vec<f32> = if query.starts_with('[') {
        serde_json::from_str(query).map_err(|_| invalid())?
    } else {
        query
            .split(',')
            .map(|value| value.trim().parse::<f32>())
            .collect::<Result<_, _>>()
            .map_err(|_| invalid())?
    };
    if values.is_empty() || values.iter().any(|value| !value.is_finite()) {
        return Err(invalid());
    }
    serde_json::to_string(&values).map_err(|_| invalid())
}
