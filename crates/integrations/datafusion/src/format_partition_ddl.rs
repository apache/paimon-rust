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

//! SHOW, ADD and DROP PARTITION for Format Tables with catalog-managed partitions.
//!
//! `SQLContext` parses these statements and dispatches them here. DROP PARTITION on a Paimon
//! table stays in `SQLContext`, where it is a snapshot commit.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, ObjectName, Partition as SqlPartition, Value as SqlValue,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use paimon::catalog::{Catalog, Identifier};
use paimon::spec::{CoreOptions, DataType as PaimonDataType};
use paimon::table::{
    format_partition_value, parse_format_partition_value, FormatTablePartitionPaths,
};

use crate::error::to_datafusion_error;
use crate::sql_context::{is_table_not_exist, ok_result, partition_assignment, SQLContext};

#[derive(Debug)]
pub(crate) struct ShowPartitionsStatement {
    table_name: ObjectName,
    partition_filter: Vec<SqlExpr>,
}

pub(crate) fn parse_show_partitions(sql: &str) -> DFResult<Option<ShowPartitionsStatement>> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .map_err(sql_parse_error)?;
    let significant = tokens
        .iter()
        .filter_map(|token| match &token.token {
            Token::Whitespace(_) => None,
            Token::Word(word) => Some(word.keyword),
            _ => Some(Keyword::NoKeyword),
        })
        .take(2)
        .collect::<Vec<_>>();
    if !matches!(significant.as_slice(), [Keyword::SHOW, Keyword::PARTITIONS]) {
        return Ok(None);
    }

    let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens);
    parser
        .expect_keyword_is(Keyword::SHOW)
        .map_err(sql_parse_error)?;
    parser
        .expect_keyword_is(Keyword::PARTITIONS)
        .map_err(sql_parse_error)?;
    let table_name = parser.parse_object_name(false).map_err(sql_parse_error)?;
    let partition_filter = if parser.parse_keyword(Keyword::PARTITION) {
        parser
            .expect_token(&Token::LParen)
            .map_err(sql_parse_error)?;
        let expressions = parser
            .parse_comma_separated(Parser::parse_expr)
            .map_err(sql_parse_error)?;
        parser
            .expect_token(&Token::RParen)
            .map_err(sql_parse_error)?;
        expressions
    } else {
        Vec::new()
    };
    let _ = parser.consume_token(&Token::SemiColon);
    if parser.peek_token().token != Token::EOF {
        return Err(DataFusionError::Plan(format!(
            "SQL parse error: unexpected token {} after SHOW PARTITIONS statement",
            parser.peek_token().token
        )));
    }
    Ok(Some(ShowPartitionsStatement {
        table_name,
        partition_filter,
    }))
}

fn sql_parse_error(error: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Plan(format!("SQL parse error: {error}"))
}

pub(crate) async fn execute_show_partitions(
    ctx: &SQLContext,
    show_partitions: &ShowPartitionsStatement,
    enable_ident_normalization: bool,
) -> DFResult<DataFrame> {
    SQLContext::ensure_partition_command_target(&show_partitions.table_name, "SHOW PARTITIONS")?;
    let (catalog, _catalog_name, identifier) =
        ctx.resolve_catalog_and_table(&show_partitions.table_name)?;
    let table = catalog
        .get_table(&identifier)
        .await
        .map_err(to_datafusion_error)?;
    ensure_catalog_managed_format_table(&table, "SHOW PARTITIONS")?;
    let filter = if show_partitions.partition_filter.is_empty() {
        None
    } else {
        let spec = parse_format_partition_spec(
            &show_partitions.partition_filter,
            &table,
            false,
            None,
            enable_ident_normalization,
        )?;
        Some(display_partition_values(&spec, &table)?)
    };

    let partition_paths = FormatTablePartitionPaths::new(
        table.schema().partition_keys().iter().cloned(),
        CoreOptions::new(table.schema().options()).format_table_partition_only_value_in_path(),
    );
    let mut names = Vec::new();
    for partition in catalog
        .list_partitions(&identifier)
        .await
        .map_err(to_datafusion_error)?
    {
        let values = display_partition_values(&partition.spec, &table)?;
        if !filter.as_ref().is_none_or(|filter| {
            filter
                .iter()
                .all(|(key, value)| values.get(key) == Some(value))
        }) {
            continue;
        }
        let display_spec = values
            .into_iter()
            .map(|(key, value)| (key, value.unwrap_or_else(|| "null".to_string())))
            .collect();
        let name = partition_paths
            .partition_name(&display_spec)
            .map_err(to_datafusion_error)?;
        names.push(name);
    }
    names.sort();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "partition",
        ArrowDataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))])?;
    ctx.ctx().read_batch(batch)
}

pub(crate) async fn execute_add_partitions(
    ctx: &SQLContext,
    catalog: &Arc<dyn Catalog>,
    identifier: &Identifier,
    partitions: &[SqlPartition],
    ignore_if_exists: bool,
    ignore_if_table_not_exists: bool,
    enable_ident_normalization: bool,
) -> DFResult<DataFrame> {
    let table = match catalog.get_table(identifier).await {
        Ok(table) => table,
        Err(error) if ignore_if_table_not_exists && is_table_not_exist(&error) => {
            return ok_result(ctx.ctx());
        }
        Err(error) => return Err(to_datafusion_error(error)),
    };
    ensure_catalog_managed_format_table(&table, "ALTER TABLE ADD PARTITION")?;
    if partitions.is_empty() {
        return Err(DataFusionError::Plan(
            "ADD PARTITION requires at least one partition specification".to_string(),
        ));
    }

    let core_options = CoreOptions::new(table.schema().options());
    let partition_paths = FormatTablePartitionPaths::new(
        table.schema().partition_keys().iter().cloned(),
        core_options.format_table_partition_only_value_in_path(),
    );
    let table_path = table.location();
    let mut specs = Vec::with_capacity(partitions.len());
    let mut directories = Vec::with_capacity(partitions.len());
    for partition in partitions {
        let expressions = match partition {
            SqlPartition::Partitions(expressions) => expressions,
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported ADD PARTITION specification: {other}"
                )))
            }
        };
        let spec = parse_format_partition_spec(
            expressions,
            &table,
            true,
            Some("ADD PARTITION"),
            enable_ident_normalization,
        )?;
        let relative_path = partition_paths
            .relative_path(&spec)
            .map_err(to_datafusion_error)?;
        directories.push(format!(
            "{}/{}",
            table_path.trim_end_matches('/'),
            relative_path
        ));
        specs.push(spec);
    }

    catalog
        .create_partitions(identifier, specs, ignore_if_exists)
        .await
        .map_err(to_datafusion_error)?;
    for directory in directories {
        table
            .file_io()
            .mkdirs(&directory)
            .await
            .map_err(to_datafusion_error)?;
    }
    ok_result(ctx.ctx())
}

/// Unregister catalog-managed partitions and then delete their directories.
///
/// A specification that fixes only some of the partition keys expands to every
/// registered partition it matches, the way Java Format Tables behave. The keys need
/// not be a leading prefix, so `hh = '10'` alone is a valid specification. One catalog
/// listing serves the whole statement, however many specifications it carries.
pub(crate) async fn drop_catalog_managed_partitions(
    ctx: &SQLContext,
    catalog: &Arc<dyn Catalog>,
    identifier: &Identifier,
    table: &paimon::Table,
    requests: &[(&[SqlExpr], bool)],
    enable_ident_normalization: bool,
) -> DFResult<DataFrame> {
    ensure_catalog_managed_format_table(table, "ALTER TABLE DROP PARTITION")?;
    let partition_key_count = table.schema().partition_keys().len();
    let mut requested = Vec::with_capacity(requests.len());
    for (expressions, ignore_if_not_exists) in requests {
        let spec = parse_format_partition_spec(
            expressions,
            table,
            false,
            Some("DROP PARTITION"),
            enable_ident_normalization,
        )?;
        requested.push((spec, *ignore_if_not_exists));
    }

    // Only a partial specification needs the whole registry; complete ones are looked up by
    // name, which keeps the common exact drop from reading every registration.
    let complete_specs = requested
        .iter()
        .filter(|(spec, _)| spec.len() == partition_key_count)
        .map(|(spec, _)| spec.clone())
        .collect::<Vec<_>>();
    let registered = if complete_specs.len() == requested.len() {
        catalog
            .list_partitions_by_names(identifier, complete_specs)
            .await
    } else {
        catalog.list_partitions(identifier).await
    }
    .map_err(to_datafusion_error)?;

    let core_options = CoreOptions::new(table.schema().options());
    let partition_paths = FormatTablePartitionPaths::new(
        table.schema().partition_keys().iter().cloned(),
        core_options.format_table_partition_only_value_in_path(),
    );
    let table_path = table.location().trim_end_matches('/');
    // Every directory is resolved before the first mutation, so a registration that cannot
    // be turned into a path fails the statement as a whole.
    let registered = registered
        .into_iter()
        .map(|partition| {
            let relative_path = partition_paths
                .relative_path(&partition.spec)
                .map_err(to_datafusion_error)?;
            let custom_located = has_custom_location(&partition);
            Ok((
                partition.spec,
                format!("{table_path}/{relative_path}"),
                custom_located,
            ))
        })
        .collect::<DFResult<Vec<_>>>()?;

    let mut selected: Vec<(HashMap<String, String>, String, bool)> = Vec::new();
    let mut selected_paths = HashSet::new();
    for (spec, ignore_if_not_exists) in &requested {
        // Values are compared as the catalog holds them. A request is spelled the way ADD
        // PARTITION writes it, while repair registers the directory spelling, so a partition
        // registered as `month=01` is not the partition `month = 1` names.
        let mut matched = false;
        for (registered_spec, path, custom_located) in &registered {
            if !spec
                .iter()
                .all(|(key, value)| registered_spec.get(key) == Some(value))
            {
                continue;
            }
            matched = true;
            if selected_paths.insert(path.clone()) {
                selected.push((registered_spec.clone(), path.clone(), *custom_located));
            }
        }
        // Only a complete specification names one partition, so only it can be
        // reported as missing. A partial one describes a set that is allowed to be
        // empty, which is how Java reads it too.
        if !matched && spec.len() == partition_key_count && !ignore_if_not_exists {
            return Err(DataFusionError::Plan(format!(
                "Partition {spec:?} does not exist in table {}",
                identifier.full_name()
            )));
        }
    }

    if selected.is_empty() {
        return ok_result(ctx.ctx());
    }

    catalog
        .drop_partitions(
            identifier,
            selected.iter().map(|(spec, _, _)| spec.clone()).collect(),
        )
        .await
        .map_err(to_datafusion_error)?;
    for (_, path, custom_located) in selected {
        // A partition registered at a location of its own keeps its data there, as in Java:
        // dropping it only unregisters it, and the table directory it does not use is left
        // alone.
        if custom_located {
            continue;
        }
        table
            .file_io()
            .delete_dir(&path)
            .await
            .map_err(to_datafusion_error)?;
    }
    ok_result(ctx.ctx())
}

/// Whether the catalog registered a partition at a location of its own rather than under the
/// table directory.
fn has_custom_location(partition: &paimon::spec::Partition) -> bool {
    partition
        .options
        .as_ref()
        .is_some_and(|options| options.contains_key("path"))
}

pub(crate) fn ensure_catalog_managed_format_table(
    table: &paimon::Table,
    operation: &str,
) -> DFResult<()> {
    if table.schema().partition_keys().is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{operation} requires a partitioned table, but {} is not partitioned",
            table.identifier().full_name()
        )));
    }
    if !table.has_catalog_managed_partitions() {
        return Err(DataFusionError::Plan(format!(
            "{operation} is supported only for catalog-managed partitions on an internal Format Table loaded from REST Catalog; table {} does not have this configuration",
            table.identifier().full_name()
        )));
    }
    Ok(())
}

/// `mutating_operation` names the statement when it changes partitions (ADD or DROP PARTITION),
/// which refuses a blank string for a string partition column.
fn parse_format_partition_spec(
    exprs: &[SqlExpr],
    table: &paimon::Table,
    require_complete: bool,
    mutating_operation: Option<&str>,
    enable_ident_normalization: bool,
) -> DFResult<HashMap<String, String>> {
    let fields = table
        .schema()
        .fields()
        .iter()
        .map(|field| (field.name(), field))
        .collect::<HashMap<_, _>>();
    let partition_keys = table.schema().partition_keys();
    let options = CoreOptions::new(table.schema().options());
    let mut spec = HashMap::with_capacity(exprs.len());

    for expr in exprs {
        let (column, literal) = partition_assignment(expr, enable_ident_normalization)?;
        if !partition_keys.contains(&column) {
            return Err(DataFusionError::Plan(format!(
                "Column '{column}' is not a partition column"
            )));
        }
        if spec.contains_key(&column) {
            return Err(DataFusionError::Plan(format!(
                "Duplicate partition column '{column}'"
            )));
        }
        let field = fields.get(column.as_str()).ok_or_else(|| {
            DataFusionError::Plan(format!("Column '{column}' not found in table schema"))
        })?;
        let data_type = field.data_type();
        let value = match partition_literal_to_string(literal)? {
            None => options.partition_default_name().to_string(),
            // The literal is read with the column type the way a registration or directory is,
            // which is how Java casts partition strings (`TypeUtils.castFromString`), and written
            // back in the spelling ADD PARTITION registers.
            Some(text) => {
                let text = match data_type {
                    PaimonDataType::Char(_) | PaimonDataType::VarChar(_) => {
                        // A blank string is written to the default partition, so a statement
                        // that changes partitions would address that partition instead. Java
                        // `PaimonFormatTable.requireNameablePartitionValues` refuses it too.
                        if let Some(operation) = mutating_operation {
                            if text.trim().is_empty() {
                                return Err(DataFusionError::Plan(format!(
                                    "{operation} does not support an empty or whitespace-only \
                                     string for partition column '{column}' of Format Table {}. \
                                     Such a value is written to the partition named {}, name it \
                                     directly to address it",
                                    table.identifier().full_name(),
                                    options.partition_default_name()
                                )));
                            }
                        }
                        text
                    }
                    _ => text.trim().to_string(),
                };
                parse_format_partition_value(&text, data_type)
                    .and_then(|datum| {
                        format_partition_value(
                            &datum,
                            data_type,
                            options.partition_default_name(),
                            options.legacy_partition_name(),
                        )
                    })
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "Cannot use {literal} as a value of partition column '{column}' \
                             with type {data_type:?}"
                        ))
                    })?
            }
        };
        spec.insert(column, value);
    }

    if require_complete {
        let missing = partition_keys
            .iter()
            .filter(|key| !spec.contains_key(key.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "Incomplete partition spec: missing keys [{}]",
                missing.join(", ")
            )));
        }
    }
    Ok(spec)
}

/// The string a SQL partition literal stands for, the way Spark hands partition values to Paimon:
/// a quoted or `DATE '...'` string as written, a boolean as `true` or `false`, and a number in its
/// canonical form, so `month = 01` and `month = 1` name the same partition. `None` is NULL.
fn partition_literal_to_string(expr: &SqlExpr) -> DFResult<Option<String>> {
    use datafusion::sql::sqlparser::ast::{DataType as SqlDataType, UnaryOperator};

    let unsupported =
        || DataFusionError::Plan(format!("Unsupported partition value expression: {expr}"));
    let (sign, value, signed) = match expr {
        SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => ("-", expr.as_ref(), true),
        SqlExpr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => ("", expr.as_ref(), true),
        other => ("", other, false),
    };
    match value {
        SqlExpr::Value(value) => match &value.value {
            SqlValue::Number(number, _) => {
                let canonical = number
                    .parse::<i128>()
                    .map_or_else(|_| number.clone(), |number| number.to_string());
                Ok(Some(format!("{sign}{canonical}")))
            }
            _ if signed => Err(unsupported()),
            SqlValue::Null => Ok(None),
            SqlValue::Boolean(value) => Ok(Some(value.to_string())),
            other => other
                .clone()
                .into_string()
                .map(Some)
                .ok_or_else(unsupported),
        },
        SqlExpr::TypedString(typed) if !signed && matches!(typed.data_type, SqlDataType::Date) => {
            typed
                .value
                .value
                .clone()
                .into_string()
                .map(Some)
                .ok_or_else(unsupported)
        }
        _ => Err(unsupported()),
    }
}

/// The values of a partition spec as SHOW PARTITIONS prints and filters them: read with the column
/// type, so `month=01` and `month=1` are the same partition, and `None` for the default partition.
fn display_partition_values(
    spec: &HashMap<String, String>,
    table: &paimon::Table,
) -> DFResult<HashMap<String, Option<String>>> {
    let options = CoreOptions::new(table.schema().options());
    let default_partition_name = options.partition_default_name();
    let fields = table.schema().partition_fields();
    spec.iter()
        .map(|(key, raw)| {
            if raw == default_partition_name {
                return Ok((key.clone(), None));
            }
            let data_type = fields
                .iter()
                .find(|field| field.name() == key)
                .map(|field| field.data_type())
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Invalid partition spec {spec:?} for table {}",
                        table.identifier().full_name()
                    ))
                })?;
            parse_format_partition_value(raw, data_type)
                .and_then(|datum| {
                    format_partition_value(&datum, data_type, default_partition_name, false)
                })
                .map(|value| (key.clone(), Some(value)))
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Invalid catalog partition value {raw:?} for column '{key}' with type \
                         {data_type:?}"
                    ))
                })
        })
        .collect()
}
