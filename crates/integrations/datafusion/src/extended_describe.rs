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

use crate::SQLContext;
use datafusion::arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::common::{DataFusionError, Result};
use datafusion::prelude::DataFrame;
use datafusion::sql::sqlparser::{
    ast::{DescribeAlias, HiveDescribeFormat, ObjectName, Statement},
    dialect::GenericDialect,
    keywords::Keyword,
    parser::Parser,
    tokenizer::Token,
};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) fn parse_describe(sql: &str) -> Result<Option<Statement>> {
    let mut parser = Parser::new(&GenericDialect {})
        .try_with_sql(sql)
        .map_err(|e| DataFusionError::Plan(e.to_string()))?;
    if !(parser.parse_keyword(Keyword::DESCRIBE) || parser.parse_keyword(Keyword::DESC)) {
        return Ok(None);
    }
    let _ = parser.parse_keyword(Keyword::TABLE);
    let format = if parser.parse_keyword(Keyword::EXTENDED) {
        HiveDescribeFormat::Extended
    } else if parser.parse_keyword(Keyword::FORMATTED) {
        HiveDescribeFormat::Formatted
    } else {
        return Ok(None);
    };
    let _ = parser.parse_keyword(Keyword::TABLE);
    let table_name = parser
        .parse_object_name(false)
        .map_err(|e| DataFusionError::Plan(e.to_string()))?;
    let _ = parser.consume_token(&Token::SemiColon);
    if parser.peek_token().token != Token::EOF {
        return Err(DataFusionError::Plan(
            "Expected exactly one DESCRIBE statement".into(),
        ));
    }
    Ok(Some(Statement::ExplainTable {
        describe_alias: DescribeAlias::Describe,
        hive_format: Some(format),
        has_table_keyword: true,
        table_name,
    }))
}
pub(crate) async fn describe(ctx: &SQLContext, name: &ObjectName) -> Result<DataFrame> {
    let mut name = name.clone();
    for part in &mut name.0 {
        if let datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(ident) = part {
            ident.value = crate::sql_context::normalize_schema_identifier(
                ident,
                ctx.ctx().enable_ident_normalization(),
            );
        }
    }
    let native = format!("DESCRIBE {name}");
    if ctx.temp_table_for(&name).await?.is_some() {
        return ctx.ctx().sql(&native).await;
    }
    let Ok((catalog, catalog_name, id)) = ctx.resolve_catalog_and_table(&name) else {
        return ctx.ctx().sql(&native).await;
    };
    let table = match catalog.load_table(&id).await {
        Ok(paimon::catalog::LoadedTable::Paimon(table)) => table,
        Ok(_) => return ctx.ctx().sql(&native).await,
        Err(paimon::Error::TableNotExist { .. }) => return ctx.ctx().sql(&native).await,
        Err(error) => return Err(crate::to_datafusion_error(error)),
    };
    let mut names = Vec::new();
    let mut types = Vec::new();
    let mut comments = Vec::new();
    for field in table.schema().fields() {
        names.push(field.name().to_string());
        types.push(field.data_type().to_string());
        comments.push(field.description().unwrap_or("").to_string());
    }
    let properties = table
        .schema()
        .options()
        .iter()
        .map(|(key, value)| {
            let normalized = key.to_ascii_lowercase().replace('_', "-");
            let sensitive = ["access-key", "secret", "password", "token", "credential"]
                .iter()
                .any(|part| normalized.contains(part));
            (
                key.clone(),
                if sensitive {
                    "<redacted>".to_string()
                } else {
                    value.clone()
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let properties =
        serde_json::to_string(&properties).map_err(|e| DataFusionError::External(Box::new(e)))?;
    for (key, value) in [
        ("# Catalog", catalog_name),
        ("# Database", id.database().to_string()),
        (
            "# Table",
            id.table_name().map_err(crate::to_datafusion_error)?,
        ),
        ("# Type", "PAIMON".to_string()),
        ("# Schema ID", table.schema().id().to_string()),
        ("# Location", table.location().to_string()),
        (
            "# Comment",
            table.schema().comment().unwrap_or("").to_string(),
        ),
        (
            "# Partition Keys",
            table.schema().partition_keys().join(","),
        ),
        ("# Primary Keys", table.schema().primary_keys().join(",")),
        ("# Properties", properties),
    ] {
        names.push(key.to_string());
        types.push(value);
        comments.push(String::new());
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("comment", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(comments)),
        ],
    )?;
    ctx.ctx().read_batch(batch)
}
