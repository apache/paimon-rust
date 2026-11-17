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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::error::Result;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{Expr, ExprSchemable};
use datafusion::object_store::path::Path;
use datafusion::sql::sqlparser::{ast::Statement, dialect::GenericDialect, parser::Parser};
use glob::Pattern;

use super::error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Format {
    Csv,
    Parquet,
    Json,
    Text,
    Binary,
}

impl Format {
    fn parse(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "csv" => Ok(Self::Csv),
            "parquet" => Ok(Self::Parquet),
            "json" => Ok(Self::Json),
            "text" => Ok(Self::Text),
            "binaryfile" => Ok(Self::Binary),
            _ => Err(error(format!(
                "unsupported format '{value}'; expected csv, parquet, json, text, or binaryFile"
            ))),
        }
    }

    pub(super) fn infer(path: &Path) -> Result<Self> {
        let name = path.filename().unwrap_or("");
        match name
            .rsplit('.')
            .next()
            .unwrap_or("")
            .to_ascii_lowercase()
            .as_str()
        {
            "csv" => Ok(Self::Csv),
            "parquet" => Ok(Self::Parquet),
            "json" | "jsonl" | "ndjson" => Ok(Self::Json),
            "text" | "txt" => Ok(Self::Text),
            _ => Err(error("cannot infer file format; specify format explicitly")),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Options {
    pub path: String,
    pub format: Option<Format>,
    pub header: Option<bool>,
    pub delimiter: Option<u8>,
    pub quote: Option<u8>,
    pub escape: Option<u8>,
    pub multi_line: bool,
    pub whole_text: bool,
    pub line_sep: Option<String>,
    pub null_value: Option<String>,
    pub file_pattern: Option<Pattern>,
    schema: Option<String>,
    specified: Vec<String>,
}

fn string(expr: &Expr, key: &str) -> Result<String> {
    let Expr::Literal(value, _) = expr else {
        return Err(error(format!("{key} must be a constant expression")));
    };
    value
        .try_as_str()
        .flatten()
        .map(str::to_string)
        .ok_or_else(|| error(format!("{key} must be a non-null STRING")))
}

fn boolean(expr: &Expr, key: &str) -> Result<bool> {
    match expr {
        Expr::Literal(ScalarValue::Boolean(Some(v)), _) => Ok(*v),
        _ => match string(expr, key)?.to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(error(format!("{key} must be BOOLEAN"))),
        },
    }
}

fn character(expr: &Expr, key: &str) -> Result<u8> {
    let value = string(expr, key)?;
    if value.len() != 1 || !value.is_ascii() {
        return Err(error(format!("{key} must be one ASCII character")));
    }
    Ok(value.as_bytes()[0])
}

impl Options {
    pub(super) fn parse(args: &[Expr]) -> Result<Self> {
        let Some(path) = args.first() else {
            return Err(error("requires a path"));
        };
        if args.len() % 2 != 1 {
            return Err(error("options must be named"));
        }
        let mut values = HashMap::new();
        for pair in args[1..].as_chunks::<2>().0 {
            let name = string(&pair[0], "option name")?.to_ascii_lowercase();
            let name = match name.as_str() {
                "delimiter" => "sep",
                "filenamepattern" => "pathglobfilter",
                _ => &name,
            }
            .to_string();
            if values.insert(name.clone(), &pair[1]).is_some() {
                return Err(error(format!("duplicate option '{name}'")));
            }
        }
        let known = [
            "format",
            "schema",
            "header",
            "sep",
            "quote",
            "escape",
            "multiline",
            "wholetext",
            "linesep",
            "nullvalue",
            "pathglobfilter",
        ];
        for name in values.keys() {
            if !known.contains(&name.as_str()) {
                return Err(error(format!("unsupported option '{name}'")));
            }
        }
        let path = string(path, "path")?;
        if path.trim().is_empty() {
            return Err(error("path must not be empty"));
        }
        let text = |key: &str| values.get(key).map(|expr| string(expr, key)).transpose();
        let flag = |key: &str| values.get(key).map(|expr| boolean(expr, key)).transpose();
        let byte = |key: &str| values.get(key).map(|expr| character(expr, key)).transpose();
        Ok(Self {
            path,
            format: text("format")?.map(|v| Format::parse(&v)).transpose()?,
            schema: text("schema")?,
            header: flag("header")?,
            delimiter: byte("sep")?,
            quote: byte("quote")?,
            escape: Some(byte("escape")?.unwrap_or(b'\\')),
            multi_line: flag("multiline")?.unwrap_or(false),
            whole_text: flag("wholetext")?.unwrap_or(false),
            line_sep: text("linesep")?,
            null_value: text("nullvalue")?,
            file_pattern: text("pathglobfilter")?
                .map(|v| Pattern::new(&v).map_err(error))
                .transpose()?,
            specified: values.keys().cloned().collect(),
        })
    }

    pub(super) fn validate(&self, format: Format) -> Result<()> {
        for name in &self.specified {
            let valid = match name.as_str() {
                "header" | "sep" | "quote" | "escape" | "nullvalue" => format == Format::Csv,
                "multiline" => matches!(format, Format::Csv | Format::Json),
                "wholetext" | "linesep" => format == Format::Text,
                "schema" => !matches!(format, Format::Text | Format::Binary),
                _ => true,
            };
            if !valid {
                return Err(error(format!(
                    "option '{name}' is not valid for {format:?}"
                )));
            }
        }
        if self.line_sep.as_ref().is_some_and(String::is_empty) {
            return Err(error("lineSep must not be empty"));
        }
        Ok(())
    }

    pub(super) fn schema(&self, state: &SessionState) -> Result<Option<SchemaRef>> {
        let Some(schema) = &self.schema else {
            return Ok(None);
        };
        let statements = Parser::parse_sql(
            &GenericDialect,
            &format!("CREATE TABLE __read_files_schema ({schema})"),
        )?;
        let [Statement::CreateTable(table)] = statements.as_slice() else {
            return Err(error("schema must be a DDL column list"));
        };
        if table.columns.is_empty()
            || !table.constraints.is_empty()
            || table.query.is_some()
            || table.location.is_some()
        {
            return Err(error("schema must be a non-empty DDL column list"));
        }
        let fields = table
            .columns
            .iter()
            .map(|column| {
                if !column.options.is_empty() {
                    return Err(error("schema column options are not supported"));
                }
                let expr = state.create_logical_expr(
                    &format!("CAST(NULL AS {})", column.data_type),
                    &DFSchema::empty(),
                )?;
                Ok(Field::new(
                    &column.name.value,
                    expr.get_type(&DFSchema::empty())?,
                    true,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut names = std::collections::HashSet::new();
        if fields.iter().any(|field| !names.insert(field.name())) {
            return Err(error("schema has duplicate column names"));
        }
        Ok(Some(Arc::new(Schema::new(fields))))
    }
}
