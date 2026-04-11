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

//! DDL support for Paimon tables.
//!
//! DataFusion does not natively support all DDL statements needed by Paimon.
//! This module provides [`PaimonDdlHandler`] which intercepts CREATE TABLE and
//! ALTER TABLE SQL, translates them to Paimon catalog operations, and delegates
//! everything else (SELECT, CREATE/DROP SCHEMA, DROP TABLE, etc.) to the
//! underlying [`SessionContext`].
//!
//! Supported DDL:
//! - `CREATE TABLE db.t (col TYPE, ..., PRIMARY KEY (col, ...)) [PARTITIONED BY (col TYPE, ...)] [WITH ('key' = 'val')]`
//! - `ALTER TABLE db.t ADD COLUMN col TYPE`
//! - `ALTER TABLE db.t DROP COLUMN col`
//! - `ALTER TABLE db.t RENAME COLUMN old TO new`
//! - `ALTER TABLE db.t RENAME TO new_name`

use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    AlterTableOperation, ColumnDef, CreateTable, CreateTableOptions, HiveDistributionStyle,
    ObjectName, RenameTableNameKind, SqlOption, Statement,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use paimon::catalog::{Catalog, Identifier};
use paimon::spec::SchemaChange;

use crate::error::to_datafusion_error;
use paimon::arrow::arrow_to_paimon_type;

/// Wraps a [`SessionContext`] and a Paimon [`Catalog`] to handle DDL statements
/// that DataFusion does not natively support (e.g. ALTER TABLE).
///
/// For all other SQL, it delegates to the inner `SessionContext`.
///
/// # Example
/// ```ignore
/// let handler = PaimonDdlHandler::new(ctx, catalog);
/// let df = handler.sql("ALTER TABLE paimon.db.t ADD COLUMN age INT").await?;
/// ```
pub struct PaimonDdlHandler {
    ctx: SessionContext,
    catalog: Arc<dyn Catalog>,
    /// The catalog name registered in the SessionContext (used to strip the catalog prefix).
    catalog_name: String,
}

impl PaimonDdlHandler {
    pub fn new(
        ctx: SessionContext,
        catalog: Arc<dyn Catalog>,
        catalog_name: impl Into<String>,
    ) -> Self {
        Self {
            ctx,
            catalog,
            catalog_name: catalog_name.into(),
        }
    }

    /// Returns a reference to the inner [`SessionContext`].
    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    /// Execute a SQL statement. ALTER TABLE is handled by Paimon directly;
    /// everything else is delegated to DataFusion.
    pub async fn sql(&self, sql: &str) -> DFResult<DataFrame> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| DataFusionError::Plan(format!("SQL parse error: {e}")))?;

        if statements.len() != 1 {
            return Err(DataFusionError::Plan(
                "Expected exactly one SQL statement".to_string(),
            ));
        }

        match &statements[0] {
            Statement::CreateTable(create_table) => self.handle_create_table(create_table).await,
            Statement::AlterTable {
                name, operations, ..
            } => self.handle_alter_table(name, operations).await,
            _ => self.ctx.sql(sql).await,
        }
    }

    async fn handle_create_table(&self, ct: &CreateTable) -> DFResult<DataFrame> {
        if ct.location.is_some() {
            return Err(DataFusionError::Plan(
                "LOCATION is not supported for Paimon tables. Table path is determined by the catalog warehouse.".to_string(),
            ));
        }
        if ct.query.is_some() {
            return Err(DataFusionError::Plan(
                "CREATE TABLE AS SELECT is not yet supported for Paimon tables.".to_string(),
            ));
        }

        let identifier = self.resolve_table_name(&ct.name)?;

        let mut builder = paimon::spec::Schema::builder();

        // Columns
        for col in &ct.columns {
            let arrow_type = sql_data_type_to_arrow(&col.data_type)?;
            let nullable = !col.options.iter().any(|opt| {
                matches!(
                    opt.option,
                    datafusion::sql::sqlparser::ast::ColumnOption::NotNull
                )
            });
            let paimon_type =
                arrow_to_paimon_type(&arrow_type, nullable).map_err(to_datafusion_error)?;
            builder = builder.column(col.name.value.clone(), paimon_type);
        }

        // Primary key from constraints: PRIMARY KEY (col, ...)
        for constraint in &ct.constraints {
            if let datafusion::sql::sqlparser::ast::TableConstraint::PrimaryKey {
                columns, ..
            } = constraint
            {
                let pk_cols: Vec<String> =
                    columns.iter().map(|c| c.column.expr.to_string()).collect();
                builder = builder.primary_key(pk_cols);
            }
        }

        // Partition keys from PARTITIONED BY (col, ...)
        if let HiveDistributionStyle::PARTITIONED { columns } = &ct.hive_distribution {
            let partition_keys: Vec<String> =
                columns.iter().map(|c| c.name.value.clone()).collect();
            builder = builder.partition_keys(partition_keys);
        }

        // Table options from WITH ('key' = 'value', ...)
        for (k, v) in extract_options(&ct.table_options)? {
            builder = builder.option(k, v);
        }

        let schema = builder.build().map_err(to_datafusion_error)?;

        self.catalog
            .create_table(&identifier, schema, ct.if_not_exists)
            .await
            .map_err(to_datafusion_error)?;

        ok_result(&self.ctx)
    }

    async fn handle_alter_table(
        &self,
        name: &ObjectName,
        operations: &[AlterTableOperation],
    ) -> DFResult<DataFrame> {
        let identifier = self.resolve_table_name(name)?;

        let mut changes = Vec::new();
        let mut rename_to: Option<Identifier> = None;

        for op in operations {
            match op {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    let change = column_def_to_add_column(column_def)?;
                    changes.push(change);
                }
                AlterTableOperation::DropColumn {
                    column_names,
                    if_exists: _,
                    ..
                } => {
                    for col in column_names {
                        changes.push(SchemaChange::drop_column(col.value.clone()));
                    }
                }
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    changes.push(SchemaChange::rename_column(
                        old_column_name.value.clone(),
                        new_column_name.value.clone(),
                    ));
                }
                AlterTableOperation::RenameTable { table_name } => {
                    let new_name = match table_name {
                        RenameTableNameKind::To(name) | RenameTableNameKind::As(name) => {
                            object_name_to_string(name)
                        }
                    };
                    rename_to = Some(Identifier::new(identifier.database().to_string(), new_name));
                }
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "Unsupported ALTER TABLE operation: {other}"
                    )));
                }
            }
        }

        if let Some(new_identifier) = rename_to {
            self.catalog
                .rename_table(&identifier, &new_identifier, false)
                .await
                .map_err(to_datafusion_error)?;
        }

        if !changes.is_empty() {
            self.catalog
                .alter_table(&identifier, changes, false)
                .await
                .map_err(to_datafusion_error)?;
        }

        ok_result(&self.ctx)
    }

    /// Resolve an ObjectName like `paimon.db.table` or `db.table` to a Paimon Identifier.
    fn resolve_table_name(&self, name: &ObjectName) -> DFResult<Identifier> {
        let parts: Vec<String> = name
            .0
            .iter()
            .filter_map(|p| p.as_ident().map(|id| id.value.clone()))
            .collect();
        match parts.len() {
            3 => {
                // catalog.database.table — strip catalog prefix
                if parts[0] != self.catalog_name {
                    return Err(DataFusionError::Plan(format!(
                        "Unknown catalog '{}', expected '{}'",
                        parts[0], self.catalog_name
                    )));
                }
                Ok(Identifier::new(parts[1].clone(), parts[2].clone()))
            }
            2 => Ok(Identifier::new(parts[0].clone(), parts[1].clone())),
            1 => Err(DataFusionError::Plan(format!(
                "ALTER TABLE requires at least database.table, got: {}",
                parts[0]
            ))),
            _ => Err(DataFusionError::Plan(format!(
                "Invalid table reference: {name}"
            ))),
        }
    }
}

/// Convert a sqlparser [`ColumnDef`] to a Paimon [`SchemaChange::AddColumn`].
fn column_def_to_add_column(col: &ColumnDef) -> DFResult<SchemaChange> {
    let arrow_type = sql_data_type_to_arrow(&col.data_type)?;
    let nullable = !col.options.iter().any(|opt| {
        matches!(
            opt.option,
            datafusion::sql::sqlparser::ast::ColumnOption::NotNull
        )
    });
    let paimon_type = arrow_to_paimon_type(&arrow_type, nullable).map_err(to_datafusion_error)?;
    Ok(SchemaChange::add_column(
        col.name.value.clone(),
        paimon_type,
    ))
}

/// Minimal conversion from sqlparser SQL data types to Arrow data types.
fn sql_data_type_to_arrow(
    sql_type: &datafusion::sql::sqlparser::ast::DataType,
) -> DFResult<ArrowDataType> {
    use datafusion::sql::sqlparser::ast::DataType as SqlType;
    match sql_type {
        SqlType::Boolean => Ok(ArrowDataType::Boolean),
        SqlType::TinyInt(_) => Ok(ArrowDataType::Int8),
        SqlType::SmallInt(_) => Ok(ArrowDataType::Int16),
        SqlType::Int(_) | SqlType::Integer(_) => Ok(ArrowDataType::Int32),
        SqlType::BigInt(_) => Ok(ArrowDataType::Int64),
        SqlType::Float(_) => Ok(ArrowDataType::Float32),
        SqlType::Real => Ok(ArrowDataType::Float32),
        SqlType::Double(_) | SqlType::DoublePrecision => Ok(ArrowDataType::Float64),
        SqlType::Varchar(_) | SqlType::CharVarying(_) | SqlType::Text | SqlType::String(_) => {
            Ok(ArrowDataType::Utf8)
        }
        SqlType::Char(_) | SqlType::Character(_) => Ok(ArrowDataType::Utf8),
        SqlType::Binary(_) | SqlType::Varbinary(_) | SqlType::Blob(_) | SqlType::Bytea => {
            Ok(ArrowDataType::Binary)
        }
        SqlType::Date => Ok(ArrowDataType::Date32),
        SqlType::Timestamp(precision, tz_info) => {
            use datafusion::sql::sqlparser::ast::TimezoneInfo;
            let unit = match precision {
                Some(0) => datafusion::arrow::datatypes::TimeUnit::Second,
                Some(1..=3) | None => datafusion::arrow::datatypes::TimeUnit::Millisecond,
                Some(4..=6) => datafusion::arrow::datatypes::TimeUnit::Microsecond,
                _ => datafusion::arrow::datatypes::TimeUnit::Nanosecond,
            };
            let tz = match tz_info {
                TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => None,
                _ => Some("UTC".into()),
            };
            Ok(ArrowDataType::Timestamp(unit, tz))
        }
        SqlType::Decimal(info) => {
            use datafusion::sql::sqlparser::ast::ExactNumberInfo;
            let (p, s) = match info {
                ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as i8),
                ExactNumberInfo::Precision(p) => (*p as u8, 0),
                ExactNumberInfo::None => (10, 0),
            };
            Ok(ArrowDataType::Decimal128(p, s))
        }
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported SQL data type for ALTER TABLE: {sql_type}"
        ))),
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|p| p.as_ident().map(|id| id.value.clone()))
        .collect::<Vec<_>>()
        .join(".")
}

/// Extract key-value pairs from [`CreateTableOptions`].
fn extract_options(opts: &CreateTableOptions) -> DFResult<Vec<(String, String)>> {
    let sql_options = match opts {
        CreateTableOptions::With(options)
        | CreateTableOptions::Options(options)
        | CreateTableOptions::TableProperties(options)
        | CreateTableOptions::Plain(options) => options,
        CreateTableOptions::None => return Ok(Vec::new()),
    };
    sql_options
        .iter()
        .map(|opt| match opt {
            SqlOption::KeyValue { key, value } => {
                let v = value.to_string();
                // Strip surrounding quotes from the value if present.
                let v = v
                    .strip_prefix('\'')
                    .and_then(|s| s.strip_suffix('\''))
                    .unwrap_or(&v)
                    .to_string();
                Ok((key.value.clone(), v))
            }
            other => Err(DataFusionError::Plan(format!(
                "Unsupported table option: {other}"
            ))),
        })
        .collect()
}

/// Return an empty DataFrame with a single "result" column containing "OK".
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
    let df = ctx.read_batch(batch)?;
    Ok(df)
}
