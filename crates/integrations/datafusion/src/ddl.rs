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
            Statement::AlterTable(alter_table) => {
                self.handle_alter_table(
                    &alter_table.name,
                    &alter_table.operations,
                    alter_table.if_exists,
                )
                .await
            }
            _ => self.ctx.sql(sql).await,
        }
    }

    async fn handle_create_table(&self, ct: &CreateTable) -> DFResult<DataFrame> {
        if ct.external {
            return Err(DataFusionError::Plan(
                "CREATE EXTERNAL TABLE is not supported. Use CREATE TABLE instead.".to_string(),
            ));
        }
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
            if let datafusion::sql::sqlparser::ast::TableConstraint::PrimaryKey(pk) = constraint {
                let pk_cols: Vec<String> = pk
                    .columns
                    .iter()
                    .map(|c| c.column.expr.to_string())
                    .collect();
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
        if_exists: bool,
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
                .rename_table(&identifier, &new_identifier, if_exists)
                .await
                .map_err(to_datafusion_error)?;
        }

        if !changes.is_empty() {
            self.catalog
                .alter_table(&identifier, changes, if_exists)
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

/// Convert a sqlparser SQL data type to an Arrow data type.
fn sql_data_type_to_arrow(
    sql_type: &datafusion::sql::sqlparser::ast::DataType,
) -> DFResult<ArrowDataType> {
    use datafusion::sql::sqlparser::ast::{ArrayElemTypeDef, DataType as SqlType};
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
        SqlType::Array(elem_def) => {
            let elem_type = match elem_def {
                ArrayElemTypeDef::AngleBracket(t)
                | ArrayElemTypeDef::SquareBracket(t, _)
                | ArrayElemTypeDef::Parenthesis(t) => sql_data_type_to_arrow(t)?,
                ArrayElemTypeDef::None => {
                    return Err(DataFusionError::Plan(
                        "ARRAY type requires an element type".to_string(),
                    ));
                }
            };
            Ok(ArrowDataType::List(Arc::new(Field::new(
                "element", elem_type, true,
            ))))
        }
        SqlType::Map(key_type, value_type) => {
            let key = sql_data_type_to_arrow(key_type)?;
            let value = sql_data_type_to_arrow(value_type)?;
            let entries = Field::new(
                "entries",
                ArrowDataType::Struct(
                    vec![
                        Field::new("key", key, false),
                        Field::new("value", value, true),
                    ]
                    .into(),
                ),
                false,
            );
            Ok(ArrowDataType::Map(Arc::new(entries), false))
        }
        SqlType::Struct(fields, _) => {
            let arrow_fields: Vec<Field> = fields
                .iter()
                .map(|f| {
                    let name = f
                        .field_name
                        .as_ref()
                        .map(|n| n.value.clone())
                        .unwrap_or_default();
                    let dt = sql_data_type_to_arrow(&f.field_type)?;
                    Ok(Field::new(name, dt, true))
                })
                .collect::<DFResult<_>>()?;
            Ok(ArrowDataType::Struct(arrow_fields.into()))
        }
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported SQL data type: {sql_type}"
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::TimeUnit;
    use paimon::catalog::Database;
    use paimon::spec::Schema as PaimonSchema;
    use paimon::table::Table;

    // ==================== Mock Catalog ====================

    #[allow(clippy::enum_variant_names)]
    #[derive(Debug)]
    enum CatalogCall {
        CreateTable {
            identifier: Identifier,
            schema: PaimonSchema,
            ignore_if_exists: bool,
        },
        AlterTable {
            identifier: Identifier,
            changes: Vec<SchemaChange>,
            ignore_if_not_exists: bool,
        },
        RenameTable {
            from: Identifier,
            to: Identifier,
            ignore_if_not_exists: bool,
        },
    }

    struct MockCatalog {
        calls: Mutex<Vec<CatalogCall>>,
    }

    impl MockCatalog {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
            }
        }

        fn take_calls(&self) -> Vec<CatalogCall> {
            std::mem::take(&mut *self.calls.lock().unwrap())
        }
    }

    #[async_trait]
    impl Catalog for MockCatalog {
        async fn list_databases(&self) -> paimon::Result<Vec<String>> {
            Ok(vec![])
        }
        async fn create_database(
            &self,
            _name: &str,
            _ignore_if_exists: bool,
            _properties: HashMap<String, String>,
        ) -> paimon::Result<()> {
            Ok(())
        }
        async fn get_database(&self, _name: &str) -> paimon::Result<Database> {
            unimplemented!()
        }
        async fn drop_database(
            &self,
            _name: &str,
            _ignore_if_not_exists: bool,
            _cascade: bool,
        ) -> paimon::Result<()> {
            Ok(())
        }
        async fn get_table(&self, _identifier: &Identifier) -> paimon::Result<Table> {
            unimplemented!()
        }
        async fn list_tables(&self, _database_name: &str) -> paimon::Result<Vec<String>> {
            Ok(vec![])
        }
        async fn create_table(
            &self,
            identifier: &Identifier,
            creation: PaimonSchema,
            ignore_if_exists: bool,
        ) -> paimon::Result<()> {
            self.calls.lock().unwrap().push(CatalogCall::CreateTable {
                identifier: identifier.clone(),
                schema: creation,
                ignore_if_exists,
            });
            Ok(())
        }
        async fn drop_table(
            &self,
            _identifier: &Identifier,
            _ignore_if_not_exists: bool,
        ) -> paimon::Result<()> {
            Ok(())
        }
        async fn rename_table(
            &self,
            from: &Identifier,
            to: &Identifier,
            ignore_if_not_exists: bool,
        ) -> paimon::Result<()> {
            self.calls.lock().unwrap().push(CatalogCall::RenameTable {
                from: from.clone(),
                to: to.clone(),
                ignore_if_not_exists,
            });
            Ok(())
        }
        async fn alter_table(
            &self,
            identifier: &Identifier,
            changes: Vec<SchemaChange>,
            ignore_if_not_exists: bool,
        ) -> paimon::Result<()> {
            self.calls.lock().unwrap().push(CatalogCall::AlterTable {
                identifier: identifier.clone(),
                changes,
                ignore_if_not_exists,
            });
            Ok(())
        }
    }

    fn make_handler(catalog: Arc<MockCatalog>) -> PaimonDdlHandler {
        PaimonDdlHandler::new(SessionContext::new(), catalog, "paimon")
    }

    // ==================== sql_data_type_to_arrow tests ====================

    #[test]
    fn test_sql_type_boolean() {
        use datafusion::sql::sqlparser::ast::DataType as SqlType;
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::Boolean).unwrap(),
            ArrowDataType::Boolean
        );
    }

    #[test]
    fn test_sql_type_integers() {
        use datafusion::sql::sqlparser::ast::DataType as SqlType;
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::TinyInt(None)).unwrap(),
            ArrowDataType::Int8
        );
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::SmallInt(None)).unwrap(),
            ArrowDataType::Int16
        );
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::Int(None)).unwrap(),
            ArrowDataType::Int32
        );
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::Integer(None)).unwrap(),
            ArrowDataType::Int32
        );
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::BigInt(None)).unwrap(),
            ArrowDataType::Int64
        );
    }

    #[test]
    fn test_sql_type_floats() {
        use datafusion::sql::sqlparser::ast::{DataType as SqlType, ExactNumberInfo};
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::Float(ExactNumberInfo::None)).unwrap(),
            ArrowDataType::Float32
        );
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::Real).unwrap(),
            ArrowDataType::Float32
        );
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::DoublePrecision).unwrap(),
            ArrowDataType::Float64
        );
    }

    #[test]
    fn test_sql_type_string_variants() {
        use datafusion::sql::sqlparser::ast::DataType as SqlType;
        for sql_type in [SqlType::Varchar(None), SqlType::Text, SqlType::String(None)] {
            assert_eq!(
                sql_data_type_to_arrow(&sql_type).unwrap(),
                ArrowDataType::Utf8,
                "failed for {sql_type:?}"
            );
        }
    }

    #[test]
    fn test_sql_type_binary() {
        use datafusion::sql::sqlparser::ast::DataType as SqlType;
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::Bytea).unwrap(),
            ArrowDataType::Binary
        );
    }

    #[test]
    fn test_sql_type_date() {
        use datafusion::sql::sqlparser::ast::DataType as SqlType;
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::Date).unwrap(),
            ArrowDataType::Date32
        );
    }

    #[test]
    fn test_sql_type_timestamp_default() {
        use datafusion::sql::sqlparser::ast::{DataType as SqlType, TimezoneInfo};
        let result = sql_data_type_to_arrow(&SqlType::Timestamp(None, TimezoneInfo::None)).unwrap();
        assert_eq!(
            result,
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None)
        );
    }

    #[test]
    fn test_sql_type_timestamp_with_precision() {
        use datafusion::sql::sqlparser::ast::{DataType as SqlType, TimezoneInfo};
        // precision 0 => Second
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::Timestamp(Some(0), TimezoneInfo::None)).unwrap(),
            ArrowDataType::Timestamp(TimeUnit::Second, None)
        );
        // precision 3 => Millisecond
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::Timestamp(Some(3), TimezoneInfo::None)).unwrap(),
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None)
        );
        // precision 6 => Microsecond
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::Timestamp(Some(6), TimezoneInfo::None)).unwrap(),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
        );
        // precision 9 => Nanosecond
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::Timestamp(Some(9), TimezoneInfo::None)).unwrap(),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn test_sql_type_timestamp_with_tz() {
        use datafusion::sql::sqlparser::ast::{DataType as SqlType, TimezoneInfo};
        let result =
            sql_data_type_to_arrow(&SqlType::Timestamp(None, TimezoneInfo::WithTimeZone)).unwrap();
        assert_eq!(
            result,
            ArrowDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
    }

    #[test]
    fn test_sql_type_decimal() {
        use datafusion::sql::sqlparser::ast::{DataType as SqlType, ExactNumberInfo};
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::Decimal(ExactNumberInfo::PrecisionAndScale(18, 2)))
                .unwrap(),
            ArrowDataType::Decimal128(18, 2)
        );
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::Decimal(ExactNumberInfo::Precision(10))).unwrap(),
            ArrowDataType::Decimal128(10, 0)
        );
        assert_eq!(
            sql_data_type_to_arrow(&SqlType::Decimal(ExactNumberInfo::None)).unwrap(),
            ArrowDataType::Decimal128(10, 0)
        );
    }

    #[test]
    fn test_sql_type_unsupported() {
        use datafusion::sql::sqlparser::ast::DataType as SqlType;
        assert!(sql_data_type_to_arrow(&SqlType::Regclass).is_err());
    }

    #[test]
    fn test_sql_type_array() {
        use datafusion::sql::sqlparser::ast::{ArrayElemTypeDef, DataType as SqlType};
        let result = sql_data_type_to_arrow(&SqlType::Array(ArrayElemTypeDef::AngleBracket(
            Box::new(SqlType::Int(None)),
        )))
        .unwrap();
        assert_eq!(
            result,
            ArrowDataType::List(Arc::new(Field::new("element", ArrowDataType::Int32, true)))
        );
    }

    #[test]
    fn test_sql_type_array_no_element() {
        use datafusion::sql::sqlparser::ast::{ArrayElemTypeDef, DataType as SqlType};
        assert!(sql_data_type_to_arrow(&SqlType::Array(ArrayElemTypeDef::None)).is_err());
    }

    #[test]
    fn test_sql_type_map() {
        use datafusion::sql::sqlparser::ast::DataType as SqlType;
        let result = sql_data_type_to_arrow(&SqlType::Map(
            Box::new(SqlType::Varchar(None)),
            Box::new(SqlType::Int(None)),
        ))
        .unwrap();
        let expected = ArrowDataType::Map(
            Arc::new(Field::new(
                "entries",
                ArrowDataType::Struct(
                    vec![
                        Field::new("key", ArrowDataType::Utf8, false),
                        Field::new("value", ArrowDataType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_sql_type_struct() {
        use datafusion::sql::sqlparser::ast::{
            DataType as SqlType, Ident, StructBracketKind, StructField,
        };
        let result = sql_data_type_to_arrow(&SqlType::Struct(
            vec![
                StructField {
                    field_name: Some(Ident::new("name")),
                    field_type: SqlType::Varchar(None),
                    options: None,
                },
                StructField {
                    field_name: Some(Ident::new("age")),
                    field_type: SqlType::Int(None),
                    options: None,
                },
            ],
            StructBracketKind::AngleBrackets,
        ))
        .unwrap();
        assert_eq!(
            result,
            ArrowDataType::Struct(
                vec![
                    Field::new("name", ArrowDataType::Utf8, true),
                    Field::new("age", ArrowDataType::Int32, true),
                ]
                .into()
            )
        );
    }

    // ==================== resolve_table_name tests ====================

    #[test]
    fn test_resolve_three_part_name() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog);
        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, "SELECT * FROM paimon.mydb.mytable").unwrap();
        if let Statement::Query(q) = &stmts[0] {
            if let datafusion::sql::sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                if let datafusion::sql::sqlparser::ast::TableFactor::Table { name, .. } =
                    &sel.from[0].relation
                {
                    let id = handler.resolve_table_name(name).unwrap();
                    assert_eq!(id.database(), "mydb");
                    assert_eq!(id.object(), "mytable");
                }
            }
        }
    }

    #[test]
    fn test_resolve_two_part_name() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog);
        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, "SELECT * FROM mydb.mytable").unwrap();
        if let Statement::Query(q) = &stmts[0] {
            if let datafusion::sql::sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                if let datafusion::sql::sqlparser::ast::TableFactor::Table { name, .. } =
                    &sel.from[0].relation
                {
                    let id = handler.resolve_table_name(name).unwrap();
                    assert_eq!(id.database(), "mydb");
                    assert_eq!(id.object(), "mytable");
                }
            }
        }
    }

    #[test]
    fn test_resolve_wrong_catalog_name() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog);
        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, "SELECT * FROM other.mydb.mytable").unwrap();
        if let Statement::Query(q) = &stmts[0] {
            if let datafusion::sql::sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                if let datafusion::sql::sqlparser::ast::TableFactor::Table { name, .. } =
                    &sel.from[0].relation
                {
                    let err = handler.resolve_table_name(name).unwrap_err();
                    assert!(err.to_string().contains("Unknown catalog"));
                }
            }
        }
    }

    #[test]
    fn test_resolve_single_part_name_error() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog);
        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, "SELECT * FROM mytable").unwrap();
        if let Statement::Query(q) = &stmts[0] {
            if let datafusion::sql::sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                if let datafusion::sql::sqlparser::ast::TableFactor::Table { name, .. } =
                    &sel.from[0].relation
                {
                    let err = handler.resolve_table_name(name).unwrap_err();
                    assert!(err.to_string().contains("at least database.table"));
                }
            }
        }
    }

    // ==================== extract_options tests ====================

    #[test]
    fn test_extract_options_none() {
        let opts = extract_options(&CreateTableOptions::None).unwrap();
        assert!(opts.is_empty());
    }

    #[test]
    fn test_extract_options_with_kv() {
        // Parse a CREATE TABLE with WITH options to get a real CreateTableOptions
        let dialect = GenericDialect {};
        let stmts =
            Parser::parse_sql(&dialect, "CREATE TABLE t (id INT) WITH ('bucket' = '4')").unwrap();
        if let Statement::CreateTable(ct) = &stmts[0] {
            let opts = extract_options(&ct.table_options).unwrap();
            assert_eq!(opts.len(), 1);
            assert_eq!(opts[0].0, "bucket");
            assert_eq!(opts[0].1, "4");
        } else {
            panic!("expected CreateTable");
        }
    }

    // ==================== PaimonDdlHandler::sql integration tests ====================

    #[tokio::test]
    async fn test_create_table_basic() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());

        handler
            .sql("CREATE TABLE mydb.t1 (id INT NOT NULL, name VARCHAR, PRIMARY KEY (id))")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::CreateTable {
            identifier,
            schema,
            ignore_if_exists,
        } = &calls[0]
        {
            assert_eq!(identifier.database(), "mydb");
            assert_eq!(identifier.object(), "t1");
            assert!(!ignore_if_exists);
            assert_eq!(schema.primary_keys(), &["id"]);
        } else {
            panic!("expected CreateTable call");
        }
    }

    #[tokio::test]
    async fn test_create_table_if_not_exists() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());

        handler
            .sql("CREATE TABLE IF NOT EXISTS mydb.t1 (id INT)")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::CreateTable {
            ignore_if_exists, ..
        } = &calls[0]
        {
            assert!(ignore_if_exists);
        } else {
            panic!("expected CreateTable call");
        }
    }

    #[tokio::test]
    async fn test_create_table_with_options() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());

        handler
            .sql("CREATE TABLE mydb.t1 (id INT) WITH ('bucket' = '4', 'file.format' = 'parquet')")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::CreateTable { schema, .. } = &calls[0] {
            let opts = schema.options();
            assert_eq!(opts.get("bucket").unwrap(), "4");
            assert_eq!(opts.get("file.format").unwrap(), "parquet");
        } else {
            panic!("expected CreateTable call");
        }
    }

    #[tokio::test]
    async fn test_create_table_three_part_name() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());

        handler
            .sql("CREATE TABLE paimon.mydb.t1 (id INT)")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        if let CatalogCall::CreateTable { identifier, .. } = &calls[0] {
            assert_eq!(identifier.database(), "mydb");
            assert_eq!(identifier.object(), "t1");
        } else {
            panic!("expected CreateTable call");
        }
    }

    #[tokio::test]
    async fn test_alter_table_add_column() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());

        handler
            .sql("ALTER TABLE mydb.t1 ADD COLUMN age INT")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::AlterTable {
            identifier,
            changes,
            ..
        } = &calls[0]
        {
            assert_eq!(identifier.database(), "mydb");
            assert_eq!(identifier.object(), "t1");
            assert_eq!(changes.len(), 1);
            assert!(
                matches!(&changes[0], SchemaChange::AddColumn { field_name, .. } if field_name == "age")
            );
        } else {
            panic!("expected AlterTable call");
        }
    }

    #[tokio::test]
    async fn test_alter_table_drop_column() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());

        handler
            .sql("ALTER TABLE mydb.t1 DROP COLUMN age")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::AlterTable { changes, .. } = &calls[0] {
            assert_eq!(changes.len(), 1);
            assert!(
                matches!(&changes[0], SchemaChange::DropColumn { field_name } if field_name == "age")
            );
        } else {
            panic!("expected AlterTable call");
        }
    }

    #[tokio::test]
    async fn test_alter_table_rename_column() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());

        handler
            .sql("ALTER TABLE mydb.t1 RENAME COLUMN old_name TO new_name")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::AlterTable { changes, .. } = &calls[0] {
            assert_eq!(changes.len(), 1);
            assert!(matches!(
                &changes[0],
                SchemaChange::RenameColumn { field_name, new_name }
                    if field_name == "old_name" && new_name == "new_name"
            ));
        } else {
            panic!("expected AlterTable call");
        }
    }

    #[tokio::test]
    async fn test_alter_table_rename_table() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());

        handler
            .sql("ALTER TABLE mydb.t1 RENAME TO t2")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::RenameTable { from, to, .. } = &calls[0] {
            assert_eq!(from.database(), "mydb");
            assert_eq!(from.object(), "t1");
            assert_eq!(to.database(), "mydb");
            assert_eq!(to.object(), "t2");
        } else {
            panic!("expected RenameTable call");
        }
    }

    #[tokio::test]
    async fn test_alter_table_if_exists_add_column() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());

        handler
            .sql("ALTER TABLE IF EXISTS mydb.t1 ADD COLUMN age INT")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::AlterTable {
            ignore_if_not_exists,
            ..
        } = &calls[0]
        {
            assert!(ignore_if_not_exists);
        } else {
            panic!("expected AlterTable call");
        }
    }

    #[tokio::test]
    async fn test_alter_table_without_if_exists() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());

        handler
            .sql("ALTER TABLE mydb.t1 ADD COLUMN age INT")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        if let CatalogCall::AlterTable {
            ignore_if_not_exists,
            ..
        } = &calls[0]
        {
            assert!(!ignore_if_not_exists);
        } else {
            panic!("expected AlterTable call");
        }
    }

    #[tokio::test]
    async fn test_alter_table_if_exists_rename() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());

        handler
            .sql("ALTER TABLE IF EXISTS mydb.t1 RENAME TO t2")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::RenameTable {
            from,
            to,
            ignore_if_not_exists,
        } = &calls[0]
        {
            assert!(ignore_if_not_exists);
            assert_eq!(from.object(), "t1");
            assert_eq!(to.object(), "t2");
        } else {
            panic!("expected RenameTable call");
        }
    }

    #[tokio::test]
    async fn test_alter_table_rename_three_part_name() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());

        handler
            .sql("ALTER TABLE paimon.mydb.t1 RENAME TO t2")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::RenameTable { from, to, .. } = &calls[0] {
            assert_eq!(from.database(), "mydb");
            assert_eq!(from.object(), "t1");
            assert_eq!(to.database(), "mydb");
            assert_eq!(to.object(), "t2");
        } else {
            panic!("expected RenameTable call");
        }
    }

    #[tokio::test]
    async fn test_sql_parse_error() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog);
        let result = handler.sql("NOT VALID SQL !!!").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("SQL parse error"));
    }

    #[tokio::test]
    async fn test_multiple_statements_error() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog);
        let result = handler.sql("SELECT 1; SELECT 2").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("exactly one SQL statement"));
    }

    #[tokio::test]
    async fn test_create_external_table_rejected() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog);
        let result = handler
            .sql("CREATE EXTERNAL TABLE mydb.t1 (id INT) STORED AS PARQUET")
            .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("CREATE EXTERNAL TABLE is not supported"));
    }

    #[tokio::test]
    async fn test_non_ddl_delegates_to_datafusion() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());
        // SELECT should be delegated to DataFusion, not intercepted
        let df = handler.sql("SELECT 1 AS x").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        // No catalog calls
        assert!(catalog.take_calls().is_empty());
    }
}
