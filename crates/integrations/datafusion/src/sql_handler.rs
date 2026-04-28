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

//! SQL support for Paimon tables.
//!
//! DataFusion does not natively support all SQL statements needed by Paimon.
//! This module provides [`PaimonSqlHandler`] which intercepts CREATE TABLE,
//! ALTER TABLE, MERGE INTO, UPDATE and other SQL, translates them to Paimon
//! catalog operations, and delegates everything else (SELECT, CREATE/DROP
//! SCHEMA, DROP TABLE, etc.) to the underlying [`SessionContext`].
//!
//! Supported DDL:
//! - `CREATE TABLE db.t (col TYPE, ..., PRIMARY KEY (col, ...)) [PARTITIONED BY (col, ...)] [WITH ('key' = 'val')]`
//! - `ALTER TABLE db.t ADD COLUMN col TYPE`
//! - `ALTER TABLE db.t DROP COLUMN col`
//! - `ALTER TABLE db.t RENAME COLUMN old TO new`
//! - `ALTER TABLE db.t RENAME TO new_name`
//! - `ALTER TABLE db.t DROP PARTITION (col = val, ...)`
//! - `TRUNCATE TABLE db.t`
//! - `TRUNCATE TABLE db.t PARTITION (col = val, ...)`

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    new_null_array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{
    AlterTableOperation, ColumnDef, CreateTable, CreateTableOptions, Delete, Expr as SqlExpr,
    FromTable, Insert, Merge, ObjectName, RenameTableNameKind, Reset, ResetStatement, Set,
    SqlOption, Statement, TableFactor, TableObject, Truncate, Update, Value as SqlValue,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use futures::StreamExt;
use paimon::catalog::{Catalog, Identifier};
use paimon::spec::{
    ArrayType as PaimonArrayType, BigIntType, BlobType, BooleanType, DataField as PaimonDataField,
    DataType as PaimonDataType, DateType, Datum, DecimalType, DoubleType, FloatType, IntType,
    LocalZonedTimestampType, MapType as PaimonMapType, RowType as PaimonRowType, SchemaChange,
    SmallIntType, TimestampType, TinyIntType, VarBinaryType, VarCharType,
};

use crate::error::to_datafusion_error;
use crate::DynamicOptions;

/// Wraps a [`SessionContext`] and a Paimon [`Catalog`] to handle DDL statements
/// that DataFusion does not natively support (e.g. ALTER TABLE).
///
/// For all other SQL, it delegates to the inner `SessionContext`.
///
/// # Example
/// ```ignore
/// let ctx = SessionContext::new();
/// let handler = PaimonSqlHandler::new(ctx, catalog, "paimon")?;
/// let df = handler.sql("ALTER TABLE paimon.db.t ADD COLUMN age INT").await?;
/// ```
pub struct PaimonSqlHandler {
    ctx: SessionContext,
    catalog: Arc<dyn Catalog>,
    /// The catalog name registered in the SessionContext (used to strip the catalog prefix).
    catalog_name: String,
    /// Session-scoped dynamic options set via `SET 'paimon.key' = 'value'`.
    dynamic_options: DynamicOptions,
}

impl PaimonSqlHandler {
    /// Creates a new handler that registers the Paimon catalog and relation planner
    /// on the given [`SessionContext`].
    ///
    /// Dynamic options for `SET`/`RESET` are managed internally.
    pub fn new(
        ctx: SessionContext,
        catalog: Arc<dyn Catalog>,
        catalog_name: impl Into<String>,
    ) -> DFResult<Self> {
        let catalog_name = catalog_name.into();
        let dynamic_options: DynamicOptions = Default::default();
        ctx.register_catalog(
            &catalog_name,
            Arc::new(crate::catalog::PaimonCatalogProvider::with_dynamic_options(
                catalog.clone(),
                dynamic_options.clone(),
            )),
        );
        ctx.register_relation_planner(Arc::new(
            crate::relation_planner::PaimonRelationPlanner::new(),
        ))?;
        Ok(Self {
            ctx,
            catalog,
            catalog_name,
            dynamic_options,
        })
    }

    /// Returns a reference to the inner [`SessionContext`].
    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    #[cfg(test)]
    pub(crate) fn dynamic_options(&self) -> &DynamicOptions {
        &self.dynamic_options
    }

    /// Execute a SQL statement. ALTER TABLE is handled by Paimon directly;
    /// everything else is delegated to DataFusion.
    pub async fn sql(&self, sql: &str) -> DFResult<DataFrame> {
        let is_create_table = looks_like_create_table(sql);
        let (rewritten_sql, partition_keys) = if is_create_table {
            extract_partition_by(sql)?
        } else {
            (sql.to_string(), vec![])
        };
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, &rewritten_sql)
            .map_err(|e| DataFusionError::Plan(format!("SQL parse error: {e}")))?;

        if statements.len() != 1 {
            return Err(DataFusionError::Plan(
                "Expected exactly one SQL statement".to_string(),
            ));
        }

        match &statements[0] {
            Statement::CreateTable(create_table) => {
                self.handle_create_table(create_table, partition_keys).await
            }
            Statement::AlterTable(alter_table) => {
                self.handle_alter_table(
                    &alter_table.name,
                    &alter_table.operations,
                    alter_table.if_exists,
                )
                .await
            }
            Statement::Merge(merge) => self.handle_merge_into(merge).await,
            Statement::Update(update) => self.handle_update(update).await,
            Statement::Delete(delete) => self.handle_delete(delete).await,
            Statement::Insert(insert)
                if insert.overwrite
                    && insert.partitioned.as_ref().is_some_and(|p| !p.is_empty()) =>
            {
                self.handle_insert_overwrite_partition(insert).await
            }
            Statement::Set(Set::SingleAssignment {
                variable, values, ..
            }) => {
                let key = variable.to_string();
                let key = key.trim_matches('\'').trim_matches('"');
                if let Some(paimon_key) = key.strip_prefix("paimon.") {
                    let value = values
                        .first()
                        .ok_or_else(|| DataFusionError::Plan("SET requires a value".to_string()))?
                        .to_string();
                    let value = value
                        .strip_prefix('\'')
                        .and_then(|s| s.strip_suffix('\''))
                        .unwrap_or(&value)
                        .to_string();
                    self.dynamic_options
                        .write()
                        .unwrap()
                        .insert(paimon_key.to_string(), value);
                    return ok_result(&self.ctx);
                }
                self.ctx.sql(sql).await
            }
            Statement::Reset(ResetStatement {
                reset: Reset::ConfigurationParameter(name),
            }) => {
                let key = name.to_string();
                let key = key.trim_matches('\'').trim_matches('"');
                if let Some(paimon_key) = key.strip_prefix("paimon.") {
                    self.dynamic_options.write().unwrap().remove(paimon_key);
                    return ok_result(&self.ctx);
                }
                self.ctx.sql(sql).await
            }
            Statement::Truncate(truncate) => self.handle_truncate_table(truncate).await,
            _ => self.ctx.sql(sql).await,
        }
    }

    async fn handle_create_table(
        &self,
        ct: &CreateTable,
        partition_keys: Vec<String>,
    ) -> DFResult<DataFrame> {
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
            let paimon_type = column_def_to_paimon_type(col)?;
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

        // Partition keys (extracted and validated before parsing)
        if !partition_keys.is_empty() {
            let col_names: Vec<&str> = ct.columns.iter().map(|c| c.name.value.as_str()).collect();
            for pk in &partition_keys {
                if !col_names.contains(&pk.as_str()) {
                    return Err(DataFusionError::Plan(format!(
                        "PARTITIONED BY column '{pk}' is not defined in the table"
                    )));
                }
            }
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
                AlterTableOperation::SetTblProperties { table_properties } => {
                    for opt in table_properties {
                        if let SqlOption::KeyValue { key, value } = opt {
                            let v = value.to_string();
                            let v = v
                                .strip_prefix('\'')
                                .and_then(|s| s.strip_suffix('\''))
                                .unwrap_or(&v)
                                .to_string();
                            changes.push(SchemaChange::set_option(key.value.clone(), v));
                        }
                    }
                }
                AlterTableOperation::DropPartitions {
                    partitions,
                    if_exists: partition_if_exists,
                } => {
                    return self
                        .handle_drop_partitions(
                            &identifier,
                            partitions,
                            if_exists || *partition_if_exists,
                        )
                        .await;
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

    async fn handle_merge_into(&self, merge: &Merge) -> DFResult<DataFrame> {
        // Resolve the target table name from the MERGE INTO clause
        let table_name = match &merge.table {
            TableFactor::Table { name, .. } => name.clone(),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported target table in MERGE INTO: {other}"
                )))
            }
        };
        let identifier = self.resolve_table_name(&table_name)?;

        // Load the Paimon table from the catalog
        let table = self
            .catalog
            .get_table(&identifier)
            .await
            .map_err(to_datafusion_error)?;

        crate::merge_into::execute_merge_into(&self.ctx, merge, table).await
    }

    async fn handle_update(&self, update: &Update) -> DFResult<DataFrame> {
        let table_name = match &update.table.relation {
            TableFactor::Table { name, .. } => name.clone(),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported target table in UPDATE: {other}"
                )))
            }
        };
        let identifier = self.resolve_table_name(&table_name)?;

        let table = self
            .catalog
            .get_table(&identifier)
            .await
            .map_err(to_datafusion_error)?;

        crate::update::execute_update(&self.ctx, update, table).await
    }

    async fn handle_delete(&self, delete: &Delete) -> DFResult<DataFrame> {
        let tables = match &delete.from {
            FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
        };
        let table_factor = tables
            .first()
            .map(|t| &t.relation)
            .ok_or_else(|| DataFusionError::Plan("DELETE requires a target table".to_string()))?;
        let table_name = match table_factor {
            TableFactor::Table { name, .. } => name.clone(),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported target table in DELETE: {other}"
                )))
            }
        };
        let identifier = self.resolve_table_name(&table_name)?;

        let table = self
            .catalog
            .get_table(&identifier)
            .await
            .map_err(to_datafusion_error)?;

        let table_ref = table_name.to_string();
        crate::delete::execute_delete(&self.ctx, delete, table, &table_ref).await
    }

    async fn handle_insert_overwrite_partition(&self, insert: &Insert) -> DFResult<DataFrame> {
        let table_name = match &insert.table {
            TableObject::TableName(name) => name.clone(),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported target table in INSERT OVERWRITE: {other}"
                )))
            }
        };
        let identifier = self.resolve_table_name(&table_name)?;
        let table = self
            .catalog
            .get_table(&identifier)
            .await
            .map_err(to_datafusion_error)?;

        let partition_exprs = insert.partitioned.as_ref().ok_or_else(|| {
            DataFusionError::Plan("INSERT OVERWRITE PARTITION requires a PARTITION clause".into())
        })?;
        let partition_fields = table.schema().partition_fields();
        let static_partitions =
            parse_static_partitions(partition_exprs, &partition_fields, table.schema().fields())?;

        let source = insert.source.as_ref().ok_or_else(|| {
            DataFusionError::Plan("INSERT OVERWRITE requires a source query".into())
        })?;
        let df = self.ctx.sql(&source.to_string()).await?;

        let all_fields = table.schema().fields();
        let non_static_fields: Vec<&PaimonDataField> = all_fields
            .iter()
            .filter(|f| !static_partitions.contains_key(f.name()))
            .collect();
        let expected_source_cols = non_static_fields.len();

        // Resolve target column mapping from the explicit column list.
        // `columns` = before PARTITION, `after_columns` = after PARTITION (Hive-style).
        let target_columns = if !insert.columns.is_empty() {
            Some(&insert.columns)
        } else if !insert.after_columns.is_empty() {
            Some(&insert.after_columns)
        } else {
            None
        };
        let column_reorder: Option<Vec<usize>> = if let Some(cols) = target_columns {
            if cols.len() != expected_source_cols {
                return Err(DataFusionError::Plan(format!(
                    "Column list has {} columns, but expected {} non-partition columns",
                    cols.len(),
                    expected_source_cols
                )));
            }
            let col_names: Vec<&str> = cols.iter().map(|id| id.value.as_str()).collect();
            let mut reorder = Vec::with_capacity(expected_source_cols);
            for field in &non_static_fields {
                let pos = col_names
                    .iter()
                    .position(|c| c == &field.name())
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "Column '{}' not found in target column list",
                            field.name()
                        ))
                    })?;
                reorder.push(pos);
            }
            Some(reorder)
        } else {
            None
        };

        // Validate column count from the DataFrame schema before consuming any batches.
        let source_col_count = df.schema().fields().len();
        if source_col_count != expected_source_cols {
            return Err(DataFusionError::Plan(format!(
                "Source query has {} columns, but expected {} non-partition columns",
                source_col_count, expected_source_cols
            )));
        }

        let mut stream = df.execute_stream().await?;

        let wb = table.new_write_builder();
        let mut tw = wb
            .new_write()
            .map_err(to_datafusion_error)?
            .with_overwrite();
        let mut row_count = 0u64;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = if let Some(ref reorder) = column_reorder {
                let reordered_cols: Vec<ArrayRef> =
                    reorder.iter().map(|&i| batch.column(i).clone()).collect();
                let reordered_fields: Vec<Field> = reorder
                    .iter()
                    .map(|&i| batch.schema().field(i).clone())
                    .collect();
                let reordered_schema = Arc::new(Schema::new(reordered_fields));
                RecordBatch::try_new(reordered_schema, reordered_cols)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
            } else {
                batch
            };
            let augmented = append_partition_columns(
                &batch,
                &static_partitions,
                expected_source_cols,
                all_fields,
            )?;
            row_count += augmented.num_rows() as u64;
            tw.write_arrow_batch(&augmented)
                .await
                .map_err(to_datafusion_error)?;
        }

        let messages = tw.prepare_commit().await.map_err(to_datafusion_error)?;
        let commit = wb.new_commit();

        let overwrite_partitions = if static_partitions.is_empty() {
            None
        } else {
            Some(static_partitions)
        };
        commit
            .overwrite(messages, overwrite_partitions)
            .await
            .map_err(to_datafusion_error)?;

        crate::merge_into::ok_result(&self.ctx, row_count)
    }

    async fn handle_truncate_table(&self, truncate: &Truncate) -> DFResult<DataFrame> {
        if truncate.table_names.len() > 1 {
            return Err(DataFusionError::Plan(
                "TRUNCATE TABLE does not support multiple tables".to_string(),
            ));
        }
        let target = truncate.table_names.first().ok_or_else(|| {
            DataFusionError::Plan("TRUNCATE TABLE requires a table name".to_string())
        })?;
        let identifier = self.resolve_table_name(&target.name)?;
        let table = match self.catalog.get_table(&identifier).await {
            Ok(t) => t,
            Err(e) if truncate.if_exists && is_table_not_exist(&e) => {
                return ok_result(&self.ctx);
            }
            Err(e) => return Err(to_datafusion_error(e)),
        };

        let wb = table.new_write_builder();
        let commit = wb.new_commit();

        if let Some(partitions) = &truncate.partitions {
            if partitions.is_empty() {
                return Err(DataFusionError::Plan(
                    "PARTITION clause requires at least one column = value".to_string(),
                ));
            }
            let partition_values = parse_partition_values(
                partitions,
                table.schema().fields(),
                table.schema().partition_keys(),
            )?;
            commit
                .truncate_partitions(partition_values)
                .await
                .map_err(to_datafusion_error)?;
            return ok_result(&self.ctx);
        }

        commit.truncate_table().await.map_err(to_datafusion_error)?;
        ok_result(&self.ctx)
    }

    async fn handle_drop_partitions(
        &self,
        identifier: &Identifier,
        partitions: &[SqlExpr],
        if_exists: bool,
    ) -> DFResult<DataFrame> {
        if partitions.is_empty() {
            return Err(DataFusionError::Plan(
                "DROP PARTITIONS requires at least one partition specification".to_string(),
            ));
        }
        let table = match self.catalog.get_table(identifier).await {
            Ok(t) => t,
            Err(e) if if_exists && is_table_not_exist(&e) => {
                return ok_result(&self.ctx);
            }
            Err(e) => return Err(to_datafusion_error(e)),
        };

        let partition_values = parse_partition_values(
            partitions,
            table.schema().fields(),
            table.schema().partition_keys(),
        )?;

        let wb = table.new_write_builder();
        let commit = wb.new_commit();
        commit
            .truncate_partitions(partition_values)
            .await
            .map_err(to_datafusion_error)?;

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

/// Quick check whether the SQL looks like a CREATE TABLE statement.
/// Skips leading whitespace, `--` line comments, and `/* */` block comments.
fn looks_like_create_table(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    // Skip leading whitespace and comments
    loop {
        while i < len && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i + 1 < len && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            i += 2;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if i + 1 < len && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < len {
                if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }
        break;
    }
    // Match "CREATE" then whitespace then "TABLE" (all ASCII, byte-safe)
    if i + 6 > len || !bytes[i..i + 6].eq_ignore_ascii_case(b"CREATE") {
        return false;
    }
    i += 6;
    if i >= len || !bytes[i].is_ascii_whitespace() {
        return false;
    }
    while i < len && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    i + 5 <= len && bytes[i..i + 5].eq_ignore_ascii_case(b"TABLE")
}

/// Find `PARTITIONED BY` keyword position, skipping string literals and comments.
fn find_partitioned_by(sql: &str) -> Option<(usize, usize)> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    while i < len {
        match bytes[i] {
            b'\'' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'\'' {
                        i += 1;
                        if i < len && bytes[i] == b'\'' {
                            i += 1;
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                i += 2;
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < len {
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            b if b.is_ascii_alphabetic() && i + 11 <= len => {
                if bytes[i..i + 11].eq_ignore_ascii_case(b"PARTITIONED") {
                    let rest = &bytes[i + 11..];
                    let ws = rest.iter().take_while(|b| b.is_ascii_whitespace()).count();
                    if ws > 0
                        && i + 11 + ws + 2 <= len
                        && rest[ws..ws + 2].eq_ignore_ascii_case(b"BY")
                    {
                        let by_end = i + 11 + ws + 2;
                        return Some((i, by_end));
                    }
                }
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }
    None
}

/// Parse a single partition column token, handling quoted identifiers.
fn parse_partition_column(token: &str) -> DFResult<String> {
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return Err(DataFusionError::Plan(
            "Empty column name in PARTITIONED BY".to_string(),
        ));
    }

    let first = trimmed.as_bytes()[0];
    if first == b'"' || first == b'`' {
        let close = if first == b'"' { b'"' } else { b'`' };
        if let Some(end) = trimmed[1..].find(close as char) {
            let after_quote = trimmed[1 + end + 1..].trim();
            if after_quote.is_empty() {
                return Ok(trimmed[1..1 + end].to_string());
            }
        }
        return Err(DataFusionError::Plan(format!(
            "Invalid quoted identifier in PARTITIONED BY: {trimmed}"
        )));
    }

    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    match parts.len() {
        1 => Ok(parts[0].to_string()),
        _ => Err(DataFusionError::Plan(format!(
            "PARTITIONED BY column '{}' should not specify a type. \
             Use column references only, e.g. PARTITIONED BY ({})",
            parts[0], parts[0]
        ))),
    }
}

/// Extract `PARTITIONED BY (col1, col2, ...)` from SQL before parsing.
///
/// Paimon only allows column references (no types) in PARTITIONED BY.
/// Since sqlparser's GenericDialect requires types in column definitions,
/// we extract and validate the clause ourselves, then strip it from the SQL
/// so sqlparser can parse the rest.
fn extract_partition_by(sql: &str) -> DFResult<(String, Vec<String>)> {
    let Some((kw_start, by_end)) = find_partitioned_by(sql) else {
        return Ok((sql.to_string(), vec![]));
    };

    let after_by = sql[by_end..].trim_start();
    let paren_start = by_end + (sql[by_end..].len() - after_by.len());

    if !after_by.starts_with('(') {
        return Err(DataFusionError::Plan(
            "Expected '(' after PARTITIONED BY".to_string(),
        ));
    }

    let inner_start = paren_start + 1;
    let mut depth = 1;
    let mut paren_end = None;
    for (i, ch) in sql[inner_start..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    paren_end = Some(inner_start + i);
                    break;
                }
            }
            _ => {}
        }
    }
    let paren_end = paren_end.ok_or_else(|| {
        DataFusionError::Plan("Unmatched '(' in PARTITIONED BY clause".to_string())
    })?;

    let inner = sql[inner_start..paren_end].trim();
    if inner.is_empty() {
        return Err(DataFusionError::Plan(
            "PARTITIONED BY must specify at least one column".to_string(),
        ));
    }

    let mut partition_keys = Vec::new();
    for token in inner.split(',') {
        partition_keys.push(parse_partition_column(token)?);
    }

    let clause_end = paren_end + 1;
    let mut rewritten = String::with_capacity(sql.len());
    rewritten.push_str(&sql[..kw_start]);
    rewritten.push_str(&sql[clause_end..]);
    Ok((rewritten, partition_keys))
}

/// Convert a sqlparser [`ColumnDef`] to a Paimon [`SchemaChange::AddColumn`].
fn column_def_to_add_column(col: &ColumnDef) -> DFResult<SchemaChange> {
    let paimon_type = column_def_to_paimon_type(col)?;
    Ok(SchemaChange::add_column(
        col.name.value.clone(),
        paimon_type,
    ))
}

fn column_def_to_paimon_type(col: &ColumnDef) -> DFResult<PaimonDataType> {
    sql_data_type_to_paimon_type(&col.data_type, column_def_nullable(col))
}

fn column_def_nullable(col: &ColumnDef) -> bool {
    !col.options.iter().any(|opt| {
        matches!(
            opt.option,
            datafusion::sql::sqlparser::ast::ColumnOption::NotNull
        )
    })
}

/// Convert a sqlparser SQL data type to a Paimon data type.
///
/// DDL schema translation must use this function instead of going through Arrow,
/// because Arrow cannot preserve logical distinctions such as `BLOB` vs `VARBINARY`.
fn sql_data_type_to_paimon_type(
    sql_type: &datafusion::sql::sqlparser::ast::DataType,
    nullable: bool,
) -> DFResult<PaimonDataType> {
    use datafusion::sql::sqlparser::ast::{
        ArrayElemTypeDef, DataType as SqlType, ExactNumberInfo, TimezoneInfo,
    };

    match sql_type {
        SqlType::Boolean => Ok(PaimonDataType::Boolean(BooleanType::with_nullable(
            nullable,
        ))),
        SqlType::TinyInt(_) => Ok(PaimonDataType::TinyInt(TinyIntType::with_nullable(
            nullable,
        ))),
        SqlType::SmallInt(_) => Ok(PaimonDataType::SmallInt(SmallIntType::with_nullable(
            nullable,
        ))),
        SqlType::Int(_) | SqlType::Integer(_) => {
            Ok(PaimonDataType::Int(IntType::with_nullable(nullable)))
        }
        SqlType::BigInt(_) => Ok(PaimonDataType::BigInt(BigIntType::with_nullable(nullable))),
        SqlType::Float(_) | SqlType::Real => {
            Ok(PaimonDataType::Float(FloatType::with_nullable(nullable)))
        }
        SqlType::Double(_) | SqlType::DoublePrecision => {
            Ok(PaimonDataType::Double(DoubleType::with_nullable(nullable)))
        }
        SqlType::Varchar(_)
        | SqlType::CharVarying(_)
        | SqlType::Text
        | SqlType::String(_)
        | SqlType::Char(_)
        | SqlType::Character(_) => Ok(PaimonDataType::VarChar(
            VarCharType::with_nullable(nullable, VarCharType::MAX_LENGTH)
                .map_err(to_datafusion_error)?,
        )),
        SqlType::Binary(_) | SqlType::Varbinary(_) | SqlType::Bytea => {
            Ok(PaimonDataType::VarBinary(
                VarBinaryType::try_new(nullable, VarBinaryType::MAX_LENGTH)
                    .map_err(to_datafusion_error)?,
            ))
        }
        SqlType::Blob(_) => Ok(PaimonDataType::Blob(BlobType::with_nullable(nullable))),
        SqlType::Date => Ok(PaimonDataType::Date(DateType::with_nullable(nullable))),
        SqlType::Timestamp(precision, tz_info) => {
            let precision = match precision {
                Some(0) => 0,
                Some(1..=3) | None => 3,
                Some(4..=6) => 6,
                _ => 9,
            };
            match tz_info {
                TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => {
                    Ok(PaimonDataType::Timestamp(
                        TimestampType::with_nullable(nullable, precision)
                            .map_err(to_datafusion_error)?,
                    ))
                }
                _ => Ok(PaimonDataType::LocalZonedTimestamp(
                    LocalZonedTimestampType::with_nullable(nullable, precision)
                        .map_err(to_datafusion_error)?,
                )),
            }
        }
        SqlType::Decimal(info) => {
            let (precision, scale) = match info {
                ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                    (*precision as u32, *scale as u32)
                }
                ExactNumberInfo::Precision(precision) => (*precision as u32, 0),
                ExactNumberInfo::None => (10, 0),
            };
            Ok(PaimonDataType::Decimal(
                DecimalType::with_nullable(nullable, precision, scale)
                    .map_err(to_datafusion_error)?,
            ))
        }
        SqlType::Array(elem_def) => {
            let element_type = match elem_def {
                ArrayElemTypeDef::AngleBracket(t)
                | ArrayElemTypeDef::SquareBracket(t, _)
                | ArrayElemTypeDef::Parenthesis(t) => sql_data_type_to_paimon_type(t, true)?,
                ArrayElemTypeDef::None => {
                    return Err(DataFusionError::Plan(
                        "ARRAY type requires an element type".to_string(),
                    ));
                }
            };
            Ok(PaimonDataType::Array(PaimonArrayType::with_nullable(
                nullable,
                element_type,
            )))
        }
        SqlType::Map(key_type, value_type) => {
            let key = sql_data_type_to_paimon_type(key_type, false)?;
            let value = sql_data_type_to_paimon_type(value_type, true)?;
            Ok(PaimonDataType::Map(PaimonMapType::with_nullable(
                nullable, key, value,
            )))
        }
        SqlType::Struct(fields, _) => {
            let paimon_fields = fields
                .iter()
                .enumerate()
                .map(|(idx, field)| {
                    let name = field
                        .field_name
                        .as_ref()
                        .map(|n| n.value.clone())
                        .unwrap_or_default();
                    let data_type = sql_data_type_to_paimon_type(&field.field_type, true)?;
                    Ok(PaimonDataField::new(idx as i32, name, data_type))
                })
                .collect::<DFResult<Vec<_>>>()?;
            Ok(PaimonDataType::Row(PaimonRowType::with_nullable(
                nullable,
                paimon_fields,
            )))
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

fn is_table_not_exist(e: &paimon::Error) -> bool {
    matches!(e, paimon::Error::TableNotExist { .. })
}

/// Parse partition expressions (`col = val, ...`) into partition value maps
/// suitable for `TableCommit::truncate_partitions`.
///
/// All expressions are treated as belonging to a single partition specification.
/// For multiple partitions, callers should invoke this once per partition clause.
fn parse_partition_values(
    exprs: &[SqlExpr],
    all_fields: &[PaimonDataField],
    partition_keys: &[String],
) -> DFResult<Vec<HashMap<String, Option<Datum>>>> {
    let field_map: HashMap<&str, &PaimonDataField> =
        all_fields.iter().map(|f| (f.name(), f)).collect();

    let mut partition = HashMap::new();
    for expr in exprs {
        let (col_name, val_expr) = match expr {
            SqlExpr::BinaryOp {
                left,
                op: datafusion::sql::sqlparser::ast::BinaryOperator::Eq,
                right,
            } => {
                let col = match left.as_ref() {
                    SqlExpr::Identifier(ident) => ident.value.clone(),
                    other => {
                        return Err(DataFusionError::Plan(format!(
                            "Expected column name in partition spec, got: {other}"
                        )))
                    }
                };
                (col, right.as_ref())
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Expected 'column = value' in partition spec, got: {other}"
                )))
            }
        };

        if !partition_keys.iter().any(|k| k == &col_name) {
            return Err(DataFusionError::Plan(format!(
                "Column '{col_name}' is not a partition column"
            )));
        }

        let field = field_map.get(col_name.as_str()).ok_or_else(|| {
            DataFusionError::Plan(format!("Column '{col_name}' not found in table schema"))
        })?;
        let datum = sql_expr_to_datum(val_expr, field.data_type())?;
        partition.insert(col_name, Some(datum));
    }

    let missing: Vec<&str> = partition_keys
        .iter()
        .filter(|k| !partition.contains_key(k.as_str()))
        .map(|k| k.as_str())
        .collect();
    if !missing.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Incomplete partition spec: missing keys [{}]. All partition columns must be specified.",
            missing.join(", ")
        )));
    }

    Ok(vec![partition])
}

/// Parse static partition assignments from `PARTITION (col = val, ...)` expressions.
/// Dynamic partition columns (bare identifiers without `= val`) are skipped —
/// they will be read from the source query.
fn parse_static_partitions(
    exprs: &[SqlExpr],
    partition_fields: &[PaimonDataField],
    all_fields: &[PaimonDataField],
) -> DFResult<HashMap<String, Option<Datum>>> {
    let mut result = HashMap::new();
    let field_map: HashMap<&str, &PaimonDataField> =
        all_fields.iter().map(|f| (f.name(), f)).collect();
    let partition_names: Vec<&str> = partition_fields.iter().map(|f| f.name()).collect();

    for expr in exprs {
        let (col_name, val_expr) = match expr {
            SqlExpr::BinaryOp {
                left,
                op: datafusion::sql::sqlparser::ast::BinaryOperator::Eq,
                right,
            } => {
                let col = match left.as_ref() {
                    SqlExpr::Identifier(ident) => ident.value.clone(),
                    other => {
                        return Err(DataFusionError::Plan(format!(
                            "Expected column name in PARTITION clause, got: {other}"
                        )))
                    }
                };
                (col, right.as_ref())
            }
            // Dynamic partition: bare column name without value — skip it,
            // the column will be read from the source query.
            SqlExpr::Identifier(ident) => {
                let col_name = &ident.value;
                if !partition_names.contains(&col_name.as_str()) {
                    return Err(DataFusionError::Plan(format!(
                        "Column '{col_name}' is not a partition column"
                    )));
                }
                continue;
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported expression in PARTITION clause: {other}"
                )))
            }
        };

        if !partition_names.contains(&col_name.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "Column '{col_name}' is not a partition column"
            )));
        }

        let field = field_map.get(col_name.as_str()).ok_or_else(|| {
            DataFusionError::Plan(format!("Column '{col_name}' not found in table schema"))
        })?;
        let datum = sql_expr_to_datum(val_expr, field.data_type())?;
        result.insert(col_name, Some(datum));
    }

    Ok(result)
}

/// Convert a SQL literal expression to a Paimon Datum.
fn sql_expr_to_datum(expr: &SqlExpr, data_type: &PaimonDataType) -> DFResult<Datum> {
    let (value, negate) = match expr {
        SqlExpr::Value(v) => (&v.value, false),
        SqlExpr::UnaryOp {
            op: datafusion::sql::sqlparser::ast::UnaryOperator::Minus,
            expr: inner,
        } => {
            if let SqlExpr::Value(v) = inner.as_ref() {
                (&v.value, true)
            } else {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported partition value expression: {expr}"
                )));
            }
        }
        other => {
            return Err(DataFusionError::Plan(format!(
                "Unsupported partition value expression: {other}"
            )))
        }
    };

    match (value, data_type) {
        (SqlValue::Number(n, _), _) => parse_number_datum(n, data_type, negate),
        (SqlValue::SingleQuotedString(s), PaimonDataType::VarChar(_)) if !negate => {
            Ok(Datum::String(s.clone()))
        }
        (SqlValue::SingleQuotedString(s), PaimonDataType::Date(_)) if !negate => {
            let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| DataFusionError::Plan(format!("Invalid DATE '{s}': {e}")))?;
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            Ok(Datum::Date((date - epoch).num_days() as i32))
        }
        (SqlValue::Boolean(b), PaimonDataType::Boolean(_)) if !negate => Ok(Datum::Bool(*b)),
        _ if negate => Err(DataFusionError::Plan(format!(
            "Cannot negate value for type {data_type:?}"
        ))),
        _ => Err(DataFusionError::Plan(format!(
            "Cannot convert {value} to {data_type:?}"
        ))),
    }
}

fn parse_number_datum(n: &str, data_type: &PaimonDataType, negate: bool) -> DFResult<Datum> {
    let s: String = if negate {
        format!("-{n}")
    } else {
        n.to_string()
    };
    match data_type {
        PaimonDataType::TinyInt(_) => {
            Ok(Datum::TinyInt(s.parse::<i8>().map_err(|e| {
                DataFusionError::Plan(format!("Invalid TINYINT: {e}"))
            })?))
        }
        PaimonDataType::SmallInt(_) => {
            Ok(Datum::SmallInt(s.parse::<i16>().map_err(|e| {
                DataFusionError::Plan(format!("Invalid SMALLINT: {e}"))
            })?))
        }
        PaimonDataType::Int(_) => {
            Ok(Datum::Int(s.parse::<i32>().map_err(|e| {
                DataFusionError::Plan(format!("Invalid INT: {e}"))
            })?))
        }
        PaimonDataType::BigInt(_) => {
            Ok(Datum::Long(s.parse::<i64>().map_err(|e| {
                DataFusionError::Plan(format!("Invalid BIGINT: {e}"))
            })?))
        }
        PaimonDataType::Float(_) => {
            Ok(Datum::Float(s.parse::<f32>().map_err(|e| {
                DataFusionError::Plan(format!("Invalid FLOAT: {e}"))
            })?))
        }
        PaimonDataType::Double(_) => {
            Ok(Datum::Double(s.parse::<f64>().map_err(|e| {
                DataFusionError::Plan(format!("Invalid DOUBLE: {e}"))
            })?))
        }
        _ => Err(DataFusionError::Plan(format!(
            "Cannot convert {n} to {data_type:?}"
        ))),
    }
}

/// Append static partition columns to a RecordBatch.
fn append_partition_columns(
    batch: &RecordBatch,
    partitions: &HashMap<String, Option<Datum>>,
    expected_source_cols: usize,
    all_fields: &[PaimonDataField],
) -> DFResult<RecordBatch> {
    let num_rows = batch.num_rows();

    let mut columns: Vec<(String, ArrayRef)> = Vec::with_capacity(all_fields.len());

    let mut source_col_idx = 0;
    for field in all_fields {
        let name = field.name().to_string();
        if let Some(datum_opt) = partitions.get(&name) {
            let array = datum_to_constant_array(datum_opt, field.data_type(), num_rows)?;
            columns.push((name, array));
        } else {
            if source_col_idx >= batch.num_columns() {
                return Err(DataFusionError::Plan(format!(
                    "Source query has fewer columns than expected non-partition columns. \
                     Expected column '{name}' at position {source_col_idx}"
                )));
            }
            let col = batch.column(source_col_idx).clone();
            let target_type = paimon::arrow::paimon_type_to_arrow(field.data_type())
                .map_err(to_datafusion_error)?;
            let col = if col.data_type() != &target_type {
                cast(&col, &target_type).map_err(|e| {
                    DataFusionError::Plan(format!(
                        "Cannot cast column '{name}' from {:?} to {:?}: {e}",
                        col.data_type(),
                        target_type
                    ))
                })?
            } else {
                col
            };
            columns.push((name, col));
            source_col_idx += 1;
        }
    }

    if source_col_idx != batch.num_columns() || source_col_idx != expected_source_cols {
        return Err(DataFusionError::Plan(format!(
            "Source query has {} columns, but expected {} non-partition columns",
            batch.num_columns(),
            expected_source_cols
        )));
    }

    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, arr)| Field::new(name, arr.data_type().clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let arrays: Vec<ArrayRef> = columns.into_iter().map(|(_, arr)| arr).collect();
    RecordBatch::try_new(schema, arrays).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Create a constant Arrow array from a Datum value.
/// Only variants produced by `sql_expr_to_datum` are supported here.
fn datum_to_constant_array(
    datum: &Option<Datum>,
    data_type: &PaimonDataType,
    num_rows: usize,
) -> DFResult<ArrayRef> {
    match datum {
        None => {
            let arrow_type =
                paimon::arrow::paimon_type_to_arrow(data_type).map_err(to_datafusion_error)?;
            Ok(new_null_array(&arrow_type, num_rows))
        }
        Some(d) => match d {
            Datum::Bool(v) => Ok(Arc::new(BooleanArray::from(vec![*v; num_rows]))),
            Datum::TinyInt(v) => Ok(Arc::new(Int8Array::from(vec![*v; num_rows]))),
            Datum::SmallInt(v) => Ok(Arc::new(Int16Array::from(vec![*v; num_rows]))),
            Datum::Int(v) => Ok(Arc::new(Int32Array::from(vec![*v; num_rows]))),
            Datum::Long(v) => Ok(Arc::new(Int64Array::from(vec![*v; num_rows]))),
            Datum::Float(v) => Ok(Arc::new(Float32Array::from(vec![*v; num_rows]))),
            Datum::Double(v) => Ok(Arc::new(Float64Array::from(vec![*v; num_rows]))),
            Datum::String(v) => Ok(Arc::new(StringArray::from(vec![v.as_str(); num_rows]))),
            Datum::Date(v) => Ok(Arc::new(Date32Array::from(vec![*v; num_rows]))),
            Datum::Time(_)
            | Datum::Timestamp { .. }
            | Datum::LocalZonedTimestamp { .. }
            | Datum::Decimal { .. }
            | Datum::Bytes(_) => Err(DataFusionError::Plan(format!(
                "Unsupported datum type for partition column: {d}"
            ))),
        },
    }
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
    use paimon::catalog::Database;
    use paimon::spec::{DataType as PaimonDataType, Schema as PaimonSchema};
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

    fn make_handler(catalog: Arc<MockCatalog>) -> PaimonSqlHandler {
        PaimonSqlHandler::new(SessionContext::new(), catalog, "paimon").unwrap()
    }

    fn assert_sql_type_to_paimon(
        sql_type: datafusion::sql::sqlparser::ast::DataType,
        expected: PaimonDataType,
    ) {
        assert_eq!(
            sql_data_type_to_paimon_type(&sql_type, true).unwrap(),
            expected
        );
    }

    // ==================== sql_data_type_to_paimon_type tests ====================

    #[test]
    fn test_sql_type_boolean() {
        use datafusion::sql::sqlparser::ast::DataType as SqlType;
        assert_sql_type_to_paimon(
            SqlType::Boolean,
            PaimonDataType::Boolean(BooleanType::new()),
        );
    }

    #[test]
    fn test_sql_type_integers() {
        use datafusion::sql::sqlparser::ast::DataType as SqlType;
        assert_sql_type_to_paimon(
            SqlType::TinyInt(None),
            PaimonDataType::TinyInt(TinyIntType::new()),
        );
        assert_sql_type_to_paimon(
            SqlType::SmallInt(None),
            PaimonDataType::SmallInt(SmallIntType::new()),
        );
        assert_sql_type_to_paimon(SqlType::Int(None), PaimonDataType::Int(IntType::new()));
        assert_sql_type_to_paimon(SqlType::Integer(None), PaimonDataType::Int(IntType::new()));
        assert_sql_type_to_paimon(
            SqlType::BigInt(None),
            PaimonDataType::BigInt(BigIntType::new()),
        );
    }

    #[test]
    fn test_sql_type_floats() {
        use datafusion::sql::sqlparser::ast::{DataType as SqlType, ExactNumberInfo};
        assert_sql_type_to_paimon(
            SqlType::Float(ExactNumberInfo::None),
            PaimonDataType::Float(FloatType::new()),
        );
        assert_sql_type_to_paimon(SqlType::Real, PaimonDataType::Float(FloatType::new()));
        assert_sql_type_to_paimon(
            SqlType::DoublePrecision,
            PaimonDataType::Double(DoubleType::new()),
        );
    }

    #[test]
    fn test_sql_type_string_variants() {
        use datafusion::sql::sqlparser::ast::DataType as SqlType;
        for sql_type in [SqlType::Varchar(None), SqlType::Text, SqlType::String(None)] {
            assert_sql_type_to_paimon(
                sql_type.clone(),
                PaimonDataType::VarChar(
                    VarCharType::with_nullable(true, VarCharType::MAX_LENGTH).unwrap(),
                ),
            );
        }
    }

    #[test]
    fn test_sql_type_binary() {
        use datafusion::sql::sqlparser::ast::DataType as SqlType;
        assert_sql_type_to_paimon(
            SqlType::Bytea,
            PaimonDataType::VarBinary(
                VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).unwrap(),
            ),
        );
    }

    #[test]
    fn test_sql_type_date() {
        use datafusion::sql::sqlparser::ast::DataType as SqlType;
        assert_sql_type_to_paimon(SqlType::Date, PaimonDataType::Date(DateType::new()));
    }

    #[test]
    fn test_sql_type_timestamp_default() {
        use datafusion::sql::sqlparser::ast::{DataType as SqlType, TimezoneInfo};
        assert_sql_type_to_paimon(
            SqlType::Timestamp(None, TimezoneInfo::None),
            PaimonDataType::Timestamp(TimestampType::with_nullable(true, 3).unwrap()),
        );
    }

    #[test]
    fn test_sql_type_timestamp_with_precision() {
        use datafusion::sql::sqlparser::ast::{DataType as SqlType, TimezoneInfo};
        assert_sql_type_to_paimon(
            SqlType::Timestamp(Some(0), TimezoneInfo::None),
            PaimonDataType::Timestamp(TimestampType::with_nullable(true, 0).unwrap()),
        );
        assert_sql_type_to_paimon(
            SqlType::Timestamp(Some(3), TimezoneInfo::None),
            PaimonDataType::Timestamp(TimestampType::with_nullable(true, 3).unwrap()),
        );
        assert_sql_type_to_paimon(
            SqlType::Timestamp(Some(6), TimezoneInfo::None),
            PaimonDataType::Timestamp(TimestampType::with_nullable(true, 6).unwrap()),
        );
        assert_sql_type_to_paimon(
            SqlType::Timestamp(Some(9), TimezoneInfo::None),
            PaimonDataType::Timestamp(TimestampType::with_nullable(true, 9).unwrap()),
        );
    }

    #[test]
    fn test_sql_type_timestamp_with_tz() {
        use datafusion::sql::sqlparser::ast::{DataType as SqlType, TimezoneInfo};
        assert_sql_type_to_paimon(
            SqlType::Timestamp(None, TimezoneInfo::WithTimeZone),
            PaimonDataType::LocalZonedTimestamp(
                LocalZonedTimestampType::with_nullable(true, 3).unwrap(),
            ),
        );
    }

    #[test]
    fn test_sql_type_decimal() {
        use datafusion::sql::sqlparser::ast::{DataType as SqlType, ExactNumberInfo};
        assert_sql_type_to_paimon(
            SqlType::Decimal(ExactNumberInfo::PrecisionAndScale(18, 2)),
            PaimonDataType::Decimal(DecimalType::with_nullable(true, 18, 2).unwrap()),
        );
        assert_sql_type_to_paimon(
            SqlType::Decimal(ExactNumberInfo::Precision(10)),
            PaimonDataType::Decimal(DecimalType::with_nullable(true, 10, 0).unwrap()),
        );
        assert_sql_type_to_paimon(
            SqlType::Decimal(ExactNumberInfo::None),
            PaimonDataType::Decimal(DecimalType::with_nullable(true, 10, 0).unwrap()),
        );
    }

    #[test]
    fn test_sql_type_unsupported() {
        use datafusion::sql::sqlparser::ast::DataType as SqlType;
        assert!(sql_data_type_to_paimon_type(&SqlType::Regclass, true).is_err());
    }

    #[test]
    fn test_sql_type_array() {
        use datafusion::sql::sqlparser::ast::{ArrayElemTypeDef, DataType as SqlType};
        assert_sql_type_to_paimon(
            SqlType::Array(ArrayElemTypeDef::AngleBracket(Box::new(SqlType::Int(None)))),
            PaimonDataType::Array(PaimonArrayType::with_nullable(
                true,
                PaimonDataType::Int(IntType::new()),
            )),
        );
    }

    #[test]
    fn test_sql_type_array_no_element() {
        use datafusion::sql::sqlparser::ast::{ArrayElemTypeDef, DataType as SqlType};
        assert!(
            sql_data_type_to_paimon_type(&SqlType::Array(ArrayElemTypeDef::None), true).is_err()
        );
    }

    #[test]
    fn test_sql_type_map() {
        use datafusion::sql::sqlparser::ast::DataType as SqlType;
        assert_sql_type_to_paimon(
            SqlType::Map(
                Box::new(SqlType::Varchar(None)),
                Box::new(SqlType::Int(None)),
            ),
            PaimonDataType::Map(PaimonMapType::with_nullable(
                true,
                PaimonDataType::VarChar(
                    VarCharType::with_nullable(false, VarCharType::MAX_LENGTH).unwrap(),
                ),
                PaimonDataType::Int(IntType::new()),
            )),
        );
    }

    #[test]
    fn test_sql_type_struct() {
        use datafusion::sql::sqlparser::ast::{
            DataType as SqlType, Ident, StructBracketKind, StructField,
        };
        assert_sql_type_to_paimon(
            SqlType::Struct(
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
            ),
            PaimonDataType::Row(PaimonRowType::with_nullable(
                true,
                vec![
                    PaimonDataField::new(
                        0,
                        "name".to_string(),
                        PaimonDataType::VarChar(
                            VarCharType::with_nullable(true, VarCharType::MAX_LENGTH).unwrap(),
                        ),
                    ),
                    PaimonDataField::new(1, "age".to_string(), PaimonDataType::Int(IntType::new())),
                ],
            )),
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

    // ==================== PaimonSqlHandler::sql integration tests ====================

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
    async fn test_create_table_blob_type_preserved() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());

        handler
            .sql("CREATE TABLE mydb.t1 (id INT, payload BLOB NOT NULL) WITH ('data-evolution.enabled' = 'true')")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::CreateTable { schema, .. } = &calls[0] {
            assert_eq!(schema.fields().len(), 2);
            assert!(matches!(
                schema.fields()[1].data_type(),
                PaimonDataType::Blob(_)
            ));
            assert!(!schema.fields()[1].data_type().is_nullable());
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
    async fn test_alter_table_add_blob_column() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());

        handler
            .sql("ALTER TABLE mydb.t1 ADD COLUMN payload BLOB")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::AlterTable { changes, .. } = &calls[0] {
            assert_eq!(changes.len(), 1);
            assert!(matches!(
                &changes[0],
                SchemaChange::AddColumn {
                    field_name,
                    data_type,
                    ..
                } if field_name == "payload" && matches!(data_type, PaimonDataType::Blob(_))
            ));
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

    // ==================== extract_partition_by tests ====================

    #[test]
    fn test_extract_partition_by_no_clause() {
        let (rewritten, keys) = extract_partition_by("CREATE TABLE t (id INT)").unwrap();
        assert_eq!(rewritten, "CREATE TABLE t (id INT)");
        assert!(keys.is_empty());
    }

    #[test]
    fn test_extract_partition_by_single_column() {
        let (rewritten, keys) = extract_partition_by(
            "CREATE TABLE t (id INT, dt STRING) PARTITIONED BY (dt) WITH ('k'='v')",
        )
        .unwrap();
        assert_eq!(keys, vec!["dt"]);
        assert!(!rewritten.contains("PARTITIONED"));
        assert!(rewritten.contains("WITH"));
    }

    #[test]
    fn test_extract_partition_by_multiple_columns() {
        let (_, keys) =
            extract_partition_by("CREATE TABLE t (a INT, b INT, c INT) PARTITIONED BY (a, b)")
                .unwrap();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn test_extract_partition_by_mixed_case() {
        let (_, keys) =
            extract_partition_by("CREATE TABLE t (dt INT) Partitioned by (dt)").unwrap();
        assert_eq!(keys, vec!["dt"]);
    }

    #[test]
    fn test_extract_partition_by_rejects_typed_column() {
        let err = extract_partition_by("CREATE TABLE t (dt STRING) PARTITIONED BY (dt STRING)")
            .unwrap_err();
        assert!(err.to_string().contains("should not specify a type"));
    }

    #[test]
    fn test_extract_partition_by_empty_parens() {
        let err = extract_partition_by("CREATE TABLE t (id INT) PARTITIONED BY ()").unwrap_err();
        assert!(err.to_string().contains("at least one column"));
    }

    #[test]
    fn test_extract_partition_by_unmatched_paren() {
        let err = extract_partition_by("CREATE TABLE t (id INT) PARTITIONED BY (dt").unwrap_err();
        assert!(err.to_string().contains("Unmatched"));
    }

    #[test]
    fn test_extract_partition_by_skips_string_literal() {
        let sql =
            "CREATE TABLE t (id INT) WITH ('note' = 'PARTITIONED BY (x)') PARTITIONED BY (id)";
        let (rewritten, keys) = extract_partition_by(sql).unwrap();
        assert_eq!(keys, vec!["id"]);
        assert!(rewritten.contains("WITH"));
        assert!(rewritten.contains("'PARTITIONED BY (x)'"));
    }

    #[test]
    fn test_extract_partition_by_skips_line_comment() {
        let sql = "CREATE TABLE t (id INT) -- PARTITIONED BY (x)\nPARTITIONED BY (id)";
        let (_, keys) = extract_partition_by(sql).unwrap();
        assert_eq!(keys, vec!["id"]);
    }

    #[test]
    fn test_extract_partition_by_double_quoted_identifier() {
        let (_, keys) =
            extract_partition_by("CREATE TABLE t (\"order\" INT) PARTITIONED BY (\"order\")")
                .unwrap();
        assert_eq!(keys, vec!["order"]);
    }

    #[test]
    fn test_extract_partition_by_backtick_quoted_identifier() {
        let (_, keys) =
            extract_partition_by("CREATE TABLE t (`order` INT) PARTITIONED BY (`order`)").unwrap();
        assert_eq!(keys, vec!["order"]);
    }

    #[test]
    fn test_extract_partition_by_no_paren_after_by() {
        let err = extract_partition_by("CREATE TABLE t (id INT) PARTITIONED BY dt").unwrap_err();
        assert!(err.to_string().contains("Expected '('"));
    }

    #[test]
    fn test_extract_partition_by_only_partitioned_no_by() {
        let (rewritten, keys) = extract_partition_by("CREATE TABLE partitioned (id INT)").unwrap();
        assert_eq!(rewritten, "CREATE TABLE partitioned (id INT)");
        assert!(keys.is_empty());
    }

    #[test]
    fn test_extract_partition_by_skips_block_comment() {
        let sql = "CREATE TABLE t (id INT) /* PARTITIONED BY (x) */ PARTITIONED BY (id)";
        let (rewritten, keys) = extract_partition_by(sql).unwrap();
        assert_eq!(keys, vec!["id"]);
        assert!(rewritten.contains("/* PARTITIONED BY (x) */"));
    }

    #[test]
    fn test_looks_like_create_table() {
        assert!(looks_like_create_table("CREATE TABLE t (id INT)"));
        assert!(looks_like_create_table("  create  table t (id INT)"));
        assert!(looks_like_create_table(
            "CREATE TABLE IF NOT EXISTS t (id INT)",
        ));
        assert!(looks_like_create_table(
            "/* note */ CREATE TABLE t (id INT)",
        ));
        assert!(looks_like_create_table(
            "-- comment\nCREATE TABLE t (id INT)",
        ));
        assert!(looks_like_create_table(
            "/* a */ /* b */ CREATE TABLE t (id INT)",
        ));
        assert!(!looks_like_create_table("ALTER TABLE t ADD COLUMN x INT"));
        assert!(!looks_like_create_table("SELECT 1"));
        assert!(!looks_like_create_table(
            "SELECT aaaaaaaaaaaaaaaaaaaa中文 FROM t",
        ));
    }

    // ==================== partition key validation tests ====================

    #[tokio::test]
    async fn test_create_table_partition_key_not_in_columns() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog);
        let err = handler
            .sql("CREATE TABLE mydb.t (id INT, dt STRING) PARTITIONED BY (nonexistent)")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("is not defined in the table"));
    }

    #[tokio::test]
    async fn test_create_table_partition_key_matches_column() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog.clone());
        handler
            .sql("CREATE TABLE mydb.t (id INT, dt STRING) PARTITIONED BY (dt)")
            .await
            .unwrap();
        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::CreateTable { schema, .. } = &calls[0] {
            assert_eq!(schema.partition_keys(), &["dt"]);
        } else {
            panic!("expected CreateTable call");
        }
    }

    // ==================== SET / RESET dynamic options tests ====================

    #[tokio::test]
    async fn test_set_paimon_option() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog);
        handler
            .sql("SET 'paimon.scan.version' = '1'")
            .await
            .unwrap();
        let opts = handler.dynamic_options().read().unwrap();
        assert_eq!(opts.get("scan.version").unwrap(), "1");
    }

    #[tokio::test]
    async fn test_set_paimon_option_overwrites() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog);
        handler
            .sql("SET 'paimon.scan.version' = '1'")
            .await
            .unwrap();
        handler
            .sql("SET 'paimon.scan.version' = '2'")
            .await
            .unwrap();
        let opts = handler.dynamic_options().read().unwrap();
        assert_eq!(opts.get("scan.version").unwrap(), "2");
    }

    #[tokio::test]
    async fn test_reset_paimon_option() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog);
        handler
            .sql("SET 'paimon.scan.version' = '1'")
            .await
            .unwrap();
        handler.sql("RESET 'paimon.scan.version'").await.unwrap();
        let opts = handler.dynamic_options().read().unwrap();
        assert!(opts.get("scan.version").is_none());
    }

    #[tokio::test]
    async fn test_set_non_paimon_option_delegates() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog);
        // DataFusion handles non-paimon SET; should not error and should not
        // appear in dynamic_options.
        let _ = handler.sql("SET datafusion.optimizer.max_passes = 3").await;
        let opts = handler.dynamic_options().read().unwrap();
        assert!(opts.is_empty());
    }

    #[tokio::test]
    async fn test_set_multiple_paimon_options() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog);
        handler
            .sql("SET 'paimon.scan.version' = '1'")
            .await
            .unwrap();
        handler
            .sql("SET 'paimon.scan.timestamp-millis' = '1000'")
            .await
            .unwrap();
        let opts = handler.dynamic_options().read().unwrap();
        assert_eq!(opts.len(), 2);
        assert_eq!(opts.get("scan.version").unwrap(), "1");
        assert_eq!(opts.get("scan.timestamp-millis").unwrap(), "1000");
    }

    #[tokio::test]
    async fn test_reset_nonexistent_paimon_option_is_noop() {
        let catalog = Arc::new(MockCatalog::new());
        let handler = make_handler(catalog);
        handler.sql("RESET 'paimon.scan.version'").await.unwrap();
        let opts = handler.dynamic_options().read().unwrap();
        assert!(opts.is_empty());
    }

    // ==================== TRUNCATE TABLE / DROP PARTITIONS tests ====================

    async fn setup_fs_handler() -> (tempfile::TempDir, PaimonSqlHandler) {
        use paimon::{CatalogOptions, FileSystemCatalog, Options};

        let temp_dir = tempfile::TempDir::new().unwrap();
        let warehouse = format!("file://{}", temp_dir.path().display());
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse);
        let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());

        let handler =
            PaimonSqlHandler::new(SessionContext::new(), catalog.clone(), "paimon").unwrap();
        handler.sql("CREATE SCHEMA paimon.test_db").await.unwrap();

        (temp_dir, handler)
    }

    #[tokio::test]
    async fn test_truncate_table() {
        let (_tmp, handler) = setup_fs_handler().await;

        handler
            .sql("CREATE TABLE paimon.test_db.t1 (id INT, value INT)")
            .await
            .unwrap();
        handler
            .sql("INSERT INTO paimon.test_db.t1 VALUES (1, 10), (2, 20)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        handler
            .sql("TRUNCATE TABLE paimon.test_db.t1")
            .await
            .unwrap();

        let batches = handler
            .sql("SELECT * FROM paimon.test_db.t1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn test_truncate_table_partition() {
        let (_tmp, handler) = setup_fs_handler().await;

        handler
            .sql("CREATE TABLE paimon.test_db.t2 (pt VARCHAR, id INT) PARTITIONED BY (pt)")
            .await
            .unwrap();
        handler
            .sql("INSERT INTO paimon.test_db.t2 VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        handler
            .sql("TRUNCATE TABLE paimon.test_db.t2 PARTITION (pt = 'a')")
            .await
            .unwrap();

        let batches = handler
            .sql("SELECT pt, id FROM paimon.test_db.t2 ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut rows = Vec::new();
        for batch in &batches {
            let pts = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((pts.value(i).to_string(), ids.value(i)));
            }
        }
        assert_eq!(rows, vec![("b".to_string(), 3), ("b".to_string(), 4)]);
    }

    #[tokio::test]
    async fn test_alter_table_drop_partitions() {
        let (_tmp, handler) = setup_fs_handler().await;

        handler
            .sql("CREATE TABLE paimon.test_db.t3 (pt VARCHAR, id INT) PARTITIONED BY (pt)")
            .await
            .unwrap();
        handler
            .sql("INSERT INTO paimon.test_db.t3 VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        handler
            .sql("ALTER TABLE paimon.test_db.t3 DROP PARTITION (pt = 'b')")
            .await
            .unwrap();

        let batches = handler
            .sql("SELECT pt, id FROM paimon.test_db.t3 ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut rows = Vec::new();
        for batch in &batches {
            let pts = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((pts.value(i).to_string(), ids.value(i)));
            }
        }
        assert_eq!(rows, vec![("a".to_string(), 1), ("a".to_string(), 2)]);
    }

    #[tokio::test]
    async fn test_truncate_table_incomplete_partition_spec() {
        let (_tmp, handler) = setup_fs_handler().await;

        handler
            .sql("CREATE TABLE paimon.test_db.t_multi (pt1 VARCHAR, pt2 VARCHAR, id INT) PARTITIONED BY (pt1, pt2)")
            .await
            .unwrap();
        handler
            .sql("INSERT INTO paimon.test_db.t_multi VALUES ('a', 'x', 1)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let err = handler
            .sql("TRUNCATE TABLE paimon.test_db.t_multi PARTITION (pt1 = 'a')")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Incomplete partition spec"),
            "Expected incomplete partition spec error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_truncate_table_if_exists_nonexistent() {
        let (_tmp, handler) = setup_fs_handler().await;

        handler
            .sql("TRUNCATE TABLE IF EXISTS paimon.test_db.nonexistent")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_truncate_table_nonexistent_without_if_exists() {
        let (_tmp, handler) = setup_fs_handler().await;

        let err = handler
            .sql("TRUNCATE TABLE paimon.test_db.nonexistent")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("does not exist"),
            "Expected table-not-exist error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_alter_table_if_exists_drop_partition_nonexistent() {
        let (_tmp, handler) = setup_fs_handler().await;

        handler
            .sql("ALTER TABLE IF EXISTS paimon.test_db.nonexistent DROP PARTITION (pt = 'a')")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_drop_partition_incomplete_spec() {
        let (_tmp, handler) = setup_fs_handler().await;

        handler
            .sql("CREATE TABLE paimon.test_db.t_dp (pt1 VARCHAR, pt2 VARCHAR, id INT) PARTITIONED BY (pt1, pt2)")
            .await
            .unwrap();
        handler
            .sql("INSERT INTO paimon.test_db.t_dp VALUES ('a', 'x', 1)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let err = handler
            .sql("ALTER TABLE paimon.test_db.t_dp DROP PARTITION (pt1 = 'a')")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Incomplete partition spec"),
            "Expected incomplete partition spec error, got: {err}"
        );
    }
}
