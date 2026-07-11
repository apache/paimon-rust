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
//! This module provides [`SQLContext`] which intercepts CREATE TABLE,
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
//! - `CREATE VIEW [IF NOT EXISTS] view [(col, ...)] AS query`
//! - `DROP VIEW [IF EXISTS] view`
//! - `CREATE FUNCTION name(args) RETURNS type [LANGUAGE SQL] RETURN expression`
//! - `TRUNCATE TABLE db.t`
//! - `TRUNCATE TABLE db.t PARTITION (col = val, ...)`

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{
    new_null_array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::TableReference;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{Expr as LogicalExpr, LogicalPlan, Volatility};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::sqlparser::ast::{
    AlterTableOperation, BinaryLength, CharacterLength, ColumnDef, ColumnOption, CreateFunction,
    CreateFunctionBody, CreateTable, CreateTableOptions, CreateView, Delete, Expr as SqlExpr,
    FromTable, FunctionBehavior, FunctionReturnType, Insert, Merge, ObjectName, ObjectType,
    RenameTableNameKind, Reset, ResetStatement, Set, ShowCreateObject, SqlOption, Statement,
    TableFactor, TableObject, Truncate, Update, Value as SqlValue,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use futures::StreamExt;
use paimon::catalog::{parse_object_name, Catalog, Identifier};
use paimon::spec::{
    ArrayType as PaimonArrayType, BigIntType, BinaryType, BlobType, BooleanType, CharType,
    DataField as PaimonDataField, DataType as PaimonDataType, DateType, Datum, DecimalType,
    DoubleType, FloatType, IntType, LocalZonedTimestampType, MapType as PaimonMapType,
    RowType as PaimonRowType, SchemaChange, SmallIntType, TimestampType, TinyIntType,
    VarBinaryType, VarCharType, VariantType,
};

use crate::error::to_datafusion_error;
use crate::table_loader::load_table_for_read;
use crate::{BlobReaderRegistry, DynamicOptions};

/// A SQL context that supports registering multiple Paimon catalogs and executing SQL.
///
/// # Example
/// ```ignore
/// let mut ctx = SQLContext::new();
/// ctx.register_catalog("paimon", catalog).await?;
/// ctx.set_current_catalog("paimon").await?;
/// let df = ctx.sql("ALTER TABLE paimon.db.t ADD COLUMN age INT").await?;
/// ```
pub struct SQLContext {
    ctx: SessionContext,
    catalogs: HashMap<String, Arc<dyn Catalog>>,
    /// Session-scoped dynamic options set via `SET 'paimon.key' = 'value'`.
    dynamic_options: DynamicOptions,
    blob_reader_registry: BlobReaderRegistry,
}

impl Default for SQLContext {
    fn default() -> Self {
        Self::new()
    }
}

impl SQLContext {
    /// Creates a new empty SQL context.
    pub fn new() -> Self {
        let state = SessionStateBuilder::new()
            .with_config(crate::lateral_vector_search::session_config())
            .with_default_features()
            .with_relation_planners(vec![Arc::new(
                crate::relation_planner::PaimonRelationPlanner::new(),
            )])
            .with_optimizer_rules(crate::lateral_vector_search::optimizer_rules())
            .with_query_planner(Arc::new(
                crate::lateral_vector_search::PaimonQueryPlanner::new(),
            ))
            .build();
        let ctx = SessionContext::new_with_state(state);
        crate::variant_functions::register_variant_functions(&ctx);
        Self {
            ctx,
            catalogs: HashMap::new(),
            dynamic_options: Default::default(),
            blob_reader_registry: BlobReaderRegistry::default(),
        }
    }

    pub fn blob_reader_registry(&self) -> BlobReaderRegistry {
        self.blob_reader_registry.clone()
    }

    /// Registers a Paimon catalog under the given name.
    ///
    /// The first registered catalog automatically becomes the current catalog
    /// for both Paimon-handled SQL and DataFusion-delegated SQL (SELECT, etc.).
    /// A "default" database is created if it does not already exist (matching
    /// the behavior of Spark/Flink Paimon catalogs).
    pub async fn register_catalog(
        &mut self,
        catalog_name: impl Into<String>,
        catalog: Arc<dyn Catalog>,
    ) -> DFResult<()> {
        self.register_catalog_with_default_db(catalog_name, catalog, Some("default"))
            .await
    }

    /// Like [`Self::register_catalog`] but lets the caller control default-database init.
    ///
    /// `default_db = Some(name)` ensures `name` exists and sets it as current on the
    /// first catalog. `default_db = None` skips both — required for principals that
    /// lack DESCRIBE / CREATEDATABASE on `default`. Mirrors Java `FlinkCatalog`'s
    /// `defaultDatabase` / `DISABLE_CREATE_TABLE_IN_DEFAULT_DB`.
    ///
    /// `default_db = Some("")` is rejected — pass `None` to opt out instead.
    ///
    /// **Note on built-in TVFs (`vector_search`, `full_text_search`):** when
    /// `default_db = None`, bare table names inside these functions still resolve
    /// against the literal namespace `"default"` (the fallback in
    /// [`register_table_functions`]). Callers using `None` must qualify table names
    /// (`'db.table'` or `'catalog.db.table'`) in those calls.
    pub async fn register_catalog_with_default_db(
        &mut self,
        catalog_name: impl Into<String>,
        catalog: Arc<dyn Catalog>,
        default_db: Option<&str>,
    ) -> DFResult<()> {
        if matches!(default_db, Some("")) {
            return Err(DataFusionError::Plan(
                "default_db must not be empty; pass None to skip default-database init".to_string(),
            ));
        }
        let catalog_name = catalog_name.into();
        let is_first = self.catalogs.is_empty();
        if let Some(default_db) = default_db {
            match catalog.get_database(default_db).await {
                Ok(_) => {}
                Err(paimon::Error::DatabaseNotExist { .. }) => {
                    catalog
                        .create_database(default_db, true, Default::default())
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                }
                Err(e) => return Err(DataFusionError::External(Box::new(e))),
            }
        }
        let weak_state = self.ctx.state_weak_ref();
        let session_state: crate::catalog::SessionStateProvider =
            Arc::new(move || weak_state.upgrade().map(|state| state.read().clone()));
        self.ctx.register_catalog(
            &catalog_name,
            Arc::new(crate::catalog::PaimonCatalogProvider::new(
                Some(catalog_name.clone()),
                catalog.clone(),
                self.dynamic_options.clone(),
                self.blob_reader_registry.clone(),
                Some(session_state),
            )),
        );
        register_table_functions(&self.ctx, &catalog, default_db.unwrap_or("default"));
        self.catalogs.insert(catalog_name.clone(), catalog);
        if is_first {
            self.set_current_catalog(catalog_name).await?;
            if let Some(default_db) = default_db {
                self.set_current_database(default_db).await?;
            }
        }
        Ok(())
    }

    /// Sets the current catalog for unqualified table references.
    pub async fn set_current_catalog(&mut self, catalog_name: impl Into<String>) -> DFResult<()> {
        let catalog_name = catalog_name.into();
        if !self.catalogs.contains_key(&catalog_name) {
            return Err(DataFusionError::Plan(format!(
                "Unknown catalog '{catalog_name}'"
            )));
        }
        if catalog_name.contains('\'') {
            return Err(DataFusionError::Plan(
                "Catalog name must not contain single quotes".to_string(),
            ));
        }
        self.ctx
            .sql(&format!(
                "SET datafusion.catalog.default_catalog = '{catalog_name}'"
            ))
            .await?;
        Ok(())
    }

    /// Sets the current database for unqualified table references.
    pub async fn set_current_database(&self, database_name: &str) -> DFResult<()> {
        if database_name.contains('\'') {
            return Err(DataFusionError::Plan(
                "Database name must not contain single quotes".to_string(),
            ));
        }
        self.ctx
            .sql(&format!(
                "SET datafusion.catalog.default_schema = '{database_name}'"
            ))
            .await?;
        Ok(())
    }

    /// Returns a reference to the inner [`SessionContext`].
    pub fn ctx(&self) -> &SessionContext {
        &self.ctx
    }

    /// Registers a temporary in-memory table or view.
    ///
    /// The `name` parameter accepts flexible table references, similar to DataFusion:
    /// - `"my_table"` — uses the current catalog and current database
    /// - `"database.my_table"` — uses the current catalog with the specified database
    /// - `"catalog.database.my_table"` — fully qualified
    ///
    /// The table exists only for the lifetime of this SQLContext instance.
    pub fn register_temp_table(
        &self,
        name: impl Into<TableReference>,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<()> {
        let (catalog, database, table_name) = self.resolve_temp_table_name(name.into())?;
        let catalog_provider = self
            .ctx
            .catalog(&catalog)
            .ok_or_else(|| DataFusionError::Plan(format!("Unknown catalog '{catalog}'")))?;

        let paimon_provider = catalog_provider
            .downcast_ref::<crate::catalog::PaimonCatalogProvider>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!("Catalog '{catalog}' is not a Paimon catalog"))
            })?;

        paimon_provider.register_temp_table(&database, &table_name, table)
    }

    /// Deregisters a temporary table or view.
    ///
    /// Accepts the same flexible name format as `register_temp_table`.
    pub fn deregister_temp_table(
        &self,
        name: impl Into<TableReference>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let (catalog, database, table_name) = self.resolve_temp_table_name(name.into())?;
        let catalog_provider = self
            .ctx
            .catalog(&catalog)
            .ok_or_else(|| DataFusionError::Plan(format!("Unknown catalog '{catalog}'")))?;

        let paimon_provider = catalog_provider
            .downcast_ref::<crate::catalog::PaimonCatalogProvider>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!("Catalog '{catalog}' is not a Paimon catalog"))
            })?;

        paimon_provider.deregister_temp_table(&database, &table_name)
    }

    /// Returns whether a temporary table or view with the given name already exists.
    ///
    /// Accepts the same flexible name format as `register_temp_table`.
    pub fn temp_table_exist(&self, name: impl Into<TableReference>) -> DFResult<bool> {
        let (catalog, database, table_name) = self.resolve_temp_table_name(name.into())?;
        let catalog_provider = self
            .ctx
            .catalog(&catalog)
            .ok_or_else(|| DataFusionError::Plan(format!("Unknown catalog '{catalog}'")))?;

        let paimon_provider = catalog_provider
            .downcast_ref::<crate::catalog::PaimonCatalogProvider>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!("Catalog '{catalog}' is not a Paimon catalog"))
            })?;

        Ok(paimon_provider.temp_table_exist(&database, &table_name))
    }

    /// Resolve a TableReference into (catalog, database, table_name).
    fn resolve_temp_table_name(&self, name: TableReference) -> DFResult<(String, String, String)> {
        match name {
            TableReference::Bare { table } => {
                let catalog = self.current_catalog_name();
                let database = self
                    .ctx
                    .state()
                    .config_options()
                    .catalog
                    .default_schema
                    .clone();
                Ok((catalog, database, table.to_string()))
            }
            TableReference::Partial { schema, table } => {
                let catalog = self.current_catalog_name();
                Ok((catalog, schema.to_string(), table.to_string()))
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => Ok((catalog.to_string(), schema.to_string(), table.to_string())),
        }
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
        if contains_time_travel_keyword(&rewritten_sql) {
            // Time-travel queries are not DDL; skip our own parsing and handle directly.
            return self.handle_time_travel_query(&rewritten_sql).await;
        }

        let statements = parse_sql_statements(&rewritten_sql)?;

        if statements.len() != 1 {
            return Err(DataFusionError::Plan(
                "Expected exactly one SQL statement".to_string(),
            ));
        }

        match &statements[0] {
            Statement::CreateTable(create_table) => {
                if create_table.temporary {
                    self.handle_create_temp_table(create_table).await
                } else {
                    let (catalog, _catalog_name, _) =
                        self.resolve_catalog_and_table(&create_table.name)?;
                    self.handle_create_table(&catalog, create_table, partition_keys)
                        .await
                }
            }
            Statement::ShowCreate {
                obj_type: ShowCreateObject::Table,
                obj_name,
            } => self.handle_show_create_table(sql, obj_name).await,
            Statement::AlterTable(alter_table) => {
                let (catalog, _catalog_name, _) =
                    self.resolve_catalog_and_table(&alter_table.name)?;
                self.handle_alter_table(
                    &catalog,
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
            Statement::CreateView(create_view) => {
                if create_view.temporary {
                    // Temporary views are always handled by us (Paimon catalog temp storage)
                    self.handle_create_view(create_view).await
                } else {
                    // Non-temporary views: only intercept if the target catalog is Paimon
                    let view_name = create_view.name.to_string();
                    let table_ref: TableReference = view_name.as_str().into();
                    if self.is_paimon_catalog_ref(&table_ref) {
                        self.handle_create_view(create_view).await
                    } else {
                        self.ctx.sql(sql).await
                    }
                }
            }
            Statement::CreateFunction(create_function) => {
                if self.is_paimon_function_name(&create_function.name) {
                    self.handle_create_function(create_function).await
                } else {
                    self.ctx.sql(sql).await
                }
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                restrict,
                purge,
                temporary,
                table,
            } if matches!(*object_type, ObjectType::Table | ObjectType::View) => {
                if *temporary {
                    self.handle_drop_temp_table(names, *if_exists)
                } else if *object_type == ObjectType::Table {
                    // Only intercept DROP TABLE for Paimon catalogs; fall through for others
                    let table_ref: TableReference = names[0].to_string().as_str().into();
                    if self.is_paimon_catalog_ref(&table_ref) {
                        let (catalog, _catalog_name, _) =
                            self.resolve_catalog_and_table(&names[0])?;
                        self.handle_drop_table(&catalog, names, *if_exists).await
                    } else {
                        self.ctx.sql(sql).await
                    }
                } else {
                    let targets_paimon_catalog = names.iter().any(|name| {
                        let table_ref: TableReference = name.to_string().as_str().into();
                        self.is_paimon_catalog_ref(&table_ref)
                    });
                    if !targets_paimon_catalog {
                        return self.ctx.sql(sql).await;
                    }
                    let [name] = names.as_slice() else {
                        return Err(DataFusionError::Plan(
                            "Persistent DROP VIEW does not support multiple views".to_string(),
                        ));
                    };
                    if *cascade {
                        return Err(DataFusionError::Plan(
                            "DROP VIEW CASCADE is not supported".to_string(),
                        ));
                    }
                    if *restrict {
                        return Err(DataFusionError::Plan(
                            "DROP VIEW RESTRICT is not supported".to_string(),
                        ));
                    }
                    if *purge {
                        return Err(DataFusionError::Plan(
                            "DROP VIEW PURGE is not supported".to_string(),
                        ));
                    }
                    if table.is_some() {
                        return Err(DataFusionError::Plan(
                            "DROP VIEW ON clauses are not supported".to_string(),
                        ));
                    }
                    let (catalog, _catalog_name, identifier) =
                        self.resolve_catalog_and_table(name)?;
                    self.handle_drop_view(&catalog, &identifier, *if_exists)
                        .await
                }
            }
            Statement::Call(func) => {
                crate::procedures::execute_call(
                    &self.ctx,
                    &self.catalogs,
                    &self.current_catalog_name(),
                    func,
                )
                .await
            }
            Statement::Query(_) | Statement::Explain { .. } => {
                let current_catalog = self.current_catalog_name();
                let current_database = self
                    .ctx
                    .state()
                    .config_options()
                    .catalog
                    .default_schema
                    .clone();
                let expanded = crate::sql_function::expand_statement(
                    statements[0].clone(),
                    &self.catalogs,
                    &current_catalog,
                    &current_database,
                )
                .await?;
                self.ctx.sql(&expanded.to_string()).await
            }
            _ => self.ctx.sql(sql).await,
        }
    }

    /// Handle SQL queries containing time-travel syntax (`VERSION AS OF` / `TIMESTAMP AS OF`).
    ///
    /// DataFusion's default SQL parser does not support these clauses, so we:
    /// 1. Extract all table name + version/timestamp pairs (skipping string literals and comments)
    /// 2. Strip the time-travel clauses from the SQL
    /// 3. For each table, create a `PaimonTableProvider` with the appropriate scan options
    ///    (merged with session-scoped dynamic options)
    /// 4. Register them as UUID-named temp tables, execute the rewritten SQL, then deregister
    async fn handle_time_travel_query(&self, sql: &str) -> DFResult<DataFrame> {
        use crate::table::PaimonTableProvider;
        use paimon::spec::{SCAN_TIMESTAMP_MILLIS_OPTION, SCAN_VERSION_OPTION};

        let mut tracker = crate::merge_into::TempTableTracker::new(self);

        let version_clauses = extract_all_version_as_of(sql);
        let timestamp_clauses = extract_all_timestamp_as_of(sql);

        if version_clauses.is_empty() && timestamp_clauses.is_empty() {
            return Err(DataFusionError::Plan(
                "Failed to parse time-travel clause in SQL".to_string(),
            ));
        }

        // Collect all replacements: (clause_range, uuid_name)
        let mut replacements: Vec<((usize, usize), String)> = Vec::new();

        // Process all VERSION AS OF clauses
        for info in &version_clauses {
            let table_ref: datafusion::common::TableReference = info.table_name.as_str().into();
            let (catalog, _catalog_name, identifier) =
                self.resolve_table_name_from_ref(&table_ref)?;

            let (paimon_table, base_identifier, system_name) =
                load_table_for_read(&catalog, &identifier).await?;

            // Merge dynamic options with time-travel options
            let mut options = self.dynamic_options.read().unwrap().clone();
            options.insert(SCAN_VERSION_OPTION.to_string(), info.version.clone());

            let table_with_options = paimon_table
                .copy_with_time_travel(options)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let provider: Arc<dyn TableProvider> = if let Some(system_name) = system_name {
                crate::system_tables::provider_for_table(
                    Arc::clone(&catalog),
                    base_identifier,
                    table_with_options,
                    &system_name,
                )?
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Unknown Paimon system table: {system_name}"))
                })?
            } else {
                Arc::new(PaimonTableProvider::try_new_with_blob_reader_registry(
                    table_with_options,
                    self.blob_reader_registry.clone(),
                )?)
            };

            let uuid_name = format!("__paimon_tt_{}", uuid::Uuid::new_v4().as_simple());
            self.register_temp_table(uuid_name.as_str(), provider)?;
            tracker.register(&uuid_name);
            replacements.push((info.clause_range, uuid_name));
        }

        // Process all TIMESTAMP AS OF clauses
        for info in &timestamp_clauses {
            let table_ref: datafusion::common::TableReference = info.table_name.as_str().into();
            let (catalog, _catalog_name, identifier) =
                self.resolve_table_name_from_ref(&table_ref)?;

            let (paimon_table, base_identifier, system_name) =
                load_table_for_read(&catalog, &identifier).await?;

            let millis = Self::parse_timestamp_to_millis(&info.timestamp)?;

            // Merge dynamic options with time-travel options
            let mut options = self.dynamic_options.read().unwrap().clone();
            options.insert(SCAN_TIMESTAMP_MILLIS_OPTION.to_string(), millis.to_string());

            let table_with_options = paimon_table
                .copy_with_time_travel(options)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let provider: Arc<dyn TableProvider> = if let Some(system_name) = system_name {
                crate::system_tables::provider_for_table(
                    Arc::clone(&catalog),
                    base_identifier,
                    table_with_options,
                    &system_name,
                )?
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Unknown Paimon system table: {system_name}"))
                })?
            } else {
                Arc::new(PaimonTableProvider::try_new_with_blob_reader_registry(
                    table_with_options,
                    self.blob_reader_registry.clone(),
                )?)
            };

            let uuid_name = format!("__paimon_tt_{}", uuid::Uuid::new_v4().as_simple());
            self.register_temp_table(uuid_name.as_str(), provider)?;
            tracker.register(&uuid_name);
            replacements.push((info.clause_range, uuid_name));
        }

        // Sort replacements by position (descending) so that replacements
        // from right to left don't shift indices of earlier ones
        replacements.sort_by_key(|r| std::cmp::Reverse(r.0 .0));

        // Build the rewritten SQL by replacing each clause from right to left
        let mut rewritten_sql = sql.to_string();
        for ((start, end), uuid_name) in &replacements {
            rewritten_sql = format!(
                "{}{}{}",
                &rewritten_sql[..*start],
                uuid_name,
                &rewritten_sql[*end..]
            );
        }

        // Execute the rewritten SQL; tracker auto-deregisters on drop.
        let current_catalog = self.current_catalog_name();
        let current_database = self
            .ctx
            .state()
            .config_options()
            .catalog
            .default_schema
            .clone();
        let expanded = crate::sql_function::expand_sql(
            &rewritten_sql,
            &self.catalogs,
            &current_catalog,
            &current_database,
        )
        .await?;
        self.ctx.sql(&expanded).await
    }

    /// Parse a timestamp string to milliseconds since epoch (using local timezone).
    fn parse_timestamp_to_millis(ts: &str) -> DFResult<i64> {
        use chrono::{Local, NaiveDateTime, TimeZone};

        let naive = NaiveDateTime::parse_from_str(ts, "%Y-%m-%d %H:%M:%S").map_err(|e| {
            DataFusionError::Plan(format!(
                "Cannot parse time travel timestamp '{ts}': {e}. Expected format: YYYY-MM-DD HH:MM:SS"
            ))
        })?;
        let local = Local.from_local_datetime(&naive).single().ok_or_else(|| {
            DataFusionError::Plan(format!("Ambiguous or invalid local time: '{ts}'"))
        })?;
        Ok(local.timestamp_millis())
    }

    /// Resolve a TableReference to (catalog, catalog_name, Identifier).
    fn resolve_table_name_from_ref(
        &self,
        table_ref: &datafusion::common::TableReference,
    ) -> DFResult<(Arc<dyn Catalog>, String, Identifier)> {
        match table_ref {
            datafusion::common::TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                let catalog_arc = self
                    .catalogs
                    .get(catalog.as_ref())
                    .ok_or_else(|| DataFusionError::Plan(format!("Unknown catalog '{catalog}'")))?;
                Ok((
                    catalog_arc.clone(),
                    catalog.to_string(),
                    Identifier::new(schema.as_ref(), table.as_ref()),
                ))
            }
            datafusion::common::TableReference::Partial { schema, table } => {
                let catalog = self.current_catalog()?;
                let catalog_name = self.current_catalog_name();
                Ok((
                    catalog,
                    catalog_name,
                    Identifier::new(schema.as_ref(), table.as_ref()),
                ))
            }
            datafusion::common::TableReference::Bare { table } => {
                let catalog = self.current_catalog()?;
                let catalog_name = self.current_catalog_name();
                let default_schema = self
                    .ctx
                    .state()
                    .config_options()
                    .catalog
                    .default_schema
                    .clone();
                Ok((
                    catalog,
                    catalog_name,
                    Identifier::new(default_schema, table.as_ref()),
                ))
            }
        }
    }

    async fn handle_create_table(
        &self,
        catalog: &Arc<dyn Catalog>,
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
        let table_options = extract_options(&ct.table_options)?;

        // Columns
        for col in &ct.columns {
            let paimon_type = column_def_to_paimon_type(col)?;
            let comment = column_def_comment(col);
            builder = builder.column_with_description(col.name.value.clone(), paimon_type, comment);
        }

        // Primary key from constraints: PRIMARY KEY (col, ...)
        for constraint in &ct.constraints {
            if let datafusion::sql::sqlparser::ast::TableConstraint::PrimaryKey(pk) = constraint {
                let pk_cols: Vec<String> = pk
                    .columns
                    .iter()
                    .map(|c| primary_key_column_name(&c.column.expr))
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
        for (k, v) in table_options {
            builder = builder.option(k, v);
        }

        let schema = builder.build().map_err(to_datafusion_error)?;

        catalog
            .create_table(&identifier, schema, ct.if_not_exists)
            .await
            .map_err(to_datafusion_error)?;

        ok_result(&self.ctx)
    }

    async fn handle_create_temp_table(&self, ct: &CreateTable) -> DFResult<DataFrame> {
        let table_ref: TableReference = ct.name.to_string().as_str().into();

        if ct.if_not_exists && self.temp_table_exist(table_ref.clone())? {
            return ok_result(&self.ctx);
        }

        // Build the schema from column definitions if provided
        let declared_schema = if !ct.columns.is_empty() {
            let fields: Vec<Field> = ct
                .columns
                .iter()
                .map(|col| {
                    let paimon_type =
                        sql_data_type_to_paimon_type(&col.data_type, column_def_nullable(col))?;
                    let arrow_type = paimon::arrow::paimon_type_to_arrow(&paimon_type)
                        .map_err(to_datafusion_error)?;
                    Ok(Field::new(
                        &col.name.value,
                        arrow_type,
                        column_def_nullable(col),
                    ))
                })
                .collect::<DFResult<Vec<_>>>()?;
            Some(Arc::new(Schema::new(fields)))
        } else {
            None
        };

        if let Some(query) = &ct.query {
            // CREATE TEMPORARY TABLE ... AS SELECT ...
            let query_sql = query.to_string();
            let df = self.ctx.sql(&query_sql).await?;
            let schema = df.schema().inner().clone();
            let batches = df.collect().await?;

            // If column types are specified, cast each column to the declared type
            let batches = if ct.columns.is_empty() {
                batches
            } else {
                let target_fields: Vec<(String, ArrowDataType)> = ct
                    .columns
                    .iter()
                    .map(|col| {
                        let paimon_type =
                            sql_data_type_to_paimon_type(&col.data_type, column_def_nullable(col))?;
                        let arrow_type = paimon::arrow::paimon_type_to_arrow(&paimon_type)
                            .map_err(to_datafusion_error)?;
                        Ok((col.name.value.clone(), arrow_type))
                    })
                    .collect::<DFResult<Vec<_>>>()?;

                let select_col_count = schema.fields().len();
                let declared_col_count = target_fields.len();
                if select_col_count < declared_col_count {
                    return Err(DataFusionError::Plan(format!(
                        "CREATE TEMPORARY TABLE AS SELECT: declared {declared_col_count} column(s) \
                         but SELECT query returns only {select_col_count} column(s)"
                    )));
                }

                batches
                    .into_iter()
                    .map(|batch| {
                        let columns = batch
                            .columns()
                            .iter()
                            .enumerate()
                            .map(|(i, col)| {
                                if i < target_fields.len() {
                                    let target_dt = &target_fields[i].1;
                                    if *col.data_type() != *target_dt {
                                        cast(col, target_dt)
                                            .map_err(|e| DataFusionError::External(e.into()))
                                    } else {
                                        Ok(col.clone())
                                    }
                                } else {
                                    Ok(col.clone())
                                }
                            })
                            .collect::<DFResult<Vec<_>>>()?;
                        let new_fields = target_fields
                            .iter()
                            .zip(schema.fields().iter())
                            .map(|((name, dt), _)| Field::new(name, dt.clone(), true))
                            .chain(
                                schema
                                    .fields()
                                    .iter()
                                    .skip(target_fields.len())
                                    .map(|f| f.as_ref().clone()),
                            )
                            .collect::<Vec<_>>();
                        let new_schema = Schema::new(new_fields);
                        RecordBatch::try_new(Arc::new(new_schema), columns)
                            .map_err(|e| DataFusionError::External(e.into()))
                    })
                    .collect::<DFResult<Vec<_>>>()?
            };

            let schema = batches.first().map(|b| b.schema()).unwrap_or(schema);
            let mem_table = MemTable::try_new(schema, vec![batches])?;
            self.register_temp_table(table_ref, Arc::new(mem_table))?;
        } else if let Some(schema) = declared_schema {
            // CREATE TEMPORARY TABLE (col1 TYPE, col2 TYPE, ...) — no data, just the schema
            let mem_table = MemTable::try_new(schema, vec![vec![]])?;
            self.register_temp_table(table_ref, Arc::new(mem_table))?;
        } else {
            return Err(DataFusionError::Plan(
                "CREATE TEMPORARY TABLE requires column definitions or AS SELECT".to_string(),
            ));
        }

        ok_result(&self.ctx)
    }

    fn handle_drop_temp_table(&self, names: &[ObjectName], if_exists: bool) -> DFResult<DataFrame> {
        for name in names {
            let table_ref: TableReference = name.to_string().as_str().into();
            if if_exists && !self.temp_table_exist(table_ref.clone())? {
                continue;
            }
            self.deregister_temp_table(table_ref)?;
        }
        ok_result(&self.ctx)
    }

    async fn handle_drop_table(
        &self,
        catalog: &Arc<dyn Catalog>,
        names: &[ObjectName],
        if_exists: bool,
    ) -> DFResult<DataFrame> {
        for name in names {
            let identifier = self.resolve_table_name(name)?;
            catalog
                .drop_table(&identifier, if_exists)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        ok_result(&self.ctx)
    }

    async fn handle_drop_view(
        &self,
        catalog: &Arc<dyn Catalog>,
        identifier: &Identifier,
        if_exists: bool,
    ) -> DFResult<DataFrame> {
        catalog
            .drop_view(identifier, if_exists)
            .await
            .map_err(to_datafusion_error)?;
        ok_result(&self.ctx)
    }

    async fn handle_show_create_table(&self, sql: &str, name: &ObjectName) -> DFResult<DataFrame> {
        let (catalog, catalog_name, identifier) = self.resolve_catalog_and_table(name)?;
        let table = match catalog.get_table(&identifier).await {
            Ok(table) => table,
            Err(paimon::Error::TableNotExist { .. }) => return self.ctx.sql(sql).await,
            Err(e) => return Err(to_datafusion_error(e)),
        };
        let definition = crate::table::build_table_definition(&table)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", ArrowDataType::Utf8, false),
            Field::new("table_schema", ArrowDataType::Utf8, false),
            Field::new("table_name", ArrowDataType::Utf8, false),
            Field::new("definition", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![catalog_name])),
                Arc::new(StringArray::from(vec![identifier.database().to_string()])),
                Arc::new(StringArray::from(vec![identifier.object().to_string()])),
                Arc::new(StringArray::from(vec![definition])),
            ],
        )?;
        self.ctx.read_batch(batch)
    }

    async fn handle_alter_table(
        &self,
        catalog: &Arc<dyn Catalog>,
        name: &ObjectName,
        operations: &[AlterTableOperation],
        if_exists: bool,
    ) -> DFResult<DataFrame> {
        Self::ensure_main_branch_write_target(name, "ALTER TABLE")?;
        let identifier = self.resolve_table_name(name)?;

        let mut changes = Vec::new();
        let mut rename_to: Option<Identifier> = None;

        for op in operations {
            match op {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    changes.push(column_def_to_add_column(column_def)?);
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
                            catalog,
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
            catalog
                .rename_table(&identifier, &new_identifier, if_exists)
                .await
                .map_err(to_datafusion_error)?;
        }

        if !changes.is_empty() {
            catalog
                .alter_table(&identifier, changes, if_exists)
                .await
                .map_err(to_datafusion_error)?;
        }

        ok_result(&self.ctx)
    }

    /// Reject write statements while a session-level time-travel selector is
    /// active.
    ///
    /// Writes always operate on the latest table state, but in the same
    /// session reads resolve through the time-travelled snapshot schema (and
    /// INSERT through the provider is rejected by the write builder), so
    /// silently ignoring the selector here would be inconsistent. Failing
    /// with a clear message is safer than writing against a different schema
    /// than concurrent reads observe.
    fn ensure_no_time_travel_for_write(&self, operation: &str) -> DFResult<()> {
        use paimon::spec::{
            SCAN_SNAPSHOT_ID_OPTION, SCAN_TAG_NAME_OPTION, SCAN_TIMESTAMP_MILLIS_OPTION,
            SCAN_VERSION_OPTION,
        };

        let options = self.dynamic_options.read().unwrap();
        for key in [
            SCAN_VERSION_OPTION,
            SCAN_TIMESTAMP_MILLIS_OPTION,
            SCAN_SNAPSHOT_ID_OPTION,
            SCAN_TAG_NAME_OPTION,
        ] {
            if options.contains_key(key) {
                return Err(DataFusionError::Plan(format!(
                    "Cannot execute {operation} while time-travel option '{key}' is set; \
                     RESET 'paimon.{key}' first"
                )));
            }
        }
        Ok(())
    }

    async fn handle_merge_into(&self, merge: &Merge) -> DFResult<DataFrame> {
        self.ensure_no_time_travel_for_write("MERGE INTO")?;
        let table_name = match &merge.table {
            TableFactor::Table { name, .. } => name.clone(),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported target table in MERGE INTO: {other}"
                )))
            }
        };
        Self::ensure_main_branch_write_target(&table_name, "MERGE INTO")?;
        let (catalog, _catalog_name, identifier) = self.resolve_catalog_and_table(&table_name)?;

        let table = catalog
            .get_table(&identifier)
            .await
            .map_err(to_datafusion_error)?;

        crate::merge_into::execute_merge_into(self, merge, table).await
    }

    async fn handle_update(&self, update: &Update) -> DFResult<DataFrame> {
        self.ensure_no_time_travel_for_write("UPDATE")?;
        let table_name = match &update.table.relation {
            TableFactor::Table { name, .. } => name.clone(),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported target table in UPDATE: {other}"
                )))
            }
        };
        Self::ensure_main_branch_write_target(&table_name, "UPDATE")?;
        let (catalog, _catalog_name, identifier) = self.resolve_catalog_and_table(&table_name)?;

        let table = catalog
            .get_table(&identifier)
            .await
            .map_err(to_datafusion_error)?;

        crate::update::execute_update(self, update, table).await
    }

    async fn handle_delete(&self, delete: &Delete) -> DFResult<DataFrame> {
        self.ensure_no_time_travel_for_write("DELETE")?;
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
        Self::ensure_main_branch_write_target(&table_name, "DELETE")?;
        let (catalog, _catalog_name, identifier) = self.resolve_catalog_and_table(&table_name)?;

        let table = catalog
            .get_table(&identifier)
            .await
            .map_err(to_datafusion_error)?;

        let table_ref = table_name.to_string();
        crate::delete::execute_delete(self, delete, table, &table_ref).await
    }

    async fn handle_insert_overwrite_partition(&self, insert: &Insert) -> DFResult<DataFrame> {
        self.ensure_no_time_travel_for_write("INSERT OVERWRITE")?;
        let table_name = match &insert.table {
            TableObject::TableName(name) => name.clone(),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported target table in INSERT OVERWRITE: {other}"
                )))
            }
        };
        Self::ensure_main_branch_write_target(&table_name, "INSERT OVERWRITE")?;
        let (catalog, _catalog_name, identifier) = self.resolve_catalog_and_table(&table_name)?;
        let table = catalog
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
        let target_columns: Option<Vec<String>> = if !insert.columns.is_empty() {
            Some(
                insert
                    .columns
                    .iter()
                    .map(object_name_to_single_identifier)
                    .collect::<DFResult<_>>()?,
            )
        } else if !insert.after_columns.is_empty() {
            Some(
                insert
                    .after_columns
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect(),
            )
        } else {
            None
        };
        let column_reorder: Option<Vec<usize>> = if let Some(cols) = target_columns.as_ref() {
            if cols.len() != expected_source_cols {
                return Err(DataFusionError::Plan(format!(
                    "Column list has {} columns, but expected {} non-partition columns",
                    cols.len(),
                    expected_source_cols
                )));
            }
            let col_names: Vec<&str> = cols.iter().map(String::as_str).collect();
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

        let wb = table.new_write_builder().with_overwrite();
        let mut tw = wb.new_write().map_err(to_datafusion_error)?;
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
        let commit = wb.try_new_commit().map_err(to_datafusion_error)?;

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
        self.ensure_no_time_travel_for_write("TRUNCATE TABLE")?;
        if truncate.table_names.len() > 1 {
            return Err(DataFusionError::Plan(
                "TRUNCATE TABLE does not support multiple tables".to_string(),
            ));
        }
        let target = truncate.table_names.first().ok_or_else(|| {
            DataFusionError::Plan("TRUNCATE TABLE requires a table name".to_string())
        })?;
        Self::ensure_main_branch_write_target(&target.name, "TRUNCATE TABLE")?;
        let (catalog, _catalog_name, identifier) = self.resolve_catalog_and_table(&target.name)?;
        let table = match catalog.get_table(&identifier).await {
            Ok(t) => t,
            Err(e) if truncate.if_exists && is_table_not_exist(&e) => {
                return ok_result(&self.ctx);
            }
            Err(e) => return Err(to_datafusion_error(e)),
        };

        let wb = table.new_write_builder();
        let commit = wb.try_new_commit().map_err(to_datafusion_error)?;

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

    async fn handle_create_view(&self, create_view: &CreateView) -> DFResult<DataFrame> {
        if create_view.materialized {
            return Err(DataFusionError::Plan(
                "CREATE MATERIALIZED VIEW is not supported".to_string(),
            ));
        }

        let query_sql = create_view.query.to_string();

        if create_view.temporary {
            let view_name = create_view.name.to_string();
            let table_ref: TableReference = view_name.as_str().into();
            let (catalog, database, name) = self.resolve_temp_table_name(table_ref)?;
            let df = self.ctx.sql(&query_sql).await?;
            let logical_plan = df.logical_plan().clone();
            if create_view.if_not_exists
                && self.temp_table_exist(format!("{catalog}.{database}.{name}"))?
            {
                return ok_result(&self.ctx);
            }
            // Create a ViewTable and register it as a temp table
            let view_table = datafusion::datasource::ViewTable::new(logical_plan, Some(query_sql));
            self.register_temp_table(format!("{catalog}.{database}.{name}"), Arc::new(view_table))?;
            ok_result(&self.ctx)
        } else {
            validate_persistent_create_view(create_view)?;
            let (catalog, catalog_name, identifier) =
                self.resolve_catalog_and_table(&create_view.name)?;
            let mut state = self.ctx.state();
            state.config_mut().options_mut().catalog.default_catalog = catalog_name.clone();
            state.config_mut().options_mut().catalog.default_schema =
                identifier.database().to_string();
            let expanded_query = crate::sql_function::expand_sql(
                &query_sql,
                &self.catalogs,
                &catalog_name,
                identifier.database(),
            )
            .await?;
            let logical_plan = state.create_logical_plan(&expanded_query).await?;
            let mut arrow_fields = logical_plan
                .schema()
                .as_arrow()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            if !create_view.columns.is_empty() && create_view.columns.len() != arrow_fields.len() {
                return Err(DataFusionError::Plan(format!(
                    "view column list has {} columns but query produces {} columns",
                    create_view.columns.len(),
                    arrow_fields.len()
                )));
            }
            let column_names = create_view
                .columns
                .iter()
                .map(|column| IdentNormalizer::default().normalize(column.name.clone()))
                .collect::<Vec<_>>();
            let mut unique_names = HashSet::with_capacity(column_names.len());
            for name in &column_names {
                if !unique_names.insert(name.clone()) {
                    return Err(DataFusionError::Plan(format!(
                        "duplicate view column name '{name}'"
                    )));
                }
            }
            for (field, name) in arrow_fields.iter_mut().zip(column_names) {
                *field = field.clone().with_name(name);
            }
            let fields = paimon::arrow::arrow_fields_to_paimon(&arrow_fields)
                .map_err(to_datafusion_error)?;
            let schema = paimon::catalog::ViewSchema::new(
                fields,
                query_sql.clone(),
                HashMap::from([("datafusion".to_string(), query_sql)]),
                None,
                HashMap::new(),
            );
            catalog
                .create_view(&identifier, schema, create_view.if_not_exists)
                .await
                .map_err(to_datafusion_error)?;
            ok_result(&self.ctx)
        }
    }

    async fn handle_create_function(
        &self,
        create_function: &CreateFunction,
    ) -> DFResult<DataFrame> {
        validate_persistent_create_function(create_function)?;
        if create_function
            .language
            .as_ref()
            .is_some_and(|language| !language.value.eq_ignore_ascii_case("sql"))
        {
            return Err(DataFusionError::Plan(
                "CREATE FUNCTION only supports LANGUAGE SQL".to_string(),
            ));
        }
        if matches!(
            create_function.behavior,
            Some(FunctionBehavior::Stable | FunctionBehavior::Volatile)
        ) {
            return Err(DataFusionError::Plan(
                "CREATE FUNCTION only supports deterministic SQL functions".to_string(),
            ));
        }
        let FunctionReturnType::DataType(return_type) = create_function
            .return_type
            .as_ref()
            .ok_or_else(|| DataFusionError::Plan("CREATE FUNCTION requires RETURNS".to_string()))?
        else {
            return Err(DataFusionError::Plan(
                "CREATE FUNCTION SETOF return types are not supported".to_string(),
            ));
        };
        let CreateFunctionBody::Return(body) = create_function
            .function_body
            .as_ref()
            .ok_or_else(|| DataFusionError::Plan("CREATE FUNCTION requires RETURN".to_string()))?
        else {
            return Err(DataFusionError::Plan(
                "CREATE FUNCTION only supports a RETURN expression".to_string(),
            ));
        };

        let mut parameter_names = HashSet::new();
        let input_params = create_function
            .args
            .as_deref()
            .unwrap_or_default()
            .iter()
            .enumerate()
            .map(|(id, argument)| {
                if argument.mode.is_some() || argument.default_expr.is_some() {
                    return Err(DataFusionError::Plan(
                        "CREATE FUNCTION argument modes and defaults are not supported".to_string(),
                    ));
                }
                let name = argument
                    .name
                    .clone()
                    .map(normalize_create_function_argument_name)
                    .ok_or_else(|| {
                        DataFusionError::Plan(
                            "CREATE FUNCTION arguments must have names".to_string(),
                        )
                    })?;
                if !parameter_names.insert(name.clone()) {
                    return Err(DataFusionError::Plan(format!(
                        "duplicate function argument name '{name}'"
                    )));
                }
                Ok(PaimonDataField::new(
                    id as i32,
                    name,
                    sql_data_type_to_paimon_type(&argument.data_type, true)?,
                ))
            })
            .collect::<DFResult<Vec<_>>>()?;
        let return_params = vec![PaimonDataField::new(
            0,
            "result".to_string(),
            sql_data_type_to_paimon_type(return_type, true)?,
        )];
        let (catalog, catalog_name, identifier) =
            self.resolve_catalog_and_function(&create_function.name)?;
        let function = paimon::catalog::Function::new(
            identifier,
            Some(input_params),
            Some(return_params),
            true,
            HashMap::from([(
                "datafusion".to_string(),
                paimon::catalog::FunctionDefinition::Sql {
                    definition: body.to_string(),
                },
            )]),
            None,
            HashMap::new(),
        );
        self.validate_create_function(&function, &catalog_name)
            .await?;
        catalog
            .create_function(&function, create_function.if_not_exists)
            .await
            .map_err(to_datafusion_error)?;
        ok_result(&self.ctx)
    }

    async fn validate_create_function(
        &self,
        function: &paimon::catalog::Function,
        catalog_name: &str,
    ) -> DFResult<()> {
        let arguments = function
            .input_params()
            .unwrap_or_default()
            .iter()
            .map(|field| {
                let sql_type =
                    crate::table::data_type_to_sql(field.data_type()).map_err(|error| {
                        DataFusionError::Plan(format!(
                            "Invalid CREATE FUNCTION argument type '{:?}': {error}",
                            field.data_type()
                        ))
                    })?;
                Ok(format!("CAST(NULL AS {sql_type})"))
            })
            .collect::<DFResult<Vec<_>>>()?
            .join(", ");
        let quote = |identifier: &str| format!("\"{}\"", identifier.replace('"', "\"\""));
        let validation_sql = format!(
            "SELECT {}.{}.{}({arguments})",
            quote(catalog_name),
            quote(function.identifier().database()),
            quote(function.name())
        );
        let expanded = crate::sql_function::expand_sql_with_candidate(
            &validation_sql,
            &self.catalogs,
            catalog_name,
            function.identifier().database(),
            function,
        )
        .await?;
        let mut state = self.ctx.state();
        state.config_mut().options_mut().catalog.default_catalog = catalog_name.to_string();
        state.config_mut().options_mut().catalog.default_schema =
            function.identifier().database().to_string();
        let logical_plan = state.create_logical_plan(&expanded).await?;
        validate_immutable_scalar_plan(&logical_plan)?;
        state.create_physical_plan(&logical_plan).await?;
        Ok(())
    }

    async fn handle_drop_partitions(
        &self,
        catalog: &Arc<dyn Catalog>,
        identifier: &Identifier,
        partitions: &[SqlExpr],
        if_exists: bool,
    ) -> DFResult<DataFrame> {
        if partitions.is_empty() {
            return Err(DataFusionError::Plan(
                "DROP PARTITIONS requires at least one partition specification".to_string(),
            ));
        }
        let table = match catalog.get_table(identifier).await {
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
        let commit = wb.try_new_commit().map_err(to_datafusion_error)?;
        commit
            .truncate_partitions(partition_values)
            .await
            .map_err(to_datafusion_error)?;

        ok_result(&self.ctx)
    }

    /// Returns the name of the current default catalog from DataFusion config.
    pub(crate) fn current_catalog_name(&self) -> String {
        self.ctx
            .state()
            .config_options()
            .catalog
            .default_catalog
            .clone()
    }

    fn current_catalog(&self) -> DFResult<Arc<dyn Catalog>> {
        let name = self.current_catalog_name();
        self.catalogs.get(&name).cloned().ok_or_else(|| {
            DataFusionError::Plan(
                "No catalog registered. Call register_catalog() first.".to_string(),
            )
        })
    }

    /// Check whether a TableReference targets a registered Paimon catalog.
    fn is_paimon_catalog_ref(&self, table_ref: &TableReference) -> bool {
        let catalog_name = match table_ref {
            TableReference::Full { catalog, .. } => catalog.to_string(),
            TableReference::Partial { .. } | TableReference::Bare { .. } => {
                self.current_catalog_name()
            }
        };
        self.catalogs.contains_key(&catalog_name)
    }

    fn is_paimon_function_name(&self, name: &ObjectName) -> bool {
        let Some(parts) = name
            .0
            .iter()
            .map(|part| {
                part.as_ident()
                    .map(|identifier| IdentNormalizer::default().normalize(identifier.clone()))
            })
            .collect::<Option<Vec<_>>>()
        else {
            return false;
        };
        let catalog_name = match parts.as_slice() {
            [catalog, _, _] => catalog.clone(),
            [_] | [_, _] => self.current_catalog_name(),
            _ => return false,
        };
        self.catalogs.contains_key(&catalog_name)
    }

    fn resolve_catalog_and_function(
        &self,
        name: &ObjectName,
    ) -> DFResult<(Arc<dyn Catalog>, String, Identifier)> {
        let parts = name
            .0
            .iter()
            .map(|part| {
                part.as_ident()
                    .cloned()
                    .map(|identifier| IdentNormalizer::default().normalize(identifier))
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!("Invalid function reference: {name}"))
                    })
            })
            .collect::<DFResult<Vec<_>>>()?;
        match parts.as_slice() {
            [catalog_name, database, function] => {
                let catalog = self.catalogs.get(catalog_name).ok_or_else(|| {
                    DataFusionError::Plan(format!("Unknown catalog '{catalog_name}'"))
                })?;
                Ok((
                    Arc::clone(catalog),
                    catalog_name.clone(),
                    Identifier::new(database, function),
                ))
            }
            [database, function] => Ok((
                self.current_catalog()?,
                self.current_catalog_name(),
                Identifier::new(database, function),
            )),
            [function] => Ok((
                self.current_catalog()?,
                self.current_catalog_name(),
                Identifier::new(
                    self.ctx
                        .state()
                        .config_options()
                        .catalog
                        .default_schema
                        .clone(),
                    function,
                ),
            )),
            _ => Err(DataFusionError::Plan(format!(
                "Invalid function reference: {name}"
            ))),
        }
    }

    /// Resolve an ObjectName like `catalog.db.table` or `db.table` to a catalog and Identifier.
    fn resolve_catalog_and_table(
        &self,
        name: &ObjectName,
    ) -> DFResult<(Arc<dyn Catalog>, String, Identifier)> {
        let parts: Vec<String> = name
            .0
            .iter()
            .filter_map(|p| p.as_ident().map(|id| id.value.clone()))
            .collect();
        match parts.len() {
            3 => {
                let catalog = self.catalogs.get(&parts[0]).ok_or_else(|| {
                    DataFusionError::Plan(format!("Unknown catalog '{}'", parts[0]))
                })?;
                Ok((
                    catalog.clone(),
                    parts[0].clone(),
                    Identifier::new(parts[1].clone(), parts[2].clone()),
                ))
            }
            2 => {
                let catalog = self.current_catalog()?;
                Ok((
                    catalog,
                    self.current_catalog_name(),
                    Identifier::new(parts[0].clone(), parts[1].clone()),
                ))
            }
            1 => {
                let catalog = self.current_catalog()?;
                let default_schema = self
                    .ctx
                    .state()
                    .config_options()
                    .catalog
                    .default_schema
                    .clone();
                Ok((
                    catalog,
                    self.current_catalog_name(),
                    Identifier::new(default_schema, parts[0].clone()),
                ))
            }
            _ => Err(DataFusionError::Plan(format!(
                "Invalid table reference: {name}"
            ))),
        }
    }

    fn ensure_main_branch_write_target(name: &ObjectName, operation: &str) -> DFResult<()> {
        let object = name
            .0
            .last()
            .and_then(|part| part.as_ident())
            .map(|ident| ident.value.as_str())
            .ok_or_else(|| DataFusionError::Plan(format!("Invalid table reference: {name}")))?;
        let parsed = parse_object_name(object).map_err(to_datafusion_error)?;
        if let Some(branch) = parsed.branch() {
            return Err(DataFusionError::NotImplemented(format!(
                "{operation} on Paimon branch '{branch}' is not supported"
            )));
        }
        Ok(())
    }

    /// Resolve an ObjectName to just the Identifier (for backward compat in handle_alter_table).
    fn resolve_table_name(&self, name: &ObjectName) -> DFResult<Identifier> {
        let (_catalog, _catalog_name, identifier) = self.resolve_catalog_and_table(name)?;
        Ok(identifier)
    }
}

fn validate_immutable_scalar_plan(plan: &LogicalPlan) -> DFResult<()> {
    let mut violation = None;
    plan.apply(|node| {
        node.apply_expressions(|expression| {
            expression.apply(|expression| {
                match expression {
                    LogicalExpr::ScalarFunction(function)
                        if function.func.signature().volatility != Volatility::Immutable =>
                    {
                        violation = Some(format!(
                            "CREATE FUNCTION body uses non-immutable function '{}'",
                            function.func.name()
                        ));
                    }
                    LogicalExpr::HigherOrderFunction(function)
                        if function.func.signature().volatility != Volatility::Immutable =>
                    {
                        violation = Some(format!(
                            "CREATE FUNCTION body uses non-immutable function '{}'",
                            function.func.name()
                        ));
                    }
                    LogicalExpr::AggregateFunction(_) | LogicalExpr::WindowFunction(_) => {
                        violation = Some(
                            "CREATE FUNCTION body must be a scalar expression; aggregate and window functions are not supported"
                                .to_string(),
                        );
                    }
                    LogicalExpr::Exists(_)
                    | LogicalExpr::InSubquery(_)
                    | LogicalExpr::SetComparison(_)
                    | LogicalExpr::ScalarSubquery(_) => {
                        violation = Some(
                            "CREATE FUNCTION body must be a scalar expression; subqueries are not supported"
                                .to_string(),
                        );
                    }
                    LogicalExpr::Unnest(_) => {
                        violation = Some(
                            "CREATE FUNCTION body must be a scalar expression; UNNEST is not supported"
                                .to_string(),
                        );
                    }
                    _ => {}
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    if let Some(message) = violation {
        return Err(DataFusionError::Plan(message));
    }
    Ok(())
}

fn normalize_create_function_argument_name(
    identifier: datafusion::sql::sqlparser::ast::Ident,
) -> String {
    if identifier.quote_style.is_none() {
        if let Some(value) = identifier
            .value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
        {
            return value.replace("\"\"", "\"");
        }
    }
    IdentNormalizer::default().normalize(identifier)
}

fn validate_persistent_create_function(create_function: &CreateFunction) -> DFResult<()> {
    let unsupported = if create_function.or_alter {
        Some("CREATE OR ALTER FUNCTION is not supported")
    } else if create_function.or_replace {
        Some("CREATE OR REPLACE FUNCTION is not supported")
    } else if create_function.temporary {
        Some("CREATE TEMPORARY FUNCTION is not supported")
    } else if create_function.args.is_none() {
        Some("CREATE FUNCTION requires a parenthesized argument list")
    } else if create_function.called_on_null.is_some() {
        Some("CREATE FUNCTION NULL INPUT clauses are not supported")
    } else if create_function.parallel.is_some() {
        Some("CREATE FUNCTION PARALLEL clauses are not supported")
    } else if create_function.security.is_some() {
        Some("CREATE FUNCTION SECURITY clauses are not supported")
    } else if !create_function.set_params.is_empty() {
        Some("CREATE FUNCTION SET clauses are not supported")
    } else if create_function.using.is_some() {
        Some("CREATE FUNCTION USING clauses are not supported")
    } else if create_function.determinism_specifier.is_some() {
        Some("CREATE FUNCTION determinism specifiers are not supported")
    } else if create_function.options.is_some() {
        Some("CREATE FUNCTION OPTIONS clauses are not supported")
    } else if create_function.remote_connection.is_some() {
        Some("CREATE FUNCTION REMOTE clauses are not supported")
    } else {
        None
    };
    if let Some(message) = unsupported {
        return Err(DataFusionError::Plan(message.to_string()));
    }
    Ok(())
}

fn parse_sql_statements(sql: &str) -> DFResult<Vec<Statement>> {
    let dialect = GenericDialect {};
    let mut tokens = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .map_err(|error| DataFusionError::Plan(format!("SQL parse error: {error}")))?;
    let significant = tokens
        .iter()
        .enumerate()
        .filter_map(|(index, token)| match &token.token {
            Token::Whitespace(_) => None,
            Token::Word(word) => Some((index, word.keyword)),
            _ => Some((index, Keyword::NoKeyword)),
        })
        .take(5)
        .collect::<Vec<_>>();
    let create_function_if_not_exists = matches!(
        significant.as_slice(),
        [
            (_, Keyword::CREATE),
            (_, Keyword::FUNCTION),
            (_, Keyword::IF),
            (_, Keyword::NOT),
            (_, Keyword::EXISTS)
        ]
    );
    let create_function_or_alter = matches!(
        significant.as_slice(),
        [
            (_, Keyword::CREATE),
            (_, Keyword::OR),
            (_, Keyword::ALTER),
            (_, Keyword::FUNCTION),
            ..
        ]
    );
    if create_function_if_not_exists {
        let removed = significant[2..=4]
            .iter()
            .map(|(index, _)| *index)
            .collect::<HashSet<_>>();
        tokens = tokens
            .into_iter()
            .enumerate()
            .filter(|(index, _)| !removed.contains(index))
            .map(|(_, token)| token)
            .collect();
    }
    let mut statements = Parser::new(&dialect)
        .with_tokens_with_locations(tokens)
        .parse_statements()
        .map_err(|error| DataFusionError::Plan(format!("SQL parse error: {error}")))?;
    if create_function_if_not_exists {
        let Some(Statement::CreateFunction(create_function)) = statements.first_mut() else {
            return Err(DataFusionError::Plan(
                "SQL parse error: invalid CREATE FUNCTION IF NOT EXISTS statement".to_string(),
            ));
        };
        create_function.if_not_exists = true;
    }
    if create_function_or_alter {
        let Some(Statement::CreateFunction(create_function)) = statements.first_mut() else {
            return Err(DataFusionError::Plan(
                "SQL parse error: invalid CREATE OR ALTER FUNCTION statement".to_string(),
            ));
        };
        create_function.or_alter = true;
    }
    Ok(statements)
}

fn validate_persistent_create_view(create_view: &CreateView) -> DFResult<()> {
    let unsupported = if create_view.or_alter {
        Some("CREATE OR ALTER VIEW is not supported")
    } else if create_view.or_replace {
        Some("CREATE OR REPLACE VIEW is not supported")
    } else if create_view.secure {
        Some("CREATE SECURE VIEW is not supported")
    } else if create_view.copy_grants {
        Some("CREATE VIEW COPY GRANTS is not supported")
    } else if create_view.name_before_not_exists {
        Some("CREATE VIEW with the name before IF NOT EXISTS is not supported")
    } else {
        match &create_view.options {
            CreateTableOptions::None => None,
            CreateTableOptions::With(_) => Some("CREATE VIEW WITH options are not supported"),
            CreateTableOptions::Options(_) => Some("CREATE VIEW OPTIONS are not supported"),
            _ => Some("CREATE VIEW options are not supported"),
        }
    };
    if let Some(message) = unsupported {
        return Err(DataFusionError::Plan(message.to_string()));
    }
    if create_view.comment.is_some() {
        return Err(DataFusionError::Plan(
            "CREATE VIEW COMMENT is not supported".to_string(),
        ));
    }
    if !create_view.cluster_by.is_empty() {
        return Err(DataFusionError::Plan(
            "CREATE VIEW CLUSTER BY is not supported".to_string(),
        ));
    }
    if create_view.to.is_some() {
        return Err(DataFusionError::Plan(
            "CREATE VIEW TO is not supported".to_string(),
        ));
    }
    if create_view.with_no_schema_binding {
        return Err(DataFusionError::Plan(
            "CREATE VIEW WITH NO SCHEMA BINDING is not supported".to_string(),
        ));
    }
    if create_view.params.is_some() {
        return Err(DataFusionError::Plan(
            "CREATE VIEW view parameters are not supported".to_string(),
        ));
    }
    if create_view
        .columns
        .iter()
        .any(|column| column.data_type.is_some())
    {
        return Err(DataFusionError::Plan(
            "CREATE VIEW column data types are not supported".to_string(),
        ));
    }
    if create_view
        .columns
        .iter()
        .any(|column| column.options.is_some())
    {
        return Err(DataFusionError::Plan(
            "CREATE VIEW column options are not supported".to_string(),
        ));
    }
    Ok(())
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
    // Match "CREATE" then whitespace then optional "TEMPORARY"/"TEMP" then "TABLE" (all ASCII, byte-safe)
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
    // Skip optional TEMPORARY or TEMP keyword
    if i + 9 <= len && bytes[i..i + 9].eq_ignore_ascii_case(b"TEMPORARY") {
        i += 9;
        while i < len && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
    } else if i + 4 <= len && bytes[i..i + 4].eq_ignore_ascii_case(b"TEMP") {
        i += 4;
        while i < len && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
    }
    // After optional TEMPORARY/TEMP, reject CREATE TEMPORARY VIEW / CREATE TEMP VIEW
    if i + 4 <= len && bytes[i..i + 4].eq_ignore_ascii_case(b"VIEW") {
        return false;
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
        let mut value = String::new();
        let mut end = None;
        let mut chars = trimmed[1..].char_indices().peekable();
        while let Some((idx, ch)) = chars.next() {
            if ch == first as char {
                if chars.peek().is_some_and(|(_, next)| *next == first as char) {
                    value.push(ch);
                    chars.next();
                } else {
                    end = Some(1 + idx + ch.len_utf8());
                    break;
                }
            } else {
                value.push(ch);
            }
        }
        if let Some(end) = end {
            if trimmed[end..].trim().is_empty() {
                return Ok(value);
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

fn split_partition_columns(inner: &str) -> DFResult<Vec<&str>> {
    let mut columns = Vec::new();
    let mut start = 0;
    let mut quote = None;
    let mut chars = inner.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        match quote {
            Some(q) if ch == q => {
                if chars.peek().is_some_and(|(_, next)| *next == q) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            Some(_) => {}
            None if ch == '"' || ch == '`' => quote = Some(ch),
            None if ch == ',' => {
                columns.push(&inner[start..idx]);
                start = idx + ch.len_utf8();
            }
            None => {}
        }
    }
    if quote.is_some() {
        return Err(DataFusionError::Plan(
            "Unterminated quoted identifier in PARTITIONED BY".to_string(),
        ));
    }
    columns.push(&inner[start..]);
    Ok(columns)
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
    let mut quote = None;
    let mut chars = sql[inner_start..].char_indices().peekable();
    while let Some((i, ch)) = chars.next() {
        match quote {
            Some(q) if ch == q => {
                if chars.peek().is_some_and(|(_, next)| *next == q) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            Some(_) => {}
            None if ch == '"' || ch == '`' => quote = Some(ch),
            None if ch == '(' => depth += 1,
            None if ch == ')' => {
                depth -= 1;
                if depth == 0 {
                    paren_end = Some(inner_start + i);
                    break;
                }
            }
            None => {}
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
    for token in split_partition_columns(inner)? {
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
    let comment = column_def_comment(col);

    Ok(SchemaChange::AddColumn {
        field_names: vec![col.name.value.clone()],
        data_type: paimon_type,
        comment,
        column_move: None,
    })
}

fn column_def_to_paimon_type(col: &ColumnDef) -> DFResult<PaimonDataType> {
    sql_data_type_to_paimon_type(&col.data_type, column_def_nullable(col))
}

fn column_def_comment(col: &ColumnDef) -> Option<String> {
    col.options.iter().find_map(|opt| match &opt.option {
        ColumnOption::Comment(comment) => Some(comment.clone()),
        _ => None,
    })
}

fn primary_key_column_name(expr: &SqlExpr) -> String {
    match expr {
        SqlExpr::Identifier(ident) => ident.value.clone(),
        _ => expr.to_string(),
    }
}

fn character_length_or_default(
    length: &Option<CharacterLength>,
    default_length: u32,
) -> DFResult<u32> {
    match length {
        Some(CharacterLength::IntegerLength { length, .. }) => (*length).try_into().map_err(|_| {
            DataFusionError::Plan(format!("Character length {length} exceeds supported range"))
        }),
        Some(CharacterLength::Max) => Ok(VarCharType::MAX_LENGTH),
        None => Ok(default_length),
    }
}

fn u64_length_or_default(length: Option<u64>, default_length: usize) -> DFResult<usize> {
    match length {
        Some(length) => length.try_into().map_err(|_| {
            DataFusionError::Plan(format!("Binary length {length} exceeds supported range"))
        }),
        None => Ok(default_length),
    }
}

fn binary_length_or_default(length: &Option<BinaryLength>, default_length: u32) -> DFResult<u32> {
    match length {
        Some(BinaryLength::IntegerLength { length }) => (*length).try_into().map_err(|_| {
            DataFusionError::Plan(format!("Binary length {length} exceeds supported range"))
        }),
        Some(BinaryLength::Max) => Ok(VarBinaryType::MAX_LENGTH),
        None => Ok(default_length),
    }
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
        SqlType::Char(length) | SqlType::Character(length) => Ok(PaimonDataType::Char(
            CharType::with_nullable(nullable, character_length_or_default(length, 1)? as usize)
                .map_err(to_datafusion_error)?,
        )),
        SqlType::Varchar(length)
        | SqlType::Nvarchar(length)
        | SqlType::CharVarying(length)
        | SqlType::CharacterVarying(length) => Ok(PaimonDataType::VarChar(
            VarCharType::with_nullable(
                nullable,
                character_length_or_default(length, VarCharType::MAX_LENGTH)?,
            )
            .map_err(to_datafusion_error)?,
        )),
        SqlType::Text | SqlType::String(_) => Ok(PaimonDataType::VarChar(
            VarCharType::with_nullable(nullable, VarCharType::MAX_LENGTH)
                .map_err(to_datafusion_error)?,
        )),
        SqlType::Binary(length) => Ok(PaimonDataType::Binary(
            BinaryType::with_nullable(nullable, u64_length_or_default(*length, 1)? as usize)
                .map_err(to_datafusion_error)?,
        )),
        SqlType::Varbinary(length) => Ok(PaimonDataType::VarBinary(
            VarBinaryType::try_new(
                nullable,
                binary_length_or_default(length, VarBinaryType::MAX_LENGTH)?,
            )
            .map_err(to_datafusion_error)?,
        )),
        SqlType::Bytea => Ok(PaimonDataType::VarBinary(
            VarBinaryType::try_new(nullable, VarBinaryType::MAX_LENGTH)
                .map_err(to_datafusion_error)?,
        )),
        other if other.to_string().eq_ignore_ascii_case("BYTES") => Ok(PaimonDataType::VarBinary(
            VarBinaryType::try_new(nullable, VarBinaryType::MAX_LENGTH)
                .map_err(to_datafusion_error)?,
        )),
        SqlType::Blob(_) => Ok(PaimonDataType::Blob(BlobType::with_nullable(nullable))),
        SqlType::Custom(name, modifiers)
            if name.to_string().eq_ignore_ascii_case("VARIANT") && modifiers.is_empty() =>
        {
            Ok(PaimonDataType::Variant(VariantType::with_nullable(
                nullable,
            )))
        }
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

fn object_name_to_single_identifier(name: &ObjectName) -> DFResult<String> {
    match name.0.as_slice() {
        [part] => part
            .as_ident()
            .map(|id| id.value.clone())
            .ok_or_else(|| DataFusionError::Plan(format!("Invalid column name: {name}"))),
        _ => Err(DataFusionError::Plan(format!(
            "Expected a simple column name, got: {name}"
        ))),
    }
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
            | Datum::Bytes(_)
            | Datum::Variant { .. } => Err(DataFusionError::Plan(format!(
                "Unsupported datum type for partition column: {d}"
            ))),
        },
    }
}

struct VersionAsOfInfo {
    table_name: String,
    version: String,
    /// Byte range (start, end) covering "table_name VERSION AS OF n"
    clause_range: (usize, usize),
}

struct TimestampAsOfInfo {
    table_name: String,
    timestamp: String,
    /// Byte range (start, end) covering "table_name TIMESTAMP AS OF 'ts'"
    clause_range: (usize, usize),
}

/// Check whether a SQL string contains a time-travel keyword (`VERSION AS OF` or
/// `TIMESTAMP AS OF`) **outside** of single-quoted string literals, `--` line
/// comments, and `/* */` block comments.
fn contains_time_travel_keyword(sql: &str) -> bool {
    let lower = sql.to_lowercase();
    let bytes = lower.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    while i < len {
        match bytes[i] {
            b'\'' => {
                // Skip string literal
                i += 1;
                while i < len {
                    if bytes[i] == b'\'' {
                        i += 1;
                        if i < len && bytes[i] == b'\'' {
                            i += 1; // escaped quote
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                // Skip line comment
                i += 2;
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
                // Skip block comment
                i += 2;
                while i + 1 < len {
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            _ => {
                // Check for keywords
                if i + 14 <= len && bytes[i..i + 14].eq_ignore_ascii_case(b"version as of ") {
                    return true;
                }
                if i + 16 <= len && bytes[i..i + 16].eq_ignore_ascii_case(b"timestamp as of ") {
                    return true;
                }
                i += 1;
            }
        }
    }
    false
}

/// Extract **all** `VERSION AS OF <n>` or `VERSION AS OF '<tag>'` clauses from a
/// SQL string, skipping string literals and comments.
fn extract_all_version_as_of(sql: &str) -> Vec<VersionAsOfInfo> {
    let lower = sql.to_lowercase();
    let bytes = lower.as_bytes();
    let len = bytes.len();
    let sql_bytes = sql.as_bytes();
    let mut i = 0;
    let mut results = Vec::new();

    while i < len {
        match bytes[i] {
            b'\'' => {
                // Skip string literal
                i += 1;
                while i < len {
                    if sql_bytes[i] == b'\'' {
                        i += 1;
                        if i < len && sql_bytes[i] == b'\'' {
                            i += 1; // escaped quote
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                // Skip line comment
                i += 2;
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
                // Skip block comment
                i += 2;
                while i + 1 < len {
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            _ => {
                if i + 14 <= len && bytes[i..i + 14].eq_ignore_ascii_case(b"version as of ") {
                    let kw_start = i;
                    let val_start = i + 14;
                    let remaining = &sql[val_start..];

                    // Parse either a quoted tag name or a numeric snapshot ID
                    let version = if let Some(after_quote) = remaining.strip_prefix('\'') {
                        // Tag name: VERSION AS OF 'tagname'
                        if let Some(close_quote) = after_quote.find('\'') {
                            after_quote[..close_quote].to_string()
                        } else {
                            i += 1;
                            continue;
                        }
                    } else {
                        // Numeric snapshot ID: VERSION AS OF 1
                        let v: String = remaining
                            .chars()
                            .take_while(|c| c.is_ascii_digit())
                            .collect();
                        if v.is_empty() {
                            i += 1;
                            continue;
                        }
                        v
                    };

                    let is_quoted = remaining.starts_with('\'');
                    let val_end = if is_quoted {
                        val_start + version.len() + 2 // 2 quotes
                    } else {
                        val_start + version.len()
                    };

                    // Walk backwards from kw_start to find the table name boundary
                    let table_end = sql[..kw_start].trim_end_matches(' ').len();
                    let table_start = sql[..table_end]
                        .rfind(|c: char| c.is_whitespace() || c == ',' || c == '(')
                        .map(|idx| idx + 1)
                        .unwrap_or(0);
                    let table_name = sql[table_start..table_end].to_string();

                    if !table_name.is_empty() {
                        results.push(VersionAsOfInfo {
                            table_name,
                            version,
                            clause_range: (table_start, val_end),
                        });
                    }

                    i = val_end;
                } else {
                    i += 1;
                }
            }
        }
    }

    results
}

/// Extract **all** `TIMESTAMP AS OF '<ts>'` clauses from a SQL string, skipping
/// string literals and comments.
fn extract_all_timestamp_as_of(sql: &str) -> Vec<TimestampAsOfInfo> {
    let lower = sql.to_lowercase();
    let bytes = lower.as_bytes();
    let len = bytes.len();
    let sql_bytes = sql.as_bytes();
    let mut i = 0;
    let mut results = Vec::new();

    while i < len {
        match bytes[i] {
            b'\'' => {
                // Skip string literal
                i += 1;
                while i < len {
                    if sql_bytes[i] == b'\'' {
                        i += 1;
                        if i < len && sql_bytes[i] == b'\'' {
                            i += 1; // escaped quote
                        } else {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                // Skip line comment
                i += 2;
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
                // Skip block comment
                i += 2;
                while i + 1 < len {
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        i += 2;
                        break;
                    }
                    i += 1;
                }
            }
            _ => {
                if i + 16 <= len && bytes[i..i + 16].eq_ignore_ascii_case(b"timestamp as of ") {
                    let kw_start = i;
                    let val_start = i + 16;
                    let remaining = &sql[val_start..];

                    // Read the quoted timestamp string
                    if !remaining.starts_with('\'') {
                        i += 1;
                        continue;
                    }
                    if let Some(close_quote) = remaining[1..].find('\'') {
                        let timestamp = remaining[1..close_quote + 1].to_string();
                        let val_end = val_start + close_quote + 2; // skip both quotes

                        // Walk backwards to find the table name boundary
                        let table_end = sql[..kw_start].trim_end_matches(' ').len();
                        let table_start = sql[..table_end]
                            .rfind(|c: char| c.is_whitespace() || c == ',' || c == '(')
                            .map(|idx| idx + 1)
                            .unwrap_or(0);
                        let table_name = sql[table_start..table_end].to_string();

                        if !table_name.is_empty() {
                            results.push(TimestampAsOfInfo {
                                table_name,
                                timestamp,
                                clause_range: (table_start, val_end),
                            });
                        }

                        i = val_end;
                    } else {
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
        }
    }

    results
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

/// Registers the built-in table-valued functions against `catalog` so they can
/// be used in SQL without any extra setup call. Called for every catalog
/// registered on the context; add new built-in table functions here.
fn register_table_functions(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    default_database: &str,
) {
    crate::blob_view::register_blob_view(ctx, Arc::clone(catalog), default_database);
    crate::vector_search::register_vector_search(ctx, Arc::clone(catalog), default_database);
    #[cfg(feature = "fulltext")]
    crate::full_text_search::register_full_text_search(ctx, Arc::clone(catalog), default_database);
    crate::hybrid_search::register_hybrid_search(ctx, Arc::clone(catalog), default_database);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use paimon::catalog::Database;
    use paimon::spec::{
        DataField as PaimonDataField, DataType as PaimonDataType, IntType, Schema as PaimonSchema,
    };
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
        existing_table: Mutex<Option<Table>>,
        functions: Mutex<HashMap<Identifier, paimon::catalog::Function>>,
        views: Mutex<HashMap<Identifier, paimon::catalog::View>>,
        drop_view_supported: bool,
    }

    impl MockCatalog {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                existing_table: Mutex::new(None),
                functions: Mutex::new(HashMap::new()),
                views: Mutex::new(HashMap::new()),
                drop_view_supported: true,
            }
        }

        fn without_drop_view_support() -> Self {
            Self {
                drop_view_supported: false,
                ..Self::new()
            }
        }

        fn take_calls(&self) -> Vec<CatalogCall> {
            std::mem::take(&mut *self.calls.lock().unwrap())
        }

        fn add_function(&self, function: paimon::catalog::Function) {
            self.functions
                .lock()
                .unwrap()
                .insert(function.identifier().clone(), function);
        }

        fn add_view(&self, view: paimon::catalog::View) {
            self.views
                .lock()
                .unwrap()
                .insert(view.identifier().clone(), view);
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
            Ok(Database::new(_name.to_string(), HashMap::new(), None))
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
            if let Some(table) = self.existing_table.lock().unwrap().clone() {
                return Ok(table);
            }
            Err(paimon::Error::TableNotExist {
                full_name: _identifier.to_string(),
            })
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

        async fn list_functions(&self, database_name: &str) -> paimon::Result<Vec<String>> {
            Ok(self
                .functions
                .lock()
                .unwrap()
                .keys()
                .filter(|identifier| identifier.database() == database_name)
                .map(|identifier| identifier.object().to_string())
                .collect())
        }

        async fn create_function(
            &self,
            function: &paimon::catalog::Function,
            ignore_if_exists: bool,
        ) -> paimon::Result<()> {
            let mut functions = self.functions.lock().unwrap();
            if functions.contains_key(function.identifier()) {
                if ignore_if_exists {
                    return Ok(());
                }
                return Err(paimon::Error::FunctionAlreadyExist {
                    full_name: function.full_name(),
                });
            }
            functions.insert(function.identifier().clone(), function.clone());
            Ok(())
        }

        async fn get_function(
            &self,
            identifier: &Identifier,
        ) -> paimon::Result<paimon::catalog::Function> {
            self.functions
                .lock()
                .unwrap()
                .get(identifier)
                .cloned()
                .ok_or_else(|| paimon::Error::FunctionNotExist {
                    full_name: identifier.full_name(),
                })
        }

        async fn list_views(&self, database_name: &str) -> paimon::Result<Vec<String>> {
            Ok(self
                .views
                .lock()
                .unwrap()
                .keys()
                .filter(|identifier| identifier.database() == database_name)
                .map(|identifier| identifier.object().to_string())
                .collect())
        }

        async fn get_view(&self, identifier: &Identifier) -> paimon::Result<paimon::catalog::View> {
            self.views
                .lock()
                .unwrap()
                .get(identifier)
                .cloned()
                .ok_or_else(|| paimon::Error::ViewNotExist {
                    full_name: identifier.full_name(),
                })
        }

        async fn create_view(
            &self,
            identifier: &Identifier,
            schema: paimon::catalog::ViewSchema,
            ignore_if_exists: bool,
        ) -> paimon::Result<()> {
            let mut views = self.views.lock().unwrap();
            if views.contains_key(identifier) {
                if ignore_if_exists {
                    return Ok(());
                }
                return Err(paimon::Error::ViewAlreadyExist {
                    full_name: identifier.full_name(),
                });
            }
            views.insert(
                identifier.clone(),
                paimon::catalog::View::new(identifier.clone(), schema),
            );
            Ok(())
        }

        async fn drop_view(
            &self,
            identifier: &Identifier,
            ignore_if_not_exists: bool,
        ) -> paimon::Result<()> {
            if !self.drop_view_supported {
                return Err(paimon::Error::Unsupported {
                    message: "Catalog does not support views".to_string(),
                });
            }
            if self.views.lock().unwrap().remove(identifier).is_some() || ignore_if_not_exists {
                Ok(())
            } else {
                Err(paimon::Error::ViewNotExist {
                    full_name: identifier.full_name(),
                })
            }
        }
    }

    async fn make_sql_context(catalog: Arc<MockCatalog>) -> SQLContext {
        let mut ctx = SQLContext::new();
        ctx.register_catalog("paimon", catalog).await.unwrap();
        ctx
    }

    fn add_unary_sql_function(
        catalog: &MockCatalog,
        name: &str,
        definition: &str,
        deterministic: bool,
    ) {
        add_unary_sql_function_in_database(catalog, "default", name, definition, deterministic);
    }

    fn add_unary_sql_function_in_database(
        catalog: &MockCatalog,
        database: &str,
        name: &str,
        definition: &str,
        deterministic: bool,
    ) {
        let input_params: Vec<PaimonDataField> = serde_json::from_value(serde_json::json!([
            {"id": 0, "name": "x", "type": "BIGINT"}
        ]))
        .unwrap();
        let return_params: Vec<PaimonDataField> = serde_json::from_value(serde_json::json!([
            {"id": 0, "name": "result", "type": "BIGINT"}
        ]))
        .unwrap();
        catalog.add_function(paimon::catalog::Function::new(
            Identifier::new(database, name),
            Some(input_params),
            Some(return_params),
            deterministic,
            HashMap::from([(
                "datafusion".to_string(),
                paimon::catalog::FunctionDefinition::Sql {
                    definition: definition.to_string(),
                },
            )]),
            None,
            HashMap::new(),
        ));
    }

    fn add_plus_one_function(catalog: &MockCatalog) {
        add_unary_sql_function(catalog, "plus_one", "x + 1", true);
    }

    fn add_constant_view(catalog: &MockCatalog) {
        let schema = serde_json::from_value(serde_json::json!({
            "fields": [
                {"id": 0, "name": "answer", "type": "BIGINT"}
            ],
            "query": "SELECT CAST(0 AS BIGINT) AS answer",
            "dialects": {
                "datafusion": "SELECT CAST(42 AS INT) AS source_answer"
            },
            "comment": null,
            "options": {}
        }))
        .unwrap();
        catalog.add_view(paimon::catalog::View::new(
            Identifier::new("default", "answer_view"),
            schema,
        ));
    }

    fn add_bigint_view(catalog: &MockCatalog, database: &str, name: &str, query: &str) {
        let schema = serde_json::from_value(serde_json::json!({
            "fields": [
                {"id": 0, "name": "answer", "type": "BIGINT"}
            ],
            "query": query,
            "dialects": {},
            "comment": null,
            "options": {}
        }))
        .unwrap();
        catalog.add_view(paimon::catalog::View::new(
            Identifier::new(database, name),
            schema,
        ));
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_can_be_created_and_read() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(catalog).await;

        ctx.sql(
            "CREATE VIEW answer_view AS \
             SELECT CAST(42 AS BIGINT) AS answer",
        )
        .await
        .unwrap();

        let batches = ctx
            .sql("SELECT * FROM answer_view")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_can_be_dropped() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(Arc::clone(&catalog)).await;
        ctx.sql(
            "CREATE VIEW answer_view AS \
             SELECT CAST(42 AS BIGINT) AS answer",
        )
        .await
        .unwrap();

        ctx.sql("DROP VIEW answer_view").await.unwrap();

        assert!(matches!(
            catalog
                .get_view(&Identifier::new("default", "answer_view"))
                .await,
            Err(paimon::Error::ViewNotExist { .. })
        ));
        assert!(ctx.sql("SELECT * FROM answer_view").await.is_err());
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_drop_honors_if_exists() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(catalog).await;

        let error = ctx.sql("DROP VIEW missing_view").await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("View default.missing_view does not exist"),
            "unexpected error: {error}"
        );
        ctx.sql("DROP VIEW IF EXISTS missing_view").await.unwrap();
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_drop_resolves_supported_name_forms() {
        let catalog = Arc::new(MockCatalog::new());
        add_bigint_view(&catalog, "default", "bare_view", "SELECT 1 AS answer");
        add_bigint_view(&catalog, "other", "two_part", "SELECT 1 AS answer");
        add_bigint_view(&catalog, "other", "three_part", "SELECT 1 AS answer");
        add_bigint_view(&catalog, "default", "Quoted View", "SELECT 1 AS answer");
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        ctx.sql("DROP VIEW bare_view").await.unwrap();
        ctx.sql("DROP VIEW IF EXISTS other.two_part").await.unwrap();
        ctx.sql("DROP VIEW IF EXISTS paimon.other.three_part")
            .await
            .unwrap();
        ctx.sql("DROP VIEW \"Quoted View\"").await.unwrap();

        for identifier in [
            Identifier::new("default", "bare_view"),
            Identifier::new("other", "two_part"),
            Identifier::new("other", "three_part"),
            Identifier::new("default", "Quoted View"),
        ] {
            assert!(matches!(
                catalog.get_view(&identifier).await,
                Err(paimon::Error::ViewNotExist { .. })
            ));
        }
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_drop_rejects_unsupported_modifiers() {
        let cases = [
            ("DROP VIEW paimon.default.invalid_view CASCADE", "CASCADE"),
            ("DROP VIEW paimon.default.invalid_view RESTRICT", "RESTRICT"),
            ("DROP VIEW paimon.default.invalid_view PURGE", "PURGE"),
            (
                "DROP VIEW paimon.default.invalid_view ON default.target",
                "ON clauses",
            ),
        ];

        for (sql, modifier) in cases {
            let catalog = Arc::new(MockCatalog::new());
            let ctx = make_sql_context(catalog).await;
            let error = ctx.sql(sql).await.unwrap_err();
            assert!(
                error.to_string().contains(modifier),
                "expected {modifier} error, got: {error}"
            );
        }
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_drop_rejects_multiple_targets_before_deleting() {
        let catalog = Arc::new(MockCatalog::new());
        add_bigint_view(&catalog, "default", "first", "SELECT 1 AS answer");
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        let error = ctx
            .sql("DROP VIEW paimon.default.first, datafusion.public.second")
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("Persistent DROP VIEW does not support multiple views"));
        assert!(catalog
            .get_view(&Identifier::new("default", "first"))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_drop_propagates_unsupported_catalog() {
        let catalog = Arc::new(MockCatalog::without_drop_view_support());
        add_bigint_view(&catalog, "default", "answer_view", "SELECT 1 AS answer");
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        let error = ctx.sql("DROP VIEW answer_view").await.unwrap_err();

        assert!(error.to_string().contains("Catalog does not support views"));
        assert!(catalog
            .get_view(&Identifier::new("default", "answer_view"))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_drop_delegates_non_paimon_catalog() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(catalog).await;
        ctx.sql("CREATE VIEW datafusion.public.delegated_view AS SELECT 1 AS answer")
            .await
            .unwrap();

        ctx.sql("DROP VIEW datafusion.public.delegated_view")
            .await
            .unwrap();

        assert!(ctx
            .sql("SELECT * FROM datafusion.public.delegated_view")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_can_be_created_and_called() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        ctx.sql(
            "CREATE FUNCTION plus_one(x BIGINT) RETURNS BIGINT \
             LANGUAGE SQL IMMUTABLE RETURN x + 1",
        )
        .await
        .unwrap();

        let stored = catalog
            .get_function(&Identifier::new("default", "plus_one"))
            .await
            .unwrap();
        assert_eq!(stored.input_params().unwrap()[0].id(), 0);
        assert_eq!(stored.input_params().unwrap()[0].name(), "x");
        assert!(stored.input_params().unwrap()[0].data_type().is_nullable());
        assert_eq!(stored.return_params().unwrap()[0].id(), 0);
        assert_eq!(stored.return_params().unwrap()[0].name(), "result");
        assert!(stored.return_params().unwrap()[0].data_type().is_nullable());

        let batches = ctx
            .sql("SELECT plus_one(41) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_uses_databricks_default_sql_syntax() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        ctx.sql("CREATE FUNCTION plus_one(x BIGINT) RETURNS BIGINT RETURN x + 1")
            .await
            .unwrap();

        let stored = catalog
            .get_function(&Identifier::new("default", "plus_one"))
            .await
            .unwrap();
        assert!(stored.is_deterministic());

        let batches = ctx
            .sql("SELECT plus_one(41) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_supports_array_argument() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        ctx.sql(
            "CREATE FUNCTION array_answer(x ARRAY<BIGINT>) \
             RETURNS BIGINT RETURN 42",
        )
        .await
        .unwrap();

        assert!(catalog
            .get_function(&Identifier::new("default", "array_answer"))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_supports_array_return_type() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(catalog).await;

        ctx.sql(
            "CREATE FUNCTION singleton(x BIGINT) \
             RETURNS ARRAY<BIGINT> RETURN make_array(x)",
        )
        .await
        .unwrap();

        let batches = ctx
            .sql("SELECT singleton(42) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert!(matches!(
            batches[0]
                .schema()
                .field_with_name("answer")
                .unwrap()
                .data_type(),
            ArrowDataType::List(_)
        ));
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_normalizes_unquoted_bare_name() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        ctx.sql(
            "CREATE FUNCTION PlusOne(X BIGINT) RETURNS BIGINT \
             LANGUAGE SQL IMMUTABLE RETURN X + 1",
        )
        .await
        .unwrap();

        let stored = catalog
            .get_function(&Identifier::new("default", "plusone"))
            .await
            .unwrap();
        assert_eq!(stored.input_params().unwrap()[0].name(), "x");
        let batches = ctx
            .sql("SELECT plusone(41) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_preserves_quoted_names() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        ctx.sql(
            "CREATE FUNCTION \"PlusOne\"(\"Input\" BIGINT) RETURNS BIGINT \
             LANGUAGE SQL IMMUTABLE RETURN \"Input\" + 1",
        )
        .await
        .unwrap();

        let stored = catalog
            .get_function(&Identifier::new("default", "PlusOne"))
            .await
            .unwrap();
        assert_eq!(stored.input_params().unwrap()[0].name(), "Input");
        let batches = ctx
            .sql("SELECT \"PlusOne\"(41) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_if_not_exists_preserves_existing_function() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(Arc::clone(&catalog)).await;
        ctx.sql(
            "CREATE FUNCTION answer() RETURNS BIGINT \
             LANGUAGE SQL IMMUTABLE RETURN 1",
        )
        .await
        .unwrap();

        ctx.sql(
            "CrEaTe /* keep comments */ FuNcTiOn IF /* gap */ NOT EXISTS answer() \
             RETURNS BIGINT LANGUAGE SQL IMMUTABLE RETURN 2",
        )
        .await
        .unwrap();

        let stored = catalog
            .get_function(&Identifier::new("default", "answer"))
            .await
            .unwrap();
        assert_eq!(
            stored
                .definition("datafusion")
                .and_then(paimon::catalog::FunctionDefinition::sql),
            Some("1")
        );
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_if_not_exists_validates_proposed_body() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(Arc::clone(&catalog)).await;
        ctx.sql(
            "CREATE FUNCTION answer() RETURNS BIGINT \
             LANGUAGE SQL IMMUTABLE RETURN 1",
        )
        .await
        .unwrap();

        let error = ctx
            .sql(
                "CREATE FUNCTION IF NOT EXISTS answer() RETURNS BIGINT \
                 LANGUAGE SQL IMMUTABLE RETURN undeclared + 1",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("undeclared identifier"));
        let stored = catalog
            .get_function(&Identifier::new("default", "answer"))
            .await
            .unwrap();
        assert_eq!(
            stored
                .definition("datafusion")
                .and_then(paimon::catalog::FunctionDefinition::sql),
            Some("1")
        );
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_uses_owning_database_for_dependencies() {
        let catalog = Arc::new(MockCatalog::new());
        add_unary_sql_function_in_database(&catalog, "default", "plus_one", "x + 1", true);
        add_unary_sql_function_in_database(&catalog, "other", "plus_one", "x + 100", true);
        let ctx = make_sql_context(Arc::clone(&catalog)).await;
        ctx.set_current_database("other").await.unwrap();

        ctx.sql(
            "CREATE FUNCTION paimon.default.wrapper(x BIGINT) RETURNS BIGINT \
             LANGUAGE SQL IMMUTABLE RETURN plus_one(x)",
        )
        .await
        .unwrap();

        let batches = ctx
            .sql("SELECT paimon.default.wrapper(41) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_rejects_nondeterministic_dependency() {
        let catalog = Arc::new(MockCatalog::new());
        add_unary_sql_function(&catalog, "unstable", "x + 1", false);
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        let error = ctx
            .sql(
                "CREATE FUNCTION wrapper(x BIGINT) RETURNS BIGINT \
                 LANGUAGE SQL IMMUTABLE RETURN unstable(x)",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("non-deterministic"));
        assert!(matches!(
            catalog
                .get_function(&Identifier::new("default", "wrapper"))
                .await,
            Err(paimon::Error::FunctionNotExist { .. })
        ));
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_rejects_direct_recursion_before_create() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        let error = ctx
            .sql(
                "CREATE FUNCTION abs(x BIGINT) RETURNS BIGINT \
                 LANGUAGE SQL IMMUTABLE RETURN abs(x)",
            )
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("recursive REST SQL function"),
            "unexpected error: {error}"
        );
        assert!(matches!(
            catalog
                .get_function(&Identifier::new("default", "abs"))
                .await,
            Err(paimon::Error::FunctionNotExist { .. })
        ));
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_rejects_indirect_recursion_before_create() {
        let catalog = Arc::new(MockCatalog::new());
        add_unary_sql_function(&catalog, "existing", "candidate(x)", true);
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        let error = ctx
            .sql(
                "CREATE FUNCTION candidate(x BIGINT) RETURNS BIGINT \
                 LANGUAGE SQL IMMUTABLE RETURN existing(x)",
            )
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("recursive REST SQL function"),
            "unexpected error: {error}"
        );
        assert!(matches!(
            catalog
                .get_function(&Identifier::new("default", "candidate"))
                .await,
            Err(paimon::Error::FunctionNotExist { .. })
        ));
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_rejects_volatile_datafusion_function() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        let error = ctx
            .sql(
                "CREATE FUNCTION random_value() RETURNS DOUBLE \
                 LANGUAGE SQL IMMUTABLE RETURN random()",
            )
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("non-immutable function 'random'"),
            "unexpected error: {error}"
        );
        assert!(matches!(
            catalog
                .get_function(&Identifier::new("default", "random_value"))
                .await,
            Err(paimon::Error::FunctionNotExist { .. })
        ));
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_rejects_unsupported_clauses() {
        let cases = [
            (
                "CREATE OR REPLACE FUNCTION invalid() RETURNS BIGINT LANGUAGE SQL IMMUTABLE RETURN 1",
                "OR REPLACE",
            ),
            (
                "CREATE OR ALTER FUNCTION invalid() RETURNS BIGINT LANGUAGE SQL IMMUTABLE RETURN 1",
                "OR ALTER",
            ),
            (
                "CREATE TEMPORARY FUNCTION invalid() RETURNS BIGINT LANGUAGE SQL IMMUTABLE RETURN 1",
                "TEMPORARY",
            ),
            (
                "CREATE FUNCTION invalid() RETURNS BIGINT LANGUAGE SQL IMMUTABLE CALLED ON NULL INPUT RETURN 1",
                "NULL INPUT",
            ),
            (
                "CREATE FUNCTION invalid() RETURNS BIGINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE RETURN 1",
                "PARALLEL",
            ),
            (
                "CREATE FUNCTION invalid() RETURNS BIGINT LANGUAGE SQL IMMUTABLE SECURITY INVOKER RETURN 1",
                "SECURITY",
            ),
            (
                "CREATE FUNCTION invalid() RETURNS BIGINT LANGUAGE SQL IMMUTABLE SET search_path TO public RETURN 1",
                "SET",
            ),
        ];

        for (sql, clause) in cases {
            let catalog = Arc::new(MockCatalog::new());
            let ctx = make_sql_context(catalog).await;
            let error = match ctx.sql(sql).await {
                Ok(_) => panic!("expected error for {clause}: {sql}"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains(clause),
                "expected error for {clause}, got: {error}"
            );
        }
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_rejects_non_scalar_bodies() {
        let cases = [
            (
                "CREATE FUNCTION invalid() RETURNS BIGINT LANGUAGE SQL IMMUTABLE RETURN count(1)",
                "aggregate and window",
            ),
            (
                "CREATE FUNCTION invalid() RETURNS BIGINT LANGUAGE SQL IMMUTABLE RETURN row_number() OVER ()",
                "aggregate and window",
            ),
            (
                "CREATE FUNCTION invalid() RETURNS BIGINT LANGUAGE SQL IMMUTABLE RETURN (SELECT 1)",
                "subqueries",
            ),
        ];

        for (sql, expected) in cases {
            let catalog = Arc::new(MockCatalog::new());
            let ctx = make_sql_context(catalog).await;
            let error = match ctx.sql(sql).await {
                Ok(_) => panic!("expected error containing {expected}: {sql}"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains(expected),
                "expected {expected}, got: {error}"
            );
        }
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_rejects_invalid_signature_and_body_forms() {
        let cases = [
            (
                "CREATE FUNCTION invalid() RETURNS BIGINT LANGUAGE PYTHON RETURN 1",
                "LANGUAGE SQL",
            ),
            (
                "CREATE FUNCTION invalid() RETURNS BIGINT LANGUAGE SQL STABLE RETURN 1",
                "deterministic SQL",
            ),
            (
                "CREATE FUNCTION invalid() RETURNS BIGINT LANGUAGE SQL IMMUTABLE AS '1'",
                "RETURN expression",
            ),
            (
                "CREATE FUNCTION invalid() RETURNS SETOF BIGINT LANGUAGE SQL IMMUTABLE RETURN 1",
                "SETOF",
            ),
            (
                "CREATE FUNCTION invalid(BIGINT) RETURNS BIGINT LANGUAGE SQL IMMUTABLE RETURN 1",
                "must have names",
            ),
            (
                "CREATE FUNCTION invalid(IN x BIGINT) RETURNS BIGINT LANGUAGE SQL IMMUTABLE RETURN x",
                "modes and defaults",
            ),
            (
                "CREATE FUNCTION invalid(x BIGINT = 1) RETURNS BIGINT LANGUAGE SQL IMMUTABLE RETURN x",
                "modes and defaults",
            ),
            (
                "CREATE FUNCTION invalid(X BIGINT, x BIGINT) RETURNS BIGINT LANGUAGE SQL IMMUTABLE RETURN x",
                "duplicate function argument",
            ),
        ];

        for (sql, expected) in cases {
            let catalog = Arc::new(MockCatalog::new());
            let ctx = make_sql_context(catalog).await;
            let error = match ctx.sql(sql).await {
                Ok(_) => panic!("expected error containing {expected}: {sql}"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains(expected),
                "expected {expected}, got: {error}"
            );
        }
    }

    #[tokio::test]
    async fn persistent_rest_catalog_function_rejects_incompatible_return_type() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        let error = ctx
            .sql(
                "CREATE FUNCTION invalid() RETURNS BIGINT \
                 LANGUAGE SQL IMMUTABLE RETURN named_struct('value', 1)",
            )
            .await
            .unwrap_err();

        assert!(
            error.to_string().to_ascii_lowercase().contains("cast"),
            "unexpected error: {error}"
        );
        assert!(matches!(
            catalog
                .get_function(&Identifier::new("default", "invalid"))
                .await,
            Err(paimon::Error::FunctionNotExist { .. })
        ));
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_infers_type_and_nullability() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        ctx.sql(
            "CREATE VIEW paimon.default.inferred_view AS \
             SELECT CAST(1 AS BIGINT) AS required, CAST(NULL AS BIGINT) AS optional",
        )
        .await
        .unwrap();

        let view = catalog
            .get_view(&Identifier::new("default", "inferred_view"))
            .await
            .unwrap();
        let fields = view.schema().fields();
        assert!(matches!(fields[0].data_type(), PaimonDataType::BigInt(_)));
        assert!(!fields[0].data_type().is_nullable());
        assert!(matches!(fields[1].data_type(), PaimonDataType::BigInt(_)));
        assert!(fields[1].data_type().is_nullable());
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_expands_function_in_owning_database() {
        let catalog = Arc::new(MockCatalog::new());
        add_unary_sql_function_in_database(&catalog, "default", "plus_one", "x + 1", true);
        add_unary_sql_function_in_database(&catalog, "other", "plus_one", "x + 100", true);
        let ctx = make_sql_context(Arc::clone(&catalog)).await;
        ctx.set_current_database("other").await.unwrap();

        ctx.sql(
            "CREATE VIEW paimon.default.function_view AS \
             SELECT plus_one(41) AS answer",
        )
        .await
        .unwrap();

        let view = catalog
            .get_view(&Identifier::new("default", "function_view"))
            .await
            .unwrap();
        assert!(view.query_for("datafusion").contains("plus_one(41)"));
        let batches = ctx
            .sql("SELECT * FROM paimon.default.function_view")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_binds_to_owning_database() {
        let catalog = Arc::new(MockCatalog::new());
        add_bigint_view(
            &catalog,
            "default",
            "base_view",
            "SELECT CAST(42 AS BIGINT) AS answer",
        );
        let other_schema = serde_json::from_value(serde_json::json!({
            "fields": [
                {"id": 0, "name": "answer", "type": "BIGINT"},
                {"id": 1, "name": "extra", "type": "BIGINT"}
            ],
            "query": "SELECT CAST(7 AS BIGINT) AS answer, CAST(8 AS BIGINT) AS extra",
            "dialects": {},
            "comment": null,
            "options": {}
        }))
        .unwrap();
        catalog.add_view(paimon::catalog::View::new(
            Identifier::new("other", "base_view"),
            other_schema,
        ));
        let ctx = make_sql_context(Arc::clone(&catalog)).await;
        ctx.set_current_database("other").await.unwrap();

        ctx.sql("CREATE VIEW paimon.default.created_view AS SELECT * FROM base_view")
            .await
            .unwrap();

        let view = catalog
            .get_view(&Identifier::new("default", "created_view"))
            .await
            .unwrap();
        assert_eq!(view.schema().fields().len(), 1);
        assert_eq!(view.schema().fields()[0].name(), "answer");
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_columns_override_names() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        ctx.sql(
            "CREATE VIEW paimon.default.named_view (renamed) \
             AS SELECT CAST(42 AS BIGINT) AS answer",
        )
        .await
        .unwrap();

        let view = catalog
            .get_view(&Identifier::new("default", "named_view"))
            .await
            .unwrap();
        assert_eq!(view.schema().fields()[0].name(), "renamed");
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_rejects_column_count_mismatch() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(catalog).await;

        let error = ctx
            .sql(
                "CREATE VIEW paimon.default.invalid_view (first, second) \
                 AS SELECT CAST(42 AS BIGINT) AS answer",
            )
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("view column list has 2 columns but query produces 1 columns"));
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_rejects_duplicate_column_names() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(catalog).await;

        let error = ctx
            .sql(
                "CREATE VIEW paimon.default.invalid_view (duplicate, duplicate) \
                 AS SELECT CAST(42 AS BIGINT), CAST(7 AS BIGINT)",
            )
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("duplicate view column name 'duplicate'"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_rejects_duplicate_inferred_names() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(catalog).await;

        let error = ctx
            .sql(
                "CREATE VIEW paimon.default.invalid_view AS \
                 SELECT CAST(42 AS BIGINT) AS duplicate, \
                        CAST(7 AS BIGINT) AS duplicate",
            )
            .await
            .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("Projections require unique expression names"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_if_not_exists_preserves_existing_view() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(Arc::clone(&catalog)).await;

        ctx.sql(
            "CREATE VIEW paimon.default.existing_view \
             AS SELECT CAST(1 AS BIGINT) AS answer",
        )
        .await
        .unwrap();
        ctx.sql(
            "CREATE VIEW IF NOT EXISTS paimon.default.existing_view \
             AS SELECT CAST(2 AS BIGINT) AS answer",
        )
        .await
        .unwrap();

        let view = catalog
            .get_view(&Identifier::new("default", "existing_view"))
            .await
            .unwrap();
        assert!(view.query_for("datafusion").contains("CAST(1 AS BIGINT)"));
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_rejects_or_replace() {
        let catalog = Arc::new(MockCatalog::new());
        let ctx = make_sql_context(catalog).await;

        let error = ctx
            .sql(
                "CREATE OR REPLACE VIEW paimon.default.invalid_view \
                 AS SELECT CAST(1 AS BIGINT) AS answer",
            )
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("CREATE OR REPLACE VIEW is not supported"));
    }

    #[tokio::test]
    async fn persistent_rest_catalog_view_rejects_unsupported_clauses() {
        let cases = [
            (
                "CREATE OR ALTER VIEW paimon.default.invalid_view AS SELECT 1",
                "OR ALTER",
            ),
            (
                "CREATE SECURE VIEW paimon.default.invalid_view AS SELECT 1",
                "SECURE",
            ),
            (
                "CREATE VIEW paimon.default.invalid_view COPY GRANTS AS SELECT 1",
                "COPY GRANTS",
            ),
            (
                "CREATE VIEW paimon.default.invalid_view IF NOT EXISTS AS SELECT 1",
                "name before IF NOT EXISTS",
            ),
            (
                "CREATE VIEW paimon.default.invalid_view WITH ('key' = 'value') AS SELECT 1",
                "WITH options",
            ),
            (
                "CREATE VIEW paimon.default.invalid_view OPTIONS(key = 'value') AS SELECT 1",
                "OPTIONS",
            ),
            (
                "CREATE VIEW paimon.default.invalid_view COMMENT = 'comment' AS SELECT 1",
                "COMMENT",
            ),
            (
                "CREATE VIEW paimon.default.invalid_view CLUSTER BY (answer) AS SELECT 1 AS answer",
                "CLUSTER BY",
            ),
            (
                "CREATE VIEW paimon.default.invalid_view TO default.sink AS SELECT 1",
                "TO",
            ),
            (
                "CREATE VIEW paimon.default.invalid_view AS SELECT 1 WITH NO SCHEMA BINDING",
                "WITH NO SCHEMA BINDING",
            ),
            (
                "CREATE ALGORITHM = MERGE VIEW paimon.default.invalid_view AS SELECT 1",
                "view parameters",
            ),
            (
                "CREATE VIEW paimon.default.invalid_view (answer COMMENT 'comment') AS SELECT 1",
                "column options",
            ),
        ];

        for (sql, clause) in cases {
            let catalog = Arc::new(MockCatalog::new());
            let ctx = make_sql_context(catalog).await;
            let error = ctx.sql(sql).await.unwrap_err();
            assert!(
                error.to_string().contains(clause),
                "expected error for {clause}, got: {error}"
            );
        }
    }

    #[tokio::test]
    async fn rest_catalog_view_is_planned_lazily() {
        let catalog = Arc::new(MockCatalog::new());
        add_constant_view(&catalog);
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql("SELECT * FROM answer_view")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn rest_catalog_view_can_call_bare_sql_function() {
        let catalog = Arc::new(MockCatalog::new());
        add_plus_one_function(&catalog);
        let schema = serde_json::from_value(serde_json::json!({
            "fields": [
                {"id": 0, "name": "answer", "type": "BIGINT"}
            ],
            "query": "SELECT plus_one(41) AS answer",
            "dialects": {},
            "comment": null,
            "options": {}
        }))
        .unwrap();
        catalog.add_view(paimon::catalog::View::new(
            Identifier::new("default", "function_view"),
            schema,
        ));
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql("SELECT * FROM function_view")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn rest_catalog_view_is_discoverable_from_schema_provider() {
        let catalog = Arc::new(MockCatalog::new());
        add_constant_view(&catalog);
        let ctx = make_sql_context(catalog).await;

        let schema = ctx
            .ctx
            .catalog("paimon")
            .unwrap()
            .schema("default")
            .unwrap();

        assert!(schema.table_names().contains(&"answer_view".to_string()));
        assert!(schema.table_exist("answer_view"));
    }

    #[tokio::test]
    async fn nested_rest_catalog_view_binds_bare_names_to_owning_database() {
        let catalog = Arc::new(MockCatalog::new());
        add_bigint_view(
            &catalog,
            "default",
            "base_view",
            "SELECT CAST(42 AS BIGINT) AS answer",
        );
        add_bigint_view(
            &catalog,
            "other",
            "base_view",
            "SELECT CAST(7 AS BIGINT) AS answer",
        );
        add_bigint_view(&catalog, "default", "outer_view", "SELECT * FROM base_view");
        let ctx = make_sql_context(catalog).await;
        ctx.set_current_database("other").await.unwrap();

        let batches = ctx
            .sql("SELECT * FROM paimon.default.outer_view")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn recursive_rest_catalog_views_are_rejected() {
        let catalog = Arc::new(MockCatalog::new());
        add_bigint_view(
            &catalog,
            "default",
            "first_view",
            "SELECT * FROM second_view",
        );
        add_bigint_view(
            &catalog,
            "default",
            "second_view",
            "SELECT * FROM first_view",
        );
        let ctx = make_sql_context(catalog).await;

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            ctx.sql("SELECT * FROM first_view"),
        )
        .await
        .expect("recursive view planning should terminate")
        .unwrap_err()
        .to_string();

        assert!(
            error.contains("recursive REST catalog view"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn rest_catalog_view_allows_cte_with_same_name() {
        let catalog = Arc::new(MockCatalog::new());
        add_bigint_view(
            &catalog,
            "default",
            "cte_view",
            "WITH wrapper AS (\
                 WITH cte_view AS (SELECT CAST(42 AS BIGINT) AS answer) \
                 SELECT * FROM cte_view\
             ) SELECT * FROM wrapper",
        );
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql("SELECT * FROM cte_view")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn rest_catalog_view_normalizes_cte_identifiers() {
        let catalog = Arc::new(MockCatalog::new());
        add_bigint_view(
            &catalog,
            "default",
            "cte_view",
            "WITH cte_view AS (SELECT CAST(42 AS BIGINT) AS answer) \
             SELECT * FROM \"cte_view\"",
        );
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql("SELECT * FROM cte_view")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn rest_catalog_view_rejects_non_query_sql() {
        let catalog = Arc::new(MockCatalog::new());
        add_bigint_view(
            &catalog,
            "default",
            "unsafe_view",
            "DELETE FROM missing_table",
        );
        let ctx = make_sql_context(catalog).await;

        let error = ctx
            .sql("SELECT * FROM unsafe_view")
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("read-only query"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn bare_rest_sql_function_is_expanded() {
        let catalog = Arc::new(MockCatalog::new());
        add_plus_one_function(&catalog);
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql("SELECT plus_one(41) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn rest_sql_function_normalizes_call_identifiers() {
        let catalog = Arc::new(MockCatalog::new());
        add_plus_one_function(&catalog);
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql("SELECT PAIMON.DEFAULT.PLUS_ONE(41) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn rest_sql_function_accepts_outer_column_argument() {
        let catalog = Arc::new(MockCatalog::new());
        add_plus_one_function(&catalog);
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql("SELECT plus_one(x) AS answer FROM (VALUES (41)) AS t(x)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn fully_qualified_rest_sql_function_is_expanded() {
        let catalog = Arc::new(MockCatalog::new());
        add_plus_one_function(&catalog);
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql("SELECT paimon.default.plus_one(41) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn rest_sql_function_result_is_cast_to_declared_type() {
        let catalog = Arc::new(MockCatalog::new());
        add_unary_sql_function(&catalog, "narrow_body", "CAST(x AS INT)", true);
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql("SELECT narrow_body(41) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 41);
    }

    #[tokio::test]
    async fn rest_sql_function_normalizes_definition_parameters() {
        let catalog = Arc::new(MockCatalog::new());
        add_unary_sql_function(&catalog, "uppercase_parameter", "X + 1", true);
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql("SELECT uppercase_parameter(41) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn rest_sql_function_preserves_quoted_metadata_parameter() {
        let catalog = Arc::new(MockCatalog::new());
        let input_params: Vec<PaimonDataField> = serde_json::from_value(serde_json::json!([
            {"id": 0, "name": "X", "type": "BIGINT"}
        ]))
        .unwrap();
        let return_params: Vec<PaimonDataField> = serde_json::from_value(serde_json::json!([
            {"id": 0, "name": "result", "type": "BIGINT"}
        ]))
        .unwrap();
        catalog.add_function(paimon::catalog::Function::new(
            Identifier::new("default", "quoted_parameter"),
            Some(input_params),
            Some(return_params),
            true,
            HashMap::from([(
                "datafusion".to_string(),
                paimon::catalog::FunctionDefinition::Sql {
                    definition: "\"X\" + 1".to_string(),
                },
            )]),
            None,
            HashMap::new(),
        ));
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql("SELECT quoted_parameter(41) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn rest_sql_function_is_expanded_in_explain() {
        let catalog = Arc::new(MockCatalog::new());
        add_plus_one_function(&catalog);
        let ctx = make_sql_context(catalog).await;

        ctx.sql("EXPLAIN SELECT plus_one(1)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn rest_sql_function_is_expanded_in_time_travel_query() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut options = paimon::Options::new();
        options.set(
            paimon::CatalogOptions::WAREHOUSE,
            temp_dir.path().to_string_lossy(),
        );
        let storage_catalog = Arc::new(paimon::FileSystemCatalog::new(options).unwrap());
        let mut setup = SQLContext::new();
        setup
            .register_catalog("paimon", storage_catalog.clone())
            .await
            .unwrap();
        setup
            .sql("CREATE TABLE paimon.default.time_travel_source (id INT)")
            .await
            .unwrap();
        setup
            .sql("INSERT INTO paimon.default.time_travel_source VALUES (41)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let catalog = Arc::new(MockCatalog::new());
        *catalog.existing_table.lock().unwrap() = Some(
            storage_catalog
                .get_table(&Identifier::new("default", "time_travel_source"))
                .await
                .unwrap(),
        );
        add_plus_one_function(&catalog);
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql(
                "SELECT plus_one(id) AS answer \
                 FROM time_travel_source VERSION AS OF 1",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn rest_sql_function_rejects_undeclared_identifiers() {
        let catalog = Arc::new(MockCatalog::new());
        add_unary_sql_function(&catalog, "captures_column", "x + y", true);
        let ctx = make_sql_context(catalog).await;

        let error = ctx
            .sql("SELECT captures_column(41)")
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("undeclared identifier 'y'"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn non_deterministic_rest_sql_function_is_rejected() {
        let catalog = Arc::new(MockCatalog::new());
        add_unary_sql_function(&catalog, "unsafe_plus_one", "x + 1", false);
        let ctx = make_sql_context(catalog).await;

        let error = ctx
            .sql("SELECT unsafe_plus_one(41)")
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("non-deterministic"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn rest_sql_function_without_single_return_is_rejected() {
        let catalog = Arc::new(MockCatalog::new());
        let input_params: Vec<PaimonDataField> = serde_json::from_value(serde_json::json!([
            {"id": 0, "name": "x", "type": "BIGINT"}
        ]))
        .unwrap();
        catalog.add_function(paimon::catalog::Function::new(
            Identifier::new("default", "missing_return"),
            Some(input_params),
            None,
            true,
            HashMap::from([(
                "datafusion".to_string(),
                paimon::catalog::FunctionDefinition::Sql {
                    definition: "x + 1".to_string(),
                },
            )]),
            None,
            HashMap::new(),
        ));
        let ctx = make_sql_context(catalog).await;

        let error = ctx
            .sql("SELECT missing_return(41)")
            .await
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("exactly one return parameter"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn nested_rest_sql_functions_are_expanded() {
        let catalog = Arc::new(MockCatalog::new());
        add_plus_one_function(&catalog);
        add_unary_sql_function(&catalog, "plus_two", "plus_one(x) + 1", true);
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql("SELECT plus_two(40) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn nested_rest_sql_function_binds_to_owning_database() {
        let catalog = Arc::new(MockCatalog::new());
        add_unary_sql_function(&catalog, "plus_one", "x + 100", true);
        add_unary_sql_function_in_database(&catalog, "other", "plus_one", "x + 1", true);
        add_unary_sql_function_in_database(&catalog, "other", "plus_two", "plus_one(x) + 1", true);
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql("SELECT paimon.other.plus_two(40) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn nested_rest_sql_function_normalizes_owning_database_reference() {
        let catalog = Arc::new(MockCatalog::new());
        add_unary_sql_function(&catalog, "plus_one", "x + 100", true);
        add_unary_sql_function_in_database(&catalog, "other", "plus_one", "x + 1", true);
        add_unary_sql_function_in_database(&catalog, "other", "plus_two", "PLUS_ONE(x) + 1", true);
        let ctx = make_sql_context(catalog).await;

        let batches = ctx
            .sql("SELECT paimon.other.plus_two(40) AS answer")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let answers = batches[0]
            .column_by_name("answer")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(answers.value(0), 42);
    }

    #[tokio::test]
    async fn recursive_rest_sql_functions_are_rejected() {
        let catalog = Arc::new(MockCatalog::new());
        add_unary_sql_function(&catalog, "first", "second(x)", true);
        add_unary_sql_function(&catalog, "second", "first(x)", true);
        let ctx = make_sql_context(catalog).await;

        let error = ctx.sql("SELECT first(1)").await.unwrap_err().to_string();

        assert!(error.contains("recursive"), "unexpected error: {error}");
    }

    #[tokio::test]
    async fn branching_rest_sql_function_expansion_is_bounded() {
        let catalog = Arc::new(MockCatalog::new());
        for index in 0..3 {
            let next = index + 1;
            add_unary_sql_function(
                &catalog,
                &format!("f{index}"),
                &format!("f{next}(x) + f{next}(x)"),
                true,
            );
        }
        add_unary_sql_function(&catalog, "f3", "x", true);
        let ctx = make_sql_context(catalog).await;
        let statement = Parser::parse_sql(&GenericDialect {}, "SELECT f0(1)")
            .unwrap()
            .remove(0);

        let error = crate::sql_function::expand_statement_with_budget(
            statement,
            &ctx.catalogs,
            &ctx.current_catalog_name(),
            "default",
            4,
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(
            error.contains("expansion budget"),
            "unexpected error: {error}"
        );
    }

    // ==================== register_catalog_with_default_db tests ====================

    /// Counts get/create_database calls so tests can assert whether the default-db
    /// init path fired. `get_database` returns `Unsupported` rather than
    /// `DatabaseNotExist` so that *not* skipping the probe surfaces as a hard error
    /// (mimics a "Forbidden: DESCRIBE on DATABASE default" failure).
    struct ProbeTrackingCatalog {
        get_calls: std::sync::atomic::AtomicUsize,
        create_calls: std::sync::atomic::AtomicUsize,
    }

    impl ProbeTrackingCatalog {
        fn new() -> Self {
            Self {
                get_calls: std::sync::atomic::AtomicUsize::new(0),
                create_calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn get_count(&self) -> usize {
            self.get_calls.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn create_count(&self) -> usize {
            self.create_calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Catalog for ProbeTrackingCatalog {
        async fn list_databases(&self) -> paimon::Result<Vec<String>> {
            Ok(vec![])
        }
        async fn create_database(
            &self,
            _name: &str,
            _ignore_if_exists: bool,
            _properties: HashMap<String, String>,
        ) -> paimon::Result<()> {
            self.create_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn get_database(&self, _name: &str) -> paimon::Result<Database> {
            self.get_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(paimon::Error::Unsupported {
                message: "simulated Forbidden".to_string(),
            })
        }
        async fn drop_database(
            &self,
            _name: &str,
            _ignore_if_not_exists: bool,
            _cascade: bool,
        ) -> paimon::Result<()> {
            Ok(())
        }
        async fn get_table(&self, identifier: &Identifier) -> paimon::Result<Table> {
            Err(paimon::Error::TableNotExist {
                full_name: identifier.to_string(),
            })
        }
        async fn list_tables(&self, _database_name: &str) -> paimon::Result<Vec<String>> {
            Ok(vec![])
        }
        async fn create_table(
            &self,
            _identifier: &Identifier,
            _creation: PaimonSchema,
            _ignore_if_exists: bool,
        ) -> paimon::Result<()> {
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
            _from: &Identifier,
            _to: &Identifier,
            _ignore_if_not_exists: bool,
        ) -> paimon::Result<()> {
            Ok(())
        }
        async fn alter_table(
            &self,
            _identifier: &Identifier,
            _changes: Vec<SchemaChange>,
            _ignore_if_not_exists: bool,
        ) -> paimon::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn register_catalog_with_none_skips_default_db_probe() {
        let catalog = Arc::new(ProbeTrackingCatalog::new());
        let mut ctx = SQLContext::new();
        ctx.register_catalog_with_default_db("paimon", catalog.clone(), None)
            .await
            .expect("None must skip probe so Forbidden-shaped error never fires");
        assert_eq!(catalog.get_count(), 0, "get_database must not be called");
        assert_eq!(
            catalog.create_count(),
            0,
            "create_database must not be called"
        );
        // Current catalog still set; current database left unchanged.
        assert_eq!(
            ctx.ctx().state().config().options().catalog.default_catalog,
            "paimon"
        );
    }

    #[tokio::test]
    async fn register_catalog_with_some_default_propagates_probe_error() {
        let catalog = Arc::new(ProbeTrackingCatalog::new());
        let mut ctx = SQLContext::new();
        let err = ctx
            .register_catalog_with_default_db("paimon", catalog.clone(), Some("default"))
            .await
            .expect_err("non-DatabaseNotExist error from get_database must propagate");
        assert!(
            err.to_string().contains("simulated Forbidden"),
            "unexpected error: {err}"
        );
        assert_eq!(catalog.get_count(), 1);
        assert_eq!(catalog.create_count(), 0);
    }

    #[tokio::test]
    async fn register_catalog_with_some_empty_string_is_rejected() {
        // Footgun guard: raw Rust callers could pass `Some("")` and silently probe
        // `get_database("")`. Reject at the API boundary; tell them to use `None` instead.
        let catalog = Arc::new(ProbeTrackingCatalog::new());
        let mut ctx = SQLContext::new();
        let err = ctx
            .register_catalog_with_default_db("paimon", catalog.clone(), Some(""))
            .await
            .expect_err("empty default_db must be rejected at the API");
        assert!(
            err.to_string().contains("must not be empty"),
            "unexpected error: {err}"
        );
        assert_eq!(
            catalog.get_count(),
            0,
            "guard must short-circuit before any catalog call"
        );
    }

    #[tokio::test]
    async fn register_catalog_with_none_table_function_resolves_bare_name_to_literal_default() {
        // Documents the fallback in register_table_functions: `default_db.unwrap_or("default")`.
        // When the caller opts out of default-db init, bare table names inside built-in TVFs
        // (vector_search / full_text_search) still resolve against the literal namespace
        // `"default"` — so a caller using `None` MUST use fully-qualified names with these
        // functions or they'll hit a `default.<name>` lookup that may not exist / be readable.
        let catalog = Arc::new(ProbeTrackingCatalog::new());
        let mut ctx = SQLContext::new();
        ctx.register_catalog_with_default_db("paimon", catalog, None)
            .await
            .unwrap();

        let err = ctx
            .sql("SELECT * FROM vector_search('bare', 'col', '[1.0]', 1)")
            .await
            .expect_err("bare name must error out — no `default.bare` table in mock catalog");
        let msg = err.to_string();
        assert!(
            msg.contains("default") && msg.contains("bare"),
            "error must surface the fallback 'default' namespace + bare name, got: {msg}"
        );
    }

    #[tokio::test]
    async fn register_catalog_default_wrapper_uses_default_db() {
        // The bare register_catalog() must delegate with Some("default"), so the
        // probe fires and Forbidden propagates — same as Some("default") above.
        let catalog = Arc::new(ProbeTrackingCatalog::new());
        let mut ctx = SQLContext::new();
        assert!(ctx
            .register_catalog("paimon", catalog.clone())
            .await
            .is_err());
        assert_eq!(catalog.get_count(), 1);
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
        use datafusion::sql::sqlparser::ast::{CharacterLength, DataType as SqlType};
        for sql_type in [SqlType::Varchar(None), SqlType::Text, SqlType::String(None)] {
            assert_sql_type_to_paimon(
                sql_type.clone(),
                PaimonDataType::VarChar(
                    VarCharType::with_nullable(true, VarCharType::MAX_LENGTH).unwrap(),
                ),
            );
        }
        assert_sql_type_to_paimon(
            SqlType::Char(Some(CharacterLength::IntegerLength {
                length: 7,
                unit: None,
            })),
            PaimonDataType::Char(CharType::with_nullable(true, 7).unwrap()),
        );
        assert_sql_type_to_paimon(
            SqlType::Varchar(Some(CharacterLength::IntegerLength {
                length: 42,
                unit: None,
            })),
            PaimonDataType::VarChar(VarCharType::with_nullable(true, 42).unwrap()),
        );
    }

    #[test]
    fn test_sql_type_binary() {
        use datafusion::sql::sqlparser::ast::{BinaryLength, DataType as SqlType};
        assert_sql_type_to_paimon(
            SqlType::Bytea,
            PaimonDataType::VarBinary(
                VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).unwrap(),
            ),
        );
        assert_sql_type_to_paimon(
            SqlType::Binary(Some(8)),
            PaimonDataType::Binary(BinaryType::with_nullable(true, 8).unwrap()),
        );
        assert_sql_type_to_paimon(
            SqlType::Varbinary(Some(BinaryLength::IntegerLength { length: 32 })),
            PaimonDataType::VarBinary(VarBinaryType::try_new(true, 32).unwrap()),
        );
    }

    #[test]
    fn test_sql_type_variant() {
        use datafusion::sql::sqlparser::ast::{DataType as SqlType, Ident, ObjectName};
        assert_sql_type_to_paimon(
            SqlType::Custom(ObjectName::from(Ident::new("VARIANT")), vec![]),
            PaimonDataType::Variant(VariantType::new()),
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

    #[tokio::test]
    async fn test_resolve_three_part_name() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;
        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, "SELECT * FROM paimon.mydb.mytable").unwrap();
        if let Statement::Query(q) = &stmts[0] {
            if let datafusion::sql::sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                if let datafusion::sql::sqlparser::ast::TableFactor::Table { name, .. } =
                    &sel.from[0].relation
                {
                    let id = sql_context.resolve_table_name(name).unwrap();
                    assert_eq!(id.database(), "mydb");
                    assert_eq!(id.object(), "mytable");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_two_part_name() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;
        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, "SELECT * FROM mydb.mytable").unwrap();
        if let Statement::Query(q) = &stmts[0] {
            if let datafusion::sql::sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                if let datafusion::sql::sqlparser::ast::TableFactor::Table { name, .. } =
                    &sel.from[0].relation
                {
                    let id = sql_context.resolve_table_name(name).unwrap();
                    assert_eq!(id.database(), "mydb");
                    assert_eq!(id.object(), "mytable");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_wrong_catalog_name() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;
        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, "SELECT * FROM other.mydb.mytable").unwrap();
        if let Statement::Query(q) = &stmts[0] {
            if let datafusion::sql::sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                if let datafusion::sql::sqlparser::ast::TableFactor::Table { name, .. } =
                    &sel.from[0].relation
                {
                    let err = sql_context.resolve_table_name(name).unwrap_err();
                    assert!(err.to_string().contains("Unknown catalog"));
                }
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_single_part_name_uses_default_schema() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;
        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, "SELECT * FROM mytable").unwrap();
        if let Statement::Query(q) = &stmts[0] {
            if let datafusion::sql::sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() {
                if let datafusion::sql::sqlparser::ast::TableFactor::Table { name, .. } =
                    &sel.from[0].relation
                {
                    let id = sql_context.resolve_table_name(name).unwrap();
                    assert_eq!(id.database(), "default");
                    assert_eq!(id.object(), "mytable");
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

    // ==================== SQLContext::sql integration tests ====================

    #[tokio::test]
    async fn test_create_table_basic() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
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
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
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
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
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
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
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
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
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
    async fn test_create_table_blob_comment_directives() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
            .sql(
                "CREATE TABLE mydb.t1 (\
                 id INT, \
                 photo BYTES COMMENT '__BLOB_FIELD; raw photo', \
                 thumb BINARY COMMENT '__BLOB_DESCRIPTOR_FIELD', \
                 preview VARBINARY COMMENT '__BLOB_VIEW_FIELD; preview ref'\
                 ) WITH ('data-evolution.enabled' = 'true')",
            )
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::CreateTable { schema, .. } = &calls[0] {
            assert!(matches!(
                schema.fields()[1].data_type(),
                PaimonDataType::Blob(_)
            ));
            assert!(matches!(
                schema.fields()[2].data_type(),
                PaimonDataType::Blob(_)
            ));
            assert!(matches!(
                schema.fields()[3].data_type(),
                PaimonDataType::Blob(_)
            ));
            assert_eq!(schema.fields()[1].description(), Some("raw photo"));
            assert_eq!(schema.fields()[2].description(), None);
            assert_eq!(schema.fields()[3].description(), Some("preview ref"));
            assert_eq!(
                schema.options().get("blob-field").map(String::as_str),
                Some("photo")
            );
            assert_eq!(
                schema
                    .options()
                    .get("blob-descriptor-field")
                    .map(String::as_str),
                Some("thumb")
            );
            assert_eq!(
                schema.options().get("blob-view-field").map(String::as_str),
                Some("preview")
            );
        } else {
            panic!("expected CreateTable call");
        }
    }

    #[tokio::test]
    async fn test_alter_table_add_column() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
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
                matches!(&changes[0], SchemaChange::AddColumn { field_names, .. } if field_names.first().map(String::as_str) == Some("age"))
            );
        } else {
            panic!("expected AlterTable call");
        }
    }

    #[tokio::test]
    async fn test_alter_table_add_blob_column() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
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
                    field_names,
                    data_type,
                    ..
                } if field_names.first().map(String::as_str) == Some("payload") && matches!(data_type, PaimonDataType::Blob(_))
            ));
        } else {
            panic!("expected AlterTable call");
        }
    }

    #[tokio::test]
    async fn test_alter_table_add_blob_comment_directive_passes_core_input() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
            .sql("ALTER TABLE mydb.t1 ADD COLUMN preview BYTES COMMENT '__BLOB_DESCRIPTOR_FIELD; preview descriptor'")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::AlterTable { changes, .. } = &calls[0] {
            assert_eq!(changes.len(), 1);
            assert!(matches!(
                &changes[0],
                SchemaChange::AddColumn {
                    field_names,
                    data_type,
                    comment,
                    ..
                } if field_names.first().map(String::as_str) == Some("preview")
                    && matches!(data_type, PaimonDataType::VarBinary(_))
                    && comment.as_deref() == Some("__BLOB_DESCRIPTOR_FIELD; preview descriptor")
            ));
        } else {
            panic!("expected AlterTable call");
        }
    }

    #[tokio::test]
    async fn test_alter_table_drop_column() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
            .sql("ALTER TABLE mydb.t1 DROP COLUMN age")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::AlterTable { changes, .. } = &calls[0] {
            assert_eq!(changes.len(), 1);
            assert!(
                matches!(&changes[0], SchemaChange::DropColumn { field_names } if field_names.first().map(String::as_str) == Some("age"))
            );
        } else {
            panic!("expected AlterTable call");
        }
    }

    #[tokio::test]
    async fn test_alter_table_rename_column() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
            .sql("ALTER TABLE mydb.t1 RENAME COLUMN old_name TO new_name")
            .await
            .unwrap();

        let calls = catalog.take_calls();
        assert_eq!(calls.len(), 1);
        if let CatalogCall::AlterTable { changes, .. } = &calls[0] {
            assert_eq!(changes.len(), 1);
            assert!(matches!(
                &changes[0],
                SchemaChange::RenameColumn { field_names, new_name }
                    if field_names.first().map(String::as_str) == Some("old_name") && new_name == "new_name"
            ));
        } else {
            panic!("expected AlterTable call");
        }
    }

    #[tokio::test]
    async fn test_alter_table_rename_table() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
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
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
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
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
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
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
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
        let sql_context = make_sql_context(catalog.clone()).await;

        sql_context
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
        let sql_context = make_sql_context(catalog).await;
        let result = sql_context.sql("NOT VALID SQL !!!").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("SQL parse error"));
    }

    #[tokio::test]
    async fn test_multiple_statements_error() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;
        let result = sql_context.sql("SELECT 1; SELECT 2").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("exactly one SQL statement"));
    }

    #[tokio::test]
    async fn test_create_external_table_rejected() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;
        let result = sql_context
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
        let sql_context = make_sql_context(catalog.clone()).await;
        // SELECT should be delegated to DataFusion, not intercepted
        let df = sql_context.sql("SELECT 1 AS x").await.unwrap();
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
    fn test_extract_partition_by_double_quoted_identifier_with_escaped_quote_and_comma() {
        let (_, keys) = extract_partition_by(
            "CREATE TABLE t (\"a\"\"b,c\" INT, `d``e,f` INT) \
             PARTITIONED BY (\"a\"\"b,c\", `d``e,f`)",
        )
        .unwrap();
        assert_eq!(keys, vec!["a\"b,c", "d`e,f"]);
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
        let sql_context = make_sql_context(catalog).await;
        let err = sql_context
            .sql("CREATE TABLE mydb.t (id INT, dt STRING) PARTITIONED BY (nonexistent)")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("is not defined in the table"));
    }

    #[tokio::test]
    async fn test_create_table_partition_key_matches_column() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog.clone()).await;
        sql_context
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
        let sql_context = make_sql_context(catalog).await;
        sql_context
            .sql("SET 'paimon.scan.version' = '1'")
            .await
            .unwrap();
        let opts = sql_context.dynamic_options().read().unwrap();
        assert_eq!(opts.get("scan.version").unwrap(), "1");
    }

    #[tokio::test]
    async fn test_set_paimon_option_overwrites() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;
        sql_context
            .sql("SET 'paimon.scan.version' = '1'")
            .await
            .unwrap();
        sql_context
            .sql("SET 'paimon.scan.version' = '2'")
            .await
            .unwrap();
        let opts = sql_context.dynamic_options().read().unwrap();
        assert_eq!(opts.get("scan.version").unwrap(), "2");
    }

    #[tokio::test]
    async fn test_reset_paimon_option() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;
        sql_context
            .sql("SET 'paimon.scan.version' = '1'")
            .await
            .unwrap();
        sql_context
            .sql("RESET 'paimon.scan.version'")
            .await
            .unwrap();
        let opts = sql_context.dynamic_options().read().unwrap();
        assert!(opts.get("scan.version").is_none());
    }

    #[tokio::test]
    async fn test_set_non_paimon_option_delegates() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;
        // DataFusion handles non-paimon SET; should not error and should not
        // appear in dynamic_options.
        let _ = sql_context
            .sql("SET datafusion.optimizer.max_passes = 3")
            .await;
        let opts = sql_context.dynamic_options().read().unwrap();
        assert!(opts.is_empty());
    }

    #[tokio::test]
    async fn test_set_multiple_paimon_options() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;
        sql_context
            .sql("SET 'paimon.scan.version' = '1'")
            .await
            .unwrap();
        sql_context
            .sql("SET 'paimon.scan.timestamp-millis' = '1000'")
            .await
            .unwrap();
        let opts = sql_context.dynamic_options().read().unwrap();
        assert_eq!(opts.len(), 2);
        assert_eq!(opts.get("scan.version").unwrap(), "1");
        assert_eq!(opts.get("scan.timestamp-millis").unwrap(), "1000");
    }

    #[tokio::test]
    async fn test_reset_nonexistent_paimon_option_is_noop() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;
        sql_context
            .sql("RESET 'paimon.scan.version'")
            .await
            .unwrap();
        let opts = sql_context.dynamic_options().read().unwrap();
        assert!(opts.is_empty());
    }

    // ==================== TRUNCATE TABLE / DROP PARTITIONS tests ====================

    async fn setup_fs_sql_context() -> (tempfile::TempDir, SQLContext) {
        use paimon::{CatalogOptions, FileSystemCatalog, Options};

        let temp_dir = tempfile::TempDir::new().unwrap();
        let warehouse = format!("file://{}", temp_dir.path().display());
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse);
        let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());

        let mut sql_context = SQLContext::new();
        sql_context
            .register_catalog("paimon", catalog.clone())
            .await
            .unwrap();
        sql_context
            .sql("CREATE SCHEMA paimon.test_db")
            .await
            .unwrap();

        (temp_dir, sql_context)
    }

    #[tokio::test]
    async fn test_truncate_table() {
        let (_tmp, sql_context) = setup_fs_sql_context().await;

        sql_context
            .sql("CREATE TABLE paimon.test_db.t1 (id INT, value INT)")
            .await
            .unwrap();
        sql_context
            .sql("INSERT INTO paimon.test_db.t1 VALUES (1, 10), (2, 20)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        sql_context
            .sql("TRUNCATE TABLE paimon.test_db.t1")
            .await
            .unwrap();

        let batches = sql_context
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
        let (_tmp, sql_context) = setup_fs_sql_context().await;

        sql_context
            .sql("CREATE TABLE paimon.test_db.t2 (pt VARCHAR, id INT) PARTITIONED BY (pt)")
            .await
            .unwrap();
        sql_context
            .sql("INSERT INTO paimon.test_db.t2 VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        sql_context
            .sql("TRUNCATE TABLE paimon.test_db.t2 PARTITION (pt = 'a')")
            .await
            .unwrap();

        let batches = sql_context
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
        let (_tmp, sql_context) = setup_fs_sql_context().await;

        sql_context
            .sql("CREATE TABLE paimon.test_db.t3 (pt VARCHAR, id INT) PARTITIONED BY (pt)")
            .await
            .unwrap();
        sql_context
            .sql("INSERT INTO paimon.test_db.t3 VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        sql_context
            .sql("ALTER TABLE paimon.test_db.t3 DROP PARTITION (pt = 'b')")
            .await
            .unwrap();

        let batches = sql_context
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
        let (_tmp, sql_context) = setup_fs_sql_context().await;

        sql_context
            .sql("CREATE TABLE paimon.test_db.t_multi (pt1 VARCHAR, pt2 VARCHAR, id INT) PARTITIONED BY (pt1, pt2)")
            .await
            .unwrap();
        sql_context
            .sql("INSERT INTO paimon.test_db.t_multi VALUES ('a', 'x', 1)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let err = sql_context
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
        let (_tmp, sql_context) = setup_fs_sql_context().await;

        sql_context
            .sql("TRUNCATE TABLE IF EXISTS paimon.test_db.nonexistent")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_truncate_table_nonexistent_without_if_exists() {
        let (_tmp, sql_context) = setup_fs_sql_context().await;

        let err = sql_context
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
        let (_tmp, sql_context) = setup_fs_sql_context().await;

        sql_context
            .sql("ALTER TABLE IF EXISTS paimon.test_db.nonexistent DROP PARTITION (pt = 'a')")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_drop_partition_incomplete_spec() {
        let (_tmp, sql_context) = setup_fs_sql_context().await;

        sql_context
            .sql("CREATE TABLE paimon.test_db.t_dp (pt1 VARCHAR, pt2 VARCHAR, id INT) PARTITIONED BY (pt1, pt2)")
            .await
            .unwrap();
        sql_context
            .sql("INSERT INTO paimon.test_db.t_dp VALUES ('a', 'x', 1)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let err = sql_context
            .sql("ALTER TABLE paimon.test_db.t_dp DROP PARTITION (pt1 = 'a')")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Incomplete partition spec"),
            "Expected incomplete partition spec error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_create_temp_table_if_not_exists() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;

        // First creation succeeds
        sql_context
            .sql("CREATE TEMPORARY TABLE mydb.t1 (id INT)")
            .await
            .unwrap();

        // Second creation without IF NOT EXISTS should fail
        let err = sql_context
            .sql("CREATE TEMPORARY TABLE mydb.t1 (id INT)")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("already exists"),
            "Expected already-exists error, got: {err}"
        );

        // With IF NOT EXISTS, it should succeed silently
        sql_context
            .sql("CREATE TEMPORARY TABLE IF NOT EXISTS mydb.t1 (id INT)")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_temp_table_if_not_exists_as_select() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;

        // Create temp table with AS SELECT
        sql_context
            .sql("CREATE TEMPORARY TABLE mydb.t2 AS SELECT 1 AS id")
            .await
            .unwrap();

        // IF NOT EXISTS should skip when the table already exists
        sql_context
            .sql("CREATE TEMPORARY TABLE IF NOT EXISTS mydb.t2 AS SELECT 2 AS id")
            .await
            .unwrap();

        // Verify the original data is still there (not overwritten)
        let df = sql_context.sql("SELECT * FROM mydb.t2").await.unwrap();
        let batches = df.collect().await.unwrap();
        let val = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(val.value(0), 1);
    }

    #[tokio::test]
    async fn test_create_temp_view_if_not_exists() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;

        // First creation succeeds
        sql_context
            .sql("CREATE TEMPORARY VIEW mydb.v1 AS SELECT 1 AS id")
            .await
            .unwrap();

        // Second creation without IF NOT EXISTS should fail
        let err = sql_context
            .sql("CREATE TEMPORARY VIEW mydb.v1 AS SELECT 2 AS id")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("already exists"),
            "Expected already-exists error, got: {err}"
        );

        // With IF NOT EXISTS, it should succeed silently
        sql_context
            .sql("CREATE TEMPORARY VIEW IF NOT EXISTS mydb.v1 AS SELECT 3 AS id")
            .await
            .unwrap();

        // Verify the original view is still intact
        let df = sql_context.sql("SELECT * FROM mydb.v1").await.unwrap();
        let batches = df.collect().await.unwrap();
        let val = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(val.value(0), 1);
    }

    #[tokio::test]
    async fn test_drop_temp_table_if_exists() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;

        // Dropping a nonexistent temp table without IF EXISTS should error
        let err = sql_context
            .sql("DROP TEMPORARY TABLE mydb.nonexistent")
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("doesn't exist")
                || msg.contains("does not exist")
                || msg.contains("Unknown temp database"),
            "Expected table-not-exist error, got: {msg}"
        );

        // Dropping with IF EXISTS should succeed silently
        sql_context
            .sql("DROP TEMPORARY TABLE IF EXISTS mydb.nonexistent")
            .await
            .unwrap();

        // Create, then drop with IF EXISTS should actually drop it
        sql_context
            .sql("CREATE TEMPORARY TABLE mydb.t1 (id INT)")
            .await
            .unwrap();

        sql_context
            .sql("DROP TEMPORARY TABLE IF EXISTS mydb.t1")
            .await
            .unwrap();

        // Verify the table is gone
        assert!(
            !sql_context.temp_table_exist("mydb.t1").unwrap(),
            "Expected temp table to be gone after DROP"
        );
    }

    #[tokio::test]
    async fn test_drop_temp_view_if_exists() {
        let catalog = Arc::new(MockCatalog::new());
        let sql_context = make_sql_context(catalog).await;

        // Dropping a nonexistent temp view without IF EXISTS should error
        let err = sql_context
            .sql("DROP TEMPORARY VIEW mydb.nonexistent")
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("doesn't exist")
                || msg.contains("does not exist")
                || msg.contains("Unknown temp database"),
            "Expected view-not-exist error, got: {msg}"
        );

        // Dropping with IF EXISTS should succeed silently
        sql_context
            .sql("DROP TEMPORARY VIEW IF EXISTS mydb.nonexistent")
            .await
            .unwrap();

        // Create a temp view, then drop with IF EXISTS
        sql_context
            .sql("CREATE TEMPORARY VIEW mydb.v1 AS SELECT 1 AS id")
            .await
            .unwrap();

        sql_context
            .sql("DROP TEMPORARY VIEW IF EXISTS mydb.v1")
            .await
            .unwrap();

        // Verify the view is gone
        assert!(
            !sql_context.temp_table_exist("mydb.v1").unwrap(),
            "Expected temp view to be gone after DROP"
        );
    }

    #[test]
    fn test_extract_version_as_of() {
        let sql = "SELECT id, name FROM paimon.default.time_travel_table VERSION AS OF 1";
        let infos = extract_all_version_as_of(sql);
        assert_eq!(infos.len(), 1);
        let info = &infos[0];
        assert_eq!(info.version, "1");
        assert_eq!(info.table_name, "paimon.default.time_travel_table");
        let rewritten = format!(
            "{}__uuid{}",
            &sql[..info.clause_range.0],
            &sql[info.clause_range.1..]
        );
        assert_eq!(rewritten, "SELECT id, name FROM __uuid");
    }

    #[test]
    fn test_extract_version_as_of_multi_digit() {
        let sql = "SELECT * FROM mydb.t VERSION AS OF 42";
        let infos = extract_all_version_as_of(sql);
        assert_eq!(infos.len(), 1);
        let info = &infos[0];
        assert_eq!(info.version, "42");
        assert_eq!(info.table_name, "mydb.t");
        let rewritten = format!(
            "{}__uuid{}",
            &sql[..info.clause_range.0],
            &sql[info.clause_range.1..]
        );
        assert_eq!(rewritten, "SELECT * FROM __uuid");
    }

    #[test]
    fn test_extract_version_as_of_case_insensitive() {
        let sql = "SELECT * FROM t version as of 5";
        let infos = extract_all_version_as_of(sql);
        assert_eq!(infos.len(), 1);
        let info = &infos[0];
        assert_eq!(info.version, "5");
        assert_eq!(info.table_name, "t");
        let rewritten = format!(
            "{}__uuid{}",
            &sql[..info.clause_range.0],
            &sql[info.clause_range.1..]
        );
        assert_eq!(rewritten, "SELECT * FROM __uuid");
    }

    #[test]
    fn test_extract_version_as_of_not_present() {
        let sql = "SELECT * FROM t";
        assert!(extract_all_version_as_of(sql).is_empty());
    }

    #[test]
    fn test_extract_version_as_of_tag() {
        let sql = "SELECT id, name FROM paimon.default.t VERSION AS OF 'snapshot1'";
        let infos = extract_all_version_as_of(sql);
        assert_eq!(infos.len(), 1);
        let info = &infos[0];
        assert_eq!(info.version, "snapshot1");
        assert_eq!(info.table_name, "paimon.default.t");
        let rewritten = format!(
            "{}__uuid{}",
            &sql[..info.clause_range.0],
            &sql[info.clause_range.1..]
        );
        assert_eq!(rewritten, "SELECT id, name FROM __uuid");
    }

    #[test]
    fn test_extract_version_as_of_tag_case_insensitive() {
        let sql = "SELECT * FROM t version as of 'my_tag'";
        let infos = extract_all_version_as_of(sql);
        assert_eq!(infos.len(), 1);
        let info = &infos[0];
        assert_eq!(info.version, "my_tag");
        assert_eq!(info.table_name, "t");
        let rewritten = format!(
            "{}__uuid{}",
            &sql[..info.clause_range.0],
            &sql[info.clause_range.1..]
        );
        assert_eq!(rewritten, "SELECT * FROM __uuid");
    }

    #[test]
    fn test_extract_version_as_of_numeric_still_works() {
        let sql = "SELECT * FROM t VERSION AS OF 123";
        let infos = extract_all_version_as_of(sql);
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].version, "123");
        assert_eq!(infos[0].table_name, "t");
    }

    #[test]
    fn test_extract_version_as_of_multiple() {
        // JOIN two time-travel tables
        let sql = "SELECT * FROM t1 VERSION AS OF 1 JOIN t2 VERSION AS OF 2 ON t1.id = t2.id";
        let infos = extract_all_version_as_of(sql);
        assert_eq!(infos.len(), 2);
        assert_eq!(infos[0].version, "1");
        assert_eq!(infos[0].table_name, "t1");
        assert_eq!(infos[1].version, "2");
        assert_eq!(infos[1].table_name, "t2");
    }

    #[test]
    fn test_extract_version_as_of_skips_string_literal() {
        let sql = "SELECT * FROM t WHERE note = 'version as of 1'";
        let infos = extract_all_version_as_of(sql);
        assert!(infos.is_empty());
    }

    #[test]
    fn test_extract_version_as_of_skips_comment() {
        let sql = "SELECT * FROM t -- version as of 1\n WHERE id > 0";
        let infos = extract_all_version_as_of(sql);
        assert!(infos.is_empty());
    }

    #[test]
    fn test_contains_time_travel_keyword() {
        assert!(contains_time_travel_keyword(
            "SELECT * FROM t VERSION AS OF 1"
        ));
        assert!(contains_time_travel_keyword(
            "SELECT * FROM t TIMESTAMP AS OF '2024-01-01 00:00:00'"
        ));
        // Inside string literal — should NOT match
        assert!(!contains_time_travel_keyword(
            "SELECT * FROM t WHERE note = 'version as of 1'"
        ));
        // Inside comment — should NOT match
        assert!(!contains_time_travel_keyword(
            "SELECT * FROM t -- version as of 1"
        ));
        assert!(!contains_time_travel_keyword(
            "SELECT * FROM t /* timestamp as of now */ WHERE id > 0"
        ));
        // No keyword at all
        assert!(!contains_time_travel_keyword("SELECT * FROM t"));
    }

    #[test]
    fn test_extract_timestamp_as_of() {
        let sql = "SELECT * FROM paimon.default.t TIMESTAMP AS OF '2024-01-15 10:30:00'";
        let infos = extract_all_timestamp_as_of(sql);
        assert_eq!(infos.len(), 1);
        let info = &infos[0];
        assert_eq!(info.timestamp, "2024-01-15 10:30:00");
        assert_eq!(info.table_name, "paimon.default.t");
        let rewritten = format!(
            "{}__uuid{}",
            &sql[..info.clause_range.0],
            &sql[info.clause_range.1..]
        );
        assert_eq!(rewritten, "SELECT * FROM __uuid");
    }

    #[test]
    fn test_extract_timestamp_as_of_case_insensitive() {
        let sql = "SELECT * FROM t timestamp as of '2024-06-01 00:00:00'";
        let infos = extract_all_timestamp_as_of(sql);
        assert_eq!(infos.len(), 1);
        let info = &infos[0];
        assert_eq!(info.timestamp, "2024-06-01 00:00:00");
        assert_eq!(info.table_name, "t");
        let rewritten = format!(
            "{}__uuid{}",
            &sql[..info.clause_range.0],
            &sql[info.clause_range.1..]
        );
        assert_eq!(rewritten, "SELECT * FROM __uuid");
    }

    #[test]
    fn test_extract_timestamp_as_of_not_present() {
        let sql = "SELECT * FROM t";
        assert!(extract_all_timestamp_as_of(sql).is_empty());
    }
}
