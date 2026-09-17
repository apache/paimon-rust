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

//! Paimon catalog integration for DataFusion.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::common::{plan_datafusion_err, Column};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{expr_fn::cast, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::sqlparser::ast::{Ident, ObjectName, Query, Statement, Visit, Visitor};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use paimon::catalog::{Catalog, Identifier, View};
use paimon::spec::TableType as PaimonTableType;

use crate::error::to_datafusion_error;
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::system_tables;
use crate::table::{ObjectTableProvider, PaimonTableProvider};
use crate::{BlobReaderRegistry, DynamicOptions};

pub(crate) type SessionStateProvider = Arc<dyn Fn() -> Option<SessionState> + Send + Sync>;

/// Engine registry shared between the catalog provider and its schema
/// providers, so registrations stay visible to schemas obtained earlier.
type TableEngines = Arc<RwLock<HashMap<PaimonTableType, Arc<dyn TableEngineResolver>>>>;

/// What an engine is asked to resolve. Non-exhaustive so later releases can
/// carry more of the request — a snapshot selector, say — without breaking
/// existing resolvers.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EngineTableRequest {
    pub database: String,
    pub table: String,
    /// The type the table's metadata declares.
    pub declared: PaimonTableType,
}

impl EngineTableRequest {
    pub fn new(database: String, table: String, declared: PaimonTableType) -> Self {
        Self {
            database,
            table,
            declared,
        }
    }
}

/// Resolves tables owned by another engine (see
/// [`PaimonCatalogProvider::register_table_engine`]). `Ok(None)` means not
/// found; errors propagate, so an engine failure never looks like a missing
/// table.
#[async_trait]
pub trait TableEngineResolver: Debug + Send + Sync {
    /// Resolve a request to the engine's table provider.
    async fn resolve_table(
        &self,
        request: &EngineTableRequest,
    ) -> DFResult<Option<Arc<dyn TableProvider>>>;
}

/// Read-only wrapper around an engine-resolved provider: reads delegate,
/// DML is rejected even when the engine's own provider is writable.
#[derive(Debug)]
pub(crate) struct ReadOnlyTableProvider {
    inner: Arc<dyn TableProvider>,
    pub(crate) declared: PaimonTableType,
    pub(crate) table_name: String,
}

#[async_trait]
impl TableProvider for ReadOnlyTableProvider {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&datafusion::common::Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<std::borrow::Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _input: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Err(plan_datafusion_err!(
            "write is not supported for routed '{}' tables ('{}')",
            self.declared,
            self.table_name
        ))
    }
}

/// Metadata-only provider for an external table whose engine is not registered.
///
/// DataFusion's information schema asks every catalog table for its schema.
/// Returning this provider keeps those metadata queries available without
/// weakening the fail-closed behavior for actual table reads or writes.
#[derive(Debug)]
struct UnavailableEngineTableProvider {
    schema: datafusion::arrow::datatypes::SchemaRef,
    declared: PaimonTableType,
    table_name: String,
}

impl UnavailableEngineTableProvider {
    fn unavailable_error(&self) -> datafusion::error::DataFusionError {
        plan_datafusion_err!(
            "no table engine is registered for '{}' tables ('{}')",
            self.declared,
            self.table_name
        )
    }
}

#[async_trait]
impl TableProvider for UnavailableEngineTableProvider {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Err(self.unavailable_error())
    }

    async fn insert_into(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _input: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        _insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Err(self.unavailable_error())
    }
}

/// Register `resolver` as the engine for `table_type` on the Paimon catalog
/// named `catalog_name`.
///
/// Also installs [`PaimonRelationPlanner`](crate::PaimonRelationPlanner), so
/// version clauses cannot slip past this crate (see [`PaimonCatalogProvider`]).
/// [`SQLContext::register_catalog_table_engine`](crate::SQLContext::register_catalog_table_engine)
/// is equivalent.
pub fn register_catalog_table_engine(
    ctx: &SessionContext,
    catalog_name: &str,
    table_type: PaimonTableType,
    resolver: Arc<dyn TableEngineResolver>,
) -> DFResult<()> {
    ctx.register_relation_planner(Arc::new(crate::PaimonRelationPlanner::new()))?;
    let provider = ctx
        .catalog(catalog_name)
        .ok_or_else(|| plan_datafusion_err!("Unknown catalog '{catalog_name}'"))?;
    provider
        .downcast_ref::<PaimonCatalogProvider>()
        .ok_or_else(|| plan_datafusion_err!("Catalog '{catalog_name}' is not a Paimon catalog"))?
        .register_table_engine(table_type, resolver)
}

/// Provides an interface to manage and access multiple schemas (databases)
/// within a Paimon [`Catalog`].
///
/// This provider uses lazy loading - databases and tables are fetched
/// on-demand from the catalog, ensuring data is always fresh.
///
/// # Table-version clauses
///
/// SQL queries using `VERSION`/`TIMESTAMP AS OF` need
/// [`PaimonRelationPlanner`](crate::PaimonRelationPlanner) installed on the
/// session; DataFusion's default planner drops the clause before this crate
/// sees it, so the query would read current data. [`SQLContext`](crate::SQLContext)
/// installs it, as does
/// [`register_catalog_table_engine`]. A plain `SessionContext` querying Paimon
/// tables directly must install it with
/// `ctx.register_relation_planner(Arc::new(PaimonRelationPlanner::new()))`.
pub struct PaimonCatalogProvider {
    catalog_name: Option<String>,
    /// Reference to the Paimon catalog.
    catalog: Arc<dyn Catalog>,
    /// Session-scoped dynamic options shared with the SQL context.
    dynamic_options: DynamicOptions,
    /// Temporary in-memory tables and views stored in MemorySchemaProvider per database.
    ///
    /// Uses `RwLock` with poison recovery (`unwrap_or_else(|e| e.into_inner())`) throughout.
    /// This is a deliberate choice: since temp tables are session-scoped and non-critical,
    /// it is preferable to continue with potentially stale data after a panic rather than
    /// propagate the panic to all subsequent operations. The worst case is a temp table
    /// becoming invisible or stale, which is recoverable by re-registering it.
    temp_tables: Arc<RwLock<HashMap<String, Arc<MemorySchemaProvider>>>>,
    blob_reader_registry: BlobReaderRegistry,
    session_state: Option<SessionStateProvider>,
    schema_force_view_types: bool,
    /// Engines for table types served elsewhere, keyed by declared
    /// [`PaimonTableType`]. Same poison-recovery stance as `temp_tables`.
    table_engines: TableEngines,
}

impl Debug for PaimonCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaimonCatalogProvider").finish()
    }
}

impl PaimonCatalogProvider {
    /// Creates a new [`PaimonCatalogProvider`].
    pub fn new(
        catalog_name: Option<String>,
        catalog: Arc<dyn Catalog>,
        dynamic_options: DynamicOptions,
        blob_reader_registry: BlobReaderRegistry,
        session_state: Option<SessionStateProvider>,
    ) -> Self {
        PaimonCatalogProvider {
            catalog_name,
            catalog,
            dynamic_options,
            temp_tables: Arc::new(RwLock::new(HashMap::new())),
            blob_reader_registry,
            session_state,
            schema_force_view_types: true,
            table_engines: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Configure whether table schemas use Arrow view types when available.
    ///
    /// Disable this for consumers that cannot operate on Arrow view arrays. This changes the
    /// schema exposed to DataFusion, so query operators above the table scan will use the classic
    /// Arrow types as well.
    pub fn with_schema_force_view_types(mut self, schema_force_view_types: bool) -> Self {
        self.schema_force_view_types = schema_force_view_types;
        self
    }

    /// Register an engine for a table type the Paimon reader cannot serve
    /// (e.g. [`PaimonTableType::IcebergTable`]); everything else takes the
    /// Paimon path unchanged. Kept inside the provider so the registered
    /// catalog type never changes and downcast-based paths (temp tables,
    /// time travel) keep working.
    pub(crate) fn register_table_engine(
        &self,
        table_type: PaimonTableType,
        resolver: Arc<dyn TableEngineResolver>,
    ) -> DFResult<()> {
        // Routing a Paimon-served type would split it between engines:
        // reads via the resolver, raw get_table paths via Paimon.
        if !table_type.requires_table_engine() {
            return Err(plan_datafusion_err!(
                "table type '{table_type}' is served by the Paimon reader and cannot be \
                 routed to a table engine"
            ));
        }
        self.table_engines
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(table_type, resolver);
        Ok(())
    }

    fn table_engines(&self) -> TableEngines {
        Arc::clone(&self.table_engines)
    }

    fn paimon_schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let catalog = Arc::clone(&self.catalog);
        let table_engines = self.table_engines();
        let dynamic_options = Arc::clone(&self.dynamic_options);
        let blob_reader_registry = self.blob_reader_registry.clone();
        let catalog_name = self.catalog_name.clone();
        let session_state = self.session_state.clone();
        let schema_force_view_types = self.schema_force_view_types;
        let name = name.to_string();

        let temp_provider = {
            let databases = self.temp_tables.read().unwrap_or_else(|e| e.into_inner());
            databases.get(&name).cloned()
        };

        block_on_with_runtime(
            async move {
                match catalog.get_database(&name).await {
                    Ok(_) => Some(Arc::new(
                        PaimonSchemaProvider::new(
                            catalog_name,
                            Arc::clone(&catalog),
                            name,
                            dynamic_options,
                            temp_provider,
                            blob_reader_registry,
                            session_state,
                        )
                        .with_schema_force_view_types(schema_force_view_types)
                        .with_table_engines(Arc::clone(&table_engines)),
                    ) as Arc<dyn SchemaProvider>),
                    Err(paimon::Error::DatabaseNotExist { .. }) => {
                        if temp_provider.is_some() {
                            Some(Arc::new(
                                PaimonSchemaProvider::new(
                                    catalog_name,
                                    Arc::clone(&catalog),
                                    name,
                                    dynamic_options,
                                    temp_provider,
                                    blob_reader_registry,
                                    session_state,
                                )
                                .with_schema_force_view_types(schema_force_view_types)
                                .with_table_engines(Arc::clone(&table_engines)),
                            ) as Arc<dyn SchemaProvider>)
                        } else {
                            None
                        }
                    }
                    Err(e) => {
                        log::error!("failed to get database '{}': {e}", name);
                        None
                    }
                }
            },
            "paimon catalog access thread panicked",
        )
    }
}

impl CatalogProvider for PaimonCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        let catalog = Arc::clone(&self.catalog);
        block_on_with_runtime(
            async move {
                catalog.list_databases().await.unwrap_or_else(|e| {
                    log::error!("failed to list databases: {e}");
                    vec![]
                })
            },
            "paimon catalog access thread panicked",
        )
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.paimon_schema(name)
    }

    fn register_schema(
        &self,
        name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        let catalog = Arc::clone(&self.catalog);
        let dynamic_options = Arc::clone(&self.dynamic_options);
        let blob_reader_registry = self.blob_reader_registry.clone();
        let catalog_name = self.catalog_name.clone();
        let session_state = self.session_state.clone();
        let schema_force_view_types = self.schema_force_view_types;
        let name = name.to_string();
        block_on_with_runtime(
            async move {
                catalog
                    .create_database(&name, false, HashMap::new())
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(Some(Arc::new(
                    PaimonSchemaProvider::new(
                        catalog_name,
                        Arc::clone(&catalog),
                        name,
                        dynamic_options,
                        None,
                        blob_reader_registry,
                        session_state,
                    )
                    .with_schema_force_view_types(schema_force_view_types),
                ) as Arc<dyn SchemaProvider>))
            },
            "paimon catalog access thread panicked",
        )
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> DFResult<Option<Arc<dyn SchemaProvider>>> {
        let catalog = Arc::clone(&self.catalog);
        let dynamic_options = Arc::clone(&self.dynamic_options);
        let blob_reader_registry = self.blob_reader_registry.clone();
        let catalog_name = self.catalog_name.clone();
        let session_state = self.session_state.clone();
        let schema_force_view_types = self.schema_force_view_types;
        let name = name.to_string();
        block_on_with_runtime(
            async move {
                catalog
                    .drop_database(&name, false, cascade)
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(Some(Arc::new(
                    PaimonSchemaProvider::new(
                        catalog_name,
                        Arc::clone(&catalog),
                        name,
                        dynamic_options,
                        None,
                        blob_reader_registry,
                        session_state,
                    )
                    .with_schema_force_view_types(schema_force_view_types),
                ) as Arc<dyn SchemaProvider>))
            },
            "paimon catalog access thread panicked",
        )
    }
}

impl PaimonCatalogProvider {
    /// Registers a temporary table or view in the specified database.
    /// Creates the database if it does not exist.
    ///
    /// Returns an error if a temp table with the same name already exists in
    /// the same database. Logs a warning if the name shadows a real Paimon table.
    pub fn register_temp_table(
        &self,
        database: &str,
        table_name: &str,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<()> {
        // Warn if this shadows a real Paimon table (outside the lock — not critical)
        let catalog = Arc::clone(&self.catalog);
        let db = database.to_string();
        let tbl = table_name.to_string();
        let identifier = Identifier::new(db, tbl);
        if let Ok(true) = block_on_with_runtime(
            async move {
                match catalog.get_table(&identifier).await {
                    Ok(_) => Ok::<bool, paimon::Error>(true),
                    Err(paimon::Error::TableNotExist { .. }) => Ok(false),
                    Err(_) => Ok(false),
                }
            },
            "paimon catalog access thread panicked",
        ) {
            log::warn!(
                "Temporary table '{database}.{table_name}' shadows an existing Paimon table"
            );
        }

        // Atomically check-then-register under a single write lock to avoid TOCTOU
        let mut databases = self.temp_tables.write().unwrap_or_else(|e| e.into_inner());
        let mem_database = databases
            .entry(database.to_string())
            .or_insert_with(|| Arc::new(MemorySchemaProvider::new()));

        // register_table returns Ok(Some(old_table)) if the name already existed
        let old = mem_database.register_table(table_name.to_string(), table)?;
        if old.is_some() {
            return Err(plan_datafusion_err!(
                "Temporary table '{database}.{table_name}' already exists"
            ));
        }
        Ok(())
    }

    /// Deregisters a temporary table or view from the specified database.
    pub fn deregister_temp_table(
        &self,
        database: &str,
        table_name: &str,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let databases = self.temp_tables.read().unwrap_or_else(|e| e.into_inner());
        let mem_database = databases
            .get(database)
            .ok_or_else(|| plan_datafusion_err!("Unknown temp database '{database}'"))?;
        mem_database.deregister_table(table_name)
    }

    /// Returns whether a temp table database exists with the given name.
    pub fn has_temp_table_database(&self, name: &str) -> bool {
        self.temp_tables
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(name)
    }

    /// Returns whether a temp table with the given name exists in the specified database.
    pub fn temp_table_exist(&self, database: &str, table_name: &str) -> bool {
        let databases = self.temp_tables.read().unwrap_or_else(|e| e.into_inner());
        databases
            .get(database)
            .is_some_and(|db| db.table_exist(table_name))
    }
}

/// Represents a [`SchemaProvider`] for the Paimon [`Catalog`], managing
/// access to table providers within a specific database.
///
/// Tables are loaded lazily when accessed via the `table()` method.
pub struct PaimonSchemaProvider {
    catalog_name: Option<String>,
    /// Reference to the Paimon catalog.
    catalog: Arc<dyn Catalog>,
    /// Database name this schema represents.
    database: String,
    /// Session-scoped dynamic options shared with the SQL context.
    dynamic_options: DynamicOptions,
    /// Optional temporary in-memory provider for temp tables and views.
    temp_provider: Option<Arc<MemorySchemaProvider>>,
    /// Table types populated together with `table_names` for metadata-only lookups.
    catalog_table_types: RwLock<HashMap<String, TableType>>,
    blob_reader_registry: BlobReaderRegistry,
    session_state: Option<SessionStateProvider>,
    schema_force_view_types: bool,
    /// Engines for table types served elsewhere; empty without routing.
    table_engines: TableEngines,
}

impl Debug for PaimonSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaimonSchemaProvider")
            .field("database", &self.database)
            .field("has_temp_provider", &self.temp_provider.is_some())
            .finish()
    }
}

impl PaimonSchemaProvider {
    /// Creates a new [`PaimonSchemaProvider`].
    pub fn new(
        catalog_name: Option<String>,
        catalog: Arc<dyn Catalog>,
        database: String,
        dynamic_options: DynamicOptions,
        temp_provider: Option<Arc<MemorySchemaProvider>>,
        blob_reader_registry: BlobReaderRegistry,
        session_state: Option<SessionStateProvider>,
    ) -> Self {
        PaimonSchemaProvider {
            catalog_name,
            catalog,
            database,
            dynamic_options,
            temp_provider,
            catalog_table_types: RwLock::new(HashMap::new()),
            blob_reader_registry,
            session_state,
            schema_force_view_types: true,
            table_engines: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn with_schema_force_view_types(mut self, schema_force_view_types: bool) -> Self {
        self.schema_force_view_types = schema_force_view_types;
        self
    }

    pub(crate) fn with_table_engines(mut self, table_engines: TableEngines) -> Self {
        self.table_engines = table_engines;
        self
    }
}

#[async_trait]
impl SchemaProvider for PaimonSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        let catalog = Arc::clone(&self.catalog);
        let database = self.database.clone();
        let (mut names, views) = block_on_with_runtime(
            {
                let db = database.clone();
                async move {
                    let names = match catalog.list_tables(&db).await {
                        Ok(names) => names,
                        Err(e) => {
                            log::error!("failed to list tables in '{}': {e}", db);
                            vec![]
                        }
                    };
                    let views = match catalog.list_views(&db).await {
                        Ok(views) => views,
                        Err(paimon::Error::Unsupported { .. }) => vec![],
                        Err(error) => {
                            log::error!("failed to list views in '{}': {error}", db);
                            vec![]
                        }
                    };
                    (names, views)
                }
            },
            "paimon catalog access thread panicked",
        );

        let mut catalog_table_types = HashMap::with_capacity(names.len() + views.len());
        for name in &names {
            catalog_table_types.insert(name.clone(), TableType::Base);
        }
        for view in views {
            catalog_table_types
                .entry(view.clone())
                .or_insert(TableType::View);
            names.push(view);
        }
        *self
            .catalog_table_types
            .write()
            .unwrap_or_else(|e| e.into_inner()) = catalog_table_types;

        if let Some(temp) = &self.temp_provider {
            names.extend(temp.table_names());
        }

        let mut seen = std::collections::HashSet::new();
        names.retain(|name| seen.insert(name.clone()));

        names
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if let Some(temp) = &self.temp_provider {
            if let Some(table) = temp.table(name).await? {
                return Ok(Some(table));
            }
        }

        let object = system_tables::parse_object_name_for_datafusion(name)?;
        if let Some(system_name) = object.system_table().map(str::to_string) {
            let dynamic_options = self
                .dynamic_options
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            return await_with_runtime(system_tables::load(
                Arc::clone(&self.catalog),
                self.database.clone(),
                object,
                system_name,
                dynamic_options,
            ))
            .await;
        }

        let catalog = Arc::clone(&self.catalog);
        let dynamic_options = Arc::clone(&self.dynamic_options);
        let blob_reader_registry = self.blob_reader_registry.clone();
        let catalog_name = self.catalog_name.clone();
        let session_state = self.session_state.clone();
        let schema_force_view_types = self.schema_force_view_types;
        let identifier = Identifier::new(self.database.clone(), object.table().to_string());
        let branch = object.branch().map(str::to_string);
        let table_engines: HashMap<PaimonTableType, Arc<dyn TableEngineResolver>> = self
            .table_engines
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        await_with_runtime(async move {
            match catalog.load_table(&identifier).await {
                Ok(paimon::catalog::LoadedTable::Object(table)) => {
                    if branch.is_some() {
                        return Err(plan_datafusion_err!(
                            "branches are not supported for 'object-table' tables ('{}')",
                            identifier.full_name()
                        ));
                    }
                    let session_options = dynamic_options
                        .read()
                        .unwrap_or_else(|e| e.into_inner())
                        .clone();
                    paimon::spec::CoreOptions::new(&session_options)
                        .ensure_engine_can_serve(&identifier.full_name())
                        .map_err(to_datafusion_error)?;
                    Ok(Some(Arc::new(ObjectTableProvider::try_new(
                        table,
                        schema_force_view_types,
                    )?) as Arc<dyn TableProvider>))
                }
                Ok(paimon::catalog::LoadedTable::External(external)) => {
                    let declared = external.declared();
                    if branch.is_some() {
                        return Err(plan_datafusion_err!(
                            "branches are not supported for '{}' tables ('{}')",
                            declared,
                            identifier.full_name()
                        ));
                    }
                    let Some(resolver) = table_engines.get(&declared) else {
                        let schema = match external.fields() {
                            Some(fields) => crate::table::datafusion_arrow_schema(
                                fields,
                                schema_force_view_types,
                            )?,
                            None => Arc::new(datafusion::arrow::datatypes::Schema::empty()),
                        };
                        return Ok(Some(Arc::new(UnavailableEngineTableProvider {
                            schema,
                            declared,
                            table_name: identifier.full_name(),
                        }) as Arc<dyn TableProvider>));
                    };
                    // The Paimon arm below applies these; an engine would
                    // ignore them and answer from current data. A missing
                    // engine only exposes catalog metadata, so read-specific
                    // session options do not apply to that fallback.
                    let session_options = dynamic_options
                        .read()
                        .unwrap_or_else(|e| e.into_inner())
                        .clone();
                    paimon::spec::CoreOptions::new(&session_options)
                        .ensure_engine_can_serve(&identifier.full_name())
                        .map_err(to_datafusion_error)?;
                    let resolved = resolver
                        .resolve_table(&EngineTableRequest::new(
                            identifier.database().to_string(),
                            identifier.object().to_string(),
                            declared,
                        ))
                        .await?;
                    Ok(resolved.map(|inner| {
                        Arc::new(ReadOnlyTableProvider {
                            inner,
                            declared,
                            table_name: identifier.full_name(),
                        }) as Arc<dyn TableProvider>
                    }))
                }
                Ok(paimon::catalog::LoadedTable::Paimon(table)) => {
                    let mut table = *table;
                    if let Some(branch) = branch.as_deref() {
                        table = table
                            .copy_with_branch(branch)
                            .await
                            .map_err(to_datafusion_error)?;
                    }
                    let opts = dynamic_options.read().unwrap().clone();
                    let provider = if opts.is_empty() {
                        PaimonTableProvider::try_new_with_blob_reader_registry(
                            table,
                            blob_reader_registry,
                        )?
                    } else {
                        let table_definition = crate::table::build_table_definition(&table).ok();
                        // Dynamic options may select a historical snapshot
                        // (e.g. `SET 'paimon.scan.version'`); switch to its
                        // schema so planning sees the snapshot's columns.
                        let table = table
                            .copy_with_time_travel(opts)
                            .await
                            .map_err(to_datafusion_error)?;
                        PaimonTableProvider::try_new_with_blob_reader_registry_and_definition(
                            table,
                            blob_reader_registry,
                            table_definition,
                        )?
                    }
                    .with_schema_force_view_types(schema_force_view_types)?;
                    Ok(Some(Arc::new(provider) as Arc<dyn TableProvider>))
                }
                Err(paimon::Error::TableNotExist { .. }) => {
                    if branch.is_some() {
                        return Ok(None);
                    }
                    // DataFusion preloads every relation name before planning, including
                    // registered table functions. Do not reinterpret a missing UDTF name as
                    // a REST view; the planner will resolve it through the UDTF registry.
                    if session_state
                        .as_ref()
                        .and_then(|provider| provider())
                        .is_some_and(|state| state.table_functions().contains_key(identifier.object()))
                    {
                        return Ok(None);
                    }
                    let view = match catalog.get_view(&identifier).await {
                        Ok(view) => view,
                        Err(paimon::Error::ViewNotExist { .. })
                        | Err(paimon::Error::Unsupported { .. }) => return Ok(None),
                        Err(error) => return Err(to_datafusion_error(error)),
                    };
                    let catalog_name = catalog_name.ok_or_else(|| {
                        plan_datafusion_err!(
                            "REST catalog view '{}' requires a session-aware catalog provider",
                            identifier.full_name()
                        )
                    })?;
                    validate_view_dependencies(&catalog, &catalog_name, &view)
                        .await?;
                    let mut state = session_state
                        .and_then(|provider| provider())
                        .ok_or_else(|| {
                            plan_datafusion_err!(
                                "DataFusion session is unavailable while planning REST catalog view '{}'",
                                identifier.full_name()
                            )
                        })?;
                    state.config_mut().options_mut().catalog.default_catalog =
                        catalog_name.clone();
                    state.config_mut().options_mut().catalog.default_schema =
                        identifier.database().to_string();
                    let catalogs =
                        HashMap::from([(catalog_name.clone(), Arc::clone(&catalog))]);
                    let query = crate::sql_function::expand_sql(
                        view.query_for("datafusion"),
                        &catalogs,
                        &catalog_name,
                        identifier.database(),
                    )
                    .await?;
                    let plan = state.create_logical_plan(&query).await?;
                    let plan = enforce_view_schema(plan, &view)?;
                    Ok(Some(Arc::new(datafusion::datasource::ViewTable::new(
                        plan,
                        Some(query),
                    )) as Arc<dyn TableProvider>))
                }
                Err(e) => Err(to_datafusion_error(e)),
                Ok(_) => Err(plan_datafusion_err!(
                    "catalog returned an unsupported loaded table kind for '{}'",
                    identifier.full_name()
                )),
            }
        })
        .await
    }

    async fn table_type(&self, name: &str) -> DFResult<Option<TableType>> {
        if let Some(temp) = &self.temp_provider {
            if let Some(table_type) = temp.table_type(name).await? {
                return Ok(Some(table_type));
            }
        }

        if let Some(table_type) = self
            .catalog_table_types
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
        {
            return Ok(Some(*table_type));
        }

        self.table(name)
            .await
            .map(|table| table.map(|table| table.table_type()))
    }

    fn table_exist(&self, name: &str) -> bool {
        if let Some(temp) = &self.temp_provider {
            if temp.table_exist(name) {
                return true;
            }
        }

        let object = match system_tables::parse_object_name_for_datafusion(name) {
            Ok(object) => object,
            Err(e) => {
                log::error!("failed to parse Paimon object name '{name}': {e}");
                return false;
            }
        };
        if let Some(system_name) = object.system_table() {
            if !system_tables::is_registered(system_name) {
                return false;
            }
        }

        let catalog = Arc::clone(&self.catalog);
        let identifier = Identifier::new(self.database.clone(), object.table().to_string());
        let branch = object.branch().map(str::to_string);
        let is_branches_table = object
            .system_table()
            .is_some_and(|name| name.eq_ignore_ascii_case("branches"));
        let has_system_suffix = object.system_table().is_some();
        let engines: HashMap<PaimonTableType, Arc<dyn TableEngineResolver>> = self
            .table_engines
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        block_on_with_runtime(
            async move {
                match catalog.load_table(&identifier).await {
                    Ok(paimon::catalog::LoadedTable::Object(_)) => {
                        branch.is_none() && !has_system_suffix
                    }
                    Ok(paimon::catalog::LoadedTable::External(external)) => {
                        let declared = external.declared();
                        // Paimon-only; `table()` rejects them here too.
                        if branch.is_some() || has_system_suffix {
                            return false;
                        }
                        match engines.get(&declared) {
                            Some(resolver) => match resolver
                                .resolve_table(&EngineTableRequest::new(
                                    identifier.database().to_string(),
                                    identifier.object().to_string(),
                                    declared,
                                ))
                                .await
                            {
                                Ok(table) => table.is_some(),
                                // Report failures as existing so `table()`
                                // surfaces the real error.
                                Err(err) => {
                                    log::warn!(
                                        "failed to probe engine table existence for '{}': {err}",
                                        identifier.full_name()
                                    );
                                    true
                                }
                            },
                            // `table()` returns a metadata-only provider in this
                            // case, so the SchemaProvider existence contract
                            // requires the same answer here.
                            None => true,
                        }
                    }
                    Ok(paimon::catalog::LoadedTable::Paimon(table)) => {
                        if let Some(branch) = branch.as_deref() {
                            if is_branches_table {
                                return true;
                            }
                            (*table).copy_with_branch(branch).await.is_ok()
                        } else {
                            true
                        }
                    }
                    Err(paimon::Error::TableNotExist { .. }) => {
                        if branch.is_some() {
                            return false;
                        }
                        match catalog.get_view(&identifier).await {
                            Ok(_) => true,
                            Err(paimon::Error::ViewNotExist { .. })
                            | Err(paimon::Error::Unsupported { .. }) => false,
                            Err(error) => {
                                log::error!("failed to check view '{}': {error}", identifier);
                                false
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("failed to check table '{}': {e}", identifier);
                        false
                    }
                    Ok(_) => {
                        log::error!(
                            "catalog returned an unsupported loaded table kind for '{}'",
                            identifier
                        );
                        false
                    }
                }
            },
            "paimon catalog access thread panicked",
        )
    }

    fn register_table(
        &self,
        _name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // DataFusion calls register_table after table creation, so we just
        // acknowledge it here.
        Ok(Some(table))
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let catalog = Arc::clone(&self.catalog);
        let identifier = Identifier::new(self.database.clone(), name);
        block_on_with_runtime(
            async move {
                // Try to get the table first so we can return it.
                let table = match catalog.get_table(&identifier).await {
                    Ok(t) => t,
                    Err(paimon::Error::TableNotExist { .. }) => return Ok(None),
                    Err(e) => return Err(to_datafusion_error(e)),
                };
                let provider = PaimonTableProvider::try_new(table)?;
                catalog
                    .drop_table(&identifier, false)
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(Some(Arc::new(provider) as Arc<dyn TableProvider>))
            },
            "paimon catalog access thread panicked",
        )
    }
}

fn enforce_view_schema(plan: LogicalPlan, view: &View) -> DFResult<LogicalPlan> {
    let declared_fields = view.schema().fields();
    let actual_fields = plan.schema().fields();
    if actual_fields.len() != declared_fields.len() {
        return Err(plan_datafusion_err!(
            "REST catalog view '{}' declares {} fields but its query returns {}",
            view.full_name(),
            declared_fields.len(),
            actual_fields.len()
        ));
    }

    let expressions = declared_fields
        .iter()
        .enumerate()
        .map(|(index, declared)| {
            let (qualifier, actual) = plan.schema().qualified_field(index);
            let column = match qualifier {
                Some(qualifier) => Column::new(Some(qualifier.clone()), actual.name()),
                None => Column::new_unqualified(actual.name()),
            };
            let target_type = paimon::arrow::paimon_type_to_arrow(declared.data_type())
                .map_err(to_datafusion_error)?;
            Ok(cast(Expr::Column(column), target_type).alias(declared.name()))
        })
        .collect::<DFResult<Vec<_>>>()?;

    LogicalPlanBuilder::from(plan).project(expressions)?.build()
}

const MAX_VIEW_DEPENDENCIES: usize = 64;

async fn validate_view_dependencies(
    catalog: &Arc<dyn Catalog>,
    catalog_name: &str,
    root: &View,
) -> DFResult<()> {
    let mut queue = VecDeque::from([root.clone()]);
    let mut loaded = HashSet::from([root.identifier().clone()]);
    let mut dependencies = HashMap::<Identifier, Vec<Identifier>>::new();

    while let Some(view) = queue.pop_front() {
        let candidates = view_relation_identifiers(&view, catalog_name)?;
        let mut view_dependencies = Vec::new();
        for identifier in candidates {
            // Routed engine tables count as existing dependencies.
            match catalog.load_table(&identifier).await {
                Ok(_) => continue,
                Err(paimon::Error::TableNotExist { .. })
                | Err(paimon::Error::Unsupported { .. }) => {}
                Err(error) => return Err(to_datafusion_error(error)),
            }

            let dependency = match catalog.get_view(&identifier).await {
                Ok(view) => view,
                Err(paimon::Error::ViewNotExist { .. })
                | Err(paimon::Error::Unsupported { .. }) => continue,
                Err(error) => return Err(to_datafusion_error(error)),
            };
            view_dependencies.push(identifier.clone());
            if loaded.insert(identifier) {
                if loaded.len() > MAX_VIEW_DEPENDENCIES {
                    return Err(plan_datafusion_err!(
                        "REST catalog view '{}' exceeds the dependency limit of {}",
                        root.full_name(),
                        MAX_VIEW_DEPENDENCIES
                    ));
                }
                queue.push_back(dependency);
            }
        }
        dependencies.insert(view.identifier().clone(), view_dependencies);

        if let Some(cycle) = find_view_dependency_cycle(&dependencies) {
            let path = cycle
                .iter()
                .map(Identifier::full_name)
                .collect::<Vec<_>>()
                .join(" -> ");
            return Err(plan_datafusion_err!(
                "recursive REST catalog view dependency detected: {path}"
            ));
        }
    }
    Ok(())
}

fn view_relation_identifiers(view: &View, catalog_name: &str) -> DFResult<Vec<Identifier>> {
    let statements =
        Parser::parse_sql(&GenericDialect {}, view.query_for("datafusion")).map_err(|error| {
            plan_datafusion_err!(
                "Invalid SQL for REST catalog view '{}': {error}",
                view.full_name()
            )
        })?;
    if statements.len() != 1 {
        return Err(plan_datafusion_err!(
            "REST catalog view '{}' must contain exactly one SQL statement",
            view.full_name()
        ));
    }
    if !matches!(statements.first(), Some(Statement::Query(_))) {
        return Err(plan_datafusion_err!(
            "REST catalog view '{}' must contain a single read-only query",
            view.full_name()
        ));
    }

    let mut visitor = ViewRelationVisitor::new(catalog_name, view.identifier().database());
    let _: std::ops::ControlFlow<()> = statements.visit(&mut visitor);
    Ok(visitor.identifiers)
}

type SqlIdentifierKey = String;

struct QueryCteScope {
    visible: HashSet<SqlIdentifierKey>,
    cte_query_visibility: HashMap<usize, HashSet<SqlIdentifierKey>>,
}

struct ViewRelationVisitor<'a> {
    catalog_name: &'a str,
    current_database: &'a str,
    scopes: Vec<QueryCteScope>,
    identifiers: Vec<Identifier>,
}

impl<'a> ViewRelationVisitor<'a> {
    fn new(catalog_name: &'a str, current_database: &'a str) -> Self {
        Self {
            catalog_name,
            current_database,
            scopes: Vec::new(),
            identifiers: Vec::new(),
        }
    }
}

impl Visitor for ViewRelationVisitor<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> std::ops::ControlFlow<Self::Break> {
        let query_address = query as *const Query as usize;
        let inherited = self
            .scopes
            .last()
            .map(|scope| {
                scope
                    .cte_query_visibility
                    .get(&query_address)
                    .unwrap_or(&scope.visible)
                    .clone()
            })
            .unwrap_or_default();
        let mut visible = inherited.clone();
        let mut cte_query_visibility = HashMap::new();

        if let Some(with) = &query.with {
            let local_ctes = with
                .cte_tables
                .iter()
                .map(|cte| sql_identifier_key(&cte.alias.name))
                .collect::<Vec<_>>();
            if with.recursive {
                visible.extend(local_ctes);
                for cte in &with.cte_tables {
                    cte_query_visibility
                        .insert(cte.query.as_ref() as *const Query as usize, visible.clone());
                }
            } else {
                for (cte, alias) in with.cte_tables.iter().zip(local_ctes) {
                    cte_query_visibility
                        .insert(cte.query.as_ref() as *const Query as usize, visible.clone());
                    visible.insert(alias);
                }
            }
        }

        self.scopes.push(QueryCteScope {
            visible,
            cte_query_visibility,
        });
        std::ops::ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> std::ops::ControlFlow<Self::Break> {
        self.scopes.pop();
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> std::ops::ControlFlow<Self::Break> {
        let is_cte = match relation.0.as_slice() {
            [part] => part.as_ident().is_some_and(|identifier| {
                self.scopes
                    .last()
                    .is_some_and(|scope| scope.visible.contains(&sql_identifier_key(identifier)))
            }),
            _ => false,
        };
        if !is_cte {
            if let Some(identifier) =
                relation_identifier(relation, self.catalog_name, self.current_database)
            {
                self.identifiers.push(identifier);
            }
        }
        std::ops::ControlFlow::Continue(())
    }
}

fn sql_identifier_key(identifier: &Ident) -> SqlIdentifierKey {
    IdentNormalizer::default().normalize(identifier.clone())
}

fn relation_identifier(
    relation: &ObjectName,
    catalog_name: &str,
    current_database: &str,
) -> Option<Identifier> {
    let parts = relation
        .0
        .iter()
        .map(|part| part.as_ident().map(sql_identifier_key))
        .collect::<Option<Vec<_>>>()?;
    match parts.as_slice() {
        [object] => Some(Identifier::new(current_database, object.as_str())),
        [database, object] => Some(Identifier::new(database.as_str(), object.as_str())),
        [catalog, database, object] if catalog == catalog_name => {
            Some(Identifier::new(database.as_str(), object.as_str()))
        }
        _ => None,
    }
}

fn find_view_dependency_cycle(
    dependencies: &HashMap<Identifier, Vec<Identifier>>,
) -> Option<Vec<Identifier>> {
    fn visit(
        identifier: &Identifier,
        dependencies: &HashMap<Identifier, Vec<Identifier>>,
        finished: &mut HashSet<Identifier>,
        path: &mut Vec<Identifier>,
    ) -> Option<Vec<Identifier>> {
        if let Some(start) = path.iter().position(|entry| entry == identifier) {
            let mut cycle = path[start..].to_vec();
            cycle.push(identifier.clone());
            return Some(cycle);
        }
        if finished.contains(identifier) {
            return None;
        }

        path.push(identifier.clone());
        if let Some(next_identifiers) = dependencies.get(identifier) {
            for next in next_identifiers {
                if let Some(cycle) = visit(next, dependencies, finished, path) {
                    return Some(cycle);
                }
            }
        }
        path.pop();
        finished.insert(identifier.clone());
        None
    }

    let mut finished = HashSet::new();
    for identifier in dependencies.keys() {
        if let Some(cycle) = visit(identifier, dependencies, &mut finished, &mut Vec::new()) {
            return Some(cycle);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relation_identifiers_follow_datafusion_normalization() {
        let relation = ObjectName(vec![
            datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(Ident::new("PAIMON")),
            datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(Ident::new("DEFAULT")),
            datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(Ident::new("ANSWER_VIEW")),
        ]);

        assert_eq!(
            relation_identifier(&relation, "paimon", "unused"),
            Some(Identifier::new("default", "answer_view"))
        );
    }
}
