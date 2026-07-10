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
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{expr_fn::cast, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::sqlparser::ast::{Ident, ObjectName, Query, Statement, Visit, Visitor};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use paimon::catalog::{Catalog, Identifier, View};

use crate::error::to_datafusion_error;
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::system_tables;
use crate::table::PaimonTableProvider;
use crate::{BlobReaderRegistry, DynamicOptions};

pub(crate) type SessionStateProvider = Arc<dyn Fn() -> Option<SessionState> + Send + Sync>;

/// Provides an interface to manage and access multiple schemas (databases)
/// within a Paimon [`Catalog`].
///
/// This provider uses lazy loading - databases and tables are fetched
/// on-demand from the catalog, ensuring data is always fresh.
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
        }
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
        let catalog = Arc::clone(&self.catalog);
        let dynamic_options = Arc::clone(&self.dynamic_options);
        let blob_reader_registry = self.blob_reader_registry.clone();
        let catalog_name = self.catalog_name.clone();
        let session_state = self.session_state.clone();
        let name = name.to_string();

        let temp_provider = {
            let databases = self.temp_tables.read().unwrap_or_else(|e| e.into_inner());
            databases.get(&name).cloned()
        };

        block_on_with_runtime(
            async move {
                match catalog.get_database(&name).await {
                    Ok(_) => Some(Arc::new(PaimonSchemaProvider::new(
                        catalog_name,
                        Arc::clone(&catalog),
                        name,
                        dynamic_options,
                        temp_provider,
                        blob_reader_registry,
                        session_state,
                    )) as Arc<dyn SchemaProvider>),
                    Err(paimon::Error::DatabaseNotExist { .. }) => {
                        if temp_provider.is_some() {
                            Some(Arc::new(PaimonSchemaProvider::new(
                                catalog_name,
                                Arc::clone(&catalog),
                                name,
                                dynamic_options,
                                temp_provider,
                                blob_reader_registry,
                                session_state,
                            )) as Arc<dyn SchemaProvider>)
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
        let name = name.to_string();
        block_on_with_runtime(
            async move {
                catalog
                    .create_database(&name, false, HashMap::new())
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(Some(Arc::new(PaimonSchemaProvider::new(
                    catalog_name,
                    Arc::clone(&catalog),
                    name,
                    dynamic_options,
                    None,
                    blob_reader_registry,
                    session_state,
                )) as Arc<dyn SchemaProvider>))
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
        let name = name.to_string();
        block_on_with_runtime(
            async move {
                catalog
                    .drop_database(&name, false, cascade)
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(Some(Arc::new(PaimonSchemaProvider::new(
                    catalog_name,
                    Arc::clone(&catalog),
                    name,
                    dynamic_options,
                    None,
                    blob_reader_registry,
                    session_state,
                )) as Arc<dyn SchemaProvider>))
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
    blob_reader_registry: BlobReaderRegistry,
    session_state: Option<SessionStateProvider>,
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
            blob_reader_registry,
            session_state,
        }
    }
}

#[async_trait]
impl SchemaProvider for PaimonSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        let catalog = Arc::clone(&self.catalog);
        let database = self.database.clone();
        let mut names = block_on_with_runtime(
            {
                let db = database.clone();
                async move {
                    let mut names = match catalog.list_tables(&db).await {
                        Ok(names) => names,
                        Err(e) => {
                            log::error!("failed to list tables in '{}': {e}", db);
                            vec![]
                        }
                    };
                    match catalog.list_views(&db).await {
                        Ok(views) => names.extend(views),
                        Err(paimon::Error::Unsupported { .. }) => {}
                        Err(error) => {
                            log::error!("failed to list views in '{}': {error}", db);
                        }
                    }
                    names
                }
            },
            "paimon catalog access thread panicked",
        );

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
            return await_with_runtime(system_tables::load(
                Arc::clone(&self.catalog),
                self.database.clone(),
                object,
                system_name,
            ))
            .await;
        }

        let catalog = Arc::clone(&self.catalog);
        let dynamic_options = Arc::clone(&self.dynamic_options);
        let blob_reader_registry = self.blob_reader_registry.clone();
        let catalog_name = self.catalog_name.clone();
        let session_state = self.session_state.clone();
        let identifier = Identifier::new(self.database.clone(), object.table().to_string());
        let branch = object.branch().map(str::to_string);
        await_with_runtime(async move {
            match catalog.get_table(&identifier).await {
                Ok(mut table) => {
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
                    };
                    Ok(Some(Arc::new(provider) as Arc<dyn TableProvider>))
                }
                Err(paimon::Error::TableNotExist { .. }) => {
                    if branch.is_some() {
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
                    validate_view_dependencies(&catalog, &catalog_name, &view).await?;
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
            }
        })
        .await
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
        block_on_with_runtime(
            async move {
                match catalog.get_table(&identifier).await {
                    Ok(table) => {
                        if let Some(branch) = branch.as_deref() {
                            if is_branches_table {
                                return true;
                            }
                            table.copy_with_branch(branch).await.is_ok()
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
            match catalog.get_table(&identifier).await {
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
