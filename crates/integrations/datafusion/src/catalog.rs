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

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::common::{plan_datafusion_err, Column};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{expr_fn::cast, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use datafusion::sql::planner::IdentNormalizer;
use datafusion::sql::sqlparser::ast::{Ident, ObjectName, Query, Statement, Visit, Visitor};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use futures::{stream, StreamExt, TryStreamExt};
use indexmap::IndexMap;
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

#[derive(Clone, Debug, Default)]
struct CatalogMetadataSnapshot {
    databases: IndexMap<String, Arc<DatabaseMetadata>>,
}

#[derive(Clone, Debug, Default)]
struct DatabaseMetadata {
    objects: IndexMap<String, TableType>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectResolution {
    Paimon,
    Routed,
    Unavailable,
}

#[derive(Debug)]
struct CatalogMetadataState {
    snapshot: RwLock<Arc<CatalogMetadataSnapshot>>,
    next_generation: AtomicU64,
    database_generations: RwLock<HashMap<String, u64>>,
    active_refreshes: Mutex<BTreeSet<u64>>,
    object_resolutions: RwLock<HashMap<(String, String), ObjectResolution>>,
}

impl Default for CatalogMetadataState {
    fn default() -> Self {
        Self {
            snapshot: RwLock::new(Arc::new(CatalogMetadataSnapshot::default())),
            next_generation: AtomicU64::new(0),
            database_generations: RwLock::new(HashMap::new()),
            active_refreshes: Mutex::new(BTreeSet::new()),
            object_resolutions: RwLock::new(HashMap::new()),
        }
    }
}

struct RefreshGeneration<'a> {
    generation: u64,
    state: &'a CatalogMetadataState,
}

impl RefreshGeneration<'_> {
    fn get(&self) -> u64 {
        self.generation
    }
}

impl Drop for RefreshGeneration<'_> {
    fn drop(&mut self) {
        self.state.finish_refresh(self.generation);
    }
}

impl Deref for CatalogMetadataState {
    type Target = RwLock<Arc<CatalogMetadataSnapshot>>;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

impl CatalogMetadataState {
    fn next_generation(&self) -> u64 {
        self.next_generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn begin_refresh(&self) -> RefreshGeneration<'_> {
        let generation = self.next_generation();
        self.active_refreshes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(generation);
        RefreshGeneration {
            generation,
            state: self,
        }
    }

    fn finish_refresh(&self, generation: u64) {
        let mut active_refreshes = self
            .active_refreshes
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        active_refreshes.remove(&generation);
        let oldest_active = active_refreshes.first().copied();
        let current = self.snapshot.read().unwrap_or_else(|e| e.into_inner());
        let mut database_generations = self
            .database_generations
            .write()
            .unwrap_or_else(|e| e.into_inner());
        Self::prune_database_generations(
            current.as_ref(),
            &mut database_generations,
            oldest_active,
        );
    }

    fn publish(&self, generation: u64, refreshed: CatalogMetadataSnapshot) -> HashSet<String> {
        let mut current = self.snapshot.write().unwrap_or_else(|e| e.into_inner());
        let mut database_generations = self
            .database_generations
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let mut next = (**current).clone();
        let mut conflicts = HashSet::new();
        let mut published_databases = HashSet::new();
        let refreshed_names: HashSet<_> = refreshed.databases.keys().cloned().collect();

        let removed_names: HashSet<_> = next
            .databases
            .keys()
            .chain(database_generations.keys())
            .filter(|name| !refreshed_names.contains(*name))
            .cloned()
            .collect();
        for name in removed_names {
            if database_generations.get(&name).copied().unwrap_or_default() <= generation {
                next.databases.shift_remove(&name);
                database_generations.insert(name.clone(), generation);
                published_databases.insert(name);
            } else {
                conflicts.insert(name);
            }
        }
        for (name, metadata) in refreshed.databases {
            if database_generations.get(&name).copied().unwrap_or_default() <= generation {
                next.databases.insert(name.clone(), metadata);
                database_generations.insert(name.clone(), generation);
                published_databases.insert(name);
            } else {
                conflicts.insert(name);
            }
        }
        *current = Arc::new(next);
        self.retain_object_resolutions(current.as_ref());
        self.clear_unavailable_resolutions(&published_databases);
        conflicts
    }

    fn publish_database(
        &self,
        generation: u64,
        database: String,
        metadata: Arc<DatabaseMetadata>,
    ) -> bool {
        let mut current = self.snapshot.write().unwrap_or_else(|e| e.into_inner());
        let mut database_generations = self
            .database_generations
            .write()
            .unwrap_or_else(|e| e.into_inner());
        if database_generations
            .get(&database)
            .copied()
            .unwrap_or_default()
            > generation
        {
            return false;
        }
        let mut next = (**current).clone();
        next.databases.insert(database.clone(), metadata);
        *current = Arc::new(next);
        self.retain_object_resolutions(current.as_ref());
        self.clear_unavailable_resolution_for_database(&database);
        database_generations.insert(database, generation);
        true
    }

    fn mutate_database(&self, database: &str, update: impl FnOnce(&mut CatalogMetadataSnapshot)) {
        let generation = self.next_generation();
        let active_refreshes = self
            .active_refreshes
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let oldest_active = active_refreshes.first().copied();
        let mut current = self.snapshot.write().unwrap_or_else(|e| e.into_inner());
        let mut database_generations = self
            .database_generations
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let mut next = (**current).clone();
        update(&mut next);
        database_generations.insert(database.to_string(), generation);
        Self::prune_database_generations(&next, &mut database_generations, oldest_active);
        *current = Arc::new(next);
    }

    fn set_object_resolution(&self, database: &str, object: &str, resolution: ObjectResolution) {
        self.object_resolutions
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert((database.to_string(), object.to_string()), resolution);
    }

    fn object_resolution(&self, database: &str, object: &str) -> Option<ObjectResolution> {
        self.object_resolutions
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(database.to_string(), object.to_string()))
            .copied()
    }

    fn remove_object_resolution(&self, database: &str, object: &str) {
        self.object_resolutions
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(database.to_string(), object.to_string()));
    }

    fn remove_database_resolutions(&self, database: &str) {
        self.object_resolutions
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(resolved_database, _), _| resolved_database != database);
    }

    fn rename_object_resolution(&self, database: &str, from: &str, to: &str) {
        let mut resolutions = self
            .object_resolutions
            .write()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(resolution) = resolutions.remove(&(database.to_string(), from.to_string())) {
            resolutions.insert((database.to_string(), to.to_string()), resolution);
        }
    }

    fn retain_object_resolutions(&self, snapshot: &CatalogMetadataSnapshot) {
        self.object_resolutions
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(database, object), _| {
                snapshot
                    .databases
                    .get(database)
                    .is_some_and(|metadata| metadata.objects.contains_key(object))
            });
    }

    fn clear_unavailable_resolutions(&self, databases: &HashSet<String>) {
        self.object_resolutions
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(database, _), resolution| {
                *resolution != ObjectResolution::Unavailable || !databases.contains(database)
            });
    }

    fn clear_unavailable_resolution_for_database(&self, database: &str) {
        self.object_resolutions
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(resolved_database, _), resolution| {
                *resolution != ObjectResolution::Unavailable || resolved_database != database
            });
    }

    fn prune_database_generations(
        snapshot: &CatalogMetadataSnapshot,
        database_generations: &mut HashMap<String, u64>,
        oldest_active: Option<u64>,
    ) {
        database_generations.retain(|database, generation| {
            !snapshot.databases.contains_key(database)
                || oldest_active.is_some_and(|oldest| *generation >= oldest)
        });

        let mut evictable_tombstones: Vec<_> = database_generations
            .iter()
            .filter(|(database, generation)| {
                !snapshot.databases.contains_key(*database)
                    && !oldest_active.is_some_and(|oldest| **generation >= oldest)
            })
            .map(|(database, generation)| (database.clone(), *generation))
            .collect();
        if evictable_tombstones.len() > MAX_RETAINED_DATABASE_TOMBSTONES {
            evictable_tombstones.sort_unstable_by_key(|(_, generation)| *generation);
            let remove_count = evictable_tombstones.len() - MAX_RETAINED_DATABASE_TOMBSTONES;
            for (database, _) in evictable_tombstones.into_iter().take(remove_count) {
                database_generations.remove(&database);
            }
        }
    }
}

type SharedCatalogMetadata = Arc<CatalogMetadataState>;

const MAX_CONCURRENT_METADATA_LISTINGS: usize = 16;
// Recent tombstones guard against eventually consistent database listings. Tombstones still
// needed by an active older refresh are exempt from this bound until that refresh completes.
const MAX_RETAINED_DATABASE_TOMBSTONES: usize = 1024;

async fn load_database_metadata(
    catalog: &dyn Catalog,
    database: &str,
    ignore_missing_views_endpoint: bool,
) -> DFResult<DatabaseMetadata> {
    let tables = async {
        catalog
            .list_tables(database)
            .await
            .map_err(to_datafusion_error)
    };
    let views = async {
        match catalog.list_views(database).await {
            Ok(names) => Ok(names),
            Err(paimon::Error::Unsupported { .. }) => Ok(vec![]),
            Err(
                error @ paimon::Error::RestApi {
                    source: paimon::api::RestError::NoSuchResource { .. },
                },
            ) if ignore_missing_views_endpoint => {
                log::debug!(
                    "ignoring unavailable views endpoint while initializing database \
                     '{database}': {error}"
                );
                Ok(vec![])
            }
            Err(error) => Err(to_datafusion_error(error)),
        }
    };
    let (table_names, view_names) = futures::try_join!(tables, views)?;

    let mut objects = IndexMap::with_capacity(table_names.len() + view_names.len());
    for name in table_names {
        objects.entry(name).or_insert(TableType::Base);
    }
    for name in view_names {
        objects.entry(name).or_insert(TableType::View);
    }
    Ok(DatabaseMetadata { objects })
}

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
    error_message: String,
}

impl UnavailableEngineTableProvider {
    fn unavailable_error(&self) -> datafusion::error::DataFusionError {
        plan_datafusion_err!("{}", self.error_message)
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
/// Database and object listings are refreshed asynchronously and served from an
/// in-memory snapshot because DataFusion's catalog discovery callbacks are
/// synchronous. Concrete tables are still loaded lazily by the async
/// [`SchemaProvider::table`] callback.
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
    /// Remotely refreshed metadata used by DataFusion's synchronous catalog callbacks.
    metadata: SharedCatalogMetadata,
}

impl Debug for PaimonCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaimonCatalogProvider").finish()
    }
}

impl PaimonCatalogProvider {
    /// Creates a provider with an empty metadata snapshot.
    ///
    /// This preserves the original synchronous constructor signature. New callers should use
    /// [`Self::try_new`] so discovery callbacks are not exposed before metadata is initialized.
    pub fn new(
        catalog_name: Option<String>,
        catalog: Arc<dyn Catalog>,
        dynamic_options: DynamicOptions,
        blob_reader_registry: BlobReaderRegistry,
        session_state: Option<SessionStateProvider>,
    ) -> Self {
        Self::new_uninitialized(
            catalog_name,
            catalog,
            dynamic_options,
            blob_reader_registry,
            session_state,
        )
    }

    /// Creates a provider with an empty metadata snapshot.
    ///
    /// Callers must refresh it before exposing synchronous discovery callbacks.
    pub fn new_uninitialized(
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
            metadata: Arc::new(CatalogMetadataState::default()),
        }
    }

    /// Creates a provider with an initialized metadata snapshot.
    pub async fn try_new(
        catalog_name: Option<String>,
        catalog: Arc<dyn Catalog>,
        dynamic_options: DynamicOptions,
        blob_reader_registry: BlobReaderRegistry,
        session_state: Option<SessionStateProvider>,
    ) -> DFResult<Self> {
        let provider = Self::new_uninitialized(
            catalog_name,
            catalog,
            dynamic_options,
            blob_reader_registry,
            session_state,
        );
        provider.initialize_metadata().await?;
        Ok(provider)
    }

    /// Refresh the metadata consumed by DataFusion's synchronous catalog callbacks.
    ///
    /// Remote calls finish before the shared snapshot is replaced, so readers either
    /// observe the previous complete snapshot or the new complete snapshot.
    pub async fn refresh_metadata(&self) -> DFResult<()> {
        self.refresh_metadata_inner(false).await
    }

    /// Initialize metadata while tolerating a REST server without the optional views endpoint.
    pub async fn initialize_metadata(&self) -> DFResult<()> {
        self.refresh_metadata_inner(true).await
    }

    async fn refresh_metadata_inner(&self, ignore_missing_views_endpoint: bool) -> DFResult<()> {
        let generation = self.metadata.begin_refresh();
        let mut database_names = self
            .catalog
            .list_databases()
            .await
            .map_err(to_datafusion_error)?;
        let mut seen_databases = HashSet::new();
        database_names.retain(|name| seen_databases.insert(name.clone()));

        let entries: HashMap<_, _> = stream::iter(database_names.iter().cloned())
            .map(|database| async move {
                let metadata = load_database_metadata(
                    self.catalog.as_ref(),
                    database.as_str(),
                    ignore_missing_views_endpoint,
                )
                .await?;
                Ok::<_, datafusion::error::DataFusionError>((database, metadata))
            })
            .buffer_unordered(MAX_CONCURRENT_METADATA_LISTINGS)
            .try_collect()
            .await?;
        let mut entries = entries;
        let databases = database_names
            .into_iter()
            .filter_map(|database| {
                entries
                    .remove(&database)
                    .map(|metadata| (database, Arc::new(metadata)))
            })
            .collect();

        let mut conflicts = self
            .metadata
            .publish(generation.get(), CatalogMetadataSnapshot { databases });
        if !conflicts.is_empty() {
            let current_databases: HashSet<_> = self
                .catalog
                .list_databases()
                .await
                .map_err(to_datafusion_error)?
                .into_iter()
                .collect();
            conflicts.retain(|database| current_databases.contains(database));
        }
        stream::iter(conflicts)
            .map(|database| async move {
                self.refresh_database_metadata_inner(
                    database.as_str(),
                    ignore_missing_views_endpoint,
                )
                .await
            })
            .buffer_unordered(MAX_CONCURRENT_METADATA_LISTINGS)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    /// Refresh one database in the metadata snapshot.
    pub(crate) async fn refresh_database_metadata(&self, database: &str) -> DFResult<()> {
        self.refresh_database_metadata_inner(database, true).await
    }

    async fn refresh_database_metadata_inner(
        &self,
        database: &str,
        ignore_missing_views_endpoint: bool,
    ) -> DFResult<()> {
        const MAX_PUBLICATION_ATTEMPTS: usize = 3;
        for _ in 0..MAX_PUBLICATION_ATTEMPTS {
            let generation = self.metadata.begin_refresh();
            let database_metadata = load_database_metadata(
                self.catalog.as_ref(),
                database,
                ignore_missing_views_endpoint,
            )
            .await?;
            if self.metadata.publish_database(
                generation.get(),
                database.to_string(),
                Arc::new(database_metadata),
            ) {
                return Ok(());
            }
        }
        Err(DataFusionError::Execution(format!(
            "metadata for database '{database}' changed during {MAX_PUBLICATION_ATTEMPTS} refresh attempts"
        )))
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

    pub(crate) fn metadata_contains_object(&self, database: &str, name: &str) -> bool {
        if self
            .temp_tables
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(database)
            .is_some_and(|provider| provider.table_exist(name))
        {
            return true;
        }
        let object_name = system_tables::parse_object_name_for_datafusion(name)
            .map(|object| object.table().to_string())
            .unwrap_or_else(|_| name.to_string());
        self.metadata
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .databases
            .get(database)
            .is_some_and(|metadata| metadata.objects.contains_key(&object_name))
    }

    pub(crate) fn record_object_created(&self, database: &str, name: &str, table_type: TableType) {
        self.metadata.mutate_database(database, |next| {
            let database = next
                .databases
                .entry(database.to_string())
                .or_insert_with(|| Arc::new(DatabaseMetadata::default()));
            Arc::make_mut(database)
                .objects
                .insert(name.to_string(), table_type);
        });
        if table_type == TableType::Base {
            self.metadata
                .set_object_resolution(database, name, ObjectResolution::Paimon);
        } else {
            self.metadata.remove_object_resolution(database, name);
        }
    }

    pub(crate) fn record_database_created(&self, database: &str) {
        self.metadata.mutate_database(database, |next| {
            next.databases
                .entry(database.to_string())
                .or_insert_with(|| Arc::new(DatabaseMetadata::default()));
        });
    }

    pub(crate) fn record_database_dropped(&self, database: &str) {
        self.metadata.mutate_database(database, |next| {
            next.databases.shift_remove(database);
        });
        self.metadata.remove_database_resolutions(database);
    }

    pub(crate) fn record_object_dropped(&self, database: &str, name: &str) {
        self.metadata.mutate_database(database, |next| {
            if let Some(metadata) = next.databases.get_mut(database) {
                Arc::make_mut(metadata).objects.shift_remove(name);
            }
        });
        self.metadata.remove_object_resolution(database, name);
    }

    pub(crate) fn record_table_renamed(&self, database: &str, from: &str, to: &str) {
        let mut renamed = false;
        self.metadata.mutate_database(database, |next| {
            if let Some(metadata) = next.databases.get_mut(database) {
                let objects = &mut Arc::make_mut(metadata).objects;
                if let Some(table_type) = objects.shift_remove(from) {
                    objects.insert(to.to_string(), table_type);
                    renamed = true;
                }
            }
        });
        if renamed {
            self.metadata.rename_object_resolution(database, from, to);
        }
    }

    fn paimon_schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let temp_provider = {
            let databases = self.temp_tables.read().unwrap_or_else(|e| e.into_inner());
            databases.get(name).cloned()
        };
        let catalog_has_database = self
            .metadata
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .databases
            .contains_key(name);
        if !catalog_has_database && temp_provider.is_none() {
            return None;
        }

        Some(Arc::new(
            PaimonSchemaProvider::new_uninitialized(
                self.catalog_name.clone(),
                Arc::clone(&self.catalog),
                name.to_string(),
                Arc::clone(&self.dynamic_options),
                temp_provider,
                self.blob_reader_registry.clone(),
                self.session_state.clone(),
            )
            .with_schema_force_view_types(self.schema_force_view_types)
            .with_table_engines(self.table_engines())
            .with_metadata_snapshot(Arc::clone(&self.metadata)),
        ) as Arc<dyn SchemaProvider>)
    }
}

impl CatalogProvider for PaimonCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .metadata
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .databases
            .keys()
            .cloned()
            .collect();
        let mut temp_names: Vec<_> = self
            .temp_tables
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect();
        temp_names.sort_unstable();
        let mut seen: HashSet<_> = names.iter().cloned().collect();
        names.extend(
            temp_names
                .into_iter()
                .filter(|name| seen.insert(name.clone())),
        );
        names
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
        let table_engines = self.table_engines();
        let metadata = Arc::clone(&self.metadata);
        let name = name.to_string();
        block_on_with_runtime(
            async move {
                catalog
                    .create_database(&name, false, HashMap::new())
                    .await
                    .map_err(to_datafusion_error)?;
                metadata.mutate_database(&name, |next| {
                    next.databases
                        .entry(name.clone())
                        .or_insert_with(|| Arc::new(DatabaseMetadata::default()));
                });
                Ok(Some(Arc::new(
                    PaimonSchemaProvider::new_uninitialized(
                        catalog_name,
                        Arc::clone(&catalog),
                        name,
                        dynamic_options,
                        None,
                        blob_reader_registry,
                        session_state,
                    )
                    .with_schema_force_view_types(schema_force_view_types)
                    .with_table_engines(table_engines)
                    .with_metadata_snapshot(metadata),
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
        let table_engines = self.table_engines();
        let metadata = Arc::clone(&self.metadata);
        let name = name.to_string();
        block_on_with_runtime(
            async move {
                catalog
                    .drop_database(&name, false, cascade)
                    .await
                    .map_err(to_datafusion_error)?;
                metadata.mutate_database(&name, |next| {
                    next.databases.shift_remove(&name);
                });
                Ok(Some(Arc::new(
                    PaimonSchemaProvider::new_uninitialized(
                        catalog_name,
                        Arc::clone(&catalog),
                        name,
                        dynamic_options,
                        None,
                        blob_reader_registry,
                        session_state,
                    )
                    .with_schema_force_view_types(schema_force_view_types)
                    .with_table_engines(table_engines)
                    .with_metadata_snapshot(metadata),
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
        // The warning is best-effort and must not turn this synchronous API into remote I/O.
        if self
            .metadata
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .databases
            .get(database)
            .is_some_and(|metadata| metadata.objects.contains_key(table_name))
        {
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
    /// Shared metadata used by synchronous schema callbacks.
    metadata: SharedCatalogMetadata,
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
    /// Creates a schema provider with an empty metadata snapshot.
    ///
    /// This preserves the original synchronous constructor signature. New callers should use
    /// [`Self::try_new`] so discovery callbacks are not exposed before metadata is initialized.
    pub fn new(
        catalog_name: Option<String>,
        catalog: Arc<dyn Catalog>,
        database: String,
        dynamic_options: DynamicOptions,
        temp_provider: Option<Arc<MemorySchemaProvider>>,
        blob_reader_registry: BlobReaderRegistry,
        session_state: Option<SessionStateProvider>,
    ) -> Self {
        Self::new_uninitialized(
            catalog_name,
            catalog,
            database,
            dynamic_options,
            temp_provider,
            blob_reader_registry,
            session_state,
        )
    }

    /// Creates a schema provider with an empty metadata snapshot.
    fn new_uninitialized(
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
            metadata: Arc::new(CatalogMetadataState::default()),
            blob_reader_registry,
            session_state,
            schema_force_view_types: true,
            table_engines: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates a schema provider with initialized metadata.
    pub async fn try_new(
        catalog_name: Option<String>,
        catalog: Arc<dyn Catalog>,
        database: String,
        dynamic_options: DynamicOptions,
        temp_provider: Option<Arc<MemorySchemaProvider>>,
        blob_reader_registry: BlobReaderRegistry,
        session_state: Option<SessionStateProvider>,
    ) -> DFResult<Self> {
        let provider = Self::new_uninitialized(
            catalog_name,
            catalog,
            database,
            dynamic_options,
            temp_provider,
            blob_reader_registry,
            session_state,
        );
        provider.initialize_metadata().await?;
        Ok(provider)
    }

    /// Refresh this database in the snapshot used by synchronous callbacks.
    pub async fn refresh_metadata(&self) -> DFResult<()> {
        self.refresh_metadata_inner(false).await
    }

    /// Initialize metadata while tolerating a REST server without the optional views endpoint.
    pub async fn initialize_metadata(&self) -> DFResult<()> {
        self.refresh_metadata_inner(true).await
    }

    async fn refresh_metadata_inner(&self, ignore_missing_views_endpoint: bool) -> DFResult<()> {
        let generation = self.metadata.begin_refresh();
        let database = load_database_metadata(
            self.catalog.as_ref(),
            &self.database,
            ignore_missing_views_endpoint,
        )
        .await?;
        if !self.metadata.publish_database(
            generation.get(),
            self.database.clone(),
            Arc::new(database),
        ) {
            return Err(DataFusionError::Execution(format!(
                "metadata for database '{}' changed during refresh",
                self.database
            )));
        }
        Ok(())
    }

    fn with_schema_force_view_types(mut self, schema_force_view_types: bool) -> Self {
        self.schema_force_view_types = schema_force_view_types;
        self
    }

    pub(crate) fn with_table_engines(mut self, table_engines: TableEngines) -> Self {
        self.table_engines = table_engines;
        self
    }

    fn with_metadata_snapshot(mut self, metadata: SharedCatalogMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

#[async_trait]
impl SchemaProvider for PaimonSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .metadata
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .databases
            .get(&self.database)
            .map(|database| database.objects.keys().cloned().collect())
            .unwrap_or_default();

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
        let metadata = Arc::clone(&self.metadata);
        let identifier = Identifier::new(self.database.clone(), object.table().to_string());
        let branch = object.branch().map(str::to_string);
        if branch.is_none()
            && session_state
                .as_ref()
                .and_then(|provider| provider())
                .is_some_and(|state| state.table_functions().contains_key(identifier.object()))
        {
            return Ok(None);
        }
        let table_engines: HashMap<PaimonTableType, Arc<dyn TableEngineResolver>> = self
            .table_engines
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        await_with_runtime(async move {
            match catalog.load_table(&identifier).await {
                Ok(paimon::catalog::LoadedTable::Object(table)) => {
                    metadata.set_object_resolution(
                        identifier.database(),
                        identifier.object(),
                        ObjectResolution::Paimon,
                    );
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
                    metadata.set_object_resolution(
                        identifier.database(),
                        identifier.object(),
                        ObjectResolution::Routed,
                    );
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
                            error_message: format!(
                                "no table engine is registered for '{declared}' tables ('{}')",
                                identifier.full_name()
                            ),
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
                    if resolved.is_none() {
                        metadata.set_object_resolution(
                            identifier.database(),
                            identifier.object(),
                            ObjectResolution::Unavailable,
                        );
                    }
                    Ok(resolved.map(|inner| {
                        Arc::new(ReadOnlyTableProvider {
                            inner,
                            declared,
                            table_name: identifier.full_name(),
                        }) as Arc<dyn TableProvider>
                    }))
                }
                Ok(paimon::catalog::LoadedTable::Paimon(table)) => {
                    metadata.set_object_resolution(
                        identifier.database(),
                        identifier.object(),
                        ObjectResolution::Paimon,
                    );
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
        if self.metadata.object_resolution(&self.database, name)
            == Some(ObjectResolution::Unavailable)
        {
            return Ok(None);
        }

        if let Some(table_type) = self
            .metadata
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .databases
            .get(&self.database)
            .and_then(|database| database.objects.get(name))
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
        if object
            .system_table()
            .is_some_and(|system_name| !system_tables::is_registered(system_name))
        {
            return false;
        }
        let table_type = self
            .metadata
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .databases
            .get(&self.database)
            .and_then(|database| database.objects.get(object.table()))
            .copied();
        let resolution = self
            .metadata
            .object_resolution(&self.database, object.table());
        if resolution == Some(ObjectResolution::Unavailable) {
            return false;
        }
        if object.system_table().is_some() {
            return table_type == Some(TableType::Base)
                && resolution != Some(ObjectResolution::Routed);
        }
        table_type.is_some()
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
        let database = self.database.clone();
        let identifier = Identifier::new(database.clone(), name);
        let metadata = Arc::clone(&self.metadata);
        let name = name.to_string();
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
                metadata.mutate_database(&database, |next| {
                    if let Some(database) = next.databases.get_mut(&database) {
                        Arc::make_mut(database).objects.shift_remove(&name);
                    }
                });
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
    fn database_generation_tombstones_are_bounded() {
        let metadata = CatalogMetadataState::default();
        for index in 0..2048 {
            let database = format!("deleted_{index}");
            metadata.mutate_database(&database, |snapshot| {
                snapshot.databases.shift_remove(&database);
            });
        }

        assert_eq!(
            metadata
                .database_generations
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            MAX_RETAINED_DATABASE_TOMBSTONES
        );
    }

    #[test]
    fn active_refresh_protects_tombstones_from_bounded_eviction() {
        let metadata = CatalogMetadataState::default();
        let refresh = metadata.begin_refresh();
        for index in 0..(MAX_RETAINED_DATABASE_TOMBSTONES * 2) {
            let database = format!("deleted_{index}");
            metadata.mutate_database(&database, |snapshot| {
                snapshot.databases.shift_remove(&database);
            });
        }

        assert_eq!(
            metadata
                .database_generations
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            MAX_RETAINED_DATABASE_TOMBSTONES * 2
        );

        drop(refresh);
        assert_eq!(
            metadata
                .database_generations
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            MAX_RETAINED_DATABASE_TOMBSTONES
        );
    }

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
