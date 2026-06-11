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

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::common::plan_datafusion_err;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use paimon::catalog::{Catalog, Identifier};

use crate::error::to_datafusion_error;
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::system_tables;
use crate::table::PaimonTableProvider;
use crate::{BlobReaderRegistry, DynamicOptions};

/// Provides an interface to manage and access multiple schemas (databases)
/// within a Paimon [`Catalog`].
///
/// This provider uses lazy loading - databases and tables are fetched
/// on-demand from the catalog, ensuring data is always fresh.
pub struct PaimonCatalogProvider {
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
}

impl Debug for PaimonCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaimonCatalogProvider").finish()
    }
}

impl PaimonCatalogProvider {
    /// Creates a new [`PaimonCatalogProvider`].
    ///
    /// For standalone use without `SET`/`RESET` support.
    /// When used via [`SQLContext`], the handler creates the provider
    /// internally with shared dynamic options.
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        PaimonCatalogProvider {
            catalog,
            dynamic_options: Default::default(),
            temp_tables: Arc::new(RwLock::new(HashMap::new())),
            blob_reader_registry: BlobReaderRegistry::default(),
        }
    }

    pub(crate) fn with_dynamic_options(
        catalog: Arc<dyn Catalog>,
        dynamic_options: DynamicOptions,
        blob_reader_registry: BlobReaderRegistry,
    ) -> Self {
        PaimonCatalogProvider {
            catalog,
            dynamic_options,
            temp_tables: Arc::new(RwLock::new(HashMap::new())),
            blob_reader_registry,
        }
    }
}

impl CatalogProvider for PaimonCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        let name = name.to_string();

        let temp_provider = {
            let databases = self.temp_tables.read().unwrap_or_else(|e| e.into_inner());
            databases.get(&name).cloned()
        };

        block_on_with_runtime(
            async move {
                match catalog.get_database(&name).await {
                    Ok(_) => Some(Arc::new(PaimonSchemaProvider::new(
                        Arc::clone(&catalog),
                        name,
                        dynamic_options,
                        temp_provider,
                        blob_reader_registry,
                    )) as Arc<dyn SchemaProvider>),
                    Err(paimon::Error::DatabaseNotExist { .. }) => {
                        if temp_provider.is_some() {
                            Some(Arc::new(PaimonSchemaProvider::new(
                                Arc::clone(&catalog),
                                name,
                                dynamic_options,
                                temp_provider,
                                blob_reader_registry,
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
        let name = name.to_string();
        block_on_with_runtime(
            async move {
                catalog
                    .create_database(&name, false, HashMap::new())
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(Some(Arc::new(PaimonSchemaProvider::new(
                    Arc::clone(&catalog),
                    name,
                    dynamic_options,
                    None,
                    blob_reader_registry,
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
        let name = name.to_string();
        block_on_with_runtime(
            async move {
                catalog
                    .drop_database(&name, false, cascade)
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(Some(Arc::new(PaimonSchemaProvider::new(
                    Arc::clone(&catalog),
                    name,
                    dynamic_options,
                    None,
                    blob_reader_registry,
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
    /// Reference to the Paimon catalog.
    catalog: Arc<dyn Catalog>,
    /// Database name this schema represents.
    database: String,
    /// Session-scoped dynamic options shared with the SQL context.
    dynamic_options: DynamicOptions,
    /// Optional temporary in-memory provider for temp tables and views.
    temp_provider: Option<Arc<MemorySchemaProvider>>,
    blob_reader_registry: BlobReaderRegistry,
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
    /// Creates a new [`PaimonSchemaProvider`] with shared dynamic options.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        database: String,
        dynamic_options: DynamicOptions,
        temp_provider: Option<Arc<MemorySchemaProvider>>,
        blob_reader_registry: BlobReaderRegistry,
    ) -> Self {
        PaimonSchemaProvider {
            catalog,
            database,
            dynamic_options,
            temp_provider,
            blob_reader_registry,
        }
    }
}

#[async_trait]
impl SchemaProvider for PaimonSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let catalog = Arc::clone(&self.catalog);
        let database = self.database.clone();
        let mut names = block_on_with_runtime(
            {
                let db = database.clone();
                async move {
                    match catalog.list_tables(&db).await {
                        Ok(names) => names,
                        Err(e) => {
                            log::error!("failed to list tables in '{}': {e}", db);
                            vec![]
                        }
                    }
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

        let (base, system_name) = system_tables::split_object_name(name);
        if let Some(system_name) = system_name {
            return await_with_runtime(system_tables::load(
                Arc::clone(&self.catalog),
                self.database.clone(),
                base.to_string(),
                system_name.to_string(),
            ))
            .await;
        }

        let catalog = Arc::clone(&self.catalog);
        let dynamic_options = Arc::clone(&self.dynamic_options);
        let blob_reader_registry = self.blob_reader_registry.clone();
        let identifier = Identifier::new(self.database.clone(), base);
        await_with_runtime(async move {
            match catalog.get_table(&identifier).await {
                Ok(table) => {
                    let opts = dynamic_options.read().unwrap().clone();
                    let table = if opts.is_empty() {
                        table
                    } else {
                        // Dynamic options may select a historical snapshot
                        // (e.g. `SET 'paimon.scan.version'`); switch to its
                        // schema so planning sees the snapshot's columns.
                        table
                            .copy_with_time_travel(opts)
                            .await
                            .map_err(to_datafusion_error)?
                    };
                    let provider = PaimonTableProvider::try_new_with_blob_reader_registry(
                        table,
                        blob_reader_registry,
                    )?;
                    Ok(Some(Arc::new(provider) as Arc<dyn TableProvider>))
                }
                Err(paimon::Error::TableNotExist { .. }) => Ok(None),
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

        let (base, system_name) = system_tables::split_object_name(name);
        if let Some(system_name) = system_name {
            if !system_tables::is_registered(system_name) {
                return false;
            }
        }

        let catalog = Arc::clone(&self.catalog);
        let identifier = Identifier::new(self.database.clone(), base.to_string());
        block_on_with_runtime(
            async move {
                match catalog.get_table(&identifier).await {
                    Ok(_) => true,
                    Err(paimon::Error::TableNotExist { .. }) => false,
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
