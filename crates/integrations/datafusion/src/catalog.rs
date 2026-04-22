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

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use paimon::catalog::{Catalog, Identifier};

use crate::error::to_datafusion_error;
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::system_tables;
use crate::table::PaimonTableProvider;
use crate::DynamicOptions;

/// Provides an interface to manage and access multiple schemas (databases)
/// within a Paimon [`Catalog`].
///
/// This provider uses lazy loading - databases and tables are fetched
/// on-demand from the catalog, ensuring data is always fresh.
pub struct PaimonCatalogProvider {
    /// Reference to the Paimon catalog.
    catalog: Arc<dyn Catalog>,
    /// Session-scoped dynamic options shared with the SQL handler.
    dynamic_options: DynamicOptions,
}

impl Debug for PaimonCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaimonCatalogProvider").finish()
    }
}

impl PaimonCatalogProvider {
    /// Creates a new [`PaimonCatalogProvider`] with shared dynamic options.
    ///
    /// All data is loaded lazily when accessed.
    pub fn new(catalog: Arc<dyn Catalog>, dynamic_options: DynamicOptions) -> Self {
        PaimonCatalogProvider {
            catalog,
            dynamic_options,
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
                match catalog.list_databases().await {
                    Ok(names) => names,
                    Err(e) => {
                        log::error!("failed to list databases: {e}");
                        vec![]
                    }
                }
            },
            "paimon catalog access thread panicked",
        )
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let catalog = Arc::clone(&self.catalog);
        let dynamic_options = Arc::clone(&self.dynamic_options);
        let name = name.to_string();
        block_on_with_runtime(
            async move {
                match catalog.get_database(&name).await {
                    Ok(_) => Some(Arc::new(PaimonSchemaProvider::new(
                        Arc::clone(&catalog),
                        name,
                        dynamic_options,
                    )) as Arc<dyn SchemaProvider>),
                    Err(paimon::Error::DatabaseNotExist { .. }) => None,
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
                )) as Arc<dyn SchemaProvider>))
            },
            "paimon catalog access thread panicked",
        )
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
    /// Session-scoped dynamic options shared with the SQL handler.
    dynamic_options: DynamicOptions,
}

impl Debug for PaimonSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaimonSchemaProvider")
            .field("database", &self.database)
            .finish()
    }
}

impl PaimonSchemaProvider {
    /// Creates a new [`PaimonSchemaProvider`] with shared dynamic options.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        database: String,
        dynamic_options: DynamicOptions,
    ) -> Self {
        PaimonSchemaProvider {
            catalog,
            database,
            dynamic_options,
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
        block_on_with_runtime(
            async move {
                match catalog.list_tables(&database).await {
                    Ok(names) => names,
                    Err(e) => {
                        log::error!("failed to list tables in '{}': {e}", database);
                        vec![]
                    }
                }
            },
            "paimon catalog access thread panicked",
        )
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
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
        let identifier = Identifier::new(self.database.clone(), base);
        await_with_runtime(async move {
            match catalog.get_table(&identifier).await {
                Ok(table) => {
                    let opts = dynamic_options.read().unwrap().clone();
                    let table = if opts.is_empty() {
                        table
                    } else {
                        table.copy_with_options(opts)
                    };
                    let provider = PaimonTableProvider::try_new(table)?;
                    Ok(Some(Arc::new(provider) as Arc<dyn TableProvider>))
                }
                Err(paimon::Error::TableNotExist { .. }) => Ok(None),
                Err(e) => Err(to_datafusion_error(e)),
            }
        })
        .await
    }

    fn table_exist(&self, name: &str) -> bool {
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
