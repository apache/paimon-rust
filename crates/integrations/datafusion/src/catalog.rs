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
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use paimon::catalog::{Catalog, Identifier};

use crate::error::to_datafusion_error;
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::table::PaimonTableProvider;

/// Provides an interface to manage and access multiple schemas (databases)
/// within a Paimon [`Catalog`].
///
/// This provider uses lazy loading - databases and tables are fetched
/// on-demand from the catalog, ensuring data is always fresh.
pub struct PaimonCatalogProvider {
    /// Reference to the Paimon catalog.
    catalog: Arc<dyn Catalog>,
}

impl Debug for PaimonCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaimonCatalogProvider").finish()
    }
}

impl PaimonCatalogProvider {
    /// Creates a new [`PaimonCatalogProvider`].
    ///
    /// All data is loaded lazily when accessed.
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        PaimonCatalogProvider { catalog }
    }
}

impl CatalogProvider for PaimonCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let catalog = Arc::clone(&self.catalog);
        block_on_with_runtime(
            async move { catalog.list_databases().await.unwrap_or_default() },
            "paimon catalog access thread panicked",
        )
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let catalog = Arc::clone(&self.catalog);
        let name = name.to_string();
        block_on_with_runtime(
            async move {
                match catalog.get_database(&name).await {
                    Ok(_) => Some(
                        Arc::new(PaimonSchemaProvider::new(Arc::clone(&catalog), name))
                            as Arc<dyn SchemaProvider>,
                    ),
                    Err(paimon::Error::DatabaseNotExist { .. }) => None,
                    Err(_) => None,
                }
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
}

impl Debug for PaimonSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaimonSchemaProvider")
            .field("database", &self.database)
            .finish()
    }
}

impl PaimonSchemaProvider {
    /// Creates a new [`PaimonSchemaProvider`] for the given database.
    pub fn new(catalog: Arc<dyn Catalog>, database: String) -> Self {
        PaimonSchemaProvider { catalog, database }
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
            async move { catalog.list_tables(&database).await.unwrap_or_default() },
            "paimon catalog access thread panicked",
        )
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let catalog = Arc::clone(&self.catalog);
        let identifier = Identifier::new(self.database.clone(), name);
        await_with_runtime(async move {
            match catalog.get_table(&identifier).await {
                Ok(table) => {
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
        let catalog = Arc::clone(&self.catalog);
        let identifier = Identifier::new(self.database.clone(), name);
        block_on_with_runtime(
            async move {
                match catalog.get_table(&identifier).await {
                    Ok(_) => true,
                    Err(paimon::Error::TableNotExist { .. }) => false,
                    Err(_) => false,
                }
            },
            "paimon catalog access thread panicked",
        )
    }
}
