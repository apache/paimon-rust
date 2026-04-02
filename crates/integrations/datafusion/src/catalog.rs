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
        futures::executor::block_on(async {
            self.catalog.list_databases().await.unwrap_or_default()
        })
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        futures::executor::block_on(async {
            match self.catalog.get_database(name).await {
                Ok(_) => Some(Arc::new(PaimonSchemaProvider::new(
                    self.catalog.clone(),
                    name.to_string(),
                )) as Arc<dyn SchemaProvider>),
                Err(paimon::Error::DatabaseNotExist { .. }) => None,
                Err(_) => None,
            }
        })
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
        // Use blocking call to fetch table names synchronously.
        // This is acceptable as table name listing is lightweight.
        futures::executor::block_on(async {
            self.catalog
                .list_tables(&self.database)
                .await
                .unwrap_or_default()
        })
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let identifier = Identifier::new(self.database.clone(), name);
        match self.catalog.get_table(&identifier).await {
            Ok(table) => {
                let provider = PaimonTableProvider::try_new(table)?;
                Ok(Some(Arc::new(provider) as Arc<dyn TableProvider>))
            }
            Err(paimon::Error::TableNotExist { .. }) => Ok(None),
            Err(e) => Err(to_datafusion_error(e)),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        let identifier = Identifier::new(self.database.clone(), name);
        futures::executor::block_on(async {
            match self.catalog.get_table(&identifier).await {
                Ok(_) => true,
                Err(paimon::Error::TableNotExist { .. }) => false,
                Err(_) => false,
            }
        })
    }
}
