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

//! Catalog factory for creating catalogs based on configuration options.
//!
//! This module provides a factory pattern for creating different catalog types
//! (filesystem, REST, etc.) based on the `metastore` option.
//!
//! # Example
//!
//! ```ignore
//! use std::collections::HashMap;
//! use paimon::{CatalogFactory, Options};
//!
//! // Create a filesystem catalog
//! let mut options = Options::new();
//! options.set("warehouse", "/path/to/warehouse");
//! let catalog = CatalogFactory::create(options).await?;
//!
//! // Create a REST catalog
//! let mut options = Options::new();
//! options.set("metastore", "rest");
//! options.set("uri", "http://localhost:8080");
//! options.set("warehouse", "my_warehouse");
//! let catalog = CatalogFactory::create(options).await?;
//! ```

use std::sync::Arc;

use crate::catalog::{Catalog, FileSystemCatalog, RESTCatalog};
use crate::common::{CatalogOptions, Options};
use crate::error::{ConfigInvalidSnafu, Result};

/// Supported catalog types.
const METASTORE_FILESYSTEM: &str = "filesystem";
const METASTORE_REST: &str = "rest";

/// Factory for creating Paimon catalogs.
///
/// The factory determines the catalog type based on the `metastore` option:
/// - `"filesystem"` (default): Creates a [`FileSystemCatalog`]
/// - `"rest"`: Creates a [`RESTCatalog`]
///
/// # Example
///
/// ```ignore
/// use paimon::{CatalogFactory, Options};
///
/// let mut options = Options::new();
/// options.set("warehouse", "/path/to/warehouse");
/// let catalog = CatalogFactory::create(options).await?;
/// ```
pub struct CatalogFactory;

impl CatalogFactory {
    /// Create a catalog based on the provided options.
    ///
    /// The catalog type is determined by the `metastore` option:
    /// - `"filesystem"` (default): Creates a filesystem-based catalog
    /// - `"rest"`: Creates a REST-based catalog
    ///
    /// # Arguments
    /// * `options` - Configuration options containing warehouse path, URI, etc.
    ///
    /// # Returns
    /// An `Arc<dyn Catalog>` that can be used for database and table operations.
    ///
    /// # Errors
    /// - Returns an error if required options are missing
    /// - Returns an error if the metastore type is unknown
    pub async fn create(options: Options) -> Result<Arc<dyn Catalog>> {
        let metastore = options
            .get(CatalogOptions::METASTORE)
            .map(|s| s.as_str())
            .unwrap_or(METASTORE_FILESYSTEM);

        match metastore {
            METASTORE_FILESYSTEM => Self::create_filesystem_catalog(options),
            METASTORE_REST => Self::create_rest_catalog(options).await,
            _ => ConfigInvalidSnafu {
                message: format!(
                    "Unknown metastore type: '{metastore}'. Available types: {METASTORE_FILESYSTEM}, {METASTORE_REST}"
                ),
            }
            .fail(),
        }
    }

    /// Create a filesystem catalog.
    fn create_filesystem_catalog(options: Options) -> Result<Arc<dyn Catalog>> {
        let catalog = FileSystemCatalog::new(options)?;
        Ok(Arc::new(catalog))
    }

    /// Create a REST catalog.
    async fn create_rest_catalog(options: Options) -> Result<Arc<dyn Catalog>> {
        let catalog = RESTCatalog::new(options, true).await?;
        Ok(Arc::new(catalog))
    }
}

#[cfg(test)]
#[cfg(not(windows))] // Skip on Windows due to path compatibility issues
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_filesystem_catalog() {
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, "/tmp/test-warehouse");

        let result = CatalogFactory::create(options).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_filesystem_catalog_explicit() {
        let mut options = Options::new();
        options.set(CatalogOptions::METASTORE, "filesystem");
        options.set(CatalogOptions::WAREHOUSE, "/tmp/test-warehouse");

        let result = CatalogFactory::create(options).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_missing_warehouse_option() {
        let options = Options::new();
        let result = CatalogFactory::create(options).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unknown_metastore_type() {
        let mut options = Options::new();
        options.set(CatalogOptions::METASTORE, "unknown");
        options.set(CatalogOptions::WAREHOUSE, "/tmp/test");

        let result = CatalogFactory::create(options).await;
        assert!(result.is_err());
    }
}
