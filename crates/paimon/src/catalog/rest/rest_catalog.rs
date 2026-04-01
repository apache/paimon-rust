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

//! REST catalog implementation for Apache Paimon.
//!
//! This module provides a REST-based catalog that communicates with
//! a Paimon REST catalog server for database and table CRUD operations.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::api::rest_api::RESTApi;
use crate::api::rest_error::RestError;
use crate::api::PagedList;
use crate::catalog::{Catalog, Database, Identifier, DB_LOCATION_PROP};
use crate::common::{CatalogOptions, Options};
use crate::error::Error;
use crate::io::FileIO;
use crate::spec::{Schema, SchemaChange, TableSchema};
use crate::table::Table;
use crate::Result;

use super::rest_token_file_io::RESTTokenFileIO;

/// REST catalog implementation.
///
/// This catalog communicates with a Paimon REST catalog server
/// for all metadata operations (database and table CRUD).
///
/// Corresponds to Python `RESTCatalog` in `pypaimon/catalog/rest/rest_catalog.py`.
pub struct RESTCatalog {
    /// The REST API client.
    api: RESTApi,
    /// Catalog configuration options.
    options: Options,
    /// Warehouse path.
    warehouse: String,
    /// Whether data token is enabled for FileIO construction.
    data_token_enabled: bool,
}

impl RESTCatalog {
    /// Create a new REST catalog.
    ///
    /// # Arguments
    /// * `options` - Configuration options containing URI, warehouse, etc.
    /// * `config_required` - Whether to fetch config from server and merge with options.
    ///
    /// # Errors
    /// Returns an error if required options are missing or if initialization fails.
    pub async fn new(options: Options, config_required: bool) -> Result<Self> {
        let warehouse = options
            .get(CatalogOptions::WAREHOUSE)
            .cloned()
            .ok_or_else(|| RestError::BadRequest {
                message: format!("Missing required option: {}", CatalogOptions::WAREHOUSE),
            })?;

        let api = RESTApi::new(options.clone(), config_required).await?;

        let data_token_enabled = api
            .options()
            .get(CatalogOptions::DATA_TOKEN_ENABLED)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let api_options = api.options().clone();

        Ok(Self {
            api,
            options: api_options,
            warehouse,
            data_token_enabled,
        })
    }

    /// Get the warehouse path.
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    /// Get the catalog options.
    pub fn options(&self) -> &Options {
        &self.options
    }

    /// Whether data token is enabled.
    pub fn data_token_enabled(&self) -> bool {
        self.data_token_enabled
    }

    /// List databases with pagination.
    pub async fn list_databases_paged(
        &self,
        max_results: Option<u32>,
        page_token: Option<&str>,
        database_name_pattern: Option<&str>,
    ) -> Result<PagedList<String>> {
        self.api
            .list_databases_paged(max_results, page_token, database_name_pattern)
            .await
    }
}

// ============================================================================
// Catalog trait implementation
// ============================================================================

#[async_trait]
impl Catalog for RESTCatalog {
    // ======================= database methods ===============================

    async fn list_databases(&self) -> Result<Vec<String>> {
        self.api.list_databases().await
    }

    async fn create_database(
        &self,
        name: &str,
        ignore_if_exists: bool,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let result = self
            .api
            .create_database(name, Some(properties))
            .await
            .map_err(|e| map_rest_error_for_database(e, name));
        ignore_error_if(result, |e| {
            ignore_if_exists && matches!(e, Error::DatabaseAlreadyExist { .. })
        })
    }

    async fn get_database(&self, name: &str) -> Result<Database> {
        let response = self
            .api
            .get_database(name)
            .await
            .map_err(|e| map_rest_error_for_database(e, name))?;

        let mut options = response.options;
        if let Some(location) = response.location {
            options.insert(DB_LOCATION_PROP.to_string(), location);
        }

        Ok(Database::new(name.to_string(), options, None))
    }

    async fn drop_database(
        &self,
        name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> Result<()> {
        // If not cascade, check if database is empty first
        if !cascade {
            let tables = match self.api.list_tables(name).await {
                Ok(tables) => tables,
                Err(err) => {
                    return ignore_error_if(Err(map_rest_error_for_database(err, name)), |e| {
                        ignore_if_not_exists && matches!(e, Error::DatabaseNotExist { .. })
                    });
                }
            };
            if !tables.is_empty() {
                return Err(Error::DatabaseNotEmpty {
                    database: name.to_string(),
                });
            }
        }

        let result = self
            .api
            .drop_database(name)
            .await
            .map_err(|e| map_rest_error_for_database(e, name));
        ignore_error_if(result, |e| {
            ignore_if_not_exists && matches!(e, Error::DatabaseNotExist { .. })
        })
    }

    // ======================= table methods ===============================

    async fn get_table(&self, identifier: &Identifier) -> Result<Table> {
        let response = self
            .api
            .get_table(identifier)
            .await
            .map_err(|e| map_rest_error_for_table(e, identifier))?;

        // Extract schema from response
        let schema = response.schema.ok_or_else(|| Error::DataInvalid {
            message: format!("Table {} response missing schema", identifier.full_name()),
            source: None,
        })?;

        let schema_id = response.schema_id.ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Table {} response missing schema_id",
                identifier.full_name()
            ),
            source: None,
        })?;
        let table_schema = TableSchema::new(schema_id, &schema);

        // Extract table path from response
        let table_path = response.path.ok_or_else(|| Error::DataInvalid {
            message: format!("Table {} response missing path", identifier.full_name()),
            source: None,
        })?;

        // Check if the table is external
        let is_external = response.is_external.ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Table {} response missing is_external",
                identifier.full_name()
            ),
            source: None,
        })?;

        // Build FileIO based on data_token_enabled and is_external
        // TODO Support token cache and direct oss access
        let file_io = if self.data_token_enabled && !is_external {
            // Use RESTTokenFileIO to get token-based FileIO
            let token_file_io =
                RESTTokenFileIO::new(identifier.clone(), table_path.clone(), self.options.clone());
            token_file_io.build_file_io().await?
        } else {
            // Use standard FileIO from path
            FileIO::from_path(&table_path)?.build()?
        };

        Ok(Table::new(
            file_io,
            identifier.clone(),
            table_path,
            table_schema,
        ))
    }

    async fn list_tables(&self, database_name: &str) -> Result<Vec<String>> {
        self.api
            .list_tables(database_name)
            .await
            .map_err(|e| map_rest_error_for_database(e, database_name))
    }

    async fn create_table(
        &self,
        identifier: &Identifier,
        creation: Schema,
        ignore_if_exists: bool,
    ) -> Result<()> {
        let result = self
            .api
            .create_table(identifier, creation)
            .await
            .map_err(|e| map_rest_error_for_table(e, identifier));
        ignore_error_if(result, |e| {
            ignore_if_exists && matches!(e, Error::TableAlreadyExist { .. })
        })
    }

    async fn drop_table(&self, identifier: &Identifier, ignore_if_not_exists: bool) -> Result<()> {
        let result = self
            .api
            .drop_table(identifier)
            .await
            .map_err(|e| map_rest_error_for_table(e, identifier));
        ignore_error_if(result, |e| {
            ignore_if_not_exists && matches!(e, Error::TableNotExist { .. })
        })
    }

    async fn rename_table(
        &self,
        from: &Identifier,
        to: &Identifier,
        ignore_if_not_exists: bool,
    ) -> Result<()> {
        let result = self
            .api
            .rename_table(from, to)
            .await
            .map_err(|e| map_rest_error_for_table(e, from))
            // Remap TableAlreadyExist to use destination identifier
            .map_err(|e| match e {
                Error::TableAlreadyExist { .. } => Error::TableAlreadyExist {
                    full_name: to.full_name(),
                },
                other => other,
            });
        ignore_error_if(result, |e| {
            ignore_if_not_exists && matches!(e, Error::TableNotExist { .. })
        })
    }

    async fn alter_table(
        &self,
        _identifier: &Identifier,
        _changes: Vec<SchemaChange>,
        _ignore_if_not_exists: bool,
    ) -> Result<()> {
        // TODO: Implement alter_table when RESTApi supports it
        Err(Error::Unsupported {
            message: "Alter table is not yet implemented for REST catalog".to_string(),
        })
    }
}
// ============================================================================
// Error mapping helpers
// ============================================================================

/// Map a REST API error to a catalog-level database error.
///
/// Converts `RestError::NoSuchResource` -> `Error::DatabaseNotExist`,
/// `RestError::AlreadyExists` -> `Error::DatabaseAlreadyExist`,
/// and passes through other errors via `Error::RestApi`.
fn map_rest_error_for_database(err: Error, database_name: &str) -> Error {
    match err {
        Error::RestApi {
            source: RestError::NoSuchResource { .. },
        } => Error::DatabaseNotExist {
            database: database_name.to_string(),
        },
        Error::RestApi {
            source: RestError::AlreadyExists { .. },
        } => Error::DatabaseAlreadyExist {
            database: database_name.to_string(),
        },
        other => other,
    }
}

/// Map a REST API error to a catalog-level table error.
///
/// Converts `RestError::NoSuchResource` -> `Error::TableNotExist`,
/// `RestError::AlreadyExists` -> `Error::TableAlreadyExist`,
/// and passes through other errors via `Error::RestApi`.
fn map_rest_error_for_table(err: Error, identifier: &Identifier) -> Error {
    match err {
        Error::RestApi {
            source: RestError::NoSuchResource { .. },
        } => Error::TableNotExist {
            full_name: identifier.full_name(),
        },
        Error::RestApi {
            source: RestError::AlreadyExists { .. },
        } => Error::TableAlreadyExist {
            full_name: identifier.full_name(),
        },
        other => other,
    }
}

/// Execute a fallible operation and ignore a specific error variant.
///
/// If the operation succeeds, returns `Ok(())`.
/// If it fails with an error that `should_ignore` returns `true` for, returns `Ok(())`.
/// Otherwise, returns the error.
fn ignore_error_if<F>(result: Result<()>, should_ignore: F) -> Result<()>
where
    F: Fn(&Error) -> bool,
{
    result.or_else(|err| {
        if should_ignore(&err) {
            Ok(())
        } else {
            Err(err)
        }
    })
}
