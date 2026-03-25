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

//! REST API implementation for Paimon.
//!
//! This module provides a REST API client for interacting with
//! Paimon rest catalog services, supporting database operations.

use std::collections::HashMap;

use crate::api::rest_client::HttpClient;
use crate::catalog::Identifier;
use crate::common::{CatalogOptions, Options};
use crate::spec::Schema;
use crate::Result;

use super::api_request::{
    AlterDatabaseRequest, CreateDatabaseRequest, CreateTableRequest, RenameTableRequest,
};
use super::api_response::{
    ConfigResponse, GetDatabaseResponse, GetTableResponse, ListDatabasesResponse,
    ListTablesResponse, PagedList,
};
use super::auth::{AuthProviderFactory, RESTAuthFunction};
use super::resource_paths::ResourcePaths;
use super::rest_util::RESTUtil;

/// Validate that a string is not empty after trimming.
///
/// # Arguments
/// * `value` - The string to validate.
/// * `field_name` - The name of the field for error messages.
///
/// # Returns
/// `Ok(())` if valid, `Err` if empty.
fn validate_non_empty(value: &str, field_name: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(crate::Error::ConfigInvalid {
            message: format!("{} cannot be empty", field_name),
        });
    }
    Ok(())
}

/// Validate that multiple strings are not empty after trimming.
///
/// # Arguments
/// * `values` - Slice of (value, field_name) pairs to validate.
///
/// # Returns
/// `Ok(())` if all valid, `Err` if any is empty.
fn validate_non_empty_multi(values: &[(&str, &str)]) -> Result<()> {
    for (value, field_name) in values {
        validate_non_empty(value, field_name)?;
    }
    Ok(())
}

/// REST API wrapper for Paimon catalog operations.
///
/// This struct provides methods for database and table CRUD operations
/// through a REST API client.
pub struct RESTApi {
    client: HttpClient,
    resource_paths: ResourcePaths,
    #[allow(dead_code)]
    options: Options,
}

impl RESTApi {
    // Constants for query parameters and headers
    pub const HEADER_PREFIX: &'static str = "header.";
    pub const MAX_RESULTS: &'static str = "maxResults";
    pub const PAGE_TOKEN: &'static str = "pageToken";
    pub const DATABASE_NAME_PATTERN: &'static str = "databaseNamePattern";
    pub const TABLE_NAME_PATTERN: &'static str = "tableNamePattern";
    pub const TABLE_TYPE: &'static str = "tableType";

    /// Create a new RESTApi from options.
    ///
    /// # Arguments
    /// * `options` - The configuration options containing URI, warehouse, etc.
    /// * `config_required` - Whether to fetch config from server and merge with options.
    ///
    /// # Returns
    /// A new RESTApi instance.
    ///
    /// # Errors
    /// Returns an error if required options are missing or if config fetch fails.
    pub async fn new(mut options: Options, config_required: bool) -> Result<Self> {
        let uri = options
            .get(CatalogOptions::URI)
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: "URI cannot be empty".to_string(),
            })?;

        if uri.trim().is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "URI cannot be empty".to_string(),
            });
        }

        let auth_provider = AuthProviderFactory::create_auth_provider(&options)?;
        let mut base_headers: HashMap<String, String> =
            RESTUtil::extract_prefix_map(&options, Self::HEADER_PREFIX);
        // Create auth function first, before making any requests
        let rest_auth_function = RESTAuthFunction::new(base_headers.clone(), auth_provider);

        let mut client = HttpClient::new(uri, Some(rest_auth_function))?;

        let options = if config_required {
            let warehouse = options.get(CatalogOptions::WAREHOUSE).ok_or_else(|| {
                crate::Error::ConfigInvalid {
                    message: "Warehouse name cannot be empty".to_string(),
                }
            })?;

            if warehouse.trim().is_empty() {
                return Err(crate::Error::ConfigInvalid {
                    message: "Warehouse name cannot be empty".to_string(),
                });
            }

            let query_params: Vec<(&str, String)> = vec![(
                CatalogOptions::WAREHOUSE,
                RESTUtil::encode_string(warehouse),
            )];
            let config_response: ConfigResponse = client
                .get(&ResourcePaths::config(), Some(&query_params))
                .await?;

            // Merge config response with options (client config takes priority)
            options = config_response.merge_options(&options);

            // Update base headers from merged options and recreate auth function
            base_headers.extend(RESTUtil::extract_prefix_map(&options, Self::HEADER_PREFIX));
            // Recreate auth function with updated headers if needed
            let auth_provider = AuthProviderFactory::create_auth_provider(&options)?;
            let rest_auth_function = RESTAuthFunction::new(base_headers, auth_provider);

            client.set_auth_function(rest_auth_function);

            options
        } else {
            options
        };

        let resource_paths = ResourcePaths::for_catalog_properties(&options);

        Ok(RESTApi {
            client,
            resource_paths,
            options,
        })
    }

    // ==================== Database Operations ====================

    /// List all databases.
    pub async fn list_databases(&mut self) -> Result<Vec<String>> {
        let mut results = Vec::new();
        let mut page_token: Option<String> = None;

        loop {
            let paged = self
                .list_databases_paged(None, page_token.as_deref(), None)
                .await?;
            let is_empty = paged.elements.is_empty();
            results.extend(paged.elements);
            page_token = paged.next_page_token;
            if page_token.is_none() || is_empty {
                break;
            }
        }

        Ok(results)
    }

    /// List databases with pagination.
    pub async fn list_databases_paged(
        &mut self,
        max_results: Option<u32>,
        page_token: Option<&str>,
        database_name_pattern: Option<&str>,
    ) -> Result<PagedList<String>> {
        let path = self.resource_paths.databases();
        let mut params: Vec<(&str, String)> = Vec::new();

        if let Some(max) = max_results {
            params.push((Self::MAX_RESULTS, max.to_string()));
        }

        if let Some(token) = page_token {
            params.push((Self::PAGE_TOKEN, token.to_string()));
        }

        if let Some(pattern) = database_name_pattern {
            params.push((Self::DATABASE_NAME_PATTERN, pattern.to_string()));
        }

        let response: ListDatabasesResponse = if params.is_empty() {
            self.client.get(&path, None::<&[(&str, &str)]>).await?
        } else {
            self.client.get(&path, Some(&params)).await?
        };

        Ok(PagedList::new(response.databases, response.next_page_token))
    }

    /// Create a new database.
    pub async fn create_database(
        &mut self,
        name: &str,
        options: Option<std::collections::HashMap<String, String>>,
    ) -> Result<()> {
        validate_non_empty(name, "database name")?;
        let path = self.resource_paths.databases();
        let request = CreateDatabaseRequest::new(name.to_string(), options.unwrap_or_default());
        let _resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(())
    }

    /// Get database information.
    pub async fn get_database(&mut self, name: &str) -> Result<GetDatabaseResponse> {
        validate_non_empty(name, "database name")?;
        let path = self.resource_paths.database(name);
        self.client.get(&path, None::<&[(&str, &str)]>).await
    }

    /// Alter database configuration.
    pub async fn alter_database(
        &mut self,
        name: &str,
        removals: Vec<String>,
        updates: std::collections::HashMap<String, String>,
    ) -> Result<()> {
        validate_non_empty(name, "database name")?;
        let path = self.resource_paths.database(name);
        let request = AlterDatabaseRequest::new(removals, updates);
        let _resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(())
    }

    /// Drop a database.
    pub async fn drop_database(&mut self, name: &str) -> Result<()> {
        validate_non_empty(name, "database name")?;
        let path = self.resource_paths.database(name);
        let _resp: serde_json::Value = self.client.delete(&path, None::<&[(&str, &str)]>).await?;
        Ok(())
    }

    // ==================== Table Operations ====================

    /// List all tables in a database.
    pub async fn list_tables(&mut self, database: &str) -> Result<Vec<String>> {
        validate_non_empty(database, "database name")?;

        let mut results = Vec::new();
        let mut page_token: Option<String> = None;

        loop {
            let paged = self
                .list_tables_paged(database, None, page_token.as_deref(), None, None)
                .await?;
            let is_empty = paged.elements.is_empty();
            results.extend(paged.elements);
            page_token = paged.next_page_token;
            if page_token.is_none() || is_empty {
                break;
            }
        }

        Ok(results)
    }

    /// List tables with pagination.
    pub async fn list_tables_paged(
        &mut self,
        database: &str,
        max_results: Option<u32>,
        page_token: Option<&str>,
        table_name_pattern: Option<&str>,
        table_type: Option<&str>,
    ) -> Result<PagedList<String>> {
        validate_non_empty(database, "database name")?;
        let path = self.resource_paths.tables(Some(database));
        let mut params: Vec<(&str, String)> = Vec::new();

        if let Some(max) = max_results {
            params.push((Self::MAX_RESULTS, max.to_string()));
        }

        if let Some(token) = page_token {
            params.push((Self::PAGE_TOKEN, token.to_string()));
        }

        if let Some(pattern) = table_name_pattern {
            params.push((Self::TABLE_NAME_PATTERN, pattern.to_string()));
        }

        if let Some(ttype) = table_type {
            params.push((Self::TABLE_TYPE, ttype.to_string()));
        }

        let response: ListTablesResponse = if params.is_empty() {
            self.client.get(&path, None::<&[(&str, &str)]>).await?
        } else {
            self.client.get(&path, Some(&params)).await?
        };

        Ok(PagedList::new(
            response.tables.unwrap_or_default(),
            response.next_page_token,
        ))
    }

    /// Create a new table.
    pub async fn create_table(&mut self, identifier: &Identifier, schema: Schema) -> Result<()> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self.resource_paths.tables(Some(database));
        let request = CreateTableRequest::new(identifier.clone(), schema);
        let _resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(())
    }

    /// Get table information.
    pub async fn get_table(&mut self, identifier: &Identifier) -> Result<GetTableResponse> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self.resource_paths.table(database, table);
        self.client.get(&path, None::<&[(&str, &str)]>).await
    }

    /// Rename a table.
    pub async fn rename_table(
        &mut self,
        source: &Identifier,
        destination: &Identifier,
    ) -> Result<()> {
        validate_non_empty_multi(&[
            (source.database(), "source database name"),
            (source.object(), "source table name"),
            (destination.database(), "destination database name"),
            (destination.object(), "destination table name"),
        ])?;
        let path = self.resource_paths.rename_table();
        let request = RenameTableRequest::new(source.clone(), destination.clone());
        let _resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(())
    }

    /// Drop a table.
    pub async fn drop_table(&mut self, identifier: &Identifier) -> Result<()> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self.resource_paths.table(database, table);
        let _resp: serde_json::Value = self.client.delete(&path, None::<&[(&str, &str)]>).await?;
        Ok(())
    }
}
