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

use std::collections::{HashMap, HashSet};

use crate::api::rest_client::HttpClient;
use crate::catalog::{Function, Identifier, ViewSchema};
use crate::common::{CatalogOptions, Options};
use crate::spec::{Partition, PartitionStatistics, Schema, SchemaChange, Snapshot};
use crate::Result;

use super::api_request::{
    AlterDatabaseRequest, AlterTableRequest, AuthTableQueryRequest, CreateDatabaseRequest,
    CreateFunctionRequest, CreatePartitionsRequest, CreateTableRequest, CreateViewRequest,
    DropPartitionsRequest, ListPartitionsByFilterRequest, ListPartitionsByNamesRequest,
    RenameTableRequest,
};
use super::api_response::{
    AuthTableQueryResponse, ConfigResponse, GetDatabaseResponse, GetFunctionResponse,
    GetTableResponse, GetViewResponse, ListDatabasesResponse, ListFunctionsResponse,
    ListPartitionsResponse, ListTablesResponse, ListViewsResponse, PagedList,
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
            message: format!("{field_name} cannot be empty"),
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
    options: Options,
}

impl RESTApi {
    // Constants for query parameters and headers
    pub const HEADER_PREFIX: &'static str = "header.";
    pub const MAX_RESULTS: &'static str = "maxResults";
    pub const PAGE_TOKEN: &'static str = "pageToken";
    pub const DATABASE_NAME_PATTERN: &'static str = "databaseNamePattern";
    pub const TABLE_NAME_PATTERN: &'static str = "tableNamePattern";
    pub const VIEW_NAME_PATTERN: &'static str = "viewNamePattern";
    pub const FUNCTION_NAME_PATTERN: &'static str = "functionNamePattern";
    pub const PARTITION_NAME_PATTERN: &'static str = "partitionNamePattern";
    /// Bounds one request: catalog services cap the partitions a single call may carry.
    const PARTITION_REQUEST_SIZE: u32 = 1000;
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
    pub async fn new(options: Options, config_required: bool) -> Result<Self> {
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
            let merged = config_response.merge_options(&options);

            // Update base headers from merged options and recreate auth function
            base_headers.extend(RESTUtil::extract_prefix_map(&merged, Self::HEADER_PREFIX));
            // Recreate auth function with updated headers if needed
            let auth_provider = AuthProviderFactory::create_auth_provider(&merged)?;
            let rest_auth_function = RESTAuthFunction::new(base_headers, auth_provider);

            client.set_auth_function(rest_auth_function);

            merged
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

    /// Get the options (potentially merged with server config).
    pub fn options(&self) -> &Options {
        &self.options
    }

    // ==================== Database Operations ====================

    /// List all databases.
    pub async fn list_databases(&self) -> Result<Vec<String>> {
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
        &self,
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
        &self,
        name: &str,
        options: Option<HashMap<String, String>>,
    ) -> Result<()> {
        validate_non_empty(name, "database name")?;
        let path = self.resource_paths.databases();
        let request = CreateDatabaseRequest::new(name.to_string(), options.unwrap_or_default());
        let _resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(())
    }

    /// Get database information.
    pub async fn get_database(&self, name: &str) -> Result<GetDatabaseResponse> {
        validate_non_empty(name, "database name")?;
        let path = self.resource_paths.database(name);
        self.client.get(&path, None::<&[(&str, &str)]>).await
    }

    /// Alter database configuration.
    pub async fn alter_database(
        &self,
        name: &str,
        removals: Vec<String>,
        updates: HashMap<String, String>,
    ) -> Result<()> {
        validate_non_empty(name, "database name")?;
        let path = self.resource_paths.database(name);
        let request = AlterDatabaseRequest::new(removals, updates);
        let _resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(())
    }

    /// Drop a database.
    pub async fn drop_database(&self, name: &str) -> Result<()> {
        validate_non_empty(name, "database name")?;
        let path = self.resource_paths.database(name);
        let _resp: serde_json::Value = self.client.delete(&path, None::<&[(&str, &str)]>).await?;
        Ok(())
    }

    // ==================== Table Operations ====================

    /// List all tables in a database.
    pub async fn list_tables(&self, database: &str) -> Result<Vec<String>> {
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
        &self,
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
    pub async fn create_table(&self, identifier: &Identifier, schema: Schema) -> Result<()> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self.resource_paths.tables(Some(database));
        let request = CreateTableRequest::new(identifier.clone(), schema);
        let _resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(())
    }

    /// Alter a table's schema by applying a list of schema changes.
    pub async fn alter_table(
        &self,
        identifier: &Identifier,
        changes: Vec<SchemaChange>,
    ) -> Result<()> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self.resource_paths.table(database, table);
        let request = AlterTableRequest::new(changes);
        let _resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(())
    }

    /// Get table information.
    pub async fn get_table(&self, identifier: &Identifier) -> Result<GetTableResponse> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self.resource_paths.table(database, table);
        self.client.get(&path, None::<&[(&str, &str)]>).await
    }

    /// Rename a table.
    pub async fn rename_table(&self, source: &Identifier, destination: &Identifier) -> Result<()> {
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
    pub async fn drop_table(&self, identifier: &Identifier) -> Result<()> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self.resource_paths.table(database, table);
        let _resp: serde_json::Value = self.client.delete(&path, None::<&[(&str, &str)]>).await?;
        Ok(())
    }

    // ==================== View Operations ====================

    /// Create a persistent view.
    pub async fn create_view(&self, identifier: &Identifier, schema: ViewSchema) -> Result<()> {
        let database = identifier.database();
        let view = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (view, "view name")])?;
        let path = self.resource_paths.views(database);
        let request = CreateViewRequest::new(identifier.clone(), schema);
        let _resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(())
    }

    /// List persistent views in a database.
    pub async fn list_views(&self, database: &str) -> Result<Vec<String>> {
        validate_non_empty(database, "database name")?;
        let mut results = Vec::new();
        let mut page_token = None;
        loop {
            let page = self
                .list_views_paged(database, None, page_token.as_deref(), None)
                .await?;
            let is_empty = page.elements.is_empty();
            results.extend(page.elements);
            page_token = page.next_page_token;
            if page_token.is_none() || is_empty {
                break;
            }
        }
        Ok(results)
    }

    /// List persistent views in a database with pagination.
    pub async fn list_views_paged(
        &self,
        database: &str,
        max_results: Option<u32>,
        page_token: Option<&str>,
        view_name_pattern: Option<&str>,
    ) -> Result<PagedList<String>> {
        validate_non_empty(database, "database name")?;
        let path = self.resource_paths.views(database);
        let mut params = Vec::new();
        if let Some(max_results) = max_results {
            params.push((Self::MAX_RESULTS, max_results.to_string()));
        }
        if let Some(page_token) = page_token {
            params.push((Self::PAGE_TOKEN, page_token.to_string()));
        }
        if let Some(view_name_pattern) = view_name_pattern {
            params.push((Self::VIEW_NAME_PATTERN, view_name_pattern.to_string()));
        }
        let response: ListViewsResponse = if params.is_empty() {
            self.client.get(&path, None::<&[(&str, &str)]>).await?
        } else {
            self.client.get(&path, Some(&params)).await?
        };
        Ok(PagedList::new(
            response.views.unwrap_or_default(),
            response.next_page_token,
        ))
    }

    /// Get persistent view information.
    pub async fn get_view(&self, identifier: &Identifier) -> Result<GetViewResponse> {
        let database = identifier.database();
        let view = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (view, "view name")])?;
        let path = self.resource_paths.view(database, view);
        self.client.get(&path, None::<&[(&str, &str)]>).await
    }

    /// Drop a persistent view.
    pub async fn drop_view(&self, identifier: &Identifier) -> Result<()> {
        let database = identifier.database();
        let view = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (view, "view name")])?;
        let path = self.resource_paths.view(database, view);
        let _resp: serde_json::Value = self.client.delete(&path, None::<&[(&str, &str)]>).await?;
        Ok(())
    }

    // ==================== Function Operations ====================

    /// Create a persistent function.
    pub async fn create_function(&self, function: &Function) -> Result<()> {
        let database = function.identifier().database();
        validate_non_empty_multi(&[
            (database, "database name"),
            (function.name(), "function name"),
        ])?;
        let path = self.resource_paths.functions(database);
        let request = CreateFunctionRequest::from_function(function);
        let _resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(())
    }

    /// List persistent functions in a database.
    pub async fn list_functions(&self, database: &str) -> Result<Vec<String>> {
        validate_non_empty(database, "database name")?;
        let mut results = Vec::new();
        let mut page_token = None;
        loop {
            let page = self
                .list_functions_paged(database, None, page_token.as_deref(), None)
                .await?;
            let is_empty = page.elements.is_empty();
            results.extend(page.elements);
            page_token = page.next_page_token;
            if page_token.is_none() || is_empty {
                break;
            }
        }
        Ok(results)
    }

    /// List persistent functions in a database with pagination.
    pub async fn list_functions_paged(
        &self,
        database: &str,
        max_results: Option<u32>,
        page_token: Option<&str>,
        function_name_pattern: Option<&str>,
    ) -> Result<PagedList<String>> {
        validate_non_empty(database, "database name")?;
        let path = self.resource_paths.functions(database);
        let mut params = Vec::new();
        if let Some(max_results) = max_results {
            params.push((Self::MAX_RESULTS, max_results.to_string()));
        }
        if let Some(page_token) = page_token {
            params.push((Self::PAGE_TOKEN, page_token.to_string()));
        }
        if let Some(function_name_pattern) = function_name_pattern {
            params.push((
                Self::FUNCTION_NAME_PATTERN,
                function_name_pattern.to_string(),
            ));
        }
        let response: ListFunctionsResponse = if params.is_empty() {
            self.client.get(&path, None::<&[(&str, &str)]>).await?
        } else {
            self.client.get(&path, Some(&params)).await?
        };
        Ok(PagedList::new(
            response.functions.unwrap_or_default(),
            response.next_page_token,
        ))
    }

    /// Get persistent function information.
    pub async fn get_function(&self, identifier: &Identifier) -> Result<GetFunctionResponse> {
        let database = identifier.database();
        let function = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (function, "function name")])?;
        let path = self.resource_paths.function(database, function);
        self.client.get(&path, None::<&[(&str, &str)]>).await
    }

    // ==================== Partition Operations ====================

    /// Create table partitions in a single REST request.
    pub async fn create_partitions(
        &self,
        identifier: &Identifier,
        partition_specs: Vec<HashMap<String, String>>,
        ignore_if_exists: bool,
    ) -> Result<()> {
        self.create_partitions_with_statistics(
            identifier,
            partition_specs,
            ignore_if_exists,
            None,
            false,
        )
        .await
    }

    /// Create table partitions and report statistics for them in a single REST request.
    ///
    /// Statistics are matched to the specs by spec and may cover only some of them.
    /// `replace_statistics` says whether they replace what the catalog holds or add to it, and is
    /// not sent when no statistics are.
    pub async fn create_partitions_with_statistics(
        &self,
        identifier: &Identifier,
        partition_specs: Vec<HashMap<String, String>>,
        ignore_if_exists: bool,
        statistics: Option<Vec<PartitionStatistics>>,
        replace_statistics: bool,
    ) -> Result<()> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self.resource_paths.partitions(database, table);
        let mut request = CreatePartitionsRequest::new(partition_specs, ignore_if_exists);
        if let Some(statistics) = statistics {
            request = request.with_statistics(statistics, replace_statistics);
        }
        let _resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(())
    }

    /// Unregister table partitions in a single REST request.
    ///
    /// The REST service removes metadata only; it does not delete partition
    /// directories or data files.
    pub async fn drop_partitions(
        &self,
        identifier: &Identifier,
        partition_specs: Vec<HashMap<String, String>>,
        ignore_if_not_exists: bool,
    ) -> Result<()> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self.resource_paths.drop_partitions(database, table);
        let request = DropPartitionsRequest::new(partition_specs, ignore_if_not_exists);
        let _resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(())
    }

    /// List all partitions of a table, paging internally.
    pub async fn list_partitions(&self, identifier: &Identifier) -> Result<Vec<Partition>> {
        self.drain_partitions(identifier, None, None, None).await
    }

    /// List partitions, asking the catalog to return only those whose partition name
    /// matches `partition_name_pattern`.
    ///
    /// The pattern is a pushdown hint: a catalog may apply it partially or not at all, so
    /// the result is a superset of the matching partitions and never misses one. Callers
    /// keep applying their own filter to what comes back.
    pub async fn list_partitions_by_name_pattern(
        &self,
        identifier: &Identifier,
        partition_name_pattern: Option<&str>,
    ) -> Result<Vec<Partition>> {
        self.drain_partitions(
            identifier,
            Some(Self::PARTITION_REQUEST_SIZE),
            partition_name_pattern,
            None,
        )
        .await
    }

    /// List partitions, asking the catalog to return only those matching `filter`, a partition
    /// predicate in the REST catalog predicate JSON format, together with
    /// `partition_name_pattern` when one is given.
    ///
    /// Like the pattern, the filter is a pushdown hint: a catalog may apply it partially or not
    /// at all, so callers keep applying their own filter to what comes back.
    pub async fn list_partitions_by_filter(
        &self,
        identifier: &Identifier,
        filter: &str,
        partition_name_pattern: Option<&str>,
    ) -> Result<Vec<Partition>> {
        self.drain_partitions(
            identifier,
            Some(Self::PARTITION_REQUEST_SIZE),
            partition_name_pattern,
            Some(filter),
        )
        .await
    }

    /// List one page of partitions matching `filter`. See [`Self::list_partitions_by_filter`].
    pub async fn list_partitions_by_filter_paged(
        &self,
        identifier: &Identifier,
        filter: &str,
        max_results: Option<u32>,
        page_token: Option<&str>,
        partition_name_pattern: Option<&str>,
    ) -> Result<PagedList<Partition>> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self
            .resource_paths
            .list_partitions_by_filter(database, table);
        let request = ListPartitionsByFilterRequest::new(
            filter.to_string(),
            partition_name_pattern
                .filter(|pattern| !pattern.is_empty())
                .map(str::to_string),
            max_results,
            page_token.map(str::to_string),
        );
        let response: ListPartitionsResponse = self.client.post(&path, &request).await?;
        Ok(PagedList::new(
            response.partitions.unwrap_or_default(),
            response.next_page_token,
        ))
    }

    /// Return those of the given complete partition specs that are registered.
    ///
    /// The specs go out in one request, so callers bound how many they send at once.
    pub async fn list_partitions_by_names(
        &self,
        identifier: &Identifier,
        partition_specs: Vec<HashMap<String, String>>,
    ) -> Result<Vec<Partition>> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self
            .resource_paths
            .list_partitions_by_names(database, table);
        let request = ListPartitionsByNamesRequest::new(partition_specs);
        let response: ListPartitionsResponse = self.client.post(&path, &request).await?;
        Ok(response.partitions.unwrap_or_default())
    }

    async fn drain_partitions(
        &self,
        identifier: &Identifier,
        max_results: Option<u32>,
        partition_name_pattern: Option<&str>,
        filter: Option<&str>,
    ) -> Result<Vec<Partition>> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;

        let mut results = Vec::new();
        let mut page_token: Option<String> = None;
        let mut seen_page_tokens = HashSet::new();

        loop {
            let paged = match filter {
                Some(filter) => {
                    self.list_partitions_by_filter_paged(
                        identifier,
                        filter,
                        max_results,
                        page_token.as_deref(),
                        partition_name_pattern,
                    )
                    .await?
                }
                None => {
                    self.list_partitions_paged(
                        identifier,
                        max_results,
                        page_token.as_deref(),
                        partition_name_pattern,
                    )
                    .await?
                }
            };
            results.extend(paged.elements);

            let Some(next_page_token) = paged.next_page_token.filter(|token| !token.is_empty())
            else {
                break;
            };
            if !seen_page_tokens.insert(next_page_token.clone()) {
                return Err(crate::Error::UnexpectedError {
                    message: format!(
                        "REST catalog returned partition page token '{next_page_token}' more than \
                         once for table {}",
                        identifier.full_name()
                    ),
                    source: None,
                });
            }
            page_token = Some(next_page_token);
        }

        Ok(results)
    }

    /// List partitions with pagination.
    ///
    /// `partition_name_pattern` is a SQL LIKE pattern over partition names, where `%` is
    /// the only wildcard. Like [`Self::list_partitions_by_name_pattern`], it is a hint the
    /// catalog may ignore.
    pub async fn list_partitions_paged(
        &self,
        identifier: &Identifier,
        max_results: Option<u32>,
        page_token: Option<&str>,
        partition_name_pattern: Option<&str>,
    ) -> Result<PagedList<Partition>> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self.resource_paths.partitions(database, table);
        let mut params: Vec<(&str, String)> = Vec::new();

        if let Some(max) = max_results {
            params.push((Self::MAX_RESULTS, max.to_string()));
        }
        if let Some(token) = page_token {
            params.push((Self::PAGE_TOKEN, token.to_string()));
        }
        if let Some(pattern) = partition_name_pattern.filter(|pattern| !pattern.is_empty()) {
            params.push((Self::PARTITION_NAME_PATTERN, pattern.to_string()));
        }

        let response: ListPartitionsResponse = if params.is_empty() {
            self.client.get(&path, None::<&[(&str, &str)]>).await?
        } else {
            self.client.get(&path, Some(&params)).await?
        };

        Ok(PagedList::new(
            response.partitions.unwrap_or_default(),
            response.next_page_token,
        ))
    }

    // ==================== Token Operations ====================

    /// Load table token for data access.
    ///
    /// Corresponds to Python `RESTApi.load_table_token`.
    pub async fn load_table_token(
        &self,
        identifier: &Identifier,
    ) -> Result<super::api_response::GetTableTokenResponse> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self.resource_paths.table_token(database, table);
        self.client.get(&path, None::<&[(&str, &str)]>).await
    }

    /// Fetch the per-user row filter and column masking for a `query-auth.enabled`
    /// table. `select` is the query's projected columns (`None` = all columns).
    pub async fn auth_table_query(
        &self,
        identifier: &Identifier,
        select: Option<Vec<String>>,
    ) -> Result<AuthTableQueryResponse> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self.resource_paths.auth_table(database, table);
        let request = AuthTableQueryRequest::new(select);
        self.client.post(&path, &request).await
    }

    // ==================== Commit Operations ====================

    /// Commit a snapshot for a table.
    ///
    /// Corresponds to Python `RESTApi.commit_snapshot`.
    pub async fn commit_snapshot(
        &self,
        identifier: &Identifier,
        table_uuid: &str,
        snapshot: &Snapshot,
        statistics: &[PartitionStatistics],
    ) -> Result<bool> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self.resource_paths.commit_table(database, table);
        let request = serde_json::json!({
            "tableUuid": table_uuid,
            "snapshot": snapshot,
            "statistics": statistics,
        });
        let resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(resp
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(false))
    }

    /// Rollback a table to a specific snapshot.
    pub async fn rollback_to_snapshot(
        &self,
        identifier: &Identifier,
        snapshot_id: i64,
    ) -> Result<()> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[(database, "database name"), (table, "table name")])?;
        let path = self.resource_paths.rollback(database, table);
        let request = serde_json::json!({
            "instant": {
                "type": "snapshot",
                "snapshotId": snapshot_id,
            }
        });
        let _resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(())
    }

    /// Rollback a table to a specific tag.
    pub async fn rollback_to_tag(&self, identifier: &Identifier, tag_name: &str) -> Result<()> {
        let database = identifier.database();
        let table = identifier.object();
        validate_non_empty_multi(&[
            (database, "database name"),
            (table, "table name"),
            (tag_name, "tag name"),
        ])?;
        let path = self.resource_paths.rollback(database, table);
        let request = serde_json::json!({
            "instant": {
                "type": "tag",
                "tagName": tag_name,
            }
        });
        let _resp: serde_json::Value = self.client.post(&path, &request).await?;
        Ok(())
    }
}
