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

//! REST token-based FileIO for Apache Paimon.
//!
//! This module provides a FileIO wrapper that supports getting data access
//! tokens from a REST Server. It handles token caching, expiration detection,
//! and automatic refresh.
//!
//! Corresponds to Python `RESTTokenFileIO` in
//! `pypaimon/catalog/rest/rest_token_file_io.py`.

use std::collections::HashMap;

use tokio::sync::RwLock;

use crate::api::rest_api::RESTApi;
use crate::catalog::Identifier;
use crate::common::Options;
use crate::io::FileIO;
use crate::Result;

use super::rest_token::RESTToken;

/// Safe time margin (in milliseconds) before token expiration to trigger refresh.
/// Matches `RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS` in Python.
const TOKEN_EXPIRATION_SAFE_TIME_MILLIS: i64 = 3_600_000;

/// A FileIO wrapper that supports getting data access tokens from a REST Server.
///
/// This struct handles:
/// - Token caching with expiration detection
/// - Automatic token refresh via `RESTApi::load_table_token`
/// - Merging token credentials into catalog options to build the underlying `FileIO`
///
/// Corresponds to Python `RESTTokenFileIO` in
/// `pypaimon/catalog/rest/rest_token_file_io.py`.
pub struct RESTTokenFileIO {
    /// Table identifier for token requests.
    identifier: Identifier,
    /// Table path (e.g. "oss://bucket/warehouse/db.db/table").
    path: String,
    /// Catalog options used to create RESTApi and build FileIO.
    catalog_options: Options,
    /// Cached token with RwLock for concurrent access.
    token: RwLock<Option<RESTToken>>,
}

impl RESTTokenFileIO {
    /// Create a new RESTTokenFileIO.
    ///
    /// # Arguments
    /// * `identifier` - Table identifier for token requests.
    /// * `path` - Table path for FileIO construction.
    /// * `catalog_options` - Catalog options for RESTApi and FileIO.
    pub fn new(identifier: Identifier, path: String, catalog_options: Options) -> Self {
        Self {
            identifier,
            path,
            catalog_options,
            token: RwLock::new(None),
        }
    }

    /// Build a `FileIO` instance with the current token merged into options.
    ///
    /// This method:
    /// 1. Refreshes the token if expired or not yet obtained.
    /// 2. Merges token credentials into catalog options.
    /// 3. Builds a `FileIO` from the merged options.
    ///
    /// This method builds a FileIO with the current token,
    /// which can be passed to `Table::new`. If the token expires, a new
    /// `get_table` call is needed.
    pub async fn build_file_io(&self) -> Result<FileIO> {
        // Ensure token is fresh
        self.try_to_refresh_token().await?;

        let token_guard = self.token.read().await;
        match token_guard.as_ref() {
            Some(token) => {
                // Merge token credentials with catalog options
                // token.token["fs.oss.endpoint"] = oss-cn-hangzhou.aliyuncs.com 
                let mut token_with_endpoint = token.token.clone();
                token_with_endpoint.insert("fs.oss.endpoint".to_string(), "oss-cn-hangzhou.aliyuncs.com".to_string());
                let merged_props = self.merge_token_with_options(&token_with_endpoint);
                // let merged_props = self.merge_token_with_options(&token.token);
                // Build FileIO with merged properties
                let mut builder = FileIO::from_path(&self.path)?;
                builder = builder.with_props(merged_props);
                builder.build()
            }
            None => {
                // No token available, build FileIO from path only
                FileIO::from_path(&self.path)?.build()
            }
        }
    }

    /// Try to refresh the token if it is expired or not yet obtained.
    ///
    /// Corresponds to Python `RESTTokenFileIO.try_to_refresh_token`.
    async fn try_to_refresh_token(&self) -> Result<()> {
        // Fast path: check if token is still valid under read lock
        {
            let token_guard = self.token.read().await;
            if let Some(ref token) = *token_guard {
                if !Self::is_token_expired(token) {
                    return Ok(());
                }
            }
        }

        // Slow path: acquire write lock and refresh
        let mut token_guard = self.token.write().await;

        // Double-check after acquiring write lock (another task may have refreshed)
        if let Some(ref token) = *token_guard {
            if !Self::is_token_expired(token) {
                return Ok(());
            }
        }

        // Refresh the token
        let new_token = self.refresh_token().await?;
        *token_guard = Some(new_token);
        Ok(())
    }

    /// Refresh the token by calling `RESTApi::load_table_token`.
    ///
    /// Creates a temporary `RESTApi` instance (with `config_required=false`)
    /// and loads the table token.
    ///
    /// Corresponds to Python `RESTTokenFileIO.refresh_token`.
    async fn refresh_token(&self) -> Result<RESTToken> {
        let mut api = RESTApi::new(self.catalog_options.clone(), false).await?;
        let response = api.load_table_token(&self.identifier).await?;

        let expires_at_millis = response.expires_at_millis.unwrap_or(0);

        // Merge token with catalog options (e.g. DLF OSS endpoint override)
        let merged_token = self.merge_token_with_catalog_options(response.token);
        Ok(RESTToken::new(merged_token, expires_at_millis))
    }

    /// Check if a token is expired (within the safe time margin).
    fn is_token_expired(token: &RESTToken) -> bool {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        (token.expire_at_millis - current_time) < TOKEN_EXPIRATION_SAFE_TIME_MILLIS
    }

    /// Merge token credentials with catalog options for DLF OSS endpoint override.
    ///
    /// Corresponds to Python `RESTTokenFileIO._merge_token_with_catalog_options`.
    fn merge_token_with_catalog_options(
        &self,
        token: HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut merged = token;
        // If catalog options contain a DLF OSS endpoint, override the standard OSS endpoint
        if let Some(dlf_oss_endpoint) = self.catalog_options.get("dlf.oss-endpoint") {
            if !dlf_oss_endpoint.trim().is_empty() {
                merged.insert("fs.oss.endpoint".to_string(), dlf_oss_endpoint.clone());
            }
        }
        merged
    }

    /// Merge token credentials into catalog options map for FileIO construction.
    fn merge_token_with_options(
        &self,
        token: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut merged = self.catalog_options.to_map().clone();
        // Token values override catalog options
        merged.extend(token.iter().map(|(k, v)| (k.clone(), v.clone())));
        // DLF OSS endpoint override
        if let Some(dlf_oss_endpoint) = self.catalog_options.get("dlf.oss-endpoint") {
            if !dlf_oss_endpoint.trim().is_empty() {
                merged.insert("fs.oss.endpoint".to_string(), dlf_oss_endpoint.clone());
            }
        }
        merged
    }
}
