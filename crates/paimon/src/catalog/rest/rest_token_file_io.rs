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
//! Unlike the previous implementation that only refreshed tokens during
//! `build_file_io()`, this implementation implements `FileIOProvider` and
//! checks token validity before each file operation, matching the Java
//! implementation behavior.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use moka::future::Cache;
use tokio::sync::{OnceCell, RwLock};

use crate::api::rest_api::RESTApi;
use crate::api::rest_util::RESTUtil;
use crate::catalog::Identifier;
use crate::common::{CatalogOptions, Options};
use crate::io::storage_oss::OSS_ENDPOINT;
use crate::io::{FileIO, FileIOProvider, FileStatus, InputFile, OutputFile};
use crate::{Error, Result};

use super::rest_token::RESTToken;

/// Safe time margin (in milliseconds) before token expiration to trigger refresh.
const TOKEN_EXPIRATION_SAFE_TIME_MILLIS: i64 = 3_600_000;

/// Maximum number of entries in the global FileIO cache.
const FILE_IO_CACHE_MAX_CAPACITY: u64 = 1000;
/// Time-to-live for cache entries in seconds (10 hours).
const FILE_IO_CACHE_TTL_SECS: u64 = 10 * 60 * 60;

/// Global static FileIO cache, similar to Java's Caffeine cache.
///
/// This cache stores FileIO instances keyed by their corresponding RESTToken.
/// Features:
/// - max_capacity: 1000 entries
/// - time_to_live: 10 hours (entries expire after this duration)
/// - thread-safe via moka's internal synchronization
static FILE_IO_CACHE: OnceLock<Cache<RESTToken, FileIO>> = OnceLock::new();

/// Get the global FileIO cache, initializing it if necessary.
fn get_file_io_cache() -> &'static Cache<RESTToken, FileIO> {
    FILE_IO_CACHE.get_or_init(|| {
        Cache::builder()
            .max_capacity(FILE_IO_CACHE_MAX_CAPACITY)
            .time_to_live(Duration::from_secs(FILE_IO_CACHE_TTL_SECS))
            .build()
    })
}

/// A FileIO wrapper that supports getting data access tokens from a REST Server.
///
/// This struct handles:
/// - Token caching with expiration detection
/// - Automatic token refresh via `RESTApi::load_table_token`
/// - Merging token credentials into catalog options to build the underlying `FileIO`
/// - FileIO caching based on token to avoid rebuilding FileIO unnecessarily
pub struct RESTTokenFileIO {
    /// Table identifier for token requests.
    identifier: Identifier,
    /// Table path (e.g. "oss://bucket/warehouse/db.db/table").
    path: String,
    /// Catalog options used to build FileIO and create RESTApi.
    catalog_options: Options,
    /// Lazily-initialized REST API client for token refresh.
    /// Created on first token refresh and reused for subsequent refreshes.
    api: OnceCell<RESTApi>,
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
            api: OnceCell::new(),
            token: RwLock::new(None),
        }
    }

    /// Get or create a valid FileIO instance.
    ///
    /// This method:
    /// 1. Refreshes the token if expired or not yet obtained.
    /// 2. Returns cached FileIO from global cache if token exists.
    /// 3. Otherwise creates a new FileIO with the new token and caches it.
    async fn get_file_io(&self) -> Result<FileIO> {
        // Ensure token is fresh (this will update self.token if needed)
        self.try_to_refresh_token().await?;

        // Get current token
        let token_guard = self.token.read().await;
        let current_token = token_guard
            .as_ref()
            .ok_or_else(|| Error::DataInvalid {
                message: "Token should be available after refresh".to_string(),
                source: None,
            })?
            .clone();

        // Drop the read lock before checking cache
        drop(token_guard);

        // Get or create FileIO from global cache (thread-safe, prevents duplicate creation)
        let cache = get_file_io_cache();
        let current_token_clone = current_token.clone();
        let path = self.path.clone();
        let catalog_options = self.catalog_options.clone();

        let file_io = cache
            .try_get_with(current_token, async move {
                let merged_props = RESTUtil::merge(
                    Some(catalog_options.to_map()),
                    Some(&current_token_clone.token),
                );
                let mut builder = FileIO::from_path(&path)?;
                builder = builder.with_props(merged_props);
                builder.build()
            })
            .await
            .map_err(|e| Error::DataInvalid {
                message: format!("Failed to create FileIO: {}", e),
                source: Some(Box::new(std::io::Error::other(e.to_string()))),
            })?;

        Ok(file_io)
    }

    /// Try to refresh the token if it is expired or not yet obtained.
    async fn try_to_refresh_token(&self) -> Result<()> {
        // Fast path: check if token is still valid under read lock
        {
            let token_guard = self.token.read().await;
            if let Some(token) = token_guard.as_ref() {
                if !Self::is_token_expired(token) {
                    return Ok(());
                }
            }
        }

        // Slow path: acquire write lock and check again
        {
            let token_guard = self.token.write().await;
            if let Some(token) = token_guard.as_ref() {
                if !Self::is_token_expired(token) {
                    return Ok(());
                }
            }
        }
        // Write lock released before .await to avoid potential deadlock

        // Refresh the token WITHOUT holding the lock
        let new_token = self.refresh_token().await?;

        // Acquire write lock again to update
        let mut token_guard = self.token.write().await;
        *token_guard = Some(new_token);
        Ok(())
    }

    /// Refresh the token by calling `RESTApi::load_table_token`.
    ///
    /// Lazily creates a `RESTApi` instance on first call and reuses it
    /// for subsequent refreshes.
    async fn refresh_token(&self) -> Result<RESTToken> {
        let api = self
            .api
            .get_or_try_init(|| async { RESTApi::new(self.catalog_options.clone(), false).await })
            .await?;

        let response = api.load_table_token(&self.identifier).await?;

        let expires_at_millis = response
            .expires_at_millis
            .ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "Token response for table '{}' missing expires_at_millis",
                    self.identifier.full_name()
                ),
                source: None,
            })?;

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
    fn merge_token_with_catalog_options(
        &self,
        token: HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut merged = token;
        // If catalog options contain a DLF OSS endpoint, override the standard OSS endpoint
        if let Some(dlf_oss_endpoint) = self.catalog_options.get(CatalogOptions::DLF_OSS_ENDPOINT) {
            if !dlf_oss_endpoint.trim().is_empty() {
                merged.insert(OSS_ENDPOINT.to_string(), dlf_oss_endpoint.clone());
            }
        }
        merged
    }
}

#[async_trait::async_trait]
impl FileIOProvider for RESTTokenFileIO {
    async fn new_input(&self, path: &str) -> Result<InputFile> {
        let file_io = self.get_file_io().await?;
        file_io.new_input(path).await
    }

    async fn new_output(&self, path: &str) -> Result<OutputFile> {
        let file_io = self.get_file_io().await?;
        file_io.new_output(path).await
    }

    async fn get_status(&self, path: &str) -> Result<FileStatus> {
        let file_io = self.get_file_io().await?;
        file_io.get_status(path).await
    }

    async fn list_status(&self, path: &str) -> Result<Vec<FileStatus>> {
        let file_io = self.get_file_io().await?;
        file_io.list_status(path).await
    }

    async fn exists(&self, path: &str) -> Result<bool> {
        let file_io = self.get_file_io().await?;
        file_io.exists(path).await
    }

    async fn delete_file(&self, path: &str) -> Result<()> {
        let file_io = self.get_file_io().await?;
        file_io.delete_file(path).await
    }

    async fn delete_dir(&self, path: &str) -> Result<()> {
        let file_io = self.get_file_io().await?;
        file_io.delete_dir(path).await
    }

    async fn mkdirs(&self, path: &str) -> Result<()> {
        let file_io = self.get_file_io().await?;
        file_io.mkdirs(path).await
    }

    async fn rename(&self, src: &str, dst: &str) -> Result<()> {
        let file_io = self.get_file_io().await?;
        file_io.rename(src, dst).await
    }
}

impl std::fmt::Debug for RESTTokenFileIO {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RESTTokenFileIO")
            .field("identifier", &self.identifier)
            .field("path", &self.path)
            .finish()
    }
}
