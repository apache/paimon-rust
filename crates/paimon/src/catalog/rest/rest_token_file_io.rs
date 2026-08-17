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

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};

use crate::api::rest_api::RESTApi;
use crate::api::rest_util::RESTUtil;
use crate::catalog::Identifier;
use crate::common::{CatalogOptions, Options};
use crate::io::cache::LocalCache;
use crate::io::{FileIO, FileIOProvider};
use crate::Result;

use super::rest_token::RESTToken;

/// Safe time margin (in milliseconds) before token expiration to trigger refresh.
const TOKEN_EXPIRATION_SAFE_TIME_MILLIS: i64 = 3_600_000;
const OSS_ENDPOINT: &str = "fs.oss.endpoint";

/// A FileIO wrapper that refreshes data access tokens from the REST server.
#[derive(Debug)]
struct TokenState {
    token: RESTToken,
    file_io: FileIO,
}

pub struct RESTTokenFileIO {
    identifier: Identifier,
    path: String,
    catalog_options: Options,
    api: Arc<RESTApi>,
    state: RwLock<Option<TokenState>>,
    refresh_lock: Mutex<()>,
    local_cache: Option<Arc<LocalCache>>,
}

impl std::fmt::Debug for RESTTokenFileIO {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RESTTokenFileIO")
            .field("identifier", &self.identifier)
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl RESTTokenFileIO {
    pub(crate) fn new(
        identifier: Identifier,
        path: String,
        catalog_options: Options,
        api: Arc<RESTApi>,
        local_cache: Option<Arc<LocalCache>>,
    ) -> Self {
        Self {
            identifier,
            path,
            catalog_options,
            api,
            state: RwLock::new(None),
            refresh_lock: Mutex::new(()),
            local_cache,
        }
    }

    pub(crate) async fn build_file_io(self: &Arc<Self>) -> Result<FileIO> {
        let file_io = self.current_file_io().await?;
        Ok(file_io.with_provider(self.clone()))
    }

    async fn current_file_io(&self) -> Result<FileIO> {
        if let Some(file_io) = self.valid_file_io().await {
            return Ok(file_io);
        }

        let _refresh_guard = self.refresh_lock.lock().await;
        if let Some(file_io) = self.valid_file_io().await {
            return Ok(file_io);
        }

        let token = self.refresh_token().await?;
        let file_io = self.build_static_file_io(&token)?;
        *self.state.write().await = Some(TokenState {
            token,
            file_io: file_io.clone(),
        });
        Ok(file_io)
    }

    async fn valid_file_io(&self) -> Option<FileIO> {
        self.state
            .read()
            .await
            .as_ref()
            .filter(|state| !Self::is_token_expired(&state.token))
            .map(|state| state.file_io.clone())
    }

    fn build_static_file_io(&self, token: &RESTToken) -> Result<FileIO> {
        let merged_props = RESTUtil::merge(Some(self.catalog_options.to_map()), Some(&token.token));
        let mut builder = FileIO::from_path(&self.path)?.with_props(merged_props);
        if let Some(local_cache) = &self.local_cache {
            builder = builder.with_local_cache(local_cache.clone());
        }
        builder.build()
    }

    async fn refresh_token(&self) -> Result<RESTToken> {
        let response = self.api.load_table_token(&self.identifier).await?;
        let expires_at_millis =
            response
                .expires_at_millis
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!(
                        "Token response for table '{}' missing expires_at_millis",
                        self.identifier.full_name()
                    ),
                    source: None,
                })?;

        let merged_token = self.merge_token_with_catalog_options(response.token);
        Ok(RESTToken::new(merged_token, expires_at_millis))
    }

    fn is_token_expired(token: &RESTToken) -> bool {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        token.expire_at_millis - current_time < TOKEN_EXPIRATION_SAFE_TIME_MILLIS
    }

    fn merge_token_with_catalog_options(
        &self,
        token: HashMap<String, String>,
    ) -> HashMap<String, String> {
        let mut merged = token;
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
    async fn create(&self, path: &str) -> Result<(opendal::Operator, String)> {
        self.current_file_io().await?.create_static(path)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::extract::State;
    use axum::routing::get;
    use axum::{Json, Router};
    use bytes::Bytes;

    use super::*;
    use crate::api::GetTableTokenResponse;
    use crate::io::cache::create_local_cache;

    async fn token(State(requests): State<Arc<AtomicUsize>>) -> Json<GetTableTokenResponse> {
        let request = requests.fetch_add(1, Ordering::SeqCst);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let lifetime = if request == 0 {
            TOKEN_EXPIRATION_SAFE_TIME_MILLIS / 2
        } else {
            TOKEN_EXPIRATION_SAFE_TIME_MILLIS * 2
        };
        Json(GetTableTokenResponse {
            token: HashMap::new(),
            expires_at_millis: Some(now + lifetime),
        })
    }

    async fn token_api() -> (
        Options,
        Arc<RESTApi>,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let requests = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/v1/databases/database/tables/table/token", get(token))
            .with_state(requests.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut options = Options::new();
        options.set(CatalogOptions::URI, format!("http://{address}"));
        options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
        options.set(CatalogOptions::TOKEN, "test-token");
        let api = Arc::new(RESTApi::new(options.clone(), false).await.unwrap());
        (options, api, requests, server)
    }

    #[tokio::test]
    async fn test_token_file_io_keeps_catalog_local_cache() {
        let table_directory = tempfile::tempdir().unwrap();
        let (mut options, api, _, server) = token_api().await;
        options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");
        let local_cache = create_local_cache(&options).unwrap();
        let token_file_io = Arc::new(RESTTokenFileIO::new(
            Identifier::new("database", "table"),
            table_directory.path().to_string_lossy().into_owned(),
            options,
            api,
            local_cache,
        ));

        let file_io = token_file_io.build_file_io().await.unwrap();

        assert!(file_io.has_local_cache());
        server.abort();
    }

    #[tokio::test]
    async fn test_file_io_refreshes_expiring_token() {
        let table_directory = tempfile::tempdir().unwrap();
        let file_path = table_directory.path().join("data");
        let (options, api, requests, server) = token_api().await;
        let token_file_io = Arc::new(RESTTokenFileIO::new(
            Identifier::new("database", "table"),
            table_directory.path().to_string_lossy().into_owned(),
            options,
            api,
            None,
        ));

        let file_io = token_file_io.build_file_io().await.unwrap();
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        let file_io = Arc::new(file_io);
        let mut checks = Vec::new();
        for _ in 0..8 {
            let file_io = file_io.clone();
            let path = file_path.to_string_lossy().into_owned();
            checks.push(tokio::spawn(async move { file_io.exists(&path).await }));
        }
        for check in checks {
            assert!(!check.await.unwrap().unwrap());
        }
        assert_eq!(requests.load(Ordering::SeqCst), 2);

        file_io
            .new_output(file_path.to_string_lossy().as_ref())
            .unwrap()
            .write(Bytes::from_static(b"data"))
            .await
            .unwrap();
        assert_eq!(requests.load(Ordering::SeqCst), 2);

        let bytes = file_io
            .new_input(file_path.to_string_lossy().as_ref())
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(bytes, Bytes::from_static(b"data"));
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        server.abort();
    }
}
