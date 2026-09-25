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

//! DLF Authentication Provider for Alibaba Cloud Data Lake Formation.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::base::{AuthProvider, RESTAuthParameter, AUTHORIZATION_HEADER_KEY};
use super::dlf_signer::{DLFRequestSigner, DLFSignerFactory};
use crate::common::{CatalogOptions, Options};
use crate::error::Error;
use crate::Result;

// ============================================================================
// DLF Token and Token Loader
// ============================================================================

/// DLF Token containing access credentials for Alibaba Cloud Data Lake Formation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DLFToken {
    /// Access key ID for Alibaba Cloud.
    #[serde(rename = "AccessKeyId")]
    pub access_key_id: String,
    /// Access key secret for Alibaba Cloud.
    #[serde(rename = "AccessKeySecret")]
    pub access_key_secret: String,
    /// Security token for temporary credentials (optional).
    #[serde(rename = "SecurityToken")]
    pub security_token: Option<String>,
    /// Expiration timestamp in milliseconds (PyPaimon compatibility).
    #[serde(rename = "ExpirationAt", default, skip_serializing)]
    pub expiration_at_millis: Option<i64>,
    /// Expiration time string (ISO 8601 format).
    #[serde(
        rename = "Expiration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub expiration: Option<String>,
}

impl DLFToken {
    /// Token date format for parsing expiration.
    const TOKEN_DATE_FORMAT: &'static str = "%Y-%m-%dT%H:%M:%SZ";

    /// Create a new DLFToken.
    ///
    /// # Arguments
    /// * `access_key_id` - The access key ID
    /// * `access_key_secret` - The access key secret
    /// * `security_token` - Optional security token
    /// * `expiration` - Optional expiration time string (ISO 8601 format)
    /// * `expiration_at_millis` - Optional expiration timestamp in milliseconds.
    ///   If provided, this value is used directly. Otherwise, it will be parsed from `expiration`.
    pub fn new(
        access_key_id: impl Into<String>,
        access_key_secret: impl Into<String>,
        security_token: Option<String>,
        expiration_at_millis: Option<i64>,
        expiration: Option<String>,
    ) -> Self {
        let access_key_id = access_key_id.into();
        let access_key_secret = access_key_secret.into();

        // Use provided expiration_at_millis, or parse from expiration string if not provided
        let expiration_at_millis = expiration_at_millis.or_else(|| {
            expiration
                .as_deref()
                .and_then(Self::parse_expiration_to_millis)
        });

        Self {
            access_key_id,
            access_key_secret,
            security_token,
            expiration_at_millis,
            expiration,
        }
    }

    /// Create a DLFToken from configuration options.
    pub fn from_options(options: &Options) -> Option<Self> {
        let access_key_id = options.get(CatalogOptions::DLF_ACCESS_KEY_ID)?.clone();
        let access_key_secret = options.get(CatalogOptions::DLF_ACCESS_KEY_SECRET)?.clone();
        let security_token = options
            .get(CatalogOptions::DLF_ACCESS_SECURITY_TOKEN)
            .cloned();

        Some(Self::new(
            access_key_id,
            access_key_secret,
            security_token,
            None,
            None,
        ))
    }

    /// Parse expiration string to milliseconds timestamp.
    pub fn parse_expiration_to_millis(expiration: &str) -> Option<i64> {
        let datetime = chrono::NaiveDateTime::parse_from_str(expiration, Self::TOKEN_DATE_FORMAT)
            .ok()?
            .and_utc();
        Some(datetime.timestamp_millis())
    }

    fn from_json(token_json: &str) -> Result<Self> {
        let mut token: Self = serde_json::from_str(token_json).map_err(|_| Error::DataInvalid {
            message: "Failed to parse token JSON".to_string(),
            source: None,
        })?;
        if token.access_key_id.trim().is_empty() || token.access_key_secret.trim().is_empty() {
            return Err(Error::DataInvalid {
                message: "DLF token access key ID and secret must not be empty".to_string(),
                source: None,
            });
        }
        if let Some(expiration) = token.expiration.as_deref() {
            token.expiration_at_millis =
                Some(Self::parse_expiration_to_millis(expiration).ok_or_else(|| {
                    Error::DataInvalid {
                        message: "Failed to parse token Expiration".to_string(),
                        source: None,
                    }
                })?);
        }
        Ok(token)
    }
}
/// Trait for DLF token loaders.
#[async_trait]
pub trait DLFTokenLoader: Send + Sync {
    /// Load a DLF token.
    async fn load_token(&self) -> Result<DLFToken>;

    /// Get a description of the loader.
    fn description(&self) -> &str;
}

/// Loads DLF credentials from a local JSON file.
pub struct DLFLocalFileTokenLoader {
    token_file_path: String,
}

impl DLFLocalFileTokenLoader {
    const DEFAULT_MAX_RETRIES: u32 = 5;

    /// Create a local-file token loader.
    pub fn new(token_file_path: impl Into<String>) -> Result<Self> {
        let token_file_path = token_file_path.into();
        if token_file_path.trim().is_empty() {
            return Err(Error::ConfigInvalid {
                message: "dlf.token-path must not be empty".to_string(),
            });
        }
        Ok(Self { token_file_path })
    }

    async fn read_token(&self, max_retries: u32) -> Result<DLFToken> {
        let mut last_error = None;
        for attempt in 0..max_retries.max(1) {
            let result = match tokio::fs::read_to_string(&self.token_file_path).await {
                Ok(token_json) => DLFToken::from_json(&token_json),
                Err(error) => Err(Error::DataInvalid {
                    message: format!("Failed to read DLF token file: {}", self.token_file_path),
                    source: Some(Box::new(error)),
                }),
            };
            match result {
                Ok(token) => return Ok(token),
                Err(error) => last_error = Some(error),
            }
            if attempt + 1 < max_retries {
                tokio::time::sleep(std::time::Duration::from_secs(u64::from(attempt + 1))).await;
            }
        }
        Err(last_error.expect("at least one token load attempt"))
    }
}

#[async_trait]
impl DLFTokenLoader for DLFLocalFileTokenLoader {
    async fn load_token(&self) -> Result<DLFToken> {
        self.read_token(Self::DEFAULT_MAX_RETRIES).await
    }

    fn description(&self) -> &str {
        &self.token_file_path
    }
}

/// DLF ECS Token Loader.
///
/// Loads DLF tokens from ECS metadata service.
///
/// This implementation mirrors the Python DLFECSTokenLoader class,
/// using class-level HTTP client for connection reuse and retry logic.
pub struct DLFECSTokenLoader {
    ecs_metadata_url: String,
    role_name: Option<String>,
    http_client: TokenHTTPClient,
}

impl DLFECSTokenLoader {
    /// Create a new DLFECSTokenLoader.
    ///
    /// # Arguments
    /// * `ecs_metadata_url` - ECS metadata service URL
    /// * `role_name` - Optional role name. If None, will be fetched from metadata service
    pub fn new(ecs_metadata_url: impl Into<String>, role_name: Option<String>) -> Self {
        Self {
            ecs_metadata_url: ecs_metadata_url.into(),
            role_name,
            http_client: TokenHTTPClient::new(),
        }
    }

    /// Get the role name from ECS metadata service.
    async fn get_role(&self) -> Result<String> {
        self.http_client.get(&self.ecs_metadata_url).await
    }

    /// Get the token from ECS metadata service.
    async fn get_token(&self, url: &str) -> Result<DLFToken> {
        let token_json = self.http_client.get(url).await?;
        DLFToken::from_json(&token_json)
    }

    /// Build the token URL from base URL and role name.
    fn build_token_url(&self, role_name: &str) -> String {
        let base_url = self.ecs_metadata_url.trim_end_matches('/');
        format!("{base_url}/{role_name}")
    }
}

#[async_trait]
impl DLFTokenLoader for DLFECSTokenLoader {
    async fn load_token(&self) -> Result<DLFToken> {
        let role_name = match &self.role_name {
            Some(name) => name.clone(),
            None => {
                // Fetch role name from metadata service
                self.get_role().await?
            }
        };

        // Build token URL
        let token_url = self.build_token_url(&role_name);

        // Get token
        self.get_token(&token_url).await
    }

    fn description(&self) -> &str {
        &self.ecs_metadata_url
    }
}
/// Factory for creating DLF token loaders.
pub struct DLFTokenLoaderFactory;

impl DLFTokenLoaderFactory {
    /// Create a token loader based on options.
    pub fn create_token_loader(options: &Options) -> Result<Option<Arc<dyn DLFTokenLoader>>> {
        match options
            .get(CatalogOptions::DLF_TOKEN_LOADER)
            .map(String::as_str)
        {
            Some("ecs") => {
                let ecs_metadata_url = options
                    .get(CatalogOptions::DLF_TOKEN_ECS_METADATA_URL)
                    .cloned()
                    .unwrap_or_else(|| {
                        "http://100.100.100.200/latest/meta-data/Ram/security-credentials/"
                            .to_string()
                    });
                let role_name = options
                    .get(CatalogOptions::DLF_TOKEN_ECS_ROLE_NAME)
                    .cloned();
                Ok(Some(Arc::new(DLFECSTokenLoader::new(
                    ecs_metadata_url,
                    role_name,
                ))))
            }
            Some("local_file") => Self::create_local_file_loader(options).map(Some),
            Some(loader) => Err(Error::ConfigInvalid {
                message: format!("Unknown DLF token loader: {loader}"),
            }),
            None if options.get(CatalogOptions::DLF_TOKEN_PATH).is_some() => {
                Self::create_local_file_loader(options).map(Some)
            }
            None => Ok(None),
        }
    }

    fn create_local_file_loader(options: &Options) -> Result<Arc<dyn DLFTokenLoader>> {
        let token_path =
            options
                .get(CatalogOptions::DLF_TOKEN_PATH)
                .ok_or_else(|| Error::ConfigInvalid {
                    message: "dlf.token-path is required for local_file token loading".to_string(),
                })?;
        Ok(Arc::new(DLFLocalFileTokenLoader::new(token_path)?))
    }
}
// ============================================================================
// DLF Auth Provider
// ============================================================================

/// Token expiration safe time in milliseconds (1 hour).
/// Token will be refreshed if it expires within this time.
const TOKEN_EXPIRATION_SAFE_TIME_MILLIS: i64 = 3_600_000;

/// DLF Authentication Provider for Alibaba Cloud Data Lake Formation.
///
/// This provider implements authentication for Alibaba Cloud DLF service,
/// supporting both VPC endpoints (DLF4-HMAC-SHA256) and public endpoints
/// (ROA v2 HMAC-SHA1).
pub struct DLFAuthProvider {
    uri: String,
    token: tokio::sync::Mutex<Option<DLFToken>>,
    token_loader: Option<Arc<dyn DLFTokenLoader>>,
    signer: Box<dyn DLFRequestSigner>,
}

impl DLFAuthProvider {
    /// Create a new DLFAuthProvider.
    ///
    /// # Arguments
    /// * `uri` - The DLF service URI
    /// * `token` - Optional DLF token containing access credentials
    /// * `token_loader` - Optional token loader for dynamic token retrieval
    ///
    /// # Errors
    /// Returns an error if both `token` and `token_loader` are `None`.
    pub fn new(
        uri: impl Into<String>,
        region: impl Into<String>,
        signing_algorithm: impl Into<String>,
        token: Option<DLFToken>,
        token_loader: Option<Arc<dyn DLFTokenLoader>>,
    ) -> Result<Self> {
        if token.is_none() && token_loader.is_none() {
            return Err(Error::ConfigInvalid {
                message: "Either token or token_loader must be provided".to_string(),
            });
        }

        let uri = uri.into();
        let region = region.into();
        let signing_algorithm = signing_algorithm.into();
        let signer = DLFSignerFactory::create_signer(&signing_algorithm, &region);

        Ok(Self {
            uri,
            token: tokio::sync::Mutex::new(token),
            token_loader,
            signer,
        })
    }

    /// Get or refresh the token.
    ///
    /// If token_loader is configured, this method will:
    /// - Load a new token if current token is None
    /// - Refresh the token if it's about to expire (within TOKEN_EXPIRATION_SAFE_TIME_MILLIS)
    async fn get_or_refresh_token(&self) -> Result<DLFToken> {
        let mut token_guard = self.token.lock().await;

        if let Some(loader) = &self.token_loader {
            let need_reload = match &*token_guard {
                None => true,
                Some(token) => match token.expiration_at_millis {
                    Some(expiration_at_millis) => {
                        let now = chrono::Utc::now().timestamp_millis();
                        expiration_at_millis - now < TOKEN_EXPIRATION_SAFE_TIME_MILLIS
                    }
                    None => false,
                },
            };

            if need_reload {
                let new_token = loader.load_token().await?;
                *token_guard = Some(new_token);
            }
        }

        token_guard.clone().ok_or_else(|| Error::DataInvalid {
            message: "Either token or token_loader must be provided".to_string(),
            source: None,
        })
    }

    /// Extract host from URI.
    fn extract_host(uri: &str) -> String {
        let without_protocol = uri
            .strip_prefix("https://")
            .or_else(|| uri.strip_prefix("http://"))
            .unwrap_or(uri);

        let path_index = without_protocol.find('/').unwrap_or(without_protocol.len());
        without_protocol[..path_index].to_string()
    }
}

#[async_trait]
impl AuthProvider for DLFAuthProvider {
    async fn merge_auth_header(
        &self,
        mut base_header: HashMap<String, String>,
        rest_auth_parameter: &RESTAuthParameter,
    ) -> crate::Result<HashMap<String, String>> {
        // Get token (will auto-refresh if needed via token_loader)
        let token = self.get_or_refresh_token().await?;

        let now = Utc::now();
        let host = Self::extract_host(&self.uri);

        // Generate signature headers
        let sign_headers = self.signer.sign_headers(
            rest_auth_parameter.data.as_deref(),
            &now,
            token.security_token.as_deref(),
            &host,
        );

        // Generate authorization header
        let authorization =
            self.signer
                .authorization(rest_auth_parameter, &token, &host, &sign_headers);

        // Merge all headers
        base_header.extend(sign_headers);
        base_header.insert(AUTHORIZATION_HEADER_KEY.to_string(), authorization);

        Ok(base_header)
    }
}

// ============================================================================
// DLF Token Loader Implementation
// ============================================================================

/// HTTP client for token loading with retry and timeout configuration.
struct TokenHTTPClient {
    max_retries: u32,
    client: Client,
}

impl TokenHTTPClient {
    /// Create a new HTTP client with default settings.
    fn new() -> Self {
        let connect_timeout = std::time::Duration::from_secs(180); // 3 minutes
        let read_timeout = std::time::Duration::from_secs(180); // 3 minutes

        let client = Client::builder()
            .timeout(read_timeout)
            .connect_timeout(connect_timeout)
            .build()
            .expect("Failed to create HTTP client");

        Self {
            max_retries: 3,
            client,
        }
    }

    /// Perform HTTP GET request with retry logic.
    async fn get(&self, url: &str) -> Result<String> {
        let mut last_error = String::new();
        for attempt in 0..self.max_retries {
            match self.client.get(url).send().await {
                Ok(response) if response.status().is_success() => {
                    return response.text().await.map_err(|e| Error::DataInvalid {
                        message: format!("Failed to read response: {e}"),
                        source: None,
                    });
                }
                Ok(response) => {
                    last_error = format!("HTTP error: {}", response.status());
                }
                Err(e) => {
                    last_error = format!("Request failed: {e}");
                }
            }

            if attempt < self.max_retries - 1 {
                // Exponential backoff
                let delay = std::time::Duration::from_millis(100 * 2u64.pow(attempt));
                tokio::time::sleep(delay).await;
            }
        }

        Err(Error::DataInvalid {
            message: last_error,
            source: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_host() {
        let uri = "http://dlf-abcdfgerrf.net/api/v1";
        let host = DLFAuthProvider::extract_host(uri);
        assert_eq!(host, "dlf-abcdfgerrf.net");
    }

    #[test]
    fn test_extract_host_no_path() {
        let uri = "https://dlf.cn-abcdfgerrf.aliyuncs.com";
        let host = DLFAuthProvider::extract_host(uri);
        assert_eq!(host, "dlf.cn-abcdfgerrf.aliyuncs.com");
    }

    #[test]
    fn test_dlf_token_from_options() {
        let mut options = Options::new();
        options.set(CatalogOptions::DLF_ACCESS_KEY_ID, "test_key_id");
        options.set(CatalogOptions::DLF_ACCESS_KEY_SECRET, "test_key_secret");
        options.set(
            CatalogOptions::DLF_ACCESS_SECURITY_TOKEN,
            "test_security_token",
        );

        let token = DLFToken::from_options(&options).unwrap();
        assert_eq!(token.access_key_id, "test_key_id");
        assert_eq!(token.access_key_secret, "test_key_secret");
        assert_eq!(
            token.security_token,
            Some("test_security_token".to_string())
        );
    }

    #[test]
    fn test_dlf_token_missing_credentials() {
        let options = Options::new();
        assert!(DLFToken::from_options(&options).is_none());
    }

    #[test]
    fn test_parse_expiration() {
        let expiration = "2024-12-31T23:59:59Z";
        let millis = DLFToken::parse_expiration_to_millis(expiration);
        assert!(millis.is_some());
    }

    #[tokio::test]
    async fn test_local_file_token_loader_formats() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token.json");
        let loader = DLFLocalFileTokenLoader::new(path.display().to_string()).unwrap();

        tokio::fs::write(
            &path,
            r#"{"AccessKeyId":"ak-1","AccessKeySecret":"sk-1","SecurityToken":"sts-1"}"#,
        )
        .await
        .unwrap();
        let token = loader.read_token(1).await.unwrap();
        assert_eq!(token.access_key_id, "ak-1");
        assert_eq!(token.security_token.as_deref(), Some("sts-1"));
        assert_eq!(token.expiration_at_millis, None);

        tokio::fs::write(
            &path,
            r#"{"AccessKeyId":"ak-2","AccessKeySecret":"sk-2","ExpirationAt":123}"#,
        )
        .await
        .unwrap();
        let token = loader.read_token(1).await.unwrap();
        assert_eq!(token.security_token, None);
        assert_eq!(token.expiration_at_millis, Some(123));

        tokio::fs::write(
            &path,
            r#"{"AccessKeyId":"ak-3","AccessKeySecret":"sk-3","Expiration":"2099-01-01T00:00:00Z"}"#,
        )
        .await
        .unwrap();
        let token = loader.read_token(1).await.unwrap();
        assert_eq!(
            token.expiration_at_millis,
            DLFToken::parse_expiration_to_millis("2099-01-01T00:00:00Z")
        );

        tokio::fs::write(
            &path,
            r#"{"AccessKeyId":"ak-4","AccessKeySecret":"sk-4","ExpirationAt":123,"Expiration":"2099-01-01T00:00:00Z"}"#,
        )
        .await
        .unwrap();
        let token = loader.read_token(1).await.unwrap();
        assert_eq!(
            token.expiration_at_millis,
            DLFToken::parse_expiration_to_millis("2099-01-01T00:00:00Z")
        );
    }

    #[tokio::test]
    async fn test_local_file_token_loader_errors_do_not_leak_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token.json");
        let loader = DLFLocalFileTokenLoader::new(path.display().to_string()).unwrap();

        let error = loader.read_token(1).await.unwrap_err();
        assert!(error.to_string().contains("Failed to read DLF token file"));

        let secret = "SECRET_MUST_NOT_LEAK";
        tokio::fs::write(
            &path,
            format!(r#"{{"AccessKeyId":"ak","AccessKeySecret":"{secret}" invalid}}"#),
        )
        .await
        .unwrap();
        let error = loader.read_token(1).await.unwrap_err();
        assert!(error.to_string().contains("Failed to parse token JSON"));
        assert!(!error.to_string().contains(secret));

        tokio::fs::write(&path, r#"{"AccessKeyId":"ak"}"#)
            .await
            .unwrap();
        let error = loader.read_token(1).await.unwrap_err();
        assert!(error.to_string().contains("Failed to parse token JSON"));

        tokio::fs::write(
            &path,
            r#"{"AccessKeyId":"","AccessKeySecret":"sk","Expiration":"invalid"}"#,
        )
        .await
        .unwrap();
        let error = loader.read_token(1).await.unwrap_err();
        assert!(error.to_string().contains("must not be empty"));

        tokio::fs::write(
            &path,
            r#"{"AccessKeyId":"ak","AccessKeySecret":"sk","Expiration":"invalid"}"#,
        )
        .await
        .unwrap();
        let error = loader.read_token(1).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("Failed to parse token Expiration"));

        tokio::fs::write(
            &path,
            r#"{"AccessKeyId":"ak","AccessKeySecret":"sk","ExpirationAt":123,"Expiration":"invalid"}"#,
        )
        .await
        .unwrap();
        let error = loader.read_token(1).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("Failed to parse token Expiration"));
    }

    #[test]
    fn test_local_file_token_loader_factory_validation() {
        let mut options = Options::new();
        options.set(CatalogOptions::DLF_TOKEN_LOADER, "local_file");
        assert!(DLFTokenLoaderFactory::create_token_loader(&options)
            .err()
            .unwrap()
            .to_string()
            .contains("dlf.token-path is required"));

        options.set(CatalogOptions::DLF_TOKEN_PATH, " ");
        assert!(DLFTokenLoaderFactory::create_token_loader(&options)
            .err()
            .unwrap()
            .to_string()
            .contains("must not be empty"));

        options.set(CatalogOptions::DLF_TOKEN_LOADER, "unknown");
        assert!(DLFTokenLoaderFactory::create_token_loader(&options)
            .err()
            .unwrap()
            .to_string()
            .contains("Unknown DLF token loader"));

        options.remove(CatalogOptions::DLF_TOKEN_LOADER);
        options.set(CatalogOptions::DLF_TOKEN_PATH, "/tmp/token.json");
        assert_eq!(
            DLFTokenLoaderFactory::create_token_loader(&options)
                .unwrap()
                .unwrap()
                .description(),
            "/tmp/token.json"
        );
    }

    #[tokio::test]
    async fn test_local_file_provider_refreshes_expiring_token() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token.json");
        let expires_soon = (Utc::now()
            + chrono::Duration::milliseconds(TOKEN_EXPIRATION_SAFE_TIME_MILLIS / 2))
        .format(DLFToken::TOKEN_DATE_FORMAT);
        tokio::fs::write(
            &path,
            format!(
                r#"{{"AccessKeyId":"old-ak","AccessKeySecret":"old-sk","Expiration":"{expires_soon}"}}"#
            ),
        )
        .await
        .unwrap();
        let loader = Arc::new(DLFLocalFileTokenLoader::new(path.display().to_string()).unwrap());
        let provider = DLFAuthProvider::new(
            "https://dlf.cn-hangzhou.aliyuncs.com",
            "cn-hangzhou",
            "default",
            None,
            Some(loader),
        )
        .unwrap();

        let auth = RESTAuthParameter::new("GET", "/test", None, HashMap::new());
        let headers = provider
            .merge_auth_header(HashMap::new(), &auth)
            .await
            .unwrap();
        assert!(headers[AUTHORIZATION_HEADER_KEY].contains("old-ak"));
        let expires_later = (Utc::now()
            + chrono::Duration::milliseconds(TOKEN_EXPIRATION_SAFE_TIME_MILLIS * 2))
        .format(DLFToken::TOKEN_DATE_FORMAT);
        tokio::fs::write(
            &path,
            format!(
                r#"{{"AccessKeyId":"new-ak","AccessKeySecret":"new-sk","Expiration":"{expires_later}"}}"#
            ),
        )
        .await
        .unwrap();
        let headers = provider
            .merge_auth_header(HashMap::new(), &auth)
            .await
            .unwrap();
        assert!(headers[AUTHORIZATION_HEADER_KEY].contains("new-ak"));
        tokio::fs::remove_file(path).await.unwrap();
        let headers = provider
            .merge_auth_header(HashMap::new(), &auth)
            .await
            .unwrap();
        assert!(headers[AUTHORIZATION_HEADER_KEY].contains("new-ak"));
    }

    struct RotatingTokenLoader {
        requests: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl DLFTokenLoader for RotatingTokenLoader {
        async fn load_token(&self) -> Result<DLFToken> {
            use std::sync::atomic::Ordering;

            let request = self.requests.fetch_add(1, Ordering::SeqCst);
            let lifetime = if request == 0 {
                TOKEN_EXPIRATION_SAFE_TIME_MILLIS / 2
            } else {
                TOKEN_EXPIRATION_SAFE_TIME_MILLIS * 2
            };
            Ok(DLFToken::new(
                format!("key-{request}"),
                "secret",
                None,
                Some(Utc::now().timestamp_millis() + lifetime),
                None,
            ))
        }

        fn description(&self) -> &str {
            "test"
        }
    }

    #[tokio::test]
    async fn test_refreshes_expiring_loaded_token() {
        use std::sync::atomic::Ordering;

        let loader = Arc::new(RotatingTokenLoader {
            requests: std::sync::atomic::AtomicUsize::new(0),
        });
        let provider = DLFAuthProvider::new(
            "https://dlf.cn-hangzhou.aliyuncs.com",
            "cn-hangzhou",
            "default",
            None,
            Some(loader.clone()),
        )
        .unwrap();

        assert_eq!(
            provider.get_or_refresh_token().await.unwrap().access_key_id,
            "key-0"
        );
        assert_eq!(
            provider.get_or_refresh_token().await.unwrap().access_key_id,
            "key-1"
        );
        assert_eq!(
            provider.get_or_refresh_token().await.unwrap().access_key_id,
            "key-1"
        );
        assert_eq!(loader.requests.load(Ordering::SeqCst), 2);
    }
}
