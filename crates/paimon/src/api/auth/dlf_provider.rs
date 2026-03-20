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

use chrono::Utc;
use serde::{Deserialize, Serialize};

use super::base::{AuthProvider, RESTAuthParameter};
use super::dlf_signer::{DLFRequestSigner, DLFSignerFactory};
use crate::common::{CatalogOptions, Options};

// ============================================================================
// DLF Token
// ============================================================================

/// DLF Token containing access credentials for Alibaba Cloud Data Lake Formation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DLFToken {
    /// Access key ID for Alibaba Cloud.
    pub access_key_id: String,
    /// Access key secret for Alibaba Cloud.
    pub access_key_secret: String,
    /// Security token for temporary credentials (optional).
    pub security_token: Option<String>,
    /// Expiration time string (ISO 8601 format).
    pub expiration: Option<String>,
    /// Expiration timestamp in milliseconds.
    #[serde(default)]
    pub expiration_at_millis: Option<i64>,
}

impl DLFToken {
    /// Token date format for parsing expiration.
    const TOKEN_DATE_FORMAT: &'static str = "%Y-%m-%dT%H:%M:%SZ";

    /// Create a new DLFToken.
    pub fn new(
        access_key_id: impl Into<String>,
        access_key_secret: impl Into<String>,
        security_token: Option<String>,
        expiration: Option<String>,
    ) -> Self {
        let access_key_id = access_key_id.into();
        let access_key_secret = access_key_secret.into();

        let expiration_at_millis = expiration
            .as_ref()
            .and_then(|exp| Self::parse_expiration_to_millis(exp));

        Self {
            access_key_id,
            access_key_secret,
            security_token,
            expiration,
            expiration_at_millis,
        }
    }

    /// Create a DLFToken from configuration options.
    pub fn from_options(options: &Options) -> Option<Self> {
        let access_key_id = options.get(CatalogOptions::DLF_ACCESS_KEY_ID)?;
        let access_key_secret = options.get(CatalogOptions::DLF_ACCESS_KEY_SECRET)?;
        let security_token = options
            .get(CatalogOptions::DLF_ACCESS_SECURITY_TOKEN)
            .cloned();

        Some(Self::new(
            access_key_id.clone(),
            access_key_secret.clone(),
            security_token,
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

    /// Check if the token is expired or about to expire.
    #[allow(dead_code)]
    pub fn is_expired(&self, safe_time_millis: i64) -> bool {
        if let Some(expiration) = self.expiration_at_millis {
            let now = Utc::now().timestamp_millis();
            expiration - now < safe_time_millis
        } else {
            false
        }
    }
}

// ============================================================================
// DLF Auth Provider
// ============================================================================

/// DLF Authorization header key.
const DLF_AUTHORIZATION_HEADER_KEY: &str = "Authorization";

/// DLF Authentication Provider for Alibaba Cloud Data Lake Formation.
///
/// This provider implements authentication for Alibaba Cloud DLF service,
/// supporting both VPC endpoints (DLF4-HMAC-SHA256) and public endpoints
/// (ROA v2 HMAC-SHA1).
pub struct DLFAuthProvider {
    uri: String,
    region: String,
    signing_algorithm: String,
    token: Option<DLFToken>,
    signer: Box<dyn DLFRequestSigner>,
}

impl DLFAuthProvider {
    /// Create a new DLFAuthProvider.
    ///
    /// # Arguments
    /// * `uri` - The DLF service URI
    /// * `region` - The DLF region (e.g., "cn-hangzhou")
    /// * `signing_algorithm` - The signing algorithm ("default" or "openapi")
    /// * `token` - The DLF token containing access credentials
    pub fn new(
        uri: impl Into<String>,
        region: impl Into<String>,
        signing_algorithm: impl Into<String>,
        token: DLFToken,
    ) -> Self {
        let uri = uri.into();
        let region = region.into();
        let signing_algorithm = signing_algorithm.into();
        let signer = DLFSignerFactory::create_signer(&signing_algorithm, &region);

        Self {
            uri,
            region,
            signing_algorithm,
            token: Some(token),
            signer,
        }
    }

    /// Create a DLFAuthProvider from options.
    ///
    /// # Arguments
    /// * `options` - Configuration options containing DLF credentials
    ///
    /// # Returns
    /// A new DLFAuthProvider instance, or None if required options are missing.
    pub fn from_options(options: &Options) -> Option<Self> {
        let uri = options.get(CatalogOptions::URI)?.clone();

        // Get region from options or parse from URI
        let region = options
            .get(CatalogOptions::DLF_REGION)
            .cloned()
            .or_else(|| DLFSignerFactory::parse_region_from_uri(Some(&uri)))?;

        // Get signing algorithm from options or auto-detect from URI
        let signing_algorithm = options
            .get(CatalogOptions::DLF_SIGNING_ALGORITHM)
            .map(|s| s.as_str())
            .filter(|s| *s != "default")
            .unwrap_or_else(|| DLFSignerFactory::parse_signing_algo_from_uri(Some(&uri)))
            .to_string();

        // Get token from options
        let token = DLFToken::from_options(options)?;

        Some(Self::new(uri, region, signing_algorithm, token))
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

impl AuthProvider for DLFAuthProvider {
    fn merge_auth_header(
        &self,
        mut base_header: HashMap<String, String>,
        rest_auth_parameter: &RESTAuthParameter,
    ) -> HashMap<String, String> {
        let token = match &self.token {
            Some(t) => t,
            None => return base_header,
        };

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
                .authorization(rest_auth_parameter, token, &host, &sign_headers);

        // Merge all headers
        base_header.extend(sign_headers);
        base_header.insert(DLF_AUTHORIZATION_HEADER_KEY.to_string(), authorization);

        base_header
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_host() {
        let uri = "http://dlf-regres-test-cn-hangzhou-vpc.taobao.net/api/v1";
        let host = DLFAuthProvider::extract_host(uri);
        assert_eq!(host, "dlf-regres-test-cn-hangzhou-vpc.taobao.net");
    }

    #[test]
    fn test_extract_host_no_path() {
        let uri = "https://dlf.cn-hangzhou.aliyuncs.com";
        let host = DLFAuthProvider::extract_host(uri);
        assert_eq!(host, "dlf.cn-hangzhou.aliyuncs.com");
    }

    #[test]
    fn test_dlf_auth_provider_from_options() {
        let mut options = Options::new();
        options.set(
            CatalogOptions::URI,
            "http://dlf-regres-test-cn-hangzhou-vpc.taobao.net/",
        );
        options.set(CatalogOptions::DLF_REGION, "cn-hangzhou");
        options.set(CatalogOptions::DLF_ACCESS_KEY_ID, "test_key_id");
        options.set(CatalogOptions::DLF_ACCESS_KEY_SECRET, "test_key_secret");

        let provider = DLFAuthProvider::from_options(&options);
        assert!(provider.is_some());

        let provider = provider.unwrap();
        assert_eq!(provider.region, "cn-hangzhou");
        assert!(provider.token.is_some());
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
}
