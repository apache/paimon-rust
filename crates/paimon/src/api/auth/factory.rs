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

//! Authentication provider factory.

use crate::api::auth::dlf_provider::DLFTokenLoaderFactory;
use crate::api::auth::{BearerTokenAuthProvider, DLFAuthProvider, DLFToken};
use crate::api::AuthProvider;
use crate::common::{CatalogOptions, Options};
use crate::Error;
use regex::Regex;

/// Factory for creating DLF authentication providers.
pub struct DLFAuthProviderFactory;

impl DLFAuthProviderFactory {
    /// OpenAPI identifier.
    pub const OPENAPI_IDENTIFIER: &'static str = "openapi";
    /// Default identifier.
    pub const DEFAULT_IDENTIFIER: &'static str = "default";
    /// Region pattern for parsing from URI.
    const REGION_PATTERN: &'static str = r"(?:pre-)?([a-z]+-[a-z]+(?:-\d+)?)";

    /// Parse region from DLF endpoint URI.
    pub fn parse_region_from_uri(uri: Option<&str>) -> Option<String> {
        let uri = uri?;
        let re = Regex::new(Self::REGION_PATTERN).ok()?;
        let caps = re.captures(uri)?;
        caps.get(1).map(|m| m.as_str().to_string())
    }

    /// Parse signing algorithm from URI.
    ///
    /// Returns "openapi" for public endpoints (dlfnext in host),
    /// otherwise returns "default".
    pub fn parse_signing_algo_from_uri(uri: Option<&str>) -> &'static str {
        if let Some(uri) = uri {
            let host = uri.to_lowercase();
            let host = host
                .strip_prefix("http://")
                .unwrap_or(host.strip_prefix("https://").unwrap_or(&host));
            let host = host.split('/').next().unwrap_or("");
            let host = host.split(':').next().unwrap_or("");

            if host.starts_with("dlfnext") {
                return Self::OPENAPI_IDENTIFIER;
            }
        }
        Self::DEFAULT_IDENTIFIER
    }

    /// Create a DLF authentication provider from options.
    ///
    /// # Arguments
    /// * `options` - The configuration options.
    ///
    /// # Returns
    /// A boxed AuthProvider trait object.
    ///
    /// # Errors
    /// Returns an error if required configuration is missing.
    pub fn create_provider(options: &Options) -> Result<Box<dyn AuthProvider>, Error> {
        let uri = options
            .get(CatalogOptions::URI)
            .ok_or_else(|| Error::ConfigInvalid {
                message: "URI is required for DLF authentication".to_string(),
            })?
            .clone();

        // Get region from options or parse from URI
        let region = options
            .get(CatalogOptions::DLF_REGION)
            .cloned()
            .or_else(|| Self::parse_region_from_uri(Some(&uri)))
            .ok_or_else(|| Error::ConfigInvalid {
                message: "Could not get region from config or URI. Please set 'dlf.region' or use a standard DLF endpoint URI.".to_string(),
            })?;

        // Get signing algorithm from options, or auto-detect from URI
        let signing_algorithm = options
            .get(CatalogOptions::DLF_SIGNING_ALGORITHM)
            .map(|s| s.as_str())
            .filter(|s| *s != "default")
            .unwrap_or_else(|| Self::parse_signing_algo_from_uri(Some(&uri)))
            .to_string();

        let dlf_provider = DLFAuthProvider::new(
            uri,
            region,
            signing_algorithm,
            DLFToken::from_options(options),
            DLFTokenLoaderFactory::create_token_loader(options),
        )?;

        Ok(Box::new(dlf_provider))
    }
}

/// Factory for creating authentication providers.
pub struct AuthProviderFactory;

impl AuthProviderFactory {
    /// Create an authentication provider based on the given options.
    ///
    /// # Arguments
    /// * `options` - The configuration options.
    ///
    /// # Returns
    /// A boxed AuthProvider trait object.
    ///
    /// # Errors
    /// Returns an error if the provider type is unknown or required configuration is missing.
    pub fn create_auth_provider(options: &Options) -> Result<Box<dyn AuthProvider>, Error> {
        let provider = options.get(CatalogOptions::TOKEN_PROVIDER);

        match provider.map(|s| s.as_str()) {
            Some("bear") => {
                let token =
                    options
                        .get(CatalogOptions::TOKEN)
                        .ok_or_else(|| Error::ConfigInvalid {
                            message: "token is required for bearer authentication".to_string(),
                        })?;
                Ok(Box::new(BearerTokenAuthProvider::new(token)))
            }
            Some("dlf") => DLFAuthProviderFactory::create_provider(options),
            Some(unknown) => Err(Error::ConfigInvalid {
                message: format!("Unknown auth provider: {unknown}"),
            }),
            None => Err(Error::ConfigInvalid {
                message: "auth provider is required".to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api::auth::base::AUTHORIZATION_HEADER_KEY;

    use super::super::RESTAuthParameter;
    use super::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_create_bearer_provider() {
        let mut options = Options::new();
        options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
        options.set(CatalogOptions::TOKEN, "test-token");

        let provider = AuthProviderFactory::create_auth_provider(&options).unwrap();

        let base_header = HashMap::new();
        let param = RESTAuthParameter::new("GET", "/test", None, HashMap::new());
        let result = provider
            .merge_auth_header(base_header, &param)
            .await
            .unwrap();

        assert_eq!(
            result.get("Authorization"),
            Some(&"Bearer test-token".to_string())
        );
    }

    #[test]
    fn test_none_provider_error() {
        let options = Options::new();
        let result = AuthProviderFactory::create_auth_provider(&options);
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_provider() {
        let mut options = Options::new();
        options.set(CatalogOptions::TOKEN_PROVIDER, "unknown");

        let result = AuthProviderFactory::create_auth_provider(&options);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_dlf_provider() {
        let mut options = Options::new();
        options.set(CatalogOptions::TOKEN_PROVIDER, "dlf");
        options.set(CatalogOptions::URI, "http://dlf-asdaswfnb.net/");
        options.set(CatalogOptions::DLF_REGION, "cn-hangzhou");
        options.set(CatalogOptions::DLF_ACCESS_KEY_ID, "test_key_id");
        options.set(CatalogOptions::DLF_ACCESS_KEY_SECRET, "test_key_secret");

        let provider = AuthProviderFactory::create_auth_provider(&options).unwrap();

        let base_header = HashMap::new();
        let param = RESTAuthParameter::new("GET", "/test", None, HashMap::new());
        let result = provider
            .merge_auth_header(base_header, &param)
            .await
            .unwrap();

        assert!(result.contains_key(AUTHORIZATION_HEADER_KEY));
    }

    #[test]
    fn test_dlf_provider_missing_region() {
        let mut options = Options::new();
        options.set(CatalogOptions::TOKEN_PROVIDER, "dlf");
        options.set(CatalogOptions::URI, "http://example.com/");
        options.set(CatalogOptions::DLF_ACCESS_KEY_ID, "test_key_id");
        options.set(CatalogOptions::DLF_ACCESS_KEY_SECRET, "test_key_secret");

        let result = AuthProviderFactory::create_auth_provider(&options);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_region_from_uri() {
        let region = DLFAuthProviderFactory::parse_region_from_uri(Some(
            "http://cn-hangzhou-vpc.dlf.aliyuncs.com",
        ));
        assert_eq!(region, Some("cn-hangzhou".to_string()));
    }

    #[test]
    fn test_parse_signing_algo_from_uri() {
        let algo = DLFAuthProviderFactory::parse_signing_algo_from_uri(Some(
            "http://dlfnext.cn-hangzhou.aliyuncs.com",
        ));
        assert_eq!(algo, "openapi");

        let algo = DLFAuthProviderFactory::parse_signing_algo_from_uri(Some(
            "http://cn-hangzhou-vpc.dlf.aliyuncs.com",
        ));
        assert_eq!(algo, "default");
    }
}
