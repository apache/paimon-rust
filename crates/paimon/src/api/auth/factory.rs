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

use crate::common::{CatalogOptions, Options};
use crate::Error;

use super::{AuthProvider, BearerTokenAuthProvider};

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
            None => Err(Error::ConfigInvalid {
                message: "auth provider is required".to_string(),
            }),
            Some(unknown) => Err(Error::ConfigInvalid {
                message: format!("Unknown auth provider: {}", unknown),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::RESTAuthParameter;
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_create_bearer_provider() {
        let mut options = Options::new();
        options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
        options.set(CatalogOptions::TOKEN, "test-token");

        let provider = AuthProviderFactory::create_auth_provider(&options).unwrap();

        let base_header = HashMap::new();
        let param = RESTAuthParameter::new("GET", "/test", None, HashMap::new());
        let result = provider.merge_auth_header(base_header, &param);

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
}
