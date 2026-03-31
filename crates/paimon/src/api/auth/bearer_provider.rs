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

//! Bearer token authentication provider.

use std::collections::HashMap;

use async_trait::async_trait;

use super::base::{AuthProvider, RESTAuthParameter};

/// Authentication provider using Bearer token.
///
/// This provider adds an `Authorization: Bearer <token>` header
/// to all requests.
pub struct BearerTokenAuthProvider {
    token: String,
}

impl BearerTokenAuthProvider {
    /// Create a new BearerTokenAuthProvider.
    ///
    /// # Arguments
    /// * `token` - The bearer token to use for authentication
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

#[async_trait]
impl AuthProvider for BearerTokenAuthProvider {
    async fn merge_auth_header(
        &self,
        mut base_header: HashMap<String, String>,
        _parameter: &RESTAuthParameter,
    ) -> crate::Result<HashMap<String, String>> {
        base_header.insert(
            "Authorization".to_string(),
            format!("Bearer {}", self.token),
        );
        Ok(base_header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bearer_token_auth() {
        let provider = BearerTokenAuthProvider::new("test-token");
        let base_header = HashMap::new();
        let parameter = RESTAuthParameter::for_get("/test", HashMap::new());

        let headers = provider
            .merge_auth_header(base_header, &parameter)
            .await
            .unwrap();

        assert_eq!(
            headers.get("Authorization"),
            Some(&"Bearer test-token".to_string())
        );
    }

    #[tokio::test]
    async fn test_bearer_token_with_base_headers() {
        let provider = BearerTokenAuthProvider::new("my-token");
        let mut base_header = HashMap::new();
        base_header.insert("Content-Type".to_string(), "application/json".to_string());
        let parameter = RESTAuthParameter::for_get("/test", HashMap::new());

        let headers = provider
            .merge_auth_header(base_header, &parameter)
            .await
            .unwrap();

        assert_eq!(
            headers.get("Authorization"),
            Some(&"Bearer my-token".to_string())
        );
        assert_eq!(
            headers.get("Content-Type"),
            Some(&"application/json".to_string())
        );
    }
}
