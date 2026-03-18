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

//! Base types for authentication.

use std::collections::HashMap;

/// Parameter for REST authentication.
///
/// Contains information about the request being authenticated.
#[derive(Debug, Clone)]
pub struct RESTAuthParameter {
    /// HTTP method (GET, POST, DELETE, etc.)
    pub method: String,
    /// Request path
    pub path: String,
    /// Request body data (for POST/PUT requests)
    pub data: Option<String>,
    /// Query parameters
    pub parameters: HashMap<String, String>,
}

impl RESTAuthParameter {
    /// Create a new RESTAuthParameter.
    pub fn new(
        method: impl Into<String>,
        path: impl Into<String>,
        data: Option<String>,
        parameters: HashMap<String, String>,
    ) -> Self {
        Self {
            method: method.into(),
            path: path.into(),
            data,
            parameters,
        }
    }

    /// Create a parameter for a GET request.
    pub fn for_get(path: impl Into<String>, parameters: HashMap<String, String>) -> Self {
        Self::new("GET", path, None, parameters)
    }

    /// Create a parameter for a POST request.
    pub fn for_post(path: impl Into<String>, data: String) -> Self {
        Self::new("POST", path, Some(data), HashMap::new())
    }

    /// Create a parameter for a DELETE request.
    pub fn for_delete(path: impl Into<String>) -> Self {
        Self::new("DELETE", path, None, HashMap::new())
    }
}

/// Trait for authentication providers.
///
/// Implement this trait to provide custom authentication mechanisms
/// for REST API requests.
pub trait AuthProvider: Send + Sync {
    /// Merge authentication headers into the base headers.
    ///
    /// # Arguments
    /// * `base_header` - The base headers to merge into
    /// * `parameter` - Information about the request being authenticated
    ///
    /// # Returns
    /// A new HashMap containing the merged headers.
    fn merge_auth_header(
        &self,
        base_header: HashMap<String, String>,
        parameter: &RESTAuthParameter,
    ) -> HashMap<String, String>;

    /// Clone this provider into a boxed trait object.
    fn clone_box(&self) -> Box<dyn AuthProvider>;
}

impl Clone for Box<dyn AuthProvider> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Function wrapper for REST authentication.
///
/// This struct combines an initial set of headers with an authentication provider
/// to produce authenticated headers for each request.
pub struct RESTAuthFunction {
    init_header: HashMap<String, String>,
    auth_provider: Box<dyn AuthProvider>,
}

impl RESTAuthFunction {
    /// Create a new RESTAuthFunction.
    ///
    /// # Arguments
    /// * `init_header` - Initial headers to include in all requests
    /// * `auth_provider` - The authentication provider to use
    pub fn new(init_header: HashMap<String, String>, auth_provider: Box<dyn AuthProvider>) -> Self {
        Self {
            init_header,
            auth_provider,
        }
    }

    /// Apply authentication to get headers for a request.
    ///
    /// # Arguments
    /// * `parameter` - Information about the request being authenticated
    ///
    /// # Returns
    /// A HashMap containing the authenticated headers.
    pub fn apply(&self, parameter: &RESTAuthParameter) -> HashMap<String, String> {
        self.auth_provider
            .merge_auth_header(self.init_header.clone(), parameter)
    }
}

// ============================================================================
// NoOp Auth Provider
// ============================================================================

/// A no-operation authentication provider that returns headers unchanged.
///
/// This provider is used when no authentication is required.
pub struct NoOpAuthProvider {
    initial_headers: HashMap<String, String>,
}

impl NoOpAuthProvider {
    /// Create a new NoOpAuthProvider.
    pub fn new() -> Self {
        NoOpAuthProvider {
            initial_headers: HashMap::new(),
        }
    }

    /// Create a NoOpAuthProvider with initial headers.
    pub fn with_headers(initial_headers: HashMap<String, String>) -> Self {
        NoOpAuthProvider { initial_headers }
    }
}

impl Default for NoOpAuthProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthProvider for NoOpAuthProvider {
    fn merge_auth_header(
        &self,
        mut base_header: HashMap<String, String>,
        _parameter: &RESTAuthParameter,
    ) -> HashMap<String, String> {
        for (key, value) in &self.initial_headers {
            base_header.insert(key.clone(), value.clone());
        }
        base_header
    }

    fn clone_box(&self) -> Box<dyn AuthProvider> {
        Box::new(NoOpAuthProvider::with_headers(self.initial_headers.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_auth_provider() {
        let provider = NoOpAuthProvider::new();
        let base_header = HashMap::new();
        let parameter = RESTAuthParameter::for_get("/test", HashMap::new());

        let headers = provider.merge_auth_header(base_header, &parameter);
        assert!(headers.is_empty());
    }

    #[test]
    fn test_noop_auth_provider_with_headers() {
        let mut initial_headers = HashMap::new();
        initial_headers.insert("X-Custom-Header".to_string(), "value".to_string());
        
        let provider = NoOpAuthProvider::with_headers(initial_headers);
        let base_header = HashMap::new();
        let parameter = RESTAuthParameter::for_get("/test", HashMap::new());

        let headers = provider.merge_auth_header(base_header, &parameter);
        assert_eq!(headers.get("X-Custom-Header"), Some(&"value".to_string()));
    }
}
