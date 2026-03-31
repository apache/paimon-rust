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

use async_trait::async_trait;

use crate::Result;

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
#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// Merge authentication headers into the base headers.
    ///
    /// # Arguments
    /// * `base_header` - The base headers to merge into
    /// * `parameter` - Information about the request being authenticated
    ///
    /// # Returns
    async fn merge_auth_header(
        &self,
        base_header: HashMap<String, String>,
        parameter: &RESTAuthParameter,
    ) -> Result<HashMap<String, String>>;
}
/// Authorization header key.
pub const AUTHORIZATION_HEADER_KEY: &str = "Authorization";
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
    pub async fn apply(&self, parameter: &RESTAuthParameter) -> Result<HashMap<String, String>> {
        self.auth_provider
            .merge_auth_header(self.init_header.clone(), parameter)
            .await
    }
}
