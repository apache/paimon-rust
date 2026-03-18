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

//! Asynchronous HTTP client for REST API calls.

use super::auth::{RESTAuthFunction, RESTAuthParameter};
use super::rest_error::RestError;
use crate::Error;
use crate::Result;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::time::Duration;

/// Asynchronous HTTP client for REST API calls.
pub struct HttpClient {
    client: reqwest::Client,
    base_url: String,
    auth_function: Option<RESTAuthFunction>,
}

impl HttpClient {
    /// Create a new HttpClient with the given base URL.
    ///
    /// # Arguments
    /// * `base_url` - The base URL for all HTTP requests.
    /// * `auth_function` - Optional authentication function for requests.
    ///
    /// # Returns
    /// A new HttpClient instance.
    pub fn new(base_url: &str, auth_function: Option<RESTAuthFunction>) -> Result<Self> {
        if base_url.trim().is_empty() {
            return Err(Error::ConfigInvalid {
                message: "base_url is empty".to_string(),
            });
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| Error::ConfigInvalid {
                message: format!("Failed to create HTTP client: {}", e),
            })?;

        Ok(HttpClient {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            auth_function,
        })
    }

    /// Perform a GET request and parse the response as JSON.
    ///
    /// # Arguments
    /// * `path` - The path to append to the base URL.
    ///
    /// # Returns
    /// The parsed JSON response.
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.url(path);
        let headers = self.build_auth_headers("GET", path, None, HashMap::new());
        let request = self.client.get(&url);
        let request = Self::apply_headers(request, &headers);
        let resp = request.send().await.map_err(|e| Error::UnexpectedError {
            message: "http get failed".to_string(),
            source: Some(Box::new(e)),
        })?;
        self.parse_response(resp).await
    }

    /// Perform a GET request with query parameters.
    ///
    /// # Arguments
    /// * `path` - The path to append to the base URL.
    /// * `params` - Query parameters as key-value pairs (supports both `&str` and `String`).
    ///
    /// # Returns
    /// The parsed JSON response.
    pub async fn get_with_params<T: DeserializeOwned>(
        &self,
        path: &str,
        params: &[(impl AsRef<str>, impl AsRef<str>)],
    ) -> Result<T> {
        let url = self.url(path);
        let params_map: HashMap<String, String> = params
            .iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.as_ref().to_string()))
            .collect();
        let headers = self.build_auth_headers("GET", path, None, params_map.clone());

        let mut request = self.client.get(&url);
        for (key, value) in params {
            request = request.query(&[(key.as_ref(), value.as_ref())]);
        }

        let request = Self::apply_headers(request, &headers);
        let resp = request.send().await.map_err(|e| Error::UnexpectedError {
            message: "http get failed".to_string(),
            source: Some(Box::new(e)),
        })?;
        self.parse_response(resp).await
    }

    /// Perform a POST request with a JSON body.
    ///
    /// # Arguments
    /// * `path` - The path to append to the base URL.
    /// * `body` - The JSON body to send.
    ///
    /// # Returns
    /// The parsed JSON response.
    pub async fn post<T: DeserializeOwned, B: serde::Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let url = self.url(path);
        let body_str = serde_json::to_string(body).ok();
        let headers = self.build_auth_headers("POST", path, body_str.as_deref(), HashMap::new());
        let request = self.client.post(&url).json(body);
        let request = Self::apply_headers(request, &headers);
        let resp = request.send().await.map_err(|e| Error::UnexpectedError {
            message: "http post failed".to_string(),
            source: Some(Box::new(e)),
        })?;
        self.parse_response(resp).await
    }

    /// Perform a DELETE request.
    ///
    /// # Arguments
    /// * `path` - The path to append to the base URL.
    ///
    /// # Returns
    /// The parsed JSON response.
    pub async fn delete<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.url(path);
        let headers = self.build_auth_headers("DELETE", path, None, HashMap::new());
        let request = self.client.delete(&url);
        let request = Self::apply_headers(request, &headers);
        let resp = request.send().await.map_err(|e| Error::UnexpectedError {
            message: "http delete failed".to_string(),
            source: Some(Box::new(e)),
        })?;
        self.parse_response(resp).await
    }

    /// Perform a DELETE request with query parameters.
    ///
    /// # Arguments
    /// * `path` - The path to append to the base URL.
    /// * `params` - Query parameters as key-value pairs (supports both `&str` and `String`).
    ///
    /// # Returns
    /// The parsed JSON response.
    pub async fn delete_with_params<T: DeserializeOwned>(
        &self,
        path: &str,
        params: &[(impl AsRef<str>, impl AsRef<str>)],
    ) -> Result<T> {
        let url = self.url(path);
        let params_map: HashMap<String, String> = params
            .iter()
            .map(|(k, v)| (k.as_ref().to_string(), v.as_ref().to_string()))
            .collect();
        let headers = self.build_auth_headers("DELETE", path, None, params_map.clone());

        let mut request = self.client.delete(&url);
        for (key, value) in params {
            request = request.query(&[(key.as_ref(), value.as_ref())]);
        }

        let request = Self::apply_headers(request, &headers);
        let resp = request.send().await.map_err(|e| Error::UnexpectedError {
            message: "http delete failed".to_string(),
            source: Some(Box::new(e)),
        })?;
        self.parse_response(resp).await
    }

    /// Set the authentication function for this client.
    pub fn set_auth_function(&mut self, auth_function: RESTAuthFunction) {
        self.auth_function = Some(auth_function);
    }

    /// Build auth headers for a request.
    fn build_auth_headers(
        &self,
        method: &str,
        path: &str,
        data: Option<&str>,
        params: HashMap<String, String>,
    ) -> HashMap<String, String> {
        if let Some(ref auth_fn) = self.auth_function {
            let parameter =
                RESTAuthParameter::new(method, path, data.map(|s| s.to_string()), params);
            auth_fn.apply(&parameter)
        } else {
            HashMap::new()
        }
    }

    /// Apply headers to a request builder.
    fn apply_headers(
        request: reqwest::RequestBuilder,
        headers: &HashMap<String, String>,
    ) -> reqwest::RequestBuilder {
        let mut request = request;
        for (key, value) in headers {
            request = request.header(key, value);
        }
        request
    }

    fn url(&self, path: &str) -> String {
        if path.is_empty() || path == "/" {
            self.base_url.clone()
        } else if path.starts_with('/') {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}/{}", self.base_url, path)
        }
    }

    async fn parse_response<T: DeserializeOwned>(&self, resp: reqwest::Response) -> Result<T> {
        let status = resp.status();

        if !status.is_success() {
            let text = resp.text().await.map_err(|e| Error::UnexpectedError {
                message: "failed to read response".to_string(),
                source: Some(Box::new(e)),
            })?;

            // Try to parse error response as JSON to extract message and resource info
            let error_info = RestError::parse_error_response(&text);
            let message = error_info
                .message
                .clone()
                .unwrap_or_else(|| format!("HTTP {}: {}", status, text));

            let rest_error = RestError::from_status(status, message, error_info);
            return Err(Error::from(rest_error));
        }

        // Parse successful response
        let text = resp.text().await.map_err(|e| Error::UnexpectedError {
            message: "failed to read response".to_string(),
            source: Some(Box::new(e)),
        })?;

        serde_json::from_str(&text).map_err(|e| Error::UnexpectedError {
            message: "failed to parse json".to_string(),
            source: Some(Box::new(e)),
        })
    }
}
