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
        let final_url = Self::normalize_uri(base_url)?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| Error::ConfigInvalid {
                message: format!("Failed to create HTTP client: {e}"),
            })?;

        Ok(HttpClient {
            client,
            base_url: final_url,
            auth_function,
        })
    }

    /// Normalize and validate a URI.
    ///
    /// # Arguments
    /// * `uri` - The URI to normalize.
    ///
    /// # Returns
    /// A normalized URI string, or an error if the URI is invalid.
    fn normalize_uri(uri: &str) -> Result<String> {
        let uri = uri.trim();

        if uri.is_empty() {
            return Err(Error::ConfigInvalid {
                message: "uri is empty which must be defined".to_string(),
            });
        }

        // Add http:// prefix if missing
        let normalized_url = if uri.starts_with("http://") || uri.starts_with("https://") {
            uri.to_string()
        } else {
            format!("http://{uri}")
        };

        // Remove trailing slash
        Ok(normalized_url.trim_end_matches('/').to_string())
    }

    /// Perform a GET request with optional query parameters.
    ///
    /// # Arguments
    /// * `path` - The path to append to the base URL.
    /// * `params` - Optional query parameters as key-value pairs.
    ///
    /// # Returns
    /// The parsed JSON response.
    pub async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        params: Option<&[(impl AsRef<str>, impl AsRef<str>)]>,
    ) -> Result<T> {
        let url = self.request_url(path);

        let params_map: HashMap<String, String> = match params {
            Some(p) => p
                .iter()
                .map(|(k, v)| (k.as_ref().to_string(), v.as_ref().to_string()))
                .collect(),
            None => HashMap::new(),
        };

        let headers = self
            .build_auth_headers("GET", path, None, params_map)
            .await?;

        let mut request = self.client.get(&url);
        if let Some(p) = params {
            for (key, value) in p {
                request = request.query(&[(key.as_ref(), value.as_ref())]);
            }
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
        let url = self.request_url(path);
        let body_str = serde_json::to_string(body).ok();
        let headers = self
            .build_auth_headers("POST", path, body_str.as_deref(), HashMap::new())
            .await?;
        let request = self.client.post(&url).json(body);
        let request = Self::apply_headers(request, &headers);
        let resp = request.send().await.map_err(|e| Error::UnexpectedError {
            message: "http post failed".to_string(),
            source: Some(Box::new(e)),
        })?;
        self.parse_response(resp).await
    }

    /// Perform a DELETE request with optional query parameters.
    ///
    /// # Arguments
    /// * `path` - The path to append to the base URL.
    /// * `params` - Optional query parameters as key-value pairs.
    ///
    /// # Returns
    /// The parsed JSON response.
    pub async fn delete<T: DeserializeOwned>(
        &self,
        path: &str,
        params: Option<&[(impl AsRef<str>, impl AsRef<str>)]>,
    ) -> Result<T> {
        let url = self.request_url(path);

        let params_map: HashMap<String, String> = match params {
            Some(p) => p
                .iter()
                .map(|(k, v)| (k.as_ref().to_string(), v.as_ref().to_string()))
                .collect(),
            None => HashMap::new(),
        };

        let headers = self
            .build_auth_headers("DELETE", path, None, params_map)
            .await?;

        let mut request = self.client.delete(&url);
        if let Some(p) = params {
            for (key, value) in p {
                request = request.query(&[(key.as_ref(), value.as_ref())]);
            }
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
    async fn build_auth_headers(
        &self,
        method: &str,
        path: &str,
        data: Option<&str>,
        params: HashMap<String, String>,
    ) -> Result<HashMap<String, String>> {
        if let Some(ref auth_fn) = self.auth_function {
            let parameter =
                RESTAuthParameter::new(method, path, data.map(|s| s.to_string()), params);
            auth_fn.apply(&parameter).await
        } else {
            Ok(HashMap::new())
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

    fn request_url(&self, path: &str) -> String {
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

            // Parse error response as ErrorResponse and map code to corresponding error
            let error_response: super::ErrorResponse =
                RestError::parse_error_response(&text, status.as_u16());
            let rest_error: RestError = RestError::from_error_response(error_response);
            return Err(Error::from(rest_error));
        }

        // Parse successful response
        let text = resp.text().await.map_err(|e| Error::UnexpectedError {
            message: "failed to read response".to_string(),
            source: Some(Box::new(e)),
        })?;

        // Handle empty response body - return null as default for types like serde_json::Value
        if text.trim().is_empty() {
            return serde_json::from_str("null").map_err(|e| Error::UnexpectedError {
                message: "failed to parse empty response".to_string(),
                source: Some(Box::new(e)),
            });
        }

        serde_json::from_str(&text).map_err(|e| Error::UnexpectedError {
            message: "failed to parse json".to_string(),
            source: Some(Box::new(e)),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use axum::extract::State;
    use axum::http::Uri;
    use axum::routing::get;
    use axum::{Json, Router};

    use super::*;
    use crate::api::auth::AuthProvider;

    struct RecordingProvider(Arc<Mutex<Option<HashMap<String, String>>>>);

    #[async_trait]
    impl AuthProvider for RecordingProvider {
        async fn merge_auth_header(
            &self,
            base_header: HashMap<String, String>,
            parameter: &RESTAuthParameter,
        ) -> Result<HashMap<String, String>> {
            *self.0.lock().unwrap() = Some(parameter.parameters.clone());
            Ok(base_header)
        }
    }

    async fn probe(
        State(query): State<Arc<Mutex<Option<String>>>>,
        uri: Uri,
    ) -> Json<serde_json::Value> {
        *query.lock().unwrap() = uri.query().map(str::to_string);
        Json(serde_json::json!({}))
    }

    fn canonical(pairs: impl Iterator<Item = String>) -> String {
        let mut parts: Vec<String> = pairs.collect();
        parts.sort();
        parts.join("&")
    }

    #[tokio::test]
    async fn test_query_parameters_are_signed_as_sent() {
        let sent_query = Arc::new(Mutex::new(None));
        let app = Router::new()
            .route("/probe", get(probe))
            .with_state(sent_query.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let _server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let signed_params = Arc::new(Mutex::new(None));
        let auth_function = RESTAuthFunction::new(
            HashMap::new(),
            Box::new(RecordingProvider(signed_params.clone())),
        );
        let client = HttpClient::new(&format!("http://{address}"), Some(auth_function)).unwrap();

        client
            .get::<serde_json::Value>(
                "/probe",
                Some(&[("principal", "acs:ram::1:role/Admin"), ("pattern", "db%")]),
            )
            .await
            .unwrap();

        let signed = signed_params.lock().unwrap().clone().unwrap();
        let sent = sent_query.lock().unwrap().clone().unwrap();

        assert_eq!(
            canonical(signed.iter().map(|(key, value)| format!("{key}={value}"))),
            canonical(sent.split('&').map(str::to_string)),
        );
        assert_eq!(
            signed.get("principal").map(String::as_str),
            Some("acs%3Aram%3A%3A1%3Arole%2FAdmin"),
        );
    }
}
