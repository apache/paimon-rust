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

//! REST API response types for Paimon.
//!
//! This module contains all response structures used in REST API calls.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Base trait for REST responses.
pub trait RESTResponse {}

/// Error response from REST API calls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    /// The type of resource that caused the error.
    pub resource_type: Option<String>,
    /// The name of the resource that caused the error.
    pub resource_name: Option<String>,
    /// The error message.
    pub message: Option<String>,
    /// The error code.
    pub code: Option<i32>,
}

impl RESTResponse for ErrorResponse {}
impl ErrorResponse {
    /// Create a new ErrorResponse.
    pub fn new(
        resource_type: Option<String>,
        resource_name: Option<String>,
        message: Option<String>,
        code: Option<i32>,
    ) -> Self {
        Self {
            resource_type,
            resource_name,
            message,
            code,
        }
    }
}

/// Response containing configuration defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigResponse {
    /// Default configuration values.
    pub defaults: HashMap<String, String>,
}

impl RESTResponse for ConfigResponse {}

impl ConfigResponse {
    /// Create a new ConfigResponse.
    pub fn new(defaults: HashMap<String, String>) -> Self {
        Self { defaults }
    }

    /// Merge these defaults with the provided Options.
    /// User options take precedence over defaults.
    pub fn merge_options(&self, options: &crate::common::Options) -> crate::common::Options {
        let mut merged = self.defaults.clone();
        merged.extend(options.to_map().clone());
        crate::common::Options::from_map(merged)
    }

    /// Convert to Options struct.
    pub fn to_options(&self) -> crate::common::Options {
        crate::common::Options::from_map(self.defaults.clone())
    }
}

/// Response for listing databases.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListDatabasesResponse {
    /// List of database names.
    pub databases: Vec<String>,
    /// Token for the next page.
    pub next_page_token: Option<String>,
}

impl RESTResponse for ListDatabasesResponse {}

impl ListDatabasesResponse {
    /// Create a new ListDatabasesResponse.
    pub fn new(databases: Vec<String>, next_page_token: Option<String>) -> Self {
        Self {
            databases,
            next_page_token,
        }
    }
}

/// A paginated list of elements with an optional next page token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PagedList<T> {
    /// The list of elements on this page.
    pub elements: Vec<T>,
    /// Token to retrieve the next page, if available.
    pub next_page_token: Option<String>,
}

impl<T> PagedList<T> {
    /// Create a new PagedList.
    pub fn new(elements: Vec<T>, next_page_token: Option<String>) -> Self {
        Self {
            elements,
            next_page_token,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_response_serialization() {
        let resp = ErrorResponse::new(
            Some("table".to_string()),
            Some("test_table".to_string()),
            Some("Table not found".to_string()),
            Some(404),
        );

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"resourceType\":\"table\""));
        assert!(json.contains("\"resourceName\":\"test_table\""));
        assert!(json.contains("\"message\":\"Table not found\""));
        assert!(json.contains("\"code\":404"));
    }

    #[test]
    fn test_list_databases_response_serialization() {
        let resp = ListDatabasesResponse::new(
            vec!["db1".to_string(), "db2".to_string()],
            Some("token123".to_string()),
        );

        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"databases\":[\"db1\",\"db2\"]"));
        assert!(json.contains("\"nextPageToken\":\"token123\""));
    }
}
