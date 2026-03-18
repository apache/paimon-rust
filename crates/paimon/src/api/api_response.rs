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

use crate::spec::Schema;

/// Base trait for REST responses.
pub trait RESTResponse {}

/// Error response from REST API calls.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    /// The type of resource that caused the error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_type: Option<String>,
    /// The name of the resource that caused the error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_name: Option<String>,
    /// The error message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The error code.
    #[serde(skip_serializing_if = "Option::is_none")]
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

/// Base response containing audit information.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditRESTResponse {
    /// The owner of the resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// Timestamp when the resource was created.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    /// User who created the resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    /// Timestamp when the resource was last updated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    /// User who last updated the resource.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_by: Option<String>,
}

impl RESTResponse for AuditRESTResponse {}
impl AuditRESTResponse {
    /// Create a new AuditRESTResponse.
    pub fn new(
        owner: Option<String>,
        created_at: Option<i64>,
        created_by: Option<String>,
        updated_at: Option<i64>,
        updated_by: Option<String>,
    ) -> Self {
        Self {
            owner,
            created_at,
            created_by,
            updated_at,
            updated_by,
        }
    }

    /// Put audit options into the provided dictionary.
    pub fn put_audit_options_to(&self, options: &mut HashMap<String, String>) {
        if let Some(owner) = &self.owner {
            options.insert("owner".to_string(), owner.clone());
        }
        if let Some(created_by) = &self.created_by {
            options.insert("createdBy".to_string(), created_by.clone());
        }
        if let Some(created_at) = self.created_at {
            options.insert("createdAt".to_string(), created_at.to_string());
        }
        if let Some(updated_by) = &self.updated_by {
            options.insert("updatedBy".to_string(), updated_by.clone());
        }
        if let Some(updated_at) = self.updated_at {
            options.insert("updatedAt".to_string(), updated_at.to_string());
        }
    }
}

/// Response for getting a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTableResponse {
    /// Audit information.
    #[serde(flatten)]
    pub audit: AuditRESTResponse,
    /// The unique identifier of the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The name of the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The path to the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Whether the table is external.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_external: Option<bool>,
    /// The schema ID of the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i64>,
    /// The schema of the table.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,
}

impl RESTResponse for GetTableResponse {}

impl GetTableResponse {
    /// Create a new GetTableResponse.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: Option<String>,
        name: Option<String>,
        path: Option<String>,
        is_external: Option<bool>,
        schema_id: Option<i64>,
        schema: Option<Schema>,
        audit: AuditRESTResponse,
    ) -> Self {
        Self {
            audit,
            id,
            name,
            path,
            is_external,
            schema_id,
            schema,
        }
    }
}

/// Response for getting a database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetDatabaseResponse {
    /// Audit information.
    #[serde(flatten)]
    pub audit: AuditRESTResponse,
    /// The unique identifier of the database.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The name of the database.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The location of the database.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// Configuration options for the database.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, String>,
}

impl RESTResponse for GetDatabaseResponse {}

impl GetDatabaseResponse {
    /// Create a new GetDatabaseResponse.
    pub fn new(
        id: Option<String>,
        name: Option<String>,
        location: Option<String>,
        options: HashMap<String, String>,
        audit: AuditRESTResponse,
    ) -> Self {
        Self {
            audit,
            id,
            name,
            location,
            options,
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

    /// Merge these defaults with the provided options.
    /// User options take precedence over defaults.
    pub fn merge(&self, options: &HashMap<String, String>) -> HashMap<String, String> {
        let mut merged = self.defaults.clone();
        merged.extend(options.clone());
        merged
    }

    /// Convert to Options struct.
    pub fn to_options(&self) -> crate::common::Options {
        crate::common::Options::from_map(self.defaults.clone())
    }
}

/// Response containing a table token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTableTokenResponse {
    /// The token data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<HashMap<String, String>>,
    /// Expiration timestamp in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_millis: Option<i64>,
}

impl RESTResponse for GetTableTokenResponse {}

impl GetTableTokenResponse {
    /// Create a new GetTableTokenResponse.
    pub fn new(token: Option<HashMap<String, String>>, expires_at_millis: Option<i64>) -> Self {
        Self {
            token,
            expires_at_millis,
        }
    }
}

/// A paged response with data and optional next page token.
pub trait PagedResponse<T>: RESTResponse {
    /// Get the data elements.
    fn data(&self) -> &[T];
    /// Get the next page token.
    fn get_next_page_token(&self) -> Option<&str>;
}

/// Response for listing databases.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListDatabasesResponse {
    /// List of database names.
    pub databases: Vec<String>,
    /// Token for the next page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

impl RESTResponse for ListDatabasesResponse {}

impl PagedResponse<String> for ListDatabasesResponse {
    fn data(&self) -> &[String] {
        &self.databases
    }

    fn get_next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }
}

impl ListDatabasesResponse {
    /// Create a new ListDatabasesResponse.
    pub fn new(databases: Vec<String>, next_page_token: Option<String>) -> Self {
        Self {
            databases,
            next_page_token,
        }
    }
}

/// Response for listing tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListTablesResponse {
    /// List of table names.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<String>>,
    /// Token for the next page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

impl RESTResponse for ListTablesResponse {}

impl PagedResponse<String> for ListTablesResponse {
    fn data(&self) -> &[String] {
        static EMPTY: &[String] = &[];
        self.tables.as_ref().map(|v| v.as_slice()).unwrap_or(EMPTY)
    }

    fn get_next_page_token(&self) -> Option<&str> {
        self.next_page_token.as_deref()
    }
}

impl ListTablesResponse {
    /// Create a new ListTablesResponse.
    pub fn new(tables: Option<Vec<String>>, next_page_token: Option<String>) -> Self {
        Self {
            tables,
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
    #[serde(skip_serializing_if = "Option::is_none")]
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

    #[test]
    fn test_config_response_merge() {
        let mut defaults = HashMap::new();
        defaults.insert("key1".to_string(), "default1".to_string());
        defaults.insert("key2".to_string(), "default2".to_string());

        let config = ConfigResponse::new(defaults);

        let mut user_options = HashMap::new();
        user_options.insert("key2".to_string(), "user2".to_string());
        user_options.insert("key3".to_string(), "user3".to_string());

        let merged = config.merge(&user_options);
        assert_eq!(merged.get("key1"), Some(&"default1".to_string()));
        assert_eq!(merged.get("key2"), Some(&"user2".to_string())); // user overrides default
        assert_eq!(merged.get("key3"), Some(&"user3".to_string()));
    }

    #[test]
    fn test_audit_response_options() {
        let audit = AuditRESTResponse::new(
            Some("owner1".to_string()),
            Some(1000),
            Some("creator".to_string()),
            Some(2000),
            Some("updater".to_string()),
        );

        let mut options = HashMap::new();
        audit.put_audit_options_to(&mut options);

        assert_eq!(options.get("owner"), Some(&"owner1".to_string()));
        assert_eq!(options.get("createdBy"), Some(&"creator".to_string()));
        assert_eq!(options.get("createdAt"), Some(&"1000".to_string()));
        assert_eq!(options.get("updatedBy"), Some(&"updater".to_string()));
        assert_eq!(options.get("updatedAt"), Some(&"2000".to_string()));
    }
}
