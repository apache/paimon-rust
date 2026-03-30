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
    pub owner: Option<String>,
    /// Timestamp when the resource was created.
    pub created_at: Option<i64>,
    /// User who created the resource.
    pub created_by: Option<String>,
    /// Timestamp when the resource was last updated.
    pub updated_at: Option<i64>,
    /// User who last updated the resource.
    pub updated_by: Option<String>,
}

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
    pub id: Option<String>,
    /// The name of the table.
    pub name: Option<String>,
    /// The path to the table.
    pub path: Option<String>,
    /// Whether the table is external.
    pub is_external: Option<bool>,
    /// The schema ID of the table.
    pub schema_id: Option<i64>,
    /// The schema of the table.
    pub schema: Option<Schema>,
}

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
    pub id: Option<String>,
    /// The name of the database.
    pub name: Option<String>,
    /// The location of the database.
    pub location: Option<String>,
    /// Configuration options for the database.
    pub options: HashMap<String, String>,
}

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
    pub tables: Option<Vec<String>>,
    /// Token for the next page.
    pub next_page_token: Option<String>,
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

/// Response for getting table token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTableTokenResponse {
    /// Token key-value pairs (e.g. access_key_id, access_key_secret, etc.)
    pub token: HashMap<String, String>,
    /// Token expiration time in milliseconds since epoch.
    pub expires_at_millis: Option<i64>,
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
