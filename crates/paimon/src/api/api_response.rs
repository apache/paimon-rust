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

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

use crate::catalog::{Function, FunctionDefinition, ViewSchema};
use crate::spec::{DataField, Schema};

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

/// Response for getting a persistent view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetViewResponse {
    /// Audit information.
    #[serde(flatten)]
    pub audit: AuditRESTResponse,
    /// The unique identifier of the view.
    pub id: Option<String>,
    /// The name of the view.
    pub name: Option<String>,
    /// Stored view schema and SQL representations.
    pub schema: ViewSchema,
}

/// Response for getting a persistent function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetFunctionResponse {
    /// Audit information.
    #[serde(flatten)]
    pub audit: AuditRESTResponse,
    /// The unique identifier of the function.
    pub uuid: Option<String>,
    /// The name of the function.
    pub name: Option<String>,
    /// Declared input parameters.
    pub input_params: Option<Vec<DataField>>,
    /// Declared return parameters.
    pub return_params: Option<Vec<DataField>>,
    /// Whether the function is deterministic.
    pub deterministic: bool,
    /// Engine-specific function definitions.
    pub definitions: HashMap<String, FunctionDefinition>,
    /// Optional function comment.
    pub comment: Option<String>,
    /// Function options.
    #[serde(default)]
    pub options: HashMap<String, String>,
}

impl GetFunctionResponse {
    /// Create a response from a catalog function.
    pub fn from_function(function: &Function, audit: AuditRESTResponse) -> Self {
        Self {
            audit,
            uuid: None,
            name: Some(function.name().to_string()),
            input_params: function.input_params().map(<[DataField]>::to_vec),
            return_params: function.return_params().map(<[DataField]>::to_vec),
            deterministic: function.is_deterministic(),
            definitions: function.definitions().clone(),
            comment: function.comment().map(ToString::to_string),
            options: function.options().clone(),
        }
    }
}

impl GetViewResponse {
    /// Create a new get-view response.
    pub fn new(
        id: Option<String>,
        name: Option<String>,
        schema: ViewSchema,
        audit: AuditRESTResponse,
    ) -> Self {
        Self {
            audit,
            id,
            name,
            schema,
        }
    }
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

/// Response containing server-provided configuration defaults and overrides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigResponse {
    /// Default configuration values.
    #[serde(default, deserialize_with = "deserialize_config_defaults")]
    pub defaults: HashMap<String, String>,
    /// Configuration values that override client-provided options.
    ///
    /// A null value removes the corresponding option after all maps are merged.
    #[serde(default, deserialize_with = "deserialize_null_to_default")]
    pub overrides: HashMap<String, Option<String>>,
}

fn deserialize_null_to_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

fn deserialize_config_defaults<'de, D>(deserializer: D) -> Result<HashMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    let defaults =
        Option::<HashMap<String, Option<String>>>::deserialize(deserializer)?.unwrap_or_default();
    // Java filters null values after merging. Defaults have the lowest precedence, so dropping
    // their null entries here produces the same result while preserving the existing field type.
    Ok(defaults
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key, value)))
        .collect())
}

impl ConfigResponse {
    /// Create a new ConfigResponse.
    pub fn new(defaults: HashMap<String, String>) -> Self {
        Self {
            defaults,
            overrides: HashMap::new(),
        }
    }

    /// Merge server defaults and overrides with the provided Options.
    /// Overrides take precedence over user options, which take precedence over defaults.
    pub fn merge_options(&self, options: &crate::common::Options) -> crate::common::Options {
        let mut merged = self.defaults.clone();
        merged.extend(options.to_map().clone());
        self.apply_overrides(&mut merged);
        crate::common::Options::from_map(merged)
    }

    /// Convert server-provided defaults and overrides to Options.
    pub fn to_options(&self) -> crate::common::Options {
        let mut options = self.defaults.clone();
        self.apply_overrides(&mut options);
        crate::common::Options::from_map(options)
    }

    fn apply_overrides(&self, options: &mut HashMap<String, String>) {
        for (key, value) in &self.overrides {
            match value {
                Some(value) => {
                    options.insert(key.clone(), value.clone());
                }
                None => {
                    options.remove(key);
                }
            }
        }
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

/// Response for listing persistent views.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListViewsResponse {
    /// View names.
    pub views: Option<Vec<String>>,
    /// Token for the next page.
    pub next_page_token: Option<String>,
}

/// Response for listing persistent functions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListFunctionsResponse {
    /// Function names.
    pub functions: Option<Vec<String>>,
    /// Token for the next page.
    pub next_page_token: Option<String>,
}

impl ListFunctionsResponse {
    /// Create a list-functions response.
    pub fn new(functions: Vec<String>, next_page_token: Option<String>) -> Self {
        Self {
            functions: Some(functions),
            next_page_token,
        }
    }
}

impl ListViewsResponse {
    /// Create a list-views response.
    pub fn new(views: Vec<String>, next_page_token: Option<String>) -> Self {
        Self {
            views: Some(views),
            next_page_token,
        }
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

/// Response for listing partitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListPartitionsResponse {
    /// List of partitions.
    pub partitions: Option<Vec<crate::spec::Partition>>,
    /// Token for the next page.
    pub next_page_token: Option<String>,
}

impl ListPartitionsResponse {
    /// Create a new ListPartitionsResponse.
    pub fn new(
        partitions: Option<Vec<crate::spec::Partition>>,
        next_page_token: Option<String>,
    ) -> Self {
        Self {
            partitions,
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

/// Response for auth table query: the per-user row filter and column masking the
/// client must enforce at read time for a `query-auth.enabled` table.
///
/// Unknown fields are rejected: an absent one reads as "no rule", so protocol
/// drift would look like an unrestricted grant.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthTableQueryResponse {
    /// JSON-serialized row-filter predicates, ANDed together. Empty/None = no filter.
    pub filter: Option<Vec<String>>,
    /// column name -> JSON-serialized masking transform. Empty/None = no masking.
    pub column_masking: Option<HashMap<String, String>>,
}

impl AuthTableQueryResponse {
    /// True when the server imposes no row filter and no column masking.
    pub fn is_unrestricted(&self) -> bool {
        self.filter.as_ref().is_none_or(|f| f.is_empty())
            && self.column_masking.as_ref().is_none_or(|m| m.is_empty())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_auth_table_query_response_rejects_unknown_fields() {
        let drifted = r#"{"rowFilter":["restricted"]}"#;
        assert!(
            serde_json::from_str::<AuthTableQueryResponse>(drifted).is_err(),
            "an auth response this client does not understand must not parse"
        );
        assert!(serde_json::from_str::<AuthTableQueryResponse>("{}")
            .unwrap()
            .is_unrestricted());
    }
    use super::*;

    #[test]
    fn test_config_response_overrides_client_options() {
        let response: ConfigResponse = serde_json::from_str(
            r#"{
                "defaults": {
                    "default-only": "default",
                    "client-wins": "default",
                    "override-wins": "default"
                },
                "overrides": {
                    "override-only": "override",
                    "override-wins": "override"
                }
            }"#,
        )
        .unwrap();
        let client_options = crate::common::Options::from_map(HashMap::from([
            ("client-only".to_string(), "client".to_string()),
            ("client-wins".to_string(), "client".to_string()),
            ("override-wins".to_string(), "client".to_string()),
        ]));

        let merged = response.merge_options(&client_options);

        assert_eq!(merged.get("default-only"), Some(&"default".to_string()));
        assert_eq!(merged.get("client-only"), Some(&"client".to_string()));
        assert_eq!(merged.get("client-wins"), Some(&"client".to_string()));
        assert_eq!(merged.get("override-only"), Some(&"override".to_string()));
        assert_eq!(merged.get("override-wins"), Some(&"override".to_string()));
    }

    #[test]
    fn test_config_response_without_overrides_is_backward_compatible() {
        let response: ConfigResponse =
            serde_json::from_str(r#"{"defaults": {"warehouse": "s3://warehouse"}}"#).unwrap();

        assert!(response.overrides.is_empty());
        assert_eq!(
            response.to_options().get("warehouse"),
            Some(&"s3://warehouse".to_string())
        );
    }

    #[test]
    fn test_config_response_accepts_null_maps() {
        let response: ConfigResponse =
            serde_json::from_str(r#"{"defaults": null, "overrides": null}"#).unwrap();
        let client_options = crate::common::Options::from_map(HashMap::from([(
            "client-only".to_string(),
            "client".to_string(),
        )]));

        let merged = response.merge_options(&client_options);

        assert_eq!(merged.get("client-only"), Some(&"client".to_string()));
    }

    #[test]
    fn test_config_response_filters_null_values_after_merge() {
        let response: ConfigResponse = serde_json::from_str(
            r#"{
                "defaults": {
                    "default-null": null,
                    "client-replaces-null": null,
                    "override-removes": "default",
                    "override-wins": "default"
                },
                "overrides": {
                    "override-null-only": null,
                    "override-removes": null,
                    "override-wins": "override"
                }
            }"#,
        )
        .unwrap();
        let client_options = crate::common::Options::from_map(HashMap::from([
            ("client-only".to_string(), "client".to_string()),
            ("client-replaces-null".to_string(), "client".to_string()),
            ("override-removes".to_string(), "client".to_string()),
            ("override-wins".to_string(), "client".to_string()),
        ]));

        let merged = response.merge_options(&client_options);

        assert!(!merged.contains("default-null"));
        assert_eq!(
            merged.get("client-replaces-null"),
            Some(&"client".to_string())
        );
        assert_eq!(merged.get("client-only"), Some(&"client".to_string()));
        assert!(!merged.contains("override-null-only"));
        assert!(!merged.contains("override-removes"));
        assert_eq!(merged.get("override-wins"), Some(&"override".to_string()));
    }

    #[test]
    fn test_config_response_to_options_applies_overrides() {
        let response: ConfigResponse = serde_json::from_str(
            r#"{
                "defaults": {"data-token.enabled": "false"},
                "overrides": {"data-token.enabled": "true"}
            }"#,
        )
        .unwrap();

        let options = response.to_options();

        assert_eq!(options.get("data-token.enabled"), Some(&"true".to_string()));
    }

    #[test]
    fn test_auth_table_query_response_deserialization() {
        // A restricted grant: pin the exact wire field names. A drift in either
        // (`filter` / `columnMasking`) would deserialize to None and silently
        // skip authorization, so this test fails closed against that.
        let resp: AuthTableQueryResponse =
            serde_json::from_str(r#"{"filter":["p0","p1"],"columnMasking":{"ssn":"m0"}}"#).unwrap();
        assert_eq!(resp.filter, Some(vec!["p0".to_string(), "p1".to_string()]));
        assert_eq!(
            resp.column_masking,
            Some(HashMap::from([("ssn".to_string(), "m0".to_string())]))
        );
        assert!(!resp.is_unrestricted());

        // The real server sends `{}` for an unrestricted grant (both fields are
        // `@JsonInclude(NON_NULL)` in Java); it must parse to an empty grant.
        let empty: AuthTableQueryResponse = serde_json::from_str("{}").unwrap();
        assert_eq!(empty, AuthTableQueryResponse::default());
        assert!(empty.is_unrestricted());

        // Present-but-empty collections are also unrestricted.
        let blank: AuthTableQueryResponse =
            serde_json::from_str(r#"{"filter":[],"columnMasking":{}}"#).unwrap();
        assert!(blank.is_unrestricted());
    }

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
