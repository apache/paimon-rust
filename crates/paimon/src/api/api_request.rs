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

//! REST API request types for Paimon.
//!
//! This module contains all request structures used in REST API calls.

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

use crate::{
    catalog::{Function, FunctionDefinition, Identifier, ViewSchema},
    spec::{DataField, PartitionStatistics, Schema, SchemaChange},
};

/// Request to create a new database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateDatabaseRequest {
    /// The name of the database to create.
    pub name: String,
    /// Optional configuration options for the database.
    pub options: HashMap<String, String>,
}

impl CreateDatabaseRequest {
    /// Create a new CreateDatabaseRequest.
    pub fn new(name: String, options: HashMap<String, String>) -> Self {
        Self { name, options }
    }
}

/// Request to alter a database's configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlterDatabaseRequest {
    /// Keys to remove from the database options.
    pub removals: Vec<String>,
    /// Key-value pairs to update in the database options.
    pub updates: HashMap<String, String>,
}

impl AlterDatabaseRequest {
    /// Create a new AlterDatabaseRequest.
    pub fn new(removals: Vec<String>, updates: HashMap<String, String>) -> Self {
        Self { removals, updates }
    }
}

/// Request to rename a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenameTableRequest {
    /// The source table identifier.
    pub source: Identifier,
    /// The destination table identifier.
    pub destination: Identifier,
}

impl RenameTableRequest {
    /// Create a new RenameTableRequest.
    pub fn new(source: Identifier, destination: Identifier) -> Self {
        Self {
            source,
            destination,
        }
    }
}

/// Request to create a new table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTableRequest {
    /// The identifier for the table to create.
    pub identifier: Identifier,
    /// The schema definition for the table.
    pub schema: Schema,
}

impl CreateTableRequest {
    /// Create a new CreateTableRequest.
    pub fn new(identifier: Identifier, schema: Schema) -> Self {
        Self { identifier, schema }
    }
}

/// Request to create a persistent view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateViewRequest {
    /// The identifier for the view to create.
    pub identifier: Identifier,
    /// The schema and SQL definitions for the view.
    pub schema: ViewSchema,
}

impl CreateViewRequest {
    /// Create a new create-view request.
    pub fn new(identifier: Identifier, schema: ViewSchema) -> Self {
        Self { identifier, schema }
    }
}

/// Request to create a persistent function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateFunctionRequest {
    /// Unqualified function name.
    pub name: String,
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
    pub options: HashMap<String, String>,
}

impl CreateFunctionRequest {
    /// Create a request from a catalog function.
    pub fn from_function(function: &Function) -> Self {
        Self {
            name: function.name().to_string(),
            input_params: function.input_params().map(<[_]>::to_vec),
            return_params: function.return_params().map(<[_]>::to_vec),
            deterministic: function.is_deterministic(),
            definitions: function.definitions().clone(),
            comment: function.comment().map(str::to_string),
            options: function.options().clone(),
        }
    }
}

/// Request to alter a table's schema.
///
/// Wire-compatible with Java Paimon's `AlterTableRequest` (`{"changes": [...]}`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlterTableRequest {
    /// The ordered list of schema changes to apply.
    pub changes: Vec<SchemaChange>,
}

impl AlterTableRequest {
    /// Create a new AlterTableRequest.
    pub fn new(changes: Vec<SchemaChange>) -> Self {
        Self { changes }
    }
}

/// Request to create table partitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatePartitionsRequest {
    /// Partition specs to register.
    pub partition_specs: Vec<HashMap<String, String>>,
    /// Whether already registered partitions should be ignored.
    #[serde(
        default = "default_true",
        deserialize_with = "deserialize_null_to_true"
    )]
    pub ignore_if_exists: bool,
    /// Statistics reported for the partitions, matched to them by spec; absent unless reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_statistics: Option<Vec<PartitionStatistics>>,
    /// Whether the reported statistics replace what the catalog holds rather than add to it;
    /// present only together with `partition_statistics`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace_statistics: Option<bool>,
}

impl CreatePartitionsRequest {
    /// Create a request to register partitions.
    pub fn new(partition_specs: Vec<HashMap<String, String>>, ignore_if_exists: bool) -> Self {
        Self {
            partition_specs,
            ignore_if_exists,
            partition_statistics: None,
            replace_statistics: None,
        }
    }

    /// Report statistics for the partitions in the same request.
    pub fn with_statistics(mut self, statistics: Vec<PartitionStatistics>, replace: bool) -> Self {
        self.partition_statistics = Some(statistics);
        self.replace_statistics = Some(replace);
        self
    }
}

/// Request to drop (unregister) table partitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DropPartitionsRequest {
    /// Partition specs to unregister.
    pub partition_specs: Vec<HashMap<String, String>>,
    /// Whether missing partitions should be ignored.
    #[serde(
        default = "default_true",
        deserialize_with = "deserialize_null_to_true"
    )]
    pub ignore_if_not_exists: bool,
}

impl DropPartitionsRequest {
    /// Create a request to unregister partitions.
    pub fn new(partition_specs: Vec<HashMap<String, String>>, ignore_if_not_exists: bool) -> Self {
        Self {
            partition_specs,
            ignore_if_not_exists,
        }
    }
}

/// Request to look up registered partitions by their complete specs.
///
/// Wire-compatible with Java `ListPartitionsByNamesRequest`, whose field is `specs` rather than
/// the `partitionSpecs` the create and drop requests use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListPartitionsByNamesRequest {
    /// Complete partition specs to look up.
    pub specs: Vec<HashMap<String, String>>,
}

impl ListPartitionsByNamesRequest {
    /// Create a request to look up partitions by their complete specs.
    pub fn new(specs: Vec<HashMap<String, String>>) -> Self {
        Self { specs }
    }
}

/// Request to list partitions matching a partition predicate.
///
/// Wire-compatible with Java `ListPartitionsByFilterRequest`: the predicate travels as its JSON
/// text inside the request body, and absent fields are left out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListPartitionsByFilterRequest {
    /// Partition predicate in the REST catalog predicate JSON format.
    pub filter: String,
    /// Partition-name pattern combined with the filter as a conjunction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_name_pattern: Option<String>,
    /// Maximum number of partitions in one page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_results: Option<u32>,
    /// Token of the page to return.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

impl ListPartitionsByFilterRequest {
    /// Create a request for one page of partitions matching `filter`.
    pub fn new(
        filter: String,
        partition_name_pattern: Option<String>,
        max_results: Option<u32>,
        page_token: Option<String>,
    ) -> Self {
        Self {
            filter,
            partition_name_pattern,
            max_results,
            page_token,
        }
    }
}

fn default_true() -> bool {
    true
}

fn deserialize_null_to_true<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<bool>::deserialize(deserializer)?.unwrap_or(true))
}

/// Request for auth table query: the projected columns of the query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthTableQueryRequest {
    /// Projected column names; `None` means all columns.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub select: Option<Vec<String>>,
}

impl AuthTableQueryRequest {
    /// Create a new AuthTableQueryRequest.
    pub fn new(select: Option<Vec<String>>) -> Self {
        Self { select }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_database_request_serialization() {
        let mut options = HashMap::new();
        options.insert("key".to_string(), "value".to_string());
        let req = CreateDatabaseRequest::new("test_db".to_string(), options);

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"name\":\"test_db\""));
        assert!(json.contains("\"options\""));
    }

    #[test]
    fn test_alter_database_request_serialization() {
        let mut updates = HashMap::new();
        updates.insert("key".to_string(), "new_value".to_string());
        let req = AlterDatabaseRequest::new(vec!["old_key".to_string()], updates);

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"removals\":[\"old_key\"]"));
        assert!(json.contains("\"updates\""));
    }

    #[test]
    fn test_auth_table_query_request_serialization() {
        let req = AuthTableQueryRequest::new(Some(vec!["a".to_string(), "b".to_string()]));
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"select":["a","b"]}"#
        );
        // `None` omits the key entirely (matches the server's optional field).
        let req = AuthTableQueryRequest::new(None);
        assert_eq!(serde_json::to_string(&req).unwrap(), "{}");
    }

    #[test]
    fn test_create_partitions_request_serialization() {
        let req = CreatePartitionsRequest::new(
            vec![HashMap::from([
                ("dt".to_string(), "2026-07-22".to_string()),
                ("hour".to_string(), "10".to_string()),
            ])],
            false,
        );

        assert_eq!(
            serde_json::to_value(req).unwrap(),
            serde_json::json!({
                "partitionSpecs": [{
                    "dt": "2026-07-22",
                    "hour": "10"
                }],
                "ignoreIfExists": false
            })
        );
    }

    #[test]
    fn test_create_partitions_request_sends_statistics_only_when_reported() {
        let spec = HashMap::from([("dt".to_string(), "2026-07-22".to_string())]);
        let request = CreatePartitionsRequest::new(vec![spec.clone()], true).with_statistics(
            vec![PartitionStatistics {
                spec: spec.clone(),
                record_count: -1,
                file_size_in_bytes: 1024,
                file_count: 2,
                last_file_creation_time: 1_700_000_000_000,
                total_buckets: -1,
            }],
            true,
        );

        assert_eq!(
            serde_json::to_value(request).unwrap(),
            serde_json::json!({
                "partitionSpecs": [{"dt": "2026-07-22"}],
                "ignoreIfExists": true,
                "partitionStatistics": [{
                    "spec": {"dt": "2026-07-22"},
                    "recordCount": -1,
                    "fileSizeInBytes": 1024,
                    "fileCount": 2,
                    "lastFileCreationTime": 1_700_000_000_000_i64,
                    "totalBuckets": -1
                }],
                "replaceStatistics": true
            })
        );
    }

    #[test]
    fn test_drop_partitions_request_serialization() {
        let req = DropPartitionsRequest::new(
            vec![HashMap::from([(
                "dt".to_string(),
                "2026-07-22".to_string(),
            )])],
            false,
        );

        assert_eq!(
            serde_json::to_value(req).unwrap(),
            serde_json::json!({
                "partitionSpecs": [{"dt": "2026-07-22"}],
                "ignoreIfNotExists": false
            })
        );
    }

    #[test]
    fn test_partition_request_flags_default_to_true() {
        for json in [
            serde_json::json!({"partitionSpecs": []}),
            serde_json::json!({"partitionSpecs": [], "ignoreIfExists": null}),
        ] {
            let request: CreatePartitionsRequest = serde_json::from_value(json).unwrap();
            assert!(request.ignore_if_exists);
        }
        for json in [
            serde_json::json!({"partitionSpecs": []}),
            serde_json::json!({"partitionSpecs": [], "ignoreIfNotExists": null}),
        ] {
            let request: DropPartitionsRequest = serde_json::from_value(json).unwrap();
            assert!(request.ignore_if_not_exists);
        }
    }

    #[test]
    fn test_list_partitions_by_names_request_uses_the_specs_field() {
        let request = ListPartitionsByNamesRequest::new(vec![HashMap::from([(
            "dt".to_string(),
            "2026-07-22".to_string(),
        )])]);

        assert_eq!(
            serde_json::to_value(request).unwrap(),
            serde_json::json!({"specs": [{"dt": "2026-07-22"}]})
        );
    }

    #[test]
    fn test_list_partitions_by_filter_request_leaves_out_absent_fields() {
        let request = ListPartitionsByFilterRequest::new("{}".to_string(), None, None, None);
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            serde_json::json!({"filter": "{}"})
        );

        let request = ListPartitionsByFilterRequest::new(
            "{}".to_string(),
            Some("dt=a/%".to_string()),
            Some(1000),
            Some("next".to_string()),
        );
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            serde_json::json!({
                "filter": "{}",
                "partitionNamePattern": "dt=a/%",
                "maxResults": 1000,
                "pageToken": "next"
            })
        );
    }

    #[test]
    fn test_rename_table_request_serialization() {
        let source = Identifier::new("db1".to_string(), "table1".to_string());
        let destination = Identifier::new("db2".to_string(), "table2".to_string());
        let req = RenameTableRequest::new(source, destination);

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"source\""));
        assert!(json.contains("\"destination\""));
    }
}
