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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{
    catalog::{Function, FunctionDefinition, Identifier, ViewSchema},
    spec::{DataField, Schema, SchemaChange},
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
    fn test_rename_table_request_serialization() {
        let source = Identifier::new("db1".to_string(), "table1".to_string());
        let destination = Identifier::new("db2".to_string(), "table2".to_string());
        let req = RenameTableRequest::new(source, destination);

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"source\""));
        assert!(json.contains("\"destination\""));
    }
}
