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

use crate::spec::types::DataType;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;

/// The table schema for paimon table.
///
/// Impl References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/TableSchema.java#L47>
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSchema {
    /// version of schema for paimon
    version: i32,
    id: i64,
    fields: Vec<DataField>,
    highest_field_id: i32,
    partition_keys: Vec<String>,
    primary_keys: Vec<String>,
    options: HashMap<String, String>,
    comment: Option<String>,
    time_millis: i64,
}

/// Data field for paimon table.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/DataField.java#L40>
#[serde_as]
#[derive(Debug, Clone, PartialEq, Hash, Eq, Deserialize, Serialize)]
pub struct DataField {
    id: i32,
    name: String,
    #[serde(rename = "type")]
    typ: DataType,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

impl DataField {
    pub fn new(id: i32, name: String, typ: DataType) -> Self {
        Self {
            id,
            name,
            typ,
            description: None,
        }
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.typ
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn with_id(mut self, new_id: i32) -> Self {
        self.id = new_id;
        self
    }

    pub fn with_name(mut self, new_name: String) -> Self {
        self.name = new_name;
        self
    }

    pub fn with_description(mut self, new_description: Option<String>) -> Self {
        self.description = new_description;
        self
    }
}

pub fn escape_identifier(identifier: &str) -> String {
    identifier.replace('"', "\"\"")
}

pub fn escape_single_quotes(text: &str) -> String {
    text.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use crate::spec::IntType;

    use super::*;

    #[test]
    fn test_table_schema_serialize_deserialize() {
        let json_data = r#"
        {
          "version" : 2,
          "id" : 1,
          "fields" : [ {
            "id" : 0,
            "name" : "f0",
            "type" : "INT"
          }, {
            "id" : 1,
            "name" : "f1",
            "type" : "INT"
          }, {
            "id" : 2,
            "name" : "f2",
            "type" : "INT"
          } ],
          "highestFieldId" : 10,
          "partitionKeys" : [ "f0" ],
          "primaryKeys" : [ "f1" ],
          "options" : { },
          "comment" : "",
          "timeMillis" : 1723440320019
        }"#;

        let table_schema: TableSchema =
            serde_json::from_str(json_data).expect("Failed to deserialize TableSchema");

        assert_eq!(table_schema.version, 2);
        assert_eq!(table_schema.id, 1);
        assert_eq!(table_schema.highest_field_id, 10);
        assert_eq!(table_schema.partition_keys, vec!["f0"]);
        assert_eq!(table_schema.primary_keys, vec!["f1"]);
        assert_eq!(table_schema.options, HashMap::new());
        assert_eq!(table_schema.comment, Some("".to_string()));
        assert_eq!(table_schema.time_millis, 1723440320019);

        assert_eq!(table_schema.fields.len(), 3);
        assert_eq!(table_schema.fields[0].id, 0);
        assert_eq!(table_schema.fields[0].name, "f0");
        assert_eq!(
            table_schema.fields[0].data_type(),
            &DataType::Int(IntType::new())
        );

        assert_eq!(table_schema.fields[1].id, 1);
        assert_eq!(table_schema.fields[1].name, "f1");
        assert_eq!(
            table_schema.fields[1].data_type(),
            &DataType::Int(IntType::new())
        );

        assert_eq!(table_schema.fields[2].id, 2);
        assert_eq!(table_schema.fields[2].name, "f2");
        assert_eq!(
            table_schema.fields[2].data_type(),
            &DataType::Int(IntType::new())
        );
    }
}
