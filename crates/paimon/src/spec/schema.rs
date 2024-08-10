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
    fn test_create_data_field() {
        let id = 1;
        let name = "field1".to_string();
        let typ = DataType::Int(IntType::new());
        let description = "test description".to_string();

        let data_field = DataField::new(id, name.clone(), typ.clone())
            .with_description(Some(description.clone()));

        assert_eq!(data_field.id(), id);
        assert_eq!(data_field.name(), name);
        assert_eq!(data_field.data_type(), &typ);
        assert_eq!(data_field.description(), Some(description).as_deref());
    }

    #[test]
    fn test_new_id() {
        let d_type = DataType::Int(IntType::new());
        let new_data_field = DataField::new(1, "field1".to_string(), d_type.clone()).with_id(2);

        assert_eq!(new_data_field.id(), 2);
        assert_eq!(new_data_field.name(), "field1");
        assert_eq!(new_data_field.data_type(), &d_type);
        assert_eq!(new_data_field.description(), None);
    }

    #[test]
    fn test_new_name() {
        let d_type = DataType::Int(IntType::new());
        let new_data_field =
            DataField::new(1, "field1".to_string(), d_type.clone()).with_name("field2".to_string());

        assert_eq!(new_data_field.id(), 1);
        assert_eq!(new_data_field.name(), "field2");
        assert_eq!(new_data_field.data_type(), &d_type);
        assert_eq!(new_data_field.description(), None);
    }

    #[test]
    fn test_new_description() {
        let d_type = DataType::Int(IntType::new());
        let new_data_field = DataField::new(1, "field1".to_string(), d_type.clone())
            .with_description(Some("new description".to_string()));

        assert_eq!(new_data_field.id(), 1);
        assert_eq!(new_data_field.name(), "field1");
        assert_eq!(new_data_field.data_type(), &d_type);
        assert_eq!(new_data_field.description(), Some("new description"));
    }

    #[test]
    fn test_escape_identifier() {
        let escaped_identifier = escape_identifier("\"identifier\"");
        assert_eq!(escaped_identifier, "\"\"identifier\"\"");
    }

    #[test]
    fn test_escape_single_quotes() {
        let escaped_text = escape_single_quotes("text with 'single' quotes");
        assert_eq!(escaped_text, "text with ''single'' quotes");
    }
}
