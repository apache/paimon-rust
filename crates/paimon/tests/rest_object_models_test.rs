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

use std::collections::HashMap;

use paimon::catalog::{Function, FunctionDefinition, Identifier, View, ViewSchema};
use paimon::spec::DataField;
use serde_json::json;

#[test]
fn deserialize_view_schema_contract() {
    let schema: ViewSchema = serde_json::from_value(json!({
        "fields": [{"id": 0, "name": "id", "type": "INT"}],
        "query": "SELECT id FROM source",
        "dialects": {"datafusion": "SELECT id FROM source"},
        "comment": "active ids",
        "options": {"owner": "analytics"}
    }))
    .unwrap();

    assert_eq!(schema.fields()[0].name(), "id");
    assert_eq!(schema.query_for("datafusion"), "SELECT id FROM source");
    assert_eq!(schema.comment(), Some("active ids"));
    assert_eq!(
        schema.options().get("owner").map(String::as_str),
        Some("analytics")
    );
}

#[test]
fn view_binds_identifier_to_schema() {
    let schema: ViewSchema = serde_json::from_value(json!({
        "fields": [{"id": 0, "name": "id", "type": "INT"}],
        "query": "SELECT id FROM source",
        "dialects": {"datafusion": "SELECT id FROM source WHERE id > 0"},
        "comment": null,
        "options": {}
    }))
    .unwrap();
    let view = View::new(Identifier::new("analytics", "active_ids"), schema);

    assert_eq!(view.name(), "active_ids");
    assert_eq!(view.full_name(), "analytics.active_ids");
    assert_eq!(
        view.query_for("datafusion"),
        "SELECT id FROM source WHERE id > 0"
    );
}

#[test]
fn deserialize_sql_function_definition_contract() {
    let definition: FunctionDefinition = serde_json::from_value(json!({
        "type": "sql",
        "definition": "length * width"
    }))
    .unwrap();

    assert_eq!(definition.sql(), Some("length * width"));
}

#[test]
fn deserialize_lambda_function_definition_contract() {
    let definition: FunctionDefinition = serde_json::from_value(json!({
        "type": "lambda",
        "definition": "(x) -> x + 1",
        "language": "java"
    }))
    .unwrap();

    assert!(matches!(
        definition,
        FunctionDefinition::Lambda { definition, language }
            if definition == "(x) -> x + 1" && language == "java"
    ));
}

#[test]
fn deserialize_file_function_definition_contract() {
    let definition: FunctionDefinition = serde_json::from_value(json!({
        "type": "file",
        "fileResources": [{"resourceType": "jar", "uri": "file:///functions.jar"}],
        "language": "java",
        "className": "com.example.Area",
        "functionName": "eval"
    }))
    .unwrap();

    assert!(matches!(
        definition,
        FunctionDefinition::File {
            file_resources,
            language,
            class_name,
            function_name,
        } if file_resources.len() == 1
            && file_resources[0].resource_type() == "jar"
            && file_resources[0].uri() == "file:///functions.jar"
            && language == "java"
            && class_name == "com.example.Area"
            && function_name == "eval"
    ));
}

#[test]
fn function_binds_identifier_signature_and_definition() {
    let input_params: Vec<DataField> = serde_json::from_value(json!([
        {"id": 0, "name": "length", "type": "DOUBLE"},
        {"id": 1, "name": "width", "type": "DOUBLE"}
    ]))
    .unwrap();
    let return_params: Vec<DataField> = serde_json::from_value(json!([
        {"id": 0, "name": "area", "type": "DOUBLE"}
    ]))
    .unwrap();
    let definitions = HashMap::from([(
        "datafusion".to_string(),
        FunctionDefinition::Sql {
            definition: "length * width".to_string(),
        },
    )]);
    let function = Function::new(
        Identifier::new("analytics", "area"),
        Some(input_params),
        Some(return_params),
        true,
        definitions,
        Some("rectangle area".to_string()),
        HashMap::new(),
    );

    assert_eq!(function.full_name(), "analytics.area");
    assert_eq!(function.input_params().unwrap()[0].name(), "length");
    assert_eq!(function.return_params().unwrap()[0].name(), "area");
    assert_eq!(
        function
            .definition("datafusion")
            .and_then(FunctionDefinition::sql),
        Some("length * width")
    );
}
