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

use serde::{Deserialize, Serialize};

use crate::spec::DataField;

use super::Identifier;

/// A persistent catalog function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Function {
    identifier: Identifier,
    input_params: Option<Vec<DataField>>,
    return_params: Option<Vec<DataField>>,
    deterministic: bool,
    definitions: HashMap<String, FunctionDefinition>,
    comment: Option<String>,
    options: HashMap<String, String>,
}

impl Function {
    /// Create a catalog function from REST metadata.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identifier: Identifier,
        input_params: Option<Vec<DataField>>,
        return_params: Option<Vec<DataField>>,
        deterministic: bool,
        definitions: HashMap<String, FunctionDefinition>,
        comment: Option<String>,
        options: HashMap<String, String>,
    ) -> Self {
        Self {
            identifier,
            input_params,
            return_params,
            deterministic,
            definitions,
            comment,
            options,
        }
    }

    /// Function identifier.
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }

    /// Unqualified function name.
    pub fn name(&self) -> &str {
        self.identifier.object()
    }

    /// Database-qualified function name.
    pub fn full_name(&self) -> String {
        self.identifier.full_name()
    }

    /// Declared input parameters, when present.
    pub fn input_params(&self) -> Option<&[DataField]> {
        self.input_params.as_deref()
    }

    /// Declared return parameters, when present.
    pub fn return_params(&self) -> Option<&[DataField]> {
        self.return_params.as_deref()
    }

    /// Whether the function is deterministic.
    pub fn is_deterministic(&self) -> bool {
        self.deterministic
    }

    /// Definition for an execution engine.
    pub fn definition(&self, dialect: &str) -> Option<&FunctionDefinition> {
        self.definitions.get(dialect)
    }

    /// All engine definitions.
    pub fn definitions(&self) -> &HashMap<String, FunctionDefinition> {
        &self.definitions
    }

    /// Optional function comment.
    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    /// Function options.
    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }
}

/// Engine-specific implementation of a catalog function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum FunctionDefinition {
    /// A function loaded from external resources.
    File {
        #[serde(rename = "fileResources")]
        file_resources: Vec<FunctionFileResource>,
        language: String,
        #[serde(rename = "className")]
        class_name: String,
        #[serde(rename = "functionName")]
        function_name: String,
    },
    /// A scalar SQL expression.
    Sql { definition: String },
    /// A language-specific lambda expression.
    Lambda {
        definition: String,
        language: String,
    },
}

/// External resource referenced by a file function definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionFileResource {
    resource_type: String,
    uri: String,
}

impl FunctionFileResource {
    /// Resource type such as `jar`.
    pub fn resource_type(&self) -> &str {
        &self.resource_type
    }

    /// Resource URI.
    pub fn uri(&self) -> &str {
        &self.uri
    }
}

impl FunctionDefinition {
    /// Return the SQL expression when this is an SQL definition.
    pub fn sql(&self) -> Option<&str> {
        match self {
            Self::Sql { definition } => Some(definition),
            _ => None,
        }
    }
}
