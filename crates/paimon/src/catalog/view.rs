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

/// A persistent catalog view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct View {
    identifier: Identifier,
    schema: ViewSchema,
}

impl View {
    /// Create a catalog view from its identifier and stored schema.
    pub fn new(identifier: Identifier, schema: ViewSchema) -> Self {
        Self { identifier, schema }
    }

    /// View identifier.
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }

    /// Unqualified view name.
    pub fn name(&self) -> &str {
        self.identifier.object()
    }

    /// Database-qualified view name.
    pub fn full_name(&self) -> String {
        self.identifier.full_name()
    }

    /// Stored schema and SQL representations.
    pub fn schema(&self) -> &ViewSchema {
        &self.schema
    }

    /// SQL representation for a dialect, falling back to the default query.
    pub fn query_for(&self, dialect: &str) -> &str {
        self.schema.query_for(dialect)
    }
}

/// Schema and SQL representations stored for a catalog view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ViewSchema {
    fields: Vec<DataField>,
    query: String,
    #[serde(default)]
    dialects: HashMap<String, String>,
    comment: Option<String>,
    #[serde(default)]
    options: HashMap<String, String>,
}

impl ViewSchema {
    /// Declared output fields of the view.
    pub fn fields(&self) -> &[DataField] {
        &self.fields
    }

    /// Default SQL representation of the view.
    pub fn query(&self) -> &str {
        &self.query
    }

    /// SQL representation for a dialect, falling back to the default query.
    pub fn query_for(&self, dialect: &str) -> &str {
        self.dialects
            .get(dialect)
            .map(String::as_str)
            .unwrap_or(&self.query)
    }

    /// Optional view comment.
    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    /// View options.
    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }
}
