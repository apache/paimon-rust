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

//! Database structure for Apache Paimon catalogs.

use std::collections::HashMap;

/// Structure representing a database in a Paimon catalog.
#[derive(Debug, Clone, PartialEq)]
pub struct Database {
    /// Database name.
    pub name: String,
    /// Database options/properties.
    pub options: HashMap<String, String>,
    /// Optional comment describing the database.
    pub comment: Option<String>,
}

impl Database {
    /// Create a new Database.
    pub fn new(name: String, options: HashMap<String, String>, comment: Option<String>) -> Self {
        Self {
            name,
            options,
            comment,
        }
    }
}
