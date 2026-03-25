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

//! REST API resource paths.

use crate::common::{CatalogOptions, Options};

use super::rest_util::RESTUtil;

/// Resource paths for REST API endpoints.
#[derive(Clone)]
pub struct ResourcePaths {
    base_path: String,
}

impl ResourcePaths {
    const V1: &'static str = "v1";
    const DATABASES: &'static str = "databases";
    const TABLES: &'static str = "tables";
    const TABLE_DETAILS: &'static str = "table-details";

    /// Create a new ResourcePaths with the given prefix.
    pub fn new(prefix: &str) -> Self {
        let base_path = if prefix.is_empty() {
            format!("/{}", Self::V1)
        } else {
            format!("/{}/{}", Self::V1, prefix.trim_matches('/'))
        };
        ResourcePaths { base_path }
    }

    /// Create ResourcePaths from catalog options.
    pub fn for_catalog_properties(options: &Options) -> Self {
        let prefix = options
            .get(CatalogOptions::PREFIX)
            .map(|s| s.as_str())
            .unwrap_or("");
        Self::new(prefix)
    }

    /// Get the base path.
    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    /// Get the config endpoint path.
    pub fn config() -> String {
        format!("/{}/config", Self::V1)
    }

    /// Get the databases endpoint path.
    pub fn databases(&self) -> String {
        format!("{}/{}", self.base_path, Self::DATABASES)
    }

    /// Get a specific database endpoint path.
    pub fn database(&self, name: &str) -> String {
        format!(
            "{}/{}/{}",
            self.base_path,
            Self::DATABASES,
            RESTUtil::encode_string(name)
        )
    }

    /// Get the tables endpoint path.
    pub fn tables(&self, database_name: Option<&str>) -> String {
        if let Some(db_name) = database_name {
            format!(
                "{}/{}/{}/{}",
                self.base_path,
                Self::DATABASES,
                RESTUtil::encode_string(db_name),
                Self::TABLES
            )
        } else {
            format!("{}/{}", self.base_path, Self::TABLES)
        }
    }

    /// Get a specific table endpoint path.
    pub fn table(&self, database_name: &str, table_name: &str) -> String {
        format!(
            "{}/{}/{}/{}/{}",
            self.base_path,
            Self::DATABASES,
            RESTUtil::encode_string(database_name),
            Self::TABLES,
            RESTUtil::encode_string(table_name)
        )
    }

    /// Get the table details endpoint path.
    pub fn table_details(&self, database_name: &str) -> String {
        format!(
            "{}/{}/{}/{}",
            self.base_path,
            Self::DATABASES,
            RESTUtil::encode_string(database_name),
            Self::TABLE_DETAILS
        )
    }

    /// Get the table token endpoint path.
    pub fn table_token(&self, database_name: &str, table_name: &str) -> String {
        format!(
            "{}/{}/{}/{}/{}/token",
            self.base_path,
            Self::DATABASES,
            RESTUtil::encode_string(database_name),
            Self::TABLES,
            RESTUtil::encode_string(table_name)
        )
    }

    /// Get the rename table endpoint path.
    pub fn rename_table(&self) -> String {
        format!("{}/{}/rename", self.base_path, Self::TABLES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_paths_basic() {
        let paths = ResourcePaths::new("");
        assert_eq!(paths.databases(), "/v1/databases");
        assert_eq!(paths.tables(None), "/v1/tables");
    }

    #[test]
    fn test_resource_paths_with_prefix() {
        let paths = ResourcePaths::new("my-catalog");
        assert_eq!(paths.databases(), "/v1/my-catalog/databases");
        assert_eq!(
            paths.database("test-db"),
            "/v1/my-catalog/databases/test-db"
        );
    }

    #[test]
    fn test_resource_paths_table() {
        let paths = ResourcePaths::new("");
        let table_path = paths.table("my-db", "my-table");
        assert!(table_path.contains("my-db"));
        assert!(table_path.contains("my-table"));
    }

    #[test]
    fn test_config_path() {
        assert_eq!(ResourcePaths::config(), "/v1/config");
    }
}
