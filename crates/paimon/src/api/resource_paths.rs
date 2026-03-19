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

/// Resource paths for REST API endpoints.
#[derive(Clone)]
pub struct ResourcePaths {
    base_path: String,
}

impl ResourcePaths {
    const V1: &'static str = "v1";
    const DATABASES: &'static str = "databases";

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_paths_basic() {
        let paths = ResourcePaths::new("");
        assert_eq!(paths.databases(), "/v1/databases");
    }

    #[test]
    fn test_resource_paths_with_prefix() {
        let paths = ResourcePaths::new("my-catalog");
        assert_eq!(paths.databases(), "/v1/my-catalog/databases");
    }

    #[test]
    fn test_config_path() {
        assert_eq!(ResourcePaths::config(), "/v1/config");
    }
}
