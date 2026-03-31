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

//! Configuration options for Paimon catalog.

use std::collections::HashMap;

/// Catalog configuration options.
pub struct CatalogOptions;

impl CatalogOptions {
    /// Catalog URI.
    pub const URI: &'static str = "uri";

    /// Metastore type (default: "filesystem").
    pub const METASTORE: &'static str = "metastore";

    /// Warehouse path.
    pub const WAREHOUSE: &'static str = "warehouse";

    /// Token provider type.
    pub const TOKEN_PROVIDER: &'static str = "token.provider";

    /// Authentication token.
    pub const TOKEN: &'static str = "token";

    /// Data token enabled flag.
    pub const DATA_TOKEN_ENABLED: &'static str = "data-token.enabled";

    /// Prefix for catalog resources.
    pub const PREFIX: &'static str = "prefix";

    // DLF (Data Lake Formation) configuration options

    /// DLF region.
    pub const DLF_REGION: &'static str = "dlf.region";

    /// DLF access key ID.
    pub const DLF_ACCESS_KEY_ID: &'static str = "dlf.access-key-id";

    /// DLF access key secret.
    pub const DLF_ACCESS_KEY_SECRET: &'static str = "dlf.access-key-secret";

    /// DLF security token (optional, for temporary credentials).
    pub const DLF_ACCESS_SECURITY_TOKEN: &'static str = "dlf.security-token";

    /// DLF signing algorithm (default or openapi).
    pub const DLF_SIGNING_ALGORITHM: &'static str = "dlf.signing-algorithm";

    /// DLF token loader type (e.g., "ecs").
    pub const DLF_TOKEN_LOADER: &'static str = "dlf.token-loader";

    /// DLF ECS metadata URL.
    pub const DLF_TOKEN_ECS_METADATA_URL: &'static str = "dlf.token-ecs-metadata-url";

    /// DLF ECS role name.
    pub const DLF_TOKEN_ECS_ROLE_NAME: &'static str = "dlf.token-ecs-role-name";

    /// DLF OSS endpoint override.
    pub const DLF_OSS_ENDPOINT: &'static str = "dlf.oss-endpoint";
}

/// Configuration options container.
///
/// This is a simple key-value store for catalog configuration.
#[derive(Debug, Clone, Default)]
pub struct Options {
    data: HashMap<String, String>,
}

impl Options {
    /// Create a new empty Options instance.
    pub fn new() -> Self {
        Options {
            data: HashMap::new(),
        }
    }

    /// Create Options from a HashMap.
    pub fn from_map(data: HashMap<String, String>) -> Self {
        Options { data }
    }

    /// Get the underlying HashMap.
    pub fn to_map(&self) -> &HashMap<String, String> {
        &self.data
    }

    /// Get a value by key.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    /// Get a value by key with a default.
    pub fn get_or_default(&self, key: &str, default: &str) -> String {
        self.data
            .get(key)
            .cloned()
            .unwrap_or_else(|| default.to_string())
    }

    /// Set a key-value pair.
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.data.insert(key.into(), value.into());
    }

    /// Check if a key exists.
    pub fn contains(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    /// Remove a key.
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.data.remove(key)
    }

    /// Merge another Options into this one, overwriting existing keys.
    pub fn merge(&mut self, other: &Options) {
        for (key, value) in &other.data {
            self.data.insert(key.clone(), value.clone());
        }
    }

    /// Create a copy of this Options.
    pub fn copy(&self) -> Self {
        Options {
            data: self.data.clone(),
        }
    }

    /// Extract all keys with a given prefix, returning a new HashMap with the prefix removed.
    pub fn extract_prefix_map(&self, prefix: &str) -> HashMap<String, String> {
        let mut result = HashMap::new();
        for (key, value) in &self.data {
            if let Some(stripped) = key.strip_prefix(prefix) {
                result.insert(stripped.to_string(), value.clone());
            }
        }
        result
    }
}

impl From<HashMap<String, String>> for Options {
    fn from(data: HashMap<String, String>) -> Self {
        Options { data }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_options_basic() {
        let mut options = Options::new();
        options.set("uri", "http://localhost:8080");
        options.set("warehouse", "/data/warehouse");

        assert_eq!(
            options.get("uri"),
            Some(&"http://localhost:8080".to_string())
        );
        assert_eq!(
            options.get("warehouse"),
            Some(&"/data/warehouse".to_string())
        );
        assert!(!options.contains("nonexistent"));
    }

    #[test]
    fn test_options_extract_prefix() {
        let mut options = Options::new();
        options.set("header.Content-Type", "application/json");
        options.set("header.Authorization", "Bearer token");
        options.set("other.key", "value");

        let headers = options.extract_prefix_map("header.");
        assert_eq!(headers.len(), 2);
        assert_eq!(
            headers.get("Content-Type"),
            Some(&"application/json".to_string())
        );
        assert_eq!(
            headers.get("Authorization"),
            Some(&"Bearer token".to_string())
        );
    }

    #[test]
    fn test_options_merge() {
        let mut options1 = Options::new();
        options1.set("key1", "value1");

        let mut options2 = Options::new();
        options2.set("key2", "value2");
        options2.set("key1", "overwritten");

        options1.merge(&options2);

        assert_eq!(options1.get("key1"), Some(&"overwritten".to_string()));
        assert_eq!(options1.get("key2"), Some(&"value2".to_string()));
    }
}
