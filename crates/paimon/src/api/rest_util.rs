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

//! REST API utilities.

use std::collections::HashMap;

use crate::common::Options;

/// REST API utility functions.
pub struct RESTUtil;

impl RESTUtil {
    /// URL-encode a string value.
    pub fn encode_string(value: &str) -> String {
        url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
    }

    /// URL-decode a string value.
    pub fn decode_string(encoded: &str) -> String {
        url::form_urlencoded::parse(encoded.as_bytes())
            .map(|(k, _)| k.to_string())
            .collect()
    }

    /// Extract all keys with a given prefix from options, returning a new HashMap with the prefix removed.
    pub fn extract_prefix_map(options: &Options, prefix: &str) -> HashMap<String, String> {
        options.extract_prefix_map(prefix)
    }

    /// Merge two property maps, with `override_properties` taking precedence.
    ///
    /// For keys present in both maps, the value from `override_properties` wins.
    /// `None` values are skipped (only relevant at the map level; individual
    /// entries are always `String`).
    pub fn merge(
        base_properties: Option<&HashMap<String, String>>,
        override_properties: Option<&HashMap<String, String>>,
    ) -> HashMap<String, String> {
        let mut result = HashMap::new();

        if let Some(base) = base_properties {
            for (key, value) in base {
                result.insert(key.clone(), value.clone());
            }
        }

        if let Some(overrides) = override_properties {
            for (key, value) in overrides {
                result.insert(key.clone(), value.clone());
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_string() {
        let original = "hello world=/&?#";
        let encoded = RESTUtil::encode_string(original);
        let decoded = RESTUtil::decode_string(&encoded);
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_extract_prefix_map() {
        let mut options = Options::new();
        options.set("header.Content-Type", "application/json");
        options.set("header.Authorization", "Bearer token");
        options.set("other.key", "value");

        let headers = RESTUtil::extract_prefix_map(&options, "header.");
        assert_eq!(headers.len(), 2);
        assert_eq!(
            headers.get("Content-Type"),
            Some(&"application/json".to_string())
        );
    }
}
