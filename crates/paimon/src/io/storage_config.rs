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

pub(super) fn normalize_storage_config(
    props: HashMap<String, String>,
    config_prefixes: &[&str],
    canonical_prefix: &str,
    mirrored_keys: &[(&str, &str)],
) -> HashMap<String, String> {
    let mut result = HashMap::new();

    for prefix in config_prefixes {
        for (key, value) in &props {
            if let Some(suffix) = key.strip_prefix(prefix) {
                result.insert(format!("{canonical_prefix}{suffix}"), value.clone());
            }
        }
    }

    let mirrored_additions: Vec<(String, String)> = mirrored_keys
        .iter()
        .flat_map(|(a, b)| {
            let mut pairs = Vec::new();

            if !result.contains_key(*b) {
                if let Some(v) = result.get(*a) {
                    pairs.push((b.to_string(), v.clone()));
                }
            }
            if !result.contains_key(*a) {
                if let Some(v) = result.get(*b) {
                    pairs.push((a.to_string(), v.clone()));
                }
            }
            pairs
        })
        .collect();

    for (k, v) in mirrored_additions {
        result.insert(k, v);
    }

    result
}
