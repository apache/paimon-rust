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

const DELETION_VECTORS_ENABLED_OPTION: &str = "deletion-vectors.enabled";
const SOURCE_SPLIT_TARGET_SIZE_OPTION: &str = "source.split.target-size";
const SOURCE_SPLIT_OPEN_FILE_COST_OPTION: &str = "source.split.open-file-cost";
const DEFAULT_SOURCE_SPLIT_TARGET_SIZE: i64 = 128 * 1024 * 1024;
const DEFAULT_SOURCE_SPLIT_OPEN_FILE_COST: i64 = 4 * 1024 * 1024;

/// Typed accessors for common table options.
///
/// This mirrors pypaimon's `CoreOptions` pattern while staying lightweight.
#[derive(Debug, Clone, Copy)]
pub struct CoreOptions<'a> {
    options: &'a HashMap<String, String>,
}

impl<'a> CoreOptions<'a> {
    pub fn new(options: &'a HashMap<String, String>) -> Self {
        Self { options }
    }

    pub fn deletion_vectors_enabled(&self) -> bool {
        self.options
            .get(DELETION_VECTORS_ENABLED_OPTION)
            .map(|value| matches!(value.to_ascii_lowercase().as_str(), "true"))
            .unwrap_or(false)
    }

    pub fn source_split_target_size(&self) -> i64 {
        self.options
            .get(SOURCE_SPLIT_TARGET_SIZE_OPTION)
            .and_then(|value| parse_memory_size(value))
            .unwrap_or(DEFAULT_SOURCE_SPLIT_TARGET_SIZE)
    }

    pub fn source_split_open_file_cost(&self) -> i64 {
        self.options
            .get(SOURCE_SPLIT_OPEN_FILE_COST_OPTION)
            .and_then(|value| parse_memory_size(value))
            .unwrap_or(DEFAULT_SOURCE_SPLIT_OPEN_FILE_COST)
    }
}

/// Parse a memory size string to bytes using binary (1024-based) semantics.
///
/// Supports formats like `128 mb`, `128mb`, `4 gb`, `1024` (plain bytes).
/// Uses binary units: `kb` = 1024, `mb` = 1024², `gb` = 1024³, matching Java Paimon's `MemorySize`.
///
/// NOTE: Java Paimon's `MemorySize` also accepts long unit names such as `bytes`,
/// `kibibytes`, `mebibytes`, `gibibytes`, and `tebibytes`. This implementation
/// only supports short units (`b`, `kb`, `mb`, `gb`, `tb`), which covers all practical usage.
fn parse_memory_size(value: &str) -> Option<i64> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }

    let pos = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let (num_str, unit_str) = value.split_at(pos);
    let num: i64 = num_str.trim().parse().ok()?;
    let multiplier = match unit_str.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kb" | "k" => 1024,
        "mb" | "m" => 1024 * 1024,
        "gb" | "g" => 1024 * 1024 * 1024,
        "tb" | "t" => 1024 * 1024 * 1024 * 1024,
        _ => return None,
    };
    Some(num * multiplier)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_split_defaults() {
        let options = HashMap::new();
        let core_options = CoreOptions::new(&options);

        assert_eq!(core_options.source_split_target_size(), 128 * 1024 * 1024);
        assert_eq!(core_options.source_split_open_file_cost(), 4 * 1024 * 1024);
    }

    #[test]
    fn test_source_split_custom_values() {
        let options = HashMap::from([
            (
                SOURCE_SPLIT_TARGET_SIZE_OPTION.to_string(),
                "256 mb".to_string(),
            ),
            (
                SOURCE_SPLIT_OPEN_FILE_COST_OPTION.to_string(),
                "8 mb".to_string(),
            ),
        ]);
        let core_options = CoreOptions::new(&options);

        assert_eq!(core_options.source_split_target_size(), 256 * 1024 * 1024);
        assert_eq!(core_options.source_split_open_file_cost(), 8 * 1024 * 1024);
    }

    #[test]
    fn test_parse_memory_size() {
        assert_eq!(parse_memory_size("1024"), Some(1024));
        assert_eq!(parse_memory_size("128 mb"), Some(128 * 1024 * 1024));
        assert_eq!(parse_memory_size("128mb"), Some(128 * 1024 * 1024));
        assert_eq!(parse_memory_size("4MB"), Some(4 * 1024 * 1024));
        assert_eq!(parse_memory_size("1 gb"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory_size("1024 kb"), Some(1024 * 1024));
        assert_eq!(parse_memory_size("100 b"), Some(100));
        assert_eq!(parse_memory_size(""), None);
        assert_eq!(parse_memory_size("abc"), None);
    }
}
