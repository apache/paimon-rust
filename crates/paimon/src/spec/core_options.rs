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
const DATA_EVOLUTION_ENABLED_OPTION: &str = "data-evolution.enabled";
const SOURCE_SPLIT_TARGET_SIZE_OPTION: &str = "source.split.target-size";
const SOURCE_SPLIT_OPEN_FILE_COST_OPTION: &str = "source.split.open-file-cost";
const PARTITION_DEFAULT_NAME_OPTION: &str = "partition.default-name";
const PARTITION_LEGACY_NAME_OPTION: &str = "partition.legacy-name";
const BUCKET_KEY_OPTION: &str = "bucket-key";
const BUCKET_FUNCTION_TYPE_OPTION: &str = "bucket-function.type";
pub const SCAN_SNAPSHOT_ID_OPTION: &str = "scan.snapshot-id";
pub const SCAN_TIMESTAMP_MILLIS_OPTION: &str = "scan.timestamp-millis";
pub const SCAN_TAG_NAME_OPTION: &str = "scan.tag-name";
const DEFAULT_SOURCE_SPLIT_TARGET_SIZE: i64 = 128 * 1024 * 1024;
const DEFAULT_SOURCE_SPLIT_OPEN_FILE_COST: i64 = 4 * 1024 * 1024;
const DEFAULT_PARTITION_DEFAULT_NAME: &str = "__DEFAULT_PARTITION__";

/// Typed accessors for common table options.
///
/// This mirrors pypaimon's `CoreOptions` pattern while staying lightweight.
#[derive(Debug, Clone, Copy)]
pub struct CoreOptions<'a> {
    options: &'a HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimeTravelSelector<'a> {
    TagName(&'a str),
    SnapshotId(i64),
    TimestampMillis(i64),
}

impl<'a> CoreOptions<'a> {
    pub fn new(options: &'a HashMap<String, String>) -> Self {
        Self { options }
    }

    pub fn deletion_vectors_enabled(&self) -> bool {
        self.options
            .get(DELETION_VECTORS_ENABLED_OPTION)
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    pub fn data_evolution_enabled(&self) -> bool {
        self.options
            .get(DATA_EVOLUTION_ENABLED_OPTION)
            .map(|value| value.eq_ignore_ascii_case("true"))
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

    /// The default partition name for null/blank partition values.
    ///
    /// Corresponds to Java `CoreOptions.PARTITION_DEFAULT_NAME`.
    pub fn partition_default_name(&self) -> &str {
        self.options
            .get(PARTITION_DEFAULT_NAME_OPTION)
            .map(String::as_str)
            .unwrap_or(DEFAULT_PARTITION_DEFAULT_NAME)
    }

    /// Whether to use legacy partition name formatting (toString semantics).
    ///
    /// Corresponds to Java `CoreOptions.PARTITION_GENERATE_LEGACY_NAME`.
    /// Default: `true` to match Java Paimon.
    pub fn legacy_partition_name(&self) -> bool {
        self.options
            .get(PARTITION_LEGACY_NAME_OPTION)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(true)
    }

    fn parse_i64_option(&self, option_name: &'static str) -> crate::Result<Option<i64>> {
        match self.options.get(option_name) {
            Some(value) => value
                .parse::<i64>()
                .map(Some)
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Invalid value for {option_name}: '{value}'"),
                    source: Some(Box::new(e)),
                }),
            None => Ok(None),
        }
    }

    /// Raw snapshot id accessor for `scan.snapshot-id`.
    ///
    /// This compatibility accessor is lossy: it returns `None` for absent or
    /// invalid values and does not validate selector conflicts. Internal
    /// time-travel planning should use `try_time_travel_selector`.
    pub fn scan_snapshot_id(&self) -> Option<i64> {
        self.options
            .get(SCAN_SNAPSHOT_ID_OPTION)
            .and_then(|v| v.parse().ok())
    }

    /// Raw timestamp accessor for `scan.timestamp-millis`.
    ///
    /// This compatibility accessor is lossy: it returns `None` for absent or
    /// invalid values and does not validate selector conflicts. Internal
    /// time-travel planning should use `try_time_travel_selector`.
    pub fn scan_timestamp_millis(&self) -> Option<i64> {
        self.options
            .get(SCAN_TIMESTAMP_MILLIS_OPTION)
            .and_then(|v| v.parse().ok())
    }

    /// Raw tag name accessor for `scan.tag-name`.
    ///
    /// This compatibility accessor does not validate selector conflicts.
    /// Internal time-travel planning should use `try_time_travel_selector`.
    pub fn scan_tag_name(&self) -> Option<&'a str> {
        self.options.get(SCAN_TAG_NAME_OPTION).map(String::as_str)
    }

    fn configured_time_travel_selectors(&self) -> Vec<&'static str> {
        let mut selectors = Vec::with_capacity(3);
        if self.options.contains_key(SCAN_TAG_NAME_OPTION) {
            selectors.push(SCAN_TAG_NAME_OPTION);
        }
        if self.options.contains_key(SCAN_SNAPSHOT_ID_OPTION) {
            selectors.push(SCAN_SNAPSHOT_ID_OPTION);
        }
        if self.options.contains_key(SCAN_TIMESTAMP_MILLIS_OPTION) {
            selectors.push(SCAN_TIMESTAMP_MILLIS_OPTION);
        }
        selectors
    }

    /// Validates and normalizes the internal time-travel selector.
    ///
    /// This is the semantic owner for selector mutual exclusion and strict
    /// numeric parsing.
    pub(crate) fn try_time_travel_selector(&self) -> crate::Result<Option<TimeTravelSelector<'a>>> {
        let selectors = self.configured_time_travel_selectors();
        if selectors.len() > 1 {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Only one time-travel selector may be set, found: {}",
                    selectors.join(", ")
                ),
                source: None,
            });
        }

        if let Some(tag_name) = self.scan_tag_name() {
            Ok(Some(TimeTravelSelector::TagName(tag_name)))
        } else if let Some(id) = self.parse_i64_option(SCAN_SNAPSHOT_ID_OPTION)? {
            Ok(Some(TimeTravelSelector::SnapshotId(id)))
        } else if let Some(ts) = self.parse_i64_option(SCAN_TIMESTAMP_MILLIS_OPTION)? {
            Ok(Some(TimeTravelSelector::TimestampMillis(ts)))
        } else {
            Ok(None)
        }
    }

    /// Explicit bucket key columns. If not set, defaults to primary keys for PK tables.
    pub fn bucket_key(&self) -> Option<Vec<String>> {
        self.options
            .get(BUCKET_KEY_OPTION)
            .map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
    }

    /// Whether the bucket function type is the default hash-based function.
    ///
    /// Only the default function (`Math.abs(hash % numBuckets)`) is supported
    /// for bucket predicate pruning. `mod` and `hive` use different algorithms.
    pub fn is_default_bucket_function(&self) -> bool {
        self.options
            .get(BUCKET_FUNCTION_TYPE_OPTION)
            .map(|v| v.eq_ignore_ascii_case("default"))
            .unwrap_or(true)
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

    #[test]
    fn test_partition_options_defaults() {
        let options = HashMap::new();
        let core = CoreOptions::new(&options);
        assert_eq!(core.partition_default_name(), "__DEFAULT_PARTITION__");
        assert!(core.legacy_partition_name());
    }

    #[test]
    fn test_partition_options_custom() {
        let options = HashMap::from([
            (
                PARTITION_DEFAULT_NAME_OPTION.to_string(),
                "NULL_PART".to_string(),
            ),
            (
                PARTITION_LEGACY_NAME_OPTION.to_string(),
                "false".to_string(),
            ),
        ]);
        let core = CoreOptions::new(&options);
        assert_eq!(core.partition_default_name(), "NULL_PART");
        assert!(!core.legacy_partition_name());
    }

    #[test]
    fn test_try_time_travel_selector_rejects_conflicting_selectors() {
        let options = HashMap::from([
            (SCAN_TAG_NAME_OPTION.to_string(), "tag1".to_string()),
            (SCAN_SNAPSHOT_ID_OPTION.to_string(), "7".to_string()),
        ]);
        let core = CoreOptions::new(&options);

        let err = core
            .try_time_travel_selector()
            .expect_err("conflicting selectors should fail");
        match err {
            crate::Error::DataInvalid { message, .. } => {
                assert!(message.contains("Only one time-travel selector may be set"));
                assert!(message.contains(SCAN_TAG_NAME_OPTION));
                assert!(message.contains(SCAN_SNAPSHOT_ID_OPTION));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_try_time_travel_selector_rejects_invalid_numeric_values() {
        let snapshot_options =
            HashMap::from([(SCAN_SNAPSHOT_ID_OPTION.to_string(), "abc".to_string())]);
        let snapshot_core = CoreOptions::new(&snapshot_options);

        let snapshot_err = snapshot_core
            .try_time_travel_selector()
            .expect_err("invalid snapshot id should fail");
        match snapshot_err {
            crate::Error::DataInvalid { message, .. } => {
                assert!(message.contains(SCAN_SNAPSHOT_ID_OPTION));
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let timestamp_options =
            HashMap::from([(SCAN_TIMESTAMP_MILLIS_OPTION.to_string(), "xyz".to_string())]);
        let timestamp_core = CoreOptions::new(&timestamp_options);

        let timestamp_err = timestamp_core
            .try_time_travel_selector()
            .expect_err("invalid timestamp millis should fail");
        match timestamp_err {
            crate::Error::DataInvalid { message, .. } => {
                assert!(message.contains(SCAN_TIMESTAMP_MILLIS_OPTION));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_try_time_travel_selector_normalizes_valid_selector() {
        let tag_options = HashMap::from([(SCAN_TAG_NAME_OPTION.to_string(), "tag1".to_string())]);
        let tag_core = CoreOptions::new(&tag_options);
        assert_eq!(
            tag_core.try_time_travel_selector().expect("tag selector"),
            Some(TimeTravelSelector::TagName("tag1"))
        );

        let snapshot_options =
            HashMap::from([(SCAN_SNAPSHOT_ID_OPTION.to_string(), "7".to_string())]);
        let snapshot_core = CoreOptions::new(&snapshot_options);
        assert_eq!(
            snapshot_core
                .try_time_travel_selector()
                .expect("snapshot selector"),
            Some(TimeTravelSelector::SnapshotId(7))
        );

        let timestamp_options =
            HashMap::from([(SCAN_TIMESTAMP_MILLIS_OPTION.to_string(), "1234".to_string())]);
        let timestamp_core = CoreOptions::new(&timestamp_options);
        assert_eq!(
            timestamp_core
                .try_time_travel_selector()
                .expect("timestamp selector"),
            Some(TimeTravelSelector::TimestampMillis(1234))
        );
    }
}
