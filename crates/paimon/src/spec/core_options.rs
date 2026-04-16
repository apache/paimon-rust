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
const GLOBAL_INDEX_ENABLED_OPTION: &str = "global-index.enabled";
const SOURCE_SPLIT_TARGET_SIZE_OPTION: &str = "source.split.target-size";
const SOURCE_SPLIT_OPEN_FILE_COST_OPTION: &str = "source.split.open-file-cost";
const PARTITION_DEFAULT_NAME_OPTION: &str = "partition.default-name";
const PARTITION_LEGACY_NAME_OPTION: &str = "partition.legacy-name";
const BUCKET_KEY_OPTION: &str = "bucket-key";
const BUCKET_FUNCTION_TYPE_OPTION: &str = "bucket-function.type";
const BUCKET_OPTION: &str = "bucket";
const DEFAULT_BUCKET: i32 = -1;
/// Postpone bucket mode: data is written to `bucket-postpone` directory
/// and is invisible to readers until compaction assigns real bucket numbers.
pub const POSTPONE_BUCKET: i32 = -2;
/// Directory name for postpone bucket files.
pub const POSTPONE_BUCKET_DIR: &str = "bucket-postpone";
const COMMIT_MAX_RETRIES_OPTION: &str = "commit.max-retries";
const COMMIT_TIMEOUT_OPTION: &str = "commit.timeout";
const COMMIT_MIN_RETRY_WAIT_OPTION: &str = "commit.min-retry-wait";
const COMMIT_MAX_RETRY_WAIT_OPTION: &str = "commit.max-retry-wait";
const FILE_COMPRESSION_OPTION: &str = "file.compression";
const FILE_COMPRESSION_ZSTD_LEVEL_OPTION: &str = "file.compression.zstd-level";
const ROW_TRACKING_ENABLED_OPTION: &str = "row-tracking.enabled";
const WRITE_PARQUET_BUFFER_SIZE_OPTION: &str = "write.parquet-buffer-size";
const SEQUENCE_FIELD_OPTION: &str = "sequence.field";
const MERGE_ENGINE_OPTION: &str = "merge-engine";
const CHANGELOG_PRODUCER_OPTION: &str = "changelog-producer";
const ROWKIND_FIELD_OPTION: &str = "rowkind.field";
const DEFAULT_COMMIT_MAX_RETRIES: u32 = 10;
const DEFAULT_COMMIT_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_COMMIT_MIN_RETRY_WAIT_MS: u64 = 1_000;
const DEFAULT_COMMIT_MAX_RETRY_WAIT_MS: u64 = 10_000;
pub const SCAN_TIMESTAMP_MILLIS_OPTION: &str = "scan.timestamp-millis";
pub const SCAN_VERSION_OPTION: &str = "scan.version";
const DEFAULT_SOURCE_SPLIT_TARGET_SIZE: i64 = 128 * 1024 * 1024;
const DEFAULT_SOURCE_SPLIT_OPEN_FILE_COST: i64 = 4 * 1024 * 1024;
const DEFAULT_PARTITION_DEFAULT_NAME: &str = "__DEFAULT_PARTITION__";
const DEFAULT_TARGET_FILE_SIZE: i64 = 256 * 1024 * 1024;
const DEFAULT_WRITE_PARQUET_BUFFER_SIZE: i64 = 256 * 1024 * 1024;

/// Merge engine for primary-key tables.
///
/// Reference: Java `CoreOptions.MergeEngine`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeEngine {
    /// Keep the row with the highest sequence number (default).
    Deduplicate,
    /// Keep the first row for each key (ignore later updates).
    FirstRow,
}

/// Format the bucket directory name for a given bucket number.
/// Returns `"bucket-postpone"` for `POSTPONE_BUCKET` (-2), otherwise `"bucket-{N}"`.
pub fn bucket_dir_name(bucket: i32) -> String {
    if bucket == POSTPONE_BUCKET {
        POSTPONE_BUCKET_DIR.to_string()
    } else {
        format!("bucket-{bucket}")
    }
}

/// Typed accessors for common table options.
///
/// This mirrors pypaimon's `CoreOptions` pattern while staying lightweight.
#[derive(Debug, Clone, Copy)]
pub struct CoreOptions<'a> {
    options: &'a HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TimeTravelSelector<'a> {
    TimestampMillis(i64),
    /// Raw version string from `VERSION AS OF`. Resolved at scan time:
    /// tag name (if tag exists) → snapshot id (if parseable as i64) → error.
    Version(&'a str),
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

    /// Returns the user-specified sequence field names, if configured.
    /// When set, the values of these columns are used as `_SEQUENCE_NUMBER` instead of auto-increment.
    /// Multiple fields can be comma-separated (e.g. `"col_a,col_b"`).
    pub fn sequence_fields(&self) -> Vec<&str> {
        self.options
            .get(SEQUENCE_FIELD_OPTION)
            .map(|s| s.split(',').map(str::trim).collect())
            .unwrap_or_default()
    }

    /// Merge engine for primary-key tables. Default is `Deduplicate`.
    pub fn merge_engine(&self) -> crate::Result<MergeEngine> {
        match self.options.get(MERGE_ENGINE_OPTION) {
            None => Ok(MergeEngine::Deduplicate),
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "deduplicate" => Ok(MergeEngine::Deduplicate),
                "first-row" => Ok(MergeEngine::FirstRow),
                other => Err(crate::Error::Unsupported {
                    message: format!("Unsupported merge-engine: '{other}'"),
                }),
            },
        }
    }

    /// Changelog producer setting. Default is "none".
    pub fn changelog_producer(&self) -> &str {
        self.options
            .get(CHANGELOG_PRODUCER_OPTION)
            .map(String::as_str)
            .unwrap_or("none")
    }

    /// The `rowkind.field` option: a user column whose value encodes the row kind.
    pub fn rowkind_field(&self) -> Option<&str> {
        self.options.get(ROWKIND_FIELD_OPTION).map(String::as_str)
    }

    pub fn data_evolution_enabled(&self) -> bool {
        self.options
            .get(DATA_EVOLUTION_ENABLED_OPTION)
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    pub fn global_index_enabled(&self) -> bool {
        self.options
            .get(GLOBAL_INDEX_ENABLED_OPTION)
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

    fn configured_time_travel_selectors(&self) -> Vec<&'static str> {
        let mut selectors = Vec::with_capacity(2);
        if self.options.contains_key(SCAN_TIMESTAMP_MILLIS_OPTION) {
            selectors.push(SCAN_TIMESTAMP_MILLIS_OPTION);
        }
        if self.options.contains_key(SCAN_VERSION_OPTION) {
            selectors.push(SCAN_VERSION_OPTION);
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

        if let Some(ts) = self.parse_i64_option(SCAN_TIMESTAMP_MILLIS_OPTION)? {
            Ok(Some(TimeTravelSelector::TimestampMillis(ts)))
        } else if let Some(version) = self.options.get(SCAN_VERSION_OPTION).map(String::as_str) {
            Ok(Some(TimeTravelSelector::Version(version)))
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

    pub fn commit_max_retries(&self) -> u32 {
        self.options
            .get(COMMIT_MAX_RETRIES_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_COMMIT_MAX_RETRIES)
    }

    pub fn commit_timeout_ms(&self) -> u64 {
        self.options
            .get(COMMIT_TIMEOUT_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_COMMIT_TIMEOUT_MS)
    }

    pub fn commit_min_retry_wait_ms(&self) -> u64 {
        self.options
            .get(COMMIT_MIN_RETRY_WAIT_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_COMMIT_MIN_RETRY_WAIT_MS)
    }

    pub fn commit_max_retry_wait_ms(&self) -> u64 {
        self.options
            .get(COMMIT_MAX_RETRY_WAIT_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_COMMIT_MAX_RETRY_WAIT_MS)
    }

    pub fn row_tracking_enabled(&self) -> bool {
        self.options
            .get(ROW_TRACKING_ENABLED_OPTION)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Number of buckets for the table. Default is 1.
    pub fn bucket(&self) -> i32 {
        self.options
            .get(BUCKET_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_BUCKET)
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

    /// Target file size for data files. Default is 128MB.
    pub fn target_file_size(&self) -> i64 {
        self.options
            .get("target-file-size")
            .and_then(|v| parse_memory_size(v))
            .unwrap_or(DEFAULT_TARGET_FILE_SIZE)
    }

    /// File compression codec (e.g. "lz4", "zstd", "snappy", "none").
    /// Default is "zstd".
    pub fn file_compression(&self) -> &str {
        self.options
            .get(FILE_COMPRESSION_OPTION)
            .map(String::as_str)
            .unwrap_or("zstd")
    }

    /// Zstd compression level. Only meaningful when `file.compression` is `"zstd"`.
    /// Default is 1 (matching Paimon Java).
    pub fn file_compression_zstd_level(&self) -> i32 {
        self.options
            .get(FILE_COMPRESSION_ZSTD_LEVEL_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(1)
    }

    /// Parquet writer in-progress buffer size limit. Default is 256MB.
    /// When the buffered data exceeds this, the writer flushes the current row group.
    pub fn write_parquet_buffer_size(&self) -> i64 {
        self.options
            .get(WRITE_PARQUET_BUFFER_SIZE_OPTION)
            .and_then(|v| parse_memory_size(v))
            .unwrap_or(DEFAULT_WRITE_PARQUET_BUFFER_SIZE)
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
            (SCAN_VERSION_OPTION.to_string(), "tag1".to_string()),
            (SCAN_TIMESTAMP_MILLIS_OPTION.to_string(), "1234".to_string()),
        ]);
        let core = CoreOptions::new(&options);

        let err = core
            .try_time_travel_selector()
            .expect_err("conflicting selectors should fail");
        match err {
            crate::Error::DataInvalid { message, .. } => {
                assert!(message.contains("Only one time-travel selector may be set"));
                assert!(message.contains(SCAN_VERSION_OPTION));
                assert!(message.contains(SCAN_TIMESTAMP_MILLIS_OPTION));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_try_time_travel_selector_rejects_invalid_numeric_values() {
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
    fn test_commit_options_defaults() {
        let options = HashMap::new();
        let core = CoreOptions::new(&options);
        assert_eq!(core.bucket(), -1);
        assert_eq!(core.commit_max_retries(), 10);
        assert_eq!(core.commit_timeout_ms(), 120_000);
        assert_eq!(core.commit_min_retry_wait_ms(), 1_000);
        assert_eq!(core.commit_max_retry_wait_ms(), 10_000);
        assert!(!core.row_tracking_enabled());
    }

    #[test]
    fn test_commit_options_custom() {
        let options = HashMap::from([
            (BUCKET_OPTION.to_string(), "4".to_string()),
            (COMMIT_MAX_RETRIES_OPTION.to_string(), "20".to_string()),
            (COMMIT_TIMEOUT_OPTION.to_string(), "60000".to_string()),
            (COMMIT_MIN_RETRY_WAIT_OPTION.to_string(), "500".to_string()),
            (COMMIT_MAX_RETRY_WAIT_OPTION.to_string(), "5000".to_string()),
            (ROW_TRACKING_ENABLED_OPTION.to_string(), "true".to_string()),
        ]);
        let core = CoreOptions::new(&options);
        assert_eq!(core.bucket(), 4);
        assert_eq!(core.commit_max_retries(), 20);
        assert_eq!(core.commit_timeout_ms(), 60_000);
        assert_eq!(core.commit_min_retry_wait_ms(), 500);
        assert_eq!(core.commit_max_retry_wait_ms(), 5_000);
        assert!(core.row_tracking_enabled());
    }

    #[test]
    fn test_try_time_travel_selector_normalizes_valid_selector() {
        let timestamp_options =
            HashMap::from([(SCAN_TIMESTAMP_MILLIS_OPTION.to_string(), "1234".to_string())]);
        let timestamp_core = CoreOptions::new(&timestamp_options);
        assert_eq!(
            timestamp_core
                .try_time_travel_selector()
                .expect("timestamp selector"),
            Some(TimeTravelSelector::TimestampMillis(1234))
        );

        let version_options =
            HashMap::from([(SCAN_VERSION_OPTION.to_string(), "my-tag".to_string())]);
        let version_core = CoreOptions::new(&version_options);
        assert_eq!(
            version_core
                .try_time_travel_selector()
                .expect("version selector"),
            Some(TimeTravelSelector::Version("my-tag"))
        );

        let version_num_options =
            HashMap::from([(SCAN_VERSION_OPTION.to_string(), "3".to_string())]);
        let version_num_core = CoreOptions::new(&version_num_options);
        assert_eq!(
            version_num_core
                .try_time_travel_selector()
                .expect("version numeric selector"),
            Some(TimeTravelSelector::Version("3"))
        );
    }

    #[test]
    fn test_write_options_defaults() {
        let options = HashMap::new();
        let core = CoreOptions::new(&options);
        assert_eq!(core.write_parquet_buffer_size(), 256 * 1024 * 1024);
    }

    #[test]
    fn test_write_options_custom() {
        let options = HashMap::from([(
            WRITE_PARQUET_BUFFER_SIZE_OPTION.to_string(),
            "32mb".to_string(),
        )]);
        let core = CoreOptions::new(&options);
        assert_eq!(core.write_parquet_buffer_size(), 32 * 1024 * 1024);
    }
}
