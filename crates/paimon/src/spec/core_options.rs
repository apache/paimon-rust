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

use std::collections::{HashMap, HashSet};

const DELETION_VECTORS_ENABLED_OPTION: &str = "deletion-vectors.enabled";
pub(crate) const QUERY_AUTH_ENABLED_OPTION: &str = "query-auth.enabled";
const DATA_EVOLUTION_ENABLED_OPTION: &str = "data-evolution.enabled";
const GLOBAL_INDEX_ENABLED_OPTION: &str = "global-index.enabled";
const GLOBAL_INDEX_SEARCH_MODE_OPTION: &str = "global-index.search-mode";
const GLOBAL_INDEX_ROW_COUNT_PER_SHARD_OPTION: &str = "global-index.row-count-per-shard";
const GLOBAL_INDEX_COLUMN_UPDATE_ACTION_OPTION: &str = "global-index.column-update-action";
const SORTED_INDEX_RECORDS_PER_RANGE_OPTION: &str = "sorted-index.records-per-range";
const BTREE_INDEX_FALLBACK_SCAN_MAX_SIZE_OPTION: &str = "btree-index.fallback-scan-max-size";
const BITMAP_INDEX_FALLBACK_SCAN_MAX_SIZE_OPTION: &str = "bitmap-index.fallback-scan-max-size";
const SOURCE_SPLIT_TARGET_SIZE_OPTION: &str = "source.split.target-size";
const SOURCE_SPLIT_OPEN_FILE_COST_OPTION: &str = "source.split.open-file-cost";
const PARTITION_DEFAULT_NAME_OPTION: &str = "partition.default-name";
const PARTITION_LEGACY_NAME_OPTION: &str = "partition.legacy-name";
pub(crate) const BUCKET_KEY_OPTION: &str = "bucket-key";
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
const FILE_FORMAT_OPTION: &str = "file.format";
const VECTOR_FILE_FORMAT_OPTION: &str = "vector.file.format";
const VECTOR_TARGET_FILE_SIZE_OPTION: &str = "vector.target-file-size";
const CHANGELOG_FILE_PREFIX_OPTION: &str = "changelog-file.prefix";
const CHANGELOG_FILE_FORMAT_OPTION: &str = "changelog-file.format";
const CHANGELOG_FILE_COMPRESSION_OPTION: &str = "changelog-file.compression";
const CHANGELOG_FILE_STATS_MODE_OPTION: &str = "changelog-file.stats-mode";
const ROW_TRACKING_ENABLED_OPTION: &str = "row-tracking.enabled";
const MANIFEST_COMPRESSION_OPTION: &str = "manifest.compression";
const MANIFEST_TARGET_FILE_SIZE_OPTION: &str = "manifest.target-file-size";
const MANIFEST_TARGET_SIZE_OPTION: &str = "manifest.target-size";
const MANIFEST_MERGE_MIN_COUNT_OPTION: &str = "manifest.merge-min-count";
const WRITE_PARQUET_BUFFER_SIZE_OPTION: &str = "write.parquet-buffer-size";
pub(crate) const SEQUENCE_FIELD_OPTION: &str = "sequence.field";
pub(crate) const DISABLE_EXPLICIT_TYPE_CASTING_OPTION: &str = "disable-explicit-type-casting";
pub(crate) const DISABLE_ALTER_COLUMN_NULL_TO_NOT_NULL_OPTION: &str =
    "alter-column-null-to-not-null.disabled";
const MERGE_ENGINE_OPTION: &str = "merge-engine";
const CHANGELOG_PRODUCER_OPTION: &str = "changelog-producer";
const ROWKIND_FIELD_OPTION: &str = "rowkind.field";
const DEFAULT_COMMIT_MAX_RETRIES: u32 = 10;
const DEFAULT_COMMIT_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_COMMIT_MIN_RETRY_WAIT_MS: u64 = 1_000;
const DEFAULT_COMMIT_MAX_RETRY_WAIT_MS: u64 = 10_000;
pub const SCAN_TIMESTAMP_MILLIS_OPTION: &str = "scan.timestamp-millis";
pub const SCAN_VERSION_OPTION: &str = "scan.version";
pub const SCAN_SNAPSHOT_ID_OPTION: &str = "scan.snapshot-id";
pub const SCAN_TAG_NAME_OPTION: &str = "scan.tag-name";
const INCREMENTAL_BETWEEN_OPTION: &str = "incremental-between";
const INCREMENTAL_BETWEEN_TIMESTAMP_OPTION: &str = "incremental-between-timestamp";
const INCREMENTAL_BETWEEN_SCAN_MODE_OPTION: &str = "incremental-between-scan-mode";
const SCAN_WATERMARK_OPTION: &str = "scan.watermark";
const SCAN_MODE_OPTION: &str = "scan.mode";
const DEFAULT_SOURCE_SPLIT_TARGET_SIZE: i64 = 128 * 1024 * 1024;
const DEFAULT_SOURCE_SPLIT_OPEN_FILE_COST: i64 = 4 * 1024 * 1024;
const DEFAULT_MANIFEST_COMPRESSION: &str = "zstd";
const DEFAULT_MANIFEST_TARGET_FILE_SIZE: i64 = 8 * 1024 * 1024;
const DEFAULT_MANIFEST_MERGE_MIN_COUNT: usize = 30;
const DEFAULT_PARTITION_DEFAULT_NAME: &str = "__DEFAULT_PARTITION__";
const DEFAULT_CHANGELOG_FILE_PREFIX: &str = "changelog-";
const DEFAULT_TARGET_FILE_SIZE: i64 = 256 * 1024 * 1024;
const DEFAULT_WRITE_PARQUET_BUFFER_SIZE: i64 = 256 * 1024 * 1024;
const DYNAMIC_BUCKET_TARGET_ROW_NUM_OPTION: &str = "dynamic-bucket.target-row-num";
const DEFAULT_DYNAMIC_BUCKET_TARGET_ROW_NUM: i64 = 200_000;
const DEFAULT_GLOBAL_INDEX_ROW_COUNT_PER_SHARD: i64 = 100_000;
const DEFAULT_GLOBAL_INDEX_FALLBACK_SCAN_MAX_SIZE: i64 = 256 * 1024 * 1024;
const BLOB_AS_DESCRIPTOR_OPTION: &str = "blob-as-descriptor";
const BLOB_DESCRIPTOR_FIELD_OPTION: &str = "blob-descriptor-field";

/// Merge engine for primary-key tables.
///
/// Reference: Java `CoreOptions.MergeEngine`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeEngine {
    /// Keep the row with the highest sequence number (default).
    Deduplicate,
    /// Merge same-key rows field-by-field, usually keeping non-null updates.
    PartialUpdate,
    /// Keep the first row for each key (ignore later updates).
    FirstRow,
    /// Apply per-field aggregate functions across rows sharing the same key.
    Aggregation,
}

/// Changelog producer for table writes.
///
/// Reference: Java `CoreOptions.ChangelogProducer`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangelogProducer {
    /// No changelog file.
    None,
    /// Double write input rows to changelog files.
    Input,
    /// Generate changelog files during full compaction.
    FullCompaction,
    /// Generate changelog files through lookup compaction.
    Lookup,
}

/// Action when a partial-column update touches globally indexed columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GlobalIndexColumnUpdateAction {
    ThrowError,
    DropPartitionIndex,
}

/// Search mode for global index queries.
///
/// Reference: Java `CoreOptions.GlobalIndexSearchMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GlobalIndexSearchMode {
    /// Only search indexed data.
    Fast,
    /// Use snapshot `next_row_id` and global index coverage to detect missing row IDs.
    Full,
    /// Use actual data-file row ID ranges to detect exact missing row IDs.
    Detail,
}

/// Bucket function used to map bucket keys to fixed bucket ids.
///
/// Reference: Java `CoreOptions.BucketFunctionType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketFunctionType {
    Default,
    Mod,
    Hive,
}

impl BucketFunctionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Mod => "mod",
            Self::Hive => "hive",
        }
    }
}

impl ChangelogProducer {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Input => "input",
            Self::FullCompaction => "full-compaction",
            Self::Lookup => "lookup",
        }
    }
}

pub(crate) fn first_row_supports_changelog_producer(producer: ChangelogProducer) -> bool {
    matches!(
        producer,
        ChangelogProducer::None | ChangelogProducer::Lookup
    )
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
    /// `scan.version` (SQL `VERSION AS OF`): ambiguous by design. Resolved at
    /// scan time as tag name (if a tag exists) → snapshot id (if parseable) →
    /// error. `option_name` is kept for error attribution.
    Version {
        value: &'a str,
        option_name: &'static str,
    },
    /// `scan.snapshot-id`: an explicit snapshot id. Resolved strictly by
    /// parsing `value` as an id — never falls back to a tag lookup.
    SnapshotId {
        value: &'a str,
        option_name: &'static str,
    },
    /// `scan.tag-name`: an explicit tag name. Resolved strictly by tag lookup —
    /// never falls back to a snapshot id.
    TagName {
        value: &'a str,
        option_name: &'static str,
    },
}

impl<'a> CoreOptions<'a> {
    pub fn new(options: &'a HashMap<String, String>) -> Self {
        Self { options }
    }

    /// Reject scan options whose semantics the Rust core does not yet implement.
    ///
    /// These are not malformed input — they are unimplemented scan modes — so
    /// they surface as `Error::Unsupported` (mapped to `NotImplementedError` at
    /// the Python boundary). Explicit `scan.mode=from-snapshot` /
    /// `from-timestamp` are the modes Java's `CoreOptions.setDefaultValues()`
    /// writes next to the corresponding selector, so they are accepted when
    /// that selector is present (the batch-read semantics are identical to
    /// leaving the mode at `default`); an explicit mode without its selector
    /// is malformed input (`Error::DataInvalid`), mirroring Java's
    /// `SchemaValidation`. All other non-default modes are unimplemented.
    pub fn validate_scan_options(&self) -> crate::Result<()> {
        for key in [
            INCREMENTAL_BETWEEN_OPTION,
            INCREMENTAL_BETWEEN_TIMESTAMP_OPTION,
            INCREMENTAL_BETWEEN_SCAN_MODE_OPTION,
            SCAN_WATERMARK_OPTION,
        ] {
            if self.options.contains_key(key) {
                return Err(crate::Error::Unsupported {
                    message: format!("Scan option '{key}' is not supported by the Rust reader yet"),
                });
            }
        }
        if let Some(mode) = self.options.get(SCAN_MODE_OPTION) {
            let selector_keys: &[&str] = if mode.eq_ignore_ascii_case("default") {
                return Ok(());
            } else if mode.eq_ignore_ascii_case("from-snapshot") {
                &[
                    SCAN_SNAPSHOT_ID_OPTION,
                    SCAN_TAG_NAME_OPTION,
                    SCAN_VERSION_OPTION,
                ]
            } else if mode.eq_ignore_ascii_case("from-timestamp") {
                &[SCAN_TIMESTAMP_MILLIS_OPTION]
            } else {
                return Err(crate::Error::Unsupported {
                    message: format!(
                        "Scan option 'scan.mode={mode}' is not supported by the Rust reader yet"
                    ),
                });
            };
            if !selector_keys
                .iter()
                .any(|key| self.options.contains_key(*key))
            {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Scan option 'scan.mode={mode}' requires one of {} to be set",
                        selector_keys.join(", ")
                    ),
                    source: None,
                });
            }
        }
        Ok(())
    }

    pub fn deletion_vectors_enabled(&self) -> bool {
        self.options
            .get(DELETION_VECTORS_ENABLED_OPTION)
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Whether `query-auth.enabled` is set.
    ///
    /// When set, the server enforces a per-user row filter / column masking that this client
    /// can't yet apply, so read paths fail closed (see `ensure_read_authorized`).
    pub fn query_auth_enabled(&self) -> bool {
        self.options
            .get(QUERY_AUTH_ENABLED_OPTION)
            .map(|value| value.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Fail closed when `query-auth.enabled` is set: this client can't enforce the row
    /// filter / column masking, so refuse to read. Call at every read boundary (build,
    /// plan, materialize) so no binding fast-path can bypass it.
    pub fn ensure_read_authorized(&self) -> crate::Result<()> {
        if self.query_auth_enabled() {
            return Err(crate::Error::Unsupported {
                message: "reading a table with 'query-auth.enabled' = true is not supported: \
                          the Rust client cannot yet enforce its row-level auth filter / column \
                          masking, so it refuses to read to avoid returning unfiltered data"
                    .to_string(),
            });
        }
        Ok(())
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
                "partial-update" => Ok(MergeEngine::PartialUpdate),
                "first-row" => Ok(MergeEngine::FirstRow),
                "aggregation" => Ok(MergeEngine::Aggregation),
                other => Err(crate::Error::Unsupported {
                    message: format!("Unsupported merge-engine: '{other}'"),
                }),
            },
        }
    }

    /// Raw changelog producer setting. Default is `"none"`.
    pub fn changelog_producer(&self) -> &str {
        self.options
            .get(CHANGELOG_PRODUCER_OPTION)
            .map(String::as_str)
            .unwrap_or("none")
    }

    /// Typed changelog producer setting. Default is `None`.
    pub fn try_changelog_producer(&self) -> crate::Result<ChangelogProducer> {
        match self.options.get(CHANGELOG_PRODUCER_OPTION) {
            None => Ok(ChangelogProducer::None),
            Some(v) => match v.to_ascii_lowercase().as_str() {
                "none" => Ok(ChangelogProducer::None),
                "input" => Ok(ChangelogProducer::Input),
                "full-compaction" => Ok(ChangelogProducer::FullCompaction),
                "lookup" => Ok(ChangelogProducer::Lookup),
                other => Err(crate::Error::Unsupported {
                    message: format!("Unsupported changelog-producer: '{other}'"),
                }),
            },
        }
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

    pub fn global_index_search_mode(&self) -> crate::Result<GlobalIndexSearchMode> {
        match self
            .options
            .get(GLOBAL_INDEX_SEARCH_MODE_OPTION)
            .map(|v| v.to_ascii_lowercase())
            .as_deref()
            .unwrap_or("fast")
        {
            "fast" => Ok(GlobalIndexSearchMode::Fast),
            "full" => Ok(GlobalIndexSearchMode::Full),
            "detail" => Ok(GlobalIndexSearchMode::Detail),
            other => Err(crate::Error::ConfigInvalid {
                message: format!("Unsupported global-index.search-mode: {other}"),
            }),
        }
    }

    pub fn global_index_row_count_per_shard(&self) -> crate::Result<i64> {
        let value = self
            .parse_i64_option(GLOBAL_INDEX_ROW_COUNT_PER_SHARD_OPTION)?
            .unwrap_or(DEFAULT_GLOBAL_INDEX_ROW_COUNT_PER_SHARD);
        if value <= 0 {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Option '{}' must be greater than 0, got: {}",
                    GLOBAL_INDEX_ROW_COUNT_PER_SHARD_OPTION, value
                ),
                source: None,
            });
        }
        Ok(value)
    }

    pub fn sorted_index_records_per_range(&self) -> crate::Result<i64> {
        let value = self
            .parse_i64_option(SORTED_INDEX_RECORDS_PER_RANGE_OPTION)?
            .unwrap_or(DEFAULT_GLOBAL_INDEX_ROW_COUNT_PER_SHARD);
        if value <= 0 {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Option '{}' must be greater than 0, got: {}",
                    SORTED_INDEX_RECORDS_PER_RANGE_OPTION, value
                ),
                source: None,
            });
        }
        Ok(value)
    }

    pub fn btree_index_fallback_scan_max_size(&self) -> crate::Result<i64> {
        self.fallback_scan_max_size(BTREE_INDEX_FALLBACK_SCAN_MAX_SIZE_OPTION)
    }

    pub fn bitmap_index_fallback_scan_max_size(&self) -> crate::Result<i64> {
        self.fallback_scan_max_size(BITMAP_INDEX_FALLBACK_SCAN_MAX_SIZE_OPTION)
    }

    fn fallback_scan_max_size(&self, option_name: &'static str) -> crate::Result<i64> {
        let value = match self.options.get(option_name) {
            Some(raw) => parse_memory_size(raw).ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "Option '{}' must be a valid memory size, got: {}",
                    option_name, raw
                ),
                source: None,
            })?,
            None => DEFAULT_GLOBAL_INDEX_FALLBACK_SCAN_MAX_SIZE,
        };
        if value < 0 {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Option '{}' must be greater than or equal to 0, got: {}",
                    option_name, value
                ),
                source: None,
            });
        }
        Ok(value)
    }

    pub fn global_index_column_update_action(
        &self,
    ) -> crate::Result<GlobalIndexColumnUpdateAction> {
        match self
            .options
            .get(GLOBAL_INDEX_COLUMN_UPDATE_ACTION_OPTION)
            .map(|v| v.to_ascii_uppercase())
            .as_deref()
            .unwrap_or("THROW_ERROR")
        {
            "THROW_ERROR" => Ok(GlobalIndexColumnUpdateAction::ThrowError),
            "DROP_PARTITION_INDEX" => Ok(GlobalIndexColumnUpdateAction::DropPartitionIndex),
            other => Err(crate::Error::ConfigInvalid {
                message: format!("Unsupported global-index.column-update-action: {other}"),
            }),
        }
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
        let mut selectors = Vec::with_capacity(4);
        if self.options.contains_key(SCAN_TIMESTAMP_MILLIS_OPTION) {
            selectors.push(SCAN_TIMESTAMP_MILLIS_OPTION);
        }
        if self.options.contains_key(SCAN_VERSION_OPTION) {
            selectors.push(SCAN_VERSION_OPTION);
        }
        if self.options.contains_key(SCAN_SNAPSHOT_ID_OPTION) {
            selectors.push(SCAN_SNAPSHOT_ID_OPTION);
        }
        if self.options.contains_key(SCAN_TAG_NAME_OPTION) {
            selectors.push(SCAN_TAG_NAME_OPTION);
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
        } else if let Some(value) = self.options.get(SCAN_VERSION_OPTION).map(String::as_str) {
            Ok(Some(TimeTravelSelector::Version {
                value,
                option_name: SCAN_VERSION_OPTION,
            }))
        } else if let Some(value) = self
            .options
            .get(SCAN_SNAPSHOT_ID_OPTION)
            .map(String::as_str)
        {
            Ok(Some(TimeTravelSelector::SnapshotId {
                value,
                option_name: SCAN_SNAPSHOT_ID_OPTION,
            }))
        } else if let Some(value) = self.options.get(SCAN_TAG_NAME_OPTION).map(String::as_str) {
            Ok(Some(TimeTravelSelector::TagName {
                value,
                option_name: SCAN_TAG_NAME_OPTION,
            }))
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

    /// Suggested target size for a manifest file. Default is 8 MiB.
    ///
    /// `manifest.target-file-size` is the Java/Python option. The shorter
    /// `manifest.target-size` alias is accepted for older Rust callers that
    /// used the name from early parity discussions.
    pub fn manifest_target_size(&self) -> i64 {
        self.options
            .get(MANIFEST_TARGET_FILE_SIZE_OPTION)
            .or_else(|| self.options.get(MANIFEST_TARGET_SIZE_OPTION))
            .and_then(|v| parse_memory_size(v))
            .unwrap_or(DEFAULT_MANIFEST_TARGET_FILE_SIZE)
    }

    /// Minimum number of small manifest files required before minor manifest
    /// compaction rewrites them into a new rolling manifest set.
    pub fn manifest_merge_min_count(&self) -> usize {
        self.options
            .get(MANIFEST_MERGE_MIN_COUNT_OPTION)
            .and_then(|v| v.parse().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_MANIFEST_MERGE_MIN_COUNT)
    }

    /// Number of buckets for the table. Default is 1.
    pub fn bucket(&self) -> i32 {
        self.options
            .get(BUCKET_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_BUCKET)
    }

    /// Bucket function type. Defaults to Java-compatible Paimon hash.
    pub fn bucket_function_type(&self) -> crate::Result<BucketFunctionType> {
        match self
            .options
            .get(BUCKET_FUNCTION_TYPE_OPTION)
            .map(|v| v.to_ascii_lowercase())
            .as_deref()
            .unwrap_or("default")
        {
            "default" => Ok(BucketFunctionType::Default),
            "mod" => Ok(BucketFunctionType::Mod),
            "hive" => Ok(BucketFunctionType::Hive),
            other => Err(crate::Error::ConfigInvalid {
                message: format!("Unsupported bucket-function.type: {other}"),
            }),
        }
    }

    /// Target file size for data files. Default is 128MB.
    pub fn target_file_size(&self) -> i64 {
        self.options
            .get("target-file-size")
            .and_then(|v| parse_memory_size(v))
            .unwrap_or(DEFAULT_TARGET_FILE_SIZE)
    }

    pub fn blob_target_file_size(&self) -> i64 {
        self.options
            .get("blob.target-file-size")
            .and_then(|v| parse_memory_size(v))
            .unwrap_or_else(|| self.target_file_size())
    }

    /// Dedicated vector-store file format, if configured.
    ///
    /// Java leaves this unset by default. When present, vector columns are
    /// written to files named `*.vector.<format>`.
    pub fn vector_file_format(&self) -> Option<&str> {
        self.options
            .get(VECTOR_FILE_FORMAT_OPTION)
            .map(String::as_str)
            .filter(|format| !format.trim().is_empty())
    }

    pub fn vector_target_file_size(&self) -> i64 {
        self.options
            .get(VECTOR_TARGET_FILE_SIZE_OPTION)
            .and_then(|v| parse_memory_size(v))
            .unwrap_or_else(|| self.target_file_size())
    }

    /// File format for data files (e.g. "parquet", "orc", "avro", "vortex").
    /// Default is "parquet".
    pub fn file_format(&self) -> &str {
        self.options
            .get(FILE_FORMAT_OPTION)
            .map(String::as_str)
            .unwrap_or("parquet")
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

    /// File name prefix for changelog files. Default is `"changelog-"`.
    pub fn changelog_file_prefix(&self) -> &str {
        self.options
            .get(CHANGELOG_FILE_PREFIX_OPTION)
            .map(String::as_str)
            .unwrap_or(DEFAULT_CHANGELOG_FILE_PREFIX)
    }

    /// Effective file format for changelog files.
    ///
    /// When `changelog-file.format` is not configured, Java Paimon falls back
    /// to the table `file.format`.
    pub fn changelog_file_format(&self) -> &str {
        self.options
            .get(CHANGELOG_FILE_FORMAT_OPTION)
            .map(String::as_str)
            .unwrap_or_else(|| self.file_format())
    }

    /// Effective compression codec for changelog files.
    ///
    /// When `changelog-file.compression` is not configured, Java Paimon falls
    /// back to the table `file.compression`.
    pub fn changelog_file_compression(&self) -> &str {
        self.options
            .get(CHANGELOG_FILE_COMPRESSION_OPTION)
            .map(String::as_str)
            .unwrap_or_else(|| self.file_compression())
    }

    /// Metadata stats collection mode for changelog files, if configured.
    pub fn changelog_file_stats_mode(&self) -> Option<&str> {
        self.options
            .get(CHANGELOG_FILE_STATS_MODE_OPTION)
            .map(String::as_str)
    }

    /// Avro compression codec for manifest, manifest-list and index-manifest files.
    /// Default is `"zstd"`, matching Java Paimon `CoreOptions.MANIFEST_COMPRESSION`.
    pub fn manifest_compression(&self) -> &str {
        self.options
            .get(MANIFEST_COMPRESSION_OPTION)
            .map(String::as_str)
            .unwrap_or(DEFAULT_MANIFEST_COMPRESSION)
    }

    /// Parquet writer in-progress buffer size limit. Default is 256MB.
    /// When the buffered data exceeds this, the writer flushes the current row group.
    pub fn write_parquet_buffer_size(&self) -> i64 {
        self.options
            .get(WRITE_PARQUET_BUFFER_SIZE_OPTION)
            .and_then(|v| parse_memory_size(v))
            .unwrap_or(DEFAULT_WRITE_PARQUET_BUFFER_SIZE)
    }

    /// Target row number per bucket for dynamic bucket mode (bucket=-1).
    /// When a bucket reaches this number, a new bucket is created.
    /// Default is 200,000 (matching Java Paimon).
    pub fn dynamic_bucket_target_row_num(&self) -> i64 {
        self.options
            .get(DYNAMIC_BUCKET_TARGET_ROW_NUM_OPTION)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_DYNAMIC_BUCKET_TARGET_ROW_NUM)
    }

    /// When true, blob field reads return serialized BlobDescriptor bytes
    /// instead of actual blob bytes. Default is false.
    pub fn blob_as_descriptor(&self) -> bool {
        self.options
            .get(BLOB_AS_DESCRIPTOR_OPTION)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    }

    /// Comma-separated BLOB field names stored as serialized BlobDescriptor
    /// bytes inline in normal data files (no .blob files for these fields).
    pub fn blob_descriptor_fields(&self) -> HashSet<String> {
        self.options
            .get(BLOB_DESCRIPTOR_FIELD_OPTION)
            .map(|s| s.split(',').map(|f| f.trim().to_string()).collect())
            .unwrap_or_default()
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
        assert_eq!(
            core_options.global_index_row_count_per_shard().unwrap(),
            100_000
        );
        assert_eq!(
            core_options.sorted_index_records_per_range().unwrap(),
            100_000
        );
        assert_eq!(
            core_options.btree_index_fallback_scan_max_size().unwrap(),
            256 * 1024 * 1024
        );
        assert_eq!(
            core_options.bitmap_index_fallback_scan_max_size().unwrap(),
            256 * 1024 * 1024
        );
        assert_eq!(
            core_options.global_index_column_update_action().unwrap(),
            GlobalIndexColumnUpdateAction::ThrowError
        );
        assert_eq!(
            core_options.global_index_search_mode().unwrap(),
            GlobalIndexSearchMode::Fast
        );
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
            (
                GLOBAL_INDEX_ROW_COUNT_PER_SHARD_OPTION.to_string(),
                "2048".to_string(),
            ),
            (
                SORTED_INDEX_RECORDS_PER_RANGE_OPTION.to_string(),
                "4096".to_string(),
            ),
            (
                BTREE_INDEX_FALLBACK_SCAN_MAX_SIZE_OPTION.to_string(),
                "4 mb".to_string(),
            ),
            (
                BITMAP_INDEX_FALLBACK_SCAN_MAX_SIZE_OPTION.to_string(),
                "8 mb".to_string(),
            ),
            (
                GLOBAL_INDEX_COLUMN_UPDATE_ACTION_OPTION.to_string(),
                "DROP_PARTITION_INDEX".to_string(),
            ),
            (
                GLOBAL_INDEX_SEARCH_MODE_OPTION.to_string(),
                "detail".to_string(),
            ),
        ]);
        let core_options = CoreOptions::new(&options);

        assert_eq!(core_options.source_split_target_size(), 256 * 1024 * 1024);
        assert_eq!(core_options.source_split_open_file_cost(), 8 * 1024 * 1024);
        assert_eq!(
            core_options.global_index_row_count_per_shard().unwrap(),
            2048
        );
        assert_eq!(core_options.sorted_index_records_per_range().unwrap(), 4096);
        assert_eq!(
            core_options.btree_index_fallback_scan_max_size().unwrap(),
            4 * 1024 * 1024
        );
        assert_eq!(
            core_options.bitmap_index_fallback_scan_max_size().unwrap(),
            8 * 1024 * 1024
        );
        assert_eq!(
            core_options.global_index_column_update_action().unwrap(),
            GlobalIndexColumnUpdateAction::DropPartitionIndex
        );
        assert_eq!(
            core_options.global_index_search_mode().unwrap(),
            GlobalIndexSearchMode::Detail
        );
    }

    #[test]
    fn test_global_index_search_mode_values() {
        for (raw, expected) in [
            ("fast", GlobalIndexSearchMode::Fast),
            ("FAST", GlobalIndexSearchMode::Fast),
            ("full", GlobalIndexSearchMode::Full),
            ("detail", GlobalIndexSearchMode::Detail),
        ] {
            let options =
                HashMap::from([(GLOBAL_INDEX_SEARCH_MODE_OPTION.to_string(), raw.to_string())]);
            let core = CoreOptions::new(&options);
            assert_eq!(core.global_index_search_mode().unwrap(), expected);
        }
    }

    #[test]
    fn test_global_index_search_mode_rejects_invalid_value() {
        let options = HashMap::from([(
            GLOBAL_INDEX_SEARCH_MODE_OPTION.to_string(),
            "slow".to_string(),
        )]);
        let core = CoreOptions::new(&options);

        let err = core.global_index_search_mode().expect_err("invalid mode");
        assert!(matches!(err, crate::Error::ConfigInvalid { message }
                if message.contains(GLOBAL_INDEX_SEARCH_MODE_OPTION)));
    }

    #[test]
    fn test_global_index_row_count_per_shard_rejects_invalid_values() {
        for value in ["0", "-1", "abc"] {
            let options = HashMap::from([(
                GLOBAL_INDEX_ROW_COUNT_PER_SHARD_OPTION.to_string(),
                value.to_string(),
            )]);
            let core = CoreOptions::new(&options);

            let err = core
                .global_index_row_count_per_shard()
                .expect_err("invalid rows-per-shard should fail");
            assert!(matches!(err, crate::Error::DataInvalid { message, .. }
                    if message.contains(GLOBAL_INDEX_ROW_COUNT_PER_SHARD_OPTION)));
        }
    }

    #[test]
    fn test_sorted_index_records_per_range_rejects_invalid_values() {
        for value in ["0", "-1", "abc"] {
            let options = HashMap::from([(
                SORTED_INDEX_RECORDS_PER_RANGE_OPTION.to_string(),
                value.to_string(),
            )]);
            let core = CoreOptions::new(&options);

            let err = core
                .sorted_index_records_per_range()
                .expect_err("invalid records-per-range should fail");
            assert!(matches!(err, crate::Error::DataInvalid { message, .. }
                    if message.contains(SORTED_INDEX_RECORDS_PER_RANGE_OPTION)));
        }
    }

    #[test]
    fn test_global_index_fallback_scan_max_size_values() {
        for option_name in [
            BTREE_INDEX_FALLBACK_SCAN_MAX_SIZE_OPTION,
            BITMAP_INDEX_FALLBACK_SCAN_MAX_SIZE_OPTION,
        ] {
            let options = HashMap::from([(option_name.to_string(), "0".to_string())]);
            let core = CoreOptions::new(&options);
            let value = match option_name {
                BTREE_INDEX_FALLBACK_SCAN_MAX_SIZE_OPTION => {
                    core.btree_index_fallback_scan_max_size()
                }
                BITMAP_INDEX_FALLBACK_SCAN_MAX_SIZE_OPTION => {
                    core.bitmap_index_fallback_scan_max_size()
                }
                _ => unreachable!(),
            };
            assert_eq!(value.unwrap(), 0);

            for value in ["-1", "abc"] {
                let options = HashMap::from([(option_name.to_string(), value.to_string())]);
                let core = CoreOptions::new(&options);

                let err = match option_name {
                    BTREE_INDEX_FALLBACK_SCAN_MAX_SIZE_OPTION => {
                        core.btree_index_fallback_scan_max_size()
                    }
                    BITMAP_INDEX_FALLBACK_SCAN_MAX_SIZE_OPTION => {
                        core.bitmap_index_fallback_scan_max_size()
                    }
                    _ => unreachable!(),
                }
                .expect_err("invalid fallback scan max size should fail");
                assert!(matches!(err, crate::Error::DataInvalid { message, .. }
                    if message.contains(option_name)));
            }
        }
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
    fn test_merge_engine_accepts_partial_update() {
        let options = HashMap::from([(MERGE_ENGINE_OPTION.to_string(), "partial-update".into())]);
        let core = CoreOptions::new(&options);

        assert_eq!(core.merge_engine().unwrap(), MergeEngine::PartialUpdate);
    }

    #[test]
    fn test_merge_engine_accepts_aggregation() {
        let options = HashMap::from([(MERGE_ENGINE_OPTION.to_string(), "aggregation".into())]);
        let core = CoreOptions::new(&options);

        assert_eq!(core.merge_engine().unwrap(), MergeEngine::Aggregation);
    }

    #[test]
    fn test_changelog_producer_defaults_to_none() {
        let options = HashMap::new();
        let core = CoreOptions::new(&options);

        assert_eq!(core.changelog_producer(), "none");
        assert_eq!(
            core.try_changelog_producer().unwrap(),
            ChangelogProducer::None
        );
    }

    #[test]
    fn test_changelog_producer_accepts_known_values() {
        for (value, expected) in [
            ("none", ChangelogProducer::None),
            ("input", ChangelogProducer::Input),
            ("full-compaction", ChangelogProducer::FullCompaction),
            ("lookup", ChangelogProducer::Lookup),
            ("INPUT", ChangelogProducer::Input),
        ] {
            let options = HashMap::from([(CHANGELOG_PRODUCER_OPTION.to_string(), value.into())]);
            let core = CoreOptions::new(&options);

            assert_eq!(core.try_changelog_producer().unwrap(), expected);
        }
    }

    #[test]
    fn test_changelog_producer_rejects_unknown_values() {
        let options = HashMap::from([(CHANGELOG_PRODUCER_OPTION.to_string(), "other".into())]);
        let core = CoreOptions::new(&options);

        let err = core
            .try_changelog_producer()
            .expect_err("unknown producer should fail");
        assert!(
            matches!(err, crate::Error::Unsupported { message } if message.contains("Unsupported changelog-producer"))
        );
    }

    #[test]
    fn test_changelog_file_options_defaults_and_overrides() {
        let default_options = HashMap::from([
            (FILE_FORMAT_OPTION.to_string(), "avro".to_string()),
            (FILE_COMPRESSION_OPTION.to_string(), "snappy".to_string()),
        ]);
        let default_core = CoreOptions::new(&default_options);

        assert_eq!(default_core.changelog_file_prefix(), "changelog-");
        assert_eq!(default_core.changelog_file_format(), "avro");
        assert_eq!(default_core.changelog_file_compression(), "snappy");
        assert_eq!(default_core.changelog_file_stats_mode(), None);

        let custom_options = HashMap::from([
            (
                CHANGELOG_FILE_PREFIX_OPTION.to_string(),
                "custom-".to_string(),
            ),
            (
                CHANGELOG_FILE_FORMAT_OPTION.to_string(),
                "parquet".to_string(),
            ),
            (
                CHANGELOG_FILE_COMPRESSION_OPTION.to_string(),
                "zstd".to_string(),
            ),
            (
                CHANGELOG_FILE_STATS_MODE_OPTION.to_string(),
                "counts".to_string(),
            ),
        ]);
        let custom_core = CoreOptions::new(&custom_options);

        assert_eq!(custom_core.changelog_file_prefix(), "custom-");
        assert_eq!(custom_core.changelog_file_format(), "parquet");
        assert_eq!(custom_core.changelog_file_compression(), "zstd");
        assert_eq!(custom_core.changelog_file_stats_mode(), Some("counts"));
    }

    #[test]
    fn test_vector_file_options_defaults_and_overrides() {
        let default_options =
            HashMap::from([("target-file-size".to_string(), "32 mb".to_string())]);
        let default_core = CoreOptions::new(&default_options);
        assert_eq!(default_core.vector_file_format(), None);
        assert_eq!(default_core.vector_target_file_size(), 32 * 1024 * 1024);

        let custom_options = HashMap::from([
            (VECTOR_FILE_FORMAT_OPTION.to_string(), "vortex".to_string()),
            (
                VECTOR_TARGET_FILE_SIZE_OPTION.to_string(),
                "64 mb".to_string(),
            ),
        ]);
        let custom_core = CoreOptions::new(&custom_options);
        assert_eq!(custom_core.vector_file_format(), Some("vortex"));
        assert_eq!(custom_core.vector_target_file_size(), 64 * 1024 * 1024);
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
        assert_eq!(core.manifest_compression(), "zstd");
        assert_eq!(core.manifest_target_size(), 8 * 1024 * 1024);
        assert_eq!(core.manifest_merge_min_count(), 30);
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
            (
                MANIFEST_TARGET_FILE_SIZE_OPTION.to_string(),
                "1kb".to_string(),
            ),
            (MANIFEST_COMPRESSION_OPTION.to_string(), "null".to_string()),
            (MANIFEST_MERGE_MIN_COUNT_OPTION.to_string(), "3".to_string()),
        ]);
        let core = CoreOptions::new(&options);
        assert_eq!(core.bucket(), 4);
        assert_eq!(core.commit_max_retries(), 20);
        assert_eq!(core.commit_timeout_ms(), 60_000);
        assert_eq!(core.commit_min_retry_wait_ms(), 500);
        assert_eq!(core.commit_max_retry_wait_ms(), 5_000);
        assert!(core.row_tracking_enabled());
        assert_eq!(core.manifest_compression(), "null");
        assert_eq!(core.manifest_target_size(), 1024);
        assert_eq!(core.manifest_merge_min_count(), 3);
    }

    #[test]
    fn test_manifest_target_size_accepts_compat_alias() {
        let options = HashMap::from([(MANIFEST_TARGET_SIZE_OPTION.to_string(), "2kb".into())]);
        let core = CoreOptions::new(&options);

        assert_eq!(core.manifest_target_size(), 2 * 1024);
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
            Some(TimeTravelSelector::Version {
                value: "my-tag",
                option_name: SCAN_VERSION_OPTION
            })
        );

        let version_num_options =
            HashMap::from([(SCAN_VERSION_OPTION.to_string(), "3".to_string())]);
        let version_num_core = CoreOptions::new(&version_num_options);
        assert_eq!(
            version_num_core
                .try_time_travel_selector()
                .expect("version numeric selector"),
            Some(TimeTravelSelector::Version {
                value: "3",
                option_name: SCAN_VERSION_OPTION
            })
        );
    }

    #[test]
    fn test_snapshot_id_and_tag_name_map_to_distinct_selectors() {
        let snap = HashMap::from([(SCAN_SNAPSHOT_ID_OPTION.to_string(), "2".to_string())]);
        assert_eq!(
            CoreOptions::new(&snap).try_time_travel_selector().unwrap(),
            Some(TimeTravelSelector::SnapshotId {
                value: "2",
                option_name: SCAN_SNAPSHOT_ID_OPTION
            })
        );
        let tag = HashMap::from([(SCAN_TAG_NAME_OPTION.to_string(), "t1".to_string())]);
        assert_eq!(
            CoreOptions::new(&tag).try_time_travel_selector().unwrap(),
            Some(TimeTravelSelector::TagName {
                value: "t1",
                option_name: SCAN_TAG_NAME_OPTION
            })
        );
    }

    #[test]
    fn test_snapshot_id_conflicts_with_version_lists_original_keys() {
        let options = HashMap::from([
            (SCAN_SNAPSHOT_ID_OPTION.to_string(), "1".to_string()),
            (SCAN_TAG_NAME_OPTION.to_string(), "t".to_string()),
        ]);
        let err = CoreOptions::new(&options)
            .try_time_travel_selector()
            .unwrap_err();
        match err {
            crate::Error::DataInvalid { message, .. } => {
                assert!(message.contains(SCAN_SNAPSHOT_ID_OPTION));
                assert!(message.contains(SCAN_TAG_NAME_OPTION));
            }
            other => panic!("unexpected: {other:?}"),
        }
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

    #[test]
    fn test_validate_scan_options_rejects_unsupported() {
        for key in [
            "incremental-between",
            "incremental-between-timestamp",
            "incremental-between-scan-mode",
            "scan.watermark",
        ] {
            let options = HashMap::from([(key.to_string(), "x".to_string())]);
            let err = CoreOptions::new(&options)
                .validate_scan_options()
                .unwrap_err();
            assert!(matches!(err, crate::Error::Unsupported { message } if message.contains(key)));
        }
    }

    #[test]
    fn test_validate_scan_options_scan_mode_whitelist() {
        // absent OK
        assert!(CoreOptions::new(&HashMap::new())
            .validate_scan_options()
            .is_ok());
        // default OK
        let ok = HashMap::from([("scan.mode".to_string(), "default".to_string())]);
        assert!(CoreOptions::new(&ok).validate_scan_options().is_ok());
        // unimplemented modes Unsupported
        for mode in ["compacted-full", "incremental", "latest", "latest-full"] {
            let bad = HashMap::from([("scan.mode".to_string(), mode.to_string())]);
            let err = CoreOptions::new(&bad).validate_scan_options().unwrap_err();
            assert!(
                matches!(err, crate::Error::Unsupported { message } if message.contains("scan.mode")),
                "scan.mode={mode} should be Unsupported"
            );
        }
    }

    #[test]
    fn test_validate_scan_options_explicit_mode_with_matching_selector() {
        // Java's CoreOptions.setDefaultValues() writes scan.mode=from-snapshot
        // next to scan.snapshot-id, so these combinations are standard input.
        for selector in [
            SCAN_SNAPSHOT_ID_OPTION,
            SCAN_TAG_NAME_OPTION,
            SCAN_VERSION_OPTION,
        ] {
            let options = HashMap::from([
                ("scan.mode".to_string(), "from-snapshot".to_string()),
                (selector.to_string(), "1".to_string()),
            ]);
            assert!(
                CoreOptions::new(&options).validate_scan_options().is_ok(),
                "scan.mode=from-snapshot with {selector} should be accepted"
            );
        }
        let options = HashMap::from([
            ("scan.mode".to_string(), "from-timestamp".to_string()),
            (SCAN_TIMESTAMP_MILLIS_OPTION.to_string(), "1".to_string()),
        ]);
        assert!(CoreOptions::new(&options).validate_scan_options().is_ok());
    }

    #[test]
    fn test_validate_scan_options_explicit_mode_without_selector() {
        // An explicit mode missing its selector must fail loudly instead of
        // silently reading latest (mirrors Java SchemaValidation).
        let options = HashMap::from([("scan.mode".to_string(), "from-snapshot".to_string())]);
        let err = CoreOptions::new(&options)
            .validate_scan_options()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("from-snapshot")),
            "got {err:?}"
        );

        let options = HashMap::from([("scan.mode".to_string(), "from-timestamp".to_string())]);
        let err = CoreOptions::new(&options)
            .validate_scan_options()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("from-timestamp")),
            "got {err:?}"
        );

        // A mismatched selector doesn't satisfy the mode either.
        let options = HashMap::from([
            ("scan.mode".to_string(), "from-timestamp".to_string()),
            (SCAN_SNAPSHOT_ID_OPTION.to_string(), "1".to_string()),
        ]);
        assert!(CoreOptions::new(&options).validate_scan_options().is_err());
    }

    #[test]
    fn test_validate_scan_options_allows_supported_selectors() {
        let options = HashMap::from([(SCAN_SNAPSHOT_ID_OPTION.to_string(), "1".to_string())]);
        assert!(CoreOptions::new(&options).validate_scan_options().is_ok());
    }
}
