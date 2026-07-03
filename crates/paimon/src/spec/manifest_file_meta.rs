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

use crate::spec::stats::BinaryTableStats;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Metadata of a manifest file.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/manifest/ManifestFileMeta.java>
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct ManifestFileMeta {
    #[serde(rename = "_VERSION")]
    version: i32,

    /// manifest file name
    #[serde(rename = "_FILE_NAME")]
    file_name: String,

    /// manifest file size.
    #[serde(rename = "_FILE_SIZE")]
    file_size: i64,

    /// number added files in manifest.
    #[serde(rename = "_NUM_ADDED_FILES")]
    num_added_files: i64,

    /// number deleted files in manifest.
    #[serde(rename = "_NUM_DELETED_FILES")]
    num_deleted_files: i64,

    /// partition stats, the minimum and maximum values of partition fields in this manifest are beneficial for skipping certain manifest files during queries, it is a SimpleStats.
    #[serde(rename = "_PARTITION_STATS")]
    partition_stats: BinaryTableStats,

    /// schema id when writing this manifest file.
    #[serde(rename = "_SCHEMA_ID")]
    schema_id: i64,

    /// minimum bucket covered by entries in this manifest, used by the Java reader to
    /// prune manifests that do not overlap a requested bucket. Always `None` together
    /// with `max_bucket` for back-compat manifests written before bucket statistics
    /// were introduced (apache/paimon#5345).
    #[serde(
        rename = "_MIN_BUCKET",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    min_bucket: Option<i32>,

    /// maximum bucket covered by entries in this manifest. See `min_bucket`.
    #[serde(
        rename = "_MAX_BUCKET",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    max_bucket: Option<i32>,

    /// minimum LSM level covered by entries in this manifest, used by the Java reader
    /// for level-based pruning (e.g. compaction's level filter). Same back-compat note
    /// as `min_bucket`.
    #[serde(
        rename = "_MIN_LEVEL",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    min_level: Option<i32>,

    /// maximum LSM level covered by entries in this manifest. See `min_level`.
    #[serde(
        rename = "_MAX_LEVEL",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    max_level: Option<i32>,

    /// minimum row id covered by this manifest, when row tracking is enabled.
    #[serde(
        rename = "_MIN_ROW_ID",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    min_row_id: Option<i64>,

    /// maximum row id covered by this manifest, when row tracking is enabled.
    #[serde(
        rename = "_MAX_ROW_ID",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    max_row_id: Option<i64>,
}

impl ManifestFileMeta {
    /// Get the manifest file name
    #[inline]
    pub fn file_name(&self) -> &str {
        self.file_name.as_str()
    }

    /// Get the manifest file size.
    #[inline]
    pub fn file_size(&self) -> i64 {
        self.file_size
    }

    /// Get the number added files in manifest.
    #[inline]
    pub fn num_added_files(&self) -> i64 {
        self.num_added_files
    }

    /// Get the number deleted files in manifest.
    #[inline]
    pub fn num_deleted_files(&self) -> i64 {
        self.num_deleted_files
    }

    /// Get the partition stats
    pub fn partition_stats(&self) -> &BinaryTableStats {
        &self.partition_stats
    }

    /// Get the schema id when writing this manifest file.
    #[inline]
    pub fn schema_id(&self) -> i64 {
        self.schema_id
    }

    /// Get the version of this manifest file
    #[inline]
    pub fn version(&self) -> i32 {
        self.version
    }

    /// Get the minimum bucket covered by this manifest (None when bucket stats are absent,
    /// e.g. manifests written before the field was introduced).
    #[inline]
    pub fn min_bucket(&self) -> Option<i32> {
        self.min_bucket
    }

    /// Get the maximum bucket covered by this manifest (None when bucket stats are absent).
    #[inline]
    pub fn max_bucket(&self) -> Option<i32> {
        self.max_bucket
    }

    /// Get the minimum LSM level covered by this manifest (None when level stats are absent).
    #[inline]
    pub fn min_level(&self) -> Option<i32> {
        self.min_level
    }

    /// Get the maximum LSM level covered by this manifest (None when level stats are absent).
    #[inline]
    pub fn max_level(&self) -> Option<i32> {
        self.max_level
    }

    /// Get the minimum row id covered by this manifest (None when row tracking is disabled).
    #[inline]
    pub fn min_row_id(&self) -> Option<i64> {
        self.min_row_id
    }

    /// Get the maximum row id covered by this manifest (None when row tracking is disabled).
    #[inline]
    pub fn max_row_id(&self) -> Option<i64> {
        self.max_row_id
    }

    /// Attach bucket / level statistics aggregated from manifest entries.
    ///
    /// Use this in writers that have access to the entries that the manifest covers.
    /// Setting all four to `None` is equivalent to leaving the stats absent (the Java
    /// reader treats `null` here as "no information; do not prune").
    #[inline]
    #[must_use]
    pub fn with_bucket_level_stats(
        mut self,
        min_bucket: Option<i32>,
        max_bucket: Option<i32>,
        min_level: Option<i32>,
        max_level: Option<i32>,
    ) -> Self {
        self.min_bucket = min_bucket;
        self.max_bucket = max_bucket;
        self.min_level = min_level;
        self.max_level = max_level;
        self
    }

    /// Attach row-id range statistics aggregated from manifest entries.
    #[inline]
    #[must_use]
    pub fn with_row_id_stats(mut self, min_row_id: Option<i64>, max_row_id: Option<i64>) -> Self {
        self.min_row_id = min_row_id;
        self.max_row_id = max_row_id;
        self
    }

    #[inline]
    pub fn new(
        file_name: String,
        file_size: i64,
        num_added_files: i64,
        num_deleted_files: i64,
        partition_stats: BinaryTableStats,
        schema_id: i64,
    ) -> ManifestFileMeta {
        Self {
            version: 2,
            file_name,
            file_size,
            num_added_files,
            num_deleted_files,
            partition_stats,
            schema_id,
            min_bucket: None,
            max_bucket: None,
            min_level: None,
            max_level: None,
            min_row_id: None,
            max_row_id: None,
        }
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_version(
        version: i32,
        file_name: String,
        file_size: i64,
        num_added_files: i64,
        num_deleted_files: i64,
        partition_stats: BinaryTableStats,
        schema_id: i64,
        min_bucket: Option<i32>,
        max_bucket: Option<i32>,
        min_level: Option<i32>,
        max_level: Option<i32>,
        min_row_id: Option<i64>,
        max_row_id: Option<i64>,
    ) -> ManifestFileMeta {
        Self {
            version,
            file_name,
            file_size,
            num_added_files,
            num_deleted_files,
            partition_stats,
            schema_id,
            min_bucket,
            max_bucket,
            min_level,
            max_level,
            min_row_id,
            max_row_id,
        }
    }
}

/// Avro schema for ManifestFileMeta (used in manifest-list files).
pub const MANIFEST_FILE_META_SCHEMA: &str = r#"["null", {
    "type": "record",
    "name": "record",
    "namespace": "org.apache.paimon.avro.generated",
    "fields": [
        {"name": "_VERSION", "type": "int"},
        {"name": "_FILE_NAME", "type": "string"},
        {"name": "_FILE_SIZE", "type": "long"},
        {"name": "_NUM_ADDED_FILES", "type": "long"},
        {"name": "_NUM_DELETED_FILES", "type": "long"},
        {"name": "_PARTITION_STATS", "type": ["null", {
            "type": "record",
            "name": "record__PARTITION_STATS",
            "fields": [
                {"name": "_MIN_VALUES", "type": "bytes"},
                {"name": "_MAX_VALUES", "type": "bytes"},
                {"name": "_NULL_COUNTS", "type": ["null", {"type": "array", "items": ["null", "long"]}], "default": null}
            ]
        }], "default": null},
        {"name": "_SCHEMA_ID", "type": "long"},
        {"name": "_MIN_BUCKET", "type": ["null", "int"], "default": null},
        {"name": "_MAX_BUCKET", "type": ["null", "int"], "default": null},
        {"name": "_MIN_LEVEL", "type": ["null", "int"], "default": null},
        {"name": "_MAX_LEVEL", "type": ["null", "int"], "default": null},
        {"name": "_MIN_ROW_ID", "type": ["null", "long"], "default": null},
        {"name": "_MAX_ROW_ID", "type": ["null", "long"], "default": null}
    ]
}]"#;

impl Display for ManifestFileMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{{}, {}, {}, {}, {:?}, {}}}",
            self.file_name,
            self.file_size,
            self.num_added_files,
            self.num_deleted_files,
            self.partition_stats,
            self.schema_id
        )
    }
}
