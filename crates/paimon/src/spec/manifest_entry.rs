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

use crate::spec::manifest_common::FileKind;
use crate::spec::DataFileMeta;
use serde::{Deserialize, Serialize};

/// The same {@link Identifier} indicates that the {@link ManifestEntry} refers to the same data file.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/manifest/FileEntry.java#L58>
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Identifier {
    pub partition: Vec<u8>,
    pub bucket: i32,
    pub level: i32,
    pub file_name: String,
    pub extra_files: Vec<String>,
    pub embedded_index: Option<Vec<u8>>,
    pub external_path: Option<String>,
}

/// Entry of a manifest file, representing an addition / deletion of a data file.
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/manifest/ManifestEntry.java>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestEntry {
    #[serde(rename = "_KIND")]
    kind: FileKind,

    #[serde(rename = "_PARTITION", with = "serde_bytes")]
    partition: Vec<u8>,

    #[serde(rename = "_BUCKET")]
    bucket: i32,

    #[serde(rename = "_TOTAL_BUCKETS")]
    total_buckets: i32,

    #[serde(rename = "_FILE")]
    pub(crate) file: DataFileMeta,

    #[serde(rename = "_VERSION")]
    version: i32,
}

#[allow(dead_code)]
impl ManifestEntry {
    pub(crate) fn kind(&self) -> &FileKind {
        &self.kind
    }

    /// Partition bytes for this entry (for grouping splits).
    pub fn partition(&self) -> &[u8] {
        &self.partition
    }

    pub(crate) fn bucket(&self) -> i32 {
        self.bucket
    }

    fn level(&self) -> i32 {
        self.file.level
    }

    fn file_name(&self) -> &str {
        &self.file.file_name
    }

    fn min_key(&self) -> &Vec<u8> {
        &self.file.min_key
    }

    fn max_key(&self) -> &Vec<u8> {
        &self.file.max_key
    }

    pub(crate) fn identifier(&self) -> Identifier {
        Identifier {
            partition: self.partition.clone(),
            bucket: self.bucket,
            level: self.file.level,
            file_name: self.file.file_name.clone(),
            extra_files: self.file.extra_files.clone(),
            embedded_index: self.file.embedded_index.clone(),
            external_path: self.file.external_path.clone(),
        }
    }

    pub fn total_buckets(&self) -> i32 {
        self.total_buckets
    }

    pub fn file(&self) -> &DataFileMeta {
        &self.file
    }

    pub fn new(
        kind: FileKind,
        partition: Vec<u8>,
        bucket: i32,
        total_buckets: i32,
        file: DataFileMeta,
        version: i32,
    ) -> Self {
        ManifestEntry {
            kind,
            partition,
            bucket,
            total_buckets,
            file,
            version,
        }
    }

    /// Return a copy with a different kind.
    pub fn with_kind(mut self, kind: FileKind) -> Self {
        self.kind = kind;
        self
    }

    /// Return a copy with sequence numbers set on the file.
    pub fn with_sequence_number(mut self, min_seq: i64, max_seq: i64) -> Self {
        self.file.min_sequence_number = min_seq;
        self.file.max_sequence_number = max_seq;
        self
    }

    /// Return a copy with first_row_id set on the file.
    pub fn with_first_row_id(mut self, first_row_id: i64) -> Self {
        self.file.first_row_id = Some(first_row_id);
        self
    }
}

/// Avro schema for ManifestEntry (used in manifest files).
pub const MANIFEST_ENTRY_SCHEMA: &str = r#"["null", {
    "type": "record",
    "name": "record",
    "namespace": "org.apache.paimon.avro.generated",
    "fields": [
        {"name": "_KIND", "type": "int"},
        {"name": "_PARTITION", "type": "bytes"},
        {"name": "_BUCKET", "type": "int"},
        {"name": "_TOTAL_BUCKETS", "type": "int"},
        {"name": "_FILE", "type": ["null", {
            "type": "record",
            "name": "record__FILE",
            "fields": [
                {"name": "_FILE_NAME", "type": "string"},
                {"name": "_FILE_SIZE", "type": "long"},
                {"name": "_ROW_COUNT", "type": "long"},
                {"name": "_MIN_KEY", "type": "bytes"},
                {"name": "_MAX_KEY", "type": "bytes"},
                {"name": "_KEY_STATS", "type": ["null", {
                    "type": "record",
                    "name": "record__FILE__KEY_STATS",
                    "fields": [
                        {"name": "_MIN_VALUES", "type": "bytes"},
                        {"name": "_MAX_VALUES", "type": "bytes"},
                        {"name": "_NULL_COUNTS", "type": ["null", {"type": "array", "items": ["null", "long"]}], "default": null}
                    ]
                }], "default": null},
                {"name": "_VALUE_STATS", "type": ["null", {
                    "type": "record",
                    "name": "record__FILE__VALUE_STATS",
                    "fields": [
                        {"name": "_MIN_VALUES", "type": "bytes"},
                        {"name": "_MAX_VALUES", "type": "bytes"},
                        {"name": "_NULL_COUNTS", "type": ["null", {"type": "array", "items": ["null", "long"]}], "default": null}
                    ]
                }], "default": null},
                {"name": "_MIN_SEQUENCE_NUMBER", "type": "long"},
                {"name": "_MAX_SEQUENCE_NUMBER", "type": "long"},
                {"name": "_SCHEMA_ID", "type": "long"},
                {"name": "_LEVEL", "type": "int"},
                {"name": "_EXTRA_FILES", "type": {"type": "array", "items": "string"}},
                {"name": "_CREATION_TIME", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null},
                {"name": "_DELETE_ROW_COUNT", "type": ["null", "long"], "default": null},
                {"name": "_EMBEDDED_FILE_INDEX", "type": ["null", "bytes"], "default": null},
                {"name": "_FILE_SOURCE", "type": ["null", "int"], "default": null},
                {"name": "_VALUE_STATS_COLS", "type": ["null", {"type": "array", "items": "string"}], "default": null},
                {"name": "_EXTERNAL_PATH", "type": ["null", "string"], "default": null},
                {"name": "_FIRST_ROW_ID", "type": ["null", "long"], "default": null},
                {"name": "_WRITE_COLS", "type": ["null", {"type": "array", "items": "string"}], "default": null}
            ]
        }], "default": null},
        {"name": "_VERSION", "type": "int"}
    ]
}]"#;
