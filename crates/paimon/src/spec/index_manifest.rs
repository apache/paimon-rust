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

use crate::io::FileIO;
use crate::spec::manifest_common::FileKind;
use crate::spec::IndexFileMeta;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

use crate::Result;

/// Avro schema for IndexManifestEntry OCF serialization.
///
/// Must match the serde layout of `IndexManifestEntry`.
///
/// Note: `_FILE_SIZE` and `_ROW_COUNT` are declared as Avro `long` to match
/// Java Paimon's schema, while the Rust `IndexFileMeta` fields are `i32`.
/// `serde_avro_fast` transparently coerces between integer widths during
/// serialization/deserialization, so the mismatch is intentional.
pub const INDEX_MANIFEST_ENTRY_SCHEMA: &str = r#"{
    "type": "record",
    "name": "org.apache.paimon.avro.generated.record",
    "fields": [
        {"name": "_VERSION", "type": "int"},
        {"name": "_KIND", "type": "int"},
        {"name": "_PARTITION", "type": "bytes"},
        {"name": "_BUCKET", "type": "int"},
        {"name": "_INDEX_TYPE", "type": "string"},
        {"name": "_FILE_NAME", "type": "string"},
        {"name": "_FILE_SIZE", "type": "long"},
        {"name": "_ROW_COUNT", "type": "long"},
        {
            "default": null,
            "name": "_DELETIONS_VECTORS_RANGES",
            "type": ["null", {
                "type": "array",
                "items": ["null", {
                    "type": "record",
                    "name": "org.apache.paimon.avro.generated.record__DELETIONS_VECTORS_RANGES",
                    "fields": [
                        {"name": "f0", "type": "string"},
                        {"name": "f1", "type": "int"},
                        {"name": "f2", "type": "int"},
                        {"name": "_CARDINALITY", "type": ["null", "long"], "default": null}
                    ]
                }]
            }]
        },
        {
            "default": null,
            "name": "_GLOBAL_INDEX",
            "type": ["null", {
                "type": "record",
                "name": "org.apache.paimon.avro.generated.record__GLOBAL_INDEX",
                "fields": [
                    {"name": "_ROW_RANGE_START", "type": "long"},
                    {"name": "_ROW_RANGE_END", "type": "long"},
                    {"name": "_INDEX_FIELD_ID", "type": "int"},
                    {"name": "_EXTRA_FIELD_IDS", "type": ["null", {"type": "array", "items": "int"}], "default": null},
                    {"name": "_INDEX_META", "type": ["null", "bytes"], "default": null}
                ]
            }]
        }
    ]
}"#;

/// Manifest entry for index file.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/manifest/IndexManifestEntry.java>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexManifestEntry {
    #[serde(rename = "_KIND")]
    pub kind: FileKind,

    #[serde(rename = "_PARTITION", with = "serde_bytes")]
    pub partition: Vec<u8>,

    #[serde(rename = "_BUCKET")]
    pub bucket: i32,

    #[serde(flatten)]
    pub index_file: IndexFileMeta,

    #[serde(rename = "_VERSION")]
    pub version: i32,
}

impl Display for IndexManifestEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IndexManifestEntry{{kind={:?}, partition={:?}, bucket={}, index_file={}}}",
            self.kind, self.partition, self.bucket, self.index_file,
        )
    }
}

/// Index manifest file reader (entries describing index files per partition/bucket).
///
/// Reference: [org.apache.paimon.index.IndexFileHandler](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/index/IndexFileHandler.java)
pub struct IndexManifest;

impl IndexManifest {
    /// Read index manifest entries from a file.
    pub async fn read(file_io: &FileIO, path: &str) -> Result<Vec<IndexManifestEntry>> {
        let input_file = file_io.new_input(path)?;
        let content = input_file.read().await?;
        Self::read_from_bytes(&content)
    }

    /// Read index manifest entries from Avro-encoded bytes.
    pub fn read_from_bytes(bytes: &[u8]) -> Result<Vec<IndexManifestEntry>> {
        crate::spec::avro::from_avro_bytes_fast(bytes)
    }

    /// Write index manifest entries to a file.
    pub async fn write(file_io: &FileIO, path: &str, entries: &[IndexManifestEntry]) -> Result<()> {
        let bytes = crate::spec::to_avro_bytes(INDEX_MANIFEST_ENTRY_SCHEMA, entries)?;
        let output = file_io.new_output(path)?;
        output.write(bytes::Bytes::from(bytes)).await
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value, types::Value, Schema};
    use indexmap::IndexMap;

    use super::*;
    use crate::spec::DeletionVectorMeta;

    #[test]
    fn test_read_index_manifest_file() {
        let workdir =
            std::env::current_dir().unwrap_or_else(|err| panic!("current_dir must exist: {err}"));
        let path = workdir
            .join("tests/fixtures/manifest/index-manifest-7e816ed9-9f3b-4786-9985-8937d4e07b6e-0");
        let source = std::fs::read(path.to_str().unwrap()).unwrap();
        let res = IndexManifest::read_from_bytes(&source).unwrap();
        assert_eq!(
            res,
            vec![IndexManifestEntry {
                version: 1,
                kind: FileKind::Add,
                partition: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                bucket: 0,
                index_file: IndexFileMeta {
                    index_type: "DELETION_VECTORS".into(),
                    file_name: "index-4326356b-aad7-4fd8-9d88-2bb6993c8ce9-0".into(),
                    file_size: 35,
                    row_count: 1,
                    deletion_vectors_ranges: Some(IndexMap::from([(
                        "data-a989fc44-a361-42c2-801f-e50baba95a92-0.parquet".into(),
                        DeletionVectorMeta {
                            offset: 1,
                            length: 26,
                            cardinality: Some(3),
                        }
                    )])),
                    global_index_meta: None,
                }
            }]
        );
    }

    #[test]
    fn test_single_object_serde() {
        let sample = IndexManifestEntry {
            version: 1,
            kind: FileKind::Delete,
            partition: vec![0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6],
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: "DELETION_VECTORS".into(),
                file_name: "test1".into(),
                file_size: 33,
                row_count: 1,
                deletion_vectors_ranges: Some(IndexMap::from([(
                    "test1".into(),
                    DeletionVectorMeta {
                        offset: 1,
                        length: 24,
                        cardinality: Some(7),
                    },
                )])),
                global_index_meta: None,
            },
        };

        let schema = Schema::parse_str(r#"["null", {
            "type": "record", 
            "name": "org.apache.paimon.avro.generated.record", 
            "fields": [
                {"name": "_VERSION", "type": "int"}, 
                {"name": "_KIND", "type": "int"}, 
                {"name": "_PARTITION", "type": "bytes"}, 
                {"name": "_BUCKET", "type": "int"}, 
                {"name": "_INDEX_TYPE", "type": "string"}, 
                {"name": "_FILE_NAME", "type": "string"}, 
                {"name": "_FILE_SIZE", "type": "long"}, 
                {"name": "_ROW_COUNT", "type": "long"}, 
                {
                    "default": null, 
                    "name": "_DELETIONS_VECTORS_RANGES", 
                    "type": ["null", {
                        "type": "array", 
                        "items": ["null", {
                            "type": "record", 
                            "name": "org.apache.paimon.avro.generated.record__DELETIONS_VECTORS_RANGES", 
                            "fields": [
                                {"name": "f0", "type": "string"}, 
                                {"name": "f1", "type": "int"}, 
                                {"name": "f2", "type": "int"},
                                {"name": "_CARDINALITY", "type": ["null", "long"], "default": null}
                            ]
                        }]
                    }]
                }
            ]
            }]"#
        )
        .unwrap();

        let value = to_value(&sample).unwrap().resolve(&schema).unwrap();
        let encoded = to_avro_datum(&schema, value).unwrap();
        let decoded_value = from_avro_datum(&schema, &mut encoded.as_slice(), None).unwrap();
        let decoded: IndexManifestEntry = match decoded_value {
            Value::Union(_, inner) => from_value(inner.as_ref()).unwrap(),
            other => from_value(&other).unwrap(),
        };
        assert_eq!(sample, decoded);
    }
}
