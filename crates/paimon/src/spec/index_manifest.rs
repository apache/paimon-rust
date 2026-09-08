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
/// Note: `_FILE_SIZE` and `_ROW_COUNT` are Avro `long`s and Rust `i64`s,
/// matching Java Paimon's schema.
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
        {"name": "_EXTERNAL_PATH", "type": ["null", "string"], "default": null},
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
                    {"name": "_INDEX_META", "type": ["null", "bytes"], "default": null},
                    {"name": "_SOURCE_META", "type": ["null", "bytes"], "default": null}
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
    // Keep serde field order aligned with Java's IndexManifestEntrySerializer.
    #[serde(rename = "_VERSION")]
    pub version: i32,

    #[serde(rename = "_KIND")]
    pub kind: FileKind,

    #[serde(rename = "_PARTITION", with = "serde_bytes")]
    pub partition: Vec<u8>,

    #[serde(rename = "_BUCKET")]
    pub bucket: i32,

    #[serde(flatten)]
    pub index_file: IndexFileMeta,
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

    /// Read index manifest entries and return their byte size.
    pub async fn read_with_size(
        file_io: &FileIO,
        path: &str,
    ) -> Result<(Vec<IndexManifestEntry>, i64)> {
        let input_file = file_io.new_input(path)?;
        let content = input_file.read().await?;
        let size = content.len() as i64;
        let entries = Self::read_from_bytes(&content)?;
        Ok((entries, size))
    }

    /// Read index manifest entries from Avro-encoded bytes.
    pub fn read_from_bytes(bytes: &[u8]) -> Result<Vec<IndexManifestEntry>> {
        crate::spec::avro::from_avro_bytes_fast(bytes)
    }

    /// Write index manifest entries to a file.
    pub async fn write(file_io: &FileIO, path: &str, entries: &[IndexManifestEntry]) -> Result<()> {
        Self::write_with_compression(
            file_io,
            path,
            entries,
            crate::spec::DEFAULT_AVRO_COMPRESSION,
        )
        .await
    }

    /// Write index manifest entries with the configured Avro compression.
    pub async fn write_with_compression(
        file_io: &FileIO,
        path: &str,
        entries: &[IndexManifestEntry],
        compression: &str,
    ) -> Result<()> {
        let bytes = crate::spec::to_avro_bytes_with_compression(
            INDEX_MANIFEST_ENTRY_SCHEMA,
            entries,
            compression,
        )?;
        let output = file_io.new_output(path)?;
        output.write(bytes::Bytes::from(bytes)).await
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value, types::Value, Schema};
    use indexmap::IndexMap;

    use super::*;
    use crate::spec::{DeletionVectorMeta, GlobalIndexMeta};

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
                    external_path: None,
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
                external_path: None,
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

    fn global_index_entry(source_meta: Option<Vec<u8>>) -> IndexManifestEntry {
        IndexManifestEntry {
            version: 1,
            kind: FileKind::Add,
            partition: vec![0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6],
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: "GLOBAL_INDEX".into(),
                file_name: "gi-1".into(),
                file_size: 42,
                row_count: 7,
                deletion_vectors_ranges: None,
                external_path: None,
                global_index_meta: Some(GlobalIndexMeta {
                    row_range_start: 10,
                    row_range_end: 20,
                    index_field_id: 3,
                    extra_field_ids: Some(vec![4, 5]),
                    index_meta: Some(vec![9, 8, 7]),
                    source_meta,
                }),
            },
        }
    }

    #[test]
    fn test_index_manifest_serializes_version_first() {
        let entry = global_index_entry(None);
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.starts_with(r#"{"_VERSION":1,"_KIND":0,"#), "{json}");
    }

    #[test]
    fn source_meta_round_trips_through_index_manifest() {
        // New-format writer schema carries _SOURCE_META; a Some(..) value must round-trip.
        let entry = global_index_entry(Some(vec![1, 2, 3]));
        let bytes = crate::spec::to_avro_bytes_with_compression(
            INDEX_MANIFEST_ENTRY_SCHEMA,
            std::slice::from_ref(&entry),
            crate::spec::DEFAULT_AVRO_COMPRESSION,
        )
        .unwrap();
        let decoded = IndexManifest::read_from_bytes(&bytes).unwrap();
        assert_eq!(decoded[0], entry);
        assert_eq!(
            decoded[0]
                .index_file
                .global_index_meta
                .as_ref()
                .unwrap()
                .source_meta,
            Some(vec![1, 2, 3])
        );

        // A None source_meta must also round-trip as None.
        let entry_none = global_index_entry(None);
        let bytes_none = crate::spec::to_avro_bytes_with_compression(
            INDEX_MANIFEST_ENTRY_SCHEMA,
            std::slice::from_ref(&entry_none),
            crate::spec::DEFAULT_AVRO_COMPRESSION,
        )
        .unwrap();
        let decoded_none = IndexManifest::read_from_bytes(&bytes_none).unwrap();
        assert_eq!(decoded_none[0], entry_none);
        assert_eq!(
            decoded_none[0]
                .index_file
                .global_index_meta
                .as_ref()
                .unwrap()
                .source_meta,
            None
        );
    }

    #[test]
    fn file_size_above_i32_max_round_trips_through_index_manifest() {
        let file_size = i64::from(i32::MAX) + 1;
        let entry: IndexManifestEntry = serde_json::from_value(serde_json::json!({
            "_VERSION": 1,
            "_KIND": 0,
            "_PARTITION": [0, 0, 0, 0],
            "_BUCKET": 0,
            "_INDEX_TYPE": "TEST",
            "_FILE_NAME": "index",
            "_FILE_SIZE": file_size,
            "_ROW_COUNT": 1
        }))
        .unwrap();

        let bytes = crate::spec::to_avro_bytes_with_compression(
            INDEX_MANIFEST_ENTRY_SCHEMA,
            std::slice::from_ref(&entry),
            crate::spec::DEFAULT_AVRO_COMPRESSION,
        )
        .unwrap();

        let decoded = IndexManifest::read_from_bytes(&bytes).unwrap();
        assert_eq!(decoded, vec![entry]);
    }

    #[test]
    fn row_count_above_i32_max_round_trips_through_index_manifest() {
        // _ROW_COUNT is an Avro `long`; a source-backed index over a large data
        // file can exceed i32::MAX. The decoder must not truncate it.
        let row_count = i64::from(i32::MAX) + 100;
        let entry: IndexManifestEntry = serde_json::from_value(serde_json::json!({
            "_VERSION": 1,
            "_KIND": 0,
            "_PARTITION": [0, 0, 0, 0],
            "_BUCKET": 0,
            "_INDEX_TYPE": "TEST",
            "_FILE_NAME": "index",
            "_FILE_SIZE": 42,
            "_ROW_COUNT": row_count
        }))
        .unwrap();

        let bytes = crate::spec::to_avro_bytes_with_compression(
            INDEX_MANIFEST_ENTRY_SCHEMA,
            std::slice::from_ref(&entry),
            crate::spec::DEFAULT_AVRO_COMPRESSION,
        )
        .unwrap();

        let decoded = IndexManifest::read_from_bytes(&bytes).unwrap();
        assert_eq!(decoded[0].index_file.row_count, row_count);
        assert_eq!(decoded, vec![entry]);
    }

    #[test]
    fn decodes_index_external_path_at_its_schema_position() {
        // `_EXTERNAL_PATH` (nullable string) is field 5 of Java `IndexFileMeta.SCHEMA`,
        // between `_DELETIONS_VECTORS_RANGES` and `_GLOBAL_INDEX`. A manifest written
        // for an externally-stored index file records the absolute path there; the
        // decoder must read it from that position rather than skip it.
        const SCHEMA_WITH_EXTERNAL_PATH: &str = r#"{
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
        {"name": "_EXTERNAL_PATH", "type": ["null", "string"], "default": null},
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
                    {"name": "_INDEX_META", "type": ["null", "bytes"], "default": null},
                    {"name": "_SOURCE_META", "type": ["null", "bytes"], "default": null}
                ]
            }]
        }
    ]
}"#;

        let external = "s3://bucket/warehouse/db/tbl/index/idx-external-0";
        let entry: IndexManifestEntry = serde_json::from_value(serde_json::json!({
            "_VERSION": 1,
            "_KIND": 0,
            "_PARTITION": [0, 0, 0, 0],
            "_BUCKET": 0,
            "_INDEX_TYPE": "TEST",
            "_FILE_NAME": "idx-external-0",
            "_FILE_SIZE": 42,
            "_ROW_COUNT": 7,
            "_EXTERNAL_PATH": external
        }))
        .unwrap();

        let bytes = crate::spec::to_avro_bytes_with_compression(
            SCHEMA_WITH_EXTERNAL_PATH,
            std::slice::from_ref(&entry),
            crate::spec::DEFAULT_AVRO_COMPRESSION,
        )
        .unwrap();

        let decoded = IndexManifest::read_from_bytes(&bytes).unwrap();
        assert_eq!(
            decoded[0].index_file.external_path.as_deref(),
            Some(external)
        );
    }

    #[test]
    fn decodes_null_index_external_path_as_none() {
        // A manifest whose `_EXTERNAL_PATH` is present but null decodes to `None`.
        // The field being absent from the writer schema entirely is covered by
        // `legacy_schema_without_external_path_field_decodes_as_none`.
        let entry: IndexManifestEntry = serde_json::from_value(serde_json::json!({
            "_VERSION": 1,
            "_KIND": 0,
            "_PARTITION": [0, 0, 0, 0],
            "_BUCKET": 0,
            "_INDEX_TYPE": "TEST",
            "_FILE_NAME": "idx-local-0",
            "_FILE_SIZE": 42,
            "_ROW_COUNT": 7
        }))
        .unwrap();

        let bytes = crate::spec::to_avro_bytes_with_compression(
            INDEX_MANIFEST_ENTRY_SCHEMA,
            std::slice::from_ref(&entry),
            crate::spec::DEFAULT_AVRO_COMPRESSION,
        )
        .unwrap();

        let decoded = IndexManifest::read_from_bytes(&bytes).unwrap();
        assert_eq!(decoded[0].index_file.external_path, None);
    }

    #[test]
    fn legacy_schema_without_external_path_field_decodes_as_none() {
        // A writer schema from before either field existed: no `_SOURCE_META` inside
        // `_GLOBAL_INDEX` (pre-#8549), and no `_EXTERNAL_PATH` at all. The decoder
        // walks the writer's own field list, so both must simply be absent from the
        // decoded entry without misaligning the stream.
        const LEGACY_SCHEMA: &str = r#"{
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

        // Written by a pre-#8549 writer (5-field record, source_meta absent).
        let entry = global_index_entry(None);
        let bytes = crate::spec::to_avro_bytes_with_compression(
            LEGACY_SCHEMA,
            std::slice::from_ref(&entry),
            crate::spec::DEFAULT_AVRO_COMPRESSION,
        )
        .unwrap();
        // Decoding with the current reader must not misalign the stream.
        let decoded = IndexManifest::read_from_bytes(&bytes).unwrap();
        assert_eq!(decoded[0], entry);
        assert_eq!(
            decoded[0]
                .index_file
                .global_index_meta
                .as_ref()
                .unwrap()
                .source_meta,
            None
        );
        assert_eq!(decoded[0].index_file.external_path, None);
    }
}
