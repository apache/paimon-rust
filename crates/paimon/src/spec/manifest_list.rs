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
use crate::spec::manifest_file_meta::MANIFEST_FILE_META_SCHEMA;
use crate::spec::ManifestFileMeta;
use crate::Result;

/// Manifest list file reader and writer.
///
/// A manifest list file contains a list of ManifestFileMeta records in Avro format.
/// Each record describes a manifest file.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/manifest/ManifestList.java>
pub struct ManifestList;

impl ManifestList {
    /// Read manifest file metas from a manifest list file.
    pub async fn read(file_io: &FileIO, path: &str) -> Result<Vec<ManifestFileMeta>> {
        let input = file_io.new_input(path)?;
        let content = input.read().await?;
        crate::spec::avro::from_avro_bytes_fast(&content)
    }

    /// Write manifest file metas to a manifest list file.
    pub async fn write(file_io: &FileIO, path: &str, metas: &[ManifestFileMeta]) -> Result<()> {
        Self::write_with_compression(file_io, path, metas, crate::spec::DEFAULT_AVRO_COMPRESSION)
            .await
    }

    /// Write manifest file metas with the configured Avro compression.
    pub async fn write_with_compression(
        file_io: &FileIO,
        path: &str,
        metas: &[ManifestFileMeta],
        compression: &str,
    ) -> Result<()> {
        let bytes = crate::spec::to_avro_bytes_with_compression(
            MANIFEST_FILE_META_SCHEMA,
            metas,
            compression,
        )?;
        let output = file_io.new_output(path)?;
        output.write(bytes::Bytes::from(bytes)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    #[tokio::test]
    async fn test_manifest_list_roundtrip() {
        let file_io = test_file_io();
        let path = "memory:/test_manifest_list_roundtrip/manifest-list-0";
        file_io
            .mkdirs("memory:/test_manifest_list_roundtrip/")
            .await
            .unwrap();

        let value_bytes = vec![
            0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 49, 0, 0, 0, 0, 0, 0, 129,
        ];
        let original = vec![
            ManifestFileMeta::new(
                "manifest-a".to_string(),
                1024,
                5,
                2,
                BinaryTableStats::new(value_bytes.clone(), value_bytes.clone(), vec![Some(1)]),
                0,
            ),
            ManifestFileMeta::new(
                "manifest-b".to_string(),
                2048,
                10,
                0,
                BinaryTableStats::new(value_bytes.clone(), value_bytes.clone(), vec![Some(3)]),
                1,
            ),
        ];

        ManifestList::write(&file_io, path, &original)
            .await
            .unwrap();
        let decoded = ManifestList::read(&file_io, path).await.unwrap();
        assert_eq!(original, decoded);
    }

    #[tokio::test]
    async fn test_manifest_list_read_nonexistent() {
        let file_io = test_file_io();
        let result = ManifestList::read(&file_io, "memory:/nonexistent/manifest-list").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_manifest_list_write_empty() {
        let file_io = test_file_io();
        let path = "memory:/test_manifest_list_empty/manifest-list-0";
        file_io
            .mkdirs("memory:/test_manifest_list_empty/")
            .await
            .unwrap();

        ManifestList::write(&file_io, path, &[]).await.unwrap();
        let decoded = ManifestList::read(&file_io, path).await.unwrap();
        assert!(decoded.is_empty());
    }

    /// Round-trip bucket / level statistics through Avro so future schema drift is caught
    /// here, not in production. Matches the fields added in apache/paimon#5345.
    #[tokio::test]
    async fn test_manifest_list_roundtrip_preserves_bucket_level_stats() {
        let file_io = test_file_io();
        let path = "memory:/test_manifest_list_bucket_level/manifest-list-0";
        file_io
            .mkdirs("memory:/test_manifest_list_bucket_level/")
            .await
            .unwrap();

        let value_bytes = vec![
            0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 49, 0, 0, 0, 0, 0, 0, 129,
        ];
        let original = vec![ManifestFileMeta::new(
            "manifest-bucket-level".to_string(),
            4096,
            3,
            0,
            BinaryTableStats::new(value_bytes.clone(), value_bytes.clone(), vec![Some(0)]),
            0,
        )
        .with_bucket_level_stats(Some(-1), Some(7), Some(0), Some(5))];

        ManifestList::write(&file_io, path, &original)
            .await
            .unwrap();
        let decoded = ManifestList::read(&file_io, path).await.unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].min_bucket(), Some(-1));
        assert_eq!(decoded[0].max_bucket(), Some(7));
        assert_eq!(decoded[0].min_level(), Some(0));
        assert_eq!(decoded[0].max_level(), Some(5));
        // Sanity: nothing else changed.
        assert_eq!(decoded[0].file_name(), "manifest-bucket-level");
        assert_eq!(decoded[0].num_added_files(), 3);
    }

    /// Back-compat: a manifest list written without the bucket / level fields (e.g. by an
    /// older Rust writer or any Java writer pre apache/paimon#5345) must decode into
    /// `None` rather than failing or yielding bogus values.
    #[tokio::test]
    async fn test_manifest_list_decodes_legacy_without_bucket_level_fields() {
        use apache_avro::{Codec, Schema, Writer};
        use std::collections::HashMap;

        let file_io = test_file_io();
        let path = "memory:/test_manifest_list_legacy/manifest-list-0";
        file_io
            .mkdirs("memory:/test_manifest_list_legacy/")
            .await
            .unwrap();

        // Avro schema with the pre-5345 shape: no _MIN/_MAX_BUCKET/LEVEL fields.
        let legacy_schema = r#"["null", {
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
                }], "default": null}
            ]
        }]"#;
        let schema = Schema::parse_str(legacy_schema).unwrap();
        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Null);
        let value_bytes = vec![0u8; 12];
        let mut record: HashMap<String, apache_avro::types::Value> = HashMap::new();
        record.insert("_VERSION".to_string(), apache_avro::types::Value::Int(2));
        record.insert(
            "_FILE_NAME".to_string(),
            apache_avro::types::Value::String("manifest-legacy".to_string()),
        );
        record.insert(
            "_FILE_SIZE".to_string(),
            apache_avro::types::Value::Long(1024),
        );
        record.insert(
            "_NUM_ADDED_FILES".to_string(),
            apache_avro::types::Value::Long(2),
        );
        record.insert(
            "_NUM_DELETED_FILES".to_string(),
            apache_avro::types::Value::Long(0),
        );
        record.insert(
            "_PARTITION_STATS".to_string(),
            apache_avro::types::Value::Union(
                1,
                Box::new(apache_avro::types::Value::Record(vec![
                    (
                        "_MIN_VALUES".to_string(),
                        apache_avro::types::Value::Bytes(value_bytes.clone()),
                    ),
                    (
                        "_MAX_VALUES".to_string(),
                        apache_avro::types::Value::Bytes(value_bytes.clone()),
                    ),
                    (
                        "_NULL_COUNTS".to_string(),
                        apache_avro::types::Value::Union(
                            0,
                            Box::new(apache_avro::types::Value::Null),
                        ),
                    ),
                ])),
            ),
        );
        let value = apache_avro::types::Value::Union(
            1,
            Box::new(apache_avro::types::Value::Record(
                record.into_iter().collect(),
            )),
        );
        let resolved = value.resolve(&schema).unwrap();
        writer.append(resolved).unwrap();
        let bytes = writer.into_inner().unwrap();
        file_io
            .new_output(path)
            .unwrap()
            .write(bytes::Bytes::from(bytes))
            .await
            .unwrap();

        let decoded = ManifestList::read(&file_io, path).await.unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].file_name(), "manifest-legacy");
        assert_eq!(decoded[0].min_bucket(), None);
        assert_eq!(decoded[0].max_bucket(), None);
        assert_eq!(decoded[0].min_level(), None);
        assert_eq!(decoded[0].max_level(), None);
    }
}
