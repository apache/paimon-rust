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
use serde_avro_fast::object_container_file_encoding::Reader;
use snafu::ResultExt;
use std::fmt::{Display, Formatter};

use crate::Result;

/// Manifest entry for index file.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/manifest/IndexManifestEntry.java>
#[allow(dead_code)] // Part of spec; used when index manifest is implemented.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
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
        if !input_file.exists().await? {
            return Ok(Vec::new());
        }
        let content = input_file.read().await?;
        Self::read_from_bytes(&content)
    }

    /// Read index manifest entries from Avro-encoded bytes.
    pub fn read_from_bytes(bytes: &[u8]) -> Result<Vec<IndexManifestEntry>> {
        let mut reader = Reader::from_slice(bytes)
            .whatever_context::<_, crate::Error>("read index manifest avro")?;
        reader
            .deserialize::<IndexManifestEntry>()
            .collect::<std::result::Result<Vec<_>, _>>()
            .whatever_context::<_, crate::Error>("deserialize index manifest entry")
    }
}

#[cfg(test)]
mod tests {
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
            },
        };

        let schema: serde_avro_fast::Schema = r#"["null", {
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
            .parse().unwrap();

        let serializer_config = &mut serde_avro_fast::ser::SerializerConfig::new(&schema);
        let encoded = serde_avro_fast::to_single_object_vec(&sample, serializer_config).unwrap();
        let decoded: IndexManifestEntry =
            serde_avro_fast::from_single_object_slice(encoded.as_slice(), &schema).unwrap();
        assert_eq!(sample, decoded);
    }
}
