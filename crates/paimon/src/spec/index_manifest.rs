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
use crate::spec::IndexFileMeta;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Manifest entry for index file.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/manifest/IndexManifestEntry.java>
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

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;

    use super::*;

    #[test]
    fn test_read_index_manifest_file() {
        let workdir =
            std::env::current_dir().unwrap_or_else(|err| panic!("current_dir must exist: {err}"));
        let path = workdir
            .join("tests/fixtures/manifest/index-manifest-85cc6729-f5af-431a-a1c3-ef45319328fb-0");
        let source = std::fs::read(path.to_str().unwrap()).unwrap();
        let mut reader =
            serde_avro_fast::object_container_file_encoding::Reader::from_slice(source.as_slice())
                .unwrap();
        let res: Vec<_> = reader
            .deserialize::<IndexManifestEntry>()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            res,
            vec![
                IndexManifestEntry {
                    version: 1,
                    kind: FileKind::Add,
                    partition: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                    bucket: 0,
                    index_file: IndexFileMeta {
                        index_type: "HASH".into(),
                        file_name: "index-a984b43a-c3fb-40b4-ad29-536343c239a6-0".into(),
                        file_size: 16,
                        row_count: 4,
                        deletion_vectors_ranges: None,
                    }
                },
                IndexManifestEntry {
                    version: 1,
                    kind: FileKind::Add,
                    partition: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                    bucket: 0,
                    index_file: IndexFileMeta {
                        index_type: "DELETION_VECTORS".into(),
                        file_name: "index-3f0986c5-4398-449b-be82-95f019d7a748-0".into(),
                        file_size: 33,
                        row_count: 1,
                        deletion_vectors_ranges: Some(IndexMap::from([(
                            "data-9b76122c-6bb5-4952-a946-b5bce29694a1-0.orc".into(),
                            (1, 24)
                        )])),
                    }
                }
            ]
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
                deletion_vectors_ranges: Some(IndexMap::from([("test1".into(), (1, 24))])),
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
                                {"name": "f2", "type": "int"}
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
