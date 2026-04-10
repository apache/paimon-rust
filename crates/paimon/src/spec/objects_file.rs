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

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_avro_fast::object_container_file_encoding::{Compression, Reader};
use snafu::ResultExt;

pub fn from_avro_bytes<T: DeserializeOwned>(bytes: &[u8]) -> crate::Result<Vec<T>> {
    let mut reader = Reader::from_slice(bytes)
        .whatever_context::<_, crate::Error>("read avro object container")?;
    reader
        .deserialize::<T>()
        .collect::<std::result::Result<Vec<_>, _>>()
        .whatever_context::<_, crate::Error>("deserialize avro records")
}

/// Serialize records into Avro Object Container File bytes.
///
/// The `schema_json` must be a valid Avro schema JSON string that matches
/// the serde serialization layout of `T`.
pub fn to_avro_bytes<T: Serialize>(schema_json: &str, records: &[T]) -> crate::Result<Vec<u8>> {
    let schema: serde_avro_fast::Schema =
        schema_json
            .parse()
            .map_err(
                |e: serde_avro_fast::schema::SchemaError| crate::Error::DataInvalid {
                    message: format!("invalid avro schema: {e}"),
                    source: Some(Box::new(e)),
                },
            )?;
    serde_avro_fast::object_container_file_encoding::write_all(
        &schema,
        Compression::Null,
        Vec::new(),
        records.iter(),
    )
    .map_err(|e| crate::Error::DataInvalid {
        message: format!("avro serialization failed: {e}"),
        source: Some(Box::new(e)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::manifest_common::FileKind;
    use crate::spec::manifest_entry::{ManifestEntry, MANIFEST_ENTRY_SCHEMA};
    use crate::spec::manifest_file_meta::MANIFEST_FILE_META_SCHEMA;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{DataFileMeta, ManifestFileMeta};
    use chrono::{DateTime, Utc};

    #[test]
    fn test_roundtrip_manifest_file_meta() {
        let value_bytes = vec![
            0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 49, 0, 0, 0, 0, 0, 0, 129,
        ];
        let original = vec![ManifestFileMeta::new(
            "manifest-test-0".to_string(),
            1024,
            5,
            2,
            BinaryTableStats::new(value_bytes.clone(), value_bytes.clone(), vec![Some(1)]),
            0,
        )];
        let bytes = to_avro_bytes(MANIFEST_FILE_META_SCHEMA, &original).unwrap();
        let decoded = from_avro_bytes::<ManifestFileMeta>(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_roundtrip_manifest_entry() {
        let value_bytes = vec![
            0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 49, 0, 0, 0, 0, 0, 0, 129, 1, 0, 0, 0, 0, 0, 0, 0,
        ];
        let single_value = vec![0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        let original = vec![ManifestEntry::new(
            FileKind::Add,
            single_value.clone(),
            1,
            10,
            DataFileMeta {
                file_name: "test.parquet".to_string(),
                file_size: 100,
                row_count: 50,
                min_key: single_value.clone(),
                max_key: single_value.clone(),
                key_stats: BinaryTableStats::new(
                    value_bytes.clone(),
                    value_bytes.clone(),
                    vec![Some(1), Some(2)],
                ),
                value_stats: BinaryTableStats::new(
                    value_bytes.clone(),
                    value_bytes.clone(),
                    vec![Some(1), Some(2)],
                ),
                min_sequence_number: 1,
                max_sequence_number: 50,
                schema_id: 0,
                level: 0,
                extra_files: vec![],
                creation_time: Some(
                    "2024-09-06T07:45:55.039+00:00"
                        .parse::<DateTime<Utc>>()
                        .unwrap(),
                ),
                delete_row_count: Some(0),
                embedded_index: None,
                first_row_id: None,
                write_cols: None,
                external_path: None,
                file_source: None,
                value_stats_cols: None,
            },
            2,
        )];
        let bytes = to_avro_bytes(MANIFEST_ENTRY_SCHEMA, &original).unwrap();
        let decoded = from_avro_bytes::<ManifestEntry>(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[tokio::test]
    async fn test_read_manifest_list() {
        let workdir =
            std::env::current_dir().unwrap_or_else(|err| panic!("current_dir must exist: {err}"));
        let path = workdir
            .join("tests/fixtures/manifest/manifest-list-5c7399a0-46ae-4a5e-9c13-3ab07212cdb6-0");
        let v = std::fs::read(path.to_str().unwrap()).unwrap();
        let res = from_avro_bytes::<ManifestFileMeta>(&v).unwrap();
        let value_bytes = vec![
            0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 49, 0, 0, 0, 0, 0, 0, 129,
        ];
        assert_eq!(
            res,
            vec![
                ManifestFileMeta::new(
                    "manifest-19d138df-233f-46f7-beb6-fadaf4741c0e".to_string(),
                    10,
                    10,
                    10,
                    BinaryTableStats::new(
                        value_bytes.clone(),
                        value_bytes.clone(),
                        vec![Some(1), Some(2)]
                    ),
                    1
                ),
                ManifestFileMeta::new(
                    "manifest-a703ee48-c411-413e-b84e-c03bdb179631".to_string(),
                    11,
                    0,
                    10,
                    BinaryTableStats::new(
                        value_bytes.clone(),
                        value_bytes.clone(),
                        vec![Some(1), Some(2)]
                    ),
                    2
                )
            ],
        );
    }

    #[tokio::test]
    async fn test_read_manifest_entry() {
        let workdir =
            std::env::current_dir().unwrap_or_else(|err| panic!("current_dir must exist: {err}"));
        let path =
            workdir.join("tests/fixtures/manifest/manifest-8ded1f09-fcda-489e-9167-582ac0f9f846-0");
        let v = std::fs::read(path.to_str().unwrap()).unwrap();
        let res = from_avro_bytes::<ManifestEntry>(&v).unwrap();
        let value_bytes = vec![
            0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 49, 0, 0, 0, 0, 0, 0, 129, 1, 0, 0, 0, 0, 0, 0, 0,
        ];
        let single_value = vec![0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(
            res,
            vec![
                ManifestEntry::new(
                    FileKind::Delete,
                    single_value.clone(),
                    1,
                    10,
                    DataFileMeta {
                        file_name: "f1.parquet".to_string(),

                        file_size: 10,
                        row_count: 100,
                        min_key: single_value.clone(),
                        max_key: single_value.clone(),
                        key_stats: BinaryTableStats::new(
                            value_bytes.clone(),
                            value_bytes.clone(),
                            vec![Some(1), Some(2)]
                        ),
                        value_stats: BinaryTableStats::new(
                            value_bytes.clone(),
                            value_bytes.clone(),
                            vec![Some(1), Some(2)]
                        ),
                        min_sequence_number: 1,
                        max_sequence_number: 100,
                        schema_id: 0,
                        level: 1,
                        extra_files: vec![],
                        creation_time: Some(
                            "2024-09-06T07:45:55.039+00:00"
                                .parse::<DateTime<Utc>>()
                                .unwrap()
                        ),
                        delete_row_count: Some(0),
                        embedded_index: None,
                        first_row_id: None,
                        write_cols: None,
                        external_path: None,
                        file_source: None,
                        value_stats_cols: None,
                    },
                    2
                ),
                ManifestEntry::new(
                    FileKind::Add,
                    single_value.clone(),
                    2,
                    10,
                    DataFileMeta {
                        file_name: "f2.parquet".to_string(),
                        file_size: 10,
                        row_count: 100,
                        min_key: single_value.clone(),
                        max_key: single_value.clone(),
                        key_stats: BinaryTableStats::new(
                            value_bytes.clone(),
                            value_bytes.clone(),
                            vec![Some(1), Some(2)]
                        ),
                        value_stats: BinaryTableStats::new(
                            value_bytes.clone(),
                            value_bytes.clone(),
                            vec![Some(1), Some(2)]
                        ),
                        min_sequence_number: 1,
                        max_sequence_number: 100,
                        schema_id: 0,
                        level: 1,
                        extra_files: vec![],
                        creation_time: Some(
                            "2024-09-06T07:45:55.039+00:00"
                                .parse::<DateTime<Utc>>()
                                .unwrap()
                        ),
                        delete_row_count: Some(1),
                        embedded_index: None,
                        first_row_id: None,
                        write_cols: None,
                        external_path: None,
                        file_source: None,
                        value_stats_cols: None,
                    },
                    2
                ),
            ]
        )
    }
}
