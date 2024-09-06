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
use crate::spec::manifest_entry::ManifestEntry;
use crate::spec::ManifestFileMeta;
use crate::Error;
use apache_avro::types::Value;
use apache_avro::{from_value, Reader};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
/// This file includes several [T]
pub struct ObjectsFile<T> {
    entries: Vec<T>,
}

#[allow(dead_code)]
impl<T> ObjectsFile<T>
where
    T: DeserializeOwned,
{
    pub fn entries(&self) -> &Vec<T> {
        &self.entries
    }

    pub fn from_avro_bytes(bytes: &[u8]) -> crate::Result<ObjectsFile<T>> {
        let reader = Reader::new(bytes).map_err(Error::from)?;
        let records = reader
            .collect::<Result<Vec<Value>, _>>()
            .map_err(Error::from)?;
        let values = Value::Array(records);
        from_value::<ObjectsFile<T>>(&values).map_err(Error::from)
    }
}

pub struct ObjectsFileFactory {
    file_io: FileIO,
}

/// The factory to read and write [`ObjectsFile`]
#[allow(dead_code)]
impl ObjectsFileFactory {
    pub fn new(file_io: FileIO) -> ObjectsFileFactory {
        Self { file_io }
    }

    /// Write several [`ManifestFileMeta`]s into a manifest list.
    ///
    /// NOTE: This method is atomic.
    pub fn write(&mut self, _metas: Vec<ManifestFileMeta>) -> &str {
        todo!()
    }

    /// Read [`ManifestList`] from the manifest file.
    pub async fn read_manifest_file_meta(
        &self,
        path: &str,
    ) -> crate::Result<ObjectsFile<ManifestFileMeta>> {
        let bs = self.file_io.new_input(path)?.read().await?;
        // todo support other formats
        ObjectsFile::<ManifestFileMeta>::from_avro_bytes(bs.as_ref())
    }

    /// Read [`ManifestEntry`] from the manifest file.
    pub async fn read_manifest_entry(
        &self,
        path: &str,
    ) -> crate::Result<ObjectsFile<ManifestEntry>> {
        let bs = self.file_io.new_input(path)?.read().await?;
        // todo support other formats
        ObjectsFile::<ManifestEntry>::from_avro_bytes(bs.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use crate::spec::manifest_entry::{FileKind, ManifestEntry};
    use crate::spec::objects_file::ObjectsFile;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{DataFileMeta, ManifestFileMeta};
    use chrono::{DateTime, Utc};

    #[tokio::test]
    async fn test_read_manifest_list() {
        let workdir =
            std::env::current_dir().unwrap_or_else(|err| panic!("current_dir must exist: {err}"));
        let path = workdir
            .join("tests/fixtures/manifest/manifest-list-5c7399a0-46ae-4a5e-9c13-3ab07212cdb6-0");
        let v = std::fs::read(path.to_str().unwrap()).unwrap();
        let res = ObjectsFile::<ManifestFileMeta>::from_avro_bytes(&v).unwrap();
        let value_bytes = vec![
            0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 49, 0, 0, 0, 0, 0, 0, 129,
        ];
        assert_eq!(
            res,
            ObjectsFile {
                entries: vec![
                    ManifestFileMeta::new(
                        "manifest-19d138df-233f-46f7-beb6-fadaf4741c0e".to_string(),
                        10,
                        10,
                        10,
                        BinaryTableStats::new(value_bytes.clone(), value_bytes.clone(), vec![1, 2]),
                        1
                    ),
                    ManifestFileMeta::new(
                        "manifest-a703ee48-c411-413e-b84e-c03bdb179631".to_string(),
                        11,
                        0,
                        10,
                        BinaryTableStats::new(value_bytes.clone(), value_bytes.clone(), vec![1, 2]),
                        2
                    )
                ],
            }
        );
    }

    #[tokio::test]
    async fn test_read_manifest_entry() {
        let workdir =
            std::env::current_dir().unwrap_or_else(|err| panic!("current_dir must exist: {err}"));
        let path =
            workdir.join("tests/fixtures/manifest/manifest-8ded1f09-fcda-489e-9167-582ac0f9f846-0");
        let v = std::fs::read(path.to_str().unwrap()).unwrap();
        let res = ObjectsFile::<ManifestEntry>::from_avro_bytes(&v).unwrap();
        let value_bytes = vec![
            0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 49, 0, 0, 0, 0, 0, 0, 129, 1, 0, 0, 0, 0, 0, 0, 0,
        ];
        let single_value = vec![0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(
            res,
            ObjectsFile {
                entries: vec![
                    ManifestEntry::new(
                        FileKind::Delete,
                        single_value.clone(),
                        1,
                        10,
                        DataFileMeta::new(
                            "f1.parquet".to_string(),
                            10,
                            100,
                            single_value.clone(),
                            single_value.clone(),
                            BinaryTableStats::new(
                                value_bytes.clone(),
                                value_bytes.clone(),
                                vec![1, 2]
                            ),
                            BinaryTableStats::new(
                                value_bytes.clone(),
                                value_bytes.clone(),
                                vec![1, 2]
                            ),
                            1,
                            100,
                            0,
                            1,
                            vec![],
                            "2024-09-06T07:45:55.039+00:00"
                                .parse::<DateTime<Utc>>()
                                .unwrap(),
                            Some(0),
                            None,
                        ),
                        2
                    ),
                    ManifestEntry::new(
                        FileKind::Add,
                        single_value.clone(),
                        2,
                        10,
                        DataFileMeta::new(
                            "f2.parquet".to_string(),
                            10,
                            100,
                            single_value.clone(),
                            single_value.clone(),
                            BinaryTableStats::new(
                                value_bytes.clone(),
                                value_bytes.clone(),
                                vec![1, 2]
                            ),
                            BinaryTableStats::new(
                                value_bytes.clone(),
                                value_bytes.clone(),
                                vec![1, 2]
                            ),
                            1,
                            100,
                            0,
                            1,
                            vec![],
                            "2024-09-06T07:45:55.039+00:00"
                                .parse::<DateTime<Utc>>()
                                .unwrap(),
                            Some(1),
                            None,
                        ),
                        2
                    ),
                ]
            }
        )
    }
}
