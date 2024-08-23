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

use super::manifest_file_meta::ManifestFileMeta;
use crate::io::FileIO;
use crate::{Error, Result};
use apache_avro::types::Value;
use apache_avro::{from_value, Reader};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
/// This file includes several [`ManifestFileMeta`], representing all data of the whole table at the corresponding snapshot.
pub struct ManifestList {
    entries: Vec<ManifestFileMeta>,
}

impl ManifestList {
    pub fn entries(&self) -> &Vec<ManifestFileMeta> {
        &self.entries
    }

    pub fn from_avro_bytes(bytes: &[u8]) -> Result<ManifestList> {
        let reader = Reader::new(bytes).map_err(Error::from)?;
        let records = reader
            .collect::<std::result::Result<Vec<Value>, _>>()
            .map_err(Error::from)?;
        let values = Value::Array(records);
        from_value::<ManifestList>(&values).map_err(Error::from)
    }
}

pub struct ManifestListFactory {
    file_io: FileIO,
}

/// The factory to read and write [`ManifestList`]
impl ManifestListFactory {
    pub fn new(file_io: FileIO) -> ManifestListFactory {
        Self { file_io }
    }

    /// Write several [`ManifestFileMeta`]s into a manifest list.
    ///
    /// NOTE: This method is atomic.
    pub fn write(&mut self, _metas: Vec<ManifestFileMeta>) -> &str {
        todo!()
    }

    /// Read [`ManifestList`] from the manifest file.
    pub async fn read(&self, path: &str) -> Result<ManifestList> {
        let bs = self.file_io.new_input(path)?.read().await?;
        // todo support other formats
        ManifestList::from_avro_bytes(bs.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use crate::spec::{BinaryTableStats, ManifestFileMeta, ManifestList};

    #[tokio::test]
    async fn test_read_manifest_list() {
        let workdir =
            std::env::current_dir().unwrap_or_else(|err| panic!("current_dir must exist: {err}"));
        let path = workdir
            .join("tests/fixtures/manifest/manifest-list-5c7399a0-46ae-4a5e-9c13-3ab07212cdb6-0");
        let v = std::fs::read(path.to_str().unwrap()).unwrap();
        let res = ManifestList::from_avro_bytes(&v).unwrap();
        let value_bytes = vec![
            0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 49, 0, 0, 0, 0, 0, 0, 129,
        ];
        assert_eq!(
            res,
            ManifestList {
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
}
