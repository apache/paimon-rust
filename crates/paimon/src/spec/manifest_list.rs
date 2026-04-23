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
        let bytes = crate::spec::to_avro_bytes(MANIFEST_FILE_META_SCHEMA, metas)?;
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
}
