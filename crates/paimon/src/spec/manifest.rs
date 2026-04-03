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
use serde_avro_fast::object_container_file_encoding::Reader;
use snafu::ResultExt;

use crate::Result;

/// Manifest file reader and writer.
///
/// A manifest file contains a list of ManifestEntry records in Avro format.
/// Each entry represents an addition or deletion of a data file.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/manifest/ManifestFile.java>
pub struct Manifest;

#[allow(dead_code)]
impl Manifest {
    /// Read manifest entries from a file.
    ///
    /// # Arguments
    /// * `file_io` - FileIO instance for reading files
    /// * `path` - Path to the manifest file
    ///
    /// # Returns
    /// A vector of ManifestEntry records
    pub async fn read(file_io: &FileIO, path: &str) -> Result<Vec<ManifestEntry>> {
        let input_file = file_io.new_input(path)?;

        if !input_file.exists().await? {
            return Ok(Vec::new());
        }

        let content = input_file.read().await?;
        Self::read_from_bytes(&content)
    }

    /// Read manifest entries from bytes.
    ///
    /// # Arguments
    /// * `bytes` - Avro-encoded manifest file content
    ///
    /// # Returns
    /// A vector of ManifestEntry records
    fn read_from_bytes(bytes: &[u8]) -> Result<Vec<ManifestEntry>> {
        let mut reader =
            Reader::from_slice(bytes).whatever_context::<_, crate::Error>("read manifest avro")?;
        reader
            .deserialize::<ManifestEntry>()
            .collect::<std::result::Result<Vec<_>, _>>()
            .whatever_context::<_, crate::Error>("deserialize manifest entry")
    }
}

#[cfg(test)]
#[cfg(not(windows))] // Skip on Windows due to path compatibility issues
mod tests {
    use super::*;
    use crate::io::FileIO;
    use crate::spec::manifest_common::FileKind;
    use std::env::current_dir;

    #[tokio::test]
    async fn test_read_manifest_from_file() {
        let workdir = current_dir().unwrap();
        let path =
            workdir.join("tests/fixtures/manifest/manifest-8ded1f09-fcda-489e-9167-582ac0f9f846-0");

        let file_io = FileIO::from_url("file://").unwrap().build().unwrap();
        let entries = Manifest::read(&file_io, path.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(entries.len(), 2);
        // verify manifest entry
        let t1 = &entries[0];
        assert_eq!(t1.kind(), &FileKind::Delete);
        assert_eq!(t1.bucket(), 1);

        let t2 = &entries[1];
        assert_eq!(t2.kind(), &FileKind::Add);
        assert_eq!(t2.bucket(), 2);
    }
}
