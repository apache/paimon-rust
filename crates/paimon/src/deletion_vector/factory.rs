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

use crate::deletion_vector::core::DeletionVector;
use crate::io::{FileIO, FileRead};
use crate::spec::DataFileMeta;
use crate::Result;
use std::collections::HashMap;
use std::sync::Arc;

/// Factory for creating DeletionVector instances from files and metadata.
///
/// Corresponds to Java's [DeletionVector.Factory](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/deletionvectors/DeletionVector.java)
/// (create(fileName) -> Optional<DeletionVector>). Can be built from split-level deletion files
/// ([create_from_deletion_files]) or from index manifest entries ([create]).
pub struct DeletionVectorFactory {
    /// Map from data file name to its deletion vector
    deletion_vectors: HashMap<String, Arc<DeletionVector>>,
}

impl DeletionVectorFactory {
    /// Create a DeletionVectorFactory from data file names and their optional deletion files.
    /// Same as Java's `DeletionVector.factory(fileIO, files, deletionFiles)`: for each file that
    /// has a DeletionFile, reads path/offset/length and loads the DV.
    pub async fn new(
        file_io: &FileIO,
        data_files: &[DataFileMeta],
        data_deletion_files: Option<&[Option<crate::DeletionFile>]>,
    ) -> Result<Self> {
        let mut deletion_vectors = HashMap::new();
        let Some(data_deletion_files) = data_deletion_files else {
            return Ok(DeletionVectorFactory { deletion_vectors });
        };

        for (data_file, opt_df) in data_files.iter().zip(data_deletion_files.iter()) {
            let Some(df) = opt_df.as_ref() else {
                continue;
            };
            let dv = Self::read(file_io, df).await?;
            deletion_vectors.insert(data_file.file_name.clone(), Arc::new(dv));
        }
        Ok(DeletionVectorFactory { deletion_vectors })
    }

    /// Get the deletion vector for a specific data file.
    pub fn get_deletion_vector(&self, data_file_name: &str) -> Option<&Arc<DeletionVector>> {
        self.deletion_vectors.get(data_file_name)
    }

    /// Read a single DeletionVector from storage using DeletionFile (path/offset/length).
    /// Same as Java's DeletionVector.read(FileIO, DeletionFile).
    pub(crate) async fn read(file_io: &FileIO, df: &crate::DeletionFile) -> Result<DeletionVector> {
        let input = file_io.new_input(df.path())?;
        let reader = input.reader().await?;
        let offset = df.offset() as u64;
        let len = df.length() as u64;
        // A v1 entry occupies `len + 8` bytes on disk while a v2 entry occupies
        // exactly `len` -- v2 counts the length prefix and the CRC in
        // `DeletionFile.length()`, v1 counts neither (Java `DeletionVector.read`).
        // Neither format needs its CRC, so `len + 4` is the most either can
        // require. Reading that much overruns a v2 entry that ends the index
        // file, and a range past EOF is rejected outright rather than truncated,
        // so retry with the v2 size. v1 always satisfies the first range, so it
        // still costs a single read.
        let wanted = offset..offset.saturating_add(len).saturating_add(4);
        let bytes = match reader.read(wanted).await {
            Ok(bytes) => bytes,
            // The two ranges differ by 4 bytes, so an unrelated I/O failure
            // fails this one too and its error is what surfaces.
            Err(_) => reader.read(offset..offset.saturating_add(len)).await?,
        };
        DeletionVector::read_from_bytes(&bytes, Some(len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIO;
    use bytes::Bytes;
    use roaring::RoaringBitmap;

    fn local_file_path(path: &std::path::Path) -> String {
        let normalized = path.to_string_lossy().replace('\\', "/");
        if normalized.starts_with('/') {
            format!("file:{normalized}")
        } else {
            format!("file:/{normalized}")
        }
    }

    /// One `Bitmap64DeletionVector` run as Java writes it:
    /// `i32 BE bitmapLength | i32 LE magic | portable 64-bit roaring | i32 BE crc`.
    fn java_bitmap64_entry(values: &[u32]) -> Vec<u8> {
        let mut bitmap = RoaringBitmap::new();
        for value in values {
            bitmap.insert(*value);
        }
        let mut payload = 1u64.to_le_bytes().to_vec();
        payload.extend_from_slice(&0u32.to_le_bytes());
        bitmap.serialize_into(&mut payload).unwrap();

        let mut bytes = ((4 + payload.len()) as i32).to_be_bytes().to_vec();
        bytes.extend_from_slice(&1681511377u32.to_le_bytes());
        bytes.extend_from_slice(&payload);
        let mut crc = crc32fast::Hasher::new();
        crc.update(&bytes[4..]);
        bytes.extend_from_slice(&(crc.finalize() as i32).to_be_bytes());
        bytes
    }

    /// A v2 vector that ends the index file must still be readable. Its
    /// `DeletionFile.length()` covers the whole run, so reading `length + 8`
    /// runs past EOF and the storage layer rejects the range outright.
    #[test]
    fn test_read_bitmap64_at_end_of_index_file() {
        let entry = java_bitmap64_entry(&[3u32, 11u32]);
        let dir = tempfile::tempdir().unwrap();
        let file_io = FileIO::from_path(dir.path().to_string_lossy())
            .unwrap()
            .build()
            .unwrap();
        let path = local_file_path(&dir.path().join("index-bitmap64-last"));

        // Index files start with a one-byte version, then the vectors back to back.
        let mut file = vec![1u8];
        file.extend_from_slice(&entry);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            file_io
                .new_output(&path)
                .unwrap()
                .write(Bytes::from(file))
                .await
                .unwrap();
            let df = crate::DeletionFile::new(path.clone(), 1, entry.len() as i64, Some(2));
            let dv = DeletionVectorFactory::read(&file_io, &df)
                .await
                .expect("v2 vector at end of file must be readable");
            assert!(dv.is_deleted(3) && dv.is_deleted(11));
            assert!(!dv.is_deleted(4));
            assert_eq!(dv.cardinality(), 2);
        });
    }
}
