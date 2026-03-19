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
#![allow(dead_code)]

use roaring::RoaringBitmap;
use std::sync::Arc;

/// DeletionVector represents a set of row positions that have been deleted.
/// Uses RoaringBitmap for efficient storage, similar to Java's BitmapDeletionVector.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/deletionvectors/BitmapDeletionVector.java>
#[derive(Debug, Clone)]
pub struct DeletionVector {
    /// RoaringBitmap storing deleted row positions (0-indexed)
    /// Using u32 as RoaringBitmap32 in Java supports up to 2^31-1 rows
    bitmap: Arc<RoaringBitmap>,
}

/// Magic number for BitmapDeletionVector serialization format
/// Same as Java: 1581511376
const MAGIC_NUMBER: u32 = 1581511376;
const MAGIC_NUMBER_SIZE_BYTES: usize = 4;

impl DeletionVector {
    /// Create a new empty DeletionVector
    pub fn empty() -> Self {
        Self {
            bitmap: Arc::new(RoaringBitmap::new()),
        }
    }

    /// Create a new DeletionVector from a RoaringBitmap
    pub fn from_bitmap(bitmap: RoaringBitmap) -> Self {
        Self {
            bitmap: Arc::new(bitmap),
        }
    }

    /// Check if a row at the given position is deleted
    pub fn is_deleted(&self, row_position: u64) -> crate::Result<bool> {
        // RoaringBitmap32 in Java supports up to 2^31-1, so we check i32 range
        if row_position > i32::MAX as u64 {
            return Err(crate::Error::UnexpectedError {
                message: format!("The file has too many rows, RoaringBitmap32 only supports files with row count not exceeding {}.", i32::MAX),
                source: None,
            });
        }
        Ok(self.bitmap.contains(row_position as u32))
    }

    /// Returns an iterator over deleted positions that supports [DeletionVectorIterator::advance_to].
    /// Required for efficient row selection building when skipping row groups (avoid re-scanning
    /// deletes in skipped ranges).
    ///
    /// Ideally we would wrap `roaring::RoaringBitmap::iter()` directly, but that iterator does not
    /// expose `advance_to`. There is a PR open on roaring to add this
    /// (<https://github.com/RoaringBitmap/roaring-rs/pull/314>); once merged we can simplify
    /// by delegating `advance_to` to the underlying iterator.
    pub fn iter(&self) -> DeletionVectorIterator {
        DeletionVectorIterator::new(self.bitmap.iter().map(u64::from).collect())
    }

    /// Get the number of deleted rows (cardinality)
    pub fn deleted_count(&self) -> u64 {
        self.bitmap.len()
    }

    /// Check if the deletion vector is empty (no deleted rows)
    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }

    /// Get the underlying bitmap (read-only)
    pub fn bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    /// Read a DeletionVector from bytes, similar to Java DeletionVector.read(DataInputStream, length)
    ///
    /// Format (as read by DeletionVector.read):
    /// - bitmapLength (4 bytes int): total size including magic
    /// - magicNumber (4 bytes int): BitmapDeletionVector.MAGIC_NUMBER
    /// - bitmap data (bitmapLength - 4 bytes): serialized RoaringBitmap
    /// - CRC (4 bytes): checksum (skipped during read)
    pub fn read_from_bytes(bytes: &[u8], expected_length: Option<u64>) -> crate::Result<Self> {
        use bytes::Buf;
        if bytes.len() < 8 {
            return Err(crate::Error::DataInvalid {
                message: "Deletion vector data too short".to_string(),
                source: None,
            });
        }

        let mut buf = bytes;

        // Read bitmapLength (total size including magic)
        let bitmap_length = buf.get_i32() as usize;

        // Read magic number
        let magic_number = buf.get_i32() as u32;
        if magic_number != MAGIC_NUMBER {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Invalid magic number: expected {MAGIC_NUMBER}, got {magic_number}"
                ),
                source: None,
            });
        }

        // Verify length if provided
        if let Some(expected) = expected_length {
            if bitmap_length as u64 != expected {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Size not match, actual size: {bitmap_length}, expected size: {expected}"
                    ),
                    source: None,
                });
            }
        }

        // Read bitmap data (bitmapLength - 4 bytes, since magic is already included in bitmapLength)
        let bitmap_data_size = bitmap_length - MAGIC_NUMBER_SIZE_BYTES;
        // 4(bitmap_length) + 4(magic_number) + bitmap_data_size + 4(crc)
        if bytes.len() < 8 + bitmap_data_size + 4 {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Deletion vector data incomplete: need {} bytes, got {}",
                    8 + bitmap_data_size + 4,
                    bytes.len()
                ),
                source: None,
            });
        }

        let bitmap_data = &bytes[8..8 + bitmap_data_size];

        // Skip CRC (4 bytes) - Java code does: dis.skipBytes(4)
        // We don't need to verify it here as it's skipped

        // Deserialize RoaringBitmap
        let bitmap = RoaringBitmap::deserialize_from(bitmap_data).map_err(|e| {
            crate::Error::DataInvalid {
                message: format!("Failed to deserialize RoaringBitmap: {e}"),
                source: Some(Box::new(e)),
            }
        })?;

        Ok(Self::from_bitmap(bitmap))
    }
}

impl Default for DeletionVector {
    fn default() -> Self {
        Self::empty()
    }
}

/// Iterator over deleted row positions with [advance_to](DeletionVectorIterator::advance_to) support.
///
/// See [DeletionVector::iter] for why we use an internal sorted vec instead of wrapping
/// `roaring::RoaringBitmap::iter()` (which does not provide `advance_to`).
#[derive(Debug)]
pub struct DeletionVectorIterator {
    /// Sorted deleted positions (from bitmap.iter()).
    positions: Vec<u64>,
    cursor: usize,
}

impl DeletionVectorIterator {
    pub(crate) fn new(positions: Vec<u64>) -> Self {
        Self {
            positions,
            cursor: 0,
        }
    }
}

impl Iterator for DeletionVectorIterator {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor < self.positions.len() {
            let v = self.positions[self.cursor];
            self.cursor += 1;
            Some(v)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;
    use std::env::current_dir;
    #[test]
    fn test_read_deletion_vector() {
        let workdir = current_dir().unwrap();
        let path =
            workdir.join("tests/fixtures/index/index-7e53780d-2faa-4e4c-9f2e-93af5082bbdb-0");

        // the first byte is for version, we skip to read the first byte
        let bytes = &std::fs::read(&path).expect("fixture index file must exist")[1..];
        assert!(!bytes.is_empty(), "fixture file must not be empty");

        // the expected bitmap length is 24
        let dv = DeletionVector::read_from_bytes(bytes, Some(24))
            .expect("failed to read DeletionVector");

        let expected_bitmap = RoaringBitmap::from_iter([1u32, 2u32]);
        assert_eq!(dv.bitmap(), &expected_bitmap, "bitmap should be [1, 2]");
        assert_eq!(dv.deleted_count(), 2);
        assert!(dv.is_deleted(1).unwrap());
        assert!(dv.is_deleted(2).unwrap());
        assert!(!dv.is_deleted(0).unwrap());
    }
}
