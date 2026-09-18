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
/// Magic number for Java `Bitmap64DeletionVector` (v2). Written little-endian,
/// unlike the v1 magic, so the two are told apart by byte order as well as value.
const MAGIC_NUMBER_64: u32 = 1681511377;
const MAGIC_NUMBER_SIZE_BYTES: u64 = 4;
/// Size of the `bitmapLength` prefix and of the trailing CRC. Mirrors Java
/// `Bitmap64DeletionVector.LENGTH_SIZE_BYTES` / `CRC_SIZE_BYTES`; a v2 entry
/// counts both in `DeletionFile.length()` while a v1 entry counts neither.
const LENGTH_SIZE_BYTES: u64 = 4;
const CRC_SIZE_BYTES: u64 = 4;

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

    /// Clone the underlying bitmap for mutation by writers.
    pub(crate) fn to_bitmap(&self) -> RoaringBitmap {
        (*self.bitmap).clone()
    }

    /// Number of deleted positions in this vector.
    pub fn cardinality(&self) -> u64 {
        self.bitmap.len()
    }

    /// Returns true if `position` is deleted. Positions above `u32::MAX` cannot be
    /// present in a roaring32 bitmap and are therefore reported as not deleted.
    /// Mirrors Java `BitmapDeletionVector#isDeleted` / the searchers' `LongPredicate`.
    pub fn is_deleted(&self, position: u64) -> bool {
        u32::try_from(position)
            .ok()
            .is_some_and(|p| self.bitmap.contains(p))
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

    /// Check if the deletion vector is empty (no deleted rows)
    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }

    /// Serialize using Java `BitmapDeletionVector` format:
    /// `i32 bitmapLength | i32 magic | roaring bitmap bytes | i32 crc`.
    pub(crate) fn serialize_to_bytes(&self) -> crate::Result<Vec<u8>> {
        let mut bitmap_bytes = Vec::new();
        self.bitmap
            .serialize_into(&mut bitmap_bytes)
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to serialize RoaringBitmap: {e}"),
                source: Some(Box::new(e)),
            })?;

        let bitmap_length = i32::try_from(MAGIC_NUMBER_SIZE_BYTES as usize + bitmap_bytes.len())
            .map_err(|_| crate::Error::DataInvalid {
                message: "Deletion vector bitmap is too large to serialize".to_string(),
                source: None,
            })?;

        let mut payload = Vec::with_capacity(8 + bitmap_bytes.len() + 4);
        payload.extend_from_slice(&bitmap_length.to_be_bytes());
        payload.extend_from_slice(&(MAGIC_NUMBER as i32).to_be_bytes());
        payload.extend_from_slice(&bitmap_bytes);

        let mut crc = crc32fast::Hasher::new();
        crc.update(&payload[4..]);
        payload.extend_from_slice(&(crc.finalize() as i32).to_be_bytes());
        Ok(payload)
    }

    /// Get the underlying bitmap (read-only)
    #[cfg(test)]
    fn bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    /// Read a DeletionVector from bytes, mirroring Java `DeletionVector.read(DataInputStream, length)`,
    /// which accepts two on-disk formats and dispatches on the magic number.
    ///
    /// `BitmapDeletionVector` (v1):
    /// - `bitmapLength` (i32 big-endian): magic + bitmap data
    /// - magic [`MAGIC_NUMBER`] (i32 big-endian)
    /// - bitmap data (`bitmapLength - 4` bytes): serialized roaring32 bitmap
    /// - CRC (i32): not verified here, matching Java's `dis.skipBytes(4)`
    ///
    /// `Bitmap64DeletionVector` (v2), whose payload is little-endian:
    /// - `bitmapLength` (i32 **big**-endian): magic + bitmap data
    /// - magic [`MAGIC_NUMBER_64`] (i32 **little**-endian)
    /// - bitmap data: portable 64-bit roaring (`u64` bucket count, then `u32` key
    ///   plus a roaring32 bitmap per bucket), possibly run-length encoded
    /// - CRC (i32): not verified here either
    ///
    /// `expected_length` is `DeletionFile.length()`. The two formats count it
    /// differently: v1 excludes the length prefix and the CRC, v2 includes both.
    pub fn read_from_bytes(bytes: &[u8], expected_length: Option<u64>) -> crate::Result<Self> {
        use bytes::Buf;
        if bytes.len() < 8 {
            return Err(crate::Error::DataInvalid {
                message: "Deletion vector data too short".to_string(),
                source: None,
            });
        }

        let mut buf = bytes;

        // Read bitmapLength (magic + bitmap data). Both formats store it
        // big-endian. Reject a negative value here so the size arithmetic below
        // cannot wrap: `as usize` would turn -1 into u64::MAX and make the
        // "data incomplete" guard compute a tiny requirement and pass.
        let bitmap_length =
            u64::try_from(buf.get_i32()).map_err(|_| crate::Error::DataInvalid {
                message: "Deletion vector bitmap length is negative".to_string(),
                source: None,
            })?;

        // Read magic number. v1 is big-endian, v2 little-endian.
        let magic_number = buf.get_i32() as u32;
        let is_bitmap64 = if magic_number == MAGIC_NUMBER {
            false
        } else if magic_number.swap_bytes() == MAGIC_NUMBER_64 {
            true
        } else {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Invalid magic number: {magic_number}, \
                     v1 dv magic number: {MAGIC_NUMBER}, v2 magic number: {MAGIC_NUMBER_64}"
                ),
                source: None,
            });
        };

        // Verify length if provided, using each format's own convention.
        if let Some(expected) = expected_length {
            let expected_bitmap_length = if is_bitmap64 {
                expected
                    .checked_sub(LENGTH_SIZE_BYTES + CRC_SIZE_BYTES)
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: format!("Deletion vector length {expected} is too small"),
                        source: None,
                    })?
            } else {
                expected
            };
            if bitmap_length != expected_bitmap_length {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Size not match, actual size: {bitmap_length}, expected size: {expected_bitmap_length}"
                    ),
                    source: None,
                });
            }
        }

        // Bitmap data follows the magic, which bitmapLength counts.
        let bitmap_data_size = bitmap_length
            .checked_sub(MAGIC_NUMBER_SIZE_BYTES)
            .and_then(|size| usize::try_from(size).ok())
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("Deletion vector bitmap length {bitmap_length} is too small"),
                source: None,
            })?;
        // The CRC is not verified, so it need not be present: a v2 entry that
        // ends the index file cannot be over-read (see `DeletionVectorFactory::read`).
        let needed = 8 + bitmap_data_size;
        if bytes.len() < needed {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Deletion vector data incomplete: need {needed} bytes, got {}",
                    bytes.len()
                ),
                source: None,
            });
        }

        let bitmap_data = &bytes[8..needed];
        if is_bitmap64 {
            Self::from_bitmap64_bytes(bitmap_data)
        } else {
            let bitmap = RoaringBitmap::deserialize_from(bitmap_data).map_err(|e| {
                crate::Error::DataInvalid {
                    message: format!("Failed to deserialize RoaringBitmap: {e}"),
                    source: Some(Box::new(e)),
                }
            })?;
            Ok(Self::from_bitmap(bitmap))
        }
    }

    /// Decode a `Bitmap64DeletionVector` payload into the roaring32 representation
    /// this type stores.
    ///
    /// Row positions are offsets inside a single data file, and every existing API
    /// here is already roaring32-bound -- [`Self::is_deleted`] documents that
    /// positions above `u32::MAX` cannot be present, and `to_bitmap` hands a
    /// `RoaringBitmap` to the writer. A position that does not fit is therefore
    /// rejected rather than truncated, so a 64-bit vector can never silently lose
    /// deletes; a data file with more than `u32::MAX` rows would be needed to
    /// reach it.
    fn from_bitmap64_bytes(bitmap_data: &[u8]) -> crate::Result<Self> {
        // The outer layer is decoded here rather than through
        // `RoaringTreemap::deserialize_from`, which validates each inner roaring32
        // bitmap but inserts the bucket keys straight into a `BTreeMap` -- a
        // duplicate key silently replaces the earlier bucket and its deletes are
        // lost. Java's `OptimizedRoaringBitmap64.deserialize` rejects that, so the
        // count and every key are checked the same way here
        // (`readBitmapCount` / `readKey`).
        let mut cursor = bitmap_data;
        let bucket_count = read_u64_le(&mut cursor)?;
        if bucket_count > i32::MAX as u64 {
            return Err(crate::Error::DataInvalid {
                message: format!("Invalid deletion vector bitmap count: {bucket_count}"),
                source: None,
            });
        }

        let mut bitmap = RoaringBitmap::new();
        let mut last_key: Option<u32> = None;
        for _ in 0..bucket_count {
            let key = read_u32_le(&mut cursor)?;
            if key > (i32::MAX as u32 - 1) {
                return Err(crate::Error::DataInvalid {
                    message: format!("Deletion vector bitmap key is too large: {key}"),
                    source: None,
                });
            }
            if last_key.is_some_and(|previous| key <= previous) {
                return Err(crate::Error::DataInvalid {
                    message: "Deletion vector bitmap keys must be sorted in ascending order"
                        .to_string(),
                    source: None,
                });
            }
            last_key = Some(key);

            let bucket = RoaringBitmap::deserialize_from(&mut cursor).map_err(|e| {
                crate::Error::DataInvalid {
                    message: format!("Failed to deserialize 64-bit RoaringBitmap bucket: {e}"),
                    source: Some(Box::new(e)),
                }
            })?;
            if key > 0 && !bucket.is_empty() {
                // Positions in bucket `key` are `key << 32 | low`, so anything
                // above bucket 0 exceeds `u32::MAX`. Rejected rather than
                // truncated: every API here is roaring32-bound, and reaching one
                // needs a data file with more than `u32::MAX` rows.
                let position = (u64::from(key) << 32) | u64::from(bucket.min().unwrap_or(0));
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Deletion vector position {position} exceeds u32::MAX, \
                         which this reader cannot represent"
                    ),
                    source: None,
                });
            }
            bitmap |= bucket;
        }
        Ok(Self::from_bitmap(bitmap))
    }
}

/// Read a little-endian `u64`, advancing `cursor`.
fn read_u64_le(cursor: &mut &[u8]) -> crate::Result<u64> {
    let bytes: [u8; 8] = cursor
        .get(..8)
        .and_then(|slice| slice.try_into().ok())
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "Deletion vector bitmap data truncated".to_string(),
            source: None,
        })?;
    *cursor = &cursor[8..];
    Ok(u64::from_le_bytes(bytes))
}

/// Read a little-endian `u32`, advancing `cursor`.
fn read_u32_le(cursor: &mut &[u8]) -> crate::Result<u32> {
    let bytes: [u8; 4] = cursor
        .get(..4)
        .and_then(|slice| slice.try_into().ok())
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "Deletion vector bitmap data truncated".to_string(),
            source: None,
        })?;
    *cursor = &cursor[4..];
    Ok(u32::from_le_bytes(bytes))
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
    }

    /// Build the on-disk bytes Java `Bitmap64DeletionVector.serializeTo` writes:
    /// `i32 BE bitmapLength | i32 LE magic | portable 64-bit roaring | i32 BE crc`,
    /// where the payload is `u64 LE bucket count` then `u32 LE key` plus a
    /// roaring32 bitmap per bucket, densely for every key from 0 upwards.
    fn java_bitmap64_bytes(buckets: &[&[u32]], run_length_encode: bool) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(buckets.len() as u64).to_le_bytes());
        for (key, values) in buckets.iter().enumerate() {
            payload.extend_from_slice(&(key as u32).to_le_bytes());
            let mut bitmap = RoaringBitmap::new();
            for value in *values {
                bitmap.insert(*value);
            }
            if run_length_encode {
                // Java calls `roaringBitmap.runLengthEncode()` before serializing.
                bitmap.optimize();
            }
            bitmap.serialize_into(&mut payload).unwrap();
        }

        let bitmap_length = 4 + payload.len();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(bitmap_length as i32).to_be_bytes());
        bytes.extend_from_slice(&MAGIC_NUMBER_64.to_le_bytes());
        bytes.extend_from_slice(&payload);
        let mut crc = crc32fast::Hasher::new();
        crc.update(&bytes[4..]);
        bytes.extend_from_slice(&(crc.finalize() as i32).to_be_bytes());
        bytes
    }

    /// `DeletionFile.length()` for a v2 entry is the whole on-disk run.
    fn bitmap64_declared_length(bytes: &[u8]) -> u64 {
        bytes.len() as u64
    }

    /// Wrap a bitmap64 payload in the on-disk envelope Java writes.
    fn wrap_bitmap64_payload(payload: &[u8]) -> Vec<u8> {
        let bitmap_length = 4 + payload.len();
        let mut bytes = (bitmap_length as i32).to_be_bytes().to_vec();
        bytes.extend_from_slice(&MAGIC_NUMBER_64.to_le_bytes());
        bytes.extend_from_slice(payload);
        let mut crc = crc32fast::Hasher::new();
        crc.update(&bytes[4..]);
        bytes.extend_from_slice(&(crc.finalize() as i32).to_be_bytes());
        bytes
    }

    #[test]
    fn test_read_bitmap64_deletion_vector() {
        let bytes = java_bitmap64_bytes(&[&[1u32, 2u32, 9u32]], false);
        let dv = DeletionVector::read_from_bytes(&bytes, Some(bitmap64_declared_length(&bytes)))
            .expect("v2 deletion vector must be readable");
        assert_eq!(dv.bitmap(), &RoaringBitmap::from_iter([1u32, 2, 9]));
        assert!(dv.is_deleted(2) && !dv.is_deleted(3));
        assert_eq!(dv.cardinality(), 3);
    }

    #[test]
    fn test_read_bitmap64_run_length_encoded() {
        // Java run-length encodes before writing, which switches the container
        // type and the serialization cookie.
        let run: Vec<u32> = (100u32..400).collect();
        let bytes = java_bitmap64_bytes(&[&run], true);
        let dv = DeletionVector::read_from_bytes(&bytes, Some(bitmap64_declared_length(&bytes)))
            .expect("run-length encoded v2 deletion vector must be readable");
        assert_eq!(dv.cardinality(), run.len() as u64);
        assert!(dv.is_deleted(100) && dv.is_deleted(399) && !dv.is_deleted(400));
    }

    /// `RoaringTreemap::deserialize_from` would accept a duplicate bucket key and
    /// let the later bucket replace the earlier one, dropping its deletes; Java
    /// rejects it in `readKey`. Descending keys and a bogus count are the same
    /// class of corruption.
    #[test]
    fn test_read_bitmap64_rejects_bad_bucket_keys() {
        // Two buckets both keyed 0: {1} then {2}. Without the check this decodes
        // to {2} alone and position 1 reads as a live row.
        let mut payload = 2u64.to_le_bytes().to_vec();
        for values in [[1u32], [2u32]] {
            payload.extend_from_slice(&0u32.to_le_bytes());
            let mut bucket = RoaringBitmap::new();
            bucket.insert(values[0]);
            bucket.serialize_into(&mut payload).unwrap();
        }
        let bytes = wrap_bitmap64_payload(&payload);
        let err = DeletionVector::read_from_bytes(&bytes, Some(bytes.len() as u64))
            .expect_err("a duplicate bucket key must be rejected");
        assert!(err.to_string().contains("ascending order"), "got: {err}");

        // Descending keys. Bucket 1 is left empty so the order check is what
        // rejects this, not the `u32::MAX` guard.
        let mut payload = 2u64.to_le_bytes().to_vec();
        for (key, values) in [(1u32, &[][..]), (0u32, &[6u32][..])] {
            payload.extend_from_slice(&key.to_le_bytes());
            let mut bucket = RoaringBitmap::new();
            for value in values {
                bucket.insert(*value);
            }
            bucket.serialize_into(&mut payload).unwrap();
        }
        let bytes = wrap_bitmap64_payload(&payload);
        let err = DeletionVector::read_from_bytes(&bytes, Some(bytes.len() as u64))
            .expect_err("descending bucket keys must be rejected");
        assert!(err.to_string().contains("ascending order"), "got: {err}");

        // A count larger than i32::MAX cannot be a real bucket count.
        let payload = (i32::MAX as u64 + 1).to_le_bytes().to_vec();
        let bytes = wrap_bitmap64_payload(&payload);
        let err = DeletionVector::read_from_bytes(&bytes, Some(bytes.len() as u64))
            .expect_err("a bogus bucket count must be rejected");
        assert!(
            err.to_string()
                .contains("Invalid deletion vector bitmap count"),
            "got: {err}"
        );

        // A truncated payload must error, not panic.
        let bytes = wrap_bitmap64_payload(&1u64.to_le_bytes()[..4]);
        let err = DeletionVector::read_from_bytes(&bytes, Some(bytes.len() as u64))
            .expect_err("a truncated payload must be rejected");
        assert!(err.to_string().contains("truncated"), "got: {err}");
    }

    #[test]
    fn test_read_bitmap64_with_empty_leading_bucket() {
        // Java writes every key from 0 densely, so bucket 0 is present and empty
        // when no position below 2^32 is deleted. Rust's own writer omits it.
        let bytes = java_bitmap64_bytes(&[&[], &[7u32]], false);
        let err = DeletionVector::read_from_bytes(&bytes, Some(bitmap64_declared_length(&bytes)))
            .expect_err("a position above u32::MAX must be rejected, not truncated");
        assert!(
            err.to_string().contains("exceeds u32::MAX"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_read_bitmap64_uses_its_own_length_convention() {
        let bytes = java_bitmap64_bytes(&[&[5u32]], false);
        let declared = bitmap64_declared_length(&bytes);
        // v1's convention (length == bitmapLength) must not be applied to v2.
        let err = DeletionVector::read_from_bytes(&bytes, Some(declared - 8))
            .expect_err("v2 length is the whole on-disk run, prefix and CRC included");
        assert!(err.to_string().contains("Size not match"), "got: {err}");
        assert!(DeletionVector::read_from_bytes(&bytes, Some(declared)).is_ok());
    }

    #[test]
    fn test_read_rejects_unknown_magic_naming_both_formats() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&8i32.to_be_bytes());
        bytes.extend_from_slice(&0xDEAD_BEEFu32.to_be_bytes());
        bytes.extend_from_slice(&[0u8; 8]);
        let err = DeletionVector::read_from_bytes(&bytes, None).expect_err("magic must be checked");
        let message = err.to_string();
        assert!(
            message.contains(&MAGIC_NUMBER.to_string()),
            "got: {message}"
        );
        assert!(
            message.contains(&MAGIC_NUMBER_64.to_string()),
            "got: {message}"
        );
    }

    #[test]
    fn test_read_rejects_negative_bitmap_length() {
        // Taken straight from the file, a negative length used to become a huge
        // usize and wrap the "data incomplete" guard into passing.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(-1i32).to_be_bytes());
        bytes.extend_from_slice(&MAGIC_NUMBER.to_be_bytes());
        bytes.extend_from_slice(&[0u8; 8]);
        let err = DeletionVector::read_from_bytes(&bytes, None).expect_err("must not panic");
        assert!(err.to_string().contains("negative"), "got: {err}");
    }

    #[test]
    fn test_is_deleted_reports_membership_and_guards_u32_overflow() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(2);
        let dv = DeletionVector::from_bitmap(bitmap);
        assert!(dv.is_deleted(2), "position 2 was deleted");
        assert!(!dv.is_deleted(0), "position 0 was not deleted");
        // Positions above u32::MAX cannot exist in a roaring32 bitmap -> not deleted.
        assert!(!dv.is_deleted(u64::from(u32::MAX) + 1));
    }
}
