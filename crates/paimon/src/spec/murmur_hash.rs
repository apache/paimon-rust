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

//! Murmur3 32-bit hash compatible with Java Paimon's `MurmurHashUtils`.
//!
//! Reference: <https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/utils/MurmurHashUtils.java>

const C1: u32 = 0xcc9e2d51;
const C2: u32 = 0x1b873593;
const DEFAULT_SEED: u32 = 42;

fn mix_k1(mut k1: u32) -> u32 {
    k1 = k1.wrapping_mul(C1);
    k1 = k1.rotate_left(15);
    k1 = k1.wrapping_mul(C2);
    k1
}

fn mix_h1(mut h1: u32, k1: u32) -> u32 {
    h1 ^= k1;
    h1 = h1.rotate_left(13);
    h1 = h1.wrapping_mul(5).wrapping_add(0xe6546b64);
    h1
}

fn fmix(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85ebca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2ae35);
    h ^= h >> 16;
    h
}

/// Murmur3 32-bit hash over word-aligned data (length must be a multiple of 4).
///
/// This matches Java Paimon's `MurmurHashUtils.hashBytesByWords` with `DEFAULT_SEED = 42`.
/// Java's `BinaryRow.hashCode()` calls `hashByWords(segments, offset, sizeInBytes)`.
///
/// Note: Java reads ints in native (little-endian on x86) byte order via `Unsafe.getInt`.
/// We use `i32::from_le_bytes` to match.
pub fn hash_by_words(data: &[u8]) -> i32 {
    assert!(
        data.len().is_multiple_of(4),
        "hash_by_words: data length must be word-aligned (multiple of 4), got {}",
        data.len()
    );
    let mut h1 = DEFAULT_SEED;
    for chunk in data.chunks_exact(4) {
        let word = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        let k1 = mix_k1(word);
        h1 = mix_h1(h1, k1);
    }
    fmix(h1 ^ data.len() as u32) as i32
}

/// Compute the bucket for a BinaryRow's raw data bytes.
///
/// Matches Java's `DefaultBucketFunction.bucket(row, numBuckets)`:
/// `Math.abs(row.hashCode() % numBuckets)`
pub fn compute_bucket(row_data: &[u8], total_buckets: i32) -> i32 {
    let hash = hash_by_words(row_data);
    (hash % total_buckets).abs()
}

/// Build a BinaryRow from typed Datum values and compute its bucket.
///
/// This matches Java's `DefaultBucketFunction`: project the bucket key fields
/// into a new `BinaryRow`, then `Math.abs(row.hashCode() % numBuckets)`.
///
/// Only supports fixed-length types that are commonly used as bucket keys.
/// Returns `None` for unsupported types (fail-open: no bucket pruning).
pub fn compute_bucket_from_datums(
    datums: &[(&crate::spec::Datum, &crate::spec::DataType)],
    total_buckets: i32,
) -> Option<i32> {
    let arity = datums.len() as i32;
    let null_bits_size = crate::spec::BinaryRow::cal_bit_set_width_in_bytes(arity) as usize;
    let fixed_part_size = null_bits_size + (datums.len()) * 8;
    let mut data = vec![0u8; fixed_part_size];

    for (pos, (datum, _data_type)) in datums.iter().enumerate() {
        let field_offset = null_bits_size + pos * 8;
        match datum {
            crate::spec::Datum::Bool(v) => {
                data[field_offset] = if *v { 1 } else { 0 };
            }
            crate::spec::Datum::TinyInt(v) => {
                data[field_offset] = *v as u8;
            }
            crate::spec::Datum::SmallInt(v) => {
                data[field_offset..field_offset + 2].copy_from_slice(&v.to_le_bytes());
            }
            crate::spec::Datum::Int(v)
            | crate::spec::Datum::Date(v)
            | crate::spec::Datum::Time(v) => {
                data[field_offset..field_offset + 4].copy_from_slice(&v.to_le_bytes());
            }
            crate::spec::Datum::Long(v) => {
                data[field_offset..field_offset + 8].copy_from_slice(&v.to_le_bytes());
            }
            crate::spec::Datum::Float(v) => {
                data[field_offset..field_offset + 4].copy_from_slice(&v.to_le_bytes());
            }
            crate::spec::Datum::Double(v) => {
                data[field_offset..field_offset + 8].copy_from_slice(&v.to_le_bytes());
            }
            crate::spec::Datum::Timestamp { millis, .. }
            | crate::spec::Datum::LocalZonedTimestamp { millis, .. } => {
                // Compact timestamp (precision <= 3): stored as epoch millis.
                // For bucket key hashing, we use the millis value directly in the fixed part.
                data[field_offset..field_offset + 8].copy_from_slice(&millis.to_le_bytes());
            }
            crate::spec::Datum::Decimal {
                unscaled,
                precision,
                ..
            } => {
                if *precision <= 18 {
                    let v = *unscaled as i64;
                    data[field_offset..field_offset + 8].copy_from_slice(&v.to_le_bytes());
                } else {
                    // Non-compact decimal requires variable-length encoding — not supported for bucket key hashing.
                    return None;
                }
            }
            crate::spec::Datum::String(s) => {
                let bytes = s.as_bytes();
                if bytes.len() <= 7 {
                    // Inline encoding: data in lower bytes, mark + length in highest byte.
                    data[field_offset..field_offset + bytes.len()].copy_from_slice(bytes);
                    data[field_offset + 7] = 0x80 | (bytes.len() as u8);
                } else {
                    // Variable-length: append to data, store offset+length in fixed part.
                    let var_offset = data.len();
                    data.extend_from_slice(bytes);
                    let encoded = ((var_offset as u64) << 32) | (bytes.len() as u64);
                    data[field_offset..field_offset + 8].copy_from_slice(&encoded.to_le_bytes());
                }
            }
            crate::spec::Datum::Bytes(b) => {
                if b.len() <= 7 {
                    data[field_offset..field_offset + b.len()].copy_from_slice(b);
                    data[field_offset + 7] = 0x80 | (b.len() as u8);
                } else {
                    let var_offset = data.len();
                    data.extend_from_slice(b);
                    let encoded = ((var_offset as u64) << 32) | (b.len() as u64);
                    data[field_offset..field_offset + 8].copy_from_slice(&encoded.to_le_bytes());
                }
            }
        }
    }

    // Pad data to word-aligned (multiple of 4) for hash_by_words.
    while !data.len().is_multiple_of(4) {
        data.push(0);
    }

    Some(compute_bucket(&data, total_buckets))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_empty() {
        // Empty data (0 bytes, word-aligned)
        let h = hash_by_words(&[]);
        // fmix(42 ^ 0) = fmix(42)
        let expected = fmix(DEFAULT_SEED) as i32;
        assert_eq!(h, expected);
    }

    #[test]
    fn test_hash_single_int() {
        // Hash a single little-endian int32 = 1
        let data = 1_i32.to_le_bytes();
        let h = hash_by_words(&data);
        // Manually compute: mix_k1(1), mix_h1(42, k1), fmix(h1 ^ 4)
        let k1 = mix_k1(1);
        let h1 = mix_h1(DEFAULT_SEED, k1);
        let expected = fmix(h1 ^ 4) as i32;
        assert_eq!(h, expected);
    }

    #[test]
    fn test_compute_bucket_deterministic() {
        let data = 42_i32.to_le_bytes();
        let pad = [0u8; 4]; // null_bits padding
        let mut row_data = Vec::new();
        row_data.extend_from_slice(&pad);
        row_data.extend_from_slice(&pad);
        row_data.extend_from_slice(&data);
        row_data.extend_from_slice(&[0u8; 4]); // pad to 8 bytes per field

        let bucket = compute_bucket(&row_data, 4);
        assert!((0..4).contains(&bucket));
    }

    #[test]
    #[should_panic(expected = "word-aligned")]
    fn test_hash_non_aligned_panics() {
        hash_by_words(&[1, 2, 3]);
    }
}
