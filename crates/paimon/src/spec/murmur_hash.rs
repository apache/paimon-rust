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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_empty() {
        let h = hash_by_words(&[]);
        let expected = fmix(DEFAULT_SEED) as i32;
        assert_eq!(h, expected);
    }

    #[test]
    fn test_hash_single_int() {
        let data = 1_i32.to_le_bytes();
        let h = hash_by_words(&data);
        let k1 = mix_k1(1);
        let h1 = mix_h1(DEFAULT_SEED, k1);
        let expected = fmix(h1 ^ 4) as i32;
        assert_eq!(h, expected);
    }

    #[test]
    #[should_panic(expected = "word-aligned")]
    fn test_hash_non_aligned_panics() {
        hash_by_words(&[1, 2, 3]);
    }
}
