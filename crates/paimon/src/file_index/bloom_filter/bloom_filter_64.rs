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

use bytes::Bytes;

use crate::{Error, Result};

const BITS_PER_BYTE: usize = u8::BITS as usize;

pub(super) struct BloomFilter64 {
    bit_set: BitSet,
    num_hash_functions: i32,
}

impl BloomFilter64 {
    pub(super) fn try_new(items: i32, fpp: f64) -> Result<Self> {
        if items <= 0 {
            return Err(Error::ConfigInvalid {
                message: format!("Bloom filter items must be positive, but was {items}"),
            });
        }
        if !fpp.is_finite() || fpp <= 0.0 || fpp >= 1.0 {
            return Err(Error::ConfigInvalid {
                message: format!("Bloom filter fpp must be finite and in (0, 1), but was {fpp}"),
            });
        }

        let log_two = 2.0_f64.ln();
        let estimated_bits = -(f64::from(items) * fpp.ln()) / (log_two * log_two);
        if !estimated_bits.is_finite() || estimated_bits > f64::from(i32::MAX) {
            return Err(Error::ConfigInvalid {
                message: format!(
                    "Bloom filter size is not representable for items {items} and fpp {fpp}"
                ),
            });
        }

        // Java truncates the estimate and always advances to the next byte boundary.
        let estimated_bits = estimated_bits as i32;
        let num_bits = estimated_bits
            .checked_add(8 - estimated_bits.rem_euclid(8))
            .ok_or_else(|| Error::ConfigInvalid {
                message: format!("Bloom filter size overflows for items {items} and fpp {fpp}"),
            })?;
        let num_hash_functions = ((f64::from(num_bits) / f64::from(items)) * log_two)
            .round()
            .max(1.0) as i32;

        Ok(Self {
            bit_set: BitSet::try_zeroed(num_bits as usize)?,
            num_hash_functions,
        })
    }

    pub(super) fn from_serialized(num_hash_functions: i32, bytes: Bytes) -> Result<Self> {
        if bytes.is_empty() {
            return Err(Error::FileIndexFormatInvalid {
                message: "Bloom filter bitset must not be empty".to_string(),
            });
        }
        let bit_size = bytes.len().checked_mul(BITS_PER_BYTE).ok_or_else(|| {
            Error::FileIndexFormatInvalid {
                message: "Bloom filter bit count overflows usize".to_string(),
            }
        })?;
        if num_hash_functions <= 0 {
            return Err(Error::FileIndexFormatInvalid {
                message: format!(
                    "Bloom filter hash function count must be positive, but was {num_hash_functions}"
                ),
            });
        }
        if num_hash_functions as usize > bit_size {
            return Err(Error::FileIndexFormatInvalid {
                message: format!(
                    "Bloom filter hash function count {num_hash_functions} exceeds bit count {}",
                    bit_size
                ),
            });
        }
        let bit_set = BitSet::Shared(bytes);

        Ok(Self {
            bit_set,
            num_hash_functions,
        })
    }

    pub(super) fn add_hash(&mut self, hash64: u64) {
        for iteration in 1..=self.num_hash_functions {
            let position = self.position(hash64, iteration);
            self.bit_set.set(position);
        }
    }

    pub(super) fn test_hash(&self, hash64: u64) -> bool {
        (1..=self.num_hash_functions).all(|iteration| {
            let position = self.position(hash64, iteration);
            self.bit_set.get(position)
        })
    }

    pub(super) fn num_hash_functions(&self) -> i32 {
        self.num_hash_functions
    }

    pub(super) fn bytes(&self) -> &[u8] {
        self.bit_set.bytes()
    }

    fn position(&self, hash64: u64, iteration: i32) -> usize {
        let hash1 = hash64 as i32;
        let hash2 = (hash64 >> 32) as i32;
        let mut combined_hash = hash1.wrapping_add(iteration.wrapping_mul(hash2));
        if combined_hash < 0 {
            combined_hash = !combined_hash;
        }
        combined_hash as usize % self.bit_set.bit_size()
    }
}

enum BitSet {
    Mutable(Vec<u8>),
    Shared(Bytes),
}

impl BitSet {
    fn try_zeroed(num_bits: usize) -> Result<Self> {
        debug_assert!(num_bits > 0 && num_bits.is_multiple_of(BITS_PER_BYTE));
        let len = num_bits / BITS_PER_BYTE;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(len)
            .map_err(|error| Error::ConfigInvalid {
                message: format!("Failed to allocate {len} bytes for Bloom filter: {error}"),
            })?;
        bytes.resize(len, 0);
        Ok(Self::Mutable(bytes))
    }

    fn set(&mut self, index: usize) {
        match self {
            Self::Mutable(bytes) => bytes[index >> 3] |= 1 << (index & 0x07),
            Self::Shared(_) => unreachable!("serialized Bloom filter bitset is immutable"),
        }
    }

    fn get(&self, index: usize) -> bool {
        self.bytes()[index >> 3] & (1 << (index & 0x07)) != 0
    }

    fn bit_size(&self) -> usize {
        self.bytes().len() * BITS_PER_BYTE
    }

    fn bytes(&self) -> &[u8] {
        match self {
            Self::Mutable(bytes) => bytes,
            Self::Shared(bytes) => bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_java_byte_alignment() {
        let filter = BloomFilter64::try_new(82, 0.1).unwrap();

        assert_eq!(filter.bytes().len(), 50);
        assert_eq!(filter.num_hash_functions(), 3);
    }

    #[test]
    fn test_add_and_test_hash() {
        let mut filter = BloomFilter64::try_new(10, 0.1).unwrap();
        let hash = 0x1234_5678_90ab_cdef;

        assert!(!filter.test_hash(hash));
        filter.add_hash(hash);
        assert!(filter.test_hash(hash));
    }

    #[test]
    fn test_reject_invalid_config() {
        for items in [i32::MIN, -1, 0] {
            assert!(matches!(
                BloomFilter64::try_new(items, 0.1),
                Err(Error::ConfigInvalid { .. })
            ));
        }
        for fpp in [
            f64::NEG_INFINITY,
            -0.1,
            0.0,
            1.0,
            2.0,
            f64::INFINITY,
            f64::NAN,
        ] {
            assert!(matches!(
                BloomFilter64::try_new(10, fpp),
                Err(Error::ConfigInvalid { .. })
            ));
        }
        assert!(matches!(
            BloomFilter64::try_new(i32::MAX, f64::MIN_POSITIVE),
            Err(Error::ConfigInvalid { .. })
        ));
    }

    #[test]
    fn test_reject_malformed_serialized_parts() {
        for (hash_functions, bytes) in [(0, &b"\0"[..]), (-1, &b"\0"[..]), (9, &b"\0"[..])] {
            assert!(matches!(
                BloomFilter64::from_serialized(hash_functions, Bytes::copy_from_slice(bytes)),
                Err(Error::FileIndexFormatInvalid { .. })
            ));
        }
        assert!(matches!(
            BloomFilter64::from_serialized(1, Bytes::new()),
            Err(Error::FileIndexFormatInvalid { .. })
        ));
    }
}
