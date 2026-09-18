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

//! Bloom filter used by Java-compatible BTree SST files.

use bytes::Bytes;
use std::io;

const BITS_PER_BYTE: usize = u8::BITS as usize;

/// Bloom filter with its hash count derived from the expected entry count and serialized size.
pub(super) struct BloomFilter {
    expected_entries: u64,
    bits: BitSet,
    num_hash_functions: i32,
}

enum BitSet {
    Mutable(Vec<u8>),
    Shared(Bytes),
}

impl BloomFilter {
    pub(super) fn from_hashes(hashes: &[i32], fpp: f64) -> io::Result<Option<Self>> {
        if hashes.is_empty() {
            return Ok(None);
        }
        if !fpp.is_finite() || fpp <= 0.0 || fpp >= 1.0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Bloom filter fpp must be finite and in (0, 1), but was {fpp}"),
            ));
        }

        let expected_entries = hashes.len() as u64;
        let log_two = 2.0_f64.ln();
        // Java first narrows the optimal bit count to int, then rounds the byte count up.
        let optimal_bits = (-(expected_entries as f64) * fpp.ln() / (log_two * log_two)) as i32;
        let byte_size = (f64::from(optimal_bits) / BITS_PER_BYTE as f64).ceil() as usize;
        if byte_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Bloom filter size must be positive for {expected_entries} entries and fpp {fpp}"
                ),
            ));
        }

        let mut bits = Vec::new();
        bits.try_reserve_exact(byte_size).map_err(|error| {
            io::Error::other(format!("Failed to allocate Bloom filter: {error}"))
        })?;
        bits.resize(byte_size, 0);
        let num_hash_functions = num_hash_functions(expected_entries, bits.len())?;
        let mut filter = Self {
            expected_entries,
            bits: BitSet::Mutable(bits),
            num_hash_functions,
        };
        for &hash in hashes {
            filter.add_hash(hash);
        }
        Ok(Some(filter))
    }

    pub(super) fn from_bytes(expected_entries: u64, bits: Bytes) -> io::Result<Self> {
        if expected_entries == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Bloom filter expected entry count must be positive",
            ));
        }
        if bits.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Bloom filter bitset must not be empty",
            ));
        }

        let num_hash_functions = num_hash_functions(expected_entries, bits.len())?;
        Ok(Self {
            expected_entries,
            bits: BitSet::Shared(bits),
            num_hash_functions,
        })
    }

    pub(super) fn expected_entries(&self) -> u64 {
        self.expected_entries
    }

    pub(super) fn bytes(&self) -> &[u8] {
        self.bits.bytes()
    }

    pub(super) fn test_hash(&self, hash: i32) -> bool {
        (1..=self.num_hash_functions).all(|iteration| self.is_set(self.position(hash, iteration)))
    }

    fn add_hash(&mut self, hash: i32) {
        for iteration in 1..=self.num_hash_functions {
            let position = self.position(hash, iteration);
            match &mut self.bits {
                BitSet::Mutable(bits) => bits[position >> 3] |= 1 << (position & 7),
                BitSet::Shared(_) => unreachable!("serialized Bloom filter is immutable"),
            }
        }
    }

    fn position(&self, hash: i32, iteration: i32) -> usize {
        let hash2 = ((hash as u32) >> 16) as i32;
        let mut combined = hash.wrapping_add(iteration.wrapping_mul(hash2));
        if combined < 0 {
            combined = !combined;
        }
        combined as usize % (self.bits.bytes().len() * BITS_PER_BYTE)
    }

    fn is_set(&self, position: usize) -> bool {
        self.bits.bytes()[position >> 3] & (1 << (position & 7)) != 0
    }
}

impl BitSet {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Mutable(bits) => bits,
            Self::Shared(bits) => bits,
        }
    }
}

fn num_hash_functions(expected_entries: u64, byte_size: usize) -> io::Result<i32> {
    let bit_size = byte_size.checked_mul(BITS_PER_BYTE).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "Bloom filter bit size overflows",
        )
    })?;
    Ok(
        (((bit_size as f64 / expected_entries as f64) * 2.0_f64.ln())
            .round()
            .max(1.0)) as i32,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::murmur_hash::hash_bytes;

    #[test]
    fn test_java_compatible_bit_layout() {
        let hashes = [hash_bytes(b"a"), hash_bytes(b"hello"), hash_bytes(b"world")];
        let filter = BloomFilter::from_hashes(&hashes, 0.05).unwrap().unwrap();

        assert_eq!(filter.expected_entries(), 3);
        assert_eq!(filter.bytes(), &[0xea, 0x22, 0x99]);
        assert!(hashes.into_iter().all(|hash| filter.test_hash(hash)));
    }

    #[test]
    fn test_empty_hashes_do_not_create_filter() {
        assert!(BloomFilter::from_hashes(&[], 0.05).unwrap().is_none());
    }

    #[test]
    fn test_rejects_invalid_serialized_filter() {
        assert!(BloomFilter::from_bytes(0, Bytes::from_static(&[1])).is_err());
        assert!(BloomFilter::from_bytes(1, Bytes::new()).is_err());
    }
}
