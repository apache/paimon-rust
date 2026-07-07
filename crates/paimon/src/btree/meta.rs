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

//! BTree index meta, compatible with Java Paimon's BTreeIndexMeta.
//!
//! Serialization format (little-endian):
//! ```text
//! | first_key_length (4) | first_key_bytes | last_key_length (4) | last_key_bytes |
//! | has_nulls (1) | format_version (1) | null_key_flags (1) |
//! ```
//! Null key flags distinguish empty serialized keys from absent keys.

use crate::spec::PredicateOperator;
use std::cmp::Ordering;
use std::io;

const FORMAT_VERSION_WITH_NULL_FLAGS: u8 = 1;
const FIRST_KEY_IS_NULL: u8 = 1;
const LAST_KEY_IS_NULL: u8 = 1 << 1;

/// Index meta for each BTree index file.
#[derive(Debug, Clone)]
pub struct BTreeIndexMeta {
    pub first_key: Option<Vec<u8>>,
    pub last_key: Option<Vec<u8>>,
    pub has_nulls: bool,
}

impl BTreeIndexMeta {
    pub fn new(first_key: Option<Vec<u8>>, last_key: Option<Vec<u8>>, has_nulls: bool) -> Self {
        Self {
            first_key,
            last_key,
            has_nulls,
        }
    }

    pub fn only_nulls(&self) -> bool {
        self.first_key.is_none() && self.last_key.is_none()
    }

    /// File-level pruning: check if this BTree file may contain matching keys.
    pub fn may_match(
        &self,
        op: PredicateOperator,
        serialized_literals: &[Vec<u8>],
        cmp: &dyn Fn(&[u8], &[u8]) -> Ordering,
    ) -> bool {
        match op {
            PredicateOperator::IsNull => self.has_nulls,
            PredicateOperator::IsNotNull => !self.only_nulls(),
            PredicateOperator::NotEq | PredicateOperator::NotIn => true,
            _ => {
                if self.only_nulls() {
                    return false;
                }
                let (first_key, last_key) = match (&self.first_key, &self.last_key) {
                    (Some(f), Some(l)) => (f.as_slice(), l.as_slice()),
                    _ => return true,
                };
                match op {
                    PredicateOperator::Eq => {
                        cmp(&serialized_literals[0], first_key) != Ordering::Less
                            && cmp(&serialized_literals[0], last_key) != Ordering::Greater
                    }
                    PredicateOperator::Lt => {
                        cmp(first_key, &serialized_literals[0]) == Ordering::Less
                    }
                    PredicateOperator::LtEq => {
                        cmp(first_key, &serialized_literals[0]) != Ordering::Greater
                    }
                    PredicateOperator::Gt => {
                        cmp(last_key, &serialized_literals[0]) == Ordering::Greater
                    }
                    PredicateOperator::GtEq => {
                        cmp(last_key, &serialized_literals[0]) != Ordering::Less
                    }
                    PredicateOperator::In => serialized_literals.iter().any(|key| {
                        cmp(key, first_key) != Ordering::Less
                            && cmp(key, last_key) != Ordering::Greater
                    }),
                    _ => true,
                }
            }
        }
    }

    /// File-level pruning for between: file may match if [first_key, last_key] overlaps [from, to].
    pub fn may_match_between(
        &self,
        from_key: &[u8],
        to_key: &[u8],
        cmp: &dyn Fn(&[u8], &[u8]) -> Ordering,
    ) -> bool {
        if self.only_nulls() {
            return false;
        }
        let (first_key, last_key) = match (&self.first_key, &self.last_key) {
            (Some(f), Some(l)) => (f.as_slice(), l.as_slice()),
            _ => return true,
        };
        cmp(first_key, to_key) != Ordering::Greater && cmp(last_key, from_key) != Ordering::Less
    }

    /// Serialize to bytes (compatible with Java SortedIndexFileMeta.serialize()).
    pub fn serialize(&self) -> Vec<u8> {
        let fk_len = self.first_key.as_ref().map_or(0, |k| k.len());
        let lk_len = self.last_key.as_ref().map_or(0, |k| k.len());
        let mut buf = Vec::with_capacity(fk_len + lk_len + 11);
        let mut null_key_flags = 0u8;

        // first key
        match &self.first_key {
            Some(k) => {
                buf.extend_from_slice(&(k.len() as i32).to_le_bytes());
                buf.extend_from_slice(k);
            }
            None => {
                buf.extend_from_slice(&0i32.to_le_bytes());
                null_key_flags |= FIRST_KEY_IS_NULL;
            }
        }

        // last key
        match &self.last_key {
            Some(k) => {
                buf.extend_from_slice(&(k.len() as i32).to_le_bytes());
                buf.extend_from_slice(k);
            }
            None => {
                buf.extend_from_slice(&0i32.to_le_bytes());
                null_key_flags |= LAST_KEY_IS_NULL;
            }
        }

        // has_nulls
        buf.push(if self.has_nulls { 1 } else { 0 });
        buf.push(FORMAT_VERSION_WITH_NULL_FLAGS);
        buf.push(null_key_flags);

        buf
    }

    /// Deserialize from bytes (compatible with Java SortedIndexFileMeta.deserialize()).
    pub fn deserialize(data: &[u8]) -> io::Result<Self> {
        if data.len() < 9 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "BTreeIndexMeta data too short",
            ));
        }

        let mut pos = 0;

        let fk_len = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let mut first_key = {
            let key = data[pos..pos + fk_len].to_vec();
            pos += fk_len;
            Some(key)
        };

        let lk_len = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let mut last_key = {
            let key = data[pos..pos + lk_len].to_vec();
            pos += lk_len;
            Some(key)
        };

        let has_nulls = data[pos] == 1;
        pos += 1;

        if data.len().saturating_sub(pos) >= 2 {
            let format_version = data[pos];
            pos += 1;
            if format_version == FORMAT_VERSION_WITH_NULL_FLAGS {
                let null_key_flags = data[pos];
                if null_key_flags & FIRST_KEY_IS_NULL != 0 {
                    first_key = None;
                }
                if null_key_flags & LAST_KEY_IS_NULL != 0 {
                    last_key = None;
                }
            }
        } else if fk_len == 0 && lk_len == 0 && has_nulls {
            first_key = None;
            last_key = None;
        }

        Ok(Self {
            first_key,
            last_key,
            has_nulls,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_roundtrip() {
        let meta = BTreeIndexMeta::new(Some(b"abc".to_vec()), Some(b"xyz".to_vec()), true);
        let encoded = meta.serialize();
        let decoded = BTreeIndexMeta::deserialize(&encoded).unwrap();
        assert_eq!(decoded.first_key, Some(b"abc".to_vec()));
        assert_eq!(decoded.last_key, Some(b"xyz".to_vec()));
        assert!(decoded.has_nulls);
    }

    #[test]
    fn test_meta_only_nulls() {
        let meta = BTreeIndexMeta::new(None, None, true);
        assert!(meta.only_nulls());
        let encoded = meta.serialize();
        let decoded = BTreeIndexMeta::deserialize(&encoded).unwrap();
        assert!(decoded.only_nulls());
        assert!(decoded.has_nulls);
    }

    #[test]
    fn test_meta_empty_string_keys_are_not_null() {
        let meta = BTreeIndexMeta::new(Some(Vec::new()), Some(Vec::new()), false);
        let encoded = meta.serialize();
        let decoded = BTreeIndexMeta::deserialize(&encoded).unwrap();
        assert_eq!(decoded.first_key, Some(Vec::new()));
        assert_eq!(decoded.last_key, Some(Vec::new()));
        assert!(!decoded.only_nulls());
        assert!(!decoded.has_nulls);
    }

    #[test]
    fn test_meta_no_nulls() {
        let meta = BTreeIndexMeta::new(Some(b"key1".to_vec()), Some(b"key2".to_vec()), false);
        let encoded = meta.serialize();
        let decoded = BTreeIndexMeta::deserialize(&encoded).unwrap();
        assert!(!decoded.has_nulls);
        assert!(!decoded.only_nulls());
    }
}
