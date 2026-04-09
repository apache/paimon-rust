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

//! BTree key serialization and comparison.
//!
//! Reference: [org.apache.paimon.globalindex.btree.KeySerializer](https://github.com/apache/paimon/blob/master/paimon-common/src/main/java/org/apache/paimon/globalindex/btree/KeySerializer.java)

use crate::btree::var_len::{decode_var_int_from_slice, encode_var_int};
use crate::spec::{DataType, Datum};
use std::cmp::Ordering;

/// Timestamp precision <= 3 is compact (millis only).
const TIMESTAMP_COMPACT_PRECISION: u32 = 3;
/// Decimal precision <= 18 is compact (fits in i64).
const DECIMAL_COMPACT_PRECISION: u32 = 18;

/// Key comparator type alias.
pub type KeyComparator = Box<dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync>;

/// Create a key comparator based on the data type.
/// For fixed-size numeric types, compares by decoded value.
/// For variable-length types (string, bytes), uses lexicographic byte comparison.
pub fn make_key_comparator(data_type: &DataType) -> KeyComparator {
    match data_type {
        DataType::TinyInt(_) => Box::new(|a: &[u8], b: &[u8]| (a[0] as i8).cmp(&(b[0] as i8))),
        DataType::SmallInt(_) => Box::new(|a: &[u8], b: &[u8]| {
            let av = i16::from_le_bytes(a[..2].try_into().unwrap());
            let bv = i16::from_le_bytes(b[..2].try_into().unwrap());
            av.cmp(&bv)
        }),
        DataType::Int(_) | DataType::Date(_) | DataType::Time(_) => {
            Box::new(|a: &[u8], b: &[u8]| {
                let av = i32::from_le_bytes(a[..4].try_into().unwrap());
                let bv = i32::from_le_bytes(b[..4].try_into().unwrap());
                av.cmp(&bv)
            })
        }
        DataType::BigInt(_) => Box::new(|a: &[u8], b: &[u8]| {
            let av = i64::from_le_bytes(a[..8].try_into().unwrap());
            let bv = i64::from_le_bytes(b[..8].try_into().unwrap());
            av.cmp(&bv)
        }),
        DataType::Float(_) => Box::new(|a: &[u8], b: &[u8]| {
            let av = f32::from_le_bytes(a[..4].try_into().unwrap());
            let bv = f32::from_le_bytes(b[..4].try_into().unwrap());
            av.total_cmp(&bv)
        }),
        DataType::Double(_) => Box::new(|a: &[u8], b: &[u8]| {
            let av = f64::from_le_bytes(a[..8].try_into().unwrap());
            let bv = f64::from_le_bytes(b[..8].try_into().unwrap());
            av.total_cmp(&bv)
        }),
        DataType::Timestamp(t) if t.precision() > TIMESTAMP_COMPACT_PRECISION => {
            // Non-compact: millis (8 bytes LE) + nanoOfMillisecond (varint)
            Box::new(|a: &[u8], b: &[u8]| {
                let a_millis = i64::from_le_bytes(a[..8].try_into().unwrap());
                let b_millis = i64::from_le_bytes(b[..8].try_into().unwrap());
                let (a_nanos, _) = decode_var_int_from_slice(a, 8);
                let (b_nanos, _) = decode_var_int_from_slice(b, 8);
                a_millis.cmp(&b_millis).then_with(|| a_nanos.cmp(&b_nanos))
            })
        }
        DataType::LocalZonedTimestamp(t) if t.precision() > TIMESTAMP_COMPACT_PRECISION => {
            Box::new(|a: &[u8], b: &[u8]| {
                let a_millis = i64::from_le_bytes(a[..8].try_into().unwrap());
                let b_millis = i64::from_le_bytes(b[..8].try_into().unwrap());
                let (a_nanos, _) = decode_var_int_from_slice(a, 8);
                let (b_nanos, _) = decode_var_int_from_slice(b, 8);
                a_millis.cmp(&b_millis).then_with(|| a_nanos.cmp(&b_nanos))
            })
        }
        DataType::Decimal(d) if d.precision() > DECIMAL_COMPACT_PRECISION => {
            // Non-compact: raw unscaled bytes, compare as big-endian signed
            Box::new(|a: &[u8], b: &[u8]| {
                let a_neg = !a.is_empty() && (a[0] & 0x80) != 0;
                let b_neg = !b.is_empty() && (b[0] & 0x80) != 0;
                match (a_neg, b_neg) {
                    (true, false) => Ordering::Less,
                    (false, true) => Ordering::Greater,
                    _ => a.cmp(b),
                }
            })
        }
        // Compact Timestamp/LocalZonedTimestamp (precision <= 3): millis as i64 LE
        DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_) => {
            Box::new(|a: &[u8], b: &[u8]| {
                let av = i64::from_le_bytes(a[..8].try_into().unwrap());
                let bv = i64::from_le_bytes(b[..8].try_into().unwrap());
                av.cmp(&bv)
            })
        }
        // Compact Decimal (precision <= 18): unscaled as i64 LE
        DataType::Decimal(_) => Box::new(|a: &[u8], b: &[u8]| {
            let av = i64::from_le_bytes(a[..8].try_into().unwrap());
            let bv = i64::from_le_bytes(b[..8].try_into().unwrap());
            av.cmp(&bv)
        }),
        // String, VarChar, Char, Bytes — lexicographic
        _ => Box::new(|a: &[u8], b: &[u8]| a.cmp(b)),
    }
}

/// Serialize a Datum to BTree key bytes (little-endian, matching Java Paimon's KeySerializer).
pub fn serialize_datum(datum: &Datum, data_type: &DataType) -> Vec<u8> {
    match datum {
        Datum::Bool(v) => vec![*v as u8],
        Datum::TinyInt(v) => vec![*v as u8],
        Datum::SmallInt(v) => v.to_le_bytes().to_vec(),
        Datum::Int(v) | Datum::Date(v) | Datum::Time(v) => v.to_le_bytes().to_vec(),
        Datum::Long(v) => v.to_le_bytes().to_vec(),
        Datum::Float(v) => v.to_le_bytes().to_vec(),
        Datum::Double(v) => v.to_le_bytes().to_vec(),
        Datum::String(v) => v.as_bytes().to_vec(),
        Datum::Timestamp { millis, nanos } | Datum::LocalZonedTimestamp { millis, nanos } => {
            let precision = match data_type {
                DataType::Timestamp(t) => t.precision(),
                DataType::LocalZonedTimestamp(t) => t.precision(),
                _ => 3,
            };
            let mut buf = millis.to_le_bytes().to_vec();
            if precision > TIMESTAMP_COMPACT_PRECISION {
                encode_var_int(&mut buf, *nanos).unwrap();
            }
            buf
        }
        Datum::Decimal {
            unscaled,
            precision,
            ..
        } => {
            if *precision <= DECIMAL_COMPACT_PRECISION {
                (*unscaled as i64).to_le_bytes().to_vec()
            } else {
                unscaled.to_be_bytes().to_vec()
            }
        }
        Datum::Bytes(v) => v.clone(),
    }
}
