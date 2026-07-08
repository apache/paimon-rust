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
use crate::spec::{DataType, Datum, VariantType};
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
            // Non-compact Decimal keys use Java BigInteger.toByteArray() bytes.
            Box::new(|a: &[u8], b: &[u8]| {
                decode_java_big_integer_i128(a).cmp(&decode_java_big_integer_i128(b))
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
            let key_precision = match data_type {
                DataType::Decimal(decimal_type) => decimal_type.precision(),
                _ => *precision,
            };
            if key_precision <= DECIMAL_COMPACT_PRECISION {
                (*unscaled as i64).to_le_bytes().to_vec()
            } else {
                encode_java_big_integer_i128(*unscaled)
            }
        }
        Datum::Bytes(v) => v.clone(),
        Datum::Variant { value, metadata } => {
            VariantType::validate_payload(value, metadata)
                .expect("invalid Variant payload for BTree key");
            let mut bytes = Vec::with_capacity(4 + value.len() + metadata.len());
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value);
            bytes.extend_from_slice(metadata);
            bytes
        }
    }
}

fn encode_java_big_integer_i128(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let mut start = 0;
    while start < bytes.len() - 1 {
        let current = bytes[start];
        let next = bytes[start + 1];
        if (current == 0x00 && next & 0x80 == 0) || (current == 0xff && next & 0x80 != 0) {
            start += 1;
        } else {
            break;
        }
    }
    bytes[start..].to_vec()
}

fn decode_java_big_integer_i128(bytes: &[u8]) -> i128 {
    if bytes.is_empty() {
        return 0;
    }
    let negative = bytes[0] & 0x80 != 0;
    let mut value = if negative { -1 } else { 0 };
    for &byte in bytes {
        value = (value << 8) | i128::from(byte);
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DecimalType, IntType};

    #[test]
    fn test_serialize_non_compact_decimal_matches_java_big_integer_bytes() {
        let data_type = DataType::Decimal(DecimalType::new(20, 0).unwrap());

        assert_eq!(
            serialize_datum(
                &Datum::Decimal {
                    unscaled: 127,
                    precision: 20,
                    scale: 0,
                },
                &data_type,
            ),
            vec![0x7f]
        );
        assert_eq!(
            serialize_datum(
                &Datum::Decimal {
                    unscaled: 128,
                    precision: 20,
                    scale: 0,
                },
                &data_type,
            ),
            vec![0x00, 0x80]
        );
        assert_eq!(
            serialize_datum(
                &Datum::Decimal {
                    unscaled: -129,
                    precision: 20,
                    scale: 0,
                },
                &data_type,
            ),
            vec![0xff, 0x7f]
        );
    }

    #[test]
    fn test_decimal_key_serialization_uses_column_precision_not_literal_precision() {
        let compact_column = DataType::Decimal(DecimalType::new(10, 0).unwrap());
        let non_compact_column = DataType::Decimal(DecimalType::new(20, 0).unwrap());

        let compact_column_key = serialize_datum(
            &Datum::Decimal {
                unscaled: 128,
                precision: 20,
                scale: 0,
            },
            &compact_column,
        );
        assert_eq!(compact_column_key, 128i64.to_le_bytes());

        let non_compact_column_key = serialize_datum(
            &Datum::Decimal {
                unscaled: 128,
                precision: 10,
                scale: 0,
            },
            &non_compact_column,
        );
        assert_eq!(non_compact_column_key, vec![0x00, 0x80]);
    }

    #[test]
    fn test_compare_non_compact_decimal_uses_numeric_order() {
        let cmp = make_key_comparator(&DataType::Decimal(DecimalType::new(20, 0).unwrap()));
        let key_127 = encode_java_big_integer_i128(127);
        let key_128 = encode_java_big_integer_i128(128);
        let key_minus_129 = encode_java_big_integer_i128(-129);

        assert_eq!(cmp(&key_127, &key_128), Ordering::Less);
        assert_eq!(cmp(&key_minus_129, &key_127), Ordering::Less);
    }

    #[test]
    fn test_compact_numeric_still_uses_little_endian() {
        let key = serialize_datum(&Datum::Int(42), &DataType::Int(IntType::new()));
        assert_eq!(key, 42i32.to_le_bytes());
    }
}
