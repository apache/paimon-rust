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

use crate::btree::var_len::{encode_var_int, try_decode_var_int_from_slice};
use crate::spec::{DataType, Datum, VariantType};
use std::cmp::Ordering;

/// Timestamp precision <= 3 is compact (millis only).
const TIMESTAMP_COMPACT_PRECISION: u32 = 3;
/// Decimal precision <= 18 is compact (fits in i64).
const DECIMAL_COMPACT_PRECISION: u32 = 18;

/// Key comparator type alias.
///
/// Fallible because the bytes come from an index file that may have been written when
/// the column had a different type. [`make_key_comparator`] picks its arm from the
/// column's *current* type, and nothing records the type the index was built with:
/// neither `IndexManifestEntry` nor `IndexFileMeta` carries a schema id.
/// `SchemaChange::UpdateColumnType` guards partition, primary-key, bucket-key and
/// primary-key-index columns but not global-index columns, so `INT` to `BIGINT` is
/// accepted and every stored key is then 4 bytes short of what the new arm reads. That
/// used to index out of bounds and panic inside a query; an `Err` lets the caller give
/// up on the index and fall back instead.
pub type KeyComparator = Box<dyn Fn(&[u8], &[u8]) -> crate::Result<Ordering> + Send + Sync>;

/// A borrowed key comparator, for signatures that only call one. [`KeyComparator`] is
/// the owned form callers build from a [`DataType`].
pub type DynKeyComparator<'a> = dyn Fn(&[u8], &[u8]) -> crate::Result<Ordering> + 'a;

/// Take the fixed-width body of a key, or explain why it cannot belong to `type_name`.
///
/// The width must match exactly: [`serialize_datum`] emits exactly `N` bytes for these
/// types, so a longer key is as much a foreign encoding as a shorter one, and silently
/// comparing its prefix would answer the query from the wrong bytes.
pub(crate) fn fixed_key_bytes<const N: usize>(
    key: &[u8],
    type_name: &str,
) -> crate::Result<[u8; N]> {
    key.try_into().map_err(|_| crate::Error::DataInvalid {
        message: format!(
            "Global index key of {} byte(s) cannot be a {type_name} key of {N}; the index was \
             built before the column's type changed and cannot be used",
            key.len()
        ),
        source: None,
    })
}

fn variable_key_body(key: &[u8], min_len: usize, type_name: &str) -> crate::Result<()> {
    if key.len() < min_len {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "Global index key of {} byte(s) cannot be a {type_name} key of at least \
                 {min_len}; the index was built before the column's type changed and cannot be \
                 used",
                key.len()
            ),
            source: None,
        });
    }
    Ok(())
}

/// A [`KeyComparator`] failure travelling through a reader's [`std::io::Result`].
///
/// The BTree and bitmap readers interleave key comparison with file I/O and report both
/// as [`std::io::Error`], but their caller has to treat the two differently: a comparison
/// failure means the stored bytes are not keys of this type, so the index simply cannot
/// answer and the query falls back to a scan, while a real I/O failure must fail the
/// query. Wrapping keeps both in one channel and lets
/// [`is_key_comparison_failure`] tell them apart.
#[derive(Debug)]
pub struct KeyComparisonFailure {
    message: String,
}

impl std::fmt::Display for KeyComparisonFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for KeyComparisonFailure {}

/// Wrap a comparator failure for a reader that returns [`std::io::Result`].
pub(crate) fn key_comparison_io_error(error: crate::Error) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        KeyComparisonFailure {
            message: error.to_string(),
        },
    )
}

/// Whether `error` is a key comparison failure rather than a real I/O failure.
pub(crate) fn is_key_comparison_failure(error: &std::io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|source| source.is::<KeyComparisonFailure>())
}

/// Create a key comparator based on the data type.
/// For fixed-size numeric types, compares by decoded value.
/// For variable-length types (string, bytes), uses lexicographic byte comparison.
pub fn make_key_comparator(data_type: &DataType) -> KeyComparator {
    match data_type {
        DataType::TinyInt(_) => Box::new(|a: &[u8], b: &[u8]| {
            let av = i8::from_le_bytes(fixed_key_bytes::<1>(a, "TINYINT")?);
            let bv = i8::from_le_bytes(fixed_key_bytes::<1>(b, "TINYINT")?);
            Ok(av.cmp(&bv))
        }),
        DataType::SmallInt(_) => Box::new(|a: &[u8], b: &[u8]| {
            let av = i16::from_le_bytes(fixed_key_bytes::<2>(a, "SMALLINT")?);
            let bv = i16::from_le_bytes(fixed_key_bytes::<2>(b, "SMALLINT")?);
            Ok(av.cmp(&bv))
        }),
        DataType::Int(_) | DataType::Date(_) | DataType::Time(_) => {
            Box::new(|a: &[u8], b: &[u8]| {
                let av = i32::from_le_bytes(fixed_key_bytes::<4>(a, "INT")?);
                let bv = i32::from_le_bytes(fixed_key_bytes::<4>(b, "INT")?);
                Ok(av.cmp(&bv))
            })
        }
        DataType::BigInt(_) => Box::new(|a: &[u8], b: &[u8]| {
            let av = i64::from_le_bytes(fixed_key_bytes::<8>(a, "BIGINT")?);
            let bv = i64::from_le_bytes(fixed_key_bytes::<8>(b, "BIGINT")?);
            Ok(av.cmp(&bv))
        }),
        DataType::Float(_) => Box::new(|a: &[u8], b: &[u8]| {
            let av = f32::from_le_bytes(fixed_key_bytes::<4>(a, "FLOAT")?);
            let bv = f32::from_le_bytes(fixed_key_bytes::<4>(b, "FLOAT")?);
            Ok(av.total_cmp(&bv))
        }),
        DataType::Double(_) => Box::new(|a: &[u8], b: &[u8]| {
            let av = f64::from_le_bytes(fixed_key_bytes::<8>(a, "DOUBLE")?);
            let bv = f64::from_le_bytes(fixed_key_bytes::<8>(b, "DOUBLE")?);
            Ok(av.total_cmp(&bv))
        }),
        DataType::Timestamp(t) if t.precision() > TIMESTAMP_COMPACT_PRECISION => {
            // Non-compact: millis (8 bytes LE) + nanoOfMillisecond (varint)
            Box::new(|a: &[u8], b: &[u8]| compare_non_compact_timestamps(a, b))
        }
        DataType::LocalZonedTimestamp(t) if t.precision() > TIMESTAMP_COMPACT_PRECISION => {
            Box::new(|a: &[u8], b: &[u8]| compare_non_compact_timestamps(a, b))
        }
        DataType::Decimal(d) if d.precision() > DECIMAL_COMPACT_PRECISION => {
            // Non-compact Decimal keys use Java BigInteger.toByteArray() bytes.
            Box::new(|a: &[u8], b: &[u8]| {
                Ok(decode_java_big_integer_i128(a)?.cmp(&decode_java_big_integer_i128(b)?))
            })
        }
        // Compact Timestamp/LocalZonedTimestamp (precision <= 3): millis as i64 LE
        DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_) => {
            Box::new(|a: &[u8], b: &[u8]| {
                let av = i64::from_le_bytes(fixed_key_bytes::<8>(a, "TIMESTAMP")?);
                let bv = i64::from_le_bytes(fixed_key_bytes::<8>(b, "TIMESTAMP")?);
                Ok(av.cmp(&bv))
            })
        }
        // Compact Decimal (precision <= 18): unscaled as i64 LE
        DataType::Decimal(_) => Box::new(|a: &[u8], b: &[u8]| {
            let av = i64::from_le_bytes(fixed_key_bytes::<8>(a, "DECIMAL")?);
            let bv = i64::from_le_bytes(fixed_key_bytes::<8>(b, "DECIMAL")?);
            Ok(av.cmp(&bv))
        }),
        // String, VarChar, Char, Bytes — lexicographic, so any width is readable
        _ => Box::new(|a: &[u8], b: &[u8]| Ok(a.cmp(b))),
    }
}

/// Millis (8 bytes LE) then the varint nano-of-millisecond, both bounds-checked.
fn compare_non_compact_timestamps(a: &[u8], b: &[u8]) -> crate::Result<Ordering> {
    variable_key_body(a, 9, "TIMESTAMP")?;
    variable_key_body(b, 9, "TIMESTAMP")?;
    let a_millis = i64::from_le_bytes(fixed_key_bytes::<8>(&a[..8], "TIMESTAMP")?);
    let b_millis = i64::from_le_bytes(fixed_key_bytes::<8>(&b[..8], "TIMESTAMP")?);
    let (a_nanos, _) = try_decode_var_int_from_slice(a, 8)?;
    let (b_nanos, _) = try_decode_var_int_from_slice(b, 8)?;
    Ok(a_millis.cmp(&b_millis).then_with(|| a_nanos.cmp(&b_nanos)))
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

/// Read the Java `BigInteger.toByteArray()` bytes of a non-compact DECIMAL key.
///
/// Anything outside 1..=16 bytes cannot be one: [`encode_java_big_integer_i128`] never
/// emits an empty slice, and more than 16 bytes does not fit the `i128` unscaled value.
/// Such a key belongs to the type the column had when the index was built.
///
/// The bound is all this arm can check, and 1..=16 covers every fixed-width key this
/// module writes, so a stale `INT` or `BIGINT` key is accepted here and read as a
/// big-endian magnitude -- `numeric` to `DECIMAL(p > 18)` is an implicit cast, so that
/// is reachable. It gives a wrong ordering rather than a panic, which puts it in the
/// same undetectable class as `INT` to `FLOAT`: telling a foreign key from a real one
/// needs the type the index was built with, and no index file records it.
fn decode_java_big_integer_i128(bytes: &[u8]) -> crate::Result<i128> {
    if bytes.is_empty() || bytes.len() > 16 {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "Global index key of {} byte(s) cannot be a non-compact DECIMAL key of 1 to 16; \
                 the index was built before the column's type changed and cannot be used",
                bytes.len()
            ),
            source: None,
        });
    }
    let negative = bytes[0] & 0x80 != 0;
    let mut value = if negative { -1 } else { 0 };
    for &byte in bytes {
        value = (value << 8) | i128::from(byte);
    }
    Ok(value)
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

        assert_eq!(cmp(&key_127, &key_128).unwrap(), Ordering::Less);
        assert_eq!(cmp(&key_minus_129, &key_127).unwrap(), Ordering::Less);
    }

    #[test]
    fn test_compact_numeric_still_uses_little_endian() {
        let key = serialize_datum(&Datum::Int(42), &DataType::Int(IntType::new()));
        assert_eq!(key, 42i32.to_le_bytes());
    }

    /// Every arm that reads a fixed number of bytes must report a key of the wrong
    /// width instead of indexing out of bounds. Each of these used to panic inside a
    /// query after `ALTER COLUMN ... TYPE` widened an indexed column.
    #[test]
    fn test_comparator_rejects_keys_of_another_type_instead_of_panicking() {
        use crate::spec::{BigIntType, TimestampType, VarCharType};

        // An index built on INT, read after the column became BIGINT.
        let big_int = make_key_comparator(&DataType::BigInt(BigIntType::new()));
        assert!(big_int(&[0; 4], &[0; 8]).is_err());
        assert!(big_int(&[0; 8], &[0; 4]).is_err());
        assert_eq!(big_int(&[0; 8], &[0; 8]).unwrap(), Ordering::Equal);

        // An INT arm handed the 8-byte keys of a column that used to be BIGINT.
        let int = make_key_comparator(&DataType::Int(IntType::new()));
        assert!(int(&[0; 8], &[0; 4]).is_err());

        // An index built on TIMESTAMP(3), read after the column became TIMESTAMP(6):
        // the non-compact arm decodes a varint at offset 8.
        let non_compact = make_key_comparator(&DataType::Timestamp(TimestampType::new(6).unwrap()));
        assert!(non_compact(&[0; 8], &[0; 9]).is_err());
        assert_eq!(non_compact(&[0; 9], &[0; 9]).unwrap(), Ordering::Equal);
        let compact = make_key_comparator(&DataType::Timestamp(TimestampType::new(3).unwrap()));
        assert!(compact(&[0; 9], &[0; 8]).is_err());

        // Non-compact DECIMAL keys are BigInteger bytes: never empty, never over 16.
        let decimal = make_key_comparator(&DataType::Decimal(DecimalType::new(20, 0).unwrap()));
        assert!(decimal(&[], &[0x01]).is_err());
        assert!(decimal(&[0; 17], &[0x01]).is_err());

        // Character and byte keys are compared lexicographically, so no width is wrong.
        let varchar = make_key_comparator(&DataType::VarChar(VarCharType::new(10).unwrap()));
        assert_eq!(varchar(&[], &[0; 3]).unwrap(), Ordering::Less);
    }
}
