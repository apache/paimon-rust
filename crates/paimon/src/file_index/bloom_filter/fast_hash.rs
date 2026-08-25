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

use twox_hash::XxHash64;

use crate::spec::{DataType, Datum};
use crate::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FastHash {
    String,
    Bytes,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Date,
    Time,
    TimestampMillis,
    TimestampMicros,
    LocalZonedTimestampMillis,
    LocalZonedTimestampMicros,
}

impl FastHash {
    pub(super) fn try_new(data_type: &DataType) -> Result<Self> {
        match data_type {
            DataType::Char(_) | DataType::VarChar(_) => Ok(Self::String),
            DataType::Binary(_) | DataType::VarBinary(_) => Ok(Self::Bytes),
            DataType::TinyInt(_) => Ok(Self::TinyInt),
            DataType::SmallInt(_) => Ok(Self::SmallInt),
            DataType::Int(_) => Ok(Self::Int),
            DataType::BigInt(_) => Ok(Self::BigInt),
            DataType::Float(_) => Ok(Self::Float),
            DataType::Double(_) => Ok(Self::Double),
            DataType::Date(_) => Ok(Self::Date),
            DataType::Time(_) => Ok(Self::Time),
            DataType::Timestamp(timestamp_type) if timestamp_type.precision() <= 3 => {
                Ok(Self::TimestampMillis)
            }
            DataType::Timestamp(_) => Ok(Self::TimestampMicros),
            DataType::LocalZonedTimestamp(timestamp_type) if timestamp_type.precision() <= 3 => {
                Ok(Self::LocalZonedTimestampMillis)
            }
            DataType::LocalZonedTimestamp(_) => Ok(Self::LocalZonedTimestampMicros),
            _ => Err(Error::Unsupported {
                message: format!("Bloom filter does not support data type {data_type:?}"),
            }),
        }
    }

    pub(super) fn hash(self, datum: &Datum) -> Result<u64> {
        match (self, datum) {
            (Self::String, Datum::String(value)) => Ok(XxHash64::oneshot(0, value.as_bytes())),
            (Self::Bytes, Datum::Bytes(value)) => Ok(XxHash64::oneshot(0, value)),
            (Self::TinyInt, Datum::TinyInt(value)) => Ok(wang_hash(i64::from(*value))),
            (Self::SmallInt, Datum::SmallInt(value)) => Ok(wang_hash(i64::from(*value))),
            (Self::Int, Datum::Int(value)) => Ok(wang_hash(i64::from(*value))),
            (Self::BigInt, Datum::Long(value)) => Ok(wang_hash(*value)),
            (Self::Float, Datum::Float(value)) => {
                Ok(wang_hash(i64::from(java_float_to_int_bits(*value))))
            }
            (Self::Double, Datum::Double(value)) => Ok(wang_hash(java_double_to_long_bits(*value))),
            (Self::Date, Datum::Date(value)) => Ok(wang_hash(i64::from(*value))),
            (Self::Time, Datum::Time(value)) => Ok(wang_hash(i64::from(*value))),
            (Self::TimestampMillis, Datum::Timestamp { millis, nanos }) => {
                hash_timestamp(false, *millis, *nanos)
            }
            (Self::TimestampMicros, Datum::Timestamp { millis, nanos }) => {
                hash_timestamp(true, *millis, *nanos)
            }
            (Self::LocalZonedTimestampMillis, Datum::LocalZonedTimestamp { millis, nanos }) => {
                hash_timestamp(false, *millis, *nanos)
            }
            (Self::LocalZonedTimestampMicros, Datum::LocalZonedTimestamp { millis, nanos }) => {
                hash_timestamp(true, *millis, *nanos)
            }
            _ => Err(Error::DataInvalid {
                message: format!("Datum {datum:?} does not match Bloom hash strategy {self:?}"),
                source: None,
            }),
        }
    }
}

fn hash_timestamp(micros: bool, millis: i64, nanos: i32) -> Result<u64> {
    if !(0..=999_999).contains(&nanos) {
        return Err(Error::DataInvalid {
            message: format!("Timestamp nanos-of-millisecond is out of range: {nanos}"),
            source: None,
        });
    }
    if !micros {
        return Ok(wang_hash(millis));
    }

    let micros = millis
        .checked_mul(1_000)
        .and_then(|value| value.checked_add(i64::from(nanos / 1_000)))
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Timestamp cannot be represented in microseconds: millis={millis}, nanos={nanos}"
            ),
            source: None,
        })?;
    Ok(wang_hash(micros))
}

fn java_float_to_int_bits(value: f32) -> i32 {
    if value.is_nan() {
        0x7fc0_0000_u32 as i32
    } else {
        value.to_bits() as i32
    }
}

fn java_double_to_long_bits(value: f64) -> i64 {
    if value.is_nan() {
        0x7ff8_0000_0000_0000_u64 as i64
    } else {
        value.to_bits() as i64
    }
}

fn wang_hash(mut key: i64) -> u64 {
    key = (!key).wrapping_add(key.wrapping_shl(21));
    key ^= key >> 24;
    key = key
        .wrapping_add(key.wrapping_shl(3))
        .wrapping_add(key.wrapping_shl(8));
    key ^= key >> 14;
    key = key
        .wrapping_add(key.wrapping_shl(2))
        .wrapping_add(key.wrapping_shl(4));
    key ^= key >> 28;
    key = key.wrapping_add(key.wrapping_shl(31));
    key as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        BigIntType, BinaryType, CharType, DateType, DoubleType, FloatType, IntType,
        LocalZonedTimestampType, SmallIntType, TimeType, TimestampType, TinyIntType, VarBinaryType,
        VarCharType,
    };

    #[test]
    fn test_java_hash_fixtures() {
        let fixtures = [
            (
                DataType::Char(CharType::new(20).unwrap()),
                Datum::String("Paimon-派蒙".to_string()),
                0xb09c_177e_1aaf_64c1,
            ),
            (
                DataType::VarChar(VarCharType::new(20).unwrap()),
                Datum::String("Paimon-派蒙".to_string()),
                0xb09c_177e_1aaf_64c1,
            ),
            (
                DataType::Binary(BinaryType::new(4).unwrap()),
                Datum::Bytes(vec![0x00, 0x01, 0xfe, 0xff]),
                0x662c_71e0_4101_34be,
            ),
            (
                DataType::VarBinary(VarBinaryType::new(10).unwrap()),
                Datum::Bytes(vec![]),
                0xef46_db37_51d8_e999,
            ),
            (
                DataType::TinyInt(TinyIntType::new()),
                Datum::TinyInt(-128),
                0xe547_e844_4a8f_cdd1,
            ),
            (
                DataType::SmallInt(SmallIntType::new()),
                Datum::SmallInt(-12_345),
                0x6a48_82d9_d48f_ffa6,
            ),
            (
                DataType::Int(IntType::new()),
                Datum::Int(-123_456_789),
                0xe60f_1a14_2420_2ebd,
            ),
            (
                DataType::BigInt(BigIntType::new()),
                Datum::Long(i64::MIN + 123),
                0x52d7_f67f_a5ee_3244,
            ),
            (
                DataType::Float(FloatType::new()),
                Datum::Float(f32::from_bits(0x7fa1_2345)),
                0x67c2_7c6d_9936_ae63,
            ),
            (
                DataType::Float(FloatType::new()),
                Datum::Float(-0.0),
                0x111e_c0fd_6aa8_626c,
            ),
            (
                DataType::Double(DoubleType::new()),
                Datum::Double(f64::from_bits(0x7ff1_2345_6789_abcd)),
                0x13d2_d3f2_cc0e_846e,
            ),
            (
                DataType::Double(DoubleType::new()),
                Datum::Double(-0.0),
                0x3be7_d0f7_780d_e548,
            ),
            (
                DataType::Date(DateType::new()),
                Datum::Date(-1),
                0x5bca_8684_3795_0d03,
            ),
            (
                DataType::Time(TimeType::new(3).unwrap()),
                Datum::Time(86_399_999),
                0x0697_db46_7133_6cb5,
            ),
            (
                DataType::Timestamp(TimestampType::new(3).unwrap()),
                Datum::Timestamp {
                    millis: -123_456_789,
                    nanos: 0,
                },
                0xe60f_1a14_2420_2ebd,
            ),
            (
                DataType::Timestamp(TimestampType::new(6).unwrap()),
                Datum::Timestamp {
                    millis: -1,
                    nanos: 999_000,
                },
                0x5bca_8684_3795_0d03,
            ),
            (
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
                Datum::LocalZonedTimestamp {
                    millis: 1_700_000_000_123,
                    nanos: 0,
                },
                0xf52b_5278_fb88_f260,
            ),
            (
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(6).unwrap()),
                Datum::LocalZonedTimestamp {
                    millis: 1_700_000_000_123,
                    nanos: 456_000,
                },
                0x5e6e_89d6_5e49_5754,
            ),
        ];

        for (data_type, datum, expected) in fixtures {
            assert_eq!(
                FastHash::try_new(&data_type).unwrap().hash(&datum).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn test_reject_mismatched_datum_and_invalid_timestamp() {
        assert!(matches!(
            FastHash::try_new(&DataType::BigInt(BigIntType::new()))
                .unwrap()
                .hash(&Datum::Int(1)),
            Err(Error::DataInvalid { .. })
        ));
        assert!(matches!(
            FastHash::try_new(&DataType::Timestamp(TimestampType::new(6).unwrap()))
                .unwrap()
                .hash(&Datum::Timestamp {
                    millis: 0,
                    nanos: 1_000_000,
                }),
            Err(Error::DataInvalid { .. })
        ));
    }

    #[test]
    fn test_hash_strategy_compatibility() {
        assert_eq!(
            FastHash::try_new(&DataType::Char(CharType::with_nullable(false, 3).unwrap())).unwrap(),
            FastHash::try_new(&DataType::VarChar(VarCharType::new(200).unwrap())).unwrap()
        );
        assert_eq!(
            FastHash::try_new(&DataType::Binary(BinaryType::new(4).unwrap())).unwrap(),
            FastHash::try_new(&DataType::VarBinary(
                VarBinaryType::try_new(false, 1_024).unwrap()
            ))
            .unwrap()
        );
        assert_eq!(
            FastHash::try_new(&DataType::BigInt(BigIntType::new())).unwrap(),
            FastHash::try_new(&DataType::BigInt(BigIntType::with_nullable(false))).unwrap()
        );
        assert_eq!(
            FastHash::try_new(&DataType::Timestamp(TimestampType::new(4).unwrap())).unwrap(),
            FastHash::try_new(&DataType::Timestamp(
                TimestampType::with_nullable(false, 9).unwrap()
            ))
            .unwrap()
        );
        assert_ne!(
            FastHash::try_new(&DataType::Timestamp(TimestampType::new(3).unwrap())).unwrap(),
            FastHash::try_new(&DataType::Timestamp(TimestampType::new(4).unwrap())).unwrap()
        );
    }
}
