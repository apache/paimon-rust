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

mod bloom_filter_64;
mod fast_hash;

use bytes::{Bytes, BytesMut};

use crate::common::Options;
use crate::file_index::file_index_reader::FileIndexReader;
use crate::file_index::file_index_result::FileIndexResult;
use crate::file_index::file_index_writer::FileIndexWriter;
use crate::spec::{DataType, Datum, PredicateOperator};
use crate::{Error, Result};

use bloom_filter_64::BloomFilter64;
use fast_hash::FastHash;

const DEFAULT_ITEMS: i32 = 1_000_000;
const DEFAULT_FPP: f64 = 0.1;
const ITEMS: &str = "items";
const FPP: &str = "fpp";

pub(crate) struct BloomFilterWriter {
    hash_function: FastHash,
    filter: BloomFilter64,
    empty: bool,
}

impl BloomFilterWriter {
    pub(crate) fn try_new(data_type: DataType, options: &Options) -> Result<Self> {
        let hash_function = FastHash::try_new(&data_type)?;
        let items = parse_option(options, ITEMS, DEFAULT_ITEMS)?;
        let fpp = parse_option(options, FPP, DEFAULT_FPP)?;
        let filter = BloomFilter64::try_new(items, fpp)?;
        Ok(Self {
            hash_function,
            filter,
            empty: true,
        })
    }
}

impl FileIndexWriter for BloomFilterWriter {
    fn write(&mut self, datum: Option<&Datum>) -> Result<()> {
        if let Some(datum) = datum {
            self.filter.add_hash(self.hash_function.hash(datum)?);
        }
        self.empty = false;
        Ok(())
    }

    fn serialized_bytes(&mut self) -> Result<Bytes> {
        let mut serialized = BytesMut::with_capacity(4 + self.filter.bytes().len());
        serialized.extend_from_slice(&self.filter.num_hash_functions().to_be_bytes());
        serialized.extend_from_slice(self.filter.bytes());
        Ok(serialized.freeze())
    }

    fn empty(&self) -> bool {
        self.empty
    }
}

pub(crate) struct BloomFilterReader {
    hash_function: FastHash,
    filter: BloomFilter64,
}

impl BloomFilterReader {
    pub(crate) fn try_new(data_type: DataType, serialized: Bytes) -> Result<Self> {
        let hash_function = FastHash::try_new(&data_type)?;
        let header = serialized
            .get(..4)
            .ok_or_else(|| Error::FileIndexFormatInvalid {
                message: format!(
                    "Bloom filter payload must contain a 4-byte header, but had {} bytes",
                    serialized.len()
                ),
            })?;
        let num_hash_functions = i32::from_be_bytes(header.try_into().unwrap());
        let filter = BloomFilter64::from_serialized(num_hash_functions, serialized.slice(4..))?;
        Ok(Self {
            hash_function,
            filter,
        })
    }

    fn may_contain_literal(&self, datum: &Datum) -> Result<bool> {
        let hash = self.hash_function.hash(datum)?;
        if self.filter.test_hash(hash) {
            return Ok(true);
        }

        // Rust predicates currently treat signed zero as equal, while the Java
        // hash contract preserves the sign bit. Test the equivalent zero hash
        // before pruning without changing the serialized Bloom format.
        match (self.hash_function, datum) {
            (FastHash::Float, Datum::Float(value)) if *value == 0.0 => {
                let opposite = Datum::Float(-*value);
                Ok(self.filter.test_hash(self.hash_function.hash(&opposite)?))
            }
            (FastHash::Double, Datum::Double(value)) if *value == 0.0 => {
                let opposite = Datum::Double(-*value);
                Ok(self.filter.test_hash(self.hash_function.hash(&opposite)?))
            }
            _ => Ok(false),
        }
    }

    fn evaluate_literal(&self, datum: &Datum) -> FileIndexResult {
        match self.may_contain_literal(datum) {
            Ok(false) => FileIndexResult::Skip,
            Ok(_) | Err(_) => FileIndexResult::Remain,
        }
    }
}

impl FileIndexReader for BloomFilterReader {
    fn evaluate(
        &self,
        _column: &str,
        _index: usize,
        data_type: &DataType,
        operator: PredicateOperator,
        literals: &[Datum],
    ) -> FileIndexResult {
        if FastHash::try_new(data_type).ok() != Some(self.hash_function) {
            return FileIndexResult::Remain;
        }

        match operator {
            PredicateOperator::Eq if literals.len() == 1 => self.evaluate_literal(&literals[0]),
            PredicateOperator::In => literals
                .iter()
                .fold(FileIndexResult::Skip, |result, datum| {
                    result.or(self.evaluate_literal(datum))
                }),
            _ => FileIndexResult::Remain,
        }
    }
}

fn parse_option<T>(options: &Options, key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match options.get(key) {
        Some(value) => value.parse().map_err(|error| Error::ConfigInvalid {
            message: format!("Invalid Bloom filter option {key}={value}: {error}"),
        }),
        None => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        BigIntType, BinaryType, CharType, DateType, DoubleType, FloatType, IntType,
        LocalZonedTimestampType, SmallIntType, TimeType, TimestampType, TinyIntType, VarBinaryType,
        VarCharType,
    };

    fn options(items: &str, fpp: &str) -> Options {
        let mut options = Options::new();
        options.set(ITEMS, items);
        options.set(FPP, fpp);
        options
    }

    fn evaluate(
        reader: &BloomFilterReader,
        data_type: &DataType,
        operator: PredicateOperator,
        literals: &[Datum],
    ) -> FileIndexResult {
        reader.evaluate("field", 0, data_type, operator, literals)
    }

    #[test]
    fn test_java_golden_payload_and_predicates() {
        let data_type = DataType::BigInt(BigIntType::new());
        let mut writer =
            BloomFilterWriter::try_new(data_type.clone(), &options("10", "0.1")).unwrap();
        for datum in [
            Datum::Long(-1),
            Datum::Long(0),
            Datum::Long(1),
            Datum::Long(42),
        ] {
            writer.write(Some(&datum)).unwrap();
        }
        writer.write(None).unwrap();

        let serialized = writer.serialized_bytes().unwrap();
        assert_eq!(
            serialized.as_ref(),
            &hex::decode("00000003818281005001").unwrap()
        );

        let bitset_ptr = serialized[4..].as_ptr();
        let reader = BloomFilterReader::try_new(data_type.clone(), serialized).unwrap();
        assert_eq!(reader.filter.bytes().as_ptr(), bitset_ptr);
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::Eq,
                &[Datum::Long(42)]
            ),
            FileIndexResult::Remain
        );
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::Eq,
                &[Datum::Long(43)]
            ),
            FileIndexResult::Skip
        );
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::In,
                &[Datum::Long(43), Datum::Long(42)]
            ),
            FileIndexResult::Remain
        );
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::In,
                &[Datum::Long(43), Datum::Long(44)]
            ),
            FileIndexResult::Skip
        );
    }

    #[test]
    fn test_high_hash_count_uses_standard_big_endian_header() {
        let data_type = DataType::BigInt(BigIntType::new());
        let mut writer =
            BloomFilterWriter::try_new(data_type.clone(), &options("1", "2.938735877055719e-39"))
                .unwrap();
        writer.write(Some(&Datum::Long(42))).unwrap();

        let serialized = writer.serialized_bytes().unwrap();
        assert_eq!(&serialized[..4], &133_i32.to_be_bytes());
        assert_eq!(serialized.len(), 28);

        let reader = BloomFilterReader::try_new(data_type.clone(), serialized).unwrap();
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::Eq,
                &[Datum::Long(42)]
            ),
            FileIndexResult::Remain
        );
    }

    #[test]
    fn test_float_predicates_remain_conservative_for_signed_zero_and_nan() {
        let float_type = DataType::Float(FloatType::new());
        let mut writer =
            BloomFilterWriter::try_new(float_type.clone(), &options("10", "0.1")).unwrap();
        writer.write(Some(&Datum::Float(0.0))).unwrap();
        writer
            .write(Some(&Datum::Float(f32::from_bits(0x7fc0_0001))))
            .unwrap();
        let reader =
            BloomFilterReader::try_new(float_type.clone(), writer.serialized_bytes().unwrap())
                .unwrap();

        assert_eq!(
            evaluate(
                &reader,
                &float_type,
                PredicateOperator::Eq,
                &[Datum::Float(0.0)]
            ),
            FileIndexResult::Remain
        );
        assert_eq!(
            evaluate(
                &reader,
                &float_type,
                PredicateOperator::In,
                &[Datum::Float(123.0), Datum::Float(-0.0)]
            ),
            FileIndexResult::Remain
        );
        assert_eq!(
            evaluate(
                &reader,
                &float_type,
                PredicateOperator::Eq,
                &[Datum::Float(-0.0)]
            ),
            FileIndexResult::Remain
        );
        assert_eq!(
            evaluate(
                &reader,
                &float_type,
                PredicateOperator::Eq,
                &[Datum::Float(f32::from_bits(0xffc0_1234))]
            ),
            FileIndexResult::Remain
        );

        let double_type = DataType::Double(DoubleType::new());
        let mut writer =
            BloomFilterWriter::try_new(double_type.clone(), &options("10", "0.1")).unwrap();
        writer.write(Some(&Datum::Double(-0.0))).unwrap();
        let reader =
            BloomFilterReader::try_new(double_type.clone(), writer.serialized_bytes().unwrap())
                .unwrap();
        assert_eq!(
            evaluate(
                &reader,
                &double_type,
                PredicateOperator::Eq,
                &[Datum::Double(0.0)]
            ),
            FileIndexResult::Remain
        );
    }

    #[test]
    fn test_hash_compatible_schema_changes_keep_pruning() {
        let options = options("10", "0.1");

        let bigint = DataType::BigInt(BigIntType::new());
        let mut writer = BloomFilterWriter::try_new(bigint.clone(), &options).unwrap();
        writer.write(Some(&Datum::Long(42))).unwrap();
        let reader =
            BloomFilterReader::try_new(bigint, writer.serialized_bytes().unwrap()).unwrap();
        assert_eq!(
            evaluate(
                &reader,
                &DataType::BigInt(BigIntType::with_nullable(false)),
                PredicateOperator::Eq,
                &[Datum::Long(43)]
            ),
            FileIndexResult::Skip
        );

        let char_type = DataType::Char(CharType::with_nullable(false, 3).unwrap());
        let mut writer = BloomFilterWriter::try_new(char_type.clone(), &options).unwrap();
        writer
            .write(Some(&Datum::String("abc".to_string())))
            .unwrap();
        let reader =
            BloomFilterReader::try_new(char_type, writer.serialized_bytes().unwrap()).unwrap();
        assert_eq!(
            evaluate(
                &reader,
                &DataType::VarChar(VarCharType::new(200).unwrap()),
                PredicateOperator::Eq,
                &[Datum::String("zzz".to_string())]
            ),
            FileIndexResult::Skip
        );

        let timestamp4 = DataType::Timestamp(TimestampType::new(4).unwrap());
        let mut writer = BloomFilterWriter::try_new(timestamp4.clone(), &options).unwrap();
        writer
            .write(Some(&Datum::Timestamp {
                millis: 1_700_000_000_123,
                nanos: 456_000,
            }))
            .unwrap();
        let reader =
            BloomFilterReader::try_new(timestamp4, writer.serialized_bytes().unwrap()).unwrap();
        let missing = [Datum::Timestamp {
            millis: 1_700_000_000_124,
            nanos: 456_000,
        }];
        assert_eq!(
            evaluate(
                &reader,
                &DataType::Timestamp(TimestampType::with_nullable(false, 9).unwrap()),
                PredicateOperator::Eq,
                &missing
            ),
            FileIndexResult::Skip
        );
        assert_eq!(
            evaluate(
                &reader,
                &DataType::Timestamp(TimestampType::new(3).unwrap()),
                PredicateOperator::Eq,
                &missing
            ),
            FileIndexResult::Remain
        );
    }

    #[test]
    fn test_no_false_negative_for_supported_types() {
        let fixtures = [
            (
                DataType::Char(CharType::new(20).unwrap()),
                Datum::String("Paimon-派蒙".to_string()),
            ),
            (
                DataType::VarChar(VarCharType::new(20).unwrap()),
                Datum::String("Paimon-派蒙".to_string()),
            ),
            (
                DataType::Binary(BinaryType::new(4).unwrap()),
                Datum::Bytes(vec![0x00, 0x01, 0xfe, 0xff]),
            ),
            (
                DataType::VarBinary(VarBinaryType::new(10).unwrap()),
                Datum::Bytes(vec![]),
            ),
            (DataType::TinyInt(TinyIntType::new()), Datum::TinyInt(-128)),
            (
                DataType::SmallInt(SmallIntType::new()),
                Datum::SmallInt(-12_345),
            ),
            (DataType::Int(IntType::new()), Datum::Int(-123_456_789)),
            (
                DataType::BigInt(BigIntType::new()),
                Datum::Long(i64::MIN + 123),
            ),
            (
                DataType::Float(FloatType::new()),
                Datum::Float(f32::from_bits(0x7fa1_2345)),
            ),
            (
                DataType::Double(DoubleType::new()),
                Datum::Double(f64::from_bits(0x7ff1_2345_6789_abcd)),
            ),
            (DataType::Date(DateType::new()), Datum::Date(-1)),
            (
                DataType::Time(TimeType::new(3).unwrap()),
                Datum::Time(86_399_999),
            ),
            (
                DataType::Timestamp(TimestampType::new(3).unwrap()),
                Datum::Timestamp {
                    millis: -123_456_789,
                    nanos: 0,
                },
            ),
            (
                DataType::Timestamp(TimestampType::new(6).unwrap()),
                Datum::Timestamp {
                    millis: -1,
                    nanos: 999_000,
                },
            ),
            (
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
                Datum::LocalZonedTimestamp {
                    millis: 1_700_000_000_123,
                    nanos: 0,
                },
            ),
            (
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(6).unwrap()),
                Datum::LocalZonedTimestamp {
                    millis: 1_700_000_000_123,
                    nanos: 456_000,
                },
            ),
        ];

        for (data_type, datum) in fixtures {
            let mut writer =
                BloomFilterWriter::try_new(data_type.clone(), &options("10", "0.1")).unwrap();
            writer.write(Some(&datum)).unwrap();
            let reader =
                BloomFilterReader::try_new(data_type.clone(), writer.serialized_bytes().unwrap())
                    .unwrap();

            assert_eq!(
                evaluate(&reader, &data_type, PredicateOperator::Eq, &[datum]),
                FileIndexResult::Remain
            );
        }
    }

    #[test]
    fn test_fixed_sequence_has_no_false_negatives_and_bounded_fpp() {
        fn next_value(state: &mut u64) -> i64 {
            *state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            *state as i64
        }

        const ITEMS: usize = 5_000;
        let data_type = DataType::BigInt(BigIntType::new());
        let mut writer =
            BloomFilterWriter::try_new(data_type.clone(), &options("5000", "0.01")).unwrap();
        let mut state = 0x4d59_5df4_d0f3_3173;
        let inserted = (0..ITEMS)
            .map(|_| next_value(&mut state))
            .collect::<Vec<_>>();
        for value in &inserted {
            writer.write(Some(&Datum::Long(*value))).unwrap();
        }
        let reader =
            BloomFilterReader::try_new(data_type.clone(), writer.serialized_bytes().unwrap())
                .unwrap();

        for value in inserted {
            assert_eq!(
                evaluate(
                    &reader,
                    &data_type,
                    PredicateOperator::Eq,
                    &[Datum::Long(value)]
                ),
                FileIndexResult::Remain
            );
        }

        let false_positives = (0..ITEMS)
            .filter(|_| {
                let value = next_value(&mut state);
                evaluate(
                    &reader,
                    &data_type,
                    PredicateOperator::Eq,
                    &[Datum::Long(value)],
                ) == FileIndexResult::Remain
            })
            .count();
        assert!(
            false_positives <= ITEMS * 3 / 100,
            "false-positive rate exceeded 3%: {false_positives}/{ITEMS}"
        );
    }

    #[test]
    fn test_unsupported_predicates_and_invalid_literals_remain() {
        let data_type = DataType::BigInt(BigIntType::new());
        let mut writer =
            BloomFilterWriter::try_new(data_type.clone(), &options("10", "0.1")).unwrap();
        writer.write(Some(&Datum::Long(42))).unwrap();
        let reader =
            BloomFilterReader::try_new(data_type.clone(), writer.serialized_bytes().unwrap())
                .unwrap();

        for operator in [
            PredicateOperator::IsNull,
            PredicateOperator::IsNotNull,
            PredicateOperator::NotEq,
            PredicateOperator::Lt,
            PredicateOperator::LtEq,
            PredicateOperator::Gt,
            PredicateOperator::GtEq,
            PredicateOperator::NotIn,
            PredicateOperator::StartsWith,
            PredicateOperator::EndsWith,
            PredicateOperator::Contains,
            PredicateOperator::ArrayContains,
            PredicateOperator::ArraysOverlap,
            PredicateOperator::ArrayContainsAll,
            PredicateOperator::Like,
            PredicateOperator::Between,
            PredicateOperator::NotBetween,
        ] {
            assert_eq!(
                evaluate(&reader, &data_type, operator, &[Datum::Long(43)]),
                FileIndexResult::Remain
            );
        }
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::Eq, &[]),
            FileIndexResult::Remain
        );
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::Eq,
                &[Datum::Long(42), Datum::Long(43)]
            ),
            FileIndexResult::Remain
        );
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::In,
                &[Datum::Long(43), Datum::Int(44)]
            ),
            FileIndexResult::Remain
        );
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::In, &[]),
            FileIndexResult::Skip
        );
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::Eq,
                &[Datum::Int(43)]
            ),
            FileIndexResult::Remain
        );
        assert_eq!(
            evaluate(
                &reader,
                &DataType::Int(IntType::new()),
                PredicateOperator::Eq,
                &[Datum::Long(43)]
            ),
            FileIndexResult::Remain
        );
    }

    #[test]
    fn test_strict_config_validation() {
        let data_type = DataType::BigInt(BigIntType::new());
        for (items, fpp) in [
            ("0", "0.1"),
            ("-1", "0.1"),
            ("abc", "0.1"),
            ("2147483648", "0.1"),
            ("10", "0"),
            ("10", "1"),
            ("10", "-0.1"),
            ("10", "NaN"),
            ("10", "inf"),
            ("10", "abc"),
        ] {
            assert!(matches!(
                BloomFilterWriter::try_new(data_type.clone(), &options(items, fpp)),
                Err(Error::ConfigInvalid { .. })
            ));
        }
    }

    #[test]
    fn test_strict_payload_validation() {
        let data_type = DataType::BigInt(BigIntType::new());
        for payload in [
            &b""[..],
            &b"\0\0\0"[..],
            &b"\0\0\0\x01"[..],
            &b"\0\0\0\0\0"[..],
            &b"\xff\xff\xff\xff\0"[..],
            &b"\0\0\0\x09\0"[..],
        ] {
            assert!(matches!(
                BloomFilterReader::try_new(data_type.clone(), Bytes::copy_from_slice(payload)),
                Err(Error::FileIndexFormatInvalid { .. })
            ));
        }

        let mut high_hash_count = vec![0, 0, 0, 133];
        high_hash_count.extend_from_slice(&[0; 24]);
        let reader =
            BloomFilterReader::try_new(data_type.clone(), Bytes::from(high_hash_count)).unwrap();
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::Eq,
                &[Datum::Long(42)]
            ),
            FileIndexResult::Skip
        );
    }
}
