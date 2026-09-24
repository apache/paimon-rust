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

use super::*;
use crate::spec::{
    BigIntType, BooleanType, DateType, DecimalType, IntType, LocalZonedTimestampType, SmallIntType,
    TimeType, TimestampType, TinyIntType,
};
use base64::Engine;

fn int_type() -> DataType {
    DataType::Int(IntType::new())
}

fn int_reader(values: &[Option<i32>]) -> BsiFileIndexReader {
    let mut writer = BsiFileIndexWriter::try_new(int_type(), &Options::new()).unwrap();
    for value in values {
        let datum = value.map(Datum::Int);
        writer.write(datum.as_ref()).unwrap();
    }
    BsiFileIndexReader::try_new(int_type(), writer.serialized_bytes().unwrap()).unwrap()
}

fn selection(rows: impl IntoIterator<Item = u32>) -> FileIndexResult {
    FileIndexResult::Selection(rows.into_iter().collect())
}

fn eval(
    reader: &BsiFileIndexReader,
    operator: PredicateOperator,
    literals: &[i32],
) -> FileIndexResult {
    reader.evaluate(
        "value",
        0,
        &int_type(),
        operator,
        &literals.iter().copied().map(Datum::Int).collect::<Vec<_>>(),
    )
}

#[test]
fn test_bsi_signs_nulls_and_comparisons() {
    let reader = int_reader(&[None, Some(-5), Some(-2), Some(0), Some(3), Some(8), None]);
    use PredicateOperator::*;
    assert_eq!(eval(&reader, IsNull, &[]), selection([0, 6]));
    assert_eq!(eval(&reader, IsNotNull, &[]), selection([1, 2, 3, 4, 5]));
    assert_eq!(eval(&reader, Eq, &[-2]), selection([2]));
    assert_eq!(eval(&reader, Eq, &[0]), selection([3]));
    assert_eq!(eval(&reader, Eq, &[3]), selection([4]));
    assert_eq!(eval(&reader, Eq, &[17]), selection([]));
    assert_eq!(eval(&reader, NotEq, &[-2]), selection([1, 3, 4, 5]));
    assert_eq!(eval(&reader, Lt, &[-2]), selection([1]));
    assert_eq!(eval(&reader, LtEq, &[-2]), selection([1, 2]));
    assert_eq!(eval(&reader, Gt, &[-2]), selection([3, 4, 5]));
    assert_eq!(eval(&reader, GtEq, &[-2]), selection([2, 3, 4, 5]));
    assert_eq!(eval(&reader, Lt, &[0]), selection([1, 2]));
    assert_eq!(eval(&reader, LtEq, &[0]), selection([1, 2, 3]));
    assert_eq!(eval(&reader, Gt, &[0]), selection([4, 5]));
    assert_eq!(eval(&reader, GtEq, &[0]), selection([3, 4, 5]));
    assert_eq!(eval(&reader, Lt, &[9]), selection([1, 2, 3, 4, 5]));
    assert_eq!(eval(&reader, Gt, &[9]), selection([]));
    assert_eq!(eval(&reader, LtEq, &[-9]), selection([]));
    assert_eq!(eval(&reader, GtEq, &[-9]), selection([1, 2, 3, 4, 5]));
    assert_eq!(eval(&reader, In, &[-5, 8, 20]), selection([1, 5]));
    assert_eq!(eval(&reader, NotIn, &[-5, 8]), selection([2, 3, 4]));
    assert_eq!(eval(&reader, Between, &[-2, 3]), selection([2, 3, 4]));
    assert_eq!(eval(&reader, NotBetween, &[-2, 3]), FileIndexResult::Remain);
}

#[test]
fn test_bsi_matches_scalar_for_varied_signed_values() {
    use PredicateOperator::*;
    let values = [
        None,
        Some(-32768),
        Some(-256),
        Some(-65),
        Some(-8),
        Some(-1),
        Some(0),
        Some(1),
        Some(7),
        Some(64),
        Some(255),
        Some(32767),
    ];
    let reader = int_reader(&values);
    for literal in [-50000, -32768, -255, -8, -1, 0, 1, 8, 64, 255, 32767, 50000] {
        for operator in [Eq, NotEq, Lt, LtEq, Gt, GtEq] {
            let expected: RoaringBitmap = values
                .iter()
                .enumerate()
                .filter_map(|(row, value)| {
                    value.and_then(|value| {
                        let matches = match operator {
                            Eq => value == literal,
                            NotEq => value != literal,
                            Lt => value < literal,
                            LtEq => value <= literal,
                            Gt => value > literal,
                            GtEq => value >= literal,
                            _ => unreachable!(),
                        };
                        matches.then_some(row as u32)
                    })
                })
                .collect();
            assert_eq!(
                eval(&reader, operator, &[literal]),
                FileIndexResult::Selection(expected),
                "{operator:?} {literal}"
            );
        }
    }
}

#[test]
fn test_bsi_empty_and_all_null_files() {
    for values in [&[][..], &[None, None][..]] {
        let reader = int_reader(values);
        assert_eq!(
            eval(&reader, PredicateOperator::IsNull, &[]),
            selection(0..values.len() as u32)
        );
        assert_eq!(
            eval(&reader, PredicateOperator::IsNotNull, &[]),
            selection([])
        );
        assert_eq!(eval(&reader, PredicateOperator::Eq, &[1]), selection([]));
    }
}

#[test]
fn test_bsi_writer_rejects_unrepresentable_values_and_types() {
    assert!(matches!(
        BsiFileIndexWriter::try_new(DataType::Boolean(BooleanType::new()), &Options::new()),
        Err(Error::Unsupported { .. })
    ));
    let mut wide_decimal = BsiFileIndexWriter::try_new(
        DataType::Decimal(DecimalType::new(38, 2).unwrap()),
        &Options::new(),
    )
    .unwrap();
    assert!(matches!(
        wide_decimal.write(Some(&Datum::Decimal {
            unscaled: i128::MAX,
            precision: 38,
            scale: 2,
        })),
        Err(Error::DataInvalid { .. })
    ));
    let mut writer = BsiFileIndexWriter::try_new(int_type(), &Options::new()).unwrap();
    assert!(matches!(
        writer.write(Some(&Datum::Long(1))),
        Err(Error::DataInvalid { .. })
    ));
    assert!(writer.empty());
    let mut long = BsiFileIndexWriter::try_new(
        DataType::BigInt(crate::spec::BigIntType::new()),
        &Options::new(),
    )
    .unwrap();
    assert!(matches!(
        long.write(Some(&Datum::Long(i64::MIN))),
        Err(Error::DataInvalid { .. })
    ));
}

#[test]
fn test_bsi_decimal_and_timestamp_mapping() {
    let decimal_type = DataType::Decimal(DecimalType::new(12, 2).unwrap());
    let mut writer = BsiFileIndexWriter::try_new(decimal_type.clone(), &Options::new()).unwrap();
    for value in [-500_i128, 0, 275] {
        writer
            .write(Some(&Datum::Decimal {
                unscaled: value,
                precision: 12,
                scale: 2,
            }))
            .unwrap();
    }
    let reader =
        BsiFileIndexReader::try_new(decimal_type.clone(), writer.serialized_bytes().unwrap())
            .unwrap();
    assert_eq!(
        reader.evaluate(
            "amount",
            0,
            &decimal_type,
            PredicateOperator::Lt,
            &[Datum::Decimal {
                unscaled: 100,
                precision: 12,
                scale: 2
            }]
        ),
        selection([0, 1])
    );
    let timestamp_type = DataType::Timestamp(TimestampType::new(9).unwrap());
    let mut writer = BsiFileIndexWriter::try_new(timestamp_type.clone(), &Options::new()).unwrap();
    writer
        .write(Some(&Datum::Timestamp {
            millis: 1000,
            nanos: 123_456,
        }))
        .unwrap();
    writer.write(None).unwrap();
    let reader =
        BsiFileIndexReader::try_new(timestamp_type.clone(), writer.serialized_bytes().unwrap())
            .unwrap();
    assert_eq!(
        reader.evaluate("ts", 0, &timestamp_type, PredicateOperator::IsNull, &[]),
        selection([1])
    );
    assert_eq!(
        reader.evaluate(
            "ts",
            0,
            &timestamp_type,
            PredicateOperator::Eq,
            &[Datum::Timestamp {
                millis: 1000,
                nanos: 123_456
            }]
        ),
        FileIndexResult::Remain
    );

    let mut invalid_timestamp =
        BsiFileIndexWriter::try_new(timestamp_type, &Options::new()).unwrap();
    assert!(matches!(
        invalid_timestamp.write(Some(&Datum::Timestamp {
            millis: 1000,
            nanos: 1_000_000,
        })),
        Err(Error::DataInvalid { .. })
    ));
    assert!(invalid_timestamp.empty());
}

#[test]
fn test_bsi_rejects_bad_headers_without_pruning() {
    let mut writer = BsiFileIndexWriter::try_new(int_type(), &Options::new()).unwrap();
    for value in [-2, 0, 3] {
        writer.write(Some(&Datum::Int(value))).unwrap();
    }
    let bytes = writer.serialized_bytes().unwrap();
    for length in 0..bytes.len() {
        assert!(
            BsiFileIndexReader::try_new(int_type(), bytes.slice(..length)).is_err(),
            "truncated length {length}"
        );
    }
    let mut bad_version = bytes.to_vec();
    bad_version[0] = 2;
    assert!(BsiFileIndexReader::try_new(int_type(), Bytes::from(bad_version)).is_err());
    let mut bad_row_count = bytes.to_vec();
    bad_row_count[1..5].copy_from_slice(&(-1_i32).to_be_bytes());
    assert!(BsiFileIndexReader::try_new(int_type(), Bytes::from(bad_row_count)).is_err());
    let mut trailing = bytes.to_vec();
    trailing.push(1);
    assert!(BsiFileIndexReader::try_new(int_type(), Bytes::from(trailing)).is_err());
}

#[test]
fn test_bsi_java_v1_fixture_round_trip() {
    // BitSliceIndexBitmapFileIndex.Writer in Java Paimon, for
    // [null, -5, -2, 0, 3, 8, null] as INT. Keep a genuine Java payload here
    // so both the outer framing and Roaring bitmap encoding are checked.
    let java_bytes = base64::engine::general_purpose::STANDARD
        .decode(concat!(
            "AQAAAAcBAQAAAAAAAAAAAAAAAAAAAAg6MAAAAQAAAAAAAgAQAAAAAwAEAAUAAAAABDowAAABAAAAAAAAABAAAAAEADowAAAB",
            "AAAAAAAAABAAAAAEADowAAAAAAAAOjAAAAEAAAAAAAAAEAAAAAUAAQEAAAAAAAAAAAAAAAAAAAAFOjAAAAEAAAAAAAEAEAAAAAEAAgAAAAADOjAAAAEAAAAAAAAAEAAAAAEAOjAAAAEAAAAAAAAAEAAAAAIAOjAAAAEAAAAAAAAAEAAAAAEA"
        ))
        .unwrap();
    let reader = BsiFileIndexReader::try_new(int_type(), Bytes::from(java_bytes.clone())).unwrap();
    assert_eq!(eval(&reader, PredicateOperator::Eq, &[-5]), selection([1]));
    assert_eq!(
        eval(&reader, PredicateOperator::Gt, &[0]),
        selection([4, 5])
    );
    assert_eq!(
        eval(&reader, PredicateOperator::IsNull, &[]),
        selection([0, 6])
    );
    let mut writer = BsiFileIndexWriter::try_new(int_type(), &Options::new()).unwrap();
    for value in [None, Some(-5), Some(-2), Some(0), Some(3), Some(8), None] {
        let datum = value.map(Datum::Int);
        writer.write(datum.as_ref()).unwrap();
    }
    assert_eq!(writer.serialized_bytes().unwrap().as_ref(), java_bytes);
}

#[test]
fn test_bsi_all_java_numeric_date_time_value_mappers() {
    let cases: Vec<(DataType, Datum, Datum)> = vec![
        (
            DataType::TinyInt(TinyIntType::new()),
            Datum::TinyInt(-5),
            Datum::TinyInt(3),
        ),
        (
            DataType::SmallInt(SmallIntType::new()),
            Datum::SmallInt(-500),
            Datum::SmallInt(300),
        ),
        (int_type(), Datum::Int(-50_000), Datum::Int(30_000)),
        (
            DataType::BigInt(BigIntType::new()),
            Datum::Long(-5_000_000_000),
            Datum::Long(3_000_000_000),
        ),
        (
            DataType::Date(DateType::new()),
            Datum::Date(-100),
            Datum::Date(20_000),
        ),
        (
            DataType::Time(TimeType::new(3).unwrap()),
            Datum::Time(1),
            Datum::Time(86_000_000),
        ),
        (
            DataType::Decimal(DecimalType::new(38, 2).unwrap()),
            Datum::Decimal {
                unscaled: -500,
                precision: 38,
                scale: 2,
            },
            Datum::Decimal {
                unscaled: 300,
                precision: 38,
                scale: 2,
            },
        ),
        (
            DataType::Timestamp(TimestampType::new(6).unwrap()),
            Datum::Timestamp {
                millis: -1000,
                nanos: 0,
            },
            Datum::Timestamp {
                millis: 1000,
                nanos: 123_000,
            },
        ),
        (
            DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
            Datum::LocalZonedTimestamp {
                millis: -1000,
                nanos: 0,
            },
            Datum::LocalZonedTimestamp {
                millis: 1000,
                nanos: 0,
            },
        ),
    ];
    for (data_type, negative, positive) in cases {
        let mut writer = BsiFileIndexWriter::try_new(data_type.clone(), &Options::new()).unwrap();
        writer.write(Some(&negative)).unwrap();
        writer.write(None).unwrap();
        writer.write(Some(&positive)).unwrap();
        let bytes = writer.serialized_bytes().unwrap();
        let reader = BsiFileIndexReader::try_new(data_type.clone(), bytes).unwrap();
        let compare =
            |operator, literal: Datum| reader.evaluate("v", 0, &data_type, operator, &[literal]);
        assert_eq!(
            compare(PredicateOperator::Eq, negative.clone()),
            selection([0])
        );
        assert_eq!(
            compare(PredicateOperator::Eq, positive.clone()),
            selection([2])
        );
        assert_eq!(
            compare(PredicateOperator::Lt, positive.clone()),
            selection([0])
        );
        assert_eq!(
            compare(PredicateOperator::Gt, negative.clone()),
            selection([2])
        );
        assert_eq!(
            reader.evaluate("v", 0, &data_type, PredicateOperator::IsNull, &[]),
            selection([1])
        );
        assert_eq!(
            reader.evaluate("v", 0, &data_type, PredicateOperator::NotIn, &[negative]),
            selection([2])
        );
    }
}

#[test]
fn test_bsi_reader_rejects_inconsistent_bitmaps() {
    let mut writer = BsiFileIndexWriter::try_new(int_type(), &Options::new()).unwrap();
    writer.write(Some(&Datum::Int(3))).unwrap();
    writer.write(Some(&Datum::Int(-2))).unwrap();
    let good = writer.serialized_bytes().unwrap();
    let mut bad_flag = good.to_vec();
    bad_flag[5] = 9;
    assert!(BsiFileIndexReader::try_new(int_type(), Bytes::from(bad_flag)).is_err());

    let mut bad_max = good.to_vec();
    // Outer header (version, row count, positive flag), then slice version,
    // min (8 bytes), max (8 bytes). Set max below the encoded slice width.
    bad_max[15..23].copy_from_slice(&0_i64.to_be_bytes());
    assert!(BsiFileIndexReader::try_new(int_type(), Bytes::from(bad_max)).is_err());

    let mut bad_min = good.to_vec();
    bad_min[7..15].copy_from_slice(&(-1_i64).to_be_bytes());
    assert!(BsiFileIndexReader::try_new(int_type(), Bytes::from(bad_min)).is_err());

    let mut bad_count = good.to_vec();
    bad_count[1..5].copy_from_slice(&1_i32.to_be_bytes());
    assert!(BsiFileIndexReader::try_new(int_type(), Bytes::from(bad_count)).is_err());
}

#[test]
fn test_bsi_bigint_extremes_and_min_literal() {
    let data_type = DataType::BigInt(BigIntType::new());
    let mut writer = BsiFileIndexWriter::try_new(data_type.clone(), &Options::new()).unwrap();
    for value in [
        Some(-i64::MAX),
        Some(-1),
        None,
        Some(0),
        Some(1),
        Some(i64::MAX),
    ] {
        let datum = value.map(Datum::Long);
        writer.write(datum.as_ref()).unwrap();
    }
    let reader =
        BsiFileIndexReader::try_new(data_type.clone(), writer.serialized_bytes().unwrap()).unwrap();
    let evaluate = |operator, values: Vec<i64>| {
        reader.evaluate(
            "v",
            0,
            &data_type,
            operator,
            &values.into_iter().map(Datum::Long).collect::<Vec<_>>(),
        )
    };
    assert_eq!(
        evaluate(PredicateOperator::Eq, vec![-i64::MAX]),
        selection([0])
    );
    assert_eq!(
        evaluate(PredicateOperator::Eq, vec![i64::MAX]),
        selection([5])
    );
    assert_eq!(
        evaluate(PredicateOperator::Eq, vec![i64::MIN]),
        selection([])
    );
    assert_eq!(
        evaluate(PredicateOperator::NotEq, vec![i64::MIN]),
        selection([0, 1, 3, 4, 5])
    );
    assert_eq!(
        evaluate(PredicateOperator::LtEq, vec![i64::MIN]),
        selection([])
    );
    assert_eq!(
        evaluate(PredicateOperator::Gt, vec![i64::MIN]),
        selection([0, 1, 3, 4, 5])
    );
    assert_eq!(evaluate(PredicateOperator::In, vec![]), selection([]));
    assert_eq!(
        evaluate(PredicateOperator::NotIn, vec![]),
        selection([0, 1, 3, 4, 5])
    );
    assert_eq!(
        evaluate(PredicateOperator::Between, vec![-1, 1]),
        selection([1, 3, 4])
    );
    assert_eq!(
        reader.evaluate("v", 0, &data_type, PredicateOperator::Eq, &[Datum::Int(1)],),
        FileIndexResult::Remain,
    );
}
