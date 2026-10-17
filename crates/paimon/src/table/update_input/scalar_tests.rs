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
use arrow_array::{Decimal128Array, Decimal256Array, FixedSizeBinaryArray};
use arrow_buffer::{Buffer, NullBuffer};
use arrow_schema::Field;

#[test]
fn fixed_binary_strings_keep_utf8_nulls_slices_and_empty_values() {
    let input: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [
                Some(b"old".as_slice()),
                Some(b"a\0b".as_slice()),
                None,
                Some("中".as_bytes()),
            ]
            .into_iter(),
            3,
        )
        .unwrap(),
    );
    let input = input.slice(1, 3);
    for target in [DataType::Utf8, DataType::LargeUtf8] {
        for mode in [CastMode::Assignment, CastMode::RowUpdate] {
            let actual = cast_update_value(&input, &target, mode).unwrap();
            let expected = arrow_cast::cast(
                &StringArray::from(vec![Some("a\0b"), None, Some("中")]),
                &target,
            )
            .unwrap();
            assert_eq!(actual.to_data(), expected.to_data());
        }
    }
    let empty: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_new(
            0,
            Buffer::from_vec(Vec::<u8>::new()),
            Some(NullBuffer::from(vec![true, false, true])),
        )
        .unwrap(),
    );
    assert_eq!(
        cast_update_value(&empty, &DataType::Utf8, CastMode::Assignment)
            .unwrap()
            .to_data(),
        StringArray::from(vec![Some(""), None, Some("")]).to_data()
    );
    let invalid: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter([b"\xffab".as_slice()].into_iter()).unwrap());
    assert!(cast_update_value(&invalid, &DataType::Utf8, CastMode::RowUpdate).is_err());
    // The adaptation is for text, not a new binary-to-numeric cast.
    let digits: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter([b"123".as_slice()].into_iter()).unwrap());
    assert!(cast_update_value(&digits, &DataType::Int32, CastMode::RowUpdate).is_err());
}

#[test]
fn half_float_text_uses_all_significant_digits() {
    let input = Float64Array::from(vec![
        Some(0.1),
        None,
        Some(1.5),
        Some(-3.25),
        Some(65504.0),
        Some(2_f64.powi(-24)),
    ]);
    let input = arrow_cast::cast(&input, &DataType::Float16).unwrap();
    let expected = StringArray::from(vec![
        Some("0.0999755859375"),
        None,
        Some("1.5"),
        Some("-3.25"),
        Some("65504"),
        Some("5.960464477539063e-8"),
    ]);
    for mode in [CastMode::Assignment, CastMode::RowUpdate] {
        assert_eq!(
            cast_update_value(&input, &DataType::Utf8, mode)
                .unwrap()
                .to_data(),
            expected.to_data()
        );
    }
}

#[test]
fn scalar_encodings_recurse_inside_containers() {
    let input: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_iter([b"abc".as_slice(), "中".as_bytes()].into_iter())
            .unwrap(),
    );
    let rows: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("s", input.data_type().clone(), true)),
        input.clone(),
    )]));
    let target = DataType::Struct(vec![Field::new("s", DataType::Utf8, true)].into());
    let output = cast_update_value(&rows, &target, CastMode::Assignment).unwrap();
    let expected = StringArray::from(vec!["abc", "中"]);
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0)
            .to_data(),
        expected.to_data()
    );
    let lists: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", input.data_type().clone(), true)),
        OffsetBuffer::from_lengths([2]),
        input,
        None,
    ));
    let target = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    let output = cast_update_value(&lists, &target, CastMode::Assignment).unwrap();
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .to_data(),
        expected.to_data()
    );
}

#[test]
fn wide_decimal_floating_values_use_java_big_decimal_rounding() {
    let coefficient = "1508079088755887087797858531882816082"
        .parse::<i128>()
        .unwrap();
    let source: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(coefficient), None, Some(-coefficient)])
            .with_precision_and_scale(38, 1)
            .unwrap(),
    );
    let expected = [
        Some(1.508079088755887e35),
        None,
        Some(-1.508079088755887e35),
    ];
    let wide: ArrayRef = Arc::new(
        Decimal256Array::from_iter(
            [Some(coefficient), None, Some(-coefficient)]
                .into_iter()
                .map(|value| value.map(arrow_buffer::i256::from_i128)),
        )
        .with_precision_and_scale(38, 1)
        .unwrap(),
    );
    for source in [source, wide] {
        for mode in [CastMode::Assignment, CastMode::RowUpdate] {
            let output = cast_update_value(&source, &DataType::Float64, mode).unwrap();
            assert_eq!(
                output.to_data(),
                Float64Array::from(expected.to_vec()).to_data()
            );
            let output = cast_update_value(&source, &DataType::Float32, mode).unwrap();
            assert_eq!(
                output.to_data(),
                Float32Array::from_iter(expected.into_iter().map(|v| v.map(|v| v as f32)))
                    .to_data()
            );
        }
    }
}

#[test]
fn compact_decimal_floating_values_keep_java_long_division() {
    let coefficient = 9_007_199_254_740_993_i128;
    let source: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(coefficient), None])
            .with_precision_and_scale(18, 3)
            .unwrap(),
    );
    let expected = coefficient as f64 / 1000.0;
    let output = cast_update_value(&source, &DataType::Float64, CastMode::Assignment).unwrap();
    assert_eq!(
        output.to_data(),
        Float64Array::from(vec![Some(expected), None]).to_data()
    );
    let output = cast_update_value(&source, &DataType::Float32, CastMode::Assignment).unwrap();
    assert_eq!(
        output.to_data(),
        Float32Array::from(vec![Some(expected as f32), None]).to_data()
    );
}
