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

//! Logical keys used by row-level matching, independent of Arrow encoding.

use arrow_array::{new_null_array, ArrayRef};
use arrow_schema::DataType;

use super::update_input::{cast_update_value, decode_dictionary, CastMode};
use super::write_batch_normalize::normalize_write_array;

/// Unify Arrow representations without changing key identity. Dictionary
/// indices are batch-local; numeric widths, decimal scales and temporal units
/// may differ only when conversion is exact. Binary keys compare their bytes.
/// Callers validate logical type families and decide whether NULL keys match.
/// Payload write rules are separate.
pub(super) fn normalize_match_key(
    array: &ArrayRef,
    expected: &DataType,
) -> crate::Result<ArrayRef> {
    let decoded = decode_dictionary(array)?;
    if decoded.data_type() == &DataType::Null {
        return Ok(new_null_array(expected, decoded.len()));
    }
    let source = decoded.data_type();
    if source == expected {
        return Ok(decoded);
    }
    if source.is_floating() && expected.is_floating() {
        return cast_float_key(&decoded, expected);
    }
    let binary = |t: &DataType| {
        matches!(
            t,
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_)
        )
    };
    if binary(source) && binary(expected) {
        if source == &DataType::LargeBinary && expected == &DataType::Binary {
            return super::write_batch_normalize::normalize_large_binary(
                decoded.as_any().downcast_ref().unwrap(),
            );
        }
        // Key identity is its bytes, independently of Arrow offset width.
        // Ordinary append validation still sees the original source array.
        let values = if matches!(source, DataType::FixedSizeBinary(_)) {
            normalize_write_array(&decoded, &DataType::Binary)?
        } else {
            decoded.clone()
        };
        return arrow_cast::cast_with_options(
            values.as_ref(),
            expected,
            &arrow_cast::CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .map_err(|error| crate::Error::DataInvalid {
            message: error.to_string(),
            source: None,
        });
    }
    let exact_family = source.is_integer() && expected.is_integer()
        || source.is_decimal() && expected.is_decimal()
        || matches!(
            (source, expected),
            (
                DataType::Date32 | DataType::Date64,
                DataType::Date32 | DataType::Date64
            ) | (
                DataType::Time32(_) | DataType::Time64(_),
                DataType::Time32(_) | DataType::Time64(_)
            )
        )
        || matches!((source, expected), (DataType::Timestamp(_, from), DataType::Timestamp(_, to))
            if from.is_some() == to.is_some());
    if exact_family {
        cast_update_value(&decoded, expected, CastMode::Assignment)
    } else {
        normalize_write_array(&decoded, expected)
    }
}

fn cast_float_key(array: &ArrayRef, expected: &DataType) -> crate::Result<ArrayRef> {
    let options = arrow_cast::CastOptions {
        safe: false,
        ..Default::default()
    };
    let cast = |array: &ArrayRef, target| {
        arrow_cast::cast_with_options(array.as_ref(), target, &options).map_err(|error| {
            crate::Error::DataInvalid {
                message: error.to_string(),
                source: None,
            }
        })
    };
    let converted = cast(array, expected)?;
    let restored = cast(&converted, array.data_type())?;
    if restored.to_data() != array.to_data() {
        return Err(crate::Error::DataInvalid {
            message: "Floating match key conversion would change key identity".into(),
            source: None,
        });
    }
    Ok(converted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Int8Type;
    use arrow_array::{
        Array, BinaryArray, Decimal128Array, DictionaryArray, FixedSizeBinaryArray, Float32Array,
        Float64Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray,
    };
    use arrow_schema::TimeUnit;
    use std::sync::Arc;

    #[test]
    fn dictionary_keys_use_value_validity_as_well_as_index_validity() {
        let input: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(0), None, Some(1), Some(0)]),
                Arc::new(StringArray::from(vec![None, Some("key")])),
            )
            .unwrap(),
        );
        let output = normalize_match_key(&input, &DataType::Utf8).unwrap();
        assert_eq!(
            output.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec![None, None, Some("key"), None])
        );
    }

    #[test]
    fn integer_keys_are_exact_and_keep_nulls() {
        let input: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(-1),
            None,
            Some(i32::MAX as i64),
        ]));
        let output = normalize_match_key(&input, &DataType::Int32).unwrap();
        assert_eq!(
            output.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(-1), None, Some(i32::MAX)])
        );
        let overflow: ArrayRef = Arc::new(Int64Array::from(vec![i32::MAX as i64 + 1]));
        assert!(normalize_match_key(&overflow, &DataType::Int32).is_err());
        let text: ArrayRef = Arc::new(StringArray::from(vec!["42"]));
        assert!(normalize_match_key(&text, &DataType::Int32).is_err());
    }

    #[test]
    fn floating_keys_change_width_only_when_identity_is_preserved() {
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(99.0),
            Some(1.5),
            None,
            Some(-2.25),
        ]));
        let input = input.slice(1, 3);
        let narrow = normalize_match_key(&input, &DataType::Float32).unwrap();
        assert_eq!(
            narrow.as_any().downcast_ref::<Float32Array>().unwrap(),
            &Float32Array::from(vec![Some(1.5), None, Some(-2.25)])
        );
        assert_eq!(
            normalize_match_key(&narrow, &DataType::Float64)
                .unwrap()
                .to_data(),
            input.to_data()
        );
        for value in [1.00000001, f64::MAX, f64::MIN_POSITIVE] {
            let lossy: ArrayRef = Arc::new(Float64Array::from(vec![value]));
            assert!(normalize_match_key(&lossy, &DataType::Float32).is_err());
        }
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(0), None, Some(1), Some(0)]),
                Arc::new(Float32Array::from(vec![Some(1.5), None])),
            )
            .unwrap(),
        );
        let decoded = normalize_match_key(&dictionary, &DataType::Float64).unwrap();
        assert_eq!(
            decoded.as_any().downcast_ref::<Float64Array>().unwrap(),
            &Float64Array::from(vec![Some(1.5), None, None, Some(1.5)])
        );
    }

    #[test]
    fn binary_keys_compare_bytes_across_offsets_and_dictionaries() {
        let expected =
            BinaryArray::from(vec![Some(b"\xff\0".as_slice()), None, Some(b"".as_slice())]);
        let large: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
            Some(b"padding".as_slice()),
            Some(b"\xff\0".as_slice()),
            None,
            Some(b"".as_slice()),
        ]));
        let output = normalize_match_key(&large.slice(1, 3), &DataType::Binary).unwrap();
        assert_eq!(
            output.as_any().downcast_ref::<BinaryArray>().unwrap(),
            &expected
        );
        let restored = normalize_match_key(&output, &DataType::LargeBinary).unwrap();
        assert_eq!(restored.to_data(), large.slice(1, 3).to_data());

        let fixed: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some(b"ab".as_slice()), None, Some(b"cd".as_slice())].into_iter(),
                2,
            )
            .unwrap(),
        );
        for target in [DataType::Binary, DataType::LargeBinary] {
            let normalized = normalize_match_key(&fixed.slice(1, 2), &target).unwrap();
            assert_eq!(
                normalized.to_data(),
                arrow_cast::cast(&fixed.slice(1, 2), &target)
                    .unwrap()
                    .to_data()
            );
        }
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(0), None, Some(1)]),
                Arc::new(LargeBinaryArray::from(vec![Some(b"ab".as_slice()), None])),
            )
            .unwrap(),
        );
        let output = normalize_match_key(&dictionary, &DataType::Binary).unwrap();
        assert_eq!(
            output.as_any().downcast_ref::<BinaryArray>().unwrap(),
            &BinaryArray::from(vec![Some(b"ab".as_slice()), None, None])
        );
    }

    #[test]
    fn decimal_keys_cannot_round_into_existing_keys() {
        let array = |value| {
            Arc::new(
                Decimal128Array::from(vec![value])
                    .with_precision_and_scale(15, 4)
                    .unwrap(),
            ) as ArrayRef
        };
        let expected = DataType::Decimal128(10, 2);
        let output = normalize_match_key(&array(12500), &expected).unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            125
        );
        assert!(normalize_match_key(&array(12501), &expected).is_err());
    }

    #[test]
    fn decimal_zero_and_null_keys_rescale_across_large_scale_differences() {
        for (from, to) in [(-2, 38), (38, -2), (-90, 38), (-128, 38), (38, -128)] {
            let input: ArrayRef = Arc::new(
                Decimal128Array::from(vec![Some(0), None])
                    .with_precision_and_scale(38, from)
                    .unwrap(),
            );
            let expected = DataType::Decimal128(38, to);
            let output = normalize_match_key(&input, &expected).unwrap();
            assert_eq!(output.data_type(), &expected);
            assert_eq!(
                output.as_any().downcast_ref::<Decimal128Array>().unwrap(),
                &Decimal128Array::from(vec![Some(0), None])
                    .with_precision_and_scale(38, to)
                    .unwrap()
            );
            let nonzero: ArrayRef = Arc::new(
                Decimal128Array::from(vec![1])
                    .with_precision_and_scale(38, from)
                    .unwrap(),
            );
            assert!(normalize_match_key(&nonzero, &expected).is_err());
        }
    }

    #[test]
    fn temporal_keys_require_lossless_units_and_preserve_instant() {
        let input: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(-1000),
            None,
            Some(2000),
        ]));
        let output =
            normalize_match_key(&input, &DataType::Timestamp(TimeUnit::Millisecond, None)).unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap(),
            &TimestampMillisecondArray::from(vec![Some(-1), None, Some(2)])
        );
        let fractional: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![-1001]));
        assert!(normalize_match_key(
            &fractional,
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        )
        .is_err());
        let local: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![0]).with_timezone("America/New_York"));
        let output = normalize_match_key(
            &local,
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        )
        .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            0
        );
        assert!(
            normalize_match_key(&local, &DataType::Timestamp(TimeUnit::Microsecond, None)).is_err()
        );
    }
}
