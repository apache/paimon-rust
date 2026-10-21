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

//! Exact DECIMAL input rescaling shared by updates and logical match keys.

use arrow_array::{cast::AsArray, Array, ArrayRef, Decimal128Array, Float32Array, Float64Array};
use arrow_buffer::i256;
use arrow_schema::DataType;
use std::sync::Arc;

use super::{invalid, parse_decimal_text};

fn precision_and_scale(array: &ArrayRef) -> (u8, i8) {
    match array.data_type() {
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => (*p, *s),
        _ => unreachable!("validated DECIMAL input"),
    }
}

fn map_decimal<T>(
    array: &ArrayRef,
    parse: impl Fn(&str) -> crate::Result<T>,
) -> crate::Result<Vec<Option<T>>> {
    let (_, scale) = precision_and_scale(array);
    map_coefficients(array, |value| {
        parse(&format!("{value}e{}", -i16::from(scale)))
    })
}

fn map_coefficients<T>(
    array: &ArrayRef,
    convert: impl Fn(i256) -> crate::Result<T>,
) -> crate::Result<Vec<Option<T>>> {
    // Preserve the full coefficient. A generic display can clip digits to the
    // declared source precision for foreign buffers containing more digits.
    macro_rules! values {
        ($ty:ty, $widen:expr) => {
            array
                .as_primitive::<$ty>()
                .iter()
                .map(|value| value.map(|value| convert(($widen)(value))).transpose())
                .collect()
        };
    }
    match array.data_type() {
        DataType::Decimal32(..) => values!(arrow_array::types::Decimal32Type, |v| i256::from_i128(
            v as i128
        )),
        DataType::Decimal64(..) => values!(arrow_array::types::Decimal64Type, |v| i256::from_i128(
            v as i128
        )),
        DataType::Decimal128(..) => values!(arrow_array::types::Decimal128Type, i256::from_i128),
        DataType::Decimal256(..) => values!(arrow_array::types::Decimal256Type, |v| v),
        _ => unreachable!("validated DECIMAL input"),
    }
}

pub(super) fn cast_to_floating(array: &ArrayRef, target: &DataType) -> crate::Result<ArrayRef> {
    let (precision, scale) = precision_and_scale(array);
    if precision <= 38 && scale >= 0 && scale as u8 <= precision {
        let canonical = cast_exact_decimal(array, &DataType::Decimal128(precision, scale))?;
        return crate::arrow::cast_decimal_to_floating(
            &canonical,
            u32::from(precision),
            scale as u32,
            target,
        );
    }
    // External Arrow decimals can exceed Paimon's logical precision or use a
    // negative scale. They still update FLOAT/DOUBLE columns via the exact
    // decimal value, without first rounding an oversized coefficient to f64.
    let values = map_decimal(array, |text| {
        text.parse::<f64>()
            .map_err(|_| invalid("Invalid decimal floating assignment"))
    })?;
    Ok(match target {
        DataType::Float32 => Arc::new(Float32Array::from_iter(
            values.into_iter().map(|v| v.map(|v| v as f32)),
        )) as ArrayRef,
        DataType::Float64 => Arc::new(Float64Array::from(values)),
        _ => unreachable!("validated DECIMAL floating target"),
    })
}

/// Extract the integral value before narrowing its physical width. Generic
/// Arrow kernels build 10^scale even for zero, overflowing at extreme external
/// scales. Row-ID constructors truncate fractions; evaluated assignments and
/// match keys require exact values. Both contracts still reject width overflow.
pub(super) fn cast_to_integer(
    array: &ArrayRef,
    target: &DataType,
    truncate: bool,
) -> crate::Result<ArrayRef> {
    let (_, scale) = precision_and_scale(array);
    // Compute the scale factor once per array, without formatting each value
    // or overflowing the narrower DECIMAL32/64/128 physical coefficient.
    let power = i256::from_i128(10).checked_pow(u32::from(scale.unsigned_abs()));
    let overflow = || invalid("Decimal assignment exceeds target integer range");
    let values = map_coefficients(array, |value| {
        if value == i256::ZERO {
            return Ok(0);
        }
        let integral = if scale > 0 {
            let Some(power) = power else {
                return if truncate {
                    Ok(0)
                } else {
                    Err(invalid("Decimal assignment would lose scale precision"))
                };
            };
            if !truncate && value % power != i256::ZERO {
                return Err(invalid("Decimal assignment would lose scale precision"));
            }
            value / power
        } else {
            value
                .checked_mul(power.ok_or_else(overflow)?)
                .ok_or_else(overflow)?
        };
        let value = integral.to_i128().ok_or_else(overflow)?;
        if value.unsigned_abs() >= 10_u128.pow(38) {
            return Err(overflow());
        }
        Ok(value)
    })?;
    let integral = Decimal128Array::from(values)
        .with_precision_and_scale(38, 0)
        .map_err(|error| invalid(error.to_string()))?;
    let options = arrow_cast::CastOptions {
        safe: false,
        ..Default::default()
    };
    let converted = arrow_cast::cast_with_options(&integral, target, &options)
        .map_err(|error| invalid(error.to_string()))?;
    let restored =
        arrow_cast::cast_with_options(converted.as_ref(), integral.data_type(), &options)
            .map_err(|error| invalid(error.to_string()))?;
    if restored.to_data() != integral.to_data() {
        return Err(invalid("Decimal assignment exceeds target integer range"));
    }
    Ok(converted)
}

pub(super) fn cast_exact_decimal(array: &ArrayRef, target: &DataType) -> crate::Result<ArrayRef> {
    let (_, source_scale) = precision_and_scale(array);
    let DataType::Decimal128(target_precision, target_scale) = target else {
        unreachable!("validated DECIMAL target")
    };
    let text_fallback = || {
        // A zero or NULL can cross scales beyond the rescaler's power table.
        // The exact text parser still rejects fractional loss and overflow.
        let values = map_decimal(array, |text| {
            parse_decimal_text(text, *target_precision, *target_scale)
        })?;
        Decimal128Array::from(values)
            .with_precision_and_scale(*target_precision, *target_scale)
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|error| invalid(error.to_string()))
    };
    let difference = i16::from(*target_scale) - i16::from(source_scale);
    if !(-38..=38).contains(&difference) {
        // Arrow subtracts scales in i8 and uses a Decimal128 power table.
        // Check before entering the kernel, including its reverse cast.
        return text_fallback();
    }
    let options = arrow_cast::CastOptions {
        safe: false,
        ..Default::default()
    };
    let exact = || {
        let converted = arrow_cast::cast_with_options(array.as_ref(), target, &options)
            .map_err(|error| invalid(error.to_string()))?;
        let restored =
            arrow_cast::cast_with_options(converted.as_ref(), array.data_type(), &options)
                .map_err(|error| invalid(error.to_string()))?;
        if restored.to_data() != array.to_data() {
            return Err(invalid("Decimal assignment would lose scale precision"));
        }
        Ok(converted)
    };
    exact().or_else(|_| text_fallback())
}

#[cfg(test)]
mod tests {
    use super::super::{cast_update_value, CastMode};
    use super::*;
    use arrow_array::{Array, Decimal128Array, Decimal256Array, Decimal32Array, Decimal64Array};
    use arrow_buffer::i256;
    use std::sync::Arc;

    fn decimal(data_type: &DataType, value: i128) -> ArrayRef {
        match data_type {
            DataType::Decimal32(p, s) => Arc::new(
                Decimal32Array::from(vec![Some(value as i32), None])
                    .with_precision_and_scale(*p, *s)
                    .unwrap(),
            ),
            DataType::Decimal64(p, s) => Arc::new(
                Decimal64Array::from(vec![Some(value as i64), None])
                    .with_precision_and_scale(*p, *s)
                    .unwrap(),
            ),
            DataType::Decimal128(p, s) => Arc::new(
                Decimal128Array::from(vec![Some(value), None])
                    .with_precision_and_scale(*p, *s)
                    .unwrap(),
            ),
            DataType::Decimal256(p, s) => Arc::new(
                Decimal256Array::from(vec![Some(i256::from_i128(value)), None])
                    .with_precision_and_scale(*p, *s)
                    .unwrap(),
            ),
            _ => unreachable!(),
        }
    }

    #[test]
    fn zero_and_null_cross_scale_and_width_boundaries_without_overflow() {
        for (source, target) in [
            (DataType::Decimal32(9, -128), DataType::Decimal128(38, 38)),
            (DataType::Decimal64(18, -90), DataType::Decimal128(38, 38)),
            (DataType::Decimal128(38, -2), DataType::Decimal128(38, 38)),
            (DataType::Decimal128(38, 38), DataType::Decimal128(20, 0)),
            (DataType::Decimal256(76, -128), DataType::Decimal128(20, 2)),
            (DataType::Decimal256(60, -40), DataType::Decimal128(20, 2)),
            (DataType::Decimal256(76, 76), DataType::Decimal128(38, 38)),
        ] {
            for mode in [CastMode::Assignment, CastMode::RowUpdate] {
                let output = cast_update_value(&decimal(&source, 0), &target, mode).unwrap();
                assert_eq!(
                    output.to_data(),
                    decimal(&target, 0).to_data(),
                    "{source:?} -> {target:?}"
                );
                assert!(
                    cast_update_value(&decimal(&source, 1), &target, mode).is_err(),
                    "{source:?} -> {target:?}"
                );
            }
        }
    }

    #[test]
    fn exact_rescaling_preserves_precision_and_nulls_for_every_input_width() {
        for source in [
            DataType::Decimal32(9, 3),
            DataType::Decimal64(18, 3),
            DataType::Decimal128(38, 3),
            DataType::Decimal256(76, 3),
        ] {
            let target = DataType::Decimal128(10, 2);
            let output =
                cast_update_value(&decimal(&source, -1250), &target, CastMode::Assignment).unwrap();
            assert_eq!(output.to_data(), decimal(&target, -125).to_data());
            assert!(
                cast_update_value(&decimal(&source, -1251), &target, CastMode::Assignment).is_err()
            );
            assert!(cast_update_value(
                &decimal(&source, 1250),
                &DataType::Decimal128(2, 2),
                CastMode::Assignment
            )
            .is_err());
        }
    }

    #[test]
    fn floating_input_keeps_coefficients_wider_than_decimal128() {
        let coefficient = format!("1508079088755887087797858531882816082{}", "0".repeat(38))
            .parse::<i256>()
            .unwrap();
        let source: ArrayRef = Arc::new(
            Decimal256Array::from(vec![Some(coefficient), None, Some(-coefficient)])
                .with_precision_and_scale(76, 39)
                .unwrap(),
        );
        for mode in [CastMode::Assignment, CastMode::RowUpdate] {
            let result = cast_update_value(&source, &DataType::Float64, mode).unwrap();
            assert_eq!(
                result.to_data(),
                Float64Array::from(vec![
                    Some(1.508079088755887e35),
                    None,
                    Some(-1.508079088755887e35),
                ])
                .to_data()
            );
        }
    }

    #[test]
    fn integer_zero_does_not_need_an_oversized_decimal_power() {
        for source in [
            DataType::Decimal32(9, -128),
            DataType::Decimal64(18, -90),
            DataType::Decimal128(38, -90),
            DataType::Decimal256(76, -128),
        ] {
            for target in [
                DataType::Int8,
                DataType::Int32,
                DataType::Int64,
                DataType::UInt64,
            ] {
                for mode in [CastMode::Assignment, CastMode::RowUpdate] {
                    let output = cast_update_value(&decimal(&source, 0), &target, mode).unwrap();
                    let expected = arrow_cast::cast(
                        &arrow_array::Int64Array::from(vec![Some(0), None]),
                        &target,
                    )
                    .unwrap();
                    assert_eq!(
                        output.to_data(),
                        expected.to_data(),
                        "{source:?} -> {target:?}"
                    );
                    assert!(cast_update_value(&decimal(&source, 1), &target, mode).is_err());
                }
            }
        }
    }

    #[test]
    fn integer_width_bounds_apply_after_fraction_truncation() {
        for source in [DataType::Decimal128(38, 2), DataType::Decimal256(76, 2)] {
            for (target, minimum, maximum) in [
                (DataType::Int8, i8::MIN as i128, i8::MAX as i128),
                (DataType::Int16, i16::MIN as i128, i16::MAX as i128),
                (DataType::Int32, i32::MIN as i128, i32::MAX as i128),
                (DataType::Int64, i64::MIN as i128, i64::MAX as i128),
                (DataType::UInt8, 0, u8::MAX as i128),
                (DataType::UInt16, 0, u16::MAX as i128),
                (DataType::UInt32, 0, u32::MAX as i128),
                (DataType::UInt64, 0, u64::MAX as i128),
            ] {
                for value in [minimum, maximum] {
                    let input = decimal(&source, value * 100);
                    for mode in [CastMode::Assignment, CastMode::RowUpdate] {
                        let output = cast_update_value(&input, &target, mode).unwrap();
                        // The output must contain the integer boundary itself,
                        // rather than a wrapped value or a newly introduced NULL.
                        let expected: ArrayRef = if target.is_signed_integer() {
                            Arc::new(arrow_array::Int64Array::from(vec![
                                Some(value as i64),
                                None,
                            ]))
                        } else {
                            Arc::new(arrow_array::UInt64Array::from(vec![
                                Some(value as u64),
                                None,
                            ]))
                        };
                        let expected = arrow_cast::cast(&expected, &target).unwrap();
                        assert_eq!(output.to_data(), expected.to_data());
                    }
                }
                for value in [minimum - 1, maximum + 1] {
                    for mode in [CastMode::Assignment, CastMode::RowUpdate] {
                        assert!(
                            cast_update_value(&decimal(&source, value * 100), &target, mode)
                                .is_err()
                        );
                    }
                }
                let fraction = decimal(&source, maximum * 100 + 99);
                assert!(cast_update_value(&fraction, &target, CastMode::Assignment).is_err());
                assert!(cast_update_value(&fraction, &target, CastMode::RowUpdate).is_ok());
            }
        }
    }

    #[test]
    fn wide_decimal_coefficients_are_reduced_before_integer_narrowing() {
        let coefficient = format!("123{}", "0".repeat(60)).parse::<i256>().unwrap();
        let input: ArrayRef = Arc::new(
            Decimal256Array::from(vec![Some(coefficient), None, Some(-coefficient)])
                .with_precision_and_scale(76, 60)
                .unwrap(),
        );
        let expected = arrow_array::Int32Array::from(vec![Some(123), None, Some(-123)]);
        for mode in [CastMode::Assignment, CastMode::RowUpdate] {
            let output = cast_update_value(&input, &DataType::Int32, mode).unwrap();
            assert_eq!(output.to_data(), expected.to_data());
        }
        let fractional: ArrayRef = Arc::new(
            Decimal256Array::from(vec![
                Some(coefficient + i256::ONE),
                None,
                Some(-coefficient - i256::ONE),
            ])
            .with_precision_and_scale(76, 60)
            .unwrap(),
        );
        assert!(cast_update_value(&fractional, &DataType::Int32, CastMode::Assignment).is_err());
        let truncated =
            cast_update_value(&fractional, &DataType::Int32, CastMode::RowUpdate).unwrap();
        assert_eq!(truncated.to_data(), expected.to_data());
    }
}
