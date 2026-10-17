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

//! Exact numeric equality for external Arrow match keys. Assignment casts may
//! round; a matching key must retain its mathematical value across families.

use std::sync::Arc;

use arrow_array::{cast::AsArray, Array, ArrayRef, Decimal128Array, Float32Array, Float64Array};
use arrow_buffer::i256;
use arrow_schema::DataType;

use super::super::update_input::{cast_update_value, CastMode};

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

pub(super) fn normalize(array: &ArrayRef, target: &DataType) -> crate::Result<ArrayRef> {
    let source = array.data_type();
    if source.is_floating() {
        if let DataType::Decimal128(precision, scale) = target {
            let values = arrow_cast::cast(array, &DataType::Float64)
                .map_err(|error| invalid(error.to_string()))?;
            let values = values
                .as_primitive::<arrow_array::types::Float64Type>()
                .iter()
                .map(|value| {
                    value
                        .map(|value| {
                            let coefficient = float_coefficient(value, *scale)
                                .and_then(i256::to_i128)
                                .filter(|value| {
                                    value.unsigned_abs() < 10_u128.pow(u32::from(*precision))
                                })
                                .ok_or_else(|| {
                                    invalid("Floating match key is not an exact DECIMAL value")
                                })?;
                            Ok(coefficient)
                        })
                        .transpose()
                })
                .collect::<crate::Result<Vec<_>>>()?;
            return Decimal128Array::from(values)
                .with_precision_and_scale(*precision, *scale)
                .map(|array| Arc::new(array) as ArrayRef)
                .map_err(|error| invalid(error.to_string()));
        }
    }
    if source.is_decimal() {
        if target.is_floating() {
            return decimal_to_float(array, target);
        }
        if target.is_integer() {
            // Canonicalize first to avoid scale arithmetic overflow in generic
            // Arrow integer casts, including external Decimal256 zero values.
            let integral =
                cast_update_value(array, &DataType::Decimal128(38, 0), CastMode::Assignment)?;
            return cast_update_value(&integral, target, CastMode::Assignment);
        }
    }
    if source.is_floating() || target.is_floating() {
        // Unlike assignment's consecutive-integer range, identity only needs
        // an exact round trip (e.g. 2^53 + 2 is a valid DOUBLE key).
        let options = arrow_cast::CastOptions {
            safe: false,
            ..Default::default()
        };
        let converted = arrow_cast::cast_with_options(array.as_ref(), target, &options)
            .map_err(|error| invalid(error.to_string()))?;
        let restored = arrow_cast::cast_with_options(converted.as_ref(), source, &options)
            .map_err(|error| invalid(error.to_string()))?;
        if restored.to_data() != array.to_data() {
            return Err(invalid(
                "Numeric match key conversion would change key identity",
            ));
        }
        return Ok(converted);
    }
    cast_update_value(array, target, CastMode::Assignment)
}

fn decimal_to_float(array: &ArrayRef, target: &DataType) -> crate::Result<ArrayRef> {
    let scale = match array.data_type() {
        DataType::Decimal32(_, s)
        | DataType::Decimal64(_, s)
        | DataType::Decimal128(_, s)
        | DataType::Decimal256(_, s) => *s,
        _ => unreachable!("validated DECIMAL key"),
    };
    let values = (0..array.len())
        .map(|row| {
            if array.is_null(row) {
                return Ok(None);
            }
            let coefficient = match array.data_type() {
                DataType::Decimal32(..) => i256::from_i128(i128::from(
                    array
                        .as_primitive::<arrow_array::types::Decimal32Type>()
                        .value(row),
                )),
                DataType::Decimal64(..) => i256::from_i128(i128::from(
                    array
                        .as_primitive::<arrow_array::types::Decimal64Type>()
                        .value(row),
                )),
                DataType::Decimal128(..) => i256::from_i128(
                    array
                        .as_primitive::<arrow_array::types::Decimal128Type>()
                        .value(row),
                ),
                DataType::Decimal256(..) => array
                    .as_primitive::<arrow_array::types::Decimal256Type>()
                    .value(row),
                _ => unreachable!("validated DECIMAL key"),
            };
            let text = format!("{coefficient}e{}", -i16::from(scale));
            let value = if target == &DataType::Float32 {
                f64::from(
                    text.parse::<f32>()
                        .map_err(|error| invalid(error.to_string()))?,
                )
            } else {
                text.parse::<f64>()
                    .map_err(|error| invalid(error.to_string()))?
            };
            if float_coefficient(value, scale) != Some(coefficient) {
                return Err(invalid("DECIMAL match key is not an exact floating value"));
            }
            Ok(Some(value))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(if target == &DataType::Float32 {
        Arc::new(Float32Array::from_iter(
            values.into_iter().map(|value| value.map(|v| v as f32)),
        )) as ArrayRef
    } else {
        Arc::new(Float64Array::from(values)) as ArrayRef
    })
}

/// Return the exact decimal coefficient of a binary float at the given scale.
/// Check divisibility before multiplying, so a fractional key cannot round
/// into an existing row. All work is bounded by the Arrow coefficient width.
fn float_coefficient(value: f64, scale: i8) -> Option<i256> {
    if !value.is_finite() {
        return None;
    }
    if value == 0.0 {
        return Some(i256::ZERO);
    }
    let bits = value.abs().to_bits();
    let raw_exponent = ((bits >> 52) & 0x7ff) as i32;
    let mut mantissa = bits & ((1_u64 << 52) - 1);
    let mut exponent = if raw_exponent == 0 {
        -1074
    } else {
        mantissa |= 1_u64 << 52;
        raw_exponent - 1023 - 52
    };
    let zeros = mantissa.trailing_zeros();
    mantissa >>= zeros;
    exponent += zeros as i32 + i32::from(scale);
    if exponent < 0 {
        return None;
    }
    let five = if scale < 0 {
        for _ in 0..-i16::from(scale) {
            if !mantissa.is_multiple_of(5) {
                return None;
            }
            mantissa /= 5;
        }
        i256::ONE
    } else {
        i256::from_i128(5).checked_pow(scale as u32)?
    };
    let coefficient = i256::from_i128(i128::from(mantissa))
        .checked_mul(five)?
        .checked_mul(i256::from_i128(2).checked_pow(exponent as u32)?)?;
    if value < 0.0 {
        coefficient.checked_neg()
    } else {
        Some(coefficient)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Decimal256Array, Decimal32Array, Decimal64Array, Int32Array, Int64Array, UInt64Array,
    };

    #[test]
    fn integer_float_keys_require_identity_without_assignment_range_limits() {
        let integer: ArrayRef = Arc::new(Int64Array::from(vec![Some((1_i64 << 53) + 2), None]));
        let converted = normalize(&integer, &DataType::Float64).unwrap();
        assert_eq!(
            normalize(&converted, &DataType::Int64).unwrap().to_data(),
            integer.to_data()
        );
        let fractional: ArrayRef = Arc::new(Float64Array::from(vec![1.00000001]));
        assert!(normalize(&fractional, &DataType::Int32).is_err());
        let rounded: ArrayRef = Arc::new(Int64Array::from(vec![(1_i64 << 53) + 1]));
        assert!(normalize(&rounded, &DataType::Float64).is_err());
        let overflow: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX]));
        assert!(normalize(&overflow, &DataType::Float64).is_err());
        let overflow: ArrayRef = Arc::new(Float64Array::from(vec![2_f64.powi(63)]));
        assert!(normalize(&overflow, &DataType::Int64).is_err());
    }

    #[test]
    fn decimal_integer_keys_share_the_exact_rescaler() {
        let decimal: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(12500), None])
                .with_precision_and_scale(15, 4)
                .unwrap(),
        );
        assert!(normalize(&decimal, &DataType::Int32).is_err());
        let integer: ArrayRef = Arc::new(Int32Array::from(vec![Some(-1), None, Some(2)]));
        let converted = normalize(&integer, &DataType::Decimal128(10, 2)).unwrap();
        assert_eq!(
            normalize(&converted, &DataType::Int32).unwrap().to_data(),
            integer.to_data()
        );
        let zero: ArrayRef = Arc::new(
            Decimal256Array::from(vec![Some(i256::ZERO), None])
                .with_precision_and_scale(76, -128)
                .unwrap(),
        );
        let actual = normalize(&zero, &DataType::Int64).unwrap();
        assert_eq!(
            actual.to_data(),
            Int64Array::from(vec![Some(0), None]).to_data()
        );
    }

    #[test]
    fn decimal_float_keys_do_not_use_rounded_decimal_equality() {
        let floating: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), None, Some(-2.25)]));
        let decimal = normalize(&floating, &DataType::Decimal128(10, 2)).unwrap();
        assert_eq!(
            decimal.to_data(),
            Decimal128Array::from(vec![Some(150), None, Some(-225)])
                .with_precision_and_scale(10, 2)
                .unwrap()
                .to_data()
        );
        assert_eq!(
            normalize(&decimal, &DataType::Float64).unwrap().to_data(),
            floating.to_data()
        );
        for value in [0.1, 1.235, 1.0 + f64::EPSILON, f64::MAX, f64::MIN_POSITIVE] {
            let input: ArrayRef = Arc::new(Float64Array::from(vec![value]));
            assert!(normalize(&input, &DataType::Decimal128(10, 2)).is_err());
        }
        let rounded: ArrayRef = Arc::new(
            Decimal128Array::from(vec![10])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        assert!(normalize(&rounded, &DataType::Float64).is_err());
        assert!(normalize(&rounded, &DataType::Float32).is_err());
        let small: ArrayRef = Arc::new(
            Decimal32Array::from(vec![Some(150), None])
                .with_precision_and_scale(9, 2)
                .unwrap(),
        );
        let medium: ArrayRef = Arc::new(
            Decimal64Array::from(vec![Some(150), None])
                .with_precision_and_scale(18, 2)
                .unwrap(),
        );
        for input in [small, medium] {
            assert_eq!(
                normalize(&input, &DataType::Float64).unwrap().to_data(),
                Float64Array::from(vec![Some(1.5), None]).to_data()
            );
        }
    }

    #[test]
    fn exact_binary_values_support_wide_and_negative_decimal_scales() {
        assert_eq!(float_coefficient(1e20, -20), Some(i256::ONE));
        assert_eq!(float_coefficient(-1e20, -20), Some(-i256::ONE));
        assert_eq!(float_coefficient(1e20, -21), None);
        assert_eq!(float_coefficient(0.0, -128), Some(i256::ZERO));
        assert_eq!(float_coefficient(0.0, 127), Some(i256::ZERO));
        // 2^-70 needs 70 decimal places, beyond Decimal128's coefficient width.
        let coefficient = i256::from_i128(5).checked_pow(70).unwrap();
        let input: ArrayRef = Arc::new(
            Decimal256Array::from(vec![Some(coefficient), None, Some(-coefficient)])
                .with_precision_and_scale(76, 70)
                .unwrap(),
        );
        let actual = normalize(&input, &DataType::Float64).unwrap();
        assert_eq!(
            actual.to_data(),
            Float64Array::from(vec![Some(2_f64.powi(-70)), None, Some(-2_f64.powi(-70))]).to_data()
        );
        let large: ArrayRef = Arc::new(
            Decimal256Array::from(vec![Some(i256::ONE), None])
                .with_precision_and_scale(76, -20)
                .unwrap(),
        );
        assert_eq!(
            normalize(&large, &DataType::Float64).unwrap().to_data(),
            Float64Array::from(vec![Some(1e20), None]).to_data()
        );
    }
}
