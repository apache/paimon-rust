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

use arrow_array::{cast::AsArray, ArrayRef, Decimal128Array, Float32Array, Float64Array};
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
    // Preserve the full coefficient. A generic display can clip digits to the
    // declared source precision for foreign buffers containing more digits.
    macro_rules! values {
        ($ty:ty) => {
            array
                .as_primitive::<$ty>()
                .iter()
                .map(|value| {
                    value
                        .map(|value| parse(&format!("{value}e{}", -i16::from(scale))))
                        .transpose()
                })
                .collect()
        };
    }
    match array.data_type() {
        DataType::Decimal32(..) => values!(arrow_array::types::Decimal32Type),
        DataType::Decimal64(..) => values!(arrow_array::types::Decimal64Type),
        DataType::Decimal128(..) => values!(arrow_array::types::Decimal128Type),
        DataType::Decimal256(..) => values!(arrow_array::types::Decimal256Type),
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
}
