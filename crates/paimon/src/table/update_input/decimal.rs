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

use arrow_array::{cast::AsArray, ArrayRef, Decimal128Array};
use arrow_schema::DataType;
use std::sync::Arc;

use super::{invalid, parse_decimal_text};

pub(super) fn cast_exact_decimal(array: &ArrayRef, target: &DataType) -> crate::Result<ArrayRef> {
    let source_scale = match array.data_type() {
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale)
        | DataType::Decimal256(_, scale) => *scale,
        _ => unreachable!("validated DECIMAL input"),
    };
    let DataType::Decimal128(target_precision, target_scale) = target else {
        unreachable!("validated DECIMAL target")
    };
    let text_fallback = || {
        // A zero or NULL can cross scales beyond the rescaler's power table.
        // The exact text parser still rejects fractional loss and overflow.
        // Use the full coefficient. Generic decimal display can clip digits
        // to the source precision even for foreign buffers containing more.
        macro_rules! values {
            ($ty:ty) => {
                array
                    .as_primitive::<$ty>()
                    .iter()
                    .map(|value| {
                        value
                            .map(|value| {
                                parse_decimal_text(
                                    &format!("{value}e{}", -i16::from(source_scale)),
                                    *target_precision,
                                    *target_scale,
                                )
                            })
                            .transpose()
                    })
                    .collect::<crate::Result<Vec<_>>>()?
            };
        }
        let values = match array.data_type() {
            DataType::Decimal32(..) => values!(arrow_array::types::Decimal32Type),
            DataType::Decimal64(..) => values!(arrow_array::types::Decimal64Type),
            DataType::Decimal128(..) => values!(arrow_array::types::Decimal128Type),
            DataType::Decimal256(..) => values!(arrow_array::types::Decimal256Type),
            _ => unreachable!("validated DECIMAL input"),
        };
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
}
