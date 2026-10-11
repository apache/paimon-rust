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

//! Java conversions which Arrow's schema-evolution cast cannot represent.

use std::sync::Arc;

use arrow_array::types::{
    Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
};
use arrow_array::{cast::AsArray, ArrayRef, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow_schema::{DataType as ArrowType, TimeUnit};

use crate::spec::DataType;

/// Java Number narrowing keeps low bits for integers. Floating-point values
/// first truncate and saturate to int/long, then narrow to byte/short. DECIMAL
/// truncates its fraction before BigDecimal.longValue keeps the low 64 bits.
pub(super) fn numeric_to_integer(source: &ArrayRef, target: &ArrowType) -> crate::Result<ArrayRef> {
    macro_rules! values {
        ($ty:ty, $convert:expr) => {
            source
                .as_primitive::<$ty>()
                .iter()
                .map(|value| value.map($convert))
                .collect::<Vec<Option<i64>>>()
        };
    }
    let wide = matches!(target, ArrowType::Int64);
    let values = match source.data_type() {
        ArrowType::Int8 => values!(Int8Type, |v| v as i64),
        ArrowType::Int16 => values!(Int16Type, |v| v as i64),
        ArrowType::Int32 => values!(Int32Type, |v| v as i64),
        ArrowType::Int64 => values!(Int64Type, |v| v),
        ArrowType::Float32 => values!(Float32Type, |v| if wide {
            v as i64
        } else {
            v as i32 as i64
        }),
        ArrowType::Float64 => values!(Float64Type, |v| if wide {
            v as i64
        } else {
            v as i32 as i64
        }),
        ArrowType::Decimal128(_, scale) => {
            let divisor = 10_i128.pow(*scale as u32);
            values!(Decimal128Type, |v| (v / divisor) as i64)
        }
        other => {
            return Err(crate::Error::DataInvalid {
                message: format!("Invalid numeric schema-evolution input {other:?}"),
                source: None,
            })
        }
    };
    let result: ArrayRef = match target {
        ArrowType::Int8 => Arc::new(Int8Array::from_iter(
            values.into_iter().map(|v| v.map(|v| v as i8)),
        )),
        ArrowType::Int16 => Arc::new(Int16Array::from_iter(
            values.into_iter().map(|v| v.map(|v| v as i16)),
        )),
        ArrowType::Int32 => Arc::new(Int32Array::from_iter(
            values.into_iter().map(|v| v.map(|v| v as i32)),
        )),
        ArrowType::Int64 => Arc::new(Int64Array::from(values)),
        _ => unreachable!("validated integral target"),
    };
    Ok(result)
}

/// Match DateTimeUtils.truncate for the same timestamp kind. Work in i128 so
/// flooring a pre-epoch value cannot overflow the source's i64 unit before
/// converting to a coarser target unit. Cross-kind timezone casts are handled
/// by the caller's existing cast path.
pub(super) fn timestamp_precision(
    source: &ArrayRef,
    source_type: &DataType,
    target_type: &DataType,
    target_arrow: &ArrowType,
) -> crate::Result<Option<ArrayRef>> {
    let precision = match (source_type, target_type) {
        (DataType::Timestamp(from), DataType::Timestamp(to))
            if from.precision() > to.precision() =>
        {
            to.precision()
        }
        (DataType::LocalZonedTimestamp(from), DataType::LocalZonedTimestamp(to))
            if from.precision() > to.precision() =>
        {
            to.precision()
        }
        _ => return Ok(None),
    };
    let nanos_per_tick = |unit: &TimeUnit| -> i128 {
        match unit {
            TimeUnit::Second => 1_000_000_000,
            TimeUnit::Millisecond => 1_000_000,
            TimeUnit::Microsecond => 1_000,
            TimeUnit::Nanosecond => 1,
        }
    };
    let ArrowType::Timestamp(source_unit, _) = source.data_type() else {
        return Err(crate::Error::DataInvalid {
            message: "Expected timestamp schema-evolution input".into(),
            source: None,
        });
    };
    let ArrowType::Timestamp(target_unit, _) = target_arrow else {
        unreachable!()
    };
    let raw = arrow_cast::cast(source, &ArrowType::Int64).map_err(cast_error)?;
    let quantum = 10_i128.pow(9 - precision);
    let ticks = Int64Array::from_iter(raw.as_primitive::<Int64Type>().iter().map(|value| {
        value.and_then(|value| {
            let nanos = i128::from(value) * nanos_per_tick(source_unit);
            let truncated = nanos.div_euclid(quantum) * quantum;
            i64::try_from(truncated / nanos_per_tick(target_unit)).ok()
        })
    }));
    Ok(Some(
        arrow_cast::cast(&(Arc::new(ticks) as ArrayRef), target_arrow).map_err(cast_error)?,
    ))
}

fn cast_error(error: arrow_schema::ArrowError) -> crate::Error {
    crate::Error::UnexpectedError {
        message: "Failed to cast timestamp during schema evolution".into(),
        source: Some(Box::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{LocalZonedTimestampType, TimestampType};
    use arrow_array::{Decimal128Array, Float32Array, Float64Array};

    #[test]
    fn floating_point_narrowing_saturates_before_retaining_low_bits() {
        let values = vec![
            Some(128.9),
            Some(-129.9),
            Some(32768.9),
            Some(-32769.9),
            Some(1e30),
            Some(-1e30),
            None,
        ];
        for source in [
            Arc::new(Float64Array::from(values.clone())) as ArrayRef,
            Arc::new(Float32Array::from_iter(
                values.iter().map(|v| v.map(|v| v as f32)),
            )) as ArrayRef,
        ] {
            for (target, expected) in [
                (
                    ArrowType::Int8,
                    vec![
                        Some(-128),
                        Some(127),
                        Some(0),
                        Some(-1),
                        Some(-1),
                        Some(0),
                        None,
                    ],
                ),
                (
                    ArrowType::Int16,
                    vec![
                        Some(128),
                        Some(-129),
                        Some(-32768),
                        Some(32767),
                        Some(-1),
                        Some(0),
                        None,
                    ],
                ),
                (
                    ArrowType::Int32,
                    vec![
                        Some(128),
                        Some(-129),
                        Some(32768),
                        Some(-32769),
                        Some(i32::MAX as i64),
                        Some(i32::MIN as i64),
                        None,
                    ],
                ),
                (
                    ArrowType::Int64,
                    vec![
                        Some(128),
                        Some(-129),
                        Some(32768),
                        Some(-32769),
                        Some(i64::MAX),
                        Some(i64::MIN),
                        None,
                    ],
                ),
            ] {
                let output = numeric_to_integer(&source.slice(0, values.len()), &target).unwrap();
                let widened = arrow_cast::cast(&output, &ArrowType::Int64).unwrap();
                assert_eq!(
                    widened.as_primitive::<Int64Type>(),
                    &Int64Array::from(expected)
                );
            }
        }
    }

    #[test]
    fn decimal_integrals_truncate_then_wrap_at_each_target_width() {
        let source: ArrayRef = Arc::new(
            Decimal128Array::from(vec![
                Some(12_899),
                Some(-12_999),
                Some(922_337_203_685_477_580_800),
                Some(-922_337_203_685_477_580_900),
                Some(99_999_999_999_999_999_999_999_999_999_999_999_999),
                None,
            ])
            .with_precision_and_scale(38, 2)
            .unwrap(),
        );
        // BigDecimal.longValue of the largest DECIMAL(38,2), after truncation.
        let values = [
            Some(128_i64),
            Some(-129),
            Some(i64::MIN),
            Some(i64::MAX),
            Some(-5_527_149_226_598_858_753),
            None,
        ];
        for target in [
            ArrowType::Int8,
            ArrowType::Int16,
            ArrowType::Int32,
            ArrowType::Int64,
        ] {
            let output = numeric_to_integer(&source, &target).unwrap();
            let expected = values
                .iter()
                .map(|value| {
                    value.map(|value| match target {
                        ArrowType::Int8 => value as i8 as i64,
                        ArrowType::Int16 => value as i16 as i64,
                        ArrowType::Int32 => value as i32 as i64,
                        _ => value,
                    })
                })
                .collect::<Vec<_>>();
            let widened = arrow_cast::cast(&output, &ArrowType::Int64).unwrap();
            assert_eq!(
                widened.as_primitive::<Int64Type>(),
                &Int64Array::from(expected)
            );
        }
    }

    #[test]
    fn timestamp_precision_covers_every_unit_and_pre_epoch_flooring() {
        for zoned in [false, true] {
            let paimon_type = |precision| {
                if zoned {
                    DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(precision).unwrap())
                } else {
                    DataType::Timestamp(TimestampType::new(precision).unwrap())
                }
            };
            let source_type = paimon_type(9);
            let source_arrow = crate::arrow::paimon_type_to_arrow(&source_type).unwrap();
            let raw: ArrayRef = Arc::new(Int64Array::from(vec![
                Some(123_456_789),
                Some(-1),
                Some(-123_456_789),
                Some(i64::MIN),
                None,
            ]));
            let source = arrow_cast::cast(&raw, &source_arrow).unwrap();
            for precision in 0..9 {
                let target = paimon_type(precision);
                let arrow_type = crate::arrow::paimon_type_to_arrow(&target).unwrap();
                let output = timestamp_precision(&source, &source_type, &target, &arrow_type)
                    .unwrap()
                    .unwrap();
                assert_eq!(output.data_type(), &arrow_type);
                let divisor = 10_i128.pow(9 - precision);
                let quantum_in_target = match precision {
                    0 => 1,
                    1..=3 => 10_i128.pow(3 - precision),
                    4..=6 => 10_i128.pow(6 - precision),
                    _ => 10_i128.pow(9 - precision),
                };
                let expected = raw
                    .as_primitive::<Int64Type>()
                    .iter()
                    .map(|v| {
                        v.and_then(|v| {
                            i64::try_from(i128::from(v).div_euclid(divisor) * quantum_in_target)
                                .ok()
                        })
                    })
                    .collect::<Vec<_>>();
                let widened = arrow_cast::cast(&output, &ArrowType::Int64).unwrap();
                assert_eq!(
                    widened.as_primitive::<Int64Type>(),
                    &Int64Array::from(expected),
                    "precision {precision}"
                );
            }
            assert!(
                timestamp_precision(&source, &source_type, &source_type, &source_arrow)
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[cfg(test)]
mod integer_tests {
    use super::*;

    #[test]
    fn every_signed_integer_width_preserves_nulls_and_java_narrowing() {
        let sources: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![
                Some(i8::MIN),
                Some(i8::MAX),
                Some(-1),
                None,
            ])),
            Arc::new(Int16Array::from(vec![
                Some(i16::MIN),
                Some(i16::MAX),
                Some(-129),
                None,
            ])),
            Arc::new(Int32Array::from(vec![
                Some(i32::MIN),
                Some(i32::MAX),
                Some(32768),
                None,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                Some(i64::MAX),
                Some(2147483648),
                None,
            ])),
        ];
        let expected: [[Vec<Option<i64>>; 4]; 4] = [
            [
                vec![Some(-128), Some(127), Some(-1), None],
                vec![Some(-128), Some(127), Some(-1), None],
                vec![Some(-128), Some(127), Some(-1), None],
                vec![Some(-128), Some(127), Some(-1), None],
            ],
            [
                vec![Some(0), Some(-1), Some(127), None],
                vec![Some(-32768), Some(32767), Some(-129), None],
                vec![Some(-32768), Some(32767), Some(-129), None],
                vec![Some(-32768), Some(32767), Some(-129), None],
            ],
            [
                vec![Some(0), Some(-1), Some(0), None],
                vec![Some(0), Some(-1), Some(-32768), None],
                vec![
                    Some(i32::MIN as i64),
                    Some(i32::MAX as i64),
                    Some(32768),
                    None,
                ],
                vec![
                    Some(i32::MIN as i64),
                    Some(i32::MAX as i64),
                    Some(32768),
                    None,
                ],
            ],
            [
                vec![Some(0), Some(-1), Some(0), None],
                vec![Some(0), Some(-1), Some(0), None],
                vec![Some(0), Some(-1), Some(i32::MIN as i64), None],
                vec![Some(i64::MIN), Some(i64::MAX), Some(2147483648), None],
            ],
        ];
        for (source, expected) in sources.into_iter().zip(expected) {
            for (target, expected) in [
                ArrowType::Int8,
                ArrowType::Int16,
                ArrowType::Int32,
                ArrowType::Int64,
            ]
            .iter()
            .zip(expected)
            {
                let output = numeric_to_integer(&source, target).unwrap();
                let widened = arrow_cast::cast(&output, &ArrowType::Int64).unwrap();
                assert_eq!(
                    widened.as_primitive::<Int64Type>(),
                    &Int64Array::from(expected)
                );
            }
        }
    }
}
