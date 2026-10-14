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

//! Preserve Java row-object classes while adapting to Arrow storage without loss.

use std::sync::Arc;

use arrow_array::{cast::AsArray, types::*, Array, ArrayRef, Decimal128Array, Int64Array};
use arrow_schema::{DataType, TimeUnit};

fn integer_object(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int32 | DataType::Date32 | DataType::Time32(TimeUnit::Millisecond)
    )
}

fn invalid() -> crate::Error {
    super::unsupported(
        "the mask result cannot be represented by the output Arrow type without loss",
    )
}

pub(super) fn convert(array: &ArrayRef, to: &DataType) -> crate::Result<ArrayRef> {
    // Java getters never check the object class of NULL or an empty row set.
    if array.null_count() == array.len() {
        return Ok(arrow_array::new_null_array(to, array.len()));
    }
    if array.data_type() == to {
        return Ok(Arc::clone(array));
    }
    match (array.data_type(), to) {
        (DataType::Decimal128(_, source_scale), DataType::Decimal128(precision, target_scale)) => {
            let delta = i32::from(*target_scale) - i32::from(*source_scale);
            let factor = 10_i128.pow(delta.unsigned_abs());
            let bound = 10_i128.pow(u32::from(*precision));
            let values = array
                .as_primitive::<Decimal128Type>()
                .iter()
                .map(|value| {
                    value
                        .map(|value| {
                            let result = if delta >= 0 {
                                value.checked_mul(factor).ok_or_else(invalid)?
                            } else {
                                if value % factor != 0 {
                                    return Err(invalid());
                                }
                                value / factor
                            };
                            if result <= -bound || result >= bound {
                                return Err(invalid());
                            }
                            Ok(result)
                        })
                        .transpose()
                })
                .collect::<crate::Result<Vec<_>>>()?;
            Ok(Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(*precision, *target_scale)
                    .map_err(|_| invalid())?,
            ))
        }
        (DataType::Timestamp(from_unit, _), DataType::Timestamp(to_unit, _)) => {
            let quantum = |unit| match unit {
                TimeUnit::Second => 1_000_000_000_i128,
                TimeUnit::Millisecond => 1_000_000,
                TimeUnit::Microsecond => 1000,
                TimeUnit::Nanosecond => 1,
            };
            let from_quantum = quantum(*from_unit);
            let to_quantum = quantum(*to_unit);
            let integers = arrow_cast::cast(array, &DataType::Int64).map_err(|_| invalid())?;
            let values = integers
                .as_primitive::<Int64Type>()
                .iter()
                .map(|value| {
                    value
                        .map(|value| {
                            let nanos = i128::from(value) * from_quantum;
                            if nanos % to_quantum != 0 {
                                return Err(invalid());
                            }
                            i64::try_from(nanos / to_quantum).map_err(|_| invalid())
                        })
                        .transpose()
                })
                .collect::<crate::Result<Vec<_>>>()?;
            arrow_cast::cast(&(Arc::new(Int64Array::from(values)) as ArrayRef), to)
                .map_err(|_| invalid())
        }
        _ if integer_object(array.data_type()) && integer_object(to) => {
            let integers = arrow_cast::cast(array, &DataType::Int32).map_err(|_| invalid())?;
            arrow_cast::cast(&integers, to).map_err(|_| invalid())
        }
        _ => Err(invalid()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, TimestampMicrosecondArray};

    #[test]
    fn decimal_output_adaptation_never_rounds_or_overflows() {
        let input = |values| {
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(7, 3)
                    .unwrap(),
            ) as ArrayRef
        };
        let target = DataType::Decimal128(4, 2);
        let converted = convert(&input(vec![Some(2670), None]), &target).unwrap();
        assert_eq!(converted.as_primitive::<Decimal128Type>().value(0), 267);
        assert!(converted.is_null(1));
        for bad in [2675, -2675, 1_000_000] {
            assert!(convert(&input(vec![Some(bad)]), &target).is_err());
        }
        assert!(convert(&input(vec![Some(2670)]), &DataType::Decimal128(7, 4)).is_ok());
    }

    #[test]
    fn timestamp_output_adaptation_preserves_exact_epoch_ticks_and_nulls() {
        let input: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(1000), None]));
        let target = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let result = convert(&input, &target).unwrap();
        let raw = arrow_cast::cast(&result, &DataType::Int64).unwrap();
        assert_eq!(raw.as_primitive::<Int64Type>().value(0), 1);
        assert!(raw.is_null(1));
        for bad in [1001, -1001] {
            let input: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(bad)]));
            assert!(convert(&input, &target).is_err());
        }
        let input: ArrayRef =
            Arc::new(arrow_array::TimestampMillisecondArray::from(vec![i64::MAX]));
        assert!(convert(&input, &DataType::Timestamp(TimeUnit::Nanosecond, None)).is_err());
    }
}

#[cfg(test)]
mod integer_tests {
    use super::*;
    #[test]
    fn date_time_and_int_masks_share_the_java_integer_class() {
        let raw: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![
            Some(-1),
            Some(123),
            None,
        ]));
        let types = [
            DataType::Date32,
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Int32,
        ];
        for from in &types {
            let source = arrow_cast::cast(&raw, from).unwrap();
            for to in &types {
                let result = convert(&source, to).unwrap();
                assert_eq!(*arrow_cast::cast(&result, &DataType::Int32).unwrap(), *raw);
            }
        }
    }
}

#[cfg(test)]
mod null_tests {
    use super::*;
    #[test]
    fn null_and_empty_values_skip_the_runtime_class_check() {
        let target = DataType::Int32;
        for len in [0, 2] {
            let input = arrow_array::new_null_array(&DataType::Utf8, len);
            let output = convert(&input, &target).unwrap();
            assert_eq!(output.data_type(), &target);
            assert_eq!(output.null_count(), len);
        }
        let input: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![Some("bad"), None]));
        assert!(convert(&input, &target).is_err());
    }
}
