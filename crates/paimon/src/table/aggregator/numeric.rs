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

//! Numeric aggregators: sum, product, min, max.
//!
//! `sum` and `product` operate on integer, floating and Decimal numeric types.
//! Integer overflow wraps by default, following Java arithmetic, and raises
//! [`Error::DataInvalid`] when the field's `fail-on-overflow` option is enabled.
//! A Decimal `sum` whose result no
//! longer fits the declared precision yields a NULL cell, matching Java
//! `DecimalUtils.add` / `Decimal.fromBigDecimal` (which return null on
//! precision or backing `i128` overflow rather than throwing).
//!
//! `min` / `max` extend to every ordered Paimon type: numerics, Decimal,
//! Date, Time, Timestamp, and Char/VarChar.  Comparison is by native value
//! order (numeric for numbers, lexicographic for strings).  Float NaN is
//! treated as greater than any other value, matching Java's
//! `Float.compare` / `Double.compare`.
//!
//! Reference: Java `FieldSumAgg`, `FieldProductAgg`, `FieldMinAgg`,
//! `FieldMaxAgg` under `org.apache.paimon.mergetree.compact.aggregate`.
//!
//! [`Error::DataInvalid`]: crate::Error::DataInvalid

use std::sync::Arc;

use arrow_array::builder::Decimal128Builder;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    Time32MillisecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType as ArrowDataType, TimeUnit};
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};

use super::{unsupported_type_error, FieldAggregator};
use crate::spec::DataType;

// ---------------------------------------------------------------------------
// Sum
// ---------------------------------------------------------------------------

/// `sum` accumulator state, parameterized by the column's numeric kind.
#[derive(Debug)]
enum SumState {
    I8(Option<i8>),
    I16(Option<i16>),
    I32(Option<i32>),
    I64(Option<i64>),
    F32(Option<f32>),
    F64(Option<f64>),
    Decimal128 {
        precision: u8,
        scale: i8,
        acc: Option<i128>,
    },
}

#[derive(Debug)]
pub(crate) struct SumAgg {
    field_name: String,
    state: SumState,
    fail_on_overflow: bool,
}

impl SumAgg {
    #[cfg(test)]
    pub(crate) fn new(field_name: &str, data_type: &DataType) -> crate::Result<Self> {
        Self::new_with_overflow(field_name, data_type, false)
    }

    pub(crate) fn new_with_overflow(
        field_name: &str,
        data_type: &DataType,
        fail_on_overflow: bool,
    ) -> crate::Result<Self> {
        let state = match data_type {
            DataType::TinyInt(_) => SumState::I8(None),
            DataType::SmallInt(_) => SumState::I16(None),
            DataType::Int(_) => SumState::I32(None),
            DataType::BigInt(_) => SumState::I64(None),
            DataType::Float(_) => SumState::F32(None),
            DataType::Double(_) => SumState::F64(None),
            DataType::Decimal(d) => SumState::Decimal128 {
                precision: decimal_precision(d.precision(), field_name)?,
                scale: decimal_scale(d.scale(), field_name)?,
                acc: None,
            },
            other => return Err(unsupported_type_error("sum", field_name, other)),
        };
        Ok(Self {
            field_name: field_name.to_string(),
            state,
            fail_on_overflow,
        })
    }
}

impl FieldAggregator for SumAgg {
    fn name(&self) -> &'static str {
        "sum"
    }

    fn reset(&mut self) {
        match &mut self.state {
            SumState::I8(acc) => *acc = None,
            SumState::I16(acc) => *acc = None,
            SumState::I32(acc) => *acc = None,
            SumState::I64(acc) => *acc = None,
            SumState::F32(acc) => *acc = None,
            SumState::F64(acc) => *acc = None,
            SumState::Decimal128 { acc, .. } => *acc = None,
        }
    }

    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
        }
        match &mut self.state {
            SumState::I8(acc) => {
                let v = downcast::<Int8Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => {
                        if self.fail_on_overflow {
                            prev.checked_add(v)
                                .ok_or_else(|| overflow_error("sum", &self.field_name))?
                        } else {
                            prev.wrapping_add(v)
                        }
                    }
                });
            }
            SumState::I16(acc) => {
                let v = downcast::<Int16Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => {
                        if self.fail_on_overflow {
                            prev.checked_add(v)
                                .ok_or_else(|| overflow_error("sum", &self.field_name))?
                        } else {
                            prev.wrapping_add(v)
                        }
                    }
                });
            }
            SumState::I32(acc) => {
                let v = downcast::<Int32Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => {
                        if self.fail_on_overflow {
                            prev.checked_add(v)
                                .ok_or_else(|| overflow_error("sum", &self.field_name))?
                        } else {
                            prev.wrapping_add(v)
                        }
                    }
                });
            }
            SumState::I64(acc) => {
                let v = downcast::<Int64Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => {
                        if self.fail_on_overflow {
                            prev.checked_add(v)
                                .ok_or_else(|| overflow_error("sum", &self.field_name))?
                        } else {
                            prev.wrapping_add(v)
                        }
                    }
                });
            }
            SumState::F32(acc) => {
                let v = downcast::<Float32Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(acc.map_or(v, |prev| prev + v));
            }
            SumState::F64(acc) => {
                let v = downcast::<Float64Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(acc.map_or(v, |prev| prev + v));
            }
            SumState::Decimal128 { precision, acc, .. } => {
                let v = downcast::<Decimal128Array>(array, &self.field_name)?.value(row_idx);
                let next = match *acc {
                    None => Some(v),
                    Some(prev) => prev.checked_add(v),
                };
                *acc = next.filter(|value| decimal_fits_precision(*value, *precision));
            }
        }
        Ok(())
    }

    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
        }
        match &mut self.state {
            SumState::F32(acc) => {
                let v = downcast::<Float32Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(acc.map_or(v, |prev| v + prev));
                Ok(())
            }
            SumState::F64(acc) => {
                let v = downcast::<Float64Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(acc.map_or(v, |prev| v + prev));
                Ok(())
            }
            _ => self.agg(array, row_idx),
        }
    }

    fn retract(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
        }
        macro_rules! retract_integer {
            ($array_type:ty, $acc:expr) => {{
                let value = downcast::<$array_type>(array, &self.field_name)?.value(row_idx);
                let previous = $acc.unwrap_or(0);
                let result = if self.fail_on_overflow {
                    previous
                        .checked_sub(value)
                        .ok_or_else(|| overflow_error("sum", &self.field_name))?
                } else {
                    previous.wrapping_sub(value)
                };
                *$acc = Some(result);
            }};
        }
        match &mut self.state {
            SumState::I8(acc) => retract_integer!(Int8Array, acc),
            SumState::I16(acc) => retract_integer!(Int16Array, acc),
            SumState::I32(acc) => retract_integer!(Int32Array, acc),
            SumState::I64(acc) => retract_integer!(Int64Array, acc),
            SumState::F32(acc) => {
                let value = downcast::<Float32Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(acc.unwrap_or(0.0) - value);
            }
            SumState::F64(acc) => {
                let value = downcast::<Float64Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(acc.unwrap_or(0.0) - value);
            }
            SumState::Decimal128 { precision, acc, .. } => {
                let value = downcast::<Decimal128Array>(array, &self.field_name)?.value(row_idx);
                *acc = acc
                    .unwrap_or(0)
                    .checked_sub(value)
                    .filter(|result| decimal_fits_precision(*result, *precision));
            }
        }
        Ok(())
    }

    fn result(&self) -> crate::Result<ArrayRef> {
        Ok(match &self.state {
            SumState::I8(acc) => Arc::new(Int8Array::from(vec![*acc])),
            SumState::I16(acc) => Arc::new(Int16Array::from(vec![*acc])),
            SumState::I32(acc) => Arc::new(Int32Array::from(vec![*acc])),
            SumState::I64(acc) => Arc::new(Int64Array::from(vec![*acc])),
            SumState::F32(acc) => Arc::new(Float32Array::from(vec![*acc])),
            SumState::F64(acc) => Arc::new(Float64Array::from(vec![*acc])),
            SumState::Decimal128 {
                precision,
                scale,
                acc,
            } => {
                // Java parity: `DecimalUtils.add` -> `Decimal.fromBigDecimal`
                // returns null when the summed value no longer fits the
                // declared precision (or the backing i128), so an overflowing
                // sum yields a NULL cell rather than a silently out-of-range
                // Decimal.
                let fitted = acc.filter(|v| decimal_fits_precision(*v, *precision));
                decimal_array(*precision, *scale, fitted, "sum", &self.field_name)?
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Product
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum ProductState {
    I8(Option<i8>),
    I16(Option<i16>),
    I32(Option<i32>),
    I64(Option<i64>),
    F32(Option<f32>),
    F64(Option<f64>),
    Decimal128 {
        precision: u8,
        scale: i8,
        acc: Option<i128>,
    },
}

#[derive(Debug)]
pub(crate) struct ProductAgg {
    field_name: String,
    state: ProductState,
    fail_on_overflow: bool,
}

impl ProductAgg {
    #[cfg(test)]
    pub(crate) fn new(field_name: &str, data_type: &DataType) -> crate::Result<Self> {
        Self::new_with_overflow(field_name, data_type, false)
    }

    pub(crate) fn new_with_overflow(
        field_name: &str,
        data_type: &DataType,
        fail_on_overflow: bool,
    ) -> crate::Result<Self> {
        let state = match data_type {
            DataType::TinyInt(_) => ProductState::I8(None),
            DataType::SmallInt(_) => ProductState::I16(None),
            DataType::Int(_) => ProductState::I32(None),
            DataType::BigInt(_) => ProductState::I64(None),
            DataType::Float(_) => ProductState::F32(None),
            DataType::Double(_) => ProductState::F64(None),
            DataType::Decimal(d) => ProductState::Decimal128 {
                precision: decimal_precision(d.precision(), field_name)?,
                scale: decimal_scale(d.scale(), field_name)?,
                acc: None,
            },
            other => return Err(unsupported_type_error("product", field_name, other)),
        };
        Ok(Self {
            field_name: field_name.to_string(),
            state,
            fail_on_overflow,
        })
    }
}

impl FieldAggregator for ProductAgg {
    fn name(&self) -> &'static str {
        "product"
    }

    fn reset(&mut self) {
        match &mut self.state {
            ProductState::I8(acc) => *acc = None,
            ProductState::I16(acc) => *acc = None,
            ProductState::I32(acc) => *acc = None,
            ProductState::I64(acc) => *acc = None,
            ProductState::F32(acc) => *acc = None,
            ProductState::F64(acc) => *acc = None,
            ProductState::Decimal128 { acc, .. } => *acc = None,
        }
    }

    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
        }
        match &mut self.state {
            ProductState::I8(acc) => {
                let v = downcast::<Int8Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => {
                        if self.fail_on_overflow {
                            prev.checked_mul(v)
                                .ok_or_else(|| overflow_error("product", &self.field_name))?
                        } else {
                            prev.wrapping_mul(v)
                        }
                    }
                });
            }
            ProductState::I16(acc) => {
                let v = downcast::<Int16Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => {
                        if self.fail_on_overflow {
                            prev.checked_mul(v)
                                .ok_or_else(|| overflow_error("product", &self.field_name))?
                        } else {
                            prev.wrapping_mul(v)
                        }
                    }
                });
            }
            ProductState::I32(acc) => {
                let v = downcast::<Int32Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => {
                        if self.fail_on_overflow {
                            prev.checked_mul(v)
                                .ok_or_else(|| overflow_error("product", &self.field_name))?
                        } else {
                            prev.wrapping_mul(v)
                        }
                    }
                });
            }
            ProductState::I64(acc) => {
                let v = downcast::<Int64Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => {
                        if self.fail_on_overflow {
                            prev.checked_mul(v)
                                .ok_or_else(|| overflow_error("product", &self.field_name))?
                        } else {
                            prev.wrapping_mul(v)
                        }
                    }
                });
            }
            ProductState::F32(acc) => {
                let v = downcast::<Float32Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(acc.map_or(v, |prev| prev * v));
            }
            ProductState::F64(acc) => {
                let v = downcast::<Float64Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(acc.map_or(v, |prev| prev * v));
            }
            ProductState::Decimal128 {
                precision,
                scale,
                acc,
            } => {
                let value = downcast::<Decimal128Array>(array, &self.field_name)?.value(row_idx);
                *acc = match *acc {
                    None => Some(value),
                    Some(previous) => {
                        let product = BigInt::from(previous) * BigInt::from(value);
                        let divisor = BigInt::from(10u8).pow(*scale as u32);
                        rounded_decimal(&product, &divisor, *precision)
                    }
                };
            }
        }
        Ok(())
    }

    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
        }
        match &mut self.state {
            ProductState::F32(acc) => {
                let v = downcast::<Float32Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(acc.map_or(v, |prev| v * prev));
                Ok(())
            }
            ProductState::F64(acc) => {
                let v = downcast::<Float64Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(acc.map_or(v, |prev| v * prev));
                Ok(())
            }
            _ => self.agg(array, row_idx),
        }
    }

    fn retract(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
        }
        macro_rules! retract_integer_product {
            ($array_type:ty, $acc:expr, $min:expr) => {{
                let value = downcast::<$array_type>(array, &self.field_name)?.value(row_idx);
                if let Some(previous) = *$acc {
                    if value == 0 {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "product retract divides by zero for '{}'",
                                self.field_name
                            ),
                            source: None,
                        });
                    }
                    *$acc = Some(
                        if previous == $min && value == -1 && !self.fail_on_overflow {
                            $min
                        } else {
                            previous
                                .checked_div(value)
                                .ok_or_else(|| overflow_error("product", &self.field_name))?
                        },
                    );
                }
            }};
        }
        match &mut self.state {
            ProductState::I8(acc) => retract_integer_product!(Int8Array, acc, i8::MIN),
            ProductState::I16(acc) => retract_integer_product!(Int16Array, acc, i16::MIN),
            ProductState::I32(acc) => retract_integer_product!(Int32Array, acc, i32::MIN),
            ProductState::I64(acc) => retract_integer_product!(Int64Array, acc, i64::MIN),
            ProductState::F32(acc) => {
                let value = downcast::<Float32Array>(array, &self.field_name)?.value(row_idx);
                if let Some(previous) = acc {
                    *previous /= value;
                }
            }
            ProductState::F64(acc) => {
                let value = downcast::<Float64Array>(array, &self.field_name)?.value(row_idx);
                if let Some(previous) = acc {
                    *previous /= value;
                }
            }
            ProductState::Decimal128 {
                precision,
                scale,
                acc,
            } => {
                let value = downcast::<Decimal128Array>(array, &self.field_name)?.value(row_idx);
                if let Some(previous) = *acc {
                    if value == 0 {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "product retract divides by zero for '{}'",
                                self.field_name
                            ),
                            source: None,
                        });
                    }
                    let numerator = BigInt::from(previous) * BigInt::from(10u8).pow(*scale as u32);
                    let denominator = BigInt::from(value);
                    if !decimal_division_terminates(&numerator, &denominator) {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "Non-terminating decimal division in product retract for '{}'",
                                self.field_name
                            ),
                            source: None,
                        });
                    }
                    *acc = rounded_decimal(&numerator, &denominator, *precision);
                }
            }
        }
        Ok(())
    }

    fn result(&self) -> crate::Result<ArrayRef> {
        Ok(match &self.state {
            ProductState::I8(acc) => Arc::new(Int8Array::from(vec![*acc])),
            ProductState::I16(acc) => Arc::new(Int16Array::from(vec![*acc])),
            ProductState::I32(acc) => Arc::new(Int32Array::from(vec![*acc])),
            ProductState::I64(acc) => Arc::new(Int64Array::from(vec![*acc])),
            ProductState::F32(acc) => Arc::new(Float32Array::from(vec![*acc])),
            ProductState::F64(acc) => Arc::new(Float64Array::from(vec![*acc])),
            ProductState::Decimal128 {
                precision,
                scale,
                acc,
            } => decimal_array(*precision, *scale, *acc, "product", &self.field_name)?,
        })
    }
}

fn rounded_decimal(numerator: &BigInt, denominator: &BigInt, precision: u8) -> Option<i128> {
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let rounded = if remainder.abs() * 2 >= denominator.abs() {
        quotient
            + if numerator.sign() == denominator.sign() {
                BigInt::from(1)
            } else {
                BigInt::from(-1)
            }
    } else {
        quotient
    };
    rounded
        .to_i128()
        .filter(|value| decimal_fits_precision(*value, precision))
}

fn decimal_division_terminates(numerator: &BigInt, denominator: &BigInt) -> bool {
    let mut a = numerator.abs();
    let mut b = denominator.abs();
    while !b.is_zero() {
        let remainder = &a % &b;
        a = b;
        b = remainder;
    }
    let mut reduced = denominator.abs() / a;
    for factor in [2u8, 5u8] {
        let factor = BigInt::from(factor);
        while (&reduced % &factor).is_zero() {
            reduced /= &factor;
        }
    }
    reduced == BigInt::from(1)
}

// ---------------------------------------------------------------------------
// Min / Max — generic comparator-driven implementation
// ---------------------------------------------------------------------------

/// `min` / `max` accumulator state.  Each variant stores `Option<T>` where
/// `None` means "no non-null value seen yet for the current group".
#[derive(Debug)]
enum MinMaxState {
    Bool(Option<bool>),
    I8(Option<i8>),
    I16(Option<i16>),
    I32(Option<i32>),
    I64(Option<i64>),
    F32(Option<f32>),
    F64(Option<f64>),
    Decimal128 {
        precision: u8,
        scale: i8,
        acc: Option<i128>,
    },
    Date32(Option<i32>),
    /// Paimon `TIME` is encoded as Arrow `Time32(Millisecond)` regardless of
    /// declared precision, so a single accumulator variant suffices.
    Time32Ms(Option<i32>),
    /// Covers `TIMESTAMP` and `TIMESTAMP WITH LOCAL TIME ZONE`, which Java
    /// compares through the same `Timestamp#compareTo` branch. `timezone` is
    /// `Some("UTC")` for the latter, mirroring `paimon_type_to_arrow`, and has to
    /// be carried here because the result array must still match the field's
    /// Arrow type.
    Timestamp {
        unit: TimeUnit,
        timezone: Option<Arc<str>>,
        acc: Option<i64>,
    },
    Utf8(Option<String>),
    /// `BINARY` and `VARBINARY`. Not `BLOB`, which Arrow also maps to `Binary`
    /// but Java's `TypeCheckUtils#isComparable` excludes.
    Binary(Option<Vec<u8>>),
}

fn make_minmax_state(
    field_name: &str,
    data_type: &DataType,
    op: &str,
) -> crate::Result<MinMaxState> {
    Ok(match data_type {
        DataType::Boolean(_) => MinMaxState::Bool(None),
        DataType::TinyInt(_) => MinMaxState::I8(None),
        DataType::SmallInt(_) => MinMaxState::I16(None),
        DataType::Int(_) => MinMaxState::I32(None),
        DataType::BigInt(_) => MinMaxState::I64(None),
        DataType::Float(_) => MinMaxState::F32(None),
        DataType::Double(_) => MinMaxState::F64(None),
        DataType::Decimal(d) => MinMaxState::Decimal128 {
            precision: decimal_precision(d.precision(), field_name)?,
            scale: decimal_scale(d.scale(), field_name)?,
            acc: None,
        },
        DataType::Date(_) => MinMaxState::Date32(None),
        DataType::Time(_) => MinMaxState::Time32Ms(None),
        DataType::Timestamp(t) => MinMaxState::Timestamp {
            unit: timestamp_time_unit(t.precision())?,
            timezone: arrow_timestamp_timezone(field_name, data_type)?,
            acc: None,
        },
        DataType::LocalZonedTimestamp(t) => MinMaxState::Timestamp {
            unit: timestamp_time_unit(t.precision())?,
            timezone: arrow_timestamp_timezone(field_name, data_type)?,
            acc: None,
        },
        DataType::Char(_) | DataType::VarChar(_) => MinMaxState::Utf8(None),
        DataType::Binary(_) | DataType::VarBinary(_) => MinMaxState::Binary(None),
        other => return Err(unsupported_type_error(op, field_name, other)),
    })
}

/// Read the Arrow timezone back out of the very mapping the read schema is built
/// with, so a min/max result array cannot drift from its field's Arrow type.
fn arrow_timestamp_timezone(
    field_name: &str,
    data_type: &DataType,
) -> crate::Result<Option<Arc<str>>> {
    match crate::arrow::paimon_type_to_arrow(data_type)? {
        ArrowDataType::Timestamp(_, timezone) => Ok(timezone),
        other => Err(crate::Error::DataInvalid {
            message: format!(
                "Aggregate column '{field_name}' maps to Arrow type {other:?}, expected a timestamp"
            ),
            source: None,
        }),
    }
}

fn timestamp_time_unit(precision: u32) -> crate::Result<TimeUnit> {
    match precision {
        0 => Ok(TimeUnit::Second),
        1..=3 => Ok(TimeUnit::Millisecond),
        4..=6 => Ok(TimeUnit::Microsecond),
        7..=9 => Ok(TimeUnit::Nanosecond),
        other => Err(crate::Error::Unsupported {
            message: format!("Unsupported TIMESTAMP precision {other} for min/max aggregator"),
        }),
    }
}

fn agg_minmax(
    state: &mut MinMaxState,
    array: &dyn Array,
    row_idx: usize,
    field_name: &str,
    keep_smaller: bool,
    reversed: bool,
) -> crate::Result<()> {
    if array.is_null(row_idx) {
        return Ok(());
    }
    macro_rules! update_primitive {
        ($acc:expr, $ty:ty) => {{
            let v = downcast::<$ty>(array, field_name)?.value(row_idx);
            *$acc = Some(match *$acc {
                None => v,
                Some(prev) => {
                    let take_new = if keep_smaller {
                        if reversed {
                            v < prev
                        } else {
                            v <= prev
                        }
                    } else if reversed {
                        v >= prev
                    } else {
                        v > prev
                    };
                    if take_new {
                        v
                    } else {
                        prev
                    }
                }
            });
        }};
    }
    macro_rules! update_float {
        ($acc:expr, $ty:ty) => {{
            let v = downcast::<$ty>(array, field_name)?.value(row_idx);
            // Match Java `Float.compare` / `Double.compare`, which order NaN
            // greater than any other value (including +Infinity) and compare
            // all NaN representations as equal.  For non-NaN values,
            // `total_cmp` preserves Java's ordering of -0.0 before +0.0.
            *$acc = Some(match *$acc {
                None => v,
                Some(prev) => {
                    let cmp = match (v.is_nan(), prev.is_nan()) {
                        (true, true) => std::cmp::Ordering::Equal,
                        (true, false) => std::cmp::Ordering::Greater,
                        (false, true) => std::cmp::Ordering::Less,
                        (false, false) => v.total_cmp(&prev),
                    };
                    let take_new = if keep_smaller {
                        if reversed {
                            cmp.is_lt()
                        } else {
                            // Java `FieldMinAgg` returns the input on ties.
                            cmp.is_le()
                        }
                    } else if reversed {
                        // Reversed max treats the older input as the
                        // accumulator, so ties select it.
                        cmp.is_ge()
                    } else {
                        // Java `FieldMaxAgg` retains the accumulator on ties.
                        cmp.is_gt()
                    };
                    if take_new {
                        v
                    } else {
                        prev
                    }
                }
            });
        }};
    }
    match state {
        MinMaxState::Bool(acc) => {
            // Java compares BOOLEAN with `Boolean.compare`, i.e. false < true
            // (`InternalRowUtils#compare`), which is what Rust's `bool: Ord` does.
            let v = downcast::<BooleanArray>(array, field_name)?.value(row_idx);
            *acc = Some(match acc.take() {
                None => v,
                Some(prev) => {
                    // Ordering rather than `<`/`>`: comparing bools with order
                    // operators is what `clippy::bool_comparison` objects to, and
                    // this keeps the tie handling identical to the other arms.
                    let ordering = v.cmp(&prev);
                    let take_new = if keep_smaller {
                        if reversed {
                            ordering.is_lt()
                        } else {
                            ordering.is_le()
                        }
                    } else if reversed {
                        ordering.is_ge()
                    } else {
                        ordering.is_gt()
                    };
                    if take_new {
                        v
                    } else {
                        prev
                    }
                }
            });
        }
        MinMaxState::I8(acc) => update_primitive!(acc, Int8Array),
        MinMaxState::I16(acc) => update_primitive!(acc, Int16Array),
        MinMaxState::I32(acc) => update_primitive!(acc, Int32Array),
        MinMaxState::I64(acc) => update_primitive!(acc, Int64Array),
        MinMaxState::F32(acc) => update_float!(acc, Float32Array),
        MinMaxState::F64(acc) => update_float!(acc, Float64Array),
        MinMaxState::Decimal128 { acc, .. } => update_primitive!(acc, Decimal128Array),
        MinMaxState::Date32(acc) => update_primitive!(acc, Date32Array),
        MinMaxState::Time32Ms(acc) => update_primitive!(acc, Time32MillisecondArray),
        MinMaxState::Timestamp { unit, acc, .. } => match unit {
            TimeUnit::Second => update_primitive!(acc, TimestampSecondArray),
            TimeUnit::Millisecond => update_primitive!(acc, TimestampMillisecondArray),
            TimeUnit::Microsecond => update_primitive!(acc, TimestampMicrosecondArray),
            TimeUnit::Nanosecond => update_primitive!(acc, TimestampNanosecondArray),
        },
        MinMaxState::Utf8(acc) => {
            let v = downcast::<StringArray>(array, field_name)?.value(row_idx);
            *acc = Some(match acc.take() {
                None => v.to_string(),
                Some(prev) => {
                    let take_new = if keep_smaller {
                        if reversed {
                            v < prev.as_str()
                        } else {
                            v <= prev.as_str()
                        }
                    } else if reversed {
                        v >= prev.as_str()
                    } else {
                        v > prev.as_str()
                    };
                    if take_new {
                        v.to_string()
                    } else {
                        prev
                    }
                }
            });
        }
        MinMaxState::Binary(acc) => {
            // Java's `byteArrayCompare` is unsigned lexicographic with a length
            // tiebreak, which is exactly `[u8]: Ord`.
            let v = downcast::<BinaryArray>(array, field_name)?.value(row_idx);
            *acc = Some(match acc.take() {
                None => v.to_vec(),
                Some(prev) => {
                    let take_new = if keep_smaller {
                        if reversed {
                            v < prev.as_slice()
                        } else {
                            v <= prev.as_slice()
                        }
                    } else if reversed {
                        v >= prev.as_slice()
                    } else {
                        v > prev.as_slice()
                    };
                    if take_new {
                        v.to_vec()
                    } else {
                        prev
                    }
                }
            });
        }
    }
    Ok(())
}

fn minmax_result(state: &MinMaxState, agg_name: &str, field_name: &str) -> crate::Result<ArrayRef> {
    Ok(match state {
        MinMaxState::Bool(acc) => Arc::new(BooleanArray::from(vec![*acc])),
        MinMaxState::I8(acc) => Arc::new(Int8Array::from(vec![*acc])),
        MinMaxState::I16(acc) => Arc::new(Int16Array::from(vec![*acc])),
        MinMaxState::I32(acc) => Arc::new(Int32Array::from(vec![*acc])),
        MinMaxState::I64(acc) => Arc::new(Int64Array::from(vec![*acc])),
        MinMaxState::F32(acc) => Arc::new(Float32Array::from(vec![*acc])),
        MinMaxState::F64(acc) => Arc::new(Float64Array::from(vec![*acc])),
        MinMaxState::Decimal128 {
            precision,
            scale,
            acc,
        } => decimal_array(*precision, *scale, *acc, agg_name, field_name)?,
        MinMaxState::Date32(acc) => Arc::new(Date32Array::from(vec![*acc])),
        MinMaxState::Time32Ms(acc) => Arc::new(Time32MillisecondArray::from(vec![*acc])),
        MinMaxState::Timestamp {
            unit,
            timezone,
            acc,
        } => match unit {
            // `with_timezone_opt` keeps the result array's Arrow type equal to the
            // field's, which `RecordBatch::try_new` checks.
            TimeUnit::Second => {
                Arc::new(TimestampSecondArray::from(vec![*acc]).with_timezone_opt(timezone.clone()))
            }
            TimeUnit::Millisecond => Arc::new(
                TimestampMillisecondArray::from(vec![*acc]).with_timezone_opt(timezone.clone()),
            ),
            TimeUnit::Microsecond => Arc::new(
                TimestampMicrosecondArray::from(vec![*acc]).with_timezone_opt(timezone.clone()),
            ),
            TimeUnit::Nanosecond => Arc::new(
                TimestampNanosecondArray::from(vec![*acc]).with_timezone_opt(timezone.clone()),
            ),
        },
        MinMaxState::Utf8(acc) => Arc::new(StringArray::from(vec![acc.clone()])),
        MinMaxState::Binary(acc) => Arc::new(BinaryArray::from_opt_vec(vec![acc.as_deref()])),
    })
}

fn reset_minmax(state: &mut MinMaxState) {
    match state {
        MinMaxState::Bool(acc) => *acc = None,
        MinMaxState::I8(acc) => *acc = None,
        MinMaxState::I16(acc) => *acc = None,
        MinMaxState::I32(acc) => *acc = None,
        MinMaxState::I64(acc) => *acc = None,
        MinMaxState::F32(acc) => *acc = None,
        MinMaxState::F64(acc) => *acc = None,
        MinMaxState::Decimal128 { acc, .. } => *acc = None,
        MinMaxState::Date32(acc) => *acc = None,
        MinMaxState::Time32Ms(acc) => *acc = None,
        MinMaxState::Timestamp { acc, .. } => *acc = None,
        MinMaxState::Utf8(acc) => *acc = None,
        MinMaxState::Binary(acc) => *acc = None,
    }
}

#[derive(Debug)]
pub(crate) struct MinAgg {
    field_name: String,
    state: MinMaxState,
}

impl MinAgg {
    pub(crate) fn new(field_name: &str, data_type: &DataType) -> crate::Result<Self> {
        Ok(Self {
            field_name: field_name.to_string(),
            state: make_minmax_state(field_name, data_type, "min")?,
        })
    }
}

impl FieldAggregator for MinAgg {
    fn name(&self) -> &'static str {
        "min"
    }

    fn reset(&mut self) {
        reset_minmax(&mut self.state);
    }

    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        agg_minmax(
            &mut self.state,
            array,
            row_idx,
            &self.field_name,
            true,
            false,
        )
    }

    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        agg_minmax(
            &mut self.state,
            array,
            row_idx,
            &self.field_name,
            true,
            true,
        )
    }

    fn result(&self) -> crate::Result<ArrayRef> {
        minmax_result(&self.state, "min", &self.field_name)
    }
}

#[derive(Debug)]
pub(crate) struct MaxAgg {
    field_name: String,
    state: MinMaxState,
}

impl MaxAgg {
    pub(crate) fn new(field_name: &str, data_type: &DataType) -> crate::Result<Self> {
        Ok(Self {
            field_name: field_name.to_string(),
            state: make_minmax_state(field_name, data_type, "max")?,
        })
    }
}

impl FieldAggregator for MaxAgg {
    fn name(&self) -> &'static str {
        "max"
    }

    fn reset(&mut self) {
        reset_minmax(&mut self.state);
    }

    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        agg_minmax(
            &mut self.state,
            array,
            row_idx,
            &self.field_name,
            false,
            false,
        )
    }

    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        agg_minmax(
            &mut self.state,
            array,
            row_idx,
            &self.field_name,
            false,
            true,
        )
    }

    fn result(&self) -> crate::Result<ArrayRef> {
        minmax_result(&self.state, "max", &self.field_name)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn downcast<'a, T: Array + 'static>(
    array: &'a dyn Array,
    field_name: &str,
) -> crate::Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!(
                "Aggregate column '{field_name}' received Arrow array of unexpected \
                 type {:?}; expected {}",
                array.data_type(),
                std::any::type_name::<T>()
            ),
            source: None,
        })
}

fn decimal_precision(precision: u32, field_name: &str) -> crate::Result<u8> {
    u8::try_from(precision).map_err(|_| crate::Error::Unsupported {
        message: format!(
            "Decimal precision {precision} on field '{field_name}' exceeds u8 (Arrow limit)"
        ),
    })
}

fn decimal_scale(scale: u32, field_name: &str) -> crate::Result<i8> {
    i8::try_from(scale as i32).map_err(|_| crate::Error::Unsupported {
        message: format!(
            "Decimal scale {scale} on field '{field_name}' is out of i8 range (Arrow limit)"
        ),
    })
}

fn overflow_error(agg_name: &str, field_name: &str) -> crate::Error {
    crate::Error::DataInvalid {
        message: format!("Aggregate function '{agg_name}' overflowed on field '{field_name}'"),
        source: None,
    }
}

/// Whether `value` (an unscaled Decimal128 raw value) fits within `precision`
/// decimal digits, i.e. `|value| < 10^precision`. Decimal128 precision is at
/// most 38, so `10^precision` always fits in `u128`; the `checked_pow` guard
/// degrades to "fits" only for impossible precisions.
fn decimal_fits_precision(value: i128, precision: u8) -> bool {
    10u128
        .checked_pow(precision as u32)
        .map(|limit| value.unsigned_abs() < limit)
        .unwrap_or(true)
}

fn decimal_array(
    precision: u8,
    scale: i8,
    value: Option<i128>,
    agg_name: &str,
    field_name: &str,
) -> crate::Result<ArrayRef> {
    let mut builder = Decimal128Builder::with_capacity(1)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| crate::Error::DataInvalid {
            message: format!(
                "Aggregate function '{agg_name}' failed to build Decimal128 array for \
                 field '{field_name}': {e}"
            ),
            source: Some(Box::new(e)),
        })?;
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        BigIntType, BinaryType, CharType, DateType, DecimalType, DoubleType, FloatType, IntType,
        LocalZonedTimestampType, SmallIntType, TimeType, TimestampType, TinyIntType, VarBinaryType,
        VarCharType,
    };
    use arrow_array::builder::Decimal128Builder;

    fn sum_agg(dt: DataType) -> SumAgg {
        SumAgg::new("v", &dt).unwrap()
    }
    fn min_agg(dt: DataType) -> MinAgg {
        MinAgg::new("v", &dt).unwrap()
    }
    fn max_agg(dt: DataType) -> MaxAgg {
        MaxAgg::new("v", &dt).unwrap()
    }

    fn collect_i32(arr: ArrayRef) -> Option<i32> {
        let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        if a.is_null(0) {
            None
        } else {
            Some(a.value(0))
        }
    }

    fn collect_i64(arr: ArrayRef) -> Option<i64> {
        let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        if a.is_null(0) {
            None
        } else {
            Some(a.value(0))
        }
    }

    fn collect_bool(arr: ArrayRef) -> Option<bool> {
        let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        if a.is_null(0) {
            None
        } else {
            Some(a.value(0))
        }
    }

    fn collect_binary(arr: ArrayRef) -> Option<Vec<u8>> {
        let a = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
        if a.is_null(0) {
            None
        } else {
            Some(a.value(0).to_vec())
        }
    }

    fn collect_string(arr: ArrayRef) -> Option<String> {
        let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
        if a.is_null(0) {
            None
        } else {
            Some(a.value(0).to_string())
        }
    }

    #[test]
    fn test_sum_int_aggregates_non_null_values() {
        let mut agg = sum_agg(DataType::Int(IntType::new()));
        let arr = Int32Array::from(vec![Some(1), None, Some(2), Some(3)]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect_i32(agg.result().unwrap()), Some(6));
    }

    #[test]
    fn test_sum_all_null_returns_null() {
        let mut agg = sum_agg(DataType::BigInt(BigIntType::new()));
        let arr = Int64Array::from(vec![None::<i64>, None]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect_i64(agg.result().unwrap()), None);
    }

    #[test]
    fn test_sum_default_wraps_and_optional_overflow_check_rejects() {
        let mut agg = sum_agg(DataType::Int(IntType::new()));
        let arr = Int32Array::from(vec![i32::MAX, 1]);
        agg.agg(&arr, 0).unwrap();
        agg.agg(&arr, 1).unwrap();
        assert_eq!(collect_i32(agg.result().unwrap()), Some(i32::MIN));
        let mut checked =
            SumAgg::new_with_overflow("v", &DataType::Int(IntType::new()), true).unwrap();
        checked.agg(&arr, 0).unwrap();
        let err = checked.agg(&arr, 1).unwrap_err();
        assert!(
            matches!(err, crate::Error::DataInvalid { message, .. } if message.contains("overflowed"))
        );
    }

    #[test]
    fn test_sum_rejects_non_numeric_type() {
        let err = SumAgg::new("v", &DataType::VarChar(VarCharType::new(255).unwrap())).unwrap_err();
        assert!(matches!(err, crate::Error::ConfigInvalid { message } if message.contains("sum")));
    }

    #[test]
    fn test_sum_reset_clears_state() {
        let mut agg = sum_agg(DataType::Int(IntType::new()));
        let arr = Int32Array::from(vec![Some(10)]);
        agg.agg(&arr, 0).unwrap();
        agg.reset();
        assert_eq!(collect_i32(agg.result().unwrap()), None);
    }

    #[test]
    fn test_sum_float_skips_null_and_handles_partial() {
        let mut agg = sum_agg(DataType::Double(DoubleType::new()));
        let arr = Float64Array::from(vec![Some(1.5), None, Some(2.5)]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        let a = agg.result().unwrap();
        let v = a.as_any().downcast_ref::<Float64Array>().unwrap().value(0);
        assert!((v - 4.0).abs() < 1e-9);
    }

    #[test]
    fn test_sum_decimal_aggregates_raw_values() {
        let mut agg = sum_agg(DataType::Decimal(DecimalType::new(10, 2).unwrap()));
        let mut b = Decimal128Builder::with_capacity(2)
            .with_precision_and_scale(10, 2)
            .unwrap();
        b.append_value(100); // 1.00
        b.append_value(250); // 2.50
        let arr = b.finish();
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        let out = agg.result().unwrap();
        let out_arr = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(out_arr.value(0), 350); // 3.50
    }

    #[test]
    fn test_sum_decimal_in_range_keeps_value() {
        // DECIMAL(3,2): 1.23 + 4.56 = 5.79 (raw 579) still fits precision 3.
        let mut agg = sum_agg(DataType::Decimal(DecimalType::new(3, 2).unwrap()));
        let mut b = Decimal128Builder::with_capacity(2)
            .with_precision_and_scale(3, 2)
            .unwrap();
        b.append_value(123); // 1.23
        b.append_value(456); // 4.56
        let arr = b.finish();
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        let out = agg.result().unwrap();
        let out_arr = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert!(!out_arr.is_null(0));
        assert_eq!(out_arr.value(0), 579); // 5.79
    }

    #[test]
    fn test_sum_decimal_precision_overflow_yields_null() {
        // DECIMAL(3,2) tops out at 9.99 (raw 999). 9.99 + 0.01 = 10.00 (raw
        // 1000) needs precision 4, so the sum no longer fits and must become
        // NULL — matching Java `Decimal.fromBigDecimal` returning null instead
        // of persisting an out-of-range value.
        let mut agg = sum_agg(DataType::Decimal(DecimalType::new(3, 2).unwrap()));
        let mut b = Decimal128Builder::with_capacity(2)
            .with_precision_and_scale(3, 2)
            .unwrap();
        b.append_value(999); // 9.99
        b.append_value(1); // 0.01
        let arr = b.finish();
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        let out = agg.result().unwrap();
        let out_arr = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert!(
            out_arr.is_null(0),
            "precision-overflowing decimal sum must be NULL, got {}",
            out_arr.value(0)
        );
    }

    #[test]
    fn test_sum_decimal_recovers_after_intermediate_precision_overflow() {
        let mut agg = sum_agg(DataType::Decimal(DecimalType::new(3, 0).unwrap()));
        let mut builder = Decimal128Builder::with_capacity(3)
            .with_precision_and_scale(3, 0)
            .unwrap();
        builder.append_value(900);
        builder.append_value(200);
        builder.append_value(-200);
        let arr = builder.finish();

        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }

        let result = agg.result().unwrap();
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            -200
        );
    }

    #[test]
    fn test_sum_decimal_recovers_after_intermediate_i128_overflow() {
        let mut agg = sum_agg(DataType::Decimal(DecimalType::new(38, 0).unwrap()));
        let max_decimal = 10_i128.pow(38) - 1;
        let mut builder = Decimal128Builder::with_capacity(3)
            .with_precision_and_scale(38, 0)
            .unwrap();
        builder.append_value(max_decimal);
        builder.append_value(max_decimal);
        builder.append_value(-max_decimal);
        let arr = builder.finish();

        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }

        let result = agg.result().unwrap();
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            -max_decimal
        );
    }

    #[test]
    fn test_sum_decimal_reversed_recovers_after_intermediate_i128_overflow() {
        let mut agg = sum_agg(DataType::Decimal(DecimalType::new(38, 0).unwrap()));
        let max_decimal = 10_i128.pow(38) - 1;
        let mut builder = Decimal128Builder::with_capacity(3)
            .with_precision_and_scale(38, 0)
            .unwrap();
        builder.append_value(max_decimal);
        builder.append_value(max_decimal);
        builder.append_value(-max_decimal);
        let arr = builder.finish();

        agg.agg(&arr, 0).unwrap();
        agg.agg_reversed(&arr, 1).unwrap();
        agg.agg_reversed(&arr, 2).unwrap();

        let result = agg.result().unwrap();
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            -max_decimal
        );
    }

    #[test]
    fn test_product_int_aggregates() {
        let mut agg = ProductAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let arr = Int32Array::from(vec![Some(2), None, Some(3), Some(4)]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect_i32(agg.result().unwrap()), Some(24));
    }

    #[test]
    fn test_product_reversed_aggregates_input() {
        let mut agg = ProductAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let arr = Int32Array::from(vec![Some(2), Some(3)]);
        agg.agg(&arr, 0).unwrap();
        agg.agg_reversed(&arr, 1).unwrap();
        assert_eq!(collect_i32(agg.result().unwrap()), Some(6));
    }

    #[test]
    fn test_product_default_wraps_and_optional_overflow_check_rejects() {
        let mut agg = ProductAgg::new("v", &DataType::SmallInt(SmallIntType::new())).unwrap();
        let arr = Int16Array::from(vec![i16::MAX, 2]);
        agg.agg(&arr, 0).unwrap();
        agg.agg(&arr, 1).unwrap();
        assert_eq!(
            agg.result()
                .unwrap()
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(0),
            -2
        );
        let mut checked =
            ProductAgg::new_with_overflow("v", &DataType::SmallInt(SmallIntType::new()), true)
                .unwrap();
        checked.agg(&arr, 0).unwrap();
        let err = checked.agg(&arr, 1).unwrap_err();
        assert!(matches!(err, crate::Error::DataInvalid { .. }));
    }

    #[test]
    fn test_product_all_null_returns_null() {
        let mut agg = ProductAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let arr = Int32Array::from(vec![None::<i32>, None]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect_i32(agg.result().unwrap()), None);
    }

    #[test]
    fn test_product_decimal_rebases_scale_like_java() {
        let mut agg =
            ProductAgg::new("v", &DataType::Decimal(DecimalType::new(10, 2).unwrap())).unwrap();
        let mut builder = Decimal128Builder::with_capacity(2)
            .with_precision_and_scale(10, 2)
            .unwrap();
        builder.append_value(150);
        builder.append_value(200);
        let input = builder.finish();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        assert_eq!(
            agg.result()
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            300
        );
        agg.retract(&input, 1).unwrap();
        assert_eq!(
            agg.result()
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            150
        );
    }

    #[test]
    fn test_sum_retract_before_add_starts_negative() {
        let mut agg = SumAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let input = Int32Array::from(vec![3, 10]);
        agg.retract(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        assert_eq!(collect_i32(agg.result().unwrap()), Some(7));
    }

    #[test]
    fn test_product_decimal_retract_rejects_nonterminating_division() {
        let mut agg =
            ProductAgg::new("v", &DataType::Decimal(DecimalType::new(10, 2).unwrap())).unwrap();
        let mut builder = Decimal128Builder::with_capacity(2)
            .with_precision_and_scale(10, 2)
            .unwrap();
        builder.append_value(100);
        builder.append_value(300);
        let input = builder.finish();
        agg.agg(&input, 0).unwrap();
        let err = agg.retract(&input, 1).unwrap_err();
        assert!(
            matches!(err, crate::Error::DataInvalid { message, .. } if message.contains("Non-terminating"))
        );
    }

    #[test]
    fn test_min_int_picks_smallest_skipping_null() {
        let mut agg = min_agg(DataType::Int(IntType::new()));
        let arr = Int32Array::from(vec![Some(3), None, Some(1), Some(2)]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect_i32(agg.result().unwrap()), Some(1));
    }

    #[test]
    fn test_max_string_picks_lex_largest() {
        let mut agg = max_agg(DataType::Char(CharType::new(8).unwrap()));
        let arr = StringArray::from(vec![Some("ant"), None, Some("zebra"), Some("bee")]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(
            collect_string(agg.result().unwrap()),
            Some("zebra".to_string())
        );
    }

    #[test]
    fn test_float32_min_max_matches_java_ordering() {
        let aggregate = |values: &[f32], keep_smaller: bool| {
            let arr = Float32Array::from_iter_values(values.iter().copied());
            let mut agg: Box<dyn FieldAggregator> = if keep_smaller {
                Box::new(min_agg(DataType::Float(FloatType::new())))
            } else {
                Box::new(max_agg(DataType::Float(FloatType::new())))
            };
            for i in 0..arr.len() {
                agg.agg(&arr, i).unwrap();
            }
            let result = agg.result().unwrap();
            result
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0)
        };

        let negative_nan = f32::from_bits(0xffc0_0001);
        let positive_nan = f32::from_bits(0x7fc0_0002);
        assert!(negative_nan.is_nan());
        assert!(positive_nan.is_nan());

        // Every NaN sorts after finite values, including when the accumulator
        // already contains NaN before the finite value arrives.
        for nan in [negative_nan, positive_nan] {
            assert_eq!(aggregate(&[nan, 1.0], true).to_bits(), 1.0f32.to_bits());
            assert_eq!(aggregate(&[nan, 1.0], false).to_bits(), nan.to_bits());
            assert_eq!(aggregate(&[1.0, nan], true).to_bits(), 1.0f32.to_bits());
            assert_eq!(aggregate(&[1.0, nan], false).to_bits(), nan.to_bits());
        }

        assert_eq!(aggregate(&[0.0, -0.0], true).to_bits(), (-0.0f32).to_bits());
        assert_eq!(aggregate(&[-0.0, 0.0], false).to_bits(), 0.0f32.to_bits());
        assert_eq!(aggregate(&[3.0, -2.0, 1.0], true), -2.0);
        assert_eq!(aggregate(&[3.0, -2.0, 1.0], false), 3.0);

        // Java canonicalizes NaNs for comparison only.  On equal values,
        // `FieldMinAgg` takes the new input while `FieldMaxAgg` retains the
        // accumulator, without rewriting either NaN's exact bits.
        for values in [[negative_nan, positive_nan], [positive_nan, negative_nan]] {
            assert_eq!(aggregate(&values, true).to_bits(), values[1].to_bits());
            assert_eq!(aggregate(&values, false).to_bits(), values[0].to_bits());
        }
    }

    #[test]
    fn test_float64_min_max_matches_java_ordering() {
        let aggregate = |values: &[f64], keep_smaller: bool| {
            let arr = Float64Array::from_iter_values(values.iter().copied());
            let mut agg: Box<dyn FieldAggregator> = if keep_smaller {
                Box::new(min_agg(DataType::Double(DoubleType::new())))
            } else {
                Box::new(max_agg(DataType::Double(DoubleType::new())))
            };
            for i in 0..arr.len() {
                agg.agg(&arr, i).unwrap();
            }
            let result = agg.result().unwrap();
            result
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
        };

        let negative_nan = f64::from_bits(0xfff8_0000_0000_0001);
        let positive_nan = f64::from_bits(0x7ff8_0000_0000_0002);
        assert!(negative_nan.is_nan());
        assert!(positive_nan.is_nan());

        for nan in [negative_nan, positive_nan] {
            assert_eq!(aggregate(&[nan, 1.0], true).to_bits(), 1.0f64.to_bits());
            assert_eq!(aggregate(&[nan, 1.0], false).to_bits(), nan.to_bits());
            assert_eq!(aggregate(&[1.0, nan], true).to_bits(), 1.0f64.to_bits());
            assert_eq!(aggregate(&[1.0, nan], false).to_bits(), nan.to_bits());
        }

        assert_eq!(aggregate(&[0.0, -0.0], true).to_bits(), (-0.0f64).to_bits());
        assert_eq!(aggregate(&[-0.0, 0.0], false).to_bits(), 0.0f64.to_bits());
        assert_eq!(aggregate(&[3.0, -2.0, 1.0], true), -2.0);
        assert_eq!(aggregate(&[3.0, -2.0, 1.0], false), 3.0);

        for values in [[negative_nan, positive_nan], [positive_nan, negative_nan]] {
            assert_eq!(aggregate(&values, true).to_bits(), values[1].to_bits());
            assert_eq!(aggregate(&values, false).to_bits(), values[0].to_bits());
        }
    }

    #[test]
    fn test_min_max_reversed_nan_ties_match_java_operand_order() {
        let current = f32::from_bits(0xffc0_0001);
        let older = f32::from_bits(0x7fc0_0002);
        let arr = Float32Array::from(vec![Some(current), Some(older)]);

        let mut min = min_agg(DataType::Float(FloatType::new()));
        min.agg(&arr, 0).unwrap();
        min.agg_reversed(&arr, 1).unwrap();
        let min_result = min.result().unwrap();
        assert_eq!(
            min_result
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            current.to_bits()
        );

        let mut max = max_agg(DataType::Float(FloatType::new()));
        max.agg(&arr, 0).unwrap();
        max.agg_reversed(&arr, 1).unwrap();
        let max_result = max.result().unwrap();
        assert_eq!(
            max_result
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            older.to_bits()
        );
    }

    #[test]
    fn test_min_max_all_null_returns_null() {
        let mut agg = max_agg(DataType::Int(IntType::new()));
        let arr = Int32Array::from(vec![None::<i32>, None]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect_i32(agg.result().unwrap()), None);
    }

    #[test]
    fn test_min_rejects_unsupported_type() {
        // Java's `TypeCheckUtils#isComparable` rejects exactly MAP, MULTISET, ROW,
        // ARRAY, VECTOR, VARIANT and BLOB. BINARY and VARBINARY remain comparable,
        // while BLOB is intentionally rejected even though it is represented as LargeBinary.
        let err = MinAgg::new("v", &DataType::Blob(crate::spec::BlobType::new())).unwrap_err();
        assert!(matches!(err, crate::Error::ConfigInvalid { message } if message.contains("min")));
    }

    #[test]
    fn test_min_max_boolean() {
        // `Boolean.compare` in Java's `InternalRowUtils#compare`: false < true.
        let arr = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);
        let mut min = min_agg(DataType::Boolean(crate::spec::BooleanType::new()));
        let mut max = max_agg(DataType::Boolean(crate::spec::BooleanType::new()));
        for i in 0..arr.len() {
            min.agg(&arr, i).unwrap();
            max.agg(&arr, i).unwrap();
        }
        assert_eq!(collect_bool(min.result().unwrap()), Some(false));
        assert_eq!(collect_bool(max.result().unwrap()), Some(true));
    }

    #[test]
    fn test_min_max_binary_compares_bytes_as_unsigned() {
        // `byteArrayCompare` masks with 0xff, so 0x80 is above 0x7f, and a prefix
        // sorts before the longer value it prefixes.
        let arr = BinaryArray::from_opt_vec(vec![
            Some(&[0x7fu8, 0x01][..]),
            Some(&[0x80u8][..]),
            Some(&[0x7fu8][..]),
        ]);
        for dt in [
            DataType::Binary(BinaryType::new(2).unwrap()),
            DataType::VarBinary(VarBinaryType::new(8).unwrap()),
        ] {
            let mut min = min_agg(dt.clone());
            let mut max = max_agg(dt);
            for i in 0..arr.len() {
                min.agg(&arr, i).unwrap();
                max.agg(&arr, i).unwrap();
            }
            assert_eq!(collect_binary(min.result().unwrap()), Some(vec![0x7f]));
            assert_eq!(collect_binary(max.result().unwrap()), Some(vec![0x80]));
        }
    }

    #[test]
    fn test_min_max_local_zoned_timestamp_keeps_its_arrow_timezone() {
        let dt = DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap());
        let arr = TimestampMillisecondArray::from(vec![Some(30), Some(10), Some(20)])
            .with_timezone_opt(Some("UTC"));
        let mut min = min_agg(dt.clone());
        let mut max = max_agg(dt.clone());
        for i in 0..arr.len() {
            min.agg(&arr, i).unwrap();
            max.agg(&arr, i).unwrap();
        }
        let min_result = min.result().unwrap();
        assert_eq!(
            min_result
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            10
        );

        // The trap: an accumulator that dropped the timezone still holds the right
        // number, but the field's Arrow type no longer matches and the batch the
        // merge engine builds is rejected.
        let arrow_type = crate::arrow::paimon_type_to_arrow(&dt).unwrap();
        assert_eq!(min_result.data_type(), &arrow_type);
        let schema =
            arrow_schema::Schema::new(vec![arrow_schema::Field::new("v", arrow_type, true)]);
        arrow_array::RecordBatch::try_new(Arc::new(schema), vec![max.result().unwrap()]).unwrap();
    }

    #[test]
    fn test_min_max_timestamp_zero_uses_seconds() {
        let dt = DataType::Timestamp(TimestampType::new(0).unwrap());
        let arr = TimestampSecondArray::from(vec![Some(30), Some(10), Some(20)]);
        let mut min = min_agg(dt.clone());
        let mut max = max_agg(dt);
        for i in 0..arr.len() {
            min.agg(&arr, i).unwrap();
            max.agg(&arr, i).unwrap();
        }
        assert_eq!(
            min.result()
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap()
                .value(0),
            10
        );
        assert_eq!(
            max.result()
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap()
                .value(0),
            30
        );
    }

    #[test]
    fn test_min_max_date_and_timestamp_supported() {
        // Date32
        let mut agg = min_agg(DataType::Date(DateType::new()));
        let arr = Date32Array::from(vec![Some(100), Some(50), Some(200)]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        let v = agg.result().unwrap();
        let v = v.as_any().downcast_ref::<Date32Array>().unwrap().value(0);
        assert_eq!(v, 50);

        // Timestamp(6) → Microsecond
        let mut agg = max_agg(DataType::Timestamp(TimestampType::new(6).unwrap()));
        let arr =
            TimestampMicrosecondArray::from(vec![Some(1_000_000), Some(2_000_000), Some(500_000)]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        let v = agg.result().unwrap();
        let v = v
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0);
        assert_eq!(v, 2_000_000);
    }

    #[test]
    fn test_min_max_time_supported() {
        // Paimon `TIME` is always stored as Arrow Time32(Millisecond) by
        // `paimon_type_to_arrow`, so milliseconds is the only carrier here.
        let mut agg = min_agg(DataType::Time(TimeType::new(3).unwrap()));
        let arr = Time32MillisecondArray::from(vec![Some(60_000), Some(30_000), Some(90_000)]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        let v = agg.result().unwrap();
        let v = v
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap()
            .value(0);
        assert_eq!(v, 30_000);
    }

    #[test]
    fn test_tinyint_sum_supported() {
        let mut agg = sum_agg(DataType::TinyInt(TinyIntType::new()));
        let arr = Int8Array::from(vec![Some(1i8), Some(2)]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        let out = agg.result().unwrap();
        let v = out.as_any().downcast_ref::<Int8Array>().unwrap().value(0);
        assert_eq!(v, 3);
    }
}
