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
//! `sum` operates on every integer / floating / Decimal numeric type.
//! `product` accepts the same numeric family except DECIMAL — basic mode does
//! not yet implement BigDecimal-style scale rebasing for Decimal product, so
//! Decimal columns are rejected at construction.  Integer overflow on either
//! aggregator is reported as [`Error::DataInvalid`] so silent wrap cannot
//! produce misleading aggregated values.  A Decimal `sum` whose result no
//! longer fits the declared precision yields a NULL cell, matching Java
//! `DecimalUtils.add` / `Decimal.fromBigDecimal` (which return null on
//! precision overflow rather than throwing).
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
    Array, ArrayRef, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, Time32MillisecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
};
use arrow_schema::TimeUnit;

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
}

impl SumAgg {
    pub(crate) fn new(field_name: &str, data_type: &DataType) -> crate::Result<Self> {
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
                    Some(prev) => prev
                        .checked_add(v)
                        .ok_or_else(|| overflow_error("sum", &self.field_name))?,
                });
            }
            SumState::I16(acc) => {
                let v = downcast::<Int16Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => prev
                        .checked_add(v)
                        .ok_or_else(|| overflow_error("sum", &self.field_name))?,
                });
            }
            SumState::I32(acc) => {
                let v = downcast::<Int32Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => prev
                        .checked_add(v)
                        .ok_or_else(|| overflow_error("sum", &self.field_name))?,
                });
            }
            SumState::I64(acc) => {
                let v = downcast::<Int64Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => prev
                        .checked_add(v)
                        .ok_or_else(|| overflow_error("sum", &self.field_name))?,
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
            SumState::Decimal128 { acc, .. } => {
                let v = downcast::<Decimal128Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => prev
                        .checked_add(v)
                        .ok_or_else(|| overflow_error("sum", &self.field_name))?,
                });
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
                // declared precision, so an overflowing sum yields a NULL cell
                // rather than a silently out-of-range Decimal.
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
    // DECIMAL `product` is intentionally rejected at construction (see
    // `ProductAgg::new`); add a variant here when the BigDecimal-style
    // scale handling lands.
}

#[derive(Debug)]
pub(crate) struct ProductAgg {
    field_name: String,
    state: ProductState,
}

impl ProductAgg {
    pub(crate) fn new(field_name: &str, data_type: &DataType) -> crate::Result<Self> {
        let state = match data_type {
            DataType::TinyInt(_) => ProductState::I8(None),
            DataType::SmallInt(_) => ProductState::I16(None),
            DataType::Int(_) => ProductState::I32(None),
            DataType::BigInt(_) => ProductState::I64(None),
            DataType::Float(_) => ProductState::F32(None),
            DataType::Double(_) => ProductState::F64(None),
            // Decimal `product` would need BigDecimal-style scale rebasing
            // (multiply raw i128, then divide by 10^scale, with precision
            // checks).  The basic mode does not implement that yet, so we
            // reject DECIMAL columns explicitly rather than silently produce
            // a scale-shifted result.
            DataType::Decimal(_) => {
                return Err(crate::Error::ConfigInvalid {
                    message: format!(
                        "Aggregate function 'product' on DECIMAL field '{field_name}' is not \
                         supported in the basic mode; use a BIGINT/DOUBLE column or wait for a \
                         follow-up commit that adds Decimal product semantics aligned with Java \
                         BigDecimal"
                    ),
                });
            }
            other => return Err(unsupported_type_error("product", field_name, other)),
        };
        Ok(Self {
            field_name: field_name.to_string(),
            state,
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
                    Some(prev) => prev
                        .checked_mul(v)
                        .ok_or_else(|| overflow_error("product", &self.field_name))?,
                });
            }
            ProductState::I16(acc) => {
                let v = downcast::<Int16Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => prev
                        .checked_mul(v)
                        .ok_or_else(|| overflow_error("product", &self.field_name))?,
                });
            }
            ProductState::I32(acc) => {
                let v = downcast::<Int32Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => prev
                        .checked_mul(v)
                        .ok_or_else(|| overflow_error("product", &self.field_name))?,
                });
            }
            ProductState::I64(acc) => {
                let v = downcast::<Int64Array>(array, &self.field_name)?.value(row_idx);
                *acc = Some(match *acc {
                    None => v,
                    Some(prev) => prev
                        .checked_mul(v)
                        .ok_or_else(|| overflow_error("product", &self.field_name))?,
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
        })
    }
}

// ---------------------------------------------------------------------------
// Min / Max — generic comparator-driven implementation
// ---------------------------------------------------------------------------

/// `min` / `max` accumulator state.  Each variant stores `Option<T>` where
/// `None` means "no non-null value seen yet for the current group".
#[derive(Debug)]
enum MinMaxState {
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
    Timestamp {
        unit: TimeUnit,
        acc: Option<i64>,
    },
    Utf8(Option<String>),
}

fn make_minmax_state(
    field_name: &str,
    data_type: &DataType,
    op: &str,
) -> crate::Result<MinMaxState> {
    Ok(match data_type {
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
            acc: None,
        },
        DataType::Char(_) | DataType::VarChar(_) => MinMaxState::Utf8(None),
        other => return Err(unsupported_type_error(op, field_name, other)),
    })
}

fn timestamp_time_unit(precision: u32) -> crate::Result<TimeUnit> {
    match precision {
        0..=3 => Ok(TimeUnit::Millisecond),
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
                    if (keep_smaller && v < prev) || (!keep_smaller && v > prev) {
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
            // greater than any other value (including +Infinity).  Using
            // `total_cmp` makes that ordering explicit and deterministic.
            *$acc = Some(match *$acc {
                None => v,
                Some(prev) => {
                    let cmp = v.total_cmp(&prev);
                    let take_new = if keep_smaller {
                        cmp.is_lt()
                    } else {
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
        MinMaxState::I8(acc) => update_primitive!(acc, Int8Array),
        MinMaxState::I16(acc) => update_primitive!(acc, Int16Array),
        MinMaxState::I32(acc) => update_primitive!(acc, Int32Array),
        MinMaxState::I64(acc) => update_primitive!(acc, Int64Array),
        MinMaxState::F32(acc) => update_float!(acc, Float32Array),
        MinMaxState::F64(acc) => update_float!(acc, Float64Array),
        MinMaxState::Decimal128 { acc, .. } => update_primitive!(acc, Decimal128Array),
        MinMaxState::Date32(acc) => update_primitive!(acc, Date32Array),
        MinMaxState::Time32Ms(acc) => update_primitive!(acc, Time32MillisecondArray),
        MinMaxState::Timestamp { unit, acc } => match unit {
            TimeUnit::Millisecond => update_primitive!(acc, TimestampMillisecondArray),
            TimeUnit::Microsecond => update_primitive!(acc, TimestampMicrosecondArray),
            TimeUnit::Nanosecond => update_primitive!(acc, TimestampNanosecondArray),
            other => {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Timestamp with unit {other:?} not expected for field '{field_name}'"
                    ),
                    source: None,
                });
            }
        },
        MinMaxState::Utf8(acc) => {
            let v = downcast::<StringArray>(array, field_name)?.value(row_idx);
            *acc = Some(match acc.take() {
                None => v.to_string(),
                Some(prev) => {
                    let take_new = if keep_smaller {
                        v < prev.as_str()
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
    }
    Ok(())
}

fn minmax_result(state: &MinMaxState, agg_name: &str, field_name: &str) -> crate::Result<ArrayRef> {
    Ok(match state {
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
        MinMaxState::Timestamp { unit, acc } => match unit {
            TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(vec![*acc])),
            TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(vec![*acc])),
            TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(vec![*acc])),
            other => {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Timestamp with unit {other:?} not expected for field '{field_name}'"
                    ),
                    source: None,
                });
            }
        },
        MinMaxState::Utf8(acc) => Arc::new(StringArray::from(vec![acc.clone()])),
    })
}

fn reset_minmax(state: &mut MinMaxState) {
    match state {
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
        agg_minmax(&mut self.state, array, row_idx, &self.field_name, true)
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
        agg_minmax(&mut self.state, array, row_idx, &self.field_name, false)
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
        BigIntType, CharType, DateType, DecimalType, DoubleType, FloatType, IntType, SmallIntType,
        TimeType, TimestampType, TinyIntType, VarCharType,
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
    fn test_sum_rejects_overflow() {
        let mut agg = sum_agg(DataType::Int(IntType::new()));
        let arr = Int32Array::from(vec![i32::MAX, 1]);
        agg.agg(&arr, 0).unwrap();
        let err = agg.agg(&arr, 1).unwrap_err();
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
    fn test_product_int_aggregates() {
        let mut agg = ProductAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let arr = Int32Array::from(vec![Some(2), None, Some(3), Some(4)]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect_i32(agg.result().unwrap()), Some(24));
    }

    #[test]
    fn test_product_rejects_overflow() {
        let mut agg = ProductAgg::new("v", &DataType::SmallInt(SmallIntType::new())).unwrap();
        let arr = Int16Array::from(vec![i16::MAX, 2]);
        agg.agg(&arr, 0).unwrap();
        let err = agg.agg(&arr, 1).unwrap_err();
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
    fn test_product_rejects_decimal_until_scale_handling_lands() {
        // DECIMAL multiplication needs BigDecimal-style scale rebasing; the
        // basic mode rejects it explicitly instead of silently shifting the
        // implied scale.
        let err =
            ProductAgg::new("v", &DataType::Decimal(DecimalType::new(10, 2).unwrap())).unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message } if message.contains("DECIMAL"))
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
    fn test_min_max_treat_nan_as_largest() {
        // Match Java `Float.compare(NaN, x) > 0`: NaN is greater than every
        // other value, so min skips it and max picks it.
        let mut min = min_agg(DataType::Float(FloatType::new()));
        let arr = Float32Array::from(vec![Some(f32::NAN), Some(1.0), Some(0.5)]);
        for i in 0..arr.len() {
            min.agg(&arr, i).unwrap();
        }
        let v = min.result().unwrap();
        let v = v.as_any().downcast_ref::<Float32Array>().unwrap().value(0);
        assert!((v - 0.5).abs() < 1e-6);

        let mut max = max_agg(DataType::Float(FloatType::new()));
        for i in 0..arr.len() {
            max.agg(&arr, i).unwrap();
        }
        let v = max.result().unwrap();
        let v = v.as_any().downcast_ref::<Float32Array>().unwrap().value(0);
        assert!(v.is_nan(), "max should pick NaN, got {v}");
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
        // Boolean has no <, > defined for min/max in Paimon basic mode.
        let err =
            MinAgg::new("v", &DataType::Boolean(crate::spec::BooleanType::new())).unwrap_err();
        assert!(matches!(err, crate::Error::ConfigInvalid { message } if message.contains("min")));
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
