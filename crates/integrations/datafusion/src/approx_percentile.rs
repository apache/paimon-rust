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

//! Spark's discrete approximate percentile, using its mergeable GK summary.

mod quantile_summary;
mod sliding;
mod validation;

pub(crate) use validation::ValidateParameters;

use std::collections::HashSet;
use std::sync::Arc;

use bigdecimal::{BigDecimal, RoundingMode, ToPrimitive};
use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, Date32Array, Date64Array, Decimal128Array, Float64Array,
    Int64Array, ListArray, RecordBatch, UInt8Array,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, FieldRef, Float16Type, IntervalDayTime,
    IntervalMonthDayNano, IntervalUnit, Schema, TimeUnit,
};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;

use quantile_summary::QuantileSummary;

const DEFAULT_ACCURACY: u32 = 10_000;

pub(crate) fn register(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(ApproxPercentile {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["percentile_approx".into()],
    }));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ApproxPercentile {
    signature: Signature,
    aliases: Vec<String>,
}

fn decoded(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => decoded(value),
        value => value,
    }
}

impl AggregateUDFImpl for ApproxPercentile {
    fn name(&self) -> &str {
        "approx_percentile"
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if !(2..=3).contains(&types.len()) {
            return Err(error(
                "approx_percentile expects value, percentage[, accuracy]",
            ));
        }
        let value = match decoded(&types[0]) {
            DataType::Decimal32(p, s) | DataType::Decimal64(p, s) | DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
                if !(1..=38).contains(p) { return Err(error("approx_percentile requires Spark decimal precision between 1 and 38")); }
                DataType::Decimal128(*p, *s)
            }
            DataType::Null | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Float64,
            ty if ty.is_numeric() || matches!(ty, DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) | DataType::Interval(_)) => ty.clone(),
            ty => return Err(error(format!("approx_percentile requires numeric, date, timestamp or ANSI interval input, got {ty}"))),
        };
        let percentage_type = |ty: &DataType, scalar: bool| -> Result<()> {
            if decoded(ty).is_numeric()
                || decoded(ty).is_null()
                || (scalar
                    && matches!(
                        decoded(ty),
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
                    ))
            {
                Ok(())
            } else {
                Err(error(
                    "approx_percentile percentage must be numeric or an array of numbers",
                ))
            }
        };
        let percentage = match decoded(&types[1]) {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => {
                percentage_type(field.data_type(), false)?;
                DataType::List(Arc::new(Field::new(
                    "item",
                    percentage_data_type(field.data_type()),
                    true,
                )))
            }
            ty => {
                percentage_type(ty, true)?;
                percentage_data_type(ty)
            }
        };
        let mut result = vec![value, percentage];
        if let Some(accuracy) = types.get(2) {
            if !decoded(accuracy).is_integer() && !decoded(accuracy).is_null() {
                return Err(error(
                    "approx_percentile accuracy must be an integer constant",
                ));
            }
            result.push(DataType::Int64);
        }
        Ok(result)
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        let types = self.coerce_types(types)?;
        Ok(if matches!(types[1], DataType::List(_)) {
            DataType::List(Arc::new(Field::new(
                "item",
                types[0].clone(),
                matches!(types[0], DataType::Decimal128(_, _)),
            )))
        } else {
            types[0].clone()
        })
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let data_type = if args.is_distinct {
            DataType::List(Arc::new(Field::new(
                "item",
                args.input_fields[0].data_type().clone(),
                false,
            )))
        } else {
            DataType::Binary
        };
        Ok(vec![
            Arc::new(Field::new(
                format!("{}[quantiles]", args.name),
                data_type,
                false,
            )),
            Arc::new(Field::new(
                format!("{}[interval_kind]", args.name),
                DataType::UInt8,
                false,
            )),
        ])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(self.create(args)?))
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(sliding::SlidingPercentileAccumulator::new(
            self.create(args)?,
        )))
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }
}

impl ApproxPercentile {
    fn create(&self, args: AccumulatorArgs) -> Result<PercentileAccumulator> {
        let percentages = read_percentages(constant(&args.exprs[1], "percentage")?)?;
        let accuracy = if let Some(expr) = args.exprs.get(2) {
            read_accuracy(constant(expr, "accuracy")?)?
        } else {
            DEFAULT_ACCURACY
        };
        Ok(PercentileAccumulator {
            summary: QuantileSummary::new(accuracy),
            accuracy,
            percentages,
            input_type: args.expr_fields[0].data_type().clone(),
            return_type: args.return_type().clone(),
            distinct: args.is_distinct.then(HashSet::new),
            distinct_bytes: 0,
            interval_kind: 0,
        })
    }
}

fn percentage_data_type(ty: &DataType) -> DataType {
    match decoded(ty) {
        // Avoid Arrow's lossy Decimal -> Double cast before rank selection.
        DataType::Decimal32(p, s)
        | DataType::Decimal64(p, s)
        | DataType::Decimal128(p, s)
        | DataType::Decimal256(p, s) => DataType::Decimal128(*p, *s),
        _ => DataType::Float64,
    }
}

fn read_percentages(value: ScalarValue) -> Result<Vec<f64>> {
    let percentages = match value {
        ScalarValue::List(list) if !list.is_null(0) => {
            let values = list.value(0);
            if values.null_count() != 0 {
                return Err(error(
                    "approx_percentile percentage array cannot contain NULL",
                ));
            }
            (0..values.len())
                .map(|row| scalar_double(ScalarValue::try_from_array(&values, row)?))
                .collect::<Result<Vec<_>>>()?
        }
        value if !value.is_null() => vec![scalar_double(value)?],
        _ => return Err(error("approx_percentile percentage cannot be NULL")),
    };
    // Spark allows NaN through analysis, but rejects it when a nonempty digest
    // is queried. Empty inputs return NULL before that query.
    if percentages
        .iter()
        .any(|p| !p.is_nan() && !(0.0..=1.0).contains(p))
    {
        return Err(error(
            "approx_percentile percentage must be between 0.0 and 1.0",
        ));
    }
    Ok(percentages)
}

fn read_accuracy(value: ScalarValue) -> Result<u32> {
    match value {
        ScalarValue::Int64(Some(value)) if (1..=i64::from(i32::MAX)).contains(&value) => {
            Ok(value as u32)
        }
        _ => Err(error(
            "approx_percentile accuracy must be an integer in [1, 2147483647]",
        )),
    }
}

fn decimal_double(unscaled: i128, scale: i8) -> Result<f64> {
    // BigDecimal.doubleValue rounds the exact decimal once. Casting the integer
    // first and dividing by 10^scale would round twice, changing values and ranks.
    format!("{unscaled}e{}", -i32::from(scale))
        .parse::<f64>()
        .map_err(|e| error(format!("approx_percentile invalid decimal: {e}")))
}

fn scalar_double(value: ScalarValue) -> Result<f64> {
    if let ScalarValue::Decimal128(Some(value), _, scale) = value {
        return decimal_double(value, scale);
    }
    match value.cast_to(&DataType::Float64)? {
        ScalarValue::Float64(Some(value)) => Ok(value),
        _ => Err(error(
            "approx_percentile could not convert a numeric value to double",
        )),
    }
}

fn constant(expr: &Arc<dyn PhysicalExpr>, name: &str) -> Result<ScalarValue> {
    fn foldable(expr: &Arc<dyn PhysicalExpr>) -> bool {
        !expr.is_volatile_node()
            && !expr.is::<Column>()
            && expr.children().iter().all(|child| foldable(child))
    }
    if foldable(expr) {
        if let Ok(ColumnarValue::Scalar(value)) =
            expr.evaluate(&RecordBatch::new_empty(Arc::new(Schema::empty())))
        {
            return Ok(value);
        }
    }
    Err(error(format!(
        "approx_percentile {name} must be a constant expression"
    )))
}

#[derive(Debug)]
struct PercentileAccumulator {
    summary: QuantileSummary,
    accuracy: u32,
    percentages: Vec<f64>,
    input_type: DataType,
    return_type: DataType,
    distinct: Option<HashSet<ScalarValue>>,
    distinct_bytes: usize,
    // Arrow's MonthDayNano can represent either Spark ANSI interval family.
    // 0 = no nonzero interval seen, 1 = year-month, 2 = day-time.
    interval_kind: u8,
}

impl PercentileAccumulator {
    fn empty_like(&self) -> Self {
        Self {
            summary: QuantileSummary::new(self.accuracy),
            accuracy: self.accuracy,
            percentages: self.percentages.clone(),
            input_type: self.input_type.clone(),
            return_type: self.return_type.clone(),
            distinct: self.distinct.as_ref().map(|_| HashSet::new()),
            distinct_bytes: 0,
            interval_kind: 0,
        }
    }

    fn interval_kind(&mut self, kind: u8) -> Result<()> {
        if kind == 0 {
            return Ok(());
        }
        if self.interval_kind != 0 && self.interval_kind != kind {
            return Err(error(
                "approx_percentile does not support mixed year-month and day-time intervals",
            ));
        }
        self.interval_kind = kind;
        Ok(())
    }

    fn insert_scalar(&mut self, value: ScalarValue) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        let value = match value {
            ScalarValue::TimestampSecond(Some(value), tz) => timestamp_scalar(
                timestamp_micros(value, &TimeUnit::Second)?,
                &TimeUnit::Second,
                tz,
            )?,
            ScalarValue::TimestampMillisecond(Some(value), tz) => timestamp_scalar(
                timestamp_micros(value, &TimeUnit::Millisecond)?,
                &TimeUnit::Millisecond,
                tz,
            )?,
            ScalarValue::TimestampMicrosecond(Some(value), tz) => {
                timestamp_scalar(value, &TimeUnit::Microsecond, tz)?
            }
            ScalarValue::TimestampNanosecond(Some(value), tz) => timestamp_scalar(
                timestamp_micros(value, &TimeUnit::Nanosecond)?,
                &TimeUnit::Nanosecond,
                tz,
            )?,
            ScalarValue::IntervalDayTime(Some(value)) => {
                let micros =
                    interval_micros(value.days, i64::from(value.milliseconds) * 1_000_000)?;
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(
                    (micros / 86_400_000_000) as i32,
                    ((micros % 86_400_000_000) / 1000) as i32,
                )))
            }
            ScalarValue::IntervalMonthDayNano(Some(value)) if value.months == 0 => {
                let micros = interval_micros(value.days, value.nanoseconds)?;
                ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(
                    0,
                    (micros / 86_400_000_000) as i32,
                    (micros % 86_400_000_000) * 1000,
                )))
            }
            value => value,
        };
        let value = if self.distinct.is_some() {
            match value {
                ScalarValue::Float16(Some(value)) => {
                    type Float16 = <Float16Type as ArrowPrimitiveType>::Native;
                    ScalarValue::Float16(Some(if value == Float16::ZERO {
                        Float16::ZERO
                    } else if value.is_nan() {
                        Float16::NAN
                    } else {
                        value
                    }))
                }
                ScalarValue::Float32(Some(value)) => ScalarValue::Float32(Some(if value == 0.0 {
                    0.0
                } else if value.is_nan() {
                    f32::NAN
                } else {
                    value
                })),
                ScalarValue::Float64(Some(value)) => ScalarValue::Float64(Some(if value == 0.0 {
                    0.0
                } else if value.is_nan() {
                    f64::NAN
                } else {
                    value
                })),
                value => value,
            }
        } else {
            value
        };
        if let Some(distinct) = &mut self.distinct {
            if !distinct.insert(value.clone()) {
                return Ok(());
            }
            self.distinct_bytes += value.size();
        }
        let number = match value {
            ScalarValue::IntervalYearMonth(Some(months)) => f64::from(months),
            ScalarValue::IntervalDayTime(Some(value)) => {
                interval_micros(value.days, i64::from(value.milliseconds) * 1_000_000)? as f64
            }
            ScalarValue::IntervalMonthDayNano(Some(value)) => {
                if value.months != 0 && (value.days != 0 || value.nanoseconds != 0) {
                    return Err(error("approx_percentile requires an ANSI interval, not a mixed calendar interval"));
                }
                if value.months != 0 {
                    self.interval_kind(1)?;
                    f64::from(value.months)
                } else {
                    if value.days != 0 || value.nanoseconds != 0 {
                        self.interval_kind(2)?;
                    }
                    interval_micros(value.days, value.nanoseconds)? as f64
                }
            }
            ScalarValue::Date32(Some(value)) => f64::from(value),
            ScalarValue::Date64(Some(value)) => value as f64,
            ScalarValue::TimestampSecond(Some(value), _) => {
                timestamp_micros(value, &TimeUnit::Second)? as f64
            }
            ScalarValue::TimestampMillisecond(Some(value), _) => {
                timestamp_micros(value, &TimeUnit::Millisecond)? as f64
            }
            ScalarValue::TimestampMicrosecond(Some(value), _) => value as f64,
            ScalarValue::TimestampNanosecond(Some(value), _) => {
                timestamp_micros(value, &TimeUnit::Nanosecond)? as f64
            }
            value => scalar_double(value)?,
        };
        self.summary.insert(number);
        Ok(())
    }

    fn result(&self, value: f64) -> Result<ScalarValue> {
        Ok(match &self.input_type {
            DataType::Int8 => ScalarValue::Int8(Some(value as i8)),
            DataType::Int16 => ScalarValue::Int16(Some(value as i16)),
            DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
            DataType::Int64 => ScalarValue::Int64(Some(value as i64)),
            DataType::UInt8 => ScalarValue::UInt8(Some(value as u8)),
            DataType::UInt16 => ScalarValue::UInt16(Some(value as u16)),
            DataType::UInt32 => ScalarValue::UInt32(Some(value as u32)),
            DataType::UInt64 => ScalarValue::UInt64(Some(value as u64)),
            DataType::Float32 => ScalarValue::Float32(Some(value as f32)),
            DataType::Float64 => ScalarValue::Float64(Some(value)),
            DataType::Decimal128(precision, scale) => {
                // Spark builds Decimal(double) from Java's shortest decimal
                // string, then rounds HALF_UP to the declared scale. Binary
                // multiplication by 10^scale would introduce a second rounding.
                let unscaled = paimon::spec::format_java_double(value)
                    .parse::<BigDecimal>()
                    .ok()
                    .and_then(|decimal| {
                        decimal
                            .with_scale_round(i64::from(*scale), RoundingMode::HalfUp)
                            .as_bigint_and_exponent()
                            .0
                            .to_i128()
                    })
                    .filter(|value| value.unsigned_abs() < 10_u128.pow(u32::from(*precision)));
                ScalarValue::Decimal128(unscaled, *precision, *scale)
            }
            DataType::Date32 => ScalarValue::Date32(Some(value as i32)),
            DataType::Date64 => ScalarValue::Date64(Some(value as i64)),
            DataType::Timestamp(unit, tz) => timestamp_scalar(value as i64, unit, tz.clone())?,
            DataType::Interval(IntervalUnit::YearMonth) => {
                ScalarValue::IntervalYearMonth(Some(value as i32))
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                let micros = value as i64;
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(
                    (micros / 86_400_000_000) as i32,
                    ((micros % 86_400_000_000) / 1000) as i32,
                )))
            }
            DataType::Interval(IntervalUnit::MonthDayNano) if self.interval_kind == 1 => {
                ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(
                    value as i32,
                    0,
                    0,
                )))
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let micros = value as i64;
                ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(
                    0,
                    (micros / 86_400_000_000) as i32,
                    (micros % 86_400_000_000) * 1000,
                )))
            }
            ty => ScalarValue::Float64(Some(value)).cast_to(ty)?,
        })
    }
}

impl Accumulator for PercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let input = &values[0];
        if self.distinct.is_some() || matches!(self.input_type, DataType::Interval(_)) {
            for row in 0..input.len() {
                self.insert_scalar(ScalarValue::try_from_array(input, row)?)?;
            }
        } else if let Some(array) = input.as_any().downcast_ref::<Date32Array>() {
            for value in array.iter().flatten() {
                self.summary.insert(f64::from(value));
            }
        } else if let Some(array) = input.as_any().downcast_ref::<Date64Array>() {
            for value in array.iter().flatten() {
                self.summary.insert(value as f64);
            }
        } else if let DataType::Decimal128(_, scale) = &self.input_type {
            for value in input
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .iter()
                .flatten()
            {
                self.summary.insert(decimal_double(value, *scale)?);
            }
        } else if let DataType::Timestamp(unit, _) = &self.input_type {
            let ticks = cast(input, &DataType::Int64)?;
            for value in ticks
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .flatten()
            {
                self.summary.insert(timestamp_micros(value, unit)? as f64);
            }
        } else {
            let numeric = cast(input, &DataType::Float64)?;
            for value in numeric
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .flatten()
            {
                self.summary.insert(value);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.summary.count() == 0 || self.percentages.is_empty() {
            return ScalarValue::try_from(&self.return_type);
        }
        if self.percentages.iter().any(|p| p.is_nan()) {
            return Err(error(
                "approx_percentile percentage must be between 0.0 and 1.0",
            ));
        }
        let values = self.summary.query(&self.percentages);
        if values.is_empty() {
            return ScalarValue::try_from(&self.return_type);
        }
        let values = values
            .into_iter()
            .map(|value| self.result(value))
            .collect::<Result<Vec<_>>>()?;
        if let DataType::List(field) = &self.return_type {
            Ok(ScalarValue::List(ScalarValue::new_list(
                &values,
                &self.input_type,
                field.is_nullable(),
            )))
        } else {
            Ok(values.into_iter().next().unwrap())
        }
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let state = if let Some(distinct) = &self.distinct {
            let values = distinct.iter().cloned().collect::<Vec<_>>();
            ScalarValue::List(ScalarValue::new_list(&values, &self.input_type, false))
        } else {
            ScalarValue::Binary(Some(self.summary.serialize()))
        };
        Ok(vec![state, ScalarValue::UInt8(Some(self.interval_kind))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let kinds = states[1]
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| error("invalid percentile interval state"))?;
        if self.distinct.is_some() {
            let lists = states[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| error("invalid distinct percentile state"))?;
            for row in 0..lists.len() {
                self.interval_kind(kinds.value(row))?;
                let values = lists.value(row);
                for row in 0..values.len() {
                    self.insert_scalar(ScalarValue::try_from_array(&values, row)?)?;
                }
            }
        } else {
            let states = states[0]
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| error("invalid percentile summary state"))?;
            for row in 0..states.len() {
                self.interval_kind(kinds.value(row))?;
                let other = QuantileSummary::from_bytes(states.value(row), self.accuracy)?;
                self.summary.merge(&other);
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.summary.size()
            + self.percentages.capacity() * std::mem::size_of::<f64>()
            + self.distinct_bytes
            + self.distinct.as_ref().map_or(0, |values| {
                (values.capacity() - values.len()) * std::mem::size_of::<ScalarValue>()
            })
    }
}

fn timestamp_micros(value: i64, unit: &TimeUnit) -> Result<i64> {
    match unit {
        TimeUnit::Second => value.checked_mul(1_000_000),
        TimeUnit::Millisecond => value.checked_mul(1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond => Some(value.div_euclid(1_000)),
    }
    .ok_or_else(|| error("approx_percentile timestamp is outside Spark's microsecond range"))
}

fn timestamp_scalar(micros: i64, unit: &TimeUnit, tz: Option<Arc<str>>) -> Result<ScalarValue> {
    Ok(match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(micros.div_euclid(1_000_000)), tz),
        TimeUnit::Millisecond => {
            ScalarValue::TimestampMillisecond(Some(micros.div_euclid(1_000)), tz)
        }
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(micros), tz),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(
            Some(micros.checked_mul(1_000).ok_or_else(|| {
                error("approx_percentile timestamp result is outside Arrow's nanosecond range")
            })?),
            tz,
        ),
    })
}

fn interval_micros(days: i32, nanoseconds: i64) -> Result<i64> {
    // Spark truncates signed interval fractions toward zero, unlike timestamps.
    let micros = (i128::from(days) * 86_400_000_000_000 + i128::from(nanoseconds)) / 1000;
    i64::try_from(micros)
        .map_err(|_| error("approx_percentile interval is outside Spark's microsecond range"))
}

fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(message.into())
}
