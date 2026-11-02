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

mod summary;
mod validation;
mod window;
pub(crate) use validation::{validate_plan, ValidatePercentiles};
pub(crate) use window::plan_window;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, Date32Array, Float64Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, TimestampMicrosecondArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, IntervalDayTime, IntervalMonthDayNano, IntervalUnit, TimeUnit,
};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;
use datafusion::prelude::SessionContext;
use std::{collections::HashSet, sync::Arc};
use summary::Summary;

const MICROS_PER_DAY: i64 = 86_400_000_000;

pub(crate) fn register(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::new_from_impl(Percentile {
        signature: Signature::user_defined(Volatility::Immutable),
        aliases: vec!["percentile_approx".into()],
    }));
}
fn invalid(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(format!("approx_percentile: {}", message.into()))
}
fn decoded(mut ty: &DataType) -> &DataType {
    while let DataType::Dictionary(_, value) = ty {
        ty = value;
    }
    ty
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Percentile {
    signature: Signature,
    aliases: Vec<String>,
}
impl AggregateUDFImpl for Percentile {
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
            return Err(invalid("expects value, percentage [, accuracy]"));
        }
        let value = match decoded(&types[0]) {
            DataType::Null => DataType::Float64,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Interval(_) => decoded(&types[0]).clone(),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => DataType::Int64,
            DataType::UInt64 => DataType::Decimal128(20, 0),
            DataType::Float16 => DataType::Float32,
            DataType::Decimal32(p, s)
            | DataType::Decimal64(p, s)
            | DataType::Decimal128(p, s)
            | DataType::Decimal256(p, s)
                if *p <= 38 && *s >= 0 =>
            {
                DataType::Decimal128(*p, *s)
            }
            DataType::Date64 => DataType::Date32,
            DataType::Timestamp(_, timezone) => {
                DataType::Timestamp(TimeUnit::Microsecond, timezone.clone())
            }
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => DataType::Float64,
            ty => return Err(invalid(format!("unsupported value type {ty}"))),
        };
        let probability = match decoded(&types[1]) {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _) => {
                let element = decoded(field.data_type());
                if !(element.is_numeric() || *element == DataType::Null) {
                    return Err(invalid("percentage array must contain numbers"));
                }
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
            }
            ty if probability_type(ty) => DataType::Float64,
            ty => {
                return Err(invalid(format!(
                    "percentage requires a number or number array, got {ty}"
                )))
            }
        };
        let mut out = vec![value, probability];
        if types.len() == 3 {
            if !matches!(
                decoded(&types[2]),
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Null
            ) {
                return Err(invalid("accuracy must be an integer"));
            }
            out.push(DataType::Int64);
        }
        Ok(out)
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        let types = self.coerce_types(types)?;
        Ok(
            if matches!(
                types[1],
                DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..)
            ) {
                DataType::List(Arc::new(Field::new("item", types[0].clone(), false)))
            } else {
                types[0].clone()
            },
        )
    }
    fn return_field(&self, fields: &[FieldRef]) -> Result<FieldRef> {
        let types = fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&types)?,
            true,
        )))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let ty = if args.is_distinct {
            DataType::List(Arc::new(Field::new(
                "item",
                args.input_fields[0].data_type().clone(),
                false,
            )))
        } else {
            DataType::Binary
        };
        Ok(vec![Arc::new(Field::new(
            format!("{}[gk]", args.name),
            ty,
            false,
        ))])
    }
    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(make_accumulator(args)?))
    }
    fn supports_null_handling_clause(&self) -> bool {
        true
    }
    fn default_value(&self, ty: &DataType) -> Result<ScalarValue> {
        ScalarValue::try_new_null(ty)
    }
}

fn make_accumulator(args: AccumulatorArgs) -> Result<PercentileAccumulator> {
    if args.ignore_nulls {
        return Err(invalid("does not accept IGNORE NULLS"));
    }
    if !args.order_bys.is_empty() {
        return Err(DataFusionError::NotImplemented(
            "approx_percentile does not accept aggregate ORDER BY".into(),
        ));
    }
    // The optimizer folds constant expressions. Requiring literals also rejects volatile
    // scalar UDFs and per-row columns rather than evaluating parameters on an arbitrary row.
    let constant = |index: usize| -> Result<ScalarValue> {
        args.exprs[index]
            .downcast_ref::<Literal>()
            .map(|literal| literal.value().clone())
            .ok_or_else(|| {
                invalid(if index == 1 {
                    "percentage must be foldable"
                } else {
                    "accuracy must be foldable"
                })
            })
    };
    let probability = constant(1)?;
    let (probabilities, array) = match probability {
        ScalarValue::Float64(Some(value)) => (vec![value], false),
        ScalarValue::List(list) if !list.is_null(0) => {
            let values = list.value(0);
            let values = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| invalid("percentage array must contain numbers"))?;
            if values.null_count() != 0 {
                return Err(invalid("percentage array cannot contain NULL"));
            }
            (values.values().to_vec(), true)
        }
        _ => return Err(invalid("percentage cannot be NULL")),
    };
    validate_probabilities(&probabilities)?;
    let accuracy = if args.exprs.len() == 3 {
        match constant(2)? {
            ScalarValue::Int64(Some(value)) if (1..=i64::from(i32::MAX)).contains(&value) => value,
            _ => return Err(invalid("accuracy must be between 1 and 2147483647")),
        }
    } else {
        10_000
    };
    Ok(PercentileAccumulator {
        summary: Summary::new(1.0 / accuracy as f64),
        probabilities,
        data_type: args.expr_fields[0].data_type().clone(),
        output_type: args.return_type().clone(),
        array,
        interval_family: 0,
        unique: args.is_distinct.then(HashSet::new),
    })
}

fn validate_probabilities(values: &[f64]) -> Result<()> {
    if values
        .iter()
        .any(|value| !value.is_finite() || !(0.0..=1.0).contains(value))
    {
        Err(invalid("percentage must be between 0 and 1"))
    } else {
        Ok(())
    }
}

fn probability_type(ty: &DataType) -> bool {
    ty.is_numeric()
        || matches!(
            ty,
            DataType::Null | DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
        )
}

#[derive(Debug)]
struct PercentileAccumulator {
    summary: Summary,
    probabilities: Vec<f64>,
    data_type: DataType,
    output_type: DataType,
    array: bool,
    // Arrow's combined interval representation is accepted only for Spark's separate
    // year-month (1) or day-time (2) families. Zero is compatible with either family.
    interval_family: u8,
    unique: Option<HashSet<ScalarValue>>,
}
impl PercentileAccumulator {
    fn set_family(&mut self, family: u8) -> Result<()> {
        if self.interval_family != 0 && family != 0 && self.interval_family != family {
            return Err(invalid("cannot mix year-month and day-time intervals"));
        }
        self.interval_family = self.interval_family.max(family);
        Ok(())
    }
    fn scalar(&self, value: f64) -> Result<ScalarValue> {
        Ok(match &self.data_type {
            DataType::Int8 => ScalarValue::Int8(Some(value as i8)),
            DataType::Int16 => ScalarValue::Int16(Some(value as i16)),
            DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
            DataType::Int64 => ScalarValue::Int64(Some(value as i64)),
            DataType::Float32 => ScalarValue::Float32(Some(value as f32)),
            DataType::Float64 => ScalarValue::Float64(Some(value)),
            DataType::Date32 => ScalarValue::Date32(Some(value as i32)),
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                ScalarValue::TimestampMicrosecond(Some(value as i64), timezone.clone())
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                ScalarValue::IntervalYearMonth(Some(value as i32))
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                let micros = value as i64;
                ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(
                    (micros / MICROS_PER_DAY) as i32,
                    ((micros % MICROS_PER_DAY) / 1_000) as i32,
                )))
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let micros = value as i64;
                let result = if self.interval_family == 1 {
                    IntervalMonthDayNano::new(value as i32, 0, 0)
                } else {
                    IntervalMonthDayNano::new(
                        0,
                        (micros / MICROS_PER_DAY) as i32,
                        (micros % MICROS_PER_DAY) * 1_000,
                    )
                };
                ScalarValue::IntervalMonthDayNano(Some(result))
            }
            DataType::Decimal128(precision, scale) => {
                // Reuse Paimon's Java BigDecimal/half-up conversion, including the
                // legacy Double decimal text used by Spark on Java 8/11/17.
                let input: ArrayRef = Arc::new(Float64Array::from(vec![value]));
                let result = paimon::arrow::cast_array_for_schema_evolution(
                    &input,
                    &paimon::spec::DataType::Double(paimon::spec::DoubleType::new()),
                    &paimon::spec::DataType::Decimal(
                        paimon::spec::DecimalType::new(u32::from(*precision), *scale as u32)
                            .map_err(crate::error::to_datafusion_error)?,
                    ),
                )
                .map_err(crate::error::to_datafusion_error)?;
                ScalarValue::try_from_array(result.as_ref(), 0)?
            }
            ty => return Err(invalid(format!("unsupported result type {ty}"))),
        })
    }
}
impl Accumulator for PercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let new_values;
        let value = if let Some(unique) = &mut self.unique {
            let mut new = Vec::new();
            for row in 0..values[0].len() {
                let value = distinct_key(ScalarValue::try_from_array(values[0].as_ref(), row)?)?;
                if !value.is_null() && unique.insert(value.clone()) {
                    new.push(value);
                }
            }
            if new.is_empty() {
                return Ok(());
            }
            new_values = ScalarValue::iter_to_array(new)?;
            &new_values
        } else {
            &values[0]
        };
        let doubles: ArrayRef = match &self.data_type {
            DataType::Date32 => Arc::new(
                value
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(f64::from))
                    .collect::<Float64Array>(),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Arc::new(
                value
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(|v| v as f64))
                    .collect::<Float64Array>(),
            ),
            DataType::Interval(IntervalUnit::YearMonth) => Arc::new(
                value
                    .as_any()
                    .downcast_ref::<IntervalYearMonthArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(f64::from))
                    .collect::<Float64Array>(),
            ),
            DataType::Interval(IntervalUnit::DayTime) => {
                let doubles = value
                    .as_any()
                    .downcast_ref::<IntervalDayTimeArray>()
                    .unwrap()
                    .iter()
                    .map(|v| {
                        v.map(|v| interval_micros(v.days, i64::from(v.milliseconds) * 1_000))
                            .transpose()
                    })
                    .collect::<Result<Vec<_>>>()?;
                Arc::new(Float64Array::from(doubles))
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let intervals = value
                    .as_any()
                    .downcast_ref::<IntervalMonthDayNanoArray>()
                    .unwrap();
                let mut doubles = Vec::with_capacity(intervals.len());
                for interval in intervals.iter() {
                    let double = if let Some(interval) = interval {
                        let day_time = interval.days != 0 || interval.nanoseconds != 0;
                        if interval.months != 0 && day_time {
                            return Err(invalid(
                                "mixed calendar intervals have no Spark equivalent",
                            ));
                        }
                        self.set_family(if interval.months != 0 {
                            1
                        } else if day_time {
                            2
                        } else {
                            0
                        })?;
                        Some(if interval.months != 0 {
                            f64::from(interval.months)
                        } else {
                            interval_micros(interval.days, interval.nanoseconds / 1_000)?
                        })
                    } else {
                        None
                    };
                    doubles.push(double);
                }
                Arc::new(Float64Array::from(doubles))
            }
            // Decimal's rational value must be rounded once, rather than first
            // rounding its unscaled integer to double and then dividing by 10^scale.
            DataType::Decimal128(..) => cast(&cast(value, &DataType::Utf8)?, &DataType::Float64)?,
            _ => cast(value, &DataType::Float64)?,
        };
        for value in doubles
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .flatten()
        {
            self.summary.insert(value)?;
        }
        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let result = self.summary.query(&self.probabilities);
        if result.is_empty() {
            return ScalarValue::try_new_null(&self.output_type);
        }
        let result = result
            .into_iter()
            .map(|value| self.scalar(value))
            .collect::<Result<Vec<_>>>()?;
        if self.array {
            if result.iter().any(ScalarValue::is_null) {
                return Err(DataFusionError::Execution(
                    "approx_percentile decimal array element exceeds its result precision".into(),
                ));
            }
            Ok(ScalarValue::List(ScalarValue::new_list(
                &result,
                &self.data_type,
                false,
            )))
        } else {
            Ok(result.into_iter().next().unwrap())
        }
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        if let Some(unique) = &self.unique {
            let values = unique.iter().cloned().collect::<Vec<_>>();
            return Ok(vec![ScalarValue::List(ScalarValue::new_list(
                &values,
                &self.data_type,
                false,
            ))]);
        }
        Ok(vec![ScalarValue::Binary(Some(
            self.summary.encode(self.interval_family),
        ))])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if self.unique.is_some() {
            let states = states[0]
                .as_any()
                .downcast_ref::<datafusion::arrow::array::ListArray>()
                .ok_or_else(|| invalid("invalid DISTINCT state type"))?;
            for row in 0..states.len() {
                if !states.is_null(row) {
                    self.update_batch(&[states.value(row)])?;
                }
            }
            return Ok(());
        }
        let states = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| invalid("invalid intermediate state type"))?;
        for state in states.iter().flatten() {
            let (summary, family) = Summary::decode(state, self.summary.error)?;
            self.set_family(family)?;
            self.summary.merge(summary)?;
        }
        Ok(())
    }
    fn size(&self) -> usize {
        size_of::<Self>() + self.summary.size() - size_of::<Summary>()
            + self.probabilities.capacity() * size_of::<f64>()
            + self.data_type.size()
            + self.output_type.size()
            + self.unique.as_ref().map_or(0, |set| {
                set.capacity() * (size_of::<ScalarValue>() + 1)
                    + set
                        .iter()
                        .map(|v| v.size() - size_of::<ScalarValue>())
                        .sum::<usize>()
            })
    }
}

impl PercentileAccumulator {
    fn empty(&self) -> Self {
        Self {
            summary: Summary::new(self.summary.error),
            probabilities: self.probabilities.clone(),
            data_type: self.data_type.clone(),
            output_type: self.output_type.clone(),
            array: self.array,
            interval_family: 0,
            unique: self.unique.as_ref().map(|_| HashSet::new()),
        }
    }
}

fn distinct_key(value: ScalarValue) -> Result<ScalarValue> {
    Ok(match value {
        ScalarValue::Float64(Some(value)) => ScalarValue::Float64(Some(if value == 0.0 {
            0.0
        } else if value.is_nan() {
            f64::NAN
        } else {
            value
        })),
        ScalarValue::Float32(Some(value)) => ScalarValue::Float32(Some(if value == 0.0 {
            0.0
        } else if value.is_nan() {
            f32::NAN
        } else {
            value
        })),
        ScalarValue::IntervalDayTime(Some(value)) => {
            let micros = interval_value(value.days, i64::from(value.milliseconds) * 1_000)?;
            ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(
                (micros / MICROS_PER_DAY) as i32,
                ((micros % MICROS_PER_DAY) / 1_000) as i32,
            )))
        }
        ScalarValue::IntervalMonthDayNano(Some(value)) => {
            if value.months != 0 {
                if value.days != 0 || value.nanoseconds != 0 {
                    return Err(invalid("mixed calendar intervals have no Spark equivalent"));
                }
                ScalarValue::IntervalMonthDayNano(Some(value))
            } else {
                let micros = interval_value(value.days, value.nanoseconds / 1_000)?;
                ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(
                    0,
                    (micros / MICROS_PER_DAY) as i32,
                    (micros % MICROS_PER_DAY) * 1_000,
                )))
            }
        }
        value => value,
    })
}

fn interval_micros(days: i32, micros: i64) -> Result<f64> {
    interval_value(days, micros).map(|value| value as f64)
}
fn interval_value(days: i32, micros: i64) -> Result<i64> {
    i64::from(days)
        .checked_mul(MICROS_PER_DAY)
        .and_then(|v| v.checked_add(micros))
        .ok_or_else(|| invalid("day-time interval exceeds Spark's BIGINT microseconds range"))
}
