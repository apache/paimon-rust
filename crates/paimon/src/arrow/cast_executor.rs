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

//! The existing Python scalar CAST subset, bound like Java CastExecutors.

use std::sync::Arc;

use arrow_array::{
    cast::AsArray, types::*, ArrayRef, Date32Array, Float32Array, Float64Array, Int64Array,
    StringArray,
};
use arrow_schema::{DataType as ArrowType, TimeUnit};
use jiff::tz::TimeZone;

use crate::spec::{BigIntType, DataType, DoubleType, IntType};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CastExecutor {
    // Java copyWithNewInputs retains the original executor. The current type
    // describes the row getter/runtime class, not the executor to select.
    from: DataType,
    current: DataType,
    to: DataType,
}

fn character(t: &DataType) -> bool {
    matches!(t, DataType::Char(_) | DataType::VarChar(_))
}
fn binary(t: &DataType) -> bool {
    matches!(t, DataType::Binary(_) | DataType::VarBinary(_))
}
fn primitive(t: &DataType) -> bool {
    matches!(
        t,
        DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::BigInt(_)
            | DataType::Float(_)
            | DataType::Double(_)
    )
}
fn numeric(t: &DataType) -> bool {
    primitive(t) || matches!(t, DataType::Decimal(_))
}
fn timestamp(t: &DataType) -> bool {
    matches!(t, DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_))
}
fn number(t: &DataType) -> bool {
    primitive(t) || matches!(t, DataType::Date(_) | DataType::Time(_))
}
fn integer_object(t: &DataType) -> bool {
    matches!(t, DataType::Int(_) | DataType::Date(_) | DataType::Time(_))
}

impl CastExecutor {
    pub(crate) fn new(from: DataType, current: DataType, to: DataType) -> crate::Result<Self> {
        if !Self::supports(&from, &to) {
            return Err(crate::Error::Unsupported {
                message: format!(
                    "CAST {from} -> {to} is outside the existing Python/Java scalar rules"
                ),
            });
        }
        Ok(Self { from, current, to })
    }

    pub(crate) fn supports(from: &DataType, to: &DataType) -> bool {
        super::supports_numeric_cast(from, to)
            || character(to)
                && (numeric(from)
                    || character(from)
                    || binary(from)
                    || matches!(
                        from,
                        DataType::Boolean(_) | DataType::Date(_) | DataType::Time(_)
                    )
                    || timestamp(from))
            || binary(to) && (binary(from) || character(from))
            || character(from)
                && (matches!(
                    to,
                    DataType::Float(_) | DataType::Double(_) | DataType::Date(_)
                ) || timestamp(to))
            || matches!(from, DataType::BigInt(_) | DataType::Date(_)) && timestamp(to)
            || timestamp(from)
                && (timestamp(to)
                    || matches!(
                        to,
                        DataType::BigInt(_) | DataType::Date(_) | DataType::Time(_)
                    ))
    }

    pub(crate) fn cast(&self, source: &ArrayRef) -> crate::Result<ArrayRef> {
        self.cast_in_zone(source, &TimeZone::system())
    }

    pub(crate) fn cast_in_zone(
        &self,
        source: &ArrayRef,
        zone: &TimeZone,
    ) -> crate::Result<ArrayRef> {
        let target = super::paimon_type_to_arrow(&self.to)?;
        // A valid executor never checks the runtime class of NULL. This also
        // preserves per-row short circuiting and typed-empty validation.
        if source.null_count() == source.len() {
            return Ok(arrow_array::new_null_array(&target, source.len()));
        }
        let source = strict_cast(source, &super::paimon_type_to_arrow(&self.current)?)?;
        let result = if numeric(&self.from) && character(&self.to) {
            self.require(
                numeric(&self.current)
                    || timestamp(&self.current)
                    || character(&self.current)
                    || matches!(
                        self.current,
                        DataType::Boolean(_) | DataType::Date(_) | DataType::Time(_)
                    ),
            )?;
            let (source, current) = self.number_input(&source)?;
            super::apply_declared_width(&number_string(&source, &current)?, &self.to)
        } else if numeric(&self.from) && numeric(&self.to) {
            self.require(if matches!(self.from, DataType::Decimal(_)) {
                matches!(self.current, DataType::Decimal(_))
            } else {
                number(&self.current)
            })?;
            let (mut source, mut current) = self.number_input(&source)?;
            if matches!(self.to, DataType::Decimal(_)) && primitive(&self.from) {
                if matches!(self.from, DataType::Float(_) | DataType::Double(_)) {
                    source = strict_cast(&source, &ArrowType::Float64)?;
                    current = DataType::Double(DoubleType::new());
                } else {
                    source = super::evolution_cast::numeric_to_integer(&source, &ArrowType::Int64)?;
                    current = DataType::BigInt(BigIntType::new());
                }
            }
            super::cast_numeric(&source, &current, &self.to)?.expect("validated numeric rule")
        } else if primitive(&self.from) && matches!(self.to, DataType::Boolean(_)) {
            self.require(number(&self.current))?;
            let (source, _) = self.number_input(&source)?;
            let long = super::evolution_cast::numeric_to_integer(&source, &ArrowType::Int64)?;
            strict_cast(&long, &ArrowType::Boolean)?
        } else if matches!(self.from, DataType::Boolean(_)) {
            self.require(matches!(self.current, DataType::Boolean(_)))?;
            super::apply_declared_width(&strict_cast(&source, &target)?, &self.to)
        } else if character(&self.from) {
            self.require(character(&self.current))?;
            self.cast_string(&source, zone)?
        } else if binary(&self.from) {
            self.require(binary(&self.current))?;
            let result = strict_cast(&source, &target)?;
            // BinaryToString returns BinaryString.fromBytes and deliberately
            // does not apply CHAR/VARCHAR width. Invalid UTF8 remains an error.
            if character(&self.to) {
                result
            } else {
                super::apply_declared_width(&result, &self.to)
            }
        } else if matches!(self.from, DataType::Date(_)) {
            if timestamp(&self.to) {
                self.require(number(&self.current))?;
                let (number, _) = self.number_input(&source)?;
                if matches!(self.to, DataType::LocalZonedTimestamp(_)) {
                    let integer =
                        super::evolution_cast::numeric_to_integer(&number, &ArrowType::Int32)?;
                    super::evolution_temporal::cast_in_zone_strict(
                        &integer, &self.from, &self.to, zone,
                    )?
                    .expect("DATE -> LTZ")
                } else {
                    let long =
                        super::evolution_cast::numeric_to_integer(&number, &ArrowType::Int64)?;
                    let values = long
                        .as_primitive::<Int64Type>()
                        .iter()
                        .map(|v| {
                            v.map(|v| timestamp_ticks(v.wrapping_mul(86_400_000), 0, &target))
                                .transpose()
                        })
                        .collect::<crate::Result<Vec<_>>>()?;
                    strict_cast(&(Arc::new(Int64Array::from(values)) as ArrayRef), &target)?
                }
            } else {
                self.require(integer_object(&self.current))?;
                let integer = strict_cast(&source, &ArrowType::Int32)?;
                if character(&self.to) {
                    let strings: ArrayRef = Arc::new(StringArray::from_iter(
                        integer
                            .as_primitive::<Int32Type>()
                            .iter()
                            .map(|v| v.map(super::cast_text::format_date)),
                    ));
                    super::apply_declared_width(&strings, &self.to)
                } else {
                    strict_cast(&integer, &target)?
                }
            }
        } else if matches!(self.from, DataType::Time(_)) {
            self.require(integer_object(&self.current))?;
            let integer = strict_cast(&source, &ArrowType::Int32)?;
            let time = strict_cast(&integer, &ArrowType::Time32(TimeUnit::Millisecond))?;
            if character(&self.to) {
                super::evolution_string::temporal_to_string_in_zone(
                    &time, &self.from, &self.to, zone,
                )?
                .expect("TIME -> character")
            } else {
                strict_cast(&time, &target)?
            }
        } else if timestamp(&self.from) {
            self.require(timestamp(&self.current))?;
            if character(&self.to) {
                super::evolution_string::temporal_to_string_in_zone(
                    &source, &self.from, &self.to, zone,
                )?
                .expect("timestamp -> character")
            } else if matches!(self.to, DataType::BigInt(_)) {
                super::evolution_cast::timestamp_to_bigint(
                    &source,
                    matches!(self.from, DataType::LocalZonedTimestamp(_)),
                )?
            } else if let Some(result) =
                super::evolution_temporal::cast_in_zone_strict(&source, &self.from, &self.to, zone)?
            {
                result
            } else {
                let ArrowType::Timestamp(unit, _) = source.data_type() else {
                    unreachable!()
                };
                let raw = strict_cast(&source, &ArrowType::Int64)?;
                let values = raw
                    .as_primitive::<Int64Type>()
                    .iter()
                    .map(|v| {
                        v.map(|v| {
                            let (mut millis, mut nanos) =
                                super::evolution_temporal::timestamp_parts(v, unit);
                            let precision = match &self.to {
                                DataType::Timestamp(t) => t.precision(),
                                DataType::LocalZonedTimestamp(t) => t.precision(),
                                _ => unreachable!(),
                            };
                            let from_precision = match &self.from {
                                DataType::Timestamp(t) => t.precision(),
                                DataType::LocalZonedTimestamp(t) => t.precision(),
                                _ => unreachable!(),
                            };
                            if from_precision > precision {
                                if precision <= 3 {
                                    let quantum = 10_i64.pow(3 - precision);
                                    millis = millis.div_euclid(quantum).wrapping_mul(quantum);
                                    nanos = 0;
                                } else {
                                    let quantum = 10_i64.pow(9 - precision);
                                    nanos = nanos / quantum * quantum;
                                }
                            }
                            timestamp_ticks(millis, nanos as u32, &target)
                        })
                        .transpose()
                    })
                    .collect::<crate::Result<Vec<_>>>()?;
                strict_cast(&(Arc::new(Int64Array::from(values)) as ArrayRef), &target)?
            }
        } else if matches!(self.from, DataType::BigInt(_)) && timestamp(&self.to) {
            self.require(number(&self.current))?;
            let (source, _) = self.number_input(&source)?;
            let long = super::evolution_cast::numeric_to_integer(&source, &ArrowType::Int64)?;
            if matches!(self.to, DataType::LocalZonedTimestamp(_)) {
                super::evolution_temporal::cast_in_zone_strict(&long, &self.from, &self.to, zone)?
                    .expect("BIGINT -> LTZ")
            } else {
                let values = long
                    .as_primitive::<Int64Type>()
                    .iter()
                    .map(|v| {
                        v.map(|v| timestamp_ticks(v.wrapping_mul(1000), 0, &target))
                            .transpose()
                    })
                    .collect::<crate::Result<Vec<_>>>()?;
                strict_cast(&(Arc::new(Int64Array::from(values)) as ArrayRef), &target)?
            }
        } else {
            unreachable!("validated scalar cast executor")
        };
        if timestamp(&self.to) && result.null_count() > source.null_count() {
            return Err(storage_error());
        }
        Ok(result)
    }

    fn require(&self, valid: bool) -> crate::Result<()> {
        if valid {
            Ok(())
        } else {
            Err(crate::Error::DataInvalid {
                message: format!(
                    "CAST {} -> {} cannot consume the Java runtime class of {}",
                    self.from, self.to, self.current
                ),
                source: None,
            })
        }
    }

    fn number_input(&self, source: &ArrayRef) -> crate::Result<(ArrayRef, DataType)> {
        if matches!(self.current, DataType::Date(_) | DataType::Time(_)) {
            Ok((
                strict_cast(source, &ArrowType::Int32)?,
                DataType::Int(IntType::new()),
            ))
        } else {
            Ok((Arc::clone(source), self.current.clone()))
        }
    }

    fn cast_string(&self, source: &ArrayRef, zone: &TimeZone) -> crate::Result<ArrayRef> {
        let target = super::paimon_type_to_arrow(&self.to)?;
        if matches!(self.to, DataType::Float(_) | DataType::Double(_)) {
            let single = matches!(self.to, DataType::Float(_));
            let values = source
                .as_string::<i32>()
                .iter()
                .map(|v| {
                    v.map(|v| crate::common::number::parse_java_float(v, single))
                        .transpose()
                })
                .collect::<crate::Result<Vec<_>>>()?;
            return Ok(if single {
                Arc::new(Float32Array::from_iter(
                    values.into_iter().map(|v| v.map(|v| v as f32)),
                )) as ArrayRef
            } else {
                Arc::new(Float64Array::from(values))
            });
        }
        if matches!(self.to, DataType::Date(_)) {
            let values = source
                .as_string::<i32>()
                .iter()
                .map(|v| v.map(super::cast_text::parse_date).transpose())
                .collect::<crate::Result<Vec<_>>>()?;
            return Ok(Arc::new(Date32Array::from(values)));
        }
        if timestamp(&self.to) {
            let (precision, local) = match &self.to {
                DataType::Timestamp(t) => (t.precision(), false),
                DataType::LocalZonedTimestamp(t) => (t.precision(), true),
                _ => unreachable!(),
            };
            let values = source
                .as_string::<i32>()
                .iter()
                .map(|v| {
                    v.map(|v| {
                        let (millis, nanos) =
                            super::cast_text::parse_timestamp(v, precision, local.then_some(zone))?;
                        timestamp_ticks(millis, nanos, &target)
                    })
                    .transpose()
                })
                .collect::<crate::Result<Vec<_>>>()?;
            return strict_cast(&(Arc::new(Int64Array::from(values)) as ArrayRef), &target);
        }
        let cast = match super::cast_numeric(source, &self.current, &self.to)? {
            Some(result) => result,
            None => strict_cast(source, &target)?,
        };
        Ok(super::apply_declared_width(&cast, &self.to))
    }
}

fn strict_cast(source: &ArrayRef, target: &ArrowType) -> crate::Result<ArrayRef> {
    arrow_cast::cast_with_options(
        source,
        target,
        &arrow_cast::CastOptions {
            safe: false,
            ..Default::default()
        },
    )
    .map_err(|error| crate::Error::DataInvalid {
        message: error.to_string(),
        source: None,
    })
}

fn number_string(source: &ArrayRef, current: &DataType) -> crate::Result<ArrayRef> {
    let single = matches!(current, DataType::Float(_));
    if timestamp(current) {
        let ArrowType::Timestamp(unit, _) = source.data_type() else {
            unreachable!()
        };
        let raw = strict_cast(source, &ArrowType::Int64)?;
        Ok(Arc::new(StringArray::from_iter(
            raw.as_primitive::<Int64Type>().iter().map(|value| {
                value.map(|value| {
                    let (millis, sub) = super::evolution_temporal::timestamp_parts(value, unit);
                    let nanos = (millis.rem_euclid(1000) * 1_000_000 + sub) as u32;
                    crate::common::date_time::format_iso_timestamp(millis.div_euclid(1000), nanos)
                })
            }),
        )))
    } else if matches!(current, DataType::Float(_) | DataType::Double(_)) {
        let source = strict_cast(source, &ArrowType::Float64)?;
        Ok(Arc::new(StringArray::from_iter(
            source
                .as_primitive::<Float64Type>()
                .iter()
                .map(|v| v.map(|v| crate::common::number::java_number_string(v, single))),
        )))
    } else if matches!(current, DataType::Decimal(_)) {
        super::evolution_decimal::decimal_to_string(source)
    } else {
        strict_cast(source, &ArrowType::Utf8)
    }
}

fn storage_error() -> crate::Error {
    crate::Error::DataInvalid {
        message: "Java timestamp result cannot be represented by the Arrow target".into(),
        source: None,
    }
}

fn timestamp_ticks(millis: i64, nanos: u32, target: &ArrowType) -> crate::Result<i64> {
    let ArrowType::Timestamp(unit, _) = target else {
        unreachable!()
    };
    let quantum = match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1000,
        TimeUnit::Nanosecond => 1,
    };
    let value = i128::from(millis) * 1_000_000 + i128::from(nanos);
    if value.rem_euclid(quantum) != 0 {
        return Err(storage_error());
    }
    i64::try_from(value / quantum).map_err(|_| storage_error())
}
