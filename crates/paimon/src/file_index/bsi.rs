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

//! Java V1 `bsi` FileIndex for signed integral, date, time, decimal and timestamp values.
//!
//! Java stores two unsigned bit-sliced indexes: one for nonnegative values and
//! one for the absolute values of negative values. The outer header and each
//! slice's min/max use big-endian Java primitive encoding; Roaring bitmaps use
//! the portable bitmap encoding used by `RoaringBitmap32`.

use std::io::{Cursor, Read};

use bytes::{BufMut, Bytes};
use roaring::RoaringBitmap;

use crate::common::Options;
use crate::file_index::file_index_reader::FileIndexReader;
use crate::file_index::file_index_result::FileIndexResult;
use crate::file_index::file_index_writer::FileIndexWriter;
use crate::spec::{DataType, Datum, PredicateOperator};
use crate::{Error, Result};

const VERSION_1: u8 = 1;

fn invalid(message: impl Into<String>) -> Error {
    Error::FileIndexFormatInvalid {
        message: message.into(),
    }
}

fn write_error(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// The BSI value mapper must agree with Java's `getValueMapper` and must never
/// silently narrow an integer or decimal. A bad literal disables index pruning.
fn mapped_value(data_type: &DataType, datum: &Datum) -> Option<i64> {
    match (data_type, datum) {
        (DataType::TinyInt(_), Datum::TinyInt(value)) => Some(i64::from(*value)),
        (DataType::SmallInt(_), Datum::SmallInt(value)) => Some(i64::from(*value)),
        (DataType::Int(_), Datum::Int(value)) => Some(i64::from(*value)),
        (DataType::BigInt(_), Datum::Long(value)) => Some(*value),
        (DataType::Date(_), Datum::Date(value)) => Some(i64::from(*value)),
        (DataType::Time(_), Datum::Time(value)) => Some(i64::from(*value)),
        (
            DataType::Decimal(data_type),
            Datum::Decimal {
                unscaled,
                precision,
                scale,
            },
        ) if *precision == data_type.precision() && *scale == data_type.scale() => {
            i64::try_from(*unscaled).ok()
        }
        (DataType::Timestamp(data_type), Datum::Timestamp { millis, nanos }) => {
            timestamp_value(data_type.precision(), *millis, *nanos)
        }
        (
            DataType::LocalZonedTimestamp(data_type),
            Datum::LocalZonedTimestamp { millis, nanos },
        ) => timestamp_value(data_type.precision(), *millis, *nanos),
        _ => None,
    }
}

fn timestamp_value(precision: u32, millis: i64, nanos: i32) -> Option<i64> {
    if !(0..1_000_000).contains(&nanos) {
        return None;
    }
    if precision <= 3 {
        Some(millis)
    } else {
        millis
            .checked_mul(1_000)?
            .checked_add(i64::from(nanos / 1_000))
    }
}

fn validate_type(data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::TinyInt(_)
        | DataType::SmallInt(_)
        | DataType::Int(_)
        | DataType::BigInt(_)
        | DataType::Date(_)
        | DataType::Time(_)
        | DataType::Timestamp(_)
        | DataType::LocalZonedTimestamp(_) => Ok(()),
        DataType::Decimal(_) => Ok(()),
        _ => Err(Error::Unsupported {
            message: format!("BSI file index does not support data type {data_type:?}"),
        }),
    }
}

pub(crate) struct BsiFileIndexWriter {
    data_type: DataType,
    values: Vec<Option<i64>>,
}

impl BsiFileIndexWriter {
    pub(crate) fn try_new(data_type: DataType, _options: &Options) -> Result<Self> {
        validate_type(&data_type)?;
        Ok(Self {
            data_type,
            values: Vec::new(),
        })
    }
}

impl FileIndexWriter for BsiFileIndexWriter {
    fn write(&mut self, datum: Option<&Datum>) -> Result<()> {
        if self.values.len() >= i32::MAX as usize {
            return Err(write_error("BSI row count exceeds Java int range"));
        }
        let value = datum
            .map(|datum| {
                mapped_value(&self.data_type, datum).ok_or_else(|| {
                    write_error(format!(
                        "Datum {datum:?} does not match BSI type {:?}",
                        self.data_type
                    ))
                })
            })
            .transpose()?;
        if value == Some(i64::MIN) {
            // Java's Math.abs(Long.MIN_VALUE) overflows its negative slice.
            return Err(write_error("BSI cannot encode Long.MIN_VALUE"));
        }
        self.values.push(value);
        Ok(())
    }

    fn serialized_bytes(&mut self) -> Result<Bytes> {
        let row_count = i32::try_from(self.values.len())
            .map_err(|_| write_error("BSI row count exceeds Java int range"))?;
        let mut positive = Vec::new();
        let mut negative = Vec::new();
        for (row, value) in self.values.iter().enumerate() {
            match value {
                Some(value) if *value < 0 => negative.push((row as u32, value.unsigned_abs())),
                Some(value) => positive.push((row as u32, *value as u64)),
                None => {}
            }
        }
        let mut bytes = Vec::new();
        bytes.put_u8(VERSION_1);
        bytes.put_i32(row_count);
        bytes.put_u8(u8::from(!positive.is_empty()));
        if !positive.is_empty() {
            write_slice_index(&mut bytes, &positive)?;
        }
        bytes.put_u8(u8::from(!negative.is_empty()));
        if !negative.is_empty() {
            write_slice_index(&mut bytes, &negative)?;
        }
        Ok(Bytes::from(bytes))
    }

    fn empty(&self) -> bool {
        self.values.is_empty()
    }
}

fn write_bitmap(bytes: &mut Vec<u8>, bitmap: &RoaringBitmap) -> Result<()> {
    bitmap
        .serialize_into(bytes)
        .map_err(|error| write_error(format!("Failed to serialize BSI bitmap: {error}")))
}

fn write_slice_index(bytes: &mut Vec<u8>, values: &[(u32, u64)]) -> Result<()> {
    let max = values.iter().map(|(_, value)| *value).max().unwrap();
    let width = (u64::BITS - max.leading_zeros()) as usize;
    let mut existing = RoaringBitmap::new();
    let mut slices = vec![RoaringBitmap::new(); width];
    for &(row, mut value) in values {
        existing.insert(row);
        while value != 0 {
            slices[value.trailing_zeros() as usize].insert(row);
            value &= value - 1;
        }
    }
    bytes.put_u8(VERSION_1);
    bytes.put_i64(0); // Java's StatsCollectList starts positiveMin/negativeMin at zero.
    bytes.put_i64(max as i64);
    write_bitmap(bytes, &existing)?;
    bytes.put_i32(width as i32);
    for slice in slices {
        write_bitmap(bytes, &slice)?;
    }
    Ok(())
}

fn read_exact<const N: usize>(input: &mut Cursor<&[u8]>, label: &str) -> Result<[u8; N]> {
    let mut bytes = [0; N];
    input
        .read_exact(&mut bytes)
        .map_err(|error| invalid(format!("Truncated BSI {label}: {error}")))?;
    Ok(bytes)
}

fn read_u8(input: &mut Cursor<&[u8]>, label: &str) -> Result<u8> {
    Ok(read_exact::<1>(input, label)?[0])
}

fn read_i32(input: &mut Cursor<&[u8]>, label: &str) -> Result<i32> {
    Ok(i32::from_be_bytes(read_exact(input, label)?))
}

fn read_i64(input: &mut Cursor<&[u8]>, label: &str) -> Result<i64> {
    Ok(i64::from_be_bytes(read_exact(input, label)?))
}

fn read_bitmap(input: &mut Cursor<&[u8]>, row_count: u32, label: &str) -> Result<RoaringBitmap> {
    let bitmap = RoaringBitmap::deserialize_from(input)
        .map_err(|error| invalid(format!("Invalid BSI {label} bitmap: {error}")))?;
    if bitmap.max().is_some_and(|row| row >= row_count) {
        return Err(invalid(format!(
            "BSI {label} contains a row past {row_count}"
        )));
    }
    Ok(bitmap)
}

struct SliceIndex {
    max: i64,
    existing: RoaringBitmap,
    slices: Vec<RoaringBitmap>,
}

impl SliceIndex {
    fn read(input: &mut Cursor<&[u8]>, row_count: u32) -> Result<Self> {
        if read_u8(input, "slice version")? != VERSION_1 {
            return Err(invalid("Unsupported BSI slice version"));
        }
        let min = read_i64(input, "slice min")?;
        let max = read_i64(input, "slice max")?;
        if min != 0 || max < 0 {
            return Err(invalid("Invalid BSI slice min/max"));
        }
        let existing = read_bitmap(input, row_count, "existence")?;
        let width = read_i32(input, "slice count")?;
        let expected = i64::BITS - max.leading_zeros();
        if width < 0 || width as u32 != expected {
            return Err(invalid("BSI slice count does not match max"));
        }
        let mut slices = Vec::with_capacity(width as usize);
        for _ in 0..width {
            let bitmap = read_bitmap(input, row_count, "slice")?;
            if !bitmap.is_subset(&existing) {
                return Err(invalid("BSI slice has rows outside existence bitmap"));
            }
            slices.push(bitmap);
        }
        Ok(Self {
            max,
            existing,
            slices,
        })
    }

    fn compare(&self, operator: PredicateOperator, value: i64) -> RoaringBitmap {
        use PredicateOperator::*;
        if value < 0 {
            return match operator {
                Eq | Lt | LtEq => RoaringBitmap::new(),
                NotEq | Gt | GtEq => self.existing.clone(),
                _ => RoaringBitmap::new(),
            };
        }
        if value > self.max {
            return match operator {
                NotEq | Lt | LtEq => self.existing.clone(),
                _ => RoaringBitmap::new(),
            };
        }
        let mut equal = self.existing.clone();
        let mut less = RoaringBitmap::new();
        let mut greater = RoaringBitmap::new();
        for (bit, slice) in self.slices.iter().enumerate().rev() {
            if (value >> bit) & 1 == 1 {
                less |= &equal - slice;
                equal &= slice;
            } else {
                greater |= &equal & slice;
                equal -= slice;
            }
        }
        match operator {
            Eq => equal,
            NotEq => &self.existing - &equal,
            Lt => less,
            LtEq => &less | &equal,
            Gt => greater,
            GtEq => &greater | &equal,
            _ => RoaringBitmap::new(),
        }
    }
}

pub(crate) struct BsiFileIndexReader {
    data_type: DataType,
    row_count: u32,
    positive: Option<SliceIndex>,
    negative: Option<SliceIndex>,
}

impl BsiFileIndexReader {
    pub(crate) fn try_new(data_type: DataType, serialized: Bytes) -> Result<Self> {
        validate_type(&data_type)?;
        let mut input = Cursor::new(serialized.as_ref());
        if read_u8(&mut input, "version")? != VERSION_1 {
            return Err(invalid("Unsupported BSI version"));
        }
        let row_count = u32::try_from(read_i32(&mut input, "row count")?)
            .map_err(|_| invalid("Negative BSI row count"))?;
        let positive = match read_u8(&mut input, "positive flag")? {
            0 => None,
            1 => Some(SliceIndex::read(&mut input, row_count)?),
            _ => return Err(invalid("Invalid BSI positive flag")),
        };
        let negative = match read_u8(&mut input, "negative flag")? {
            0 => None,
            1 => Some(SliceIndex::read(&mut input, row_count)?),
            _ => return Err(invalid("Invalid BSI negative flag")),
        };
        if input.position() as usize != serialized.len() {
            return Err(invalid("Trailing BSI payload bytes"));
        }
        if let (Some(positive), Some(negative)) = (&positive, &negative) {
            if !positive.existing.is_disjoint(&negative.existing) {
                return Err(invalid("A BSI row appears in both sign indexes"));
            }
        }
        Ok(Self {
            data_type,
            row_count,
            positive,
            negative,
        })
    }

    fn non_null(&self) -> RoaringBitmap {
        let mut rows = RoaringBitmap::new();
        if let Some(positive) = &self.positive {
            rows |= &positive.existing;
        }
        if let Some(negative) = &self.negative {
            rows |= &negative.existing;
        }
        rows
    }

    fn compare(&self, operator: PredicateOperator, value: i64) -> RoaringBitmap {
        use PredicateOperator::*;
        let positive = self.positive.as_ref();
        let negative = self.negative.as_ref();
        if value == i64::MIN {
            return match operator {
                Lt | LtEq | Eq => RoaringBitmap::new(),
                NotEq | Gt | GtEq => self.non_null(),
                _ => RoaringBitmap::new(),
            };
        }
        if value < 0 {
            let abs = -value;
            match operator {
                Eq => negative.map_or_else(RoaringBitmap::new, |idx| idx.compare(Eq, abs)),
                NotEq => {
                    let equal =
                        negative.map_or_else(RoaringBitmap::new, |idx| idx.compare(Eq, abs));
                    &self.non_null() - &equal
                }
                Lt => negative.map_or_else(RoaringBitmap::new, |idx| idx.compare(Gt, abs)),
                LtEq => negative.map_or_else(RoaringBitmap::new, |idx| idx.compare(GtEq, abs)),
                Gt => {
                    let mut rows =
                        positive.map_or_else(RoaringBitmap::new, |idx| idx.existing.clone());
                    if let Some(idx) = negative {
                        rows |= idx.compare(Lt, abs);
                    }
                    rows
                }
                GtEq => {
                    let mut rows =
                        positive.map_or_else(RoaringBitmap::new, |idx| idx.existing.clone());
                    if let Some(idx) = negative {
                        rows |= idx.compare(LtEq, abs);
                    }
                    rows
                }
                _ => RoaringBitmap::new(),
            }
        } else {
            match operator {
                Eq => positive.map_or_else(RoaringBitmap::new, |idx| idx.compare(Eq, value)),
                NotEq => {
                    let equal =
                        positive.map_or_else(RoaringBitmap::new, |idx| idx.compare(Eq, value));
                    &self.non_null() - &equal
                }
                Lt => {
                    let mut rows =
                        negative.map_or_else(RoaringBitmap::new, |idx| idx.existing.clone());
                    if let Some(idx) = positive {
                        rows |= idx.compare(Lt, value);
                    }
                    rows
                }
                LtEq => {
                    let mut rows =
                        negative.map_or_else(RoaringBitmap::new, |idx| idx.existing.clone());
                    if let Some(idx) = positive {
                        rows |= idx.compare(LtEq, value);
                    }
                    rows
                }
                Gt => positive.map_or_else(RoaringBitmap::new, |idx| idx.compare(Gt, value)),
                GtEq => positive.map_or_else(RoaringBitmap::new, |idx| idx.compare(GtEq, value)),
                _ => RoaringBitmap::new(),
            }
        }
    }
}

impl FileIndexReader for BsiFileIndexReader {
    fn evaluate(
        &self,
        _column: &str,
        _index: usize,
        _data_type: &DataType,
        operator: PredicateOperator,
        literals: &[Datum],
    ) -> FileIndexResult {
        use PredicateOperator::*;
        let rows = match operator {
            IsNull => {
                let mut rows = RoaringBitmap::new();
                rows.insert_range(0..self.row_count);
                rows -= &self.non_null();
                rows
            }
            IsNotNull => self.non_null(),
            Eq | NotEq | Lt | LtEq | Gt | GtEq => {
                let Some(value) = literals
                    .first()
                    .and_then(|datum| mapped_value(&self.data_type, datum))
                else {
                    return FileIndexResult::Remain;
                };
                if self.truncated_timestamp() {
                    return FileIndexResult::Remain;
                }
                self.compare(operator, value)
            }
            In | NotIn => {
                if self.truncated_timestamp() {
                    return FileIndexResult::Remain;
                }
                let mut equal = RoaringBitmap::new();
                for literal in literals {
                    let Some(value) = mapped_value(&self.data_type, literal) else {
                        return FileIndexResult::Remain;
                    };
                    equal |= self.compare(Eq, value);
                }
                if operator == NotIn {
                    &self.non_null() - &equal
                } else {
                    equal
                }
            }
            Between if literals.len() == 2 => {
                if self.truncated_timestamp() {
                    return FileIndexResult::Remain;
                }
                let (Some(lower), Some(upper)) = (
                    mapped_value(&self.data_type, &literals[0]),
                    mapped_value(&self.data_type, &literals[1]),
                ) else {
                    return FileIndexResult::Remain;
                };
                &self.compare(GtEq, lower) & &self.compare(LtEq, upper)
            }
            _ => return FileIndexResult::Remain,
        };
        FileIndexResult::Selection(rows)
    }
}

impl BsiFileIndexReader {
    fn truncated_timestamp(&self) -> bool {
        match &self.data_type {
            DataType::Timestamp(data_type) => data_type.precision() > 6,
            DataType::LocalZonedTimestamp(data_type) => data_type.precision() > 6,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests;
