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

//! Expand a serialized partition field into an Arrow column.

use crate::arrow::paimon_type_to_arrow;
use crate::spec::{extract_datum, BinaryRow, DataType, Datum};
use crate::Error;
use arrow_array::{
    new_null_array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    Time32MillisecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray,
};
use std::sync::Arc;

pub(crate) fn partition_array(
    partition: &BinaryRow,
    partition_index: usize,
    data_type: &DataType,
    num_rows: usize,
) -> crate::Result<ArrayRef> {
    let arrow_type = paimon_type_to_arrow(data_type)?;
    if partition.arity() <= partition_index as i32 || partition.is_null_at(partition_index) {
        return Ok(new_null_array(&arrow_type, num_rows));
    }

    let datum = extract_datum(partition, partition_index, data_type)?;
    let Some(datum) = datum else {
        return Ok(new_null_array(&arrow_type, num_rows));
    };

    Ok(match (datum, data_type) {
        (Datum::Bool(value), DataType::Boolean(_)) => {
            Arc::new(BooleanArray::from(vec![Some(value); num_rows]))
        }
        (Datum::TinyInt(value), DataType::TinyInt(_)) => {
            Arc::new(Int8Array::from(vec![Some(value); num_rows]))
        }
        (Datum::SmallInt(value), DataType::SmallInt(_)) => {
            Arc::new(Int16Array::from(vec![Some(value); num_rows]))
        }
        (Datum::Int(value), DataType::Int(_)) => {
            Arc::new(Int32Array::from(vec![Some(value); num_rows]))
        }
        (Datum::Long(value), DataType::BigInt(_)) => {
            Arc::new(Int64Array::from(vec![Some(value); num_rows]))
        }
        (Datum::Float(value), DataType::Float(_)) => {
            Arc::new(Float32Array::from(vec![Some(value); num_rows]))
        }
        (Datum::Double(value), DataType::Double(_)) => {
            Arc::new(Float64Array::from(vec![Some(value); num_rows]))
        }
        (Datum::String(value), DataType::Char(_) | DataType::VarChar(_)) => {
            let values = std::iter::repeat_with(|| Some(value.as_str()))
                .take(num_rows)
                .collect::<Vec<_>>();
            Arc::new(StringArray::from(values))
        }
        (Datum::Bytes(value), DataType::Binary(_) | DataType::VarBinary(_)) => {
            let values = std::iter::repeat_with(|| Some(value.as_slice()))
                .take(num_rows)
                .collect::<Vec<_>>();
            Arc::new(BinaryArray::from(values))
        }
        (
            Datum::Decimal {
                unscaled, scale, ..
            },
            DataType::Decimal(decimal),
        ) => Arc::new(
            Decimal128Array::from(vec![Some(unscaled); num_rows])
                .with_precision_and_scale(decimal.precision() as u8, scale as i8)
                .map_err(|error| Error::DataInvalid {
                    message: format!("Invalid decimal partition: {error}"),
                    source: Some(Box::new(error)),
                })?,
        ),
        (Datum::Date(value), DataType::Date(_)) => {
            Arc::new(Date32Array::from(vec![Some(value); num_rows]))
        }
        (Datum::Time(value), DataType::Time(_)) => {
            Arc::new(Time32MillisecondArray::from(vec![Some(value); num_rows]))
        }
        (Datum::Timestamp { millis, nanos }, DataType::Timestamp(ts)) => {
            timestamp_array(millis, nanos, ts.precision(), None, num_rows)?
        }
        (Datum::LocalZonedTimestamp { millis, nanos }, DataType::LocalZonedTimestamp(ts)) => {
            timestamp_array(millis, nanos, ts.precision(), Some("UTC"), num_rows)?
        }
        (_, other) => {
            return Err(Error::Unsupported {
                message: format!(
                    "Partition column type '{other:?}' is not supported by the Rust reader yet"
                ),
            });
        }
    })
}

fn timestamp_array(
    millis: i64,
    nanos: i32,
    precision: u32,
    timezone: Option<&'static str>,
    num_rows: usize,
) -> crate::Result<ArrayRef> {
    let array: ArrayRef = match precision {
        0..=3 => {
            let array = TimestampMillisecondArray::from(vec![Some(millis); num_rows]);
            match timezone {
                Some(tz) => Arc::new(array.with_timezone(tz)),
                None => Arc::new(array),
            }
        }
        4..=6 => {
            let value = millis * 1_000 + (nanos as i64) / 1_000;
            let array = TimestampMicrosecondArray::from(vec![Some(value); num_rows]);
            match timezone {
                Some(tz) => Arc::new(array.with_timezone(tz)),
                None => Arc::new(array),
            }
        }
        7..=9 => {
            let value = millis * 1_000_000 + (nanos as i64);
            let array = TimestampNanosecondArray::from(vec![Some(value); num_rows]);
            match timezone {
                Some(tz) => Arc::new(array.with_timezone(tz)),
                None => Arc::new(array),
            }
        }
        _ => {
            return Err(Error::Unsupported {
                message: format!("Unsupported timestamp precision for partition: {precision}"),
            });
        }
    };
    Ok(array)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        extract_datum_from_array, DecimalType, IntType, LocalZonedTimestampType, VarCharType,
    };

    #[test]
    fn partition_columns_preserve_nulls_decimals_and_timestamp_precision() {
        let values = [
            (None, DataType::VarChar(VarCharType::string_type())),
            (Some(Datum::Int(-7)), DataType::Int(IntType::new())),
            (
                Some(Datum::Decimal {
                    unscaled: -12345678901234567890,
                    precision: 20,
                    scale: 3,
                }),
                DataType::Decimal(DecimalType::new(20, 3).unwrap()),
            ),
            (
                Some(Datum::LocalZonedTimestamp {
                    millis: 1234,
                    nanos: 567890,
                }),
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(9).unwrap()),
            ),
        ];
        let datums: Vec<_> = values
            .iter()
            .map(|(value, ty)| (value.as_ref(), ty))
            .collect();
        let partition = BinaryRow::from_datums(&datums);
        for (index, (value, ty)) in values.iter().enumerate() {
            let column = partition_array(&partition, index, ty, 3).unwrap();
            assert_eq!(column.len(), 3);
            assert_eq!(column.data_type(), &paimon_type_to_arrow(ty).unwrap());
            for row in 0..3 {
                assert_eq!(
                    extract_datum_from_array(&column, row, index, ty).unwrap(),
                    *value
                );
            }
        }
    }
}
