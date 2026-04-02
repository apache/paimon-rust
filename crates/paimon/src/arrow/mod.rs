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

mod reader;

pub use crate::arrow::reader::ArrowReaderBuilder;

use arrow_schema::{DataType as ArrowDataType, TimeUnit};

/// Convert a Paimon DataType to an Arrow DataType.
pub(crate) fn paimon_type_to_arrow(dt: &crate::spec::DataType) -> crate::Result<ArrowDataType> {
    use crate::spec::DataType as PaimonDataType;

    Ok(match dt {
        PaimonDataType::Boolean(_) => ArrowDataType::Boolean,
        PaimonDataType::TinyInt(_) => ArrowDataType::Int8,
        PaimonDataType::SmallInt(_) => ArrowDataType::Int16,
        PaimonDataType::Int(_) => ArrowDataType::Int32,
        PaimonDataType::BigInt(_) => ArrowDataType::Int64,
        PaimonDataType::Float(_) => ArrowDataType::Float32,
        PaimonDataType::Double(_) => ArrowDataType::Float64,
        PaimonDataType::VarChar(_) | PaimonDataType::Char(_) => ArrowDataType::Utf8,
        PaimonDataType::Binary(_) | PaimonDataType::VarBinary(_) => ArrowDataType::Binary,
        PaimonDataType::Date(_) => ArrowDataType::Date32,
        PaimonDataType::Time(t) => match t.precision() {
            0..=3 => ArrowDataType::Time32(TimeUnit::Millisecond),
            4..=6 => ArrowDataType::Time64(TimeUnit::Microsecond),
            7..=9 => ArrowDataType::Time64(TimeUnit::Nanosecond),
            p => {
                return Err(crate::Error::Unsupported {
                    message: format!("Unsupported TIME precision {p}"),
                })
            }
        },
        PaimonDataType::Timestamp(t) => {
            ArrowDataType::Timestamp(timestamp_time_unit(t.precision())?, None)
        }
        PaimonDataType::LocalZonedTimestamp(t) => {
            ArrowDataType::Timestamp(timestamp_time_unit(t.precision())?, Some("UTC".into()))
        }
        PaimonDataType::Decimal(d) => {
            let p = u8::try_from(d.precision()).map_err(|_| crate::Error::Unsupported {
                message: "Decimal precision exceeds u8".to_string(),
            })?;
            let s = i8::try_from(d.scale() as i32).map_err(|_| crate::Error::Unsupported {
                message: "Decimal scale out of i8 range".to_string(),
            })?;
            ArrowDataType::Decimal128(p, s)
        }
        _ => {
            return Err(crate::Error::Unsupported {
                message: format!("Unsupported Paimon type for Arrow conversion: {dt:?}"),
            })
        }
    })
}

fn timestamp_time_unit(precision: u32) -> crate::Result<TimeUnit> {
    match precision {
        0..=3 => Ok(TimeUnit::Millisecond),
        4..=6 => Ok(TimeUnit::Microsecond),
        7..=9 => Ok(TimeUnit::Nanosecond),
        _ => Err(crate::Error::Unsupported {
            message: format!("Unsupported TIMESTAMP precision {precision}"),
        }),
    }
}
