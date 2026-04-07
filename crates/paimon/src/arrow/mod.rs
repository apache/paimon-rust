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

pub(crate) mod filtering;
pub(crate) mod format;
mod reader;
pub(crate) mod schema_evolution;

pub use crate::arrow::reader::ArrowReaderBuilder;

use crate::spec::{DataField, DataType as PaimonDataType};
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema, TimeUnit};
use std::sync::Arc;

/// Converts a Paimon [`DataType`](PaimonDataType) to an Arrow [`DataType`](ArrowDataType).
pub fn paimon_type_to_arrow(dt: &PaimonDataType) -> crate::Result<ArrowDataType> {
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
        PaimonDataType::Time(_) => ArrowDataType::Time32(TimeUnit::Millisecond),
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
        PaimonDataType::Array(a) => {
            let element_type = paimon_type_to_arrow(a.element_type())?;
            ArrowDataType::List(Arc::new(ArrowField::new(
                "element",
                element_type,
                a.element_type().is_nullable(),
            )))
        }
        PaimonDataType::Map(m) => {
            let key_type = paimon_type_to_arrow(m.key_type())?;
            let value_type = paimon_type_to_arrow(m.value_type())?;
            ArrowDataType::Map(
                Arc::new(ArrowField::new(
                    "entries",
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new("key", key_type, false),
                            ArrowField::new("value", value_type, m.value_type().is_nullable()),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        }
        PaimonDataType::Multiset(m) => {
            let element_type = paimon_type_to_arrow(m.element_type())?;
            ArrowDataType::Map(
                Arc::new(ArrowField::new(
                    "entries",
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new("key", element_type, m.element_type().is_nullable()),
                            ArrowField::new("value", ArrowDataType::Int32, false),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        }
        PaimonDataType::Row(r) => {
            let fields: Vec<ArrowField> = r
                .fields()
                .iter()
                .map(|f| {
                    let arrow_type = paimon_type_to_arrow(f.data_type())?;
                    Ok(ArrowField::new(
                        f.name(),
                        arrow_type,
                        f.data_type().is_nullable(),
                    ))
                })
                .collect::<crate::Result<Vec<_>>>()?;
            ArrowDataType::Struct(fields.into())
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

/// Build an Arrow [`Schema`](ArrowSchema) from Paimon [`DataField`]s.
pub fn build_target_arrow_schema(fields: &[DataField]) -> crate::Result<Arc<ArrowSchema>> {
    let arrow_fields: Vec<ArrowField> = fields
        .iter()
        .map(|f| {
            let arrow_type = paimon_type_to_arrow(f.data_type())?;
            Ok(ArrowField::new(
                f.name(),
                arrow_type,
                f.data_type().is_nullable(),
            ))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(Arc::new(ArrowSchema::new(arrow_fields)))
}
