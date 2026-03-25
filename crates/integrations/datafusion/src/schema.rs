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

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DataFusionError;
use datafusion::common::Result as DFResult;
use std::sync::Arc;

use paimon::spec::{DataField, DataType as PaimonDataType};

/// Converts Paimon table schema (logical row type fields) to DataFusion Arrow schema.
pub fn paimon_schema_to_arrow(fields: &[DataField]) -> DFResult<Arc<Schema>> {
    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            let arrow_type = paimon_data_type_to_arrow(f.data_type())?;
            Ok(Field::new(
                f.name(),
                arrow_type,
                f.data_type().is_nullable(),
            ))
        })
        .collect::<DFResult<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(arrow_fields)))
}

fn paimon_data_type_to_arrow(dt: &PaimonDataType) -> DFResult<DataType> {
    use datafusion::arrow::datatypes::TimeUnit;

    Ok(match dt {
        PaimonDataType::Boolean(_) => DataType::Boolean,
        PaimonDataType::TinyInt(_) => DataType::Int8,
        PaimonDataType::SmallInt(_) => DataType::Int16,
        PaimonDataType::Int(_) => DataType::Int32,
        PaimonDataType::BigInt(_) => DataType::Int64,
        PaimonDataType::Float(_) => DataType::Float32,
        PaimonDataType::Double(_) => DataType::Float64,
        PaimonDataType::VarChar(_) | PaimonDataType::Char(_) => DataType::Utf8,
        PaimonDataType::Binary(_) | PaimonDataType::VarBinary(_) => DataType::Binary,
        PaimonDataType::Date(_) => DataType::Date32,
        PaimonDataType::Time(t) => match t.precision() {
            // `read.to_arrow(...)` goes through the Parquet Arrow reader, which exposes INT32
            // TIME values as millisecond precision only. Mirror that here so provider schema and
            // runtime RecordBatch schema stay identical.
            0..=3 => DataType::Time32(TimeUnit::Millisecond),
            4..=6 => DataType::Time64(TimeUnit::Microsecond),
            7..=9 => DataType::Time64(TimeUnit::Nanosecond),
            precision => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported TIME precision {precision}"
                )));
            }
        },
        PaimonDataType::Timestamp(t) => {
            DataType::Timestamp(timestamp_time_unit(t.precision())?, None)
        }
        PaimonDataType::LocalZonedTimestamp(t) => {
            DataType::Timestamp(timestamp_time_unit(t.precision())?, Some("UTC".into()))
        }
        PaimonDataType::Decimal(d) => {
            let p = u8::try_from(d.precision()).map_err(|_| {
                DataFusionError::Internal("Decimal precision exceeds u8".to_string())
            })?;
            let s = i8::try_from(d.scale() as i32).map_err(|_| {
                DataFusionError::Internal("Decimal scale out of i8 range".to_string())
            })?;
            match d.precision() {
                // The Parquet Arrow reader normalizes DECIMAL columns to Decimal128 regardless of
                // Parquet physical storage width. Mirror that here to avoid DataFusion schema
                // mismatch between `TableProvider::schema()` and `execute()` output.
                1..=38 => DataType::Decimal128(p, s),
                precision => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported DECIMAL precision {precision}"
                    )));
                }
            }
        }
        PaimonDataType::Array(_)
        | PaimonDataType::Map(_)
        | PaimonDataType::Multiset(_)
        | PaimonDataType::Row(_) => {
            return Err(DataFusionError::NotImplemented(
                "Paimon DataFusion integration does not yet support nested types (Array/Map/Row)"
                    .to_string(),
            ));
        }
    })
}

fn timestamp_time_unit(precision: u32) -> DFResult<datafusion::arrow::datatypes::TimeUnit> {
    use datafusion::arrow::datatypes::TimeUnit;

    match precision {
        0..=3 => Ok(TimeUnit::Millisecond),
        4..=6 => Ok(TimeUnit::Microsecond),
        7..=9 => Ok(TimeUnit::Nanosecond),
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported TIMESTAMP precision {precision}"
        ))),
    }
}
