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

use crate::arrow::paimon_type_to_arrow;
use crate::spec::DataType;
use arrow_array::{
    cast::AsArray, types::*, Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
};
use serde_json::{json, Value};
use std::sync::Arc;

pub(crate) fn source(value: &Value, data_type: &DataType) -> ArrayRef {
    let nullable = value.is_null();
    macro_rules! integral {
        ($array:ty, $ty:ty) => {
            Arc::new(<$array>::from(vec![if nullable {
                None
            } else {
                Some(value.as_i64().unwrap() as $ty)
            }])) as ArrayRef
        };
    }
    match data_type {
        DataType::TinyInt(_) => integral!(Int8Array, i8),
        DataType::SmallInt(_) => integral!(Int16Array, i16),
        DataType::Int(_) => integral!(Int32Array, i32),
        DataType::BigInt(_) => integral!(Int64Array, i64),
        DataType::Float(_) => Arc::new(Float32Array::from(vec![value.as_f64().map(|v| v as f32)])),
        DataType::Double(_) => Arc::new(Float64Array::from(vec![value.as_f64()])),
        DataType::Boolean(_) => Arc::new(BooleanArray::from(vec![value.as_bool()])),
        DataType::Date(_) | DataType::Time(_) => {
            let integers = integral!(Int32Array, i32);
            arrow_cast::cast(&integers, &paimon_type_to_arrow(data_type).unwrap()).unwrap()
        }
        DataType::Char(_) | DataType::VarChar(_) => {
            Arc::new(StringArray::from(vec![value.as_str()]))
        }
        DataType::Decimal(t) => {
            let value = value.as_str().map(|value| {
                let negative = value.starts_with('-');
                let value = value.trim_start_matches(['-', '+']);
                let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
                let mut digits = format!("{whole}{fraction}");
                digits.extend(std::iter::repeat_n(
                    '0',
                    t.scale() as usize - fraction.len(),
                ));
                let unscaled = digits.parse::<i128>().unwrap();
                if negative {
                    -unscaled
                } else {
                    unscaled
                }
            });
            Arc::new(
                Decimal128Array::from(vec![value])
                    .with_precision_and_scale(t.precision() as u8, t.scale() as i8)
                    .unwrap(),
            )
        }
        _ => panic!("unexpected golden source type {data_type:?}"),
    }
}

pub(crate) fn external(value: &ArrayRef) -> Value {
    if value.is_null(0) {
        return Value::Null;
    }
    macro_rules! number {
        ($ty:ty) => {
            json!(value.as_primitive::<$ty>().value(0))
        };
    }
    match value.data_type() {
        arrow_schema::DataType::Int8 => number!(Int8Type),
        arrow_schema::DataType::Int16 => number!(Int16Type),
        arrow_schema::DataType::Int32 => number!(Int32Type),
        arrow_schema::DataType::Int64 => number!(Int64Type),
        arrow_schema::DataType::Float32 => {
            json!(f64::from(value.as_primitive::<Float32Type>().value(0)))
        }
        arrow_schema::DataType::Float64 => number!(Float64Type),
        arrow_schema::DataType::Boolean => json!(value.as_boolean().value(0)),
        arrow_schema::DataType::Utf8 => json!(value.as_string::<i32>().value(0)),
        arrow_schema::DataType::Decimal128(_, _) => {
            // Format target decimals via the same plain representation as Java.
            let from = crate::arrow::arrow_fields_to_paimon(&[arrow_schema::Field::new(
                "v",
                value.data_type().clone(),
                true,
            )])
            .unwrap()[0]
                .data_type()
                .clone();
            let text: DataType = serde_json::from_value(json!("STRING")).unwrap();
            let strings = crate::arrow::cast_numeric(value, &from, &text)
                .unwrap()
                .unwrap();
            json!(strings.as_string::<i32>().value(0))
        }
        other => panic!("unexpected golden target type {other:?}"),
    }
}
