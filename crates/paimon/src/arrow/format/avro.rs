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

use super::{FilePredicates, FormatFileReader};
use crate::arrow::build_target_arrow_schema;
use crate::io::FileRead;
use crate::spec::{DataField, DataType, MapType, RowType};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::{
    BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, ListArray, MapArray, RecordBatch, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::SchemaRef;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// AvroValue: a serde_json::Value replacement that handles Avro bytes
// ---------------------------------------------------------------------------

/// Lightweight value type that can represent all Avro primitives including bytes.
/// `serde_json::Value` rejects byte arrays, so we need our own.
#[derive(Debug, Clone, PartialEq)]
enum AvroValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    /// Avro array / sequence.
    Array(Vec<AvroValue>),
    /// Union wrapper or record: `{"type": value}` produced by serde_avro_fast.
    Object(HashMap<String, AvroValue>),
}

impl AvroValue {
    fn as_bool(&self) -> Option<bool> {
        match self {
            AvroValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    fn as_i64(&self) -> Option<i64> {
        match self {
            AvroValue::Int(n) => Some(*n),
            _ => None,
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            AvroValue::Float(f) => Some(*f),
            AvroValue::Int(n) => Some(*n as f64),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            AvroValue::String(s) => Some(s),
            _ => None,
        }
    }

    fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            AvroValue::Bytes(b) => Some(b),
            AvroValue::String(s) => Some(s.as_bytes()),
            _ => None,
        }
    }
}

impl<'de> serde::Deserialize<'de> for AvroValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct AvroValueVisitor;

        impl<'de> serde::de::Visitor<'de> for AvroValueVisitor {
            type Value = AvroValue;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("any Avro value")
            }

            fn visit_bool<E>(self, v: bool) -> Result<AvroValue, E> {
                Ok(AvroValue::Bool(v))
            }

            fn visit_i8<E>(self, v: i8) -> Result<AvroValue, E> {
                Ok(AvroValue::Int(v as i64))
            }

            fn visit_i16<E>(self, v: i16) -> Result<AvroValue, E> {
                Ok(AvroValue::Int(v as i64))
            }

            fn visit_i32<E>(self, v: i32) -> Result<AvroValue, E> {
                Ok(AvroValue::Int(v as i64))
            }

            fn visit_i64<E>(self, v: i64) -> Result<AvroValue, E> {
                Ok(AvroValue::Int(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<AvroValue, E> {
                Ok(AvroValue::Int(v as i64))
            }

            fn visit_f32<E>(self, v: f32) -> Result<AvroValue, E> {
                Ok(AvroValue::Float(v as f64))
            }

            fn visit_f64<E>(self, v: f64) -> Result<AvroValue, E> {
                Ok(AvroValue::Float(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<AvroValue, E> {
                Ok(AvroValue::String(v.to_owned()))
            }

            fn visit_string<E>(self, v: String) -> Result<AvroValue, E> {
                Ok(AvroValue::String(v))
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<AvroValue, E> {
                Ok(AvroValue::Bytes(v.to_vec()))
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<AvroValue, E> {
                Ok(AvroValue::Bytes(v))
            }

            fn visit_none<E>(self) -> Result<AvroValue, E> {
                Ok(AvroValue::Null)
            }

            fn visit_unit<E>(self) -> Result<AvroValue, E> {
                Ok(AvroValue::Null)
            }

            fn visit_map<A>(self, mut map: A) -> Result<AvroValue, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut m = HashMap::new();
                while let Some((k, v)) = map.next_entry::<String, AvroValue>()? {
                    m.insert(k, v);
                }
                Ok(AvroValue::Object(m))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<AvroValue, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut v = Vec::new();
                while let Some(elem) = seq.next_element::<AvroValue>()? {
                    v.push(elem);
                }
                Ok(AvroValue::Array(v))
            }
        }

        deserializer.deserialize_any(AvroValueVisitor)
    }
}

pub(crate) struct AvroFormatReader;

const DEFAULT_BATCH_SIZE: usize = 8192;

#[async_trait]
impl FormatFileReader for AvroFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        _predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        // NOTE: Avro OCF requires sequential reading, so we load the entire file into memory.
        // This is fine for typical Paimon data files but may be problematic for very large files.
        let file_bytes = reader.read(0..file_size).await?;

        let read_fields = read_fields.to_vec();
        let target_schema = build_target_arrow_schema(&read_fields)?;
        let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

        // Deserialize all Avro records from the OCF file.
        let mut reader =
            serde_avro_fast::object_container_file_encoding::Reader::from_slice(&file_bytes)
                .map_err(|e| Error::UnexpectedError {
                    message: format!("Failed to open Avro file: {e}"),
                    source: Some(Box::new(e)),
                })?;

        let mut all_records: Vec<HashMap<String, AvroValue>> = Vec::new();
        for result in reader.deserialize_borrowed::<HashMap<String, AvroValue>>() {
            let record = result.map_err(|e| Error::UnexpectedError {
                message: format!("Failed to deserialize Avro record: {e}"),
                source: Some(Box::new(e)),
            })?;
            all_records.push(record);
        }

        // Apply row selection filtering.
        let records: Vec<HashMap<String, AvroValue>> = match row_selection {
            Some(ref ranges) => {
                let total_rows = all_records.len();
                let mask = ranges_to_mask(total_rows, ranges);
                all_records
                    .into_iter()
                    .enumerate()
                    .filter(|(i, _)| mask[*i])
                    .map(|(_, r)| r)
                    .collect()
            }
            None => all_records,
        };

        Ok(try_stream! {
            for chunk in records.chunks(batch_size) {
                let batch = records_to_batch(chunk, &read_fields, &target_schema)?;
                yield batch;
            }
        }
        .boxed())
    }
}

// ---------------------------------------------------------------------------
// Row ranges → boolean mask
// ---------------------------------------------------------------------------

fn ranges_to_mask(total_rows: usize, ranges: &[RowRange]) -> Vec<bool> {
    let mut mask = vec![false; total_rows];
    if total_rows == 0 {
        return mask;
    }
    let file_end = total_rows as i64 - 1;
    for r in ranges {
        let from = r.from().max(0) as usize;
        let to = (r.to().min(file_end) as usize).min(total_rows - 1);
        for item in mask.iter_mut().take(to + 1).skip(from) {
            *item = true;
        }
    }
    mask
}

// ---------------------------------------------------------------------------
// Avro records → Arrow RecordBatch conversion
// ---------------------------------------------------------------------------

fn records_to_batch(
    records: &[HashMap<String, AvroValue>],
    fields: &[DataField],
    schema: &SchemaRef,
) -> crate::Result<RecordBatch> {
    let num_rows = records.len();
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(fields.len());

    for field in fields {
        let col = build_column(records, field.name(), field.data_type(), num_rows)?;
        columns.push(col);
    }

    if columns.is_empty() {
        RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )
    } else {
        RecordBatch::try_new(schema.clone(), columns)
    }
    .map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build Avro RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

fn build_column(
    records: &[HashMap<String, AvroValue>],
    name: &str,
    data_type: &DataType,
    num_rows: usize,
) -> crate::Result<Arc<dyn arrow_array::Array>> {
    Ok(match data_type {
        DataType::Boolean(_) => {
            let arr: BooleanArray = (0..num_rows)
                .map(|i| get_field(&records[i], name).and_then(|v| v.as_bool()))
                .collect();
            Arc::new(arr)
        }
        DataType::TinyInt(_) => {
            let arr: Int8Array = (0..num_rows)
                .map(|i| {
                    get_field(&records[i], name)
                        .and_then(|v| v.as_i64())
                        .map(|v| v as i8)
                })
                .collect();
            Arc::new(arr)
        }
        DataType::SmallInt(_) => {
            let arr: Int16Array = (0..num_rows)
                .map(|i| {
                    get_field(&records[i], name)
                        .and_then(|v| v.as_i64())
                        .map(|v| v as i16)
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Int(_) => {
            let arr: Int32Array = (0..num_rows)
                .map(|i| {
                    get_field(&records[i], name)
                        .and_then(|v| v.as_i64())
                        .map(|v| v as i32)
                })
                .collect();
            Arc::new(arr)
        }
        DataType::BigInt(_) => {
            let arr: Int64Array = (0..num_rows)
                .map(|i| get_field(&records[i], name).and_then(|v| v.as_i64()))
                .collect();
            Arc::new(arr)
        }
        DataType::Float(_) => {
            let arr: Float32Array = (0..num_rows)
                .map(|i| {
                    get_field(&records[i], name)
                        .and_then(|v| v.as_f64())
                        .map(|v| v as f32)
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Double(_) => {
            let arr: Float64Array = (0..num_rows)
                .map(|i| get_field(&records[i], name).and_then(|v| v.as_f64()))
                .collect();
            Arc::new(arr)
        }
        DataType::Char(_) | DataType::VarChar(_) => {
            let arr: StringArray = (0..num_rows)
                .map(|i| get_field(&records[i], name).and_then(|v| v.as_str()))
                .collect();
            Arc::new(arr)
        }
        DataType::Binary(_) | DataType::VarBinary(_) => {
            let values: Vec<Option<&[u8]>> = (0..num_rows)
                .map(|i| get_field(&records[i], name).and_then(|v| v.as_bytes()))
                .collect();
            let arr: BinaryArray = values.into_iter().collect();
            Arc::new(arr)
        }
        DataType::Date(_) => {
            let arr: Date32Array = (0..num_rows)
                .map(|i| {
                    get_field(&records[i], name)
                        .and_then(|v| v.as_i64())
                        .map(|v| v as i32)
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Decimal(d) => {
            let precision = u8::try_from(d.precision()).map_err(|_| Error::Unsupported {
                message: "Decimal precision exceeds u8".to_string(),
            })?;
            let scale = i8::try_from(d.scale() as i32).map_err(|_| Error::Unsupported {
                message: "Decimal scale out of i8 range".to_string(),
            })?;
            let arr: Decimal128Array = (0..num_rows)
                .map(|i| {
                    get_field(&records[i], name).and_then(|v| match v {
                        // Avro decimal is encoded as big-endian two's complement bytes.
                        AvroValue::Bytes(b) => Some(bytes_to_i128_be(b)),
                        AvroValue::Int(n) => Some(*n as i128),
                        // serde_avro_fast may deserialize decimal as string representation.
                        AvroValue::String(s) => parse_decimal_string(s, scale),
                        _ => None,
                    })
                })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(precision, scale)
                .map_err(|e| Error::UnexpectedError {
                    message: format!("Failed to build Decimal128Array: {e}"),
                    source: Some(Box::new(e)),
                })?;
            Arc::new(arr)
        }
        DataType::Timestamp(t) => {
            build_timestamp_column(records, name, num_rows, t.precision(), None)
        }
        DataType::LocalZonedTimestamp(t) => build_timestamp_column(
            records,
            name,
            num_rows,
            t.precision(),
            Some(Arc::from("UTC")),
        ),
        DataType::Array(arr_type) => {
            build_array_column(records, name, arr_type.element_type(), num_rows)?
        }
        DataType::Map(map_type) => build_map_column(records, name, map_type, num_rows)?,
        DataType::Row(row_type) => build_row_column(records, name, row_type, num_rows)?,
        other => {
            return Err(Error::Unsupported {
                message: format!("Avro reader does not support data type: {other:?}"),
            });
        }
    })
}

fn build_timestamp_column(
    records: &[HashMap<String, AvroValue>],
    name: &str,
    num_rows: usize,
    precision: u32,
    tz: Option<Arc<str>>,
) -> Arc<dyn arrow_array::Array> {
    let values: Vec<Option<i64>> = (0..num_rows)
        .map(|i| get_field(&records[i], name).and_then(|v| v.as_i64()))
        .collect();
    match precision {
        0..=3 => Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(tz)),
        4..=6 => Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(tz)),
        _ => Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(tz)),
    }
}

fn build_array_column(
    records: &[HashMap<String, AvroValue>],
    name: &str,
    element_type: &DataType,
    num_rows: usize,
) -> crate::Result<Arc<dyn arrow_array::Array>> {
    let arrow_element_type = crate::arrow::paimon_type_to_arrow(element_type)?;
    let arrow_element_field =
        arrow_schema::Field::new("element", arrow_element_type, element_type.is_nullable());

    let mut offsets = vec![0i32];
    let mut element_records: Vec<HashMap<String, AvroValue>> = Vec::new();

    for record in records.iter().take(num_rows) {
        match get_field(record, name) {
            Some(AvroValue::Array(arr)) => {
                for elem in arr {
                    let mut m = HashMap::new();
                    m.insert("element".to_string(), elem.clone());
                    element_records.push(m);
                }
                offsets.push(offsets.last().unwrap() + arr.len() as i32);
            }
            _ => {
                offsets.push(*offsets.last().unwrap());
            }
        }
    }

    let element_col = build_column(
        &element_records,
        "element",
        element_type,
        element_records.len(),
    )?;

    let offsets_buf = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let nulls = NullBuffer::new(BooleanBuffer::from(
        (0..num_rows)
            .map(|i| get_field(&records[i], name).is_some())
            .collect::<Vec<_>>(),
    ));

    let list_arr = ListArray::try_new(
        Arc::new(arrow_element_field),
        offsets_buf,
        element_col,
        Some(nulls),
    )
    .map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build ListArray: {e}"),
        source: Some(Box::new(e)),
    })?;
    Ok(Arc::new(list_arr))
}

fn build_map_column(
    records: &[HashMap<String, AvroValue>],
    name: &str,
    map_type: &MapType,
    num_rows: usize,
) -> crate::Result<Arc<dyn arrow_array::Array>> {
    let arrow_key_type = crate::arrow::paimon_type_to_arrow(map_type.key_type())?;
    let arrow_value_type = crate::arrow::paimon_type_to_arrow(map_type.value_type())?;

    let mut offsets = vec![0i32];
    let mut key_records: Vec<HashMap<String, AvroValue>> = Vec::new();
    let mut value_records: Vec<HashMap<String, AvroValue>> = Vec::new();

    for record in records.iter().take(num_rows) {
        match get_field_raw(record, name) {
            Some(AvroValue::Object(map)) => {
                for (k, v) in map {
                    let mut km = HashMap::new();
                    km.insert("key".to_string(), AvroValue::String(k.clone()));
                    key_records.push(km);
                    let mut vm = HashMap::new();
                    vm.insert("value".to_string(), v.clone());
                    value_records.push(vm);
                }
                offsets.push(offsets.last().unwrap() + map.len() as i32);
            }
            _ => {
                offsets.push(*offsets.last().unwrap());
            }
        }
    }

    let key_col = build_column(&key_records, "key", map_type.key_type(), key_records.len())?;
    let value_col = build_column(
        &value_records,
        "value",
        map_type.value_type(),
        value_records.len(),
    )?;

    let struct_arr = StructArray::try_new(
        vec![
            Arc::new(arrow_schema::Field::new("key", arrow_key_type, false)),
            Arc::new(arrow_schema::Field::new(
                "value",
                arrow_value_type.clone(),
                map_type.value_type().is_nullable(),
            )),
        ]
        .into(),
        vec![key_col, value_col],
        None,
    )
    .map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build map StructArray: {e}"),
        source: Some(Box::new(e)),
    })?;

    let entries_field = arrow_schema::Field::new(
        "entries",
        arrow_schema::DataType::Struct(struct_arr.fields().clone()),
        false,
    );

    let offsets_buf = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let nulls = NullBuffer::new(BooleanBuffer::from(
        (0..num_rows)
            .map(|i| get_field_raw(&records[i], name).is_some())
            .collect::<Vec<_>>(),
    ));

    let map_arr = MapArray::try_new(
        Arc::new(entries_field),
        offsets_buf,
        struct_arr,
        Some(nulls),
        false,
    )
    .map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build MapArray: {e}"),
        source: Some(Box::new(e)),
    })?;
    Ok(Arc::new(map_arr))
}

fn build_row_column(
    records: &[HashMap<String, AvroValue>],
    name: &str,
    row_type: &RowType,
    num_rows: usize,
) -> crate::Result<Arc<dyn arrow_array::Array>> {
    let sub_records: Vec<HashMap<String, AvroValue>> = (0..num_rows)
        .map(|i| match get_field_raw(&records[i], name) {
            Some(AvroValue::Object(obj)) => obj.clone(),
            _ => HashMap::new(),
        })
        .collect();

    let mut child_columns: Vec<Arc<dyn arrow_array::Array>> = Vec::new();
    let mut arrow_fields: Vec<Arc<arrow_schema::Field>> = Vec::new();

    for field in row_type.fields() {
        let col = build_column(&sub_records, field.name(), field.data_type(), num_rows)?;
        let arrow_type = crate::arrow::paimon_type_to_arrow(field.data_type())?;
        arrow_fields.push(Arc::new(arrow_schema::Field::new(
            field.name(),
            arrow_type,
            field.data_type().is_nullable(),
        )));
        child_columns.push(col);
    }

    let nulls = NullBuffer::new(BooleanBuffer::from(
        (0..num_rows)
            .map(|i| get_field_raw(&records[i], name).is_some())
            .collect::<Vec<_>>(),
    ));

    let struct_arr = StructArray::try_new(arrow_fields.into(), child_columns, Some(nulls))
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to build StructArray: {e}"),
            source: Some(Box::new(e)),
        })?;
    Ok(Arc::new(struct_arr))
}

/// Parse a decimal string (e.g. "999.99") into unscaled i128 with the given scale.
/// For example, "999.99" with scale=2 → 99999; "0.000000000000000001" with scale=18 → 1.
fn parse_decimal_string(s: &str, scale: i8) -> Option<i128> {
    let negative = s.starts_with('-');
    let s = s.strip_prefix('-').unwrap_or(s);
    let (integer_part, frac_part) = match s.find('.') {
        Some(pos) => (&s[..pos], &s[pos + 1..]),
        None => (s, ""),
    };
    let frac_len = frac_part.len() as i8;
    let combined = format!("{}{}", integer_part, frac_part);
    let unscaled: i128 = combined.parse().ok()?;
    // Adjust if the fractional digits differ from the target scale.
    let result = if frac_len < scale {
        unscaled * 10i128.pow((scale - frac_len) as u32)
    } else if frac_len > scale {
        unscaled / 10i128.pow((frac_len - scale) as u32)
    } else {
        unscaled
    };
    Some(if negative { -result } else { result })
}

/// Decode big-endian two's complement bytes into i128 (Avro decimal encoding).
fn bytes_to_i128_be(bytes: &[u8]) -> i128 {
    if bytes.is_empty() {
        return 0;
    }
    // Sign-extend: if the high bit is set, fill with 0xFF, otherwise 0x00.
    let sign_byte = if bytes[0] & 0x80 != 0 { 0xFF } else { 0x00 };
    let mut buf = [sign_byte; 16];
    let start = 16 - bytes.len();
    buf[start..].copy_from_slice(bytes);
    i128::from_be_bytes(buf)
}

/// Look up a field in an Avro record, unwrapping union encoding.
fn get_field<'a>(record: &'a HashMap<String, AvroValue>, name: &str) -> Option<&'a AvroValue> {
    record.get(name).and_then(unwrap_avro_union)
}

/// Look up a field without union unwrapping — for Map/Row types whose Object
/// shape overlaps with union wrappers.
fn get_field_raw<'a>(record: &'a HashMap<String, AvroValue>, name: &str) -> Option<&'a AvroValue> {
    record.get(name).and_then(|v| match v {
        AvroValue::Null => None,
        other => Some(other),
    })
}

/// Unwrap Avro union encoding: `{"type": value}` → `value`, or `"null"` → `None`.
fn unwrap_avro_union(v: &AvroValue) -> Option<&AvroValue> {
    match v {
        AvroValue::Null => None,
        AvroValue::Object(map) if map.len() == 1 => {
            let (key, inner) = map.iter().next().unwrap();
            if key == "null" {
                None
            } else {
                Some(inner)
            }
        }
        other => Some(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        BigIntType, BooleanType, DataField, DataType, DecimalType, DoubleType, FloatType, IntType,
        SmallIntType, TinyIntType, VarBinaryType, VarCharType,
    };
    use arrow_array::Array;

    // Helper to build AvroValue variants concisely in tests.
    fn av_int(v: i64) -> AvroValue {
        AvroValue::Int(v)
    }
    fn av_float(v: f64) -> AvroValue {
        AvroValue::Float(v)
    }
    fn av_bool(v: bool) -> AvroValue {
        AvroValue::Bool(v)
    }
    fn av_str(v: &str) -> AvroValue {
        AvroValue::String(v.to_string())
    }
    fn av_bytes(v: &[u8]) -> AvroValue {
        AvroValue::Bytes(v.to_vec())
    }
    fn av_null() -> AvroValue {
        AvroValue::Null
    }
    fn av_union(key: &str, val: AvroValue) -> AvroValue {
        AvroValue::Object(HashMap::from([(key.to_string(), val)]))
    }

    // -----------------------------------------------------------------------
    // unwrap_avro_union
    // -----------------------------------------------------------------------

    #[test]
    fn test_unwrap_avro_union_null() {
        assert!(unwrap_avro_union(&av_null()).is_none());
    }

    #[test]
    fn test_unwrap_avro_union_plain_value() {
        let v = av_int(42);
        assert_eq!(unwrap_avro_union(&v), Some(&av_int(42)));
    }

    #[test]
    fn test_unwrap_avro_union_wrapped_value() {
        let v = av_union("int", av_int(42));
        assert_eq!(unwrap_avro_union(&v), Some(&av_int(42)));
    }

    #[test]
    fn test_unwrap_avro_union_null_key() {
        let v = av_union("null", av_null());
        assert!(unwrap_avro_union(&v).is_none());
    }

    // -----------------------------------------------------------------------
    // get_field
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_field_present() {
        let mut record = HashMap::new();
        record.insert("name".to_string(), av_str("alice"));
        assert_eq!(get_field(&record, "name"), Some(&av_str("alice")));
    }

    #[test]
    fn test_get_field_missing() {
        let record: HashMap<String, AvroValue> = HashMap::new();
        assert!(get_field(&record, "name").is_none());
    }

    #[test]
    fn test_get_field_union_wrapped() {
        let mut record = HashMap::new();
        record.insert("age".to_string(), av_union("int", av_int(30)));
        assert_eq!(get_field(&record, "age"), Some(&av_int(30)));
    }

    // -----------------------------------------------------------------------
    // ranges_to_mask
    // -----------------------------------------------------------------------

    #[test]
    fn test_ranges_to_mask_single_range() {
        let ranges = vec![RowRange::new(1, 3)];
        let mask = ranges_to_mask(5, &ranges);
        assert_eq!(mask, vec![false, true, true, true, false]);
    }

    #[test]
    fn test_ranges_to_mask_full_range() {
        let ranges = vec![RowRange::new(0, 4)];
        let mask = ranges_to_mask(5, &ranges);
        assert_eq!(mask, vec![true, true, true, true, true]);
    }

    #[test]
    fn test_ranges_to_mask_multiple_ranges() {
        let ranges = vec![RowRange::new(0, 0), RowRange::new(3, 4)];
        let mask = ranges_to_mask(5, &ranges);
        assert_eq!(mask, vec![true, false, false, true, true]);
    }

    // -----------------------------------------------------------------------
    // build_column + records_to_batch
    // -----------------------------------------------------------------------

    fn make_records(rows: Vec<Vec<(&str, AvroValue)>>) -> Vec<HashMap<String, AvroValue>> {
        rows.into_iter()
            .map(|fields| {
                fields
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn test_build_column_int() {
        let records = make_records(vec![
            vec![("x", av_int(1))],
            vec![("x", av_int(2))],
            vec![("x", av_int(3))],
        ]);
        let col = build_column(&records, "x", &DataType::Int(IntType::new()), 3).unwrap();
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.values().as_ref(), &[1, 2, 3]);
    }

    #[test]
    fn test_build_column_bigint() {
        let records = make_records(vec![vec![("v", av_int(100))], vec![("v", av_int(200))]]);
        let col = build_column(&records, "v", &DataType::BigInt(BigIntType::new()), 2).unwrap();
        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.values().as_ref(), &[100, 200]);
    }

    #[test]
    fn test_build_column_boolean() {
        let records = make_records(vec![
            vec![("b", av_bool(true))],
            vec![("b", av_bool(false))],
        ]);
        let col = build_column(&records, "b", &DataType::Boolean(BooleanType::new()), 2).unwrap();
        let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(arr.value(0));
        assert!(!arr.value(1));
    }

    #[test]
    fn test_build_column_string() {
        let records = make_records(vec![
            vec![("s", av_str("hello"))],
            vec![("s", av_str("world"))],
        ]);
        let col = build_column(
            &records,
            "s",
            &DataType::VarChar(VarCharType::new(100).unwrap()),
            2,
        )
        .unwrap();
        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "hello");
        assert_eq!(arr.value(1), "world");
    }

    #[test]
    fn test_build_column_float_double() {
        let records = make_records(vec![vec![("f", av_float(1.5)), ("d", av_float(2.5))]]);
        let fcol = build_column(&records, "f", &DataType::Float(FloatType::new()), 1).unwrap();
        let dcol = build_column(&records, "d", &DataType::Double(DoubleType::new()), 1).unwrap();
        let farr = fcol.as_any().downcast_ref::<Float32Array>().unwrap();
        let darr = dcol.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((farr.value(0) - 1.5_f32).abs() < f32::EPSILON);
        assert!((darr.value(0) - 2.5_f64).abs() < f64::EPSILON);
    }

    #[test]
    fn test_build_column_with_nulls() {
        let records = make_records(vec![
            vec![("x", av_int(10))],
            vec![("x", av_null())],
            vec![("x", av_int(30))],
        ]);
        let col = build_column(&records, "x", &DataType::Int(IntType::new()), 3).unwrap();
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
        assert!(!arr.is_null(2));
        assert_eq!(arr.value(0), 10);
        assert_eq!(arr.value(2), 30);
    }

    #[test]
    fn test_build_column_tinyint_smallint() {
        let records = make_records(vec![vec![("t", av_int(7)), ("s", av_int(300))]]);
        let tcol = build_column(&records, "t", &DataType::TinyInt(TinyIntType::new()), 1).unwrap();
        let scol =
            build_column(&records, "s", &DataType::SmallInt(SmallIntType::new()), 1).unwrap();
        let tarr = tcol.as_any().downcast_ref::<Int8Array>().unwrap();
        let sarr = scol.as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(tarr.value(0), 7);
        assert_eq!(sarr.value(0), 300);
    }

    #[test]
    fn test_build_column_binary() {
        let records = make_records(vec![
            vec![("b", av_bytes(&[0xDE, 0xAD]))],
            vec![("b", av_bytes(&[0xBE, 0xEF]))],
        ]);
        let col = build_column(
            &records,
            "b",
            &DataType::VarBinary(VarBinaryType::new(100).unwrap()),
            2,
        )
        .unwrap();
        let arr = col.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(arr.value(0), &[0xDE, 0xAD]);
        assert_eq!(arr.value(1), &[0xBE, 0xEF]);
    }

    #[test]
    fn test_build_column_decimal_from_bytes() {
        // 12345 in big-endian two's complement = [0x30, 0x39]
        let records = make_records(vec![
            vec![("d", av_bytes(&[0x30, 0x39]))],
            vec![("d", av_bytes(&[0x01, 0xA4]))], // 420
        ]);
        let col = build_column(
            &records,
            "d",
            &DataType::Decimal(DecimalType::new(10, 2).unwrap()),
            2,
        )
        .unwrap();
        let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(arr.value(0), 12345); // 123.45 * 100
        assert_eq!(arr.value(1), 420); // 4.20 * 100
    }

    #[test]
    fn test_bytes_to_i128_be() {
        assert_eq!(bytes_to_i128_be(&[0x30, 0x39]), 12345);
        assert_eq!(bytes_to_i128_be(&[0xFF]), -1);
        assert_eq!(bytes_to_i128_be(&[]), 0);
        assert_eq!(bytes_to_i128_be(&[0x00, 0x01]), 1);
    }

    #[test]
    fn test_records_to_batch_basic() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::new(50).unwrap()),
            ),
        ];
        let schema = crate::arrow::build_target_arrow_schema(&fields).unwrap();
        let records = make_records(vec![
            vec![("id", av_int(1)), ("name", av_str("alice"))],
            vec![("id", av_int(2)), ("name", av_str("bob"))],
        ]);
        let batch = records_to_batch(&records, &fields, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_records_to_batch_empty() {
        let fields = vec![DataField::new(
            0,
            "id".to_string(),
            DataType::Int(IntType::new()),
        )];
        let schema = crate::arrow::build_target_arrow_schema(&fields).unwrap();
        let records: Vec<HashMap<String, AvroValue>> = vec![];
        let batch = records_to_batch(&records, &fields, &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }
}
