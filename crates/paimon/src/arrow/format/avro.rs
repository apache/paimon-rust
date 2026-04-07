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
use crate::spec::{DataField, DataType};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::{
    BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
};
use arrow_schema::SchemaRef;
use async_stream::try_stream;
use async_trait::async_trait;
use futures::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

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

        let mut all_records: Vec<HashMap<String, Value>> = Vec::new();
        for result in reader.deserialize_borrowed::<HashMap<String, Value>>() {
            let record = result.map_err(|e| Error::UnexpectedError {
                message: format!("Failed to deserialize Avro record: {e}"),
                source: Some(Box::new(e)),
            })?;
            all_records.push(record);
        }

        // Apply row selection filtering.
        let records: Vec<HashMap<String, Value>> = match row_selection {
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
    let file_end = total_rows as i64 - 1;
    for r in ranges {
        let from = r.from().max(0) as usize;
        let to = (r.to().min(file_end) as usize).min(total_rows - 1);
        for i in from..=to {
            mask[i] = true;
        }
    }
    mask
}

// ---------------------------------------------------------------------------
// Avro records → Arrow RecordBatch conversion
// ---------------------------------------------------------------------------

fn records_to_batch(
    records: &[HashMap<String, Value>],
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
    records: &[HashMap<String, Value>],
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
                .map(|i| {
                    get_field(&records[i], name).and_then(|v| match v {
                        Value::String(s) => Some(s.as_bytes()),
                        _ => None,
                    })
                })
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
                    get_field(&records[i], name)
                        .and_then(|v| v.as_i64())
                        .map(|v| v as i128)
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
        other => {
            return Err(Error::Unsupported {
                message: format!("Avro reader does not support data type: {other:?}"),
            });
        }
    })
}

fn build_timestamp_column(
    records: &[HashMap<String, Value>],
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

/// Look up a field in an Avro record, unwrapping union encoding.
fn get_field<'a>(record: &'a HashMap<String, Value>, name: &str) -> Option<&'a Value> {
    record.get(name).and_then(unwrap_avro_union)
}

/// Unwrap Avro union encoding: `{"type": value}` → `value`, or `"null"` → `None`.
fn unwrap_avro_union(v: &Value) -> Option<&Value> {
    match v {
        Value::Null => None,
        Value::Object(map) if map.len() == 1 => {
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
        BigIntType, BooleanType, DataField, DataType, DoubleType, FloatType, IntType, SmallIntType,
        TinyIntType, VarCharType,
    };
    use arrow_array::Array;
    use serde_json::json;

    // -----------------------------------------------------------------------
    // unwrap_avro_union
    // -----------------------------------------------------------------------

    #[test]
    fn test_unwrap_avro_union_null() {
        assert!(unwrap_avro_union(&Value::Null).is_none());
    }

    #[test]
    fn test_unwrap_avro_union_plain_value() {
        let v = json!(42);
        assert_eq!(unwrap_avro_union(&v), Some(&json!(42)));
    }

    #[test]
    fn test_unwrap_avro_union_wrapped_value() {
        let v = json!({"int": 42});
        assert_eq!(unwrap_avro_union(&v), Some(&json!(42)));
    }

    #[test]
    fn test_unwrap_avro_union_null_key() {
        let v = json!({"null": null});
        assert!(unwrap_avro_union(&v).is_none());
    }

    // -----------------------------------------------------------------------
    // get_field
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_field_present() {
        let mut record = HashMap::new();
        record.insert("name".to_string(), json!("alice"));
        assert_eq!(get_field(&record, "name"), Some(&json!("alice")));
    }

    #[test]
    fn test_get_field_missing() {
        let record: HashMap<String, Value> = HashMap::new();
        assert!(get_field(&record, "name").is_none());
    }

    #[test]
    fn test_get_field_union_wrapped() {
        let mut record = HashMap::new();
        record.insert("age".to_string(), json!({"int": 30}));
        assert_eq!(get_field(&record, "age"), Some(&json!(30)));
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

    fn make_records(rows: Vec<Vec<(&str, Value)>>) -> Vec<HashMap<String, Value>> {
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
            vec![("x", json!(1))],
            vec![("x", json!(2))],
            vec![("x", json!(3))],
        ]);
        let col = build_column(&records, "x", &DataType::Int(IntType::new()), 3).unwrap();
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.values().as_ref(), &[1, 2, 3]);
    }

    #[test]
    fn test_build_column_bigint() {
        let records = make_records(vec![
            vec![("v", json!(100_i64))],
            vec![("v", json!(200_i64))],
        ]);
        let col = build_column(&records, "v", &DataType::BigInt(BigIntType::new()), 2).unwrap();
        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.values().as_ref(), &[100, 200]);
    }

    #[test]
    fn test_build_column_boolean() {
        let records = make_records(vec![vec![("b", json!(true))], vec![("b", json!(false))]]);
        let col = build_column(&records, "b", &DataType::Boolean(BooleanType::new()), 2).unwrap();
        let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(arr.value(0));
        assert!(!arr.value(1));
    }

    #[test]
    fn test_build_column_string() {
        let records = make_records(vec![
            vec![("s", json!("hello"))],
            vec![("s", json!("world"))],
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
        let records = make_records(vec![vec![("f", json!(1.5)), ("d", json!(2.5))]]);
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
            vec![("x", json!(10))],
            vec![("x", Value::Null)],
            vec![("x", json!(30))],
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
        let records = make_records(vec![vec![("t", json!(7)), ("s", json!(300))]]);
        let tcol = build_column(&records, "t", &DataType::TinyInt(TinyIntType::new()), 1).unwrap();
        let scol =
            build_column(&records, "s", &DataType::SmallInt(SmallIntType::new()), 1).unwrap();
        let tarr = tcol.as_any().downcast_ref::<Int8Array>().unwrap();
        let sarr = scol.as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(tarr.value(0), 7);
        assert_eq!(sarr.value(0), 300);
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
            vec![("id", json!(1)), ("name", json!("alice"))],
            vec![("id", json!(2)), ("name", json!("bob"))],
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
        let records: Vec<HashMap<String, Value>> = vec![];
        let batch = records_to_batch(&records, &fields, &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }
}
