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

//! Avro OCF writer compatible with Java's `AvroSchemaConverter`. Each Arrow
//! batch becomes an OCF block. The schema and sync marker are written once;
//! later blocks stream to storage without retaining batches or a whole file.

use std::collections::HashMap;

use apache_avro::types::Value;
use apache_avro::{Codec, Schema, Writer};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, ListArray, MapArray,
    RecordBatch, StringArray, StructArray, Time32MillisecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampSecondArray,
};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::{json, Value as JsonValue};

use super::{FormatFileWriter, FormatWriteResult};
use crate::io::{FileWrite, OutputFile};
use crate::spec::{CoreOptions, DataField, DataType};
use crate::{Error, Result};

pub(crate) struct AvroFormatWriter {
    writer: Box<dyn FileWrite>,
    schema: Schema,
    arrow_schema: SchemaRef,
    fields: Vec<DataField>,
    codec: Codec,
    marker: [u8; 16],
    block_size: usize,
    bytes_written: usize,
}

impl AvroFormatWriter {
    pub(crate) async fn new(
        output: &OutputFile,
        arrow_schema: SchemaRef,
        fields: Vec<DataField>,
        compression: &str,
        zstd_level: i32,
        options: Option<&HashMap<String, String>>,
    ) -> Result<Self> {
        let compression = options
            .and_then(|options| options.get("avro.codec"))
            .map(String::as_str)
            .unwrap_or(compression);
        let codec = match compression.to_ascii_lowercase().as_str() {
            "none" | "null" | "uncompressed" => Codec::Null,
            "snappy" => Codec::Snappy,
            "zstd" | "zstandard" => Codec::Zstandard(apache_avro::ZstandardSettings::new(
                zstd_level.clamp(0, 22) as u8,
            )),
            "deflate" => Codec::Deflate(Default::default()),
            other => {
                return Err(Error::Unsupported {
                    message: format!("Unsupported Avro compression codec '{other}'"),
                });
            }
        };
        let block_size = match options {
            Some(options) => CoreOptions::new(options)
                .file_block_size()?
                .unwrap_or(64 * 1024),
            None => 64 * 1024,
        };
        if !(32..=1024 * 1024 * 1024).contains(&block_size) {
            return Err(Error::ConfigInvalid {
                message: format!(
                    "file.block-size for avro must be between 32 bytes and 1 gb, but was {block_size} bytes"
                ),
            });
        }
        let schema_json = row_schema(&fields)?;
        let schema = Schema::parse_str(&schema_json.to_string()).map_err(avro_error)?;
        let header = Writer::with_codec(&schema, Vec::new(), codec)
            .into_inner()
            .map_err(avro_error)?;
        let marker: [u8; 16] = header[header.len() - 16..]
            .try_into()
            .expect("Avro OCF header ends with a 16-byte sync marker");
        let mut writer = output.writer().await?;
        writer.write(Bytes::from(header.clone())).await?;
        Ok(Self {
            writer,
            schema,
            arrow_schema,
            fields,
            codec,
            marker,
            block_size: block_size as usize,
            bytes_written: header.len(),
        })
    }
}

#[async_trait]
impl FormatFileWriter for AvroFormatWriter {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema() != self.arrow_schema {
            return Err(Error::DataInvalid {
                message: "Avro writer input schema differs from its file schema".into(),
                source: None,
            });
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mut block = Writer::builder()
            .schema(&self.schema)
            .writer(Vec::new())
            .codec(self.codec)
            .marker(self.marker)
            .has_header(true)
            .block_size(self.block_size)
            .build();
        for row in 0..batch.num_rows() {
            let record = self
                .fields
                .iter()
                .enumerate()
                .map(|(column, field)| {
                    Ok((
                        field.name().to_string(),
                        value_at(batch.column(column).as_ref(), row, field.data_type())?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            block.append(Value::Record(record)).map_err(avro_error)?;
        }
        let bytes = block.into_inner().map_err(avro_error)?;
        self.bytes_written += bytes.len();
        self.writer.write(Bytes::from(bytes)).await?;
        Ok(())
    }

    fn num_bytes(&self) -> usize {
        self.bytes_written
    }

    fn in_progress_size(&self) -> usize {
        0
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(mut self: Box<Self>) -> Result<FormatWriteResult> {
        self.writer.close().await?;
        Ok(FormatWriteResult::new(self.bytes_written as u64))
    }
}

fn avro_error(source: apache_avro::Error) -> Error {
    Error::DataInvalid {
        message: format!("Cannot write Avro data: {source}"),
        source: Some(Box::new(source)),
    }
}

fn row_schema(fields: &[DataField]) -> Result<JsonValue> {
    let fields = fields
        .iter()
        .map(|field| {
            let typ = avro_type(field.data_type(), &format!("record_{}", field.name()))?;
            let mut entry = json!({"name": field.name(), "type": typ});
            if field.data_type().is_nullable() {
                entry["default"] = JsonValue::Null;
            }
            Ok(entry)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({
        "type": "record",
        "name": "record",
        "namespace": "org.apache.paimon.avro.generated",
        "fields": fields,
    }))
}

fn avro_type(typ: &DataType, name: &str) -> Result<JsonValue> {
    let nonnull = match typ {
        DataType::Boolean(_) => json!("boolean"),
        DataType::TinyInt(_) | DataType::SmallInt(_) | DataType::Int(_) => json!("int"),
        DataType::BigInt(_) => json!("long"),
        DataType::Float(_) => json!("float"),
        DataType::Double(_) => json!("double"),
        DataType::Char(_) | DataType::VarChar(_) => json!("string"),
        DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob(_) => json!("bytes"),
        DataType::Date(_) => json!({"type": "int", "logicalType": "date"}),
        DataType::Time(time) if time.precision() <= 3 => {
            json!({"type": "int", "logicalType": "time-millis"})
        }
        DataType::Timestamp(timestamp) if timestamp.precision() <= 6 => {
            let logical = if timestamp.precision() <= 3 {
                "timestamp-millis"
            } else {
                "timestamp-micros"
            };
            json!({"type": "long", "logicalType": logical})
        }
        DataType::LocalZonedTimestamp(timestamp) if timestamp.precision() <= 6 => {
            let logical = if timestamp.precision() <= 3 {
                "local-timestamp-millis"
            } else {
                "local-timestamp-micros"
            };
            json!({"type": "long", "logicalType": logical})
        }
        DataType::Decimal(decimal) => json!({
            "type": "bytes", "logicalType": "decimal",
            "precision": decimal.precision(), "scale": decimal.scale(),
        }),
        DataType::Array(array) => {
            json!({"type": "array", "items": avro_type(array.element_type(), name)?})
        }
        DataType::Row(row) => {
            let fields = row
                .fields()
                .iter()
                .map(|field| {
                    let child_name = format!("{name}_{}", field.name());
                    let mut entry = json!({
                        "name": field.name(),
                        "type": avro_type(field.data_type(), &child_name)?,
                    });
                    if field.data_type().is_nullable() {
                        entry["default"] = JsonValue::Null;
                    }
                    Ok(entry)
                })
                .collect::<Result<Vec<_>>>()?;
            json!({"type": "record", "name": name, "fields": fields})
        }
        DataType::Map(map) => avro_map_type(map.key_type(), map.value_type(), name)?,
        DataType::Multiset(multiset) => avro_map_type(
            multiset.element_type(),
            &DataType::Int(crate::spec::IntType::new()),
            name,
        )?,
        other => {
            return Err(Error::Unsupported {
                message: format!("Avro writer does not support data type {other:?}"),
            });
        }
    };
    if typ.is_nullable() {
        Ok(json!(["null", nonnull]))
    } else {
        Ok(nonnull)
    }
}

fn avro_map_type(key: &DataType, value: &DataType, name: &str) -> Result<JsonValue> {
    if matches!(key, DataType::Char(_) | DataType::VarChar(_)) {
        Ok(json!({"type": "map", "values": avro_type(value, name)?}))
    } else {
        let key_name = format!("{name}_key");
        let value_name = format!("{name}_value");
        Ok(json!({
            "type": "array",
            "logicalType": "map",
            "items": {
                "type": "record", "name": name,
                "fields": [
                    {"name": "key", "type": avro_type(key, &key_name)?},
                    {"name": "value", "type": avro_type(value, &value_name)?},
                ],
            },
        }))
    }
}

fn value_at(array: &dyn Array, row: usize, typ: &DataType) -> Result<Value> {
    if array.is_null(row) {
        if !typ.is_nullable() {
            return Err(Error::DataInvalid {
                message: format!("Cannot write null to non-null Avro type {typ:?}"),
                source: None,
            });
        }
        return Ok(Value::Union(0, Box::new(Value::Null)));
    }
    let value = nonnull_value_at(array, row, typ)?;
    if typ.is_nullable() {
        Ok(Value::Union(1, Box::new(value)))
    } else {
        Ok(value)
    }
}

macro_rules! primitive {
    ($array:expr, $row:expr, $ty:ty, $variant:ident) => {
        Value::$variant(
            $array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| Error::DataInvalid {
                    message: format!("Avro input has wrong Arrow type for {}", stringify!($ty)),
                    source: None,
                })?
                .value($row)
                .into(),
        )
    };
}

fn nonnull_value_at(array: &dyn Array, row: usize, typ: &DataType) -> Result<Value> {
    let value = match typ {
        DataType::Boolean(_) => primitive!(array, row, BooleanArray, Boolean),
        DataType::TinyInt(_) => primitive!(array, row, Int8Array, Int),
        DataType::SmallInt(_) => primitive!(array, row, Int16Array, Int),
        DataType::Int(_) => primitive!(array, row, Int32Array, Int),
        DataType::BigInt(_) => primitive!(array, row, Int64Array, Long),
        DataType::Float(_) => primitive!(array, row, Float32Array, Float),
        DataType::Double(_) => primitive!(array, row, Float64Array, Double),
        DataType::Char(_) | DataType::VarChar(_) => {
            Value::String(downcast::<StringArray>(array)?.value(row).to_string())
        }
        DataType::Binary(_) | DataType::VarBinary(_) => {
            Value::Bytes(downcast::<BinaryArray>(array)?.value(row).to_vec())
        }
        DataType::Blob(_) => Value::Bytes(downcast::<LargeBinaryArray>(array)?.value(row).to_vec()),
        DataType::Date(_) => Value::Date(downcast::<Date32Array>(array)?.value(row)),
        DataType::Time(_) => {
            Value::TimeMillis(downcast::<Time32MillisecondArray>(array)?.value(row))
        }
        DataType::Timestamp(timestamp) if timestamp.precision() == 0 => Value::TimestampMillis(
            seconds_to_millis(downcast::<TimestampSecondArray>(array)?.value(row))?,
        ),
        DataType::Timestamp(timestamp) if timestamp.precision() <= 3 => {
            Value::TimestampMillis(downcast::<TimestampMillisecondArray>(array)?.value(row))
        }
        DataType::Timestamp(_) => {
            Value::TimestampMicros(downcast::<TimestampMicrosecondArray>(array)?.value(row))
        }
        DataType::LocalZonedTimestamp(timestamp) if timestamp.precision() == 0 => {
            Value::LocalTimestampMillis(seconds_to_millis(
                downcast::<TimestampSecondArray>(array)?.value(row),
            )?)
        }
        DataType::LocalZonedTimestamp(timestamp) if timestamp.precision() <= 3 => {
            Value::LocalTimestampMillis(downcast::<TimestampMillisecondArray>(array)?.value(row))
        }
        DataType::LocalZonedTimestamp(_) => {
            Value::LocalTimestampMicros(downcast::<TimestampMicrosecondArray>(array)?.value(row))
        }
        DataType::Decimal(_) => {
            let raw = downcast::<Decimal128Array>(array)?.value(row);
            Value::Decimal(apache_avro::Decimal::from(decimal_bytes(raw)))
        }
        DataType::Array(array_type) => {
            let list = downcast::<ListArray>(array)?;
            let values = list.value(row);
            Value::Array(
                (0..values.len())
                    .map(|index| value_at(values.as_ref(), index, array_type.element_type()))
                    .collect::<Result<Vec<_>>>()?,
            )
        }
        DataType::Row(row_type) => {
            let structure = downcast::<StructArray>(array)?;
            Value::Record(
                row_type
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        Ok((
                            field.name().to_string(),
                            value_at(structure.column(index).as_ref(), row, field.data_type())?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
        }
        DataType::Map(map_type) => {
            map_value(array, row, map_type.key_type(), map_type.value_type())?
        }
        DataType::Multiset(multiset) => map_value(
            array,
            row,
            multiset.element_type(),
            &DataType::Int(crate::spec::IntType::new()),
        )?,
        other => {
            return Err(Error::Unsupported {
                message: format!("Avro writer does not support data type {other:?}"),
            });
        }
    };
    Ok(value)
}

fn map_value(array: &dyn Array, row: usize, key: &DataType, value: &DataType) -> Result<Value> {
    let map = downcast::<MapArray>(array)?;
    let start = map.value_offsets()[row] as usize;
    let end = map.value_offsets()[row + 1] as usize;
    if matches!(key, DataType::Char(_) | DataType::VarChar(_)) {
        let mut entries = HashMap::with_capacity(end - start);
        for index in start..end {
            let map_key = downcast::<StringArray>(map.keys())?.value(index);
            entries.insert(
                map_key.to_string(),
                value_at(map.values().as_ref(), index, value)?,
            );
        }
        Ok(Value::Map(entries))
    } else {
        let mut entries = Vec::with_capacity(end - start);
        for index in start..end {
            entries.push(Value::Record(vec![
                ("key".into(), value_at(map.keys(), index, key)?),
                (
                    "value".into(),
                    value_at(map.values().as_ref(), index, value)?,
                ),
            ]));
        }
        Ok(Value::Array(entries))
    }
}

fn decimal_bytes(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let mut first = 0;
    while first < bytes.len() - 1
        && ((bytes[first] == 0 && bytes[first + 1] & 0x80 == 0)
            || (bytes[first] == 0xff && bytes[first + 1] & 0x80 != 0))
    {
        first += 1;
    }
    bytes[first..].to_vec()
}

fn seconds_to_millis(seconds: i64) -> Result<i64> {
    seconds.checked_mul(1000).ok_or_else(|| Error::DataInvalid {
        message: "Avro timestamp seconds overflow milliseconds".into(),
        source: None,
    })
}

fn downcast<T: Array + 'static>(array: &dyn Array) -> Result<&T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Avro writer expected Arrow array {}",
                std::any::type_name::<T>()
            ),
            source: None,
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{Int32Builder, MapBuilder, MapFieldNames, StringBuilder};
    use arrow_array::types::Int32Type;
    use arrow_array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use futures::TryStreamExt;

    use super::*;
    use crate::arrow::build_target_arrow_schema;
    use crate::arrow::format::{avro::AvroFormatReader, FormatFileReader};
    use crate::btree::test_util::BytesFileRead;
    use crate::io::FileIOBuilder;
    use crate::spec::{ArrayType, BigIntType, IntType, MapType, RowType, VarCharType};

    fn fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".into(), DataType::BigInt(BigIntType::new())),
            DataField::new(
                1,
                "name".into(),
                DataType::VarChar(VarCharType::string_type()),
            ),
            DataField::new(2, "score".into(), DataType::Int(IntType::new())),
        ]
    }

    #[tokio::test]
    async fn streams_multiple_nullable_batches_as_one_java_readable_ocf() {
        let fields = fields();
        let schema = build_target_arrow_schema(&fields).unwrap();
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/avro-writer/part.avro";
        let output = file_io.new_output(path).unwrap();
        let mut writer =
            AvroFormatWriter::new(&output, schema.clone(), fields.clone(), "zstd", 1, None)
                .await
                .unwrap();
        let first = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(Int32Array::from(vec![Some(10), Some(-5)])),
            ],
        )
        .unwrap();
        let second = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(3)])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("c")])),
                Arc::new(Int32Array::from(vec![None])),
            ],
        )
        .unwrap();
        writer.write(&first).await.unwrap();
        let first_size = writer.num_bytes();
        writer.write(&second).await.unwrap();
        assert!(writer.num_bytes() > first_size);
        assert_eq!(writer.in_progress_size(), 0);
        let size = Box::new(writer).close().await.unwrap().file_size;

        let bytes = file_io.new_input(path).unwrap().read().await.unwrap();
        assert_eq!(size, bytes.len() as u64);
        let reader = apache_avro::Reader::new(bytes.as_ref()).unwrap();
        let rows = reader.collect::<std::result::Result<Vec<_>, _>>().unwrap();
        assert_eq!(rows.len(), 3);
        let Value::Record(first) = &rows[0] else {
            panic!("expected record");
        };
        assert_eq!(first[0].1, Value::Union(1, Box::new(Value::Long(1))));
        assert_eq!(
            first[1].1,
            Value::Union(1, Box::new(Value::String("a".into())))
        );
        let Value::Record(second) = &rows[1] else {
            panic!("expected record");
        };
        assert_eq!(second[0].1, Value::Union(0, Box::new(Value::Null)));
        assert_eq!(second[1].1, Value::Union(0, Box::new(Value::Null)));

        let decoded = AvroFormatReader
            .read_batch_stream(
                Box::new(BytesFileRead(bytes.clone())),
                bytes.len() as u64,
                &fields,
                None,
                None,
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(decoded.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        let ids = decoded[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.iter().collect::<Vec<_>>(), vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn schema_matches_java_nullable_fields_and_logical_types() {
        use crate::spec::{DateType, DecimalType, TimeType, TimestampType};
        let fields = vec![
            DataField::new(0, "day".into(), DataType::Date(DateType::new())),
            DataField::new(1, "at".into(), DataType::Time(TimeType::new(3).unwrap())),
            DataField::new(
                2,
                "created".into(),
                DataType::Timestamp(TimestampType::new(6).unwrap()),
            ),
            DataField::new(
                3,
                "amount".into(),
                DataType::Decimal(DecimalType::new(12, 2).unwrap()),
            ),
        ];
        let schema = row_schema(&fields).unwrap();
        let actual = schema.to_string();
        for logical in ["date", "time-millis", "timestamp-micros", "decimal"] {
            assert!(actual.contains(logical), "missing {logical}: {actual}");
        }
        for field in schema["fields"].as_array().unwrap() {
            assert_eq!(field["type"][0], "null");
            assert_eq!(field["default"], JsonValue::Null);
        }
        Schema::parse_str(&actual).unwrap();
    }

    #[test]
    fn decimal_encoding_keeps_two_complement_sign_and_minimal_length() {
        assert_eq!(decimal_bytes(0), vec![0]);
        assert_eq!(decimal_bytes(127), vec![127]);
        assert_eq!(decimal_bytes(128), vec![0, 128]);
        assert_eq!(decimal_bytes(-1), vec![255]);
        assert_eq!(decimal_bytes(-129), vec![255, 127]);
    }

    #[test]
    fn zero_precision_timestamps_use_java_millisecond_logical_type() {
        use crate::spec::{LocalZonedTimestampType, TimestampType};
        let local = DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(0).unwrap());
        let plain = DataType::Timestamp(TimestampType::new(0).unwrap());
        let seconds = TimestampSecondArray::from(vec![Some(3), Some(-2), None]);
        assert_eq!(
            value_at(&seconds, 0, &plain).unwrap(),
            Value::Union(1, Box::new(Value::TimestampMillis(3000)))
        );
        assert_eq!(
            value_at(&seconds, 1, &local).unwrap(),
            Value::Union(1, Box::new(Value::LocalTimestampMillis(-2000)))
        );
        assert_eq!(
            value_at(&seconds, 2, &plain).unwrap(),
            Value::Union(0, Box::new(Value::Null))
        );
        let extreme = TimestampSecondArray::from(vec![i64::MAX]);
        assert!(value_at(&extreme, 0, &plain)
            .unwrap_err()
            .to_string()
            .contains("overflow"));
    }

    #[test]
    fn nested_schema_matches_java_array_map_and_row_layouts() {
        let fields = vec![
            DataField::new(
                0,
                "numbers".into(),
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            ),
            DataField::new(
                1,
                "named".into(),
                DataType::Map(MapType::new(
                    DataType::VarChar(VarCharType::string_type()),
                    DataType::Int(IntType::new()),
                )),
            ),
            DataField::new(
                2,
                "numbered".into(),
                DataType::Map(MapType::new(
                    DataType::Int(IntType::new()),
                    DataType::VarChar(VarCharType::string_type()),
                )),
            ),
            DataField::new(
                3,
                "nested".into(),
                DataType::Row(RowType::new(vec![DataField::new(
                    4,
                    "label".into(),
                    DataType::VarChar(VarCharType::string_type()),
                )])),
            ),
        ];
        let schema = row_schema(&fields).unwrap();
        assert_eq!(schema["fields"][0]["type"][1]["type"], "array");
        assert_eq!(schema["fields"][1]["type"][1]["type"], "map");
        assert_eq!(schema["fields"][2]["type"][1]["logicalType"], "map");
        assert_eq!(schema["fields"][2]["type"][1]["items"]["type"], "record");
        assert_eq!(schema["fields"][3]["type"][1]["fields"][0]["name"], "label");
        Schema::parse_str(&schema.to_string()).unwrap();
    }

    #[test]
    fn nested_arrow_values_use_java_map_and_array_encodings() {
        let int = DataType::Int(IntType::new());
        let text = DataType::VarChar(VarCharType::string_type());
        let lists = arrow_array::ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None, Some(-2)]),
            None,
        ]);
        let list_type = DataType::Array(ArrayType::new(int.clone()));
        assert_eq!(
            value_at(&lists, 0, &list_type).unwrap(),
            Value::Union(
                1,
                Box::new(Value::Array(vec![
                    Value::Union(1, Box::new(Value::Int(1))),
                    Value::Union(0, Box::new(Value::Null)),
                    Value::Union(1, Box::new(Value::Int(-2))),
                ]))
            )
        );
        assert_eq!(
            value_at(&lists, 1, &list_type).unwrap(),
            Value::Union(0, Box::new(Value::Null))
        );

        let names = MapFieldNames {
            entry: "entries".into(),
            key: "key".into(),
            value: "value".into(),
        };
        let mut string_map = MapBuilder::new(
            Some(names.clone()),
            StringBuilder::new(),
            Int32Builder::new(),
        );
        string_map.keys().append_value("x");
        string_map.values().append_value(7);
        string_map.append(true).unwrap();
        let string_map = string_map.finish();
        let string_map_type = DataType::Map(MapType::new(text.clone(), int.clone()));
        assert_eq!(
            value_at(&string_map, 0, &string_map_type).unwrap(),
            Value::Union(
                1,
                Box::new(Value::Map(HashMap::from([(
                    "x".into(),
                    Value::Union(1, Box::new(Value::Int(7))),
                )])))
            )
        );

        let mut int_map = MapBuilder::new(Some(names), Int32Builder::new(), StringBuilder::new());
        int_map.keys().append_value(3);
        int_map.values().append_value("three");
        int_map.append(true).unwrap();
        let int_map = int_map.finish();
        let int_map_type = DataType::Map(MapType::new(int, text));
        assert_eq!(
            value_at(&int_map, 0, &int_map_type).unwrap(),
            Value::Union(
                1,
                Box::new(Value::Array(vec![Value::Record(vec![
                    ("key".into(), Value::Union(1, Box::new(Value::Int(3)))),
                    (
                        "value".into(),
                        Value::Union(1, Box::new(Value::String("three".into())))
                    ),
                ])]))
            )
        );
    }

    #[tokio::test]
    async fn nested_arrays_and_maps_survive_file_roundtrip() {
        let number_type = DataType::Int(IntType::new());
        let text_type = DataType::VarChar(VarCharType::string_type());
        let fields = vec![
            DataField::new(
                0,
                "numbers".into(),
                DataType::Array(ArrayType::new(number_type.clone())),
            ),
            DataField::new(
                1,
                "named".into(),
                DataType::Map(MapType::new(text_type, number_type)),
            ),
        ];
        let numbers = arrow_array::ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None, Some(-2)]),
            Some(vec![]),
        ]);
        let mut map = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".into(),
                key: "key".into(),
                value: "value".into(),
            }),
            StringBuilder::new(),
            Int32Builder::new(),
        );
        map.keys().append_value("first");
        map.values().append_value(7);
        map.append(true).unwrap();
        map.append(true).unwrap();
        let map = map.finish();
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("numbers", numbers.data_type().clone(), true),
            arrow_schema::Field::new("named", map.data_type().clone(), true),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(numbers), Arc::new(map)]).unwrap();
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/avro-writer/nested.avro";
        let output = file_io.new_output(path).unwrap();
        let mut writer = AvroFormatWriter::new(&output, schema, fields.clone(), "zstd", 1, None)
            .await
            .unwrap();
        writer.write(&batch).await.unwrap();
        Box::new(writer).close().await.unwrap();
        let bytes = file_io.new_input(path).unwrap().read().await.unwrap();
        let decoded = AvroFormatReader
            .read_batch_stream(
                Box::new(BytesFileRead(bytes.clone())),
                bytes.len() as u64,
                &fields,
                None,
                None,
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 2);
        let decoded_numbers = decoded[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        assert_eq!(decoded_numbers.value_length(0), 3);
        assert_eq!(decoded_numbers.value_length(1), 0);
        let decoded_map = decoded[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::MapArray>()
            .unwrap();
        assert_eq!(decoded_map.value_length(0), 1);
        assert_eq!(decoded_map.value_length(1), 0);
    }

    #[tokio::test]
    async fn rejects_unknown_codec_before_opening_output() {
        let fields = fields();
        let schema = build_target_arrow_schema(&fields).unwrap();
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let output = file_io.new_output("memory:/avro-writer/bad.avro").unwrap();
        let result = AvroFormatWriter::new(&output, schema, fields, "brotli", 1, None).await;
        assert!(matches!(result, Err(Error::Unsupported { .. })));
        assert!(!output.exists().await.unwrap());
    }

    #[tokio::test]
    async fn avro_codec_option_overrides_file_compression() {
        let fields = fields();
        let schema = build_target_arrow_schema(&fields).unwrap();
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let output = file_io
            .new_output("memory:/avro-writer/override.avro")
            .unwrap();
        let options = HashMap::from([
            ("avro.codec".to_string(), "snappy".to_string()),
            ("file.block-size".to_string(), "32 b".to_string()),
        ]);
        let writer = AvroFormatWriter::new(&output, schema, fields, "zstd", 1, Some(&options))
            .await
            .unwrap();
        assert_eq!(writer.codec, Codec::Snappy);
        assert_eq!(writer.block_size, 32);
        Box::new(writer).close().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_avro_block_size_fails_before_file_creation() {
        let fields = fields();
        let schema = build_target_arrow_schema(&fields).unwrap();
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let output = file_io
            .new_output("memory:/avro-writer/bad-block.avro")
            .unwrap();
        for invalid in ["31", "2 gb", "-1", "abc"] {
            let options = HashMap::from([("file.block-size".to_string(), invalid.to_string())]);
            let error = AvroFormatWriter::new(
                &output,
                schema.clone(),
                fields.clone(),
                "zstd",
                1,
                Some(&options),
            )
            .await
            .err()
            .unwrap();
            assert!(
                error.to_string().contains("file.block-size"),
                "{invalid}: {error}"
            );
        }
        assert!(!output.exists().await.unwrap());
    }
}
