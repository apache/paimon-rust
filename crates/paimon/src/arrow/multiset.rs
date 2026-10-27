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

//! A MULTISET permits NULL elements, whereas Arrow Map keys cannot be NULL.
//! Use a list of element/count entries and mark it to distinguish ARRAY<ROW>.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, StructArray,
};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};

use crate::{Error, Result};

const MULTISET_KEY: &str = "paimon.multiset";

pub(crate) fn entries_field(element: DataType, nullable: bool) -> FieldRef {
    Arc::new(
        Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", element, nullable),
                    Field::new("value", DataType::Int32, false),
                ]
                .into(),
            ),
            false,
        )
        .with_metadata([(MULTISET_KEY.into(), "true".into())].into()),
    )
}

pub(crate) fn is_multiset(field: &Field) -> bool {
    field
        .metadata()
        .get(MULTISET_KEY)
        .is_some_and(|value| value == "true")
}

pub(crate) fn contains_multiset(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field) if is_multiset(field) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _)
        | DataType::RunEndEncoded(_, field) => contains_multiset(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_multiset(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains_multiset(field.data_type())),
        DataType::Dictionary(_, values) => contains_multiset(values),
        _ => false,
    }
}

pub(crate) fn entries(array: &dyn Array, row: usize) -> Result<StructArray> {
    let entries = if let Some(map) = array.as_any().downcast_ref::<MapArray>() {
        Some(map.value(row))
    } else if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        list.value(row)
            .as_any()
            .downcast_ref::<StructArray>()
            .cloned()
    } else {
        None
    };
    entries
        .filter(|entries| entries.num_columns() == 2 && entries.null_count() == 0)
        .ok_or_else(|| Error::DataInvalid {
            message: "MULTISET must contain non-null element/count structs with exactly two fields"
                .into(),
            source: None,
        })
}

/// Java Parquet represents MULTISET as MAP and rejects NULL elements. Preserve
/// that disk layout while keeping nullable elements representable elsewhere.
pub(crate) fn parquet_schema(schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(parquet_field)
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

fn parquet_field(field: &FieldRef) -> FieldRef {
    let data_type = match field.data_type() {
        DataType::List(child) if is_multiset(child) => {
            let DataType::Struct(fields) = child.data_type() else {
                unreachable!("MULTISET entries are structs")
            };
            let fields = vec![
                Arc::new(
                    parquet_field(&fields[0])
                        .as_ref()
                        .clone()
                        .with_nullable(false),
                ),
                parquet_field(&fields[1]),
            ];
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(fields.into()),
                    false,
                )),
                false,
            )
        }
        DataType::List(child) => DataType::List(parquet_field(child)),
        DataType::LargeList(child) => DataType::LargeList(parquet_field(child)),
        DataType::FixedSizeList(child, size) => {
            DataType::FixedSizeList(parquet_field(child), *size)
        }
        DataType::Map(child, sorted) => DataType::Map(parquet_field(child), *sorted),
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(parquet_field).collect::<Vec<_>>().into())
        }
        data_type => data_type.clone(),
    };
    Arc::new(field.as_ref().clone().with_data_type(data_type))
}

/// Convert logical MULTISET lists to the Parquet MAP layout, descending through
/// nested containers. Scalar conversion retains the existing timestamp casts.
pub(crate) fn to_parquet_array(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    // NULL parents and sliced-away entries have no logical keys. Remove their
    // hidden children before Parquet's non-null key validation.
    let array = crate::table::normalize_write_array(array, array.data_type())?;
    convert_parquet_array(&array, target)
}

fn convert_parquet_array(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(array.clone());
    }
    let converted: ArrayRef = match (array.data_type(), target) {
        (DataType::List(child), DataType::Map(entries, sorted)) if is_multiset(child) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = convert_parquet_array(list.values(), entries.data_type())?;
            let values = values.as_any().downcast_ref::<StructArray>().unwrap();
            Arc::new(
                MapArray::try_new(
                    entries.clone(),
                    list.offsets().clone(),
                    values.clone(),
                    list.nulls().cloned(),
                    *sorted,
                )
                .map_err(storage_error)?,
            )
        }
        (DataType::List(_), DataType::List(child)) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            Arc::new(
                ListArray::try_new(
                    child.clone(),
                    list.offsets().clone(),
                    convert_parquet_array(list.values(), child.data_type())?,
                    list.nulls().cloned(),
                )
                .map_err(storage_error)?,
            )
        }
        (DataType::LargeList(_), DataType::LargeList(child)) => {
            let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            Arc::new(
                LargeListArray::try_new(
                    child.clone(),
                    list.offsets().clone(),
                    convert_parquet_array(list.values(), child.data_type())?,
                    list.nulls().cloned(),
                )
                .map_err(storage_error)?,
            )
        }
        (DataType::FixedSizeList(_, size), DataType::FixedSizeList(child, _)) => {
            let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            Arc::new(
                FixedSizeListArray::try_new_with_length(
                    child.clone(),
                    *size,
                    convert_parquet_array(list.values(), child.data_type())?,
                    list.nulls().cloned(),
                    list.len(),
                )
                .map_err(storage_error)?,
            )
        }
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let structure = array.as_any().downcast_ref::<StructArray>().unwrap();
            let columns = structure
                .columns()
                .iter()
                .zip(fields)
                .map(|(column, field)| convert_parquet_array(column, field.data_type()))
                .collect::<Result<Vec<_>>>()?;
            Arc::new(
                StructArray::try_new_with_length(
                    fields.clone(),
                    columns,
                    structure.nulls().cloned(),
                    structure.len(),
                )
                .map_err(storage_error)?,
            )
        }
        (DataType::Map(_, _), DataType::Map(entries, sorted)) => {
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            let values: ArrayRef = Arc::new(map.entries().clone());
            let values = convert_parquet_array(&values, entries.data_type())?;
            Arc::new(
                MapArray::try_new(
                    entries.clone(),
                    map.offsets().clone(),
                    values
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .unwrap()
                        .clone(),
                    map.nulls().cloned(),
                    *sorted,
                )
                .map_err(storage_error)?,
            )
        }
        _ => arrow_cast::cast(array, target).map_err(storage_error)?,
    };
    Ok(converted)
}

fn storage_error(source: arrow_schema::ArrowError) -> Error {
    Error::DataInvalid { message: format!("Cannot convert MULTISET to Parquet storage (Parquet map keys cannot be NULL): {source}"), source: Some(Box::new(source)) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_buffer::{NullBuffer, OffsetBuffer};
    use bytes::Bytes;
    use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};

    fn bag(values: Vec<Option<i32>>, nulls: Option<NullBuffer>) -> ArrayRef {
        let field = entries_field(DataType::Int32, true);
        let DataType::Struct(fields) = field.data_type() else {
            unreachable!()
        };
        let len = values.len();
        let entries = StructArray::try_new(
            fields.clone(),
            vec![
                Arc::new(Int32Array::from(values)),
                Arc::new(Int32Array::from(vec![3; len])),
            ],
            None,
        )
        .unwrap();
        Arc::new(
            ListArray::try_new(
                field,
                OffsetBuffer::new((0..=len as i32).collect::<Vec<_>>().into()),
                Arc::new(entries),
                nulls,
            )
            .unwrap(),
        )
    }

    #[test]
    fn parquet_multiset_retains_java_map_layout_and_evolves_back_to_list() {
        let logical = bag(vec![Some(2), Some(4)], None);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "bag",
            logical.data_type().clone(),
            true,
        )]));
        let physical = parquet_schema(&schema);
        assert!(matches!(
            physical.field(0).data_type(),
            DataType::Map(_, false)
        ));
        let array = to_parquet_array(&logical, physical.field(0).data_type()).unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, physical.clone(), None).unwrap();
        writer
            .write(&RecordBatch::try_new(physical, vec![array]).unwrap())
            .unwrap();
        writer.close().unwrap();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let typ = crate::spec::DataType::Multiset(crate::spec::MultisetType::new(
            crate::spec::DataType::Int(crate::spec::IntType::new()),
        ));
        let restored =
            crate::arrow::nested_evolution::evolve_column(batch.column(0), &typ, &typ).unwrap();
        assert_eq!(&logical.to_data(), &restored.to_data());
    }

    #[test]
    fn parquet_drops_hidden_null_elements_but_rejects_visible_ones() {
        let source = bag(
            vec![None, None, Some(4)],
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "bag",
            source.data_type().clone(),
            true,
        )]));
        let physical = parquet_schema(&schema);
        assert!(to_parquet_array(&source, physical.field(0).data_type()).is_err());
        // A sliced-away first bag and a NULL second bag hide both NULL keys.
        let converted =
            to_parquet_array(&source.slice(1, 2), physical.field(0).data_type()).unwrap();
        let map = converted.as_any().downcast_ref::<MapArray>().unwrap();
        assert!(map.is_null(0));
        assert_eq!(
            map.keys()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[4]
        );
    }

    #[test]
    fn legacy_parquet_optional_keys_remain_readable() {
        use parquet::arrow::arrow_writer::ArrowWriterOptions;
        use parquet::schema::{parser::parse_message_type, types::SchemaDescriptor};
        for values in [vec![Some(2), Some(4)], vec![None, Some(4)]] {
            let logical = bag(values, None);
            let schema = Arc::new(Schema::new(vec![Field::new(
                "bag",
                logical.data_type().clone(),
                true,
            )]));
            // Arrow 58 accepted nullable MULTISET keys and emitted this
            // non-compliant Parquet schema even when every actual key was valid.
            let descriptor = SchemaDescriptor::new(Arc::new(parse_message_type(
            "message schema { optional group bag (MAP) { repeated group key_value { optional int32 key; required int32 value; } } }",
        ).unwrap()));
            let options = ArrowWriterOptions::new()
                .with_parquet_schema(descriptor)
                .with_skip_arrow_metadata(true);
            let mut bytes = Vec::new();
            let mut writer =
                ArrowWriter::try_new_with_options(&mut bytes, schema.clone(), options).unwrap();
            writer
                .write(&RecordBatch::try_new(schema, vec![logical.clone()]).unwrap())
                .unwrap();
            writer.close().unwrap();
            let batch = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
                .unwrap()
                .build()
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            let typ = crate::spec::DataType::Multiset(crate::spec::MultisetType::new(
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            ));
            let restored =
                crate::arrow::nested_evolution::evolve_column(batch.column(0), &typ, &typ).unwrap();
            assert_eq!(logical.to_data(), restored.to_data());
        }
    }

    #[test]
    fn multiset_detection_descends_through_rows_arrays_and_maps() {
        let bag = DataType::List(entries_field(DataType::Int32, true));
        let nested = DataType::Struct(
            vec![Field::new(
                "bags",
                DataType::List(Arc::new(Field::new("element", bag, true))),
                true,
            )]
            .into(),
        );
        assert!(contains_multiset(&nested));
        let map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", nested, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert!(contains_multiset(&map));
    }

    #[test]
    fn ordinary_arrays_of_key_value_rows_keep_list_storage() {
        let field = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Int32, true),
                    Field::new("value", DataType::Int32, false),
                ]
                .into(),
            ),
            false,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "array",
            DataType::List(Arc::new(field)),
            true,
        )]));
        assert_eq!(parquet_schema(&schema), schema);
    }
}
