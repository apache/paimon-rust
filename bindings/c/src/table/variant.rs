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

//! Canonical Arrow representation of Paimon VARIANT at the C Data boundary.
use arrow::buffer::NullBuffer;
use arrow_array::{Array, ArrayRef, BinaryArray, ListArray, MapArray, RecordBatch, StructArray};
use arrow_schema::{DataType as ArrowDataType, Schema as ArrowSchema};
use futures::StreamExt;
use paimon::spec::{DataField, DataType};
use paimon::table::ArrowRecordBatchStream;
use std::collections::HashMap;
use std::sync::Arc;

fn contains_variant(data_type: &DataType) -> bool {
    match data_type {
        DataType::Variant(_) => true,
        DataType::Multiset(multiset) => contains_variant(multiset.element_type()),
        DataType::Array(array) => contains_variant(array.element_type()),
        DataType::Map(map) => {
            contains_variant(map.key_type()) || contains_variant(map.value_type())
        }
        DataType::Row(row) => row
            .fields()
            .iter()
            .any(|field| contains_variant(field.data_type())),
        _ => false,
    }
}

fn invalid_array(path: &str, expected: &str, actual: &ArrowDataType) -> paimon::Error {
    paimon::Error::DataInvalid {
        message: format!("Expected {expected} for Paimon column '{path}', got {actual:?}"),
        source: None,
    }
}

fn normalize_variant_array(array: &ArrayRef, path: &str) -> paimon::Result<ArrayRef> {
    let input = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid_array(path, "StructArray", array.data_type()))?;
    if input.num_columns() != 2 {
        return Err(invalid_array(
            path,
            "two-field Variant struct",
            array.data_type(),
        ));
    }
    let values = input
        .column_by_name("value")
        .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
        .ok_or_else(|| invalid_array(path, "BinaryArray Variant.value", array.data_type()))?;
    let metadata = input
        .column_by_name("metadata")
        .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
        .ok_or_else(|| invalid_array(path, "BinaryArray Variant.metadata", array.data_type()))?;

    // Keep the native payload byte-for-byte: JSON loses Variant scalar types.
    // Only canonicalize the child schema and empty-pair SQL-null encoding.
    let canonical_type = paimon::arrow::variant_arrow_type();
    let mut rebuild = input.data_type() != &canonical_type;
    let mut validity = Vec::with_capacity(input.len());
    for row in 0..input.len() {
        let value_is_empty = values.is_null(row) || values.value(row).is_empty();
        let metadata_is_empty = metadata.is_null(row) || metadata.value(row).is_empty();
        if input.is_null(row) || (value_is_empty && metadata_is_empty) {
            validity.push(false);
            rebuild |= input.is_valid(row) || values.is_null(row) || metadata.is_null(row);
            continue;
        }
        if value_is_empty || metadata_is_empty {
            return Err(paimon::Error::DataInvalid {
                message: format!("Incomplete Variant payload in column '{path}' at row {row}"),
                source: None,
            });
        }
        paimon::variant::validate_payload(values.value(row), metadata.value(row))?;
        validity.push(true);
    }
    if !rebuild {
        return Ok(Arc::clone(array));
    }
    let children: Vec<ArrayRef> = [values, metadata]
        .into_iter()
        .map(|child| {
            Arc::new(BinaryArray::from_iter_values((0..input.len()).map(|row| {
                if validity[row] {
                    child.value(row)
                } else {
                    &[]
                }
            }))) as ArrayRef
        })
        .collect();
    let ArrowDataType::Struct(fields) = canonical_type else {
        unreachable!("Variant Arrow type is a struct");
    };
    let nulls = validity
        .iter()
        .any(|valid| !valid)
        .then(|| NullBuffer::from(validity));
    StructArray::try_new(fields, children, nulls)
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(|error| paimon::Error::UnexpectedError {
            message: format!("Failed to normalize Variant column '{path}': {error}"),
            source: Some(Box::new(error)),
        })
}

fn canonicalize_variants(
    array: &ArrayRef,
    data_type: &DataType,
    path: &str,
) -> paimon::Result<ArrayRef> {
    // Do not reinterpret an ordinary ROW<value BINARY, metadata BINARY> as a
    // Variant just because its physical shape happens to match.
    if !contains_variant(data_type) {
        return Ok(Arc::clone(array));
    }

    match data_type {
        DataType::Variant(_) => normalize_variant_array(array, path),
        DataType::Array(array_type) => {
            let input = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| invalid_array(path, "ListArray", array.data_type()))?;
            let input = paimon::arrow::visible_generic_list(input)?;
            let values = canonicalize_variants(
                input.values(),
                array_type.element_type(),
                &format!("{path}[]"),
            )?;
            let ArrowDataType::List(element_field) = input.data_type() else {
                return Err(invalid_array(path, "Arrow List", input.data_type()));
            };
            let element_field = Arc::new(
                element_field
                    .as_ref()
                    .clone()
                    .with_data_type(values.data_type().clone()),
            );
            Ok(Arc::new(ListArray::new(
                element_field,
                input.offsets().clone(),
                values,
                input.nulls().cloned(),
            )))
        }
        DataType::Map(_) | DataType::Multiset(_) => {
            let input = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| invalid_array(path, "MapArray", array.data_type()))?;
            let input = paimon::arrow::visible_map(input)?;
            let ArrowDataType::Map(entries_field, ordered) = input.data_type() else {
                return Err(invalid_array(path, "Arrow Map", input.data_type()));
            };
            let ArrowDataType::Struct(entry_fields) = entries_field.data_type() else {
                return Err(invalid_array(
                    path,
                    "Arrow Map entries Struct",
                    input.data_type(),
                ));
            };
            if entry_fields.len() != 2 {
                return Err(paimon::Error::DataInvalid {
                    message: format!("Expected two Map entry fields for Paimon column '{path}'"),
                    source: None,
                });
            }
            let (key_type, value_type) = match data_type {
                DataType::Map(map) => (map.key_type(), Some(map.value_type())),
                DataType::Multiset(multiset) => (multiset.element_type(), None),
                _ => unreachable!(),
            };
            let keys = canonicalize_variants(input.keys(), key_type, &format!("{path}.key"))?;
            let values = match value_type {
                Some(value_type) => {
                    canonicalize_variants(input.values(), value_type, &format!("{path}.value"))?
                }
                None => Arc::clone(input.values()),
            };
            let entry_fields = vec![
                Arc::new(
                    entry_fields[0]
                        .as_ref()
                        .clone()
                        .with_data_type(keys.data_type().clone()),
                ),
                Arc::new(
                    entry_fields[1]
                        .as_ref()
                        .clone()
                        .with_data_type(values.data_type().clone()),
                ),
            ]
            .into();
            let entries =
                StructArray::try_new(entry_fields, vec![keys, values], None).map_err(|error| {
                    paimon::Error::UnexpectedError {
                        message: format!("Failed to normalize Map column '{path}': {error}"),
                        source: Some(Box::new(error)),
                    }
                })?;
            let entries_field = Arc::new(
                entries_field
                    .as_ref()
                    .clone()
                    .with_data_type(entries.data_type().clone()),
            );
            Ok(Arc::new(MapArray::new(
                entries_field,
                input.offsets().clone(),
                entries,
                input.nulls().cloned(),
                *ordered,
            )))
        }
        DataType::Row(row_type) => {
            let input = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| invalid_array(path, "StructArray", array.data_type()))?;
            if input.num_columns() != row_type.fields().len() {
                return Err(paimon::Error::DataInvalid {
                    message: format!("Arrow ROW field count mismatch for Paimon column '{path}'"),
                    source: None,
                });
            }
            let ArrowDataType::Struct(arrow_fields) = input.data_type() else {
                return Err(invalid_array(path, "Arrow Struct", input.data_type()));
            };
            let (_, visible_children) = input.flatten();
            let mut fields = Vec::with_capacity(row_type.fields().len());
            let mut columns = Vec::with_capacity(row_type.fields().len());
            for (index, field) in row_type.fields().iter().enumerate() {
                let column = canonicalize_variants(
                    &visible_children[index],
                    field.data_type(),
                    &format!("{path}.{}", field.name()),
                )?;
                fields.push(Arc::new(
                    arrow_fields[index]
                        .as_ref()
                        .clone()
                        .with_data_type(column.data_type().clone()),
                ));
                columns.push(column);
            }
            if input
                .columns()
                .iter()
                .zip(&columns)
                .all(|(a, b)| Arc::ptr_eq(a, b))
            {
                return Ok(Arc::clone(array));
            }
            Ok(Arc::new(
                StructArray::try_new(fields.into(), columns, input.nulls().cloned()).map_err(
                    |error| paimon::Error::UnexpectedError {
                        message: format!("Failed to normalize ROW column '{path}': {error}"),
                        source: Some(Box::new(error)),
                    },
                )?,
            ))
        }
        _ => Ok(Arc::clone(array)),
    }
}

pub(crate) fn canonicalize_read_stream(
    stream: ArrowRecordBatchStream,
    read_type: Vec<DataField>,
) -> ArrowRecordBatchStream {
    stream
        .map(move |batch| canonicalize_batch(batch?, &read_type))
        .boxed()
}

pub(super) fn canonicalize_batch(
    batch: RecordBatch,
    read_type: &[DataField],
) -> paimon::Result<RecordBatch> {
    if batch.num_columns() != read_type.len() {
        return Err(paimon::Error::DataInvalid {
            message: format!(
                "Arrow batch column count {} does not match Paimon read type {}",
                batch.num_columns(),
                read_type.len()
            ),
            source: None,
        });
    }
    canonicalize_columns(
        batch,
        read_type
            .iter()
            .enumerate()
            .filter(|(_, field)| contains_variant(field.data_type())),
    )
}

/// A materialized vector batch has projected user columns followed by the search score.
/// Resolve VARIANT columns once in projection order and leave the score untouched.
pub(crate) fn canonicalize_vector_stream(
    stream: ArrowRecordBatchStream,
    table_fields: &[DataField],
    projection: Option<&[String]>,
) -> ArrowRecordBatchStream {
    let positions = projection.map(|names| {
        names
            .iter()
            .enumerate()
            .map(|(index, name)| (name.as_str(), index))
            .collect::<HashMap<_, _>>()
    });
    let variants: Vec<_> = table_fields
        .iter()
        .enumerate()
        .filter(|(_, field)| contains_variant(field.data_type()))
        .filter_map(|(index, field)| {
            let index = match &positions {
                Some(positions) => *positions.get(field.name())?,
                None => index,
            };
            Some((index, field.clone()))
        })
        .collect();
    if variants.is_empty() {
        return stream;
    }
    stream
        .map(move |batch| {
            canonicalize_columns(batch?, variants.iter().map(|(i, field)| (*i, field)))
        })
        .boxed()
}

fn canonicalize_columns<'a>(
    batch: RecordBatch,
    projected: impl Iterator<Item = (usize, &'a DataField)>,
) -> paimon::Result<RecordBatch> {
    let input_schema = batch.schema();
    let mut updated = None;
    for (index, field) in projected {
        let input = batch.column(index);
        let column = canonicalize_variants(input, field.data_type(), field.name())?;
        if !Arc::ptr_eq(input, &column) {
            let (fields, columns) = updated
                .get_or_insert_with(|| (input_schema.fields().to_vec(), batch.columns().to_vec()));
            fields[index] = Arc::new(
                fields[index]
                    .as_ref()
                    .clone()
                    .with_data_type(column.data_type().clone()),
            );
            columns[index] = column;
        }
    }
    let Some((fields, columns)) = updated else {
        return Ok(batch);
    };
    let schema = Arc::new(ArrowSchema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns).map_err(|error| paimon::Error::UnexpectedError {
        message: format!("Failed to build Arrow batch with canonical VARIANT fields: {error}"),
        source: Some(Box::new(error)),
    })
}
#[cfg(test)]
#[path = "native_variant_tests.rs"]
mod tests;
