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

//! Zero-copy C Data Interface bridge for Mosaic and Vortex's Arrow 58 APIs.
//! Release callbacks keep ownership of buffers in the exporting Arrow version.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Schema};

use crate::{Error, Result};

fn bridge_error(source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::DataInvalid {
        message: "Cannot bridge Arrow 58 and Arrow 59 data".into(),
        source: Some(Box::new(source)),
    }
}

pub(crate) fn to_legacy_schema(schema: &Schema) -> Result<arrow_schema_58::Schema> {
    let schema = arrow_schema::ffi::FFI_ArrowSchema::try_from(schema).map_err(bridge_error)?;
    // SAFETY: Both versions implement the same repr(C) ArrowSchema ABI. The
    // exporter remains alive while the importer copies the schema and metadata.
    let schema =
        unsafe { &*std::ptr::from_ref(&schema).cast::<arrow_schema_58::ffi::FFI_ArrowSchema>() };
    arrow_schema_58::Schema::try_from(schema).map_err(bridge_error)
}

fn from_legacy_schema(schema: &arrow_schema_58::Schema) -> Result<Schema> {
    let schema = arrow_schema_58::ffi::FFI_ArrowSchema::try_from(schema).map_err(bridge_error)?;
    // SAFETY: ArrowSchema has the same C ABI in both versions, and the exported
    // schema outlives this import, which copies its fields and metadata.
    let schema =
        unsafe { &*std::ptr::from_ref(&schema).cast::<arrow_schema::ffi::FFI_ArrowSchema>() };
    Schema::try_from(schema).map_err(bridge_error)
}

pub(crate) fn to_legacy_data_type(data_type: &DataType) -> Result<arrow_schema_58::DataType> {
    let schema = Schema::new(vec![arrow_schema::Field::new("", data_type.clone(), true)]);
    Ok(to_legacy_schema(&schema)?.field(0).data_type().clone())
}

fn to_legacy_array(array: &ArrayRef) -> Result<arrow_array_58::ArrayRef> {
    let (mut array, schema) = arrow_array::ffi::to_ffi(&array.to_data()).map_err(bridge_error)?;
    // SAFETY: The C Data Interface structs share a stable ABI. from_raw moves
    // ownership and marks the source array released, preventing a double drop.
    // from_ffi retains the exporter's release callback until all buffers die.
    let data = unsafe {
        let array =
            arrow_array_58::ffi::FFI_ArrowArray::from_raw(std::ptr::from_mut(&mut array).cast());
        let schema = &*std::ptr::from_ref(&schema).cast();
        arrow_array_58::ffi::from_ffi(array, schema)
    }
    .map_err(bridge_error)?;
    Ok(arrow_array_58::make_array(data))
}

pub(crate) fn from_legacy_array(array: &arrow_array_58::ArrayRef) -> Result<ArrayRef> {
    let (mut array, schema) =
        arrow_array_58::ffi::to_ffi(&array.to_data()).map_err(bridge_error)?;
    // SAFETY: This is the inverse of to_legacy_array: the shared C ABI moves
    // ownership via from_raw, and imported buffers retain the release callback.
    let data = unsafe {
        let array =
            arrow_array::ffi::FFI_ArrowArray::from_raw(std::ptr::from_mut(&mut array).cast());
        let schema = &*std::ptr::from_ref(&schema).cast();
        arrow_array::ffi::from_ffi(array, schema)
    }
    .map_err(bridge_error)?;
    Ok(arrow_array::make_array(data))
}

pub(crate) fn to_legacy_batch(batch: &RecordBatch) -> Result<arrow_array_58::RecordBatch> {
    let schema = Arc::new(to_legacy_schema(batch.schema().as_ref())?);
    let columns = batch
        .columns()
        .iter()
        .map(to_legacy_array)
        .collect::<Result<_>>()?;
    arrow_array_58::RecordBatch::try_new_with_options(
        schema,
        columns,
        &arrow_array_58::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(bridge_error)
}

pub(crate) fn from_legacy_batch(batch: &arrow_array_58::RecordBatch) -> Result<RecordBatch> {
    let schema = Arc::new(from_legacy_schema(batch.schema().as_ref())?);
    let columns = batch
        .columns()
        .iter()
        .map(from_legacy_array)
        .collect::<Result<_>>()?;
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(bridge_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{types::Int32Type, Int32Array, ListArray, StringArray};
    use arrow_schema::Field;

    #[test]
    fn round_trip_preserves_slices_nulls_nested_values_metadata_and_buffer_ownership() {
        let ids = Arc::new(Int32Array::from(vec![Some(0), Some(1), None, Some(3)]));
        let names = Arc::new(StringArray::from(vec![
            Some("skip"),
            Some("one"),
            None,
            Some("三"),
        ]));
        let lists = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(0)]),
            Some(vec![Some(1), None]),
            None,
            Some(vec![]),
        ]));
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int32, true)
                    .with_metadata([("field_id".into(), "42".into())].into()),
                Field::new("name", DataType::Utf8, true),
                Field::new("values", lists.data_type().clone(), true),
            ],
            [("table".into(), "bridge".into())].into(),
        ));
        let original = RecordBatch::try_new(schema, vec![ids, names, lists])
            .unwrap()
            .slice(1, 3);
        let expected = original.clone();
        let legacy = to_legacy_batch(&original).unwrap();
        drop(original);
        let imported = from_legacy_batch(&legacy).unwrap();
        drop(legacy);
        assert_eq!(imported, expected);
        let ids = imported
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let original_ids = expected
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values().as_ptr(), original_ids.values().as_ptr());
        drop(expected);
        assert_eq!(ids.value(0), 1);
        assert!(ids.is_null(1));
        assert_eq!(ids.value(2), 3);
    }

    #[test]
    fn round_trip_preserves_row_counts_without_columns() {
        for rows in [0, 7] {
            let batch = RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .unwrap();
            assert_eq!(
                from_legacy_batch(&to_legacy_batch(&batch).unwrap()).unwrap(),
                batch
            );
        }
    }
}
