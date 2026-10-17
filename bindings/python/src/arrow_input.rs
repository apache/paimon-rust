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

//! Preserve C Data offsets before Arrow Rust materializes sparse Union arrays.

use std::sync::Arc;

use arrow::array::{make_array, ArrayData, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType, Schema, UnionMode};
use arrow::error::ArrowError;
use arrow::pyarrow::FromPyArrow;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

fn contains_sparse_union(data_type: &DataType) -> bool {
    match data_type {
        DataType::Union(_, UnionMode::Sparse) => true,
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, f)| contains_sparse_union(f.data_type())),
        DataType::Struct(fields) => fields.iter().any(|f| contains_sparse_union(f.data_type())),
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::Map(f, _) => contains_sparse_union(f.data_type()),
        DataType::Dictionary(_, values) => contains_sparse_union(values),
        DataType::RunEndEncoded(_, values) => contains_sparse_union(values.data_type()),
        _ => false,
    }
}

pub(crate) fn array_from_pyarrow(value: &Bound<'_, PyAny>) -> PyResult<ArrayRef> {
    let mut data = ArrayData::from_pyarrow_bound(value)?;
    // Foreign buffers may be unaligned; normalize only after making them
    // valid for Arrow Rust's typed access and ArrayData builder validation.
    data.align_buffers();
    let data = normalize_sparse_unions(data).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(make_array(data))
}

pub(crate) fn batch_from_pyarrow(value: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    let schema = Arc::new(Schema::from_pyarrow_bound(&value.getattr("schema")?)?);
    if !schema
        .fields()
        .iter()
        .any(|f| contains_sparse_union(f.data_type()))
    {
        return RecordBatch::from_pyarrow_bound(value);
    }
    // The standard batch importer materializes its children before we can fix
    // Union offsets. Import each column's C Data representation first instead.
    let columns = value
        .getattr("columns")?
        .try_iter()?
        .map(|column| array_from_pyarrow(&column?))
        .collect::<PyResult<Vec<_>>>()?;
    let rows = value.getattr("num_rows")?.extract::<usize>()?;
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::default().with_row_count(Some(rows)),
    )
    .map_err(|e| PyValueError::new_err(e.to_string()))
}

// ArrayData::slice already propagates STRUCT offsets into its children. C Data
// has unsliced children, so preserve that representation until each parent is
// normalized. Slicing through ArrayData::slice here would apply offsets twice.
fn slice_imported(data: ArrayData, offset: usize, len: usize) -> Result<ArrayData, ArrowError> {
    let nulls = data.nulls().map(|n| n.slice(offset, len));
    let offset = data.offset() + offset;
    data.into_builder()
        .offset(offset)
        .len(len)
        .nulls(nulls)
        .build()
}

fn normalize_sparse_unions(data: ArrayData) -> Result<ArrayData, ArrowError> {
    if !contains_sparse_union(data.data_type()) {
        return Ok(data);
    }
    let (data_type, len, nulls, mut offset, mut buffers, children) = data.into_parts();
    let children = match &data_type {
        DataType::Struct(_) | DataType::Union(_, UnionMode::Sparse) => {
            if matches!(data_type, DataType::Union(_, UnionMode::Sparse)) {
                // UnionArray::from(ArrayData) applies the parent offset to type
                // IDs but not sparse children. Canonicalize both before that.
                buffers[0] = buffers[0].slice_with_length(offset, len);
            }
            let children = children
                .into_iter()
                .map(|c| slice_imported(c, offset, len))
                .collect::<Result<Vec<_>, _>>()?;
            offset = 0;
            children
        }
        DataType::FixedSizeList(_, width) => {
            let width = *width as usize;
            let children = children
                .into_iter()
                .map(|c| slice_imported(c, offset * width, len * width))
                .collect::<Result<Vec<_>, _>>()?;
            offset = 0;
            children
        }
        _ => children,
    };
    let children = children
        .into_iter()
        .map(normalize_sparse_unions)
        .collect::<Result<Vec<_>, _>>()?;
    ArrayData::builder(data_type)
        .len(len)
        .offset(offset)
        .nulls(nulls)
        .buffers(buffers)
        .child_data(children)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, FixedSizeListArray, Int32Array, StringArray, StructArray, UnionArray,
    };
    use arrow::buffer::ScalarBuffer;
    use arrow::datatypes::{Field, UnionFields};

    fn input() -> ArrayData {
        UnionArray::try_new(
            UnionFields::try_new(
                [7, 42],
                [
                    Field::new("n", DataType::Int32, true),
                    Field::new("s", DataType::Utf8, true),
                ],
            )
            .unwrap(),
            ScalarBuffer::from(vec![42, 7, 7, 42]),
            None,
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 0])),
                Arc::new(StringArray::from(vec!["hidden"; 4])),
            ],
        )
        .unwrap()
        .to_data()
    }

    fn assert_values(data: ArrayData) {
        let array = UnionArray::from(data);
        assert_eq!(array.type_ids().as_ref(), &[7, 7]);
        assert_eq!(
            array.child(7).to_data(),
            Int32Array::from(vec![1, 2]).to_data()
        );
    }

    #[test]
    fn c_data_sparse_union_slice_rebases_children_and_type_ids() {
        let raw = slice_imported(input(), 1, 2).unwrap();
        assert_values(normalize_sparse_unions(raw).unwrap());
    }

    #[test]
    fn c_data_parent_offsets_are_applied_before_union_materialization() {
        let child = input();
        let rows = ArrayData::builder(DataType::Struct(
            vec![Field::new("u", child.data_type().clone(), true)].into(),
        ))
        .offset(1)
        .len(2)
        .child_data(vec![child.clone()])
        .build()
        .unwrap();
        let rows = StructArray::from(normalize_sparse_unions(rows).unwrap());
        assert_values(rows.column(0).to_data());
        // Nested STRUCT children are still in the original C Data coordinate
        // system; the outer slice must not be propagated into them twice.
        let nested = ArrayData::builder(DataType::Struct(
            vec![Field::new("u", child.data_type().clone(), true)].into(),
        ))
        .len(4)
        .child_data(vec![child.clone()])
        .build()
        .unwrap();
        let outer = ArrayData::builder(DataType::Struct(
            vec![Field::new("row", nested.data_type().clone(), true)].into(),
        ))
        .offset(1)
        .len(2)
        .child_data(vec![nested])
        .build()
        .unwrap();
        let outer = StructArray::from(normalize_sparse_unions(outer).unwrap());
        let nested = outer
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_values(nested.column(0).to_data());
        let lists = ArrayData::builder(DataType::FixedSizeList(
            Arc::new(Field::new("item", child.data_type().clone(), true)),
            1,
        ))
        .offset(1)
        .len(2)
        .child_data(vec![child])
        .build()
        .unwrap();
        let lists = FixedSizeListArray::from(normalize_sparse_unions(lists).unwrap());
        assert_values(lists.values().to_data());
    }
}
