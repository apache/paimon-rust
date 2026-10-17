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

fn normalize_sparse_unions(data: ArrayData) -> Result<ArrayData, ArrowError> {
    if !contains_sparse_union(data.data_type()) {
        return Ok(data);
    }
    normalize_imported_data(data)
}

fn normalize_imported_data(data: ArrayData) -> Result<ArrayData, ArrowError> {
    let len = data.len();
    normalize_imported_slice(data, 0, len)
}

fn normalize_imported_slice(
    data: ArrayData,
    relative_offset: usize,
    len: usize,
) -> Result<ArrayData, ArrowError> {
    // Once a parent is sliced, normalize every descendant, including ordinary
    // STRUCT siblings and Union children. Arrow's STRUCT materializer uses
    // ArrayData::slice, which can otherwise apply nested offsets twice.
    let (data_type, _, nulls, offset, mut buffers, children) = data.into_parts();
    let nulls = nulls.map(|n| n.slice(relative_offset, len));
    let mut offset = offset + relative_offset;
    // Defer validation until both parent and children share their coordinates.
    // An intermediate ArrayData slice would validate a sliced parent's NULLs
    // against unsliced non-nullable children, or propagate STRUCT offsets twice.
    let children = match &data_type {
        DataType::Struct(_) | DataType::Union(_, UnionMode::Sparse) => {
            if matches!(data_type, DataType::Union(_, UnionMode::Sparse)) {
                // UnionArray::from(ArrayData) applies the parent offset to type
                // IDs but not sparse children. Canonicalize both before that.
                buffers[0] = buffers[0].slice_with_length(offset, len);
            }
            let children = children
                .into_iter()
                .map(|c| normalize_imported_slice(c, offset, len))
                .collect::<Result<Vec<_>, _>>()?;
            offset = 0;
            children
        }
        DataType::FixedSizeList(_, width) => {
            let width = *width as usize;
            let children = children
                .into_iter()
                .map(|c| normalize_imported_slice(c, offset * width, len * width))
                .collect::<Result<Vec<_>, _>>()?;
            offset = 0;
            children
        }
        _ => children
            .into_iter()
            .map(normalize_imported_data)
            .collect::<Result<Vec<_>, _>>()?,
    };
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
    use arrow::buffer::{NullBuffer, ScalarBuffer};
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
        let raw = input().into_builder().offset(1).len(2).build().unwrap();
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

    #[test]
    fn c_data_slices_normalize_ordinary_nested_union_children_and_siblings() {
        let leaf = Int32Array::from(vec![0, 1, 2, 3]).to_data();
        let inner = ArrayData::builder(DataType::Struct(
            vec![Field::new("b", DataType::Int32, true)].into(),
        ))
        .len(4)
        .child_data(vec![leaf])
        .build()
        .unwrap();
        let ordinary = ArrayData::builder(DataType::Struct(
            vec![Field::new("a", inner.data_type().clone(), true)].into(),
        ))
        .len(4)
        .child_data(vec![inner])
        .build()
        .unwrap();
        let sibling = ArrayData::builder(DataType::Struct(
            vec![
                Field::new("u", input().data_type().clone(), true),
                Field::new("ordinary", ordinary.data_type().clone(), true),
            ]
            .into(),
        ))
        .len(2)
        .offset(1)
        .child_data(vec![input(), ordinary.clone()])
        .build()
        .unwrap();
        let sibling = StructArray::from(normalize_sparse_unions(sibling).unwrap());
        let expected = Int32Array::from(vec![1, 2]).to_data();
        let assert_ordinary = |data: ArrayData| {
            let outer = StructArray::from(data);
            let inner = outer
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert_eq!(inner.column(0).to_data(), expected);
        };
        assert_ordinary(sibling.column(1).to_data());
        let union = ArrayData::builder(DataType::Union(
            UnionFields::try_new(
                [7],
                [Field::new("ordinary", ordinary.data_type().clone(), true)],
            )
            .unwrap(),
            UnionMode::Sparse,
        ))
        .len(2)
        .offset(1)
        .buffers(vec![ScalarBuffer::from(vec![7_i8; 4]).into_inner()])
        .child_data(vec![ordinary])
        .build()
        .unwrap();
        let union = UnionArray::from(normalize_sparse_unions(union).unwrap());
        assert_ordinary(union.child(7).to_data());
    }

    #[test]
    fn c_data_nullability_is_validated_after_parent_and_children_are_sliced() {
        let numbers = Int32Array::from(vec![None, Some(1), Some(2), None]);
        let parents = NullBuffer::from(vec![false, true, true, false]);
        let rows = ArrayData::builder(DataType::Struct(
            vec![Field::new("n", DataType::Int32, false)].into(),
        ))
        .len(4)
        .nulls(Some(parents.clone()))
        .child_data(vec![numbers.to_data()])
        .build()
        .unwrap();
        let lists = ArrayData::builder(DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Int32, false)),
            1,
        ))
        .len(4)
        .nulls(Some(parents))
        .child_data(vec![numbers.to_data()])
        .build()
        .unwrap();
        for ordinary in [rows, lists] {
            // Both children are initially valid in their complete C Data
            // coordinates. The nested parent's NULL masks must move together.
            let input = ArrayData::builder(DataType::Struct(
                vec![
                    Field::new("u", input().data_type().clone(), true),
                    Field::new("ordinary", ordinary.data_type().clone(), true),
                ]
                .into(),
            ))
            .len(2)
            .offset(1)
            .child_data(vec![input(), ordinary])
            .build()
            .unwrap();
            let result = StructArray::from(normalize_sparse_unions(input).unwrap());
            let ordinary = result.column(1);
            assert_eq!(ordinary.null_count(), 0);
            let values = if let Some(row) = ordinary.as_any().downcast_ref::<StructArray>() {
                row.column(0)
            } else {
                ordinary
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap()
                    .values()
            };
            assert_eq!(values.to_data(), Int32Array::from(vec![1, 2]).to_data());
        }
    }
}
