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

//! Incremental key matching for PyPaimon's append-table upsert. This retains
//! only keys present in the input and row IDs of matching target rows.

use std::collections::{HashMap, HashSet};

use arrow::array::{Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, SortField};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

const ROW_ID: &str = "_ROW_ID";

fn key_columns(batch: &RecordBatch, keys: &[String]) -> PyResult<Vec<arrow::array::ArrayRef>> {
    keys.iter()
        .map(|name| {
            batch
                .column_by_name(name)
                .cloned()
                .ok_or_else(|| PyValueError::new_err(format!("missing upsert key '{name}'")))
        })
        .collect()
}

fn supported_key_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
            | DataType::Date32
            | DataType::Date64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// Arrow row encoding gives composite keys deterministic null and binary
/// semantics without creating Python tuples or per-value Python objects.
#[pyclass(
    name = "UpsertKeyMatcher",
    module = "pypaimon_rust.datafusion",
    unsendable
)]
pub struct PyUpsertKeyMatcher {
    keys: Vec<String>,
    types: Vec<DataType>,
    converter: RowConverter,
    input: Vec<(usize, Vec<u8>)>,
    input_keys: HashSet<Vec<u8>>,
    target: HashMap<Vec<u8>, Vec<i64>>,
}

#[pymethods]
impl PyUpsertKeyMatcher {
    #[new]
    fn new(batch: &Bound<'_, PyAny>, keys: Vec<String>) -> PyResult<Self> {
        if keys.is_empty() {
            return Err(PyValueError::new_err("upsert keys must not be empty"));
        }
        let batch = RecordBatch::from_pyarrow_bound(batch)?;
        let columns = key_columns(&batch, &keys)?;
        let types: Vec<_> = columns
            .iter()
            .map(|column| column.data_type().clone())
            .collect();
        if let Some(unsupported) = types.iter().find(|ty| !supported_key_type(ty)) {
            return Err(PyValueError::new_err(format!(
                "unsupported upsert key type: {unsupported:?}"
            )));
        }
        let converter = RowConverter::new(types.iter().cloned().map(SortField::new).collect())
            .map_err(|err| PyValueError::new_err(format!("unsupported upsert key type: {err}")))?;
        let rows = converter
            .convert_columns(&columns)
            .map_err(|err| PyValueError::new_err(format!("cannot encode upsert keys: {err}")))?;
        let mut last = HashMap::new();
        for index in 0..batch.num_rows() {
            last.insert(rows.row(index).as_ref().to_vec(), index);
        }
        let mut input: Vec<_> = last.into_iter().map(|(key, index)| (index, key)).collect();
        input.sort_unstable_by_key(|(index, _)| *index);
        let input_keys = input.iter().map(|(_, key)| key.clone()).collect();
        Ok(Self {
            keys,
            types,
            converter,
            input,
            input_keys,
            target: HashMap::new(),
        })
    }

    fn deduplicated_indices(&self) -> Vec<usize> {
        self.input.iter().map(|(index, _)| *index).collect()
    }

    fn add_existing_batch(&mut self, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        let batch = RecordBatch::from_pyarrow_bound(batch)?;
        let columns = key_columns(&batch, &self.keys)?;
        for (column, expected) in columns.iter().zip(&self.types) {
            if column.data_type() != expected {
                return Err(PyValueError::new_err(format!(
                    "upsert key type differs between input and table: {:?} != {expected:?}",
                    column.data_type()
                )));
            }
        }
        let row_ids = batch
            .column_by_name(ROW_ID)
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| PyValueError::new_err("existing batch needs int64 _ROW_ID"))?;
        let rows = self
            .converter
            .convert_columns(&columns)
            .map_err(|err| PyValueError::new_err(format!("cannot encode existing keys: {err}")))?;
        for index in 0..batch.num_rows() {
            let row = rows.row(index);
            let key = row.as_ref();
            // An existing key that is absent from the source must not occupy
            // memory in the upsert matcher.
            if self.input_keys.contains(key) {
                if row_ids.is_null(index) {
                    return Err(PyValueError::new_err("existing _ROW_ID must not be null"));
                }
                self.target
                    .entry(key.to_vec())
                    .or_default()
                    .push(row_ids.value(index));
            }
        }
        Ok(())
    }

    /// Return source indices repeated once per matched target row, matching
    /// row IDs, and unmatched source indices. Source indices refer to the
    /// original (pre-deduplication) input batch.
    fn finish(&self) -> (Vec<usize>, Vec<i64>, Vec<usize>) {
        let mut matched = Vec::new();
        let mut row_ids = Vec::new();
        let mut new = Vec::new();
        for (source_index, key) in &self.input {
            if let Some(ids) = self.target.get(key) {
                for id in ids {
                    matched.push(*source_index);
                    row_ids.push(*id);
                }
            } else {
                new.push(*source_index);
            }
        }
        (matched, row_ids, new)
    }
}
