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

//! Typed, transportable source metadata for search plans.

use std::collections::HashMap;

use paimon::spec::IndexFileMeta;
use paimon::table::{RowRange, SearchScanSplit};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PySequence, PyString};

use crate::error::to_py_err;
use crate::read::PySplit;

#[pyclass(name = "IndexFileMeta", module = "pypaimon_rust.datafusion", frozen)]
pub struct PyIndexFileMeta {
    pub(crate) inner: IndexFileMeta,
}

#[pymethods]
impl PyIndexFileMeta {
    fn index_type(&self) -> String {
        self.inner.index_type.clone()
    }

    fn file_name(&self) -> String {
        self.inner.file_name.clone()
    }

    fn file_size(&self) -> i64 {
        self.inner.file_size
    }

    fn row_count(&self) -> i64 {
        self.inner.row_count
    }

    fn _serialize<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.inner.to_serialized_row_data().map_err(to_py_err)?;
        Ok(PyBytes::new(py, &bytes))
    }

    #[staticmethod]
    fn _deserialize(state: &Bound<'_, PyBytes>) -> PyResult<Self> {
        Ok(Self {
            inner: IndexFileMeta::from_serialized_row_data(state.as_bytes()).map_err(to_py_err)?,
        })
    }

    fn __reduce__<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<(Py<PyAny>, (Py<PyBytes>,))> {
        let bytes = slf.borrow()._serialize(py)?.unbind();
        Ok((slf.get_type().getattr("_deserialize")?.unbind(), (bytes,)))
    }
}

fn range(from: i64, to: i64) -> PyResult<RowRange> {
    if from < 0 || from > to {
        return Err(PyValueError::new_err(
            "Search row ranges must be nonnegative and ordered",
        ));
    }
    Ok(RowRange::new(from, to))
}

fn ranges(values: Vec<(i64, i64)>) -> PyResult<Vec<RowRange>> {
    values
        .into_iter()
        .map(|(from, to)| range(from, to))
        .collect()
}

fn range_values(values: &[RowRange]) -> Vec<(i64, i64)> {
    values
        .iter()
        .map(|value| (value.from(), value.to()))
        .collect()
}

fn index_files(py: Python<'_>, values: Vec<Py<PyIndexFileMeta>>) -> Vec<IndexFileMeta> {
    values
        .into_iter()
        .map(|value| value.borrow(py).inner.clone())
        .collect()
}

fn file_values(values: &[IndexFileMeta]) -> Vec<PyIndexFileMeta> {
    values
        .iter()
        .cloned()
        .map(|inner| PyIndexFileMeta { inner })
        .collect()
}

fn position_ranges(
    values: HashMap<String, Vec<(i64, i64)>>,
) -> PyResult<HashMap<String, Vec<RowRange>>> {
    values
        .into_iter()
        .map(|(name, values)| Ok((name, ranges(values)?)))
        .collect()
}

fn position_values(values: &HashMap<String, Vec<RowRange>>) -> HashMap<String, Vec<(i64, i64)>> {
    values
        .iter()
        .map(|(name, values)| (name.clone(), range_values(values)))
        .collect()
}

fn decode_split(state: &Bound<'_, PyBytes>) -> PyResult<SearchScanSplit> {
    let split: SearchScanSplit = serde_json::from_slice(state.as_bytes())
        .map_err(|error| PyValueError::new_err(format!("Invalid search split state: {error}")))?;
    // Pickle is an internal encoding, but reconstruction still preserves the
    // public constructors' range invariants.
    let check = |value: &RowRange| range(value.from(), value.to()).map(|_| ());
    match &split {
        SearchScanSplit::Index { row_range, .. } => check(row_range)?,
        SearchScanSplit::Raw { row_ranges, .. } => {
            for value in row_ranges {
                check(value)?;
            }
        }
        SearchScanSplit::PrimaryKey {
            row_ranges_by_file, ..
        } => {
            for value in row_ranges_by_file.values().flatten() {
                check(value)?;
            }
        }
    }
    Ok(split)
}

// Pickle only uses private factories; the public constructors and getters
// remain typed domain APIs and carry no scheduling protocol.
macro_rules! split_methods {
    ($class:ident, $kind:pat, { $($methods:item)* }) => {
        #[pymethods]
        impl $class {
            $($methods)*

            fn _serialize<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
                let bytes = serde_json::to_vec(&self.inner)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?;
                Ok(PyBytes::new(py, &bytes))
            }

            #[staticmethod]
            fn _deserialize(state: &Bound<'_, PyBytes>) -> PyResult<Self> {
                let inner = decode_split(state)?;
                if !matches!(&inner, $kind) {
                    return Err(PyValueError::new_err("Search split state has the wrong source kind"));
                }
                Ok(Self { inner })
            }

            fn __reduce__<'py>(
                slf: &Bound<'py, Self>,
                py: Python<'py>,
            ) -> PyResult<(Py<PyAny>, (Py<PyBytes>,))> {
                let bytes = slf.borrow()._serialize(py)?.unbind();
                Ok((slf.get_type().getattr("_deserialize")?.unbind(), (bytes,)))
            }
        }
    };
}

#[pyclass(
    name = "IndexVectorSearchSplit",
    module = "pypaimon_rust.datafusion",
    frozen
)]
pub struct PyIndexVectorSearchSplit {
    pub(crate) inner: SearchScanSplit,
}

split_methods!(PyIndexVectorSearchSplit, SearchScanSplit::Index { .. }, {
    #[new]
    #[pyo3(signature = (row_range_start, row_range_end, vector_index_files, scalar_index_files=Vec::new()))]
    fn new(
        py: Python<'_>,
        row_range_start: i64,
        row_range_end: i64,
        vector_index_files: Vec<Py<PyIndexFileMeta>>,
        scalar_index_files: Vec<Py<PyIndexFileMeta>>,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: SearchScanSplit::Index {
                column: String::new(),
                row_range: range(row_range_start, row_range_end)?,
                index_files: index_files(py, vector_index_files),
                scalar_index_files: index_files(py, scalar_index_files),
            },
        })
    }
    fn row_range_start(&self) -> i64 {
        let SearchScanSplit::Index { row_range, .. } = &self.inner else {
            unreachable!()
        };
        row_range.from()
    }
    fn row_range_end(&self) -> i64 {
        let SearchScanSplit::Index { row_range, .. } = &self.inner else {
            unreachable!()
        };
        row_range.to()
    }
    fn vector_index_files(&self) -> Vec<PyIndexFileMeta> {
        let SearchScanSplit::Index { index_files, .. } = &self.inner else {
            unreachable!()
        };
        file_values(index_files)
    }
    fn scalar_index_files(&self) -> Vec<PyIndexFileMeta> {
        let SearchScanSplit::Index {
            scalar_index_files, ..
        } = &self.inner
        else {
            unreachable!()
        };
        file_values(scalar_index_files)
    }
});

#[pyclass(
    name = "RawVectorSearchSplit",
    module = "pypaimon_rust.datafusion",
    frozen
)]
pub struct PyRawVectorSearchSplit {
    pub(crate) inner: SearchScanSplit,
}

split_methods!(PyRawVectorSearchSplit, SearchScanSplit::Raw { .. }, {
    #[new]
    #[pyo3(signature = (row_ranges, scalar_index_files=Vec::new(), index_type=None))]
    fn new(
        py: Python<'_>,
        row_ranges: Vec<(i64, i64)>,
        scalar_index_files: Vec<Py<PyIndexFileMeta>>,
        index_type: Option<String>,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: SearchScanSplit::Raw {
                row_ranges: ranges(row_ranges)?,
                scalar_index_files: index_files(py, scalar_index_files),
                index_type,
            },
        })
    }
    fn row_ranges(&self) -> Vec<(i64, i64)> {
        let SearchScanSplit::Raw { row_ranges, .. } = &self.inner else {
            unreachable!()
        };
        range_values(row_ranges)
    }
    fn scalar_index_files(&self) -> Vec<PyIndexFileMeta> {
        let SearchScanSplit::Raw {
            scalar_index_files, ..
        } = &self.inner
        else {
            unreachable!()
        };
        file_values(scalar_index_files)
    }
    fn index_type(&self) -> Option<String> {
        let SearchScanSplit::Raw { index_type, .. } = &self.inner else {
            unreachable!()
        };
        index_type.clone()
    }
});

#[pyclass(
    name = "PrimaryKeyVectorSearchSplit",
    module = "pypaimon_rust.datafusion",
    frozen
)]
pub struct PyPrimaryKeyVectorSearchSplit {
    pub(crate) inner: SearchScanSplit,
}

split_methods!(
    PyPrimaryKeyVectorSearchSplit,
    SearchScanSplit::PrimaryKey { .. },
    {
        #[new]
        fn new(
            py: Python<'_>,
            data_split: &PySplit,
            payloads: Vec<Py<PyIndexFileMeta>>,
            uncovered_data_files: Vec<String>,
            row_ranges_by_file: HashMap<String, Vec<(i64, i64)>>,
        ) -> PyResult<Self> {
            Ok(Self {
                inner: SearchScanSplit::PrimaryKey {
                    data_split: data_split.inner.clone(),
                    payloads: index_files(py, payloads),
                    uncovered_data_files,
                    row_ranges_by_file: position_ranges(row_ranges_by_file)?,
                },
            })
        }
        fn data_split(&self) -> PySplit {
            let SearchScanSplit::PrimaryKey { data_split, .. } = &self.inner else {
                unreachable!()
            };
            PySplit {
                inner: data_split.clone(),
            }
        }
        fn payloads(&self) -> Vec<PyIndexFileMeta> {
            let SearchScanSplit::PrimaryKey { payloads, .. } = &self.inner else {
                unreachable!()
            };
            file_values(payloads)
        }
        fn uncovered_data_files(&self) -> Vec<String> {
            let SearchScanSplit::PrimaryKey {
                uncovered_data_files,
                ..
            } = &self.inner
            else {
                unreachable!()
            };
            uncovered_data_files.clone()
        }
        fn row_ranges_by_file(&self) -> HashMap<String, Vec<(i64, i64)>> {
            let SearchScanSplit::PrimaryKey {
                row_ranges_by_file, ..
            } = &self.inner
            else {
                unreachable!()
            };
            position_values(row_ranges_by_file)
        }
    }
);

#[pyclass(
    name = "IndexFullTextSearchSplit",
    module = "pypaimon_rust.datafusion",
    frozen
)]
pub struct PyIndexFullTextSearchSplit {
    pub(crate) inner: SearchScanSplit,
}

split_methods!(PyIndexFullTextSearchSplit, SearchScanSplit::Index { .. }, {
    #[new]
    #[pyo3(signature = (column_name, row_range_start, row_range_end, full_text_index_files, scalar_index_files=Vec::new()))]
    fn new(
        py: Python<'_>,
        column_name: String,
        row_range_start: i64,
        row_range_end: i64,
        full_text_index_files: Vec<Py<PyIndexFileMeta>>,
        scalar_index_files: Vec<Py<PyIndexFileMeta>>,
    ) -> PyResult<Self> {
        if column_name.is_empty() {
            return Err(PyValueError::new_err(
                "Full-text source column must not be empty",
            ));
        }
        Ok(Self {
            inner: SearchScanSplit::Index {
                column: column_name,
                row_range: range(row_range_start, row_range_end)?,
                index_files: index_files(py, full_text_index_files),
                scalar_index_files: index_files(py, scalar_index_files),
            },
        })
    }
    fn column_name(&self) -> String {
        let SearchScanSplit::Index { column, .. } = &self.inner else {
            unreachable!()
        };
        column.clone()
    }
    fn row_range_start(&self) -> i64 {
        let SearchScanSplit::Index { row_range, .. } = &self.inner else {
            unreachable!()
        };
        row_range.from()
    }
    fn row_range_end(&self) -> i64 {
        let SearchScanSplit::Index { row_range, .. } = &self.inner else {
            unreachable!()
        };
        row_range.to()
    }
    fn full_text_index_files(&self) -> Vec<PyIndexFileMeta> {
        let SearchScanSplit::Index { index_files, .. } = &self.inner else {
            unreachable!()
        };
        file_values(index_files)
    }
    fn scalar_index_files(&self) -> Vec<PyIndexFileMeta> {
        let SearchScanSplit::Index {
            scalar_index_files, ..
        } = &self.inner
        else {
            unreachable!()
        };
        file_values(scalar_index_files)
    }
});

#[pyclass(
    name = "RawFullTextSearchSplit",
    module = "pypaimon_rust.datafusion",
    frozen
)]
pub struct PyRawFullTextSearchSplit {
    pub(crate) inner: SearchScanSplit,
}

split_methods!(PyRawFullTextSearchSplit, SearchScanSplit::Raw { .. }, {
    #[new]
    fn new(row_ranges: Vec<(i64, i64)>) -> PyResult<Self> {
        Ok(Self {
            inner: SearchScanSplit::Raw {
                row_ranges: ranges(row_ranges)?,
                scalar_index_files: vec![],
                index_type: None,
            },
        })
    }
    fn row_ranges(&self) -> Vec<(i64, i64)> {
        let SearchScanSplit::Raw { row_ranges, .. } = &self.inner else {
            unreachable!()
        };
        range_values(row_ranges)
    }
});

#[pyclass(
    name = "PrimaryKeyFullTextSearchSplit",
    module = "pypaimon_rust.datafusion",
    frozen
)]
pub struct PyPrimaryKeyFullTextSearchSplit {
    pub(crate) inner: SearchScanSplit,
}

split_methods!(
    PyPrimaryKeyFullTextSearchSplit,
    SearchScanSplit::PrimaryKey { .. },
    {
        #[new]
        fn new(
            py: Python<'_>,
            data_split: &PySplit,
            payloads: Vec<Py<PyIndexFileMeta>>,
            uncovered_data_files: Vec<String>,
            row_ranges_by_file: HashMap<String, Vec<(i64, i64)>>,
        ) -> PyResult<Self> {
            Ok(Self {
                inner: SearchScanSplit::PrimaryKey {
                    data_split: data_split.inner.clone(),
                    payloads: index_files(py, payloads),
                    uncovered_data_files,
                    row_ranges_by_file: position_ranges(row_ranges_by_file)?,
                },
            })
        }
        fn data_split(&self) -> PySplit {
            let SearchScanSplit::PrimaryKey { data_split, .. } = &self.inner else {
                unreachable!()
            };
            PySplit {
                inner: data_split.clone(),
            }
        }
        fn payloads(&self) -> Vec<PyIndexFileMeta> {
            let SearchScanSplit::PrimaryKey { payloads, .. } = &self.inner else {
                unreachable!()
            };
            file_values(payloads)
        }
        fn uncovered_data_files(&self) -> Vec<String> {
            let SearchScanSplit::PrimaryKey {
                uncovered_data_files,
                ..
            } = &self.inner
            else {
                unreachable!()
            };
            uncovered_data_files.clone()
        }
        fn row_ranges_by_file(&self) -> HashMap<String, Vec<(i64, i64)>> {
            let SearchScanSplit::PrimaryKey {
                row_ranges_by_file, ..
            } = &self.inner
            else {
                unreachable!()
            };
            position_values(row_ranges_by_file)
        }
    }
);

fn split_sequence<'a, 'py>(values: &'a Bound<'py, PyAny>) -> PyResult<&'a Bound<'py, PySequence>> {
    if values.is_instance_of::<PyString>()
        || values.is_instance_of::<PyBytes>()
        || values.is_instance_of::<PyByteArray>()
    {
        return Err(PyTypeError::new_err(
            "Search plans require a sequence of typed search source splits",
        ));
    }
    values.cast::<PySequence>().map_err(|_| {
        PyTypeError::new_err("Search plans require a sequence of typed search source splits")
    })
}

pub(crate) fn vector_splits_from_python(
    values: &Bound<'_, PyAny>,
) -> PyResult<Vec<SearchScanSplit>> {
    split_sequence(values)?
        .try_iter()?
        .map(|value| {
            let value = value?;
            if let Ok(value) = value.extract::<PyRef<'_, PyIndexVectorSearchSplit>>() {
                return Ok(value.inner.clone());
            }
            if let Ok(value) = value.extract::<PyRef<'_, PyRawVectorSearchSplit>>() {
                return Ok(value.inner.clone());
            }
            if let Ok(value) = value.extract::<PyRef<'_, PyPrimaryKeyVectorSearchSplit>>() {
                return Ok(value.inner.clone());
            }
            Err(PyTypeError::new_err(
                "Vector plans require typed vector search source splits",
            ))
        })
        .collect()
}

pub(crate) fn full_text_splits_from_python(
    values: &Bound<'_, PyAny>,
) -> PyResult<Vec<SearchScanSplit>> {
    split_sequence(values)?
        .try_iter()?
        .map(|value| {
            let value = value?;
            if let Ok(value) = value.extract::<PyRef<'_, PyIndexFullTextSearchSplit>>() {
                return Ok(value.inner.clone());
            }
            if let Ok(value) = value.extract::<PyRef<'_, PyRawFullTextSearchSplit>>() {
                return Ok(value.inner.clone());
            }
            if let Ok(value) = value.extract::<PyRef<'_, PyPrimaryKeyFullTextSearchSplit>>() {
                return Ok(value.inner.clone());
            }
            Err(PyTypeError::new_err(
                "Full-text plans require typed full-text search source splits",
            ))
        })
        .collect()
}

pub(crate) fn vector_splits_to_python(
    py: Python<'_>,
    sources: &[SearchScanSplit],
) -> PyResult<Vec<Py<PyAny>>> {
    sources
        .iter()
        .map(|source| match source {
            SearchScanSplit::Index { .. } => Py::new(
                py,
                PyIndexVectorSearchSplit {
                    inner: source.clone(),
                },
            )
            .map(Py::into_any),
            SearchScanSplit::Raw { .. } => Py::new(
                py,
                PyRawVectorSearchSplit {
                    inner: source.clone(),
                },
            )
            .map(Py::into_any),
            SearchScanSplit::PrimaryKey { .. } => Py::new(
                py,
                PyPrimaryKeyVectorSearchSplit {
                    inner: source.clone(),
                },
            )
            .map(Py::into_any),
        })
        .collect()
}

pub(crate) fn full_text_splits_to_python(
    py: Python<'_>,
    sources: &[SearchScanSplit],
) -> PyResult<Vec<Py<PyAny>>> {
    sources
        .iter()
        .map(|source| match source {
            SearchScanSplit::Index { .. } => Py::new(
                py,
                PyIndexFullTextSearchSplit {
                    inner: source.clone(),
                },
            )
            .map(Py::into_any),
            SearchScanSplit::Raw { .. } => Py::new(
                py,
                PyRawFullTextSearchSplit {
                    inner: source.clone(),
                },
            )
            .map(Py::into_any),
            SearchScanSplit::PrimaryKey { .. } => Py::new(
                py,
                PyPrimaryKeyFullTextSearchSplit {
                    inner: source.clone(),
                },
            )
            .map(Py::into_any),
        })
        .collect()
}
