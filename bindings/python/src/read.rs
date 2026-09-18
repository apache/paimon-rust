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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::pyarrow::ToPyArrow;
use futures::TryStreamExt;
use paimon::spec::Predicate;
use paimon::table::{ArrowRecordBatchStream, DataSplit, IncrementalScanMode, RowRange, Table};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use crate::error::to_py_err;
use crate::predicate::dict_to_predicate;

/// Time-travel selector option names, in the core's resolution priority order.
const TIME_TRAVEL_SELECTORS: [&str; 5] = [
    "scan.timestamp-millis",
    "scan.watermark",
    "scan.version",
    "scan.snapshot-id",
    "scan.tag-name",
];

/// Extract a Python dict of scan options into a String->String map, requiring
/// string keys and values (non-string → TypeError) so option semantics stay
/// unambiguous.
pub(crate) fn extract_options(options: &Bound<'_, PyDict>) -> PyResult<HashMap<String, String>> {
    let mut out = HashMap::with_capacity(options.len());
    for (k, v) in options.iter() {
        let key: String = k
            .extract()
            .map_err(|_| PyTypeError::new_err("scan option keys must be strings"))?;
        let val: String = v.extract().map_err(|_| {
            PyTypeError::new_err(format!("scan option '{key}' value must be a string"))
        })?;
        out.insert(key, val);
    }
    Ok(out)
}

/// Return the first configured time-travel selector as (name, value), if any.
fn find_time_travel_selector(opts: &HashMap<String, String>) -> Option<(&str, &str)> {
    TIME_TRAVEL_SELECTORS
        .iter()
        .find_map(|&name| opts.get(name).map(|v| (name, v.as_str())))
}

/// Apply common scan/read config onto a core ReadBuilder.
fn apply_read_config(
    builder: &mut paimon::table::ReadBuilder<'_>,
    projection: &Option<Vec<String>>,
    limit: Option<usize>,
    filter: &Option<Predicate>,
    case_sensitive: bool,
) -> PyResult<()> {
    builder.with_case_sensitive(case_sensitive);
    if let Some(projection) = projection {
        let cols: Vec<&str> = projection.iter().map(String::as_str).collect();
        builder.with_projection(&cols).map_err(to_py_err)?;
    }
    if let Some(limit) = limit {
        builder.with_limit(limit);
    }
    if let Some(filter) = filter {
        builder.with_filter(filter.clone());
    }
    Ok(())
}

/// Extract a sequence of Python `Split` objects into core `DataSplit`s. Accepts
/// any iterable (list/tuple/generator). Runs under the GIL since it touches
/// Python objects. A non-iterable argument or a non-`Split` element raises
/// `TypeError`.
fn extract_splits(splits: &Bound<'_, PyAny>) -> PyResult<Vec<DataSplit>> {
    let iter = splits
        .try_iter()
        .map_err(|_| PyTypeError::new_err("read() expects a sequence of Split objects"))?;
    let mut out = Vec::new();
    for item in iter {
        let item = item?;
        let split: PyRef<PySplit> = item
            .extract()
            .map_err(|_| PyTypeError::new_err("read() expects a sequence of Split objects"))?;
        out.push(split.inner.clone());
    }
    Ok(out)
}

#[pyclass(name = "ReadBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyReadBuilder {
    table: Arc<Table>,
    projection: Option<Vec<String>>,
    limit: Option<usize>,
    filter: Option<Predicate>,
    row_ranges: Option<Vec<RowRange>>,
    case_sensitive: bool,
}

impl PyReadBuilder {
    pub fn new(table: Arc<Table>) -> Self {
        Self {
            table,
            projection: None,
            limit: None,
            filter: None,
            row_ranges: None,
            case_sensitive: true,
        }
    }

    /// Build on a table copy resolved from scan options. Resolves time travel
    /// (may do IO) so `with_filter` later validates against the travelled
    /// schema. Raises if a selector is set but resolves to no snapshot, so a
    /// mistyped snapshot-id can never silently read latest.
    pub fn from_options(table: Arc<Table>, opts: HashMap<String, String>) -> PyResult<Self> {
        // Reject conflicting time-travel selectors here. The core swallows the
        // conflict error via its Java-parity silent fallback, so the strict
        // gate below would otherwise misattribute the failure to a single
        // selector. Surface the real conflict, listing the keys the user set.
        // scan.version must first be adapted by the core: Java allows it to
        // overwrite a selector of the same kind after resolving tag precedence.
        let present: Vec<&str> = TIME_TRAVEL_SELECTORS
            .iter()
            .copied()
            .filter(|name| opts.contains_key(*name))
            .collect();
        if present.len() > 1 && !opts.contains_key("scan.version") {
            return Err(PyValueError::new_err(format!(
                "Only one time-travel selector may be set, found: {}",
                present.join(", ")
            )));
        }
        let selector =
            find_time_travel_selector(&opts).map(|(n, v)| (n.to_string(), v.to_string()));
        let rt = runtime();
        let traveled = rt
            .block_on(async { table.copy_with_time_travel(opts).await })
            .map_err(to_py_err)?;
        if let Some((name, value)) = selector {
            if !traveled.has_resolved_travel_snapshot() {
                return Err(PyValueError::new_err(format!(
                    "time-travel selector {name}={value} did not resolve to any snapshot"
                )));
            }
        }
        Ok(Self {
            table: Arc::new(traveled),
            projection: None,
            limit: None,
            filter: None,
            row_ranges: None,
            case_sensitive: true,
        })
    }
}

#[pymethods]
impl PyReadBuilder {
    fn with_projection(mut slf: PyRefMut<'_, Self>, columns: Vec<String>) -> PyRefMut<'_, Self> {
        slf.projection = Some(columns);
        slf
    }

    fn with_limit(mut slf: PyRefMut<'_, Self>, limit: usize) -> PyRefMut<'_, Self> {
        slf.limit = Some(limit);
        slf
    }

    /// Set whether column-name matching (projection and predicate resolution) is
    /// case-sensitive. Defaults to `true` (exact match); pass `false` to match
    /// column names case-insensitively (ASCII case-folding).
    fn with_case_sensitive(
        mut slf: PyRefMut<'_, Self>,
        case_sensitive: bool,
    ) -> PyRefMut<'_, Self> {
        slf.case_sensitive = case_sensitive;
        slf
    }

    /// Convert a lightweight dict predicate into a Rust [`Predicate`] and store
    /// it for pushdown. Conversion happens immediately, so conversion errors
    /// (unknown field, type mismatch, unsupported operator/type) surface at call
    /// time. Repeated calls overwrite the previously stored filter.
    fn with_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        predicate: &Bound<'_, PyDict>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let filter = dict_to_predicate(predicate, slf.table.schema().fields(), slf.case_sensitive)?;
        slf.filter = Some(filter);
        Ok(slf)
    }

    /// Set inclusive row ID ranges for Data Evolution scan planning.
    /// Planned splits carry the ranges used by readers.
    fn with_row_ranges(
        mut slf: PyRefMut<'_, Self>,
        ranges: Vec<(i64, i64)>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let mut row_ranges = Vec::with_capacity(ranges.len());
        for (from, to) in ranges {
            if from > to {
                return Err(PyValueError::new_err(format!(
                    "row range start {from} exceeds end {to}"
                )));
            }
            row_ranges.push(RowRange::new(from, to));
        }
        slf.row_ranges = Some(row_ranges);
        Ok(slf)
    }

    fn new_scan(&self) -> PyTableScan {
        PyTableScan {
            table: Arc::clone(&self.table),
            projection: self.projection.clone(),
            limit: self.limit,
            filter: self.filter.clone(),
            row_ranges: self.row_ranges.clone(),
            case_sensitive: self.case_sensitive,
            incremental_range: None,
            row_position_slice: None,
            row_position_shard: None,
        }
    }

    /// Plan APPEND deltas in (start_snapshot_id, end_snapshot_id] as one batch.
    /// Primary-key versions are grouped across all selected snapshots.
    fn new_incremental_scan(&self, start_snapshot_id: i64, end_snapshot_id: i64) -> PyTableScan {
        let mut scan = self.new_scan();
        scan.incremental_range = Some((start_snapshot_id, end_snapshot_id));
        scan
    }

    fn new_read(&self) -> PyTableRead {
        PyTableRead {
            table: Arc::clone(&self.table),
            projection: self.projection.clone(),
            limit: self.limit,
            filter: self.filter.clone(),
            case_sensitive: self.case_sensitive,
        }
    }
}

#[pyclass(name = "TableScan", module = "pypaimon_rust.datafusion")]
pub struct PyTableScan {
    table: Arc<Table>,
    projection: Option<Vec<String>>,
    limit: Option<usize>,
    filter: Option<Predicate>,
    row_ranges: Option<Vec<RowRange>>,
    case_sensitive: bool,
    incremental_range: Option<(i64, i64)>,
    row_position_slice: Option<(u64, u64)>,
    row_position_shard: Option<(u64, u64)>,
}

impl PyTableScan {
    fn core_scan(&self) -> PyResult<paimon::table::TableScan<'_>> {
        let mut scan = self.read_builder()?.new_scan();
        if let Some((start, end)) = self.row_position_slice {
            scan = scan
                .with_row_position_slice(start, end)
                .map_err(to_py_err)?;
        }
        if let Some((index, count)) = self.row_position_shard {
            scan = scan
                .with_row_position_shard(index, count)
                .map_err(to_py_err)?;
        }
        Ok(scan)
    }

    fn core_incremental_scan(
        &self,
        start: i64,
        end: i64,
    ) -> PyResult<paimon::table::IncrementalScan<'_>> {
        let mut scan =
            self.read_builder()?
                .new_incremental_scan(IncrementalScanMode::Delta, start, end);
        if let Some((start, end)) = self.row_position_slice {
            scan = scan
                .with_row_position_slice(start, end)
                .map_err(to_py_err)?;
        }
        if let Some((index, count)) = self.row_position_shard {
            scan = scan
                .with_row_position_shard(index, count)
                .map_err(to_py_err)?;
        }
        Ok(scan)
    }

    fn read_builder(&self) -> PyResult<paimon::table::ReadBuilder<'_>> {
        let mut builder = self.table.new_read_builder();
        apply_read_config(
            &mut builder,
            &self.projection,
            self.limit,
            &self.filter,
            self.case_sensitive,
        )?;
        if let Some(row_ranges) = &self.row_ranges {
            builder.with_row_ranges(row_ranges.clone());
        }
        Ok(builder)
    }
}

#[pymethods]
impl PyTableScan {
    /// Select a half-open range of Data Evolution row positions.
    fn with_row_position_slice(
        mut slf: PyRefMut<'_, Self>,
        start: u64,
        end: u64,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.core_scan()?
            .with_row_position_slice(start, end)
            .map_err(to_py_err)?;
        slf.row_position_slice = Some((start, end));
        Ok(slf)
    }

    /// Select one Data Evolution row-position shard.
    fn with_row_position_shard(
        mut slf: PyRefMut<'_, Self>,
        index: u64,
        count: u64,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.core_scan()?
            .with_row_position_shard(index, count)
            .map_err(to_py_err)?;
        slf.row_position_shard = Some((index, count));
        Ok(slf)
    }

    fn plan(&self, py: Python<'_>) -> PyResult<PyPlan> {
        py.detach(|| {
            runtime().block_on(async {
                let plan = match self.incremental_range {
                    Some((start, end)) => {
                        self.core_incremental_scan(start, end)?
                            .plan_combined_delta()
                            .await
                    }
                    None => self.core_scan()?.plan().await,
                };
                plan.map(PyPlan::from).map_err(to_py_err)
            })
        })
    }
}

#[pyclass(name = "TableRead", module = "pypaimon_rust.datafusion")]
pub struct PyTableRead {
    table: Arc<Table>,
    projection: Option<Vec<String>>,
    limit: Option<usize>,
    filter: Option<Predicate>,
    case_sensitive: bool,
}

impl PyTableRead {
    fn read_stream(
        &self,
        py: Python<'_>,
        splits: &Bound<'_, PyAny>,
    ) -> PyResult<ArrowRecordBatchStream> {
        let splits = extract_splits(splits)?;
        py.detach(|| {
            let mut builder = self.table.new_read_builder();
            apply_read_config(
                &mut builder,
                &self.projection,
                self.limit,
                &self.filter,
                self.case_sensitive,
            )?;
            // Validate config (e.g. projection) before the empty-splits fast
            // path so an invalid projection fails consistently regardless of
            // how many splits are passed.
            let read = builder.new_read().map_err(to_py_err)?;
            read.to_arrow(&splits).map_err(to_py_err)
        })
    }
}

#[pymethods]
impl PyTableRead {
    /// Lazily read the given splits as an iterator of PyArrow RecordBatches.
    fn read_arrow(
        &self,
        py: Python<'_>,
        splits: &Bound<'_, PyAny>,
    ) -> PyResult<PyRecordBatchReader> {
        Ok(PyRecordBatchReader {
            stream: Mutex::new(self.read_stream(py, splits)?),
        })
    }

    /// Read the given splits into a list of PyArrow RecordBatches.
    fn read(&self, py: Python<'_>, splits: &Bound<'_, PyAny>) -> PyResult<Vec<Py<PyAny>>> {
        let stream = self.read_stream(py, splits)?;
        let rt = runtime();
        let batches = py.detach(|| {
            rt.block_on(stream.try_collect::<Vec<_>>())
                .map_err(to_py_err)
        })?;
        batches
            .iter()
            .map(|batch| Ok(batch.to_pyarrow(py)?.unbind()))
            .collect()
    }
}

#[pyclass(name = "RecordBatchReader", module = "pypaimon_rust.datafusion")]
pub struct PyRecordBatchReader {
    stream: Mutex<ArrowRecordBatchStream>,
}

impl PyRecordBatchReader {
    fn next_batch(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let batch = py.detach(|| {
            let mut stream = self
                .stream
                .lock()
                .map_err(|_| PyRuntimeError::new_err("native record batch reader lock poisoned"))?;
            runtime().block_on(stream.try_next()).map_err(to_py_err)
        })?;
        batch
            .map(|batch| Ok(batch.to_pyarrow(py)?.unbind()))
            .transpose()
    }
}

#[pymethods]
impl PyRecordBatchReader {
    fn read_next_batch(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.next_batch(py)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.next_batch(py)
    }
}

#[pyclass(name = "Plan", module = "pypaimon_rust.datafusion")]
pub struct PyPlan {
    splits: Vec<DataSplit>,
    snapshot_id: Option<i64>,
}

impl From<paimon::table::Plan> for PyPlan {
    fn from(plan: paimon::table::Plan) -> Self {
        Self {
            splits: plan.splits().to_vec(),
            snapshot_id: plan.snapshot_id(),
        }
    }
}

#[pymethods]
impl PyPlan {
    /// Snapshot selected by the scan, even when pruning produces no splits.
    /// `None` means no snapshot was selected, including snapshot-free format tables.
    fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    fn splits(&self) -> Vec<PySplit> {
        self.splits
            .iter()
            .cloned()
            .map(|inner| PySplit { inner })
            .collect()
    }

    fn __len__(&self) -> usize {
        self.splits.len()
    }
}

#[pyclass(name = "Split", module = "pypaimon_rust.datafusion")]
pub struct PySplit {
    pub(crate) inner: DataSplit,
}

impl PySplit {
    fn to_bytes(&self) -> PyResult<Vec<u8>> {
        serde_json::to_vec(&self.inner)
            .map_err(|e| PyValueError::new_err(format!("failed to serialize split: {e}")))
    }

    fn from_bytes(bytes: &[u8]) -> PyResult<DataSplit> {
        serde_json::from_slice(bytes)
            .map_err(|e| PyValueError::new_err(format!("failed to deserialize split: {e}")))
    }
}

#[pymethods]
impl PySplit {
    /// Physical row count: sum of data-file row counts (not a logical result count).
    fn row_count(&self) -> i64 {
        self.inner.row_count()
    }

    /// Whether the split must be read as physical change events.
    fn is_streaming(&self) -> bool {
        self.inner.is_streaming()
    }

    /// Serialize to Java SplitSerializer v1, using IndexedSplit for row ranges.
    /// Preserves the streaming flag for physical change-event reads.
    fn serialize<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.inner.serialize_split_v1().map_err(to_py_err)?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Reduce to `Split(bytes)` for pickle/copy. The bytes are an opaque,
    /// implementation-detail encoding; only same/compatible-version round-trip
    /// is guaranteed.
    fn __reduce__<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<(Py<PyAny>, (Py<PyBytes>,))> {
        let bytes = slf.borrow().to_bytes()?;
        let cls = slf.get_type().unbind().into_any();
        Ok((cls, (PyBytes::new(py, &bytes).unbind(),)))
    }

    /// Reconstruct a split from opaque bytes produced by pickling. Direct
    /// construction without those bytes is unsupported; obtain splits from
    /// `ReadBuilder.new_scan().plan()`.
    #[new]
    fn new(state: &Bound<'_, PyBytes>) -> PyResult<Self> {
        Ok(Self {
            inner: Self::from_bytes(state.as_bytes())?,
        })
    }
}
