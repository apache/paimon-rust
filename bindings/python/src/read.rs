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

use crate::global_index_result::PyGlobalIndexResult;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use paimon::spec::{is_variant_extraction_row, DataField, DataType, Predicate, RowType};
use paimon::table::{
    ArrowRecordBatchStream, DataSplit, GlobalIndexResult, IncrementalScanMode, RowRange,
    StreamTableScan, Table,
};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::{PyBaseException, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3::{PyTraverseError, PyVisit};
use tokio::sync::Notify;

use crate::error::to_py_err;
use crate::predicate::dict_to_table_predicate;

struct PyReadProjection<'a> {
    columns: &'a Option<Vec<String>>,
    read_type: &'a Option<Vec<DataField>>,
}

/// Time-travel selector option names, in the core's resolution priority order.
const TIME_TRAVEL_SELECTORS: [&str; 6] = [
    "scan.timestamp-millis",
    "scan.timestamp",
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
    projection: PyReadProjection<'_>,
    limit: Option<usize>,
    filter: &Option<Predicate>,
    case_sensitive: bool,
) -> PyResult<()> {
    builder.with_case_sensitive(case_sensitive);
    if let Some(read_type) = projection.read_type {
        builder.with_read_type(read_type.clone());
    } else if let Some(columns) = projection.columns {
        let cols: Vec<&str> = columns.iter().map(String::as_str).collect();
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
pub(crate) fn extract_splits(splits: &Bound<'_, PyAny>) -> PyResult<Vec<DataSplit>> {
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
    read_type: Option<Vec<DataField>>,
    limit: Option<usize>,
    filter: Option<Predicate>,
    partition_filter: Option<Predicate>,
    row_ranges: Option<Vec<RowRange>>,
    case_sensitive: bool,
    blob_parallelism: Option<usize>,
    include_row_kind: bool,
}

impl PyReadBuilder {
    pub fn new(table: Arc<Table>) -> Self {
        Self {
            table,
            projection: None,
            read_type: None,
            limit: None,
            filter: None,
            partition_filter: None,
            row_ranges: None,
            case_sensitive: true,
            blob_parallelism: None,
            include_row_kind: false,
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
            read_type: None,
            limit: None,
            filter: None,
            partition_filter: None,
            row_ranges: None,
            case_sensitive: true,
            blob_parallelism: None,
            include_row_kind: false,
        })
    }
}

#[pymethods]
impl PyReadBuilder {
    /// Project top-level columns by name.
    fn with_projection(mut slf: PyRefMut<'_, Self>, columns: Vec<String>) -> PyRefMut<'_, Self> {
        slf.projection = Some(columns);
        slf.read_type = None;
        slf
    }

    /// Set the complete Paimon ROW read type, including nested field metadata.
    /// The JSON representation is the same as RowType in a Paimon schema.
    fn with_read_type<'py>(
        mut slf: PyRefMut<'py, Self>,
        read_type_json: &str,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let read_type: RowType = serde_json::from_str(read_type_json)
            .map_err(|e| PyValueError::new_err(format!("invalid Paimon read type: {e}")))?;
        for field in read_type.fields() {
            let source = slf
                .table
                .schema()
                .fields()
                .iter()
                .find(|source| source.id() == field.id());
            if let (Some(source), DataType::Row(row)) = (source, field.data_type()) {
                if matches!(source.data_type(), DataType::Variant(_))
                    && is_variant_extraction_row(row)
                    && row
                        .fields()
                        .iter()
                        .any(|child| !matches!(child.data_type(), DataType::Float(_)))
                {
                    return Err(PyValueError::new_err(
                        "Variant extraction target type must be float32",
                    ));
                }
            }
        }
        slf.read_type = Some(read_type.fields().to_vec());
        slf.projection = None;
        Ok(slf)
    }

    /// Limit the number of visible rows.
    fn with_limit(mut slf: PyRefMut<'_, Self>, limit: usize) -> PyRefMut<'_, Self> {
        slf.limit = Some(limit);
        slf
    }

    /// Include a leading `rowkind` string column. Streaming primary-key
    /// splits preserve their physical change kinds; snapshot rows are `+I`.
    fn with_include_row_kind(mut slf: PyRefMut<'_, Self>, include: bool) -> PyRefMut<'_, Self> {
        slf.include_row_kind = include;
        slf
    }

    /// Set the maximum number of concurrent BLOB range reads for this read.
    fn with_blob_parallelism(
        mut slf: PyRefMut<'_, Self>,
        blob_parallelism: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        if blob_parallelism == 0 {
            return Err(PyValueError::new_err(
                "blob_parallelism must be greater than zero",
            ));
        }
        slf.blob_parallelism = Some(blob_parallelism);
        Ok(slf)
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
        let filter = dict_to_table_predicate(predicate, slf.table.schema(), slf.case_sensitive)?;
        slf.filter = Some(filter);
        Ok(slf)
    }

    /// Set a scan-only partition selector using table fields. None clears it.
    #[pyo3(signature = (predicate))]
    fn with_partition_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        predicate: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let filter = predicate
            .map(|predicate| {
                dict_to_table_predicate(predicate, slf.table.schema(), slf.case_sensitive)
            })
            .transpose()?;
        // Validation and all partition/query-auth semantics belong to core.
        slf.table
            .new_read_builder()
            .with_partition_filter(filter.clone())
            .map_err(to_py_err)?;
        slf.partition_filter = filter;
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
            read_type: self.read_type.clone(),
            limit: self.limit,
            filter: self.filter.clone(),
            partition_filter: self.partition_filter.clone(),
            row_ranges: self.row_ranges.clone(),
            case_sensitive: self.case_sensitive,
            incremental_scan: None,
            global_index_result: None,
            row_position_slice: None,
            row_position_shard: None,
            chunk_shuffle: None,
            shard: None,
            file_filters: None,
        }
    }

    /// Create a stateful Java-compatible stream scan.
    pub(crate) fn new_stream_scan(&self) -> PyResult<PyStreamTableScan> {
        Ok(PyStreamTableScan {
            scan: self
                .new_scan()
                .read_builder()?
                .new_stream_scan()
                .map_err(to_py_err)?,
            bucket_filter: Arc::new(Mutex::new(PyBucketFilterState::default())),
        })
    }

    /// Plan physical changes in (start_snapshot_id, end_snapshot_id] as one
    /// ordinary split plan. Mode is `delta` by default; `changelog` reads
    /// changelog manifest files and `auto` follows the table's producer.
    #[pyo3(signature = (start_snapshot_id, end_snapshot_id, mode = "delta"))]
    fn new_incremental_scan(
        &self,
        start_snapshot_id: i64,
        end_snapshot_id: i64,
        mode: &str,
    ) -> PyResult<PyTableScan> {
        let mut scan = self.new_scan();
        scan.incremental_scan = Some(PyIncrementalScan {
            start_snapshot_id,
            end_snapshot_id,
            mode: parse_incremental_scan_mode(mode)?,
        });
        Ok(scan)
    }

    fn new_read(&self) -> PyTableRead {
        PyTableRead {
            table: Arc::clone(&self.table),
            projection: self.projection.clone(),
            read_type: self.read_type.clone(),
            limit: self.limit,
            filter: self.filter.clone(),
            case_sensitive: self.case_sensitive,
            blob_parallelism: self.blob_parallelism,
            include_row_kind: self.include_row_kind,
        }
    }
}

#[pyclass(name = "TableScan", module = "pypaimon_rust.datafusion")]
pub struct PyTableScan {
    table: Arc<Table>,
    projection: Option<Vec<String>>,
    read_type: Option<Vec<DataField>>,
    limit: Option<usize>,
    filter: Option<Predicate>,
    partition_filter: Option<Predicate>,
    row_ranges: Option<Vec<RowRange>>,
    case_sensitive: bool,
    incremental_scan: Option<PyIncrementalScan>,
    global_index_result: Option<GlobalIndexResult>,
    row_position_slice: Option<(u64, u64)>,
    row_position_shard: Option<(u64, u64)>,
    chunk_shuffle: Option<PyChunkShuffle>,
    shard: Option<(usize, usize)>,
    file_filters: Option<(bool, bool)>,
}

#[derive(Clone)]
struct PyChunkShuffle {
    seed: String,
    chunk_size: u64,
}

#[derive(Clone, Copy)]
struct PyIncrementalScan {
    start_snapshot_id: i64,
    end_snapshot_id: i64,
    mode: IncrementalScanMode,
}

fn parse_incremental_scan_mode(mode: &str) -> PyResult<IncrementalScanMode> {
    match mode.to_ascii_lowercase().as_str() {
        "delta" => Ok(IncrementalScanMode::Delta),
        "changelog" => Ok(IncrementalScanMode::Changelog),
        "auto" => Ok(IncrementalScanMode::Auto),
        "diff" => Err(PyValueError::new_err(
            "incremental mode 'diff' requires before/after split pairs and is not supported by TableScan.plan()",
        )),
        _ => Err(PyValueError::new_err(format!(
            "unsupported incremental scan mode '{mode}'; expected delta, changelog or auto"
        ))),
    }
}

impl PyTableScan {
    fn core_scan(&self) -> PyResult<paimon::table::TableScan<'_>> {
        let mut scan = self.read_builder()?.new_scan();
        if let Some(result) = &self.global_index_result {
            scan = scan
                .with_global_index_result(result.clone())
                .map_err(to_py_err)?;
        }
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
        if let Some(chunk_shuffle) = &self.chunk_shuffle {
            scan = scan
                .with_chunk_shuffle(&chunk_shuffle.seed, chunk_shuffle.chunk_size)
                .map_err(to_py_err)?;
        }
        if let Some((index, count)) = self.shard {
            scan = scan.with_shard(index, count).map_err(to_py_err)?;
        }
        if let Some((skip_level0, only_read_real_buckets)) = self.file_filters {
            scan = scan
                .with_file_filters(skip_level0, only_read_real_buckets)
                .map_err(to_py_err)?;
        }
        Ok(scan)
    }

    fn core_incremental_scan(
        &self,
        start: i64,
        end: i64,
        mode: IncrementalScanMode,
    ) -> PyResult<paimon::table::IncrementalScan<'_>> {
        let mut scan = self.read_builder()?.new_incremental_scan(mode, start, end);
        if let Some(result) = &self.global_index_result {
            scan = scan
                .with_global_index_result(result.clone())
                .map_err(to_py_err)?;
        }
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
        if let Some(chunk_shuffle) = &self.chunk_shuffle {
            scan = scan
                .with_chunk_shuffle(&chunk_shuffle.seed, chunk_shuffle.chunk_size)
                .map_err(to_py_err)?;
        }
        if let Some((index, count)) = self.shard {
            scan = scan.with_shard(index, count).map_err(to_py_err)?;
        }
        Ok(scan)
    }

    fn read_builder(&self) -> PyResult<paimon::table::ReadBuilder<'_>> {
        let mut builder = self.table.new_read_builder();
        apply_read_config(
            &mut builder,
            PyReadProjection {
                columns: &self.projection,
                read_type: &self.read_type,
            },
            self.limit,
            &self.filter,
            self.case_sensitive,
        )?;
        if let Some(filter) = &self.partition_filter {
            builder
                .with_partition_filter(Some(filter.clone()))
                .map_err(to_py_err)?;
        }
        if let Some(row_ranges) = &self.row_ranges {
            builder.with_row_ranges(row_ranges.clone());
        }
        Ok(builder)
    }
}

/// A thin wrapper: planning, cursor and consumer progress belong to core.
#[pyclass(name = "StreamTableScan", module = "pypaimon_rust.datafusion")]
pub struct PyStreamTableScan {
    scan: StreamTableScan,
    bucket_filter: Arc<Mutex<PyBucketFilterState>>,
}

// The wrapper and core closure share one Python reference per object, so GC
// can traverse every retained reference exactly once through the wrapper.
#[derive(Default)]
struct PyBucketFilterState {
    filter: Option<Py<PyAny>>,
    error: Option<Py<PyBaseException>>,
}

impl PyStreamTableScan {
    fn install_bucket_filter(&mut self) {
        if self.bucket_filter.lock().unwrap().filter.is_none() {
            self.scan.with_bucket_filter(|_| Ok(true));
            return;
        }
        let state = Arc::clone(&self.bucket_filter);
        self.scan.with_bucket_filter(move |bucket| {
            Python::attach(|py| {
                // Release the lock before invoking Python: callbacks may run
                // GC, which traverses this same state.
                let filter = state
                    .lock()
                    .unwrap()
                    .filter
                    .as_ref()
                    .map(|filter| filter.clone_ref(py));
                let Some(filter) = filter else {
                    return Ok(true);
                };
                filter
                    .call1(py, (bucket,))
                    .and_then(|result| result.is_truthy(py))
                    .map_err(|error| {
                        let error = error.into_value(py);
                        let previous_error = state.lock().unwrap().error.replace(error);
                        drop(previous_error);
                        paimon::Error::UnexpectedError {
                            message: "Python bucket filter failed".into(),
                            source: None,
                        }
                    })
            })
        });
    }
}

#[pymethods]
impl PyStreamTableScan {
    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        let state = self.bucket_filter.lock().unwrap();
        visit.call(&state.filter)?;
        visit.call(&state.error)
    }

    fn __clear__(&mut self) {
        let references = {
            let mut state = self.bucket_filter.lock().unwrap();
            (state.filter.take(), state.error.take())
        };
        // Drop Python references outside the lock: destructors may re-enter
        // Python. The core closure sees an empty filter and retains no cycle.
        drop(references);
    }

    /// Copy scan progress with independently owned Python callback references.
    fn __copy__(&self, py: Python<'_>) -> Self {
        let filter = self
            .bucket_filter
            .lock()
            .unwrap()
            .filter
            .as_ref()
            .map(|filter| filter.clone_ref(py));
        let mut copy = Self {
            scan: self.scan.clone(),
            bucket_filter: Arc::new(Mutex::new(PyBucketFilterState {
                filter,
                error: None,
            })),
        };
        // Each wrapper owns one Python reference and its core callback captures
        // that wrapper's state. Sharing the Arc would duplicate GC traversal.
        copy.install_bucket_filter();
        copy
    }

    #[pyo3(signature = (filter = None))]
    fn with_bucket_filter(
        mut slf: PyRefMut<'_, Self>,
        filter: Option<Py<PyAny>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        if filter
            .as_ref()
            .is_some_and(|filter| !filter.bind(slf.py()).is_callable())
        {
            return Err(PyTypeError::new_err("bucket filter must be callable"));
        }
        let previous_filter =
            std::mem::replace(&mut slf.bucket_filter.lock().unwrap().filter, filter);
        drop(previous_filter);
        slf.install_bucket_filter();
        Ok(slf)
    }

    fn with_consumer_id(
        mut slf: PyRefMut<'_, Self>,
        consumer_id: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.scan.with_consumer_id(consumer_id).map_err(to_py_err)?;
        Ok(slf)
    }

    fn with_shard(
        mut slf: PyRefMut<'_, Self>,
        index: usize,
        count: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.scan.with_shard(index, count).map_err(to_py_err)?;
        Ok(slf)
    }

    fn checkpoint(&self) -> Option<i64> {
        self.scan.checkpoint()
    }

    fn watermark(&self) -> Option<i64> {
        self.scan.watermark()
    }

    #[pyo3(signature = (next_snapshot_id = None))]
    fn restore(&mut self, next_snapshot_id: Option<i64>) -> PyResult<()> {
        self.scan.restore(next_snapshot_id).map_err(to_py_err)
    }

    fn notify_checkpoint_complete(
        &self,
        py: Python<'_>,
        next_snapshot: Option<i64>,
    ) -> PyResult<()> {
        py.detach(|| runtime().block_on(self.scan.notify_checkpoint_complete(next_snapshot)))
            .map_err(to_py_err)
    }

    fn plan(&mut self, py: Python<'_>) -> PyResult<Option<PyPlan>> {
        let plan = py.detach(|| runtime().block_on(self.scan.plan()));
        let error = self.bucket_filter.lock().unwrap().error.take();
        if let Some(error) = error {
            return Err(PyErr::from_value(error.into_bound(py).into_any()));
        }
        plan.map(|plan| plan.map(PyPlan::from)).map_err(to_py_err)
    }
}

#[pymethods]
impl PyTableScan {
    /// Configure independent file filters for plan() and plan_files().
    /// plan_all() includes L0 while retaining the configured bucket policy.
    fn with_file_filters(
        mut slf: PyRefMut<'_, Self>,
        skip_level0: bool,
        only_read_real_buckets: bool,
    ) -> PyResult<PyRefMut<'_, Self>> {
        if slf.incremental_scan.is_some() {
            return Err(PyValueError::new_err(
                "file filters use table snapshot or incremental-between-timestamp options",
            ));
        }
        slf.core_scan()?
            .with_file_filters(skip_level0, only_read_real_buckets)
            .map_err(to_py_err)?;
        slf.file_filters = Some((skip_level0, only_read_real_buckets));
        Ok(slf)
    }

    /// Supply an evaluated selection; validation and authorization stay in core.
    fn with_global_index_result<'py>(
        mut slf: PyRefMut<'py, Self>,
        result: PyRef<'py, PyGlobalIndexResult>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let result = result.inner.clone();
        if let Some(incremental) = slf.incremental_scan {
            slf.core_incremental_scan(
                incremental.start_snapshot_id,
                incremental.end_snapshot_id,
                incremental.mode,
            )?
            .with_global_index_result(result.clone())
            .map_err(to_py_err)?;
        } else {
            slf.core_scan()?
                .with_global_index_result(result.clone())
                .map_err(to_py_err)?;
        }
        slf.global_index_result = Some(result);
        Ok(slf)
    }

    /// Select a half-open range of append-table row positions.
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

    /// Select one balanced append-table row-position shard.
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

    /// Deterministically shuffle fixed-live-row chunks. `seed` is a decimal
    /// Python integer string so arbitrarily large seeds retain Python's
    /// `random.Random` semantics.
    fn with_chunk_shuffle(
        mut slf: PyRefMut<'_, Self>,
        seed: String,
        chunk_size: u64,
    ) -> PyResult<PyRefMut<'_, Self>> {
        // Validate every combination immediately, not only when plan() runs.
        slf.core_scan()?
            .with_chunk_shuffle(&seed, chunk_size)
            .map_err(to_py_err)?;
        slf.chunk_shuffle = Some(PyChunkShuffle { seed, chunk_size });
        Ok(slf)
    }

    /// Select a Java file-name or bucket shard, or balanced shuffled chunks.
    fn with_shard(
        mut slf: PyRefMut<'_, Self>,
        index: usize,
        count: usize,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.core_scan()?
            .with_shard(index, count)
            .map_err(to_py_err)?;
        slf.shard = Some((index, count));
        Ok(slf)
    }

    /// Return a split Plan including L0 and all matching data-evolution column files.
    /// Scan filters and limits remain in effect; requires unrestricted query authorization.
    fn plan_all(&self, py: Python<'_>) -> PyResult<PyPlan> {
        py.detach(|| {
            runtime().block_on(async {
                self.core_scan()?
                    .plan_all()
                    .await
                    .map(PyPlan::from)
                    .map_err(to_py_err)
            })
        })
    }

    /// Return filtered file metadata before split packing or row selection.
    fn plan_files(&self, py: Python<'_>) -> PyResult<PyFileScanPlan> {
        if self.incremental_scan.is_some() {
            return Err(PyValueError::new_err(
                "plan_files uses table snapshot or incremental-between-timestamp options",
            ));
        }
        py.detach(|| {
            runtime().block_on(async {
                let mut scan = self.read_builder()?.new_scan();
                if let Some((skip_level0, only_read_real_buckets)) = self.file_filters {
                    scan = scan
                        .with_file_filters(skip_level0, only_read_real_buckets)
                        .map_err(to_py_err)?;
                }
                scan.plan_files()
                    .await
                    .map(|inner| PyFileScanPlan { inner })
                    .map_err(to_py_err)
            })
        })
    }

    fn plan(&self, py: Python<'_>) -> PyResult<PyPlan> {
        py.detach(|| {
            runtime().block_on(async {
                let plan = match self.incremental_scan {
                    Some(incremental) => {
                        self.core_incremental_scan(
                            incremental.start_snapshot_id,
                            incremental.end_snapshot_id,
                            incremental.mode,
                        )?
                        .plan_combined()
                        .await
                    }
                    None => self.core_scan()?.plan().await,
                };
                plan.map(PyPlan::from).map_err(to_py_err)
            })
        })
    }
}

#[pyclass(name = "ScanStats", module = "pypaimon_rust.datafusion", frozen)]
pub struct PyScanStats {
    inner: paimon::table::ScanStats,
}

#[pymethods]
impl PyScanStats {
    #[getter]
    fn base_manifest_files(&self) -> usize {
        self.inner.base_manifest_files
    }
    #[getter]
    fn delta_manifest_files(&self) -> usize {
        self.inner.delta_manifest_files
    }
    #[getter]
    fn manifest_files_before_partition_pruning(&self) -> usize {
        self.inner.manifest_files_before_partition_pruning
    }
    #[getter]
    fn manifest_files_after_partition_pruning(&self) -> usize {
        self.inner.manifest_files_after_partition_pruning
    }
    #[getter]
    fn manifest_files_pruned_by_row_ranges(&self) -> usize {
        self.inner.manifest_files_pruned_by_row_ranges
    }
    #[getter]
    fn manifest_entries_read(&self) -> usize {
        self.inner.manifest_entries_read
    }
    #[getter]
    fn manifest_entries_pruned_by_bucket(&self) -> usize {
        self.inner.manifest_entries_pruned_by_bucket
    }
    #[getter]
    fn manifest_entries_pruned_by_partition(&self) -> usize {
        self.inner.manifest_entries_pruned_by_partition
    }
    #[getter]
    fn manifest_entries_after_entry_pruning(&self) -> usize {
        self.inner.manifest_entries_after_entry_pruning
    }
    #[getter]
    fn manifest_entries_pruned_by_level(&self) -> usize {
        self.inner.manifest_entries_pruned_by_level
    }
    #[getter]
    fn manifest_entries_pruned_by_row_ranges(&self) -> usize {
        self.inner.manifest_entries_pruned_by_row_ranges
    }
    #[getter]
    fn manifest_entries_pruned_by_data_stats(&self) -> usize {
        self.inner.manifest_entries_pruned_by_data_stats
    }
    #[getter]
    fn manifest_entries_after_manifest_filters(&self) -> usize {
        self.inner.manifest_entries_after_manifest_filters
    }
    #[getter]
    fn manifest_entries_after_merge(&self) -> usize {
        self.inner.manifest_entries_after_merge
    }
    #[getter]
    fn manifest_entries_pruned_by_cross_schema_stats(&self) -> usize {
        self.inner.manifest_entries_pruned_by_cross_schema_stats
    }
    #[getter]
    fn manifest_entries_after_cross_schema_stats(&self) -> usize {
        self.inner.manifest_entries_after_cross_schema_stats
    }
    #[getter]
    fn data_evolution_groups_before_stats(&self) -> usize {
        self.inner.data_evolution_groups_before_stats
    }
    #[getter]
    fn data_evolution_groups_pruned_by_stats(&self) -> usize {
        self.inner.data_evolution_groups_pruned_by_stats
    }
    #[getter]
    fn data_evolution_groups_pruned_by_row_ranges(&self) -> usize {
        self.inner.data_evolution_groups_pruned_by_row_ranges
    }
    #[getter]
    fn split_candidates_built(&self) -> usize {
        self.inner.split_candidates_built
    }
    #[getter]
    fn limit_early_stopped(&self) -> bool {
        self.inner.limit_early_stopped
    }
    #[getter]
    fn splits_before_limit(&self) -> usize {
        self.inner.splits_before_limit
    }
    #[getter]
    fn splits_after_limit(&self) -> usize {
        self.inner.splits_after_limit
    }
    #[getter]
    fn final_splits(&self) -> usize {
        self.inner.final_splits
    }
    #[getter]
    fn final_files(&self) -> usize {
        self.inner.final_files
    }
    #[getter]
    fn planned_data_file_bytes(&self) -> u64 {
        self.inner.planned_data_file_bytes
    }
    #[getter]
    fn limit(&self) -> Option<usize> {
        self.inner.limit
    }
}

#[pyclass(name = "TableRead", module = "pypaimon_rust.datafusion")]
pub struct PyTableRead {
    table: Arc<Table>,
    projection: Option<Vec<String>>,
    read_type: Option<Vec<DataField>>,
    limit: Option<usize>,
    filter: Option<Predicate>,
    case_sensitive: bool,
    blob_parallelism: Option<usize>,
    include_row_kind: bool,
}

impl PyTableRead {
    fn read_stream(
        &self,
        py: Python<'_>,
        splits: &Bound<'_, PyAny>,
        parallelism: usize,
        ordered: bool,
    ) -> PyResult<ArrowRecordBatchStream> {
        let splits = extract_splits(splits)?;
        py.detach(|| {
            let mut builder = self.table.new_read_builder();
            apply_read_config(
                &mut builder,
                PyReadProjection {
                    columns: &self.projection,
                    read_type: &self.read_type,
                },
                self.limit,
                &self.filter,
                self.case_sensitive,
            )?;
            if let Some(blob_parallelism) = self.blob_parallelism {
                builder
                    .with_blob_parallelism(blob_parallelism)
                    .map_err(to_py_err)?;
            }
            // Validate config (e.g. projection) before the empty-splits fast
            // path so an invalid projection fails consistently regardless of
            // how many splits are passed.
            let read = builder
                .new_read()
                .map_err(to_py_err)?
                .with_parallelism(parallelism)
                .map_err(to_py_err)?
                .with_ordered(ordered);
            let splits = runtime()
                .block_on(read.authorize_splits(&splits))
                .map_err(to_py_err)?;
            if self.include_row_kind {
                read.to_arrow_with_row_kind(&splits).map_err(to_py_err)
            } else {
                read.to_arrow(&splits).map_err(to_py_err)
            }
        })
    }
}

#[pymethods]
impl PyTableRead {
    /// Lazily read the given splits as PyArrow RecordBatches.
    /// Core owns split scheduling, the global row limit and cancellation.
    /// ordered=True retains the supplied split order; the default emits ready groups first.
    #[pyo3(signature = (splits, *, parallelism = 1, ordered = false))]
    fn read_arrow(
        &self,
        py: Python<'_>,
        splits: &Bound<'_, PyAny>,
        parallelism: usize,
        ordered: bool,
    ) -> PyResult<PyRecordBatchReader> {
        Ok(PyRecordBatchReader::from_stream(self.read_stream(
            py,
            splits,
            parallelism,
            ordered,
        )?))
    }

    /// Read the given splits into a list of PyArrow RecordBatches.
    #[pyo3(signature = (splits, *, parallelism = 1, ordered = false))]
    fn read(
        &self,
        py: Python<'_>,
        splits: &Bound<'_, PyAny>,
        parallelism: usize,
        ordered: bool,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let stream = self.read_stream(py, splits, parallelism, ordered)?;
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
    schema: Option<SchemaRef>,
    stream: Mutex<Option<BoxStream<'static, PyResult<RecordBatch>>>>,
    closed: AtomicBool,
    close_notify: Notify,
}

impl PyRecordBatchReader {
    pub(crate) fn from_stream(stream: ArrowRecordBatchStream) -> Self {
        Self::from_py_stream(None, Box::pin(stream.map_err(to_py_err)))
    }

    pub(crate) fn from_py_stream(
        schema: Option<SchemaRef>,
        stream: BoxStream<'static, PyResult<RecordBatch>>,
    ) -> Self {
        Self {
            schema,
            stream: Mutex::new(Some(stream)),
            closed: AtomicBool::new(false),
            close_notify: Notify::new(),
        }
    }

    fn next_record_batch(&self) -> PyResult<Option<RecordBatch>> {
        let mut stream = self
            .stream
            .lock()
            .map_err(|_| PyRuntimeError::new_err("native record batch reader lock poisoned"))?;
        if self.closed.load(Ordering::Acquire) {
            stream.take();
            return Ok(None);
        }
        let result = match stream.as_mut() {
            Some(stream) => runtime().block_on(async {
                tokio::select! {
                    biased;
                    _ = self.close_notify.notified() => Ok(None),
                    batch = stream.try_next() => batch,
                }
            }),
            None => Ok(None),
        };
        if self.closed.load(Ordering::Acquire) || !matches!(result, Ok(Some(_))) {
            stream.take();
        }
        result
    }

    fn next_batch(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let batch = py.detach(|| self.next_record_batch())?;
        batch
            .map(|batch| Ok(batch.to_pyarrow(py)?.unbind()))
            .transpose()
    }

    fn close_reader(&self) {
        self.closed.store(true, Ordering::Release);
        // notify_one retains a permit if the read has not started polling.
        self.close_notify.notify_one();
        if let Ok(mut stream) = self.stream.try_lock() {
            stream.take();
        }
    }
}

#[pymethods]
impl PyRecordBatchReader {
    /// Result schema, known before the first batch for SQL streams.
    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.schema
            .as_ref()
            .map(|schema| Ok(schema.to_pyarrow(py)?.unbind()))
            .transpose()
    }

    fn read_next_batch(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.next_batch(py)
    }

    /// Stop an in-flight read and release the underlying stream. Idempotent.
    fn close(&self) {
        self.close_reader();
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
    pub(crate) inner: paimon::table::Plan,
}

#[pyclass(name = "FileScanPlan", module = "pypaimon_rust.datafusion", frozen)]
pub struct PyFileScanPlan {
    inner: paimon::table::FileScanPlan,
}

#[pymethods]
impl PyFileScanPlan {
    fn snapshot_id(&self) -> Option<i64> {
        self.inner.snapshot_id()
    }

    fn files(&self) -> Vec<crate::commit_callback::PyManifestEntry> {
        self.inner.files().iter().cloned().map(Into::into).collect()
    }
}

impl From<paimon::table::Plan> for PyPlan {
    fn from(plan: paimon::table::Plan) -> Self {
        Self { inner: plan }
    }
}

#[pymethods]
impl PyPlan {
    /// Restore captured planning metadata using typed splits. Execution
    /// authorizes the transported plan independently.
    #[new]
    #[pyo3(signature = (snapshot_id, splits))]
    fn new(snapshot_id: Option<i64>, splits: &Bound<'_, PyAny>) -> PyResult<Self> {
        let splits = splits
            .try_iter()?
            .map(|split| Ok(split?.extract::<PyRef<'_, PySplit>>()?.inner.clone()))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self {
            inner: paimon::table::Plan::from_splits(snapshot_id, splits),
        })
    }

    /// Snapshot selected by the scan, even when pruning produces no splits.
    /// `None` means no snapshot was selected, including snapshot-free format tables.
    fn snapshot_id(&self) -> Option<i64> {
        self.inner.snapshot_id()
    }

    fn stats(&self) -> PyScanStats {
        PyScanStats {
            inner: self.inner.stats().clone(),
        }
    }

    fn splits(&self) -> Vec<PySplit> {
        self.inner
            .splits()
            .iter()
            .cloned()
            .map(|inner| PySplit { inner })
            .collect()
    }

    fn __len__(&self) -> usize {
        self.inner.splits().len()
    }
}

/// Row-tracking routing metadata; constructing it performs no I/O.
#[pyclass(name = "RowIdPlan", module = "pypaimon_rust.datafusion")]
pub struct PyRowIdPlan {
    inner: paimon::table::RowIdPlan,
}

#[pymethods]
impl PyRowIdPlan {
    #[new]
    fn new(plan: &PyPlan) -> PyResult<Self> {
        Ok(Self {
            inner: paimon::table::RowIdPlan::from_plan(plan.inner.clone()).map_err(to_py_err)?,
        })
    }

    fn snapshot_id(&self) -> Option<i64> {
        self.inner.snapshot_id()
    }

    fn groups(&self) -> Vec<(i64, i64, PySplit)> {
        self.inner
            .groups()
            .iter()
            .map(|(first, last, split)| {
                (
                    *first,
                    *last,
                    PySplit {
                        inner: split.clone(),
                    },
                )
            })
            .collect()
    }

    fn plan(&self) -> PyPlan {
        self.inner.plan().clone().into()
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
    /// Selected row count for IndexedSplit-compatible row ranges, otherwise
    /// the sum of physical data-file row counts.
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

    /// Reconstruct a native split from the stable, cross-language
    /// `SplitSerializer` v1 wire format.
    ///
    /// This is deliberately separate from the constructor used by pickle:
    /// pickle bytes are an opaque Rust JSON encoding, while this method accepts
    /// Java/Python-compatible DataSplit and IndexedSplit frames, including scores.
    #[staticmethod]
    fn deserialize(state: &Bound<'_, PyBytes>) -> PyResult<Self> {
        Ok(Self {
            inner: DataSplit::deserialize_split_v1(state.as_bytes()).map_err(to_py_err)?,
        })
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

#[cfg(test)]
mod tests {
    use std::sync::{mpsc, Arc};
    use std::task::Poll;
    use std::time::Duration;

    use paimon::catalog::Identifier;
    use paimon::io::FileIOBuilder;
    use paimon::spec::{IntType, Schema, TableSchema};
    use pyo3::types::PyList;

    use super::*;

    #[test]
    fn file_planning_binding_keeps_complete_entries_and_independent_query_filters() {
        Python::attach(|py| {
            let schema = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("bucket", "1")
                .option("merge-engine", "first-row")
                .build()
                .unwrap();
            let table = Arc::new(Table::new(
                FileIOBuilder::new("memory").build().unwrap(),
                Identifier::new("default", "file_binding"),
                "memory:/file-binding".into(),
                TableSchema::new(0, &schema),
                None,
            ));
            py.detach(|| {
                runtime().block_on(async {
                    table
                        .file_io()
                        .mkdirs("memory:/file-binding/schema")
                        .await
                        .unwrap();
                    table
                        .file_io()
                        .mkdirs("memory:/file-binding/snapshot")
                        .await
                        .unwrap();
                    table
                        .file_io()
                        .new_output("memory:/file-binding/schema/schema-0")
                        .unwrap()
                        .write(serde_json::to_vec(table.schema()).unwrap().into())
                        .await
                        .unwrap();
                    for id in [1, 1] {
                        let builder = table.new_write_builder();
                        let mut writer = builder.new_write().unwrap();
                        let batch = RecordBatch::try_from_iter(vec![(
                            "id",
                            Arc::new(arrow::array::Int32Array::from(vec![id]))
                                as arrow::array::ArrayRef,
                        )])
                        .unwrap();
                        writer.write_arrow_batch(&batch).await.unwrap();
                        builder
                            .new_commit()
                            .commit(writer.prepare_commit().await.unwrap())
                            .await
                            .unwrap();
                        writer.close().await;
                    }
                })
            });
            let mut builder = PyReadBuilder::new(Arc::clone(&table));
            builder.limit = Some(0);
            let scan = Py::new(py, builder.new_scan()).unwrap();
            for name in ["skip_level0", "only_read_real_buckets"] {
                let kwargs = PyDict::new(py);
                kwargs.set_item(name, true).unwrap();
                let error = scan
                    .bind(py)
                    .call_method("plan_files", (), Some(&kwargs))
                    .unwrap_err();
                assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
            }
            assert_eq!(
                scan.bind(py)
                    .call_method0("plan_all")
                    .unwrap()
                    .len()
                    .unwrap(),
                0,
                "plan_all retains row LIMIT"
            );
            let files = scan.bind(py).call_method0("plan_files").unwrap();
            assert_eq!(
                files
                    .call_method0("snapshot_id")
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                2
            );
            let entries = files.call_method0("files").unwrap();
            assert_eq!(
                entries.len().unwrap(),
                2,
                "metadata planning ignores row LIMIT"
            );
            let first = entries.get_item(0).unwrap();
            assert_eq!(first.getattr("kind").unwrap().extract::<i32>().unwrap(), 0);
            assert_eq!(
                first
                    .getattr("total_buckets")
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                1
            );
            let file = first.getattr("file").unwrap();
            assert_eq!(
                file.getattr("row_count").unwrap().extract::<i64>().unwrap(),
                1
            );
            assert!(file
                .getattr("file_name")
                .unwrap()
                .extract::<String>()
                .unwrap()
                .ends_with(".parquet"));
            let encoded = first
                .call_method0("_to_commit_message")
                .unwrap()
                .call_method0("serialize")
                .unwrap();
            let encoded = encoded.cast::<PyBytes>().unwrap();
            let restored = paimon::table::CommitMessage::deserialize(
                paimon::table::COMMIT_MESSAGE_SERIALIZER_VERSION,
                encoded.as_bytes(),
            )
            .unwrap();
            let expected = py.detach(|| {
                runtime().block_on(async {
                    table
                        .new_read_builder()
                        .new_scan()
                        .plan_files()
                        .await
                        .unwrap()
                })
            });
            assert_eq!(restored.new_files, vec![expected.files()[0].file().clone()]);

            builder.limit = None;
            let scan = Py::new(py, builder.new_scan()).unwrap();
            assert_eq!(
                scan.bind(py).call_method0("plan").unwrap().len().unwrap(),
                0
            );
            scan.bind(py)
                .call_method1("with_file_filters", (false, true))
                .unwrap();
            assert_eq!(
                scan.bind(py).call_method0("plan").unwrap().len().unwrap(),
                1
            );
            for (skip_level0, expected_files, expected_splits) in
                [(false, 2, 1), (true, 0, 0), (false, 2, 1)]
            {
                scan.bind(py)
                    .call_method1("with_file_filters", (skip_level0, true))
                    .unwrap();
                assert_eq!(
                    scan.bind(py)
                        .call_method0("plan_files")
                        .unwrap()
                        .call_method0("files")
                        .unwrap()
                        .len()
                        .unwrap(),
                    expected_files
                );
                assert_eq!(
                    scan.bind(py).call_method0("plan").unwrap().len().unwrap(),
                    expected_splits
                );
                let all = scan.bind(py).call_method0("plan_all").unwrap();
                let all = all.extract::<PyRef<'_, PyPlan>>().unwrap();
                assert_eq!(
                    all.inner
                        .splits()
                        .iter()
                        .flat_map(|split| split.data_files())
                        .count(),
                    2,
                    "plan_all includes L0 even when explicit file filters exclude it"
                );
            }
            let fresh = Py::new(py, builder.new_scan()).unwrap();
            assert_eq!(
                fresh.bind(py).call_method0("plan").unwrap().len().unwrap(),
                0,
                "file filters must not change a fresh query scan's defaults"
            );
        });
    }

    #[test]
    fn stream_scan_copy_keeps_callback_ownership_and_progress_independent() {
        Python::attach(|py| {
            for bound_method in [false, true] {
                let schema = Schema::builder()
                    .column("id", DataType::Int(IntType::new()))
                    .build()
                    .unwrap();
                let table = Arc::new(Table::new(
                    FileIOBuilder::new("memory").build().unwrap(),
                    Identifier::new("default", "stream_copy"),
                    "memory:/stream-copy".into(),
                    TableSchema::new(0, &schema),
                    None,
                ));
                let scan =
                    Py::new(py, PyReadBuilder::new(table).new_stream_scan().unwrap()).unwrap();
                let globals = PyDict::new(py);
                globals.set_item("scan", scan).unwrap();
                globals.set_item("bound_method", bound_method).unwrap();
                py.run(
                    c"
import copy
import gc
import weakref

class Owner:
    def __init__(self):
        self.scan = scan.with_bucket_filter(self.select if bound_method else self)
        self.scan.restore(1)
        self.copied = copy.copy(self.scan)

    def select(self, bucket):
        return True

    def __call__(self, bucket):
        return self.select(bucket)

owner = Owner()
assert owner.scan is not owner.copied
owner.copied.restore(2)
assert owner.scan.checkpoint() == 1
assert owner.copied.checkpoint() == 2
owner.scan.restore(3)
assert owner.copied.checkpoint() == 2
reference = weakref.ref(owner)
del scan
gc.collect()
assert reference() is owner
del owner
gc.collect()
assert reference() is None
",
                    Some(&globals),
                    None,
                )
                .unwrap();
            }
        });
    }

    #[test]
    fn table_read_execution_keywords_validate_before_empty_splits() {
        Python::attach(|py| {
            let schema = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .build()
                .unwrap();
            let table = Arc::new(Table::new(
                FileIOBuilder::new("memory").build().unwrap(),
                Identifier::new("default", "read_execution_keywords"),
                "memory:/read-execution-keywords".into(),
                TableSchema::new(0, &schema),
                None,
            ));
            let read = Py::new(py, PyReadBuilder::new(table).new_read()).unwrap();
            for method in ["read", "read_arrow"] {
                for ordered in [false, true] {
                    let kwargs = PyDict::new(py);
                    kwargs.set_item("parallelism", 3).unwrap();
                    kwargs.set_item("ordered", ordered).unwrap();
                    let result = read
                        .bind(py)
                        .call_method(method, (PyList::empty(py),), Some(&kwargs))
                        .unwrap();
                    if method == "read" {
                        assert_eq!(result.len().unwrap(), 0);
                    } else {
                        let reader: PyRef<PyRecordBatchReader> = result.extract().unwrap();
                        assert!(reader.next_record_batch().unwrap().is_none());
                        reader.close_reader();
                        reader.close_reader();
                    }

                    kwargs.set_item("parallelism", 0).unwrap();
                    let error = read
                        .bind(py)
                        .call_method(method, (PyList::empty(py),), Some(&kwargs))
                        .unwrap_err();
                    assert!(error.is_instance_of::<PyValueError>(py));
                    assert!(error.to_string().contains("parallelism"));
                }

                let signature = py
                    .import("inspect")
                    .unwrap()
                    .getattr("signature")
                    .unwrap()
                    .call1((read.bind(py).getattr(method).unwrap(),))
                    .unwrap();
                let ordered = signature
                    .getattr("parameters")
                    .unwrap()
                    .get_item("ordered")
                    .unwrap();
                assert!(!ordered
                    .getattr("default")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap());
                assert_eq!(
                    ordered
                        .getattr("kind")
                        .unwrap()
                        .getattr("name")
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    "KEYWORD_ONLY"
                );

                // Keep the existing no-keyword entry point and reject positional
                // execution options, which would be ambiguous in future APIs.
                read.bind(py)
                    .call_method1(method, (PyList::empty(py),))
                    .unwrap();
                let error = read
                    .bind(py)
                    .call_method1(method, (PyList::empty(py), 3))
                    .unwrap_err();
                assert!(error.is_instance_of::<PyTypeError>(py));
            }
        });
    }

    #[test]
    fn record_batch_reader_releases_stream_on_completion_or_error() {
        for fail in [false, true] {
            let resource = Arc::new(());
            let weak = Arc::downgrade(&resource);
            let stream: ArrowRecordBatchStream = Box::pin(futures::stream::poll_fn(move |_cx| {
                let _resource = &resource;
                Poll::Ready(fail.then(|| {
                    Err(paimon::Error::Unsupported {
                        message: "read failed".to_string(),
                    })
                }))
            }));
            let reader = PyRecordBatchReader::from_stream(stream);
            assert!(weak.upgrade().is_some());

            let result = reader.next_record_batch();
            assert_eq!(result.is_err(), fail);
            assert!(weak.upgrade().is_none());
            assert!(matches!(reader.next_record_batch(), Ok(None)));
        }
    }

    #[test]
    fn record_batch_reader_close_interrupts_pending_next() {
        let (polled_tx, polled_rx) = mpsc::channel();
        let mut announced = false;
        let stream: ArrowRecordBatchStream = Box::pin(futures::stream::poll_fn(move |_cx| {
            if !announced {
                announced = true;
                polled_tx.send(()).unwrap();
            }
            Poll::<Option<paimon::Result<RecordBatch>>>::Pending
        }));
        let reader = Arc::new(PyRecordBatchReader::from_stream(stream));
        let worker_reader = Arc::clone(&reader);
        let (result_tx, result_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            result_tx
                .send(matches!(worker_reader.next_record_batch(), Ok(None)))
                .unwrap();
        });

        polled_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("stream was not polled");
        reader.close_reader();

        assert!(result_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("pending next was not interrupted"));
        worker.join().unwrap();
        assert!(matches!(reader.next_record_batch(), Ok(None)));
        reader.close_reader();
    }
}
