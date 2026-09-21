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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use paimon::spec::{DataField, DataType, Predicate, RowType};
use paimon::table::{ArrowRecordBatchStream, DataSplit, IncrementalScanMode, RowRange, Table};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use tokio::sync::Notify;

use crate::error::to_py_err;
use crate::predicate::dict_to_predicate;

const MAP_SELECTED_KEYS_PREFIX: &str = "__PAIMON_MAP_SELECTED_KEYS:";
const MAP_SELECTED_KEYS_DELIMITER: char = ';';

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
    read_type: &Option<Vec<DataField>>,
    limit: Option<usize>,
    filter: &Option<Predicate>,
    case_sensitive: bool,
) -> PyResult<()> {
    builder.with_case_sensitive(case_sensitive);
    if let Some(read_type) = read_type {
        builder.with_read_type(read_type.clone());
    } else if let Some(projection) = projection {
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

/// Resolve flat output paths to the authoritative nested read type used by the
/// core reader. ROW children are pruned recursively. MAP values remain MAPs,
/// but their requested string keys are carried in the temporary field
/// description so shared-shredding Parquet files can prune physical columns.
fn project_nested_read_type(
    fields: &[DataField],
    paths: &[Vec<String>],
) -> PyResult<Vec<DataField>> {
    if paths.iter().any(Vec::is_empty) {
        return Err(PyValueError::new_err(
            "nested projection paths must not be empty",
        ));
    }

    let mut result = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for path in paths {
        let name = &path[0];
        if !seen.insert(name.clone()) {
            continue;
        }
        let field = fields
            .iter()
            .find(|field| field.name() == name)
            .ok_or_else(|| {
                PyValueError::new_err(format!(
                    "nested projection field '{}' does not exist",
                    path.join(".")
                ))
            })?;
        let matching: Vec<&[String]> = paths
            .iter()
            .filter(|candidate| candidate.first() == Some(name))
            .map(|candidate| candidate[1..].as_ref())
            .collect();
        result.push(project_nested_field(field, &matching, name)?);
    }
    Ok(result)
}

fn project_nested_field(
    field: &DataField,
    tails: &[&[String]],
    full_name: &str,
) -> PyResult<DataField> {
    if tails.iter().any(|tail| tail.is_empty()) {
        return Ok(field.clone());
    }
    match field.data_type() {
        DataType::Row(row) => {
            let child_paths: Vec<Vec<String>> = tails.iter().map(|tail| tail.to_vec()).collect();
            let children = project_nested_read_type(row.fields(), &child_paths).map_err(|_| {
                PyValueError::new_err(format!(
                    "nested projection field '{}' does not exist",
                    tails
                        .first()
                        .map(|tail| format!("{full_name}.{}", tail.join(".")))
                        .unwrap_or_else(|| full_name.to_string())
                ))
            })?;
            Ok(DataField::new(
                field.id(),
                field.name().to_string(),
                DataType::Row(RowType::with_nullable(
                    field.data_type().is_nullable(),
                    children,
                )),
            )
            .with_description(field.description().map(str::to_string)))
        }
        DataType::Map(_) if tails.iter().all(|tail| tail.len() == 1) => {
            let mut keys = Vec::new();
            for tail in tails {
                let key = &tail[0];
                if key.contains(MAP_SELECTED_KEYS_DELIMITER)
                    || key.starts_with(MAP_SELECTED_KEYS_PREFIX)
                {
                    // Keep the complete MAP for keys which cannot be encoded
                    // by the cross-language selected-key convention.
                    return Ok(field.clone());
                }
                if !keys.contains(key) {
                    keys.push(key.clone());
                }
            }
            Ok(field.clone().with_description(Some(format!(
                "{MAP_SELECTED_KEYS_PREFIX}{}",
                keys.join(&MAP_SELECTED_KEYS_DELIMITER.to_string())
            ))))
        }
        _ => Err(PyValueError::new_err(format!(
            "nested projection field '{}' is not a ROW or MAP",
            tails
                .first()
                .map(|tail| format!("{full_name}.{}", tail.join(".")))
                .unwrap_or_else(|| full_name.to_string())
        ))),
    }
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
    read_type: Option<Vec<DataField>>,
    limit: Option<usize>,
    filter: Option<Predicate>,
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
            row_ranges: None,
            case_sensitive: true,
            blob_parallelism: None,
            include_row_kind: false,
        })
    }
}

#[pymethods]
impl PyReadBuilder {
    fn with_projection(mut slf: PyRefMut<'_, Self>, columns: Vec<String>) -> PyRefMut<'_, Self> {
        slf.projection = Some(columns);
        slf.read_type = None;
        slf
    }

    /// Project top-level fields or nested ROW leaves by their exact name paths.
    /// MAP paths keep the complete MAP so the caller can extract literal keys.
    fn with_nested_projection(
        mut slf: PyRefMut<'_, Self>,
        paths: Vec<Vec<String>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let schema = slf.table.schema();
        let mut read_type = project_nested_read_type(schema.fields(), &paths)?;
        let options = schema.options();
        let has_default_aggregation = options.contains_key("fields.default-aggregate-function");
        for projected in &mut read_type {
            let selected_map = matches!(projected.data_type(), DataType::Map(_))
                && projected
                    .description()
                    .is_some_and(|description| description.starts_with(MAP_SELECTED_KEYS_PREFIX));
            let has_field_aggregation =
                options.contains_key(&format!("fields.{}.aggregate-function", projected.name()));
            if selected_map && (has_default_aggregation || has_field_aggregation) {
                // Merge engines may need the complete MAP to calculate the
                // projected key. Match PyPaimon's complete-MAP fallback.
                *projected = schema
                    .fields()
                    .iter()
                    .find(|field| field.name() == projected.name())
                    .expect("nested projection came from this schema")
                    .clone();
            }
        }
        slf.read_type = Some(read_type);
        slf.projection = None;
        Ok(slf)
    }

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
            read_type: self.read_type.clone(),
            limit: self.limit,
            filter: self.filter.clone(),
            row_ranges: self.row_ranges.clone(),
            case_sensitive: self.case_sensitive,
            incremental_scan: None,
            row_position_slice: None,
            row_position_shard: None,
            chunk_shuffle: None,
            shard: None,
        }
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
    row_ranges: Option<Vec<RowRange>>,
    case_sensitive: bool,
    incremental_scan: Option<PyIncrementalScan>,
    row_position_slice: Option<(u64, u64)>,
    row_position_shard: Option<(u64, u64)>,
    chunk_shuffle: Option<PyChunkShuffle>,
    shard: Option<(usize, usize)>,
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

    fn core_incremental_scan(
        &self,
        start: i64,
        end: i64,
        mode: IncrementalScanMode,
    ) -> PyResult<paimon::table::IncrementalScan<'_>> {
        let mut scan = self.read_builder()?.new_incremental_scan(mode, start, end);
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
            &self.projection,
            &self.read_type,
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

    /// Select one balanced worker shard for a distributed scan.
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
    ) -> PyResult<ArrowRecordBatchStream> {
        let splits = extract_splits(splits)?;
        py.detach(|| {
            let mut builder = self.table.new_read_builder();
            apply_read_config(
                &mut builder,
                &self.projection,
                &self.read_type,
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
            let read = builder.new_read().map_err(to_py_err)?;
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
    /// Capability marker for readers that stop DE BLOB I/O at the read limit.
    #[staticmethod]
    fn supports_pruning_blob_limit() -> bool {
        true
    }

    /// Lazily read the given splits as an iterator of PyArrow RecordBatches.
    fn read_arrow(
        &self,
        py: Python<'_>,
        splits: &Bound<'_, PyAny>,
    ) -> PyResult<PyRecordBatchReader> {
        Ok(PyRecordBatchReader {
            stream: Mutex::new(Some(self.read_stream(py, splits)?)),
            closed: AtomicBool::new(false),
            close_notify: Notify::new(),
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
    stream: Mutex<Option<ArrowRecordBatchStream>>,
    closed: AtomicBool,
    close_notify: Notify,
}

impl PyRecordBatchReader {
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
            Some(stream) => runtime()
                .block_on(async {
                    tokio::select! {
                        biased;
                        _ = self.close_notify.notified() => Ok(None),
                        batch = stream.try_next() => batch,
                    }
                })
                .map_err(to_py_err),
            None => Ok(None),
        };
        if self.closed.load(Ordering::Acquire) {
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
        // notify_one stores a permit when next_record_batch has not started
        // polling yet, avoiding a lost wake-up between its closed check and
        // the select.
        self.close_notify.notify_one();
        if let Ok(mut stream) = self.stream.try_lock() {
            stream.take();
        }
    }
}

#[pymethods]
impl PyRecordBatchReader {
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
    /// Java/Python-compatible DataSplit and score-free IndexedSplit frames.
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

    use super::*;

    use paimon::spec::{DataField, DataType, IntType, MapType, RowType, VarCharType};

    #[test]
    fn nested_projection_prunes_rows_and_marks_selected_map_keys() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "payload".to_string(),
                DataType::Row(RowType::new(vec![
                    DataField::new(2, "version".to_string(), DataType::Int(IntType::new())),
                    DataField::new(
                        3,
                        "details".to_string(),
                        DataType::Row(RowType::new(vec![
                            DataField::new(
                                4,
                                "name".to_string(),
                                DataType::VarChar(VarCharType::string_type()),
                            ),
                            DataField::new(5, "score".to_string(), DataType::Int(IntType::new())),
                        ])),
                    ),
                ])),
            ),
            DataField::new(
                6,
                "attrs".to_string(),
                DataType::Map(MapType::new(
                    DataType::VarChar(VarCharType::string_type()),
                    DataType::Int(IntType::new()),
                )),
            ),
        ];

        let projected = project_nested_read_type(
            &fields,
            &[
                vec![
                    "payload".to_string(),
                    "details".to_string(),
                    "score".to_string(),
                ],
                vec!["id".to_string()],
                vec!["attrs".to_string(), "selected".to_string()],
            ],
        )
        .unwrap();

        assert_eq!(
            projected.iter().map(DataField::name).collect::<Vec<_>>(),
            vec!["payload", "id", "attrs"]
        );
        let DataType::Row(payload) = projected[0].data_type() else {
            panic!("payload must remain a row");
        };
        assert_eq!(payload.fields().len(), 1);
        assert_eq!(payload.fields()[0].name(), "details");
        let DataType::Row(details) = payload.fields()[0].data_type() else {
            panic!("details must remain a row");
        };
        assert_eq!(
            details
                .fields()
                .iter()
                .map(DataField::name)
                .collect::<Vec<_>>(),
            vec!["score"]
        );
        assert_eq!(projected[2].data_type(), fields[2].data_type());
        assert_eq!(
            projected[2].description(),
            Some("__PAIMON_MAP_SELECTED_KEYS:selected")
        );
    }

    #[test]
    fn nested_projection_rejects_an_invalid_struct_path() {
        let fields = vec![DataField::new(
            1,
            "payload".to_string(),
            DataType::Row(RowType::new(vec![DataField::new(
                2,
                "version".to_string(),
                DataType::Int(IntType::new()),
            )])),
        )];

        let error = project_nested_read_type(
            &fields,
            &[vec!["payload".to_string(), "missing".to_string()]],
        )
        .unwrap_err();
        assert!(error.to_string().contains("payload.missing"));
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
        let reader = Arc::new(PyRecordBatchReader {
            stream: Mutex::new(Some(stream)),
            closed: AtomicBool::new(false),
            close_notify: Notify::new(),
        });
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
