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

//! Shared vector-search option validation, index I/O helpers, and result ordering.

use crate::io::{FileIO, FileRead};
use crate::spec::{
    CoreOptions, DataField, FileKind, IndexManifestEntry, Predicate, ROW_ID_FIELD_NAME,
};
use crate::table::index_file_path::IndexFileLocation;
use crate::table::pk_vector_position_read::{PKEY_VECTOR_POSITION_COLUMN, SEARCH_SCORE_COLUMN};
use crate::table::read_builder::resolve_projected_fields;
use crate::table::Table;
use crate::vindex::is_vindex_index_type;
use crate::vindex::range_reader::RangeIoStats;
use arrow_array::{Int64Array, RecordBatch};
use arrow_select::interleave::interleave_record_batch;
use paimon_vindex_core::diskann_io::DISKANN_HEADER_SIZE;
use paimon_vindex_core::distance::MetricType;
use paimon_vindex_core::index::VectorIndexReader as VIndexReader;
use std::collections::HashMap;
use std::io::Cursor;

pub(super) use super::partition_filter::validate_partition_filter;

/// Persisted vector properties which must agree across all runs of one column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct IndexVectorSemantics {
    pub metric: MetricType,
    pub dimension: usize,
}

/// Read the scoring metric without requiring dimension metadata or a payload read.
/// Older callers and JSON-only metadata need only this scoring property.
pub(super) async fn read_index_vector_metric(
    file_io: &FileIO,
    table_path: &str,
    entry: &IndexManifestEntry,
) -> crate::Result<Option<MetricType>> {
    let Some(global_meta) = entry.index_file.global_index_meta.as_ref() else {
        return Ok(None);
    };
    if !is_vindex_index_type(&entry.index_file.index_type) {
        return Ok(None);
    }
    if let Some(index_meta) = global_meta.index_meta.as_ref() {
        if let Ok(options) = serde_json::from_slice::<HashMap<String, String>>(index_meta) {
            if let Some(metric) = metric_from_index_options(&options) {
                return Ok(Some(metric));
            }
        }
    }
    Ok(Some(
        read_index_vector_header(file_io, table_path, entry)
            .await?
            .metric,
    ))
}

/// Read the structural definition of an existing run before reusing its coverage.
/// Java and older metadata may omit these options, so fall back to the native header.
pub(super) async fn read_index_vector_semantics(
    file_io: &FileIO,
    table_path: &str,
    entry: &IndexManifestEntry,
) -> crate::Result<Option<IndexVectorSemantics>> {
    let Some(global_meta) = entry.index_file.global_index_meta.as_ref() else {
        return Ok(None);
    };
    if !is_vindex_index_type(&entry.index_file.index_type) {
        return Ok(None);
    }
    if let Some(index_meta) = global_meta.index_meta.as_ref() {
        if let Ok(options) = serde_json::from_slice::<HashMap<String, String>>(index_meta) {
            let dimension = options
                .get("dimension")
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|dimension| *dimension > 0);
            if let (Some(metric), Some(dimension)) =
                (metric_from_index_options(&options), dimension)
            {
                return Ok(Some(IndexVectorSemantics { metric, dimension }));
            }
        }
    }
    Ok(Some(
        read_index_vector_header(file_io, table_path, entry).await?,
    ))
}

fn metric_from_index_options(options: &HashMap<String, String>) -> Option<MetricType> {
    match normalize_metric(options.get("metric")?).as_str() {
        "l2" => Some(MetricType::L2),
        "cosine" => Some(MetricType::Cosine),
        "inner_product" => Some(MetricType::InnerProduct),
        _ => None,
    }
}

async fn read_index_vector_header(
    file_io: &FileIO,
    table_path: &str,
    entry: &IndexManifestEntry,
) -> crate::Result<IndexVectorSemantics> {
    let path = IndexFileLocation::Global { table_path }.resolve(
        &entry.index_file.file_name,
        entry.index_file.external_path.as_deref(),
    );
    let input = file_io.new_input(&path)?;
    let read_error = |e| crate::Error::DataInvalid {
        message: format!(
            "Failed to read vindex index file '{}' for vector metric: {}",
            entry.index_file.file_name, e
        ),
        source: Some(Box::new(e)),
    };
    let header_size = if entry.index_file.file_size > 0 {
        (entry.index_file.file_size as u64).min(DISKANN_HEADER_SIZE as u64)
    } else {
        input
            .metadata()
            .await
            .map_err(&read_error)?
            .size
            .min(DISKANN_HEADER_SIZE as u64)
    };
    let file_reader = input.reader().await.map_err(&read_error)?;
    let bytes = file_reader.read(0..header_size).await.map_err(read_error)?;
    let reader = VIndexReader::open(Cursor::new(bytes)).map_err(|e| crate::Error::DataInvalid {
        message: format!(
            "Failed to open paimon-vindex-core reader for vector metric: {}",
            e
        ),
        source: Some(Box::new(e)),
    })?;
    let metadata = reader.metadata();
    Ok(IndexVectorSemantics {
        metric: metadata.metric,
        dimension: metadata.dimension,
    })
}

pub(super) fn add_filter(current: &mut Option<Predicate>, filter: Predicate) {
    *current = Some(match current.take() {
        Some(previous) => Predicate::and(vec![previous, filter]),
        None => filter,
    });
}

/// A search column selects one persisted vector format, as in Java's scan.
/// Different columns, removed entries and scalar indexes do not participate.
pub(super) fn validate_global_vector_index_types(
    entries: &[IndexManifestEntry],
    field_id: i32,
    column: &str,
) -> crate::Result<()> {
    let mut index_type = None;
    for entry in entries {
        let file = &entry.index_file;
        if entry.kind != FileKind::Add
            || !is_vindex_index_type(&file.index_type)
            || !file
                .global_index_meta
                .as_ref()
                .is_some_and(|meta| meta.index_field_id == field_id)
        {
            continue;
        }
        if let Some(previous) = index_type {
            if previous != file.index_type.as_str() {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Vector column '{column}' has multiple index types: {previous} and {}",
                        file.index_type
                    ),
                    source: None,
                });
            }
        } else {
            index_type = Some(file.index_type.as_str());
        }
    }
    Ok(())
}

pub(super) fn configured_vector_index_type(
    table_options: &HashMap<String, String>,
    query_options: &HashMap<String, String>,
    column: &str,
) -> crate::Result<Option<String>> {
    let keys = [
        "index_type".to_string(),
        "index-type".into(),
        "vector.index-type".into(),
        format!("fields.{column}.index-type"),
    ];
    if let Some(value) = keys.iter().find_map(|key| query_options.get(key)) {
        return Ok(Some(value.trim().to_ascii_lowercase()));
    }
    if let Some(kind) = super::global_index_options::vector_index_type(query_options, column)? {
        return Ok(Some(kind.into()));
    }
    if let Some(kind) = super::global_index_options::vector_index_type(table_options, column)? {
        return Ok(Some(kind.into()));
    }
    Ok(keys
        .iter()
        .find_map(|key| table_options.get(key))
        .map(|value| value.trim().to_ascii_lowercase()))
}

pub(super) fn current_tokio_runtime_handle() -> crate::Result<tokio::runtime::Handle> {
    tokio::runtime::Handle::try_current().map_err(|error| crate::Error::UnexpectedError {
        message: "Vector index range reader requires a Tokio runtime".to_string(),
        source: Some(Box::new(error)),
    })
}

fn vindex_index_parallelism(entry_count: usize, max_concurrency: usize) -> usize {
    entry_count.min(max_concurrency).max(1)
}

pub(super) fn log_vindex_range_io_stats(file: &str, query_count: usize, stats: &RangeIoStats) {
    let stats = stats.snapshot();
    log::debug!(
        target: "paimon::vector_search",
        "event=paimon_vector_range_io file={} nq={} logical_ranges={} requested_bytes={} file_read_calls={} returned_bytes={} read_ahead_hits={} io_wait_sum_ms={:.3} range_permit_wait_sum_ms={:.3} peak_in_flight_reads={} read_many_merged_ranges={} read_many_chunks={} read_many_chunk_size_sum={} read_many_chunk_size_min={} read_many_chunk_size_max={}",
        file,
        query_count,
        stats.logical_ranges,
        stats.requested_bytes,
        stats.file_read_calls,
        stats.returned_bytes,
        stats.read_ahead_hits,
        stats.io_wait_nanos as f64 / 1_000_000.0,
        stats.range_permit_wait_nanos as f64 / 1_000_000.0,
        stats.peak_in_flight_reads,
        stats.read_many_merged_ranges,
        stats.read_many_chunks,
        stats.read_many_chunk_size_sum,
        stats.read_many_chunk_size_min,
        stats.read_many_chunk_size_max,
    );
}

pub(super) fn vindex_concurrency_limits(
    core_options: &CoreOptions<'_>,
    entry_count: usize,
    max_concurrency: usize,
) -> crate::Result<(usize, usize)> {
    Ok((
        vindex_index_parallelism(entry_count, max_concurrency),
        core_options.global_index_vindex_read_thread_num()?,
    ))
}

/// Unwrap a single-query result from a batch entry point that must return exactly
/// one element per input query.
///
/// The batch terminals below are handed one query, so their result vector holds
/// exactly one entry. A `debug_assert_eq!(len, 1)` followed by `remove(0)` checked
/// that only in debug builds, where a release build would instead panic on an index
/// out of bounds for an empty vector -- or SILENTLY return the first of several,
/// pairing the caller's single query with another query's result. A length that is
/// wrong means the batch ran the wrong number of searches, which is a programming
/// error in this crate rather than bad input, so it is reported as one.
pub(super) fn take_only_result<T>(results: Vec<T>, operation: &str) -> crate::Result<T> {
    let mut results = results.into_iter();
    let result = results
        .next()
        .ok_or_else(|| crate::Error::UnexpectedError {
            message: format!("{operation} returned no result for one query"),
            source: None,
        })?;
    if results.next().is_some() {
        return Err(crate::Error::UnexpectedError {
            message: format!("{operation} returned more than one result for one query"),
            source: None,
        });
    }
    Ok(result)
}

/// Resolve the projected fields for the materialization read-type. Default
/// (no projection set) is all user table fields; otherwise the requested
/// names resolved via `resolve_projected_fields`. Rejects reserved metadata
/// names and `_ROW_ID` so a user cannot request a hidden column.
pub(super) fn resolve_materialize_read_type(
    table: &Table,
    projection: Option<&[String]>,
) -> crate::Result<Vec<DataField>> {
    let fields = match projection {
        None => table.schema().fields().to_vec(),
        Some(names) => {
            for name in names {
                if is_reserved_read_column(name) {
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "vector search read projection must not request reserved column '{name}'"
                        ),
                        source: None,
                    });
                }
            }
            resolve_projected_fields(
                table.identifier().full_name(),
                table.schema().fields(),
                names,
                true,
            )?
        }
    };
    // The default projection returns every user column, so a user column
    // whose name collides with an injected metadata column must be rejected
    // on the resolved field list too — not only when explicitly requested.
    ensure_no_reserved_read_columns(&fields)?;
    Ok(fields)
}

/// Names a read injects as metadata columns — `__paimon_search_score`,
/// `_PKEY_VECTOR_POSITION`, and `_ROW_ID` — that a materialized read type must
/// not reuse for a user column.
fn is_reserved_read_column(name: &str) -> bool {
    name == PKEY_VECTOR_POSITION_COLUMN || name == SEARCH_SCORE_COLUMN || name == ROW_ID_FIELD_NAME
}

/// Reject a materialized read type whose resolved fields contain a reserved
/// metadata column name. Applied to the RESOLVED field list so the default
/// (all user columns) projection is covered, not only an explicit one.
pub(crate) fn ensure_no_reserved_read_columns(fields: &[DataField]) -> crate::Result<()> {
    for field in fields {
        if is_reserved_read_column(field.name()) {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "search read must not include reserved column '{}'",
                    field.name()
                ),
                source: None,
            });
        }
    }
    Ok(())
}

/// One materialized row tagged with its best-first `rank` and its `(batch_index,
/// row_index)` location in the retained materialization batches.
pub(crate) struct RankedRow {
    rank: usize,
    batch_index: usize,
    row_index: usize,
}

/// For each row in a materialized batch, look up its best-first rank via the
/// `(partition bytes, bucket, file, position)` key and record its location. The
/// `_PKEY_VECTOR_POSITION` column supplies the physical position; every row must
/// map to a candidate rank (the batch came from that candidate's file), so a miss
/// fails loud rather than silently dropping a row.
#[allow(clippy::too_many_arguments)]
pub(crate) fn collect_ranked_rows(
    batch: &RecordBatch,
    batch_index: usize,
    partition_bytes: &[u8],
    bucket: i32,
    file_name: &str,
    rank_of: &HashMap<(Vec<u8>, i32, String, i64), usize>,
    out: &mut Vec<RankedRow>,
) -> crate::Result<()> {
    let position_idx = batch
        .schema()
        .index_of(PKEY_VECTOR_POSITION_COLUMN)
        .map_err(|_| crate::Error::DataInvalid {
            message: format!("materialized batch missing {PKEY_VECTOR_POSITION_COLUMN} column"),
            source: None,
        })?;
    let positions = batch
        .column(position_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!("{PKEY_VECTOR_POSITION_COLUMN} column is not Int64"),
            source: None,
        })?;
    for row_index in 0..batch.num_rows() {
        let position = positions.value(row_index);
        let key = (
            partition_bytes.to_vec(),
            bucket,
            file_name.to_string(),
            position,
        );
        let rank = *rank_of.get(&key).ok_or_else(|| crate::Error::DataInvalid {
            message: format!(
                "materialized row (file {file_name}, position {position}) has no matching search candidate"
            ),
            source: None,
        })?;
        out.push(RankedRow {
            rank,
            batch_index,
            row_index,
        });
    }
    Ok(())
}

/// Reorder the materialized rows into best-first order and drop the internal
/// `_PKEY_VECTOR_POSITION` column, yielding a single output batch (empty input
/// yields no batches). The projected user columns and `__paimon_search_score` are
/// retained.
pub(crate) fn reorder_and_strip_position(
    batches: &[RecordBatch],
    mut ranked: Vec<RankedRow>,
) -> crate::Result<Vec<RecordBatch>> {
    if ranked.is_empty() {
        return Ok(Vec::new());
    }
    ranked.sort_by_key(|r| r.rank);
    let indices: Vec<(usize, usize)> = ranked
        .iter()
        .map(|r| (r.batch_index, r.row_index))
        .collect();
    let refs: Vec<&RecordBatch> = batches.iter().collect();
    let reordered =
        interleave_record_batch(&refs, &indices).map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to reorder vector search read rows: {e}"),
            source: None,
        })?;

    // Drop the internal position column; keep every other column (projected user
    // columns + __paimon_search_score) in order.
    let position_idx = reordered
        .schema()
        .index_of(PKEY_VECTOR_POSITION_COLUMN)
        .map_err(|_| crate::Error::DataInvalid {
            message: format!("reordered batch missing {PKEY_VECTOR_POSITION_COLUMN} column"),
            source: None,
        })?;
    let keep: Vec<usize> = (0..reordered.num_columns())
        .filter(|i| *i != position_idx)
        .collect();
    let projected = reordered
        .project(&keep)
        .map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to drop position column: {e}"),
            source: None,
        })?;
    Ok(vec![projected])
}

pub(super) fn indexed_search_limit(limit: usize, refine_factor: usize) -> crate::Result<usize> {
    if refine_factor == 0 {
        return Ok(limit);
    }
    let search_limit =
        limit
            .checked_mul(refine_factor)
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: format!(
                    "Vector search limit overflow: limit={limit}, refine factor={refine_factor}"
                ),
            })?;
    if search_limit > i32::MAX as usize {
        return Err(crate::Error::ConfigInvalid {
            message: format!(
                "Vector search limit overflow: limit={limit}, refine factor={refine_factor}"
            ),
        });
    }
    Ok(search_limit)
}

pub(super) fn normalize_metric(metric: &str) -> String {
    metric.to_ascii_lowercase().replace('-', "_")
}

fn indexed_type_prefixes(field_name: &str, index_type: &str) -> Vec<String> {
    let mut prefixes = Vec::new();
    add_refine_prefixes(&mut prefixes, &format!("fields.{field_name}."), index_type);
    add_refine_prefixes(&mut prefixes, "", index_type);
    prefixes
}

fn add_refine_prefixes(prefixes: &mut Vec<String>, base: &str, index_type: &str) {
    if !index_type.is_empty() {
        prefixes.push(format!("{base}{index_type}."));
        let normalized = normalize_metric(index_type);
        if normalized != index_type {
            prefixes.push(format!("{base}{normalized}."));
        }
        if normalized.starts_with("ivf") {
            prefixes.push(format!("{base}ivf."));
        }
    }
    prefixes.push(base.to_string());
}

pub(super) fn configured_refine_factor(
    search_options: &HashMap<String, String>,
    table_options: &HashMap<String, String>,
    field_name: &str,
    index_type: &str,
) -> crate::Result<usize> {
    if let Some(value) =
        configured_refine_factor_from_options(search_options, field_name, index_type)
    {
        return parse_refine_factor(&value);
    }
    if let Some(value) =
        configured_refine_factor_from_options(table_options, field_name, index_type)
    {
        return parse_refine_factor(&value);
    }
    Ok(0)
}

fn configured_refine_factor_from_options(
    options: &HashMap<String, String>,
    field_name: &str,
    index_type: &str,
) -> Option<String> {
    for prefix in indexed_type_prefixes(field_name, index_type) {
        for suffix in [
            "refine_factor",
            "refine-factor",
            "rerank_factor",
            "rerank-factor",
        ] {
            if let Some(value) = options.get(&(prefix.clone() + suffix)) {
                return Some(value.trim().to_string());
            }
        }
    }
    None
}

fn parse_refine_factor(value: &str) -> crate::Result<usize> {
    let factor = value
        .parse::<usize>()
        .map_err(|_| crate::Error::ConfigInvalid {
            message: format!("Invalid vector refine factor: {value}. Must be an integer."),
        })?;
    if factor == 0 {
        return Err(crate::Error::ConfigInvalid {
            message: format!("Vector refine factor must be positive, got: {value}"),
        });
    }
    Ok(factor)
}

/// A malformed PK configuration must not reject an unrelated DE query.
pub(super) fn targets_primary_key_column(core: &CoreOptions<'_>, column: &str) -> bool {
    core.primary_key_vector_index_enabled()
        && core
            .primary_key_vector_index_columns()
            .ok()
            .is_some_and(|columns| columns.iter().any(|c| c == column))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{GlobalIndexMeta, IndexFileMeta};

    fn entry(index_meta: Option<Vec<u8>>) -> IndexManifestEntry {
        IndexManifestEntry {
            version: 1,
            kind: FileKind::Add,
            partition: Vec::new(),
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: "ivf-flat".into(),
                file_name: "vectors.index".into(),
                file_size: 0,
                row_count: 3,
                deletion_vectors_ranges: None,
                external_path: None,
                global_index_meta: Some(GlobalIndexMeta {
                    row_range_start: 0,
                    row_range_end: 2,
                    index_field_id: 1,
                    extra_field_ids: None,
                    index_meta,
                    source_meta: None,
                }),
            },
        }
    }

    #[tokio::test]
    async fn metric_only_json_does_not_require_dimension_or_payload() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let entry = entry(Some(br#"{"metric":"L2"}"#.to_vec()));
        let table_path = "memory:/metric-only-vector-metadata";
        assert!(!file_io
            .exists(&format!("{table_path}/index/vectors.index"))
            .await
            .unwrap());
        assert_eq!(
            read_index_vector_metric(&file_io, table_path, &entry)
                .await
                .unwrap(),
            Some(MetricType::L2)
        );
    }

    #[tokio::test]
    async fn complete_vector_semantics_json_does_not_read_payload() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let entry = entry(Some(
            br#"{"metric":"inner-product","dimension":"3"}"#.to_vec(),
        ));
        let table_path = "memory:/complete-vector-metadata";
        assert!(!file_io
            .exists(&format!("{table_path}/index/vectors.index"))
            .await
            .unwrap());
        assert_eq!(
            read_index_vector_semantics(&file_io, table_path, &entry)
                .await
                .unwrap(),
            Some(IndexVectorSemantics {
                metric: MetricType::InnerProduct,
                dimension: 3,
            })
        );
    }

    #[tokio::test]
    async fn legacy_vector_semantics_read_dimension_from_native_header() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/legacy-vector-metadata";
        let payload = crate::table::vector_search_test_utils::build_vindex_segment_bytes("l2");
        file_io
            .new_output(&format!("{table_path}/index/vectors.index"))
            .unwrap()
            .write(payload.into())
            .await
            .unwrap();
        for index_meta in [
            None,
            Some(br#"{}"#.to_vec()),
            Some(br#"{"metric":"l2"}"#.to_vec()),
            Some(br#"{"metric":"l2","dimension":"0"}"#.to_vec()),
        ] {
            assert_eq!(
                read_index_vector_semantics(&file_io, table_path, &entry(index_meta))
                    .await
                    .unwrap(),
                Some(IndexVectorSemantics {
                    metric: MetricType::L2,
                    dimension: 2,
                })
            );
        }
    }
}
