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

use crate::vector_search::{GlobalIndexIOMeta, VectorSearch};
use crate::vindex::vector_search_timing_enabled;
use paimon_vindex_core::distance::MetricType;
use paimon_vindex_core::index::{
    VectorIndexMetadata, VectorIndexReader as VIndexReader, VectorSearchParams,
};
use paimon_vindex_core::io::{ReadRequest, SeekRead, SeekReadCapabilities};
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::io;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

const DEFAULT_NPROBE: usize = 16;
const NPROBE_PARAMETER: &str = "ivf.nprobe";
const NATIVE_BATCH_PROCESS_WORKING_SET_BYTES: usize = 64 * 1024 * 1024;
// Native searches run on dedicated executor threads, so blocking here does not block async I/O.
static NATIVE_BATCH_MEMORY_POOL: NativeBatchMemoryPool =
    NativeBatchMemoryPool::new(NATIVE_BATCH_PROCESS_WORKING_SET_BYTES);

struct NativeBatchMemoryPool {
    capacity: usize,
    available_bytes: Mutex<usize>,
    memory_available: Condvar,
}

impl NativeBatchMemoryPool {
    const fn new(bytes: usize) -> Self {
        Self {
            capacity: bytes,
            available_bytes: Mutex::new(bytes),
            memory_available: Condvar::new(),
        }
    }

    fn acquire(&self, bytes: usize) -> NativeBatchMemoryPermit<'_> {
        let bytes = bytes.min(self.capacity);
        let mut available = self
            .available_bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *available < bytes {
            available = self
                .memory_available
                .wait(available)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *available -= bytes;
        NativeBatchMemoryPermit { pool: self, bytes }
    }
}

struct NativeBatchMemoryPermit<'a> {
    pool: &'a NativeBatchMemoryPool,
    bytes: usize,
}

impl Drop for NativeBatchMemoryPermit<'_> {
    fn drop(&mut self) {
        let mut available = self
            .pool
            .available_bytes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *available += self.bytes;
        drop(available);
        self.pool.memory_available.notify_all();
    }
}

fn acquire_native_batch_memory(bytes: usize) -> NativeBatchMemoryPermit<'static> {
    NATIVE_BATCH_MEMORY_POOL.acquire(bytes)
}

fn native_batch_memory_reservation(index_parallelism: usize) -> usize {
    NATIVE_BATCH_PROCESS_WORKING_SET_BYTES / index_parallelism.max(1)
}

#[derive(Clone, Copy, Default)]
struct VindexLoadTiming {
    vindex_open: Duration,
    metadata: Duration,
    optimize: Duration,
}

#[derive(Default)]
struct VindexBatchStats {
    native_chunk_queries: Vec<usize>,
    scalar_chunk_count: usize,
    max_chunk_size: usize,
    memory_budget_bytes: usize,
    batch_index_parallelism: usize,
}

type VindexBatchSearchResult = (Vec<Option<HashMap<u64, f32>>>, Option<VindexBatchStats>);

trait ErasedSeekRead: Send {
    fn pread_erased(&mut self, ranges: &mut [ReadRequest<'_>]) -> io::Result<()>;

    fn try_clone_erased(&self) -> io::Result<Option<Box<dyn ErasedSeekRead>>>;

    fn capabilities_erased(&self) -> SeekReadCapabilities;
}

impl<T: SeekRead + 'static> ErasedSeekRead for T {
    fn pread_erased(&mut self, ranges: &mut [ReadRequest<'_>]) -> io::Result<()> {
        SeekRead::pread(self, ranges)
    }

    fn try_clone_erased(&self) -> io::Result<Option<Box<dyn ErasedSeekRead>>> {
        Ok(SeekRead::try_clone_reader(self)?
            .map(|reader| Box::new(reader) as Box<dyn ErasedSeekRead>))
    }

    fn capabilities_erased(&self) -> SeekReadCapabilities {
        SeekRead::read_capabilities(self)
    }
}

struct VindexInput(Box<dyn ErasedSeekRead>);

impl VindexInput {
    fn new<S: SeekRead + 'static>(source: S) -> Self {
        Self(Box::new(source))
    }
}

impl SeekRead for VindexInput {
    fn pread(&mut self, ranges: &mut [ReadRequest<'_>]) -> io::Result<()> {
        self.0.pread_erased(ranges)
    }

    fn try_clone_reader(&self) -> io::Result<Option<Self>> {
        Ok(self.0.try_clone_erased()?.map(Self))
    }

    fn read_capabilities(&self) -> SeekReadCapabilities {
        self.0.capabilities_erased()
    }
}

pub struct VindexVectorGlobalIndexReader {
    io_meta: GlobalIndexIOMeta,
    options: HashMap<String, String>,
    batch_index_parallelism: usize,
    reader: Option<VIndexReader<VindexInput>>,
    metadata: Option<VectorIndexMetadata>,
    timing_enabled: bool,
    load_timing: VindexLoadTiming,
    batch_stats: Option<VindexBatchStats>,
}

impl VindexVectorGlobalIndexReader {
    pub fn new(io_meta: GlobalIndexIOMeta, options: HashMap<String, String>) -> Self {
        Self {
            io_meta,
            options,
            batch_index_parallelism: 1,
            reader: None,
            metadata: None,
            timing_enabled: vector_search_timing_enabled(),
            load_timing: VindexLoadTiming::default(),
            batch_stats: None,
        }
    }

    pub(crate) fn with_batch_index_parallelism(mut self, parallelism: usize) -> Self {
        self.batch_index_parallelism = parallelism.max(1);
        self
    }

    pub fn visit_vector_search<S: SeekRead + 'static>(
        &mut self,
        vector_search: &VectorSearch,
        stream_fn: impl FnOnce(&str) -> crate::Result<S>,
    ) -> crate::Result<Option<HashMap<u64, f32>>> {
        Ok(self
            .visit_batch_vector_search(std::slice::from_ref(vector_search), stream_fn)?
            .pop()
            .expect("single vector search result"))
    }

    pub fn visit_batch_vector_search<S: SeekRead + 'static>(
        &mut self,
        vector_searches: &[VectorSearch],
        stream_fn: impl FnOnce(&str) -> crate::Result<S>,
    ) -> crate::Result<Vec<Option<HashMap<u64, f32>>>> {
        self.visit_batch_vector_search_validated(vector_searches, stream_fn, |_| Ok(()))
    }

    pub(crate) fn visit_batch_vector_search_validated<S, F>(
        &mut self,
        vector_searches: &[VectorSearch],
        stream_fn: impl FnOnce(&str) -> crate::Result<S>,
        validate: F,
    ) -> crate::Result<Vec<Option<HashMap<u64, f32>>>>
    where
        S: SeekRead + 'static,
        F: FnOnce(&VectorIndexMetadata) -> crate::Result<()>,
    {
        let total_start = self.timing_enabled.then(Instant::now);
        self.ensure_loaded(stream_fn, validate)?;
        let search_start = self.timing_enabled.then(Instant::now);
        let results = self.search_batch(vector_searches)?;
        if let (Some(total_start), Some(search_start), Some(stats)) =
            (total_start, search_start, self.batch_stats.as_ref())
        {
            let total = total_start.elapsed();
            let native_search = search_start.elapsed();
            let load = self
                .load_timing
                .vindex_open
                .saturating_add(self.load_timing.metadata)
                .saturating_add(self.load_timing.optimize);
            let unattributed = total.saturating_sub(load.saturating_add(native_search));
            let chunk_queries = stats
                .native_chunk_queries
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let nprobe = self
                .options
                .get(NPROBE_PARAMETER)
                .cloned()
                .unwrap_or_else(|| DEFAULT_NPROBE.to_string());
            log::debug!(
                target: "paimon::vector_search",
                "event=paimon_vindex_reader file={} nq={} nprobe={} batch_index_parallelism={} memory_budget_bytes={} max_chunk_size={} native_chunk_count={} native_chunk_queries={} scalar_chunk_count={} total_ms={:.3} vindex_open_ms={:.3} metadata_ms={:.3} optimize_ms={:.3} native_search_wall_ms={:.3} unattributed_ms={:.3}",
                self.io_meta.file_path,
                vector_searches.len(),
                nprobe,
                stats.batch_index_parallelism,
                stats.memory_budget_bytes,
                stats.max_chunk_size,
                stats.native_chunk_queries.len(),
                chunk_queries,
                stats.scalar_chunk_count,
                total.as_secs_f64() * 1000.0,
                self.load_timing.vindex_open.as_secs_f64() * 1000.0,
                self.load_timing.metadata.as_secs_f64() * 1000.0,
                self.load_timing.optimize.as_secs_f64() * 1000.0,
                native_search.as_secs_f64() * 1000.0,
                unattributed.as_secs_f64() * 1000.0,
            );
        }
        Ok(results)
    }

    #[cfg(test)]
    pub(crate) fn load<S: SeekRead + 'static>(
        &mut self,
        stream_fn: impl FnOnce(&str) -> crate::Result<S>,
    ) -> crate::Result<()> {
        self.ensure_loaded(stream_fn, |_| Ok(()))
    }

    pub(crate) fn metadata(&self) -> crate::Result<&VectorIndexMetadata> {
        self.metadata
            .as_ref()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "vindex metadata not initialized".to_string(),
                source: None,
            })
    }

    fn search_batch(
        &mut self,
        vector_searches: &[VectorSearch],
    ) -> crate::Result<Vec<Option<HashMap<u64, f32>>>> {
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "vindex reader not initialized".to_string(),
                source: None,
            })?;
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "vindex metadata not initialized".to_string(),
                source: None,
            })?;
        let (results, batch_stats) = search_batch_vindex(
            reader,
            metadata,
            &self.options,
            vector_searches,
            self.batch_index_parallelism,
            self.timing_enabled,
        )?;
        self.batch_stats = batch_stats;
        Ok(results)
    }

    #[cfg(test)]
    fn search(&mut self, vector_search: &VectorSearch) -> crate::Result<Option<HashMap<u64, f32>>> {
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "vindex reader not initialized".to_string(),
                source: None,
            })?;
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "vindex metadata not initialized".to_string(),
                source: None,
            })?;

        search_vindex(reader, metadata, &self.options, vector_search)
    }

    fn ensure_loaded<S, F>(
        &mut self,
        stream_fn: impl FnOnce(&str) -> crate::Result<S>,
        validate: F,
    ) -> crate::Result<()>
    where
        S: SeekRead + 'static,
        F: FnOnce(&VectorIndexMetadata) -> crate::Result<()>,
    {
        self.ensure_loaded_with_optimizer(stream_fn, validate, |reader| {
            reader
                .optimize_for_search()
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("Failed to optimize paimon-vindex-core reader: {}", e),
                    source: Some(Box::new(e)),
                })
        })
    }

    fn ensure_loaded_with_optimizer<S, F, O>(
        &mut self,
        stream_fn: impl FnOnce(&str) -> crate::Result<S>,
        validate: F,
        optimize: O,
    ) -> crate::Result<()>
    where
        S: SeekRead + 'static,
        F: FnOnce(&VectorIndexMetadata) -> crate::Result<()>,
        O: FnOnce(&mut VIndexReader<VindexInput>) -> crate::Result<()>,
    {
        if self.reader.is_some() {
            self.load_timing = VindexLoadTiming::default();
            return validate(self.metadata()?);
        }

        let open_start = self.timing_enabled.then(Instant::now);
        let source = stream_fn(&self.io_meta.file_path)?;
        let mut reader = VIndexReader::open(VindexInput::new(source)).map_err(|e| {
            crate::Error::DataInvalid {
                message: format!("Failed to open paimon-vindex-core reader: {}", e),
                source: Some(Box::new(e)),
            }
        })?;
        let vindex_open = open_start.map_or(Duration::ZERO, |start| start.elapsed());
        let metadata_start = self.timing_enabled.then(Instant::now);
        let metadata = reader.metadata();
        let metadata_elapsed = metadata_start.map_or(Duration::ZERO, |start| start.elapsed());
        validate(&metadata)?;
        let optimize_start = self.timing_enabled.then(Instant::now);
        optimize(&mut reader)?;
        let optimize_elapsed = optimize_start.map_or(Duration::ZERO, |start| start.elapsed());

        self.reader = Some(reader);
        self.metadata = Some(metadata);
        self.load_timing = VindexLoadTiming {
            vindex_open,
            metadata: metadata_elapsed,
            optimize: optimize_elapsed,
        };
        Ok(())
    }
}

#[cfg(test)]
fn search_vindex(
    reader: &mut VIndexReader<impl SeekRead>,
    metadata: &VectorIndexMetadata,
    options: &HashMap<String, String>,
    vector_search: &VectorSearch,
) -> crate::Result<Option<HashMap<u64, f32>>> {
    let Some(prepared) = prepare_search(metadata, options, vector_search)? else {
        return Ok(None);
    };
    let (labels, distances) = execute_scalar_search(reader, vector_search, &prepared)?;
    let id_to_scores = collect_results(&labels, &distances, prepared.top_k, metadata.metric);
    if id_to_scores.is_empty() {
        return Ok(None);
    }

    Ok(Some(id_to_scores))
}

#[derive(Clone, PartialEq, Eq)]
struct PreparedSearch {
    top_k: usize,
    nprobe: usize,
    filter_bytes: Option<Vec<u8>>,
}

fn prepare_search(
    metadata: &VectorIndexMetadata,
    options: &HashMap<String, String>,
    vector_search: &VectorSearch,
) -> crate::Result<Option<PreparedSearch>> {
    if vector_search.vector.len() != metadata.dimension {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "Query vector dimension mismatch: index expects {}, but got {}",
                metadata.dimension,
                vector_search.vector.len()
            ),
            source: None,
        });
    }

    let count = usize::try_from(metadata.total_vectors).unwrap_or(0);
    let mut top_k = vector_search.limit.min(count);
    if top_k == 0 {
        return Ok(None);
    }
    let nprobe = int_parameter(options, NPROBE_PARAMETER, DEFAULT_NPROBE)?;

    let filter_bytes = if let Some(include_ids) = &vector_search.include_row_ids {
        if include_ids.is_empty() {
            return Ok(None);
        }
        top_k = top_k.min(include_ids.len() as usize);
        let mut bytes = Vec::new();
        include_ids
            .serialize_into(&mut bytes)
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to serialize vector search row-id filter: {}", e),
                source: Some(Box::new(e)),
            })?;
        Some(bytes)
    } else {
        None
    };

    Ok(Some(PreparedSearch {
        top_k,
        nprobe,
        filter_bytes,
    }))
}

fn execute_scalar_search(
    reader: &mut VIndexReader<impl SeekRead>,
    vector_search: &VectorSearch,
    prepared: &PreparedSearch,
) -> crate::Result<(Vec<i64>, Vec<f32>)> {
    let params = VectorSearchParams::new(prepared.top_k, prepared.nprobe);
    match &prepared.filter_bytes {
        Some(filter) => reader
            .search_with_roaring_filter(&vector_search.vector, params, filter)
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("paimon-vindex-core filtered search failed: {}", e),
                source: Some(Box::new(e)),
            }),
        None => {
            reader
                .search(&vector_search.vector, params)
                .map_err(|e| crate::Error::DataInvalid {
                    message: format!("paimon-vindex-core search failed: {}", e),
                    source: Some(Box::new(e)),
                })
        }
    }
}

fn search_batch_vindex(
    reader: &mut VIndexReader<impl SeekRead>,
    metadata: &VectorIndexMetadata,
    options: &HashMap<String, String>,
    vector_searches: &[VectorSearch],
    index_parallelism: usize,
    timing_enabled: bool,
) -> crate::Result<VindexBatchSearchResult> {
    let mut results: Vec<Option<HashMap<u64, f32>>> =
        (0..vector_searches.len()).map(|_| None).collect();
    let mut groups: Vec<(PreparedSearch, Vec<usize>)> = Vec::new();
    let mut batch_stats = timing_enabled.then(|| VindexBatchStats {
        memory_budget_bytes: native_batch_memory_reservation(index_parallelism),
        batch_index_parallelism: index_parallelism,
        ..VindexBatchStats::default()
    });

    for (index, search) in vector_searches.iter().enumerate() {
        let Some(prepared) = prepare_search(metadata, options, search)? else {
            continue;
        };
        if let Some((_, indices)) = groups.iter_mut().find(|(key, _)| key == &prepared) {
            indices.push(index);
        } else {
            groups.push((prepared, vec![index]));
        }
    }

    for (prepared, indices) in groups {
        let chunk_size = native_batch_chunk_size(metadata, &prepared, index_parallelism);
        if let Some(stats) = &mut batch_stats {
            stats.max_chunk_size = stats.max_chunk_size.max(chunk_size);
        }
        for indices in indices.chunks(chunk_size) {
            if indices.len() == 1 {
                if let Some(stats) = &mut batch_stats {
                    stats.scalar_chunk_count += 1;
                }
                let index = indices[0];
                let (labels, distances) =
                    execute_scalar_search(reader, &vector_searches[index], &prepared)?;
                let map = collect_results(&labels, &distances, prepared.top_k, metadata.metric);
                if !map.is_empty() {
                    results[index] = Some(map);
                }
                continue;
            }
            if let Some(stats) = &mut batch_stats {
                stats.native_chunk_queries.push(indices.len());
            }

            let reservation =
                native_batch_chunk_working_set_bytes(metadata, &prepared, indices.len());
            debug_assert!(reservation <= native_batch_memory_reservation(index_parallelism));
            let _memory_permit = acquire_native_batch_memory(reservation);
            let mut queries = Vec::with_capacity(indices.len() * metadata.dimension);
            for &index in indices {
                queries.extend_from_slice(&vector_searches[index].vector);
            }
            let params = VectorSearchParams::new(prepared.top_k, prepared.nprobe);
            let (labels, distances) = match &prepared.filter_bytes {
                Some(filter) => reader
                    .search_batch_with_roaring_filter(&queries, indices.len(), params, filter)
                    .map_err(|e| crate::Error::DataInvalid {
                        message: format!("paimon-vindex-core filtered batch search failed: {}", e),
                        source: Some(Box::new(e)),
                    })?,
                None => reader
                    .search_batch(&queries, indices.len(), params)
                    .map_err(|e| crate::Error::DataInvalid {
                        message: format!("paimon-vindex-core batch search failed: {}", e),
                        source: Some(Box::new(e)),
                    })?,
            };
            let expected = indices.len() * prepared.top_k;
            if labels.len() != expected || distances.len() != expected {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "paimon-vindex-core batch search returned labels/distances of length {}/{}, expected {expected}",
                        labels.len(),
                        distances.len()
                    ),
                    source: None,
                });
            }
            for (query_index, &result_index) in indices.iter().enumerate() {
                let start = query_index * prepared.top_k;
                let end = start + prepared.top_k;
                let map = collect_results(
                    &labels[start..end],
                    &distances[start..end],
                    prepared.top_k,
                    metadata.metric,
                );
                if !map.is_empty() {
                    results[result_index] = Some(map);
                }
            }
        }
    }

    Ok((results, batch_stats))
}

fn native_batch_chunk_size(
    metadata: &VectorIndexMetadata,
    prepared: &PreparedSearch,
    index_parallelism: usize,
) -> usize {
    let per_index_budget = native_batch_memory_reservation(index_parallelism);
    let filter_bytes = native_batch_filter_working_set_bytes(prepared);
    let query_budget = per_index_budget.saturating_sub(filter_bytes);
    query_budget
        .checked_div(native_batch_query_working_set_bytes(metadata, prepared))
        .unwrap_or(0)
        .max(1)
}

fn native_batch_chunk_working_set_bytes(
    metadata: &VectorIndexMetadata,
    prepared: &PreparedSearch,
    query_count: usize,
) -> usize {
    native_batch_filter_working_set_bytes(prepared).saturating_add(
        query_count.saturating_mul(native_batch_query_working_set_bytes(metadata, prepared)),
    )
}

fn native_batch_filter_working_set_bytes(prepared: &PreparedSearch) -> usize {
    prepared
        .filter_bytes
        .as_ref()
        .map_or(0, |filter| filter.len().saturating_mul(2))
}

fn native_batch_query_working_set_bytes(
    metadata: &VectorIndexMetadata,
    prepared: &PreparedSearch,
) -> usize {
    let query_vectors = metadata
        .dimension
        .saturating_mul(std::mem::size_of::<f32>() * 2);
    let centroid_products = metadata.nlist.saturating_mul(std::mem::size_of::<f32>());
    let probe_results = prepared
        .nprobe
        .min(metadata.nlist)
        .saturating_mul(std::mem::size_of::<usize>() + std::mem::size_of::<f32>());
    let top_k_results = prepared.top_k.saturating_mul(
        std::mem::size_of::<i64>() + std::mem::size_of::<f32>() + std::mem::size_of::<(f32, i64)>(),
    );
    let pq_tables = match (metadata.pq_m, metadata.pq_bits) {
        (Some(m), Some(bits)) => 1usize
            .checked_shl(bits as u32)
            .unwrap_or(usize::MAX)
            .saturating_mul(m)
            .saturating_mul(std::mem::size_of::<f32>()),
        _ => 0,
    };

    query_vectors
        .saturating_add(centroid_products)
        .saturating_add(probe_results)
        .saturating_add(top_k_results)
        .saturating_add(pq_tables)
        .saturating_add(256)
        .max(1)
}

fn collect_results(
    labels: &[i64],
    distances: &[f32],
    top_k: usize,
    metric: MetricType,
) -> HashMap<u64, f32> {
    #[derive(PartialEq)]
    struct ScoredRow {
        row_id: u64,
        score: f32,
    }
    impl Eq for ScoredRow {}
    impl PartialOrd for ScoredRow {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Ord for ScoredRow {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            other.score.total_cmp(&self.score)
        }
    }

    let mut min_heap: BinaryHeap<ScoredRow> = BinaryHeap::with_capacity(top_k + 1);
    for (&row_id, &distance) in labels.iter().zip(distances.iter()) {
        if row_id < 0 {
            continue;
        }
        let score = convert_distance_to_score(distance, metric);
        let row_id = row_id as u64;
        if min_heap.len() < top_k {
            min_heap.push(ScoredRow { row_id, score });
        } else if let Some(peek) = min_heap.peek() {
            if score > peek.score {
                min_heap.pop();
                min_heap.push(ScoredRow { row_id, score });
            }
        }
    }

    let mut result = HashMap::with_capacity(min_heap.len());
    for entry in min_heap {
        result.insert(entry.row_id, entry.score);
    }
    result
}

fn convert_distance_to_score(distance: f32, metric: MetricType) -> f32 {
    match metric {
        MetricType::L2 => 1.0 / (1.0 + distance),
        MetricType::Cosine => 1.0 - distance,
        MetricType::InnerProduct => -distance,
    }
}

fn int_parameter(
    options: &HashMap<String, String>,
    key: &str,
    default_value: usize,
) -> crate::Result<usize> {
    match options.get(key) {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| crate::Error::DataInvalid {
                message: format!(
                    "Invalid value for '{}': {}. Must be a non-negative integer.",
                    key, value
                ),
                source: None,
            }),
        None => Ok(default_value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileRead;
    use crate::vindex::range_reader::VindexFileReader;
    use async_trait::async_trait;
    use bytes::Bytes;
    use paimon_vindex_core::index::{VectorIndexConfig, VectorIndexTrainer, VectorIndexWriter};
    use paimon_vindex_core::io::{PosWriter, SeekReadCapabilities};
    use std::cell::Cell;
    use std::io::Cursor;
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    const TEST_DIMENSION: usize = 16;

    #[derive(Clone)]
    struct CloneableSeekRead {
        capabilities: SeekReadCapabilities,
    }

    impl SeekRead for CloneableSeekRead {
        fn pread(&mut self, _ranges: &mut [ReadRequest<'_>]) -> io::Result<()> {
            Ok(())
        }

        fn try_clone_reader(&self) -> io::Result<Option<Self>> {
            Ok(Some(self.clone()))
        }

        fn read_capabilities(&self) -> SeekReadCapabilities {
            self.capabilities
        }
    }

    struct TrackingIndexRead {
        data: Bytes,
        ranges: Mutex<Vec<Range<u64>>>,
        bytes_read: AtomicUsize,
    }

    impl TrackingIndexRead {
        fn new(data: Bytes) -> Arc<Self> {
            Arc::new(Self {
                data,
                ranges: Mutex::new(Vec::new()),
                bytes_read: AtomicUsize::new(0),
            })
        }

        fn ranges(&self) -> Vec<Range<u64>> {
            self.ranges.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl FileRead for TrackingIndexRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            self.bytes_read
                .fetch_add((range.end - range.start) as usize, Ordering::SeqCst);
            self.ranges.lock().unwrap().push(range.clone());
            Ok(self.data.slice(range.start as usize..range.end as usize))
        }
    }

    fn build_ivf_flat_index() -> Bytes {
        let vector_count = 8192usize;
        let mut vectors = Vec::with_capacity(vector_count * TEST_DIMENSION);
        for row in 0..vector_count {
            let cluster = (row % 16) as f32 * 100.0;
            for dimension in 0..TEST_DIMENSION {
                vectors.push(cluster + dimension as f32 * 0.01 + row as f32 * 0.000001);
            }
        }
        let ids: Vec<i64> = (0..vector_count as i64).collect();
        let options = HashMap::from([
            ("index.type".to_string(), "ivf_flat".to_string()),
            ("dimension".to_string(), TEST_DIMENSION.to_string()),
            ("nlist".to_string(), "16".to_string()),
            ("metric".to_string(), "l2".to_string()),
        ]);
        let config = VectorIndexConfig::from_options(&options).unwrap();
        let training = VectorIndexTrainer::train(config, &vectors, vector_count).unwrap();
        let mut writer = VectorIndexWriter::new(training);
        writer.add_vectors(&ids, &vectors, vector_count).unwrap();
        let mut output = Vec::new();
        writer.write(&mut PosWriter::new(&mut output)).unwrap();
        Bytes::from(output)
    }

    fn query_for_cluster(cluster: usize, limit: usize) -> VectorSearch {
        let cluster = cluster as f32 * 100.0;
        VectorSearch::new(
            (0..TEST_DIMENSION)
                .map(|dimension| cluster + dimension as f32 * 0.01)
                .collect(),
            limit,
            "embedding".to_string(),
        )
        .unwrap()
    }

    fn query() -> VectorSearch {
        query_for_cluster(0, 10)
    }

    async fn tracked_batch_search(
        index: Bytes,
        query_count: usize,
    ) -> (Vec<Option<HashMap<u64, f32>>>, usize) {
        tracked_batch_search_with_options(
            index,
            query_count,
            HashMap::from([(NPROBE_PARAMETER.to_string(), "1".to_string())]),
            1,
        )
        .await
    }

    async fn tracked_batch_search_with_options(
        index: Bytes,
        query_count: usize,
        options: HashMap<String, String>,
        index_parallelism: usize,
    ) -> (Vec<Option<HashMap<u64, f32>>>, usize) {
        let tracking = TrackingIndexRead::new(index.clone());
        let source: Arc<dyn FileRead> = tracking.clone();
        let runtime = tokio::runtime::Handle::current();
        let results = tokio::task::spawn_blocking(move || {
            let source = VindexFileReader::new(
                source,
                runtime,
                index.len() as u64,
                "batch.index".to_string(),
            );
            let io_meta =
                GlobalIndexIOMeta::new("batch.index".to_string(), index.len() as u64, Vec::new());
            let searches = vec![query(); query_count];
            let mut reader = VindexVectorGlobalIndexReader::new(io_meta, options)
                .with_batch_index_parallelism(index_parallelism);
            reader
                .visit_batch_vector_search(&searches, |_| Ok(source))
                .unwrap()
        })
        .await
        .unwrap();
        (results, tracking.bytes_read.load(Ordering::SeqCst))
    }

    #[test]
    fn test_convert_distance_to_score() {
        assert_eq!(convert_distance_to_score(0.0, MetricType::L2), 1.0);
        assert_eq!(convert_distance_to_score(1.0, MetricType::L2), 0.5);
        assert_eq!(convert_distance_to_score(0.0, MetricType::Cosine), 1.0);
        assert_eq!(convert_distance_to_score(1.0, MetricType::Cosine), 0.0);
        assert_eq!(
            convert_distance_to_score(-0.75, MetricType::InnerProduct),
            0.75
        );
    }

    #[test]
    fn test_collect_results_converts_inner_product_distance_to_similarity() {
        let labels = vec![9, 5, 1];
        let distances = vec![-0.9, -0.5, -0.1];

        let result = collect_results(&labels, &distances, 2, MetricType::InnerProduct);

        assert_eq!(result.len(), 2);
        assert!(result.contains_key(&9), "0.9 similarity should be retained");
        assert!(result.contains_key(&5), "0.5 similarity should be retained");
        assert!(!result.contains_key(&1), "0.1 similarity should be trimmed");
        assert_eq!(result.get(&9), Some(&0.9));
        assert_eq!(result.get(&5), Some(&0.5));
    }

    #[test]
    fn test_collect_results_skips_negative_labels() {
        let labels = vec![0, -1, 2, 3];
        let distances = vec![0.5, 0.0, 0.1, 0.9];
        let result = collect_results(&labels, &distances, 2, MetricType::L2);
        assert_eq!(result.len(), 2);
        assert!(result.contains_key(&2));
        assert!(result.contains_key(&0));
        assert!(!result.contains_key(&3));
    }

    #[test]
    fn native_batch_chunk_size_tracks_working_set_inputs() {
        let base_metadata = VectorIndexMetadata {
            index_type: paimon_vindex_core::index::IndexType::IvfFlat,
            dimension: 128,
            nlist: 256,
            metric: MetricType::L2,
            total_vectors: 8192,
            pq_m: None,
            pq_bits: None,
            rq_bits: None,
            diskann: None,
        };
        let base_prepared = PreparedSearch {
            top_k: 10,
            nprobe: 16,
            filter_bytes: None,
        };
        let index_parallelism = 32;
        let base = native_batch_chunk_size(&base_metadata, &base_prepared, index_parallelism);

        let mut larger_index = base_metadata.clone();
        larger_index.dimension *= 2;
        larger_index.nlist *= 2;
        assert!(native_batch_chunk_size(&larger_index, &base_prepared, index_parallelism) < base);

        let mut larger_top_k = base_prepared.clone();
        larger_top_k.top_k *= 4;
        assert!(native_batch_chunk_size(&base_metadata, &larger_top_k, index_parallelism) < base);

        let mut pq_metadata = base_metadata.clone();
        pq_metadata.pq_m = Some(64);
        pq_metadata.pq_bits = Some(8);
        assert!(native_batch_chunk_size(&pq_metadata, &base_prepared, index_parallelism) < base);

        let per_index_working_set = base.saturating_mul(native_batch_query_working_set_bytes(
            &base_metadata,
            &base_prepared,
        ));
        assert!(
            per_index_working_set.saturating_mul(index_parallelism)
                <= NATIVE_BATCH_PROCESS_WORKING_SET_BYTES
        );
        for parallelism in [1, 2, 3, 32, 64] {
            assert!(
                native_batch_memory_reservation(parallelism).saturating_mul(parallelism)
                    <= NATIVE_BATCH_PROCESS_WORKING_SET_BYTES
            );
        }
    }

    #[test]
    fn native_batch_chunk_reservation_tracks_actual_chunk() {
        let metadata = VectorIndexMetadata {
            index_type: paimon_vindex_core::index::IndexType::IvfFlat,
            dimension: 128,
            nlist: 256,
            metric: MetricType::L2,
            total_vectors: 8192,
            pq_m: None,
            pq_bits: None,
            rq_bits: None,
            diskann: None,
        };
        let prepared = PreparedSearch {
            top_k: 10,
            nprobe: 16,
            filter_bytes: Some(vec![0; 128]),
        };
        let chunk_size = native_batch_chunk_size(&metadata, &prepared, 1);
        let full_chunk = native_batch_chunk_working_set_bytes(&metadata, &prepared, chunk_size);
        let final_chunk = native_batch_chunk_working_set_bytes(&metadata, &prepared, 2);

        assert!(chunk_size > 2);
        assert!(full_chunk <= native_batch_memory_reservation(1));
        assert!(final_chunk < full_chunk);
    }

    #[test]
    fn native_batch_memory_pool_admits_only_available_bytes() {
        let pool = NativeBatchMemoryPool::new(64);
        let large = pool.acquire(48);

        std::thread::scope(|scope| {
            let pool = &pool;
            let (fits_tx, fits_rx) = std::sync::mpsc::channel();
            scope.spawn(move || {
                let _permit = pool.acquire(16);
                fits_tx.send(()).unwrap();
            });
            fits_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("reservation fitting the available bytes should not wait");

            let (blocked_tx, blocked_rx) = std::sync::mpsc::channel();
            scope.spawn(move || {
                let _permit = pool.acquire(17);
                blocked_tx.send(()).unwrap();
            });
            assert!(
                blocked_rx
                    .recv_timeout(std::time::Duration::from_millis(50))
                    .is_err(),
                "reservation exceeding the available bytes should wait"
            );

            drop(large);
            blocked_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("waiting reservation should proceed after bytes are released");
        });
    }

    #[test]
    fn native_batch_memory_pool_oversized_request_occupies_pool() {
        let pool = NativeBatchMemoryPool::new(64);

        std::thread::scope(|scope| {
            let pool = &pool;
            let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
            scope.spawn(move || {
                let permit = pool.acquire(65);
                let _ = acquired_tx.send(permit.bytes);
            });
            let acquired = acquired_rx.recv_timeout(std::time::Duration::from_secs(1));
            if acquired.is_err() {
                // Unblock the old behavior so a regression fails instead of hanging the test.
                *pool.available_bytes.lock().unwrap() = 65;
                pool.memory_available.notify_all();
            }
            assert_eq!(acquired.unwrap(), 64);
        });
    }

    #[test]
    fn test_int_parameter() {
        let mut options = HashMap::new();
        options.insert(NPROBE_PARAMETER.to_string(), "32".to_string());

        assert_eq!(
            int_parameter(&options, NPROBE_PARAMETER, DEFAULT_NPROBE).unwrap(),
            32
        );
        options.insert(NPROBE_PARAMETER.to_string(), "abc".to_string());
        assert!(int_parameter(&options, NPROBE_PARAMETER, DEFAULT_NPROBE).is_err());
    }

    #[test]
    fn erased_input_forwards_clone_and_capabilities() {
        let capabilities = SeekReadCapabilities {
            estimated_random_read_latency_nanos: 123,
            preferred_window_bytes: 64 * 1024,
            max_ranges_per_pread: 7,
        };
        let input = VindexInput::new(CloneableSeekRead { capabilities });

        assert_eq!(input.read_capabilities(), capabilities);
        let cloned = input.try_clone_reader().unwrap().unwrap();
        assert_eq!(cloned.read_capabilities(), capabilities);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scalar_search_range_reads_instead_of_loading_the_whole_index() {
        let index = build_ivf_flat_index();
        let tracking = TrackingIndexRead::new(index.clone());
        let source: Arc<dyn FileRead> = tracking.clone();
        let index_size = index.len();
        let runtime = tokio::runtime::Handle::current();

        let result = tokio::task::spawn_blocking(move || {
            let source = VindexFileReader::new(
                source,
                runtime,
                index_size as u64,
                "scalar.index".to_string(),
            );
            let io_meta =
                GlobalIndexIOMeta::new("scalar.index".to_string(), index_size as u64, Vec::new());
            let options = HashMap::from([(NPROBE_PARAMETER.to_string(), "1".to_string())]);
            let mut reader = VindexVectorGlobalIndexReader::new(io_meta, options);
            reader.visit_vector_search(&query(), |_| Ok(source))
        })
        .await
        .unwrap()
        .unwrap();

        assert!(result.is_some());
        let bytes_read = tracking.bytes_read.load(Ordering::SeqCst);
        assert!(
            bytes_read < index_size / 2,
            "nprobe=1 should read substantially less than the full index: read={bytes_read}, file={index_size}"
        );
        assert!(
            tracking
                .ranges()
                .iter()
                .all(|range| range.start != 0 || range.end != index_size as u64),
            "range search unexpectedly read the entire index"
        );
    }

    #[test]
    fn mixed_batch_matches_scalar_searches_and_preserves_order() {
        let index = build_ivf_flat_index();
        let options = HashMap::from([(NPROBE_PARAMETER.to_string(), "1".to_string())]);
        let mut include_row_ids = roaring::RoaringTreemap::new();
        include_row_ids.insert(0);
        include_row_ids.insert(16);
        include_row_ids.insert(32);
        let searches = vec![
            query_for_cluster(0, 4),
            query_for_cluster(5, 1),
            query_for_cluster(15, 4),
            query_for_cluster(0, 10).with_include_row_ids(include_row_ids),
        ];

        let scalar_meta =
            GlobalIndexIOMeta::new("scalar.index".to_string(), index.len() as u64, Vec::new());
        let mut scalar_reader = VindexVectorGlobalIndexReader::new(scalar_meta, options.clone());
        scalar_reader
            .load(|_| Ok(Cursor::new(index.clone())))
            .unwrap();
        let expected: Vec<_> = searches
            .iter()
            .map(|search| scalar_reader.search(search).unwrap())
            .collect();

        let batch_meta =
            GlobalIndexIOMeta::new("batch.index".to_string(), index.len() as u64, Vec::new());
        let mut batch_reader = VindexVectorGlobalIndexReader::new(batch_meta, options);
        let actual = batch_reader
            .visit_batch_vector_search(&searches, |_| Ok(Cursor::new(index)))
            .unwrap();

        assert_eq!(actual, expected);
        assert_ne!(
            actual[0], actual[2],
            "interleaved batch groups lost query order"
        );
        assert_eq!(actual[1].as_ref().map(HashMap::len), Some(1));
        assert_eq!(actual[3].as_ref().map(HashMap::len), Some(3));
    }

    #[test]
    fn metadata_validation_runs_before_optimization() {
        let index = build_ivf_flat_index();
        let io_meta = GlobalIndexIOMeta::new(
            "validated.index".to_string(),
            index.len() as u64,
            Vec::new(),
        );
        let mut reader = VindexVectorGlobalIndexReader::new(io_meta, HashMap::new());
        let optimized = Cell::new(false);

        let error = reader
            .ensure_loaded_with_optimizer(
                |_| Ok(Cursor::new(index)),
                |metadata| {
                    assert_eq!(metadata.dimension, TEST_DIMENSION);
                    Err(crate::Error::DataInvalid {
                        message: "rejected test metric".to_string(),
                        source: None,
                    })
                },
                |_| {
                    optimized.set(true);
                    Ok(())
                },
            )
            .unwrap_err();

        assert!(error.to_string().contains("rejected test metric"));
        assert!(!optimized.get());
        assert!(reader.metadata().is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_search_reuses_probed_lists_and_avoids_full_file_read() {
        let index = build_ivf_flat_index();
        let batch_index = index.clone();
        let options = HashMap::from([(NPROBE_PARAMETER.to_string(), "1".to_string())]);
        let search = query();

        let scalar_tracking = TrackingIndexRead::new(index.clone());
        let scalar_source: Arc<dyn FileRead> = scalar_tracking.clone();
        let scalar_options = options.clone();
        let scalar_search = search.clone();
        let scalar_runtime = tokio::runtime::Handle::current();
        let scalar_results = tokio::task::spawn_blocking(move || {
            let source = VindexFileReader::new(
                scalar_source,
                scalar_runtime,
                index.len() as u64,
                "scalar.index".to_string(),
            );
            let io_meta =
                GlobalIndexIOMeta::new("scalar.index".to_string(), index.len() as u64, Vec::new());
            let mut reader = VindexVectorGlobalIndexReader::new(io_meta, scalar_options);
            reader.load(|_| Ok(source)).unwrap();
            vec![
                reader.search(&scalar_search).unwrap(),
                reader.search(&scalar_search).unwrap(),
            ]
        })
        .await
        .unwrap();

        let batch_tracking = TrackingIndexRead::new(batch_index.clone());
        let batch_source: Arc<dyn FileRead> = batch_tracking.clone();
        let batch_options = options;
        let batch_search = search.clone();
        let batch_runtime = tokio::runtime::Handle::current();
        let batch_results = tokio::task::spawn_blocking(move || {
            let source = VindexFileReader::new(
                batch_source,
                batch_runtime,
                batch_index.len() as u64,
                "batch.index".to_string(),
            );
            let io_meta = GlobalIndexIOMeta::new(
                "batch.index".to_string(),
                batch_index.len() as u64,
                Vec::new(),
            );
            let mut reader = VindexVectorGlobalIndexReader::new(io_meta, batch_options);
            reader
                .visit_batch_vector_search(&[batch_search.clone(), batch_search], |_| Ok(source))
                .unwrap()
        })
        .await
        .unwrap();

        assert_eq!(batch_results, scalar_results);
        let scalar_bytes = scalar_tracking.bytes_read.load(Ordering::SeqCst);
        let batch_bytes = batch_tracking.bytes_read.load(Ordering::SeqCst);
        assert!(
            batch_bytes < scalar_bytes,
            "batch should read a shared probed list once: batch={batch_bytes}, scalar={scalar_bytes}"
        );
        assert!(
            batch_bytes < batch_tracking.data.len() / 2,
            "nprobe=1 should read substantially less than the full index: read={batch_bytes}, file={} ",
            batch_tracking.data.len()
        );
        assert!(batch_tracking
            .ranges()
            .iter()
            .all(|range| { range.start != 0 || range.end != batch_tracking.data.len() as u64 }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn filtered_batch_matches_scalar_searches_and_reuses_probed_lists() {
        let index = build_ivf_flat_index();
        let batch_index = index.clone();
        let options = HashMap::from([(NPROBE_PARAMETER.to_string(), "1".to_string())]);
        let mut include_row_ids = roaring::RoaringTreemap::new();
        for row_id in (0..256).step_by(16) {
            include_row_ids.insert(row_id);
        }
        let searches = vec![
            query().with_include_row_ids(include_row_ids.clone()),
            query().with_include_row_ids(include_row_ids),
        ];

        let scalar_tracking = TrackingIndexRead::new(index.clone());
        let scalar_source: Arc<dyn FileRead> = scalar_tracking.clone();
        let scalar_options = options.clone();
        let scalar_searches = searches.clone();
        let scalar_runtime = tokio::runtime::Handle::current();
        let scalar_results = tokio::task::spawn_blocking(move || {
            let source = VindexFileReader::new(
                scalar_source,
                scalar_runtime,
                index.len() as u64,
                "scalar-filtered.index".to_string(),
            );
            let io_meta = GlobalIndexIOMeta::new(
                "scalar-filtered.index".to_string(),
                index.len() as u64,
                Vec::new(),
            );
            let mut reader = VindexVectorGlobalIndexReader::new(io_meta, scalar_options);
            reader.load(|_| Ok(source)).unwrap();
            scalar_searches
                .iter()
                .map(|search| reader.search(search).unwrap())
                .collect::<Vec<_>>()
        })
        .await
        .unwrap();

        let batch_tracking = TrackingIndexRead::new(batch_index.clone());
        let batch_source: Arc<dyn FileRead> = batch_tracking.clone();
        let batch_runtime = tokio::runtime::Handle::current();
        let batch_results = tokio::task::spawn_blocking(move || {
            let source = VindexFileReader::new(
                batch_source,
                batch_runtime,
                batch_index.len() as u64,
                "batch-filtered.index".to_string(),
            );
            let io_meta = GlobalIndexIOMeta::new(
                "batch-filtered.index".to_string(),
                batch_index.len() as u64,
                Vec::new(),
            );
            let mut reader = VindexVectorGlobalIndexReader::new(io_meta, options);
            reader
                .visit_batch_vector_search(&searches, |_| Ok(source))
                .unwrap()
        })
        .await
        .unwrap();

        assert_eq!(batch_results, scalar_results);
        let scalar_bytes = scalar_tracking.bytes_read.load(Ordering::SeqCst);
        let batch_bytes = batch_tracking.bytes_read.load(Ordering::SeqCst);
        assert!(
            batch_bytes < scalar_bytes,
            "filtered batch should read a shared probed list once: batch={batch_bytes}, scalar={scalar_bytes}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn large_homogeneous_batch_reuses_lists_across_previous_boundary() {
        let previous_batch_boundary = 16;
        let index = build_ivf_flat_index();
        let (within_limit_results, within_limit_bytes) =
            tracked_batch_search(index.clone(), previous_batch_boundary).await;
        let (over_limit_results, over_limit_bytes) =
            tracked_batch_search(index, previous_batch_boundary + 1).await;

        assert_eq!(within_limit_results.len(), previous_batch_boundary);
        assert_eq!(over_limit_results.len(), previous_batch_boundary + 1);
        assert!(within_limit_results
            .iter()
            .all(|result| result == &within_limit_results[0]));
        assert!(over_limit_results
            .iter()
            .all(|result| result == &over_limit_results[0]));
        assert_eq!(over_limit_results[0], within_limit_results[0]);
        assert_eq!(
            over_limit_bytes, within_limit_bytes,
            "compatible queries should reuse the same probed list across the former 16-query boundary"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn homogeneous_batch_chunks_at_working_set_boundary() {
        let index_parallelism = 4096;
        let metadata = VectorIndexMetadata {
            index_type: paimon_vindex_core::index::IndexType::IvfFlat,
            dimension: TEST_DIMENSION,
            nlist: 16,
            metric: MetricType::L2,
            total_vectors: 8192,
            pq_m: None,
            pq_bits: None,
            rq_bits: None,
            diskann: None,
        };
        let options = HashMap::from([
            (NPROBE_PARAMETER.to_string(), "1".to_string()),
            ("global-index.thread-num".to_string(), "1".to_string()),
        ]);
        let prepared = prepare_search(&metadata, &options, &query())
            .unwrap()
            .unwrap();
        let chunk_size = native_batch_chunk_size(&metadata, &prepared, index_parallelism);
        assert!(chunk_size > 16);

        let index = build_ivf_flat_index();
        let (within_results, within_bytes) = tracked_batch_search_with_options(
            index.clone(),
            chunk_size,
            options.clone(),
            index_parallelism,
        )
        .await;
        let (over_results, over_bytes) =
            tracked_batch_search_with_options(index, chunk_size + 1, options, index_parallelism)
                .await;

        assert_eq!(within_results.len(), chunk_size);
        assert_eq!(over_results.len(), chunk_size + 1);
        assert!(within_results
            .iter()
            .all(|result| result == &within_results[0]));
        assert!(over_results.iter().all(|result| result == &over_results[0]));
        assert_eq!(over_results[0], within_results[0]);
        assert!(over_bytes > within_bytes);
    }
}
