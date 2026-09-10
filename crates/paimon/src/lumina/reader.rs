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

use crate::lumina::ffi::LuminaSearcher;
use crate::lumina::{strip_lumina_options, LuminaIndexMeta, LuminaVectorMetric};
use crate::vector_search::{GlobalIndexIOMeta, VectorSearch};
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

const MIN_SEARCH_LIST_SIZE: usize = 16;
// C ABI returns int64_t -1 for invalid results, which casts to u64::MAX in Rust.
const SENTINEL: u64 = u64::MAX;

trait LuminaSearch {
    fn search(
        &self,
        query: &[f32],
        n: i32,
        k: i32,
        distances: &mut [f32],
        labels: &mut [u64],
        options: &HashMap<String, String>,
    ) -> crate::Result<()>;

    #[allow(clippy::too_many_arguments)]
    fn search_with_filter(
        &self,
        query: &[f32],
        n: i32,
        k: i32,
        distances: &mut [f32],
        labels: &mut [u64],
        filter_ids: &[u64],
        options: &HashMap<String, String>,
    ) -> crate::Result<()>;

    fn get_count(&self) -> crate::Result<u64>;
}

impl LuminaSearch for LuminaSearcher {
    fn search(
        &self,
        query: &[f32],
        n: i32,
        k: i32,
        distances: &mut [f32],
        labels: &mut [u64],
        options: &HashMap<String, String>,
    ) -> crate::Result<()> {
        LuminaSearcher::search(self, query, n, k, distances, labels, options)
    }

    fn search_with_filter(
        &self,
        query: &[f32],
        n: i32,
        k: i32,
        distances: &mut [f32],
        labels: &mut [u64],
        filter_ids: &[u64],
        options: &HashMap<String, String>,
    ) -> crate::Result<()> {
        LuminaSearcher::search_with_filter(
            self, query, n, k, distances, labels, filter_ids, options,
        )
    }

    fn get_count(&self) -> crate::Result<u64> {
        LuminaSearcher::get_count(self)
    }
}

fn ensure_search_list_size(search_options: &mut HashMap<String, String>, top_k: usize) {
    if !search_options.contains_key("diskann.search.list_size") {
        let list_size = std::cmp::max((top_k as f64 * 1.5) as usize, MIN_SEARCH_LIST_SIZE);
        search_options.insert(
            "diskann.search.list_size".to_string(),
            list_size.to_string(),
        );
    }
}

fn convert_distance_to_score(distance: f32, metric: LuminaVectorMetric) -> f32 {
    match metric {
        LuminaVectorMetric::L2 => 1.0 / (1.0 + distance),
        LuminaVectorMetric::Cosine => 1.0 - distance,
        LuminaVectorMetric::InnerProduct => distance,
    }
}

/// Order two search scores, best last, with NaN ranked below every real score.
///
/// This is the score-domain mirror of `vindex::pkvector::metric`'s
/// `java_float_compare`, which ranks a NaN *distance* worst. `f32::total_cmp`
/// alone is unsuitable in either domain: it places a positive NaN above every
/// finite value, so a NaN score -- reachable here because a non-finite stored
/// vector yields a NaN distance and [`convert_distance_to_score`] passes NaN
/// through -- would outrank real neighbours. Both NaN signs lose, and two NaNs
/// compare equal so the caller's row-id tie-break decides between them.
fn compare_scores(a: f32, b: f32) -> std::cmp::Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        (false, false) => a.total_cmp(&b),
    }
}

/// Allocate the label buffer for one native search, filled with [`SENTINEL`].
///
/// [`SENTINEL`] is the "no result" marker (the C ABI's `-1`), and
/// [`collect_results`] drops any slot carrying it. Zero-filling the buffer
/// instead makes that marker unreliable: `0` is a *legal* row id, so a slot the
/// searcher leaves untouched is indistinguishable from a real hit, and it pairs
/// with distance `0.0` -- the best distance both L2 and cosine can report -- so
/// a search returning fewer neighbours than requested would surface row 0 as its
/// top match. The FFI reports only a status code, never how many slots it wrote,
/// so the Rust side cannot detect a short return; allocating the sentinel is
/// correct either way.
fn new_label_buffer(len: usize) -> Vec<u64> {
    vec![SENTINEL; len]
}

/// Post-filter search results to top_k.
fn collect_results(
    labels: &[u64],
    distances: &[f32],
    top_k: usize,
    metric: LuminaVectorMetric,
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
        // Reversed on score so the heap top is the weakest candidate; among
        // equal scores the larger row id sorts first and is therefore evicted
        // first, which keeps the retained set independent of the order the
        // searcher returned the pairs in. Same shape as
        // `vector_search::ScoredRow`, except that scores are compared through
        // `compare_scores` so a NaN cannot claim the strongest slot.
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            compare_scores(other.score, self.score).then_with(|| self.row_id.cmp(&other.row_id))
        }
    }

    impl ScoredRow {
        fn is_stronger_than(&self, other: &Self) -> bool {
            compare_scores(self.score, other.score).then_with(|| other.row_id.cmp(&self.row_id))
                == std::cmp::Ordering::Greater
        }
    }

    let mut min_heap: BinaryHeap<ScoredRow> = BinaryHeap::with_capacity(top_k + 1);
    for (&row_id, &distance) in labels.iter().zip(distances.iter()) {
        if row_id == SENTINEL {
            continue;
        }
        let score = convert_distance_to_score(distance, metric);
        let entry = ScoredRow { row_id, score };
        if min_heap.len() < top_k {
            min_heap.push(entry);
        } else if min_heap
            .peek()
            .is_some_and(|weakest| entry.is_stronger_than(weakest))
        {
            min_heap.pop();
            min_heap.push(entry);
        }
    }

    let mut result = HashMap::with_capacity(min_heap.len());
    for entry in min_heap {
        result.insert(entry.row_id, entry.score);
    }
    result
}

pub struct LuminaVectorGlobalIndexReader {
    io_meta: GlobalIndexIOMeta,
    options: HashMap<String, String>,
    searcher: Option<LuminaSearcher>,
    index_meta: Option<LuminaIndexMeta>,
    search_options: Option<HashMap<String, String>>,
    local_index_file: Option<PathBuf>,
}

impl LuminaVectorGlobalIndexReader {
    pub fn new(io_meta: GlobalIndexIOMeta, options: HashMap<String, String>) -> Self {
        Self {
            io_meta,
            options,
            searcher: None,
            index_meta: None,
            search_options: None,
            local_index_file: None,
        }
    }

    pub fn visit_vector_search<S: Read + Seek + Send + 'static>(
        &mut self,
        vector_search: &VectorSearch,
        stream_fn: impl FnOnce(&str) -> crate::Result<S>,
    ) -> crate::Result<Option<HashMap<u64, f32>>> {
        self.ensure_loaded(stream_fn)?;
        self.search(vector_search)
    }

    pub fn visit_batch_vector_search<S: Read + Seek + Send + 'static>(
        &mut self,
        vector_searches: &[VectorSearch],
        stream_fn: impl FnOnce(&str) -> crate::Result<S>,
    ) -> crate::Result<Vec<Option<HashMap<u64, f32>>>> {
        self.ensure_loaded(stream_fn)?;
        self.search_batch(vector_searches)
    }

    fn search(&self, vector_search: &VectorSearch) -> crate::Result<Option<HashMap<u64, f32>>> {
        let index_meta = self
            .index_meta
            .as_ref()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "index_meta not initialized".to_string(),
                source: None,
            })?;
        let searcher = self
            .searcher
            .as_ref()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "searcher not initialized".to_string(),
                source: None,
            })?;
        let search_options_base =
            self.search_options
                .as_ref()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "search_options not initialized".to_string(),
                    source: None,
                })?;

        search_lumina(searcher, index_meta, search_options_base, vector_search)
    }

    fn search_batch(
        &self,
        vector_searches: &[VectorSearch],
    ) -> crate::Result<Vec<Option<HashMap<u64, f32>>>> {
        let index_meta = self
            .index_meta
            .as_ref()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "index_meta not initialized".to_string(),
                source: None,
            })?;
        let searcher = self
            .searcher
            .as_ref()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "searcher not initialized".to_string(),
                source: None,
            })?;
        let search_options_base =
            self.search_options
                .as_ref()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "search_options not initialized".to_string(),
                    source: None,
                })?;

        search_lumina_batch(searcher, index_meta, search_options_base, vector_searches)
    }

    fn ensure_loaded<S: Read + Seek + Send + 'static>(
        &mut self,
        stream_fn: impl FnOnce(&str) -> crate::Result<S>,
    ) -> crate::Result<()> {
        if self.searcher.is_some() {
            return Ok(());
        }

        let index_meta = LuminaIndexMeta::deserialize(&self.io_meta.metadata)?;

        let mut searcher_options = index_meta.options().clone();
        for (k, v) in strip_lumina_options(&self.options) {
            searcher_options.insert(k, v);
        }

        let mut searcher = LuminaSearcher::create(&searcher_options)?;

        let mut stream = stream_fn(&self.io_meta.file_path)?;
        let local_index_file = write_temp_index_file(&mut stream)?;
        let local_index_path =
            local_index_file
                .to_str()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!(
                        "Temporary Lumina index path is not valid UTF-8: {}",
                        local_index_file.display()
                    ),
                    source: None,
                })?;
        if let Err(err) = searcher.open_file(local_index_path) {
            let _ = std::fs::remove_file(&local_index_file);
            return Err(err);
        }

        self.search_options = Some(searcher_options);
        self.index_meta = Some(index_meta);
        self.searcher = Some(searcher);
        self.local_index_file = Some(local_index_file);
        Ok(())
    }

    pub fn close(&mut self) {
        self.searcher = None;
        self.index_meta = None;
        self.search_options = None;
        if let Some(path) = self.local_index_file.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn search_lumina<S: LuminaSearch + ?Sized>(
    searcher: &S,
    index_meta: &LuminaIndexMeta,
    search_options_base: &HashMap<String, String>,
    vector_search: &VectorSearch,
) -> crate::Result<Option<HashMap<u64, f32>>> {
    let expected_dim = index_meta.dim()? as usize;
    if vector_search.vector.len() != expected_dim {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "Query vector dimension mismatch: index expects {}, but got {}",
                expected_dim,
                vector_search.vector.len()
            ),
            source: None,
        });
    }

    let limit = vector_search.limit;
    // Java's order: `index.size()` decides first, and the metric is parsed only once
    // there is something to search. An empty index has no hits to rank, so reading
    // the metric ahead of the size would report a malformed metric for a query whose
    // answer is empty whatever the metric says.
    let count = searcher.get_count()? as usize;
    let effective_k = std::cmp::min(limit, count);
    if effective_k == 0 {
        return Ok(None);
    }
    let index_metric = index_meta.metric()?;

    let include_row_ids = vector_search.effective_include_row_ids();

    let (distances, labels) = if let Some(include_ids) = include_row_ids {
        let filter_id_list: Vec<u64> = include_ids.iter().collect();
        if filter_id_list.is_empty() {
            return Ok(None);
        }
        let ek = std::cmp::min(effective_k, filter_id_list.len());
        let mut distances = vec![0.0f32; ek];
        let mut labels = new_label_buffer(ek);
        let mut search_opts: HashMap<String, String> = search_options_base.clone();
        search_opts.insert("search.thread_safe_filter".to_string(), "true".to_string());
        ensure_search_list_size(&mut search_opts, ek);
        searcher.search_with_filter(
            &vector_search.vector,
            1,
            ek as i32,
            &mut distances,
            &mut labels,
            &filter_id_list,
            &search_opts,
        )?;
        (distances, labels)
    } else {
        let mut distances = vec![0.0f32; effective_k];
        let mut labels = new_label_buffer(effective_k);
        let mut search_opts: HashMap<String, String> = search_options_base.clone();
        ensure_search_list_size(&mut search_opts, effective_k);
        searcher.search(
            &vector_search.vector,
            1,
            effective_k as i32,
            &mut distances,
            &mut labels,
            &search_opts,
        )?;
        (distances, labels)
    };

    let id_to_scores = collect_results(&labels, &distances, effective_k, index_metric);
    if id_to_scores.is_empty() {
        return Ok(None);
    }

    Ok(Some(id_to_scores))
}

fn search_lumina_batch<S: LuminaSearch + ?Sized>(
    searcher: &S,
    index_meta: &LuminaIndexMeta,
    search_options_base: &HashMap<String, String>,
    vector_searches: &[VectorSearch],
) -> crate::Result<Vec<Option<HashMap<u64, f32>>>> {
    if vector_searches.is_empty() {
        return Ok(Vec::new());
    }

    let limit = vector_searches[0].limit;
    let same_limit = vector_searches
        .iter()
        .all(|vector_search| vector_search.limit == limit);
    let shared_filter = same_limit
        .then(|| shared_batch_include_row_ids(vector_searches))
        .flatten();
    let has_filter = vector_searches
        .iter()
        .any(|vector_search| vector_search.effective_include_row_ids().is_some());
    if has_filter && shared_filter.is_none() || !same_limit {
        return vector_searches
            .iter()
            .map(|vector_search| {
                search_lumina(searcher, index_meta, search_options_base, vector_search)
            })
            .collect();
    }

    let expected_dim = index_meta.dim()? as usize;
    for vector_search in vector_searches {
        if vector_search.vector.len() != expected_dim {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Query vector dimension mismatch: index expects {}, but got {}",
                    expected_dim,
                    vector_search.vector.len()
                ),
                source: None,
            });
        }
    }

    // An include-set that permits nothing can match nothing, whatever the index
    // holds. Answered from the set itself, so no native call is made at all.
    if shared_filter
        .as_ref()
        .is_some_and(|filter| filter.is_empty())
    {
        return Ok(vec![None; vector_searches.len()]);
    }

    // Java's order, and it decides what an empty index reports: `index.size()` first,
    // then the dense filter is built, and the metric is parsed last. An empty index
    // has nothing to return whatever the filter says, so converting the filter first
    // would report an oversized one instead, and reading the metric first would
    // report a malformed one. Neither is what happened.
    let count = searcher.get_count()? as usize;
    if count == 0 {
        return Ok(vec![None; vector_searches.len()]);
    }
    let filter_id_list =
        shared_filter.map(|include_row_ids| include_row_ids.iter().collect::<Vec<_>>());
    let index_metric = index_meta.metric()?;
    let effective_k = filter_id_list.as_ref().map_or_else(
        || std::cmp::min(limit, count),
        |ids| std::cmp::min(std::cmp::min(limit, count), ids.len()),
    );
    if effective_k == 0 {
        return Ok(vec![None; vector_searches.len()]);
    }

    let mut query = Vec::with_capacity(vector_searches.len() * expected_dim);
    for vector_search in vector_searches {
        query.extend_from_slice(&vector_search.vector);
    }

    let mut distances = vec![0.0f32; vector_searches.len() * effective_k];
    let mut labels = new_label_buffer(vector_searches.len() * effective_k);
    let mut search_opts: HashMap<String, String> = search_options_base.clone();
    ensure_search_list_size(&mut search_opts, effective_k);
    if let Some(filter_ids) = filter_id_list {
        search_opts.insert("search.thread_safe_filter".to_string(), "true".to_string());
        searcher.search_with_filter(
            &query,
            vector_searches.len() as i32,
            effective_k as i32,
            &mut distances,
            &mut labels,
            &filter_ids,
            &search_opts,
        )?;
    } else {
        searcher.search(
            &query,
            vector_searches.len() as i32,
            effective_k as i32,
            &mut distances,
            &mut labels,
            &search_opts,
        )?;
    }

    let mut results = Vec::with_capacity(vector_searches.len());
    for query_index in 0..vector_searches.len() {
        let start = query_index * effective_k;
        let end = start + effective_k;
        let id_to_scores = collect_results(
            &labels[start..end],
            &distances[start..end],
            effective_k,
            index_metric,
        );
        if id_to_scores.is_empty() {
            results.push(None);
        } else {
            results.push(Some(id_to_scores));
        }
    }
    Ok(results)
}

fn shared_batch_include_row_ids(
    vector_searches: &[VectorSearch],
) -> Option<&std::sync::Arc<roaring::RoaringTreemap>> {
    let first = vector_searches.first()?.shared_include_row_ids.as_ref()?;
    vector_searches
        .iter()
        .skip(1)
        .all(|vector_search| {
            vector_search
                .shared_include_row_ids
                .as_ref()
                .is_some_and(|include_row_ids| std::sync::Arc::ptr_eq(first, include_row_ids))
        })
        .then_some(first)
}

fn write_temp_index_file<S: Read + Seek>(stream: &mut S) -> crate::Result<PathBuf> {
    stream
        .seek(SeekFrom::Start(0))
        .map_err(|e| crate::Error::UnexpectedError {
            message: format!("Failed to seek Lumina index stream to start: {}", e),
            source: Some(Box::new(e)),
        })?;

    let path = std::env::temp_dir().join(format!(
        "paimon-lumina-index-{}.index",
        uuid::Uuid::new_v4()
    ));
    let mut file = std::fs::File::create(&path).map_err(|e| crate::Error::UnexpectedError {
        message: format!(
            "Failed to create temporary Lumina index file '{}': {}",
            path.display(),
            e
        ),
        source: Some(Box::new(e)),
    })?;
    std::io::copy(stream, &mut file).map_err(|e| crate::Error::UnexpectedError {
        message: format!(
            "Failed to write temporary Lumina index file '{}': {}",
            path.display(),
            e
        ),
        source: Some(Box::new(e)),
    })?;
    file.sync_all().map_err(|e| crate::Error::UnexpectedError {
        message: format!(
            "Failed to sync temporary Lumina index file '{}': {}",
            path.display(),
            e
        ),
        source: Some(Box::new(e)),
    })?;
    Ok(path)
}

impl Drop for LuminaVectorGlobalIndexReader {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lumina::{KEY_DIMENSION, KEY_DISTANCE_METRIC};
    use crate::vector_search::GlobalIndexIOMeta;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Debug, PartialEq)]
    struct FilteredSearchCall {
        query: Vec<f32>,
        n: i32,
        k: i32,
        filter_ids: Vec<u64>,
    }

    struct RecordingSearcher {
        count: u64,
        count_calls: AtomicUsize,
        unfiltered_calls: Mutex<Vec<(Vec<f32>, i32, i32)>>,
        filtered_calls: Mutex<Vec<FilteredSearchCall>>,
    }

    impl RecordingSearcher {
        fn new(count: u64) -> Self {
            Self {
                count,
                count_calls: AtomicUsize::new(0),
                unfiltered_calls: Mutex::new(Vec::new()),
                filtered_calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl LuminaSearch for RecordingSearcher {
        fn search(
            &self,
            query: &[f32],
            n: i32,
            k: i32,
            distances: &mut [f32],
            labels: &mut [u64],
            _options: &HashMap<String, String>,
        ) -> crate::Result<()> {
            self.unfiltered_calls
                .lock()
                .expect("unfiltered call lock")
                .push((query.to_vec(), n, k));
            for (index, (distance, label)) in
                distances.iter_mut().zip(labels.iter_mut()).enumerate()
            {
                *distance = index as f32;
                *label = index as u64;
            }
            Ok(())
        }

        fn search_with_filter(
            &self,
            query: &[f32],
            n: i32,
            k: i32,
            distances: &mut [f32],
            labels: &mut [u64],
            filter_ids: &[u64],
            _options: &HashMap<String, String>,
        ) -> crate::Result<()> {
            self.filtered_calls
                .lock()
                .expect("filtered call lock")
                .push(FilteredSearchCall {
                    query: query.to_vec(),
                    n,
                    k,
                    filter_ids: filter_ids.to_vec(),
                });
            for (index, (distance, label)) in
                distances.iter_mut().zip(labels.iter_mut()).enumerate()
            {
                *distance = index as f32;
                *label = filter_ids[index % filter_ids.len()];
            }
            Ok(())
        }

        fn get_count(&self) -> crate::Result<u64> {
            self.count_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.count)
        }
    }

    fn test_index_meta(dim: usize) -> LuminaIndexMeta {
        LuminaIndexMeta::new(HashMap::from([
            (KEY_DIMENSION.to_string(), dim.to_string()),
            (KEY_DISTANCE_METRIC.to_string(), "l2".to_string()),
        ]))
    }

    #[test]
    fn test_shared_filter_uses_one_lumina_batch_search() {
        let searcher = RecordingSearcher::new(10);
        let shared_filter = Arc::new(roaring::RoaringTreemap::from_iter([2, 4, 6]));
        let mut first = VectorSearch::new(vec![1.0, 0.0], 2, "embedding".to_string()).unwrap();
        first.set_shared_include_row_ids(Arc::clone(&shared_filter));
        let mut second = VectorSearch::new(vec![0.0, 1.0], 2, "embedding".to_string()).unwrap();
        second.set_shared_include_row_ids(Arc::clone(&shared_filter));

        let results = search_lumina_batch(
            &searcher,
            &test_index_meta(2),
            &HashMap::new(),
            &[first, second],
        )
        .expect("shared filtered batch search should succeed");

        assert_eq!(results.len(), 2);
        assert!(searcher
            .unfiltered_calls
            .lock()
            .expect("unfiltered call lock")
            .is_empty());
        assert_eq!(
            *searcher.filtered_calls.lock().expect("filtered call lock"),
            vec![FilteredSearchCall {
                query: vec![1.0, 0.0, 0.0, 1.0],
                n: 2,
                k: 2,
                filter_ids: vec![2, 4, 6],
            }]
        );
    }

    #[test]
    fn test_equal_but_distinct_filters_keep_scalar_fallback() {
        let searcher = RecordingSearcher::new(10);
        let mut first = VectorSearch::new(vec![1.0, 0.0], 2, "embedding".to_string()).unwrap();
        first.set_shared_include_row_ids(Arc::new(roaring::RoaringTreemap::from_iter([2, 4, 6])));
        let mut second = VectorSearch::new(vec![0.0, 1.0], 2, "embedding".to_string()).unwrap();
        second.set_shared_include_row_ids(Arc::new(roaring::RoaringTreemap::from_iter([2, 4, 6])));

        search_lumina_batch(
            &searcher,
            &test_index_meta(2),
            &HashMap::new(),
            &[first, second],
        )
        .expect("distinct filtered searches should succeed");

        let calls = searcher.filtered_calls.lock().expect("filtered call lock");
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().all(|call| call.n == 1));
    }

    #[test]
    fn test_mixed_filters_and_limits_keep_scalar_fallback() {
        let shared_filter = Arc::new(roaring::RoaringTreemap::from_iter([2, 4, 6]));
        let mut filtered = VectorSearch::new(vec![1.0, 0.0], 2, "embedding".to_string()).unwrap();
        filtered.set_shared_include_row_ids(Arc::clone(&shared_filter));
        let unfiltered = VectorSearch::new(vec![0.0, 1.0], 2, "embedding".to_string()).unwrap();
        let mixed_searcher = RecordingSearcher::new(10);

        search_lumina_batch(
            &mixed_searcher,
            &test_index_meta(2),
            &HashMap::new(),
            &[filtered, unfiltered],
        )
        .expect("mixed filtered searches should succeed");

        assert_eq!(
            mixed_searcher
                .filtered_calls
                .lock()
                .expect("filtered call lock")
                .len(),
            1
        );
        assert_eq!(
            mixed_searcher
                .unfiltered_calls
                .lock()
                .expect("unfiltered call lock")
                .len(),
            1
        );

        let mut first = VectorSearch::new(vec![1.0, 0.0], 1, "embedding".to_string()).unwrap();
        first.set_shared_include_row_ids(Arc::clone(&shared_filter));
        let mut second = VectorSearch::new(vec![0.0, 1.0], 2, "embedding".to_string()).unwrap();
        second.set_shared_include_row_ids(shared_filter);
        let differing_limit_searcher = RecordingSearcher::new(10);

        search_lumina_batch(
            &differing_limit_searcher,
            &test_index_meta(2),
            &HashMap::new(),
            &[first, second],
        )
        .expect("differing-limit filtered searches should succeed");

        let calls = differing_limit_searcher
            .filtered_calls
            .lock()
            .expect("filtered call lock");
        assert_eq!(calls.len(), 2);
        assert_eq!(calls.iter().map(|call| call.k).collect::<Vec<_>>(), [1, 2]);
        assert!(calls.iter().all(|call| call.n == 1));
    }

    #[test]
    fn test_empty_shared_filter_skips_native_search() {
        let searcher = RecordingSearcher::new(10);
        let shared_filter = Arc::new(roaring::RoaringTreemap::new());
        let mut first = VectorSearch::new(vec![1.0, 0.0], 2, "embedding".to_string()).unwrap();
        first.set_shared_include_row_ids(Arc::clone(&shared_filter));
        let mut second = VectorSearch::new(vec![0.0, 1.0], 2, "embedding".to_string()).unwrap();
        second.set_shared_include_row_ids(Arc::clone(&shared_filter));

        let results = search_lumina_batch(
            &searcher,
            &test_index_meta(2),
            &HashMap::new(),
            &[first, second],
        )
        .expect("empty shared filter should succeed");

        assert_eq!(results, vec![None, None]);
        assert_eq!(
            searcher.count_calls.load(Ordering::Relaxed),
            0,
            "an empty shared filter should avoid all native searcher calls"
        );
        assert!(searcher
            .unfiltered_calls
            .lock()
            .expect("unfiltered call lock")
            .is_empty());
        assert!(searcher
            .filtered_calls
            .lock()
            .expect("filtered call lock")
            .is_empty());
    }

    /// An index whose metadata names a metric Lumina does not define, so parsing it
    /// fails. Everything else about the index is well formed -- only the ORDER the
    /// steps run in decides whether the caller sees that failure or an empty answer.
    fn test_index_meta_with_unknown_metric(dim: usize) -> LuminaIndexMeta {
        LuminaIndexMeta::new(HashMap::from([
            (KEY_DIMENSION.to_string(), dim.to_string()),
            (KEY_DISTANCE_METRIC.to_string(), "not-a-metric".to_string()),
        ]))
    }

    #[test]
    fn an_empty_index_reports_empty_before_the_metric_is_parsed() {
        // Java's order, scalar path: `index.size()` decides first. A query against an
        // index holding nothing has an empty answer whatever the metric says, so
        // reporting a malformed metric here would be an error about something that
        // could not have changed the result.
        let searcher = RecordingSearcher::new(0);
        let search = VectorSearch::new(vec![1.0, 0.0], 2, "embedding".to_string()).unwrap();
        let result = search_lumina(
            &searcher,
            &test_index_meta_with_unknown_metric(2),
            &HashMap::new(),
            &search,
        )
        .expect("an empty index reports no hits, not a malformed metric");
        assert_eq!(result, None);
        assert!(searcher
            .unfiltered_calls
            .lock()
            .expect("unfiltered call lock")
            .is_empty());
    }

    #[test]
    fn an_empty_index_reports_empty_before_the_filter_is_densified() {
        // Same order on the batch path, where it also keeps the dense filter from
        // being built: `index.size()` first, then the filter, then the metric.
        let searcher = RecordingSearcher::new(0);
        let shared_filter = Arc::new(roaring::RoaringTreemap::from_iter([2, 4, 6]));
        let mut first = VectorSearch::new(vec![1.0, 0.0], 2, "embedding".to_string()).unwrap();
        first.set_shared_include_row_ids(Arc::clone(&shared_filter));
        let mut second = VectorSearch::new(vec![0.0, 1.0], 2, "embedding".to_string()).unwrap();
        second.set_shared_include_row_ids(Arc::clone(&shared_filter));

        let results = search_lumina_batch(
            &searcher,
            &test_index_meta_with_unknown_metric(2),
            &HashMap::new(),
            &[first, second],
        )
        .expect("an empty index reports no hits, not a malformed metric");

        assert_eq!(results, vec![None, None]);
        assert_eq!(
            searcher.count_calls.load(Ordering::Relaxed),
            1,
            "the index size is what the answer came from"
        );
        assert!(searcher
            .unfiltered_calls
            .lock()
            .expect("unfiltered call lock")
            .is_empty());
        assert!(searcher
            .filtered_calls
            .lock()
            .expect("filtered call lock")
            .is_empty());
    }

    #[test]
    fn test_convert_distance_to_score() {
        assert_eq!(convert_distance_to_score(0.0, LuminaVectorMetric::L2), 1.0);
        assert_eq!(convert_distance_to_score(1.0, LuminaVectorMetric::L2), 0.5);
        assert_eq!(
            convert_distance_to_score(0.0, LuminaVectorMetric::Cosine),
            1.0
        );
        assert_eq!(
            convert_distance_to_score(1.0, LuminaVectorMetric::Cosine),
            0.0
        );
        assert_eq!(
            convert_distance_to_score(0.75, LuminaVectorMetric::InnerProduct),
            0.75
        );
    }

    #[test]
    fn test_ensure_search_list_size() {
        let mut opts = HashMap::new();
        ensure_search_list_size(&mut opts, 10);
        assert_eq!(opts.get("diskann.search.list_size").unwrap(), "16"); // max(15, 16)

        let mut opts = HashMap::new();
        ensure_search_list_size(&mut opts, 100);
        assert_eq!(opts.get("diskann.search.list_size").unwrap(), "150"); // 100*1.5

        // does not override existing
        let mut opts = HashMap::new();
        opts.insert("diskann.search.list_size".to_string(), "999".to_string());
        ensure_search_list_size(&mut opts, 100);
        assert_eq!(opts.get("diskann.search.list_size").unwrap(), "999");
    }

    #[test]
    fn test_collect_results() {
        let labels = vec![0, 1, 2, SENTINEL, 3];
        let distances = vec![0.5, 0.3, 0.1, 0.0, 0.9];
        let result = collect_results(&labels, &distances, 2, LuminaVectorMetric::InnerProduct);
        assert_eq!(result.len(), 2);
        // top 2 by score: row 3 (0.9) and row 0 (0.5)
        assert!(result.contains_key(&3));
        assert!(result.contains_key(&0));
        assert!(!result.contains_key(&2)); // 0.1 is lowest
    }

    /// Rows sharing a score must be kept by ascending row id, not by the order
    /// the native searcher happened to return them in. Feeding the same tied
    /// scores in two different label orders must select the same rows.
    #[test]
    fn test_collect_results_breaks_ties_by_row_id() {
        // Rows 10, 20, 30 all score 0.5; row 40 scores higher and always wins.
        let distances = vec![0.9, 0.5, 0.5, 0.5];

        let forward = collect_results(
            &[40, 10, 20, 30],
            &distances,
            2,
            LuminaVectorMetric::InnerProduct,
        );
        let reversed = collect_results(
            &[40, 30, 20, 10],
            &distances,
            2,
            LuminaVectorMetric::InnerProduct,
        );

        let mut forward_ids: Vec<u64> = forward.keys().copied().collect();
        forward_ids.sort_unstable();
        let mut reversed_ids: Vec<u64> = reversed.keys().copied().collect();
        reversed_ids.sort_unstable();

        assert_eq!(
            forward_ids, reversed_ids,
            "tied rows must not depend on label order"
        );
        assert_eq!(
            forward_ids,
            vec![10, 40],
            "among equal scores the smallest row id wins"
        );
    }

    /// The same invariant when every candidate ties: the retained set is the
    /// `top_k` smallest row ids regardless of input order.
    #[test]
    fn test_collect_results_all_tied_keeps_smallest_row_ids() {
        let distances = vec![0.25; 5];

        let forward = collect_results(
            &[1, 2, 3, 4, 5],
            &distances,
            3,
            LuminaVectorMetric::InnerProduct,
        );
        let shuffled = collect_results(
            &[4, 1, 5, 3, 2],
            &distances,
            3,
            LuminaVectorMetric::InnerProduct,
        );

        let mut forward_ids: Vec<u64> = forward.keys().copied().collect();
        forward_ids.sort_unstable();
        let mut shuffled_ids: Vec<u64> = shuffled.keys().copied().collect();
        shuffled_ids.sort_unstable();

        assert_eq!(forward_ids, vec![1, 2, 3]);
        assert_eq!(shuffled_ids, vec![1, 2, 3]);
    }

    /// A NaN score must never outrank a finite one. `f32::total_cmp` alone ranks
    /// a positive NaN above every finite value, so using it here would let a NaN
    /// score -- which a non-finite stored vector can produce -- take the only
    /// top-1 slot. Both NaN signs must lose, in either arrival order.
    #[test]
    fn test_collect_results_ranks_nan_below_finite_scores() {
        for (labels, distances) in [
            (vec![7u64, 8], vec![f32::NAN, 0.5]),
            (vec![8u64, 7], vec![0.5, f32::NAN]),
            (vec![7u64, 8], vec![-f32::NAN, 0.5]),
            (vec![8u64, 7], vec![0.5, -f32::NAN]),
        ] {
            let result = collect_results(&labels, &distances, 1, LuminaVectorMetric::InnerProduct);
            assert_eq!(result.len(), 1);
            assert!(
                result.contains_key(&8),
                "the finite score must win regardless of arrival order, got {result:?}"
            );
        }
    }

    /// The label buffer handed to the native searcher must start out as
    /// [`SENTINEL`], never zero -- row id `0` is a legal result.
    #[test]
    fn test_new_label_buffer_is_sentinel_filled() {
        assert_eq!(new_label_buffer(3), vec![SENTINEL; 3]);
        assert!(new_label_buffer(0).is_empty());
    }

    /// A search that returns fewer neighbours than requested leaves the tail of
    /// the buffer exactly as it was allocated. Those slots must not surface as
    /// rows: with a zero-filled buffer the tail reads as row `0` at distance
    /// `0.0`, and `1.0 / (1.0 + 0.0)` is the highest score L2 can produce, so
    /// row 0 would be reported as the best match for every short result.
    #[test]
    fn test_unfilled_label_slots_are_not_reported_as_hits() {
        let mut labels = new_label_buffer(4);
        let mut distances = vec![0.0f32; 4];
        labels[0] = 11;
        distances[0] = 3.0;
        labels[1] = 22;
        distances[1] = 7.0;

        let result = collect_results(&labels, &distances, 4, LuminaVectorMetric::L2);

        assert_eq!(
            result.len(),
            2,
            "only the slots the searcher filled may be reported, got {result:?}"
        );
        assert!(result.contains_key(&11) && result.contains_key(&22));
        assert!(
            !result.contains_key(&0),
            "an untouched slot must not become row 0"
        );
    }

    #[test]
    fn test_reader_new() {
        let m = GlobalIndexIOMeta::new("a".into(), 100, vec![]);
        let reader = LuminaVectorGlobalIndexReader::new(m, HashMap::new());
        assert!(reader.searcher.is_none());
    }

    #[test]
    fn test_write_temp_index_file_copies_stream() {
        let bytes = b"lumina-index-bytes".to_vec();
        let mut stream = Cursor::new(bytes.clone());
        stream.seek(SeekFrom::End(0)).unwrap();

        let path = write_temp_index_file(&mut stream).unwrap();
        let actual = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(actual, bytes);
    }
}
