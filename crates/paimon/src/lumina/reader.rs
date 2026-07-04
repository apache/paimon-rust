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
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            other.score.total_cmp(&self.score)
        }
    }

    let mut min_heap: BinaryHeap<ScoredRow> = BinaryHeap::with_capacity(top_k + 1);
    for (&row_id, &distance) in labels.iter().zip(distances.iter()) {
        if row_id == SENTINEL {
            continue;
        }
        let score = convert_distance_to_score(distance, metric);
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

fn search_lumina(
    searcher: &LuminaSearcher,
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
    let index_metric = index_meta.metric()?;
    let count = searcher.get_count()? as usize;
    let effective_k = std::cmp::min(limit, count);
    if effective_k == 0 {
        return Ok(None);
    }

    let include_row_ids = &vector_search.include_row_ids;

    let (distances, labels) = if let Some(ref include_ids) = include_row_ids {
        let filter_id_list: Vec<u64> = include_ids.iter().collect();
        if filter_id_list.is_empty() {
            return Ok(None);
        }
        let ek = std::cmp::min(effective_k, filter_id_list.len());
        let mut distances = vec![0.0f32; ek];
        let mut labels = vec![0u64; ek];
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
        let mut labels = vec![0u64; effective_k];
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

fn search_lumina_batch(
    searcher: &LuminaSearcher,
    index_meta: &LuminaIndexMeta,
    search_options_base: &HashMap<String, String>,
    vector_searches: &[VectorSearch],
) -> crate::Result<Vec<Option<HashMap<u64, f32>>>> {
    if vector_searches.is_empty() {
        return Ok(Vec::new());
    }
    if vector_searches
        .iter()
        .any(|vector_search| vector_search.include_row_ids.is_some())
    {
        return vector_searches
            .iter()
            .map(|vector_search| {
                search_lumina(searcher, index_meta, search_options_base, vector_search)
            })
            .collect();
    }

    let limit = vector_searches[0].limit;
    if vector_searches
        .iter()
        .any(|vector_search| vector_search.limit != limit)
    {
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

    let index_metric = index_meta.metric()?;
    let count = searcher.get_count()? as usize;
    let effective_k = std::cmp::min(limit, count);
    if effective_k == 0 {
        return Ok(vec![None; vector_searches.len()]);
    }

    let mut query = Vec::with_capacity(vector_searches.len() * expected_dim);
    for vector_search in vector_searches {
        query.extend_from_slice(&vector_search.vector);
    }

    let mut distances = vec![0.0f32; vector_searches.len() * effective_k];
    let mut labels = vec![0u64; vector_searches.len() * effective_k];
    let mut search_opts: HashMap<String, String> = search_options_base.clone();
    ensure_search_list_size(&mut search_opts, effective_k);
    searcher.search(
        &query,
        vector_searches.len() as i32,
        effective_k as i32,
        &mut distances,
        &mut labels,
        &search_opts,
    )?;

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
    use crate::vector_search::GlobalIndexIOMeta;
    use std::io::Cursor;

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
