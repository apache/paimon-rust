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
use paimon_vindex_core::distance::MetricType;
use paimon_vindex_core::index::{
    VectorIndexMetadata, VectorIndexReader as VIndexReader, VectorSearchParams,
};
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::io::{Cursor, Read, Seek, SeekFrom};

const DEFAULT_NPROBE: usize = 16;
const DEFAULT_EF_SEARCH: usize = 0;
const NPROBE_PARAMETER: &str = "ivf.nprobe";
const EF_SEARCH_PARAMETER: &str = "hnsw.ef_search";

pub struct VindexVectorGlobalIndexReader {
    io_meta: GlobalIndexIOMeta,
    options: HashMap<String, String>,
    reader: Option<VIndexReader<Cursor<Vec<u8>>>>,
    metadata: Option<VectorIndexMetadata>,
}

impl VindexVectorGlobalIndexReader {
    pub fn new(io_meta: GlobalIndexIOMeta, options: HashMap<String, String>) -> Self {
        Self {
            io_meta,
            options,
            reader: None,
            metadata: None,
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
        vector_searches
            .iter()
            .map(|vector_search| self.search(vector_search))
            .collect()
    }

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

    fn ensure_loaded<S: Read + Seek + Send + 'static>(
        &mut self,
        stream_fn: impl FnOnce(&str) -> crate::Result<S>,
    ) -> crate::Result<()> {
        if self.reader.is_some() {
            return Ok(());
        }

        let mut stream = stream_fn(&self.io_meta.file_path)?;
        stream
            .seek(SeekFrom::Start(0))
            .map_err(|e| crate::Error::UnexpectedError {
                message: format!("Failed to seek vindex stream to start: {}", e),
                source: Some(Box::new(e)),
            })?;
        let mut bytes = Vec::with_capacity(self.io_meta.file_size as usize);
        stream
            .read_to_end(&mut bytes)
            .map_err(|e| crate::Error::UnexpectedError {
                message: format!("Failed to read vindex stream: {}", e),
                source: Some(Box::new(e)),
            })?;

        let mut reader =
            VIndexReader::open(Cursor::new(bytes)).map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to open paimon-vindex-core reader: {}", e),
                source: Some(Box::new(e)),
            })?;
        let metadata = reader.metadata();
        reader
            .optimize_for_search()
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to optimize paimon-vindex-core reader: {}", e),
                source: Some(Box::new(e)),
            })?;

        self.reader = Some(reader);
        self.metadata = Some(metadata);
        Ok(())
    }
}

fn search_vindex(
    reader: &mut VIndexReader<Cursor<Vec<u8>>>,
    metadata: &VectorIndexMetadata,
    options: &HashMap<String, String>,
    vector_search: &VectorSearch,
) -> crate::Result<Option<HashMap<u64, f32>>> {
    let expected_dim = metadata.dimension;
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

    let count = usize::try_from(metadata.total_vectors).unwrap_or(0);
    let effective_k = std::cmp::min(vector_search.limit, count);
    if effective_k == 0 {
        return Ok(None);
    }

    let params = VectorSearchParams::with_ef_search(
        effective_k,
        int_parameter(options, NPROBE_PARAMETER, DEFAULT_NPROBE)?,
        int_parameter(options, EF_SEARCH_PARAMETER, DEFAULT_EF_SEARCH)?,
    );

    let (labels, distances) = if let Some(include_ids) = &vector_search.include_row_ids {
        if include_ids.is_empty() {
            return Ok(None);
        }
        let ek = std::cmp::min(effective_k, include_ids.len() as usize);
        let params = VectorSearchParams::with_ef_search(
            params.top_k.min(ek),
            params.nprobe,
            params.ef_search,
        );
        let mut filter_bytes = Vec::new();
        include_ids
            .serialize_into(&mut filter_bytes)
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to serialize vector search row-id filter: {}", e),
                source: Some(Box::new(e)),
            })?;
        reader
            .search_with_roaring_filter(&vector_search.vector, params, &filter_bytes)
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("paimon-vindex-core filtered search failed: {}", e),
                source: Some(Box::new(e)),
            })?
    } else {
        reader
            .search(&vector_search.vector, params)
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("paimon-vindex-core search failed: {}", e),
                source: Some(Box::new(e)),
            })?
    };

    let id_to_scores = collect_results(&labels, &distances, effective_k, metadata.metric);
    if id_to_scores.is_empty() {
        return Ok(None);
    }

    Ok(Some(id_to_scores))
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
    fn test_int_parameter() {
        let mut options = HashMap::new();
        options.insert(NPROBE_PARAMETER.to_string(), "32".to_string());

        assert_eq!(
            int_parameter(&options, NPROBE_PARAMETER, DEFAULT_NPROBE).unwrap(),
            32
        );
        assert_eq!(
            int_parameter(&options, EF_SEARCH_PARAMETER, DEFAULT_EF_SEARCH).unwrap(),
            DEFAULT_EF_SEARCH
        );

        options.insert(EF_SEARCH_PARAMETER.to_string(), "abc".to_string());
        assert!(int_parameter(&options, EF_SEARCH_PARAMETER, DEFAULT_EF_SEARCH).is_err());
    }
}
