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

pub mod ffi;
pub mod reader;

use std::collections::HashMap;

pub const LUMINA_IDENTIFIER: &str = "lumina";
pub const LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER: &str = "lumina-vector-ann";
pub const LUMINA_VECTOR_ANN_IDENTIFIER: &str = LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER;

pub fn is_lumina_index_type(index_type: &str) -> bool {
    matches!(
        index_type,
        LUMINA_IDENTIFIER | LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LuminaVectorMetric {
    L2,
    Cosine,
    InnerProduct,
}

impl LuminaVectorMetric {
    pub fn lumina_name(&self) -> &str {
        match self {
            LuminaVectorMetric::L2 => "l2",
            LuminaVectorMetric::Cosine => "cosine",
            LuminaVectorMetric::InnerProduct => "inner_product",
        }
    }

    pub fn from_string(name: &str) -> crate::Result<Self> {
        match name.to_uppercase().as_str() {
            "L2" => Ok(LuminaVectorMetric::L2),
            "COSINE" => Ok(LuminaVectorMetric::Cosine),
            "INNER_PRODUCT" => Ok(LuminaVectorMetric::InnerProduct),
            _ => Err(crate::Error::DataInvalid {
                message: format!("Unknown metric name: {}", name),
                source: None,
            }),
        }
    }

    pub fn from_lumina_name(lumina_name: &str) -> crate::Result<Self> {
        match lumina_name {
            "l2" => Ok(LuminaVectorMetric::L2),
            "cosine" => Ok(LuminaVectorMetric::Cosine),
            "inner_product" => Ok(LuminaVectorMetric::InnerProduct),
            _ => Err(crate::Error::DataInvalid {
                message: format!("Unknown lumina metric name: {}", lumina_name),
                source: None,
            }),
        }
    }
}

const LUMINA_PREFIX: &str = "lumina.";

const ALL_OPTIONS_DEFAULTS: &[(&str, &str)] = &[
    ("lumina.index.dimension", "128"),
    ("lumina.index.type", "diskann"),
    ("lumina.distance.metric", "inner_product"),
    ("lumina.encoding.type", "pq"),
    ("lumina.pretrain.sample_ratio", "0.2"),
    ("lumina.diskann.build.ef_construction", "1024"),
    ("lumina.diskann.build.neighbor_count", "64"),
    ("lumina.diskann.build.thread_count", "32"),
    ("lumina.diskann.search.beam_width", "4"),
    ("lumina.encoding.pq.m", "64"),
    ("lumina.search.parallel_number", "5"),
];

pub struct LuminaVectorIndexOptions {
    pub dimension: i32,
    pub metric: LuminaVectorMetric,
    pub index_type: String,
    lumina_options: HashMap<String, String>,
}

impl LuminaVectorIndexOptions {
    pub fn new(paimon_options: &HashMap<String, String>) -> crate::Result<Self> {
        let dimension_str = paimon_options
            .get("lumina.index.dimension")
            .map(|s| s.as_str())
            .unwrap_or("128");
        let dimension: i32 = dimension_str
            .parse()
            .map_err(|_| crate::Error::DataInvalid {
                message: format!("Invalid dimension: {}", dimension_str),
                source: None,
            })?;
        if dimension <= 0 {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Invalid value for 'lumina.index.dimension': {}. Must be a positive integer.",
                    dimension
                ),
                source: None,
            });
        }

        let metric_str = paimon_options
            .get("lumina.distance.metric")
            .map(|s| s.as_str())
            .unwrap_or("inner_product");
        let metric = LuminaVectorMetric::from_lumina_name(metric_str)
            .or_else(|_| LuminaVectorMetric::from_string(metric_str))?;

        let encoding = paimon_options
            .get("lumina.encoding.type")
            .map(|s| s.as_str())
            .unwrap_or("pq");
        validate_encoding_metric(encoding, metric)?;

        let index_type = paimon_options
            .get("lumina.index.type")
            .cloned()
            .unwrap_or_else(|| "diskann".to_string());

        let lumina_options = build_lumina_options(paimon_options, dimension)?;

        Ok(Self {
            dimension,
            metric,
            index_type,
            lumina_options,
        })
    }

    pub fn to_lumina_options(&self) -> HashMap<String, String> {
        self.lumina_options.clone()
    }
}

fn validate_encoding_metric(encoding: &str, metric: LuminaVectorMetric) -> crate::Result<()> {
    if encoding.eq_ignore_ascii_case("pq") && metric == LuminaVectorMetric::Cosine {
        return Err(crate::Error::DataInvalid {
            message:
                "Lumina does not support PQ encoding with cosine metric. \
                Please use 'rawf32' or 'sq8' encoding, or switch to 'l2' or 'inner_product' metric."
                    .to_string(),
            source: None,
        });
    }
    Ok(())
}

fn validate_and_cap_pq_m(opts: &mut HashMap<String, String>, dimension: i32) -> crate::Result<()> {
    let encoding = opts.get("encoding.type").map(|s| s.as_str()).unwrap_or("");
    if !encoding.eq_ignore_ascii_case("pq") {
        return Ok(());
    }
    if let Some(pq_m_str) = opts.get("encoding.pq.m") {
        let pq_m: i32 = pq_m_str.parse().map_err(|_| crate::Error::DataInvalid {
            message: format!("encoding.pq.m must be an integer, got: {}", pq_m_str),
            source: None,
        })?;
        if pq_m <= 0 {
            return Err(crate::Error::DataInvalid {
                message: format!("encoding.pq.m must be positive, got: {}", pq_m),
                source: None,
            });
        }
        if pq_m > dimension {
            opts.insert("encoding.pq.m".to_string(), dimension.to_string());
        }
    }
    Ok(())
}

fn build_lumina_options(
    paimon_options: &HashMap<String, String>,
    dimension: i32,
) -> crate::Result<HashMap<String, String>> {
    let mut result = HashMap::new();

    for &(paimon_key, default_value) in ALL_OPTIONS_DEFAULTS {
        let native_key = &paimon_key[LUMINA_PREFIX.len()..];
        let value = paimon_options
            .get(paimon_key)
            .map(|s| s.as_str())
            .unwrap_or(default_value);
        result.insert(native_key.to_string(), value.to_string());
    }

    for (key, value) in paimon_options {
        if let Some(native_key) = key.strip_prefix(LUMINA_PREFIX) {
            result
                .entry(native_key.to_string())
                .or_insert_with(|| value.to_string());
        }
    }

    validate_and_cap_pq_m(&mut result, dimension)?;
    Ok(result)
}

pub fn strip_lumina_options(paimon_options: &HashMap<String, String>) -> HashMap<String, String> {
    let mut result = HashMap::new();
    for (key, value) in paimon_options {
        if let Some(native_key) = key.strip_prefix(LUMINA_PREFIX) {
            result.insert(native_key.to_string(), value.to_string());
        }
    }
    result
}

#[derive(Clone)]
pub struct VectorSearch {
    pub vector: Vec<f32>,
    pub limit: usize,
    pub field_name: String,
    pub include_row_ids: Option<roaring::RoaringTreemap>,
}

impl VectorSearch {
    pub fn new(vector: Vec<f32>, limit: usize, field_name: String) -> crate::Result<Self> {
        if vector.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Search vector cannot be empty".to_string(),
                source: None,
            });
        }
        if limit == 0 || limit > i32::MAX as usize {
            return Err(crate::Error::DataInvalid {
                message: format!("Limit must be between 1 and {}, got: {}", i32::MAX, limit),
                source: None,
            });
        }
        if field_name.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Field name cannot be null or empty".to_string(),
                source: None,
            });
        }
        Ok(Self {
            vector,
            limit,
            field_name,
            include_row_ids: None,
        })
    }

    pub fn with_include_row_ids(mut self, include_row_ids: roaring::RoaringTreemap) -> Self {
        self.include_row_ids = Some(include_row_ids);
        self
    }
}

impl std::fmt::Display for VectorSearch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "VectorSearch(field_name={}, limit={})",
            self.field_name, self.limit
        )
    }
}

pub struct GlobalIndexIOMeta {
    pub file_path: String,
    pub file_size: u64,
    pub metadata: Vec<u8>,
}

impl GlobalIndexIOMeta {
    pub fn new(file_path: String, file_size: u64, metadata: Vec<u8>) -> Self {
        Self {
            file_path,
            file_size,
            metadata,
        }
    }
}

pub const KEY_DIMENSION: &str = "index.dimension";
pub const KEY_DISTANCE_METRIC: &str = "distance.metric";
pub const KEY_INDEX_TYPE: &str = "index.type";

pub struct LuminaIndexMeta {
    options: HashMap<String, String>,
}

impl LuminaIndexMeta {
    pub fn new(options: HashMap<String, String>) -> Self {
        Self { options }
    }

    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }

    pub fn dim(&self) -> crate::Result<i32> {
        let val = self
            .options
            .get(KEY_DIMENSION)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("Missing required key: {}", KEY_DIMENSION),
                source: None,
            })?;
        val.parse::<i32>().map_err(|_| crate::Error::DataInvalid {
            message: format!("Invalid dimension value: {}", val),
            source: None,
        })
    }

    pub fn distance_metric(&self) -> &str {
        self.options
            .get(KEY_DISTANCE_METRIC)
            .map(String::as_str)
            .unwrap_or("")
    }

    pub fn metric(&self) -> crate::Result<LuminaVectorMetric> {
        LuminaVectorMetric::from_lumina_name(self.distance_metric())
    }

    pub fn index_type(&self) -> &str {
        self.options
            .get(KEY_INDEX_TYPE)
            .map(String::as_str)
            .unwrap_or("diskann")
    }

    pub fn serialize(&self) -> crate::Result<Vec<u8>> {
        serde_json::to_vec(&self.options).map_err(|e| crate::Error::DataInvalid {
            message: format!("Failed to serialize LuminaIndexMeta: {}", e),
            source: None,
        })
    }

    pub fn deserialize(data: &[u8]) -> crate::Result<Self> {
        let options: HashMap<String, String> =
            serde_json::from_slice(data).map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to deserialize LuminaIndexMeta: {}", e),
                source: None,
            })?;
        if !options.contains_key(KEY_DIMENSION) {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Missing required key in Lumina index metadata: {}",
                    KEY_DIMENSION
                ),
                source: None,
            });
        }
        if !options.contains_key(KEY_DISTANCE_METRIC) {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Missing required key in Lumina index metadata: {}",
                    KEY_DISTANCE_METRIC
                ),
                source: None,
            });
        }
        Ok(Self { options })
    }
}

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub row_ids: Vec<u64>,
    pub scores: Vec<f32>,
}

impl SearchResult {
    pub fn new(row_ids: Vec<u64>, scores: Vec<f32>) -> Self {
        assert_eq!(row_ids.len(), scores.len());
        Self { row_ids, scores }
    }

    pub fn empty() -> Self {
        Self {
            row_ids: Vec::new(),
            scores: Vec::new(),
        }
    }

    pub fn from_scored_map(map: HashMap<u64, f32>) -> Self {
        let mut row_ids = Vec::with_capacity(map.len());
        let mut scores = Vec::with_capacity(map.len());
        for (id, score) in map {
            row_ids.push(id);
            scores.push(score);
        }
        Self { row_ids, scores }
    }

    pub fn len(&self) -> usize {
        self.row_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.row_ids.is_empty()
    }

    pub fn offset(&self, offset: i64) -> Self {
        if offset == 0 {
            return self.clone();
        }
        let row_ids = self
            .row_ids
            .iter()
            .map(|&id| {
                if offset >= 0 {
                    id.saturating_add(offset as u64)
                } else {
                    id.saturating_sub(offset.unsigned_abs())
                }
            })
            .collect();
        Self {
            row_ids,
            scores: self.scores.clone(),
        }
    }

    pub fn or(&self, other: &SearchResult) -> Self {
        let mut row_ids = self.row_ids.clone();
        let mut scores = self.scores.clone();
        row_ids.extend_from_slice(&other.row_ids);
        scores.extend_from_slice(&other.scores);
        Self { row_ids, scores }
    }

    pub fn top_k(&self, k: usize) -> Self {
        if self.row_ids.len() <= k {
            return self.clone();
        }
        let mut indices: Vec<usize> = (0..self.row_ids.len()).collect();
        indices.sort_by(|&a, &b| {
            self.scores[b]
                .partial_cmp(&self.scores[a])
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        indices.truncate(k);
        let row_ids = indices.iter().map(|&i| self.row_ids[i]).collect();
        let scores = indices.iter().map(|&i| self.scores[i]).collect();
        Self { row_ids, scores }
    }

    pub fn to_row_ranges(&self) -> crate::Result<Vec<crate::table::RowRange>> {
        if self.row_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut sorted = self
            .row_ids
            .iter()
            .copied()
            .map(|id| {
                i64::try_from(id).map_err(|_| crate::Error::DataInvalid {
                    message: format!(
                        "Lumina search row id {id} exceeds i64::MAX and cannot be converted to RowRange"
                    ),
                    source: None,
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;

        sorted.sort_unstable();
        sorted.dedup();
        let mut ranges = Vec::new();
        let mut start = sorted[0];
        let mut end = start;
        for &id in &sorted[1..] {
            if end.checked_add(1) == Some(id) {
                end = id;
            } else {
                ranges.push(crate::table::RowRange::new(start, end));
                start = id;
                end = id;
            }
        }
        ranges.push(crate::table::RowRange::new(start, end));
        Ok(ranges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_roundtrip() {
        for metric in [
            LuminaVectorMetric::L2,
            LuminaVectorMetric::Cosine,
            LuminaVectorMetric::InnerProduct,
        ] {
            let name = metric.lumina_name();
            assert_eq!(LuminaVectorMetric::from_lumina_name(name).unwrap(), metric);
            assert_eq!(
                LuminaVectorMetric::from_string(&name.to_uppercase()).unwrap(),
                metric
            );
        }
        assert!(LuminaVectorMetric::from_string("hamming").is_err());
    }

    #[test]
    fn test_lumina_index_type_identifier_helper() {
        assert!(is_lumina_index_type(LUMINA_IDENTIFIER));
        assert!(is_lumina_index_type(LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER));
        assert!(is_lumina_index_type(LUMINA_VECTOR_ANN_IDENTIFIER));
        assert!(!is_lumina_index_type(""));
        assert!(!is_lumina_index_type("btree"));
        assert!(!is_lumina_index_type("lumina-vector"));
        assert!(!is_lumina_index_type("LUMINA"));
    }

    #[test]
    fn test_index_meta_serialize_deserialize() {
        let mut options = HashMap::new();
        options.insert(KEY_DIMENSION.to_string(), "128".to_string());
        options.insert(KEY_DISTANCE_METRIC.to_string(), "l2".to_string());
        options.insert(KEY_INDEX_TYPE.to_string(), "diskann".to_string());
        let meta = LuminaIndexMeta::new(options);

        let bytes = meta.serialize().unwrap();
        let meta2 = LuminaIndexMeta::deserialize(&bytes).unwrap();
        assert_eq!(meta2.dim().unwrap(), 128);
        assert_eq!(meta2.distance_metric(), "l2");
        assert_eq!(meta2.index_type(), "diskann");
    }

    #[test]
    fn test_index_meta_deserialize_missing_fields() {
        // missing dimension
        let mut opts = HashMap::new();
        opts.insert(KEY_DISTANCE_METRIC.to_string(), "l2".to_string());
        assert!(LuminaIndexMeta::deserialize(&serde_json::to_vec(&opts).unwrap()).is_err());

        // missing metric
        let mut opts = HashMap::new();
        opts.insert(KEY_DIMENSION.to_string(), "128".to_string());
        assert!(LuminaIndexMeta::deserialize(&serde_json::to_vec(&opts).unwrap()).is_err());

        // invalid json
        assert!(LuminaIndexMeta::deserialize(b"not json").is_err());
    }

    #[test]
    fn test_dim_error_on_invalid() {
        let mut opts = HashMap::new();
        opts.insert(KEY_DIMENSION.to_string(), "abc".to_string());
        opts.insert(KEY_DISTANCE_METRIC.to_string(), "l2".to_string());
        assert!(LuminaIndexMeta::new(opts).dim().is_err());
    }

    #[test]
    fn test_index_options_invalid_dimension() {
        let mut opts = HashMap::new();
        opts.insert("lumina.index.dimension".to_string(), "-1".to_string());
        assert!(LuminaVectorIndexOptions::new(&opts).is_err());
    }

    #[test]
    fn test_strip_lumina_options() {
        let mut opts = HashMap::new();
        opts.insert("lumina.index.dimension".to_string(), "128".to_string());
        opts.insert(
            "lumina.diskann.search.beam_width".to_string(),
            "8".to_string(),
        );
        opts.insert("non_lumina_key".to_string(), "ignored".to_string());
        let result = strip_lumina_options(&opts);
        assert_eq!(result.get("index.dimension").unwrap(), "128");
        assert_eq!(result.get("diskann.search.beam_width").unwrap(), "8");
        assert!(!result.contains_key("non_lumina_key"));
    }

    #[test]
    fn test_pq_cosine_rejected() {
        let mut opts = HashMap::new();
        opts.insert("lumina.index.dimension".to_string(), "128".to_string());
        opts.insert("lumina.distance.metric".to_string(), "cosine".to_string());
        opts.insert("lumina.encoding.type".to_string(), "pq".to_string());
        assert!(LuminaVectorIndexOptions::new(&opts).is_err());
    }

    #[test]
    fn test_pq_l2_accepted() {
        let mut opts = HashMap::new();
        opts.insert("lumina.index.dimension".to_string(), "128".to_string());
        opts.insert("lumina.distance.metric".to_string(), "l2".to_string());
        opts.insert("lumina.encoding.type".to_string(), "pq".to_string());
        assert!(LuminaVectorIndexOptions::new(&opts).is_ok());
    }

    #[test]
    fn test_pq_m_zero_rejected() {
        let mut opts = HashMap::new();
        opts.insert("lumina.index.dimension".to_string(), "128".to_string());
        opts.insert("lumina.encoding.pq.m".to_string(), "0".to_string());
        assert!(LuminaVectorIndexOptions::new(&opts).is_err());
    }

    #[test]
    fn test_pq_m_non_numeric_rejected() {
        let mut opts = HashMap::new();
        opts.insert("lumina.index.dimension".to_string(), "128".to_string());
        opts.insert("lumina.encoding.pq.m".to_string(), "abc".to_string());
        assert!(LuminaVectorIndexOptions::new(&opts).is_err());
    }

    #[test]
    fn test_cap_pq_m() {
        let mut opts = HashMap::new();
        opts.insert("lumina.index.dimension".to_string(), "32".to_string());
        opts.insert("lumina.encoding.pq.m".to_string(), "64".to_string());
        let index_opts = LuminaVectorIndexOptions::new(&opts).unwrap();
        let lumina_opts = index_opts.to_lumina_options();
        assert_eq!(lumina_opts.get("encoding.pq.m").unwrap(), "32");
    }

    #[test]
    fn test_build_lumina_options_defaults() {
        let opts = HashMap::new();
        let index_opts = LuminaVectorIndexOptions::new(&opts).unwrap();
        let lumina_opts = index_opts.to_lumina_options();
        assert_eq!(lumina_opts.get("index.dimension").unwrap(), "128");
        assert_eq!(lumina_opts.get("distance.metric").unwrap(), "inner_product");
        assert_eq!(lumina_opts.get("encoding.type").unwrap(), "pq");
        assert_eq!(lumina_opts.get("pretrain.sample_ratio").unwrap(), "0.2");
        assert_eq!(
            lumina_opts.get("diskann.build.ef_construction").unwrap(),
            "1024"
        );
        assert_eq!(
            lumina_opts.get("diskann.build.neighbor_count").unwrap(),
            "64"
        );
        assert_eq!(lumina_opts.get("diskann.build.thread_count").unwrap(), "32");
        assert_eq!(lumina_opts.get("diskann.search.beam_width").unwrap(), "4");
        assert_eq!(lumina_opts.get("encoding.pq.m").unwrap(), "64");
        assert_eq!(lumina_opts.get("search.parallel_number").unwrap(), "5");
    }

    #[test]
    fn test_vector_search_clone_preserves_include_row_ids() {
        let mut include_row_ids = roaring::RoaringTreemap::new();
        include_row_ids.insert(1);
        include_row_ids.insert(3);

        let vector_search = VectorSearch::new(vec![1.0, 2.0], 10, "embedding".to_string())
            .unwrap()
            .with_include_row_ids(include_row_ids.clone());

        let cloned = vector_search.clone();
        assert_eq!(cloned.vector, vector_search.vector);
        assert_eq!(cloned.limit, vector_search.limit);
        assert_eq!(cloned.field_name, vector_search.field_name);
        assert_eq!(cloned.include_row_ids.as_ref(), Some(&include_row_ids));
    }

    #[test]
    fn test_search_result_from_scored_map() {
        let mut map = HashMap::new();
        map.insert(1u64, 0.9f32);
        map.insert(2, 0.5);
        let result = SearchResult::from_scored_map(map);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_search_result_top_k() {
        let result = SearchResult::new(vec![1, 2, 3, 4, 5], vec![0.1, 0.9, 0.5, 0.8, 0.3]);
        let top = result.top_k(2);
        assert_eq!(top.len(), 2);
        assert!(top.row_ids.contains(&2));
        assert!(top.row_ids.contains(&4));
    }

    #[test]
    fn test_search_result_offset() {
        let result = SearchResult::new(vec![0, 1], vec![0.5, 0.6]);
        let offset = result.offset(100);
        assert_eq!(offset.row_ids, vec![100, 101]);
        assert_eq!(offset.scores, vec![0.5, 0.6]);
    }

    #[test]
    fn test_search_result_or() {
        let a = SearchResult::new(vec![1, 2], vec![0.5, 0.6]);
        let b = SearchResult::new(vec![3], vec![0.7]);
        let merged = a.or(&b);
        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn test_search_result_to_row_ranges() {
        let result = SearchResult::new(vec![5, 1, 2, 3, 10], vec![0.1; 5]);
        let ranges = result.to_row_ranges().unwrap();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].from(), 1);
        assert_eq!(ranges[0].to(), 3);
        assert_eq!(ranges[1].from(), 5);
        assert_eq!(ranges[1].to(), 5);
        assert_eq!(ranges[2].from(), 10);
        assert_eq!(ranges[2].to(), 10);
    }

    #[test]
    fn test_search_result_to_row_ranges_rejects_i64_overflow() {
        let result = SearchResult::new(vec![i64::MAX as u64 + 1], vec![0.1]);
        let err = result.to_row_ranges().unwrap_err();
        assert!(
            err.to_string().contains("exceeds i64::MAX"),
            "unexpected error: {err}"
        );
    }
}
