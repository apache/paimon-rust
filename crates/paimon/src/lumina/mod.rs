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

pub const KEY_DIMENSION: &str = "index.dimension";
pub const KEY_DISTANCE_METRIC: &str = "distance.metric";
pub const KEY_INDEX_TYPE: &str = "index.type";

/// Paimon-prefixed option key for the vector index dimension.
pub const LUMINA_DIMENSION_OPTION: &str = "lumina.index.dimension";

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
}
