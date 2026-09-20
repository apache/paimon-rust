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

pub(crate) mod executor;
pub(crate) mod range_reader;
pub mod reader;

pub mod pkvector;

use crate::spec::{DataField, DataType};
use paimon_vindex_core::index::VectorIndexConfig;
use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

pub const IVF_FLAT_IDENTIFIER: &str = "ivf-flat";
pub const IVF_PQ_IDENTIFIER: &str = "ivf-pq";
pub const IVF_SQ_IDENTIFIER: &str = "ivf-sq";
pub const IVF_RQ_IDENTIFIER: &str = "ivf-rq";
pub const DISKANN_IDENTIFIER: &str = "diskann";

const DEFAULT_DIMENSION: &str = "128";
const DEFAULT_METRIC: &str = "inner_product";
const DEFAULT_NLIST: &str = "256";
const DEFAULT_PQ_M: &str = "16";
const DEFAULT_PQ_USE_OPQ: &str = "false";
const DEFAULT_TRAIN_SAMPLE_RATIO: f64 = 1.0;
const VECTOR_SEARCH_TIMING_ENV: &str = "PAIMON_LOG_VECTOR_SEARCH_TIMING";
const DISKANN_OPTION_KEYS: &[(&str, &str)] = &[
    ("deployment-profile", "deployment-profile"),
    ("target-recall", "target-recall"),
    ("max-bytes-per-vector", "max-bytes-per-vector"),
    ("pq.code-ratio", "pq.code-ratio"),
    ("pq.m", "pq.m"),
    ("pq.bits", "pq.bits"),
    ("diskann.build-preset", "build-preset"),
    ("diskann.seed", "seed"),
    ("diskann.memory-budget-bytes", "memory-budget-bytes"),
    ("diskann.max-degree", "max-degree"),
    ("diskann.build-search-list-size", "build-search-list-size"),
    ("diskann.alpha", "alpha"),
    ("diskann.storage-layout", "storage-layout"),
    ("diskann.raw-vector-encoding", "raw-vector-encoding"),
    ("diskann.build-distance", "build-distance"),
];

#[cfg(test)]
static VECTOR_SEARCH_TIMING_TEST_GUARDS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub(crate) struct VectorSearchTimingTestGuard;

#[cfg(test)]
impl Drop for VectorSearchTimingTestGuard {
    fn drop(&mut self) {
        VECTOR_SEARCH_TIMING_TEST_GUARDS.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
pub(crate) fn enable_vector_search_timing_for_test() -> VectorSearchTimingTestGuard {
    VECTOR_SEARCH_TIMING_TEST_GUARDS.fetch_add(1, Ordering::Relaxed);
    VectorSearchTimingTestGuard
}

pub(crate) fn vector_search_timing_enabled() -> bool {
    #[cfg(test)]
    if VECTOR_SEARCH_TIMING_TEST_GUARDS.load(Ordering::Relaxed) > 0 {
        return true;
    }
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os(VECTOR_SEARCH_TIMING_ENV).is_some_and(|v| v == "1"))
}

pub fn is_vindex_index_type(index_type: &str) -> bool {
    matches!(
        index_type,
        IVF_FLAT_IDENTIFIER
            | IVF_PQ_IDENTIFIER
            | IVF_SQ_IDENTIFIER
            | IVF_RQ_IDENTIFIER
            | DISKANN_IDENTIFIER
    )
}

pub(crate) fn native_index_type(index_type: &str) -> Option<&'static str> {
    match index_type {
        IVF_FLAT_IDENTIFIER => Some("ivf_flat"),
        IVF_PQ_IDENTIFIER => Some("ivf_pq"),
        IVF_SQ_IDENTIFIER => Some("ivf_sq"),
        IVF_RQ_IDENTIFIER => Some("ivf_rq"),
        DISKANN_IDENTIFIER => Some("diskann"),
        _ => None,
    }
}

#[derive(Debug)]
pub(crate) struct VindexVectorIndexOptions {
    pub config: VectorIndexConfig,
    pub native_options: HashMap<String, String>,
    pub train_sample_ratio: f64,
}

impl VindexVectorIndexOptions {
    pub fn new(
        table_options: &HashMap<String, String>,
        user_options: &HashMap<String, String>,
        index_type: &str,
        field: &DataField,
    ) -> crate::Result<Self> {
        let native_index_type =
            native_index_type(index_type).ok_or_else(|| crate::Error::DataInvalid {
                message: format!("Unsupported vindex index type: {index_type}"),
                source: None,
            })?;

        validate_user_option_keys(user_options, index_type, field.name())?;
        validate_index_type_option(table_options, user_options, native_index_type)?;

        let mut native_options = HashMap::new();
        native_options.insert("index.type".to_string(), native_index_type.to_string());
        native_options.insert(
            "dimension".to_string(),
            resolve_dimension(table_options, user_options, index_type, field)?,
        );
        if index_type != DISKANN_IDENTIFIER {
            native_options.insert(
                "nlist".to_string(),
                option_value(
                    table_options,
                    user_options,
                    field.name(),
                    index_type,
                    "nlist",
                    "nlist",
                    DEFAULT_NLIST,
                ),
            );
        }
        native_options.insert(
            "metric".to_string(),
            normalize_metric(&option_value(
                table_options,
                user_options,
                field.name(),
                index_type,
                "metric",
                "distance.metric",
                DEFAULT_METRIC,
            )),
        );

        if index_type == IVF_PQ_IDENTIFIER {
            native_options.insert(
                "pq.m".to_string(),
                option_value(
                    table_options,
                    user_options,
                    field.name(),
                    index_type,
                    "pq.m",
                    "pq.m",
                    DEFAULT_PQ_M,
                ),
            );
            native_options.insert(
                "use-opq".to_string(),
                option_value(
                    table_options,
                    user_options,
                    field.name(),
                    index_type,
                    "use-opq",
                    "pq.use-opq",
                    DEFAULT_PQ_USE_OPQ,
                ),
            );
        }
        if index_type == IVF_RQ_IDENTIFIER {
            for key in ["rq.bits", "max-bytes-per-vector"] {
                if let Some(value) = optional_value(
                    table_options,
                    user_options,
                    field.name(),
                    index_type,
                    key,
                    key,
                ) {
                    native_options.insert(key.to_string(), value);
                }
            }
        }
        if index_type == DISKANN_IDENTIFIER {
            for &(native_key, paimon_suffix) in DISKANN_OPTION_KEYS {
                if let Some(value) = optional_value(
                    table_options,
                    user_options,
                    field.name(),
                    index_type,
                    native_key,
                    paimon_suffix,
                ) {
                    native_options.insert(native_key.to_string(), value);
                }
            }
        }
        for key in [
            "ivf.coarse-assignment",
            "ivf.pq-encoding",
            "ivf.train.max-points-per-centroid",
            "pq.train.max-points-per-centroid",
        ] {
            if is_allowed_native_key(key, index_type) {
                if let Some(value) = optional_value(
                    table_options,
                    user_options,
                    field.name(),
                    index_type,
                    key,
                    key,
                ) {
                    native_options.insert(key.to_string(), value);
                }
            }
        }

        let config = VectorIndexConfig::from_options(&native_options).map_err(|e| {
            crate::Error::DataInvalid {
                message: format!("Invalid vindex options: {e}"),
                source: Some(Box::new(e)),
            }
        })?;
        let train_sample_ratio =
            resolve_train_sample_ratio(table_options, user_options, index_type, field.name())?;
        Ok(Self {
            config,
            native_options,
            train_sample_ratio,
        })
    }

    pub fn dimension(&self) -> usize {
        self.config.dimension()
    }
}

fn validate_index_type_option(
    table_options: &HashMap<String, String>,
    user_options: &HashMap<String, String>,
    expected_native: &str,
) -> crate::Result<()> {
    for options in [table_options, user_options] {
        if let Some(value) = options.get("index.type") {
            let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
            if normalized != expected_native {
                return Err(crate::Error::ConfigInvalid {
                    message: format!(
                        "Option 'index.type' is '{}', but procedure index_type resolves to '{}'. \
                         Remove 'index.type' from options or set it to '{}'.",
                        value, expected_native, expected_native
                    ),
                });
            }
        }
    }
    Ok(())
}

fn validate_user_option_keys(
    user_options: &HashMap<String, String>,
    index_type: &str,
    field_name: &str,
) -> crate::Result<()> {
    let mut unknown = user_options
        .keys()
        .filter(|key| !is_supported_user_option_key(key, index_type, field_name))
        .cloned()
        .collect::<Vec<_>>();
    if unknown.is_empty() {
        return Ok(());
    }

    unknown.sort();
    Err(crate::Error::ConfigInvalid {
        message: format!(
            "Unknown vindex option(s) for index_type '{}': {}",
            index_type,
            unknown.join(", ")
        ),
    })
}

fn is_supported_user_option_key(key: &str, index_type: &str, field_name: &str) -> bool {
    if key == "index.type" {
        return true;
    }
    if is_allowed_native_key(key, index_type) {
        return true;
    }

    let index_prefix = format!("{index_type}.");
    if let Some(suffix) = key.strip_prefix(&index_prefix) {
        return is_allowed_paimon_suffix(suffix, index_type);
    }

    let field_prefix = format!("fields.{field_name}.");
    if let Some(suffix) = key.strip_prefix(&field_prefix) {
        return is_allowed_paimon_suffix(suffix, index_type);
    }

    false
}

fn is_allowed_native_key(key: &str, index_type: &str) -> bool {
    match key {
        "dimension" | "metric" => true,
        "nlist" => index_type != DISKANN_IDENTIFIER,
        "use-opq" => index_type == IVF_PQ_IDENTIFIER,
        "rq.bits" => index_type == IVF_RQ_IDENTIFIER,
        "max-bytes-per-vector" => {
            matches!(index_type, IVF_RQ_IDENTIFIER | DISKANN_IDENTIFIER)
        }
        "pq.m" if index_type == IVF_PQ_IDENTIFIER => true,
        "ivf.coarse-assignment" | "ivf.train.max-points-per-centroid" => {
            index_type != DISKANN_IDENTIFIER
        }
        "ivf.pq-encoding" => index_type == IVF_PQ_IDENTIFIER,
        "pq.train.max-points-per-centroid" => {
            matches!(index_type, IVF_PQ_IDENTIFIER | DISKANN_IDENTIFIER)
        }
        _ => {
            index_type == DISKANN_IDENTIFIER
                && DISKANN_OPTION_KEYS
                    .iter()
                    .any(|(native_key, _)| *native_key == key)
        }
    }
}

fn is_allowed_paimon_suffix(suffix: &str, index_type: &str) -> bool {
    match suffix {
        "dimension" | "distance.metric" => true,
        "nlist" => index_type != DISKANN_IDENTIFIER,
        "train.sample-ratio" => true,
        "pq.use-opq" => index_type == IVF_PQ_IDENTIFIER,
        "rq.bits" => index_type == IVF_RQ_IDENTIFIER,
        "max-bytes-per-vector" => {
            matches!(index_type, IVF_RQ_IDENTIFIER | DISKANN_IDENTIFIER)
        }
        "pq.m" if index_type == IVF_PQ_IDENTIFIER => true,
        "ivf.coarse-assignment"
        | "ivf.pq-encoding"
        | "ivf.train.max-points-per-centroid"
        | "pq.train.max-points-per-centroid" => is_allowed_native_key(suffix, index_type),
        _ => {
            index_type == DISKANN_IDENTIFIER
                && DISKANN_OPTION_KEYS
                    .iter()
                    .any(|(_, paimon_suffix)| *paimon_suffix == suffix)
        }
    }
}

fn resolve_train_sample_ratio(
    table_options: &HashMap<String, String>,
    user_options: &HashMap<String, String>,
    index_type: &str,
    field_name: &str,
) -> crate::Result<f64> {
    let mut value = None;
    for options in [user_options, table_options] {
        for key in [
            format!("fields.{field_name}.train.sample-ratio"),
            format!("{index_type}.train.sample-ratio"),
        ] {
            if let Some(candidate) = options.get(&key) {
                value = Some(candidate.as_str());
                break;
            }
        }
        if value.is_some() {
            break;
        }
    }

    let Some(value) = value else {
        return Ok(DEFAULT_TRAIN_SAMPLE_RATIO);
    };
    let ratio = value
        .parse::<f64>()
        .map_err(|_| crate::Error::ConfigInvalid {
            message: format!("Invalid vindex train.sample-ratio: '{value}'"),
        })?;
    if !(ratio > 0.0 && ratio <= 1.0) {
        return Err(crate::Error::ConfigInvalid {
            message: format!(
                "Invalid vindex train.sample-ratio: {value}; expected a finite value in (0, 1]"
            ),
        });
    }
    Ok(ratio)
}

fn resolve_dimension(
    table_options: &HashMap<String, String>,
    user_options: &HashMap<String, String>,
    index_type: &str,
    field: &DataField,
) -> crate::Result<String> {
    if let DataType::Vector(vector) = field.data_type() {
        return Ok(vector.length().to_string());
    }

    Ok(option_value(
        table_options,
        user_options,
        field.name(),
        index_type,
        "dimension",
        "dimension",
        DEFAULT_DIMENSION,
    ))
}

fn option_value(
    table_options: &HashMap<String, String>,
    user_options: &HashMap<String, String>,
    field_name: &str,
    index_type: &str,
    native_key: &str,
    paimon_suffix: &str,
    default_value: &str,
) -> String {
    optional_value(
        table_options,
        user_options,
        field_name,
        index_type,
        native_key,
        paimon_suffix,
    )
    .unwrap_or_else(|| default_value.to_string())
}

fn optional_value(
    table_options: &HashMap<String, String>,
    user_options: &HashMap<String, String>,
    field_name: &str,
    index_type: &str,
    native_key: &str,
    paimon_suffix: &str,
) -> Option<String> {
    for options in [user_options, table_options] {
        for key in [
            format!("fields.{field_name}.{paimon_suffix}"),
            format!("{index_type}.{paimon_suffix}"),
            native_key.to_string(),
        ] {
            if let Some(value) = options.get(&key) {
                return Some(value.clone());
            }
        }
    }
    None
}

fn normalize_metric(metric: &str) -> String {
    metric.trim().to_ascii_lowercase().replace('-', "_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{ArrayType, FloatType, VectorType};
    use paimon_vindex_core::index::{
        IndexType, VectorIndexMetadata, VectorIndexReader, VectorIndexTrainer, VectorIndexWriter,
    };
    use paimon_vindex_core::io::PosWriter;
    use std::io::Cursor;

    fn array_float_field() -> DataField {
        DataField::new(
            7,
            "embedding".to_string(),
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
    }

    fn roundtrip_metadata(index_type: &str, user_options: &[(&str, &str)]) -> VectorIndexMetadata {
        let user_options = user_options
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        let options = VindexVectorIndexOptions::new(
            &HashMap::new(),
            &user_options,
            index_type,
            &array_float_field(),
        )
        .unwrap();
        let n = 512;
        let dimension = options.config.dimension();
        let data = (0..n * dimension)
            .map(|offset| {
                let row = offset / dimension;
                let column = offset % dimension;
                (row % 4) as f32 * 20.0 + column as f32 * 0.01 + row as f32 * 0.0001
            })
            .collect::<Vec<_>>();
        let training = VectorIndexTrainer::train(options.config, &data, n).unwrap();
        let mut writer = VectorIndexWriter::new(training);
        writer
            .add_vectors(&(0..n as i64).collect::<Vec<_>>(), &data, n)
            .unwrap();
        let mut bytes = Vec::new();
        writer.write(&mut PosWriter::new(&mut bytes)).unwrap();
        VectorIndexReader::open(Cursor::new(bytes))
            .unwrap()
            .metadata()
    }

    #[test]
    fn test_vindex_index_type_identifier_helper() {
        assert!(is_vindex_index_type(IVF_FLAT_IDENTIFIER));
        assert!(is_vindex_index_type(IVF_PQ_IDENTIFIER));
        assert!(is_vindex_index_type("ivf-sq"));
        assert!(is_vindex_index_type("ivf-rq"));
        assert!(is_vindex_index_type("diskann"));
        assert!(!is_vindex_index_type("ivf-hnsw-flat"));
        assert!(!is_vindex_index_type("ivf-hnsw-sq"));
        assert!(!is_vindex_index_type(""));
        assert!(!is_vindex_index_type("btree"));
        assert!(!is_vindex_index_type("lumina"));
        assert!(!is_vindex_index_type("IVF-FLAT"));
    }

    #[test]
    fn test_vindex_options_map_java_prefixed_keys_to_native_config() {
        let table_options = HashMap::new();
        let user_options = HashMap::from([
            ("ivf-pq.dimension".to_string(), "8".to_string()),
            ("ivf-pq.nlist".to_string(), "4".to_string()),
            ("ivf-pq.distance.metric".to_string(), "cosine".to_string()),
            ("ivf-pq.pq.m".to_string(), "2".to_string()),
            ("ivf-pq.pq.use-opq".to_string(), "true".to_string()),
        ]);

        let options = VindexVectorIndexOptions::new(
            &table_options,
            &user_options,
            IVF_PQ_IDENTIFIER,
            &array_float_field(),
        )
        .unwrap();

        assert_eq!(options.dimension(), 8);
        assert_eq!(
            options.native_options.get("index.type").map(String::as_str),
            Some("ivf_pq")
        );
        assert_eq!(
            options.native_options.get("metric").map(String::as_str),
            Some("cosine")
        );
        assert_eq!(
            options.native_options.get("pq.m").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            options.native_options.get("use-opq").map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn test_vindex_options_map_build_options() {
        let user_options = HashMap::from([
            (
                "ivf-pq.ivf.coarse-assignment".to_string(),
                "exact".to_string(),
            ),
            (
                "fields.embedding.ivf.pq-encoding".to_string(),
                "canonical".to_string(),
            ),
            (
                "ivf-pq.ivf.train.max-points-per-centroid".to_string(),
                "32".to_string(),
            ),
            (
                "fields.embedding.pq.train.max-points-per-centroid".to_string(),
                "64".to_string(),
            ),
        ]);

        let options = VindexVectorIndexOptions::new(
            &HashMap::new(),
            &user_options,
            IVF_PQ_IDENTIFIER,
            &array_float_field(),
        )
        .unwrap();

        assert_eq!(
            options
                .native_options
                .get("ivf.coarse-assignment")
                .map(String::as_str),
            Some("exact")
        );
        assert_eq!(
            options
                .native_options
                .get("ivf.pq-encoding")
                .map(String::as_str),
            Some("canonical")
        );
        let resolved = options.config.resolved();
        assert!(!resolved.use_approximate_coarse_assignment);
        assert!(resolved.canonical_pq_encoding);
        assert_eq!(resolved.ivf_train_max_points_per_centroid, Some(32));
        assert_eq!(resolved.pq_train_max_points_per_centroid, Some(64));

        let diskann = VindexVectorIndexOptions::new(
            &HashMap::new(),
            &HashMap::from([(
                "diskann.pq.train.max-points-per-centroid".to_string(),
                "16".to_string(),
            )]),
            DISKANN_IDENTIFIER,
            &array_float_field(),
        )
        .unwrap();
        assert_eq!(
            diskann.config.resolved().pq_train_max_points_per_centroid,
            Some(16)
        );
    }

    #[test]
    fn test_vindex_options_map_ivf_rq_bits() {
        let user_options = HashMap::from([
            ("dimension".to_string(), "8".to_string()),
            ("nlist".to_string(), "4".to_string()),
            ("rq.bits".to_string(), "3".to_string()),
        ]);

        let options = VindexVectorIndexOptions::new(
            &HashMap::new(),
            &user_options,
            IVF_RQ_IDENTIFIER,
            &array_float_field(),
        )
        .unwrap();

        assert_eq!(
            options.native_options.get("rq.bits").map(String::as_str),
            Some("3")
        );
        assert_eq!(options.config.resolved().rq_bits, Some(3));

        let defaults = VindexVectorIndexOptions::new(
            &HashMap::new(),
            &HashMap::new(),
            IVF_RQ_IDENTIFIER,
            &array_float_field(),
        )
        .unwrap();
        assert_eq!(defaults.config.resolved().rq_bits, Some(4));
        assert!(!defaults.native_options.contains_key("rq.bits"));

        let capacity_goal = HashMap::from([
            ("dimension".to_string(), "100".to_string()),
            ("nlist".to_string(), "16".to_string()),
            ("ivf-rq.max-bytes-per-vector".to_string(), "88".to_string()),
        ]);
        let inferred = VindexVectorIndexOptions::new(
            &HashMap::new(),
            &capacity_goal,
            IVF_RQ_IDENTIFIER,
            &array_float_field(),
        )
        .unwrap();
        assert_eq!(inferred.config.resolved().rq_bits, Some(3));
        assert_eq!(
            inferred
                .native_options
                .get("max-bytes-per-vector")
                .map(String::as_str),
            Some("88")
        );
        assert!(!inferred.native_options.contains_key("rq.bits"));

        let invalid = HashMap::from([("ivf-rq.rq.bits".to_string(), "9".to_string())]);
        let error = VindexVectorIndexOptions::new(
            &HashMap::new(),
            &invalid,
            IVF_RQ_IDENTIFIER,
            &array_float_field(),
        )
        .expect_err("rq.bits outside 1..=8 must be rejected");
        assert!(error.to_string().contains("rq.bits"));
    }

    #[test]
    fn test_vindex_options_map_diskann_config() {
        let user_options = HashMap::from([
            ("diskann.dimension".to_string(), "128".to_string()),
            ("diskann.distance.metric".to_string(), "l2".to_string()),
            (
                "diskann.deployment-profile".to_string(),
                "local_storage".to_string(),
            ),
            ("diskann.target-recall".to_string(), "0.9".to_string()),
            (
                "diskann.max-bytes-per-vector".to_string(),
                "1024".to_string(),
            ),
            ("diskann.pq.code-ratio".to_string(), "0.125".to_string()),
            ("diskann.pq.m".to_string(), "16".to_string()),
            ("diskann.pq.bits".to_string(), "4".to_string()),
            ("diskann.build-preset".to_string(), "balanced".to_string()),
            ("diskann.seed".to_string(), "7".to_string()),
            ("diskann.max-degree".to_string(), "32".to_string()),
            (
                "diskann.build-search-list-size".to_string(),
                "64".to_string(),
            ),
            ("diskann.alpha".to_string(), "1.4".to_string()),
            (
                "diskann.memory-budget-bytes".to_string(),
                "123456".to_string(),
            ),
            (
                "diskann.storage-layout".to_string(),
                "interleaved".to_string(),
            ),
            ("diskann.raw-vector-encoding".to_string(), "f16".to_string()),
            (
                "diskann.build-distance".to_string(),
                "full_precision".to_string(),
            ),
        ]);

        let options = VindexVectorIndexOptions::new(
            &HashMap::new(),
            &user_options,
            DISKANN_IDENTIFIER,
            &array_float_field(),
        )
        .unwrap();
        let resolved = options.config.resolved();

        assert_eq!(resolved.pq_m, Some(16));
        assert_eq!(resolved.pq_bits, Some(4));
        let build = resolved.diskann_build.unwrap();
        assert_eq!(build.max_degree, 32);
        assert_eq!(build.build_search_list_size, 64);
        assert_eq!(build.alpha, 1.4);
        assert!(!options.native_options.contains_key("nlist"));
        for key in [
            "deployment-profile",
            "target-recall",
            "max-bytes-per-vector",
            "pq.code-ratio",
            "diskann.build-preset",
            "diskann.seed",
            "diskann.storage-layout",
            "diskann.raw-vector-encoding",
            "diskann.build-distance",
        ] {
            assert!(options.native_options.contains_key(key), "missing {key}");
        }
    }

    #[test]
    fn test_new_index_types_roundtrip_metadata() {
        let sq = roundtrip_metadata(
            IVF_SQ_IDENTIFIER,
            &[("ivf-sq.dimension", "8"), ("ivf-sq.nlist", "4")],
        );
        assert_eq!(sq.index_type, IndexType::IvfSq);
        assert_eq!(
            (sq.dimension, sq.nlist, sq.total_vectors, sq.pq_bits),
            (8, 4, 512, Some(8))
        );

        let rq = roundtrip_metadata(
            IVF_RQ_IDENTIFIER,
            &[
                ("ivf-rq.dimension", "8"),
                ("ivf-rq.nlist", "4"),
                ("ivf-rq.rq.bits", "4"),
            ],
        );
        assert_eq!(rq.index_type, IndexType::IvfRq);
        assert_eq!(
            (rq.dimension, rq.nlist, rq.total_vectors, rq.rq_bits),
            (8, 4, 512, Some(4))
        );

        let diskann = roundtrip_metadata(
            DISKANN_IDENTIFIER,
            &[
                ("diskann.dimension", "8"),
                ("diskann.pq.m", "4"),
                ("diskann.pq.bits", "4"),
                ("diskann.max-degree", "8"),
                ("diskann.build-search-list-size", "16"),
                ("diskann.alpha", "1.2"),
                ("diskann.raw-vector-encoding", "f32"),
            ],
        );
        assert_eq!(diskann.index_type, IndexType::DiskAnn);
        assert_eq!(
            (
                diskann.dimension,
                diskann.total_vectors,
                diskann.pq_m,
                diskann.pq_bits
            ),
            (8, 512, Some(4), Some(4))
        );
        let diskann = diskann.diskann.unwrap();
        assert_eq!(
            (
                diskann.max_degree,
                diskann.build_search_list_size,
                diskann.alpha
            ),
            (8, 16, 1.2)
        );
    }

    #[test]
    fn test_vindex_options_field_options_override_shared_table_options() {
        let table_options = HashMap::from([
            ("ivf-flat.dimension".to_string(), "8".to_string()),
            ("ivf-flat.nlist".to_string(), "4".to_string()),
            ("fields.embedding.nlist".to_string(), "2".to_string()),
        ]);
        let user_options = HashMap::new();

        let options = VindexVectorIndexOptions::new(
            &table_options,
            &user_options,
            IVF_FLAT_IDENTIFIER,
            &array_float_field(),
        )
        .unwrap();

        assert_eq!(
            options.native_options.get("nlist").map(String::as_str),
            Some("2")
        );
    }

    #[test]
    fn test_vindex_options_train_sample_ratio_default_and_precedence() {
        let defaults = VindexVectorIndexOptions::new(
            &HashMap::new(),
            &HashMap::new(),
            IVF_FLAT_IDENTIFIER,
            &array_float_field(),
        )
        .unwrap();
        assert_eq!(defaults.train_sample_ratio, 1.0);

        let table_options = HashMap::from([
            ("ivf-flat.train.sample-ratio".to_string(), "0.5".to_string()),
            (
                "fields.embedding.train.sample-ratio".to_string(),
                "0.25".to_string(),
            ),
        ]);
        let user_options =
            HashMap::from([("ivf-flat.train.sample-ratio".to_string(), "0.1".to_string())]);
        let options = VindexVectorIndexOptions::new(
            &table_options,
            &user_options,
            IVF_FLAT_IDENTIFIER,
            &array_float_field(),
        )
        .unwrap();
        assert_eq!(options.train_sample_ratio, 0.1);

        let field_user_options = HashMap::from([
            ("ivf-flat.train.sample-ratio".to_string(), "0.5".to_string()),
            (
                "fields.embedding.train.sample-ratio".to_string(),
                "1.0".to_string(),
            ),
        ]);
        let options = VindexVectorIndexOptions::new(
            &HashMap::new(),
            &field_user_options,
            IVF_FLAT_IDENTIFIER,
            &array_float_field(),
        )
        .unwrap();
        assert_eq!(options.train_sample_ratio, 1.0);
        assert!(!options.native_options.contains_key("train.sample-ratio"));

        let diskann = VindexVectorIndexOptions::new(
            &HashMap::new(),
            &HashMap::from([("diskann.train.sample-ratio".to_string(), "0.5".to_string())]),
            DISKANN_IDENTIFIER,
            &array_float_field(),
        )
        .unwrap();
        assert_eq!(diskann.train_sample_ratio, 0.5);
    }

    #[test]
    fn test_vindex_options_reject_invalid_train_sample_ratio() {
        for value in ["0", "-0.1", "1.1", "NaN", "inf", "not-a-number"] {
            let user_options =
                HashMap::from([("ivf-flat.train.sample-ratio".to_string(), value.to_string())]);
            let err = VindexVectorIndexOptions::new(
                &HashMap::new(),
                &user_options,
                IVF_FLAT_IDENTIFIER,
                &array_float_field(),
            )
            .expect_err("invalid ratio should be rejected");
            assert!(
                matches!(err, crate::Error::ConfigInvalid { message } if message.contains("train.sample-ratio")),
                "value {value} returned unexpected error"
            );
        }
    }

    #[test]
    fn test_vindex_options_vector_type_uses_type_dimension() {
        let field = DataField::new(
            8,
            "embedding".to_string(),
            DataType::Vector(
                VectorType::try_new(true, 16, DataType::Float(FloatType::new())).unwrap(),
            ),
        );
        let table_options = HashMap::from([
            ("ivf-flat.dimension".to_string(), "128".to_string()),
            ("ivf-flat.nlist".to_string(), "4".to_string()),
        ]);
        let user_options = HashMap::new();

        let options = VindexVectorIndexOptions::new(
            &table_options,
            &user_options,
            IVF_FLAT_IDENTIFIER,
            &field,
        )
        .unwrap();

        assert_eq!(options.dimension(), 16);
        assert_eq!(
            options.native_options.get("dimension").map(String::as_str),
            Some("16")
        );
    }

    #[test]
    fn test_vindex_options_reject_mismatched_native_index_type() {
        let table_options = HashMap::new();
        let user_options = HashMap::from([
            ("index.type".to_string(), "ivf_flat".to_string()),
            ("dimension".to_string(), "8".to_string()),
            ("nlist".to_string(), "4".to_string()),
        ]);

        let err = VindexVectorIndexOptions::new(
            &table_options,
            &user_options,
            IVF_PQ_IDENTIFIER,
            &array_float_field(),
        )
        .expect_err("mismatched index.type should be rejected");

        assert!(matches!(err, crate::Error::ConfigInvalid { .. }));
    }

    #[test]
    fn test_vindex_options_reject_invalid_pq_config() {
        let table_options = HashMap::new();
        let user_options = HashMap::from([
            ("ivf-pq.dimension".to_string(), "7".to_string()),
            ("ivf-pq.nlist".to_string(), "4".to_string()),
            ("ivf-pq.pq.m".to_string(), "2".to_string()),
        ]);

        let err = VindexVectorIndexOptions::new(
            &table_options,
            &user_options,
            IVF_PQ_IDENTIFIER,
            &array_float_field(),
        )
        .expect_err("invalid native config should be rejected");

        assert!(
            matches!(err, crate::Error::DataInvalid { message, .. } if message.contains("dimension 7 must be divisible by m 2"))
        );
    }

    #[test]
    fn test_vindex_options_reject_unknown_user_options() {
        let table_options = HashMap::new();
        let user_options = HashMap::from([
            ("ivf-flat.dimension".to_string(), "8".to_string()),
            ("ivf-flat.nlsit".to_string(), "4".to_string()),
        ]);

        let err = VindexVectorIndexOptions::new(
            &table_options,
            &user_options,
            IVF_FLAT_IDENTIFIER,
            &array_float_field(),
        )
        .expect_err("unknown user option should be rejected");

        assert!(
            matches!(err, crate::Error::ConfigInvalid { message } if message.contains("ivf-flat.nlsit"))
        );
    }

    #[test]
    fn test_vindex_options_reject_non_applicable_user_options() {
        for (index_type, key) in [
            (IVF_FLAT_IDENTIFIER, "ivf-flat.pq.m"),
            (IVF_FLAT_IDENTIFIER, "ivf-flat.ivf.pq-encoding"),
            (
                IVF_FLAT_IDENTIFIER,
                "ivf-flat.pq.train.max-points-per-centroid",
            ),
            (IVF_FLAT_IDENTIFIER, "diskann.max-degree"),
            (DISKANN_IDENTIFIER, "diskann.nlist"),
            (DISKANN_IDENTIFIER, "diskann.ivf.coarse-assignment"),
            (
                DISKANN_IDENTIFIER,
                "diskann.ivf.train.max-points-per-centroid",
            ),
        ] {
            let user_options = HashMap::from([(key.to_string(), "2".to_string())]);
            let err = VindexVectorIndexOptions::new(
                &HashMap::new(),
                &user_options,
                index_type,
                &array_float_field(),
            )
            .expect_err("non-applicable user option should be rejected");

            assert!(
                matches!(err, crate::Error::ConfigInvalid { message } if message.contains(key))
            );
        }
    }

    #[test]
    fn test_vindex_options_defaults_align_java_docs() {
        let options = VindexVectorIndexOptions::new(
            &HashMap::new(),
            &HashMap::new(),
            IVF_FLAT_IDENTIFIER,
            &array_float_field(),
        )
        .unwrap();

        assert_eq!(
            options.native_options.get("dimension").map(String::as_str),
            Some("128")
        );
        assert_eq!(
            options.native_options.get("metric").map(String::as_str),
            Some("inner_product")
        );
        assert_eq!(
            options.native_options.get("nlist").map(String::as_str),
            Some("256")
        );
    }

    #[test]
    fn test_native_index_type_helper() {
        assert_eq!(native_index_type(IVF_FLAT_IDENTIFIER), Some("ivf_flat"));
        assert_eq!(native_index_type(IVF_PQ_IDENTIFIER), Some("ivf_pq"));
        assert_eq!(native_index_type("ivf-sq"), Some("ivf_sq"));
        assert_eq!(native_index_type("ivf-rq"), Some("ivf_rq"));
        assert_eq!(native_index_type("diskann"), Some("diskann"));
        assert_eq!(native_index_type("ivf-hnsw-flat"), None);
        assert_eq!(native_index_type("ivf-hnsw-sq"), None);
        assert_eq!(native_index_type("btree"), None);
    }

    #[test]
    fn test_array_field_helper_is_not_vector() {
        assert!(matches!(
            array_float_field().data_type(),
            DataType::Array(_)
        ));
    }
}
