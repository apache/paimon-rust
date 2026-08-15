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

pub const IVF_FLAT_IDENTIFIER: &str = "ivf-flat";
pub const IVF_PQ_IDENTIFIER: &str = "ivf-pq";

const DEFAULT_DIMENSION: &str = "128";
const DEFAULT_METRIC: &str = "inner_product";
const DEFAULT_NLIST: &str = "256";
const DEFAULT_PQ_M: &str = "16";
const DEFAULT_PQ_USE_OPQ: &str = "false";
const DEFAULT_TRAIN_SAMPLE_RATIO: f64 = 1.0;

pub fn is_vindex_index_type(index_type: &str) -> bool {
    matches!(index_type, IVF_FLAT_IDENTIFIER | IVF_PQ_IDENTIFIER)
}

pub(crate) fn native_index_type(index_type: &str) -> Option<&'static str> {
    match index_type {
        IVF_FLAT_IDENTIFIER => Some("ivf_flat"),
        IVF_PQ_IDENTIFIER => Some("ivf_pq"),
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
        "dimension" | "nlist" | "metric" => true,
        "pq.m" | "use-opq" => index_type == IVF_PQ_IDENTIFIER,
        _ => false,
    }
}

fn is_allowed_paimon_suffix(suffix: &str, index_type: &str) -> bool {
    match suffix {
        "dimension" | "nlist" | "distance.metric" | "train.sample-ratio" => true,
        "pq.m" | "pq.use-opq" => index_type == IVF_PQ_IDENTIFIER,
        _ => false,
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

    fn array_float_field() -> DataField {
        DataField::new(
            7,
            "embedding".to_string(),
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
    }

    #[test]
    fn test_vindex_index_type_identifier_helper() {
        assert!(is_vindex_index_type(IVF_FLAT_IDENTIFIER));
        assert!(is_vindex_index_type(IVF_PQ_IDENTIFIER));
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
        let table_options = HashMap::new();
        let user_options = HashMap::from([
            ("ivf-flat.dimension".to_string(), "8".to_string()),
            ("ivf-flat.nlist".to_string(), "4".to_string()),
            ("ivf-flat.pq.m".to_string(), "2".to_string()),
        ]);

        let err = VindexVectorIndexOptions::new(
            &table_options,
            &user_options,
            IVF_FLAT_IDENTIFIER,
            &array_float_field(),
        )
        .expect_err("non-applicable user option should be rejected");

        assert!(
            matches!(err, crate::Error::ConfigInvalid { message } if message.contains("ivf-flat.pq.m"))
        );
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
