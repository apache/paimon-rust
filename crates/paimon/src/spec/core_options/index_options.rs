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

//! Configuration contracts shared by PK and Data Evolution index consumers.

use super::CoreOptions;
use crate::vindex::pkvector::metric::normalize_metric as normalize_metric_name;
use crate::{Error, Result};
use std::collections::HashMap;

fn normalize_metric(value: &str) -> String {
    normalize_metric_name(value.trim())
}

fn vector(kind: &str) -> bool {
    !matches!(kind, "btree" | "bitmap" | "multivalue" | "fm" | "full-text")
}

fn columns(raw: Option<&String>) -> Vec<String> {
    raw.map(|raw| raw.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default()
}

fn invalid(message: impl Into<String>) -> Error {
    Error::ConfigInvalid {
        message: message.into(),
    }
}

/// One captured vector configuration. A supplied metadata implementation wins
/// over declarations; readers can leave an unspecified metric out of `options`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VectorIndexOptions {
    pub index_type: Option<String>,
    pub options: HashMap<String, String>,
    pub metric: String,
}

impl CoreOptions<'_> {
    /// Declared implementation identifiers, in deterministic order.
    pub fn global_index_types(&self) -> Vec<String> {
        let mut types = self
            .options
            .keys()
            .filter_map(|key| {
                key.strip_prefix("global-index.")
                    .and_then(|s| {
                        s.strip_suffix(".index-columns")
                            .or_else(|| s.strip_suffix(".index-column"))
                    })
                    .map(str::to_string)
            })
            .collect::<Vec<_>>();
        types.sort();
        types.dedup();
        types
    }

    /// Canonical plural, singular DLF alias, then the legacy PK declaration.
    /// Preserve empty tokens so validation cannot silently disable malformed declarations.
    pub fn global_index_columns(&self, kind: &str) -> Vec<String> {
        for key in [
            format!("global-index.{kind}.index-columns"),
            format!("global-index.{kind}.index-column"),
            format!("pk-{kind}.index.columns"),
        ] {
            if let Some(raw) = self.options.get(&key) {
                return columns(Some(raw));
            }
        }
        if vector(kind) && kind != "vector" {
            return columns(self.options.get("pk-vector.index.columns"))
                .into_iter()
                .filter(|col| {
                    self.options
                        .get(&format!("fields.{col}.pk-vector.index.type"))
                        .is_some_and(|v| v.trim() == kind)
                })
                .collect();
        }
        Vec::new()
    }

    pub fn vector_index_columns(&self) -> Vec<String> {
        let types = self
            .global_index_types()
            .into_iter()
            .filter(|t| vector(t) && t != "vector")
            .collect::<Vec<_>>();
        if types.is_empty() {
            self.global_index_columns("vector")
        } else {
            types
                .iter()
                .flat_map(|kind| self.global_index_columns(kind))
                .collect()
        }
    }

    /// Resolve the implementation and its parameters together. Explicit types
    /// from captured metadata take precedence without consulting declarations.
    pub fn vector_index_options(
        &self,
        kind: Option<&str>,
        column: &str,
        default_metric: bool,
    ) -> Result<VectorIndexOptions> {
        let selected = if let Some(kind) = kind {
            Some(kind.trim().to_ascii_lowercase())
        } else {
            let declared = self
                .global_index_types()
                .into_iter()
                .filter(|kind| {
                    vector(kind)
                        && kind != "vector"
                        && self.global_index_columns(kind).iter().any(|c| c == column)
                })
                .collect::<Vec<_>>();
            if declared.len() > 1 {
                return Err(invalid(format!(
                    "Multiple vector index types declared for '{column}'"
                )));
            }
            if let Some(kind) = declared.into_iter().next() {
                Some(kind)
            } else {
                let legacy_pk = columns(self.options.get("pk-vector.index.columns"))
                    .iter()
                    .any(|declared| declared == column);
                self.options
                    .get(&format!("fields.{column}.global-index.vector.index-type"))
                    .or_else(|| {
                        legacy_pk
                            .then(|| {
                                self.options
                                    .get(&format!("fields.{column}.pk-vector.index.type"))
                            })
                            .flatten()
                    })
                    .map(|s| s.trim().to_ascii_lowercase())
                    .filter(|s| !s.is_empty())
            }
        };
        let Some(kind) = selected else {
            let metric = normalize_metric(
                self.options
                    .get(&format!("fields.{column}.pk-vector.distance.metric"))
                    .map(String::as_str)
                    .unwrap_or("inner_product"),
            );
            crate::vindex::pkvector::metric::VectorSearchMetric::parse(&metric)?;
            return Ok(VectorIndexOptions {
                index_type: None,
                options: self.options.clone(),
                metric,
            });
        };
        self.resolve_vector_parameters(kind, column, default_metric)
    }

    /// Resolve implementation defaults and one field's JSON without mutating
    /// table options. Metric aliases are resolved separately as one parameter.
    pub fn global_index_options(
        &self,
        kind: &str,
        column: &str,
    ) -> Result<HashMap<String, String>> {
        let is_vector = vector(kind);
        let family = if is_vector { "vector" } else { kind };
        let prefix = if is_vector || kind == "full-text" {
            format!("{kind}.")
        } else {
            format!("{kind}-index.")
        };
        let field = format!("fields.{column}.");
        let shared_key = format!("{field}index-options");
        let rust_key = format!("{field}global-index.{family}.index-options");
        let legacy_key = format!("{field}pk-{family}.index.options");
        let shared = self.options.contains_key(&shared_key);
        let key = if shared {
            &shared_key
        } else if self.options.contains_key(&rust_key) {
            &rust_key
        } else {
            &legacy_key
        };
        let mut resolved = self.options.clone();
        if kind == "full-text" {
            for (_, value) in resolved
                .iter_mut()
                .filter(|(key, _)| key.starts_with("full-text."))
            {
                if let Ok(values) = serde_json::from_str::<Vec<serde_json::Value>>(value) {
                    *value = values
                        .into_iter()
                        .map(|value| {
                            value
                                .as_str()
                                .map(str::to_string)
                                .unwrap_or_else(|| value.to_string())
                        })
                        .collect::<Vec<_>>()
                        .join(";");
                }
            }
        }
        if let Some(value) = self
            .options
            .get(&format!("{field}global-index.row-count-per-shard"))
        {
            resolved.insert("global-index.row-count-per-shard".into(), value.clone());
        }
        if let Some(raw) = self.options.get(key).filter(|s| !s.trim().is_empty()) {
            let parsed: indexmap::IndexMap<String, serde_json::Value> = serde_json::from_str(raw)
                .map_err(|_| {
                invalid(format!(
                    "{key} must be a JSON object of option key-value pairs"
                ))
            })?;
            for (name, value) in parsed {
                if name.trim().is_empty() {
                    return Err(invalid(format!("{key} contains an empty option key")));
                }
                let value = match value {
                    serde_json::Value::Null => {
                        return Err(invalid(format!(
                            "{key} contains an invalid option value for {name}"
                        )))
                    }
                    serde_json::Value::String(value) => value,
                    serde_json::Value::Array(values) if shared && kind == "full-text" => values
                        .into_iter()
                        .map(|v| {
                            v.as_str()
                                .map(str::to_string)
                                .unwrap_or_else(|| v.to_string())
                        })
                        .collect::<Vec<_>>()
                        .join(";"),
                    value if shared || value.is_number() || value.is_boolean() => value.to_string(),
                    _ => {
                        return Err(invalid(format!(
                            "{key} contains an invalid option value for {name}"
                        )))
                    }
                };
                let qualified = if name.starts_with(&prefix)
                    || name.starts_with("fields.")
                    || name.starts_with("global-index.")
                    || name.starts_with("sorted-index.")
                    || (is_vector
                        && matches!(name.as_str(), "index.type" | "vindex.build.granule.enabled"))
                {
                    name
                } else {
                    format!("{prefix}{name}")
                };
                if !shared
                    && !qualified.starts_with("global-index.")
                    && resolved
                        .get(&qualified)
                        .is_some_and(|previous| previous != &value)
                {
                    return Err(invalid(format!(
                        "Conflicting field index options for {column}: {qualified}"
                    )));
                }
                resolved.insert(qualified.clone(), value.clone());
                if is_vector {
                    if let Some(short) = qualified.strip_prefix(&prefix) {
                        resolved.insert(format!("{field}{short}"), value);
                    }
                }
            }
        }
        Ok(resolved)
    }

    /// Merge parameters only after the implementation has been selected.
    fn resolve_vector_parameters(
        &self,
        kind: String,
        column: &str,
        default_metric: bool,
    ) -> Result<VectorIndexOptions> {
        if !vector(&kind) {
            return Err(invalid(format!("Unsupported vector index type: {kind}")));
        }
        let field = format!("fields.{column}.");
        let prefix = format!("{kind}.");
        let aliases = [
            format!("{field}distance.metric"),
            format!("{field}metric"),
            format!("{prefix}distance.metric"),
            format!("{prefix}metric"),
        ];
        let mut metric = [
            format!("{field}distance.metric"),
            format!("{field}metric"),
            format!("{field}global-index.vector.distance-metric"),
            format!("{field}pk-vector.distance.metric"),
            format!("{prefix}distance.metric"),
            format!("{prefix}metric"),
            "distance.metric".into(),
            "metric".into(),
        ]
        .iter()
        .find_map(|key| self.options.get(key).cloned());
        let mut defaults = self.options.clone();
        for key in &aliases {
            defaults.remove(key);
        }
        let mut resolved = CoreOptions::new(&defaults).global_index_options(&kind, column)?;
        metric = aliases
            .iter()
            .find_map(|key| resolved.get(key).cloned())
            .or(metric);
        // Preserve the former Rust family declaration's metric/default priority.
        let legacy_rust = !self.options.contains_key(&format!("{field}index-options"))
            && (self
                .options
                .contains_key("global-index.vector.index-columns")
                || self
                    .options
                    .contains_key("global-index.vector.index-column"))
            && !self
                .global_index_types()
                .iter()
                .any(|kind| vector(kind) && kind != "vector");
        let legacy_metric = legacy_rust.then(|| {
            self.options
                .get(&format!("{field}global-index.vector.distance-metric"))
                .cloned()
                .unwrap_or_else(|| "inner_product".into())
        });
        if default_metric && legacy_rust {
            metric = legacy_metric.clone();
        }
        if metric.is_some() || default_metric {
            let metric = normalize_metric(metric.as_deref().unwrap_or("inner_product"));
            crate::vindex::pkvector::metric::VectorSearchMetric::parse(&metric)?;
            for key in aliases {
                resolved.insert(key, metric.clone());
            }
        }
        Ok(VectorIndexOptions {
            index_type: Some(kind),
            options: resolved,
            metric: normalize_metric(
                legacy_metric
                    .as_deref()
                    .or(metric.as_deref())
                    .unwrap_or("inner_product"),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(values: &[(&str, &str)]) -> HashMap<String, String> {
        values
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn vector_resolution_captures_type_options_and_metric_atomically() {
        let values = map(&[
            ("global-index.ivf-pq.index-columns", "embedding"),
            ("ivf-pq.dimension", "16"),
            ("ivf-pq.nlist", "8"),
            ("ivf-pq.metric", "l2"),
            (
                "fields.embedding.pk-vector.index.options",
                r#"{"dimension": 99, "m": 4}"#,
            ),
            (
                "fields.embedding.index-options",
                r#"{"dimension": 2, "nlist": 1, "metric": "COSINE"}"#,
            ),
        ]);
        let before = values.clone();
        let resolved = CoreOptions::new(&values)
            .vector_index_options(None, "embedding", true)
            .unwrap();
        assert_eq!(resolved.index_type.as_deref(), Some("ivf-pq"));
        assert_eq!(resolved.metric, "cosine");
        assert_eq!(resolved.options["ivf-pq.dimension"], "2");
        assert_eq!(resolved.options["ivf-pq.nlist"], "1");
        assert_eq!(resolved.options["ivf-pq.metric"], "cosine");
        assert!(
            !resolved.options.contains_key("ivf-pq.m"),
            "new JSON replaces the whole legacy JSON"
        );
        let restored: VectorIndexOptions =
            serde_json::from_str(&serde_json::to_string(&resolved).unwrap()).unwrap();
        assert_eq!(restored, resolved);
        assert_eq!(values, before);
    }

    #[test]
    fn captured_format_ignores_declaration_changes_without_injecting_query_metric() {
        let values = map(&[
            ("global-index.ivf-pq.index-columns", "embedding"),
            ("global-index.diskann.index-columns", "embedding"),
            ("diskann.metric", "invalid"),
            ("ivf-flat.nlist", "3"),
            ("fields.embedding.index-options", r#"{"nlist": 1}"#),
        ]);
        assert!(CoreOptions::new(&values)
            .vector_index_options(None, "embedding", false)
            .is_err());
        let resolved = CoreOptions::new(&values)
            .vector_index_options(Some("ivf-flat"), "embedding", false)
            .unwrap();
        assert_eq!(resolved.index_type.as_deref(), Some("ivf-flat"));
        assert_eq!(resolved.options["ivf-flat.nlist"], "1");
        assert_eq!(resolved.metric, "inner_product");
        for key in [
            "ivf-flat.metric",
            "ivf-flat.distance.metric",
            "fields.embedding.metric",
            "fields.embedding.distance.metric",
        ] {
            assert!(!resolved.options.contains_key(key), "{key}");
        }
    }

    #[test]
    fn legacy_rust_result_preserves_build_metric_without_query_defaults() {
        let mut values = map(&[
            ("global-index.vector.index-columns", "embedding"),
            (
                "fields.embedding.global-index.vector.index-type",
                "ivf-flat",
            ),
            ("ivf-flat.metric", "l2"),
        ]);
        let read = CoreOptions::new(&values)
            .vector_index_options(None, "embedding", false)
            .unwrap();
        let build = CoreOptions::new(&values)
            .vector_index_options(None, "embedding", true)
            .unwrap();
        assert_eq!(read.metric, build.metric);
        assert_eq!(read.metric, "inner_product");
        assert_eq!(
            read.options["ivf-flat.metric"], "l2",
            "retain explicit read parameters"
        );
        values.remove("ivf-flat.metric");
        let query = CoreOptions::new(&values)
            .vector_index_options(None, "embedding", false)
            .unwrap();
        assert!(!query.options.contains_key("ivf-flat.metric"));
        assert_eq!(query.metric, "inner_product");
    }

    #[test]
    fn shared_schema_guards_apply_to_pk_but_allow_append_and_de_type_evolution() {
        use crate::spec::{BigIntType, DataType, IntType, Schema, SchemaChange, TableSchema};
        for suffix in ["index-columns", "index-column"] {
            for mode in ["pk", "append", "de"] {
                let mut schema = Schema::builder()
                    .column("id", DataType::Int(IntType::new()))
                    .column("value", DataType::Int(IntType::new()))
                    .option(format!("global-index.btree.{suffix}"), "value");
                if mode == "pk" {
                    schema = schema
                        .primary_key(["id"])
                        .option("bucket", "1")
                        .option("deletion-vectors.enabled", "true");
                } else {
                    schema = schema
                        .option("row-tracking.enabled", "true")
                        .option("data-evolution.enabled", (mode == "de").to_string());
                }
                let schema = TableSchema::new(0, &schema.build().unwrap());
                let change = SchemaChange::update_column_type(
                    "value".into(),
                    DataType::BigInt(BigIntType::new()),
                );
                let result = schema.apply_changes(vec![change]);
                if mode == "pk" {
                    assert!(result
                        .unwrap_err()
                        .to_string()
                        .contains("primary-key index column"));
                } else {
                    result.unwrap();
                }
            }
        }
    }

    #[test]
    fn dlf_dimension_aliases_have_stable_field_precedence() {
        use crate::spec::{ArrayType, DataField, DataType, FloatType};
        let field = DataField::new(
            1,
            "embedding".into(),
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        );
        let values = map(&[
            ("ivf-flat.dimension", "4"),
            ("ivf-flat.index.dimension", "5"),
            ("fields.embedding.dimension", "2"),
            ("fields.embedding.index.dimension", "3"),
        ]);
        let native = crate::vindex::VindexVectorIndexOptions::new(
            &values,
            &HashMap::new(),
            "ivf-flat",
            &field,
        )
        .unwrap();
        assert_eq!(native.dimension(), 2);
        let values = map(&[
            ("ivf-flat.dimension", "4"),
            ("ivf-flat.index.dimension", "5"),
        ]);
        let native = crate::vindex::VindexVectorIndexOptions::new(
            &values,
            &HashMap::new(),
            "ivf-flat",
            &field,
        )
        .unwrap();
        assert_eq!(native.dimension(), 4);
        let values = map(&[
            ("ivf-flat.index.dimension", "5"),
            ("fields.embedding.index.dimension", "3"),
        ]);
        let native = crate::vindex::VindexVectorIndexOptions::new(
            &values,
            &HashMap::new(),
            "ivf-flat",
            &field,
        )
        .unwrap();
        assert_eq!(native.dimension(), 3);
    }

    #[test]
    fn declaration_precedence_and_legacy_vector_selection() {
        for kind in [
            "btree",
            "bitmap",
            "multivalue",
            "fm",
            "full-text",
            "ivf-flat",
        ] {
            let family = if vector(kind) { "vector" } else { kind };
            let mut values = map(&[
                (&format!("pk-{family}.index.columns"), "old"),
                ("fields.old.pk-vector.index.type", "ivf-flat"),
            ]);
            assert_eq!(
                CoreOptions::new(&values).global_index_columns(kind),
                vec!["old"]
            );
            values.insert(
                format!("global-index.{kind}.index-column"),
                "singular".into(),
            );
            assert_eq!(
                CoreOptions::new(&values).global_index_columns(kind),
                vec!["singular"]
            );
            values.insert(
                format!("global-index.{kind}.index-columns"),
                " first, second ".into(),
            );
            assert_eq!(
                CoreOptions::new(&values).global_index_columns(kind),
                vec!["first", "second"]
            );
            if vector(kind) {
                let core = CoreOptions::new(&values);
                assert_eq!(core.vector_index_columns(), vec!["first", "second"]);
                assert_eq!(
                    core.vector_index_options(None, "first", false)
                        .map(|resolved| resolved.index_type.ok_or_else(|| {
                            crate::Error::ConfigInvalid {
                                message: "Vector index implementation is not configured".into(),
                            }
                        }))
                        .and_then(|kind| kind)
                        .unwrap(),
                    kind
                );
            }
            values.insert(format!("global-index.{kind}.index-columns"), "".into());
            assert_eq!(
                CoreOptions::new(&values).global_index_columns(kind),
                vec![""]
            );
        }
    }

    #[test]
    fn scalar_algorithm_in_vector_declaration_is_an_error() {
        for kind in ["btree", "bitmap", "multivalue", "fm", "full-text"] {
            let values = map(&[
                ("pk-vector.index.columns", "embedding"),
                ("fields.embedding.pk-vector.index.type", kind),
            ]);
            assert!(CoreOptions::new(&values)
                .vector_index_options(None, "embedding", true)
                .map(|resolved| resolved.metric)
                .unwrap_err()
                .to_string()
                .contains("Unsupported vector index type"));
        }
    }

    #[test]
    fn shared_json_aliases_follow_java_insertion_order() {
        for (json, expected) in [
            (
                r#"{"block-size":"4 kb","btree-index.block-size":"8 kb"}"#,
                "8 kb",
            ),
            (
                r#"{"btree-index.block-size":"8 kb","block-size":"4 kb"}"#,
                "4 kb",
            ),
        ] {
            let values = map(&[("fields.amount.index-options", json)]);
            assert_eq!(
                CoreOptions::new(&values)
                    .global_index_options("btree", "amount")
                    .unwrap()["btree-index.block-size"],
                expected
            );
        }
    }

    #[test]
    fn vector_json_alias_priority_is_independent_of_json_key_order() {
        for suffix in ["index-options", "pk-vector.index.options"] {
            for json in [
                r#"{"metric":"L2","distance.metric":"COSINE"}"#,
                r#"{"distance.metric":"COSINE","metric":"L2"}"#,
            ] {
                let values = map(&[
                    ("fields.embedding.pk-vector.distance.metric", "l2"),
                    (&format!("fields.embedding.{suffix}"), json),
                ]);
                let resolved = CoreOptions::new(&values)
                    .vector_index_options(Some("ivf-pq"), "embedding", true)
                    .unwrap()
                    .options;
                for key in [
                    "fields.embedding.distance.metric",
                    "fields.embedding.metric",
                    "ivf-pq.distance.metric",
                    "ivf-pq.metric",
                ] {
                    assert_eq!(resolved[key], "cosine");
                }
                assert_eq!(
                    CoreOptions::new(&resolved)
                        .vector_index_options(Some("ivf-pq"), "embedding", true)
                        .unwrap()
                        .options,
                    resolved
                );
            }
        }
    }

    #[test]
    fn shared_field_json_overrides_defaults_and_replaces_invalid_legacy_json() {
        for (kind, prefix, key) in [
            ("btree", "btree-index.", "block-size"),
            ("bitmap", "bitmap-index.", "dictionary-block-size"),
            ("multivalue", "multivalue-index.", "dictionary-block-size"),
            ("fm", "fm-index.", "sa-sample-rate"),
            ("full-text", "full-text.", "tokenizer"),
            ("ivf-pq", "ivf-pq.", "nlist"),
        ] {
            let values = map(&[
                (&format!("{prefix}{key}"), "32"),
                (
                    "fields.payload.index-options",
                    &format!(r#"{{"{key}":"64"}}"#),
                ),
                (
                    &format!(
                        "fields.payload.pk-{}.index.options",
                        if vector(kind) { "vector" } else { kind }
                    ),
                    "invalid",
                ),
                ("sorted-index.records-per-file", "20"),
            ]);
            let resolved = CoreOptions::new(&values)
                .global_index_options(kind, "payload")
                .unwrap();
            assert_eq!(resolved[&format!("{prefix}{key}")], "64");
            assert_eq!(resolved["sorted-index.records-per-file"], "20");
            assert_eq!(
                CoreOptions::new(&resolved)
                    .global_index_options(kind, "payload")
                    .unwrap(),
                resolved
            );
            assert_eq!(values[&format!("{prefix}{key}")], "32");
        }
    }

    #[test]
    fn dlf_flat_vector_parameters_and_shared_json_keep_metric_aliases_consistent() {
        let mut values = map(&[
            ("global-index.ivf-pq.index-column", "embedding"),
            ("ivf-pq.dimension", "768"),
            ("ivf-pq.distance.metric", "l2"),
            ("ivf-pq.nlist", "32"),
            ("fields.embedding.nlist", "48"),
            ("fields.embedding.distance.metric", "cosine"),
        ]);
        assert_eq!(
            CoreOptions::new(&values)
                .vector_index_options(None, "embedding", true)
                .map(|resolved| resolved.metric)
                .unwrap(),
            "cosine"
        );
        values.insert(
            "fields.embedding.index-options".into(),
            r#"{"nlist":64,"distance.metric":"Inner-Product"}"#.into(),
        );
        let resolved = CoreOptions::new(&values)
            .vector_index_options(Some("ivf-pq"), "embedding", true)
            .unwrap()
            .options;
        for key in [
            "ivf-pq.metric",
            "ivf-pq.distance.metric",
            "fields.embedding.metric",
            "fields.embedding.distance.metric",
        ] {
            assert_eq!(resolved[key], "inner_product");
        }
        assert_eq!(resolved["ivf-pq.dimension"], "768");
        assert_eq!(resolved["ivf-pq.nlist"], "64");
        assert_eq!(resolved["fields.embedding.nlist"], "64");
        assert_eq!(
            CoreOptions::new(&values)
                .vector_index_options(None, "embedding", true)
                .map(|resolved| resolved.metric)
                .unwrap(),
            "inner_product"
        );
    }

    #[test]
    fn full_text_arrays_and_boolean_values_use_native_encoding() {
        let values = map(&[(
            "fields.text.index-options",
            r#"{"lower-case":false,"stop-words":["paimon","lake"]}"#,
        )]);
        let resolved = CoreOptions::new(&values)
            .global_index_options("full-text", "text")
            .unwrap();
        assert_eq!(resolved["full-text.lower-case"], "false");
        assert_eq!(resolved["full-text.stop-words"], "paimon;lake");
    }

    #[test]
    fn malformed_shared_json_is_not_hidden_by_valid_legacy_values() {
        for raw in ["[]", r#"{"nlist":null}"#, r#"{"":64}"#, "invalid"] {
            let values = map(&[
                ("fields.embedding.index-options", raw),
                ("fields.embedding.pk-vector.index.options", r#"{"nlist":1}"#),
            ]);
            assert!(CoreOptions::new(&values)
                .vector_index_options(Some("ivf-flat"), "embedding", true)
                .unwrap_err()
                .to_string()
                .contains("fields.embedding.index-options"));
        }
        let values = map(&[("fields.embedding.index-options", r#"{"nlist":64}"#)]);
        let resolved = CoreOptions::new(&values)
            .vector_index_options(Some("ivf-pq"), "embedding", false)
            .unwrap()
            .options;
        assert!(!resolved.contains_key("ivf-pq.metric"));
    }

    #[test]
    fn legacy_json_conflicts_and_rust_de_aliases_remain_supported() {
        let mut values = map(&[
            ("btree-index.block-size", "4 kb"),
            (
                "fields.amount.pk-btree.index.options",
                r#"{"block-size":"8 kb"}"#,
            ),
        ]);
        assert!(CoreOptions::new(&values)
            .global_index_options("btree", "amount")
            .is_err());
        values = map(&[
            ("global-index.vector.index-columns", "embedding"),
            (
                "fields.embedding.global-index.vector.index-type",
                "ivf-flat",
            ),
            ("fields.embedding.global-index.vector.distance-metric", "l2"),
            (
                "fields.embedding.global-index.vector.index-options",
                r#"{"dimension":2,"nlist":1}"#,
            ),
        ]);
        let core = CoreOptions::new(&values);
        assert_eq!(
            core.vector_index_options(None, "embedding", false)
                .map(|resolved| resolved
                    .index_type
                    .ok_or_else(|| crate::Error::ConfigInvalid {
                        message: "Vector index implementation is not configured".into()
                    }))
                .and_then(|kind| kind)
                .unwrap(),
            "ivf-flat"
        );
        let resolved = core
            .vector_index_options(Some("ivf-flat"), "embedding", true)
            .unwrap()
            .options;
        assert_eq!(resolved["ivf-flat.dimension"], "2");
        assert_eq!(resolved["ivf-flat.metric"], "l2");
    }

    #[test]
    fn removed_vector_type_hints_do_not_select_an_implementation() {
        for key in [
            "index_type",
            "index-type",
            "vector.index-type",
            "fields.embedding.index-type",
        ] {
            let mut values = map(&[(key, "diskann")]);
            assert_eq!(
                CoreOptions::new(&values)
                    .vector_index_options(None, "embedding", false)
                    .map(|resolved| resolved.index_type)
                    .unwrap(),
                None
            );
            values.insert(
                "global-index.ivf-flat.index-columns".into(),
                "embedding".into(),
            );
            assert_eq!(
                CoreOptions::new(&values)
                    .vector_index_options(None, "embedding", false)
                    .map(|resolved| resolved.index_type)
                    .unwrap()
                    .as_deref(),
                Some("ivf-flat")
            );
        }
    }

    #[test]
    fn legacy_pk_vector_type_requires_the_complete_column_declaration() {
        let mut values = map(&[("fields.embedding.pk-vector.index.type", " IVF-FLAT ")]);
        for columns in [None, Some(""), Some("other")] {
            if let Some(columns) = columns {
                values.insert("pk-vector.index.columns".into(), columns.into());
            } else {
                values.remove("pk-vector.index.columns");
            }
            assert_eq!(
                CoreOptions::new(&values)
                    .vector_index_options(None, "embedding", false)
                    .map(|resolved| resolved.index_type)
                    .unwrap(),
                None
            );
        }
        values.insert(
            "pk-vector.index.columns".into(),
            " other, embedding ".into(),
        );
        assert_eq!(
            CoreOptions::new(&values)
                .vector_index_options(None, "embedding", false)
                .map(|resolved| resolved.index_type)
                .unwrap()
                .as_deref(),
            Some("ivf-flat")
        );
        values.remove("fields.embedding.pk-vector.index.type");
        assert_eq!(
            CoreOptions::new(&values)
                .vector_index_options(None, "embedding", false)
                .map(|resolved| resolved.index_type)
                .unwrap(),
            None
        );
        values.insert(
            "fields.embedding.pk-vector.index.type".into(),
            "diskann".into(),
        );
        for suffix in ["index-columns", "index-column"] {
            values.insert(format!("global-index.ivf-pq.{suffix}"), "embedding".into());
            assert_eq!(
                CoreOptions::new(&values)
                    .vector_index_options(None, "embedding", false)
                    .map(|resolved| resolved.index_type)
                    .unwrap()
                    .as_deref(),
                Some("ivf-pq")
            );
            values.remove(&format!("global-index.ivf-pq.{suffix}"));
        }
    }

    #[test]
    fn short_field_options_replace_old_json_and_override_prefixed_defaults_without_query_metric() {
        let values = map(&[
            ("global-index.ivf-pq.index-column", "embedding"),
            ("ivf-pq.dimension", "8"),
            ("ivf-pq.nlist", "16"),
            ("fields.embedding.dimension", "4"),
            (
                "fields.embedding.pk-vector.index.options",
                r#"{"pq.m":3,"nlist":32}"#,
            ),
            (
                "fields.embedding.index-options",
                r#"{"dimension":2,"nlist":1}"#,
            ),
        ]);
        let resolved = CoreOptions::new(&values)
            .vector_index_options(Some("ivf-pq"), "embedding", false)
            .unwrap()
            .options;
        assert_eq!(resolved["ivf-pq.nlist"], "1");
        assert_eq!(resolved["fields.embedding.dimension"], "2");
        assert!(!resolved.contains_key("ivf-pq.pq.m"));
        assert!(!resolved.contains_key("ivf-pq.metric"));
        assert!(!resolved.contains_key("fields.embedding.metric"));
        let defaults = CoreOptions::new(&values)
            .vector_index_options(Some("ivf-pq"), "embedding", true)
            .unwrap()
            .options;
        assert_eq!(defaults["ivf-pq.metric"], "inner_product");
        assert_eq!(values["ivf-pq.dimension"], "8");
    }
}
