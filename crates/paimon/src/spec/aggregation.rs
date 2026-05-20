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

use std::collections::HashMap;

const MERGE_ENGINE_OPTION: &str = "merge-engine";
const AGGREGATION_ENGINE: &str = "aggregation";
const IGNORE_DELETE_OPTION: &str = "ignore-delete";
const IGNORE_DELETE_SUFFIX: &str = ".ignore-delete";
const AGGREGATION_REMOVE_RECORD_ON_DELETE_OPTION: &str = "aggregation.remove-record-on-delete";
const FIELDS_DEFAULT_AGG_FUNCTION_OPTION: &str = "fields.default-aggregate-function";
const FIELDS_PREFIX: &str = "fields.";
const AGG_FUNCTION_SUFFIX: &str = ".aggregate-function";
const IGNORE_RETRACT_SUFFIX: &str = ".ignore-retract";
const DISTINCT_SUFFIX: &str = ".distinct";
const SEQUENCE_GROUP_SUFFIX: &str = ".sequence-group";
const NESTED_KEY_SUFFIX: &str = ".nested-key";
const COUNT_LIMIT_SUFFIX: &str = ".count-limit";

/// Minimal aggregation mode recognized by the current Rust implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AggregationMode {
    Basic,
}

/// Aggregation-merge-engine option inspection and validation.
///
/// The basic mode accepts only `merge-engine=aggregation` on a PK table with
/// the following option keys:
/// - `fields.default-aggregate-function`
/// - `fields.<col>.aggregate-function`
/// - `fields.<col>.list-agg-delimiter`
///
/// All other aggregation-specific knobs (`ignore-retract`, `distinct`,
/// `nested-key`, `count-limit`, `aggregation.remove-record-on-delete`,
/// `sequence-group`, `ignore-delete`) are rejected.  Retract rows
/// (DELETE / UPDATE_BEFORE) are rejected at runtime by the merge function.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AggregationConfig<'a> {
    options: &'a HashMap<String, String>,
}

impl<'a> AggregationConfig<'a> {
    pub(crate) fn new(options: &'a HashMap<String, String>) -> Self {
        Self { options }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.options
            .get(MERGE_ENGINE_OPTION)
            .is_some_and(|value| value.eq_ignore_ascii_case(AGGREGATION_ENGINE))
    }

    /// Validate options at CREATE TABLE time.
    pub(crate) fn validate_create_mode(
        &self,
        has_primary_keys: bool,
    ) -> crate::Result<Option<AggregationMode>> {
        match self.validated_mode(has_primary_keys) {
            Ok(mode) => Ok(mode),
            Err(unsupported_options) => Err(crate::Error::ConfigInvalid {
                message: format!(
                    "merge-engine=aggregation only supports the basic mode in this build; unsupported options: {}",
                    unsupported_options.join(", ")
                ),
            }),
        }
    }

    /// Validate options at read/write runtime.
    pub(crate) fn validate_runtime_mode(
        &self,
        has_primary_keys: bool,
        table_name: &str,
    ) -> crate::Result<Option<AggregationMode>> {
        match self.validated_mode(has_primary_keys) {
            Ok(mode) => Ok(mode),
            Err(unsupported_options) => Err(crate::Error::Unsupported {
                message: format!(
                    "Table '{table_name}' uses merge-engine=aggregation options not supported by this build: {}",
                    unsupported_options.join(", ")
                ),
            }),
        }
    }

    fn validated_mode(
        &self,
        has_primary_keys: bool,
    ) -> std::result::Result<Option<AggregationMode>, Vec<String>> {
        if !has_primary_keys || !self.is_enabled() {
            return Ok(None);
        }

        let unsupported_options = self.unsupported_option_keys();
        if !unsupported_options.is_empty() {
            return Err(unsupported_options);
        }

        Ok(Some(AggregationMode::Basic))
    }

    fn unsupported_option_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .options
            .keys()
            .filter(|key| is_unsupported_aggregation_option(key))
            .cloned()
            .collect();
        keys.sort();
        keys
    }

    /// Per-field aggregate function configured via `fields.<col>.aggregate-function`.
    pub(crate) fn agg_function_for_field(&self, field_name: &str) -> Option<&str> {
        let key = format!("{FIELDS_PREFIX}{field_name}{AGG_FUNCTION_SUFFIX}");
        self.options.get(&key).map(String::as_str)
    }

    /// Default aggregate function from `fields.default-aggregate-function`.
    pub(crate) fn default_agg_function(&self) -> Option<&str> {
        self.options
            .get(FIELDS_DEFAULT_AGG_FUNCTION_OPTION)
            .map(String::as_str)
    }
}

fn is_unsupported_aggregation_option(key: &str) -> bool {
    key == IGNORE_DELETE_OPTION
        || key.ends_with(IGNORE_DELETE_SUFFIX)
        || key == AGGREGATION_REMOVE_RECORD_ON_DELETE_OPTION
        || is_fields_option_with_suffix(key, IGNORE_RETRACT_SUFFIX)
        || is_fields_option_with_suffix(key, DISTINCT_SUFFIX)
        || is_fields_option_with_suffix(key, SEQUENCE_GROUP_SUFFIX)
        || is_fields_option_with_suffix(key, NESTED_KEY_SUFFIX)
        || is_fields_option_with_suffix(key, COUNT_LIMIT_SUFFIX)
}

fn is_fields_option_with_suffix(key: &str, suffix: &str) -> bool {
    key.starts_with(FIELDS_PREFIX) && key.ends_with(suffix)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aggregation_options(extra: &[(&str, &str)]) -> HashMap<String, String> {
        let mut options = HashMap::from([(
            MERGE_ENGINE_OPTION.to_string(),
            AGGREGATION_ENGINE.to_string(),
        )]);
        options.extend(
            extra
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
        );
        options
    }

    #[test]
    fn test_validate_create_mode_accepts_basic_pk_aggregation() {
        let options = aggregation_options(&[
            ("fields.price.aggregate-function", "sum"),
            ("fields.default-aggregate-function", "last_non_null_value"),
            ("fields.tags.list-agg-delimiter", ";"),
        ]);
        let config = AggregationConfig::new(&options);

        assert_eq!(
            config.validate_create_mode(true).unwrap(),
            Some(AggregationMode::Basic)
        );
    }

    #[test]
    fn test_validate_create_mode_ignores_non_pk_tables() {
        let options = aggregation_options(&[("fields.x.ignore-retract", "true")]);
        let config = AggregationConfig::new(&options);

        assert_eq!(config.validate_create_mode(false).unwrap(), None);
    }

    #[test]
    fn test_is_enabled_disabled_for_other_engines() {
        let options = HashMap::from([(MERGE_ENGINE_OPTION.to_string(), "partial-update".into())]);
        let config = AggregationConfig::new(&options);
        assert!(!config.is_enabled());
        assert_eq!(config.validate_create_mode(true).unwrap(), None);
    }

    #[test]
    fn test_validate_create_mode_rejects_unsupported_options() {
        for key in [
            IGNORE_DELETE_OPTION,
            "fields.price.ignore-delete",
            AGGREGATION_REMOVE_RECORD_ON_DELETE_OPTION,
            "fields.price.ignore-retract",
            "fields.tags.distinct",
            "fields.price.sequence-group",
            "fields.payload.nested-key",
            "fields.payload.count-limit",
        ] {
            let options = aggregation_options(&[(key, "value")]);
            let config = AggregationConfig::new(&options);
            let err = config.validate_create_mode(true).unwrap_err();
            assert!(
                matches!(err, crate::Error::ConfigInvalid { ref message } if message.contains(key)),
                "expected create-time rejection to mention '{key}', got {err:?}"
            );
        }
    }

    #[test]
    fn test_validate_runtime_mode_rejects_unsupported_options() {
        let options = aggregation_options(&[("fields.price.ignore-retract", "true")]);
        let config = AggregationConfig::new(&options);
        let err = config.validate_runtime_mode(true, "default.t").unwrap_err();

        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("fields.price.ignore-retract")),
            "expected runtime rejection to mention the unsupported option, got {err:?}"
        );
    }
}
