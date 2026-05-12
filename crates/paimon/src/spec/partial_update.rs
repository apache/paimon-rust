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
const PARTIAL_UPDATE_ENGINE: &str = "partial-update";
const IGNORE_DELETE_OPTION: &str = "ignore-delete";
const IGNORE_DELETE_SUFFIX: &str = ".ignore-delete";
const PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE_OPTION: &str =
    "partial-update.remove-record-on-delete";
const PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP_OPTION: &str =
    "partial-update.remove-record-on-sequence-group";
const FIELDS_DEFAULT_AGG_FUNCTION_OPTION: &str = "fields.default-aggregate-function";
const FIELDS_PREFIX: &str = "fields.";
const SEQUENCE_GROUP_SUFFIX: &str = ".sequence-group";
const AGGREGATION_FUNCTION_SUFFIX: &str = ".aggregate-function";

/// Minimal partial-update mode recognized by the current Rust implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartialUpdateMode {
    Basic,
}

/// Partial-update-specific option inspection and validation.
///
/// PR1 only recognizes the basic mode: `merge-engine=partial-update` on a PK
/// table without delete, sequence-group, or aggregation controls.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PartialUpdateConfig<'a> {
    options: &'a HashMap<String, String>,
}

impl<'a> PartialUpdateConfig<'a> {
    pub(crate) fn new(options: &'a HashMap<String, String>) -> Self {
        Self { options }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.options
            .get(MERGE_ENGINE_OPTION)
            .is_some_and(|value| value.eq_ignore_ascii_case(PARTIAL_UPDATE_ENGINE))
    }

    pub(crate) fn validate_create_mode(
        &self,
        has_primary_keys: bool,
    ) -> crate::Result<Option<PartialUpdateMode>> {
        match self.validated_mode(has_primary_keys) {
            Ok(mode) => Ok(mode),
            Err(unsupported_options) => Err(crate::Error::ConfigInvalid {
                message: format!(
                    "merge-engine=partial-update only supports the basic mode in this build; unsupported options: {}",
                    unsupported_options.join(", ")
                ),
            }),
        }
    }

    pub(crate) fn validate_runtime_mode(
        &self,
        has_primary_keys: bool,
        table_name: &str,
    ) -> crate::Result<Option<PartialUpdateMode>> {
        match self.validated_mode(has_primary_keys) {
            Ok(mode) => Ok(mode),
            Err(unsupported_options) => Err(crate::Error::Unsupported {
                message: format!(
                    "Table '{table_name}' uses merge-engine=partial-update options not supported by this build: {}",
                    unsupported_options.join(", ")
                ),
            }),
        }
    }

    fn validated_mode(
        &self,
        has_primary_keys: bool,
    ) -> std::result::Result<Option<PartialUpdateMode>, Vec<String>> {
        if !has_primary_keys || !self.is_enabled() {
            return Ok(None);
        }

        let unsupported_options = self.unsupported_option_keys();
        if !unsupported_options.is_empty() {
            return Err(unsupported_options);
        }

        Ok(Some(PartialUpdateMode::Basic))
    }

    fn unsupported_option_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .options
            .keys()
            .filter(|key| is_unsupported_partial_update_option(key))
            .cloned()
            .collect();
        keys.sort();
        keys
    }
}

fn is_unsupported_partial_update_option(key: &str) -> bool {
    key == IGNORE_DELETE_OPTION
        || key.ends_with(IGNORE_DELETE_SUFFIX)
        || key == PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE_OPTION
        || key == PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP_OPTION
        || key == FIELDS_DEFAULT_AGG_FUNCTION_OPTION
        || is_fields_option_with_suffix(key, SEQUENCE_GROUP_SUFFIX)
        || is_fields_option_with_suffix(key, AGGREGATION_FUNCTION_SUFFIX)
}

fn is_fields_option_with_suffix(key: &str, suffix: &str) -> bool {
    key.starts_with(FIELDS_PREFIX) && key.ends_with(suffix)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partial_update_options(extra: &[(&str, &str)]) -> HashMap<String, String> {
        let mut options = HashMap::from([(
            MERGE_ENGINE_OPTION.to_string(),
            PARTIAL_UPDATE_ENGINE.to_string(),
        )]);
        options.extend(
            extra
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
        );
        options
    }

    #[test]
    fn test_validate_create_mode_accepts_basic_pk_partial_update() {
        let options = partial_update_options(&[]);
        let config = PartialUpdateConfig::new(&options);

        assert_eq!(
            config.validate_create_mode(true).unwrap(),
            Some(PartialUpdateMode::Basic)
        );
    }

    #[test]
    fn test_validate_create_mode_ignores_non_pk_tables() {
        let options = partial_update_options(&[(IGNORE_DELETE_OPTION, "true")]);
        let config = PartialUpdateConfig::new(&options);

        assert_eq!(config.validate_create_mode(false).unwrap(), None);
    }

    #[test]
    fn test_validate_create_mode_rejects_unsupported_partial_update_options() {
        for key in [
            IGNORE_DELETE_OPTION,
            "partial-update.ignore-delete",
            PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE_OPTION,
            PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP_OPTION,
            "fields.price.sequence-group",
            "fields.price.aggregate-function",
            FIELDS_DEFAULT_AGG_FUNCTION_OPTION,
        ] {
            let options = partial_update_options(&[(key, "value")]);
            let config = PartialUpdateConfig::new(&options);
            let err = config.validate_create_mode(true).unwrap_err();

            assert!(
                matches!(err, crate::Error::ConfigInvalid { ref message } if message.contains(key)),
                "expected create-time rejection to mention '{key}', got {err:?}"
            );
        }
    }

    #[test]
    fn test_validate_runtime_mode_rejects_unsupported_partial_update_options() {
        let options =
            partial_update_options(&[("fields.price.aggregate-function", "last_non_null")]);
        let config = PartialUpdateConfig::new(&options);
        let err = config.validate_runtime_mode(true, "default.t").unwrap_err();

        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("fields.price.aggregate-function")),
            "expected runtime rejection to mention the unsupported option, got {err:?}"
        );
    }
}
