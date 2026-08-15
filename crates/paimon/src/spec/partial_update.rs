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

use std::collections::{HashMap, HashSet};

use super::aggregation::{is_known_aggregator_name, validate_aggregator_for_type};
use crate::spec::DataField;

const MERGE_ENGINE_OPTION: &str = "merge-engine";
const PARTIAL_UPDATE_ENGINE: &str = "partial-update";
const IGNORE_DELETE_OPTION: &str = "ignore-delete";
const IGNORE_DELETE_SUFFIX: &str = ".ignore-delete";
const PARTIAL_UPDATE_IGNORE_DELETE_OPTION: &str = "partial-update.ignore-delete";
const PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE_OPTION: &str =
    "partial-update.remove-record-on-delete";
const PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP_OPTION: &str =
    "partial-update.remove-record-on-sequence-group";
const FIELDS_DEFAULT_AGG_FUNCTION_OPTION: &str = "fields.default-aggregate-function";
const FIELDS_PREFIX: &str = "fields.";
const SEQUENCE_GROUP_SUFFIX: &str = ".sequence-group";
const AGGREGATION_FUNCTION_SUFFIX: &str = ".aggregate-function";
const LIST_AGG_DELIMITER_SUFFIX: &str = ".list-agg-delimiter";
const IGNORE_RETRACT_SUFFIX: &str = ".ignore-retract";
const DISTINCT_SUFFIX: &str = ".distinct";
const NESTED_KEY_SUFFIX: &str = ".nested-key";
const COUNT_LIMIT_SUFFIX: &str = ".count-limit";

/// Partial-update mode recognized by the current Rust implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartialUpdateMode {
    Basic,
    SequenceGroup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SequenceGroup {
    pub(crate) sequence_fields: Vec<String>,
    pub(crate) protected_fields: Vec<String>,
}

impl SequenceGroup {
    fn option_key(&self) -> String {
        format!(
            "{FIELDS_PREFIX}{}{SEQUENCE_GROUP_SUFFIX}",
            self.sequence_fields.join(",")
        )
    }
}

/// Partial-update-specific option inspection and validation.
///
/// Reads support basic partial update, sequence groups, and field aggregation.
/// Table creation and writes remain restricted to basic partial update.
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

    pub(crate) fn validate_write_mode(
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

    pub(crate) fn validate_read_mode(
        &self,
        has_primary_keys: bool,
        table_name: &str,
    ) -> crate::Result<Option<PartialUpdateMode>> {
        if !has_primary_keys || !self.is_enabled() {
            return Ok(None);
        }

        let unsupported_options = self.read_unsupported_option_keys();
        if !unsupported_options.is_empty() {
            return Err(crate::Error::Unsupported {
                message: format!(
                    "Table '{table_name}' uses merge-engine=partial-update options not supported by this build: {}",
                    unsupported_options.join(", ")
                ),
            });
        }

        Ok(Some(if self.sequence_groups()?.is_empty() {
            PartialUpdateMode::Basic
        } else {
            PartialUpdateMode::SequenceGroup
        }))
    }

    pub(crate) fn sequence_groups(&self) -> crate::Result<Vec<SequenceGroup>> {
        let mut options: Vec<(&String, &String)> = self
            .options
            .iter()
            .filter(|(key, _)| is_fields_option_with_suffix(key, SEQUENCE_GROUP_SUFFIX))
            .collect();
        options.sort_by_key(|(key, _)| *key);

        options
            .into_iter()
            .map(|(key, value)| {
                let sequence_fields = key
                    .strip_prefix(FIELDS_PREFIX)
                    .and_then(|key| key.strip_suffix(SEQUENCE_GROUP_SUFFIX))
                    .ok_or_else(|| crate::Error::ConfigInvalid {
                        message: format!(
                            "Invalid partial-update sequence-group option '{key}={value}'"
                        ),
                    })
                    .and_then(|fields| parse_field_list(fields, key, value))?;
                let protected_fields = parse_field_list(value, key, value)?;
                Ok(SequenceGroup {
                    sequence_fields,
                    protected_fields,
                })
            })
            .collect()
    }

    pub(crate) fn validated_sequence_groups(
        &self,
        fields: &[DataField],
        primary_keys: &[String],
    ) -> crate::Result<Vec<SequenceGroup>> {
        let groups = self.sequence_groups()?;
        let field_names: HashSet<&str> = fields.iter().map(DataField::name).collect();
        let primary_keys: HashSet<&str> = primary_keys.iter().map(String::as_str).collect();
        let mut protected_field_owners: HashMap<String, String> = HashMap::new();

        for group in &groups {
            let option_key = group.option_key();
            for field in group
                .sequence_fields
                .iter()
                .chain(group.protected_fields.iter())
            {
                if !field_names.contains(field.as_str()) {
                    return Err(crate::Error::ConfigInvalid {
                        message: format!(
                            "Field '{field}' referenced by partial-update sequence-group \
                             option '{option_key}' does not exist in the table schema"
                        ),
                    });
                }
                if primary_keys.contains(field.as_str()) {
                    return Err(crate::Error::ConfigInvalid {
                        message: format!(
                            "The sequence-group '{option_key}' contains primary key field \
                             '{field}', which is not allowed. Primary key columns cannot be put \
                             in sequence-group."
                        ),
                    });
                }
            }
            for field in &group.protected_fields {
                if let Some(previous_option) =
                    protected_field_owners.insert(field.clone(), option_key.clone())
                {
                    return Err(crate::Error::ConfigInvalid {
                        message: format!(
                            "Field '{field}' is protected by multiple sequence groups: \
                             '{previous_option}' and '{option_key}'"
                        ),
                    });
                }
            }
        }

        Ok(groups)
    }

    pub(crate) fn required_sequence_fields(
        &self,
        fields: &[DataField],
        primary_keys: &[String],
        projected_fields: &[String],
    ) -> crate::Result<Vec<String>> {
        let groups = self.validated_sequence_groups(fields, primary_keys)?;
        let projected: HashSet<&str> = projected_fields.iter().map(String::as_str).collect();
        let mut required = Vec::new();
        let mut seen = HashSet::new();

        for group in groups {
            let group_is_projected = group
                .sequence_fields
                .iter()
                .chain(group.protected_fields.iter())
                .any(|field| projected.contains(field.as_str()));
            if !group_is_projected {
                continue;
            }
            for field in group.sequence_fields {
                if seen.insert(field.clone()) {
                    required.push(field);
                }
            }
        }

        Ok(required)
    }

    pub(crate) fn validated_aggregate_functions(
        &self,
        fields: &[DataField],
        primary_keys: &[String],
    ) -> crate::Result<HashMap<String, String>> {
        let groups = self.validated_sequence_groups(fields, primary_keys)?;
        let sequence_fields: HashSet<&str> = groups
            .iter()
            .flat_map(|group| group.sequence_fields.iter().map(String::as_str))
            .collect();
        let protected_fields: HashSet<&str> = groups
            .iter()
            .flat_map(|group| group.protected_fields.iter().map(String::as_str))
            .collect();
        let field_names: HashSet<&str> = fields.iter().map(DataField::name).collect();
        let primary_keys: HashSet<&str> = primary_keys.iter().map(String::as_str).collect();

        for (key, value) in self
            .options
            .iter()
            .filter(|(key, _)| is_fields_option_with_suffix(key, LIST_AGG_DELIMITER_SUFFIX))
        {
            let field_name = key
                .strip_prefix(FIELDS_PREFIX)
                .and_then(|key| key.strip_suffix(LIST_AGG_DELIMITER_SUFFIX))
                .filter(|field| !field.is_empty())
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: format!("Invalid partial-update listagg option '{key}={value}'"),
                })?;
            if !field_names.contains(field_name) {
                return Err(crate::Error::ConfigInvalid {
                    message: format!(
                        "Aggregation field '{field_name}' referenced by '{key}' is not declared \
                         in the table schema"
                    ),
                });
            }
        }

        let mut per_field = HashMap::new();
        for (key, value) in self
            .options
            .iter()
            .filter(|(key, _)| is_fields_option_with_suffix(key, AGGREGATION_FUNCTION_SUFFIX))
        {
            let field_name = key
                .strip_prefix(FIELDS_PREFIX)
                .and_then(|key| key.strip_suffix(AGGREGATION_FUNCTION_SUFFIX))
                .filter(|field| !field.is_empty())
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: format!(
                        "Invalid partial-update aggregate-function option '{key}={value}'"
                    ),
                })?;
            if !field_names.contains(field_name) {
                return Err(crate::Error::ConfigInvalid {
                    message: format!(
                        "Aggregation field '{field_name}' referenced by '{key}' is not declared \
                         in the table schema"
                    ),
                });
            }
            if !is_known_aggregator_name(value) {
                validate_aggregator_for_type(
                    value,
                    field_name,
                    fields
                        .iter()
                        .find(|field| field.name() == field_name)
                        .expect("field existence checked above")
                        .data_type(),
                )?;
            }
            per_field.insert(field_name, value.as_str());
        }

        let default = self
            .options
            .get(FIELDS_DEFAULT_AGG_FUNCTION_OPTION)
            .map(String::as_str);
        if let Some(default) = default {
            if !is_known_aggregator_name(default) {
                return Err(crate::Error::ConfigInvalid {
                    message: format!(
                        "Unknown aggregate function '{default}' configured via \
                         '{FIELDS_DEFAULT_AGG_FUNCTION_OPTION}'"
                    ),
                });
            }
        }

        let mut functions = HashMap::new();
        for field in fields {
            let field_name = field.name();
            if sequence_fields.contains(field_name) || primary_keys.contains(field_name) {
                continue;
            }
            let Some(function) = per_field.get(field_name).copied().or(default) else {
                continue;
            };
            validate_aggregator_for_type(function, field_name, field.data_type())?;
            if function != "last_non_null_value" && !protected_fields.contains(field_name) {
                return Err(crate::Error::ConfigInvalid {
                    message: format!(
                        "Must use sequence group for aggregate function '{function}' on field \
                         '{field_name}'"
                    ),
                });
            }
            functions.insert(field_name.to_string(), function.to_string());
        }
        Ok(functions)
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

    fn read_unsupported_option_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self
            .options
            .keys()
            .filter(|key| {
                is_unsupported_partial_update_option(key)
                    && !is_fields_option_with_suffix(key, SEQUENCE_GROUP_SUFFIX)
                    && !is_fields_option_with_suffix(key, AGGREGATION_FUNCTION_SUFFIX)
                    && !is_fields_option_with_suffix(key, LIST_AGG_DELIMITER_SUFFIX)
                    && key.as_str() != FIELDS_DEFAULT_AGG_FUNCTION_OPTION
            })
            .cloned()
            .collect();
        keys.sort();
        keys
    }
}

fn is_unsupported_partial_update_option(key: &str) -> bool {
    (key.ends_with(IGNORE_DELETE_SUFFIX)
        && key != IGNORE_DELETE_OPTION
        && key != PARTIAL_UPDATE_IGNORE_DELETE_OPTION)
        || key == PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE_OPTION
        || key == PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP_OPTION
        || key == FIELDS_DEFAULT_AGG_FUNCTION_OPTION
        || is_fields_option_with_suffix(key, SEQUENCE_GROUP_SUFFIX)
        || is_fields_option_with_suffix(key, AGGREGATION_FUNCTION_SUFFIX)
        || is_fields_option_with_suffix(key, LIST_AGG_DELIMITER_SUFFIX)
        || is_fields_option_with_suffix(key, IGNORE_RETRACT_SUFFIX)
        || is_fields_option_with_suffix(key, DISTINCT_SUFFIX)
        || is_fields_option_with_suffix(key, NESTED_KEY_SUFFIX)
        || is_fields_option_with_suffix(key, COUNT_LIMIT_SUFFIX)
}

fn is_fields_option_with_suffix(key: &str, suffix: &str) -> bool {
    key.starts_with(FIELDS_PREFIX) && key.ends_with(suffix)
}

fn parse_field_list(
    value: &str,
    option_key: &str,
    option_value: &str,
) -> crate::Result<Vec<String>> {
    value
        .split(',')
        .map(str::trim)
        .map(|field| {
            if field.is_empty() {
                Err(crate::Error::ConfigInvalid {
                    message: format!(
                        "Invalid partial-update sequence-group option \
                         '{option_key}={option_value}': empty field name"
                    ),
                })
            } else {
                Ok(field.to_string())
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataField, DataType, IntType};

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
    fn test_validate_create_mode_accepts_partial_update_ignore_delete() {
        for value in ["true", "false"] {
            let options = partial_update_options(&[(PARTIAL_UPDATE_IGNORE_DELETE_OPTION, value)]);
            let config = PartialUpdateConfig::new(&options);

            assert_eq!(
                config.validate_create_mode(true).unwrap(),
                Some(PartialUpdateMode::Basic)
            );
        }
    }

    #[test]
    fn test_validate_create_mode_accepts_ignore_delete() {
        for value in ["true", "false"] {
            let options = partial_update_options(&[(IGNORE_DELETE_OPTION, value)]);
            let config = PartialUpdateConfig::new(&options);

            assert_eq!(
                config.validate_create_mode(true).unwrap(),
                Some(PartialUpdateMode::Basic)
            );
        }
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
            PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE_OPTION,
            PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP_OPTION,
            "deduplicate.ignore-delete",
            "fields.price.ignore-delete",
            "fields.price.sequence-group",
            "fields.price.aggregate-function",
            "fields.price.list-agg-delimiter",
            "fields.price.ignore-retract",
            "fields.price.distinct",
            "fields.price.nested-key",
            "fields.price.count-limit",
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
    fn test_validate_write_mode_rejects_unsupported_partial_update_options() {
        let options =
            partial_update_options(&[("fields.price.aggregate-function", "last_non_null")]);
        let config = PartialUpdateConfig::new(&options);
        let err = config.validate_write_mode(true, "default.t").unwrap_err();

        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("fields.price.aggregate-function")),
            "expected runtime rejection to mention the unsupported option, got {err:?}"
        );
    }

    #[test]
    fn test_validate_read_mode_accepts_sequence_group() {
        let options =
            partial_update_options(&[("fields.updated_at.sequence-group", "price,quantity")]);
        let config = PartialUpdateConfig::new(&options);

        assert_eq!(
            config.validate_read_mode(true, "default.t").unwrap(),
            Some(PartialUpdateMode::SequenceGroup)
        );
    }

    #[test]
    fn test_validate_read_mode_accepts_field_aggregation() {
        let options =
            partial_update_options(&[("fields.price.aggregate-function", "last_non_null_value")]);
        let config = PartialUpdateConfig::new(&options);

        assert_eq!(
            config.validate_read_mode(true, "default.t").unwrap(),
            Some(PartialUpdateMode::Basic)
        );
    }

    #[test]
    fn test_validate_read_mode_accepts_sequence_group_field_aggregation() {
        let options = partial_update_options(&[
            ("fields.version.sequence-group", "price"),
            ("fields.price.aggregate-function", "sum"),
        ]);
        let config = PartialUpdateConfig::new(&options);

        assert_eq!(
            config.validate_read_mode(true, "default.t").unwrap(),
            Some(PartialUpdateMode::SequenceGroup)
        );
    }

    #[test]
    fn test_validate_read_mode_rejects_unsupported_aggregation_modifiers() {
        for key in [
            "fields.price.ignore-retract",
            "fields.price.distinct",
            "fields.price.nested-key",
            "fields.price.count-limit",
        ] {
            let options = partial_update_options(&[(key, "value")]);
            let config = PartialUpdateConfig::new(&options);
            let err = config.validate_read_mode(true, "default.t").unwrap_err();

            assert!(
                matches!(err, crate::Error::Unsupported { ref message } if message.contains(key)),
                "expected read-time rejection to mention '{key}', got {err:?}"
            );
        }
    }

    #[test]
    fn test_parse_sequence_groups() {
        let options = partial_update_options(&[
            (
                "fields.event_time,source_order.sequence-group",
                "price,quantity",
            ),
            ("fields.profile_version.sequence-group", "name,address"),
        ]);
        let config = PartialUpdateConfig::new(&options);

        assert_eq!(
            config.sequence_groups().unwrap(),
            vec![
                SequenceGroup {
                    sequence_fields: vec!["event_time".to_string(), "source_order".to_string()],
                    protected_fields: vec!["price".to_string(), "quantity".to_string()],
                },
                SequenceGroup {
                    sequence_fields: vec!["profile_version".to_string()],
                    protected_fields: vec!["name".to_string(), "address".to_string()],
                },
            ]
        );
    }

    #[test]
    fn test_parse_sequence_groups_rejects_empty_field_names() {
        for (key, value) in [
            ("fields.version,,source.sequence-group", "price"),
            ("fields.version.sequence-group", "price,,quantity"),
        ] {
            let options = partial_update_options(&[(key, value)]);
            let config = PartialUpdateConfig::new(&options);

            let err = config.sequence_groups().unwrap_err();
            assert!(
                matches!(err, crate::Error::ConfigInvalid { ref message }
                    if message.contains(key) && message.contains("empty field name")),
                "expected malformed field list to be rejected, got {err:?}"
            );
        }
    }

    #[test]
    fn test_validate_sequence_groups_rejects_primary_key() {
        let options = partial_update_options(&[("fields.version.sequence-group", "id,price")]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "version".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "price".to_string(), DataType::Int(IntType::new())),
        ];

        let err = config
            .validated_sequence_groups(&fields, &["id".to_string()])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("fields.version.sequence-group")
                    && message.contains("primary key field 'id'")),
            "expected primary-key conflict, got {err:?}"
        );
    }

    #[test]
    fn test_validate_sequence_groups_rejects_duplicate_protected_field() {
        let options = partial_update_options(&[
            ("fields.version_a.sequence-group", "price"),
            ("fields.version_b.sequence-group", "price"),
        ]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "version_a".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "version_b".to_string(), DataType::Int(IntType::new())),
            DataField::new(3, "price".to_string(), DataType::Int(IntType::new())),
        ];

        let err = config
            .validated_sequence_groups(&fields, &["id".to_string()])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("price") && message.contains("multiple sequence groups")),
            "expected duplicate protected-field error, got {err:?}"
        );
    }

    #[test]
    fn test_required_sequence_fields_for_projection() {
        let options = partial_update_options(&[
            (
                "fields.event_time,source_order.sequence-group",
                "price,quantity",
            ),
            ("fields.profile_version.sequence-group", "name,address"),
        ]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "event_time".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "source_order".to_string(), DataType::Int(IntType::new())),
            DataField::new(3, "price".to_string(), DataType::Int(IntType::new())),
            DataField::new(4, "quantity".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                5,
                "profile_version".to_string(),
                DataType::Int(IntType::new()),
            ),
            DataField::new(6, "name".to_string(), DataType::Int(IntType::new())),
            DataField::new(7, "address".to_string(), DataType::Int(IntType::new())),
        ];

        assert_eq!(
            config
                .required_sequence_fields(
                    &fields,
                    &["id".to_string()],
                    &["price".to_string(), "name".to_string()],
                )
                .unwrap(),
            vec![
                "event_time".to_string(),
                "source_order".to_string(),
                "profile_version".to_string(),
            ]
        );
    }

    #[test]
    fn test_validate_aggregate_functions_accepts_last_non_null_without_sequence_group() {
        let options =
            partial_update_options(&[("fields.price.aggregate-function", "last_non_null_value")]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "price".to_string(), DataType::Int(IntType::new())),
        ];

        assert_eq!(
            config
                .validated_aggregate_functions(&fields, &["id".to_string()])
                .unwrap(),
            HashMap::from([("price".to_string(), "last_non_null_value".to_string())])
        );
    }

    #[test]
    fn test_validate_aggregate_functions_rejects_non_last_without_sequence_group() {
        let options = partial_update_options(&[("fields.price.aggregate-function", "sum")]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "price".to_string(), DataType::Int(IntType::new())),
        ];

        let err = config
            .validated_aggregate_functions(&fields, &["id".to_string()])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("sum")
                    && message.contains("price")
                    && message.contains("sequence group")),
            "expected missing sequence-group error, got {err:?}"
        );
    }

    #[test]
    fn test_validate_aggregate_functions_rejects_unknown_function() {
        let options = partial_update_options(&[
            ("fields.version.sequence-group", "price"),
            ("fields.price.aggregate-function", "sume"),
        ]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "version".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "price".to_string(), DataType::Int(IntType::new())),
        ];

        let err = config
            .validated_aggregate_functions(&fields, &["id".to_string()])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("sume") && message.contains("price")),
            "expected unknown function error, got {err:?}"
        );
    }

    #[test]
    fn test_validate_aggregate_functions_rejects_unknown_field() {
        let options =
            partial_update_options(&[("fields.prcie.aggregate-function", "last_non_null_value")]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "price".to_string(), DataType::Int(IntType::new())),
        ];

        let err = config
            .validated_aggregate_functions(&fields, &["id".to_string()])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("prcie")
                    && message.contains("fields.prcie.aggregate-function")),
            "expected unknown field error, got {err:?}"
        );
    }

    #[test]
    fn test_validate_aggregate_functions_rejects_unknown_listagg_delimiter_field() {
        let options = partial_update_options(&[
            ("fields.version.sequence-group", "tag"),
            ("fields.tag.aggregate-function", "listagg"),
            ("fields.tga.list-agg-delimiter", "|"),
        ]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "version".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                2,
                "tag".to_string(),
                DataType::VarChar(crate::spec::VarCharType::string_type()),
            ),
        ];

        let err = config
            .validated_aggregate_functions(&fields, &["id".to_string()])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("tga")
                    && message.contains("fields.tga.list-agg-delimiter")),
            "expected unknown delimiter field error, got {err:?}"
        );
    }

    #[test]
    fn test_validate_aggregate_functions_rejects_incompatible_type() {
        let options = partial_update_options(&[
            ("fields.version.sequence-group", "name"),
            ("fields.name.aggregate-function", "sum"),
        ]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "version".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                2,
                "name".to_string(),
                DataType::VarChar(crate::spec::VarCharType::string_type()),
            ),
        ];

        let err = config
            .validated_aggregate_functions(&fields, &["id".to_string()])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("sum") && message.contains("name")),
            "expected incompatible type error, got {err:?}"
        );
    }

    #[test]
    fn test_validate_default_aggregate_function_applies_to_protected_fields() {
        let options = partial_update_options(&[
            ("fields.version.sequence-group", "price"),
            (FIELDS_DEFAULT_AGG_FUNCTION_OPTION, "sum"),
        ]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "version".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "price".to_string(), DataType::Int(IntType::new())),
        ];

        assert_eq!(
            config
                .validated_aggregate_functions(&fields, &["id".to_string()])
                .unwrap(),
            HashMap::from([("price".to_string(), "sum".to_string())])
        );
    }

    #[test]
    fn test_per_field_aggregate_function_overrides_default() {
        let options = partial_update_options(&[
            ("fields.version.sequence-group", "price"),
            ("fields.price.aggregate-function", "sum"),
            (FIELDS_DEFAULT_AGG_FUNCTION_OPTION, "last_non_null_value"),
        ]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "version".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "price".to_string(), DataType::Int(IntType::new())),
            DataField::new(3, "note".to_string(), DataType::Int(IntType::new())),
        ];

        assert_eq!(
            config
                .validated_aggregate_functions(&fields, &["id".to_string()])
                .unwrap(),
            HashMap::from([
                ("price".to_string(), "sum".to_string()),
                ("note".to_string(), "last_non_null_value".to_string()),
            ])
        );
    }

    #[test]
    fn test_validate_sequence_groups_rejects_unknown_field() {
        let options = partial_update_options(&[("fields.version.sequence-group", "prcie")]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "version".to_string(), DataType::Int(IntType::new())),
            DataField::new(2, "price".to_string(), DataType::Int(IntType::new())),
        ];

        let err = config
            .validated_sequence_groups(&fields, &["id".to_string()])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("prcie")
                    && message.contains("does not exist in the table schema")),
            "expected unknown sequence-group field error, got {err:?}"
        );
    }

    #[test]
    fn test_validate_aggregate_functions_rejects_empty_listagg_field() {
        let options = partial_update_options(&[("fields..list-agg-delimiter", ";")]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "note".to_string(), DataType::Int(IntType::new())),
        ];

        let err = config
            .validated_aggregate_functions(&fields, &["id".to_string()])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("Invalid partial-update listagg option")
                    && message.contains("fields..list-agg-delimiter")),
            "expected invalid listagg option error, got {err:?}"
        );
    }

    #[test]
    fn test_validate_aggregate_functions_rejects_empty_aggregate_function_field() {
        let options = partial_update_options(&[("fields..aggregate-function", "sum")]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "price".to_string(), DataType::Int(IntType::new())),
        ];

        let err = config
            .validated_aggregate_functions(&fields, &["id".to_string()])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("Invalid partial-update aggregate-function option")
                    && message.contains("fields..aggregate-function")),
            "expected invalid aggregate-function option error, got {err:?}"
        );
    }

    #[test]
    fn test_validate_aggregate_functions_rejects_unknown_default_function() {
        let options = partial_update_options(&[(FIELDS_DEFAULT_AGG_FUNCTION_OPTION, "sume")]);
        let config = PartialUpdateConfig::new(&options);
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "price".to_string(), DataType::Int(IntType::new())),
        ];

        let err = config
            .validated_aggregate_functions(&fields, &["id".to_string()])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("sume")
                    && message.contains(FIELDS_DEFAULT_AGG_FUNCTION_OPTION)),
            "expected unknown default aggregate function error, got {err:?}"
        );
    }
}
