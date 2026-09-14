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

use crate::spec::{CoreOptions, DataField, DataType, VarCharType};

const MERGE_ENGINE_OPTION: &str = "merge-engine";
const AGGREGATION_ENGINE: &str = "aggregation";
const IGNORE_DELETE_OPTION: &str = "ignore-delete";
const IGNORE_DELETE_SUFFIX: &str = ".ignore-delete";
const AGGREGATION_REMOVE_RECORD_ON_DELETE_OPTION: &str = "aggregation.remove-record-on-delete";
const FIELDS_DEFAULT_AGG_FUNCTION_OPTION: &str = "fields.default-aggregate-function";
const FIELDS_PREFIX: &str = "fields.";
const AGG_FUNCTION_SUFFIX: &str = ".aggregate-function";
const LIST_AGG_DELIMITER_SUFFIX: &str = ".list-agg-delimiter";
const IGNORE_RETRACT_SUFFIX: &str = ".ignore-retract";
const DISTINCT_SUFFIX: &str = ".distinct";
const SEQUENCE_GROUP_SUFFIX: &str = ".sequence-group";
const NESTED_KEY_SUFFIX: &str = ".nested-key";
const COUNT_LIMIT_SUFFIX: &str = ".count-limit";
const MAP_STORAGE_LAYOUT_SUFFIX: &str = ".map.storage-layout";
const MAP_SHARED_SHREDDING_MAX_COLUMNS_SUFFIX: &str = ".map.shared-shredding.max-columns";
const MAP_SHARED_SHREDDING_COLUMN_PLACEMENT_POLICY_SUFFIX: &str =
    ".map.shared-shredding.column-placement-policy";

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

    /// Validate options at CREATE TABLE time, using the schema's fields and
    /// primary keys to reject typo'd column names, unknown aggregate
    /// functions, and function/type pairs that the runtime would refuse.
    ///
    /// Java upstream rejects unknown columns and unknown function names in
    /// `SchemaValidation.validateFieldsPrefix` + `validateMergeFunctionFactory`;
    /// the function/type compatibility check is stricter than Java, which
    /// defers it to `FieldAggregatorFactory#create` at runtime.  Catching all
    /// three at CREATE TABLE keeps invalid metadata from being persisted.
    pub(crate) fn validate_create_mode(
        &self,
        primary_keys: &[String],
        fields: &[DataField],
    ) -> crate::Result<Option<AggregationMode>> {
        let mode = match self.validated_mode(!primary_keys.is_empty()) {
            Ok(mode) => mode,
            Err(unsupported_options) => {
                return Err(crate::Error::ConfigInvalid {
                    message: format!(
                        "merge-engine=aggregation only supports the basic mode in this build; unsupported options: {}",
                        unsupported_options.join(", ")
                    ),
                });
            }
        };
        if mode.is_some() {
            self.validate_field_scoped_options(fields, primary_keys)?;
        }
        Ok(mode)
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

    /// Schema-aware checks run by [`validate_create_mode`] once the engine is
    /// confirmed active.  For every `fields.<col>.<known-suffix>` key
    /// (currently `aggregate-function` and `list-agg-delimiter`):
    /// * the `<col>` segment must name an existing schema field; this catches
    ///   typo'd column names that would otherwise silently fall back to the
    ///   default function / default delimiter at read time.
    ///
    /// For `aggregate-function` keys additionally:
    /// * the function name must be one of the supported aggregators
    /// * the field must not be listed in `sequence.field` — Java rejects
    ///   aggregation definitions on sequence fields during schema validation.
    /// * the function must accept the field's declared data type — except for
    ///   primary-key columns (no aggregator; copied through), where the
    ///   configured function is ignored by the merge function's priority
    ///   order (Java `AggregateMergeFunction#getAggFuncName`), so only the
    ///   function name is validated.
    ///
    /// `fields.default-aggregate-function` only has its name validated;
    /// per-column type compatibility for the default is deferred to runtime
    /// because the default applies broadly across columns.
    fn validate_field_scoped_options(
        &self,
        fields: &[DataField],
        primary_keys: &[String],
    ) -> crate::Result<()> {
        for (key, value) in self.options {
            let Some((col, kind)) = parse_field_scoped_option_key(key) else {
                continue;
            };
            let Some(field) = fields.iter().find(|f| f.name() == col) else {
                let mut available: Vec<&str> = fields.iter().map(DataField::name).collect();
                available.sort();
                return Err(crate::Error::ConfigInvalid {
                    message: format!(
                        "Aggregation field '{col}' referenced by '{key}' is not declared in \
                         the table schema; available columns: [{}]",
                        available.join(", ")
                    ),
                });
            };
            if matches!(kind, FieldScopedOptionKind::AggregateFunction) {
                if primary_keys.iter().any(|pk| pk == col) {
                    if !is_known_aggregator_name(value) {
                        return Err(crate::Error::ConfigInvalid {
                            message: format!(
                                "Unknown aggregate function '{value}' for field '{col}'; \
                                 {SUPPORTED_AGGREGATOR_NAMES_HINT}"
                            ),
                        });
                    }
                    continue;
                }

                validate_aggregator_for_type(value, col, field.data_type())?;
            }
        }

        if let Some(default) = self
            .options
            .get(FIELDS_DEFAULT_AGG_FUNCTION_OPTION)
            .map(String::as_str)
        {
            if !is_known_aggregator_name(default) {
                return Err(crate::Error::ConfigInvalid {
                    message: format!(
                        "Unknown aggregate function '{default}' configured via \
                         '{FIELDS_DEFAULT_AGG_FUNCTION_OPTION}'; {SUPPORTED_AGGREGATOR_NAMES_HINT}"
                    ),
                });
            }
        }

        Ok(())
    }
}

/// Field-scoped option suffixes that schema-aware validation recognizes.
/// Each variant maps to a single `fields.<col>.<suffix>` key shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldScopedOptionKind {
    AggregateFunction,
    ListAggDelimiter,
}

/// Parse the `<col>` segment and option kind out of a
/// `fields.<col>.<known-suffix>` key, or return `None` if `key` doesn't
/// match any known field-scoped option suffix.
fn parse_field_scoped_option_key(key: &str) -> Option<(&str, FieldScopedOptionKind)> {
    let inner = key.strip_prefix(FIELDS_PREFIX)?;
    for (suffix, kind) in [
        (
            AGG_FUNCTION_SUFFIX,
            FieldScopedOptionKind::AggregateFunction,
        ),
        (
            LIST_AGG_DELIMITER_SUFFIX,
            FieldScopedOptionKind::ListAggDelimiter,
        ),
    ] {
        if let Some(col) = inner.strip_suffix(suffix) {
            if col.is_empty() {
                // `fields..<suffix>` is malformed; treat as "no match" so the
                // caller surfaces a typo-style error elsewhere.
                continue;
            }
            return Some((col, kind));
        }
    }
    None
}

/// Field-scoped option suffixes whose key names a single column
/// (`fields.<col>.<suffix>`) while the value carries no column names, so the
/// column rename/drop path can keep them in sync with the schema by rewriting
/// the key only.  Mirrors case 2 of Java
/// `SchemaManager.applyRenameColumnsToOptions` (Java's case-2 list; the
/// aggregation/partial-update engines do not accept every entry at create
/// time, but the keys can still arrive in Java-written metadata).
const FIELD_SCOPED_RENAMEABLE_SUFFIXES: [&str; 7] = [
    AGG_FUNCTION_SUFFIX,
    IGNORE_RETRACT_SUFFIX,
    DISTINCT_SUFFIX,
    LIST_AGG_DELIMITER_SUFFIX,
    MAP_STORAGE_LAYOUT_SUFFIX,
    MAP_SHARED_SHREDDING_MAX_COLUMNS_SUFFIX,
    MAP_SHARED_SHREDDING_COLUMN_PLACEMENT_POLICY_SUFFIX,
];

/// Field-scoped option suffixes where BOTH the key's comma-separated field
/// list and the value may name columns (`fields.<col,col,...>.<suffix>`).
/// Mirrors case 3 of Java `SchemaManager.applyRenameColumnsToOptions`.
const FIELD_SCOPED_FIELD_LIST_SUFFIXES: [&str; 2] = [SEQUENCE_GROUP_SUFFIX, NESTED_KEY_SUFFIX];

/// Rename a column inside field-scoped option keys (and, for field-list
/// options, values), mirroring Java `SchemaManager.applyRenameColumnsToOptions`:
///
/// - case 2: `fields.<old>.<suffix>` -> `fields.<new>.<suffix>`, value kept
/// - case 3 (`sequence-group`, `nested-key`): every comma-separated entry
///   equal to `old` is renamed in BOTH the key's field list and the value
pub(crate) fn rename_field_scoped_options(
    options: &mut HashMap<String, String>,
    old: &str,
    new: &str,
) {
    for suffix in FIELD_SCOPED_RENAMEABLE_SUFFIXES {
        let old_key = format!("{FIELDS_PREFIX}{old}{suffix}");
        if let Some(value) = options.remove(&old_key) {
            options.insert(format!("{FIELDS_PREFIX}{new}{suffix}"), value);
        }
    }

    // Collect first: mutating the map while iterating it is not allowed, and
    // options that don't reference `old` must stay untouched.
    let mut rewritten = Vec::new();
    for (key, value) in options.iter() {
        let Some(suffix) = FIELD_SCOPED_FIELD_LIST_SUFFIXES
            .iter()
            .find(|suffix| key.starts_with(FIELDS_PREFIX) && key.ends_with(**suffix))
        else {
            continue;
        };
        let key_fields = &key[FIELDS_PREFIX.len()..key.len() - suffix.len()];
        let new_key_fields = rename_in_field_list(key_fields, old, new);
        let new_value = rename_in_field_list(value, old, new);
        if new_key_fields != key_fields || new_value != *value {
            rewritten.push((
                key.clone(),
                format!("{FIELDS_PREFIX}{new_key_fields}{suffix}"),
                new_value,
            ));
        }
    }
    for (old_key, new_key, new_value) in rewritten {
        options.remove(&old_key);
        options.insert(new_key, new_value);
    }
}

/// Rename exact matches of `old` inside a comma-separated field list,
/// preserving order and any entries that don't match.
fn rename_in_field_list(list: &str, old: &str, new: &str) -> String {
    list.split(',')
        .map(|field| if field == old { new } else { field })
        .collect::<Vec<_>>()
        .join(",")
}

/// Remove a dropped column's field-scoped aggregation option keys so no
/// orphaned `fields.<col>.*` options remain after the column is gone.
pub(crate) fn remove_field_scoped_options(options: &mut HashMap<String, String>, col: &str) {
    for suffix in FIELD_SCOPED_RENAMEABLE_SUFFIXES {
        options.remove(&format!("{FIELDS_PREFIX}{col}{suffix}"));
    }
}

const SUPPORTED_AGGREGATOR_NAMES_HINT: &str = "supported: sum, product, min, max, last_value, \
    first_value, last_non_null_value, first_non_null_value, bool_and, bool_or, listagg";

/// Java keeps `first_not_null_value` registered as an SPI alias of
/// `first_non_null_value`: `FieldFirstNonNullValueAggLegacyFactory` is listed in
/// `paimon-core/src/main/resources/META-INF/services/org.apache.paimon.factories.Factory`
/// next to the canonical factory, and both build the same
/// `FieldFirstNonNullValueAgg`. The alias survives from the switch-case to SPI
/// refactor and is deliberately undocumented, so a Java-written schema can carry
/// it even though nothing suggests it — which is why it is resolved here but
/// stays out of [`SUPPORTED_AGGREGATOR_NAMES_HINT`].
///
/// Matching is exact: Java's `FactoryUtil#discoverFactory` selects a factory
/// with `identifier().equals(identifier)`, so neither case nor `-`/`_` is
/// normalised.
pub(crate) fn canonical_aggregator_name(name: &str) -> &str {
    match name {
        "first_not_null_value" => "first_non_null_value",
        other => other,
    }
}

/// Whether `name` matches one of the basic-mode aggregator identifiers.  Must
/// stay in sync with the `match` arms in
/// `crate::table::aggregator::new_aggregator`.  Both resolve the name through
/// [`canonical_aggregator_name`] first; `tests::validation_table_matches_constructors`
/// guards the type table below rather than this predicate.
pub(crate) fn is_known_aggregator_name(name: &str) -> bool {
    matches!(
        canonical_aggregator_name(name),
        "sum"
            | "product"
            | "min"
            | "max"
            | "last_value"
            | "first_value"
            | "last_non_null_value"
            | "first_non_null_value"
            | "bool_and"
            | "bool_or"
            | "listagg"
    )
}

/// Mirror of the per-aggregator type checks in `crate::table::aggregator::*`.
/// `Ok(())` means the runtime `*Agg::new` constructor will accept the given
/// `(name, data_type)` pair.  Must stay in sync — guarded by
/// `tests::validation_table_matches_constructors`.
pub(crate) fn validate_aggregator_for_type(
    name: &str,
    field_name: &str,
    dt: &DataType,
) -> crate::Result<()> {
    // Errors below echo the caller's `name`, not the canonical one, so a user
    // never reads back a function name they did not write.
    let ok = match canonical_aggregator_name(name) {
        "sum" => matches!(
            dt,
            DataType::TinyInt(_)
                | DataType::SmallInt(_)
                | DataType::Int(_)
                | DataType::BigInt(_)
                | DataType::Float(_)
                | DataType::Double(_)
                | DataType::Decimal(_)
        ),
        "product" => matches!(
            dt,
            DataType::TinyInt(_)
                | DataType::SmallInt(_)
                | DataType::Int(_)
                | DataType::BigInt(_)
                | DataType::Float(_)
                | DataType::Double(_)
        ),
        "min" | "max" => matches!(
            dt,
            DataType::TinyInt(_)
                | DataType::SmallInt(_)
                | DataType::Int(_)
                | DataType::BigInt(_)
                | DataType::Float(_)
                | DataType::Double(_)
                | DataType::Decimal(_)
                | DataType::Date(_)
                | DataType::Time(_)
                | DataType::Timestamp(_)
                | DataType::Char(_)
                | DataType::VarChar(_)
        ),
        "bool_and" | "bool_or" => matches!(dt, DataType::Boolean(_)),
        // Java `FieldListaggAggFactory` only accepts unbounded VARCHAR (STRING);
        // CHAR and bounded VARCHAR(n) are rejected.
        "listagg" => matches!(dt, DataType::VarChar(v) if v.length() == VarCharType::MAX_LENGTH),
        "last_value" | "first_value" | "last_non_null_value" | "first_non_null_value" => true,
        _ => {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "Unknown aggregate function '{name}' for field '{field_name}'; \
                     {SUPPORTED_AGGREGATOR_NAMES_HINT}"
                ),
            });
        }
    };
    if ok {
        Ok(())
    } else {
        Err(crate::Error::ConfigInvalid {
            message: format!(
                "Aggregate function '{name}' does not support data type {dt:?} for field \
                 '{field_name}'"
            ),
        })
    }
}

/// Reject `fields.<col>.aggregate-function` on a column listed in
/// `sequence.field`, mirroring Java `SchemaValidation#validateSequenceField`,
/// which checks `options.fieldAggFunc(field) == null` for every sequence field
/// regardless of the configured merge engine.
pub(crate) fn validate_no_aggregation_on_sequence_field(
    options: &HashMap<String, String>,
) -> crate::Result<()> {
    let core_options = CoreOptions::new(options);
    let sequence_fields = core_options.sequence_fields();
    if sequence_fields.is_empty() {
        return Ok(());
    }

    for field in sequence_fields {
        let key = format!("{FIELDS_PREFIX}{field}{AGG_FUNCTION_SUFFIX}");
        if options.contains_key(&key) {
            return Err(crate::Error::ConfigInvalid {
                message: format!("Should not define aggregation on sequence field: '{field}'."),
            });
        }
    }

    Ok(())
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
    use crate::spec::{IntType, VarCharType};
    use crate::table::aggregator::new_aggregator;

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

    fn pk() -> Vec<String> {
        vec!["id".to_string()]
    }

    fn sample_fields() -> Vec<DataField> {
        // String columns are unbounded VARCHAR (STRING): that is what Java
        // `listagg` requires, and what DataFusion produces for `STRING`.
        vec![
            DataField::new(0, "id".into(), DataType::Int(IntType::new())),
            DataField::new(1, "price".into(), DataType::Int(IntType::new())),
            DataField::new(2, "amount".into(), DataType::Int(IntType::new())),
            DataField::new(
                3,
                "tag".into(),
                DataType::VarChar(VarCharType::string_type()),
            ),
            DataField::new(
                4,
                "tags".into(),
                DataType::VarChar(VarCharType::string_type()),
            ),
            DataField::new(
                5,
                "payload".into(),
                DataType::VarChar(VarCharType::string_type()),
            ),
        ]
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
            config
                .validate_create_mode(&pk(), &sample_fields())
                .unwrap(),
            Some(AggregationMode::Basic)
        );
    }

    #[test]
    fn test_validate_create_mode_ignores_non_pk_tables() {
        let options = aggregation_options(&[("fields.x.ignore-retract", "true")]);
        let config = AggregationConfig::new(&options);

        assert_eq!(
            config.validate_create_mode(&[], &sample_fields()).unwrap(),
            None
        );
    }

    #[test]
    fn test_is_enabled_disabled_for_other_engines() {
        let options = HashMap::from([(MERGE_ENGINE_OPTION.to_string(), "partial-update".into())]);
        let config = AggregationConfig::new(&options);
        assert!(!config.is_enabled());
        assert_eq!(
            config
                .validate_create_mode(&pk(), &sample_fields())
                .unwrap(),
            None
        );
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
            let err = config
                .validate_create_mode(&pk(), &sample_fields())
                .unwrap_err();
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

    #[test]
    fn test_validate_create_mode_rejects_unknown_field() {
        // typo: `amout` instead of `amount`
        let options = aggregation_options(&[("fields.amout.aggregate-function", "sum")]);
        let err = AggregationConfig::new(&options)
            .validate_create_mode(&pk(), &sample_fields())
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("amout")
                    && message.contains("fields.amout.aggregate-function")
                    && message.contains("amount")),
            "expected unknown-field error to surface the typo + available columns, got {err:?}"
        );
    }

    #[test]
    fn test_validate_create_mode_rejects_unknown_field_for_list_agg_delimiter() {
        // typo: `tga` instead of `tag`; without this check `listagg` on `tag`
        // would silently fall back to the default delimiter at read time.
        let options = aggregation_options(&[
            ("fields.tag.aggregate-function", "listagg"),
            ("fields.tga.list-agg-delimiter", "|"),
        ]);
        let err = AggregationConfig::new(&options)
            .validate_create_mode(&pk(), &sample_fields())
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("tga")
                    && message.contains("fields.tga.list-agg-delimiter")
                    && message.contains("tag")),
            "expected unknown-field error for list-agg-delimiter typo, got {err:?}"
        );
    }

    #[test]
    fn test_validate_create_mode_rejects_unknown_function_name() {
        let options = aggregation_options(&[("fields.amount.aggregate-function", "sume")]);
        let err = AggregationConfig::new(&options)
            .validate_create_mode(&pk(), &sample_fields())
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("sume") && message.contains("amount")),
            "expected unknown-function error, got {err:?}"
        );
    }

    #[test]
    fn test_validate_create_mode_rejects_incompatible_function_type() {
        // sum on a VarChar column.
        let options = aggregation_options(&[("fields.tag.aggregate-function", "sum")]);
        let err = AggregationConfig::new(&options)
            .validate_create_mode(&pk(), &sample_fields())
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("sum") && message.contains("tag")),
            "expected incompatible-type error, got {err:?}"
        );
    }

    /// Java registers `first_not_null_value` as an SPI alias of
    /// `first_non_null_value`, so a Java-written schema can carry it on all three
    /// name-keyed paths: a non-primary-key column (type-checked), a primary-key
    /// column (name-only), and `fields.default-aggregate-function`.
    #[test]
    fn test_legacy_first_not_null_value_alias_is_accepted() {
        for (key, value) in [
            ("fields.amount.aggregate-function", "first_not_null_value"),
            ("fields.id.aggregate-function", "first_not_null_value"),
            (FIELDS_DEFAULT_AGG_FUNCTION_OPTION, "first_not_null_value"),
        ] {
            let options = aggregation_options(&[(key, value)]);
            AggregationConfig::new(&options)
                .validate_create_mode(&pk(), &sample_fields())
                .unwrap_or_else(|err| panic!("'{key}' = '{value}' should be accepted: {err:?}"));
        }
    }

    /// The alias resolves by exact match, mirroring Java's
    /// `FactoryUtil#discoverFactory`, which compares identifiers with `equals`.
    /// Neither case nor `-`/`_` is normalised, and the reported name is the one
    /// the user wrote.
    #[test]
    fn test_legacy_alias_near_misses_are_still_rejected() {
        for name in [
            "first_not_null_valu",
            "first-not-null-value",
            "FIRST_NOT_NULL_VALUE",
            "not_null_value",
        ] {
            let options = aggregation_options(&[("fields.amount.aggregate-function", name)]);
            let err = AggregationConfig::new(&options)
                .validate_create_mode(&pk(), &sample_fields())
                .unwrap_err();
            assert!(
                matches!(err, crate::Error::ConfigInvalid { ref message }
                    if message.contains(name) && message.contains("amount")),
                "'{name}' should be rejected and echoed back verbatim, got {err:?}"
            );
        }
    }

    #[test]
    fn test_rejects_aggregation_on_sequence_field_for_every_merge_engine() {
        // Java rejects aggregation definitions on sequence fields inside
        // `validateSequenceField`, which runs for every merge engine; the
        // runtime still forces sequence fields to last_value when reading old
        // or externally-created metadata.
        for engine in [
            None,
            Some("deduplicate"),
            Some("first-row"),
            Some("partial-update"),
            Some("aggregation"),
        ] {
            let mut options = HashMap::from([
                ("sequence.field".to_string(), "amount".to_string()),
                (
                    "fields.amount.aggregate-function".to_string(),
                    "listagg".to_string(),
                ),
            ]);
            if let Some(engine) = engine {
                options.insert(MERGE_ENGINE_OPTION.to_string(), engine.to_string());
            }

            let err = validate_no_aggregation_on_sequence_field(&options).unwrap_err();
            assert!(
                matches!(err, crate::Error::ConfigInvalid { ref message }
                    if message.contains("sequence field") && message.contains("amount")),
                "merge-engine={engine:?} should reject aggregation on a sequence field, got {err:?}"
            );
        }
    }

    #[test]
    fn test_accepts_aggregation_on_a_non_sequence_field() {
        // Guard against over-rejecting: only the sequence field itself is
        // off limits, other columns may carry an aggregate function.
        let options = aggregation_options(&[
            ("sequence.field", "amount"),
            ("fields.price.aggregate-function", "sum"),
        ]);
        assert!(validate_no_aggregation_on_sequence_field(&options).is_ok());
    }

    #[test]
    fn test_accepts_options_without_a_sequence_field() {
        let options = aggregation_options(&[("fields.amount.aggregate-function", "sum")]);
        assert!(validate_no_aggregation_on_sequence_field(&options).is_ok());
    }

    #[test]
    fn test_validate_create_mode_skips_type_check_for_primary_key() {
        // `id` is an INT primary key; the runtime copies PK columns through
        // without an aggregator, so the incompatible `listagg` is ignored.
        let options = aggregation_options(&[("fields.id.aggregate-function", "listagg")]);
        let config = AggregationConfig::new(&options);

        assert_eq!(
            config
                .validate_create_mode(&pk(), &sample_fields())
                .unwrap(),
            Some(AggregationMode::Basic)
        );
    }

    #[test]
    fn test_validate_create_mode_rejects_unknown_function_on_primary_key() {
        // Primary-key fields are copied through at runtime, but a configured
        // aggregation function name still must be valid so typos fail fast.
        let options = aggregation_options(&[("fields.id.aggregate-function", "lisstagg")]);
        let err = AggregationConfig::new(&options)
            .validate_create_mode(&pk(), &sample_fields())
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("lisstagg") && message.contains("id")),
            "expected unknown-function error on primary-key field, got {err:?}"
        );
    }

    #[test]
    fn test_validate_create_mode_rejects_unknown_default_function() {
        let options =
            aggregation_options(&[("fields.default-aggregate-function", "totally_made_up")]);
        let err = AggregationConfig::new(&options)
            .validate_create_mode(&pk(), &sample_fields())
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("totally_made_up")
                    && message.contains("fields.default-aggregate-function")),
            "expected unknown-default-function error, got {err:?}"
        );
    }

    /// Lock the type-compatibility table in this module against the actual
    /// constructors in `crate::table::aggregator`.  Adding or removing a
    /// supported type in one place but not the other will break this test.
    #[test]
    fn validation_table_matches_constructors() {
        use crate::spec::{
            BigIntType, BooleanType, DateType, DecimalType, DoubleType, FloatType, SmallIntType,
            TimeType, TimestampType, TinyIntType,
        };

        let names = [
            "sum",
            "product",
            "min",
            "max",
            "last_value",
            "first_value",
            "last_non_null_value",
            "first_non_null_value",
            // Java's SPI alias of the entry above; both sides must resolve it
            // identically, which is exactly what this test locks.
            "first_not_null_value",
            "bool_and",
            "bool_or",
            "listagg",
        ];

        let sample_types: Vec<DataType> = vec![
            DataType::Boolean(BooleanType::new()),
            DataType::TinyInt(TinyIntType::new()),
            DataType::SmallInt(SmallIntType::new()),
            DataType::Int(IntType::new()),
            DataType::BigInt(BigIntType::new()),
            DataType::Float(FloatType::new()),
            DataType::Double(DoubleType::new()),
            DataType::Decimal(DecimalType::new(10, 2).unwrap()),
            DataType::Date(DateType::new()),
            DataType::Time(TimeType::new(3).unwrap()),
            DataType::Timestamp(TimestampType::new(6).unwrap()),
            // Bounded VARCHAR (listagg must reject) and unbounded STRING
            // (listagg must accept) — exercises both sides of the listagg rule.
            DataType::VarChar(VarCharType::new(255).unwrap()),
            DataType::VarChar(VarCharType::string_type()),
        ];

        let opts: HashMap<String, String> = HashMap::new();
        for name in names {
            for dt in &sample_types {
                let from_validator = validate_aggregator_for_type(name, "field", dt);
                let from_constructor = new_aggregator(name, "field", dt, &opts).map(|_| ());
                assert_eq!(
                    from_validator.is_ok(),
                    from_constructor.is_ok(),
                    "validate_aggregator_for_type and new_aggregator disagree on \
                     ({name}, {dt:?}): validator={from_validator:?} constructor={from_constructor:?}"
                );
            }
        }
    }

    #[test]
    fn test_rename_field_scoped_options_case2_rewrites_key_only() {
        let mut options: HashMap<String, String> = [
            ("fields.a.aggregate-function", "sum"),
            ("fields.a.ignore-retract", "true"),
            ("fields.a.distinct", "true"),
            ("fields.a.list-agg-delimiter", ";"),
            ("fields.a.map.storage-layout", "shared-shredding"),
            ("fields.a.map.shared-shredding.max-columns", "64"),
            (
                "fields.a.map.shared-shredding.column-placement-policy",
                "even",
            ),
            // Same leading characters but a different column: must not move.
            ("fields.ab.aggregate-function", "max"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();

        rename_field_scoped_options(&mut options, "a", "b");

        for (suffix, value) in [
            ("aggregate-function", "sum"),
            ("ignore-retract", "true"),
            ("distinct", "true"),
            ("list-agg-delimiter", ";"),
            ("map.storage-layout", "shared-shredding"),
            ("map.shared-shredding.max-columns", "64"),
            ("map.shared-shredding.column-placement-policy", "even"),
        ] {
            assert_eq!(
                options
                    .get(&format!("fields.b.{suffix}"))
                    .map(String::as_str),
                Some(value),
                "expected fields.b.{suffix} to carry the original value"
            );
            assert!(
                !options.contains_key(&format!("fields.a.{suffix}")),
                "old key fields.a.{suffix} should be gone"
            );
        }
        assert_eq!(
            options
                .get("fields.ab.aggregate-function")
                .map(String::as_str),
            Some("max")
        );
    }

    #[test]
    fn test_rename_field_scoped_options_case3_rewrites_key_and_value() {
        let mut options: HashMap<String, String> = [
            // Rename hits both the key's field list and the value.
            ("fields.g1,g2.sequence-group", "g2,price"),
            // No reference to the renamed column: must stay untouched.
            ("fields.other.sequence-group", "price"),
            // nested-key: the column can appear in the key and in the value.
            ("fields.g2.nested-key", "g2"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();

        rename_field_scoped_options(&mut options, "g2", "g3");

        assert_eq!(
            options
                .get("fields.g1,g3.sequence-group")
                .map(String::as_str),
            Some("g3,price")
        );
        assert_eq!(
            options
                .get("fields.other.sequence-group")
                .map(String::as_str),
            Some("price")
        );
        assert_eq!(
            options.get("fields.g3.nested-key").map(String::as_str),
            Some("g3")
        );
        assert!(!options.contains_key("fields.g1,g2.sequence-group"));
        assert!(!options.contains_key("fields.g2.nested-key"));
    }
}
