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

use crate::spec::core_options::{
    first_row_supports_changelog_producer, CoreOptions, BUCKET_KEY_OPTION,
    QUERY_AUTH_ENABLED_OPTION, SEQUENCE_FIELD_OPTION,
};
use crate::spec::types::{ArrayType, DataType, MapType, MultisetType, RowType};
use crate::spec::{
    remove_field_scoped_options, rename_field_scoped_options, AggregationConfig, ColumnMove,
    ColumnMoveType, PartialUpdateConfig,
};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::{HashMap, HashSet};

/// The table schema for paimon table.
///
/// Impl References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/TableSchema.java#L47>
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSchema {
    /// version of schema for paimon
    version: i32,
    id: i64,
    fields: Vec<DataField>,
    highest_field_id: i32,
    partition_keys: Vec<String>,
    primary_keys: Vec<String>,
    options: HashMap<String, String>,
    comment: Option<String>,
    time_millis: i64,
}

impl TableSchema {
    pub const CURRENT_VERSION: i32 = 3;

    /// Create a TableSchema from a Schema with the given ID.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/TableSchema.java#L373>
    pub fn new(id: i64, schema: &Schema) -> Self {
        let fields = schema.fields().to_vec();
        let highest_field_id = Self::current_highest_field_id(&fields);

        Self {
            version: Self::CURRENT_VERSION,
            id,
            fields,
            highest_field_id,
            partition_keys: schema.partition_keys().to_vec(),
            primary_keys: schema.primary_keys().to_vec(),
            options: schema.options().clone(),
            comment: schema.comment().map(|s| s.to_string()),
            time_millis: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Get the highest field ID from a list of fields, including fields nested
    /// inside row types (mirrors Java `RowType.currentHighestFieldId`).
    pub fn current_highest_field_id(fields: &[DataField]) -> i32 {
        fields
            .iter()
            .map(|f| f.id().max(highest_nested_field_id(f.data_type())))
            .max()
            .unwrap_or(-1)
    }

    pub fn version(&self) -> i32 {
        self.version
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn fields(&self) -> &[DataField] {
        &self.fields
    }

    pub fn highest_field_id(&self) -> i32 {
        self.highest_field_id
    }

    pub fn partition_keys(&self) -> &[String] {
        &self.partition_keys
    }

    pub fn partition_fields(&self) -> Vec<DataField> {
        self.partition_keys
            .iter()
            .filter_map(|key| self.fields.iter().find(|f| f.name() == key).cloned())
            .collect()
    }

    pub fn primary_keys(&self) -> &[String] {
        &self.primary_keys
    }

    /// Primary keys with partition columns removed.
    ///
    /// Within a single partition the partition columns are constant, so they
    /// are redundant in the KV key. Java Paimon calls these "trimmed primary keys".
    pub fn trimmed_primary_keys(&self) -> Vec<String> {
        if self.partition_keys.is_empty() {
            return self.primary_keys.clone();
        }
        let partition_set: HashSet<&str> = self.partition_keys.iter().map(String::as_str).collect();
        self.primary_keys
            .iter()
            .filter(|pk| !partition_set.contains(pk.as_str()))
            .cloned()
            .collect()
    }

    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }

    /// Create a copy of this schema with extra options merged in.
    ///
    /// A stored `query-auth.enabled = true` can't be turned off by a dynamic override.
    pub fn copy_with_options(&self, mut extra: HashMap<String, String>) -> Self {
        if CoreOptions::new(&self.options).query_auth_enabled() {
            extra.insert(QUERY_AUTH_ENABLED_OPTION.to_string(), "true".to_string());
        }
        let mut new_schema = self.clone();
        new_schema.options.extend(extra);
        new_schema
    }

    /// Create a copy of this schema with the options replaced entirely,
    /// keeping id, fields, keys, comment, and timestamps.
    ///
    /// Corresponds to Java `TableSchema.copy(Map<String, String> newOptions)`,
    /// which constructs a new schema with the given options rather than
    /// merging them.
    pub fn copy_with_replaced_options(&self, options: HashMap<String, String>) -> Self {
        let mut new_schema = self.clone();
        new_schema.options = options;
        new_schema
    }

    /// Apply a list of schema changes and return a new schema with incremented ID.
    ///
    /// Column-level changes operate on **top-level** columns only: a
    /// `field_names` path with more than one element (a nested struct field) is
    /// rejected with [`crate::Error::Unsupported`].
    ///
    /// Column errors ([`crate::Error::ColumnNotExist`] /
    /// [`crate::Error::ColumnAlreadyExist`]) are returned with an empty table
    /// name; the calling catalog fills in the table's full name.
    pub fn apply_changes(&self, changes: Vec<crate::spec::SchemaChange>) -> crate::Result<Self> {
        use crate::spec::SchemaChange;

        // Column errors carry no table name here; the catalog layer fills it in.
        let full_name = "";

        // Both flags are read from the pre-alter options, mirroring Java
        // `SchemaManager.applySchemaChanges`.
        let disable_null_to_not_null = self
            .options
            .get(crate::spec::DISABLE_ALTER_COLUMN_NULL_TO_NOT_NULL_OPTION)
            .map(|v| v == "true")
            .unwrap_or(true);
        let allow_explicit_cast = self
            .options
            .get(crate::spec::DISABLE_EXPLICIT_TYPE_CASTING_OPTION)
            .map(|v| v != "true")
            .unwrap_or(true);

        let mut new_schema = self.clone();
        new_schema.id += 1;
        new_schema.time_millis = chrono::Utc::now().timestamp_millis();

        // Operate on an owned field list, then write it back.
        let mut fields = std::mem::take(&mut new_schema.fields);
        let mut highest_field_id = new_schema.highest_field_id;

        for change in changes {
            match change {
                SchemaChange::SetOption { key, value } => {
                    new_schema.options.insert(key, value);
                }
                SchemaChange::RemoveOption { key } => {
                    new_schema.options.remove(&key);
                }
                SchemaChange::UpdateComment { comment } => {
                    new_schema.comment = comment;
                }
                SchemaChange::AddColumn {
                    field_names,
                    data_type,
                    comment,
                    column_move,
                } => {
                    let name = top_level_field(&field_names)?;
                    if field_index(&fields, name).is_some() {
                        return Err(crate::Error::ColumnAlreadyExist {
                            full_name: full_name.to_string(),
                            column: name.to_string(),
                        });
                    }
                    // Mirrors Java: an added column has no value for existing
                    // rows, so it must be nullable.
                    if !data_type.is_nullable() {
                        return Err(crate::Error::ConfigInvalid {
                            message: format!("Column {name} cannot specify NOT NULL."),
                        });
                    }
                    highest_field_id += 1;
                    let id = highest_field_id;
                    let data_type = reassign_field_ids(data_type, &mut highest_field_id);
                    let field =
                        DataField::new(id, name.to_string(), data_type).with_description(comment);
                    insert_field_with_move(&mut fields, field, column_move.as_ref(), full_name)?;
                }
                SchemaChange::RenameColumn {
                    field_names,
                    new_name,
                } => {
                    let name = top_level_field(&field_names)?;
                    // Existing partition data is laid out with the old key name
                    // in paths and metadata; renaming would break resolution.
                    if new_schema.partition_keys.iter().any(|k| k == name) {
                        return Err(crate::Error::Unsupported {
                            message: format!("Cannot rename partition column: [{name}]"),
                        });
                    }
                    let idx =
                        field_index(&fields, name).ok_or_else(|| crate::Error::ColumnNotExist {
                            full_name: full_name.to_string(),
                            column: name.to_string(),
                        })?;
                    if new_name != name && field_index(&fields, &new_name).is_some() {
                        return Err(crate::Error::ColumnAlreadyExist {
                            full_name: full_name.to_string(),
                            column: new_name,
                        });
                    }
                    fields[idx] = fields[idx].clone().with_name(new_name.clone());
                    rename_in_keys(&mut new_schema.primary_keys, name, &new_name);
                    rename_in_option_list(
                        &mut new_schema.options,
                        BUCKET_KEY_OPTION,
                        name,
                        &new_name,
                    );
                    rename_in_option_list(
                        &mut new_schema.options,
                        SEQUENCE_FIELD_OPTION,
                        name,
                        &new_name,
                    );
                    // Field-scoped aggregation options encode the column in the
                    // key (`fields.<col>.aggregate-function` / `.list-agg-delimiter`),
                    // so they must be rewritten too, mirroring Java
                    // `SchemaManager.applyRenameColumnsToOptions`.
                    rename_field_scoped_options(&mut new_schema.options, name, &new_name);
                }
                SchemaChange::DropColumn { field_names } => {
                    let name = top_level_field(&field_names)?;
                    let idx =
                        field_index(&fields, name).ok_or_else(|| crate::Error::ColumnNotExist {
                            full_name: full_name.to_string(),
                            column: name.to_string(),
                        })?;
                    if new_schema.partition_keys.iter().any(|k| k == name)
                        || new_schema.primary_keys.iter().any(|k| k == name)
                    {
                        return Err(crate::Error::Unsupported {
                            message: format!(
                                "Cannot drop partition or primary key column '{name}' of table {full_name}"
                            ),
                        });
                    }
                    // Dropping a column referenced by `bucket-key` / `sequence.field`
                    // would silently break bucket assignment / sequence ordering on
                    // existing data (e.g. `bucket_key_indices` becomes empty and writes
                    // fall back to bucket 0), so reject it instead.
                    {
                        let core_options = CoreOptions::new(&new_schema.options);
                        if core_options
                            .bucket_key()
                            .is_some_and(|keys| keys.iter().any(|k| k == name))
                        {
                            return Err(crate::Error::Unsupported {
                                message: format!(
                                    "Cannot drop column '{name}' referenced by '{BUCKET_KEY_OPTION}'"
                                ),
                            });
                        }
                        if core_options.sequence_fields().contains(&name) {
                            return Err(crate::Error::Unsupported {
                                message: format!(
                                    "Cannot drop column '{name}' referenced by '{SEQUENCE_FIELD_OPTION}'"
                                ),
                            });
                        }
                    }
                    if fields.len() == 1 {
                        return Err(crate::Error::Unsupported {
                            message: "Cannot drop all fields in table".to_string(),
                        });
                    }
                    fields.remove(idx);
                    // Drop the column's field-scoped aggregation options so no
                    // orphaned `fields.<col>.*` keys remain (which would otherwise
                    // fail the aggregation re-validation below).
                    remove_field_scoped_options(&mut new_schema.options, name);
                }
                SchemaChange::UpdateColumnType {
                    field_names,
                    new_data_type,
                    keep_nullability,
                } => {
                    let name = top_level_field(&field_names)?;
                    // Existing partitions, bucket assignment, and key encoding
                    // were all written with the old key type.
                    if new_schema.partition_keys.iter().any(|k| k == name) {
                        return Err(crate::Error::Unsupported {
                            message: format!("Cannot update partition column: [{name}]"),
                        });
                    }
                    if new_schema.primary_keys.iter().any(|k| k == name) {
                        return Err(crate::Error::Unsupported {
                            message: "Cannot update primary key".to_string(),
                        });
                    }
                    let idx =
                        field_index(&fields, name).ok_or_else(|| crate::Error::ColumnNotExist {
                            full_name: full_name.to_string(),
                            column: name.to_string(),
                        })?;
                    let old = &fields[idx];
                    // Mirrors Java `assertNotChangingBlobColumnType`: BLOB
                    // columns use a dedicated storage layout that other types
                    // cannot be converted to or from.
                    if old.data_type().is_blob_type() || new_data_type.is_blob_type() {
                        return Err(crate::Error::Unsupported {
                            message: format!(
                                "Cannot change column type involving BLOB: [{name}] {:?} -> {new_data_type:?}",
                                old.data_type()
                            ),
                        });
                    }
                    let target = if keep_nullability {
                        new_data_type.copy_with_nullable(old.data_type().is_nullable())?
                    } else {
                        assert_nullability_change(
                            old.data_type().is_nullable(),
                            new_data_type.is_nullable(),
                            name,
                            disable_null_to_not_null,
                        )?;
                        new_data_type
                    };
                    // Existing data files keep the old schema; the read path
                    // casts old columns to the new type, so the change must be
                    // both a supported Paimon cast and executable by arrow.
                    let arrow_castable = arrow_cast::can_cast_types(
                        &crate::arrow::paimon_type_to_arrow(old.data_type())?,
                        &crate::arrow::paimon_type_to_arrow(&target)?,
                    );
                    if !crate::spec::supports_cast(old.data_type(), &target, allow_explicit_cast)
                        || !arrow_castable
                    {
                        return Err(crate::Error::Unsupported {
                            message: format!(
                                "Column type {name}[{:?}] cannot be converted to {target:?} without losing information.",
                                old.data_type()
                            ),
                        });
                    }
                    fields[idx] = DataField::new(old.id(), old.name().to_string(), target)
                        .with_description(old.description().map(|s| s.to_string()));
                }
                SchemaChange::UpdateColumnNullability {
                    field_names,
                    new_nullability,
                } => {
                    let name = top_level_field(&field_names)?;
                    // Primary keys are normalized to NOT NULL at create time;
                    // a nullable key column would break key/bucket semantics.
                    if new_nullability && new_schema.primary_keys.iter().any(|k| k == name) {
                        return Err(crate::Error::Unsupported {
                            message: "Cannot change nullability of primary key".to_string(),
                        });
                    }
                    let idx =
                        field_index(&fields, name).ok_or_else(|| crate::Error::ColumnNotExist {
                            full_name: full_name.to_string(),
                            column: name.to_string(),
                        })?;
                    let old = &fields[idx];
                    assert_nullability_change(
                        old.data_type().is_nullable(),
                        new_nullability,
                        name,
                        disable_null_to_not_null,
                    )?;
                    let nt = old.data_type().copy_with_nullable(new_nullability)?;
                    fields[idx] = DataField::new(old.id(), old.name().to_string(), nt)
                        .with_description(old.description().map(|s| s.to_string()));
                }
                SchemaChange::UpdateColumnComment {
                    field_names,
                    new_comment,
                } => {
                    let name = top_level_field(&field_names)?;
                    let idx =
                        field_index(&fields, name).ok_or_else(|| crate::Error::ColumnNotExist {
                            full_name: full_name.to_string(),
                            column: name.to_string(),
                        })?;
                    fields[idx] = fields[idx].clone().with_description(Some(new_comment));
                }
                SchemaChange::UpdateColumnPosition { column_move } => {
                    apply_move(&mut fields, &column_move, full_name)?;
                }
            }
        }

        new_schema.fields = fields;
        new_schema.highest_field_id =
            highest_field_id.max(Self::current_highest_field_id(&new_schema.fields));

        // Re-run create-time validations on the final schema, mirroring Java
        // `SchemaValidation.validateTableSchema` after applying changes.
        Schema::validate_key_field_types(
            &new_schema.fields,
            &new_schema.primary_keys,
            &new_schema.options,
        )?;
        Schema::validate_blob_fields(
            &new_schema.fields,
            &new_schema.partition_keys,
            &new_schema.options,
        )?;
        Schema::validate_vector_store_fields(
            &new_schema.fields,
            &new_schema.partition_keys,
            &new_schema.options,
        )?;
        PartialUpdateConfig::new(&new_schema.options)
            .validate_create_mode(!new_schema.primary_keys.is_empty())?;
        AggregationConfig::new(&new_schema.options)
            .validate_create_mode(&new_schema.primary_keys, &new_schema.fields)?;
        Schema::validate_first_row_changelog_producer(&new_schema.options)?;
        Ok(new_schema)
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn time_millis(&self) -> i64 {
        self.time_millis
    }

    /// Compute the effective bucket key columns.
    ///
    /// Priority: explicit `bucket-key` option > primary keys > all non-partition fields.
    pub fn bucket_keys(&self) -> Vec<String> {
        let core_options = CoreOptions::new(&self.options);
        if let Some(keys) = core_options.bucket_key() {
            return keys;
        }
        if !self.primary_keys.is_empty() {
            return self.trimmed_primary_keys();
        }
        let partition_set: HashSet<&str> = self.partition_keys.iter().map(String::as_str).collect();
        self.fields
            .iter()
            .filter(|f| !partition_set.contains(f.name()))
            .map(|f| f.name().to_string())
            .collect()
    }
}

/// Extract the single top-level column name from a `field_names` path.
///
/// Nested struct field paths (length > 1) are not yet supported.
fn top_level_field(field_names: &[String]) -> crate::Result<&str> {
    match field_names {
        [name] => Ok(name.as_str()),
        [] => Err(crate::Error::ConfigInvalid {
            message: "Schema change has empty fieldNames".to_string(),
        }),
        _ => Err(crate::Error::Unsupported {
            message: format!("Altering nested struct fields is not supported yet: {field_names:?}"),
        }),
    }
}

/// Index of the field with the given name, if any.
fn field_index(fields: &[DataField], name: &str) -> Option<usize> {
    fields.iter().position(|f| f.name() == name)
}

/// Mirrors Java `SchemaManager.assertNullabilityChange`: converting a nullable
/// column to NOT NULL is rejected unless explicitly enabled, because existing
/// rows may already contain NULLs.
fn assert_nullability_change(
    old_nullable: bool,
    new_nullable: bool,
    field_name: &str,
    disable_null_to_not_null: bool,
) -> crate::Result<()> {
    if disable_null_to_not_null && old_nullable && !new_nullable {
        return Err(crate::Error::Unsupported {
            message: format!(
                "Cannot update column type from nullable to non nullable for {field_name}. \
                 You can set table configuration option 'alter-column-null-to-not-null.disabled' = 'false' \
                 to allow converting null columns to not null"
            ),
        });
    }
    Ok(())
}

/// Rename a key in a partition/primary key list, if present.
fn rename_in_keys(keys: &mut [String], old: &str, new: &str) {
    for key in keys.iter_mut() {
        if key == old {
            *key = new.to_string();
        }
    }
}

/// Rename a column inside a comma-separated column-list option (`bucket-key`,
/// `sequence.field`), if the option is set and references the column.
///
/// Mirrors Java `SchemaManager.applyRenameColumnsToOptions`.
fn rename_in_option_list(
    options: &mut HashMap<String, String>,
    option_key: &str,
    old: &str,
    new: &str,
) {
    let Some(value) = options.get(option_key) else {
        return;
    };
    let renamed = value
        .split(',')
        .map(|col| if col == old { new } else { col })
        .collect::<Vec<_>>()
        .join(",");
    options.insert(option_key.to_string(), renamed);
}

/// The highest field ID nested inside a data type, or -1 if it contains none.
fn highest_nested_field_id(data_type: &DataType) -> i32 {
    match data_type {
        DataType::Array(t) => highest_nested_field_id(t.element_type()),
        DataType::Multiset(t) => highest_nested_field_id(t.element_type()),
        DataType::Map(t) => {
            highest_nested_field_id(t.key_type()).max(highest_nested_field_id(t.value_type()))
        }
        DataType::Row(t) => t
            .fields()
            .iter()
            .map(|f| f.id().max(highest_nested_field_id(f.data_type())))
            .max()
            .unwrap_or(-1),
        _ => -1,
    }
}

/// Reassign the IDs of all row fields nested inside a data type from the
/// table-wide highest field ID, so they cannot collide with existing fields.
///
/// Mirrors Java `ReassignFieldId`: IDs nested inside a field's type are
/// assigned before the field's own ID.
fn reassign_field_ids(data_type: DataType, next_id: &mut i32) -> DataType {
    let nullable = data_type.is_nullable();
    match data_type {
        DataType::Array(t) => DataType::Array(ArrayType::with_nullable(
            nullable,
            reassign_field_ids(t.element_type().clone(), next_id),
        )),
        DataType::Multiset(t) => DataType::Multiset(MultisetType::with_nullable(
            nullable,
            reassign_field_ids(t.element_type().clone(), next_id),
        )),
        DataType::Map(t) => DataType::Map(MapType::with_nullable(
            nullable,
            reassign_field_ids(t.key_type().clone(), next_id),
            reassign_field_ids(t.value_type().clone(), next_id),
        )),
        DataType::Row(t) => {
            let fields = t
                .fields()
                .iter()
                .map(|f| {
                    let typ = reassign_field_ids(f.data_type().clone(), next_id);
                    *next_id += 1;
                    DataField::new(*next_id, f.name().to_string(), typ)
                        .with_description(f.description().map(|s| s.to_string()))
                })
                .collect();
            DataType::Row(RowType::with_nullable(nullable, fields))
        }
        other => other,
    }
}

/// Insert a brand-new field according to an optional move (used by `AddColumn`).
fn insert_field_with_move(
    fields: &mut Vec<DataField>,
    field: DataField,
    column_move: Option<&ColumnMove>,
    full_name: &str,
) -> crate::Result<()> {
    let Some(mv) = column_move else {
        fields.push(field);
        return Ok(());
    };
    match mv.move_type() {
        ColumnMoveType::FIRST => fields.insert(0, field),
        ColumnMoveType::LAST => fields.push(field),
        ColumnMoveType::AFTER | ColumnMoveType::BEFORE => {
            let reference = move_reference(mv)?;
            let ref_idx =
                field_index(fields, reference).ok_or_else(|| crate::Error::ColumnNotExist {
                    full_name: full_name.to_string(),
                    column: reference.to_string(),
                })?;
            let at = match mv.move_type() {
                ColumnMoveType::AFTER => ref_idx + 1,
                _ => ref_idx,
            };
            fields.insert(at, field);
        }
    }
    Ok(())
}

/// Move an existing field to a new position (used by `UpdateColumnPosition`).
///
/// Mirrors Java `SchemaManager.applyMove`: remove the field first, then resolve
/// the reference index in the reduced list so the offset is already adjusted.
fn apply_move(fields: &mut Vec<DataField>, mv: &ColumnMove, full_name: &str) -> crate::Result<()> {
    let idx = field_index(fields, mv.field_name()).ok_or_else(|| crate::Error::ColumnNotExist {
        full_name: full_name.to_string(),
        column: mv.field_name().to_string(),
    })?;
    let field = fields.remove(idx);
    match mv.move_type() {
        ColumnMoveType::FIRST => fields.insert(0, field),
        ColumnMoveType::LAST => fields.push(field),
        ColumnMoveType::AFTER | ColumnMoveType::BEFORE => {
            let reference = move_reference(mv)?;
            let ref_idx =
                field_index(fields, reference).ok_or_else(|| crate::Error::ColumnNotExist {
                    full_name: full_name.to_string(),
                    column: reference.to_string(),
                })?;
            let at = match mv.move_type() {
                ColumnMoveType::AFTER => ref_idx + 1,
                _ => ref_idx,
            };
            fields.insert(at, field);
        }
    }
    Ok(())
}

/// The reference (anchor) field name required by `AFTER`/`BEFORE` moves.
fn move_reference(mv: &ColumnMove) -> crate::Result<&str> {
    mv.reference_field_name()
        .ok_or_else(|| crate::Error::ConfigInvalid {
            message: format!(
                "Move of type {:?} requires a reference field name",
                mv.move_type()
            ),
        })
}

pub const ROW_ID_FIELD_NAME: &str = "_ROW_ID";

pub const ROW_ID_FIELD_ID: i32 = i32::MAX - 5;

pub const SEQUENCE_NUMBER_FIELD_NAME: &str = "_SEQUENCE_NUMBER";

/// Must match Java Paimon's `SpecialFields.SEQUENCE_NUMBER` (Integer.MAX_VALUE - 1).
pub const SEQUENCE_NUMBER_FIELD_ID: i32 = i32::MAX - 1;

pub const VALUE_KIND_FIELD_NAME: &str = "_VALUE_KIND";

/// Must match Java Paimon's `SpecialFields.VALUE_KIND` (Integer.MAX_VALUE - 2).
pub const VALUE_KIND_FIELD_ID: i32 = i32::MAX - 2;

/// Data field for paimon table.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/types/DataField.java#L40>
#[serde_as]
#[derive(Debug, Clone, PartialEq, Hash, Eq, Deserialize, Serialize)]
pub struct DataField {
    id: i32,
    name: String,
    #[serde(rename = "type")]
    typ: DataType,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

impl DataField {
    pub fn new(id: i32, name: String, typ: DataType) -> Self {
        Self {
            id,
            name,
            typ,
            description: None,
        }
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.typ
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn with_id(mut self, new_id: i32) -> Self {
        self.id = new_id;
        self
    }

    pub fn with_name(mut self, new_name: String) -> Self {
        self.name = new_name;
        self
    }

    pub fn with_description(mut self, new_description: Option<String>) -> Self {
        self.description = new_description;
        self
    }
}

pub fn escape_identifier(identifier: &str) -> String {
    identifier.replace('"', "\"\"")
}

pub fn escape_single_quotes(text: &str) -> String {
    text.replace('\'', "''")
}

// ======================= Schema (DDL) ===============================

/// Option key for primary key in table options (same as [CoreOptions.PRIMARY_KEY](https://github.com/apache/paimon/blob/release-1.3/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java)).
pub const PRIMARY_KEY_OPTION: &str = "primary-key";
/// Option key for partition in table options (same as [CoreOptions.PARTITION](https://github.com/apache/paimon/blob/release-1.3/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java)).
pub const PARTITION_OPTION: &str = "partition";
const MERGE_ENGINE_OPTION: &str = "merge-engine";

/// Schema of a table (logical DDL schema).
///
/// Corresponds to [org.apache.paimon.schema.Schema](https://github.com/apache/paimon/blob/release-1.3/paimon-api/src/main/java/org/apache/paimon/schema/Schema.java).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Schema {
    fields: Vec<DataField>,
    partition_keys: Vec<String>,
    primary_keys: Vec<String>,
    options: HashMap<String, String>,
    comment: Option<String>,
}

impl Schema {
    /// Build a schema with validation. Normalizes partition/primary keys from options if present.
    fn new(
        fields: Vec<DataField>,
        partition_keys: Vec<String>,
        primary_keys: Vec<String>,
        mut options: HashMap<String, String>,
        comment: Option<String>,
    ) -> crate::Result<Self> {
        let primary_keys = Self::normalize_primary_keys(&primary_keys, &mut options)?;
        let partition_keys = Self::normalize_partition_keys(&partition_keys, &mut options)?;
        let fields = Self::normalize_fields(&fields, &partition_keys, &primary_keys)?;
        Self::validate_key_field_types(&fields, &primary_keys, &options)?;
        Self::validate_blob_fields(&fields, &partition_keys, &options)?;
        Self::validate_vector_store_fields(&fields, &partition_keys, &options)?;
        PartialUpdateConfig::new(&options).validate_create_mode(!primary_keys.is_empty())?;
        AggregationConfig::new(&options).validate_create_mode(&primary_keys, &fields)?;
        Self::validate_first_row_changelog_producer(&options)?;

        Ok(Self {
            fields,
            partition_keys,
            primary_keys,
            options,
            comment,
        })
    }

    /// Normalize primary keys: optionally take from table options (`primary-key`), remove from options.
    /// Corresponds to Java `normalizePrimaryKeys`.
    fn normalize_primary_keys(
        primary_keys: &[String],
        options: &mut HashMap<String, String>,
    ) -> crate::Result<Vec<String>> {
        if let Some(pk) = options.remove(PRIMARY_KEY_OPTION) {
            if !primary_keys.is_empty() {
                return Err(crate::Error::ConfigInvalid {
                    message: "Cannot define primary key on DDL and table options at the same time."
                        .to_string(),
                });
            }
            return Ok(pk
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect());
        }
        Ok(primary_keys.to_vec())
    }

    /// Normalize partition keys: optionally take from table options (`partition`), remove from options.
    /// Corresponds to Java `normalizePartitionKeys`.
    fn normalize_partition_keys(
        partition_keys: &[String],
        options: &mut HashMap<String, String>,
    ) -> crate::Result<Vec<String>> {
        if let Some(part) = options.remove(PARTITION_OPTION) {
            if !partition_keys.is_empty() {
                return Err(crate::Error::ConfigInvalid {
                    message: "Cannot define partition on DDL and table options at the same time."
                        .to_string(),
                });
            }
            return Ok(part
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect());
        }
        Ok(partition_keys.to_vec())
    }

    /// Normalize fields: validate (duplicate/subset checks) and make primary key columns non-nullable.
    /// Corresponds to Java `normalizeFields`.
    fn normalize_fields(
        fields: &[DataField],
        partition_keys: &[String],
        primary_keys: &[String],
    ) -> crate::Result<Vec<DataField>> {
        let field_names: Vec<String> = fields.iter().map(|f| f.name().to_string()).collect();
        Self::validate_no_duplicate_fields(&field_names)?;
        Self::validate_partition_keys(&field_names, partition_keys)?;
        Self::validate_primary_keys(&field_names, primary_keys)?;
        Self::validate_primary_keys_not_partition_only(partition_keys, primary_keys)?;

        if primary_keys.is_empty() {
            return Ok(fields.to_vec());
        }

        let pk_set: HashSet<&str> = primary_keys.iter().map(String::as_str).collect();
        let mut new_fields = Vec::with_capacity(fields.len());
        for f in fields {
            if pk_set.contains(f.name()) && f.data_type().is_nullable() {
                new_fields.push(
                    DataField::new(
                        f.id(),
                        f.name().to_string(),
                        f.data_type().copy_with_nullable(false)?,
                    )
                    .with_description(f.description().map(|s| s.to_string())),
                );
            } else {
                new_fields.push(f.clone());
            }
        }
        Ok(new_fields)
    }

    /// Table columns must not contain duplicate field names.
    fn validate_no_duplicate_fields(field_names: &[String]) -> crate::Result<()> {
        let duplicates = Self::duplicate_fields(field_names);
        if duplicates.is_empty() {
            Ok(())
        } else {
            Err(crate::Error::ConfigInvalid {
                message: format!(
                    "Table column {field_names:?} must not contain duplicate fields. Found: {duplicates:?}"
                ),
            })
        }
    }

    /// Partition key constraint must not contain duplicates; all partition keys must be in table columns.
    fn validate_partition_keys(
        field_names: &[String],
        partition_keys: &[String],
    ) -> crate::Result<()> {
        let all_names: HashSet<&str> = field_names.iter().map(String::as_str).collect();
        let duplicates = Self::duplicate_fields(partition_keys);
        if !duplicates.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "Partition key constraint {partition_keys:?} must not contain duplicate columns. Found: {duplicates:?}"
                ),
            });
        }
        if !partition_keys
            .iter()
            .all(|k| all_names.contains(k.as_str()))
        {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "Table column {field_names:?} should include all partition fields {partition_keys:?}"
                ),
            });
        }
        Ok(())
    }

    /// Primary key constraint must not contain duplicates; all primary keys must be in table columns.
    fn validate_primary_keys(field_names: &[String], primary_keys: &[String]) -> crate::Result<()> {
        if primary_keys.is_empty() {
            return Ok(());
        }
        let all_names: HashSet<&str> = field_names.iter().map(String::as_str).collect();
        let duplicates = Self::duplicate_fields(primary_keys);
        if !duplicates.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "Primary key constraint {primary_keys:?} must not contain duplicate columns. Found: {duplicates:?}"
                ),
            });
        }
        if !primary_keys.iter().all(|k| all_names.contains(k.as_str())) {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "Table column {field_names:?} should include all primary key constraint {primary_keys:?}"
                ),
            });
        }
        Ok(())
    }

    fn validate_primary_keys_not_partition_only(
        partition_keys: &[String],
        primary_keys: &[String],
    ) -> crate::Result<()> {
        if primary_keys.is_empty() || partition_keys.is_empty() {
            return Ok(());
        }

        let partition_set: HashSet<&str> = partition_keys.iter().map(String::as_str).collect();
        if primary_keys
            .iter()
            .all(|pk| partition_set.contains(pk.as_str()))
        {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "Primary key constraint {primary_keys:?} should not be same with partition fields {partition_keys:?}, this will result in only one record in a partition"
                ),
            });
        }

        Ok(())
    }

    /// Reject types that cannot serve as a key (primary key or explicit
    /// `bucket-key`). Currently only `VECTOR` is rejected here: it is densely
    /// stored and has no key ordering, so it cannot be used as a key column.
    fn validate_key_field_types(
        fields: &[DataField],
        primary_keys: &[String],
        options: &HashMap<String, String>,
    ) -> crate::Result<()> {
        let reject = |key_kind: &str, name: &str| -> crate::Result<()> {
            let field = fields.iter().find(|f| f.name() == name);
            if let Some(field) = field {
                if matches!(field.data_type(), DataType::Vector(_)) {
                    return Err(crate::Error::ConfigInvalid {
                        message: format!(
                            "The VECTOR type of {key_kind} field '{name}' is unsupported."
                        ),
                    });
                }
            }
            Ok(())
        };

        for pk in primary_keys {
            reject("primary key", pk)?;
        }
        if let Some(bucket_keys) = CoreOptions::new(options).bucket_key() {
            for bk in &bucket_keys {
                reject("bucket key", bk)?;
            }
        }
        Ok(())
    }

    fn validate_blob_fields(
        fields: &[DataField],
        partition_keys: &[String],
        options: &HashMap<String, String>,
    ) -> crate::Result<()> {
        let blob_field_names = Self::top_level_blob_field_names(fields);
        if blob_field_names.is_empty() {
            return Ok(());
        }

        let core_options = CoreOptions::new(options);
        if !core_options.data_evolution_enabled() {
            return Err(crate::Error::ConfigInvalid {
                message: "Data evolution config must enabled for table with BLOB type column."
                    .to_string(),
            });
        }

        if fields.len() == blob_field_names.len() {
            return Err(crate::Error::ConfigInvalid {
                message: "Table with BLOB type column must have other normal columns.".to_string(),
            });
        }

        let partition_key_set: HashSet<&str> = partition_keys.iter().map(String::as_str).collect();
        if blob_field_names
            .iter()
            .any(|name| partition_key_set.contains(name))
        {
            return Err(crate::Error::ConfigInvalid {
                message: "The BLOB type column can not be part of partition keys.".to_string(),
            });
        }

        Ok(())
    }

    fn validate_vector_store_fields(
        fields: &[DataField],
        partition_keys: &[String],
        options: &HashMap<String, String>,
    ) -> crate::Result<()> {
        let vector_field_names = Self::top_level_vector_field_names(fields);
        let core_options = CoreOptions::new(options);
        if vector_field_names.is_empty() || core_options.vector_file_format().is_none() {
            return Ok(());
        }

        if !core_options.data_evolution_enabled() {
            return Err(crate::Error::ConfigInvalid {
                message:
                    "Data evolution config must enabled for table with dedicated VECTOR storage."
                        .to_string(),
            });
        }
        if !core_options.row_tracking_enabled() {
            return Err(crate::Error::ConfigInvalid {
                message:
                    "Row tracking config must enabled for table with dedicated VECTOR storage."
                        .to_string(),
            });
        }

        if fields.len() == vector_field_names.len() {
            return Err(crate::Error::ConfigInvalid {
                message: "Table with dedicated VECTOR storage must have other normal columns."
                    .to_string(),
            });
        }

        let partition_key_set: HashSet<&str> = partition_keys.iter().map(String::as_str).collect();
        if vector_field_names
            .iter()
            .any(|name| partition_key_set.contains(name))
        {
            return Err(crate::Error::ConfigInvalid {
                message: "The VECTOR type column can not be part of partition keys.".to_string(),
            });
        }

        Ok(())
    }

    fn validate_first_row_changelog_producer(
        options: &HashMap<String, String>,
    ) -> crate::Result<()> {
        if !options
            .get(MERGE_ENGINE_OPTION)
            .is_some_and(|value| value.eq_ignore_ascii_case("first-row"))
        {
            return Ok(());
        }

        let changelog_producer = CoreOptions::new(options)
            .try_changelog_producer()
            .map_err(Self::options_error_to_config_invalid)?;
        if first_row_supports_changelog_producer(changelog_producer) {
            return Ok(());
        }

        Err(crate::Error::ConfigInvalid {
            message: format!(
                "merge-engine=first-row only supports changelog-producer=none or lookup, but found changelog-producer={}",
                changelog_producer.as_str()
            ),
        })
    }

    fn options_error_to_config_invalid(error: crate::Error) -> crate::Error {
        match error {
            crate::Error::Unsupported { message } => crate::Error::ConfigInvalid { message },
            other => crate::Error::ConfigInvalid {
                message: other.to_string(),
            },
        }
    }

    /// Returns top-level Blob field names for create-time Blob contract checks.
    fn top_level_blob_field_names(fields: &[DataField]) -> Vec<&str> {
        fields
            .iter()
            .filter_map(|field| match field.data_type() {
                DataType::Blob(_) => Some(field.name()),
                _ => None,
            })
            .collect()
    }

    /// Returns top-level Vector field names for dedicated vector-store checks.
    fn top_level_vector_field_names(fields: &[DataField]) -> Vec<&str> {
        fields
            .iter()
            .filter_map(|field| match field.data_type() {
                DataType::Vector(_) => Some(field.name()),
                _ => None,
            })
            .collect()
    }

    /// Returns the set of names that appear more than once.
    pub fn duplicate_fields(names: &[String]) -> HashSet<String> {
        let mut seen = HashMap::new();
        for n in names {
            *seen.entry(n.clone()).or_insert(0) += 1;
        }
        seen.into_iter()
            .filter(|(_, count)| *count > 1)
            .map(|(name, _)| name)
            .collect()
    }

    /// Row type with these fields (nullable = false for table row).
    pub fn row_type(&self) -> RowType {
        RowType::with_nullable(false, self.fields.clone())
    }

    pub fn fields(&self) -> &[DataField] {
        &self.fields
    }

    pub fn partition_keys(&self) -> &[String] {
        &self.partition_keys
    }

    pub fn primary_keys(&self) -> &[String] {
        &self.primary_keys
    }

    /// Primary keys with partition columns removed.
    ///
    /// Within a single partition the partition columns are constant, so they
    /// are redundant in the KV key. Java Paimon calls these "trimmed primary keys".
    pub fn trimmed_primary_keys(&self) -> Vec<String> {
        if self.partition_keys.is_empty() {
            return self.primary_keys.clone();
        }
        let partition_set: HashSet<&str> = self.partition_keys.iter().map(String::as_str).collect();
        self.primary_keys
            .iter()
            .filter(|pk| !partition_set.contains(pk.as_str()))
            .cloned()
            .collect()
    }

    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    /// Create a new schema with the same keys/options/comment but different row type
    pub fn copy(&self, row_type: RowType) -> crate::Result<Self> {
        Self::new(
            row_type.fields().to_vec(),
            self.partition_keys.clone(),
            self.primary_keys.clone(),
            self.options.clone(),
            self.comment.clone(),
        )
    }

    /// Create a new builder for configuring a schema.
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }
}

/// Builder for [`Schema`].
pub struct SchemaBuilder {
    columns: Vec<DataField>,
    partition_keys: Vec<String>,
    primary_keys: Vec<String>,
    options: HashMap<String, String>,
    comment: Option<String>,
    next_field_id: i32,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            partition_keys: Vec::new(),
            primary_keys: Vec::new(),
            options: HashMap::new(),
            comment: None,
            next_field_id: 0,
        }
    }

    /// Add a column (name, data type).
    pub fn column(self, column_name: impl Into<String>, data_type: DataType) -> Self {
        self.column_with_description(column_name, data_type, None)
    }

    /// Add a column with optional description.
    pub fn column_with_description(
        mut self,
        column_name: impl Into<String>,
        data_type: DataType,
        description: Option<String>,
    ) -> Self {
        let name = column_name.into();
        let id = self.next_field_id;
        self.next_field_id += 1;
        let data_type = Self::assign_nested_field_ids(data_type, &mut self.next_field_id);
        self.columns
            .push(DataField::new(id, name, data_type).with_description(description));
        self
    }

    /// Set partition keys.
    pub fn partition_keys(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.partition_keys = names.into_iter().map(Into::into).collect();
        self
    }

    /// Set primary key columns. They must not be nullable.
    pub fn primary_key(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.primary_keys = names.into_iter().map(Into::into).collect();
        self
    }

    /// Set table options (merged with existing).
    pub fn options(mut self, opts: impl IntoIterator<Item = (String, String)>) -> Self {
        self.options.extend(opts);
        self
    }

    /// Set a single option.
    pub fn option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
        self
    }

    /// Set table comment.
    pub fn comment(mut self, comment: Option<String>) -> Self {
        self.comment = comment;
        self
    }

    /// Build the schema (validates and normalizes).
    pub fn build(self) -> crate::Result<Schema> {
        Schema::new(
            self.columns,
            self.partition_keys,
            self.primary_keys,
            self.options,
            self.comment,
        )
    }

    /// Recursively assign field IDs to nested fields in complex types.
    fn assign_nested_field_ids(data_type: DataType, next_id: &mut i32) -> DataType {
        let nullable = data_type.is_nullable();
        match data_type {
            DataType::Row(row) => {
                let fields = row
                    .fields()
                    .iter()
                    .map(|f| {
                        let id = *next_id;
                        *next_id += 1;
                        let typ = Self::assign_nested_field_ids(f.data_type().clone(), next_id);
                        DataField::new(id, f.name().to_string(), typ)
                    })
                    .collect();
                DataType::Row(RowType::with_nullable(nullable, fields))
            }
            DataType::Array(arr) => {
                let element = Self::assign_nested_field_ids(arr.element_type().clone(), next_id);
                DataType::Array(ArrayType::with_nullable(nullable, element))
            }
            DataType::Map(map) => {
                let key = Self::assign_nested_field_ids(map.key_type().clone(), next_id);
                let value = Self::assign_nested_field_ids(map.value_type().clone(), next_id);
                DataType::Map(MapType::with_nullable(nullable, key, value))
            }
            DataType::Multiset(ms) => {
                let element = Self::assign_nested_field_ids(ms.element_type().clone(), next_id);
                DataType::Multiset(MultisetType::with_nullable(nullable, element))
            }
            other => other,
        }
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::spec::{BlobType, FloatType, IntType, VarCharType, VectorType};

    use super::*;

    #[test]
    fn test_create_data_field() {
        let id = 1;
        let name = "field1".to_string();
        let typ = DataType::Int(IntType::new());
        let description = "test description".to_string();

        let data_field = DataField::new(id, name.clone(), typ.clone())
            .with_description(Some(description.clone()));

        assert_eq!(data_field.id(), id);
        assert_eq!(data_field.name(), name);
        assert_eq!(data_field.data_type(), &typ);
        assert_eq!(data_field.description(), Some(description).as_deref());
    }

    #[test]
    fn test_current_highest_field_id_includes_nested_fields() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "s".to_string(),
                DataType::Row(RowType::new(vec![DataField::new(
                    7,
                    "a".to_string(),
                    DataType::Array(ArrayType::new(DataType::Row(RowType::new(vec![
                        DataField::new(9, "b".to_string(), DataType::Int(IntType::new())),
                    ])))),
                )])),
            ),
        ];
        assert_eq!(TableSchema::current_highest_field_id(&fields), 9);
    }

    #[test]
    fn test_new_id() {
        let d_type = DataType::Int(IntType::new());
        let new_data_field = DataField::new(1, "field1".to_string(), d_type.clone()).with_id(2);

        assert_eq!(new_data_field.id(), 2);
        assert_eq!(new_data_field.name(), "field1");
        assert_eq!(new_data_field.data_type(), &d_type);
        assert_eq!(new_data_field.description(), None);
    }

    #[test]
    fn test_new_name() {
        let d_type = DataType::Int(IntType::new());
        let new_data_field =
            DataField::new(1, "field1".to_string(), d_type.clone()).with_name("field2".to_string());

        assert_eq!(new_data_field.id(), 1);
        assert_eq!(new_data_field.name(), "field2");
        assert_eq!(new_data_field.data_type(), &d_type);
        assert_eq!(new_data_field.description(), None);
    }

    #[test]
    fn test_new_description() {
        let d_type = DataType::Int(IntType::new());
        let new_data_field = DataField::new(1, "field1".to_string(), d_type.clone())
            .with_description(Some("new description".to_string()));

        assert_eq!(new_data_field.id(), 1);
        assert_eq!(new_data_field.name(), "field1");
        assert_eq!(new_data_field.data_type(), &d_type);
        assert_eq!(new_data_field.description(), Some("new description"));
    }

    #[test]
    fn test_escape_identifier() {
        let escaped_identifier = escape_identifier("\"identifier\"");
        assert_eq!(escaped_identifier, "\"\"identifier\"\"");
    }

    #[test]
    fn test_escape_single_quotes() {
        let escaped_text = escape_single_quotes("text with 'single' quotes");
        assert_eq!(escaped_text, "text with ''single'' quotes");
    }

    #[test]
    fn test_schema_builder_build() {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::with_nullable(true)))
            .column("name", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("k", "v")
            .comment(Some("table comment".into()))
            .build()
            .unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.primary_keys(), &["id"]);
        assert_eq!(schema.options().get("k"), Some(&"v".to_string()));
        assert_eq!(schema.comment(), Some("table comment"));
        let id_field = schema.fields().iter().find(|f| f.name() == "id").unwrap();
        assert!(
            !id_field.data_type().is_nullable(),
            "primary key column should be normalized to NOT NULL"
        );
    }

    #[test]
    fn test_copy_with_replaced_options() {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("old-key", "old-value")
            .comment(Some("c".into()))
            .build()
            .unwrap();
        let table_schema = TableSchema::new(3, &schema);

        let mut new_options = HashMap::new();
        new_options.insert("new-key".to_string(), "new-value".to_string());
        let copied = table_schema.copy_with_replaced_options(new_options);

        // Options are replaced entirely, not merged.
        assert_eq!(copied.options().get("old-key"), None);
        assert_eq!(
            copied.options().get("new-key"),
            Some(&"new-value".to_string())
        );
        // Everything else is preserved.
        assert_eq!(copied.id(), table_schema.id());
        assert_eq!(copied.fields(), table_schema.fields());
        assert_eq!(copied.primary_keys(), table_schema.primary_keys());
        assert_eq!(copied.comment(), table_schema.comment());
        assert_eq!(copied.time_millis(), table_schema.time_millis());
    }

    #[test]
    fn test_schema_validation() {
        // Duplicate field names
        let res = Schema::builder()
            .column("a", DataType::Int(IntType::new()))
            .column("b", DataType::Int(IntType::new()))
            .column("a", DataType::Int(IntType::new()))
            .build();
        assert!(res.is_err(), "duplicate field names should be rejected");

        // Duplicate partition keys
        let res = Schema::builder()
            .column("a", DataType::Int(IntType::new()))
            .column("b", DataType::Int(IntType::new()))
            .partition_keys(["a", "a"])
            .build();
        assert!(res.is_err(), "duplicate partition keys should be rejected");

        // Partition key not in fields
        let res = Schema::builder()
            .column("a", DataType::Int(IntType::new()))
            .column("b", DataType::Int(IntType::new()))
            .partition_keys(["c"])
            .build();
        assert!(
            res.is_err(),
            "partition key not in columns should be rejected"
        );

        // Duplicate primary keys
        let res = Schema::builder()
            .column("a", DataType::Int(IntType::with_nullable(false)))
            .column("b", DataType::Int(IntType::new()))
            .primary_key(["a", "a"])
            .build();
        assert!(res.is_err(), "duplicate primary keys should be rejected");

        // Primary key not in fields
        let res = Schema::builder()
            .column("a", DataType::Int(IntType::with_nullable(false)))
            .column("b", DataType::Int(IntType::new()))
            .primary_key(["c"])
            .build();
        assert!(
            res.is_err(),
            "primary key not in columns should be rejected"
        );

        // Primary key cannot be fully covered by partition keys.
        let res = Schema::builder()
            .column("a", DataType::Int(IntType::with_nullable(false)))
            .column("b", DataType::Int(IntType::new()))
            .partition_keys(["a", "b"])
            .primary_key(["a"])
            .build();
        assert!(
            matches!(res, Err(crate::Error::ConfigInvalid { message }) if message.contains("only one record in a partition")),
            "primary key fully covered by partition keys should be rejected"
        );

        // primary-key in options and DDL at same time
        let res = Schema::builder()
            .column("a", DataType::Int(IntType::with_nullable(false)))
            .column("b", DataType::Int(IntType::new()))
            .primary_key(["a"])
            .option(PRIMARY_KEY_OPTION, "a")
            .build();
        assert!(
            res.is_err(),
            "primary key defined in both DDL and options should be rejected"
        );

        // partition in options and DDL at same time
        let res = Schema::builder()
            .column("a", DataType::Int(IntType::new()))
            .column("b", DataType::Int(IntType::new()))
            .partition_keys(["a"])
            .option(PARTITION_OPTION, "a")
            .build();
        assert!(
            res.is_err(),
            "partition defined in both DDL and options should be rejected"
        );

        // Valid: partition keys and primary key subset of fields
        let schema = Schema::builder()
            .column("a", DataType::Int(IntType::with_nullable(false)))
            .column("b", DataType::Int(IntType::new()))
            .column("c", DataType::Int(IntType::new()))
            .partition_keys(["a"])
            .primary_key(["a", "b"])
            .build()
            .unwrap();
        assert_eq!(schema.partition_keys(), &["a"]);
        assert_eq!(schema.primary_keys(), &["a", "b"]);
    }

    #[test]
    fn test_blob_schema_validation_requires_data_evolution() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Blob(BlobType::new()))
            .build()
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { message } if message.contains("Data evolution config must enabled")),
            "blob columns should require data-evolution.enabled"
        );
    }

    #[test]
    fn test_blob_schema_validation_rejects_all_blob_columns() {
        let err = Schema::builder()
            .column("payload", DataType::Blob(BlobType::new()))
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { message } if message.contains("must have other normal columns")),
            "blob-only tables should be rejected"
        );
    }

    #[test]
    fn test_blob_schema_validation_rejects_blob_partition_keys() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Blob(BlobType::new()))
            .partition_keys(["payload"])
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { message } if message.contains("can not be part of partition keys")),
            "blob columns should be rejected as partition keys during schema validation"
        );
    }

    #[test]
    fn test_blob_schema_validation_accepts_valid_blob_table() {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Blob(BlobType::new()))
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap();

        assert_eq!(schema.fields().len(), 2);
    }

    #[test]
    fn test_partial_update_schema_validation_rejects_unsupported_options() {
        for (key, value) in [
            ("ignore-delete", "true"),
            ("fields.value.sequence-group", "g1"),
            ("fields.default-aggregate-function", "last_non_null"),
        ] {
            let err = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "partial-update")
                .option(key, value)
                .build()
                .unwrap_err();

            assert!(
                matches!(err, crate::Error::ConfigInvalid { ref message } if message.contains(key)),
                "partial-update create-time validation should reject '{key}', got {err:?}"
            );
        }
    }

    #[test]
    fn test_aggregation_schema_validation_accepts_basic_options() {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("tags", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("merge-engine", "aggregation")
            .option("fields.value.aggregate-function", "sum")
            .option("fields.tags.aggregate-function", "listagg")
            .option("fields.tags.list-agg-delimiter", ";")
            .option("fields.default-aggregate-function", "last_non_null_value")
            .build()
            .unwrap();

        assert_eq!(schema.fields().len(), 3);
    }

    #[test]
    fn test_aggregation_schema_validation_rejects_unsupported_options() {
        for (key, value) in [
            ("ignore-delete", "true"),
            ("aggregation.remove-record-on-delete", "true"),
            ("fields.value.ignore-retract", "true"),
            ("fields.value.distinct", "true"),
            ("fields.value.sequence-group", "g1"),
            ("fields.value.nested-key", "id"),
            ("fields.value.count-limit", "10"),
        ] {
            let err = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "aggregation")
                .option(key, value)
                .build()
                .unwrap_err();

            assert!(
                matches!(err, crate::Error::ConfigInvalid { ref message } if message.contains(key)),
                "aggregation create-time validation should reject '{key}', got {err:?}"
            );
        }
    }

    #[test]
    fn test_aggregation_schema_validation_rejects_unknown_field() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("amount", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("merge-engine", "aggregation")
            // typo: `amout` instead of `amount`
            .option("fields.amout.aggregate-function", "sum")
            .build()
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("amout") && message.contains("amount")),
            "expected unknown-field rejection at CREATE TABLE, got {err:?}"
        );
    }

    #[test]
    fn test_aggregation_schema_validation_rejects_unknown_function() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("amount", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("merge-engine", "aggregation")
            .option("fields.amount.aggregate-function", "sume")
            .build()
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("sume")),
            "expected unknown-function rejection at CREATE TABLE, got {err:?}"
        );
    }

    #[test]
    fn test_aggregation_schema_validation_rejects_incompatible_function_type() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("tag", DataType::VarChar(VarCharType::new(255).unwrap()))
            .primary_key(["id"])
            .option("merge-engine", "aggregation")
            // sum on a VarChar column
            .option("fields.tag.aggregate-function", "sum")
            .build()
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("sum") && message.contains("tag")),
            "expected incompatible-type rejection at CREATE TABLE, got {err:?}"
        );
    }

    #[test]
    fn test_first_row_schema_validation_accepts_supported_changelog_producers() {
        for producer in ["none", "lookup"] {
            Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "first-row")
                .option("changelog-producer", producer)
                .build()
                .unwrap();
        }
    }

    #[test]
    fn test_first_row_schema_validation_rejects_incompatible_changelog_producers() {
        for producer in ["input", "full-compaction"] {
            let err = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "first-row")
                .option("changelog-producer", producer)
                .build()
                .unwrap_err();

            assert!(
                matches!(err, crate::Error::ConfigInvalid { ref message }
                    if message.contains("merge-engine=first-row")
                        && message.contains("changelog-producer")
                        && message.contains(producer)),
                "first-row should reject changelog-producer={producer}, got {err:?}"
            );
        }
    }

    #[test]
    fn test_first_row_apply_changes_rejects_incompatible_changelog_producers() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "first-row")
                .option("changelog-producer", "lookup")
                .build()
                .unwrap(),
        );

        for producer in ["input", "full-compaction"] {
            let err = table_schema
                .apply_changes(vec![crate::spec::SchemaChange::set_option(
                    "changelog-producer".to_string(),
                    producer.to_string(),
                )])
                .unwrap_err();

            assert!(
                matches!(err, crate::Error::ConfigInvalid { ref message }
                    if message.contains("merge-engine=first-row")
                        && message.contains("changelog-producer")
                        && message.contains(producer)),
                "first-row alter should reject changelog-producer={producer}, got {err:?}"
            );
        }
    }

    #[test]
    fn test_first_row_apply_changes_validates_final_options() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("changelog-producer", "input")
                .build()
                .unwrap(),
        );

        let err = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::set_option(
                "merge-engine".to_string(),
                "first-row".to_string(),
            )])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("merge-engine=first-row")
                    && message.contains("changelog-producer")
                    && message.contains("input")),
            "first-row alter should reject incompatible final options, got {err:?}"
        );

        let new_schema = table_schema
            .apply_changes(vec![
                crate::spec::SchemaChange::set_option(
                    "merge-engine".to_string(),
                    "first-row".to_string(),
                ),
                crate::spec::SchemaChange::set_option(
                    "changelog-producer".to_string(),
                    "lookup".to_string(),
                ),
            ])
            .unwrap();

        assert_eq!(
            new_schema.options().get("merge-engine").map(String::as_str),
            Some("first-row")
        );
        assert_eq!(
            new_schema
                .options()
                .get("changelog-producer")
                .map(String::as_str),
            Some("lookup")
        );
    }

    fn cast_test_schema(options: &[(&str, &str)]) -> TableSchema {
        let mut builder = Schema::builder()
            .column("a", DataType::Int(IntType::new()))
            .column("b", DataType::BigInt(crate::spec::BigIntType::new()))
            .column(
                "d",
                DataType::Timestamp(crate::spec::TimestampType::new(3).unwrap()),
            );
        for (key, value) in options {
            builder = builder.option(*key, *value);
        }
        TableSchema::new(0, &builder.build().unwrap())
    }

    #[test]
    fn test_apply_changes_update_column_type_cast_compatibility() {
        let table_schema = cast_test_schema(&[]);

        // Implicit widening.
        let new_schema = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::update_column_type(
                "a".to_string(),
                DataType::BigInt(crate::spec::BigIntType::new()),
            )])
            .unwrap();
        assert!(matches!(
            new_schema.fields()[0].data_type(),
            DataType::BigInt(_)
        ));

        // Narrowing is an explicit cast, allowed by default.
        let new_schema = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::update_column_type(
                "b".to_string(),
                DataType::Int(IntType::new()),
            )])
            .unwrap();
        assert!(matches!(
            new_schema.fields()[1].data_type(),
            DataType::Int(_)
        ));

        // Unsupported conversions are rejected before committing the schema.
        for new_type in [
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            DataType::Boolean(crate::spec::BooleanType::new()),
        ] {
            let err = table_schema
                .apply_changes(vec![crate::spec::SchemaChange::update_column_type(
                    "d".to_string(),
                    new_type,
                )])
                .unwrap_err();
            assert!(
                matches!(err, crate::Error::Unsupported { ref message }
                    if message.contains("cannot be converted") && message.contains('d')),
                "expected cast rejection, got {err:?}"
            );
        }
    }

    #[test]
    fn test_apply_changes_update_column_type_respects_disable_explicit_casting() {
        let table_schema = cast_test_schema(&[("disable-explicit-type-casting", "true")]);

        let err = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::update_column_type(
                "b".to_string(),
                DataType::Int(IntType::new()),
            )])
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message }
                if message.contains("cannot be converted")),
            "narrowing should be rejected when explicit casting is disabled, got {err:?}"
        );

        // Implicit widening is still allowed.
        table_schema
            .apply_changes(vec![crate::spec::SchemaChange::update_column_type(
                "a".to_string(),
                DataType::BigInt(crate::spec::BigIntType::new()),
            )])
            .unwrap();
    }

    #[test]
    fn test_apply_changes_update_column_type_rejects_blob() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("payload", DataType::Blob(BlobType::new()))
                .option("data-evolution.enabled", "true")
                .build()
                .unwrap(),
        );

        for (column, new_type) in [
            (
                "payload",
                DataType::VarChar(crate::spec::VarCharType::new(10).unwrap()),
            ),
            ("id", DataType::Blob(BlobType::new())),
        ] {
            let err = table_schema
                .apply_changes(vec![crate::spec::SchemaChange::update_column_type(
                    column.to_string(),
                    new_type,
                )])
                .unwrap_err();
            assert!(
                matches!(err, crate::Error::Unsupported { ref message }
                    if message.contains("involving BLOB") && message.contains(column)),
                "expected BLOB type-change rejection for {column}, got {err:?}"
            );
        }
    }

    #[test]
    fn test_apply_changes_nullable_to_not_null_guard() {
        let table_schema = cast_test_schema(&[]);
        let not_null_int = DataType::Int(IntType::new())
            .copy_with_nullable(false)
            .unwrap();

        // Both nullability change paths are rejected by default.
        let changes: Vec<crate::spec::SchemaChange> = vec![
            crate::spec::SchemaChange::update_column_nullability("a".to_string(), false),
            crate::spec::SchemaChange::update_column_type("a".to_string(), not_null_int.clone()),
        ];
        for change in changes {
            let err = table_schema.apply_changes(vec![change]).unwrap_err();
            assert!(
                matches!(err, crate::Error::Unsupported { ref message }
                    if message.contains("nullable to non nullable")),
                "expected null-to-not-null rejection, got {err:?}"
            );
        }

        // Allowed when explicitly enabled via table option.
        let table_schema = cast_test_schema(&[("alter-column-null-to-not-null.disabled", "false")]);
        let new_schema = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::update_column_nullability(
                "a".to_string(),
                false,
            )])
            .unwrap();
        assert!(!new_schema.fields()[0].data_type().is_nullable());
        let new_schema = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::update_column_type(
                "a".to_string(),
                not_null_int,
            )])
            .unwrap();
        assert!(!new_schema.fields()[0].data_type().is_nullable());
    }

    #[test]
    fn test_apply_changes_revalidates_blob_fields() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );

        let err = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::add_column(
                "payload".to_string(),
                DataType::Blob(BlobType::new()),
            )])
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("Data evolution config must enabled")),
            "adding a BLOB column without data-evolution.enabled should fail, got {err:?}"
        );

        // Enabling data evolution in the same alter makes the final schema valid.
        let new_schema = table_schema
            .apply_changes(vec![
                crate::spec::SchemaChange::set_option(
                    "data-evolution.enabled".to_string(),
                    "true".to_string(),
                ),
                crate::spec::SchemaChange::add_column(
                    "payload".to_string(),
                    DataType::Blob(BlobType::new()),
                ),
            ])
            .unwrap();
        assert_eq!(new_schema.fields().len(), 2);
    }

    fn vector_4f() -> DataType {
        DataType::Vector(VectorType::try_new(true, 4, DataType::Float(FloatType::new())).unwrap())
    }

    #[test]
    fn test_vector_rejected_as_primary_key() {
        let err = Schema::builder()
            .column("id", vector_4f())
            .column("name", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .build()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("primary key") && message.contains("VECTOR")),
            "VECTOR primary key should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_vector_rejected_as_explicit_bucket_key() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("embedding", vector_4f())
            .option(BUCKET_KEY_OPTION, "embedding")
            .build()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("bucket key") && message.contains("VECTOR")),
            "VECTOR explicit bucket key should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_vector_allowed_as_non_key_column() {
        // A VECTOR column that is not a key (no pk, no explicit bucket-key) must
        // build fine — the implicit "all non-partition fields" bucket-key fallback
        // must NOT reject it.
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("embedding", vector_4f())
            .build()
            .unwrap();
        assert_eq!(schema.fields().len(), 2);
    }

    #[test]
    fn test_vector_store_requires_data_evolution_and_row_tracking() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("embedding", vector_4f())
            .option("vector.file.format", "vortex")
            .build()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("Data evolution config must enabled")),
            "dedicated VECTOR storage should require data-evolution.enabled, got {err:?}"
        );

        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("embedding", vector_4f())
            .option("vector.file.format", "vortex")
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("Row tracking config must enabled")),
            "dedicated VECTOR storage should require row-tracking.enabled, got {err:?}"
        );
    }

    #[test]
    fn test_vector_store_rejects_vector_only_table() {
        let err = Schema::builder()
            .column("embedding", vector_4f())
            .option("vector.file.format", "vortex")
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("must have other normal columns")),
            "dedicated VECTOR storage should require a normal anchor file, got {err:?}"
        );
    }

    #[test]
    fn test_apply_changes_revalidates_vector_bucket_key() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("embedding", vector_4f())
                .build()
                .unwrap(),
        );

        let err = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::set_option(
                BUCKET_KEY_OPTION.to_string(),
                "embedding".to_string(),
            )])
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("bucket key") && message.contains("VECTOR")),
            "altering to a VECTOR bucket key should fail, got {err:?}"
        );
    }

    #[test]
    fn test_apply_changes_revalidates_partial_update_options() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "partial-update")
                .build()
                .unwrap(),
        );

        let err = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::set_option(
                "fields.value.sequence-group".to_string(),
                "value".to_string(),
            )])
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("partial-update") && message.contains("sequence-group")),
            "unsupported partial-update option should be rejected on alter, got {err:?}"
        );
    }

    #[test]
    fn test_aggregation_apply_changes_rejects_unknown_field() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "aggregation")
                .option("fields.value.aggregate-function", "sum")
                .build()
                .unwrap(),
        );

        let err = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::set_option(
                "fields.valuee.aggregate-function".to_string(),
                "sum".to_string(),
            )])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("is not declared")
                    && message.contains("valuee")),
            "aggregation alter should reject typo'd column, got {err:?}"
        );
    }

    #[test]
    fn test_partial_update_apply_changes_rejects_unsupported_option() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "partial-update")
                .build()
                .unwrap(),
        );

        let err = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::set_option(
                "ignore-delete".to_string(),
                "true".to_string(),
            )])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("merge-engine=partial-update")
                    && message.contains("ignore-delete")),
            "partial-update alter should reject unsupported option, got {err:?}"
        );
    }

    #[test]
    fn test_aggregation_apply_changes_accepts_valid_option() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "aggregation")
                .option("fields.value.aggregate-function", "sum")
                .build()
                .unwrap(),
        );

        let new_schema = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::set_option(
                "fields.value.aggregate-function".to_string(),
                "max".to_string(),
            )])
            .unwrap();

        assert_eq!(
            new_schema
                .options()
                .get("fields.value.aggregate-function")
                .map(String::as_str),
            Some("max")
        );
    }

    #[test]
    fn test_aggregation_apply_changes_rejects_sequence_field_function() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("seq", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "aggregation")
                .option("sequence.field", "seq")
                .option("fields.value.aggregate-function", "sum")
                .build()
                .unwrap(),
        );

        let err = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::set_option(
                "fields.seq.aggregate-function".to_string(),
                "sum".to_string(),
            )])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("sequence field") && message.contains("seq")),
            "aggregation alter should reject sequence-field aggregate function, got {err:?}"
        );
    }

    #[test]
    fn test_rename_column_rewrites_field_scoped_agg_options() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("tag", DataType::VarChar(VarCharType::string_type()))
                .primary_key(["id"])
                .option("merge-engine", "aggregation")
                .option("fields.tag.aggregate-function", "listagg")
                .option("fields.tag.list-agg-delimiter", ";")
                .build()
                .unwrap(),
        );

        let new_schema = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::rename_column(
                "tag".to_string(),
                "label".to_string(),
            )])
            .unwrap();

        // Field-scoped option keys follow the column to its new name.
        assert_eq!(
            new_schema
                .options()
                .get("fields.label.aggregate-function")
                .map(String::as_str),
            Some("listagg")
        );
        assert_eq!(
            new_schema
                .options()
                .get("fields.label.list-agg-delimiter")
                .map(String::as_str),
            Some(";")
        );
        // The old keys are gone.
        assert_eq!(
            new_schema.options().get("fields.tag.aggregate-function"),
            None
        );
        assert_eq!(
            new_schema.options().get("fields.tag.list-agg-delimiter"),
            None
        );
    }

    #[test]
    fn test_drop_column_referenced_by_bucket_key_rejected() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("name", DataType::VarChar(VarCharType::string_type()))
                .option("bucket", "4")
                .option("bucket-key", "name")
                .build()
                .unwrap(),
        );

        let err = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::drop_column(
                "name".to_string(),
            )])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::Unsupported { ref message }
                if message.contains("bucket-key") && message.contains("name")),
            "drop of a bucket-key column should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_drop_column_referenced_by_sequence_field_rejected() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("ts", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("sequence.field", "ts")
                .build()
                .unwrap(),
        );

        let err = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::drop_column(
                "ts".to_string(),
            )])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::Unsupported { ref message }
                if message.contains("sequence.field") && message.contains("ts")),
            "drop of a sequence.field column should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_drop_column_removes_field_scoped_agg_options() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .column("tag", DataType::VarChar(VarCharType::string_type()))
                .primary_key(["id"])
                .option("merge-engine", "aggregation")
                .option("fields.value.aggregate-function", "sum")
                .option("fields.tag.aggregate-function", "listagg")
                .option("fields.tag.list-agg-delimiter", ";")
                .build()
                .unwrap(),
        );

        let new_schema = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::drop_column(
                "tag".to_string(),
            )])
            .unwrap();

        // The dropped column's field-scoped options are removed...
        assert_eq!(
            new_schema.options().get("fields.tag.aggregate-function"),
            None
        );
        assert_eq!(
            new_schema.options().get("fields.tag.list-agg-delimiter"),
            None
        );
        // ...while the surviving column's option is untouched.
        assert_eq!(
            new_schema
                .options()
                .get("fields.value.aggregate-function")
                .map(String::as_str),
            Some("sum")
        );
        assert!(new_schema.fields().iter().all(|f| f.name() != "tag"));
    }

    #[test]
    fn test_schema_builder_column_row_type() {
        let row_type = RowType::new(vec![DataField::new(
            0,
            "nested".into(),
            DataType::Int(IntType::new()),
        )]);
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Row(row_type))
            .build()
            .unwrap();

        assert_eq!(schema.fields().len(), 2);
        // id gets field_id=0, payload gets field_id=1, nested gets field_id=2
        assert_eq!(schema.fields()[0].id(), 0);
        assert_eq!(schema.fields()[1].id(), 1);
        if let DataType::Row(row) = schema.fields()[1].data_type() {
            assert_eq!(row.fields().len(), 1);
            assert_eq!(row.fields()[0].id(), 2);
            assert_eq!(row.fields()[0].name(), "nested");
        } else {
            panic!("expected Row type");
        }
    }
}
