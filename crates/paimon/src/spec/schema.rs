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
    first_row_supports_changelog_producer, ChangelogProducer, CoreOptions, MergeEngine,
    BLOB_DESCRIPTOR_FIELD_OPTION, BLOB_FIELD_OPTION, BLOB_VIEW_FIELD_OPTION, BUCKET_KEY_OPTION,
    POSTPONE_BUCKET, QUERY_AUTH_ENABLED_OPTION, SEQUENCE_FIELD_OPTION,
};
use crate::spec::types::{ArrayType, DataType, MapType, MultisetType, RowType, VarCharType};
use crate::spec::{
    remove_field_scoped_options, rename_field_scoped_options, AggregationConfig, BlobType,
    ColumnMove, ColumnMoveType, PartialUpdateConfig,
};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::{HashMap, HashSet};

const BLOB_FIELD_DIRECTIVE: &str = "__BLOB_FIELD";
const BLOB_DESCRIPTOR_FIELD_DIRECTIVE: &str = "__BLOB_DESCRIPTOR_FIELD";
const BLOB_VIEW_FIELD_DIRECTIVE: &str = "__BLOB_VIEW_FIELD";

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

    /// Typed view over this schema's table options.
    pub fn core_options(&self) -> CoreOptions<'_> {
        CoreOptions::new(&self.options)
    }

    /// Create a copy of this schema with extra options merged in.
    ///
    /// A stored `query-auth.enabled = true` can't be turned off by a dynamic override.
    pub fn copy_with_options(&self, mut extra: HashMap<String, String>) -> Self {
        if self.core_options().query_auth_enabled() {
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

    /// Validate the structural invariants of an externally supplied, already
    /// resolved schema, without normalizing or mutating it.
    ///
    /// This is the safety check for [`crate::table::Table::from_resolved_schema`],
    /// whose input is untrusted JSON. It rejects the malformed shapes that would
    /// otherwise panic or silently read the wrong column downstream:
    /// - duplicate top-level field names (a projected name could resolve to a
    ///   different field than a predicate on the same name),
    /// - duplicate field ids (including nested), which break schema-evolution
    ///   column mapping,
    /// - primary-key or partition columns that do not exist (e.g. the
    ///   `PartitionComputer` unwraps on a missing partition column),
    /// - primary keys that are exactly the partition columns (the read path
    ///   selects the key-value merge path from the raw primary keys but feeds
    ///   the reader the *trimmed* keys, which are then empty, panicking on a
    ///   zero-column key),
    /// - reserved system field names / ids (e.g. a user column named `_ROW_ID`
    ///   would be silently replaced by the system row number on read).
    ///
    /// It intentionally does NOT run create-time policy checks (merge-engine,
    /// changelog, aggregation, rowkind, blob strategy, bucket-key existence, …):
    /// those normalize the schema or reject shapes that are valid to *read*,
    /// which is not this entry point's contract.
    pub(crate) fn validate_resolved_structure(&self) -> crate::Result<()> {
        let field_names: Vec<String> = self.fields.iter().map(|f| f.name().to_string()).collect();
        Schema::validate_no_duplicate_fields(&field_names)?;
        Schema::validate_primary_keys(&field_names, &self.primary_keys)?;
        Schema::validate_partition_keys(&field_names, &self.partition_keys)?;
        Schema::validate_primary_keys_not_partition_only(&self.partition_keys, &self.primary_keys)?;
        self.validate_no_duplicate_field_ids()?;
        self.validate_no_reserved_fields()?;
        Ok(())
    }

    /// Reject reserved system field names, the `_KEY_` key-field prefix, and
    /// reserved system field ids, mirroring Java `SpecialFields`. A user column
    /// colliding with a system field (e.g. `_ROW_ID`) is otherwise excluded
    /// from the physical read and silently filled with the system value.
    fn validate_no_reserved_fields(&self) -> crate::Result<()> {
        // Java SpecialFields.SYSTEM_FIELD_ID_START = Integer.MAX_VALUE / 2.
        const SYSTEM_FIELD_ID_START: i32 = i32::MAX / 2;

        validate_no_reserved_field_names(&self.fields)?;

        for field in &self.fields {
            if field.id() >= SYSTEM_FIELD_ID_START {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Field '{}' uses reserved system field id {}",
                        field.name(),
                        field.id()
                    ),
                    source: None,
                });
            }
        }
        Ok(())
    }

    /// Reject duplicate field ids, including ids nested inside complex types.
    /// Field ids key schema-evolution column mapping, so a collision would map
    /// two logical columns onto one id.
    fn validate_no_duplicate_field_ids(&self) -> crate::Result<()> {
        let mut seen = HashSet::new();
        let mut ids = Vec::new();
        collect_field_ids(&self.fields, &mut ids);
        for id in ids {
            if !seen.insert(id) {
                return Err(crate::Error::DataInvalid {
                    message: format!("Table schema must not contain duplicate field id: {id}"),
                    source: None,
                });
            }
        }
        Ok(())
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
        // Capture stable IDs before applying changes so removing the option or
        // renaming its column cannot bypass historical bucket-key protection.
        let old_bucket_key_field_ids: HashSet<i32> = self
            .core_options()
            .bucket_key()
            .into_iter()
            .flatten()
            .filter_map(|name| {
                self.fields
                    .iter()
                    .find(|field| field.name() == name)
                    .map(DataField::id)
            })
            .collect();

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
                    mut data_type,
                    mut comment,
                    column_move,
                } => {
                    let name = top_level_field(&field_names)?;
                    if field_index(&fields, name).is_some() {
                        return Err(crate::Error::ColumnAlreadyExist {
                            full_name: full_name.to_string(),
                            column: name.to_string(),
                        });
                    }
                    if let Some(directive) = parse_blob_comment_directive(comment.as_deref())? {
                        append_csv_option(&mut new_schema.options, directive.option_key, name);
                        comment = directive.comment;
                        data_type = normalize_blob_field_type(name, data_type)?;
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
                    assert_not_updating_primary_key_index_column(&self.options, name, "rename")?;
                    let idx =
                        field_index(&fields, name).ok_or_else(|| crate::Error::ColumnNotExist {
                            full_name: full_name.to_string(),
                            column: name.to_string(),
                        })?;
                    if fields[idx].data_type().is_blob_file_field() {
                        return Err(crate::Error::Unsupported {
                            message: format!("Cannot rename BLOB column: [{name}]"),
                        });
                    }
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
                    // Field-scoped options encode column names in the key
                    // (`fields.<col>.aggregate-function`, `fields.<cols>.sequence-group`,
                    // ...) and, for field-list options, in the value too, so they
                    // must be rewritten as well, mirroring Java
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
                    assert_not_updating_primary_key_index_column(&self.options, name, "drop")?;
                    assert_not_updating_bucket_key_column(
                        &old_bucket_key_field_ids,
                        &fields[idx],
                        "drop",
                    )?;
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
                    assert_not_updating_primary_key_index_column(
                        &self.options,
                        name,
                        "update type of",
                    )?;
                    let idx =
                        field_index(&fields, name).ok_or_else(|| crate::Error::ColumnNotExist {
                            full_name: full_name.to_string(),
                            column: name.to_string(),
                        })?;
                    let old = &fields[idx];
                    assert_not_updating_bucket_key_column(
                        &old_bucket_key_field_ids,
                        old,
                        "update type of",
                    )?;
                    // Mirrors Java `assertNotChangingBlobColumnType`: BLOB
                    // columns use a dedicated storage layout that other types
                    // cannot be converted to or from.
                    if old.data_type().is_blob_file_field() || new_data_type.is_blob_file_field() {
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

        if PartialUpdateConfig::new(&new_schema.options).is_enabled()
            && self.core_options().ignore_delete()
            && !CoreOptions::new(&new_schema.options).ignore_delete()
        {
            return Err(crate::Error::Unsupported {
                message: "Cannot change ignore-delete from true to false.".to_string(),
            });
        }

        Schema::validate_final_schema(
            &new_schema.fields,
            &new_schema.partition_keys,
            &new_schema.primary_keys,
            &new_schema.options,
        )?;
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

/// Reject column names reserved for system use, mirroring Java `SpecialFields`:
/// the five `SYSTEM_FIELD_NAMES` and the `_KEY_` key-field prefix.
///
/// A user column colliding with a system field is otherwise excluded from the
/// physical read and silently filled with the system value.
fn validate_no_reserved_field_names(fields: &[DataField]) -> crate::Result<()> {
    // Java SpecialFields.SYSTEM_FIELD_NAMES.
    const SYSTEM_FIELD_NAMES: [&str; 5] = [
        SEQUENCE_NUMBER_FIELD_NAME,
        VALUE_KIND_FIELD_NAME,
        "_LEVEL",
        ROW_KIND_FIELD_NAME,
        ROW_ID_FIELD_NAME,
    ];
    const KEY_FIELD_PREFIX: &str = "_KEY_";

    for field in fields {
        let name = field.name();
        if name.starts_with(KEY_FIELD_PREFIX) || SYSTEM_FIELD_NAMES.contains(&name) {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "Field name '{name}' is reserved for system use and cannot be used in a table schema"
                ),
            });
        }
    }
    Ok(())
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

/// Reject destructive changes to columns referenced by a primary-key index.
///
/// The index metadata and existing index files are tied to the original column
/// name and type. Mirrors Java
/// `SchemaManager.assertNotUpdatingPrimaryKeyIndexColumn`.
fn assert_not_updating_primary_key_index_column(
    options: &HashMap<String, String>,
    field_name: &str,
    operation: &str,
) -> crate::Result<()> {
    let core_options = CoreOptions::new(options);
    let is_vector_index_column = core_options.primary_key_vector_index_enabled()
        && core_options
            .primary_key_vector_index_columns()?
            .iter()
            .any(|column| column == field_name);
    let is_full_text_index_column = core_options
        .primary_key_full_text_index_columns()
        .iter()
        .any(|column| column == field_name);

    if is_vector_index_column || is_full_text_index_column {
        return Err(crate::Error::Unsupported {
            message: format!("Cannot {operation} primary-key index column: [{field_name}]"),
        });
    }
    Ok(())
}

fn assert_not_updating_bucket_key_column(
    old_bucket_key_field_ids: &HashSet<i32>,
    field: &DataField,
    operation: &str,
) -> crate::Result<()> {
    if old_bucket_key_field_ids.contains(&field.id()) {
        return Err(crate::Error::Unsupported {
            message: format!("Cannot {operation} bucket-key column: [{}]", field.name()),
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

/// Collect the field ids of `fields` and of any fields nested inside their
/// complex types, in traversal order. Used to detect duplicate ids.
fn collect_field_ids(fields: &[DataField], out: &mut Vec<i32>) {
    for field in fields {
        out.push(field.id());
        collect_nested_field_ids(field.data_type(), out);
    }
}

fn collect_nested_field_ids(data_type: &DataType, out: &mut Vec<i32>) {
    match data_type {
        DataType::Array(t) => collect_nested_field_ids(t.element_type(), out),
        DataType::Multiset(t) => collect_nested_field_ids(t.element_type(), out),
        DataType::Map(t) => {
            collect_nested_field_ids(t.key_type(), out);
            collect_nested_field_ids(t.value_type(), out);
        }
        DataType::Row(t) => collect_field_ids(t.fields(), out),
        _ => {}
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

struct BlobCommentDirective {
    option_key: &'static str,
    comment: Option<String>,
}

fn parse_blob_comment_directive(
    comment: Option<&str>,
) -> crate::Result<Option<BlobCommentDirective>> {
    let Some(comment) = comment else {
        return Ok(None);
    };
    let comment = comment.trim();
    if !comment.starts_with("__BLOB") {
        return Ok(None);
    }
    let Some((option_key, marker)) = match_blob_comment_directive(comment) else {
        return Err(crate::Error::ConfigInvalid {
            message: format!(
                "Unsupported BLOB comment directive '{comment}'. Supported directives are \
                 '{BLOB_FIELD_DIRECTIVE}', '{BLOB_DESCRIPTOR_FIELD_DIRECTIVE}', and \
                 '{BLOB_VIEW_FIELD_DIRECTIVE}'."
            ),
        });
    };
    let real_comment = if comment.len() == marker.len() {
        None
    } else {
        let real_comment = comment[marker.len() + 1..].trim();
        (!real_comment.is_empty()).then(|| real_comment.to_string())
    };
    Ok(Some(BlobCommentDirective {
        option_key,
        comment: real_comment,
    }))
}

fn match_blob_comment_directive(comment: &str) -> Option<(&'static str, &'static str)> {
    [
        (BLOB_VIEW_FIELD_DIRECTIVE, BLOB_VIEW_FIELD_OPTION),
        (
            BLOB_DESCRIPTOR_FIELD_DIRECTIVE,
            BLOB_DESCRIPTOR_FIELD_OPTION,
        ),
        (BLOB_FIELD_DIRECTIVE, BLOB_FIELD_OPTION),
    ]
    .into_iter()
    .find_map(|(marker, option_key)| {
        if !comment.starts_with(marker) {
            return None;
        }
        if comment.len() == marker.len() || comment.as_bytes().get(marker.len()) == Some(&b';') {
            Some((option_key, marker))
        } else {
            None
        }
    })
}

fn append_csv_option(options: &mut HashMap<String, String>, key: &'static str, field_name: &str) {
    let value = append_csv_field(options.get(key).map(String::as_str), field_name);
    options.insert(key.to_string(), value);
}

fn append_csv_field(existing: Option<&str>, field_name: &str) -> String {
    let mut fields = existing
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|field| !field.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if !fields.iter().any(|field| field == field_name) {
        fields.push(field_name.to_string());
    }
    fields.join(",")
}

fn normalize_blob_field_type(field_name: &str, data_type: DataType) -> crate::Result<DataType> {
    let nullable = data_type.is_nullable();
    match data_type {
        DataType::Blob(_) => Ok(data_type),
        DataType::Binary(_) | DataType::VarBinary(_) => {
            Ok(DataType::Blob(BlobType::with_nullable(nullable)))
        }
        other => Err(crate::Error::ConfigInvalid {
            message: format!(
                "BLOB field option references non-binary column '{field_name}' with type {other:?}"
            ),
        }),
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

pub const ROW_KIND_FIELD_NAME: &str = "rowkind";

/// Must match Java Paimon's `SpecialFields.ROW_KIND` (Integer.MAX_VALUE - 4).
pub const ROW_KIND_FIELD_ID: i32 = i32::MAX - 4;

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
        mut fields: Vec<DataField>,
        partition_keys: Vec<String>,
        primary_keys: Vec<String>,
        mut options: HashMap<String, String>,
        comment: Option<String>,
    ) -> crate::Result<Self> {
        let primary_keys = Self::normalize_primary_keys(&primary_keys, &mut options)?;
        let partition_keys = Self::normalize_partition_keys(&partition_keys, &mut options)?;
        Self::normalize_blob_comment_directives(&mut fields, &mut options)?;
        let fields = Self::normalize_fields(&fields, &partition_keys, &primary_keys, &options)?;
        Self::validate_final_schema(&fields, &partition_keys, &primary_keys, &options)?;

        Ok(Self {
            fields,
            partition_keys,
            primary_keys,
            options,
            comment,
        })
    }

    fn validate_final_schema(
        fields: &[DataField],
        partition_keys: &[String],
        primary_keys: &[String],
        options: &HashMap<String, String>,
    ) -> crate::Result<()> {
        validate_no_reserved_field_names(fields)?;
        Self::validate_key_field_types(fields, primary_keys, options)?;
        Self::validate_blob_fields(fields, partition_keys, options)?;
        Self::validate_vector_store_fields(fields, partition_keys, options)?;
        PartialUpdateConfig::new(options).validate_create_mode(!primary_keys.is_empty())?;
        AggregationConfig::new(options).validate_create_mode(primary_keys, fields)?;
        Self::validate_first_row_changelog_producer(options)?;
        Self::validate_rowkind_field(options, primary_keys, fields)?;
        Self::validate_deletion_vectors(options)?;
        Self::validate_bucket_keys(options, fields, partition_keys, primary_keys)?;
        Self::validate_read_batch_size(options)?;
        Self::validate_primary_key_vector_index(fields, primary_keys, options)?;
        Self::validate_primary_key_full_text_index(fields, primary_keys, options)?;
        Ok(())
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

    /// Normalize fields: validate duplicate/subset checks, promote configured
    /// binary BLOB fields, and make primary key columns non-nullable.
    /// Corresponds to Java `normalizeFields`.
    fn normalize_fields(
        fields: &[DataField],
        partition_keys: &[String],
        primary_keys: &[String],
        options: &HashMap<String, String>,
    ) -> crate::Result<Vec<DataField>> {
        let field_names: Vec<String> = fields.iter().map(|f| f.name().to_string()).collect();
        Self::validate_no_duplicate_fields(&field_names)?;
        Self::validate_partition_keys(&field_names, partition_keys)?;
        Self::validate_primary_keys(&field_names, primary_keys)?;
        Self::validate_primary_keys_not_partition_only(partition_keys, primary_keys)?;

        let blob_field_names = CoreOptions::new(options).blob_fields();
        for name in &blob_field_names {
            if !field_names.iter().any(|field| field == name) {
                return Err(crate::Error::ConfigInvalid {
                    message: format!("BLOB field option references missing column '{name}'"),
                });
            }
        }

        if primary_keys.is_empty() && blob_field_names.is_empty() {
            return Ok(fields.to_vec());
        }

        let pk_set: HashSet<&str> = primary_keys.iter().map(String::as_str).collect();
        let mut new_fields = Vec::with_capacity(fields.len());
        for f in fields {
            let mut data_type = if blob_field_names.contains(f.name()) {
                normalize_blob_field_type(f.name(), f.data_type().clone())?
            } else {
                f.data_type().clone()
            };
            if pk_set.contains(f.name()) && data_type.is_nullable() {
                data_type = data_type.copy_with_nullable(false)?;
            }
            new_fields.push(
                DataField::new(f.id(), f.name().to_string(), data_type)
                    .with_description(f.description().map(|s| s.to_string())),
            );
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

    fn normalize_blob_comment_directives(
        fields: &mut [DataField],
        options: &mut HashMap<String, String>,
    ) -> crate::Result<()> {
        for field in fields {
            let Some(directive) = parse_blob_comment_directive(field.description())? else {
                continue;
            };
            append_csv_option(options, directive.option_key, field.name());
            *field = field.clone().with_description(directive.comment);
        }
        Ok(())
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
        let blob_descriptor_fields = core_options.blob_descriptor_fields();
        let blob_view_fields = core_options.blob_view_fields();
        let mut overlapping_fields = blob_view_fields
            .intersection(&blob_descriptor_fields)
            .cloned()
            .collect::<Vec<_>>();
        overlapping_fields.sort();
        if let Some(field) = overlapping_fields.first() {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "Field '{field}' in '{BLOB_VIEW_FIELD_OPTION}' can not also be in \
                     '{BLOB_DESCRIPTOR_FIELD_OPTION}'."
                ),
            });
        }

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

    fn validate_deletion_vectors(options: &HashMap<String, String>) -> crate::Result<()> {
        let core = CoreOptions::new(options);
        if !core.deletion_vectors_enabled() {
            return Ok(());
        }

        let changelog_producer = core
            .try_changelog_producer()
            .map_err(Self::options_error_to_config_invalid)?;
        if !matches!(
            changelog_producer,
            ChangelogProducer::None | ChangelogProducer::Input | ChangelogProducer::Lookup
        ) {
            return Err(crate::Error::ConfigInvalid {
                message: "Deletion vectors mode is only supported for NONE/INPUT/LOOKUP changelog producer now.".to_string(),
            });
        }

        let merge_engine = core
            .merge_engine()
            .map_err(Self::options_error_to_config_invalid)?;
        if merge_engine == MergeEngine::FirstRow {
            return Err(crate::Error::ConfigInvalid {
                message: "First row merge engine does not need deletion vectors because there is no deletion of old data in this merge engine.".to_string(),
            });
        }
        Ok(())
    }

    fn validate_rowkind_field(
        options: &HashMap<String, String>,
        primary_keys: &[String],
        fields: &[DataField],
    ) -> crate::Result<()> {
        let core = CoreOptions::new(options);
        let Some(field_name) = core.rowkind_field() else {
            return Ok(());
        };

        if primary_keys.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "rowkind.field requires a primary-key table".to_string(),
            });
        }

        let merge_engine = core
            .merge_engine()
            .map_err(Self::options_error_to_config_invalid)?;
        if merge_engine != MergeEngine::Deduplicate {
            return Err(crate::Error::ConfigInvalid {
                message: "rowkind.field only supports merge-engine=deduplicate".to_string(),
            });
        }

        let producer = core
            .try_changelog_producer()
            .map_err(Self::options_error_to_config_invalid)?;
        if producer == ChangelogProducer::Input {
            return Err(crate::Error::ConfigInvalid {
                message: "rowkind.field cannot be used with changelog-producer=input".to_string(),
            });
        }

        let Some(field) = fields.iter().find(|f| f.name() == field_name) else {
            return Err(crate::Error::ConfigInvalid {
                message: format!("Rowkind field '{field_name}' can not be found in table schema."),
            });
        };

        if !matches!(
            field.data_type(),
            DataType::VarChar(v) if v.length() == VarCharType::MAX_LENGTH
        ) {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "rowkind.field '{field_name}' must be STRING (unbounded VARCHAR) type"
                ),
            });
        }

        Ok(())
    }

    /// Validate the explicit `bucket-key` option against the schema, mirroring
    /// Java `TableSchema#originalBucketKeys`. A bucket key that is missing,
    /// partitioned, or outside the primary key otherwise resolves to no field
    /// index in `TableWrite`, which silently degrades to a constant bucket 0
    /// assigner instead of hashing.
    fn validate_bucket_keys(
        options: &HashMap<String, String>,
        fields: &[DataField],
        partition_keys: &[String],
        primary_keys: &[String],
    ) -> crate::Result<()> {
        let Some(bucket_keys) = CoreOptions::new(options).bucket_key() else {
            return Ok(());
        };

        let mut seen: HashSet<&str> = HashSet::new();
        for key in &bucket_keys {
            if fields.iter().all(|f| f.name() != key) {
                return Err(crate::Error::ConfigInvalid {
                    message: format!(
                        "Field names should contain all bucket keys, but bucket key '{key}' \
                         can not be found in table schema."
                    ),
                });
            }
            if !seen.insert(key.as_str()) {
                return Err(crate::Error::ConfigInvalid {
                    message: format!("Bucket key '{key}' is defined repeatedly."),
                });
            }
            if partition_keys.contains(key) {
                return Err(crate::Error::ConfigInvalid {
                    message: format!(
                        "Bucket keys should not be in partition keys, but bucket key '{key}' \
                         is a partition field."
                    ),
                });
            }
            if !primary_keys.is_empty() && !primary_keys.contains(key) {
                return Err(crate::Error::ConfigInvalid {
                    message: format!(
                        "Primary keys {primary_keys:?} should contain all bucket keys, but \
                         bucket key '{key}' is not a primary key field."
                    ),
                });
            }
        }

        Ok(())
    }

    fn options_error_to_config_invalid(error: crate::Error) -> crate::Error {
        match error {
            crate::Error::Unsupported { message } => crate::Error::ConfigInvalid { message },
            other => crate::Error::ConfigInvalid {
                message: other.to_string(),
            },
        }
    }

    fn validate_read_batch_size(options: &HashMap<String, String>) -> crate::Result<()> {
        CoreOptions::new(options)
            .read_batch_size()
            .map(|_| ())
            .map_err(Self::options_error_to_config_invalid)
    }

    fn validate_primary_key_vector_index(
        fields: &[DataField],
        primary_keys: &[String],
        options: &HashMap<String, String>,
    ) -> crate::Result<()> {
        let core = CoreOptions::new(options);
        if !core.primary_key_vector_index_enabled() {
            return Ok(());
        }

        let column = core.primary_key_vector_index_column()?;
        if column.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "pk-vector.index.columns must name a non-empty column".to_string(),
            });
        }
        core.primary_key_vector_index_type(&column)?;
        Self::validate_primary_key_index_prerequisites("vector", primary_keys, &core)?;

        let field = fields
            .iter()
            .find(|field| field.name() == column)
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: format!(
                    "pk-vector.index.columns entry '{column}' must reference an existing column."
                ),
            })?;
        let supported_type = match field.data_type() {
            DataType::Vector(vector) => matches!(vector.element_type(), DataType::Float(_)),
            DataType::Array(array) => matches!(array.element_type(), DataType::Float(_)),
            _ => false,
        };
        if !supported_type {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "pk-vector.index.columns entry '{column}' must reference an ARRAY<FLOAT> or VECTOR<FLOAT> column."
                ),
            });
        }

        core.primary_key_vector_distance_metric(&column)
            .map(|_| ())
            .map_err(Self::options_error_to_config_invalid)
    }

    fn validate_primary_key_full_text_index(
        fields: &[DataField],
        primary_keys: &[String],
        options: &HashMap<String, String>,
    ) -> crate::Result<()> {
        let core = CoreOptions::new(options);
        if !core.primary_key_full_text_index_enabled() {
            return Ok(());
        }

        let columns = core.primary_key_full_text_index_columns();
        if columns.len() != 1 {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "pk-full-text.index.columns must name exactly one column, got {}",
                    columns.len()
                ),
            });
        }
        let column = &columns[0];
        if column.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "pk-full-text.index.columns must name a non-empty column".to_string(),
            });
        }

        Self::validate_primary_key_index_prerequisites("full-text", primary_keys, &core)?;
        if core.primary_key_vector_index_enabled()
            && core.primary_key_vector_index_column()? == *column
        {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "Primary-key vector and full-text indexes cannot reference the same column: '{column}'."
                ),
            });
        }

        let field = fields
            .iter()
            .find(|field| field.name() == column)
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: format!(
                    "pk-full-text.index.columns entry '{column}' must reference an existing column."
                ),
            })?;
        if !matches!(field.data_type(), DataType::Char(_) | DataType::VarChar(_)) {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "pk-full-text.index.columns entry '{column}' must reference a CHAR or VARCHAR column."
                ),
            });
        }
        Ok(())
    }

    fn validate_primary_key_index_prerequisites(
        index_name: &str,
        primary_keys: &[String],
        core: &CoreOptions<'_>,
    ) -> crate::Result<()> {
        if primary_keys.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: format!("Primary-key {index_name} index requires a primary-key table."),
            });
        }

        let merge_engine = core
            .merge_engine()
            .map_err(Self::options_error_to_config_invalid)?;
        if merge_engine != MergeEngine::FirstRow && !core.deletion_vectors_enabled() {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "Primary-key {index_name} index requires deletion-vectors.enabled = true."
                ),
            });
        }
        if core.deletion_vectors_enabled() && core.deletion_vectors_merge_on_read() {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "Primary-key {index_name} index requires deletion-vectors.merge-on-read = false."
                ),
            });
        }

        let bucket = core.bucket();
        if bucket <= 0 && bucket != POSTPONE_BUCKET {
            return Err(crate::Error::ConfigInvalid {
                message: format!(
                    "Primary-key {index_name} index requires fixed or postpone bucket mode (bucket > 0 or bucket = -2), but bucket is {bucket}."
                ),
            });
        }
        Ok(())
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
    use crate::spec::{BlobType, CharType, FloatType, IntType, VarCharType, VectorType};

    use super::*;

    fn build_index_test_schema(
        columns: Vec<(&str, DataType)>,
        primary_keys: &[&str],
        options: &[(&str, &str)],
    ) -> crate::Result<Schema> {
        let mut builder = Schema::builder();
        for (name, data_type) in columns {
            builder = builder.column(name, data_type);
        }
        if !primary_keys.is_empty() {
            builder = builder.primary_key(primary_keys.iter().copied());
        }
        for (key, value) in options {
            builder = builder.option(*key, *value);
        }
        builder.build()
    }

    fn assert_config_invalid<T: std::fmt::Debug>(result: crate::Result<T>, expected_message: &str) {
        let error = result.unwrap_err();
        assert!(
            matches!(&error, crate::Error::ConfigInvalid { message }
                if message.contains(expected_message)),
            "expected ConfigInvalid containing '{expected_message}', got {error:?}"
        );
    }

    fn build_vector_index_test_schema(
        data_type: DataType,
        primary_keys: &[&str],
        configure: impl FnOnce(&mut HashMap<String, String>),
    ) -> crate::Result<Schema> {
        let mut options = HashMap::from([
            ("bucket".to_string(), "1".to_string()),
            ("deletion-vectors.enabled".to_string(), "true".to_string()),
            (
                "pk-vector.index.columns".to_string(),
                "embedding".to_string(),
            ),
            (
                "fields.embedding.pk-vector.index.type".to_string(),
                "ivf-flat".to_string(),
            ),
            (
                "fields.embedding.pk-vector.distance.metric".to_string(),
                "l2".to_string(),
            ),
        ]);
        configure(&mut options);
        let option_refs: Vec<(&str, &str)> = options
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        build_index_test_schema(
            vec![
                ("id", DataType::Int(IntType::new())),
                ("embedding", data_type),
            ],
            primary_keys,
            &option_refs,
        )
    }

    fn build_full_text_index_test_schema(
        data_type: DataType,
        primary_keys: &[&str],
        configure: impl FnOnce(&mut HashMap<String, String>),
    ) -> crate::Result<Schema> {
        let mut options = HashMap::from([
            ("bucket".to_string(), "1".to_string()),
            ("deletion-vectors.enabled".to_string(), "true".to_string()),
            (
                "pk-full-text.index.columns".to_string(),
                "content".to_string(),
            ),
        ]);
        configure(&mut options);
        let option_refs: Vec<(&str, &str)> = options
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        build_index_test_schema(
            vec![
                ("id", DataType::Int(IntType::new())),
                ("content", data_type),
            ],
            primary_keys,
            &option_refs,
        )
    }

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
    fn test_schema_rejects_invalid_read_batch_size() {
        for value in ["0", "-1", "invalid"] {
            let err = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .option("read.batch-size", value)
                .build()
                .unwrap_err();
            assert!(
                matches!(err, crate::Error::ConfigInvalid { ref message }
                    if message.contains("read.batch-size")),
                "got {err:?} for read.batch-size={value}"
            );
        }
    }

    #[test]
    fn test_apply_changes_rejects_invalid_read_batch_size() {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let table_schema = TableSchema::new(0, &schema);

        let err = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::set_option(
                "read.batch-size".to_string(),
                "0".to_string(),
            )])
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("read.batch-size")),
            "got {err:?}"
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
    fn test_blob_field_option_promotes_binary_column() {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "payload",
                DataType::VarBinary(
                    crate::spec::VarBinaryType::new(crate::spec::VarBinaryType::DEFAULT_LENGTH)
                        .unwrap(),
                ),
            )
            .option("blob-field", "payload")
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap();

        assert!(matches!(schema.fields()[1].data_type(), DataType::Blob(_)));
    }

    #[test]
    fn test_blob_comment_directive_promotes_column_and_strips_comment() {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column_with_description(
                "payload",
                DataType::VarBinary(
                    crate::spec::VarBinaryType::new(crate::spec::VarBinaryType::DEFAULT_LENGTH)
                        .unwrap(),
                ),
                Some("__BLOB_FIELD; payload bytes".to_string()),
            )
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap();

        assert!(matches!(schema.fields()[1].data_type(), DataType::Blob(_)));
        assert_eq!(schema.fields()[1].description(), Some("payload bytes"));
        assert_eq!(
            schema.options().get("blob-field").map(String::as_str),
            Some("payload")
        );
    }

    #[test]
    fn test_blob_comment_directive_rejects_unknown_create_directive() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column_with_description(
                "payload",
                DataType::VarBinary(
                    crate::spec::VarBinaryType::new(crate::spec::VarBinaryType::DEFAULT_LENGTH)
                        .unwrap(),
                ),
                Some("__BLOB_FIEL; payload bytes".to_string()),
            )
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { message } if message.contains("Unsupported BLOB comment directive") && message.contains("__BLOB_FIEL")),
            "unknown __BLOB* comment directive should be rejected"
        );
    }

    #[test]
    fn test_blob_comment_directive_rejects_malformed_marker_separator() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column_with_description(
                "payload",
                DataType::VarBinary(
                    crate::spec::VarBinaryType::new(crate::spec::VarBinaryType::DEFAULT_LENGTH)
                        .unwrap(),
                ),
                Some("__BLOB_FIELD ; payload bytes".to_string()),
            )
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { message } if message.contains("Unsupported BLOB comment directive") && message.contains("__BLOB_FIELD ; payload bytes")),
            "BLOB directive marker should be followed immediately by ';'"
        );
    }

    #[test]
    fn test_blob_comment_directive_rejects_descriptor_view_conflict() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column_with_description(
                "preview",
                DataType::VarBinary(
                    crate::spec::VarBinaryType::new(crate::spec::VarBinaryType::DEFAULT_LENGTH)
                        .unwrap(),
                ),
                Some("__BLOB_VIEW_FIELD".to_string()),
            )
            .option("blob-descriptor-field", "preview")
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { message }
                if message.contains("preview")
                    && message.contains("blob-view-field")
                    && message.contains("blob-descriptor-field")),
            "field configured as both blob descriptor and blob view should be rejected"
        );
    }

    #[test]
    fn test_blob_comment_directive_add_column_updates_options() {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("thumb", DataType::Blob(BlobType::new()))
            .option("blob-descriptor-field", "thumb")
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap();

        let schema = TableSchema::new(0, &schema)
            .apply_changes(vec![crate::spec::SchemaChange::AddColumn {
                field_names: vec!["preview".to_string()],
                data_type: DataType::VarBinary(
                    crate::spec::VarBinaryType::new(crate::spec::VarBinaryType::DEFAULT_LENGTH)
                        .unwrap(),
                ),
                comment: Some("__BLOB_DESCRIPTOR_FIELD; preview descriptor".to_string()),
                column_move: None,
            }])
            .unwrap();

        let preview = schema
            .fields()
            .iter()
            .find(|field| field.name() == "preview")
            .unwrap();
        assert!(matches!(preview.data_type(), DataType::Blob(_)));
        assert_eq!(preview.description(), Some("preview descriptor"));
        assert_eq!(
            schema
                .options()
                .get("blob-descriptor-field")
                .map(String::as_str),
            Some("thumb,preview")
        );
    }

    #[test]
    fn test_blob_comment_directive_rejects_unknown_add_column_directive() {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap();

        let err = TableSchema::new(0, &schema)
            .apply_changes(vec![crate::spec::SchemaChange::AddColumn {
                field_names: vec!["preview".to_string()],
                data_type: DataType::VarBinary(
                    crate::spec::VarBinaryType::new(crate::spec::VarBinaryType::DEFAULT_LENGTH)
                        .unwrap(),
                ),
                comment: Some("__BLOB_UNKNOWN; preview descriptor".to_string()),
                column_move: None,
            }])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { message } if message.contains("Unsupported BLOB comment directive") && message.contains("__BLOB_UNKNOWN")),
            "unknown __BLOB* add-column directive should be rejected"
        );
    }

    #[test]
    fn test_blob_field_option_rejects_non_binary_column() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Int(IntType::new()))
            .option("blob-field", "payload")
            .option("data-evolution.enabled", "true")
            .build()
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { message } if message.contains("non-binary column")),
            "blob-field on a non-binary column should be rejected"
        );
    }

    #[test]
    fn test_partial_update_schema_validation_rejects_unsupported_options() {
        for (key, value) in [
            ("fields.value.ignore-delete", "true"),
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
    fn test_partial_update_schema_validation_accepts_ignore_delete_options() {
        for (key, value) in [
            ("ignore-delete", "true"),
            ("ignore-delete", "false"),
            ("partial-update.ignore-delete", "true"),
            ("partial-update.ignore-delete", "false"),
        ] {
            let schema = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "partial-update")
                .option(key, value)
                .build()
                .unwrap();

            assert_eq!(schema.options().get(key).map(String::as_str), Some(value));
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

    #[test]
    fn test_deletion_vector_schema_validation_accepts_supported_changelog_producers() {
        for producer in ["none", "input", "lookup"] {
            Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("deletion-vectors.enabled", "true")
                .option("changelog-producer", producer)
                .build()
                .unwrap();
        }
    }

    #[test]
    fn test_deletion_vector_schema_validation_rejects_incompatible_changelog_producers() {
        for (producer, expected_message) in [
            ("full-compaction", "NONE/INPUT/LOOKUP"),
            ("unknown", "Unsupported changelog-producer"),
        ] {
            assert_config_invalid(
                Schema::builder()
                    .column("id", DataType::Int(IntType::new()))
                    .column("value", DataType::Int(IntType::new()))
                    .primary_key(["id"])
                    .option("deletion-vectors.enabled", "true")
                    .option("changelog-producer", producer)
                    .build(),
                expected_message,
            );
        }
    }

    #[test]
    fn test_deletion_vector_apply_changes_rejects_incompatible_changelog_producers() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("deletion-vectors.enabled", "true")
                .build()
                .unwrap(),
        );

        for (producer, expected_message) in [
            ("full-compaction", "NONE/INPUT/LOOKUP"),
            ("unknown", "Unsupported changelog-producer"),
        ] {
            assert_config_invalid(
                table_schema.apply_changes(vec![crate::spec::SchemaChange::set_option(
                    "changelog-producer".to_string(),
                    producer.to_string(),
                )]),
                expected_message,
            );
        }
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
                .column(
                    "payloads",
                    DataType::Array(ArrayType::new(DataType::Blob(BlobType::new()))),
                )
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
            (
                "payloads",
                DataType::Array(ArrayType::new(DataType::VarBinary(
                    crate::spec::VarBinaryType::new(10).unwrap(),
                ))),
            ),
            (
                "id",
                DataType::Array(ArrayType::new(DataType::Blob(BlobType::new()))),
            ),
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
    fn test_apply_changes_rejects_blob_column_rename() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("payload", DataType::Blob(BlobType::new()))
                .column(
                    "payloads",
                    DataType::Array(ArrayType::new(DataType::Blob(BlobType::new()))),
                )
                .option("data-evolution.enabled", "true")
                .build()
                .unwrap(),
        );

        for column in ["payload", "payloads"] {
            let err = table_schema
                .apply_changes(vec![crate::spec::SchemaChange::rename_column(
                    column.to_string(),
                    format!("renamed_{column}"),
                )])
                .unwrap_err();
            assert!(
                matches!(err, crate::Error::Unsupported { ref message }
                    if message == &format!("Cannot rename BLOB column: [{column}]")),
                "expected BLOB rename rejection for {column}, got {err:?}"
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

    // Build a raw TableSchema without going through Schema::build validation,
    // so we can exercise validate_resolved_structure against malformed input.
    fn raw_table_schema(
        fields: Vec<DataField>,
        partition_keys: Vec<String>,
        primary_keys: Vec<String>,
    ) -> TableSchema {
        let highest_field_id = TableSchema::current_highest_field_id(&fields);
        TableSchema {
            version: TableSchema::CURRENT_VERSION,
            id: 0,
            fields,
            highest_field_id,
            partition_keys,
            primary_keys,
            options: HashMap::new(),
            comment: None,
            time_millis: 0,
        }
    }

    #[test]
    fn test_validate_resolved_structure_accepts_valid_schema() {
        let schema = raw_table_schema(
            vec![
                DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
                DataField::new(1, "name".to_string(), DataType::Int(IntType::new())),
            ],
            vec![],
            vec!["id".to_string()],
        );
        assert!(schema.validate_resolved_structure().is_ok());
    }

    #[test]
    fn test_validate_resolved_structure_rejects_missing_primary_key() {
        let schema = raw_table_schema(
            vec![DataField::new(
                0,
                "id".to_string(),
                DataType::Int(IntType::new()),
            )],
            vec![],
            vec!["missing".to_string()],
        );
        let err = schema.validate_resolved_structure().unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { .. }),
            "missing PK column should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_validate_resolved_structure_rejects_missing_partition_key() {
        let schema = raw_table_schema(
            vec![DataField::new(
                0,
                "id".to_string(),
                DataType::Int(IntType::new()),
            )],
            vec!["missing".to_string()],
            vec![],
        );
        let err = schema.validate_resolved_structure().unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { .. }),
            "missing partition column should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_validate_resolved_structure_rejects_duplicate_field_names() {
        let schema = raw_table_schema(
            vec![
                DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
                DataField::new(1, "id".to_string(), DataType::Int(IntType::new())),
            ],
            vec![],
            vec![],
        );
        let err = schema.validate_resolved_structure().unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { .. }),
            "duplicate field names should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_validate_resolved_structure_rejects_duplicate_field_ids() {
        let schema = raw_table_schema(
            vec![
                DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
                DataField::new(0, "name".to_string(), DataType::Int(IntType::new())),
            ],
            vec![],
            vec![],
        );
        let err = schema.validate_resolved_structure().unwrap_err();
        assert!(
            matches!(err, crate::Error::DataInvalid { .. }),
            "duplicate field ids should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_validate_resolved_structure_rejects_partition_only_primary_key() {
        // PK == partition columns: the read path selects the KV/merge path from
        // the raw primary keys but feeds the reader the trimmed keys (empty),
        // which panics on a zero-column key. Must be rejected up front.
        let schema = raw_table_schema(
            vec![
                DataField::new(0, "p".to_string(), DataType::Int(IntType::new())),
                DataField::new(1, "v".to_string(), DataType::Int(IntType::new())),
            ],
            vec!["p".to_string()],
            vec!["p".to_string()],
        );
        let err = schema.validate_resolved_structure().unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { .. }),
            "partition-only primary key should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_validate_resolved_structure_rejects_reserved_field_name() {
        for reserved in [
            "_ROW_ID",
            "_SEQUENCE_NUMBER",
            "_VALUE_KIND",
            "_LEVEL",
            "_KEY_x",
        ] {
            let schema = raw_table_schema(
                vec![
                    DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
                    DataField::new(1, reserved.to_string(), DataType::Int(IntType::new())),
                ],
                vec![],
                vec![],
            );
            let err = schema.validate_resolved_structure().unwrap_err();
            assert!(
                matches!(err, crate::Error::ConfigInvalid { .. }),
                "reserved field name {reserved:?} should be rejected, got {err:?}"
            );
        }
    }

    #[test]
    fn test_validate_resolved_structure_rejects_reserved_field_id() {
        let schema = raw_table_schema(
            vec![DataField::new(
                i32::MAX / 2,
                "id".to_string(),
                DataType::Int(IntType::new()),
            )],
            vec![],
            vec![],
        );
        let err = schema.validate_resolved_structure().unwrap_err();
        assert!(
            matches!(err, crate::Error::DataInvalid { .. }),
            "reserved system field id should be rejected, got {err:?}"
        );
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
    fn test_partial_update_apply_changes_accepts_ignore_delete_option() {
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

        let new_schema = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::set_option(
                "ignore-delete".to_string(),
                "true".to_string(),
            )])
            .unwrap();

        assert_eq!(
            new_schema
                .options()
                .get("ignore-delete")
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn test_partial_update_apply_changes_rejects_disabling_ignore_delete() {
        for (existing_options, change) in [
            (
                vec![("ignore-delete", "true")],
                crate::spec::SchemaChange::set_option(
                    "ignore-delete".to_string(),
                    "false".to_string(),
                ),
            ),
            (
                vec![("ignore-delete", "true")],
                crate::spec::SchemaChange::remove_option("ignore-delete".to_string()),
            ),
            (
                vec![("partial-update.ignore-delete", "true")],
                crate::spec::SchemaChange::set_option(
                    "ignore-delete".to_string(),
                    "false".to_string(),
                ),
            ),
            (
                vec![("partial-update.ignore-delete", "true")],
                crate::spec::SchemaChange::remove_option(
                    "partial-update.ignore-delete".to_string(),
                ),
            ),
        ] {
            let mut builder = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "partial-update");
            for (key, value) in existing_options {
                builder = builder.option(key, value);
            }
            let table_schema = TableSchema::new(0, &builder.build().unwrap());

            let err = table_schema.apply_changes(vec![change]).unwrap_err();

            assert!(
                matches!(err, crate::Error::Unsupported { ref message }
                    if message.contains("Cannot change ignore-delete from true to false")),
                "got {err:?}"
            );
        }
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
    fn test_rename_column_rewrites_sequence_group_options() {
        // `sequence-group` is rejected by Rust's create-time merge-engine
        // validation, so the fixture carries the option on a table without
        // `merge-engine` — the shape in which Java-written metadata arrives.
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("version", DataType::Int(IntType::new()))
                .column("source_order", DataType::Int(IntType::new()))
                .column("price", DataType::Int(IntType::new()))
                .column("quantity", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option(
                    "fields.version,source_order.sequence-group",
                    "price,quantity",
                )
                .build()
                .unwrap(),
        );

        // Rename a sequence field (key side) and a protected field (value side).
        let new_schema = table_schema
            .apply_changes(vec![
                crate::spec::SchemaChange::rename_column(
                    "source_order".to_string(),
                    "order_seq".to_string(),
                ),
                crate::spec::SchemaChange::rename_column("price".to_string(), "amount".to_string()),
            ])
            .unwrap();

        assert_eq!(
            new_schema
                .options()
                .get("fields.version,order_seq.sequence-group")
                .map(String::as_str),
            Some("amount,quantity")
        );
        assert_eq!(
            new_schema
                .options()
                .get("fields.version,source_order.sequence-group"),
            None
        );
    }

    #[test]
    fn test_rename_column_rewrites_nested_key_options() {
        // Same rationale as the sequence-group test: `nested-key` cannot be
        // created through Rust, but Java-written tables carry it.
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("profile", DataType::Int(IntType::new()))
                .column("region", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("fields.profile.nested-key", "region")
                .build()
                .unwrap(),
        );

        let new_schema = table_schema
            .apply_changes(vec![
                crate::spec::SchemaChange::rename_column("profile".to_string(), "info".to_string()),
                crate::spec::SchemaChange::rename_column("region".to_string(), "area".to_string()),
            ])
            .unwrap();

        assert_eq!(
            new_schema
                .options()
                .get("fields.info.nested-key")
                .map(String::as_str),
            Some("area")
        );
        assert_eq!(new_schema.options().get("fields.profile.nested-key"), None);
    }

    #[test]
    fn test_rename_column_rewrites_remaining_case2_suffixes() {
        // `ignore-retract` / `distinct` are rejected by Rust's create-time
        // merge-engine validation; the fixture carries them (and the
        // map-shredding options, which Rust does honor) as plain metadata,
        // like a Java-written schema.
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("price", DataType::Int(IntType::new()))
                .column(
                    "props",
                    DataType::Map(MapType::new(
                        DataType::VarChar(VarCharType::string_type()),
                        DataType::Int(IntType::new()),
                    )),
                )
                .primary_key(["id"])
                .option("fields.price.ignore-retract", "true")
                .option("fields.price.distinct", "true")
                .option("fields.props.map.storage-layout", "shared-shredding")
                .option("fields.props.map.shared-shredding.max-columns", "64")
                .build()
                .unwrap(),
        );

        let new_schema = table_schema
            .apply_changes(vec![
                crate::spec::SchemaChange::rename_column("price".to_string(), "amount".to_string()),
                crate::spec::SchemaChange::rename_column(
                    "props".to_string(),
                    "properties".to_string(),
                ),
            ])
            .unwrap();

        for (key, value) in [
            ("fields.amount.ignore-retract", "true"),
            ("fields.amount.distinct", "true"),
            ("fields.properties.map.storage-layout", "shared-shredding"),
            ("fields.properties.map.shared-shredding.max-columns", "64"),
        ] {
            assert_eq!(
                new_schema.options().get(key).map(String::as_str),
                Some(value),
                "expected {key} to follow the rename"
            );
        }
        for old_key in [
            "fields.price.ignore-retract",
            "fields.price.distinct",
            "fields.props.map.storage-layout",
            "fields.props.map.shared-shredding.max-columns",
        ] {
            assert_eq!(new_schema.options().get(old_key), None);
        }
    }

    #[test]
    fn test_rename_column_field_scoped_options_match_whole_names_only() {
        // `price2` merely starts with `price`: its keys must not move when
        // `price` is renamed, while a value-side exact reference to `price`
        // still gets rewritten (Java `applyNotNestedColumnRename` semantics).
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("price", DataType::Int(IntType::new()))
                .column("price2", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("fields.price2.sequence-group", "price")
                .option("fields.price2.aggregate-function", "sum")
                .build()
                .unwrap(),
        );

        let new_schema = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::rename_column(
                "price".to_string(),
                "amount".to_string(),
            )])
            .unwrap();

        assert_eq!(
            new_schema
                .options()
                .get("fields.price2.sequence-group")
                .map(String::as_str),
            Some("amount")
        );
        assert_eq!(
            new_schema
                .options()
                .get("fields.price2.aggregate-function")
                .map(String::as_str),
            Some("sum")
        );
        assert_eq!(
            new_schema.options().get("fields.amount.sequence-group"),
            None
        );
    }

    #[test]
    fn test_create_schema_validates_primary_key_vector_index() {
        for vector_type in [
            vector_4f(),
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        ] {
            build_vector_index_test_schema(vector_type, &["id"], |_| {}).unwrap();
        }

        build_vector_index_test_schema(vector_4f(), &["id"], |options| {
            options.insert("bucket".to_string(), "-2".to_string());
            options.insert("merge-engine".to_string(), "first-row".to_string());
            options.remove("deletion-vectors.enabled");
        })
        .unwrap();

        assert_config_invalid(
            build_vector_index_test_schema(vector_4f(), &["id"], |options| {
                options.insert("merge-engine".to_string(), "first-row".to_string());
            }),
            "does not need deletion vectors",
        );

        assert_config_invalid(
            build_vector_index_test_schema(vector_4f(), &[], |_| {}),
            "primary-key table",
        );
        assert_config_invalid(
            build_vector_index_test_schema(vector_4f(), &["id"], |options| {
                options.insert("pk-vector.index.columns".to_string(), " ".to_string());
            }),
            "non-empty column",
        );
        for columns in ["embedding,", ",embedding", "embedding,,", "embedding,other"] {
            assert_config_invalid(
                build_vector_index_test_schema(vector_4f(), &["id"], |options| {
                    options.insert("pk-vector.index.columns".to_string(), columns.to_string());
                }),
                "exactly one",
            );
        }
        assert_config_invalid(
            build_vector_index_test_schema(vector_4f(), &["id"], |options| {
                options.insert("pk-vector.index.columns".to_string(), "missing".to_string());
                options.insert(
                    "fields.missing.pk-vector.index.type".to_string(),
                    "ivf-flat".to_string(),
                );
            }),
            "existing column",
        );
        assert_config_invalid(
            build_vector_index_test_schema(
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
                &["id"],
                |_| {},
            ),
            "ARRAY<FLOAT> or VECTOR<FLOAT>",
        );
        assert_config_invalid(
            build_vector_index_test_schema(vector_4f(), &["id"], |options| {
                options.remove("fields.embedding.pk-vector.index.type");
            }),
            "index.type is required",
        );
        assert_config_invalid(
            build_vector_index_test_schema(vector_4f(), &["id"], |options| {
                options.insert(
                    "fields.embedding.pk-vector.distance.metric".to_string(),
                    "manhattan".to_string(),
                );
            }),
            "unsupported vector distance metric",
        );
        assert_config_invalid(
            build_vector_index_test_schema(vector_4f(), &["id"], |options| {
                options.insert("bucket".to_string(), "-1".to_string());
            }),
            "fixed or postpone bucket",
        );
        assert_config_invalid(
            build_vector_index_test_schema(vector_4f(), &["id"], |options| {
                options.remove("deletion-vectors.enabled");
            }),
            "deletion-vectors.enabled",
        );
        assert_config_invalid(
            build_vector_index_test_schema(vector_4f(), &["id"], |options| {
                options.insert(
                    "deletion-vectors.merge-on-read".to_string(),
                    "true".to_string(),
                );
            }),
            "merge-on-read = false",
        );
    }

    #[test]
    fn test_apply_changes_validates_primary_key_vector_index() {
        let table_schema = TableSchema::new(
            0,
            &build_index_test_schema(
                vec![
                    ("id", DataType::Int(IntType::new())),
                    ("embedding", vector_4f()),
                ],
                &["id"],
                &[("bucket", "1")],
            )
            .unwrap(),
        );

        table_schema
            .apply_changes(vec![
                crate::spec::SchemaChange::set_option(
                    "deletion-vectors.enabled".to_string(),
                    "true".to_string(),
                ),
                crate::spec::SchemaChange::set_option(
                    "pk-vector.index.columns".to_string(),
                    "embedding".to_string(),
                ),
                crate::spec::SchemaChange::set_option(
                    "fields.embedding.pk-vector.index.type".to_string(),
                    "ivf-flat".to_string(),
                ),
            ])
            .unwrap();

        assert_config_invalid(
            table_schema.apply_changes(vec![
                crate::spec::SchemaChange::set_option(
                    "pk-vector.index.columns".to_string(),
                    "embedding".to_string(),
                ),
                crate::spec::SchemaChange::set_option(
                    "fields.embedding.pk-vector.index.type".to_string(),
                    "ivf-flat".to_string(),
                ),
            ]),
            "deletion-vectors.enabled",
        );

        assert_config_invalid(
            table_schema.apply_changes(vec![
                crate::spec::SchemaChange::set_option(
                    "merge-engine".to_string(),
                    "first-row".to_string(),
                ),
                crate::spec::SchemaChange::set_option(
                    "deletion-vectors.enabled".to_string(),
                    "true".to_string(),
                ),
                crate::spec::SchemaChange::set_option(
                    "pk-vector.index.columns".to_string(),
                    "embedding".to_string(),
                ),
                crate::spec::SchemaChange::set_option(
                    "fields.embedding.pk-vector.index.type".to_string(),
                    "ivf-flat".to_string(),
                ),
            ]),
            "does not need deletion vectors",
        );

        for (columns, expected_message) in [
            (" ", "non-empty column"),
            ("embedding,", "exactly one"),
            (",embedding", "exactly one"),
            ("embedding,,", "exactly one"),
        ] {
            assert_config_invalid(
                table_schema.apply_changes(vec![
                    crate::spec::SchemaChange::set_option(
                        "deletion-vectors.enabled".to_string(),
                        "true".to_string(),
                    ),
                    crate::spec::SchemaChange::set_option(
                        "pk-vector.index.columns".to_string(),
                        columns.to_string(),
                    ),
                    crate::spec::SchemaChange::set_option(
                        "fields.embedding.pk-vector.index.type".to_string(),
                        "ivf-flat".to_string(),
                    ),
                ]),
                expected_message,
            );
        }
    }

    #[test]
    fn test_create_schema_validates_primary_key_full_text_index() {
        for text_type in [
            DataType::Char(CharType::new(32).unwrap()),
            DataType::VarChar(VarCharType::string_type()),
        ] {
            build_full_text_index_test_schema(text_type, &["id"], |_| {}).unwrap();
        }

        build_full_text_index_test_schema(
            DataType::VarChar(VarCharType::string_type()),
            &["id"],
            |options| {
                options.insert("bucket".to_string(), "-2".to_string());
                options.insert("merge-engine".to_string(), "first-row".to_string());
                options.remove("deletion-vectors.enabled");
            },
        )
        .unwrap();

        assert_config_invalid(
            build_full_text_index_test_schema(
                DataType::VarChar(VarCharType::string_type()),
                &["id"],
                |options| {
                    options.insert("merge-engine".to_string(), "first-row".to_string());
                },
            ),
            "does not need deletion vectors",
        );

        assert_config_invalid(
            build_full_text_index_test_schema(
                DataType::VarChar(VarCharType::string_type()),
                &[],
                |_| {},
            ),
            "primary-key table",
        );
        assert_config_invalid(
            build_full_text_index_test_schema(
                DataType::VarChar(VarCharType::string_type()),
                &["id"],
                |options| {
                    options.insert("pk-full-text.index.columns".to_string(), " ".to_string());
                },
            ),
            "non-empty column",
        );
        assert_config_invalid(
            build_full_text_index_test_schema(
                DataType::VarChar(VarCharType::string_type()),
                &["id"],
                |options| {
                    options.insert(
                        "pk-full-text.index.columns".to_string(),
                        "content,other".to_string(),
                    );
                },
            ),
            "exactly one",
        );
        assert_config_invalid(
            build_full_text_index_test_schema(
                DataType::VarChar(VarCharType::string_type()),
                &["id"],
                |options| {
                    options.insert(
                        "pk-full-text.index.columns".to_string(),
                        "missing".to_string(),
                    );
                },
            ),
            "existing column",
        );
        assert_config_invalid(
            build_full_text_index_test_schema(DataType::Int(IntType::new()), &["id"], |_| {}),
            "CHAR or VARCHAR",
        );
        assert_config_invalid(
            build_full_text_index_test_schema(
                DataType::VarChar(VarCharType::string_type()),
                &["id"],
                |options| {
                    options.insert("bucket".to_string(), "-1".to_string());
                },
            ),
            "fixed or postpone bucket",
        );
        assert_config_invalid(
            build_full_text_index_test_schema(
                DataType::VarChar(VarCharType::string_type()),
                &["id"],
                |options| {
                    options.remove("deletion-vectors.enabled");
                },
            ),
            "deletion-vectors.enabled",
        );
        assert_config_invalid(
            build_full_text_index_test_schema(
                DataType::VarChar(VarCharType::string_type()),
                &["id"],
                |options| {
                    options.insert(
                        "deletion-vectors.merge-on-read".to_string(),
                        "true".to_string(),
                    );
                },
            ),
            "merge-on-read = false",
        );
        assert_config_invalid(
            build_index_test_schema(
                vec![
                    ("id", DataType::Int(IntType::new())),
                    (
                        "embedding",
                        DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
                    ),
                ],
                &["id"],
                &[
                    ("bucket", "1"),
                    ("deletion-vectors.enabled", "true"),
                    ("pk-vector.index.columns", "embedding"),
                    ("fields.embedding.pk-vector.index.type", "ivf-flat"),
                    ("pk-full-text.index.columns", "embedding"),
                ],
            ),
            "cannot reference the same column",
        );
    }

    #[test]
    fn test_apply_changes_validates_primary_key_full_text_index() {
        let table_schema = TableSchema::new(
            0,
            &build_index_test_schema(
                vec![
                    ("id", DataType::Int(IntType::new())),
                    ("content", DataType::VarChar(VarCharType::string_type())),
                ],
                &["id"],
                &[("bucket", "1")],
            )
            .unwrap(),
        );

        table_schema
            .apply_changes(vec![
                crate::spec::SchemaChange::set_option(
                    "deletion-vectors.enabled".to_string(),
                    "true".to_string(),
                ),
                crate::spec::SchemaChange::set_option(
                    "pk-full-text.index.columns".to_string(),
                    "content".to_string(),
                ),
            ])
            .unwrap();

        assert_config_invalid(
            table_schema.apply_changes(vec![crate::spec::SchemaChange::set_option(
                "pk-full-text.index.columns".to_string(),
                "content".to_string(),
            )]),
            "deletion-vectors.enabled",
        );

        assert_config_invalid(
            table_schema.apply_changes(vec![
                crate::spec::SchemaChange::set_option(
                    "merge-engine".to_string(),
                    "first-row".to_string(),
                ),
                crate::spec::SchemaChange::set_option(
                    "deletion-vectors.enabled".to_string(),
                    "true".to_string(),
                ),
                crate::spec::SchemaChange::set_option(
                    "pk-full-text.index.columns".to_string(),
                    "content".to_string(),
                ),
            ]),
            "does not need deletion vectors",
        );
    }

    fn assert_primary_key_index_column_changes_rejected(
        table_schema: &TableSchema,
        column_name: &str,
        new_data_type: DataType,
    ) {
        let changes = [
            (
                crate::spec::SchemaChange::rename_column(
                    column_name.to_string(),
                    format!("renamed_{column_name}"),
                ),
                format!("Cannot rename primary-key index column: [{column_name}]"),
            ),
            (
                crate::spec::SchemaChange::drop_column(column_name.to_string()),
                format!("Cannot drop primary-key index column: [{column_name}]"),
            ),
            (
                crate::spec::SchemaChange::update_column_type(
                    column_name.to_string(),
                    new_data_type,
                ),
                format!("Cannot update type of primary-key index column: [{column_name}]"),
            ),
        ];

        for (change, expected_message) in changes {
            let err = table_schema.apply_changes(vec![change]).unwrap_err();
            assert!(
                matches!(err, crate::Error::Unsupported { ref message }
                    if message == &expected_message),
                "expected primary-key index guard, got {err:?}"
            );
        }
    }

    #[test]
    fn test_rejects_destructive_primary_key_full_text_index_column_changes() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("content", DataType::VarChar(VarCharType::string_type()))
                .primary_key(["id"])
                .option("bucket", "1")
                .option("deletion-vectors.enabled", "true")
                .option("pk-full-text.index.columns", "content")
                .build()
                .unwrap(),
        );

        assert_primary_key_index_column_changes_rejected(
            &table_schema,
            "content",
            DataType::Int(IntType::new()),
        );

        let err = table_schema
            .apply_changes(vec![
                crate::spec::SchemaChange::remove_option("pk-full-text.index.columns".to_string()),
                crate::spec::SchemaChange::rename_column(
                    "content".to_string(),
                    "renamed_content".to_string(),
                ),
            ])
            .unwrap_err();
        assert!(matches!(
            err,
            crate::Error::Unsupported { ref message }
                if message == "Cannot rename primary-key index column: [content]"
        ));
    }

    #[test]
    fn test_rejects_destructive_primary_key_vector_index_column_changes() {
        let vector_type = DataType::Vector(
            VectorType::try_new(true, 3, DataType::Float(FloatType::new())).unwrap(),
        );
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("embedding", vector_type.clone())
                .primary_key(["id"])
                .option("bucket", "1")
                .option("deletion-vectors.enabled", "true")
                .option("pk-vector.index.columns", "embedding")
                .option("fields.embedding.pk-vector.index.type", "ivf-flat")
                .option("fields.embedding.pk-vector.distance.metric", "l2")
                .build()
                .unwrap(),
        );

        assert_primary_key_index_column_changes_rejected(&table_schema, "embedding", vector_type);

        let err = table_schema
            .apply_changes(vec![
                crate::spec::SchemaChange::remove_option("pk-vector.index.columns".to_string()),
                crate::spec::SchemaChange::rename_column(
                    "embedding".to_string(),
                    "renamed_embedding".to_string(),
                ),
            ])
            .unwrap_err();
        assert!(matches!(
            err,
            crate::Error::Unsupported { ref message }
                if message == "Cannot rename primary-key index column: [embedding]"
        ));
    }

    #[test]
    fn test_create_schema_rejects_reserved_field_names() {
        // Java `SpecialFields.SYSTEM_FIELD_NAMES` plus the `_KEY_` prefix. A
        // user column with one of these names is excluded from the physical
        // read and silently filled with the system value.
        for name in [
            "_SEQUENCE_NUMBER",
            "_VALUE_KIND",
            "_LEVEL",
            "rowkind",
            "_ROW_ID",
            "_KEY_id",
        ] {
            let err = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column(name, DataType::Int(IntType::new()))
                .build()
                .unwrap_err();

            assert!(
                matches!(err, crate::Error::ConfigInvalid { ref message }
                    if message.contains(name) && message.contains("reserved")),
                "reserved field name '{name}' should be rejected at create time, got {err:?}"
            );
        }
    }

    #[test]
    fn test_create_schema_accepts_names_that_only_look_reserved() {
        // Guard against over-rejecting: only exact system names and the
        // `_KEY_` prefix are reserved.
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("_sequence_number", DataType::Int(IntType::new()))
            .column("row_kind", DataType::Int(IntType::new()))
            .column("_KEY", DataType::Int(IntType::new()))
            .build();

        assert!(
            schema.is_ok(),
            "names that merely resemble system fields should be accepted, got {schema:?}"
        );
    }

    #[test]
    fn test_alter_add_reserved_field_name_rejected() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );

        let err = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::add_column(
                "_ROW_ID".to_string(),
                DataType::Int(IntType::new()),
            )])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("_ROW_ID") && message.contains("reserved")),
            "adding a reserved column name should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_alter_rename_to_reserved_field_name_rejected() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("v", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );

        let err = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::rename_column(
                "v".to_string(),
                "_VALUE_KIND".to_string(),
            )])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("_VALUE_KIND") && message.contains("reserved")),
            "renaming a column to a reserved name should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_create_schema_rejects_unknown_bucket_key() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .option("bucket", "4")
            // typo: `nmae` instead of `name`
            .option("bucket-key", "nmae")
            .build()
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("nmae") && message.contains("can not be found")),
            "bucket key missing from the schema should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_create_schema_rejects_repeated_bucket_key() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .option("bucket", "4")
            .option("bucket-key", "name,name")
            .build()
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("name") && message.contains("repeatedly")),
            "repeated bucket key should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_create_schema_rejects_partitioned_bucket_key() {
        let err = Schema::builder()
            .column("pt", DataType::Int(IntType::new()))
            .column("id", DataType::Int(IntType::new()))
            .partition_keys(["pt"])
            .option("bucket", "4")
            .option("bucket-key", "pt")
            .build()
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("pt") && message.contains("partition")),
            "partition field used as bucket key should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_create_schema_rejects_bucket_key_outside_primary_key() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("bucket", "4")
            .option("bucket-key", "name")
            .build()
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("name") && message.contains("primary key")),
            "non-primary-key bucket key on a PK table should be rejected, got {err:?}"
        );
    }

    #[test]
    fn test_create_schema_accepts_bucket_key_subset_of_primary_key() {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .column("v", DataType::Int(IntType::new()))
            .primary_key(["id", "name"])
            .option("bucket", "4")
            .option("bucket-key", "id")
            .build();

        assert!(
            schema.is_ok(),
            "a bucket key that is a primary key field should be accepted, got {schema:?}"
        );
    }

    #[test]
    fn test_blank_bucket_key_falls_back_to_primary_keys() {
        // A blank option must not resolve to a `""` column: `TableWrite` would
        // find no field index for it and silently write every row to bucket 0.
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("name", DataType::VarChar(VarCharType::string_type()))
                .primary_key(["id"])
                .option("bucket", "4")
                .option("bucket-key", "  ")
                .build()
                .unwrap(),
        );

        assert_eq!(table_schema.bucket_keys(), vec!["id".to_string()]);
    }

    #[test]
    fn test_alter_set_unknown_bucket_key_rejected() {
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
            .apply_changes(vec![crate::spec::SchemaChange::set_option(
                BUCKET_KEY_OPTION.to_string(),
                "nmae".to_string(),
            )])
            .unwrap_err();

        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("nmae") && message.contains("can not be found")),
            "alter setting an unknown bucket key should be rejected, got {err:?}"
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
    fn test_bucket_key_history_rejects_destructive_column_changes() {
        let table_schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("bucket_col", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .option("bucket", "4")
                .option("bucket-key", "bucket_col")
                .build()
                .unwrap(),
        );

        for changes in [
            vec![crate::spec::SchemaChange::update_column_type(
                "bucket_col".to_string(),
                DataType::BigInt(crate::spec::BigIntType::new()),
            )],
            vec![
                crate::spec::SchemaChange::remove_option(BUCKET_KEY_OPTION.to_string()),
                crate::spec::SchemaChange::drop_column("bucket_col".to_string()),
            ],
            vec![
                crate::spec::SchemaChange::rename_column(
                    "bucket_col".to_string(),
                    "renamed_bucket_col".to_string(),
                ),
                crate::spec::SchemaChange::update_column_type(
                    "renamed_bucket_col".to_string(),
                    DataType::BigInt(crate::spec::BigIntType::new()),
                ),
            ],
        ] {
            let err = table_schema.apply_changes(changes).unwrap_err();
            assert!(
                matches!(err, crate::Error::Unsupported { ref message }
                    if message.contains("bucket-key column") && message.contains("bucket_col")),
                "expected historical bucket-key protection, got {err:?}"
            );
        }

        let updated = table_schema
            .apply_changes(vec![crate::spec::SchemaChange::update_column_type(
                "value".to_string(),
                DataType::BigInt(crate::spec::BigIntType::new()),
            )])
            .unwrap();
        assert!(matches!(
            updated.fields()[2].data_type(),
            DataType::BigInt(_)
        ));
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

    #[test]
    fn rowkind_field_requires_primary_key_table() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("op", DataType::VarChar(VarCharType::string_type()))
            .option("rowkind.field", "op")
            .build()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message } if message.contains("rowkind.field")),
            "got {err:?}"
        );
    }

    #[test]
    fn rowkind_field_rejects_non_deduplicate_merge_engine() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("op", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("merge-engine", "partial-update")
            .option("rowkind.field", "op")
            .build()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("deduplicate")),
            "got {err:?}"
        );
    }

    #[test]
    fn rowkind_field_rejects_changelog_producer_input() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("op", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("changelog-producer", "input")
            .option("rowkind.field", "op")
            .build()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("changelog-producer=input")),
            "got {err:?}"
        );
    }

    #[test]
    fn rowkind_field_rejects_non_string_field() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("op", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("rowkind.field", "op")
            .build()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("STRING")),
            "got {err:?}"
        );
    }

    #[test]
    fn rowkind_field_rejects_missing_field() {
        let err = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("rowkind.field", "op")
            .build()
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { ref message }
                if message.contains("can not be found")),
            "got {err:?}"
        );
    }

    #[test]
    fn rowkind_field_accepts_valid_deduplicate_table() {
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .column("op", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("rowkind.field", "op")
            .build()
            .unwrap();
    }
}
