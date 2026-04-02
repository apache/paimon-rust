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

use crate::spec::types::{DataType, RowType};
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

    /// Get the highest field ID from a list of fields.
    pub fn current_highest_field_id(fields: &[DataField]) -> i32 {
        fields.iter().map(|f| f.id()).max().unwrap_or(-1)
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

    pub fn primary_keys(&self) -> &[String] {
        &self.primary_keys
    }

    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }

    /// Create a copy of this schema with extra options merged in.
    pub fn copy_with_options(&self, extra: HashMap<String, String>) -> Self {
        let mut new_schema = self.clone();
        new_schema.options.extend(extra);
        new_schema
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn time_millis(&self) -> i64 {
        self.time_millis
    }
}

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
    ///
    /// TODO: Support RowType in schema columns with field ID assignment for nested fields.
    /// See <https://github.com/apache/paimon/pull/1547>.
    pub fn column_with_description(
        mut self,
        column_name: impl Into<String>,
        data_type: DataType,
        description: Option<String>,
    ) -> Self {
        if data_type.contains_row_type() {
            todo!(
                "Column type containing RowType is not supported yet: field ID assignment for nested row fields is not implemented. See https://github.com/apache/paimon/pull/1547"
            );
        }
        let name = column_name.into();
        let id = self.next_field_id;
        self.next_field_id += 1;
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
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::spec::IntType;

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

    /// Adding a column whose type is or contains RowType panics (todo! until field ID assignment for nested row fields).
    /// See <https://github.com/apache/paimon/pull/1547>.
    #[test]
    #[should_panic(expected = "RowType")]
    fn test_schema_builder_column_row_type_panics() {
        let row_type = RowType::new(vec![DataField::new(
            0,
            "nested".into(),
            DataType::Int(IntType::new()),
        )]);
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Row(row_type));
    }
}
