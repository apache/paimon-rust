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

use crate::spec::DataTypeRoot;
use serde::{Deserialize, Serialize};

/// Schema change to table.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L36>
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum SchemaChange {
    SetOption(SetOption),
    RemoveOption(RemoveOption),
    UpdateComment(UpdateComment),
    AddColumn(AddColumn),
    RenameColumn(RenameColumn),
    DropColumn(DropColumn),
    UpdateColumnType(UpdateColumnType),
    UpdateColumnPosition(UpdateColumnPosition),
    UpdateColumnNullability(UpdateColumnNullability),
    UpdateColumnComment(UpdateColumnComment),
}

/// A SchemaChange to set a table option.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L95>
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct SetOption {
    key: String,
    value: String,
}

impl SetOption {
    /// Create a new `SetOption`.
    pub fn new(key: String, value: String) -> Self {
        SetOption { key, value }
    }

    /// Get the key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the value.
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// A SchemaChange to remove a table option.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L134>
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct RemoveOption {
    key: String,
}

impl RemoveOption {
    /// Create a new `RemoveOption`.
    pub fn new(key: String) -> Self {
        RemoveOption { key }
    }

    /// Get the key.
    pub fn key(&self) -> &str {
        &self.key
    }
}

/// A SchemaChange to update a table comment.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L167>
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct UpdateComment {
    /// If comment is null, means to remove comment.
    comment: Option<String>,
}

impl UpdateComment {
    /// Create a new `UpdateComment`.
    pub fn new(comment: Option<String>) -> Self {
        UpdateComment { comment }
    }

    /// Get the comment.
    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }
}

/// A SchemaChange to add a new field.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L201>
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddColumn {
    field_name: String,
    data_type: DataTypeRoot,
    description: Option<String>,
    #[serde(rename = "move")]
    move_: Option<Move>,
}

impl AddColumn {
    /// Create a new `AddColumn`.
    pub fn new(
        field_name: String,
        data_type: DataTypeRoot,
        description: Option<String>,
        move_: Option<Move>,
    ) -> Self {
        AddColumn {
            field_name,
            data_type,
            description,
            move_,
        }
    }

    /// Get the field name.
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// Get the data type.
    pub fn data_type(&self) -> &DataTypeRoot {
        &self.data_type
    }

    /// Get the description.
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Get the move.
    pub fn move_(&self) -> Option<&Move> {
        self.move_.as_ref()
    }
}

/// A SchemaChange to rename a field.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L260>
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RenameColumn {
    field_name: String,
    new_name: String,
}

impl RenameColumn {
    /// Create a new `RenameColumn`.
    pub fn new(field_name: String, new_name: String) -> Self {
        RenameColumn {
            field_name,
            new_name,
        }
    }

    /// Get the field name.
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// Get the new name.
    pub fn new_name(&self) -> &str {
        &self.new_name
    }
}

/// A SchemaChange to drop a field.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L302>
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct DropColumn {
    field_name: String,
}

impl DropColumn {
    /// Create a new `DropColumn`.
    pub fn new(field_name: String) -> Self {
        DropColumn { field_name }
    }

    /// Get the field name.
    pub fn field_name(&self) -> &str {
        &self.field_name
    }
}

/// A SchemaChange to update the field's type.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L335>
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateColumnType {
    field_name: String,
    new_data_type: DataTypeRoot,
}

impl UpdateColumnType {
    /// Create a new `UpdateColumnType`.
    pub fn new(field_name: String, new_data_type: DataTypeRoot) -> Self {
        UpdateColumnType {
            field_name,
            new_data_type,
        }
    }

    /// Get the field name.
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// Get the new data type.
    pub fn new_data_type(&self) -> &DataTypeRoot {
        &self.new_data_type
    }
}

/// A SchemaChange to update the field's position.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L377>
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct UpdateColumnPosition {
    #[serde(rename = "move")]
    move_: Move,
}

impl UpdateColumnPosition {
    /// Create a new `UpdateColumnPosition`.
    pub fn new(move_: Move) -> Self {
        UpdateColumnPosition { move_ }
    }

    /// Get the move.
    pub fn move_(&self) -> &Move {
        &self.move_
    }
}

/// A SchemaChange to update the field's nullability.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L470>
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateColumnNullability {
    field_names: Vec<String>,
    new_nullability: bool,
}

impl UpdateColumnNullability {
    /// Create a new `UpdateColumnNullability`.
    pub fn new(field_names: Vec<String>, new_nullability: bool) -> Self {
        UpdateColumnNullability {
            field_names,
            new_nullability,
        }
    }

    /// Get the field names.
    pub fn field_names(&self) -> &[String] {
        &self.field_names
    }

    /// Get the new nullability.
    pub fn new_nullability(&self) -> bool {
        self.new_nullability
    }
}

/// A SchemaChange to update the (nested) field's comment.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L512>
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateColumnComment {
    field_names: Vec<String>,
    new_description: String,
}

impl UpdateColumnComment {
    /// Create a new `UpdateColumnComment`.
    pub fn new(field_names: Vec<String>, new_description: String) -> Self {
        UpdateColumnComment {
            field_names,
            new_description,
        }
    }

    /// Get the field names.
    pub fn field_names(&self) -> &[String] {
        &self.field_names
    }

    /// Get the new description.
    pub fn new_description(&self) -> &str {
        &self.new_description
    }
}

impl SchemaChange {
    /// impl the `set_option`.
    pub fn set_option(key: String, value: String) -> Self {
        SchemaChange::SetOption(SetOption::new(key, value))
    }

    /// impl the `remove_option`.
    pub fn remove_option(key: String) -> Self {
        SchemaChange::RemoveOption(RemoveOption::new(key))
    }

    /// impl the `update_comment`.
    pub fn update_comment(comment: Option<String>) -> Self {
        SchemaChange::UpdateComment(UpdateComment::new(comment))
    }

    /// impl the `add_column`.
    pub fn add_column(field_name: String, data_type: DataTypeRoot) -> Self {
        SchemaChange::AddColumn(AddColumn::new(field_name, data_type, None, None))
    }

    /// impl the `add_column_with_comment`.
    pub fn add_column_with_comment(
        field_name: String,
        data_type: DataTypeRoot,
        comment: String,
    ) -> Self {
        SchemaChange::AddColumn(AddColumn::new(field_name, data_type, Some(comment), None))
    }

    /// impl the `add_column_with_comment_and_move`.
    pub fn add_column_with_comment_and_move(
        field_name: String,
        data_type: DataTypeRoot,
        comment: String,
        move_: Move,
    ) -> Self {
        SchemaChange::AddColumn(AddColumn::new(
            field_name,
            data_type,
            Some(comment),
            Some(move_),
        ))
    }

    /// impl the `rename_column`.
    pub fn rename_column(field_name: String, new_name: String) -> Self {
        SchemaChange::RenameColumn(RenameColumn::new(field_name, new_name))
    }

    /// impl the `drop_column`.
    pub fn drop_column(field_name: String) -> Self {
        SchemaChange::DropColumn(DropColumn::new(field_name))
    }

    /// impl the `update_column_type`.
    pub fn update_column_type(field_name: String, new_data_type: DataTypeRoot) -> Self {
        SchemaChange::UpdateColumnType(UpdateColumnType::new(field_name, new_data_type))
    }

    /// impl the `update_column_position`.
    pub fn update_column_nullability(field_name: String, new_nullability: bool) -> Self {
        SchemaChange::UpdateColumnNullability(UpdateColumnNullability {
            field_names: vec![field_name],
            new_nullability,
        })
    }

    /// impl the `update_columns_nullability`.
    pub fn update_columns_nullability(field_names: Vec<String>, new_nullability: bool) -> Self {
        SchemaChange::UpdateColumnNullability(UpdateColumnNullability {
            field_names,
            new_nullability,
        })
    }

    /// impl the `update_column_comment`.
    pub fn update_column_comment(field_name: String, comment: String) -> Self {
        SchemaChange::UpdateColumnComment(UpdateColumnComment {
            field_names: vec![field_name],
            new_description: comment,
        })
    }

    /// impl the `update_columns_comment`.
    pub fn update_columns_comment(field_names: Vec<String>, comment: String) -> Self {
        SchemaChange::UpdateColumnComment(UpdateColumnComment {
            field_names,
            new_description: comment,
        })
    }

    /// impl the `update_column_position`.
    pub fn update_column_position(move_: Move) -> Self {
        SchemaChange::UpdateColumnPosition(UpdateColumnPosition::new(move_))
    }
}

/// The type of move.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L412>
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum MoveType {
    FIRST,
    AFTER,
    BEFORE,
    LAST,
}

/// Represents a requested column move in a struct.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L410>
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Move {
    field_name: String,
    referenced_field_name: Option<String>,
    #[serde(rename = "type")]
    move_type: MoveType,
}

impl Move {
    /// Create a new `Move`.
    pub fn new(
        field_name: String,
        referenced_field_name: Option<String>,
        move_type: MoveType,
    ) -> Self {
        Self {
            field_name,
            referenced_field_name,
            move_type,
        }
    }

    /// Get the field name.
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// Get the referenced field name.
    pub fn referenced_field_name(&self) -> Option<&str> {
        self.referenced_field_name.as_deref()
    }

    /// Get the move type.
    pub fn move_type(&self) -> &MoveType {
        &self.move_type
    }

    /// Create a new `Move` with `FIRST` move type.
    pub fn first(field_name: String) -> Self {
        Move {
            field_name,
            referenced_field_name: None,
            move_type: MoveType::FIRST,
        }
    }

    /// Create a new `Move` with `AFTER` move type.
    pub fn after(field_name: String, referenced_field_name: String) -> Self {
        Move {
            field_name,
            referenced_field_name: Some(referenced_field_name),
            move_type: MoveType::AFTER,
        }
    }

    /// Create a new `Move` with `BEFORE` move type.
    pub fn before(field_name: String, referenced_field_name: String) -> Self {
        Move {
            field_name,
            referenced_field_name: Some(referenced_field_name),
            move_type: MoveType::BEFORE,
        }
    }

    /// Create a new `Move` with `LAST` move type.
    pub fn last(field_name: String) -> Self {
        Move {
            field_name,
            referenced_field_name: None,
            move_type: MoveType::LAST,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_option() {
        let key = "option_key".to_string();
        let value = "option_value".to_string();
        let schema_change = SchemaChange::set_option(key.clone(), value.clone());

        if let SchemaChange::SetOption(set_option) = schema_change {
            assert_eq!(set_option.key(), key);
            assert_eq!(set_option.value(), value);
        } else {
            panic!("Expected SchemaChange::SetOption");
        }
    }

    #[test]
    fn test_remove_option() {
        let key = "option_key".to_string();
        let schema_change = SchemaChange::remove_option(key.clone());

        if let SchemaChange::RemoveOption(remove_option) = schema_change {
            assert_eq!(remove_option.key(), key);
        } else {
            panic!("Expected SchemaChange::RemoveOption");
        }
    }

    #[test]
    fn test_update_comment() {
        let comment = Some("New table comment".to_string());
        let schema_change = SchemaChange::update_comment(comment.clone());

        if let SchemaChange::UpdateComment(update_comment) = schema_change {
            assert_eq!(update_comment.comment(), comment.as_deref());
        } else {
            panic!("Expected SchemaChange::UpdateComment");
        }
    }

    #[test]
    fn test_add_column() {
        let field_name = "new_column".to_string();
        let data_type = DataTypeRoot::Integer;
        let schema_change = SchemaChange::add_column(field_name.clone(), data_type);

        if let SchemaChange::AddColumn(add_column) = schema_change {
            assert_eq!(add_column.field_name(), field_name);
            assert_eq!(add_column.data_type(), &data_type);
            assert_eq!(add_column.description(), None);
            assert_eq!(add_column.move_(), None);
        } else {
            panic!("Expected SchemaChange::AddColumn");
        }
    }

    #[test]
    fn test_add_column_with_comment() {
        let field_name = "new_column".to_string();
        let data_type = DataTypeRoot::Varchar;
        let comment = "This is a new column".to_string();
        let schema_change =
            SchemaChange::add_column_with_comment(field_name.clone(), data_type, comment.clone());

        if let SchemaChange::AddColumn(add_column) = schema_change {
            assert_eq!(add_column.field_name(), field_name);
            assert_eq!(add_column.data_type(), &data_type);
            assert_eq!(add_column.description(), Some(comment.as_str()));
            assert_eq!(add_column.move_(), None);
        } else {
            panic!("Expected SchemaChange::AddColumn");
        }
    }

    #[test]
    fn test_add_column_with_comment_and_move() {
        let field_name = "new_column".to_string();
        let data_type = DataTypeRoot::Double;
        let comment = "This is a new column".to_string();
        let move_ = Move::after(
            "existing_column".to_string(),
            "reference_column".to_string(),
        );
        let schema_change = SchemaChange::add_column_with_comment_and_move(
            field_name.clone(),
            data_type,
            comment.clone(),
            move_.clone(),
        );

        if let SchemaChange::AddColumn(add_column) = schema_change {
            assert_eq!(add_column.field_name(), field_name);
            assert_eq!(add_column.data_type(), &data_type);
            assert_eq!(add_column.description(), Some(comment.as_str()));
            assert_eq!(add_column.move_(), Some(&move_));
        } else {
            panic!("Expected SchemaChange::AddColumn");
        }
    }

    #[test]
    fn test_rename_column() {
        let old_name = "old_column".to_string();
        let new_name = "new_column".to_string();
        let schema_change = SchemaChange::rename_column(old_name.clone(), new_name.clone());

        if let SchemaChange::RenameColumn(rename_column) = schema_change {
            assert_eq!(rename_column.field_name(), old_name);
            assert_eq!(rename_column.new_name(), new_name);
        } else {
            panic!("Expected SchemaChange::RenameColumn");
        }
    }

    #[test]
    fn test_drop_column() {
        let field_name = "column_to_drop".to_string();
        let schema_change = SchemaChange::drop_column(field_name.clone());

        if let SchemaChange::DropColumn(drop_column) = schema_change {
            assert_eq!(drop_column.field_name(), field_name);
        } else {
            panic!("Expected SchemaChange::DropColumn");
        }
    }

    #[test]
    fn test_update_column_nullability() {
        let field_name = "column_to_update".to_string();
        let new_nullability = true;
        let schema_change =
            SchemaChange::update_column_nullability(field_name.clone(), new_nullability);

        if let SchemaChange::UpdateColumnNullability(update_nullability) = schema_change {
            assert_eq!(update_nullability.field_names(), &[field_name]);
            assert_eq!(update_nullability.new_nullability(), new_nullability);
        } else {
            panic!("Expected SchemaChange::UpdateColumnNullability");
        }
    }

    #[test]
    fn test_update_column_position() {
        let field_name = "column_to_move".to_string();
        let move_ = Move::first(field_name.clone());
        let schema_change = SchemaChange::update_column_position(move_.clone());

        if let SchemaChange::UpdateColumnPosition(update_position) = schema_change {
            assert_eq!(update_position.move_(), &move_);
        } else {
            panic!("Expected SchemaChange::UpdateColumnPosition");
        }
    }

    #[test]
    fn test_update_column_comment() {
        let field_name = "column1".to_string();
        let comment = "New comment".to_string();
        let schema_change =
            SchemaChange::update_column_comment(field_name.clone(), comment.clone());

        if let SchemaChange::UpdateColumnComment(update) = schema_change {
            assert_eq!(update.field_names(), &[field_name]);
            assert_eq!(update.new_description(), comment);
        } else {
            panic!("Expected SchemaChange::UpdateColumnComment");
        }
    }

    #[test]
    fn test_update_columns_comment() {
        let field_names = vec!["column1".to_string(), "column2".to_string()];
        let comment = "New comment".to_string();
        let schema_change =
            SchemaChange::update_columns_comment(field_names.clone(), comment.clone());

        if let SchemaChange::UpdateColumnComment(update) = schema_change {
            assert_eq!(update.field_names(), &field_names[..]);
            assert_eq!(update.new_description(), comment);
        } else {
            panic!("Expected SchemaChange::UpdateColumnComment");
        }
    }

    #[test]
    fn test_move_first() {
        let field_name = "column1".to_string();
        let move_ = Move::first(field_name.clone());

        assert_eq!(move_.field_name(), field_name);
        assert_eq!(move_.referenced_field_name(), None);
        assert_eq!(move_.move_type(), &MoveType::FIRST);
    }

    #[test]
    fn test_move_after() {
        let field_name = "column1".to_string();
        let referenced_field_name = "column2".to_string();
        let move_ = Move::after(field_name.clone(), referenced_field_name.clone());

        assert_eq!(move_.field_name(), field_name);
        assert_eq!(
            move_.referenced_field_name(),
            Some(referenced_field_name.as_str())
        );
        assert_eq!(move_.move_type(), &MoveType::AFTER);
    }

    #[test]
    fn test_move_before() {
        let field_name = "column1".to_string();
        let referenced_field_name = "column2".to_string();
        let move_ = Move::before(field_name.clone(), referenced_field_name.clone());

        assert_eq!(move_.field_name(), field_name);
        assert_eq!(
            move_.referenced_field_name(),
            Some(referenced_field_name.as_str())
        );
        assert_eq!(move_.move_type(), &MoveType::BEFORE);
    }

    #[test]
    fn test_move_last() {
        let field_name = "column1".to_string();
        let move_ = Move::last(field_name.clone());

        assert_eq!(move_.field_name(), field_name);
        assert_eq!(move_.referenced_field_name(), None);
        assert_eq!(move_.move_type(), &MoveType::LAST);
    }
}
