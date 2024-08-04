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

use crate::spec::DataType;
use serde::{Deserialize, Serialize};

/// Schema change to table.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L36>
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SchemaChange {
    /// A SchemaChange to set a table option.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L95>
    SetOption { key: String, value: String },
    /// A SchemaChange to remove a table option.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L134>
    RemoveOption { key: String },
    /// A SchemaChange to update a table comment.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L167>
    UpdateComment { comment: Option<String> },
    /// A SchemaChange to add a new field.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L201>
    AddColumn {
        field_name: String,
        data_type: DataType,
        description: Option<String>,
        #[serde(rename = "move")]
        column_move: Option<ColumnMove>,
    },
    /// A SchemaChange to rename a field.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L260>
    RenameColumn {
        field_name: String,
        new_name: String,
    },
    /// A SchemaChange to drop a field.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L302>
    DropColumn { field_name: String },
    /// A SchemaChange to update the field's type.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L335>
    UpdateColumnType {
        field_name: String,
        data_type: DataType,
    },
    /// A SchemaChange to update the field's position.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L377>
    UpdateColumnPosition {
        #[serde(rename = "move")]
        column_move: ColumnMove,
    },
    /// A SchemaChange to update the field's nullability.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L470>
    UpdateColumnNullability {
        field_name: Vec<String>,
        nullable: bool,
    },
    /// A SchemaChange to update the (nested) field's comment.
    ///
    /// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L512>
    UpdateColumnComment {
        field_names: Vec<String>,
        new_description: String,
    },
}

impl SchemaChange {
    /// impl the `set_option`.
    pub fn set_option(key: String, value: String) -> Self {
        SchemaChange::SetOption { key, value }
    }

    /// impl the `remove_option`.
    pub fn remove_option(key: String) -> Self {
        SchemaChange::RemoveOption { key }
    }

    /// impl the `update_comment`.
    pub fn update_comment(comment: Option<String>) -> Self {
        SchemaChange::UpdateComment { comment }
    }

    /// impl the `add_column`.
    pub fn add_column(field_name: String, data_type: DataType) -> Self {
        SchemaChange::AddColumn {
            field_name,
            data_type,
            description: None,
            column_move: None,
        }
    }

    /// impl the `add_column_with_description`.
    pub fn add_column_with_description(
        field_name: String,
        data_type: DataType,
        description: String,
    ) -> Self {
        SchemaChange::AddColumn {
            field_name,
            data_type,
            description: Some(description),
            column_move: None,
        }
    }

    /// impl the `add_column_with_description_and_column_move`.
    pub fn add_column_with_description_and_column_move(
        field_name: String,
        data_type: DataType,
        description: String,
        column_move: ColumnMove,
    ) -> Self {
        SchemaChange::AddColumn {
            field_name,
            data_type,
            description: Some(description),
            column_move: Some(column_move),
        }
    }

    /// impl the `rename_column`.
    pub fn rename_column(field_name: String, new_name: String) -> Self {
        SchemaChange::RenameColumn {
            field_name,
            new_name,
        }
    }

    /// impl the `drop_column`.
    pub fn drop_column(field_name: String) -> Self {
        SchemaChange::DropColumn { field_name }
    }

    /// impl the `update_column_type`.
    pub fn update_column_type(field_name: String, new_data_type: DataType) -> Self {
        SchemaChange::UpdateColumnType {
            field_name,
            data_type: new_data_type,
        }
    }

    /// impl the `update_column_position`.
    pub fn update_column_nullability(field_name: String, new_nullability: bool) -> Self {
        SchemaChange::UpdateColumnNullability {
            field_name: vec![field_name],
            nullable: new_nullability,
        }
    }

    /// impl the `update_columns_nullability`.
    pub fn update_columns_nullability(field_names: Vec<String>, new_nullability: bool) -> Self {
        SchemaChange::UpdateColumnNullability {
            field_name: field_names,
            nullable: new_nullability,
        }
    }

    /// impl the `update_column_comment`.
    pub fn update_column_comment(field_name: String, comment: String) -> Self {
        SchemaChange::UpdateColumnComment {
            field_names: vec![field_name],
            new_description: comment,
        }
    }

    /// impl the `update_columns_comment`.
    pub fn update_columns_comment(field_names: Vec<String>, comment: String) -> Self {
        SchemaChange::UpdateColumnComment {
            field_names,
            new_description: comment,
        }
    }

    /// impl the `update_column_position`.
    pub fn update_column_position(column_move: ColumnMove) -> Self {
        SchemaChange::UpdateColumnPosition { column_move }
    }
}

/// The type of move.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L412>
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum ColumnMoveType {
    FIRST,
    AFTER,
}

/// Represents a requested column move in a struct.
///
/// Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/schema/SchemaChange.java#L410>
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnMove {
    field_name: String,
    referenced_field_name: Option<String>,
    #[serde(rename = "type")]
    move_type: ColumnMoveType,
}

impl ColumnMove {
    /// Create a new `Move`.
    pub fn new(
        field_name: String,
        referenced_field_name: Option<String>,
        move_type: ColumnMoveType,
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
    pub fn move_type(&self) -> &ColumnMoveType {
        &self.move_type
    }

    /// Create a new `Move` with `FIRST` move type.
    pub fn first(field_name: String) -> Self {
        ColumnMove {
            field_name,
            referenced_field_name: None,
            move_type: ColumnMoveType::FIRST,
        }
    }

    /// Create a new `Move` with `AFTER` move type.
    pub fn after(field_name: String, referenced_field_name: String) -> Self {
        ColumnMove {
            field_name,
            referenced_field_name: Some(referenced_field_name),
            move_type: ColumnMoveType::AFTER,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataType, IntType};

    #[test]
    fn test_set_option() {
        let change = SchemaChange::set_option("key".to_string(), "value".to_string());
        if let SchemaChange::SetOption { key, value } = change {
            assert_eq!(key, "key");
            assert_eq!(value, "value");
        } else {
            panic!("Expected SetOption variant");
        }
    }

    #[test]
    fn test_remove_option() {
        let change = SchemaChange::remove_option("key".to_string());
        if let SchemaChange::RemoveOption { key } = change {
            assert_eq!(key, "key");
        } else {
            panic!("Expected RemoveOption variant");
        }
    }

    #[test]
    fn test_update_comment() {
        let change = SchemaChange::update_comment(Some("comment".to_string()));
        if let SchemaChange::UpdateComment { comment } = change {
            assert_eq!(comment, Some("comment".to_string()));
        } else {
            panic!("Expected UpdateComment variant");
        }
    }

    #[test]
    fn test_add_column() {
        let change = SchemaChange::add_column("field".to_string(), DataType::Int(IntType::new()));
        if let SchemaChange::AddColumn {
            field_name,
            data_type,
            description,
            column_move,
        } = change
        {
            assert_eq!(field_name, "field");
            assert_eq!(data_type, DataType::Int(IntType::new()));
            assert_eq!(description, None);
            assert_eq!(column_move, None);
        } else {
            panic!("Expected AddColumn variant");
        }
    }

    #[test]
    fn test_add_column_with_description() {
        let change = SchemaChange::add_column_with_description(
            "field".to_string(),
            DataType::Int(IntType::new()),
            "description".to_string(),
        );
        if let SchemaChange::AddColumn {
            field_name,
            data_type,
            description,
            column_move,
        } = change
        {
            assert_eq!(field_name, "field");
            assert_eq!(data_type, DataType::Int(IntType::new()));
            assert_eq!(description, Some("description".to_string()));
            assert_eq!(column_move, None);
        } else {
            panic!("Expected AddColumn variant");
        }
    }

    #[test]
    fn test_add_column_with_description_and_move() {
        let column_move = ColumnMove::first("field".to_string());
        let change = SchemaChange::add_column_with_description_and_column_move(
            "field".to_string(),
            DataType::Int(IntType::new()),
            "description".to_string(),
            column_move.clone(),
        );
        if let SchemaChange::AddColumn {
            field_name,
            data_type,
            description,
            column_move: move_,
        } = change
        {
            assert_eq!(field_name, "field");
            assert_eq!(data_type, DataType::Int(IntType::new()));
            assert_eq!(description, Some("description".to_string()));
            assert_eq!(move_, Some(column_move));
        } else {
            panic!("Expected AddColumn variant");
        }
    }

    #[test]
    fn test_rename_column() {
        let change = SchemaChange::rename_column("old".to_string(), "new".to_string());
        if let SchemaChange::RenameColumn {
            field_name,
            new_name,
        } = change
        {
            assert_eq!(field_name, "old");
            assert_eq!(new_name, "new");
        } else {
            panic!("Expected RenameColumn variant");
        }
    }

    #[test]
    fn test_drop_column() {
        let change = SchemaChange::drop_column("field".to_string());
        if let SchemaChange::DropColumn { field_name } = change {
            assert_eq!(field_name, "field");
        } else {
            panic!("Expected DropColumn variant");
        }
    }

    #[test]
    fn test_update_column_type() {
        let change =
            SchemaChange::update_column_type("field".to_string(), DataType::Int(IntType::new()));
        if let SchemaChange::UpdateColumnType {
            field_name,
            data_type,
        } = change
        {
            assert_eq!(field_name, "field");
            assert_eq!(data_type, DataType::Int(IntType::new()));
        } else {
            panic!("Expected UpdateColumnType variant");
        }
    }

    #[test]
    fn test_update_column_nullability() {
        let change = SchemaChange::update_column_nullability("field".to_string(), true);
        if let SchemaChange::UpdateColumnNullability {
            field_name,
            nullable,
        } = change
        {
            assert_eq!(field_name, vec!["field"]);
            assert!(nullable);
        } else {
            panic!("Expected UpdateColumnNullability variant");
        }
    }

    #[test]
    #[allow(clippy::bool_assert_comparison)]
    fn test_update_columns_nullability() {
        let change = SchemaChange::update_columns_nullability(
            vec!["field1".to_string(), "field2".to_string()],
            false,
        );
        if let SchemaChange::UpdateColumnNullability {
            field_name,
            nullable,
        } = change
        {
            assert_eq!(field_name, vec!["field1", "field2"]);
            assert_eq!(nullable, false);
        } else {
            panic!("Expected UpdateColumnNullability variant");
        }
    }

    #[test]
    fn test_update_column_comment() {
        let change =
            SchemaChange::update_column_comment("field".to_string(), "comment".to_string());
        if let SchemaChange::UpdateColumnComment {
            field_names,
            new_description,
        } = change
        {
            assert_eq!(field_names, vec!["field"]);
            assert_eq!(new_description, "comment");
        } else {
            panic!("Expected UpdateColumnComment variant");
        }
    }

    #[test]
    fn test_update_columns_comment() {
        let change = SchemaChange::update_columns_comment(
            vec!["field1".to_string(), "field2".to_string()],
            "comment".to_string(),
        );
        if let SchemaChange::UpdateColumnComment {
            field_names,
            new_description,
        } = change
        {
            assert_eq!(field_names, vec!["field1", "field2"]);
            assert_eq!(new_description, "comment");
        } else {
            panic!("Expected UpdateColumnComment variant");
        }
    }

    #[test]
    fn test_update_column_position() {
        let column_move = ColumnMove::first("field".to_string());
        let change = SchemaChange::update_column_position(column_move.clone());
        if let SchemaChange::UpdateColumnPosition { column_move: move_ } = change {
            assert_eq!(move_, column_move);
        } else {
            panic!("Expected UpdateColumnPosition variant");
        }
    }

    #[test]
    fn test_move_first() {
        let field_name = "column1".to_string();
        let move_ = ColumnMove::first(field_name.clone());

        assert_eq!(move_.field_name(), field_name);
        assert_eq!(move_.referenced_field_name(), None);
        assert_eq!(move_.move_type(), &ColumnMoveType::FIRST);
    }

    #[test]
    fn test_move_after() {
        let field_name = "column1".to_string();
        let referenced_field_name = "column2".to_string();
        let move_ = ColumnMove::after(field_name.clone(), referenced_field_name.clone());

        assert_eq!(move_.field_name(), field_name);
        assert_eq!(
            move_.referenced_field_name(),
            Some(referenced_field_name.as_str())
        );
        assert_eq!(move_.move_type(), &ColumnMoveType::AFTER);
    }
}
