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
    pub fn update_column_position(column_move: ColumnMove) -> Self {
        SchemaChange::UpdateColumnPosition { column_move }
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
    pub field_name: String,
    pub referenced_field_name: Option<String>,
    #[serde(rename = "type")]
    pub move_type: ColumnMoveType,
}

impl ColumnMove {
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
    pub fn move_first(field_name: String) -> Self {
        ColumnMove {
            field_name,
            referenced_field_name: None,
            move_type: ColumnMoveType::FIRST,
        }
    }

    /// Create a new `Move` with `AFTER` move type.
    pub fn move_after(field_name: String, referenced_field_name: String) -> Self {
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
    use crate::spec::{DoubleType, IntType};

    #[test]
    fn test_schema_change_serialize_deserialize() {
        // SchemaChange: SetOption
        let schema_change =
            SchemaChange::set_option("snapshot.time-retained".to_string(), "2h".to_string());
        let json = serde_json::to_string(&schema_change).unwrap();
        let schema_change = serde_json::from_str::<SchemaChange>(&json).unwrap();
        assert_eq!(
            schema_change,
            SchemaChange::set_option("snapshot.time-retained".to_string(), "2h".to_string())
        );

        // SchemaChange: RemoveOption
        let schema_change = SchemaChange::remove_option("compaction.max.file-num".to_string());
        let json = serde_json::to_string(&schema_change).unwrap();
        let schema_change = serde_json::from_str::<SchemaChange>(&json).unwrap();
        assert_eq!(
            schema_change,
            SchemaChange::remove_option("compaction.max.file-num".to_string())
        );

        // SchemaChange: UpdateComment
        let schema_change = SchemaChange::update_comment(Some("table.comment".to_string()));
        let json = serde_json::to_string(&schema_change).unwrap();
        let schema_change = serde_json::from_str::<SchemaChange>(&json).unwrap();
        assert_eq!(
            schema_change,
            SchemaChange::update_comment(Some("table.comment".to_string()))
        );

        // SchemaChange: AddColumn
        let schema_change =
            SchemaChange::add_column("col1".to_string(), DataType::Int(IntType::new()));
        let json = serde_json::to_string(&schema_change).unwrap();
        let schema_change = serde_json::from_str::<SchemaChange>(&json).unwrap();
        assert_eq!(
            schema_change,
            SchemaChange::add_column("col1".to_string(), DataType::Int(IntType::new()))
        );

        // SchemaChange: AddColumn with description
        let schema_change = SchemaChange::add_column_with_description(
            "col1".to_string(),
            DataType::Int(IntType::new()),
            "col1_description".to_string(),
        );
        let json = serde_json::to_string(&schema_change).unwrap();
        let schema_change = serde_json::from_str::<SchemaChange>(&json).unwrap();
        assert_eq!(
            schema_change,
            SchemaChange::add_column_with_description(
                "col1".to_string(),
                DataType::Int(IntType::new()),
                "col1_description".to_string(),
            )
        );

        // SchemaChange: AddColumn with description and column_move
        let schema_change = SchemaChange::add_column_with_description_and_column_move(
            "col1".to_string(),
            DataType::Int(IntType::new()),
            "col1_description".to_string(),
            ColumnMove::move_first("col1_first".to_string()),
        );
        let json = serde_json::to_string(&schema_change).unwrap();
        let schema_change = serde_json::from_str::<SchemaChange>(&json).unwrap();
        assert_eq!(
            schema_change,
            SchemaChange::add_column_with_description_and_column_move(
                "col1".to_string(),
                DataType::Int(IntType::new()),
                "col1_description".to_string(),
                ColumnMove::move_first("col1_first".to_string()),
            )
        );

        // SchemaChange: RenameColumn
        let schema_change =
            SchemaChange::rename_column("col3".to_string(), "col3_new_name".to_string());
        let json = serde_json::to_string(&schema_change).unwrap();
        let schema_change = serde_json::from_str::<SchemaChange>(&json).unwrap();
        assert_eq!(
            schema_change,
            SchemaChange::rename_column("col3".to_string(), "col3_new_name".to_string())
        );

        // SchemaChange: DropColumn
        let schema_change = SchemaChange::drop_column("col1".to_string());
        let json = serde_json::to_string(&schema_change).unwrap();
        let schema_change = serde_json::from_str::<SchemaChange>(&json).unwrap();
        assert_eq!(schema_change, SchemaChange::drop_column("col1".to_string()));

        // SchemaChange: UpdateColumnType
        let schema_change = SchemaChange::update_column_type(
            "col14".to_string(),
            DataType::Double(DoubleType::new()),
        );
        let json = serde_json::to_string(&schema_change).unwrap();
        let schema_change = serde_json::from_str::<SchemaChange>(&json).unwrap();
        assert_eq!(
            schema_change,
            SchemaChange::update_column_type(
                "col14".to_string(),
                DataType::Double(DoubleType::new()),
            )
        );

        // SchemaChange: UpdateColumnPosition
        let schema_change =
            SchemaChange::update_column_position(ColumnMove::move_first("col4_first".to_string()));
        let json = serde_json::to_string(&schema_change).unwrap();
        let schema_change = serde_json::from_str::<SchemaChange>(&json).unwrap();
        assert_eq!(
            schema_change,
            SchemaChange::update_column_position(ColumnMove::move_first("col4_first".to_string()))
        );

        // SchemaChange: UpdateColumnNullability
        let schema_change = SchemaChange::update_column_nullability("col4".to_string(), false);
        let json = serde_json::to_string(&schema_change).unwrap();
        let schema_change = serde_json::from_str::<SchemaChange>(&json).unwrap();
        assert_eq!(
            schema_change,
            SchemaChange::update_column_nullability("col4".to_string(), false)
        );

        // SchemaChange: UpdateColumnsNullability
        let schema_change = SchemaChange::update_columns_nullability(
            vec!["col5".to_string(), "f2".to_string()],
            false,
        );
        let json = serde_json::to_string(&schema_change).unwrap();
        let schema_change = serde_json::from_str::<SchemaChange>(&json).unwrap();
        assert_eq!(
            schema_change,
            SchemaChange::update_columns_nullability(
                vec!["col5".to_string(), "f2".to_string()],
                false
            )
        );

        // SchemaChange: UpdateColumnComment
        let schema_change =
            SchemaChange::update_column_comment("col4".to_string(), "col4 field".to_string());
        let json = serde_json::to_string(&schema_change).unwrap();
        let schema_change = serde_json::from_str::<SchemaChange>(&json).unwrap();
        assert_eq!(
            schema_change,
            SchemaChange::update_column_comment("col4".to_string(), "col4 field".to_string())
        );
        // SchemaChange: UpdateColumnsComment
        let schema_change = SchemaChange::update_columns_comment(
            vec!["col5".to_string(), "f1".to_string()],
            "col5 f1 field".to_string(),
        );
        let json = serde_json::to_string(&schema_change).unwrap();
        let schema_change = serde_json::from_str::<SchemaChange>(&json).unwrap();
        assert_eq!(
            schema_change,
            SchemaChange::update_columns_comment(
                vec!["col5".to_string(), "f1".to_string()],
                "col5 f1 field".to_string()
            )
        );
    }

    #[test]
    fn test_column_move_serialize_deserialize() {
        // ColumnMoveType: FIRST
        let column_move = ColumnMove::move_first("col1".to_string());
        let json = serde_json::to_string(&column_move).unwrap();
        let column_move = serde_json::from_str::<ColumnMove>(&json).unwrap();
        assert_eq!(column_move.field_name(), "col1");
        assert_eq!(column_move.referenced_field_name(), None);
        assert_eq!(column_move.move_type(), &ColumnMoveType::FIRST);
        // ColumnMoveType: AFTER
        let column_move = ColumnMove::move_after("col2_after".to_string(), "col2".to_string());
        let json = serde_json::to_string(&column_move).unwrap();
        let column_move = serde_json::from_str::<ColumnMove>(&json).unwrap();
        assert_eq!(column_move.field_name(), "col2_after");
        assert_eq!(column_move.referenced_field_name(), Some("col2"));
        assert_eq!(column_move.move_type(), &ColumnMoveType::AFTER);
    }
}
