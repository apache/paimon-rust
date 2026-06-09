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
/// The JSON wire format is kept compatible with Java Paimon's `SchemaChange`,
/// which is an internally-tagged polymorphic type (`@JsonTypeInfo` with an
/// `"action"` discriminator). Each variant therefore serializes as
/// `{"action": "<name>", ...fields}` with `fieldNames` arrays (a column path;
/// only top-level single-element paths are currently applied — see
/// `TableSchema::apply_changes`).
///
/// Reference: <https://github.com/apache/paimon/blob/master/paimon-api/src/main/java/org/apache/paimon/schema/SchemaChange.java>
//
// Note: `dropPrimaryKey` and `updateColumnDefaultValue` from Java are not yet
// modeled here; they are out of scope for the current alter-table support.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "camelCase")]
pub enum SchemaChange {
    /// A SchemaChange to set a table option.
    SetOption { key: String, value: String },
    /// A SchemaChange to remove a table option.
    RemoveOption { key: String },
    /// A SchemaChange to update a table comment.
    UpdateComment { comment: Option<String> },
    /// A SchemaChange to add a new field.
    #[serde(rename_all = "camelCase")]
    AddColumn {
        field_names: Vec<String>,
        data_type: DataType,
        comment: Option<String>,
        #[serde(rename = "move")]
        column_move: Option<ColumnMove>,
    },
    /// A SchemaChange to rename a field.
    #[serde(rename_all = "camelCase")]
    RenameColumn {
        field_names: Vec<String>,
        new_name: String,
    },
    /// A SchemaChange to drop a field.
    #[serde(rename_all = "camelCase")]
    DropColumn { field_names: Vec<String> },
    /// A SchemaChange to update the field's type.
    #[serde(rename_all = "camelCase")]
    UpdateColumnType {
        field_names: Vec<String>,
        new_data_type: DataType,
        /// When true, keep the existing column's nullability instead of taking
        /// it from `new_data_type`.
        #[serde(default)]
        keep_nullability: bool,
    },
    /// A SchemaChange to update the field's nullability.
    #[serde(rename_all = "camelCase")]
    UpdateColumnNullability {
        field_names: Vec<String>,
        new_nullability: bool,
    },
    /// A SchemaChange to update the (nested) field's comment.
    #[serde(rename_all = "camelCase")]
    UpdateColumnComment {
        field_names: Vec<String>,
        new_comment: String,
    },
    /// A SchemaChange to update the field's position.
    #[serde(rename_all = "camelCase")]
    UpdateColumnPosition {
        #[serde(rename = "move")]
        column_move: ColumnMove,
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
            field_names: vec![field_name],
            data_type,
            comment: None,
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
            field_names: vec![field_name],
            data_type,
            comment: Some(description),
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
            field_names: vec![field_name],
            data_type,
            comment: Some(description),
            column_move: Some(column_move),
        }
    }

    /// impl the `rename_column`.
    pub fn rename_column(field_name: String, new_name: String) -> Self {
        SchemaChange::RenameColumn {
            field_names: vec![field_name],
            new_name,
        }
    }

    /// impl the `drop_column`.
    pub fn drop_column(field_name: String) -> Self {
        SchemaChange::DropColumn {
            field_names: vec![field_name],
        }
    }

    /// impl the `update_column_type`.
    pub fn update_column_type(field_name: String, new_data_type: DataType) -> Self {
        SchemaChange::UpdateColumnType {
            field_names: vec![field_name],
            new_data_type,
            keep_nullability: false,
        }
    }

    /// impl the `update_column_position`.
    pub fn update_column_position(column_move: ColumnMove) -> Self {
        SchemaChange::UpdateColumnPosition { column_move }
    }

    /// impl the `update_column_nullability`.
    pub fn update_column_nullability(field_name: String, new_nullability: bool) -> Self {
        SchemaChange::UpdateColumnNullability {
            field_names: vec![field_name],
            new_nullability,
        }
    }

    /// impl the `update_columns_nullability`.
    pub fn update_columns_nullability(field_names: Vec<String>, new_nullability: bool) -> Self {
        SchemaChange::UpdateColumnNullability {
            field_names,
            new_nullability,
        }
    }

    /// impl the `update_column_comment`.
    pub fn update_column_comment(field_name: String, comment: String) -> Self {
        SchemaChange::UpdateColumnComment {
            field_names: vec![field_name],
            new_comment: comment,
        }
    }

    /// impl the `update_columns_comment`.
    pub fn update_columns_comment(field_names: Vec<String>, comment: String) -> Self {
        SchemaChange::UpdateColumnComment {
            field_names,
            new_comment: comment,
        }
    }
}

/// The type of move.
///
/// Reference: <https://github.com/apache/paimon/blob/master/paimon-api/src/main/java/org/apache/paimon/schema/SchemaChange.java>
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum ColumnMoveType {
    FIRST,
    AFTER,
    BEFORE,
    LAST,
}

/// Represents a requested column move in a struct.
///
/// Reference: <https://github.com/apache/paimon/blob/master/paimon-api/src/main/java/org/apache/paimon/schema/SchemaChange.java>
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnMove {
    pub field_name: String,
    /// The anchor column for `AFTER`/`BEFORE` moves (`None` for `FIRST`/`LAST`).
    /// Named `referenceFieldName` on the wire to match Java Paimon.
    pub reference_field_name: Option<String>,
    #[serde(rename = "type")]
    pub move_type: ColumnMoveType,
}

impl ColumnMove {
    /// Get the field name.
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// Get the reference field name.
    pub fn reference_field_name(&self) -> Option<&str> {
        self.reference_field_name.as_deref()
    }

    /// Get the move type.
    pub fn move_type(&self) -> &ColumnMoveType {
        &self.move_type
    }

    /// Create a new `Move` with `FIRST` move type.
    pub fn move_first(field_name: String) -> Self {
        ColumnMove {
            field_name,
            reference_field_name: None,
            move_type: ColumnMoveType::FIRST,
        }
    }

    /// Create a new `Move` with `LAST` move type.
    pub fn move_last(field_name: String) -> Self {
        ColumnMove {
            field_name,
            reference_field_name: None,
            move_type: ColumnMoveType::LAST,
        }
    }

    /// Create a new `Move` with `AFTER` move type.
    pub fn move_after(field_name: String, reference_field_name: String) -> Self {
        ColumnMove {
            field_name,
            reference_field_name: Some(reference_field_name),
            move_type: ColumnMoveType::AFTER,
        }
    }

    /// Create a new `Move` with `BEFORE` move type.
    pub fn move_before(field_name: String, reference_field_name: String) -> Self {
        ColumnMove {
            field_name,
            reference_field_name: Some(reference_field_name),
            move_type: ColumnMoveType::BEFORE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DoubleType, IntType};

    #[test]
    fn test_schema_change_serialize_deserialize() {
        // Java-compatible wire format: internally tagged by "action", with
        // `fieldNames` arrays and `referenceFieldName` move anchors.
        let json_data = r#"
        [
          {
            "action": "setOption",
            "key": "snapshot.time-retained",
            "value": "2h"
          },
          {
            "action": "removeOption",
            "key": "compaction.max.file-num"
          },
          {
            "action": "updateComment",
            "comment": "table.comment"
          },
          {
            "action": "addColumn",
            "fieldNames": ["col1"],
            "dataType": "INT",
            "comment": "col1_description",
            "move": {
              "fieldName": "col1",
              "referenceFieldName": null,
              "type": "FIRST"
            }
          },
          {
            "action": "renameColumn",
            "fieldNames": ["col3"],
            "newName": "col3_new_name"
          },
          {
            "action": "dropColumn",
            "fieldNames": ["col1"]
          },
          {
            "action": "updateColumnType",
            "fieldNames": ["col14"],
            "newDataType": "DOUBLE",
            "keepNullability": false
          },
          {
            "action": "updateColumnPosition",
            "move": {
              "fieldName": "col4",
              "referenceFieldName": "col3",
              "type": "AFTER"
            }
          },
          {
            "action": "updateColumnNullability",
            "fieldNames": ["col5", "f2"],
            "newNullability": false
          },
          {
            "action": "updateColumnComment",
            "fieldNames": ["col5", "f1"],
            "newComment": "col5 f1 field"
          }
        ]"#;

        let schema_changes: Vec<SchemaChange> =
            serde_json::from_str(json_data).expect("Failed to deserialize SchemaChange.");

        assert_eq!(
            schema_changes,
            vec![
                SchemaChange::SetOption {
                    key: "snapshot.time-retained".to_string(),
                    value: "2h".to_string(),
                },
                SchemaChange::RemoveOption {
                    key: "compaction.max.file-num".to_string(),
                },
                SchemaChange::UpdateComment {
                    comment: Some("table.comment".to_string()),
                },
                SchemaChange::AddColumn {
                    field_names: vec!["col1".to_string()],
                    data_type: DataType::Int(IntType::new()),
                    comment: Some("col1_description".to_string()),
                    column_move: Some(ColumnMove::move_first("col1".to_string())),
                },
                SchemaChange::RenameColumn {
                    field_names: vec!["col3".to_string()],
                    new_name: "col3_new_name".to_string(),
                },
                SchemaChange::DropColumn {
                    field_names: vec!["col1".to_string()],
                },
                SchemaChange::UpdateColumnType {
                    field_names: vec!["col14".to_string()],
                    new_data_type: DataType::Double(DoubleType::new()),
                    keep_nullability: false,
                },
                SchemaChange::UpdateColumnPosition {
                    column_move: ColumnMove::move_after("col4".to_string(), "col3".to_string()),
                },
                SchemaChange::UpdateColumnNullability {
                    field_names: vec!["col5".to_string(), "f2".to_string()],
                    new_nullability: false,
                },
                SchemaChange::UpdateColumnComment {
                    field_names: vec!["col5".to_string(), "f1".to_string()],
                    new_comment: "col5 f1 field".to_string(),
                },
            ]
        );
    }

    #[test]
    fn test_schema_change_serialize_shape() {
        // Verify the serialized JSON carries the Java "action" discriminator.
        let change = SchemaChange::add_column("c".to_string(), DataType::Int(IntType::new()));
        let value = serde_json::to_value(&change).unwrap();
        assert_eq!(value["action"], "addColumn");
        assert_eq!(value["fieldNames"][0], "c");

        // Round-trip through JSON.
        let round: SchemaChange = serde_json::from_value(value).unwrap();
        assert_eq!(round, change);
    }

    #[test]
    fn test_column_move_serialize_deserialize() {
        let json_data = r#"
        [
          {
            "fieldName": "col1",
            "referenceFieldName": null,
            "type": "FIRST"
          },
          {
            "fieldName": "col2_after",
            "referenceFieldName": "col2",
            "type": "AFTER"
          }
        ]"#;

        let column_moves: Vec<ColumnMove> = serde_json::from_str(json_data).unwrap();
        assert_eq!(
            column_moves,
            vec![
                ColumnMove::move_first("col1".to_string()),
                ColumnMove::move_after("col2_after".to_string(), "col2".to_string()),
            ]
        );
    }
}
