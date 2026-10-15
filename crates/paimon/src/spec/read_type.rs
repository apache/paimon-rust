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

//! Resolve nested reader requests while preserving ROW structure and field IDs.

use crate::spec::{
    map_selected_keys_field, DataField, DataType, RowType, MAP_SELECTED_KEYS_METADATA_KEY,
};
use crate::{Error, Result};
const MAP_KEYS_DELIMITER: char = ';';
fn invalid(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Resolve flat output paths to the authoritative nested read type used by the
/// core reader. ROW children are pruned recursively. Selected MAP keys use
/// Java's temporary ROW field metadata so shared-shredding Parquet files can
/// prune physical columns.
pub fn project_read_type(fields: &[DataField], paths: &[Vec<String>]) -> Result<Vec<DataField>> {
    if paths.iter().any(Vec::is_empty) {
        return Err(invalid("nested projection paths must not be empty"));
    }

    let mut result = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for path in paths {
        let name = &path[0];
        if !seen.insert(name.clone()) {
            continue;
        }
        let field = fields
            .iter()
            .find(|field| field.name() == name)
            .ok_or_else(|| {
                invalid(format!(
                    "nested projection field '{}' does not exist",
                    path.join(".")
                ))
            })?;
        let matching: Vec<&[String]> = paths
            .iter()
            .filter(|candidate| candidate.first() == Some(name))
            .map(|candidate| candidate[1..].as_ref())
            .collect();
        result.push(project_nested_field(field, &matching, name)?);
    }
    Ok(result)
}

fn project_nested_field(
    field: &DataField,
    tails: &[&[String]],
    full_name: &str,
) -> Result<DataField> {
    if tails.iter().any(|tail| tail.is_empty()) {
        return Ok(field.clone());
    }
    match field.data_type() {
        DataType::Row(row) => {
            let child_paths: Vec<Vec<String>> = tails.iter().map(|tail| tail.to_vec()).collect();
            let children = project_read_type(row.fields(), &child_paths).map_err(|_| {
                invalid(format!(
                    "nested projection field '{}' does not exist",
                    tails
                        .first()
                        .map(|tail| format!("{full_name}.{}", tail.join(".")))
                        .unwrap_or_else(|| full_name.to_string())
                ))
            })?;
            Ok(DataField::new(
                field.id(),
                field.name().to_string(),
                DataType::Row(RowType::with_nullable(
                    field.data_type().is_nullable(),
                    children,
                )),
            )
            .with_description(field.description().map(str::to_string))
            .with_default_value(field.default_value().map(str::to_string)))
        }
        DataType::Map(_) if tails.iter().all(|tail| tail.len() == 1) => {
            let mut keys = Vec::new();
            for tail in tails {
                let key = &tail[0];
                if key.contains(MAP_KEYS_DELIMITER)
                    || key.starts_with(MAP_SELECTED_KEYS_METADATA_KEY)
                {
                    // Keep the complete MAP for keys which cannot be encoded
                    // by the cross-language selected-key convention.
                    return Ok(field.clone());
                }
                if !keys.contains(key) {
                    keys.push(key.clone());
                }
            }
            map_selected_keys_field(field, &keys)
        }
        _ => Err(invalid(format!(
            "nested projection field '{}' is not a ROW or MAP",
            tails
                .first()
                .map(|tail| format!("{full_name}.{}", tail.join(".")))
                .unwrap_or_else(|| full_name.to_string())
        ))),
    }
}

/// Validate synthetic MAP readers against the current logical schema, before
/// historical physical value types are reconciled. Java selected children
/// retain the MAP value type (apart from nullability), including all ROW IDs.
pub(crate) fn validate_selected_map_fields(
    source: &[DataField],
    requested: &[DataField],
) -> Result<()> {
    for target in requested {
        let Some(field) = source.iter().find(|field| field.id() == target.id()) else {
            continue;
        };
        if crate::spec::is_map_selected_keys_field(target) {
            let (DataType::Map(map), DataType::Row(row)) = (field.data_type(), target.data_type())
            else {
                return Err(invalid("Selected-key ROW requires a MAP source field"));
            };
            crate::spec::map_selected_keys(target)?;
            if !matches!(map.key_type(), DataType::VarChar(_))
                || row
                    .fields()
                    .iter()
                    .any(|child| !map.value_type().equals_ignore_nullable(child.data_type()))
            {
                return Err(invalid(format!(
                    "Selected-key ROW '{}' must retain the STRING MAP's complete value type",
                    target.name()
                )));
            }
        } else if let (DataType::Row(source_row), DataType::Row(target_row)) =
            (field.data_type(), target.data_type())
        {
            validate_selected_map_fields(source_row.fields(), target_row.fields())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{IntType, MapType, VarCharType};

    fn fields() -> Vec<DataField> {
        vec![
            DataField::new(
                10,
                "profile".into(),
                DataType::Row(RowType::new(vec![
                    DataField::new(11, "score".into(), DataType::Int(IntType::new()))
                        .with_default_value(Some("7".into())),
                    DataField::new(
                        12,
                        "attrs".into(),
                        DataType::Map(MapType::new(
                            DataType::VarChar(VarCharType::string_type()),
                            DataType::Int(IntType::new()),
                        )),
                    ),
                ])),
            )
            .with_description(Some("profile description".into())),
            DataField::new(20, "id".into(), DataType::Int(IntType::new())),
        ]
    }

    fn path(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|part| (*part).into()).collect()
    }

    #[test]
    fn structured_projection_preserves_ids_metadata_and_key_order() {
        let read = project_read_type(
            &fields(),
            &[
                path(&["profile", "attrs", ""]),
                path(&["profile", "score"]),
                path(&["profile", "attrs", "missing"]),
                path(&["profile", "attrs", ""]),
                path(&["id"]),
            ],
        )
        .unwrap();
        assert_eq!(read[0].id(), 10);
        assert_eq!(read[0].description(), Some("profile description"));
        let DataType::Row(row) = read[0].data_type() else {
            panic!()
        };
        assert_eq!(row.fields()[0].id(), 12);
        assert_eq!(
            crate::spec::map_selected_keys(&row.fields()[0]).unwrap(),
            Some(vec!["".into(), "missing".into()])
        );
        assert_eq!(row.fields()[1].id(), 11);
        assert_eq!(row.fields()[1].default_value(), Some("7"));
        assert_eq!(read[1].id(), 20);
    }

    #[test]
    fn whole_fields_win_and_unencodable_keys_read_complete_map() {
        let fields = fields();
        assert_eq!(
            project_read_type(&fields, &[path(&["profile", "score"]), path(&["profile"])]).unwrap(),
            fields[..1]
        );
        let read = project_read_type(&fields, &[path(&["profile", "attrs", "a;b"])]).unwrap();
        let DataType::Row(row) = read[0].data_type() else {
            panic!()
        };
        assert!(matches!(row.fields()[0].data_type(), DataType::Map(_)));
        assert!(project_read_type(&fields, &[Vec::new()]).is_err());
        assert!(project_read_type(&fields, &[path(&["profile", "unknown"])]).is_err());
        assert!(project_read_type(&fields, &[]).unwrap().is_empty());
    }
}
