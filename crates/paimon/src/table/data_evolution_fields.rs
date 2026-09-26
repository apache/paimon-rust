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

//! Physical field selection for data-evolution `write_cols`.
//!
//! Java `RowType.projectByPaths` preserves the order of the first occurrence
//! of each selected field. A top-level name takes precedence over splitting at
//! a dot, because older schemas can contain literal dotted column names.

use crate::spec::{DataField, DataType, RowType};
use crate::{Error, Result};
use indexmap::IndexMap;
use std::collections::HashSet;

#[derive(Default)]
struct PathSelection {
    whole: bool,
    tails: Vec<String>,
}

pub(super) fn project_by_paths(fields: &[DataField], paths: &[String]) -> Result<Vec<DataField>> {
    let mut selected: IndexMap<String, PathSelection> = IndexMap::new();
    for path in paths {
        if fields.iter().any(|field| field.name() == path) {
            selected.entry(path.clone()).or_default().whole = true;
            continue;
        }
        let Some((head, tail)) = path.split_once('.') else {
            return Err(unknown_field(path));
        };
        if head.is_empty() || tail.is_empty() {
            return Err(unknown_field(path));
        }
        selected
            .entry(head.to_string())
            .or_default()
            .tails
            .push(tail.to_string());
    }

    selected
        .into_iter()
        .map(|(name, selection)| {
            let field = fields
                .iter()
                .find(|field| field.name() == name)
                .ok_or_else(|| unknown_field(&name))?;
            if selection.whole || selection.tails.is_empty() {
                return Ok(field.clone());
            }
            let DataType::Row(row) = field.data_type() else {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Cannot project nested write path(s) {:?} from non-ROW field '{name}'",
                        selection.tails
                    ),
                    source: None,
                });
            };
            let projected = project_by_paths(row.fields(), &selection.tails)?;
            Ok(field_with_type(
                field,
                DataType::Row(RowType::with_nullable(
                    field.data_type().is_nullable(),
                    projected,
                )),
            ))
        })
        .collect()
}

/// Java `collectLeafPaths` only emits a top-level field or one direct child
/// of a ROW. A deeper partial ROW could be decoded but cannot be assembled
/// from independently written files, so reject it before creating data files.
pub(super) fn validate_write_paths(fields: &[DataField], paths: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
    let whole = paths
        .iter()
        .filter(|path| fields.iter().any(|field| field.name() == path.as_str()))
        .map(String::as_str)
        .collect::<HashSet<_>>();
    for path in paths {
        if !seen.insert(path.as_str()) {
            return Err(Error::DataInvalid {
                message: format!("Duplicate data-evolution write path '{path}'"),
                source: None,
            });
        }
        if whole.contains(path.as_str()) {
            continue;
        }
        let Some((head, child)) = path.split_once('.') else {
            return Err(unknown_field(path));
        };
        if child.contains('.') {
            return Err(Error::Unsupported {
                message: format!(
                    "Sub-field-level data evolution supports only one level of partial nesting: '{path}'"
                ),
            });
        }
        if whole.contains(head) {
            return Err(Error::DataInvalid {
                message: format!("Cannot write both whole ROW '{head}' and its sub-field '{path}'"),
                source: None,
            });
        }
        let parent = fields
            .iter()
            .find(|field| field.name() == head)
            .ok_or_else(|| unknown_field(path))?;
        let DataType::Row(row) = parent.data_type() else {
            return Err(Error::DataInvalid {
                message: format!("Cannot write nested path '{path}' from non-ROW '{head}'"),
                source: None,
            });
        };
        if !row.fields().iter().any(|field| field.name() == child) {
            return Err(unknown_field(path));
        }
    }
    Ok(())
}

pub(super) fn field_with_type(field: &DataField, data_type: DataType) -> DataField {
    DataField::new(field.id(), field.name().to_string(), data_type)
        .with_description(field.description().map(str::to_string))
        .with_default_value(field.default_value().map(str::to_string))
}

/// Field identities for row-id conflict detection. A whole ROW write covers
/// every leaf, while a nested write covers only its selected descendants.
pub(super) fn write_leaf_ids(
    fields: &[DataField],
    paths: Option<&[String]>,
) -> Result<HashSet<i32>> {
    let selected = match paths {
        Some(paths) => project_by_paths(fields, paths)?,
        None => fields.to_vec(),
    };
    let mut ids = HashSet::new();
    collect_leaf_ids(&selected, &mut ids);
    Ok(ids)
}

fn collect_leaf_ids(fields: &[DataField], ids: &mut HashSet<i32>) {
    for field in fields {
        if let DataType::Row(row) = field.data_type() {
            collect_leaf_ids(row.fields(), ids);
        } else {
            ids.insert(field.id());
        }
    }
}

fn unknown_field(path: &str) -> Error {
    Error::DataInvalid {
        message: format!("Unknown data-evolution write path '{path}'"),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{IntType, VarCharType};

    fn field(id: i32, name: &str, data_type: DataType) -> DataField {
        DataField::new(id, name.to_string(), data_type)
    }

    fn fields() -> Vec<DataField> {
        vec![
            field(1, "id", DataType::Int(IntType::new())),
            field(
                2,
                "profile",
                DataType::Row(RowType::new(vec![
                    field(3, "name", DataType::VarChar(VarCharType::default())),
                    field(4, "age", DataType::Int(IntType::new())),
                ])),
            ),
            field(5, "profile.name", DataType::Int(IntType::new())),
        ]
    }

    #[test]
    fn projects_partial_row_in_path_order() {
        let projected = project_by_paths(
            &fields(),
            &["profile.age".into(), "id".into(), "profile.name".into()],
        )
        .unwrap();
        assert_eq!(
            projected.iter().map(DataField::id).collect::<Vec<_>>(),
            vec![2, 1, 5]
        );
        let DataType::Row(row) = projected[0].data_type() else {
            panic!("expected ROW")
        };
        assert_eq!(
            row.fields().iter().map(DataField::id).collect::<Vec<_>>(),
            vec![4]
        );
    }

    #[test]
    fn whole_field_overrides_partial_selection() {
        let projected =
            project_by_paths(&fields(), &["profile.age".into(), "profile".into()]).unwrap();
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0], fields()[1]);
    }

    #[test]
    fn rejects_unknown_or_non_row_path() {
        for path in ["missing", "profile.missing", "id.part", "profile."] {
            assert!(
                project_by_paths(&fields(), &[path.into()]).is_err(),
                "{path}"
            );
        }
    }

    #[test]
    fn whole_row_overlaps_both_nested_children_but_siblings_are_distinct() {
        let fields = fields().into_iter().take(2).collect::<Vec<_>>();
        let whole = write_leaf_ids(&fields, Some(&["profile".into()])).unwrap();
        let name = write_leaf_ids(&fields, Some(&["profile.name".into()])).unwrap();
        let age = write_leaf_ids(&fields, Some(&["profile.age".into()])).unwrap();
        assert_eq!(whole, HashSet::from([3, 4]));
        assert_eq!(name, HashSet::from([3]));
        assert_eq!(age, HashSet::from([4]));
        assert!(!whole.is_disjoint(&name));
        assert!(!whole.is_disjoint(&age));
        assert!(name.is_disjoint(&age));
    }

    #[test]
    fn write_paths_allow_direct_child_row_but_reject_deeper_partial_row() {
        let nested = vec![field(
            1,
            "profile",
            DataType::Row(RowType::new(vec![field(
                2,
                "address",
                DataType::Row(RowType::new(vec![field(
                    3,
                    "zip",
                    DataType::Int(IntType::new()),
                )])),
            )])),
        )];
        validate_write_paths(&nested, &["profile.address".into()]).unwrap();
        assert!(matches!(
            validate_write_paths(&nested, &["profile.address.zip".into()]),
            Err(Error::Unsupported { .. })
        ));
    }

    #[test]
    fn write_paths_reject_duplicate_and_overlapping_selection() {
        let fields = fields().into_iter().take(2).collect::<Vec<_>>();
        for paths in [
            vec!["profile.age".into(), "profile.age".into()],
            vec!["profile".into(), "profile.age".into()],
            vec!["profile.age".into(), "profile".into()],
        ] {
            assert!(validate_write_paths(&fields, &paths).is_err());
        }
        validate_write_paths(&fields, &["profile.age".into(), "profile.name".into()]).unwrap();
    }
}
