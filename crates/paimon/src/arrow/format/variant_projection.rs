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

//! Read-type-driven Parquet clipping, mirroring VariantShreddingTypePruner.

use crate::spec::{parse_variant_metadata, RowType};
use crate::variant::{parse_path, PathSegment};
use parquet::basic::LogicalType;
use parquet::schema::types::Type;
use std::collections::{HashMap, HashSet};

#[derive(Default)]
struct PathNode {
    children: HashMap<String, PathNode>,
    element: Option<Box<PathNode>>,
    keep_all: bool,
}

/// The requested physical leaves, relative to the Variant column. Invalid
/// paths retain the full input so strict/try extraction handles the error at
/// the same point as an unshredded read.
pub(super) fn projected_paths(row: &RowType, physical: &Type) -> HashSet<Vec<String>> {
    let mut root = PathNode::default();
    for field in row.fields() {
        let segments = field.description().and_then(|description| {
            parse_variant_metadata(description)
                .ok()
                .and_then(|metadata| parse_path(metadata.path()).ok())
        });
        let Some(segments) = segments else {
            root.keep_all = true;
            break;
        };
        let mut node = &mut root;
        for segment in segments {
            node = match segment {
                PathSegment::Key(key) => node.children.entry(key).or_default(),
                PathSegment::Index(_) => node.element.get_or_insert_with(Default::default),
            };
        }
        node.keep_all = true;
    }
    let mut result = HashSet::new();
    clip(physical, &root, &mut Vec::new(), &mut result);
    result
}

pub(super) fn all_leaves(
    physical: &Type,
    path: &mut Vec<String>,
    output: &mut HashSet<Vec<String>>,
) {
    if physical.is_primitive() {
        output.insert(path.clone());
    } else {
        for child in physical.get_fields() {
            path.push(child.name().to_string());
            all_leaves(child, path, output);
            path.pop();
        }
    }
}

fn keep_child(child: &Type, path: &mut Vec<String>, output: &mut HashSet<Vec<String>>) {
    path.push(child.name().to_string());
    all_leaves(child, path, output);
    path.pop();
}

fn clip(
    physical: &Type,
    node: &PathNode,
    path: &mut Vec<String>,
    output: &mut HashSet<Vec<String>>,
) {
    if physical.is_primitive() || node.keep_all {
        all_leaves(physical, path, output);
        return;
    }
    let fields = physical.get_fields();
    let Some(typed) = fields.iter().find(|child| child.name() == "typed_value") else {
        all_leaves(physical, path, output);
        return;
    };
    let logical = typed.get_basic_info().logical_type_ref();
    let object = !typed.is_primitive() && logical.is_none();
    let list = !typed.is_primitive()
        && matches!(logical, Some(LogicalType::List))
        && node.element.is_some();
    if (!object || node.element.is_some()) && !list {
        all_leaves(physical, path, output);
        return;
    }
    if list && node.element.as_ref().unwrap().keep_all {
        all_leaves(physical, path, output);
        return;
    }
    if let Some(metadata) = fields.iter().find(|child| child.name() == "metadata") {
        keep_child(metadata, path, output);
    }
    if object && node.element.is_none() {
        let missing = node
            .children
            .keys()
            .any(|key| !typed.get_fields().iter().any(|field| field.name() == key));
        if missing {
            if let Some(value) = fields.iter().find(|child| child.name() == "value") {
                keep_child(value, path, output);
            }
        }
        path.push("typed_value".into());
        for child in typed.get_fields() {
            if let Some(selected) = node.children.get(child.name()) {
                path.push(child.name().to_string());
                clip(child, selected, path, output);
                path.pop();
            }
        }
        path.pop();
    } else {
        // Canonical Parquet LIST: typed_value / list / element. Array
        // indices cannot prune rows, only fields within every element.
        let repeated = typed.get_fields().first();
        let element = repeated
            .filter(|repeated| !repeated.is_primitive())
            .and_then(|repeated| repeated.get_fields().first());
        let (Some(repeated), Some(element)) = (repeated, element) else {
            all_leaves(physical, path, output);
            return;
        };
        if !node.children.is_empty() {
            if let Some(value) = fields.iter().find(|child| child.name() == "value") {
                keep_child(value, path, output);
            }
        }
        path.extend([
            typed.name().into(),
            repeated.name().into(),
            element.name().into(),
        ]);
        clip(element, node.element.as_ref().unwrap(), path, output);
        path.truncate(path.len() - 3);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{variant_extraction_row, DataType, FloatType};
    use parquet::schema::parser::parse_message_type;

    fn paths(requests: &[&str]) -> HashSet<Vec<String>> {
        let physical = parse_message_type(r#"message schema { optional group v {
            optional binary metadata; optional binary value;
            optional group typed_value {
                optional group obj { optional binary value; optional group typed_value {
                    optional group wanted { optional binary value; optional float typed_value; }
                    optional group unused { optional binary value; optional float typed_value; }
                } }
                optional group arr { optional binary value; optional group typed_value (LIST) {
                    repeated group list { optional group element {
                        optional binary value; optional group typed_value {
                            optional group wanted { optional binary value; optional float typed_value; }
                            optional group unused { optional binary value; optional float typed_value; }
                        }
                    } }
                } }
            }
        } }"#).unwrap();
        let row = variant_extraction_row(
            true,
            requests.iter().map(|path| {
                (
                    DataType::Float(FloatType::new()),
                    (*path).into(),
                    false,
                    "UTC".into(),
                )
            }),
        )
        .unwrap();
        projected_paths(&row, &physical.get_fields()[0])
    }

    fn includes(paths: &HashSet<Vec<String>>, path: &str) -> bool {
        paths.contains(&path.split('.').map(str::to_string).collect::<Vec<_>>())
    }

    #[test]
    fn object_array_and_missing_paths_prune_unrequested_leaves() {
        let selected = paths(&["$.obj.wanted", "$.arr[1].wanted", "$.missing"]);
        assert!(includes(&selected, "metadata"));
        assert!(includes(&selected, "value"));
        assert!(includes(
            &selected,
            "typed_value.obj.typed_value.wanted.typed_value"
        ));
        assert!(includes(
            &selected,
            "typed_value.arr.typed_value.list.element.typed_value.wanted.typed_value"
        ));
        assert!(!selected
            .iter()
            .any(|parts| parts.iter().any(|part| part == "unused")));
        let known = paths(&["$.obj.wanted"]);
        assert!(!includes(&known, "value"));
        let wrong_case = paths(&["$.Obj.wanted"]);
        assert!(includes(&wrong_case, "value"));
        assert_eq!(wrong_case.len(), 2);
    }

    #[test]
    fn whole_values_and_invalid_paths_retain_the_required_shape() {
        let whole = paths(&["$"]);
        assert_eq!(paths(&["invalid"]), whole);
        assert!(paths(&["$.arr[0]"])
            .iter()
            .any(|parts| parts.iter().any(|part| part == "unused")));
        let mixed = paths(&["$.arr[0].wanted", "$.arr.key"]);
        assert!(includes(&mixed, "typed_value.arr.value"));
    }
}
