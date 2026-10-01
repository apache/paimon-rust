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

//! One-level ROW composition for data-evolution column groups.
//!
//! Files are ordered newest first. As in Java `DataEvolutionReadPlanner`, the
//! first file containing each leaf wins. All latest sibling providers also
//! contribute to the parent ROW's nullness, including siblings omitted by a
//! read projection. Deeper splits of the same direct subfield are rejected.

use crate::spec::{DataField, DataType, RowType};
use crate::{Error, Result};
use arrow_array::{Array, ArrayRef, RecordBatch, StructArray, UInt32Array};
use arrow_buffer::NullBuffer;
use arrow_schema::DataType as ArrowDataType;
use arrow_select::take::take;
use std::collections::HashSet;
use std::sync::Arc;

pub(super) struct PhysicalRowSource<'a> {
    pub source_index: usize,
    pub fields: &'a [DataField],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NestedSelection {
    pub anchors: Vec<usize>,
    pub children: Vec<Option<usize>>,
    pub whole_source: Option<usize>,
}

/// Return `None` only when no file supplies any leaf of this ROW.
pub(super) fn select_nested_row(
    read_field: &DataField,
    sources: &[PhysicalRowSource<'_>],
) -> Result<Option<NestedSelection>> {
    let DataType::Row(read_row) = read_field.data_type() else {
        return Err(Error::DataInvalid {
            message: format!(
                "Nested data-evolution field '{}' is not a ROW",
                read_field.name()
            ),
            source: None,
        });
    };

    let source_rows = sources
        .iter()
        .filter_map(|source| {
            source
                .fields
                .iter()
                .find(|field| field.id() == read_field.id())
                .and_then(|field| match field.data_type() {
                    DataType::Row(row) => Some((source.source_index, row.fields())),
                    _ => None,
                })
        })
        .collect::<Vec<_>>();
    if source_rows.is_empty() {
        return Ok(None);
    }

    let mut all_leaf_ids = Vec::new();
    for (_, fields) in &source_rows {
        collect_leaf_ids(fields, &mut all_leaf_ids);
    }
    let mut anchors = Vec::new();
    for leaf in all_leaf_ids {
        if let Some(source) = first_provider(leaf, &source_rows) {
            if !anchors.contains(&source) {
                anchors.push(source);
            }
        }
    }
    if anchors.is_empty() {
        return Ok(None);
    }

    let mut children = Vec::with_capacity(read_row.fields().len());
    let mut all_requested_leaves_covered = true;
    for child in read_row.fields() {
        let mut requested_leaves = Vec::new();
        collect_leaf_ids(std::slice::from_ref(child), &mut requested_leaves);
        let providers = providers_of(&requested_leaves, &source_rows);
        all_requested_leaves_covered &= requested_leaves
            .iter()
            .all(|leaf| first_provider(*leaf, &source_rows).is_some());

        let providers = if providers.is_empty() {
            // A projected sub-ROW may contain only recently added descendants.
            // Read its older siblings from their latest provider to retain the
            // sub-ROW nullness while missing descendants are NULL-filled.
            let mut sibling_leaves = Vec::new();
            for (_, fields) in &source_rows {
                if let Some(existing) = fields.iter().find(|field| field.id() == child.id()) {
                    collect_leaf_ids(std::slice::from_ref(existing), &mut sibling_leaves);
                }
            }
            providers_of(&sibling_leaves, &source_rows)
        } else {
            providers
        };
        if providers.len() > 1 {
            return Err(Error::Unsupported {
                message: format!(
                    "Sub-field-level data evolution does not support splitting nested sub-field '{}.{}' across files",
                    read_field.name(), child.name()
                ),
            });
        }
        if providers.is_empty() && !child.data_type().is_nullable() {
            return Err(Error::DataInvalid {
                message: format!(
                    "Cannot read non-nullable nested field '{}.{}' without a provider",
                    read_field.name(),
                    child.name()
                ),
                source: None,
            });
        }
        children.push(providers.first().copied());
    }

    let whole_source = if anchors.len() == 1
        && all_requested_leaves_covered
        && children.iter().all(|source| *source == Some(anchors[0]))
    {
        Some(anchors[0])
    } else {
        None
    };
    Ok(Some(NestedSelection {
        anchors,
        children,
        whole_source,
    }))
}

fn collect_leaf_ids(fields: &[DataField], output: &mut Vec<i32>) {
    for field in fields {
        match field.data_type() {
            DataType::Row(row) => collect_leaf_ids(row.fields(), output),
            _ => output.push(field.id()),
        }
    }
}

fn first_provider(leaf_id: i32, sources: &[(usize, &[DataField])]) -> Option<usize> {
    sources.iter().find_map(|(index, fields)| {
        let mut leaves = Vec::new();
        collect_leaf_ids(fields, &mut leaves);
        leaves.contains(&leaf_id).then_some(*index)
    })
}

fn providers_of(leaves: &[i32], sources: &[(usize, &[DataField])]) -> Vec<usize> {
    let mut providers = Vec::new();
    let mut seen = HashSet::new();
    for leaf in leaves {
        if let Some(source) = first_provider(*leaf, sources) {
            if seen.insert(source) {
                providers.push(source);
            }
        }
    }
    providers
}

/// Source indexes and their parent-field offsets are fixed by the outer read
/// plan after `select_nested_row` chooses the physical providers.
#[derive(Debug, Clone)]
pub(super) struct NestedFieldPlan {
    pub anchors: Vec<(usize, usize)>,
    pub children: Vec<Option<(usize, usize)>>,
}

/// Build the smallest current-schema ROW shape needed from one provider.
/// Selected children are read whole to retain a nested child's nullness after
/// schema evolution. A provider used only as a parent-nullness anchor still
/// needs one physical child decoded; otherwise a projected file with no
/// requested leaves would produce an all-NULL synthetic parent.
pub(super) fn source_read_field(
    requested: &DataField,
    current: &DataField,
    source_index: usize,
    source_fields: &[DataField],
    selection: &NestedSelection,
) -> Result<DataField> {
    let DataType::Row(requested_row) = requested.data_type() else {
        return Err(Error::DataInvalid {
            message: "Nested source read requires a ROW projection".to_string(),
            source: None,
        });
    };
    let DataType::Row(current_row) = current.data_type() else {
        return Err(Error::DataInvalid {
            message: "Nested source read requires a current ROW field".to_string(),
            source: None,
        });
    };
    let selected_ids = requested_row
        .fields()
        .iter()
        .zip(&selection.children)
        .filter_map(|(child, provider)| (*provider == Some(source_index)).then_some(child.id()))
        .collect::<HashSet<_>>();
    let mut children = current_row
        .fields()
        .iter()
        .filter(|child| selected_ids.contains(&child.id()))
        .cloned()
        .collect::<Vec<_>>();
    if children.is_empty() {
        let physical = source_fields
            .iter()
            .find(|field| field.id() == requested.id())
            .ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "Nested source {source_index} does not contain field '{}'",
                    requested.name()
                ),
                source: None,
            })?;
        let DataType::Row(source_row) = physical.data_type() else {
            return Err(Error::DataInvalid {
                message: format!("Nested source {source_index} is not a ROW"),
                source: None,
            });
        };
        let anchor = source_row
            .fields()
            .first()
            .ok_or_else(|| Error::DataInvalid {
                message: format!("Nested source {source_index} has no physical child"),
                source: None,
            })?;
        children.push(
            current_row
                .fields()
                .iter()
                .find(|field| field.id() == anchor.id())
                .unwrap_or(anchor)
                .clone(),
        );
    }
    Ok(super::data_evolution_fields::field_with_type(
        current,
        DataType::Row(RowType::with_nullable(
            current.data_type().is_nullable(),
            children,
        )),
    ))
}

pub(super) fn assemble_nested_row(
    plan: &NestedFieldPlan,
    target_type: &ArrowDataType,
    cursors: &[Option<(RecordBatch, usize)>],
    rows: usize,
) -> Result<ArrayRef> {
    let ArrowDataType::Struct(fields) = target_type else {
        return Err(Error::UnexpectedError {
            message: format!("Nested data-evolution target is not a struct: {target_type:?}"),
            source: None,
        });
    };
    if fields.len() != plan.children.len() {
        return Err(Error::UnexpectedError {
            message: "Nested data-evolution plan has the wrong number of children".to_string(),
            source: None,
        });
    }

    let mut anchors = Vec::with_capacity(plan.anchors.len());
    for &(source, field) in &plan.anchors {
        anchors.push(source_struct(cursors, source, field, rows)?);
    }
    let valid = (0..rows)
        .map(|row| anchors.iter().any(|anchor| anchor.is_valid(row)))
        .collect::<Vec<_>>();
    let parent_nulls = (!valid.iter().all(|value| *value)).then(|| NullBuffer::from(valid));

    let mut children = Vec::with_capacity(fields.len());
    for (index, child_field) in fields.iter().enumerate() {
        let Some((source, field)) = plan.children[index] else {
            children.push(arrow_array::new_null_array(child_field.data_type(), rows));
            continue;
        };
        let parent = source_struct(cursors, source, field, rows)?;
        let value =
            parent
                .column_by_name(child_field.name())
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "Nested data-evolution source {source} is missing child '{}'",
                        child_field.name()
                    ),
                    source: None,
                })?;
        let value = project_child(value, child_field.data_type())?;
        if parent.null_count() == 0 {
            children.push(value);
        } else {
            // A partial ROW's child bytes are meaningless when that source's
            // parent is NULL, even if another sibling source keeps the merged
            // parent non-NULL.
            let positions = UInt32Array::from_iter(
                (0..rows).map(|row| parent.is_valid(row).then_some(row as u32)),
            );
            children.push(take(value.as_ref(), &positions, None).map_err(|error| {
                Error::UnexpectedError {
                    message: format!("Failed to mask NULL nested data-evolution source: {error}"),
                    source: Some(Box::new(error)),
                }
            })?);
        }
    }

    let assembled =
        StructArray::try_new(fields.clone(), children, parent_nulls).map_err(|error| {
            Error::UnexpectedError {
                message: format!("Failed to compose nested data-evolution ROW: {error}"),
                source: Some(Box::new(error)),
            }
        })?;
    Ok(Arc::new(assembled))
}

/// Providers are read as the current full ROW so older siblings can preserve
/// nullness. Trim each selected child back to the requested nested projection
/// before composing the output; Arrow Struct children must match that shape.
fn project_child(value: &ArrayRef, target: &ArrowDataType) -> Result<ArrayRef> {
    if value.data_type() == target {
        return Ok(value.clone());
    }
    if let ArrowDataType::Struct(target_fields) = target {
        let source = value
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "Expected a ROW while projecting nested data-evolution child, got {:?}",
                    value.data_type()
                ),
                source: None,
            })?;
        let children = target_fields
            .iter()
            .map(|field| {
                let child =
                    source
                        .column_by_name(field.name())
                        .ok_or_else(|| Error::DataInvalid {
                            message: format!(
                                "Nested data-evolution source is missing projected child '{}'",
                                field.name()
                            ),
                            source: None,
                        })?;
                project_child(child, field.data_type())
            })
            .collect::<Result<Vec<_>>>()?;
        return StructArray::try_new(target_fields.clone(), children, source.nulls().cloned())
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|error| Error::DataInvalid {
                message: format!("Failed to project nested data-evolution ROW: {error}"),
                source: None,
            });
    }
    arrow_cast::cast(value.as_ref(), target).map_err(|error| Error::DataInvalid {
        message: format!("Failed to cast nested data-evolution child: {error}"),
        source: None,
    })
}

fn source_struct(
    cursors: &[Option<(RecordBatch, usize)>],
    source: usize,
    field: usize,
    rows: usize,
) -> Result<StructArray> {
    let (batch, offset) = cursors
        .get(source)
        .and_then(Option::as_ref)
        .ok_or_else(|| Error::UnexpectedError {
            message: format!("Missing nested data-evolution source {source}"),
            source: None,
        })?;
    let array = batch.column(field).slice(*offset, rows);
    array
        .as_any()
        .downcast_ref::<StructArray>()
        .cloned()
        .ok_or_else(|| Error::DataInvalid {
            message: format!("Nested data-evolution source {source} field {field} is not a ROW"),
            source: None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{IntType, RowType, VarCharType};
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{Field, Schema};

    fn field(id: i32, name: &str, data_type: DataType) -> DataField {
        DataField::new(id, name.to_string(), data_type)
    }

    fn name() -> DataField {
        field(2, "name", DataType::VarChar(VarCharType::default()))
    }

    fn age() -> DataField {
        field(3, "age", DataType::Int(IntType::new()))
    }

    fn row(children: Vec<DataField>) -> DataField {
        field(1, "profile", DataType::Row(RowType::new(children)))
    }

    #[test]
    fn latest_leaf_provider_wins_across_partial_files() {
        let newer = vec![row(vec![age()])];
        let base = vec![row(vec![name(), age()])];
        let sources = [
            PhysicalRowSource {
                source_index: 0,
                fields: &newer,
            },
            PhysicalRowSource {
                source_index: 1,
                fields: &base,
            },
        ];
        let selected = select_nested_row(&row(vec![name(), age()]), &sources)
            .unwrap()
            .unwrap();
        assert_eq!(selected.anchors, vec![0, 1]);
        assert_eq!(selected.children, vec![Some(1), Some(0)]);
        assert_eq!(selected.whole_source, None);
    }

    #[test]
    fn omitted_sibling_still_anchors_parent_nullness() {
        let newer = vec![row(vec![age()])];
        let base = vec![row(vec![name(), age()])];
        let sources = [
            PhysicalRowSource {
                source_index: 0,
                fields: &newer,
            },
            PhysicalRowSource {
                source_index: 1,
                fields: &base,
            },
        ];
        let selected = select_nested_row(&row(vec![name()]), &sources)
            .unwrap()
            .unwrap();
        assert_eq!(selected.anchors, vec![0, 1]);
        assert_eq!(selected.children, vec![Some(1)]);
        assert_eq!(selected.whole_source, None);
    }

    #[test]
    fn provider_reads_only_selected_children_and_hidden_anchor_reads_one_sibling() {
        let current = row(vec![name(), age()]);
        let requested = row(vec![name()]);
        let age_file = vec![row(vec![age()])];
        let base_file = vec![current.clone()];
        let sources = [
            PhysicalRowSource {
                source_index: 0,
                fields: &age_file,
            },
            PhysicalRowSource {
                source_index: 1,
                fields: &base_file,
            },
        ];
        let selection = select_nested_row(&requested, &sources).unwrap().unwrap();

        let anchor = source_read_field(&requested, &current, 0, &age_file, &selection).unwrap();
        let provider = source_read_field(&requested, &current, 1, &base_file, &selection).unwrap();
        let DataType::Row(anchor_row) = anchor.data_type() else {
            panic!("anchor must be ROW")
        };
        let DataType::Row(provider_row) = provider.data_type() else {
            panic!("provider must be ROW")
        };
        assert_eq!(anchor_row.fields(), &[age()]);
        assert_eq!(provider_row.fields(), &[name()]);
    }

    #[test]
    fn deep_added_leaf_reads_whole_direct_child_to_keep_its_nullness() {
        let existing = field(4, "existing", DataType::Int(IntType::new()));
        let added = field(5, "added", DataType::Int(IntType::new()));
        let sub = |children| field(2, "sub", DataType::Row(RowType::new(children)));
        let current = row(vec![sub(vec![existing.clone(), added.clone()]), age()]);
        let requested = row(vec![sub(vec![added])]);
        let physical = vec![row(vec![sub(vec![existing.clone()]), age()])];
        let sources = [PhysicalRowSource {
            source_index: 0,
            fields: &physical,
        }];
        let selection = select_nested_row(&requested, &sources).unwrap().unwrap();
        let read = source_read_field(&requested, &current, 0, &physical, &selection).unwrap();
        let DataType::Row(read_row) = read.data_type() else {
            panic!("read field must be ROW")
        };
        let DataType::Row(current_row) = current.data_type() else {
            panic!("current field must be ROW")
        };
        assert_eq!(read_row.fields(), &current_row.fields()[0..1]);
    }

    #[test]
    fn a_complete_latest_row_needs_no_composition() {
        let newest = vec![row(vec![name(), age()])];
        let older = vec![row(vec![name(), age()])];
        let sources = [
            PhysicalRowSource {
                source_index: 0,
                fields: &newest,
            },
            PhysicalRowSource {
                source_index: 1,
                fields: &older,
            },
        ];
        let selected = select_nested_row(&row(vec![name(), age()]), &sources)
            .unwrap()
            .unwrap();
        assert_eq!(selected.anchors, vec![0]);
        assert_eq!(selected.children, vec![Some(0), Some(0)]);
        assert_eq!(selected.whole_source, Some(0));
    }

    #[test]
    fn rejects_deeper_cross_file_subfield_split() {
        let street = field(4, "street", DataType::VarChar(VarCharType::default()));
        let zip = field(5, "zip", DataType::Int(IntType::new()));
        let address = |children| field(2, "address", DataType::Row(RowType::new(children)));
        let newer = vec![row(vec![address(vec![zip.clone()])])];
        let older = vec![row(vec![address(vec![street.clone()])])];
        let read = row(vec![address(vec![street, zip])]);
        let sources = [
            PhysicalRowSource {
                source_index: 0,
                fields: &newer,
            },
            PhysicalRowSource {
                source_index: 1,
                fields: &older,
            },
        ];
        assert!(matches!(
            select_nested_row(&read, &sources),
            Err(Error::Unsupported { message }) if message.contains("profile.address")
        ));
    }

    #[test]
    fn rejects_missing_non_nullable_subfield() {
        let required = field(4, "required", DataType::Int(IntType::with_nullable(false)));
        let available = vec![row(vec![name()])];
        let sources = [PhysicalRowSource {
            source_index: 0,
            fields: &available,
        }];
        assert!(matches!(
            select_nested_row(&row(vec![required]), &sources),
            Err(Error::DataInvalid { message, .. }) if message.contains("profile.required")
        ));
    }

    fn struct_batch(
        names: Vec<Option<&str>>,
        ages: Vec<Option<i32>>,
        valid: Vec<bool>,
    ) -> RecordBatch {
        let fields = vec![
            Arc::new(Field::new("name", ArrowDataType::Utf8, true)),
            Arc::new(Field::new("age", ArrowDataType::Int32, true)),
        ];
        let parent = StructArray::try_new(
            fields.clone().into(),
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(Int32Array::from(ages)),
            ],
            Some(NullBuffer::from(valid)),
        )
        .unwrap();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "profile",
                ArrowDataType::Struct(fields.into()),
                true,
            )])),
            vec![Arc::new(parent)],
        )
        .unwrap()
    }

    #[test]
    fn assembled_parent_is_valid_when_any_latest_sibling_is_valid() {
        let newer = struct_batch(
            vec![None, None, None],
            vec![Some(20), Some(21), None],
            vec![true, true, false],
        );
        let older = struct_batch(
            vec![Some("old"), Some("hidden"), Some("kept")],
            vec![None, None, None],
            vec![true, false, true],
        );
        let target = newer.schema().field(0).data_type().clone();
        let plan = NestedFieldPlan {
            anchors: vec![(0, 0), (1, 0)],
            children: vec![Some((1, 0)), Some((0, 0))],
        };
        let cursors = vec![Some((newer, 0)), Some((older, 0))];
        let output = assemble_nested_row(&plan, &target, &cursors, 3).unwrap();
        let output = output.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(output.null_count(), 0);
        let names = output
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ages = output
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(names.value(0), "old");
        assert!(names.is_null(1)); // Older parent was NULL despite retained child bytes.
        assert_eq!(names.value(2), "kept");
        assert_eq!(ages.value(0), 20);
        assert_eq!(ages.value(1), 21);
        assert!(ages.is_null(2));
    }

    #[test]
    fn assembled_parent_null_when_every_source_parent_is_null() {
        let newer = struct_batch(vec![None], vec![Some(20)], vec![false]);
        let older = struct_batch(vec![Some("stale")], vec![None], vec![false]);
        let target = newer.schema().field(0).data_type().clone();
        let plan = NestedFieldPlan {
            anchors: vec![(0, 0), (1, 0)],
            children: vec![Some((1, 0)), Some((0, 0))],
        };
        let output =
            assemble_nested_row(&plan, &target, &[Some((newer, 0)), Some((older, 0))], 1).unwrap();
        assert!(output.is_null(0));
    }
}
