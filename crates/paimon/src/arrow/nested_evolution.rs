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

//! Read-side reconciliation of one decoded column against the read (table)
//! schema, for the cases Arrow's own `cast` cannot express:
//!
//! - a ROW child present in the read schema but **absent from the data file**
//!   (an `ALTER TABLE ... ADD COLUMN parent.child` that landed after the file
//!   was written) is filled with NULLs;
//! - a ROW child the read schema does not ask for is dropped (nested
//!   projection);
//! - children are paired by **field id**, so a renamed nested column still
//!   resolves, and a leaf whose type was promoted is cast.
//!
//! Mirrors Java `SchemaEvolutionUtil.createRowCastExecutor`
//! (paimon-core/src/main/java/org/apache/paimon/schema/SchemaEvolutionUtil.java),
//! which builds the same id-based index mapping per ROW level and yields NULL
//! for a target child with no source counterpart.

use std::sync::Arc;

use arrow_array::{
    new_null_array, Array, ArrayRef, BooleanArray, GenericListArray, ListArray, MapArray,
    OffsetSizeTrait, StringArray, StructArray, UInt32Array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_cast::cast;
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Fields};

use crate::arrow::paimon_type_to_arrow_for_read;
use crate::spec::{
    is_variant_extraction_row_type, map_selected_keys, DataField, DataType, MapType, RowType,
};

/// Field-aware projection for synthetic read types and nested schema evolution.
pub(crate) fn evolve_field(
    source: &ArrayRef,
    source_field: &DataField,
    target: &DataField,
) -> crate::Result<ArrayRef> {
    if let (DataType::Map(map_type), DataType::Row(row_type), Some(keys)) = (
        source_field.data_type(),
        target.data_type(),
        map_selected_keys(target)?,
    ) {
        return selected_map_to_row(source, map_type, row_type, &keys);
    }
    if matches!(source_field.data_type(), DataType::Variant(_)) {
        if let DataType::Row(row) = target.data_type() {
            if is_variant_extraction_row_type(target.data_type()) {
                // A format read plan may already have assembled the synthetic
                // ROW while its field context still describes the VARIANT.
                if source
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .is_some_and(|rows| {
                        rows.column_names()
                            == row
                                .fields()
                                .iter()
                                .map(|field| field.name())
                                .collect::<Vec<_>>()
                    })
                {
                    return evolve_column(source, target.data_type(), target.data_type());
                }
                let assembled =
                    crate::arrow::shredding::variant::assemble_variant_extraction_array(
                        source.as_ref(),
                        row,
                    )?;
                return evolve_column(&assembled, target.data_type(), target.data_type());
            }
        }
    }
    evolve_column(source, source_field.data_type(), target.data_type())
}

pub(crate) fn selected_map_to_row(
    source: &ArrayRef,
    map_type: &MapType,
    row_type: &RowType,
    keys: &[String],
) -> crate::Result<ArrayRef> {
    let map =
        source
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "Selected MAP input must be a MapArray".into(),
                source: None,
            })?;
    if !matches!(
        map_type.key_type(),
        DataType::Char(_) | DataType::VarChar(_)
    ) {
        return Err(crate::Error::Unsupported {
            message: "Selected MAP keys must have a character string type".into(),
        });
    }
    let normalized_keys = evolve_column(
        map.keys(),
        map_type.key_type(),
        &DataType::VarChar(crate::spec::VarCharType::string_type()),
    )?;
    let names = normalized_keys
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "Selected MAP keys must be STRING".into(),
            source: None,
        })?;
    let mut children = Vec::new();
    for (key, field) in keys.iter().zip(row_type.fields()) {
        let indices: UInt32Array = (0..map.len())
            .map(|row| {
                if map.is_null(row) {
                    return None;
                }
                let offsets = map.value_offsets();
                (offsets[row] as usize..offsets[row + 1] as usize)
                    .find(|&index| names.is_valid(index) && names.value(index) == key)
                    .map(|index| index as u32)
            })
            .collect();
        let values =
            arrow_select::take::take(map.values().as_ref(), &indices, None).map_err(|e| {
                crate::Error::UnexpectedError {
                    message: "Failed to gather selected MAP values".into(),
                    source: Some(Box::new(e)),
                }
            })?;
        children.push(evolve_column(
            &values,
            map_type.value_type(),
            field.data_type(),
        )?);
    }
    let ArrowDataType::Struct(fields) =
        paimon_type_to_arrow_for_read(&DataType::Row(row_type.clone()))?
    else {
        unreachable!()
    };
    Ok(Arc::new(
        StructArray::try_new(fields, children, map.nulls().cloned()).map_err(|e| {
            crate::Error::DataInvalid {
                message: format!("Failed to build selected-key ROW: {e}"),
                source: Some(Box::new(e)),
            }
        })?,
    ))
}

/// Reconcile `source` (as described by `source_type`, the type the data file
/// actually holds) with `target_type` (the type the read schema wants).
///
/// Recurses through ROW so an added, dropped, renamed or promoted nested field
/// is handled at any depth. Leaf conversions use the Java-compatible scalar
/// executors also used by top-level old-file reads.
pub(crate) fn evolve_column(
    source: &ArrayRef,
    source_type: &DataType,
    target_type: &DataType,
) -> crate::Result<ArrayRef> {
    // DECIMAL has at most 38 digits in Paimon, but Arrow metadata may use the
    // wider Decimal256 storage for the same logical type. Normalize storage
    // before dispatching Java numeric executors, preserving precision/scale.
    if matches!(source_type, DataType::Decimal(_))
        && matches!(source.data_type(), ArrowDataType::Decimal256(_, _))
    {
        let source_arrow = paimon_type_to_arrow_for_read(source_type)?;
        let normalized = arrow_cast::cast_with_options(
            source,
            &source_arrow,
            &arrow_cast::CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .map_err(|error| crate::Error::DataInvalid {
            message: format!("failed to normalize DECIMAL storage: {error}"),
            source: None,
        })?;
        return evolve_column(&normalized, source_type, target_type);
    }
    let target_arrow = paimon_type_to_arrow_for_read(target_type)?;
    // Arrow equality alone is NOT enough to skip the walk: `paimon_type_to_arrow`
    // drops field ids, so a nested child that was dropped and re-added under the
    // same name and type produces an identical Arrow struct while carrying a
    // different id. Serving the old column there would return the dropped
    // field's values instead of NULL. Gate the fast path on the Paimon types
    // instead, as Java `SchemaEvolutionUtil.createCastExecutor` does with
    // `equalsIgnoreNullable`; the Arrow check stays so a decoded array that does
    // not actually match the target still goes through the cast below.
    if source_type.equals_ignore_nullable(target_type) && source.data_type() == &target_arrow {
        return Ok(source.clone());
    }
    if source_type.equals_ignore_nullable(target_type)
        && !matches!(
            source_type,
            DataType::Row(_) | DataType::Array(_) | DataType::Map(_) | DataType::Multiset(_)
        )
    {
        return arrow_cast::cast_with_options(
            source,
            &target_arrow,
            &arrow_cast::CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .map_err(|error| crate::Error::DataInvalid {
            message: format!("failed to normalize schema-evolution storage: {error}"),
            source: None,
        });
    }

    if matches!(target_type, DataType::VarChar(_) | DataType::Char(_))
        && matches!(
            source_type,
            DataType::Row(_) | DataType::Array(_) | DataType::Map(_)
        )
    {
        return Ok(Arc::new(StringArray::from(render_string_values(
            source,
            source_type,
        )?)));
    }

    match (target_type, source_type) {
        // A variant-extraction ROW is synthetic: its fields are numbered by
        // position, not by schema field id, so pairing them by id would mix
        // columns up. `prune_data_type` keeps such a row verbatim, so there is
        // nothing to evolve — leave it to the cast below, as before.
        (DataType::Row(_), DataType::Row(_))
            if is_variant_extraction_row_type(target_type)
                || is_variant_extraction_row_type(source_type) =>
        {
            return arrow_cast::cast_with_options(
                source,
                &target_arrow,
                &arrow_cast::CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
            .map_err(|error| crate::Error::DataInvalid {
                message: format!("failed to normalize variant-extraction ROW storage: {error}"),
                source: None,
            });
        }
        (DataType::Row(target_row), DataType::Row(source_row)) => {
            return evolve_struct(source, source_row, target_row)
        }
        (DataType::Array(target_array), DataType::Array(source_array)) => {
            return evolve_list(
                source,
                source_array.element_type(),
                target_array.element_type(),
            )
        }
        (DataType::Map(target_map), DataType::Map(source_map)) => {
            return evolve_map(source, source_map, target_map)
        }
        (DataType::Multiset(target_multiset), DataType::Multiset(source_multiset)) => {
            return evolve_multiset(
                source,
                source_multiset.element_type(),
                target_multiset.element_type(),
            )
        }
        _ => {}
    }

    if let Some(result) = super::cast_numeric(source, source_type, target_type)? {
        return Ok(result);
    }

    if let Some(result) =
        super::evolution_string::temporal_to_string(source, source_type, target_type)?
    {
        return Ok(result);
    }

    if let Some(result) =
        super::evolution_temporal::cast_temporal(source, source_type, target_type)?
    {
        return Ok(result);
    }

    // Apply the ordinary TIMESTAMP epoch-seconds rule at every leaf,
    // including nested containers.
    if matches!(source_type, DataType::Int(_) | DataType::BigInt(_))
        && matches!(target_type, DataType::Timestamp(_))
    {
        return super::evolution_cast::numeric_epoch_seconds_to_timestamp(source, &target_arrow);
    }
    if matches!(target_type, DataType::BigInt(_))
        && matches!(
            source_type,
            DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_)
        )
    {
        return super::evolution_cast::timestamp_to_bigint(
            source,
            matches!(source_type, DataType::LocalZonedTimestamp(_)),
        );
    }

    if let Some(result) =
        super::evolution_cast::timestamp_precision(source, source_type, target_type, &target_arrow)?
    {
        return Ok(result);
    }

    // Old-file reads and authorization use the same Java executor for lexical
    // parsing, numeric text and character/binary widths. Arrow's default cast
    // can otherwise silently replace invalid strings with NULL.
    if super::CastExecutor::supports(source_type, target_type) {
        return super::CastExecutor::new(
            source_type.clone(),
            source_type.clone(),
            target_type.clone(),
        )?
        .cast(source);
    }
    Err(crate::Error::Unsupported {
        message: format!(
            "Cannot cast from type {source_type} to type {target_type} during schema evolution"
        ),
    })
}

/// Match Paimon's constructed-value string form, including nested values and
/// NULL containers. Arrow's generic cast cannot convert ROW/ARRAY/MAP to Utf8.
fn render_string_values(
    source: &ArrayRef,
    source_type: &DataType,
) -> crate::Result<Vec<Option<String>>> {
    match source_type {
        DataType::Row(row_type) => {
            let rows = source
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!("expected ROW array, got {:?}", source.data_type()),
                    source: None,
                })?;
            let children = row_type
                .fields()
                .iter()
                .map(|field| {
                    let column = rows.column_by_name(field.name()).ok_or_else(|| {
                        crate::Error::DataInvalid {
                            message: format!(
                                "missing historical ROW field '{}' during string conversion",
                                field.name()
                            ),
                            source: None,
                        }
                    })?;
                    render_string_values(&visible_struct_child(column, rows)?, field.data_type())
                })
                .collect::<crate::Result<Vec<_>>>()?;
            Ok((0..rows.len())
                .map(|index| {
                    rows.is_valid(index).then(|| {
                        format!(
                            "{{{}}}",
                            children
                                .iter()
                                .map(|child| child[index].as_deref().unwrap_or("null"))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    })
                })
                .collect())
        }
        DataType::Array(array_type) => {
            let rows = visible_list(source)?;
            let values = render_string_values(rows.values(), array_type.element_type())?;
            let offsets = rows.value_offsets();
            Ok((0..rows.len())
                .map(|index| {
                    rows.is_valid(index).then(|| {
                        format!(
                            "[{}]",
                            values[offsets[index] as usize..offsets[index + 1] as usize]
                                .iter()
                                .map(|value| value.as_deref().unwrap_or("null"))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    })
                })
                .collect())
        }
        DataType::Map(map_type) => {
            let rows = visible_map(as_map_array(source, "MAP")?)?;
            let keys = render_string_values(rows.keys(), map_type.key_type())?;
            let values = render_string_values(rows.values(), map_type.value_type())?;
            let offsets = rows.value_offsets();
            Ok((0..rows.len())
                .map(|index| {
                    rows.is_valid(index).then(|| {
                        format!(
                            "{{{}}}",
                            (offsets[index] as usize..offsets[index + 1] as usize)
                                .map(|item| format!(
                                    "{} -> {}",
                                    keys[item].as_deref().unwrap_or("null"),
                                    values[item].as_deref().unwrap_or("null")
                                ))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    })
                })
                .collect())
        }
        _ => {
            let casted = evolve_column(
                source,
                source_type,
                &DataType::VarChar(crate::spec::VarCharType::string_type()),
            )
            .map_err(|error| crate::Error::UnexpectedError {
                message: format!(
                    "failed to render {:?} as string during schema evolution",
                    source.data_type()
                ),
                source: Some(Box::new(error)),
            })?;
            let strings = casted
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| crate::Error::UnexpectedError {
                    message: "string rendering did not produce Utf8".to_string(),
                    source: None,
                })?;
            Ok((0..strings.len())
                .map(|index| {
                    strings
                        .is_valid(index)
                        .then(|| strings.value(index).to_string())
                })
                .collect())
        }
    }
}

/// Rebuild a struct array to `target_row`: pair children by field id, recurse,
/// NULL-fill target children the source does not have, and preserve the
/// source's row-level validity buffer.
fn evolve_struct(
    source: &ArrayRef,
    source_row: &RowType,
    target_row: &RowType,
) -> crate::Result<ArrayRef> {
    let source_struct = source
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!(
                "expected a struct array for ROW schema evolution, got {:?}",
                source.data_type()
            ),
            source: None,
        })?;

    let mut fields: Vec<Arc<ArrowField>> = Vec::with_capacity(target_row.fields().len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(target_row.fields().len());
    for target_field in target_row.fields() {
        let target_arrow = paimon_type_to_arrow_for_read(target_field.data_type())?;
        fields.push(Arc::new(ArrowField::new(
            target_field.name(),
            target_arrow.clone(),
            true,
        )));

        // Pair by id (Java: `createIndexMapping` over the ROW's fields).
        let source_field = source_row
            .fields()
            .iter()
            .find(|source_field| source_field.id() == target_field.id());

        match source_field {
            // Not in the data file's schema: the column was added to the ROW
            // after this file was written.
            None => arrays.push(new_null_array(&target_arrow, source_struct.len())),
            Some(source_field) => {
                // In the file's schema, so the decoder must have produced it —
                // under the source field's own name, which is what the file
                // labels the child with. A gap here means the file, its schema
                // or the format reader's projection disagree; NULL-filling it
                // would pass that off as legitimately absent data.
                let column = source_struct
                    .column_by_name(source_field.name())
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: format!(
                            "nested field '{}' (id {}) is declared by the data file's schema \
                             but missing from the decoded struct {:?}",
                            source_field.name(),
                            source_field.id(),
                            source_struct.data_type()
                        ),
                        source: None,
                    })?;
                arrays.push(evolve_field(
                    &visible_struct_child(column, source_struct)?,
                    source_field,
                    target_field,
                )?)
            }
        }
    }

    let evolved = StructArray::try_new(fields.into(), arrays, source_struct.nulls().cloned())
        .map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to build schema-evolved struct: {e}"),
            source: None,
        })?;
    Ok(Arc::new(evolved))
}

/// Rebuild an ARRAY whose element type evolved, reusing the source offsets and
/// validity so only the element values are reconciled. Mirrors Java
/// `SchemaEvolutionUtil.createArrayCastExecutor`.
fn evolve_list(
    source: &ArrayRef,
    source_element: &DataType,
    target_element: &DataType,
) -> crate::Result<ArrayRef> {
    let list = visible_list(source)?;

    let values = evolve_column(list.values(), source_element, target_element)?;
    let element_field = Arc::new(ArrowField::new(
        "element",
        paimon_type_to_arrow_for_read(target_element)?,
        true,
    ));
    let evolved = ListArray::try_new(
        element_field,
        list.offsets().clone(),
        values,
        list.nulls().cloned(),
    )
    .map_err(|e| crate::Error::DataInvalid {
        message: format!("failed to build schema-evolved list: {e}"),
        source: None,
    })?;
    Ok(Arc::new(evolved))
}

/// Normalize physical offsets without casting children before field-id evolution.
/// Arrow metadata can store logical ARRAYs as LargeList or FixedSizeList. Their
/// children still describe the historical file schema and must remain intact.
fn normalize_list(source: &ArrayRef) -> crate::Result<ListArray> {
    if let Some(list) = source.as_any().downcast_ref::<ListArray>() {
        return Ok(list.clone());
    }
    let child = match source.data_type() {
        ArrowDataType::LargeList(child) | ArrowDataType::FixedSizeList(child, _) => child,
        _ => {
            return Err(crate::Error::DataInvalid {
                message: format!("expected ARRAY array, got {:?}", source.data_type()),
                source: None,
            });
        }
    };
    let normalized = cast(source, &ArrowDataType::List(child.clone())).map_err(|e| {
        crate::Error::DataInvalid {
            message: format!("failed to normalize ARRAY offsets: {e}"),
            source: None,
        }
    })?;
    normalized
        .as_any()
        .downcast_ref::<ListArray>()
        .cloned()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "normalizing ARRAY offsets did not yield a ListArray".into(),
            source: None,
        })
}

fn visible_struct_child(column: &ArrayRef, rows: &StructArray) -> crate::Result<ArrayRef> {
    if rows.null_count() == 0 {
        return Ok(column.clone());
    }
    let hidden = BooleanArray::from(
        (0..rows.len())
            .map(|row| rows.is_null(row))
            .collect::<Vec<_>>(),
    );
    arrow_select::nullif::nullif(column.as_ref(), &hidden).map_err(|error| {
        crate::Error::DataInvalid {
            message: format!("failed to mask a NULL ROW child: {error}"),
            source: None,
        }
    })
}

/// Only convert elements the visible containers can observe. Slices may retain
/// unrelated prefix/suffix values; NULL containers can also own physical slots.
/// Java's array/map getters never evaluate those slots.
fn visible_entries<O: OffsetSizeTrait>(
    offsets: &[O],
    nulls: Option<&NullBuffer>,
    values: &ArrayRef,
) -> crate::Result<(OffsetBuffer<i32>, ArrayRef)> {
    let start = offsets[0].as_usize();
    let end = offsets[offsets.len() - 1].as_usize();
    let has_hidden_entries = (0..offsets.len() - 1).any(|row| {
        nulls.is_some_and(|nulls| nulls.is_null(row)) && offsets[row] != offsets[row + 1]
    });
    let mut result_offsets = Vec::with_capacity(offsets.len());
    result_offsets.push(0);
    let selected = if has_hidden_entries {
        let mut pieces = Vec::new();
        let mut count = 0usize;
        for row in 0..offsets.len() - 1 {
            let from = offsets[row].as_usize();
            let to = offsets[row + 1].as_usize();
            if nulls.is_none_or(|nulls| nulls.is_valid(row)) && from != to {
                pieces.push(values.slice(from, to - from));
                count += to - from;
            }
            result_offsets.push(i32::try_from(count).map_err(|error| {
                crate::Error::DataInvalid {
                    message: format!(
                        "ARRAY/MAP visible offsets exceed Arrow List storage: {error}"
                    ),
                    source: None,
                }
            })?);
        }
        if pieces.is_empty() {
            values.slice(start, 0)
        } else {
            arrow_select::concat::concat(
                &pieces.iter().map(|part| part.as_ref()).collect::<Vec<_>>(),
            )
            .map_err(|error| crate::Error::DataInvalid {
                message: format!("failed to gather visible ARRAY/MAP entries: {error}"),
                source: None,
            })?
        }
    } else {
        for offset in &offsets[1..] {
            result_offsets.push(i32::try_from(offset.as_usize() - start).map_err(|error| {
                crate::Error::DataInvalid {
                    message: format!(
                        "ARRAY/MAP visible offsets exceed Arrow List storage: {error}"
                    ),
                    source: None,
                }
            })?);
        }
        values.slice(start, end - start)
    };
    Ok((OffsetBuffer::new(result_offsets.into()), selected))
}

fn visible_generic_list<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
) -> crate::Result<ListArray> {
    let (offsets, values) = visible_entries(list.value_offsets(), list.nulls(), list.values())?;
    let child = match list.data_type() {
        ArrowDataType::List(child) | ArrowDataType::LargeList(child) => child.clone(),
        _ => unreachable!(),
    };
    ListArray::try_new(child, offsets, values, list.nulls().cloned()).map_err(|error| {
        crate::Error::DataInvalid {
            message: format!("failed to build visible ARRAY: {error}"),
            source: None,
        }
    })
}

fn visible_list(source: &ArrayRef) -> crate::Result<ListArray> {
    if let Some(list) = source
        .as_any()
        .downcast_ref::<arrow_array::LargeListArray>()
    {
        return visible_generic_list(list);
    }
    visible_generic_list(&normalize_list(source)?)
}

fn visible_map(map: &MapArray) -> crate::Result<MapArray> {
    let entries: ArrayRef = Arc::new(map.entries().clone());
    let (offsets, entries) = visible_entries(map.value_offsets(), map.nulls(), &entries)?;
    let entries = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .clone();
    let ArrowDataType::Map(field, sorted) = map.data_type() else {
        unreachable!()
    };
    MapArray::try_new(
        field.clone(),
        offsets,
        entries,
        map.nulls().cloned(),
        *sorted,
    )
    .map_err(|error| crate::Error::DataInvalid {
        message: format!("failed to build visible MAP: {error}"),
        source: None,
    })
}

/// Rebuild a MAP whose key or value type evolved, reusing the source offsets and
/// validity. Mirrors Java `SchemaEvolutionUtil.createMapCastExecutor`.
fn evolve_map(
    source: &ArrayRef,
    source_map: &MapType,
    target_map: &MapType,
) -> crate::Result<ArrayRef> {
    let map = visible_map(as_map_array(source, "MAP")?)?;
    let keys = evolve_column(map.keys(), source_map.key_type(), target_map.key_type())?;
    let values = evolve_column(
        map.values(),
        source_map.value_type(),
        target_map.value_type(),
    )?;
    rebuild_map(
        &map,
        keys,
        values,
        ArrowField::new(
            "key",
            paimon_type_to_arrow_for_read(target_map.key_type())?,
            false,
        ),
        ArrowField::new(
            "value",
            paimon_type_to_arrow_for_read(target_map.value_type())?,
            true,
        ),
    )
}

/// Rebuild a MULTISET whose element type evolved. Arrow uses a list of
/// element/count entries; Parquet and historical files can decode as a map.
/// Only the element side evolves and the counts carry over untouched.
///
/// Java's read-side cast dispatcher has no MULTISET branch and raises "Cannot
/// cast from type ... to type ..." for such a schema change; going through the
/// element here keeps the same shapes Java would have accepted and, unlike
/// falling through to Arrow's `cast`, cannot hand back a dropped field's values
/// under a re-added field's id.
fn evolve_multiset(
    source: &ArrayRef,
    source_element: &DataType,
    target_element: &DataType,
) -> crate::Result<ArrayRef> {
    let list = if let Some(map) = source.as_any().downcast_ref::<MapArray>() {
        visible_generic_list(&ListArray::from(map.clone()))?
    } else {
        visible_list(source)?
    };
    let entries = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .filter(|entries| entries.num_columns() == 2)
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "MULTISET entries must contain an element and count".into(),
            source: None,
        })?;
    let elements = evolve_column(entries.column(0), source_element, target_element)?;
    let field =
        crate::arrow::multiset::entries_field(paimon_type_to_arrow_for_read(target_element)?, true);
    let ArrowDataType::Struct(fields) = field.data_type() else {
        unreachable!()
    };
    let entries = StructArray::try_new(
        fields.clone(),
        vec![elements, entries.column(1).clone()],
        None,
    )
    .map_err(|error| crate::Error::DataInvalid {
        message: format!("failed to build schema-evolved MULTISET: {error}"),
        source: None,
    })?;
    let result = ListArray::try_new(
        field,
        list.offsets().clone(),
        Arc::new(entries),
        list.nulls().cloned(),
    )
    .map_err(|error| crate::Error::DataInvalid {
        message: format!("failed to build schema-evolved MULTISET: {error}"),
        source: None,
    })?;
    Ok(Arc::new(result))
}

fn as_map_array<'a>(source: &'a ArrayRef, kind: &str) -> crate::Result<&'a MapArray> {
    source
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!(
                "expected a map array for {kind} schema evolution, got {:?}",
                source.data_type()
            ),
            source: None,
        })
}

/// Reassemble a map array around reconciled key/value children, keeping the
/// source offsets and validity and the entries layout `paimon_type_to_arrow`
/// produces, so the rebuilt array's type matches the read schema exactly.
fn rebuild_map(
    map: &MapArray,
    keys: ArrayRef,
    values: ArrayRef,
    key_field: ArrowField,
    value_field: ArrowField,
) -> crate::Result<ArrayRef> {
    let entry_fields = Fields::from(vec![key_field, value_field]);
    let entries = StructArray::try_new(
        entry_fields.clone(),
        vec![keys, values],
        map.entries().nulls().cloned(),
    )
    .map_err(|e| crate::Error::DataInvalid {
        message: format!("failed to build schema-evolved map entries: {e}"),
        source: None,
    })?;
    let evolved = MapArray::try_new(
        Arc::new(ArrowField::new(
            "entries",
            ArrowDataType::Struct(entry_fields),
            false,
        )),
        map.offsets().clone(),
        entries,
        map.nulls().cloned(),
        false,
    )
    .map_err(|e| crate::Error::DataInvalid {
        message: format!("failed to build schema-evolved map: {e}"),
        source: None,
    })?;
    Ok(Arc::new(evolved))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        ArrayType, BigIntType, DataField, DecimalType, IntType, MapType, TimestampType, VarCharType,
    };
    use arrow_array::{
        Decimal128Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampSecondArray,
    };
    use arrow_buffer::NullBuffer;
    use arrow_schema::{DataType as ArrowDataType, Fields};

    #[test]
    fn timestamp_string_evolution_uses_java_separator_and_precision() {
        let source: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
            Some(0),
            Some(-1),
            None,
        ]));
        let result = evolve_column(
            &source,
            &DataType::Timestamp(TimestampType::new(3).unwrap()),
            &DataType::VarChar(VarCharType::string_type()),
        )
        .unwrap();
        assert_eq!(
            result.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec![
                Some("1970-01-01 00:00:00.000"),
                Some("1969-12-31 23:59:59.999"),
                None
            ])
        );
    }

    #[test]
    fn selected_map_read_type_uses_key_metadata_not_child_names() {
        use arrow_array::builder::{Int32Builder, MapBuilder, StringBuilder};
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        for (key, value) in [("key", 7), ("key", 9), ("", 3)] {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.append(true).unwrap();
        let source: ArrayRef = Arc::new(builder.finish());
        let source_field = field(
            10,
            "attrs",
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::string_type()),
                DataType::Int(IntType::new()),
            )),
        );
        let requested = crate::spec::map_selected_keys_field(
            &source_field,
            &["key".into(), "".into(), "missing".into()],
        )
        .unwrap();
        let DataType::Row(row) = requested.data_type() else {
            panic!()
        };
        let target = DataField::new(
            requested.id(),
            requested.name().into(),
            DataType::Row(RowType::new(
                row.fields()
                    .iter()
                    .enumerate()
                    .map(|(index, child)| {
                        DataField::new(
                            child.id(),
                            format!("output_{index}"),
                            DataType::BigInt(BigIntType::new()),
                        )
                    })
                    .collect(),
            )),
        )
        .with_description(requested.description().map(str::to_string));
        let output = evolve_field(&source, &source_field, &target).unwrap();
        let output = output.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(output.is_null(1));
        assert!(output.is_valid(2));
        let first = output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(first.iter().collect::<Vec<_>>(), vec![Some(7), None, None]);
        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(3), None, None]
        );
        assert_eq!(output.column(2).null_count(), 3);
    }

    #[test]
    fn selected_map_normalizes_large_string_keys_in_sliced_input() {
        use arrow_array::builder::{Int32Builder, LargeStringBuilder, MapBuilder};
        let mut builder = MapBuilder::new(None, LargeStringBuilder::new(), Int32Builder::new());
        for (key, value) in [("prefix", 99), ("key", 7), ("key", 9), ("suffix", 88)] {
            builder.keys().append_value(key);
            builder.values().append_value(value);
            if key != "key" || value == 9 {
                builder.append(true).unwrap();
            }
        }
        let source: ArrayRef = Arc::new(builder.finish());
        let source_field = field(
            10,
            "attrs",
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::string_type()),
                DataType::Int(IntType::new()),
            )),
        );
        let requested =
            crate::spec::map_selected_keys_field(&source_field, &["key".into(), "prefix".into()])
                .unwrap();
        let output = evolve_field(&source.slice(1, 1), &source_field, &requested).unwrap();
        let output = output.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![7])
        );
        assert_eq!(output.column(1).null_count(), 1);
    }

    fn field(id: i32, name: &str, dt: DataType) -> DataField {
        DataField::new(id, name.to_string(), dt)
    }

    fn string_type() -> DataType {
        DataType::VarChar(VarCharType::new(50).unwrap())
    }

    fn row(fields: Vec<DataField>) -> DataType {
        DataType::Row(RowType::new(fields))
    }

    /// Arrow struct `{codec: Utf8, width: Int32}` — the shape a file written
    /// under the older schema holds.
    fn source_struct() -> ArrayRef {
        let codec: ArrayRef = Arc::new(StringArray::from(vec![Some("h264"), Some("h265")]));
        let width: ArrayRef = Arc::new(Int32Array::from(vec![Some(1920), Some(3840)]));
        Arc::new(StructArray::from(vec![
            (
                Arc::new(ArrowField::new("codec", ArrowDataType::Utf8, true)),
                codec,
            ),
            (
                Arc::new(ArrowField::new("width", ArrowDataType::Int32, true)),
                width,
            ),
        ]))
    }

    fn source_row() -> DataType {
        row(vec![
            field(1, "codec", string_type()),
            field(2, "width", DataType::Int(IntType::new())),
        ])
    }

    fn as_struct(array: &ArrayRef) -> StructArray {
        array
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array")
            .clone()
    }

    fn strings(array: &ArrayRef) -> StringArray {
        array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array")
            .clone()
    }

    #[test]
    fn fills_null_for_child_added_after_the_file_was_written() {
        // Read type gained `color_transfer` (id 3); the file's struct has no
        // such child.
        let target = row(vec![
            field(1, "codec", string_type()),
            field(2, "width", DataType::Int(IntType::new())),
            field(3, "color_transfer", string_type()),
        ]);

        let out = evolve_column(&source_struct(), &source_row(), &target).unwrap();

        // The rebuilt struct must match the type the read schema declares, or
        // RecordBatch assembly would reject it.
        assert_eq!(
            out.data_type(),
            &paimon_type_to_arrow_for_read(&target).unwrap()
        );
        let evolved = as_struct(&out);
        assert_eq!(
            evolved.column_names(),
            vec!["codec", "width", "color_transfer"]
        );
        let added = evolved.column_by_name("color_transfer").unwrap();
        assert_eq!(added.len(), 2);
        assert_eq!(added.null_count(), 2);
        // Existing children keep their values.
        assert_eq!(
            strings(evolved.column_by_name("codec").unwrap()).value(0),
            "h264"
        );
    }

    #[test]
    fn pairs_children_by_id_not_by_name() {
        // The nested column was renamed `codec` -> `codec_name`; the file still
        // holds it under the old name, and the ids match.
        let target = row(vec![
            field(1, "codec_name", string_type()),
            field(2, "width", DataType::Int(IntType::new())),
        ]);

        let out = evolve_column(&source_struct(), &source_row(), &target).unwrap();

        let evolved = as_struct(&out);
        assert_eq!(evolved.column_names(), vec!["codec_name", "width"]);
        let renamed = strings(evolved.column_by_name("codec_name").unwrap());
        assert_eq!(renamed.value(0), "h264");
        assert_eq!(renamed.value(1), "h265");
    }

    #[test]
    fn drops_children_the_read_type_does_not_ask_for() {
        // Nested projection: only `width` is requested.
        let target = row(vec![field(2, "width", DataType::Int(IntType::new()))]);

        let out = evolve_column(&source_struct(), &source_row(), &target).unwrap();

        let evolved = as_struct(&out);
        assert_eq!(evolved.column_names(), vec!["width"]);
        assert_eq!(evolved.len(), 2);
        let widths = evolved
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        assert_eq!(widths.value(0), 1920);
    }

    #[test]
    fn recurses_into_a_nested_row() {
        // source: Struct{ id: Int32, inner: Struct{ a: Utf8 } }
        let inner: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(ArrowField::new("a", ArrowDataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("x"), Some("y")])) as ArrayRef,
        )]));
        let id: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
        let source: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(ArrowField::new("id", ArrowDataType::Int32, true)),
                id,
            ),
            (
                Arc::new(ArrowField::new(
                    "inner",
                    ArrowDataType::Struct(Fields::from(vec![ArrowField::new(
                        "a",
                        ArrowDataType::Utf8,
                        true,
                    )])),
                    true,
                )),
                inner,
            ),
        ]));
        let source_type = row(vec![
            field(1, "id", DataType::Int(IntType::new())),
            field(2, "inner", row(vec![field(3, "a", string_type())])),
        ]);
        // The inner ROW gained `b` two levels down.
        let target = row(vec![field(
            2,
            "inner",
            row(vec![
                field(3, "a", string_type()),
                field(4, "b", string_type()),
            ]),
        )]);

        let out = evolve_column(&source, &source_type, &target).unwrap();

        let evolved = as_struct(&out);
        assert_eq!(evolved.column_names(), vec!["inner"]);
        let inner_out = as_struct(evolved.column_by_name("inner").unwrap());
        assert_eq!(inner_out.column_names(), vec!["a", "b"]);
        assert_eq!(
            strings(inner_out.column_by_name("a").unwrap()).value(1),
            "y"
        );
        assert_eq!(inner_out.column_by_name("b").unwrap().null_count(), 2);
    }

    #[test]
    fn evolves_bigint_epoch_seconds_to_timestamp_scaled_to_the_column_unit() {
        // A BIGINT column holding epoch *seconds* (Java's NumericPrimitiveToTimestamp
        // reads INTEGER/BIGINT that way) evolved to TIMESTAMP must be scaled into
        // the column's unit, not reinterpreted raw. 1_700_000_000 s is 2023-11-14.
        let secs = 1_700_000_000_i64;
        let source: ArrayRef = Arc::new(Int64Array::from(vec![Some(secs), None]));
        let source_type = DataType::BigInt(BigIntType::new());

        let ts6 = DataType::Timestamp(TimestampType::new(6).unwrap());
        let micros = evolve_column(&source, &source_type, &ts6).unwrap();
        assert_eq!(
            micros.data_type(),
            &paimon_type_to_arrow_for_read(&ts6).unwrap()
        );
        let micros = micros
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("microsecond timestamps");
        assert_eq!(micros.value(0), secs * 1_000_000);
        assert!(micros.is_null(1));

        let ts0 = DataType::Timestamp(TimestampType::new(0).unwrap());
        let seconds = evolve_column(&source, &source_type, &ts0).unwrap();
        let seconds = seconds
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .expect("second timestamps");
        assert_eq!(seconds.value(0), secs);

        let ts3 = DataType::Timestamp(TimestampType::new(3).unwrap());
        let millis = evolve_column(&source, &source_type, &ts3).unwrap();
        let millis = millis
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("millisecond timestamps");
        assert_eq!(millis.value(0), secs * 1_000);
    }

    #[test]
    fn evolves_timestamp_to_bigint_as_floored_epoch_seconds() {
        for (precision, ticks) in [(0, 1_i64), (3, 1_000), (6, 1_000_000), (9, 1_000_000_000)] {
            for local in [false, true] {
                let source_type = if local {
                    DataType::LocalZonedTimestamp(
                        crate::spec::LocalZonedTimestampType::new(precision).unwrap(),
                    )
                } else {
                    DataType::Timestamp(TimestampType::new(precision).unwrap())
                };
                let fraction = if precision == 0 { 0 } else { 1 };
                let raw: ArrayRef = Arc::new(Int64Array::from(vec![
                    None,
                    Some(ticks + fraction),
                    Some(-1),
                    Some(-ticks - fraction),
                    Some(0),
                    Some((i64::from(i32::MAX) + 3) * ticks),
                    None,
                ]));
                let source = cast(&raw, &paimon_type_to_arrow_for_read(&source_type).unwrap())
                    .unwrap()
                    .slice(1, 6);
                let out =
                    evolve_column(&source, &source_type, &DataType::BigInt(BigIntType::new()))
                        .unwrap();
                let values: Vec<_> = out
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect();
                assert_eq!(
                    values,
                    vec![
                        Some(1),
                        Some(-1),
                        Some(if precision == 0 { -1 } else { -2 }),
                        Some(0),
                        Some(i64::from(i32::MAX) + 3),
                        None
                    ],
                    "precision={precision} local={local}"
                );
            }
        }
    }

    #[test]
    fn bigint_epoch_seconds_wrap_in_millis_before_timestamp_conversion_like_java() {
        let source: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(i64::MAX),
            Some(i64::MIN),
            Some(i64::MAX - 1),
            Some(42),
            Some(-42),
            None,
        ]));
        for (precision, ticks) in [(0, 1_i64), (3, 1_000), (6, 1_000_000), (9, 1_000_000_000)] {
            let target = DataType::Timestamp(TimestampType::new(precision).unwrap());
            let out =
                evolve_column(&source, &DataType::BigInt(BigIntType::new()), &target).unwrap();
            let raw = cast(&out, &ArrowDataType::Int64).unwrap();
            let values: Vec<_> = raw
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect();
            assert_eq!(
                values,
                vec![
                    Some(-ticks),
                    Some(0),
                    Some(-2 * ticks),
                    Some(42 * ticks),
                    Some(-42 * ticks),
                    None
                ],
                "precision={precision}"
            );
        }
    }

    #[test]
    fn timestamp_to_bigint_matches_java_extreme_millis_round_trip() {
        for (local, expected) in [
            (
                false,
                vec![
                    Some(9_223_372_036_854_775),
                    Some(-9_223_372_036_854_776),
                    None,
                ],
            ),
            (
                true,
                vec![Some(-53_757_322_505_225), Some(53_757_322_505_224), None],
            ),
        ] {
            let source_type = if local {
                DataType::LocalZonedTimestamp(crate::spec::LocalZonedTimestampType::new(3).unwrap())
            } else {
                DataType::Timestamp(TimestampType::new(3).unwrap())
            };
            let raw: ArrayRef =
                Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(i64::MIN), None]));
            let source = cast(&raw, &paimon_type_to_arrow_for_read(&source_type).unwrap()).unwrap();
            let output =
                evolve_column(&source, &source_type, &DataType::BigInt(BigIntType::new())).unwrap();
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected,
                "local={local}"
            );
        }
    }

    #[test]
    fn bigint_to_timestamp_keeps_lenient_arrow_storage_overflow() {
        let source: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(9_223_372_036_854_775),
            Some(-9_223_372_036_854_775),
            None,
        ]));
        for (precision, expected) in [
            (
                0,
                vec![
                    Some(9_223_372_036_854_775),
                    Some(-9_223_372_036_854_775),
                    None,
                ],
            ),
            (
                3,
                vec![
                    Some(9_223_372_036_854_775_000),
                    Some(-9_223_372_036_854_775_000),
                    None,
                ],
            ),
            (6, vec![None, None, None]),
            (9, vec![None, None, None]),
        ] {
            let output = evolve_column(
                &source,
                &DataType::BigInt(BigIntType::new()),
                &DataType::Timestamp(TimestampType::new(precision).unwrap()),
            )
            .unwrap();
            let raw = cast(&output, &ArrowDataType::Int64).unwrap();
            assert_eq!(
                raw.as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected,
                "precision={precision}"
            );
        }
    }

    #[test]
    fn temporal_evolution_recurses_through_nullable_rows_lists_and_maps() {
        for reverse in [false, true] {
            let timestamp = DataType::Timestamp(TimestampType::new(6).unwrap());
            let bigint = DataType::BigInt(BigIntType::new());
            let (source_type, target_type, raw, expected) = if reverse {
                (
                    bigint,
                    timestamp,
                    vec![Some(42), Some(i64::MAX), None],
                    vec![Some(42_000_000), Some(-1_000_000), None],
                )
            } else {
                (
                    timestamp,
                    bigint,
                    vec![Some(42_000_001), Some(-1), None],
                    vec![Some(42), Some(-1), None],
                )
            };
            let raw: ArrayRef = Arc::new(Int64Array::from(raw));
            let leaf = cast(&raw, &paimon_type_to_arrow_for_read(&source_type).unwrap()).unwrap();
            let validity = Some(NullBuffer::from(vec![true, false, true]));
            let values = |array: &ArrayRef| {
                cast(array, &ArrowDataType::Int64)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            };

            let source_row = row(vec![field(11, "v", source_type.clone())]);
            let target_row = row(vec![field(11, "v", target_type.clone())]);
            let ArrowDataType::Struct(fields) = paimon_type_to_arrow_for_read(&source_row).unwrap()
            else {
                panic!()
            };
            let source: ArrayRef = Arc::new(
                StructArray::try_new(fields, vec![leaf.clone()], validity.clone()).unwrap(),
            );
            let out = evolve_column(&source, &source_row, &target_row).unwrap();
            let out = as_struct(&out);
            assert_eq!(out.nulls(), source.nulls());
            assert_eq!(
                values(out.column(0)),
                vec![expected[0], None, expected[2]],
                "ROW reverse={reverse}"
            );

            let source: ArrayRef = Arc::new(
                ListArray::try_new(
                    Arc::new(ArrowField::new("element", leaf.data_type().clone(), true)),
                    arrow_buffer::OffsetBuffer::new(vec![0, 2, 2, 3].into()),
                    leaf.clone(),
                    validity.clone(),
                )
                .unwrap(),
            );
            let out = evolve_column(
                &source,
                &DataType::Array(ArrayType::new(source_type.clone())),
                &DataType::Array(ArrayType::new(target_type.clone())),
            )
            .unwrap();
            let out = out.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(out.nulls(), source.nulls());
            assert_eq!(out.value_offsets(), &[0, 2, 2, 3]);
            assert_eq!(values(out.values()), expected, "ARRAY reverse={reverse}");

            let fields = Fields::from(vec![
                ArrowField::new("key", ArrowDataType::Utf8, false),
                ArrowField::new("value", leaf.data_type().clone(), true),
            ]);
            let entries = StructArray::try_new(
                fields.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["positive", "negative", "null"])) as ArrayRef,
                    leaf,
                ],
                None,
            )
            .unwrap();
            let source: ArrayRef = Arc::new(
                MapArray::try_new(
                    Arc::new(ArrowField::new(
                        "entries",
                        ArrowDataType::Struct(fields),
                        false,
                    )),
                    arrow_buffer::OffsetBuffer::new(vec![0, 2, 2, 3].into()),
                    entries,
                    validity,
                    false,
                )
                .unwrap(),
            );
            let out = evolve_column(
                &source,
                &DataType::Map(MapType::new(string_type(), source_type)),
                &DataType::Map(MapType::new(string_type(), target_type)),
            )
            .unwrap();
            let out = out.as_any().downcast_ref::<MapArray>().unwrap();
            assert_eq!(out.nulls(), source.nulls());
            assert_eq!(out.value_offsets(), &[0, 2, 2, 3]);
            assert_eq!(values(out.values()), expected, "MAP reverse={reverse}");
        }
    }

    #[test]
    fn evolves_int_epoch_seconds_to_timestamp_like_bigint() {
        // Java switches INTEGER and BIGINT together in the same cast rule.
        let secs = 1_700_000_000_i32;
        let source: ArrayRef = Arc::new(Int32Array::from(vec![Some(secs)]));
        let ts6 = DataType::Timestamp(TimestampType::new(6).unwrap());
        let out = evolve_column(&source, &DataType::Int(IntType::new()), &ts6).unwrap();
        let micros = out
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("microsecond timestamps");
        assert_eq!(micros.value(0), secs as i64 * 1_000_000);
    }

    #[test]
    fn preserves_null_rows() {
        let codec: ArrayRef = Arc::new(StringArray::from(vec![Some("h264"), None]));
        let fields = Fields::from(vec![ArrowField::new("codec", ArrowDataType::Utf8, true)]);
        let nulls = NullBuffer::from(vec![true, false]);
        let source: ArrayRef =
            Arc::new(StructArray::try_new(fields, vec![codec], Some(nulls)).unwrap());
        let source_type = row(vec![field(1, "codec", string_type())]);
        let target = row(vec![
            field(1, "codec", string_type()),
            field(3, "color_transfer", string_type()),
        ]);

        let out = evolve_column(&source, &source_type, &target).unwrap();

        assert_eq!(out.len(), 2);
        assert_eq!(out.null_count(), 1);
        assert!(out.is_valid(0));
        assert!(out.is_null(1));
    }

    #[test]
    fn casts_a_promoted_nested_leaf() {
        // `width` was promoted INT -> BIGINT.
        let target = row(vec![
            field(1, "codec", string_type()),
            field(2, "width", DataType::BigInt(BigIntType::new())),
        ]);

        let out = evolve_column(&source_struct(), &source_row(), &target).unwrap();

        let evolved = as_struct(&out);
        let widths = evolved
            .column_by_name("width")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        assert_eq!(widths.value(0), 1920);
        assert_eq!(widths.value(1), 3840);
    }

    #[test]
    fn returns_source_untouched_when_types_already_match() {
        let out = evolve_column(&source_struct(), &source_row(), &source_row()).unwrap();
        assert_eq!(out.data_type(), source_struct().data_type());
        assert_eq!(as_struct(&out).column_names(), vec!["codec", "width"]);
    }

    #[test]
    fn renders_constructed_values_as_strings_during_schema_evolution() {
        let source = source_struct();
        let out = evolve_column(&source, &source_row(), &string_type()).unwrap();
        assert_eq!(strings(&out).value(0), "{h264, 1920}");
        assert_eq!(strings(&out).value(1), "{h265, 3840}");

        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let list: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::new(ArrowField::new("element", ArrowDataType::Int32, true)),
                arrow_buffer::OffsetBuffer::new(vec![0, 2, 2, 3].into()),
                values,
                Some(NullBuffer::from(vec![true, false, true])),
            )
            .unwrap(),
        );
        let source_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));
        let out = evolve_column(&list, &source_type, &string_type()).unwrap();
        let rendered = strings(&out);
        assert_eq!(rendered.value(0), "[1, null]");
        assert!(rendered.is_null(1));
        assert_eq!(rendered.value(2), "[3]");

        let keys: ArrayRef = Arc::new(StringArray::from(vec!["k"]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(7)]));
        let entry_fields = Fields::from(vec![
            ArrowField::new("key", ArrowDataType::Utf8, false),
            ArrowField::new("value", ArrowDataType::Int32, true),
        ]);
        let entries = StructArray::try_new(entry_fields.clone(), vec![keys, values], None).unwrap();
        let map: ArrayRef = Arc::new(
            MapArray::try_new(
                Arc::new(ArrowField::new(
                    "entries",
                    ArrowDataType::Struct(entry_fields),
                    false,
                )),
                arrow_buffer::OffsetBuffer::new(vec![0, 1].into()),
                entries,
                None,
                false,
            )
            .unwrap(),
        );
        let source_type = DataType::Map(MapType::new(string_type(), DataType::Int(IntType::new())));
        let out = evolve_column(&map, &source_type, &string_type()).unwrap();
        assert_eq!(strings(&out).value(0), "{k -> 7}");
    }

    #[test]
    fn narrowing_integers_keeps_low_bits_like_java() {
        let source: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(128),
            Some(-129),
            Some(i64::MAX),
            Some(i64::MIN),
            None,
        ]));
        let target = DataType::TinyInt(crate::spec::TinyIntType::new());
        let output = evolve_column(&source, &DataType::BigInt(BigIntType::new()), &target).unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<arrow_array::Int8Array>()
                .unwrap(),
            &arrow_array::Int8Array::from(vec![Some(-128), Some(127), Some(-1), Some(0), None]),
        );
    }

    #[test]
    fn timestamp_precision_is_truncated_within_the_same_arrow_unit() {
        let source: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
            Some(123),
            Some(-1),
            Some(-123),
            None,
        ]));
        let output = evolve_column(
            &source,
            &DataType::Timestamp(TimestampType::new(3).unwrap()),
            &DataType::Timestamp(TimestampType::new(1).unwrap()),
        )
        .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap(),
            &TimestampMillisecondArray::from(vec![Some(100), Some(-100), Some(-200), None]),
        );
    }

    #[test]
    fn reducing_decimal_scale_rounds_half_up_for_both_signs() {
        let source: ArrayRef = Arc::new(
            Decimal128Array::from(vec![
                Some(4_567),
                Some(-4_567),
                Some(4_565),
                Some(-4_565),
                Some(4_564),
                Some(-4_564),
                None,
            ])
            .with_precision_and_scale(6, 3)
            .unwrap(),
        );
        let source_type = DataType::Decimal(DecimalType::new(6, 3).unwrap());
        let target_type = DataType::Decimal(DecimalType::new(6, 2).unwrap());
        let out = evolve_column(&source, &source_type, &target_type).unwrap();
        let decimals = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(decimals.data_type(), &ArrowDataType::Decimal128(6, 2));
        assert_eq!(decimals.value(0), 457);
        assert_eq!(decimals.value(1), -457);
        assert_eq!(decimals.value(2), 457);
        assert_eq!(decimals.value(3), -457);
        assert_eq!(decimals.value(4), 456);
        assert_eq!(decimals.value(5), -456);
        assert!(decimals.is_null(6));
        decimals.validate_decimal_precision(6).unwrap();
    }

    #[test]
    fn reducing_decimal_scale_nulls_target_precision_overflow() {
        let source: ArrayRef = Arc::new(
            Decimal128Array::from(vec![
                Some(9_994),
                Some(9_995),
                Some(-9_995),
                Some(999_999),
                None,
            ])
            .with_precision_and_scale(6, 3)
            .unwrap(),
        );
        let source_type = DataType::Decimal(DecimalType::new(6, 3).unwrap());
        let target_type = DataType::Decimal(DecimalType::new(3, 2).unwrap());
        let out = evolve_column(&source, &source_type, &target_type).unwrap();
        let decimals = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(decimals.data_type(), &ArrowDataType::Decimal128(3, 2));
        assert_eq!(decimals.value(0), 999);
        assert!(decimals.is_null(1));
        assert!(decimals.is_null(2));
        assert!(decimals.is_null(3));
        assert!(decimals.is_null(4));
        decimals.validate_decimal_precision(3).unwrap();
    }

    #[test]
    fn renders_nested_row_child_as_string_without_losing_parent_nulls() {
        let inner: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(ArrowField::new("a", ArrowDataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
        )]));
        let source: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![ArrowField::new(
                    "inner",
                    inner.data_type().clone(),
                    true,
                )]),
                vec![inner],
                Some(NullBuffer::from(vec![true, false])),
            )
            .unwrap(),
        );
        let source_type = row(vec![field(
            1,
            "inner",
            row(vec![field(2, "a", DataType::Int(IntType::new()))]),
        )]);
        let target_type = row(vec![field(1, "inner", string_type())]);
        let out = evolve_column(&source, &source_type, &target_type).unwrap();
        let outer = as_struct(&out);
        let rendered = strings(outer.column(0));
        assert_eq!(rendered.value(0), "{1}");
        assert!(outer.is_null(1));
    }

    #[test]
    fn fills_null_for_a_child_added_inside_an_array_element() {
        // source: ARRAY<ROW<codec>>, two elements in one list row.
        let codec: ArrayRef = Arc::new(StringArray::from(vec![Some("h264"), Some("h265")]));
        let element_fields =
            Fields::from(vec![ArrowField::new("codec", ArrowDataType::Utf8, true)]);
        let elements: ArrayRef =
            Arc::new(StructArray::try_new(element_fields.clone(), vec![codec], None).unwrap());
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 2].into());
        let source: ArrayRef = Arc::new(
            arrow_array::ListArray::try_new(
                Arc::new(ArrowField::new(
                    "element",
                    ArrowDataType::Struct(element_fields),
                    true,
                )),
                offsets,
                elements,
                None,
            )
            .unwrap(),
        );

        let source_type = DataType::Array(crate::spec::ArrayType::new(row(vec![field(
            1,
            "codec",
            string_type(),
        )])));
        let target_type = DataType::Array(crate::spec::ArrayType::new(row(vec![
            field(1, "codec", string_type()),
            field(3, "color_transfer", string_type()),
        ])));

        let out = evolve_column(&source, &source_type, &target_type).unwrap();

        assert_eq!(
            out.data_type(),
            &paimon_type_to_arrow_for_read(&target_type).unwrap()
        );
        let list = out
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap()
            .clone();
        assert_eq!(list.len(), 1);
        let evolved = as_struct(&list.values().clone());
        assert_eq!(evolved.column_names(), vec!["codec", "color_transfer"]);
        assert_eq!(
            strings(evolved.column_by_name("codec").unwrap()).value(1),
            "h265"
        );
        assert_eq!(
            evolved
                .column_by_name("color_transfer")
                .unwrap()
                .null_count(),
            2
        );
    }

    #[test]
    fn fills_null_for_a_child_added_inside_a_map_value() {
        // source: MAP<STRING, ROW<codec>> with one entry.
        let keys: ArrayRef = Arc::new(StringArray::from(vec![Some("k")]));
        let codec: ArrayRef = Arc::new(StringArray::from(vec![Some("h264")]));
        let value_fields = Fields::from(vec![ArrowField::new("codec", ArrowDataType::Utf8, true)]);
        let values: ArrayRef =
            Arc::new(StructArray::try_new(value_fields.clone(), vec![codec], None).unwrap());
        let entry_fields = Fields::from(vec![
            ArrowField::new("key", ArrowDataType::Utf8, false),
            ArrowField::new("value", ArrowDataType::Struct(value_fields.clone()), true),
        ]);
        let entries = StructArray::try_new(entry_fields.clone(), vec![keys, values], None).unwrap();
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 1].into());
        let source: ArrayRef = Arc::new(
            arrow_array::MapArray::try_new(
                Arc::new(ArrowField::new(
                    "entries",
                    ArrowDataType::Struct(entry_fields),
                    false,
                )),
                offsets,
                entries,
                None,
                false,
            )
            .unwrap(),
        );

        let source_type = DataType::Map(crate::spec::MapType::new(
            string_type(),
            row(vec![field(1, "codec", string_type())]),
        ));
        let target_type = DataType::Map(crate::spec::MapType::new(
            string_type(),
            row(vec![
                field(1, "codec", string_type()),
                field(3, "color_transfer", string_type()),
            ]),
        ));

        let out = evolve_column(&source, &source_type, &target_type).unwrap();

        assert_eq!(
            out.data_type(),
            &paimon_type_to_arrow_for_read(&target_type).unwrap()
        );
        let map = out
            .as_any()
            .downcast_ref::<arrow_array::MapArray>()
            .unwrap()
            .clone();
        assert_eq!(map.len(), 1);
        let evolved = as_struct(&map.values().clone());
        assert_eq!(evolved.column_names(), vec!["codec", "color_transfer"]);
        assert_eq!(
            evolved
                .column_by_name("color_transfer")
                .unwrap()
                .null_count(),
            1
        );
    }

    #[test]
    fn readded_child_with_the_same_name_does_not_serve_the_dropped_field_values() {
        // `media.x` (id 5) was dropped and re-added as a new field (id 9) with
        // the same name and type. The Arrow types are identical, but the old
        // file holds id 5, not id 9 — the read must not surface those values.
        let codec: ArrayRef = Arc::new(StringArray::from(vec![Some("h264"), Some("h265")]));
        let old_x: ArrayRef = Arc::new(StringArray::from(vec![Some("old1"), Some("old2")]));
        let source: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(ArrowField::new("codec", ArrowDataType::Utf8, true)),
                codec,
            ),
            (
                Arc::new(ArrowField::new("x", ArrowDataType::Utf8, true)),
                old_x,
            ),
        ]));
        let source_type = row(vec![
            field(1, "codec", string_type()),
            field(5, "x", string_type()),
        ]);
        let target = row(vec![
            field(1, "codec", string_type()),
            field(9, "x", string_type()),
        ]);

        let out = evolve_column(&source, &source_type, &target).unwrap();

        let evolved = as_struct(&out);
        let x = evolved.column_by_name("x").unwrap();
        assert_eq!(
            x.null_count(),
            2,
            "re-added field must read as NULL, got {:?}",
            strings(x)
        );
    }

    #[test]
    fn a_child_the_source_schema_declares_but_the_decoder_dropped_fails_loud() {
        // The file's schema says `media` has `codec` and `width`, but the decoded
        // struct only carries `codec`. That is not "the field was added later",
        // so it must not read back as NULL.
        let codec: ArrayRef = Arc::new(StringArray::from(vec![Some("h264"), Some("h265")]));
        let source: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(ArrowField::new("codec", ArrowDataType::Utf8, true)),
            codec,
        )]));

        let err = evolve_column(&source, &source_row(), &source_row()).unwrap_err();

        assert!(
            matches!(&err, crate::Error::DataInvalid { message, .. } if message.contains("width")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn readded_child_inside_a_multiset_does_not_serve_the_dropped_field_values() {
        // MULTISET<ROW<x>> where `x` was dropped and re-added with a new id.
        // A historical Parquet MULTISET decodes as Map<element, count>; the
        // current list representation must still reconcile the field ids.
        let x: ArrayRef = Arc::new(StringArray::from(vec![Some("old")]));
        let element_fields = Fields::from(vec![ArrowField::new("x", ArrowDataType::Utf8, true)]);
        let elements: ArrayRef =
            Arc::new(StructArray::try_new(element_fields.clone(), vec![x], None).unwrap());
        let counts: ArrayRef = Arc::new(Int32Array::from(vec![Some(1)]));
        let entry_fields = Fields::from(vec![
            ArrowField::new("key", ArrowDataType::Struct(element_fields.clone()), false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]);
        let entries =
            StructArray::try_new(entry_fields.clone(), vec![elements, counts], None).unwrap();
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 1].into());
        let source: ArrayRef = Arc::new(
            arrow_array::MapArray::try_new(
                Arc::new(ArrowField::new(
                    "entries",
                    ArrowDataType::Struct(entry_fields),
                    false,
                )),
                offsets,
                entries,
                None,
                false,
            )
            .unwrap(),
        );

        let source_type = DataType::Multiset(crate::spec::MultisetType::new(row(vec![field(
            5,
            "x",
            string_type(),
        )])));
        let target_type = DataType::Multiset(crate::spec::MultisetType::new(row(vec![field(
            9,
            "x",
            string_type(),
        )])));

        let out = evolve_column(&source, &source_type, &target_type).unwrap();

        let entries = crate::arrow::multiset::entries(out.as_ref(), 0).unwrap();
        let element = as_struct(entries.column(0));
        assert_eq!(
            element.column_by_name("x").unwrap().null_count(),
            1,
            "a re-added element field must read as NULL"
        );
    }

    #[test]
    fn casts_only_visible_list_elements_including_null_container_slots() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            "bad-prefix",
            "1.9",
            "bad-null",
            "-2.5",
            "bad-suffix",
        ]));
        let array: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::new(ArrowField::new("item", ArrowDataType::Utf8, true)),
                OffsetBuffer::new(vec![0i32, 1, 2, 3, 4, 5].into()),
                values,
                Some(NullBuffer::from(vec![true, true, false, true, true])),
            )
            .unwrap(),
        );
        let source_type = DataType::Array(ArrayType::new(DataType::VarChar(
            VarCharType::string_type(),
        )));
        let target_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));
        let output = evolve_column(&array.slice(1, 3), &source_type, &target_type).unwrap();
        let rows = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(rows.value_offsets(), &[0, 1, 1, 2]);
        assert_eq!(
            rows.values().as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![1, -2])
        );
        assert!(rows.is_null(1));
        assert!(evolve_column(&array.slice(0, 1), &source_type, &target_type).is_err());
    }

    #[test]
    fn casts_only_children_of_non_null_rows() {
        let child: ArrayRef = Arc::new(StringArray::from(vec!["1.9", "bad", "-2.5"]));
        let array: ArrayRef = Arc::new(
            StructArray::try_new(
                Fields::from(vec![ArrowField::new("old", ArrowDataType::Utf8, true)]),
                vec![child],
                Some(NullBuffer::from(vec![true, false, true])),
            )
            .unwrap(),
        );
        let output = evolve_column(
            &array,
            &row(vec![field(1, "old", string_type())]),
            &row(vec![field(1, "renamed", DataType::Int(IntType::new()))]),
        )
        .unwrap();
        let rows = as_struct(&output);
        assert_eq!(
            rows.column_by_name("renamed")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap(),
            &Int32Array::from(vec![Some(1), None, Some(-2)])
        );
        assert!(rows.is_null(1));
    }

    #[test]
    fn read_schema_allows_decimal_overflow_nulls_without_relaxing_writes() {
        let source_type = DataType::Array(ArrayType::new(DataType::Decimal(
            DecimalType::with_nullable(false, 6, 3).unwrap(),
        )));
        let target_type = DataType::Array(ArrayType::new(DataType::Decimal(
            DecimalType::with_nullable(false, 3, 2).unwrap(),
        )));
        let source: ArrayRef = Arc::new(
            ListArray::try_new(
                Arc::new(ArrowField::new(
                    "element",
                    ArrowDataType::Decimal128(6, 3),
                    false,
                )),
                OffsetBuffer::new(vec![0, 2].into()),
                Arc::new(
                    Decimal128Array::from(vec![9995, 1000])
                        .with_precision_and_scale(6, 3)
                        .unwrap(),
                ),
                None,
            )
            .unwrap(),
        );
        let output = evolve_column(&source, &source_type, &target_type).unwrap();
        output.to_data().validate_full().unwrap();
        let list = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(
            list.values()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, Some(100)]
        );
        let fields = vec![field(1, "value", target_type)];
        let read_schema = crate::arrow::build_read_arrow_schema(&fields).unwrap();
        arrow_array::RecordBatch::try_new(read_schema, vec![output]).unwrap();
        let write_schema = crate::arrow::build_target_arrow_schema(&fields).unwrap();
        let ArrowDataType::List(child) = write_schema.field(0).data_type() else {
            panic!()
        };
        assert!(!child.is_nullable());
    }

    #[test]
    fn assembled_variant_rows_normalize_runtime_nullability_and_large_strings() {
        let row = crate::spec::variant_extraction_row(
            true,
            vec![
                (
                    DataType::Int(IntType::with_nullable(false)),
                    "$.x".into(),
                    false,
                    "UTC".into(),
                ),
                (
                    DataType::VarChar(VarCharType::string_type()),
                    "$.s".into(),
                    false,
                    "UTC".into(),
                ),
            ],
        )
        .unwrap();
        let target = field(1, "payload", DataType::Row(row));
        let source_field = field(
            1,
            "payload",
            DataType::Variant(crate::spec::VariantType::new()),
        );
        let source: ArrayRef = Arc::new(
            StructArray::try_new(
                vec![
                    ArrowField::new("0", ArrowDataType::Int32, false),
                    ArrowField::new("1", ArrowDataType::LargeUtf8, true),
                ]
                .into(),
                vec![
                    Arc::new(Int32Array::from(vec![7])),
                    Arc::new(arrow_array::LargeStringArray::from(vec!["text"])),
                ],
                None,
            )
            .unwrap(),
        );
        for output in [
            evolve_column(&source, target.data_type(), target.data_type()).unwrap(),
            evolve_field(&source, &source_field, &target).unwrap(),
        ] {
            assert_eq!(
                output.data_type(),
                &paimon_type_to_arrow_for_read(target.data_type()).unwrap()
            );
            let rows = as_struct(&output);
            assert_eq!(
                rows.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                7
            );
            assert_eq!(
                rows.column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "text"
            );
        }
        let variant = crate::variant::GenericVariant::parse_json(r#"{"x":7,"s":"text"}"#).unwrap();
        let ArrowDataType::Struct(fields) = crate::arrow::variant_arrow_type() else {
            panic!()
        };
        let raw: ArrayRef = Arc::new(
            StructArray::try_new(
                fields,
                vec![
                    Arc::new(arrow_array::BinaryArray::from(vec![Some(variant.value())])),
                    Arc::new(arrow_array::BinaryArray::from(vec![Some(
                        variant.metadata(),
                    )])),
                ],
                None,
            )
            .unwrap(),
        );
        let fresh = evolve_field(&raw, &source_field, &target).unwrap();
        assert_eq!(
            fresh.data_type(),
            &paimon_type_to_arrow_for_read(target.data_type()).unwrap()
        );
    }

    #[test]
    fn casts_only_entries_of_non_null_maps() {
        let fields = Fields::from(vec![
            ArrowField::new("key", ArrowDataType::Utf8, false),
            ArrowField::new("value", ArrowDataType::Utf8, true),
        ]);
        let entries = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["one", "hidden", "two"])),
                Arc::new(StringArray::from(vec!["1.9", "bad", "-2.5"])),
            ],
            None,
        );
        let array: ArrayRef = Arc::new(
            MapArray::try_new(
                Arc::new(ArrowField::new(
                    "entries",
                    ArrowDataType::Struct(fields),
                    false,
                )),
                OffsetBuffer::new(vec![0i32, 1, 2, 3].into()),
                entries,
                Some(NullBuffer::from(vec![true, false, true])),
                false,
            )
            .unwrap(),
        );
        let from = DataType::Map(MapType::new(string_type(), string_type()));
        let to = DataType::Map(MapType::new(string_type(), DataType::Int(IntType::new())));
        let output = evolve_column(&array, &from, &to).unwrap();
        let rows = output.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(rows.value_offsets(), &[0, 1, 1, 2]);
        assert_eq!(
            rows.values().as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![1, -2])
        );
        assert!(rows.is_null(1));
    }

    #[test]
    fn renders_large_list_strings_in_sliced_containers() {
        let array: ArrayRef = Arc::new(
            arrow_array::LargeListArray::try_new(
                Arc::new(ArrowField::new("item", ArrowDataType::Int32, true)),
                arrow_buffer::OffsetBuffer::new(vec![0i64, 1, 3, 3, 4, 5].into()),
                Arc::new(Int32Array::from(vec![
                    Some(9),
                    Some(1),
                    None,
                    Some(7),
                    Some(8),
                ])),
                Some(arrow_buffer::NullBuffer::from(vec![
                    true, true, true, false, true,
                ])),
            )
            .unwrap(),
        );
        let array_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));
        let output = evolve_column(&array.slice(1, 3), &array_type, &string_type()).unwrap();
        assert_eq!(
            strings(&output),
            StringArray::from(vec![Some("[1, null]"), Some("[]"), None])
        );

        let source: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![ArrowField::new("a", array.data_type().clone(), true)]),
            vec![array],
            None,
        ));
        let output = evolve_column(
            &source.slice(1, 3),
            &row(vec![field(4, "a", array_type)]),
            &string_type(),
        )
        .unwrap();
        assert_eq!(
            strings(&output),
            StringArray::from(vec!["{[1, null]}", "{[]}", "{null}"])
        );
    }

    #[test]
    fn evolves_a_large_list_element() {
        // A format table's embedded Arrow schema can decode a Paimon ARRAY as a
        // LargeList; the logical types still have to reconcile.
        let codec: ArrayRef = Arc::new(StringArray::from(vec![Some("h264"), Some("h265")]));
        let element_fields =
            Fields::from(vec![ArrowField::new("codec", ArrowDataType::Utf8, true)]);
        let elements: ArrayRef =
            Arc::new(StructArray::try_new(element_fields.clone(), vec![codec], None).unwrap());
        let source: ArrayRef = Arc::new(
            arrow_array::LargeListArray::try_new(
                Arc::new(ArrowField::new(
                    "element",
                    ArrowDataType::Struct(element_fields),
                    true,
                )),
                arrow_buffer::OffsetBuffer::new(vec![0i64, 2].into()),
                elements,
                None,
            )
            .unwrap(),
        );
        let source_type = DataType::Array(crate::spec::ArrayType::new(row(vec![field(
            1,
            "codec",
            string_type(),
        )])));
        let target_type = DataType::Array(crate::spec::ArrayType::new(row(vec![
            field(1, "codec", string_type()),
            field(3, "color_transfer", string_type()),
        ])));

        let out = evolve_column(&source, &source_type, &target_type).unwrap();

        assert_eq!(
            out.data_type(),
            &paimon_type_to_arrow_for_read(&target_type).unwrap()
        );
        let list = out
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap()
            .clone();
        let evolved = as_struct(&list.values().clone());
        assert_eq!(
            strings(evolved.column_by_name("codec").unwrap()).value(1),
            "h265"
        );
        assert_eq!(
            evolved
                .column_by_name("color_transfer")
                .unwrap()
                .null_count(),
            2
        );
    }

    #[test]
    fn reconciles_a_fixed_size_list_layout() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]));
        let source: ArrayRef = Arc::new(
            arrow_array::FixedSizeListArray::try_new(
                Arc::new(ArrowField::new("item", ArrowDataType::Int32, true)),
                2,
                values,
                None,
            )
            .unwrap(),
        );
        let source_type =
            DataType::Array(crate::spec::ArrayType::new(DataType::Int(IntType::new())));

        let out = evolve_column(&source, &source_type, &source_type).unwrap();

        assert_eq!(
            out.data_type(),
            &paimon_type_to_arrow_for_read(&source_type).unwrap(),
            "the layout must be normalized to the Paimon ARRAY's Arrow type"
        );
        assert_eq!(out.len(), 2);
    }
}
