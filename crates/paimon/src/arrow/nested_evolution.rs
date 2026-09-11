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

use arrow_array::{new_null_array, Array, ArrayRef, ListArray, MapArray, StructArray};
use arrow_cast::cast;
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Fields};

use crate::arrow::paimon_type_to_arrow;
use crate::spec::{is_variant_extraction_row_type, DataType, MapType, RowType};

/// Reconcile `source` (as described by `source_type`, the type the data file
/// actually holds) with `target_type` (the type the read schema wants).
///
/// Recurses through ROW so an added, dropped, renamed or promoted nested field
/// is handled at any depth. Non-nested mismatches fall back to Arrow `cast`,
/// mirroring the top-level promotion path.
pub(crate) fn evolve_column(
    source: &ArrayRef,
    source_type: &DataType,
    target_type: &DataType,
) -> crate::Result<ArrayRef> {
    let target_arrow = paimon_type_to_arrow(target_type)?;
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

    match (target_type, source_type) {
        // A variant-extraction ROW is synthetic: its fields are numbered by
        // position, not by schema field id, so pairing them by id would mix
        // columns up. `prune_data_type` keeps such a row verbatim, so there is
        // nothing to evolve — leave it to the cast below, as before.
        (DataType::Row(_), DataType::Row(_))
            if is_variant_extraction_row_type(target_type)
                || is_variant_extraction_row_type(source_type) => {}
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

    cast(source, &target_arrow).map_err(|e| crate::Error::UnexpectedError {
        message: format!(
            "failed to cast nested value from {:?} to {:?} during schema evolution",
            source.data_type(),
            target_arrow
        ),
        source: Some(Box::new(e)),
    })
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
        let target_arrow = paimon_type_to_arrow(target_field.data_type())?;
        fields.push(Arc::new(ArrowField::new(
            target_field.name(),
            target_arrow.clone(),
            target_field.data_type().is_nullable(),
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
                arrays.push(evolve_column(
                    column,
                    source_field.data_type(),
                    target_field.data_type(),
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
    // A Paimon ARRAY converts to an Arrow `List`, but a decoded column can arrive
    // in another list layout — an external Parquet file's embedded Arrow schema
    // may yield `LargeList` or `FixedSizeList`. Normalize the layout with Arrow's
    // own cast (which range-checks large offsets) before reconciling the element,
    // so those files stay readable.
    let normalized;
    let list = match source.as_any().downcast_ref::<ListArray>() {
        Some(list) => list,
        None => {
            let source_layout = ArrowDataType::List(Arc::new(ArrowField::new(
                "element",
                paimon_type_to_arrow(source_element)?,
                source_element.is_nullable(),
            )));
            normalized = cast(source, &source_layout).map_err(|e| crate::Error::DataInvalid {
                message: format!(
                    "expected a list array for ARRAY schema evolution, got {:?}, which does not convert to {source_layout:?}: {e}",
                    source.data_type()
                ),
                source: None,
            })?;
            normalized
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!(
                        "normalizing {:?} did not yield a list array",
                        source.data_type()
                    ),
                    source: None,
                })?
        }
    };

    let values = evolve_column(list.values(), source_element, target_element)?;
    let element_field = Arc::new(ArrowField::new(
        "element",
        paimon_type_to_arrow(target_element)?,
        target_element.is_nullable(),
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

/// Rebuild a MAP whose key or value type evolved, reusing the source offsets and
/// validity. Mirrors Java `SchemaEvolutionUtil.createMapCastExecutor`.
fn evolve_map(
    source: &ArrayRef,
    source_map: &MapType,
    target_map: &MapType,
) -> crate::Result<ArrayRef> {
    let map = as_map_array(source, "MAP")?;
    let keys = evolve_column(map.keys(), source_map.key_type(), target_map.key_type())?;
    let values = evolve_column(
        map.values(),
        source_map.value_type(),
        target_map.value_type(),
    )?;
    rebuild_map(
        map,
        keys,
        values,
        ArrowField::new("key", paimon_type_to_arrow(target_map.key_type())?, false),
        ArrowField::new(
            "value",
            paimon_type_to_arrow(target_map.value_type())?,
            target_map.value_type().is_nullable(),
        ),
    )
}

/// Rebuild a MULTISET whose element type evolved. Arrow renders a MULTISET as a
/// map of element -> count, so only the key side is reconciled and the counts
/// carry over untouched.
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
    let map = as_map_array(source, "MULTISET")?;
    let elements = evolve_column(map.keys(), source_element, target_element)?;
    let counts = map.values().clone();
    rebuild_map(
        map,
        elements,
        counts,
        ArrowField::new(
            "key",
            paimon_type_to_arrow(target_element)?,
            target_element.is_nullable(),
        ),
        ArrowField::new("value", ArrowDataType::Int32, false),
    )
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
    use crate::spec::{BigIntType, DataField, IntType, VarCharType};
    use arrow_array::{Int32Array, Int64Array, StringArray};
    use arrow_buffer::NullBuffer;
    use arrow_schema::{DataType as ArrowDataType, Fields};

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
        assert_eq!(out.data_type(), &paimon_type_to_arrow(&target).unwrap());
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
            &paimon_type_to_arrow(&target_type).unwrap()
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
            &paimon_type_to_arrow(&target_type).unwrap()
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
        // Arrow renders a MULTISET as a Map of element -> count, so the source
        // and target Arrow types are identical and only the field ids differ.
        let x: ArrayRef = Arc::new(StringArray::from(vec![Some("old")]));
        let element_fields = Fields::from(vec![ArrowField::new("x", ArrowDataType::Utf8, true)]);
        let elements: ArrayRef =
            Arc::new(StructArray::try_new(element_fields.clone(), vec![x], None).unwrap());
        let counts: ArrayRef = Arc::new(Int32Array::from(vec![Some(1)]));
        let entry_fields = Fields::from(vec![
            ArrowField::new("key", ArrowDataType::Struct(element_fields.clone()), true),
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

        let map = out
            .as_any()
            .downcast_ref::<arrow_array::MapArray>()
            .unwrap()
            .clone();
        let element = as_struct(&map.keys().clone());
        assert_eq!(
            element.column_by_name("x").unwrap().null_count(),
            1,
            "a re-added element field must read as NULL"
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
            &paimon_type_to_arrow(&target_type).unwrap()
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
            &paimon_type_to_arrow(&source_type).unwrap(),
            "the layout must be normalized to the Paimon ARRAY's Arrow type"
        );
        assert_eq!(out.len(), 2);
    }
}
