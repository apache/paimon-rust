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

use crate::spec::{DataField, DataType};
use arrow_schema::{DataType as ArrowDataType, FieldRef, Schema, SchemaRef};
use std::sync::Arc;

const FIELD_ID_KEY: &str = "PARQUET:field_id";

/// Annotate only physical columns containing shredded VARIANTs. Runtime Arrow
/// schemas retain their existing convention; the Parquet boundary rebinds the
/// arrays to this file schema. Java's schema converter requires nested IDs.
pub(super) fn with_variant_field_ids(
    schema: &SchemaRef,
    fields: &[DataField],
    logical_fields: &[DataField],
) -> SchemaRef {
    let mut changed = false;
    let arrow_fields = schema
        .fields()
        .iter()
        .map(|arrow| {
            let Some(field) = fields.iter().find(|field| field.name() == arrow.name()) else {
                return arrow.clone();
            };
            let Some(logical) = logical_fields
                .iter()
                .find(|logical| logical.name() == field.name())
            else {
                return arrow.clone();
            };
            if !contains_variant(logical.data_type()) {
                return arrow.clone();
            }
            changed = true;
            annotated_column(arrow, field, logical)
        })
        .collect::<Vec<_>>();
    if changed {
        Arc::new(Schema::new_with_metadata(
            arrow_fields,
            schema.metadata().clone(),
        ))
    } else {
        schema.clone()
    }
}

fn contains_variant(data_type: &DataType) -> bool {
    match data_type {
        DataType::Variant(_) => true,
        DataType::Row(row) => row
            .fields()
            .iter()
            .any(|field| contains_variant(field.data_type())),
        _ => false,
    }
}

fn annotated_column(arrow: &FieldRef, physical: &DataField, logical: &DataField) -> FieldRef {
    let data_type = match (arrow.data_type(), physical.data_type(), logical.data_type()) {
        (_, _, DataType::Variant(_)) => {
            annotated_type(arrow.data_type(), physical.data_type(), physical.id(), 0)
        }
        (ArrowDataType::Struct(children), DataType::Row(physical), DataType::Row(logical)) => {
            ArrowDataType::Struct(
                children
                    .iter()
                    .map(|child| {
                        let Some(logical) = logical
                            .fields()
                            .iter()
                            .find(|field| field.name() == child.name())
                        else {
                            return child.clone();
                        };
                        if !contains_variant(logical.data_type()) {
                            return child.clone();
                        }
                        let Some(physical) = physical
                            .fields()
                            .iter()
                            .find(|field| field.name() == child.name())
                        else {
                            return child.clone();
                        };
                        annotated_column(child, physical, logical)
                    })
                    .collect::<Vec<_>>()
                    .into(),
            )
        }
        _ => return arrow.clone(),
    };
    with_id(
        &Arc::new(arrow.as_ref().clone().with_data_type(data_type)),
        physical.id(),
    )
}

fn with_id(field: &FieldRef, id: i32) -> FieldRef {
    let mut metadata = field.metadata().clone();
    metadata.insert(FIELD_ID_KEY.into(), id.to_string());
    Arc::new(field.as_ref().clone().with_metadata(metadata))
}

fn annotated_type(
    arrow: &ArrowDataType,
    data_type: &DataType,
    field_id: i32,
    depth: i32,
) -> ArrowDataType {
    match (arrow, data_type) {
        (ArrowDataType::Struct(children), DataType::Row(row)) => ArrowDataType::Struct(
            children
                .iter()
                .map(|child| {
                    let Some(field) = row
                        .fields()
                        .iter()
                        .find(|field| field.name() == child.name())
                    else {
                        return child.clone();
                    };
                    with_id(
                        &Arc::new(child.as_ref().clone().with_data_type(annotated_type(
                            child.data_type(),
                            field.data_type(),
                            field.id(),
                            0,
                        ))),
                        field.id(),
                    )
                })
                .collect::<Vec<_>>()
                .into(),
        ),
        (ArrowDataType::List(element), DataType::Array(array)) => {
            // Java SpecialFields.getArrayElementFieldId. Nested ROW children
            // reset depth; consecutive ARRAY levels retain the parent field ID.
            let next_depth = depth + 1;
            let id = (i32::MAX / 4)
                .wrapping_add(field_id.wrapping_mul(1 << 10))
                .wrapping_add(next_depth);
            let element = Arc::new(element.as_ref().clone().with_data_type(annotated_type(
                element.data_type(),
                array.element_type(),
                field_id,
                next_depth,
            )));
            ArrowDataType::List(with_id(&element, id))
        }
        _ => arrow.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::{arrow_to_paimon_type, build_target_arrow_schema};
    use crate::spec::{RowType, VarBinaryType, VariantType};

    #[test]
    fn ordinary_value_metadata_rows_and_siblings_keep_their_types() {
        let binary = DataType::VarBinary(VarBinaryType::default())
            .copy_with_nullable(false)
            .unwrap();
        let plain = DataField::new(
            7,
            "plain".into(),
            DataType::Row(RowType::new(vec![
                DataField::new(0, "value".into(), binary.clone()),
                DataField::new(1, "metadata".into(), binary),
            ])),
        );
        let schema = build_target_arrow_schema(std::slice::from_ref(&plain)).unwrap();
        let unchanged = with_variant_field_ids(
            &schema,
            std::slice::from_ref(&plain),
            std::slice::from_ref(&plain),
        );
        assert_eq!(unchanged, schema);
        assert!(matches!(
            arrow_to_paimon_type(unchanged.field(0).data_type(), true).unwrap(),
            DataType::Row(_)
        ));

        let logical = DataField::new(
            8,
            "container".into(),
            DataType::Row(RowType::new(vec![
                plain.clone(),
                DataField::new(9, "v".into(), DataType::Variant(VariantType::new())),
            ])),
        );
        let physical = DataField::new(
            8,
            "container".into(),
            DataType::Row(RowType::new(vec![
                plain,
                DataField::new(
                    9,
                    "v".into(),
                    crate::variant::variant_shredding_type(&DataType::Variant(VariantType::new()))
                        .unwrap(),
                ),
            ])),
        );
        let schema = build_target_arrow_schema(std::slice::from_ref(&physical)).unwrap();
        let annotated = with_variant_field_ids(&schema, &[physical], &[logical]);
        let ArrowDataType::Struct(children) = annotated.field(0).data_type() else {
            unreachable!()
        };
        let ArrowDataType::Struct(original) = schema.field(0).data_type() else {
            unreachable!()
        };
        assert_eq!(children[0], original[0]);
        assert!(matches!(
            arrow_to_paimon_type(children[0].data_type(), true).unwrap(),
            DataType::Row(_)
        ));
        let ArrowDataType::Struct(variant) = children[1].data_type() else {
            unreachable!()
        };
        assert_eq!(variant[0].metadata()[FIELD_ID_KEY], "0");
        assert_eq!(variant[1].metadata()[FIELD_ID_KEY], "1");
    }
}
