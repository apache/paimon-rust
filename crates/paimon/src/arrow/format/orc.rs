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

use super::{FilePredicates, FormatFileReader};
use crate::io::FileRead;
use crate::spec::{DataField, DataType, Datum, Predicate, PredicateOperator};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::{future::BoxFuture, StreamExt};
use orc_rust::predicate::PredicateValue;
use orc_rust::projection::ProjectionMask;
use orc_rust::reader::AsyncChunkReader;
use orc_rust::ArrowReaderBuilder;

const ORC_IN_PREDICATE_MAX_LITERALS: usize = 20;

pub(crate) struct OrcFormatReader;

#[async_trait]
impl FormatFileReader for OrcFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let orc_reader = OrcFileReader::new(file_size, reader);

        let builder = ArrowReaderBuilder::try_new_async(orc_reader)
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to open ORC file: {e}"),
                source: Some(Box::new(e)),
            })?;

        let mut projected_names: Vec<String> =
            read_fields.iter().map(|f| f.name().to_string()).collect();
        let orc_predicate = build_orc_predicate(predicates);
        if let Some(ref predicate) = orc_predicate {
            collect_orc_predicate_columns(predicate, &mut projected_names);
        }
        let projection =
            ProjectionMask::named_roots(builder.file_metadata().root_data_type(), &projected_names);

        let mut builder = builder.with_projection(projection);

        if let Some(predicate) = orc_predicate {
            builder = builder.with_predicate(predicate);
        }

        if let Some(size) = batch_size {
            builder = builder.with_batch_size(size);
        }

        if let Some(ref ranges) = row_selection {
            let total_rows: usize = builder
                .file_metadata()
                .stripe_metadatas()
                .iter()
                .map(|s| s.number_of_rows() as usize)
                .sum();
            let selection = build_range_row_selection(total_rows, ranges);
            builder = builder.with_row_selection(selection);
        }

        let stream = builder.build_async();
        let requested_names: Vec<String> =
            read_fields.iter().map(|f| f.name().to_string()).collect();
        Ok(stream
            .map(move |r| {
                let batch = r.map_err(|e| Error::UnexpectedError {
                    message: format!("ORC read error: {e}"),
                    source: Some(Box::new(e)),
                })?;
                project_orc_batch_to_requested_fields(batch, &requested_names)
            })
            .boxed())
    }
}

// ---------------------------------------------------------------------------
// Paimon predicates → orc-rust conservative row-group predicates.
//
// orc-rust evaluates these predicates against row-group statistics and may keep
// non-matching rows from a selected row group. Exact residual filtering remains
// the caller's responsibility.
// ---------------------------------------------------------------------------

fn build_orc_predicate(
    predicates: Option<&FilePredicates>,
) -> Option<orc_rust::predicate::Predicate> {
    let predicates = predicates?;
    let mut orc_predicates = Vec::new();
    for predicate in &predicates.predicates {
        if let Some(predicate) = build_orc_predicate_inner(
            predicate,
            &predicates.file_fields,
            CompoundPredicateMode::RootAnd,
        ) {
            orc_predicates.push(predicate);
        }
    }

    match orc_predicates.len() {
        0 => None,
        1 => orc_predicates.pop(),
        _ => Some(orc_rust::predicate::Predicate::and(orc_predicates)),
    }
}

fn build_orc_predicate_inner(
    predicate: &Predicate,
    file_fields: &[DataField],
    mode: CompoundPredicateMode,
) -> Option<orc_rust::predicate::Predicate> {
    match predicate {
        Predicate::Leaf { .. } => build_orc_leaf_predicate(predicate, file_fields),
        Predicate::And(children) => build_orc_and_predicate(children, file_fields, mode),
        Predicate::Or(children) => build_orc_or_predicate(children, file_fields),
        Predicate::AlwaysTrue => None,
        Predicate::AlwaysFalse | Predicate::Not(_) => None,
    }
}

#[derive(Clone, Copy)]
enum CompoundPredicateMode {
    RootAnd,
    RequireExact,
}

fn build_orc_and_predicate(
    children: &[Predicate],
    file_fields: &[DataField],
    mode: CompoundPredicateMode,
) -> Option<orc_rust::predicate::Predicate> {
    let require_exact = matches!(mode, CompoundPredicateMode::RequireExact);

    let mut converted = Vec::with_capacity(children.len());
    for child in children {
        match build_orc_predicate_inner(child, file_fields, CompoundPredicateMode::RootAnd) {
            Some(predicate) => converted.push(predicate),
            None if require_exact => return None,
            None => {}
        }
    }

    match converted.len() {
        0 => None,
        1 => converted.pop(),
        _ => Some(orc_rust::predicate::Predicate::and(converted)),
    }
}

fn build_orc_or_predicate(
    children: &[Predicate],
    file_fields: &[DataField],
) -> Option<orc_rust::predicate::Predicate> {
    let mut converted = Vec::with_capacity(children.len());
    for child in children {
        converted.push(build_orc_predicate_inner(
            child,
            file_fields,
            CompoundPredicateMode::RequireExact,
        )?);
    }

    match converted.len() {
        0 => None,
        1 => converted.pop(),
        _ => Some(orc_rust::predicate::Predicate::or(converted)),
    }
}

fn build_orc_leaf_predicate(
    predicate: &Predicate,
    file_fields: &[DataField],
) -> Option<orc_rust::predicate::Predicate> {
    let Predicate::Leaf {
        index,
        op,
        literals,
        ..
    } = predicate
    else {
        return None;
    };
    let file_field = file_fields.get(*index)?;
    let column = file_field.name();

    match op {
        PredicateOperator::IsNull | PredicateOperator::IsNotNull
            if data_type_supported_for_orc_predicate(file_field.data_type()) =>
        {
            Some(match op {
                PredicateOperator::IsNull => orc_rust::predicate::Predicate::is_null(column),
                PredicateOperator::IsNotNull => orc_rust::predicate::Predicate::is_not_null(column),
                _ => unreachable!(),
            })
        }
        PredicateOperator::Eq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq => {
            if *op == PredicateOperator::Eq
                && matches!(
                    file_field.data_type(),
                    DataType::Float(_) | DataType::Double(_)
                )
            {
                return None;
            }
            let literal = literals.first()?;
            let value = datum_to_orc_value(literal, file_field.data_type())?;
            Some(match op {
                PredicateOperator::Eq => orc_rust::predicate::Predicate::eq(column, value),
                PredicateOperator::Lt => orc_rust::predicate::Predicate::lt(column, value),
                PredicateOperator::LtEq => orc_rust::predicate::Predicate::lte(column, value),
                PredicateOperator::Gt => orc_rust::predicate::Predicate::gt(column, value),
                PredicateOperator::GtEq => orc_rust::predicate::Predicate::gte(column, value),
                _ => unreachable!(),
            })
        }
        PredicateOperator::In => {
            if literals.is_empty() || literals.len() > ORC_IN_PREDICATE_MAX_LITERALS {
                return None;
            }
            let mut values = Vec::with_capacity(literals.len());
            for literal in literals {
                values.push(orc_rust::predicate::Predicate::eq(
                    column,
                    datum_to_orc_value(literal, file_field.data_type())?,
                ));
            }
            Some(orc_rust::predicate::Predicate::or(values))
        }
        PredicateOperator::IsNull
        | PredicateOperator::IsNotNull
        | PredicateOperator::NotEq
        | PredicateOperator::NotIn => None,
        // String/range ops are not pushed into ORC; returning None falls open to
        // the outer stats-prune + arrow row-filter path.
        PredicateOperator::StartsWith
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Like
        | PredicateOperator::Between
        | PredicateOperator::NotBetween => None,
    }
}

fn data_type_supported_for_orc_predicate(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean(_)
            | DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::BigInt(_)
            | DataType::Float(_)
            | DataType::Double(_)
            | DataType::Char(_)
            | DataType::VarChar(_)
    )
}

fn datum_to_orc_value(datum: &Datum, data_type: &DataType) -> Option<PredicateValue> {
    match (datum, data_type) {
        (Datum::Bool(value), DataType::Boolean(_)) => Some(PredicateValue::Boolean(Some(*value))),
        (Datum::TinyInt(value), DataType::TinyInt(_)) => Some(PredicateValue::Int8(Some(*value))),
        (Datum::SmallInt(value), DataType::SmallInt(_)) => {
            Some(PredicateValue::Int16(Some(*value)))
        }
        (Datum::Int(value), DataType::Int(_)) => Some(PredicateValue::Int32(Some(*value))),
        (Datum::Long(value), DataType::BigInt(_)) => Some(PredicateValue::Int64(Some(*value))),
        (Datum::Float(value), DataType::Float(_)) => Some(PredicateValue::Float32(Some(*value))),
        (Datum::Double(value), DataType::Double(_)) => Some(PredicateValue::Float64(Some(*value))),
        (Datum::String(value), DataType::Char(_) | DataType::VarChar(_)) => {
            Some(PredicateValue::Utf8(Some(value.clone())))
        }
        _ => None,
    }
}

fn collect_orc_predicate_columns(
    predicate: &orc_rust::predicate::Predicate,
    projected_names: &mut Vec<String>,
) {
    collect_orc_predicate_columns_inner(predicate, projected_names);
}

fn collect_orc_predicate_columns_inner(
    predicate: &orc_rust::predicate::Predicate,
    projected_names: &mut Vec<String>,
) {
    match predicate {
        orc_rust::predicate::Predicate::Comparison { column, .. }
        | orc_rust::predicate::Predicate::IsNull { column }
        | orc_rust::predicate::Predicate::IsNotNull { column } => {
            if !projected_names.iter().any(|name| name == column) {
                projected_names.push(column.clone());
            }
        }
        orc_rust::predicate::Predicate::And(children)
        | orc_rust::predicate::Predicate::Or(children) => {
            for child in children {
                collect_orc_predicate_columns_inner(child, projected_names);
            }
        }
        orc_rust::predicate::Predicate::Not(child) => {
            collect_orc_predicate_columns_inner(child, projected_names);
        }
    }
}

fn project_orc_batch_to_requested_fields(
    batch: RecordBatch,
    requested_names: &[String],
) -> crate::Result<RecordBatch> {
    let indices: Vec<usize> = requested_names
        .iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map_err(|e| Error::UnexpectedError {
                    message: format!("ORC batch is missing requested column '{name}': {e}"),
                    source: Some(Box::new(e)),
                })
        })
        .collect::<crate::Result<_>>()?;
    batch.project(&indices).map_err(|e| Error::UnexpectedError {
        message: format!("Failed to project ORC batch: {e}"),
        source: Some(Box::new(e)),
    })
}

// ---------------------------------------------------------------------------
// Row ranges → orc_rust::RowSelection
// ---------------------------------------------------------------------------

fn build_range_row_selection(
    total_rows: usize,
    row_ranges: &[RowRange],
) -> orc_rust::row_selection::RowSelection {
    if total_rows == 0 {
        return orc_rust::row_selection::RowSelection::default();
    }

    let file_end = total_rows as i64 - 1;
    let mut local_ranges: Vec<(usize, usize)> = row_ranges
        .iter()
        .filter_map(|r| {
            if r.to() < 0 || r.from() > file_end {
                return None;
            }
            let local_start = r.from().max(0) as usize;
            let local_end = (r.to().min(file_end) + 1) as usize;
            Some((local_start, local_end))
        })
        .collect();
    local_ranges.sort_by_key(|&(s, _)| s);

    orc_rust::row_selection::RowSelection::from_consecutive_ranges(
        local_ranges.into_iter().map(|(s, e)| s..e),
        total_rows,
    )
}

// ---------------------------------------------------------------------------
// OrcFileReader — adapts paimon FileRead to orc-rust AsyncChunkReader
// ---------------------------------------------------------------------------

struct OrcFileReader {
    file_size: u64,
    r: Box<dyn FileRead>,
}

impl OrcFileReader {
    fn new(file_size: u64, r: Box<dyn FileRead>) -> Self {
        Self { file_size, r }
    }
}

impl AsyncChunkReader for OrcFileReader {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        Box::pin(std::future::ready(Ok(self.file_size)))
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        Box::pin(async move {
            self.r
                .read(offset_from_start..offset_from_start + length)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};
    use orc_rust::row_selection::RowSelector;
    use std::sync::Arc;

    use crate::spec::{DateType, DecimalType, DoubleType, FloatType, IntType};

    fn field(index: i32, name: &str, data_type: DataType) -> DataField {
        DataField::new(index, name.to_string(), data_type)
    }

    fn leaf(index: usize, op: PredicateOperator, literals: Vec<Datum>) -> Predicate {
        Predicate::Leaf {
            column: format!("c{index}"),
            index,
            data_type: DataType::Int(IntType::new()),
            op,
            literals,
        }
    }

    fn file_predicates(predicates: Vec<Predicate>, file_fields: Vec<DataField>) -> FilePredicates {
        FilePredicates {
            predicates,
            file_fields,
        }
    }

    #[test]
    fn test_build_range_row_selection_single_range() {
        let ranges = vec![RowRange::new(2, 4)];
        let sel = build_range_row_selection(6, &ranges);
        // rows 0,1 skip; 2,3,4 select; 5 skip
        let expected: orc_rust::row_selection::RowSelection = vec![
            RowSelector::skip(2),
            RowSelector::select(3),
            RowSelector::skip(1),
        ]
        .into();
        assert_eq!(sel, expected);
    }

    #[test]
    fn test_build_range_row_selection_with_offset() {
        let ranges = vec![RowRange::new(1, 3)];
        let sel = build_range_row_selection(5, &ranges);
        let expected: orc_rust::row_selection::RowSelection = vec![
            RowSelector::skip(1),
            RowSelector::select(3),
            RowSelector::skip(1),
        ]
        .into();
        assert_eq!(sel, expected);
    }

    #[test]
    fn test_build_range_row_selection_out_of_file() {
        let ranges = vec![RowRange::new(10, 20)];
        let sel = build_range_row_selection(5, &ranges);
        let expected: orc_rust::row_selection::RowSelection = vec![RowSelector::skip(5)].into();
        assert_eq!(sel, expected);
    }

    #[test]
    fn test_build_orc_predicate_supported_leaf() {
        let predicates = file_predicates(
            vec![leaf(0, PredicateOperator::GtEq, vec![Datum::Int(7)])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        let predicate = build_orc_predicate(Some(&predicates)).unwrap();
        assert_eq!(
            predicate,
            orc_rust::predicate::Predicate::gte("id", PredicateValue::Int32(Some(7)))
        );
    }

    #[test]
    fn test_build_orc_predicate_type_mismatch_fails_open() {
        let predicates = file_predicates(
            vec![leaf(0, PredicateOperator::Eq, vec![Datum::Long(7)])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_unsupported_type_fails_open() {
        let predicates = file_predicates(
            vec![Predicate::Leaf {
                column: "dt".to_string(),
                index: 0,
                data_type: DataType::Date(DateType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Date(1)],
            }],
            vec![field(0, "dt", DataType::Date(DateType::new()))],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_unsupported_operator_fails_open() {
        let predicates = file_predicates(
            vec![leaf(0, PredicateOperator::NotEq, vec![Datum::Int(7)])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_float_eq_fails_open() {
        let float_predicates = file_predicates(
            vec![Predicate::Leaf {
                column: "f".to_string(),
                index: 0,
                data_type: DataType::Float(FloatType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Float(1.5)],
            }],
            vec![field(0, "f", DataType::Float(FloatType::new()))],
        );
        let double_predicates = file_predicates(
            vec![Predicate::Leaf {
                column: "d".to_string(),
                index: 0,
                data_type: DataType::Double(DoubleType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Double(2.5)],
            }],
            vec![field(0, "d", DataType::Double(DoubleType::new()))],
        );

        assert!(build_orc_predicate(Some(&float_predicates)).is_none());
        assert!(build_orc_predicate(Some(&double_predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_is_null_supported_leaf() {
        let predicates = file_predicates(
            vec![leaf(0, PredicateOperator::IsNull, vec![])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        let predicate = build_orc_predicate(Some(&predicates)).unwrap();
        assert_eq!(predicate, orc_rust::predicate::Predicate::is_null("id"));
    }

    #[test]
    fn test_build_orc_predicate_is_null_requires_supported_type() {
        let decimal_type = DataType::Decimal(DecimalType::new(10, 2).unwrap());
        let predicates = file_predicates(
            vec![Predicate::Leaf {
                column: "amount".to_string(),
                index: 0,
                data_type: decimal_type.clone(),
                op: PredicateOperator::IsNull,
                literals: vec![],
            }],
            vec![field(0, "amount", decimal_type)],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_is_not_null_requires_supported_type() {
        let decimal_type = DataType::Decimal(DecimalType::new(10, 2).unwrap());
        let predicates = file_predicates(
            vec![Predicate::Leaf {
                column: "amount".to_string(),
                index: 0,
                data_type: decimal_type.clone(),
                op: PredicateOperator::IsNotNull,
                literals: vec![],
            }],
            vec![field(0, "amount", decimal_type)],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_index_out_of_bounds_fails_open() {
        let predicates = file_predicates(
            vec![leaf(1, PredicateOperator::Eq, vec![Datum::Int(7)])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_and_pushes_supported_children() {
        let predicates = file_predicates(
            vec![Predicate::and(vec![
                leaf(0, PredicateOperator::Gt, vec![Datum::Int(1)]),
                leaf(0, PredicateOperator::NotEq, vec![Datum::Int(7)]),
            ])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        let predicate = build_orc_predicate(Some(&predicates)).unwrap();
        assert_eq!(
            predicate,
            orc_rust::predicate::Predicate::gt("id", PredicateValue::Int32(Some(1)))
        );
    }

    #[test]
    fn test_build_orc_predicate_top_level_and_pushes_supported_predicates() {
        let predicates = file_predicates(
            vec![
                leaf(0, PredicateOperator::Gt, vec![Datum::Int(1)]),
                leaf(0, PredicateOperator::LtEq, vec![Datum::Int(9)]),
                leaf(0, PredicateOperator::NotEq, vec![Datum::Int(7)]),
            ],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        let predicate = build_orc_predicate(Some(&predicates)).unwrap();
        assert_eq!(
            predicate,
            orc_rust::predicate::Predicate::and(vec![
                orc_rust::predicate::Predicate::gt("id", PredicateValue::Int32(Some(1))),
                orc_rust::predicate::Predicate::lte("id", PredicateValue::Int32(Some(9))),
            ])
        );
    }

    #[test]
    fn test_build_orc_predicate_or_requires_all_children_supported() {
        let predicates = file_predicates(
            vec![Predicate::or(vec![
                leaf(0, PredicateOperator::Lt, vec![Datum::Int(1)]),
                Predicate::Not(Box::new(leaf(
                    0,
                    PredicateOperator::Eq,
                    vec![Datum::Int(7)],
                ))),
            ])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_or_with_nested_and_requires_exact_children() {
        let predicates = file_predicates(
            vec![Predicate::or(vec![
                Predicate::and(vec![
                    leaf(0, PredicateOperator::Gt, vec![Datum::Int(1)]),
                    leaf(0, PredicateOperator::NotEq, vec![Datum::Int(7)]),
                ]),
                leaf(0, PredicateOperator::Lt, vec![Datum::Int(0)]),
            ])],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_in_limit() {
        let predicates = file_predicates(
            vec![leaf(
                0,
                PredicateOperator::In,
                (0..=ORC_IN_PREDICATE_MAX_LITERALS)
                    .map(|value| Datum::Int(value as i32))
                    .collect(),
            )],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        assert!(build_orc_predicate(Some(&predicates)).is_none());
    }

    #[test]
    fn test_build_orc_predicate_in_supported_literals() {
        let predicates = file_predicates(
            vec![leaf(
                0,
                PredicateOperator::In,
                vec![Datum::Int(1), Datum::Int(3)],
            )],
            vec![field(0, "id", DataType::Int(IntType::new()))],
        );

        let predicate = build_orc_predicate(Some(&predicates)).unwrap();
        assert_eq!(
            predicate,
            orc_rust::predicate::Predicate::or(vec![
                orc_rust::predicate::Predicate::eq("id", PredicateValue::Int32(Some(1))),
                orc_rust::predicate::Predicate::eq("id", PredicateValue::Int32(Some(3))),
            ])
        );
    }

    #[test]
    fn test_collect_orc_predicate_columns_adds_filter_columns_once() {
        let predicate = orc_rust::predicate::Predicate::and(vec![
            orc_rust::predicate::Predicate::eq(
                "category",
                PredicateValue::Utf8(Some("a".to_string())),
            ),
            orc_rust::predicate::Predicate::gt("score", PredicateValue::Int32(Some(1))),
        ]);
        let mut projected_names = vec!["name".to_string()];

        collect_orc_predicate_columns(&predicate, &mut projected_names);
        collect_orc_predicate_columns(&predicate, &mut projected_names);

        assert_eq!(projected_names, vec!["name", "category", "score"]);
    }

    #[test]
    fn test_project_orc_batch_to_requested_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new("id", ArrowDataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        let projected = project_orc_batch_to_requested_fields(batch, &["name".to_string()])
            .expect("project ORC batch");

        assert_eq!(projected.num_columns(), 1);
        assert_eq!(projected.schema().field(0).name(), "name");
    }

    #[test]
    fn test_project_orc_batch_to_requested_fields_errors_on_missing_column() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            ArrowDataType::Utf8,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a"]))]).unwrap();

        let error = project_orc_batch_to_requested_fields(batch, &["id".to_string()])
            .expect_err("missing requested column should error");

        assert!(
            error
                .to_string()
                .contains("ORC batch is missing requested column 'id'"),
            "unexpected error: {error}"
        );
    }
}
