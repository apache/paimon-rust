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
use crate::arrow::filtering::{predicates_may_match_with_schema, StatsAccessor};
use crate::io::FileRead;
use crate::spec::{DataField, DataType, Datum, Predicate, PredicateOperator};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, Scalar, StringArray,
};
use arrow_ord::cmp::{
    eq as arrow_eq, gt as arrow_gt, gt_eq as arrow_gt_eq, lt as arrow_lt, lt_eq as arrow_lt_eq,
    neq as arrow_neq,
};
use arrow_schema::ArrowError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{StreamExt, TryFutureExt};
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, ArrowReaderOptions, RowFilter, RowSelection, RowSelector,
};
use parquet::arrow::async_reader::{AsyncFileReader, MetadataFetch};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use parquet::file::statistics::Statistics as ParquetStatistics;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

pub(crate) struct ParquetFormatReader;

#[async_trait]
impl FormatFileReader for ParquetFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let arrow_file_reader = ArrowFileReader::new(file_size, reader);

        let mut batch_stream_builder =
            ParquetRecordBatchStreamBuilder::new(arrow_file_reader).await?;

        let parquet_schema = batch_stream_builder.parquet_schema().clone();
        let root_schema = parquet_schema.root_schema();
        let root_indices: Vec<usize> = read_fields
            .iter()
            .filter_map(|f| {
                root_schema
                    .get_fields()
                    .iter()
                    .position(|pf| pf.name() == f.name())
            })
            .collect();

        let mask = ProjectionMask::roots(&parquet_schema, root_indices);
        batch_stream_builder = batch_stream_builder.with_projection(mask);

        let empty_predicates = Vec::new();
        let (preds, file_fields): (&[Predicate], &[DataField]) = match predicates {
            Some(fp) => (&fp.predicates, &fp.file_fields),
            None => (&empty_predicates, &[]),
        };

        let parquet_row_filter = build_parquet_row_filter(&parquet_schema, preds, file_fields)?;
        if let Some(f) = parquet_row_filter {
            batch_stream_builder = batch_stream_builder.with_row_filter(f);
        }

        let predicate_row_selection = build_predicate_row_selection(
            batch_stream_builder.metadata().row_groups(),
            preds,
            file_fields,
        )?;
        let mut combined_selection = predicate_row_selection;

        if let Some(ref ranges) = row_selection {
            let range_selection =
                build_row_ranges_selection(batch_stream_builder.metadata().row_groups(), ranges);
            combined_selection =
                intersect_optional_row_selections(combined_selection, Some(range_selection));
        }
        if let Some(sel) = combined_selection {
            batch_stream_builder = batch_stream_builder.with_row_selection(sel);
        }
        if let Some(size) = batch_size {
            batch_stream_builder = batch_stream_builder.with_batch_size(size);
        }

        let batch_stream = batch_stream_builder.build()?;
        Ok(batch_stream.map(|r| r.map_err(Error::from)).boxed())
    }
}

// ---------------------------------------------------------------------------
// Parquet row-filter helpers
// ---------------------------------------------------------------------------

fn build_parquet_row_filter(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    predicates: &[Predicate],
    file_fields: &[DataField],
) -> crate::Result<Option<RowFilter>> {
    if predicates.is_empty() {
        return Ok(None);
    }

    let mut filters: Vec<Box<dyn ArrowPredicate>> = Vec::new();

    for predicate in predicates {
        if let Some(filter) = build_parquet_arrow_predicate(parquet_schema, predicate, file_fields)?
        {
            filters.push(filter);
        }
    }

    if filters.is_empty() {
        Ok(None)
    } else {
        Ok(Some(RowFilter::new(filters)))
    }
}

fn build_parquet_arrow_predicate(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    predicate: &Predicate,
    file_fields: &[DataField],
) -> crate::Result<Option<Box<dyn ArrowPredicate>>> {
    let Predicate::Leaf {
        index,
        data_type: _,
        op,
        literals,
        ..
    } = predicate
    else {
        return Ok(None);
    };
    if !predicate_supported_for_parquet_row_filter(*op) {
        return Ok(None);
    }

    let Some(file_field) = file_fields.get(*index) else {
        return Ok(None);
    };
    let Some(root_index) = parquet_root_index(parquet_schema, file_field.name()) else {
        return Ok(None);
    };
    if !parquet_row_filter_literals_supported(*op, literals, file_field.data_type())? {
        return Ok(None);
    }

    let projection = ProjectionMask::roots(parquet_schema, [root_index]);
    let op = *op;
    let data_type = file_field.data_type().clone();
    let literals = literals.to_vec();
    Ok(Some(Box::new(ArrowPredicateFn::new(
        projection,
        move |batch: RecordBatch| {
            let Some(column) = batch.columns().first() else {
                return Ok(BooleanArray::new_null(batch.num_rows()));
            };
            evaluate_exact_leaf_predicate(column, &data_type, op, &literals)
        },
    ))))
}

fn predicate_supported_for_parquet_row_filter(op: PredicateOperator) -> bool {
    matches!(
        op,
        PredicateOperator::IsNull
            | PredicateOperator::IsNotNull
            | PredicateOperator::Eq
            | PredicateOperator::NotEq
            | PredicateOperator::Lt
            | PredicateOperator::LtEq
            | PredicateOperator::Gt
            | PredicateOperator::GtEq
            | PredicateOperator::In
            | PredicateOperator::NotIn
    )
}

fn parquet_row_filter_literals_supported(
    op: PredicateOperator,
    literals: &[Datum],
    file_data_type: &DataType,
) -> crate::Result<bool> {
    match op {
        PredicateOperator::IsNull | PredicateOperator::IsNotNull => Ok(true),
        PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq => {
            let Some(literal) = literals.first() else {
                return Ok(false);
            };
            Ok(literal_scalar_for_parquet_filter(literal, file_data_type)?.is_some())
        }
        PredicateOperator::In | PredicateOperator::NotIn => {
            for literal in literals {
                if literal_scalar_for_parquet_filter(literal, file_data_type)?.is_none() {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

fn parquet_root_index(
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
    root_name: &str,
) -> Option<usize> {
    parquet_schema
        .root_schema()
        .get_fields()
        .iter()
        .position(|field| field.name() == root_name)
}

// ---------------------------------------------------------------------------
// Predicate evaluation helpers
// ---------------------------------------------------------------------------

fn evaluate_exact_leaf_predicate(
    array: &ArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray, ArrowError> {
    match op {
        PredicateOperator::IsNull => Ok(boolean_mask_from_predicate(array.len(), |row_index| {
            array.is_null(row_index)
        })),
        PredicateOperator::IsNotNull => Ok(boolean_mask_from_predicate(array.len(), |row_index| {
            array.is_valid(row_index)
        })),
        PredicateOperator::In | PredicateOperator::NotIn => {
            evaluate_set_membership_predicate(array, data_type, op, literals)
        }
        PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq => {
            let Some(literal) = literals.first() else {
                return Ok(BooleanArray::from(vec![true; array.len()]));
            };
            let Some(scalar) = literal_scalar_for_parquet_filter(literal, data_type)
                .map_err(|e| ArrowError::ComputeError(e.to_string()))?
            else {
                return Ok(BooleanArray::from(vec![true; array.len()]));
            };
            let result = evaluate_column_predicate(array, &scalar, op)?;
            Ok(sanitize_filter_mask(result))
        }
    }
}

fn evaluate_set_membership_predicate(
    array: &ArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray, ArrowError> {
    if literals.is_empty() {
        return Ok(match op {
            PredicateOperator::In => BooleanArray::from(vec![false; array.len()]),
            PredicateOperator::NotIn => {
                boolean_mask_from_predicate(array.len(), |row_index| array.is_valid(row_index))
            }
            _ => unreachable!(),
        });
    }

    let mut combined = match op {
        PredicateOperator::In => BooleanArray::from(vec![false; array.len()]),
        PredicateOperator::NotIn => {
            boolean_mask_from_predicate(array.len(), |row_index| array.is_valid(row_index))
        }
        _ => unreachable!(),
    };

    for literal in literals {
        let Some(scalar) = literal_scalar_for_parquet_filter(literal, data_type)
            .map_err(|e| ArrowError::ComputeError(e.to_string()))?
        else {
            return Ok(BooleanArray::from(vec![true; array.len()]));
        };
        let comparison_op = match op {
            PredicateOperator::In => PredicateOperator::Eq,
            PredicateOperator::NotIn => PredicateOperator::NotEq,
            _ => unreachable!(),
        };
        let mask = sanitize_filter_mask(evaluate_column_predicate(array, &scalar, comparison_op)?);
        combined = combine_filter_masks(&combined, &mask, matches!(op, PredicateOperator::In));
    }

    Ok(combined)
}

fn evaluate_column_predicate(
    column: &ArrayRef,
    scalar: &Scalar<ArrayRef>,
    op: PredicateOperator,
) -> Result<BooleanArray, ArrowError> {
    match op {
        PredicateOperator::Eq => arrow_eq(column, scalar),
        PredicateOperator::NotEq => arrow_neq(column, scalar),
        PredicateOperator::Lt => arrow_lt(column, scalar),
        PredicateOperator::LtEq => arrow_lt_eq(column, scalar),
        PredicateOperator::Gt => arrow_gt(column, scalar),
        PredicateOperator::GtEq => arrow_gt_eq(column, scalar),
        PredicateOperator::IsNull
        | PredicateOperator::IsNotNull
        | PredicateOperator::In
        | PredicateOperator::NotIn => Ok(BooleanArray::new_null(column.len())),
    }
}

fn sanitize_filter_mask(mask: BooleanArray) -> BooleanArray {
    if mask.null_count() == 0 {
        return mask;
    }

    boolean_mask_from_predicate(mask.len(), |row_index| {
        mask.is_valid(row_index) && mask.value(row_index)
    })
}

fn combine_filter_masks(left: &BooleanArray, right: &BooleanArray, use_or: bool) -> BooleanArray {
    debug_assert_eq!(left.len(), right.len());
    boolean_mask_from_predicate(left.len(), |row_index| {
        if use_or {
            left.value(row_index) || right.value(row_index)
        } else {
            left.value(row_index) && right.value(row_index)
        }
    })
}

fn boolean_mask_from_predicate(
    len: usize,
    mut predicate: impl FnMut(usize) -> bool,
) -> BooleanArray {
    BooleanArray::from((0..len).map(&mut predicate).collect::<Vec<_>>())
}

// ---------------------------------------------------------------------------
// Row-group statistics pruning
// ---------------------------------------------------------------------------

struct ParquetRowGroupStats<'a> {
    row_group: &'a RowGroupMetaData,
    column_indices: &'a [Option<usize>],
}

impl StatsAccessor for ParquetRowGroupStats<'_> {
    fn row_count(&self) -> i64 {
        self.row_group.num_rows()
    }

    fn null_count(&self, index: usize) -> Option<i64> {
        let _ = index;
        None
    }

    fn min_value(&self, index: usize, data_type: &DataType) -> Option<Datum> {
        let column_index = self.column_indices.get(index).copied().flatten()?;
        parquet_stats_to_datum(
            self.row_group.column(column_index).statistics()?,
            data_type,
            true,
        )
    }

    fn max_value(&self, index: usize, data_type: &DataType) -> Option<Datum> {
        let column_index = self.column_indices.get(index).copied().flatten()?;
        parquet_stats_to_datum(
            self.row_group.column(column_index).statistics()?,
            data_type,
            false,
        )
    }
}

fn build_predicate_row_selection(
    row_groups: &[RowGroupMetaData],
    predicates: &[Predicate],
    file_fields: &[DataField],
) -> crate::Result<Option<RowSelection>> {
    if predicates.is_empty() || row_groups.is_empty() {
        return Ok(None);
    }

    // Predicates have already been remapped to file-level indices by the caller
    // (remap_predicates_to_file in reader.rs), so we use an identity mapping here.
    let identity_mapping: Vec<Option<usize>> = (0..file_fields.len()).map(Some).collect();
    let column_indices = build_row_group_column_indices(row_groups[0].columns(), file_fields);
    let mut selectors = Vec::with_capacity(row_groups.len());
    let mut all_selected = true;

    for row_group in row_groups {
        let stats = ParquetRowGroupStats {
            row_group,
            column_indices: &column_indices,
        };
        let may_match =
            predicates_may_match_with_schema(predicates, &stats, &identity_mapping, file_fields);
        if !may_match {
            all_selected = false;
        }
        selectors.push(if may_match {
            RowSelector::select(row_group.num_rows() as usize)
        } else {
            RowSelector::skip(row_group.num_rows() as usize)
        });
    }

    if all_selected {
        Ok(None)
    } else {
        Ok(Some(selectors.into()))
    }
}

fn build_row_group_column_indices(
    columns: &[parquet::file::metadata::ColumnChunkMetaData],
    file_fields: &[DataField],
) -> Vec<Option<usize>> {
    let mut by_root_name: HashMap<&str, Option<usize>> = HashMap::new();
    for (column_index, column) in columns.iter().enumerate() {
        let Some(root_name) = column.column_path().parts().first() else {
            continue;
        };
        let entry = by_root_name
            .entry(root_name.as_str())
            .or_insert(Some(column_index));
        if entry.is_some() && *entry != Some(column_index) {
            *entry = None;
        }
    }

    file_fields
        .iter()
        .map(|field| by_root_name.get(field.name()).copied().flatten())
        .collect()
}

// ---------------------------------------------------------------------------
// Parquet statistics → Datum conversion
// ---------------------------------------------------------------------------

fn parquet_stats_to_datum(
    stats: &ParquetStatistics,
    data_type: &DataType,
    is_min: bool,
) -> Option<Datum> {
    let exact = if is_min {
        stats.min_is_exact()
    } else {
        stats.max_is_exact()
    };
    if !exact {
        return None;
    }

    match (stats, data_type) {
        (ParquetStatistics::Boolean(stats), DataType::Boolean(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Bool)
        }
        (ParquetStatistics::Int32(stats), DataType::TinyInt(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .and_then(|value| i8::try_from(*value).ok())
                .map(Datum::TinyInt)
        }
        (ParquetStatistics::Int32(stats), DataType::SmallInt(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .and_then(|value| i16::try_from(*value).ok())
                .map(Datum::SmallInt)
        }
        (ParquetStatistics::Int32(stats), DataType::Int(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Int)
        }
        (ParquetStatistics::Int32(stats), DataType::Date(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Date)
        }
        (ParquetStatistics::Int32(stats), DataType::Time(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Time)
        }
        (ParquetStatistics::Int64(stats), DataType::BigInt(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Long)
        }
        (ParquetStatistics::Int64(stats), DataType::Timestamp(ts)) if ts.precision() <= 3 => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(|millis| Datum::Timestamp { millis, nanos: 0 })
        }
        (ParquetStatistics::Int64(stats), DataType::LocalZonedTimestamp(ts))
            if ts.precision() <= 3 =>
        {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(|millis| Datum::LocalZonedTimestamp { millis, nanos: 0 })
        }
        (ParquetStatistics::Float(stats), DataType::Float(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Float)
        }
        (ParquetStatistics::Double(stats), DataType::Double(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .copied()
                .map(Datum::Double)
        }
        (ParquetStatistics::ByteArray(stats), DataType::Char(_))
        | (ParquetStatistics::ByteArray(stats), DataType::VarChar(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .and_then(|value| std::str::from_utf8(value.data()).ok())
                .map(|value| Datum::String(value.to_string()))
        }
        (ParquetStatistics::ByteArray(stats), DataType::Binary(_))
        | (ParquetStatistics::ByteArray(stats), DataType::VarBinary(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .map(|value| Datum::Bytes(value.data().to_vec()))
        }
        (ParquetStatistics::FixedLenByteArray(stats), DataType::Binary(_))
        | (ParquetStatistics::FixedLenByteArray(stats), DataType::VarBinary(_)) => {
            exact_parquet_value(is_min, stats.min_opt(), stats.max_opt())
                .map(|value| Datum::Bytes(value.data().to_vec()))
        }
        _ => None,
    }
}

fn exact_parquet_value<'a, T>(
    is_min: bool,
    min: Option<&'a T>,
    max: Option<&'a T>,
) -> Option<&'a T> {
    if is_min {
        min
    } else {
        max
    }
}

// ---------------------------------------------------------------------------
// Literal → Arrow scalar conversion
// ---------------------------------------------------------------------------

fn literal_scalar_for_parquet_filter(
    literal: &Datum,
    file_data_type: &DataType,
) -> crate::Result<Option<Scalar<ArrayRef>>> {
    let array: ArrayRef = match file_data_type {
        DataType::Boolean(_) => match literal {
            Datum::Bool(value) => Arc::new(BooleanArray::new_scalar(*value).into_inner()),
            _ => return Ok(None),
        },
        DataType::TinyInt(_) => {
            match integer_literal(literal).and_then(|value| i8::try_from(value).ok()) {
                Some(value) => Arc::new(Int8Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::SmallInt(_) => {
            match integer_literal(literal).and_then(|value| i16::try_from(value).ok()) {
                Some(value) => Arc::new(Int16Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::Int(_) => {
            match integer_literal(literal).and_then(|value| i32::try_from(value).ok()) {
                Some(value) => Arc::new(Int32Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::BigInt(_) => {
            match integer_literal(literal).and_then(|value| i64::try_from(value).ok()) {
                Some(value) => Arc::new(Int64Array::new_scalar(value).into_inner()),
                None => return Ok(None),
            }
        }
        DataType::Float(_) => match float32_literal(literal) {
            Some(value) => Arc::new(Float32Array::new_scalar(value).into_inner()),
            None => return Ok(None),
        },
        DataType::Double(_) => match float64_literal(literal) {
            Some(value) => Arc::new(Float64Array::new_scalar(value).into_inner()),
            None => return Ok(None),
        },
        DataType::Char(_) | DataType::VarChar(_) => match literal {
            Datum::String(value) => Arc::new(StringArray::new_scalar(value.as_str()).into_inner()),
            _ => return Ok(None),
        },
        DataType::Binary(_) | DataType::VarBinary(_) => match literal {
            Datum::Bytes(value) => Arc::new(BinaryArray::new_scalar(value.as_slice()).into_inner()),
            _ => return Ok(None),
        },
        DataType::Date(_) => match literal {
            Datum::Date(value) => Arc::new(Date32Array::new_scalar(*value).into_inner()),
            _ => return Ok(None),
        },
        DataType::Decimal(decimal) => match literal {
            Datum::Decimal {
                unscaled,
                precision,
                scale,
            } if *precision <= decimal.precision() && *scale == decimal.scale() => {
                let precision =
                    u8::try_from(decimal.precision()).map_err(|_| Error::Unsupported {
                        message: "Decimal precision exceeds Arrow decimal128 range".to_string(),
                    })?;
                let scale =
                    i8::try_from(decimal.scale() as i32).map_err(|_| Error::Unsupported {
                        message: "Decimal scale exceeds Arrow decimal128 range".to_string(),
                    })?;
                Arc::new(
                    Decimal128Array::new_scalar(*unscaled)
                        .into_inner()
                        .with_precision_and_scale(precision, scale)
                        .map_err(|e| Error::UnexpectedError {
                            message: format!(
                                "Failed to build decimal scalar for parquet row filter: {e}"
                            ),
                            source: Some(Box::new(e)),
                        })?,
                )
            }
            _ => return Ok(None),
        },
        DataType::Time(_)
        | DataType::Timestamp(_)
        | DataType::LocalZonedTimestamp(_)
        | DataType::Array(_)
        | DataType::Map(_)
        | DataType::Multiset(_)
        | DataType::Row(_) => return Ok(None),
    };

    Ok(Some(Scalar::new(array)))
}

fn integer_literal(literal: &Datum) -> Option<i128> {
    match literal {
        Datum::TinyInt(value) => Some(i128::from(*value)),
        Datum::SmallInt(value) => Some(i128::from(*value)),
        Datum::Int(value) => Some(i128::from(*value)),
        Datum::Long(value) => Some(i128::from(*value)),
        _ => None,
    }
}

fn float32_literal(literal: &Datum) -> Option<f32> {
    match literal {
        Datum::Float(value) => Some(*value),
        Datum::Double(value) => {
            let casted = *value as f32;
            ((casted as f64) == *value).then_some(casted)
        }
        _ => None,
    }
}

fn float64_literal(literal: &Datum) -> Option<f64> {
    match literal {
        Datum::Float(value) => Some(f64::from(*value)),
        Datum::Double(value) => Some(*value),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Row selection helpers (DV, row ranges)
// ---------------------------------------------------------------------------

fn intersect_optional_row_selections(
    left: Option<RowSelection>,
    right: Option<RowSelection>,
) -> Option<RowSelection> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.intersection(&right)),
        (Some(selection), None) | (None, Some(selection)) => Some(selection),
        (None, None) => None,
    }
}

/// Build a Parquet [RowSelection] from inclusive `[from, to]` file-local row ranges (0-based).
fn build_row_ranges_selection(
    row_group_metadata_list: &[RowGroupMetaData],
    row_ranges: &[RowRange],
) -> RowSelection {
    let total_rows: i64 = row_group_metadata_list.iter().map(|rg| rg.num_rows()).sum();
    if total_rows == 0 {
        return vec![].into();
    }

    let file_end = total_rows - 1;
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

    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut cursor: usize = 0;
    for (start, end) in &local_ranges {
        if *start > cursor {
            selectors.push(RowSelector::skip(*start - cursor));
        }
        let select_start = (*start).max(cursor);
        if *end > select_start {
            selectors.push(RowSelector::select(*end - select_start));
        }
        cursor = cursor.max(*end);
    }
    let total = total_rows as usize;
    if cursor < total {
        selectors.push(RowSelector::skip(total - cursor));
    }
    selectors.into()
}

// ---------------------------------------------------------------------------
// ArrowFileReader — async Parquet IO adapter
// ---------------------------------------------------------------------------

/// ArrowFileReader is a wrapper around a FileRead that impls parquets AsyncFileReader.
///
/// # TODO
///
/// [ParquetObjectReader](https://docs.rs/parquet/latest/src/parquet/arrow/async_reader/store.rs.html#64)
/// contains the following hints to speed up metadata loading, similar to iceberg, we can consider adding them to this struct:
///
/// - `metadata_size_hint`: Provide a hint as to the size of the parquet file's footer.
/// - `preload_column_index`: Load the Column Index  as part of [`Self::get_metadata`].
/// - `preload_offset_index`: Load the Offset Index as part of [`Self::get_metadata`].
struct ArrowFileReader {
    file_size: u64,
    r: Box<dyn FileRead>,
}

impl ArrowFileReader {
    fn new(file_size: u64, r: Box<dyn FileRead>) -> Self {
        Self { file_size, r }
    }

    fn read_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(self.r.read(range.start..range.end).map_err(|err| {
            let err_msg = format!("{err}");
            parquet::errors::ParquetError::External(err_msg.into())
        }))
    }
}

impl MetadataFetch for ArrowFileReader {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.read_bytes(range)
    }
}

impl AsyncFileReader for ArrowFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.read_bytes(range)
    }

    fn get_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let metadata_opts = options.map(|o| o.metadata_options().clone());
        Box::pin(async move {
            let file_size = self.file_size;
            let metadata = ParquetMetaDataReader::new()
                .with_metadata_options(metadata_opts)
                .load_and_finish(self, file_size)
                .await?;
            Ok(Arc::new(metadata))
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::build_parquet_row_filter;
    use crate::spec::{DataField, DataType, Datum, IntType, PredicateBuilder};
    use parquet::schema::{parser::parse_message_type, types::SchemaDescriptor};
    use std::sync::Arc;

    fn test_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "score".to_string(), DataType::Int(IntType::new())),
        ]
    }

    fn test_parquet_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(
            parse_message_type(
                "
                message test_schema {
                  OPTIONAL INT32 id;
                  OPTIONAL INT32 score;
                }
                ",
            )
            .expect("test schema should parse"),
        ))
    }

    #[test]
    fn test_build_parquet_row_filter_supports_null_and_membership_predicates() {
        let fields = test_fields();
        let builder = PredicateBuilder::new(&fields);
        let predicates = vec![
            builder
                .is_null("id")
                .expect("is null predicate should build"),
            builder
                .is_in("score", vec![Datum::Int(7)])
                .expect("in predicate should build"),
            builder
                .is_not_in("score", vec![Datum::Int(9)])
                .expect("not in predicate should build"),
        ];

        let row_filter = build_parquet_row_filter(&test_parquet_schema(), &predicates, &fields)
            .expect("parquet row filter should build");

        assert!(row_filter.is_some());
    }
}
