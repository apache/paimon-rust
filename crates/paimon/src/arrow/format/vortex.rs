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

use super::{FilePredicates, FormatFileReader, FormatFileWriter};
use crate::io::{FileRead, OutputFile};
use crate::spec::{DataField, DataType, Datum, Predicate, PredicateOperator};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::{
    Array, ArrayRef as ArrowArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, Scalar,
    StringArray, Time32MillisecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray,
};
use arrow_ord::cmp::{
    eq as arrow_eq, gt as arrow_gt, gt_eq as arrow_gt_eq, lt as arrow_lt, lt_eq as arrow_lt_eq,
    neq as arrow_neq,
};
use arrow_schema::{ArrowError, DataType as ArrowDataType, Field, SchemaRef};
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use vortex::array::arrow::{ArrowSessionExt, FromArrowArray};
use vortex::array::dtype::arrow::FromArrowType;
use vortex::array::dtype::DType;
use vortex::array::{ArrayRef, VortexSessionExecute};
use vortex::buffer::ByteBuffer;
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::scan::split_by::SplitBy;
use vortex::scan::selection::Selection;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;

async fn acquire_vortex_io_permit() -> crate::Result<tokio::sync::SemaphorePermit<'static>> {
    static SEMAPHORE: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
    SEMAPHORE
        .get_or_init(|| tokio::sync::Semaphore::new(1))
        .acquire()
        .await
        .map_err(|e| Error::DataInvalid {
            message: format!("Failed to acquire Vortex I/O permit: {e}"),
            source: None,
        })
}

// ---------------------------------------------------------------------------
// VortexFormatReader
// ---------------------------------------------------------------------------

pub(crate) struct VortexFormatReader;

#[async_trait]
impl FormatFileReader for VortexFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        _batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let bytes = reader.read(0..file_size).await?;
        let target_schema = crate::arrow::build_target_arrow_schema(read_fields)?;
        let read_fields = read_fields.to_vec();
        let predicates = predicates.map(|fp| FilePredicates {
            predicates: fp.predicates.clone(),
            file_fields: fp.file_fields.clone(),
        });
        let scan_fields = build_vortex_scan_fields(&read_fields, predicates.as_ref());
        let scan_schema = crate::arrow::build_target_arrow_schema(&scan_fields)?;
        let _permit = acquire_vortex_io_permit().await?;

        let target_schema_for_scan = target_schema.clone();
        let plan = VortexReadPlan {
            target_schema: target_schema_for_scan,
            read_fields,
            scan_schema,
            scan_fields,
            predicates,
            row_selection,
        };
        let batches =
            tokio::task::spawn_blocking(move || read_vortex_batches_blocking(bytes, plan))
                .await
                .map_err(|e| Error::DataInvalid {
                    message: format!("Vortex read task failed: {e}"),
                    source: None,
                })??;

        Ok(Box::pin(futures::stream::iter(batches.into_iter().map(Ok))))
    }
}

struct VortexReadPlan {
    target_schema: SchemaRef,
    read_fields: Vec<DataField>,
    scan_schema: SchemaRef,
    scan_fields: Vec<DataField>,
    predicates: Option<FilePredicates>,
    row_selection: Option<Vec<RowRange>>,
}

fn read_vortex_batches_blocking(
    bytes: bytes::Bytes,
    plan: VortexReadPlan,
) -> crate::Result<Vec<RecordBatch>> {
    run_vortex_on_thread("paimon-vortex-read", move || {
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        read_vortex_batches(&runtime, session, ByteBuffer::from(bytes), plan)
    })
}

fn read_vortex_batches(
    runtime: &CurrentThreadRuntime,
    session: VortexSession,
    bytes: ByteBuffer,
    plan: VortexReadPlan,
) -> crate::Result<Vec<RecordBatch>> {
    let VortexReadPlan {
        target_schema,
        read_fields,
        scan_schema,
        scan_fields,
        predicates,
        row_selection,
    } = plan;

    let vortex_file =
        session
            .open_options()
            .open_buffer(bytes)
            .map_err(|e| Error::DataInvalid {
                message: format!("Failed to open Vortex file: {e}"),
                source: None,
            })?;

    if scan_fields.is_empty() {
        let row_count = if constant_predicates_match(predicates.as_ref()) {
            match &row_selection {
                Some(ranges) => ranges.iter().map(|r| r.count() as usize).sum(),
                None => vortex_file.row_count() as usize,
            }
        } else {
            0
        };
        let batch = RecordBatch::try_new_with_options(
            target_schema,
            vec![],
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
        .map_err(|e| Error::DataInvalid {
            message: format!("Failed to build empty RecordBatch: {e}"),
            source: None,
        })?;
        return Ok(vec![batch]);
    }

    // Build projection expression for requested fields.
    let projected_names: Vec<&str> = scan_fields.iter().map(|f| f.name()).collect();

    let mut scan_builder = vortex_file.scan().map_err(|e| Error::DataInvalid {
        message: format!("Failed to create Vortex scan: {e}"),
        source: None,
    })?;

    // Apply column projection.
    {
        use vortex::array::expr::{root, select};
        scan_builder = scan_builder.with_projection(select(projected_names, root()));
    }

    // Vortex 0.68 filtered scans can block indefinitely on some runtimes.
    // Decode predicate columns and apply the same filter with Arrow kernels below.

    // Push row selection down to Vortex.
    // For a single contiguous range, use with_row_range directly (avoids roaring bitmap overhead).
    // For multiple ranges, build a Selection::IncludeRoaring bitmap.
    if let Some(ref ranges) = row_selection {
        let total_rows = vortex_file.row_count();
        if let Some(range) = as_single_row_range(ranges, total_rows) {
            scan_builder = scan_builder.with_row_range(range);
        } else {
            let selection = row_ranges_to_selection(ranges, total_rows);
            scan_builder = scan_builder.with_selection(selection);
        }
    }

    let rows_per_split = usize::try_from(vortex_file.row_count())
        .unwrap_or(usize::MAX)
        .max(1);
    let vortex_tasks = scan_builder
        .with_concurrency(1)
        .with_split_by(SplitBy::RowCount(rows_per_split))
        .build()
        .map_err(|e| Error::DataInvalid {
            message: format!("Failed to build Vortex scan tasks: {e}"),
            source: None,
        })?;

    let mut batches = Vec::with_capacity(vortex_tasks.len());
    for task in vortex_tasks {
        let Some(vortex_array) = runtime.block_on(task).map_err(|e| Error::DataInvalid {
            message: format!("Vortex read error: {e}"),
            source: None,
        })?
        else {
            continue;
        };
        let batch = vortex_array_to_record_batch(&session, vortex_array, &scan_schema)?;
        batches.push(filter_and_project_batch(
            batch,
            &target_schema,
            &read_fields,
            &scan_fields,
            predicates.as_ref(),
        )?);
    }

    Ok(batches)
}

fn constant_predicates_match(predicates: Option<&FilePredicates>) -> bool {
    predicates.is_none_or(|fp| {
        fp.predicates
            .iter()
            .all(|predicate| constant_predicate_value(predicate).unwrap_or(true))
    })
}

fn constant_predicate_value(predicate: &Predicate) -> Option<bool> {
    match predicate {
        Predicate::AlwaysTrue => Some(true),
        Predicate::AlwaysFalse => Some(false),
        Predicate::And(children) => {
            let mut saw_unknown = false;
            for child in children {
                match constant_predicate_value(child) {
                    Some(true) => {}
                    Some(false) => return Some(false),
                    None => saw_unknown = true,
                }
            }
            (!saw_unknown).then_some(true)
        }
        Predicate::Or(children) => {
            let mut saw_unknown = false;
            for child in children {
                match constant_predicate_value(child) {
                    Some(true) => return Some(true),
                    Some(false) => {}
                    None => saw_unknown = true,
                }
            }
            (!saw_unknown).then_some(false)
        }
        Predicate::Not(inner) => constant_predicate_value(inner).map(|value| !value),
        Predicate::Leaf { .. } => None,
    }
}

/// If the ranges represent a single contiguous range, return it as a `Range<u64>`
/// for use with `ScanBuilder::with_row_range` (more efficient than a roaring bitmap).
fn as_single_row_range(ranges: &[RowRange], total_rows: u64) -> Option<std::ops::Range<u64>> {
    if ranges.is_empty() || total_rows == 0 {
        return None;
    }
    let file_end = total_rows as i64 - 1;
    if ranges.len() == 1 {
        let r = &ranges[0];
        if r.to() < 0 || r.from() > file_end {
            return None;
        }
        let from = r.from().max(0) as u64;
        let to = (r.to().min(file_end) as u64) + 1;
        return Some(from..to);
    }
    None
}

fn build_vortex_scan_fields(
    read_fields: &[DataField],
    predicates: Option<&FilePredicates>,
) -> Vec<DataField> {
    let mut fields = read_fields.to_vec();

    if let Some(fp) = predicates {
        let mut predicate_indices = Vec::new();
        for predicate in &fp.predicates {
            collect_predicate_field_indices(predicate, &mut predicate_indices);
        }
        for index in predicate_indices {
            if let Some(field) = fp.file_fields.get(index) {
                push_unique_scan_field(&mut fields, field);
            }
        }
    }

    fields
}

fn collect_predicate_field_indices(predicate: &Predicate, indices: &mut Vec<usize>) {
    match predicate {
        Predicate::Leaf { index, .. } => indices.push(*index),
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                collect_predicate_field_indices(child, indices);
            }
        }
        Predicate::Not(inner) => collect_predicate_field_indices(inner, indices),
        Predicate::AlwaysTrue | Predicate::AlwaysFalse => {}
    }
}

fn push_unique_scan_field(fields: &mut Vec<DataField>, field: &DataField) {
    if !fields
        .iter()
        .any(|existing| same_data_field(existing, field))
    {
        fields.push(field.clone());
    }
}

fn same_data_field(left: &DataField, right: &DataField) -> bool {
    left.id() == right.id() || left.name() == right.name()
}

fn filter_and_project_batch(
    batch: RecordBatch,
    target_schema: &SchemaRef,
    read_fields: &[DataField],
    scan_fields: &[DataField],
    predicates: Option<&FilePredicates>,
) -> crate::Result<RecordBatch> {
    let filtered = match predicates {
        Some(fp) => filter_record_batch_by_predicates(batch, fp, scan_fields)?,
        None => batch,
    };

    if read_fields.is_empty() {
        return RecordBatch::try_new_with_options(
            target_schema.clone(),
            vec![],
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(filtered.num_rows())),
        )
        .map_err(|e| Error::DataInvalid {
            message: format!("Failed to build projected empty RecordBatch: {e}"),
            source: None,
        });
    }

    let columns = projection_indices(read_fields, scan_fields)?
        .into_iter()
        .map(|index| filtered.column(index).clone())
        .collect::<Vec<_>>();

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| Error::DataInvalid {
        message: format!("Failed to project Vortex RecordBatch: {e}"),
        source: None,
    })
}

fn projection_indices(
    read_fields: &[DataField],
    scan_fields: &[DataField],
) -> crate::Result<Vec<usize>> {
    read_fields
        .iter()
        .map(|field| {
            scan_fields
                .iter()
                .position(|scan_field| same_data_field(scan_field, field))
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "Projected Vortex field {} was not included in the scan",
                        field.name()
                    ),
                    source: None,
                })
        })
        .collect()
}

fn filter_record_batch_by_predicates(
    batch: RecordBatch,
    predicates: &FilePredicates,
    scan_fields: &[DataField],
) -> crate::Result<RecordBatch> {
    let Some(mask) = evaluate_predicates_mask(
        &batch,
        &predicates.predicates,
        &predicates.file_fields,
        scan_fields,
    )?
    else {
        return Ok(batch);
    };

    arrow_select::filter::filter_record_batch(&batch, &mask).map_err(|e| Error::DataInvalid {
        message: format!("Failed to filter Vortex RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

fn evaluate_predicates_mask(
    batch: &RecordBatch,
    predicates: &[Predicate],
    file_fields: &[DataField],
    scan_fields: &[DataField],
) -> crate::Result<Option<BooleanArray>> {
    let mut combined = None;
    for predicate in predicates {
        let Some(mask) = evaluate_predicate_mask(batch, predicate, file_fields, scan_fields)?
        else {
            continue;
        };
        combined = Some(match combined {
            Some(existing) => combine_filter_masks(&existing, &mask, false),
            None => mask,
        });
    }
    Ok(combined)
}

fn evaluate_predicate_mask(
    batch: &RecordBatch,
    predicate: &Predicate,
    file_fields: &[DataField],
    scan_fields: &[DataField],
) -> crate::Result<Option<BooleanArray>> {
    match predicate {
        Predicate::AlwaysTrue => Ok(Some(BooleanArray::from(vec![true; batch.num_rows()]))),
        Predicate::AlwaysFalse => Ok(Some(BooleanArray::from(vec![false; batch.num_rows()]))),
        Predicate::And(children) => {
            let mut combined = None;
            for child in children {
                let Some(mask) = evaluate_predicate_mask(batch, child, file_fields, scan_fields)?
                else {
                    continue;
                };
                combined = Some(match combined {
                    Some(existing) => combine_filter_masks(&existing, &mask, false),
                    None => mask,
                });
            }
            Ok(combined)
        }
        Predicate::Or(children) => {
            let mut combined = BooleanArray::from(vec![false; batch.num_rows()]);
            for child in children {
                let Some(mask) = evaluate_predicate_mask(batch, child, file_fields, scan_fields)?
                else {
                    return Ok(None);
                };
                combined = combine_filter_masks(&combined, &mask, true);
            }
            Ok(Some(combined))
        }
        Predicate::Not(inner) => {
            let Some(mask) = evaluate_predicate_mask(batch, inner, file_fields, scan_fields)?
            else {
                return Ok(None);
            };
            Ok(Some(boolean_mask_from_predicate(mask.len(), |row_index| {
                !mask.value(row_index)
            })))
        }
        Predicate::Leaf {
            index,
            op,
            literals,
            ..
        } => {
            let Some(file_field) = file_fields.get(*index) else {
                return Ok(None);
            };
            let Some(scan_index) = scan_fields
                .iter()
                .position(|scan_field| same_data_field(scan_field, file_field))
            else {
                return Ok(None);
            };
            evaluate_arrow_leaf_predicate(
                batch.column(scan_index),
                file_field.data_type(),
                *op,
                literals,
            )
        }
    }
}

fn evaluate_arrow_leaf_predicate(
    array: &ArrowArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> crate::Result<Option<BooleanArray>> {
    match op {
        PredicateOperator::IsNull => Ok(Some(boolean_mask_from_predicate(
            array.len(),
            |row_index| array.is_null(row_index),
        ))),
        PredicateOperator::IsNotNull => Ok(Some(boolean_mask_from_predicate(
            array.len(),
            |row_index| array.is_valid(row_index),
        ))),
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
                return Ok(None);
            };
            let Some(scalar) = literal_scalar_for_arrow_filter(literal, data_type)? else {
                return Ok(None);
            };
            let mask =
                evaluate_column_predicate(array, &scalar, op).map_err(|e| Error::DataInvalid {
                    message: format!("Failed to evaluate Vortex predicate: {e}"),
                    source: Some(Box::new(e)),
                })?;
            Ok(Some(sanitize_filter_mask(mask)))
        }
        // Vortex's scalar comparators don't cover substring/range predicates, so
        // fall open: returning None defers them to the outer stats-prune + arrow
        // row-filter path, which evaluates them with full NULL (Kleene) semantics.
        PredicateOperator::StartsWith
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Like
        | PredicateOperator::Between
        | PredicateOperator::NotBetween => Ok(None),
    }
}

fn evaluate_set_membership_predicate(
    array: &ArrowArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> crate::Result<Option<BooleanArray>> {
    if literals.is_empty() {
        return Ok(Some(match op {
            PredicateOperator::In => BooleanArray::from(vec![false; array.len()]),
            PredicateOperator::NotIn => {
                boolean_mask_from_predicate(array.len(), |row_index| array.is_valid(row_index))
            }
            _ => unreachable!(),
        }));
    }

    let mut combined = match op {
        PredicateOperator::In => BooleanArray::from(vec![false; array.len()]),
        PredicateOperator::NotIn => {
            boolean_mask_from_predicate(array.len(), |row_index| array.is_valid(row_index))
        }
        _ => unreachable!(),
    };

    for literal in literals {
        let Some(scalar) = literal_scalar_for_arrow_filter(literal, data_type)? else {
            return Ok(None);
        };
        let comparison_op = match op {
            PredicateOperator::In => PredicateOperator::Eq,
            PredicateOperator::NotIn => PredicateOperator::NotEq,
            _ => unreachable!(),
        };
        let mask = evaluate_column_predicate(array, &scalar, comparison_op).map_err(|e| {
            Error::DataInvalid {
                message: format!("Failed to evaluate Vortex set predicate: {e}"),
                source: Some(Box::new(e)),
            }
        })?;
        let mask = sanitize_filter_mask(mask);
        combined = combine_filter_masks(&combined, &mask, matches!(op, PredicateOperator::In));
    }

    Ok(Some(combined))
}

fn evaluate_column_predicate(
    column: &ArrowArrayRef,
    scalar: &Scalar<ArrowArrayRef>,
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
        | PredicateOperator::NotIn
        // String/range ops never reach the scalar comparator: the leaf
        // dispatcher falls open (returns None) for them, so the value here is
        // unused. Listed only to keep the match exhaustive.
        | PredicateOperator::StartsWith
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Like
        | PredicateOperator::Between
        | PredicateOperator::NotBetween => Ok(BooleanArray::new_null(column.len())),
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

fn literal_scalar_for_arrow_filter(
    literal: &Datum,
    file_data_type: &DataType,
) -> crate::Result<Option<Scalar<ArrowArrayRef>>> {
    let array: ArrowArrayRef = match file_data_type {
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
        DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob(_) => match literal {
            Datum::Bytes(value) => Arc::new(BinaryArray::new_scalar(value.as_slice()).into_inner()),
            _ => return Ok(None),
        },
        DataType::Date(_) => match literal {
            Datum::Date(value) => Arc::new(Date32Array::new_scalar(*value).into_inner()),
            _ => return Ok(None),
        },
        DataType::Time(_) => match literal {
            Datum::Time(value) => Arc::new(Time32MillisecondArray::new_scalar(*value).into_inner()),
            _ => return Ok(None),
        },
        DataType::Timestamp(ts) => match literal {
            Datum::Timestamp { millis, nanos } => {
                let Some(array) = timestamp_scalar(*millis, *nanos, ts.precision(), None)? else {
                    return Ok(None);
                };
                array
            }
            _ => return Ok(None),
        },
        DataType::LocalZonedTimestamp(ts) => match literal {
            Datum::LocalZonedTimestamp { millis, nanos } => {
                let Some(array) = timestamp_scalar(*millis, *nanos, ts.precision(), Some("UTC"))?
                else {
                    return Ok(None);
                };
                array
            }
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
                                "Failed to build decimal scalar for Vortex row filter: {e}"
                            ),
                            source: Some(Box::new(e)),
                        })?,
                )
            }
            _ => return Ok(None),
        },
        DataType::Array(_)
        | DataType::Map(_)
        | DataType::Multiset(_)
        | DataType::Row(_)
        | DataType::Vector(_) => {
            return Ok(None);
        }
    };

    Ok(Some(Scalar::new(array)))
}

fn timestamp_scalar(
    millis: i64,
    nanos: i32,
    precision: u32,
    timezone: Option<&'static str>,
) -> crate::Result<Option<ArrowArrayRef>> {
    let array: ArrowArrayRef = match precision {
        0..=3 => {
            let array = TimestampMillisecondArray::new_scalar(millis).into_inner();
            match timezone {
                Some(tz) => Arc::new(array.with_timezone(tz)),
                None => Arc::new(array),
            }
        }
        4..=6 => {
            let value = millis * 1_000 + (nanos as i64) / 1_000;
            let array = TimestampMicrosecondArray::new_scalar(value).into_inner();
            match timezone {
                Some(tz) => Arc::new(array.with_timezone(tz)),
                None => Arc::new(array),
            }
        }
        7..=9 => {
            let value = millis * 1_000_000 + (nanos as i64);
            let array = TimestampNanosecondArray::new_scalar(value).into_inner();
            match timezone {
                Some(tz) => Arc::new(array.with_timezone(tz)),
                None => Arc::new(array),
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(array))
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

/// Convert paimon `RowRange`s (inclusive [from, to]) to a Vortex `Selection`.
fn row_ranges_to_selection(ranges: &[RowRange], total_rows: u64) -> Selection {
    let mut bitmap = roaring::RoaringTreemap::new();
    let file_end = if total_rows == 0 {
        return Selection::IncludeRoaring(bitmap);
    } else {
        total_rows as i64 - 1
    };

    for range in ranges {
        if range.to() < 0 || range.from() > file_end {
            continue;
        }
        let from = range.from().max(0) as u64;
        let to = (range.to().min(file_end) as u64) + 1; // exclusive end
        bitmap.insert_range(from..to);
    }

    Selection::IncludeRoaring(bitmap)
}

/// Convert a Vortex ArrayRef to an Arrow RecordBatch.
fn vortex_array_to_record_batch(
    session: &VortexSession,
    vortex_array: ArrayRef,
    schema: &SchemaRef,
) -> crate::Result<RecordBatch> {
    let target_field = Field::new(
        "",
        ArrowDataType::Struct(schema.fields().clone()),
        vortex_array.dtype().is_nullable(),
    );
    let mut ctx = session.create_execution_ctx();
    let arrow_array = session
        .arrow()
        .execute_arrow(vortex_array, Some(&target_field), &mut ctx)
        .map_err(|e| Error::DataInvalid {
            message: format!("Failed to convert Vortex array to Arrow: {e}"),
            source: None,
        })?;

    let struct_array = arrow_array
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .ok_or_else(|| Error::DataInvalid {
            message: "Vortex array did not convert to Arrow StructArray".to_string(),
            source: None,
        })?;

    if struct_array.columns().len() != schema.fields().len() {
        return Err(Error::DataInvalid {
            message: format!(
                "Vortex column count {} does not match target schema column count {}",
                struct_array.columns().len(),
                schema.fields().len()
            ),
            source: None,
        });
    }

    RecordBatch::try_new(schema.clone(), struct_array.columns().to_vec()).map_err(|e| {
        Error::DataInvalid {
            message: format!("Failed to build RecordBatch from Vortex data: {e}"),
            source: None,
        }
    })
}

// ---------------------------------------------------------------------------
// VortexFormatWriter
// ---------------------------------------------------------------------------

/// Vortex implementation of [`FormatFileWriter`].
///
/// `write()` converts each RecordBatch to a Vortex ArrayRef. `close()` then
/// writes all accumulated arrays through Vortex into an in-memory buffer before
/// flushing that buffer to Paimon's output file.
pub(crate) struct VortexFormatWriter {
    /// Vortex dtype derived from the target Arrow schema.
    dtype: DType,
    /// Converted arrays pending final Vortex write.
    arrays: Vec<ArrayRef>,
    /// Paimon output file receiving the finalized Vortex buffer.
    output: OutputFile,
    /// Bytes already flushed to storage.
    bytes_written: Arc<AtomicU64>,
    /// Estimated bytes staged in `arrays` before Vortex finalizes the file.
    staged_bytes: usize,
}

impl VortexFormatWriter {
    pub(crate) async fn new(output: &OutputFile, schema: SchemaRef) -> crate::Result<Self> {
        let dtype = DType::from_arrow(schema);
        let bytes_written = Arc::new(AtomicU64::new(0));

        Ok(Self {
            dtype,
            arrays: Vec::new(),
            output: output.clone(),
            bytes_written,
            staged_bytes: 0,
        })
    }
}

#[async_trait]
impl FormatFileWriter for VortexFormatWriter {
    async fn write(&mut self, batch: &RecordBatch) -> crate::Result<()> {
        let staged_bytes = batch.get_array_memory_size();
        let vortex_arr =
            ArrayRef::from_arrow(batch.clone(), false).map_err(|e| Error::DataInvalid {
                message: format!("Failed to convert RecordBatch to Vortex: {e}"),
                source: None,
            })?;

        self.arrays.push(vortex_arr);
        self.staged_bytes = self.staged_bytes.saturating_add(staged_bytes);
        Ok(())
    }

    fn num_bytes(&self) -> usize {
        let bytes_written = self.bytes_written.load(Ordering::Relaxed) as usize;
        bytes_written.max(self.staged_bytes)
    }

    fn in_progress_size(&self) -> usize {
        self.staged_bytes
    }

    async fn flush(&mut self) -> crate::Result<()> {
        // Vortex writes are finalized in close().
        Ok(())
    }

    async fn close(self: Box<Self>) -> crate::Result<u64> {
        let this = *self;
        let VortexFormatWriter {
            dtype,
            arrays,
            output,
            bytes_written,
            staged_bytes: _,
        } = this;

        let (size, buffer) = {
            let _permit = acquire_vortex_io_permit().await?;
            tokio::task::spawn_blocking(move || write_vortex_buffer_blocking(dtype, arrays))
                .await
                .map_err(|e| Error::DataInvalid {
                    message: format!("Vortex write task failed: {e}"),
                    source: None,
                })??
        };
        output.write(bytes::Bytes::from(buffer)).await?;
        bytes_written.store(size, Ordering::Relaxed);

        Ok(size)
    }
}

fn write_vortex_buffer_blocking(
    dtype: DType,
    arrays: Vec<ArrayRef>,
) -> crate::Result<(u64, Vec<u8>)> {
    run_vortex_on_thread("paimon-vortex-write", move || {
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut buffer = Vec::new();
        let summary = runtime
            .block_on(async {
                let mut writer = session.write_options().writer(&mut buffer, dtype);
                for array in arrays {
                    writer.push(array).await?;
                }
                writer.finish().await
            })
            .map_err(|e| Error::DataInvalid {
                message: format!("Failed to write Vortex file: {e}"),
                source: None,
            })?;

        Ok((summary.size(), buffer))
    })
}

fn run_vortex_on_thread<T>(
    name: &'static str,
    f: impl FnOnce() -> crate::Result<T> + Send + 'static,
) -> crate::Result<T>
where
    T: Send + 'static,
{
    let join = std::thread::Builder::new()
        .name(name.to_string())
        .spawn(f)
        .map_err(|e| Error::DataInvalid {
            message: format!("Failed to spawn Vortex worker thread: {e}"),
            source: None,
        })?;

    join.join().map_err(|_| Error::DataInvalid {
        message: "Vortex worker thread panicked".to_string(),
        source: None,
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::format::FormatFileWriter;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataField, DataType, VarCharType};
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use bytes::Bytes;
    use futures::StreamExt;
    use std::ops::Range;

    fn test_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]))
    }

    fn test_batch(schema: &Arc<ArrowSchema>, ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    }

    struct WholeFileOnlyRead {
        bytes: Bytes,
    }

    #[async_trait]
    impl FileRead for WholeFileOnlyRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            let file_size = self.bytes.len() as u64;
            if range != (0..file_size) {
                return Err(Error::DataInvalid {
                    message: format!(
                        "expected a whole-file read, got {}..{}",
                        range.start, range.end
                    ),
                    source: None,
                });
            }
            Ok(self.bytes.clone())
        }
    }

    #[test]
    fn test_vortex_writer_outlives_calling_tokio_runtime() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_vortex_writer_runtime.vortex";
        let output = file_io.new_output(path).unwrap();
        let schema = test_arrow_schema();

        let caller_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let writer = caller_runtime.block_on(async {
            let mut writer = VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap();
            let batch = test_batch(&schema, vec![1, 2, 3], vec![10, 20, 30]);
            writer.write(&batch).await.unwrap();
            writer
        });
        drop(caller_runtime);

        let verifier_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let bytes = verifier_runtime
            .block_on(async { Box::new(writer).close().await })
            .unwrap();
        assert!(bytes > 0);
    }

    #[test]
    fn test_vortex_reader_stream_outlives_calling_tokio_runtime() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_vortex_reader_runtime.vortex";
        let output = file_io.new_output(path).unwrap();
        let schema = test_arrow_schema();

        let caller_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let stream = caller_runtime.block_on(async {
            let mut writer = VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap();
            let batch = test_batch(&schema, vec![1, 2, 3], vec![10, 20, 30]);
            writer.write(&batch).await.unwrap();
            Box::new(writer).close().await.unwrap();

            let input = file_io.new_input(path).unwrap();
            let file_reader = input.reader().await.unwrap();
            let metadata = input.metadata().await.unwrap();
            let reader = VortexFormatReader;
            reader
                .read_batch_stream(
                    Box::new(file_reader),
                    metadata.size,
                    &test_file_fields(),
                    None,
                    None,
                    None,
                )
                .await
                .unwrap()
        });
        drop(caller_runtime);

        let verifier_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let rows = verifier_runtime.block_on(async {
            let mut stream = stream;
            let mut rows = 0;
            while let Some(result) = stream.next().await {
                rows += result.unwrap().num_rows();
            }
            rows
        });
        assert_eq!(rows, 3);
    }

    #[tokio::test]
    async fn test_vortex_writer_write_and_read() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_vortex_write_read.vortex";
        let output = file_io.new_output(path).unwrap();
        let schema = test_arrow_schema();

        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap(),
        );

        let batch = test_batch(&schema, vec![1, 2, 3], vec![10, 20, 30]);
        writer.write(&batch).await.unwrap();
        let bytes = writer.close().await.unwrap();
        assert!(bytes > 0);

        // Read back using VortexFormatReader.
        let input = file_io.new_input(path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let metadata = input.metadata().await.unwrap();

        let read_fields = vec![
            crate::spec::DataField::new(
                0,
                "id".to_string(),
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            ),
            crate::spec::DataField::new(
                1,
                "value".to_string(),
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            ),
        ];

        let reader = VortexFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(file_reader),
                metadata.size,
                &read_fields,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let mut total_rows = 0;
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_vortex_reader_returns_utf8_for_string_schema() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_vortex_utf8_schema.vortex";
        let output = file_io.new_output(path).unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));

        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap(),
        );
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")])),
            ],
        )
        .unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let metadata = input.metadata().await.unwrap();
        let read_fields = vec![
            DataField::new(
                0,
                "id".to_string(),
                DataType::Int(crate::spec::IntType::new()),
            ),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            ),
        ];

        let reader = VortexFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(file_reader),
                metadata.size,
                &read_fields,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let mut names = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            assert_eq!(batch.schema().field(1).data_type(), &ArrowDataType::Utf8);
            assert_eq!(batch.column(1).data_type(), &ArrowDataType::Utf8);
            let name_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                names.push(name_col.value(i).to_string());
            }
        }
        assert_eq!(names, vec!["Alice".to_string(), "Bob".to_string()]);
    }

    #[tokio::test]
    async fn test_vortex_writer_multiple_batches() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_vortex_multi.vortex";
        let output = file_io.new_output(path).unwrap();
        let schema = test_arrow_schema();

        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap(),
        );

        writer
            .write(&test_batch(&schema, vec![1, 2], vec![10, 20]))
            .await
            .unwrap();
        writer
            .write(&test_batch(&schema, vec![3, 4, 5], vec![30, 40, 50]))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let metadata = input.metadata().await.unwrap();

        let read_fields = vec![
            crate::spec::DataField::new(
                0,
                "id".to_string(),
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            ),
            crate::spec::DataField::new(
                1,
                "value".to_string(),
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            ),
        ];

        let reader = VortexFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(file_reader),
                metadata.size,
                &read_fields,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let mut total_rows = 0;
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_vortex_read_with_row_selection() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_vortex_row_sel.vortex";
        let output = file_io.new_output(path).unwrap();
        let schema = test_arrow_schema();

        // Write 5 rows: id=[1,2,3,4,5], value=[10,20,30,40,50]
        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap(),
        );
        writer
            .write(&test_batch(
                &schema,
                vec![1, 2, 3, 4, 5],
                vec![10, 20, 30, 40, 50],
            ))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let metadata = input.metadata().await.unwrap();

        let read_fields = vec![
            crate::spec::DataField::new(
                0,
                "id".to_string(),
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            ),
            crate::spec::DataField::new(
                1,
                "value".to_string(),
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            ),
        ];

        // Select rows [1,3] (inclusive), i.e. rows at index 1,2,3 → id=[2,3,4]
        let row_selection = vec![RowRange::new(1, 3)];

        let reader = VortexFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(file_reader),
                metadata.size,
                &read_fields,
                None,
                None,
                Some(row_selection),
            )
            .await
            .unwrap();

        let mut all_ids = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            all_ids.extend(id_col.values().iter().copied());
        }
        assert_eq!(all_ids, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_empty_projection() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_vortex_empty_proj.vortex";
        let output = file_io.new_output(path).unwrap();
        let schema = test_arrow_schema();

        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap(),
        );
        writer
            .write(&test_batch(
                &schema,
                vec![1, 2, 3, 4, 5],
                vec![10, 20, 30, 40, 50],
            ))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let metadata = input.metadata().await.unwrap();

        let reader = VortexFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(file_reader),
                metadata.size,
                &[], // empty projection
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let mut total_rows = 0;
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            assert_eq!(batch.num_columns(), 0);
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_vortex_reader_opens_from_whole_file_buffer() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_vortex_whole_file_buffer.vortex";
        let output = file_io.new_output(path).unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, false),
        ]));
        let ids: Vec<i32> = (0..10_000).collect();
        let names: Vec<String> = ids
            .iter()
            .map(|id| format!("row-{id:05}-abcdefghijklmnopqrstuvwxyz0123456789"))
            .collect();

        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap(),
        );
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.clone())),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let file_bytes = input.read().await.unwrap();
        let metadata = input.metadata().await.unwrap();
        assert!(metadata.size > 65_535);
        let reader = VortexFormatReader;

        let mut stream = reader
            .read_batch_stream(
                Box::new(WholeFileOnlyRead { bytes: file_bytes }),
                metadata.size,
                &[
                    DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
                    DataField::new(
                        1,
                        "name".to_string(),
                        DataType::VarChar(VarCharType::string_type()),
                    ),
                ],
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let mut all_ids = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            all_ids.extend(id_col.values().iter().copied());
        }
        assert_eq!(all_ids, ids);
    }

    use crate::spec::{DataType as PaimonDataType, IntType, PredicateBuilder};

    fn test_file_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), PaimonDataType::Int(IntType::new())),
            DataField::new(1, "value".to_string(), PaimonDataType::Int(IntType::new())),
        ]
    }

    // -----------------------------------------------------------------------
    // Integration tests: Arrow-side predicate filtering through VortexFormatReader
    // -----------------------------------------------------------------------

    /// Helper: write test data and read back with given predicates, return collected id values.
    async fn write_and_read_with_predicates(
        path: &str,
        predicates: Option<FilePredicates>,
    ) -> Vec<i32> {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let output = file_io.new_output(path).unwrap();
        let schema = test_arrow_schema();

        // Write 5 rows: id=[1,2,3,4,5], value=[10,20,30,40,50]
        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap(),
        );
        writer
            .write(&test_batch(
                &schema,
                vec![1, 2, 3, 4, 5],
                vec![10, 20, 30, 40, 50],
            ))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let metadata = input.metadata().await.unwrap();

        let read_fields = test_file_fields();

        let reader = VortexFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(file_reader),
                metadata.size,
                &read_fields,
                predicates.as_ref(),
                None,
                None,
            )
            .await
            .unwrap();

        let mut all_ids = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            all_ids.extend(id_col.values().iter().copied());
        }
        all_ids
    }

    #[tokio::test]
    async fn test_vortex_read_with_eq_predicate() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder.equal("id", Datum::Int(3)).unwrap();
        let fp = FilePredicates {
            predicates: vec![pred],
            file_fields: fields,
        };
        let ids =
            write_and_read_with_predicates("memory:/test_vortex_pred_eq.vortex", Some(fp)).await;
        assert_eq!(ids, vec![3]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_gt_predicate() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder.greater_than("id", Datum::Int(3)).unwrap();
        let fp = FilePredicates {
            predicates: vec![pred],
            file_fields: fields,
        };
        let ids =
            write_and_read_with_predicates("memory:/test_vortex_pred_gt.vortex", Some(fp)).await;
        assert_eq!(ids, vec![4, 5]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_in_predicate() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder
            .is_in("id", vec![Datum::Int(1), Datum::Int(4)])
            .unwrap();
        let fp = FilePredicates {
            predicates: vec![pred],
            file_fields: fields,
        };
        let ids =
            write_and_read_with_predicates("memory:/test_vortex_pred_in.vortex", Some(fp)).await;
        assert_eq!(ids, vec![1, 4]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_combined_predicates() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        // id >= 2 AND value < 50
        let pred1 = builder.greater_or_equal("id", Datum::Int(2)).unwrap();
        let pred2 = builder.less_than("value", Datum::Int(50)).unwrap();
        let fp = FilePredicates {
            predicates: vec![pred1, pred2],
            file_fields: fields,
        };
        let ids =
            write_and_read_with_predicates("memory:/test_vortex_pred_combined.vortex", Some(fp))
                .await;
        // id=[2,3,4] (id>=2 and value<50, excludes id=5/value=50)
        assert_eq!(ids, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_vortex_read_with_no_match_predicate() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder.equal("id", Datum::Int(99)).unwrap();
        let fp = FilePredicates {
            predicates: vec![pred],
            file_fields: fields,
        };
        let ids =
            write_and_read_with_predicates("memory:/test_vortex_pred_nomatch.vortex", Some(fp))
                .await;
        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn test_vortex_read_filter_column_not_projected() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/test_vortex_pred_unprojected.vortex";
        let output = file_io.new_output(path).unwrap();
        let schema = test_arrow_schema();

        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap(),
        );
        writer
            .write(&test_batch(
                &schema,
                vec![1, 2, 3, 4, 5],
                vec![10, 20, 30, 40, 50],
            ))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder.greater_than("value", Datum::Int(30)).unwrap();
        let fp = FilePredicates {
            predicates: vec![pred],
            file_fields: fields.clone(),
        };
        let read_fields = vec![fields[0].clone()];

        let input = file_io.new_input(path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let metadata = input.metadata().await.unwrap();
        let reader = VortexFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(file_reader),
                metadata.size,
                &read_fields,
                Some(&fp),
                None,
                None,
            )
            .await
            .unwrap();

        let mut all_ids = Vec::new();
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            assert_eq!(batch.num_columns(), 1);
            let id_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            all_ids.extend(id_col.values().iter().copied());
        }
        assert_eq!(all_ids, vec![4, 5]);
    }

    #[tokio::test]
    async fn test_vortex_empty_projection_with_predicate_returns_filtered_count() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let pred = builder.greater_than("id", Datum::Int(3)).unwrap();
        let fp = FilePredicates {
            predicates: vec![pred],
            file_fields: fields,
        };

        let count = write_and_read_empty_projection_with_predicates(
            "memory:/test_vortex_empty_proj_pred.vortex",
            Some(fp),
        )
        .await;
        assert_eq!(count, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_vortex_predicate_reads_do_not_block_each_other() {
        let fields = test_file_fields();
        let builder = PredicateBuilder::new(&fields);
        let eq = FilePredicates {
            predicates: vec![builder.equal("id", Datum::Int(3)).unwrap()],
            file_fields: fields.clone(),
        };
        let gt = FilePredicates {
            predicates: vec![builder.greater_than("id", Datum::Int(3)).unwrap()],
            file_fields: fields.clone(),
        };
        let combined = FilePredicates {
            predicates: vec![
                builder.greater_or_equal("id", Datum::Int(2)).unwrap(),
                builder.less_than("value", Datum::Int(50)).unwrap(),
            ],
            file_fields: fields,
        };

        let (empty, eq, gt, combined) = tokio::join!(
            write_and_read_empty_projection("memory:/test_vortex_concurrent_empty.vortex"),
            write_and_read_with_predicates("memory:/test_vortex_concurrent_eq.vortex", Some(eq)),
            write_and_read_with_predicates("memory:/test_vortex_concurrent_gt.vortex", Some(gt)),
            write_and_read_with_predicates(
                "memory:/test_vortex_concurrent_combined.vortex",
                Some(combined)
            ),
        );

        assert_eq!(empty, 5);
        assert_eq!(eq, vec![3]);
        assert_eq!(gt, vec![4, 5]);
        assert_eq!(combined, vec![2, 3, 4]);
    }

    async fn write_and_read_empty_projection(path: &str) -> usize {
        write_and_read_empty_projection_with_predicates(path, None).await
    }

    async fn write_and_read_empty_projection_with_predicates(
        path: &str,
        predicates: Option<FilePredicates>,
    ) -> usize {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let output = file_io.new_output(path).unwrap();
        let schema = test_arrow_schema();

        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            VortexFormatWriter::new(&output, schema.clone())
                .await
                .unwrap(),
        );
        writer
            .write(&test_batch(
                &schema,
                vec![1, 2, 3, 4, 5],
                vec![10, 20, 30, 40, 50],
            ))
            .await
            .unwrap();
        writer.close().await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let file_reader = input.reader().await.unwrap();
        let metadata = input.metadata().await.unwrap();

        let reader = VortexFormatReader;
        let mut stream = reader
            .read_batch_stream(
                Box::new(file_reader),
                metadata.size,
                &[],
                predicates.as_ref(),
                None,
                None,
            )
            .await
            .unwrap();

        let mut total_rows = 0;
        while let Some(result) = stream.next().await {
            let batch = result.unwrap();
            assert_eq!(batch.num_columns(), 0);
            total_rows += batch.num_rows();
        }
        total_rows
    }
}
