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

//! Shared, exact Arrow-batch residual predicate evaluator.
//!
//! This module holds the single source of truth for evaluating a
//! [`FilePredicates`] set against an already-decoded Arrow [`RecordBatch`],
//! producing a boolean row mask (or filtering the batch directly). It replaces
//! two near-duplicate copies that previously lived in the Parquet and Vortex
//! format readers:
//!
//! - The Parquet copy implemented the full leaf operator set (comparison,
//!   set-membership, and the string / range operators `StartsWith` / `EndsWith`
//!   / `Contains` / `Like` / `Between` / `NotBetween`).
//! - The Vortex copy carried the compound batch walker (And/Or/Not plus the
//!   `Leaf` arm that maps a `file_field` to the corresponding batch column via
//!   [`same_data_field`]) and a broader literal-to-scalar conversion (Time /
//!   Timestamp / LocalZonedTimestamp / Blob), but deferred the string / range
//!   leaves.
//!
//! The consolidation keeps the complete leaf dispatch (from Parquet), the
//! compound walker + `filter_record_batch_by_predicates` (from Vortex), and the
//! broader `literal_scalar_for_arrow_filter` (from Vortex). As a result the
//! string / range leaves are now evaluated everywhere the shared walker runs —
//! they are no longer silently deferred.
//!
//! `NULL` rows collapse to `false` via [`sanitize_filter_mask`], matching the
//! evaluator's residual-filter convention everywhere.
//!
//! The module is always compiled (independent of the `vortex` feature), so it
//! must not reference any vortex-specific types.

use crate::arrow::format::FilePredicates;
use crate::spec::{is_row_id_column, DataField, DataType, Datum, Predicate, PredicateOperator};
use crate::Error;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Datum as ArrowDatum, Decimal128Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, Scalar,
    StringArray, Time32MillisecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray,
};
use arrow_ord::cmp::{
    eq as arrow_eq, gt as arrow_gt, gt_eq as arrow_gt_eq, lt as arrow_lt, lt_eq as arrow_lt_eq,
    neq as arrow_neq,
};
use arrow_schema::ArrowError;
use arrow_string::like::{
    contains as arrow_contains, ends_with as arrow_ends_with, like as arrow_like,
    starts_with as arrow_starts_with,
};
use std::sync::Arc;

/// Filter a [`RecordBatch`] to exactly the rows satisfying `predicates`.
///
/// `scan_fields` describes the columns actually present in `batch` (in order);
/// `predicates.file_fields` describes the full file schema the predicate indices
/// point into. Each leaf's `file_field` is resolved to a `batch` column via
/// [`same_data_field`]. When no predicate produces a mask (e.g. empty predicate
/// list), the batch is returned unchanged.
pub(crate) fn filter_record_batch_by_predicates(
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
        message: format!("Failed to filter RecordBatch by predicates: {e}"),
        source: Some(Box::new(e)),
    })
}

/// Evaluate the conjunction of `predicates` against `batch`, returning the
/// combined boolean row mask, or `None` when no predicate contributed a mask
/// (identity — keep every row).
pub(crate) fn evaluate_predicates_mask(
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
            column,
            index,
            op,
            literals,
            data_type: predicate_data_type,
        } => {
            // Resolve the batch column by NAME, but pick that name carefully: a
            // real column may be renamed in the file, so its batch name comes
            // from `file_fields[index]`; `_ROW_ID` is absent from `file_fields`
            // and only its own name is meaningful. Java's `PredicateRemapper`
            // rebinds by name too.
            let file_field = if is_row_id_column(column) {
                None
            } else {
                match file_fields.get(*index) {
                    Some(field) => Some(field),
                    None => return Ok(None),
                }
            };
            let field_name = file_field.map_or(column.as_str(), DataField::name);
            let field_type = file_field.map_or(predicate_data_type, DataField::data_type);
            // Never index by position in `scan_fields`: a reader's emitted batch
            // may order columns by its file schema (e.g. ORC
            // `ProjectionMask::named_roots`) rather than by `scan_fields` order.
            // `scan_fields` is used only for the Gap-A guard below.
            let array = batch
                .schema()
                .index_of(field_name)
                .ok()
                .map(|batch_index| batch.column(batch_index));
            let Some(array) = array else {
                let _ = scan_fields;
                if file_field.is_none() {
                    // Backstop for residuals applied to a predicate-free reader
                    // (PK merge output, vector search); readers that own their
                    // predicates reject earlier.
                    return Err(crate::table::row_id_predicate::unsupported_row_id_filter(
                        "this read",
                    ));
                }
                // A real column missing here is a reader bug: it did not widen
                // its scan to the predicate columns.
                debug_assert!(
                    false,
                    "residual predicate column '{field_name}' is missing from the scanned batch; the reader must widen its scan to include predicate columns"
                );
                return Ok(None);
            };
            // Evaluate against the predicate's declared type (the table type),
            // not the file column's type. Under schema evolution these differ
            // (e.g. an old INT file column for a promoted BIGINT table column):
            // building the literal in the narrower file type could fail for an
            // out-of-range value, incorrectly erroring when the exact answer is
            // simply "no rows". Promotion is always widening, so cast the decoded
            // column up to the predicate type first — then the literal is always
            // representable and the comparison is exact.
            let predicate_arrow_type = crate::arrow::paimon_type_to_arrow(predicate_data_type)?;
            let mask = if array.data_type() == &predicate_arrow_type {
                evaluate_exact_leaf_predicate(array, field_type, *op, literals)
            } else {
                let cast_column = arrow_cast::cast(array, &predicate_arrow_type).map_err(|e| {
                    Error::DataInvalid {
                        message: format!(
                            "Failed to cast residual column '{field_name}' from {:?} to {predicate_arrow_type:?}: {e}",
                            array.data_type()
                        ),
                        source: Some(Box::new(e)),
                    }
                })?;
                evaluate_exact_leaf_predicate(&cast_column, predicate_data_type, *op, literals)
            }
            .map_err(|e| Error::DataInvalid {
                message: format!("Failed to evaluate residual predicate: {e}"),
                source: Some(Box::new(e)),
            })?;
            Ok(Some(mask))
        }
    }
}

/// Two [`DataField`]s refer to the same logical column when their IDs match, or
/// (for schemas without stable IDs) their names match.
pub(crate) fn same_data_field(left: &DataField, right: &DataField) -> bool {
    left.id() == right.id() || left.name() == right.name()
}

/// Widen `read_fields` to include every column referenced by `predicates` that
/// is not already projected, so a reader scans `read_fields ∪ predicate columns`
/// and the residual filter can see every predicate column.
///
/// Deduped by [`same_data_field`]: a predicate column already present in
/// `read_fields` is not added twice. When `predicates` is `None`, `read_fields`
/// is returned unchanged.
pub(crate) fn widen_scan_fields(
    read_fields: &[DataField],
    predicates: Option<&FilePredicates>,
) -> Vec<DataField> {
    let mut fields = read_fields.to_vec();

    if let Some(fp) = predicates {
        let mut refs = Vec::new();
        for predicate in &fp.predicates {
            collect_predicate_leaf_refs(predicate, &mut refs);
        }
        for (name, index) in refs {
            // Not read from the file, so there is nothing to widen with.
            if is_row_id_column(name) {
                continue;
            }
            if let Some(field) = fp.file_fields.get(index) {
                push_unique_scan_field(&mut fields, field);
            }
        }
    }

    fields
}

/// Collect every leaf as `(column name, leaf index)`.
///
/// Callers that resolve a leaf positionally must check the name first — see
/// [`crate::spec::is_row_id_column`].
pub(crate) fn collect_predicate_leaf_refs<'a>(
    predicate: &'a Predicate,
    refs: &mut Vec<(&'a str, usize)>,
) {
    match predicate {
        Predicate::Leaf { column, index, .. } => refs.push((column.as_str(), *index)),
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                collect_predicate_leaf_refs(child, refs);
            }
        }
        Predicate::Not(inner) => collect_predicate_leaf_refs(inner, refs),
        Predicate::AlwaysTrue | Predicate::AlwaysFalse => {}
    }
}

pub(crate) fn push_unique_scan_field(fields: &mut Vec<DataField>, field: &DataField) {
    if !fields
        .iter()
        .any(|existing| same_data_field(existing, field))
    {
        fields.push(field.clone());
    }
}

/// Error for a leaf predicate whose literal(s) cannot be converted to an Arrow
/// scalar for the column's type (e.g. a decimal literal whose scale differs from
/// the column, or a malformed leaf with too few literals). The residual pass is
/// the last line of exactness, so it must error rather than pass all rows.
fn unconvertible_literal_error(op: PredicateOperator, data_type: &DataType) -> ArrowError {
    ArrowError::ComputeError(format!(
        "residual predicate operator {op:?} has a literal that cannot be evaluated exactly against column type {data_type:?}"
    ))
}

/// Evaluate a single leaf predicate against a decoded column, producing an
/// exact boolean row mask with the `NULL` → `false` convention applied.
/// This is the *complete* leaf dispatch: comparison, null checks,
/// set-membership, the string operators (`StartsWith` / `EndsWith` / `Contains`
/// / `Like`), and the range operators (`Between` / `NotBetween`).
pub(crate) fn evaluate_exact_leaf_predicate(
    array: &ArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray, ArrowError> {
    // Decimals are compared by mathematical value across scales (Paimon
    // `datum_cmp`/`decimal_cmp`). Arrow scalar comparison requires the literal to
    // be representable at the column scale, which fails for a finer-scale literal
    // (e.g. `d > 1.05` on a DECIMAL(_,1) column). Route all decimal value
    // comparisons through a row-wise `datum_cmp` path that is exact for any scale
    // combination. IsNull/IsNotNull carry no literal and stay on the generic path.
    if matches!(array.data_type(), arrow_schema::DataType::Decimal128(_, _))
        && !matches!(op, PredicateOperator::IsNull | PredicateOperator::IsNotNull)
    {
        return evaluate_decimal_leaf(array, op, literals);
    }
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
        | PredicateOperator::GtEq
        | PredicateOperator::StartsWith
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Like => {
            let Some(literal) = literals.first() else {
                return Err(unconvertible_literal_error(op, data_type));
            };
            let Some(scalar) = literal_scalar_for_arrow_filter(literal, data_type)
                .map_err(|e| ArrowError::ComputeError(e.to_string()))?
            else {
                // The literal cannot be converted to an Arrow scalar for this
                // column type (e.g. a decimal literal whose scale differs from
                // the column). Erroring is required for correctness: returning
                // all-true here would silently pass every row (a wrong-read),
                // and this residual pass is the only place the predicate is
                // enforced when the row filter did not accept the leaf.
                return Err(unconvertible_literal_error(op, data_type));
            };
            let result = evaluate_column_predicate(array, &scalar, op)?;
            Ok(sanitize_filter_mask(result))
        }
        PredicateOperator::Between | PredicateOperator::NotBetween => {
            evaluate_between_predicate(array, data_type, op, literals)
        }
    }
}

/// Evaluate a value comparison on a Decimal128 column using Paimon's by-value
/// decimal semantics (`datum_cmp`), which normalizes across scales. This is
/// exact for any (column scale, literal scale) combination and any comparison
/// operator, including finer-scale literals that cannot be represented at the
/// column scale (e.g. `d > 1.05` on a DECIMAL(_,1) column is exactly `d >= 1.1`).
/// NULL rows stay NULL in the mask (collapsed to `false` by the caller).
fn evaluate_decimal_leaf(
    array: &ArrayRef,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray, ArrowError> {
    use std::cmp::Ordering;

    let decimals = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            ArrowError::ComputeError("decimal predicate expects a Decimal128 column".to_string())
        })?;
    let col_scale = match array.data_type() {
        arrow_schema::DataType::Decimal128(_, s) => u32::try_from(*s).map_err(|_| {
            ArrowError::ComputeError("negative decimal scale is not supported".to_string())
        })?,
        _ => unreachable!("guarded by caller"),
    };

    // Compare one column value (as a Datum::Decimal at the column scale) against a
    // literal Datum. `precision` is irrelevant to `datum_cmp` (it compares by
    // value), so any value is fine. `None` means the cross-scale normalization
    // overflowed i128 — surface it rather than silently drop rows.
    let cmp = |v: i128, lit: &Datum| -> Result<Ordering, ArrowError> {
        let cell = Datum::Decimal {
            unscaled: v,
            precision: 38,
            scale: col_scale,
        };
        crate::spec::datum_cmp(&cell, lit).ok_or_else(|| {
            ArrowError::ComputeError(
                "decimal comparison overflowed while normalizing scales".to_string(),
            )
        })
    };

    let require_decimal = |lit: &Datum| -> Result<(), ArrowError> {
        match lit {
            Datum::Decimal { .. } => Ok(()),
            _ => Err(ArrowError::ComputeError(
                "decimal column compared against a non-decimal literal".to_string(),
            )),
        }
    };

    let mut builder = Vec::with_capacity(decimals.len());
    for i in 0..decimals.len() {
        if decimals.is_null(i) {
            builder.push(None);
            continue;
        }
        let v = decimals.value(i);
        let keep = match op {
            PredicateOperator::Eq
            | PredicateOperator::NotEq
            | PredicateOperator::Lt
            | PredicateOperator::LtEq
            | PredicateOperator::Gt
            | PredicateOperator::GtEq => {
                let lit = literals
                    .first()
                    .ok_or_else(|| ArrowError::ComputeError("missing literal".to_string()))?;
                require_decimal(lit)?;
                let ord = cmp(v, lit)?;
                match op {
                    PredicateOperator::Eq => ord == Ordering::Equal,
                    PredicateOperator::NotEq => ord != Ordering::Equal,
                    PredicateOperator::Lt => ord == Ordering::Less,
                    PredicateOperator::LtEq => ord != Ordering::Greater,
                    PredicateOperator::Gt => ord == Ordering::Greater,
                    PredicateOperator::GtEq => ord != Ordering::Less,
                    _ => unreachable!(),
                }
            }
            PredicateOperator::In | PredicateOperator::NotIn => {
                let mut any_eq = false;
                for lit in literals {
                    require_decimal(lit)?;
                    if cmp(v, lit)? == Ordering::Equal {
                        any_eq = true;
                        break;
                    }
                }
                if matches!(op, PredicateOperator::In) {
                    any_eq
                } else {
                    !any_eq
                }
            }
            PredicateOperator::Between | PredicateOperator::NotBetween => {
                let (Some(low), Some(high)) = (literals.first(), literals.get(1)) else {
                    return Err(ArrowError::ComputeError(
                        "between requires two literals".to_string(),
                    ));
                };
                require_decimal(low)?;
                require_decimal(high)?;
                let in_range = cmp(v, low)? != Ordering::Less && cmp(v, high)? != Ordering::Greater;
                if matches!(op, PredicateOperator::Between) {
                    in_range
                } else {
                    !in_range
                }
            }
            _ => {
                return Err(ArrowError::ComputeError(format!(
                    "operator {op:?} is not a decimal value comparison"
                )))
            }
        };
        builder.push(Some(keep));
    }
    Ok(BooleanArray::from(builder))
}

/// `Between` / `NotBetween` translate to `gt_eq(col, low) & lt_eq(col, high)`
/// (and its negation). `arrow_ord::cmp` produces nullable masks: any null
/// row makes the comparison null, so a fully-built `Between` mask preserves
/// nulls. `NotBetween` then negates valid rows and leaves nulls null —
/// matching SQL three-valued logic; `sanitize_filter_mask` collapses nulls
/// into `false` to match the predicate evaluator's "NULL → false" rule.
fn evaluate_between_predicate(
    array: &ArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray, ArrowError> {
    let (Some(low), Some(high)) = (literals.first(), literals.get(1)) else {
        return Err(unconvertible_literal_error(op, data_type));
    };
    let Some(low_scalar) = literal_scalar_for_arrow_filter(low, data_type)
        .map_err(|e| ArrowError::ComputeError(e.to_string()))?
    else {
        return Err(unconvertible_literal_error(op, data_type));
    };
    let Some(high_scalar) = literal_scalar_for_arrow_filter(high, data_type)
        .map_err(|e| ArrowError::ComputeError(e.to_string()))?
    else {
        return Err(unconvertible_literal_error(op, data_type));
    };
    // Delegate the two bound comparisons to `evaluate_column_predicate` rather
    // than calling `arrow_gt_eq`/`arrow_lt_eq` directly, so Between inherits the
    // type-faithful comparison paths (e.g. byte ordering for Binary).
    let lo_mask = evaluate_column_predicate(array, &low_scalar, PredicateOperator::GtEq)?;
    let hi_mask = evaluate_column_predicate(array, &high_scalar, PredicateOperator::LtEq)?;
    let between = arrow_arith::boolean::and_kleene(&lo_mask, &hi_mask)?;
    let result = match op {
        PredicateOperator::Between => between,
        PredicateOperator::NotBetween => arrow_arith::boolean::not(&between)?,
        _ => unreachable!(),
    };
    Ok(sanitize_filter_mask(result))
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

    if let Some(mask) = set_membership_hash_mask(array, op, literals) {
        return Ok(mask);
    }

    let mut combined = match op {
        PredicateOperator::In => BooleanArray::from(vec![false; array.len()]),
        PredicateOperator::NotIn => {
            boolean_mask_from_predicate(array.len(), |row_index| array.is_valid(row_index))
        }
        _ => unreachable!(),
    };

    for literal in literals {
        let Some(scalar) = literal_scalar_for_arrow_filter(literal, data_type)
            .map_err(|e| ArrowError::ComputeError(e.to_string()))?
        else {
            return Err(unconvertible_literal_error(op, data_type));
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

/// One-pass hash-set evaluation of `In`/`NotIn` for byte-like, string, and
/// integer columns. The general path above OR-combines one comparison kernel
/// per literal — O(rows × literals) — which turns a large pushed-down literal
/// set (an engine probing a batch of keys as `In`) quadratic. Returns `None`
/// for column/literal shapes outside the fast path, or when any literal would
/// not convert for the column, so the general path keeps its exact semantics —
/// including its error behavior for unconvertible literals.
fn set_membership_hash_mask(
    array: &ArrayRef,
    op: PredicateOperator,
    literals: &[Datum],
) -> Option<BooleanArray> {
    use arrow_schema::DataType as ArrowType;
    use std::collections::HashSet;

    let keep = matches!(op, PredicateOperator::In);

    fn byte_mask<'a>(
        values: impl Iterator<Item = Option<&'a [u8]>>,
        literals: &[Datum],
        keep: bool,
    ) -> Option<BooleanArray> {
        let set = literals
            .iter()
            .map(|literal| match literal {
                Datum::Bytes(bytes) => Some(bytes.as_slice()),
                _ => None,
            })
            .collect::<Option<HashSet<_>>>()?;
        Some(
            values
                .map(|value| Some(value.is_some_and(|v| set.contains(v) == keep)))
                .collect(),
        )
    }

    fn str_mask<'a>(
        values: impl Iterator<Item = Option<&'a str>>,
        literals: &[Datum],
        keep: bool,
    ) -> Option<BooleanArray> {
        let set = literals
            .iter()
            .map(|literal| match literal {
                Datum::String(value) => Some(value.as_str()),
                _ => None,
            })
            .collect::<Option<HashSet<_>>>()?;
        Some(
            values
                .map(|value| Some(value.is_some_and(|v| set.contains(v) == keep)))
                .collect(),
        )
    }

    fn int_mask<T>(array: &ArrayRef, literals: &[Datum], keep: bool) -> Option<BooleanArray>
    where
        T: arrow_array::types::ArrowPrimitiveType,
        T::Native: TryFrom<i128> + std::hash::Hash + Eq,
    {
        let array = array
            .as_any()
            .downcast_ref::<arrow_array::PrimitiveArray<T>>()?;
        let set = literals
            .iter()
            .map(|literal| integer_literal(literal).and_then(|v| T::Native::try_from(v).ok()))
            .collect::<Option<HashSet<_>>>()?;
        Some(
            array
                .iter()
                .map(|value| Some(value.is_some_and(|v| set.contains(&v) == keep)))
                .collect(),
        )
    }

    match array.data_type() {
        ArrowType::Binary => byte_mask(
            array.as_any().downcast_ref::<BinaryArray>()?.iter(),
            literals,
            keep,
        ),
        ArrowType::LargeBinary => byte_mask(
            array
                .as_any()
                .downcast_ref::<arrow_array::LargeBinaryArray>()?
                .iter(),
            literals,
            keep,
        ),
        ArrowType::BinaryView => byte_mask(
            array
                .as_any()
                .downcast_ref::<arrow_array::BinaryViewArray>()?
                .iter(),
            literals,
            keep,
        ),
        ArrowType::Utf8 => str_mask(
            array.as_any().downcast_ref::<StringArray>()?.iter(),
            literals,
            keep,
        ),
        ArrowType::LargeUtf8 => str_mask(
            array
                .as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()?
                .iter(),
            literals,
            keep,
        ),
        ArrowType::Utf8View => str_mask(
            array
                .as_any()
                .downcast_ref::<arrow_array::StringViewArray>()?
                .iter(),
            literals,
            keep,
        ),
        ArrowType::Int8 => int_mask::<arrow_array::types::Int8Type>(array, literals, keep),
        ArrowType::Int16 => int_mask::<arrow_array::types::Int16Type>(array, literals, keep),
        ArrowType::Int32 => int_mask::<arrow_array::types::Int32Type>(array, literals, keep),
        ArrowType::Int64 => int_mask::<arrow_array::types::Int64Type>(array, literals, keep),
        _ => None,
    }
}

fn evaluate_column_predicate(
    column: &ArrayRef,
    scalar: &Scalar<ArrayRef>,
    op: PredicateOperator,
) -> Result<BooleanArray, ArrowError> {
    let scalar = string_scalar_for_column(scalar, column.data_type())?;

    match op {
        PredicateOperator::Eq => arrow_eq(column, &scalar),
        PredicateOperator::NotEq => arrow_neq(column, &scalar),
        PredicateOperator::Lt => arrow_lt(column, &scalar),
        PredicateOperator::LtEq => arrow_lt_eq(column, &scalar),
        PredicateOperator::Gt => arrow_gt(column, &scalar),
        PredicateOperator::GtEq => arrow_gt_eq(column, &scalar),
        PredicateOperator::StartsWith
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Like => match op {
            PredicateOperator::StartsWith => arrow_starts_with(column, &scalar),
            PredicateOperator::EndsWith => arrow_ends_with(column, &scalar),
            PredicateOperator::Contains => arrow_contains(column, &scalar),
            PredicateOperator::Like => arrow_like(column, &scalar),
            _ => unreachable!(),
        },
        PredicateOperator::IsNull
        | PredicateOperator::IsNotNull
        | PredicateOperator::In
        | PredicateOperator::NotIn
        | PredicateOperator::Between
        | PredicateOperator::NotBetween => Ok(BooleanArray::new_null(column.len())),
    }
}

/// Arrow comparison and pattern kernels reject mismatched string types. The
/// shared scalar built from Paimon's logical Char/VarChar type is Utf8, while a
/// decoded file column may be Utf8, LargeUtf8, or Utf8View. Promote the scalar
/// to the actual column representation before invoking any string kernel.
fn string_scalar_for_column(
    scalar: &Scalar<ArrayRef>,
    column_type: &arrow_schema::DataType,
) -> Result<Scalar<ArrayRef>, ArrowError> {
    use arrow_array::{LargeStringArray, StringArray, StringViewArray};
    use arrow_schema::DataType as ArrowDataType;

    let arr = scalar.get().0;
    let value = arr
        .as_any()
        .downcast_ref::<StringArray>()
        .and_then(|s| (s.len() == 1 && s.is_valid(0)).then(|| s.value(0).to_string()));
    let Some(value) = value else {
        return Ok(scalar.clone());
    };
    Ok(match column_type {
        ArrowDataType::Utf8 => Scalar::new(Arc::new(StringArray::from(vec![value])) as ArrayRef),
        ArrowDataType::LargeUtf8 => {
            Scalar::new(Arc::new(LargeStringArray::from(vec![value])) as ArrayRef)
        }
        ArrowDataType::Utf8View => {
            Scalar::new(Arc::new(StringViewArray::from(vec![value])) as ArrayRef)
        }
        ArrowDataType::Dictionary(_, value_type) if value_type.as_ref() == &ArrowDataType::Utf8 => {
            Scalar::new(Arc::new(StringArray::from(vec![value])) as ArrayRef)
        }
        other => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "string predicate against non-string column type {other:?}"
            )))
        }
    })
}

/// Collapse `NULL` mask entries to `false`, matching the residual-filter
/// convention. A mask with no nulls is returned unchanged.
pub(crate) fn sanitize_filter_mask(mask: BooleanArray) -> BooleanArray {
    if mask.null_count() == 0 {
        return mask;
    }

    let values = mask.values() & mask.nulls().expect("mask has nulls").inner();
    BooleanArray::new(values, None)
}

fn combine_filter_masks(left: &BooleanArray, right: &BooleanArray, use_or: bool) -> BooleanArray {
    debug_assert_eq!(left.len(), right.len());
    debug_assert_eq!(left.null_count(), 0);
    debug_assert_eq!(right.null_count(), 0);
    let values = if use_or {
        left.values() | right.values()
    } else {
        left.values() & right.values()
    };
    BooleanArray::new(values, None)
}

fn boolean_mask_from_predicate(
    len: usize,
    mut predicate: impl FnMut(usize) -> bool,
) -> BooleanArray {
    BooleanArray::from((0..len).map(&mut predicate).collect::<Vec<_>>())
}

// ---------------------------------------------------------------------------
// Literal → Arrow scalar conversion
// ---------------------------------------------------------------------------

/// Convert a paimon [`Datum`] literal into an Arrow scalar matching
/// `file_data_type`, or `None` when the literal / type pair is unsupported for
/// filtering (in which case the caller falls open, keeping the row).
pub(crate) fn literal_scalar_for_arrow_filter(
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
        DataType::Decimal(_) => {
            // Decimals never reach here: `evaluate_exact_leaf_predicate` routes
            // decimal columns to `evaluate_decimal_leaf`, which compares by value
            // across scales (matching Paimon `datum_cmp`). Returning `None` is a
            // defensive guard — if a decimal ever reached the Arrow-scalar path it
            // would surface as an unconvertible-literal error rather than a silent
            // wrong-read.
            return Ok(None);
        }
        DataType::Array(_)
        | DataType::Map(_)
        | DataType::Multiset(_)
        | DataType::Row(_)
        | DataType::Variant(_)
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
) -> crate::Result<Option<ArrayRef>> {
    let array: ArrayRef = match precision {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{row_id_leaf, IntType, VarCharType, ROW_ID_FIELD_NAME};
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    fn int_field(id: i32, name: &str) -> DataField {
        DataField::new(id, name.to_string(), DataType::Int(IntType::new()))
    }

    fn str_field(id: i32, name: &str) -> DataField {
        DataField::new(
            id,
            name.to_string(),
            DataType::VarChar(VarCharType::string_type()),
        )
    }

    fn leaf(
        index: usize,
        data_type: DataType,
        op: PredicateOperator,
        literals: Vec<Datum>,
    ) -> Predicate {
        Predicate::Leaf {
            column: format!("c{index}"),
            index,
            data_type,
            op,
            literals,
        }
    }

    fn file_predicates(predicates: Vec<Predicate>, file_fields: Vec<DataField>) -> FilePredicates {
        FilePredicates {
            predicates,
            row_filter_factory: None,
            file_fields,
        }
    }

    fn int_batch(name: &str, values: Vec<Option<i32>>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            name,
            ArrowDataType::Int32,
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn str_batch(name: &str, values: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            name,
            ArrowDataType::Utf8,
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
    }

    fn int_values(batch: &RecordBatch) -> Vec<i32> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn str_values(batch: &RecordBatch) -> Vec<String> {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..col.len()).map(|i| col.value(i).to_string()).collect()
    }

    #[test]
    fn test_in_hash_path_filters_exactly_with_nulls() {
        let f = int_field(0, "age");
        let b = int_batch("age", vec![Some(10), None, Some(20), Some(40), Some(50)]);
        let pred = leaf(
            0,
            DataType::Int(IntType::new()),
            PredicateOperator::In,
            vec![Datum::Int(20), Datum::Int(40), Datum::Int(999)],
        );
        let fp = file_predicates(vec![pred], vec![f.clone()]);
        let out = filter_record_batch_by_predicates(b, &fp, &[f]).unwrap();
        assert_eq!(int_values(&out), vec![20, 40]);
    }

    #[test]
    fn test_not_in_hash_path_excludes_nulls() {
        let f = str_field(0, "name");
        let b = str_batch(
            "name",
            vec![Some("apple"), None, Some("banana"), Some("cherry")],
        );
        let pred = leaf(
            0,
            DataType::VarChar(VarCharType::string_type()),
            PredicateOperator::NotIn,
            vec![Datum::String("banana".to_string())],
        );
        let fp = file_predicates(vec![pred], vec![f.clone()]);
        let out = filter_record_batch_by_predicates(b, &fp, &[f]).unwrap();
        assert_eq!(str_values(&out), vec!["apple", "cherry"]);
    }

    #[test]
    fn test_in_hash_path_on_binary_column() {
        use crate::spec::VarBinaryType;
        let array: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(b"aa".as_slice()),
            None,
            Some(b"bb".as_slice()),
            Some(b"cc".as_slice()),
        ]));
        let data_type =
            DataType::VarBinary(VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).unwrap());
        let literals = vec![Datum::Bytes(b"bb".to_vec()), Datum::Bytes(b"zz".to_vec())];
        let mask =
            evaluate_set_membership_predicate(&array, &data_type, PredicateOperator::In, &literals)
                .unwrap();
        assert_eq!(
            mask.iter().collect::<Vec<_>>(),
            vec![Some(false), Some(false), Some(true), Some(false)]
        );
        let mask = evaluate_set_membership_predicate(
            &array,
            &data_type,
            PredicateOperator::NotIn,
            &literals,
        )
        .unwrap();
        assert_eq!(
            mask.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false), Some(false), Some(true)]
        );
    }

    #[test]
    fn test_in_unconvertible_literal_still_errors() {
        // An out-of-range literal must keep the general path's error behavior:
        // the hash path declines the literal set and the per-literal loop
        // raises the unconvertible-literal error it always has.
        use crate::spec::TinyIntType;
        let array: ArrayRef = Arc::new(arrow_array::Int8Array::from(vec![Some(1i8), Some(2)]));
        let result = evaluate_set_membership_predicate(
            &array,
            &DataType::TinyInt(TinyIntType::new()),
            PredicateOperator::In,
            &[Datum::Long(300)],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_gt_filters_exactly() {
        let f = int_field(0, "age");
        let b = int_batch(
            "age",
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        );
        let pred = leaf(
            0,
            DataType::Int(IntType::new()),
            PredicateOperator::Gt,
            vec![Datum::Int(25)],
        );
        let fp = file_predicates(vec![pred], vec![f.clone()]);
        let out = filter_record_batch_by_predicates(b, &fp, &[f]).unwrap();
        assert_eq!(out.num_rows(), 3);
        assert_eq!(int_values(&out), vec![30, 40, 50]);
    }

    #[test]
    fn test_starts_with_exact() {
        let f = str_field(0, "name");
        let b = str_batch(
            "name",
            vec![
                Some("apple"),
                Some("banana"),
                Some("apricot"),
                Some("cherry"),
            ],
        );
        let pred = leaf(
            0,
            DataType::VarChar(VarCharType::string_type()),
            PredicateOperator::StartsWith,
            vec![Datum::String("ap".to_string())],
        );
        let fp = file_predicates(vec![pred], vec![f.clone()]);
        let out = filter_record_batch_by_predicates(b, &fp, &[f]).unwrap();
        assert_eq!(str_values(&out), vec!["apple", "apricot"]);
    }

    #[test]
    fn test_ends_with_exact() {
        let f = str_field(0, "name");
        let b = str_batch(
            "name",
            vec![
                Some("apple"),
                Some("banana"),
                Some("apricot"),
                Some("cherry"),
            ],
        );
        let pred = leaf(
            0,
            DataType::VarChar(VarCharType::string_type()),
            PredicateOperator::EndsWith,
            vec![Datum::String("y".to_string())],
        );
        let fp = file_predicates(vec![pred], vec![f.clone()]);
        let out = filter_record_batch_by_predicates(b, &fp, &[f]).unwrap();
        assert_eq!(str_values(&out), vec!["cherry"]);
    }

    #[test]
    fn test_contains_exact() {
        let f = str_field(0, "name");
        let b = str_batch(
            "name",
            vec![
                Some("apple"),
                Some("banana"),
                Some("apricot"),
                Some("cherry"),
            ],
        );
        let pred = leaf(
            0,
            DataType::VarChar(VarCharType::string_type()),
            PredicateOperator::Contains,
            vec![Datum::String("err".to_string())],
        );
        let fp = file_predicates(vec![pred], vec![f.clone()]);
        let out = filter_record_batch_by_predicates(b, &fp, &[f]).unwrap();
        assert_eq!(str_values(&out), vec!["cherry"]);
    }

    #[test]
    fn test_like_exact() {
        let f = str_field(0, "name");
        let b = str_batch(
            "name",
            vec![
                Some("apple"),
                Some("banana"),
                Some("apricot"),
                Some("cherry"),
            ],
        );
        let pred = leaf(
            0,
            DataType::VarChar(VarCharType::string_type()),
            PredicateOperator::Like,
            vec![Datum::String("a%".to_string())],
        );
        let fp = file_predicates(vec![pred], vec![f.clone()]);
        let out = filter_record_batch_by_predicates(b, &fp, &[f]).unwrap();
        assert_eq!(str_values(&out), vec!["apple", "apricot"]);
    }

    #[test]
    fn test_between_inclusive_exact() {
        let f = int_field(0, "age");
        let b = int_batch(
            "age",
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        );
        let pred = leaf(
            0,
            DataType::Int(IntType::new()),
            PredicateOperator::Between,
            vec![Datum::Int(20), Datum::Int(40)],
        );
        let fp = file_predicates(vec![pred], vec![f.clone()]);
        let out = filter_record_batch_by_predicates(b, &fp, &[f]).unwrap();
        // Inclusive boundaries → [20, 30, 40].
        assert_eq!(int_values(&out), vec![20, 30, 40]);
    }

    #[test]
    fn test_and_composes() {
        // age > 15 AND age < 45 -> [20, 30, 40]
        let f = int_field(0, "age");
        let b = int_batch(
            "age",
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        );
        let pred = Predicate::And(vec![
            leaf(
                0,
                DataType::Int(IntType::new()),
                PredicateOperator::Gt,
                vec![Datum::Int(15)],
            ),
            leaf(
                0,
                DataType::Int(IntType::new()),
                PredicateOperator::Lt,
                vec![Datum::Int(45)],
            ),
        ]);
        let fp = file_predicates(vec![pred], vec![f.clone()]);
        let out = filter_record_batch_by_predicates(b, &fp, &[f]).unwrap();
        assert_eq!(int_values(&out), vec![20, 30, 40]);
    }

    #[test]
    fn test_or_composes() {
        // age < 15 OR age > 45 -> [10, 50]
        let f = int_field(0, "age");
        let b = int_batch(
            "age",
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        );
        let pred = Predicate::Or(vec![
            leaf(
                0,
                DataType::Int(IntType::new()),
                PredicateOperator::Lt,
                vec![Datum::Int(15)],
            ),
            leaf(
                0,
                DataType::Int(IntType::new()),
                PredicateOperator::Gt,
                vec![Datum::Int(45)],
            ),
        ]);
        let fp = file_predicates(vec![pred], vec![f.clone()]);
        let out = filter_record_batch_by_predicates(b, &fp, &[f]).unwrap();
        assert_eq!(int_values(&out), vec![10, 50]);
    }

    #[test]
    fn test_not_composes() {
        // NOT(age > 25) -> [10, 20]
        let f = int_field(0, "age");
        let b = int_batch(
            "age",
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        );
        let pred = Predicate::Not(Box::new(leaf(
            0,
            DataType::Int(IntType::new()),
            PredicateOperator::Gt,
            vec![Datum::Int(25)],
        )));
        let fp = file_predicates(vec![pred], vec![f.clone()]);
        let out = filter_record_batch_by_predicates(b, &fp, &[f]).unwrap();
        assert_eq!(int_values(&out), vec![10, 20]);
    }

    #[test]
    fn test_null_row_excluded() {
        // age = [10, null, 30]; age > 5 -> [10, 30] (null → false).
        let f = int_field(0, "age");
        let b = int_batch("age", vec![Some(10), None, Some(30)]);
        let pred = leaf(
            0,
            DataType::Int(IntType::new()),
            PredicateOperator::Gt,
            vec![Datum::Int(5)],
        );
        let fp = file_predicates(vec![pred], vec![f.clone()]);
        let out = filter_record_batch_by_predicates(b, &fp, &[f]).unwrap();
        assert_eq!(int_values(&out), vec![10, 30]);
    }

    fn name_age_batch(names: Vec<&str>, ages: Vec<Option<i32>>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("name", ArrowDataType::Utf8, true),
            ArrowField::new("age", ArrowDataType::Int32, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(Int32Array::from(ages)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_widen_scan_fields_adds_predicate_columns_deduped() {
        let name = str_field(1, "name");
        let age = int_field(2, "age");

        // read_fields=[name]; predicate on age -> widened contains name AND age.
        let pred = leaf(
            1,
            DataType::Int(IntType::new()),
            PredicateOperator::Gt,
            vec![Datum::Int(25)],
        );
        let fp = file_predicates(vec![pred], vec![name.clone(), age.clone()]);
        let widened = widen_scan_fields(std::slice::from_ref(&name), Some(&fp));
        assert_eq!(widened.len(), 2);
        assert_eq!(widened[0].name(), "name");
        assert_eq!(widened[1].name(), "age");

        // If the predicate column is already in read_fields, no duplicate.
        let pred_on_name = leaf(
            0,
            DataType::VarChar(VarCharType::string_type()),
            PredicateOperator::StartsWith,
            vec![Datum::String("a".to_string())],
        );
        let fp2 = file_predicates(vec![pred_on_name], vec![name.clone()]);
        let widened2 = widen_scan_fields(std::slice::from_ref(&name), Some(&fp2));
        assert_eq!(widened2.len(), 1);
        assert_eq!(widened2[0].name(), "name");

        // No predicates -> read_fields unchanged.
        let widened3 = widen_scan_fields(std::slice::from_ref(&name), None);
        assert_eq!(widened3.len(), 1);
    }

    #[test]
    fn test_filter_on_batch_containing_predicate_col_is_exact() {
        // batch has [name, age]; scan_fields=[name,age]; predicate age>25.
        let name = str_field(1, "name");
        let age = int_field(2, "age");
        let batch = name_age_batch(
            vec!["a", "b", "c", "d", "e"],
            vec![Some(10), Some(20), Some(30), Some(40), Some(50)],
        );
        let pred = leaf(
            1,
            DataType::Int(IntType::new()),
            PredicateOperator::Gt,
            vec![Datum::Int(25)],
        );
        let fp = file_predicates(vec![pred], vec![name.clone(), age.clone()]);
        let scan_fields = vec![name, age];
        let out = filter_record_batch_by_predicates(batch, &fp, &scan_fields).unwrap();
        // Only matching rows kept, and the batch still has both columns.
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.num_rows(), 3);
        let names = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            (0..names.len())
                .map(|i| names.value(i).to_string())
                .collect::<Vec<_>>(),
            vec!["c", "d", "e"]
        );
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic]
    fn test_missing_predicate_column_is_caught_not_silently_skipped() {
        // batch has [name] only; predicate age>25 with age present in file_fields.
        // The debug_assert must fire (Gap A would otherwise silently skip).
        let name = str_field(1, "name");
        let age = int_field(2, "age");
        let batch = str_batch("name", vec![Some("a"), Some("b"), Some("c")]);
        let pred = leaf(
            1,
            DataType::Int(IntType::new()),
            PredicateOperator::Gt,
            vec![Datum::Int(25)],
        );
        // file_fields has age; scan_fields (present in batch) is [name] only.
        let fp = file_predicates(vec![pred], vec![name.clone(), age]);
        let scan_fields = vec![name];
        let _ = filter_record_batch_by_predicates(batch, &fp, &scan_fields);
    }

    fn batch_with_row_id(values: Vec<i32>, row_ids: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("v", ArrowDataType::Int32, true),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(values)),
                Arc::new(arrow_array::Int64Array::from(row_ids)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_row_id_leaf_resolves_by_name_not_by_placeholder_index() {
        let v = int_field(1, "v");
        let batch = batch_with_row_id(vec![99, 7, 7], vec![1, 2, 3]);
        let pred = row_id_leaf(PredicateOperator::Eq, vec![Datum::Long(1)]);
        let fp = file_predicates(vec![pred], vec![v.clone()]);
        let out = filter_record_batch_by_predicates(batch, &fp, &[v]).unwrap();
        assert_eq!(int_values(&out), vec![99]);
    }

    #[test]
    fn test_row_id_leaf_inside_a_disjunction_resolves_by_name() {
        let v = int_field(1, "v");
        let batch = batch_with_row_id(vec![99, 7, 5], vec![1, 2, 3]);
        let pred = Predicate::or(vec![
            row_id_leaf(PredicateOperator::Eq, vec![Datum::Long(1)]),
            leaf(
                0,
                DataType::Int(IntType::new()),
                PredicateOperator::Eq,
                vec![Datum::Int(7)],
            ),
        ]);
        let fp = file_predicates(vec![pred], vec![v.clone()]);
        let out = filter_record_batch_by_predicates(batch, &fp, &[v]).unwrap();
        assert_eq!(int_values(&out), vec![99, 7]);
    }

    #[test]
    fn test_widen_scan_fields_skips_system_columns() {
        let other = int_field(1, "other");
        let v = int_field(2, "v");
        let fp = file_predicates(
            vec![row_id_leaf(PredicateOperator::Eq, vec![Datum::Long(1)])],
            vec![other, v.clone()],
        );
        let widened = widen_scan_fields(std::slice::from_ref(&v), Some(&fp));
        assert_eq!(widened, vec![v]);
    }

    #[test]
    fn test_filter_when_batch_column_order_differs_from_scan_fields() {
        // Regression: a reader (e.g. ORC `ProjectionMask::named_roots`) may emit
        // batch columns in file-schema order, NOT scan_fields order. The residual
        // evaluator must resolve the predicate column by NAME in the batch, not by
        // its position in scan_fields — otherwise it compares the wrong column and
        // fails with a type mismatch (e.g. "Utf8 == Int32").
        let name = str_field(1, "name");
        let age = int_field(2, "age");
        // scan_fields order is [name, age] (index of age == 1)...
        let scan_fields = vec![name.clone(), age.clone()];
        // ...but the batch emits columns in the OPPOSITE order [age, name].
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("age", ArrowDataType::Int32, true),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(10), Some(20), Some(30)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )
        .unwrap();
        // predicate age > 15; age is file_fields index 1. Positional indexing into
        // the batch by scan_fields position (1) would hit the Utf8 `name` column.
        let pred = leaf(
            1,
            DataType::Int(IntType::new()),
            PredicateOperator::Gt,
            vec![Datum::Int(15)],
        );
        let fp = file_predicates(vec![pred], vec![name, age]);
        let out = filter_record_batch_by_predicates(batch, &fp, &scan_fields).unwrap();
        assert_eq!(out.num_rows(), 2); // age in {20, 30}
                                       // age column is still at batch position 0.
        let ages = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(
            (0..ages.len()).map(|i| ages.value(i)).collect::<Vec<_>>(),
            vec![20, 30]
        );
    }

    #[test]
    fn test_unconvertible_literal_errors_not_fail_open() {
        // A leaf whose literal cannot be converted to an Arrow scalar for the
        // column type (here: an Int column compared to a Bool literal) must
        // ERROR, not silently return all rows. The residual pass is the only
        // enforcement point when the row filter rejected the leaf, so fail-open
        // would be a silent wrong-read.
        let age = int_field(0, "age");
        let b = int_batch("age", vec![Some(10), Some(20), Some(30)]);
        let pred = leaf(
            0,
            DataType::Int(IntType::new()),
            PredicateOperator::Eq,
            vec![Datum::Bool(true)],
        );
        let fp = file_predicates(vec![pred], vec![age.clone()]);
        let result = filter_record_batch_by_predicates(b, &fp, std::slice::from_ref(&age));
        assert!(
            result.is_err(),
            "unconvertible literal must error, not fail-open to all-rows"
        );
    }

    #[test]
    fn test_promoted_column_out_of_range_literal_yields_no_rows_not_error() {
        // Schema evolution: table column promoted INT -> BIGINT, old file column
        // is still INT. A predicate value beyond i32 range must yield "no rows"
        // for this file, NOT an error. The residual casts the INT column up to
        // the predicate's BIGINT type so the literal is representable.
        use crate::spec::BigIntType;
        let file_age = int_field(0, "age"); // file column: INT
        let batch = int_batch("age", vec![Some(10), Some(20), Some(30)]);
        let pred = leaf(
            0,
            DataType::BigInt(BigIntType::new()),
            PredicateOperator::Eq,
            vec![Datum::Long(i64::from(i32::MAX) + 1)],
        );
        let fp = file_predicates(vec![pred], vec![file_age.clone()]);
        let out =
            filter_record_batch_by_predicates(batch, &fp, std::slice::from_ref(&file_age)).unwrap();
        assert_eq!(out.num_rows(), 0, "no INT value can equal i32::MAX+1");
    }

    #[test]
    fn test_promoted_column_in_range_literal_filters_exactly() {
        use crate::spec::BigIntType;
        let file_age = int_field(0, "age");
        let batch = int_batch("age", vec![Some(10), Some(20), Some(30)]);
        let pred = leaf(
            0,
            DataType::BigInt(BigIntType::new()),
            PredicateOperator::Gt,
            vec![Datum::Long(15)],
        );
        let fp = file_predicates(vec![pred], vec![file_age.clone()]);
        let out =
            filter_record_batch_by_predicates(batch, &fp, std::slice::from_ref(&file_age)).unwrap();
        assert_eq!(out.num_rows(), 2, "age in {{20, 30}}");
    }

    #[test]
    fn test_no_predicates_is_identity() {
        let f = int_field(0, "age");
        let b = int_batch("age", vec![Some(10), Some(20), Some(30)]);
        // evaluate_predicates_mask returns None with an empty predicate list.
        let mask =
            evaluate_predicates_mask(&b, &[], std::slice::from_ref(&f), std::slice::from_ref(&f))
                .unwrap();
        assert!(mask.is_none());
        // filter_record_batch_by_predicates leaves the batch unchanged.
        let fp = file_predicates(vec![], vec![f.clone()]);
        let out = filter_record_batch_by_predicates(b, &fp, &[f]).unwrap();
        assert_eq!(out.num_rows(), 3);
        assert_eq!(int_values(&out), vec![10, 20, 30]);
    }

    #[test]
    fn test_binary_ordering_uses_java_unsigned_byte_order() {
        // Paimon Datum::Bytes and Arrow both order bytes as unsigned values.
        // Verify the residual keeps 0xFF for `col > 0x00`.
        use crate::spec::BinaryType;
        let col = DataField::new(
            0,
            "b".to_string(),
            DataType::Binary(BinaryType::new(1).unwrap()),
        );
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "b",
            ArrowDataType::Binary,
            true,
        )]));
        let values: Vec<Option<&[u8]>> = vec![Some(&[0x00]), Some(&[0x01]), Some(&[0xFF])];
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(BinaryArray::from(values))]).unwrap();
        // col > 0x00 : unsigned order -> 0x01 and 0xFF.
        let pred = leaf(
            0,
            DataType::Binary(BinaryType::new(1).unwrap()),
            PredicateOperator::Gt,
            vec![Datum::Bytes(vec![0x00])],
        );
        let fp = file_predicates(vec![pred], vec![col.clone()]);
        let out =
            filter_record_batch_by_predicates(batch, &fp, std::slice::from_ref(&col)).unwrap();
        let out_col = out
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let got: Vec<&[u8]> = (0..out_col.len()).map(|i| out_col.value(i)).collect();
        assert_eq!(
            got,
            vec![&[0x01u8][..], &[0xFFu8][..]],
            "0xFF must be included (unsigned > 0x00)"
        );
    }

    #[test]
    fn test_binary_between_uses_java_unsigned_byte_order() {
        // `b BETWEEN 0x01 AND 0xFF` uses unsigned byte ordering, keeping all
        // values at or above 0x01 through 0xFF and excluding 0x00.
        use crate::spec::BinaryType;
        let col = DataField::new(
            0,
            "b".to_string(),
            DataType::Binary(BinaryType::new(1).unwrap()),
        );
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "b",
            ArrowDataType::Binary,
            true,
        )]));
        let values: Vec<Option<&[u8]>> =
            vec![Some(&[0xFF]), Some(&[0x00]), Some(&[0x01]), Some(&[0x7F])];
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(BinaryArray::from(values))]).unwrap();
        let pred = leaf(
            0,
            DataType::Binary(BinaryType::new(1).unwrap()),
            PredicateOperator::Between,
            vec![Datum::Bytes(vec![0x01]), Datum::Bytes(vec![0xFF])],
        );
        let fp = file_predicates(vec![pred], vec![col.clone()]);
        let out =
            filter_record_batch_by_predicates(batch, &fp, std::slice::from_ref(&col)).unwrap();
        let out_col = out
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let got: Vec<&[u8]> = (0..out_col.len()).map(|i| out_col.value(i)).collect();
        assert_eq!(
            got,
            vec![&[0xFFu8][..], &[0x01u8][..], &[0x7Fu8][..]],
            "unsigned range [0x01, 0xFF] excludes only 0x00"
        );
    }

    #[test]
    fn test_decimal_cross_scale_predicate_matches_by_value() {
        // Paimon compares decimals by value across scales. A literal 1.0 (scale 1)
        // against a DECIMAL(_, 2) column must not error and must match 1.00.
        use crate::spec::DecimalType;
        let col = DataField::new(
            0,
            "d".to_string(),
            DataType::Decimal(DecimalType::with_nullable(true, 10, 2).unwrap()),
        );
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "d",
            ArrowDataType::Decimal128(10, 2),
            true,
        )]));
        // values: 1.00, 2.00, 0.50  (unscaled at scale 2)
        let arr = arrow_array::Decimal128Array::from(vec![Some(100), Some(200), Some(50)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        // predicate: d = 1.0  (literal unscaled=10, scale=1) -> must match 1.00.
        let pred = leaf(
            0,
            DataType::Decimal(DecimalType::with_nullable(true, 10, 2).unwrap()),
            PredicateOperator::Eq,
            vec![Datum::Decimal {
                unscaled: 10,
                precision: 10,
                scale: 1,
            }],
        );
        let fp = file_predicates(vec![pred], vec![col.clone()]);
        let out =
            filter_record_batch_by_predicates(batch, &fp, std::slice::from_ref(&col)).unwrap();
        assert_eq!(out.num_rows(), 1, "1.0 must match 1.00 across scales");
    }

    #[test]
    fn test_decimal_finer_scale_literal_is_exact_not_error() {
        // Reviewer's case: DECIMAL(_,1) column filtered by `d > 1.05` (literal
        // scale 2, not representable at scale 1). Must be exact (== `d >= 1.1`),
        // never a read error. Column values: 1.0, 1.1, 1.2 (unscaled at scale 1).
        use crate::spec::DecimalType;
        let col = DataField::new(
            0,
            "d".to_string(),
            DataType::Decimal(DecimalType::with_nullable(true, 10, 1).unwrap()),
        );
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "d",
            ArrowDataType::Decimal128(10, 1),
            true,
        )]));
        let arr = arrow_array::Decimal128Array::from(vec![Some(10), Some(11), Some(12)])
            .with_precision_and_scale(10, 1)
            .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        // d > 1.05  (unscaled 105, scale 2)  ->  d in {1.1, 1.2}.
        let pred = leaf(
            0,
            DataType::Decimal(DecimalType::with_nullable(true, 10, 1).unwrap()),
            PredicateOperator::Gt,
            vec![Datum::Decimal {
                unscaled: 105,
                precision: 10,
                scale: 2,
            }],
        );
        let fp = file_predicates(vec![pred], vec![col.clone()]);
        let out =
            filter_record_batch_by_predicates(batch, &fp, std::slice::from_ref(&col)).unwrap();
        assert_eq!(out.num_rows(), 2, "d > 1.05 == d >= 1.1 -> {{1.1, 1.2}}");
    }
}
