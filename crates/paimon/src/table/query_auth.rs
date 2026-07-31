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

//! Query-auth enforcement: apply the REST server's per-user row filter and
//! column masking exactly to read output. Parsing of the Java `Predicate` /
//! `Transform` JSON lives in [`crate::spec`]; anything unrecognised is an
//! error, so callers keep the table fail-closed.

use crate::arrow::residual::{
    boolean_mask_from_predicate, evaluate_column_predicate, evaluate_decimal_leaf,
    literal_scalar_for_arrow_filter, sanitize_filter_mask,
};
use crate::spec::{
    DataField, DataType, Datum, Predicate, PredicateOperator, Transform, TransformInput,
};
use crate::{Error, Result};
use arrow_arith::boolean::{and_kleene, not, or_kleene};
use arrow_array::{ArrayRef, BooleanArray, Float32Array, Float64Array, RecordBatch};
use std::collections::HashSet;
use std::sync::Arc;

/// Row filters and column masks the REST server granted this user.
/// `authorized = None` means all columns; `Some(set)` scopes the grant to those
/// table-schema indices. Only the REST catalog constructs grants.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct QueryAuthGrant {
    filters: Vec<Predicate>,
    masks: Vec<ColumnMask>,
    authorized: Option<HashSet<usize>>,
    /// System fields the request asked about (`_ROW_ID`, …). They have no table
    /// index, so `authorized` cannot hold them.
    ///
    /// `Some(set)` approves exactly `set` — an empty set approves none, so a
    /// full projection is not blanket approval for `_ROW_ID`. `None` means
    /// system scoping does not apply: internal raw reads (index builds, write
    /// rewrites) authorize through `authorize_unrestricted_read`, run only when
    /// the server imposed no rules at all, and legitimately read `_ROW_ID`.
    authorized_system: Option<HashSet<String>>,
    /// Where this grant's positional indices are meaningful. The schema id
    /// alone would not do: it is a per-table counter, so two fresh tables
    /// both sit at 0.
    binding: GrantBinding,
}

/// Identity a grant is bound to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GrantBinding {
    /// REST table UUID when available: an identifier or schema id can be reused
    /// across catalogs or a drop/recreate, a UUID cannot.
    uuid: Option<String>,
    /// Catalog identity: two REST aliases can share a location.
    identifier: String,
    location: String,
    branch: String,
    schema_id: i64,
}

impl GrantBinding {
    pub(crate) fn of(table: &super::Table) -> Self {
        Self {
            uuid: table.rest_env().map(|e| e.uuid().to_string()),
            identifier: table.identifier().full_name(),
            location: table.location().to_string(),
            branch: table.branch().to_string(),
            schema_id: table.schema().id(),
        }
    }
}

impl QueryAuthGrant {
    /// `binding` is required so it cannot be forgotten.
    pub(crate) fn new(
        filters: Vec<Predicate>,
        masks: Vec<ColumnMask>,
        authorized: Option<HashSet<usize>>,
        authorized_system: Option<HashSet<String>>,
        binding: GrantBinding,
    ) -> Self {
        Self {
            filters,
            masks,
            authorized,
            authorized_system,
            binding,
        }
    }

    /// Like [`Self::check_system_scope`], for system fields a read path adds on
    /// top of its read type (the audit schema's `rowkind`).
    pub(crate) fn check_system_scope_by_name(&self, names: &[&str]) -> Result<()> {
        let Some(approved) = &self.authorized_system else {
            return Ok(());
        };
        for name in names {
            if !approved.contains(*name) {
                return Err(unsupported(format!(
                    "query-auth read emits system column `{name}` outside the authorized set"
                )));
            }
        }
        Ok(())
    }

    /// Fail closed when `read_type` projects a system field outside this grant's
    /// scope. Scoped by name: system fields have no table index.
    pub(crate) fn check_system_scope(&self, read_type: &[DataField]) -> Result<()> {
        let Some(approved) = &self.authorized_system else {
            return Ok(());
        };
        for field in read_type.iter().filter(|f| is_reserved_system_field(f)) {
            if !approved.contains(field.name()) {
                return Err(unsupported(format!(
                    "query-auth read projects system column `{}` outside the authorized set",
                    field.name()
                )));
            }
        }
        Ok(())
    }

    /// Whether this grant may be enforced on `table`.
    pub(crate) fn matches_table(&self, table: &super::Table) -> bool {
        self.binding == GrantBinding::of(table)
    }

    /// Fully unrestricted: every column approved, no filter, no masking.
    pub(crate) fn is_unrestricted(&self) -> bool {
        self.authorized.is_none() && self.filters.is_empty() && self.masks.is_empty()
    }

    /// Whether the SERVER restricted this user, as opposed to the client merely
    /// scoping its own projection. Statistics suppression and the historical /
    /// scan-all gates key off this: a column scope distorts nothing.
    pub(crate) fn has_server_restrictions(&self) -> bool {
        !self.filters.is_empty() || !self.masks.is_empty()
    }

    pub(crate) fn filters(&self) -> &[Predicate] {
        &self.filters
    }

    pub(crate) fn masks(&self) -> &[ColumnMask] {
        &self.masks
    }

    /// Whether every table-schema index in `columns` was authorized. A grant
    /// scoped to a subset does not authorize columns outside it, so a wider
    /// projection or a predicate on an un-approved column fails closed.
    pub(crate) fn authorizes_columns(&self, columns: impl IntoIterator<Item = usize>) -> bool {
        match &self.authorized {
            None => true,
            Some(set) => columns.into_iter().all(|c| set.contains(&c)),
        }
    }

    /// Whether this grant carries a row filter. Such a filter is applied as a
    /// residual pass in `TableRead::to_arrow`, so split row counts, count
    /// statistics, and count-based limit pushdown are no longer exact.
    pub(crate) fn has_row_filter(&self) -> bool {
        !self.filters.is_empty()
    }

    /// Table-schema indices of columns this grant masks (empty when no masking).
    /// Callers must not push predicates on these columns to scan pruning, which
    /// would leak the raw value via row presence.
    pub(crate) fn masked_columns(&self) -> Vec<usize> {
        self.masks.iter().map(|m| m.column).collect()
    }

    /// The first of `columns` (table-schema indices) this grant does not
    /// authorize, if any.
    pub(crate) fn first_unauthorized(
        &self,
        columns: impl IntoIterator<Item = usize>,
    ) -> Option<usize> {
        columns.into_iter().find(|c| !self.authorizes_columns([*c]))
    }

    /// Field IDs the grant must physically read (filter columns, mask targets
    /// and inputs). Projection planning must include them, or column-slice
    /// pruning drops the file and the column reads as null.
    pub(crate) fn read_field_ids(&self, fields: &[DataField]) -> Vec<i32> {
        let mut indices = HashSet::new();
        for filter in &self.filters {
            filter.collect_leaf_field_indices(&mut indices);
        }
        for mask in &self.masks {
            indices.insert(mask.column);
            mask.transform.collect_field_indices(&mut indices);
        }
        indices
            .into_iter()
            .filter_map(|i| fields.get(i).map(|f| f.id()))
            .collect()
    }
}

/// Mask one column (by table-schema index) with a transform.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnMask {
    pub(crate) column: usize,
    pub(crate) transform: Transform,
}

fn unsupported(message: String) -> Error {
    Error::Unsupported { message }
}

fn field_name(fields: &[DataField], column: usize) -> &str {
    fields.get(column).map(|f| f.name()).unwrap_or("?")
}

/// Fail-closed error for a caller predicate on a masked column (would leak the
/// raw value via row selection). Shared by every builder/scan choke point.
pub(crate) fn masked_filter_error(fields: &[DataField], column: usize) -> Error {
    unsupported(format!(
        "cannot filter on masked column `{}`",
        field_name(fields, column)
    ))
}

/// Fail-closed error for a read that touches a column outside the grant's scope.
pub(crate) fn unauthorized_column_error(fields: &[DataField], column: usize) -> Error {
    unsupported(format!(
        "query-auth read touches column `{}` outside the authorized set",
        field_name(fields, column)
    ))
}

/// Reserved system fields a read type may project. The reader produces them, so
/// they carry no grant scope. Both id and name must match, so a forged field
/// cannot borrow a system id. Mirrors Java `SpecialFields.SYSTEM_FIELD_NAMES`.
const RESERVED_SYSTEM_FIELDS: [(i32, &str); 4] = [
    (crate::spec::ROW_ID_FIELD_ID, crate::spec::ROW_ID_FIELD_NAME),
    (
        crate::spec::SEQUENCE_NUMBER_FIELD_ID,
        crate::spec::SEQUENCE_NUMBER_FIELD_NAME,
    ),
    (
        crate::spec::VALUE_KIND_FIELD_ID,
        crate::spec::VALUE_KIND_FIELD_NAME,
    ),
    (
        crate::spec::ROW_KIND_FIELD_ID,
        crate::spec::ROW_KIND_FIELD_NAME,
    ),
];

pub(crate) fn is_reserved_system_field_name(name: &str) -> bool {
    RESERVED_SYSTEM_FIELDS.iter().any(|(_, n)| *n == name)
}

pub(crate) fn is_reserved_system_field(field: &DataField) -> bool {
    RESERVED_SYSTEM_FIELDS
        .iter()
        .any(|(id, name)| *id == field.id() && *name == field.name())
}

/// Table-schema indices a read type touches, rejecting any field that is not a
/// canonical `(id, name)` pair of `fields`.
///
/// Scoping resolves by id while the physical read resolves by name, so an
/// authorized id under another column's name would be scoped by one and read as
/// the other. `TableRead::new` is public, so this is reachable.
pub(crate) fn canonical_projection(
    fields: &[DataField],
    read_type: &[DataField],
) -> crate::Result<Vec<usize>> {
    let mut indices = Vec::with_capacity(read_type.len());
    for field in read_type {
        let by_id = fields.iter().position(|s| s.id() == field.id());
        let by_name = fields.iter().position(|s| s.name() == field.name());
        match (by_id, by_name) {
            (Some(i), Some(n)) if i == n => indices.push(i),
            // Absent from the schema. Anything but a system field is a dropped
            // column, which the reader still resolves by id in older files.
            // Java rejects it in `TableQueryAuthResult.checkFieldExists`.
            (None, None) if is_reserved_system_field(field) => {}
            _ => {
                return Err(unsupported(format!(
                    "query-auth read type field #{} `{}` is not a column of this table",
                    field.id(),
                    field.name()
                )))
            }
        }
    }
    Ok(indices)
}

/// Names of the reserved system fields a read projects, plus any the caller's
/// filter references. Shared by the Paimon and Format read builders, which
/// otherwise kept two copies of this and drifted apart.
///
/// `filter_system_names` must be captured before row-id extraction rewrites the
/// predicates, and `slices_by_row_id` covers an explicit row-range slice, which
/// selects by `_ROW_ID` with no predicate at all.
pub(crate) fn projected_system_field_names(
    read_type: Option<&[DataField]>,
    filter_system_names: &HashSet<String>,
    slices_by_row_id: bool,
) -> Vec<String> {
    let mut names: HashSet<String> = read_type
        .map(|fields| {
            fields
                .iter()
                .filter(|f| is_reserved_system_field(f))
                .map(|f| f.name().to_string())
                .collect()
        })
        .unwrap_or_default();
    names.extend(filter_system_names.iter().cloned());
    if slices_by_row_id {
        names.insert(crate::spec::ROW_ID_FIELD_NAME.to_string());
    }
    let mut names: Vec<String> = names.into_iter().collect();
    names.sort();
    names
}

/// Everything a read must satisfy to run under `grant`, in one place.
///
/// Every read-side gate calls exactly this, so a check cannot be added to one
/// gate and forgotten in another — the failure mode this module kept hitting.
/// Returns the read type's table-schema indices, which callers need anyway.
///
/// `implicit_system_fields` are system columns the path emits on top of
/// `read_type` (the audit schema prepends `rowkind`), which therefore never
/// reached the projection or the auth request.
pub(crate) fn authorize_read(
    grant: &QueryAuthGrant,
    table: &super::Table,
    read_type: &[DataField],
    predicates: &[Predicate],
    implicit_system_fields: &[&str],
) -> crate::Result<Vec<usize>> {
    // The grant's positional indices only mean anything on the table it was
    // issued for; `Table::authorize_rewrite_splits` is public, so a grant can
    // arrive on another table's splits.
    if !grant.matches_table(table) {
        return Err(unsupported(
            "a query-auth grant issued for a different table or schema cannot be \
             enforced here; re-plan the scan"
                .to_string(),
        ));
    }
    grant.check_system_scope(read_type)?;
    grant.check_system_scope_by_name(implicit_system_fields)?;

    let fields = table.schema().fields();
    let projected = canonical_projection(fields, read_type)?;
    let mut filter_columns = HashSet::new();
    for predicate in predicates {
        predicate.collect_leaf_field_indices(&mut filter_columns);
    }
    scope_check(grant, fields, &filter_columns, Some(projected.clone()))?;
    Ok(projected)
}

/// Scope check shared by the read/scan gates: fail closed when the caller filter
/// references a masked column (pruning on its raw value would leak it) or
/// touches one outside the grant. `projected = None` means all columns.
pub(crate) fn scope_check(
    grant: &QueryAuthGrant,
    fields: &[DataField],
    filter_columns: &HashSet<usize>,
    projected: Option<Vec<usize>>,
) -> crate::Result<()> {
    if let Some(column) = grant
        .masked_columns()
        .into_iter()
        .find(|c| filter_columns.contains(c))
    {
        return Err(masked_filter_error(fields, column));
    }
    let projected = projected.unwrap_or_else(|| (0..fields.len()).collect());
    if let Some(column) =
        grant.first_unauthorized(projected.into_iter().chain(filter_columns.iter().copied()))
    {
        return Err(unauthorized_column_error(fields, column));
    }
    Ok(())
}

/// Parse the auth response's JSON filter strings into predicates whose leaf
/// indices refer to `fields` (table-schema order). Empty strings are skipped
/// (Java parity); anything unparseable is an error.
pub(crate) fn parse_auth_filters(
    filters: &[String],
    fields: &[DataField],
) -> Result<Vec<Predicate>> {
    filters
        .iter()
        // Java skips only length-0 entries (`StringUtils.isEmpty`). Whitespace
        // is invalid JSON: dropping it could leave a grant with no filter.
        .filter(|f| !f.is_empty())
        .map(|f| Predicate::from_rest_json(f, fields))
        .collect()
}

// ==================== Exact evaluation ====================

/// Evaluate the ANDed `predicates` against `batch` (columns 1:1 with
/// `batch_fields`; leaf indices into `schema_fields`) and drop non-matching
/// rows. Unlike the pruning evaluators, anything unevaluable is an error — a
/// security filter must not fall open.
pub(crate) fn strict_filter_batch(
    batch: &RecordBatch,
    predicates: &[Predicate],
    schema_fields: &[DataField],
    batch_fields: &[DataField],
) -> Result<RecordBatch> {
    let mut combined: Option<BooleanArray> = None;
    for predicate in predicates {
        let mask = strict_mask(batch, predicate, schema_fields, batch_fields)?;
        combined = Some(match combined {
            Some(existing) => kleene(and_kleene(&existing, &mask))?,
            None => mask,
        });
    }
    let Some(mask) = combined else {
        return Ok(batch.clone());
    };
    let mask = sanitize_filter_mask(mask);
    arrow_select::filter::filter_record_batch(batch, &mask).map_err(|e| Error::DataInvalid {
        message: format!("failed to apply query-auth row filter: {e}"),
        source: Some(Box::new(e)),
    })
}

fn strict_mask(
    batch: &RecordBatch,
    predicate: &Predicate,
    schema_fields: &[DataField],
    batch_fields: &[DataField],
) -> Result<BooleanArray> {
    match predicate {
        Predicate::AlwaysTrue => Ok(BooleanArray::from(vec![true; batch.num_rows()])),
        Predicate::AlwaysFalse => Ok(BooleanArray::from(vec![false; batch.num_rows()])),
        Predicate::And(children) => fold_masks(batch, children, schema_fields, batch_fields, true),
        Predicate::Or(children) => fold_masks(batch, children, schema_fields, batch_fields, false),
        Predicate::Not(inner) => {
            let mask = strict_mask(batch, inner, schema_fields, batch_fields)?;
            kleene(not(&mask))
        }
        Predicate::Leaf {
            index,
            op,
            literals,
            ..
        } => {
            let field = schema_fields.get(*index).ok_or_else(|| {
                unsupported(format!(
                    "query-auth filter references unknown field #{index}"
                ))
            })?;
            let position = batch_fields
                .iter()
                .position(|f| f.id() == field.id() && f.name() == field.name())
                .ok_or_else(|| {
                    unsupported(format!(
                        "query-auth filter field `{}` missing from read",
                        field.name()
                    ))
                })?;
            let column = canonicalize_nan(batch.column(position));
            strict_leaf_mask(&column, field.data_type(), *op, literals)
        }
    }
}

/// Replace every NaN in a float column with the canonical (positive) NaN.
///
/// Arrow's total ordering sorts a NEGATIVE NaN below every finite value, so
/// `f < 0` would admit it; Java's `Float`/`Double.compare` makes every NaN the
/// greatest. Authorization filters only — ordinary pushdown keeps Arrow
/// semantics. Signed zero already agrees.
fn canonicalize_nan(column: &ArrayRef) -> ArrayRef {
    match column.data_type() {
        arrow_schema::DataType::Float32 => {
            let values = column.as_any().downcast_ref::<Float32Array>();
            match values {
                Some(values) if values.iter().any(|v| v.is_some_and(f32::is_nan)) => {
                    Arc::new(values.unary::<_, arrow_array::types::Float32Type>(|v| {
                        if v.is_nan() {
                            f32::NAN
                        } else {
                            v
                        }
                    })) as ArrayRef
                }
                _ => Arc::clone(column),
            }
        }
        arrow_schema::DataType::Float64 => {
            let values = column.as_any().downcast_ref::<Float64Array>();
            match values {
                Some(values) if values.iter().any(|v| v.is_some_and(f64::is_nan)) => {
                    Arc::new(values.unary::<_, arrow_array::types::Float64Type>(|v| {
                        if v.is_nan() {
                            f64::NAN
                        } else {
                            v
                        }
                    })) as ArrayRef
                }
                _ => Arc::clone(column),
            }
        }
        _ => Arc::clone(column),
    }
}

fn fold_masks(
    batch: &RecordBatch,
    children: &[Predicate],
    schema_fields: &[DataField],
    batch_fields: &[DataField],
    use_and: bool,
) -> Result<BooleanArray> {
    let mut combined: Option<BooleanArray> = None;
    for child in children {
        let mask = strict_mask(batch, child, schema_fields, batch_fields)?;
        combined = Some(match combined {
            Some(existing) if use_and => kleene(and_kleene(&existing, &mask))?,
            Some(existing) => kleene(or_kleene(&existing, &mask))?,
            None => mask,
        });
    }
    combined.ok_or_else(|| unsupported("query-auth filter has an empty compound".to_string()))
}

fn strict_leaf_mask(
    column: &ArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray> {
    // Decimals compare by value across scales (`datum_cmp`), which no Arrow
    // scalar expresses — `literal_scalar_for_arrow_filter` returns `None` for
    // them by design, so without this every decimal policy would error. The
    // exact evaluator keeps nulls, so Kleene combination is unchanged.
    if matches!(column.data_type(), arrow_schema::DataType::Decimal128(_, _))
        && !matches!(op, PredicateOperator::IsNull | PredicateOperator::IsNotNull)
    {
        return kleene(evaluate_decimal_leaf(column, op, literals));
    }
    let scalar = |literal: &Datum| -> Result<arrow_array::Scalar<ArrayRef>> {
        literal_scalar_for_arrow_filter(literal, data_type)?.ok_or_else(|| {
            unsupported(format!(
                "query-auth filter literal is not comparable to type {data_type:?}"
            ))
        })
    };
    match op {
        PredicateOperator::IsNull => Ok(boolean_mask_from_predicate(column.len(), |row| {
            column.is_null(row)
        })),
        PredicateOperator::IsNotNull => Ok(boolean_mask_from_predicate(column.len(), |row| {
            column.is_valid(row)
        })),
        PredicateOperator::In | PredicateOperator::NotIn => {
            // Kleene IN: OR of equalities; x NOT IN (..) = NOT(IN), nulls stay null.
            let mut combined = BooleanArray::from(vec![false; column.len()]);
            for literal in literals {
                let eq = kleene(evaluate_column_predicate(
                    column,
                    &scalar(literal)?,
                    PredicateOperator::Eq,
                ))?;
                combined = kleene(or_kleene(&combined, &eq))?;
            }
            if matches!(op, PredicateOperator::NotIn) {
                combined = kleene(not(&combined))?;
                if literals.is_empty() {
                    // `x NOT IN ()` is true only for non-null rows.
                    combined =
                        boolean_mask_from_predicate(column.len(), |row| column.is_valid(row));
                }
            }
            Ok(combined)
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
            let literal = literals.first().ok_or_else(|| {
                unsupported("query-auth filter comparison without literal".to_string())
            })?;
            kleene(evaluate_column_predicate(column, &scalar(literal)?, op))
        }
        PredicateOperator::Between | PredicateOperator::NotBetween => {
            let (Some(low), Some(high)) = (literals.first(), literals.get(1)) else {
                return Err(unsupported(
                    "query-auth BETWEEN filter without bounds".to_string(),
                ));
            };
            let lo = kleene(evaluate_column_predicate(
                column,
                &scalar(low)?,
                PredicateOperator::GtEq,
            ))?;
            let hi = kleene(evaluate_column_predicate(
                column,
                &scalar(high)?,
                PredicateOperator::LtEq,
            ))?;
            let between = kleene(and_kleene(&lo, &hi))?;
            if matches!(op, PredicateOperator::NotBetween) {
                kleene(not(&between))
            } else {
                Ok(between)
            }
        }
    }
}

fn kleene(
    result: std::result::Result<BooleanArray, arrow_schema::ArrowError>,
) -> Result<BooleanArray> {
    result.map_err(|e| Error::DataInvalid {
        message: format!("failed to evaluate query-auth row filter: {e}"),
        source: Some(Box::new(e)),
    })
}

// ==================== Column masking ====================

fn mask_err(detail: impl std::fmt::Display) -> Error {
    unsupported(format!("cannot parse query-auth column masking: {detail}"))
}

/// Parse the auth response's `columnMasking` map (column name -> Java
/// `Transform` JSON) against `fields` (table-schema order).
pub(crate) fn parse_column_masking(
    masking: &std::collections::HashMap<String, String>,
    fields: &[DataField],
) -> Result<Vec<ColumnMask>> {
    let mut masks = Vec::with_capacity(masking.len());
    for (column, json) in masking {
        let target = fields
            .iter()
            .position(|f| f.name() == column)
            .ok_or_else(|| mask_err(format!("unknown field `{column}`")))?;
        let transform = Transform::from_rest_json(json, fields)?;
        // The masked value replaces the column in place, so a type-changing
        // transform (`CAST(id AS STRING)` on an INT column) cannot be
        // represented. Compared via the arrow type (nullability-agnostic).
        if let Some(out) = mask_output_type(&transform, fields) {
            let target_type = crate::arrow::paimon_type_to_arrow(fields[target].data_type())?;
            if out != target_type {
                return Err(mask_err(format!(
                    "masking `{column}` produces {out:?} but the column is {target_type:?}"
                )));
            }
        }
        // A mask that can yield null on a NOT NULL column would leave the output
        // batch's schema claiming non-nullable, letting engines fold
        // `col IS [NOT] NULL` before the masked-predicate guard. Fail closed.
        if !fields[target].data_type().is_nullable() && mask_can_be_null(&transform, fields) {
            return Err(mask_err(format!(
                "masking `{column}` can produce null but the column is NOT NULL"
            )));
        }
        masks.push(ColumnMask {
            column: target,
            transform,
        });
    }
    // Deterministic order regardless of map iteration.
    masks.sort_by_key(|m| m.column);

    // Masks read their inputs from the RAW batch (like Java), so a mask
    // referencing ANOTHER mask's target would copy its unmasked value out.
    // Referencing your own target (`name := UPPER(name)`) stays valid.
    let targets: HashSet<usize> = masks.iter().map(|m| m.column).collect();
    for mask in &masks {
        let mut inputs = HashSet::new();
        mask.transform.collect_field_indices(&mut inputs);
        if let Some(other) = inputs
            .into_iter()
            .find(|i| *i != mask.column && targets.contains(i))
        {
            return Err(mask_err(format!(
                "masking `{}` reads masked column `{}`, which would expose its raw value",
                field_name(fields, mask.column),
                field_name(fields, other)
            )));
        }
    }
    Ok(masks)
}

/// Whether a mask transform can produce a null value.
fn mask_can_be_null(transform: &Transform, fields: &[DataField]) -> bool {
    let field_nullable = |index: &usize| fields[*index].data_type().is_nullable();
    let input_nullable = |inputs: &[TransformInput]| {
        inputs.iter().any(|i| match i {
            TransformInput::Literal(literal) => literal.is_none(),
            TransformInput::Field(index) => field_nullable(index),
        })
    };
    match transform {
        Transform::Null => true,
        Transform::FieldRef(index) | Transform::Cast(index, _) => field_nullable(index),
        Transform::Upper(inputs) | Transform::Lower(inputs) | Transform::Concat(inputs) => {
            input_nullable(inputs)
        }
        // CONCAT_WS skips null payloads, so only a null separator (the first
        // input) can make the result null.
        Transform::ConcatWs(inputs) => inputs.first().is_some_and(|sep| match sep {
            TransformInput::Literal(literal) => literal.is_none(),
            TransformInput::Field(index) => field_nullable(index),
        }),
    }
}

/// Arrow output type of a mask transform, or `None` when it always matches the
/// target column (the `NULL` transform builds a null of the column's own type).
fn mask_output_type(transform: &Transform, fields: &[DataField]) -> Option<arrow_schema::DataType> {
    let of = |index: &usize| crate::arrow::paimon_type_to_arrow(fields[*index].data_type()).ok();
    match transform {
        Transform::Null => None,
        Transform::FieldRef(index) => of(index),
        Transform::Cast(_, to) => crate::arrow::paimon_type_to_arrow(to).ok(),
        Transform::Upper(_)
        | Transform::Lower(_)
        | Transform::Concat(_)
        | Transform::ConcatWs(_) => Some(arrow_schema::DataType::Utf8),
    }
}

/// Overwrite masked columns of `batch` (whose columns correspond 1:1 to
/// `batch_fields`). Masks whose target column is not in the batch are skipped
/// (Java parity); anything that cannot be evaluated is an error.
pub(crate) fn mask_batch(
    batch: &RecordBatch,
    masks: &[ColumnMask],
    schema_fields: &[DataField],
    batch_fields: &[DataField],
) -> Result<RecordBatch> {
    use arrow_array::new_null_array;

    // Nothing to mask (e.g. a `COUNT(*)` read projects no columns); return the
    // batch unchanged, preserving its row count even with zero columns.
    if masks.is_empty() {
        return Ok(batch.clone());
    }

    // All batch positions holding a given table-schema field (a projection may
    // repeat a column, so every copy must be masked, not just the first).
    let positions_of = |schema_index: usize| -> Result<Vec<usize>> {
        let field = schema_fields.get(schema_index).ok_or_else(|| {
            unsupported(format!(
                "query-auth mask references unknown field #{schema_index}"
            ))
        })?;
        // By field id alone: the read type may carry the column under another
        // name (aliasing, or a rename seen from an older file schema), and
        // matching the name too would skip the mask and emit the raw value.
        Ok(batch_fields
            .iter()
            .enumerate()
            .filter(|(_, f)| f.id() == field.id())
            .map(|(pos, _)| pos)
            .collect())
    };
    let input_column = |schema_index: usize| -> Result<ArrayRef> {
        positions_of(schema_index)?
            .first()
            .map(|pos| batch.column(*pos).clone())
            .ok_or_else(|| unsupported("query-auth mask input missing from read".to_string()))
    };

    let mut columns = batch.columns().to_vec();
    for mask in masks {
        let targets = positions_of(mask.column)?;
        // `masks` is already filtered to targets the caller projects, so a mask
        // that resolves to no batch column means the read cannot enforce it —
        // emitting the column unmasked would leak the raw value.
        let Some(&first) = targets.first() else {
            return Err(unsupported(format!(
                "query-auth mask for column `{}` cannot be applied: the column is missing \
                 from the read",
                field_name(schema_fields, mask.column)
            )));
        };
        let target_type = batch.schema().field(first).data_type().clone();
        let masked: ArrayRef = match &mask.transform {
            Transform::Null => new_null_array(&target_type, batch.num_rows()),
            Transform::FieldRef(index) => input_column(*index)?,
            Transform::Cast(index, to) => {
                let to_arrow = crate::arrow::paimon_type_to_arrow(to)?;
                cast_masked(&input_column(*index)?, &to_arrow)?
            }
            Transform::Upper(inputs) => string_mask(batch, inputs, &input_column, |v| {
                Some(v.first()?.as_ref().map(|s| s.to_uppercase()))
            })?,
            Transform::Lower(inputs) => string_mask(batch, inputs, &input_column, |v| {
                Some(v.first()?.as_ref().map(|s| s.to_lowercase()))
            })?,
            // SQL semantics: CONCAT is null if any input is null; CONCAT_WS
            // uses the first input as separator and skips null values.
            Transform::Concat(inputs) => string_mask(batch, inputs, &input_column, |v| {
                Some(
                    v.iter()
                        .cloned()
                        .collect::<Option<Vec<_>>>()
                        .map(|p| p.concat()),
                )
            })?,
            Transform::ConcatWs(inputs) => string_mask(batch, inputs, &input_column, |v| {
                let (sep, rest) = v.split_first()?;
                Some(
                    sep.as_ref()
                        .map(|sep| rest.iter().flatten().cloned().collect::<Vec<_>>().join(sep)),
                )
            })?,
        };
        // Parse-time type checks guarantee a compatible type, so this only
        // aligns arrow representations. Mask every copy of the target column.
        let masked = cast_masked(&masked, &target_type)?;
        for pos in targets {
            columns[pos] = masked.clone();
        }
    }
    RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| Error::DataInvalid {
        message: format!("failed to apply query-auth column masking: {e}"),
        source: Some(Box::new(e)),
    })
}

fn cast_masked(array: &ArrayRef, to: &arrow_schema::DataType) -> Result<ArrayRef> {
    if array.data_type() == to {
        return Ok(array.clone());
    }
    arrow_cast::cast(array, to).map_err(|e| {
        unsupported(format!(
            "query-auth mask value of type {:?} cannot be cast to column type {to:?}: {e}",
            array.data_type()
        ))
    })
}

/// Evaluate a string transform row by row. `combine` receives the resolved
/// inputs (None = SQL NULL) and returns the masked value; a `None` from
/// `combine` means the transform is malformed for this input arity.
fn string_mask(
    batch: &RecordBatch,
    inputs: &[TransformInput],
    input_column: &dyn Fn(usize) -> Result<ArrayRef>,
    combine: impl Fn(&[Option<String>]) -> Option<Option<String>>,
) -> Result<ArrayRef> {
    use arrow_array::{Array, StringArray};
    use std::sync::Arc;

    // Resolve field inputs once, as string arrays (None = literal slot).
    let resolved: Vec<Option<ArrayRef>> = inputs
        .iter()
        .map(|input| match input {
            TransformInput::Literal(_) => Ok(None),
            TransformInput::Field(index) => {
                cast_masked(&input_column(*index)?, &arrow_schema::DataType::Utf8).map(Some)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let mut values: Vec<Option<String>> = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let row_inputs = inputs
            .iter()
            .zip(&resolved)
            .map(|(input, array)| match (input, array) {
                (TransformInput::Literal(s), _) => Ok(s.clone()),
                (TransformInput::Field(_), Some(array)) => {
                    let strings = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| unsupported("mask input is not a string".to_string()))?;
                    Ok((!strings.is_null(row)).then(|| strings.value(row).to_string()))
                }
                (TransformInput::Field(_), None) => unreachable!("field inputs are resolved above"),
            })
            .collect::<Result<Vec<_>>>()?;
        let value = combine(&row_inputs)
            .ok_or_else(|| unsupported("query-auth string mask is malformed".to_string()))?;
        values.push(value);
    }
    Ok(Arc::new(StringArray::from(values)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{IntType, VarCharType};
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    fn fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::new(255).unwrap()),
            ),
        ]
    }

    fn leaf_json(function: &str, field: &str, literals: &str) -> String {
        format!(
            r#"{{"kind":"LEAF","transform":{{"name":"FIELD_REF","fieldRef":{{"index":0,"name":"{field}","type":"INT"}}}},"function":"{function}","literals":{literals}}}"#
        )
    }

    fn batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, true),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("c"),
                    Some("d"),
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_strict_filter_batch_filters_rows() {
        let fields = fields();
        let filters =
            parse_auth_filters(&[leaf_json("GREATER_THAN", "id", "[1]")], &fields).unwrap();
        let filtered = strict_filter_batch(&batch(), &filters, &fields, &fields).unwrap();
        // id > 1 keeps rows 2 and 4; the NULL row is excluded.
        assert_eq!(filtered.num_rows(), 2);
    }

    #[test]
    fn test_strict_filter_matches_java_nan_ordering() {
        use crate::spec::{DoubleType, PredicateBuilder};
        use arrow_array::Float64Array;
        use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

        // Java `Double.compare` canonicalizes NaN above every finite value, so
        // `f < 0` must reject NaN — including a NEGATIVE NaN, which Arrow's
        // total ordering would otherwise sort below finite values and admit.
        let fields = vec![DataField::new(
            0,
            "f".to_string(),
            DataType::Double(DoubleType::new()),
        )];
        let neg_nan = f64::from_bits(0xFFF8_0000_0000_0000);
        let batch = RecordBatch::try_new(
            std::sync::Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "f",
                ArrowDataType::Float64,
                true,
            )])),
            vec![std::sync::Arc::new(Float64Array::from(vec![
                neg_nan,
                f64::NAN,
                -1.0,
                1.0,
            ]))],
        )
        .unwrap();

        let less = PredicateBuilder::new(&fields)
            .less_than("f", crate::spec::Datum::Double(0.0))
            .unwrap();
        let filtered = strict_filter_batch(&batch, &[less], &fields, &fields).unwrap();
        assert_eq!(
            filtered.num_rows(),
            1,
            "only -1.0 may pass `f < 0`; neither NaN sign may"
        );

        let greater = PredicateBuilder::new(&fields)
            .greater_than("f", crate::spec::Datum::Double(0.0))
            .unwrap();
        let filtered = strict_filter_batch(&batch, &[greater], &fields, &fields).unwrap();
        assert_eq!(
            filtered.num_rows(),
            3,
            "both NaNs and 1.0 pass `f > 0` (NaN is greatest, like Java)"
        );
    }

    #[test]
    fn test_strict_filter_not_excludes_nulls() {
        let fields = fields();
        // NOT (id = 2): NULL rows must stay excluded (SQL three-valued logic).
        let json = format!(
            r#"{{"kind":"COMPOUND","function":"AND","children":[{}]}}"#,
            leaf_json("NOT_EQUAL", "id", "[2]")
        );
        let filters = parse_auth_filters(&[json], &fields).unwrap();
        let filtered = strict_filter_batch(&batch(), &filters, &fields, &fields).unwrap();
        assert_eq!(
            filtered.num_rows(),
            2,
            "rows 1 and 4 only, not the NULL row"
        );
    }

    /// Java #7034 baseline: filter each supported literal type exactly.
    #[test]
    fn test_strict_filter_batch_typed_matrix() {
        use crate::spec::{BigIntType, BooleanType, DoubleType, FloatType};
        use arrow_array::{BooleanArray, Float32Array, Float64Array, Int64Array};

        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "age".to_string(), DataType::BigInt(BigIntType::new())),
            DataField::new(2, "salary".to_string(), DataType::Double(DoubleType::new())),
            DataField::new(
                3,
                "is_active".to_string(),
                DataType::Boolean(BooleanType::new()),
            ),
            DataField::new(4, "score".to_string(), DataType::Float(FloatType::new())),
            DataField::new(
                5,
                "name".to_string(),
                DataType::VarChar(VarCharType::new(255).unwrap()),
            ),
        ];
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, true),
            ArrowField::new("age", ArrowDataType::Int64, true),
            ArrowField::new("salary", ArrowDataType::Float64, true),
            ArrowField::new("is_active", ArrowDataType::Boolean, true),
            ArrowField::new("score", ArrowDataType::Float32, true),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int64Array::from(vec![25, 30, 35, 28])),
                Arc::new(Float64Array::from(vec![50000.0, 60000.0, 70000.0, 55000.0])),
                Arc::new(BooleanArray::from(vec![true, false, true, true])),
                Arc::new(Float32Array::from(vec![85.5, 90.0, 95.5, 88.0])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "David"])),
            ],
        )
        .unwrap();
        fn typed_leaf(function: &str, field: &str, ftype: &str, literals: &str) -> String {
            format!(
                r#"{{"kind":"LEAF","transform":{{"name":"FIELD_REF","fieldRef":{{"index":0,"name":"{field}","type":"{ftype}"}}}},"function":"{function}","literals":{literals}}}"#
            )
        }
        // (filter, expected surviving ids) — mirrors Java MockRESTCatalogTest.
        let cases: Vec<(String, Vec<i32>)> = vec![
            (typed_leaf("GREATER_THAN", "id", "INT", "[2]"), vec![3, 4]),
            (
                typed_leaf("GREATER_OR_EQUAL", "age", "BIGINT", "[30]"),
                vec![2, 3],
            ),
            (
                typed_leaf("GREATER_THAN", "salary", "DOUBLE", "[55000.0]"),
                vec![2, 3],
            ),
            (
                typed_leaf("EQUAL", "is_active", "BOOLEAN", "[true]"),
                vec![1, 3, 4],
            ),
            (
                typed_leaf("GREATER_OR_EQUAL", "score", "FLOAT", "[90.0]"),
                vec![2, 3],
            ),
            (
                typed_leaf("EQUAL", "name", "STRING", "[\"Alice\"]"),
                vec![1],
            ),
            (
                // Two predicates ANDed by the grant list semantics.
                format!(
                    r#"{{"kind":"COMPOUND","function":"AND","children":[{},{}]}}"#,
                    typed_leaf("GREATER_OR_EQUAL", "age", "BIGINT", "[30]"),
                    typed_leaf("EQUAL", "is_active", "BOOLEAN", "[true]")
                ),
                vec![3],
            ),
        ];
        for (json, expected) in cases {
            let filters = parse_auth_filters(std::slice::from_ref(&json), &fields).unwrap();
            let filtered = strict_filter_batch(&batch, &filters, &fields, &fields).unwrap();
            let ids = filtered
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec();
            assert_eq!(ids, expected, "for {json}");
        }
    }

    fn masking(column: &str, json: &str) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::from([(column.to_string(), json.to_string())])
    }

    #[test]
    fn test_reject_mask_reading_another_masked_column() {
        // `name := UPPER(name)` is the normal self-reference and stays valid.
        let fields = fields();
        assert!(parse_column_masking(
            &masking(
                "name",
                r#"{"name":"UPPER","inputs":[{"index":1,"name":"name","type":"STRING"}]}"#
            ),
            &fields
        )
        .is_ok());

        // But a mask that reads ANOTHER masked column would copy that column's
        // raw value out (masks read the unmasked batch), defeating its mask.
        let both = std::collections::HashMap::from([
            ("id".to_string(), r#"{"name":"NULL"}"#.to_string()),
            (
                "name".to_string(),
                r#"{"name":"UPPER","inputs":[{"index":1,"name":"name","type":"STRING"}]}"#
                    .to_string(),
            ),
        ]);
        assert!(
            parse_column_masking(&both, &fields).is_ok(),
            "unrelated masks are fine"
        );

        let cross = std::collections::HashMap::from([
            ("name".to_string(), r#"{"name":"NULL"}"#.to_string()),
            (
                "alias".to_string(),
                r#"{"name":"UPPER","inputs":[{"index":1,"name":"name","type":"STRING"}]}"#
                    .to_string(),
            ),
        ]);
        let mut fields_with_alias = fields.clone();
        fields_with_alias.push(DataField::new(
            2,
            "alias".to_string(),
            DataType::VarChar(VarCharType::new(255).unwrap()),
        ));
        let Err(err) = parse_column_masking(&cross, &fields_with_alias) else {
            panic!("a mask reading another masked column must fail closed");
        };
        assert!(err.to_string().contains("raw value"), "got: {err}");
    }

    #[test]
    fn test_parse_column_masking() {
        let fields = fields();
        let masks = parse_column_masking(&masking("name", r#"{"name":"NULL"}"#), &fields).unwrap();
        assert!(matches!(masks[0].transform, Transform::Null));
        assert_eq!(masks[0].column, 1);

        let upper = r#"{"name":"UPPER","inputs":[{"index":1,"name":"name","type":"STRING"}]}"#;
        let masks = parse_column_masking(&masking("name", upper), &fields).unwrap();
        assert!(matches!(&masks[0].transform, Transform::Upper(inputs) if inputs.len() == 1));

        // Unknown transform / unknown column / bad JSON: all fail closed.
        for (column, json) in [
            ("name", r#"{"name":"ROT13"}"#),
            ("missing", r#"{"name":"NULL"}"#),
            ("name", "not json"),
        ] {
            assert!(parse_column_masking(&masking(column, json), &fields).is_err());
        }
    }

    #[test]
    fn test_mask_batch_null_and_string_transforms() {
        use arrow_array::Array;
        let fields = fields();

        // NULL mask: the whole column becomes null.
        let masks = parse_column_masking(&masking("name", r#"{"name":"NULL"}"#), &fields).unwrap();
        let masked = mask_batch(&batch(), &masks, &fields, &fields).unwrap();
        assert_eq!(masked.column(1).null_count(), 4);
        assert_eq!(masked.column(0).null_count(), 1, "other columns untouched");

        // CONCAT_WS("-", literal, field): "x-a", "x-b", ...
        let concat =
            r#"{"name":"CONCAT_WS","inputs":["-","x",{"index":1,"name":"name","type":"STRING"}]}"#;
        let masks = parse_column_masking(&masking("name", concat), &fields).unwrap();
        let masked = mask_batch(&batch(), &masks, &fields, &fields).unwrap();
        let names = masked
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "x-a");

        // Masked column absent from the batch: fail closed. The caller filters
        // masks to projected targets, so this means the read cannot enforce the
        // mask — emitting the column unmasked would leak the raw value.
        let one_col = batch().project(&[0]).unwrap();
        let one_field = vec![fields[0].clone()];
        let masks = parse_column_masking(&masking("name", r#"{"name":"NULL"}"#), &fields).unwrap();
        let Err(err) = mask_batch(&one_col, &masks, &fields, &one_field) else {
            panic!("a mask whose target is missing from the read must fail closed");
        };
        assert!(err.to_string().contains("cannot be applied"), "got: {err}");
    }

    #[test]
    fn test_reject_type_changing_cast_mask() {
        let fields = fields();
        // CAST(id AS STRING) on an INT column changes type -> fail closed.
        let cast =
            r#"{"name":"CAST","fieldRef":{"index":0,"name":"id","type":"INT"},"type":"STRING"}"#;
        assert!(parse_column_masking(&masking("id", cast), &fields).is_err());
        // A string transform on a non-string column also fails closed.
        let upper = r#"{"name":"UPPER","inputs":[{"index":0,"name":"id","type":"INT"}]}"#;
        assert!(parse_column_masking(&masking("id", upper), &fields).is_err());
    }

    #[test]
    fn test_mask_batch_masks_every_duplicate_target() {
        use arrow_array::Array;
        let fields = fields();
        let masks = parse_column_masking(&masking("name", r#"{"name":"NULL"}"#), &fields).unwrap();
        // A projection that repeats the masked column: both copies must be masked.
        let base = batch();
        let dup = base.project(&[1, 1]).unwrap();
        let dup_fields = vec![fields[1].clone(), fields[1].clone()];
        let masked = mask_batch(&dup, &masks, &fields, &dup_fields).unwrap();
        assert_eq!(masked.column(0).null_count(), 4);
        assert_eq!(masked.column(1).null_count(), 4);
    }

    #[test]
    fn test_implicit_system_fields_are_scope_checked() {
        // The audit schema prepends `rowkind` (and may prepend
        // `_SEQUENCE_NUMBER`) on top of the read type, so neither reaches the
        // auth request via the projection and both need a by-name check.
        let grant = QueryAuthGrant::new(
            Vec::new(),
            Vec::new(),
            Some(HashSet::from([0])),
            Some(HashSet::from(
                [crate::spec::ROW_KIND_FIELD_NAME.to_string()],
            )),
            GrantBinding::default(),
        );
        assert!(grant
            .check_system_scope_by_name(&[crate::spec::ROW_KIND_FIELD_NAME])
            .is_ok());
        assert!(grant
            .check_system_scope_by_name(&[
                crate::spec::ROW_KIND_FIELD_NAME,
                crate::spec::SEQUENCE_NUMBER_FIELD_NAME,
            ])
            .is_err());
        // An unscoped grant covers everything.
        let all = QueryAuthGrant::new(Vec::new(), Vec::new(), None, None, GrantBinding::default());
        assert!(all
            .check_system_scope_by_name(&[crate::spec::SEQUENCE_NUMBER_FIELD_NAME])
            .is_ok());
    }

    #[test]
    fn test_whitespace_only_filter_fails_closed() {
        use crate::spec::{DataType, IntType};
        let fields = vec![DataField::new(
            0,
            "id".to_string(),
            DataType::Int(IntType::new()),
        )];
        // Java skips only length-0 entries, so " " reaches the parser and throws.
        // Dropping it here would leave a grant with no filter at all.
        assert!(parse_auth_filters(&[" ".to_string()], &fields).is_err());
        assert!(parse_auth_filters(&["\n".to_string()], &fields).is_err());
        // A genuinely empty entry is still skipped (Java parity).
        assert_eq!(
            parse_auth_filters(&[String::new()], &fields).unwrap().len(),
            0
        );
    }

    #[test]
    fn test_system_scope_does_not_make_a_grant_restricted() {
        // `is_unrestricted` answers "did the server restrict, or did the client
        // scope columns". The system scope answers a different question, so
        // letting it gate this made every write/build path on a query-auth table
        // fail: `authorize_unrestricted_read` would never see an unrestricted
        // grant.
        let scoped_system = QueryAuthGrant::new(
            Vec::new(),
            Vec::new(),
            None,
            Some(HashSet::new()),
            GrantBinding::default(),
        );
        assert!(
            scoped_system.is_unrestricted(),
            "an empty system scope with no rules is still unrestricted"
        );
        assert!(!scoped_system.has_server_restrictions());

        // Internal raw reads carry no system scoping at all.
        let internal =
            QueryAuthGrant::new(Vec::new(), Vec::new(), None, None, GrantBinding::default());
        assert!(internal.is_unrestricted());
    }

    #[test]
    fn test_full_projection_is_not_blanket_system_approval() {
        use crate::spec::{DataType, IntType};
        // `select = None` means "every table column", never "every reserved
        // system field": the request only carries the system names the read
        // needs, so anything else was never shown to the server.
        let grant = QueryAuthGrant::new(
            Vec::new(),
            Vec::new(),
            None,
            Some(HashSet::new()),
            GrantBinding::default(),
        );
        let row_id = DataField::new(
            crate::spec::ROW_ID_FIELD_ID,
            crate::spec::ROW_ID_FIELD_NAME.to_string(),
            DataType::Int(IntType::new()),
        );
        assert!(grant.check_system_scope(&[row_id]).is_err());
        assert!(grant
            .check_system_scope_by_name(&[crate::spec::ROW_KIND_FIELD_NAME])
            .is_err());
    }

    #[test]
    fn test_system_field_scope_is_enforced_by_name() {
        use crate::spec::{DataType, IntType};
        let row_id = DataField::new(
            crate::spec::ROW_ID_FIELD_ID,
            crate::spec::ROW_ID_FIELD_NAME.to_string(),
            DataType::Int(IntType::new()),
        );
        let seq = DataField::new(
            crate::spec::SEQUENCE_NUMBER_FIELD_ID,
            crate::spec::SEQUENCE_NUMBER_FIELD_NAME.to_string(),
            DataType::Int(IntType::new()),
        );
        // Authorized for _ROW_ID only: system fields have no table index, so the
        // positional scope cannot cover them and they need their own check.
        let grant = QueryAuthGrant::new(
            Vec::new(),
            Vec::new(),
            Some(HashSet::from([0])),
            Some(HashSet::from([crate::spec::ROW_ID_FIELD_NAME.to_string()])),
            GrantBinding::default(),
        );
        assert!(grant
            .check_system_scope(std::slice::from_ref(&row_id))
            .is_ok());
        assert!(grant.check_system_scope(&[seq]).is_err());
        // An unscoped grant covers everything.
        let all = QueryAuthGrant::new(Vec::new(), Vec::new(), None, None, GrantBinding::default());
        assert!(all.check_system_scope(&[row_id]).is_ok());
    }

    #[test]
    fn test_canonical_projection_system_and_dropped_fields() {
        use crate::spec::{DataType, IntType};
        let fields = vec![DataField::new(
            0,
            "id".to_string(),
            DataType::Int(IntType::new()),
        )];

        // Produced by the reader, so it maps to no index and is allowed.
        let row_id = DataField::new(
            crate::spec::ROW_ID_FIELD_ID,
            crate::spec::ROW_ID_FIELD_NAME.to_string(),
            DataType::Int(IntType::new()),
        );
        assert_eq!(
            canonical_projection(&fields, std::slice::from_ref(&row_id)).unwrap(),
            Vec::<usize>::new()
        );

        // Matches neither an id nor a name, but is still readable by field id
        // from older files — must not pass as a system field.
        let dropped = DataField::new(7, "dropped".to_string(), DataType::Int(IntType::new()));
        assert!(
            canonical_projection(&fields, &[dropped]).is_err(),
            "a dropped column must not be waved through as a system field"
        );

        // A system id under another name is not a system field.
        let forged = DataField::new(
            crate::spec::ROW_ID_FIELD_ID,
            "not_row_id".to_string(),
            DataType::Int(IntType::new()),
        );
        assert!(canonical_projection(&fields, &[forged]).is_err());
    }

    #[test]
    fn test_grant_authorized_column_scope() {
        // A grant scoped to a subset is not globally unrestricted and rejects
        // columns outside the approved set.
        let grant = QueryAuthGrant::new(
            Vec::new(),
            Vec::new(),
            Some(HashSet::from([0])),
            None,
            GrantBinding::default(),
        );
        assert!(!grant.is_unrestricted());
        assert!(grant.authorizes_columns([0]));
        assert!(!grant.authorizes_columns([1]));
        // `None` (all columns) with no filter/mask is fully unrestricted.
        let all = QueryAuthGrant::new(Vec::new(), Vec::new(), None, None, GrantBinding::default());
        assert!(all.is_unrestricted());
        assert!(all.authorizes_columns([0, 1, 99]));
    }
}
