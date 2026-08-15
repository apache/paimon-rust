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

//! Predicate data structures and builder for filter pushdown.
//!
//! Provides a reusable `Predicate` tree that can be shared by partition pruning,
//! manifest statistics pruning, and data file statistics pruning.
//!
//! Reference:
//! - Java `PredicateBuilder` / `LeafPredicate` / `CompoundPredicate`

use crate::error::*;
use crate::spec::binary_row::BinaryRow;
use crate::spec::types::DataType;
use crate::spec::{is_row_id_column, DataField};
use std::cmp::Ordering;
use std::fmt;

// ---------------------------------------------------------------------------
// Datum
// ---------------------------------------------------------------------------

/// A typed literal value for predicate comparison.
///
/// Each variant corresponds to one or more Paimon `DataType`s and carries the
/// internal representation used by `BinaryRow`. This avoids untyped `Object`
/// boxing (as in Java Paimon) and provides compile-time safety.
///
/// `PartialEq` is manually implemented so that `Decimal` uses mathematical
/// equivalence (matching Java Paimon's `Decimal` which uses `compareTo() == 0`
/// rather than `BigDecimal.equals` which is scale-sensitive),
/// e.g. `Decimal(10, scale=1)` == `Decimal(100, scale=2)`
/// because both represent `1.0`.
///
#[derive(Debug, Clone)]
pub enum Datum {
    Bool(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    /// Epoch days since 1970-01-01.
    Date(i32),
    /// Millis of day.
    Time(i32),
    /// Aligns with `BinaryRow::get_timestamp_raw` which returns `(i64, i32)`.
    Timestamp {
        millis: i64,
        nanos: i32,
    },
    /// Same binary layout as `Timestamp`, different semantic (local timezone).
    LocalZonedTimestamp {
        millis: i64,
        nanos: i32,
    },
    Decimal {
        unscaled: i128,
        precision: u32,
        scale: u32,
    },
    Bytes(Vec<u8>),
    Variant {
        value: Vec<u8>,
        metadata: Vec<u8>,
    },
}

impl fmt::Display for Datum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bool(v) => write!(f, "{v}"),
            Self::TinyInt(v) => write!(f, "{v}"),
            Self::SmallInt(v) => write!(f, "{v}"),
            Self::Int(v) => write!(f, "{v}"),
            Self::Long(v) => write!(f, "{v}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Double(v) => write!(f, "{v}"),
            Self::String(v) => write!(f, "'{v}'"),
            Self::Date(v) => write!(f, "DATE({v})"),
            Self::Time(v) => write!(f, "TIME({v})"),
            Self::Timestamp { millis, nanos } => write!(f, "TS({millis},{nanos})"),
            Self::LocalZonedTimestamp { millis, nanos } => write!(f, "LZTS({millis},{nanos})"),
            Self::Decimal {
                unscaled, scale, ..
            } => write!(f, "DEC({unscaled},s{scale})"),
            Self::Bytes(v) => write!(f, "BYTES(len={})", v.len()),
            Self::Variant { value, metadata } => {
                write!(
                    f,
                    "VARIANT(value_len={},metadata_len={})",
                    value.len(),
                    metadata.len()
                )
            }
        }
    }
}

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        datum_eq(self, other)
    }
}

impl PartialOrd for Datum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        datum_cmp(self, other)
    }
}

fn datum_eq(lhs: &Datum, rhs: &Datum) -> bool {
    datum_cmp(lhs, rhs) == Some(Ordering::Equal)
}

pub(crate) fn datum_cmp(lhs: &Datum, rhs: &Datum) -> Option<Ordering> {
    match (lhs, rhs) {
        (Datum::Bool(a), Datum::Bool(b)) => a.partial_cmp(b),
        (Datum::TinyInt(a), Datum::TinyInt(b)) => a.partial_cmp(b),
        (Datum::SmallInt(a), Datum::SmallInt(b)) => a.partial_cmp(b),
        (Datum::Int(a), Datum::Int(b)) => a.partial_cmp(b),
        (Datum::Long(a), Datum::Long(b)) => a.partial_cmp(b),
        (Datum::Float(a), Datum::Float(b)) => a.partial_cmp(b),
        (Datum::Double(a), Datum::Double(b)) => a.partial_cmp(b),
        (Datum::String(a), Datum::String(b)) => a.partial_cmp(b),
        (Datum::Date(a), Datum::Date(b)) => a.partial_cmp(b),
        (Datum::Time(a), Datum::Time(b)) => a.partial_cmp(b),
        (
            Datum::Timestamp {
                millis: ma,
                nanos: na,
            },
            Datum::Timestamp {
                millis: mb,
                nanos: nb,
            },
        ) => (ma, na).partial_cmp(&(mb, nb)),
        (
            Datum::LocalZonedTimestamp {
                millis: ma,
                nanos: na,
            },
            Datum::LocalZonedTimestamp {
                millis: mb,
                nanos: nb,
            },
        ) => (ma, na).partial_cmp(&(mb, nb)),
        (
            Datum::Decimal {
                unscaled: ua,
                scale: sa,
                ..
            },
            Datum::Decimal {
                unscaled: ub,
                scale: sb,
                ..
            },
        ) => decimal_cmp(*ua, *sa, *ub, *sb),
        (Datum::Bytes(a), Datum::Bytes(b)) => Some(java_bytes_cmp(a, b)),
        (
            Datum::Variant {
                value: va,
                metadata: ma,
            },
            Datum::Variant {
                value: vb,
                metadata: mb,
            },
        ) => {
            if va == vb && ma == mb {
                Some(Ordering::Equal)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Compare two decimals by mathematical value.
///
/// Normalizes both to the larger scale, then compares unscaled values.
/// E.g. `(10, scale=1)` vs `(100, scale=2)` → both represent 1.0 → equal.
fn decimal_cmp(ua: i128, sa: u32, ub: i128, sb: u32) -> Option<Ordering> {
    if sa == sb {
        return ua.partial_cmp(&ub);
    }
    let (na, nb) = if sa < sb {
        (ua.checked_mul(pow10_i128(sb - sa))?, ub)
    } else {
        (ua, ub.checked_mul(pow10_i128(sa - sb))?)
    };
    na.partial_cmp(&nb)
}

/// Match Java `CompareUtils.compare(byte[], byte[])`, which compares bytes as
/// unsigned values lexicographically.
fn java_bytes_cmp(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}

/// 10^exp as i128.  Returns i128::MAX for exponents that would overflow.
fn pow10_i128(exp: u32) -> i128 {
    const MAX_EXP: u32 = 38; // 10^38 fits in i128
    if exp > MAX_EXP {
        return i128::MAX;
    }
    let mut result: i128 = 1;
    for _ in 0..exp {
        result = result.saturating_mul(10);
    }
    result
}

// PredicateOperator
// ---------------------------------------------------------------------------

/// Predicate operators for leaf predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateOperator {
    IsNull,
    IsNotNull,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    In,
    NotIn,
    StartsWith,
    EndsWith,
    Contains,
    Like,
    Between,
    NotBetween,
}

impl fmt::Display for PredicateOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IsNull => write!(f, "IS NULL"),
            Self::IsNotNull => write!(f, "IS NOT NULL"),
            Self::Eq => write!(f, "="),
            Self::NotEq => write!(f, "!="),
            Self::Lt => write!(f, "<"),
            Self::LtEq => write!(f, "<="),
            Self::Gt => write!(f, ">"),
            Self::GtEq => write!(f, ">="),
            Self::In => write!(f, "IN"),
            Self::NotIn => write!(f, "NOT IN"),
            Self::StartsWith => write!(f, "STARTS_WITH"),
            Self::EndsWith => write!(f, "ENDS_WITH"),
            Self::Contains => write!(f, "CONTAINS"),
            Self::Like => write!(f, "LIKE"),
            Self::Between => write!(f, "BETWEEN"),
            Self::NotBetween => write!(f, "NOT BETWEEN"),
        }
    }
}

// ---------------------------------------------------------------------------
// Predicate
// ---------------------------------------------------------------------------

/// A filter predicate — a tree of conditions.
///
/// - `Leaf`: a column-level comparison (e.g. `dt = '2024-01-01'`)
/// - `And` / `Or`: N-ary logical connectives (auto-flattened)
/// - `Not`: logical negation
/// - `AlwaysTrue` / `AlwaysFalse`: constant predicates
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Leaf: column op literal(s).
    Leaf {
        /// Column name.
        column: String,
        /// Field index in the **table schema** (not partition row).
        index: usize,
        /// DataType of this field, needed for BinaryRow value extraction in evaluators.
        data_type: DataType,
        /// Comparison operator.
        op: PredicateOperator,
        /// Literal values (empty for IsNull/IsNotNull, one for comparison ops,
        /// multiple for In/NotIn).
        literals: Vec<Datum>,
    },
    /// N-ary conjunction. Invariant: always flattened (no nested And).
    And(Vec<Predicate>),
    /// N-ary disjunction. Invariant: always flattened (no nested Or).
    Or(Vec<Predicate>),
    /// Logical negation.
    Not(Box<Predicate>),
    /// Always evaluates to true.
    AlwaysTrue,
    /// Always evaluates to false.
    AlwaysFalse,
}

impl Predicate {
    /// Combine predicates with AND, with recursive flattening and constant absorption.
    ///
    /// - `AND(p, AlwaysTrue)` → `p` (identity element filtered out)
    /// - `AND(p, AlwaysFalse)` → `AlwaysFalse` (annihilator short-circuits)
    /// - Nested `And` nodes are recursively flattened
    /// - Empty input → `AlwaysTrue`
    /// - Single element → unwrapped
    pub fn and(predicates: Vec<Predicate>) -> Predicate {
        let mut flat = Vec::with_capacity(predicates.len());
        Self::flatten_and(predicates, &mut flat);
        match flat.len() {
            0 => Predicate::AlwaysTrue,
            1 => flat.into_iter().next().unwrap(),
            _ => Predicate::And(flat),
        }
    }

    /// Recursively collect non-And children, absorbing constants.
    fn flatten_and(predicates: Vec<Predicate>, out: &mut Vec<Predicate>) {
        for p in predicates {
            match p {
                Predicate::AlwaysTrue => {}
                Predicate::AlwaysFalse => {
                    out.clear();
                    out.push(Predicate::AlwaysFalse);
                    return;
                }
                Predicate::And(children) => Self::flatten_and(children, out),
                other => out.push(other),
            }
            // Check if a nested flatten hit AlwaysFalse
            if out.first() == Some(&Predicate::AlwaysFalse) {
                return;
            }
        }
    }

    /// Combine predicates with OR, with recursive flattening and constant absorption.
    ///
    /// - `OR(p, AlwaysFalse)` → `p` (identity element filtered out)
    /// - `OR(p, AlwaysTrue)` → `AlwaysTrue` (annihilator short-circuits)
    /// - Nested `Or` nodes are recursively flattened
    /// - Empty input → `AlwaysFalse`
    /// - Single element → unwrapped
    pub fn or(predicates: Vec<Predicate>) -> Predicate {
        let mut flat = Vec::with_capacity(predicates.len());
        Self::flatten_or(predicates, &mut flat);
        match flat.len() {
            0 => Predicate::AlwaysFalse,
            1 => flat.into_iter().next().unwrap(),
            _ => Predicate::Or(flat),
        }
    }

    /// Recursively collect non-Or children, absorbing constants.
    fn flatten_or(predicates: Vec<Predicate>, out: &mut Vec<Predicate>) {
        for p in predicates {
            match p {
                Predicate::AlwaysFalse => {}
                Predicate::AlwaysTrue => {
                    out.clear();
                    out.push(Predicate::AlwaysTrue);
                    return;
                }
                Predicate::Or(children) => Self::flatten_or(children, out),
                other => out.push(other),
            }
            if out.first() == Some(&Predicate::AlwaysTrue) {
                return;
            }
        }
    }

    /// Negate a predicate with simplification.
    ///
    /// - `NOT(NOT(p))` → `p` (double negation elimination)
    /// - `NOT(AlwaysTrue)` → `AlwaysFalse`
    /// - `NOT(AlwaysFalse)` → `AlwaysTrue`
    pub fn negate(predicate: Predicate) -> Predicate {
        match predicate {
            Predicate::Not(inner) => *inner,
            Predicate::AlwaysTrue => Predicate::AlwaysFalse,
            Predicate::AlwaysFalse => Predicate::AlwaysTrue,
            other => Predicate::Not(Box::new(other)),
        }
    }

    /// Split a predicate at AND boundaries into conjuncts (recursive).
    ///
    /// Unlike a simple one-level unwrap, this recursively flattens nested
    /// `And` nodes — necessary because `Predicate` is a public enum and
    /// callers may construct `And(vec![And(...), ...])` directly without
    /// going through `Predicate::and()` which auto-flattens.
    ///
    /// Reference: Java `PredicateBuilder.splitAnd` which recursively
    /// splits `CompoundPredicate(And, children)`.
    pub(crate) fn split_and(self) -> Vec<Predicate> {
        match self {
            Predicate::And(children) => children.into_iter().flat_map(|c| c.split_and()).collect(),
            other => vec![other],
        }
    }

    /// Remap leaf field indices from table schema space to partition row space.
    ///
    /// Returns `Some(remapped)` if *all* leaf nodes in this subtree reference
    /// partition columns; `None` otherwise. This guarantees safety under NOT/OR:
    /// a mixed predicate is never partially remapped.
    ///
    /// `mapping` is the output of `field_idx_to_partition_idx`.
    pub(crate) fn remap_field_index(&self, mapping: &[Option<usize>]) -> Option<Predicate> {
        match self {
            Predicate::Leaf {
                column,
                index,
                data_type,
                op,
                literals,
            } => {
                if is_row_id_column(column) {
                    return None;
                }
                let new_index = (*mapping.get(*index)?)?;
                Some(Predicate::Leaf {
                    column: column.clone(),
                    index: new_index,
                    data_type: data_type.clone(),
                    op: *op,
                    literals: literals.clone(),
                })
            }
            Predicate::And(children) => {
                let remapped: Option<Vec<_>> = children
                    .iter()
                    .map(|c| c.remap_field_index(mapping))
                    .collect();
                Some(Predicate::and(remapped?))
            }
            Predicate::Or(children) => {
                let remapped: Option<Vec<_>> = children
                    .iter()
                    .map(|c| c.remap_field_index(mapping))
                    .collect();
                Some(Predicate::or(remapped?))
            }
            Predicate::Not(inner) => {
                let remapped = inner.remap_field_index(mapping)?;
                Some(Predicate::negate(remapped))
            }
            Predicate::AlwaysTrue => Some(Predicate::AlwaysTrue),
            Predicate::AlwaysFalse => Some(Predicate::AlwaysFalse),
        }
    }

    /// Check whether every leaf field in this subtree is present in `mapping`.
    ///
    /// This is used to decide whether the original conjunct still needs to be
    /// retained as a residual data predicate after partition projection.
    pub(crate) fn references_only_mapped_fields(&self, mapping: &[Option<usize>]) -> bool {
        match self {
            Predicate::Leaf { column, index, .. } => {
                !is_row_id_column(column) && mapping.get(*index).is_some_and(Option::is_some)
            }
            Predicate::And(children) | Predicate::Or(children) => children
                .iter()
                .all(|child| child.references_only_mapped_fields(mapping)),
            Predicate::Not(inner) => inner.references_only_mapped_fields(mapping),
            Predicate::AlwaysTrue | Predicate::AlwaysFalse => true,
        }
    }

    /// Project leaf field indices from table schema space into a smaller field space.
    ///
    /// A `_ROW_ID` leaf never maps — see [`is_row_id_column`].
    ///
    /// Unlike [`Self::remap_field_index`], mixed `AND` subtrees keep the children
    /// that can be projected and drop the rest. `OR` and `NOT` still require all
    /// children to be projectable to preserve correctness.
    ///
    /// This matches the partition predicate extraction semantics used by Java
    /// `splitPartitionPredicatesAndDataPredicates`.
    pub(crate) fn project_field_index_inclusive(
        &self,
        mapping: &[Option<usize>],
    ) -> Option<Predicate> {
        match self {
            Predicate::Leaf {
                column,
                index,
                data_type,
                op,
                literals,
            } => {
                if is_row_id_column(column) {
                    return None;
                }
                let new_index = (*mapping.get(*index)?)?;
                Some(Predicate::Leaf {
                    column: column.clone(),
                    index: new_index,
                    data_type: data_type.clone(),
                    op: *op,
                    literals: literals.clone(),
                })
            }
            Predicate::And(children) => {
                let projected: Vec<_> = children
                    .iter()
                    .filter_map(|c| c.project_field_index_inclusive(mapping))
                    .collect();
                if projected.is_empty() {
                    None
                } else {
                    Some(Predicate::and(projected))
                }
            }
            Predicate::Or(children) => {
                let projected: Option<Vec<_>> = children
                    .iter()
                    .map(|c| c.project_field_index_inclusive(mapping))
                    .collect();
                Some(Predicate::or(projected?))
            }
            Predicate::Not(inner) => {
                let projected = inner.remap_field_index(mapping)?;
                Some(Predicate::negate(projected))
            }
            Predicate::AlwaysTrue => Some(Predicate::AlwaysTrue),
            Predicate::AlwaysFalse => Some(Predicate::AlwaysFalse),
        }
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Leaf {
                column,
                op,
                literals,
                ..
            } => {
                write!(f, "{column} {op}")?;
                match op {
                    PredicateOperator::IsNull | PredicateOperator::IsNotNull => {}
                    PredicateOperator::In | PredicateOperator::NotIn => {
                        write!(f, " (")?;
                        for (i, lit) in literals.iter().enumerate() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{lit}")?;
                        }
                        write!(f, ")")?;
                    }
                    _ => {
                        if let Some(lit) = literals.first() {
                            write!(f, " {lit}")?;
                        }
                    }
                }
                Ok(())
            }
            Self::And(children) => {
                write!(f, "(")?;
                for (i, child) in children.iter().enumerate() {
                    if i > 0 {
                        write!(f, " AND ")?;
                    }
                    write!(f, "{child}")?;
                }
                write!(f, ")")
            }
            Self::Or(children) => {
                write!(f, "(")?;
                for (i, child) in children.iter().enumerate() {
                    if i > 0 {
                        write!(f, " OR ")?;
                    }
                    write!(f, "{child}")?;
                }
                write!(f, ")")
            }
            Self::Not(inner) => write!(f, "NOT ({inner})"),
            Self::AlwaysTrue => write!(f, "TRUE"),
            Self::AlwaysFalse => write!(f, "FALSE"),
        }
    }
}

// ---------------------------------------------------------------------------
// REST catalog Predicate/Transform JSON interop
// ---------------------------------------------------------------------------

impl Predicate {
    /// Parse a `Predicate` from the REST catalog wire format (Java
    /// `LeafPredicate`/`CompoundPredicate` JSON). Fields resolve by name against
    /// `fields`; anything unknown errors (fail-closed).
    pub fn from_rest_json(json: &str, fields: &[DataField]) -> Result<Predicate> {
        let value: serde_json::Value = serde_json::from_str(json).map_err(rest_json_err)?;
        let builder = PredicateBuilder::new(fields);
        parse_rest_predicate(&value, &builder, fields)
    }

    /// Collect the table-schema field indices referenced by every leaf of this
    /// predicate.
    pub fn collect_leaf_field_indices(&self, out: &mut std::collections::HashSet<usize>) {
        match self {
            Predicate::Leaf { index, .. } => {
                out.insert(*index);
            }
            Predicate::And(children) | Predicate::Or(children) => {
                children
                    .iter()
                    .for_each(|c| c.collect_leaf_field_indices(out));
            }
            Predicate::Not(inner) => inner.collect_leaf_field_indices(out),
            Predicate::AlwaysTrue | Predicate::AlwaysFalse => {}
        }
    }
}

fn rest_json_err(detail: impl fmt::Display) -> Error {
    Error::DataInvalid {
        message: format!("cannot parse REST catalog predicate JSON: {detail}"),
        source: None,
    }
}

fn parse_rest_predicate(
    value: &serde_json::Value,
    builder: &PredicateBuilder,
    fields: &[DataField],
) -> Result<Predicate> {
    let kind = str_prop(value, "kind")?;
    match kind {
        "COMPOUND" => {
            let raw_children = value
                .get("children")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| rest_json_err("COMPOUND without children"))?;
            // Java rejects empty AND/OR; normalizing an empty AND to AlwaysTrue
            // would turn a malformed auth filter into allow-all.
            if raw_children.is_empty() {
                return Err(rest_json_err("COMPOUND with empty children"));
            }
            let children = raw_children
                .iter()
                .map(|child| parse_rest_predicate(child, builder, fields))
                .collect::<Result<Vec<_>>>()?;
            match str_prop(value, "function")? {
                "AND" => Ok(Predicate::and(children)),
                "OR" => Ok(Predicate::or(children)),
                other => Err(rest_json_err(format!("compound function `{other}`"))),
            }
        }
        "LEAF" => parse_rest_leaf(value, builder, fields),
        other => Err(rest_json_err(format!("predicate kind `{other}`"))),
    }
}

fn parse_rest_leaf(
    value: &serde_json::Value,
    builder: &PredicateBuilder,
    fields: &[DataField],
) -> Result<Predicate> {
    // Java requires a well-formed transform on every leaf (`fromJson` calls
    // `transform.outputType()`), so resolve it before anything else — accepting
    // a malformed leaf as AlwaysTrue would turn it into allow-all.
    let transform = value
        .get("transform")
        .ok_or_else(|| rest_json_err("LEAF without transform"))?;
    let transform_name = str_prop(transform, "name")?;

    // AlwaysTrue/AlwaysFalse: Java builds them with the NULL transform
    // (`PredicateBuilder::alwaysTrue`), so the function decides and no field is
    // resolved. Any other transform with these functions fails closed.
    if let function @ ("TRUE" | "FALSE") = str_prop(value, "function")? {
        if transform_name != "NULL" {
            return Err(rest_json_err(format!(
                "{function} with transform `{transform_name}`"
            )));
        }
        return Ok(if function == "TRUE" {
            Predicate::AlwaysTrue
        } else {
            Predicate::AlwaysFalse
        });
    }

    // Non-FIELD_REF transforms (CAST, CONCAT, ...) cannot be evaluated here.
    if transform_name != "FIELD_REF" {
        return Err(rest_json_err(format!("transform `{transform_name}`")));
    }
    let field = transform
        .get("fieldRef")
        .and_then(|r| r.get("name"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| rest_json_err("FIELD_REF without field name"))?;
    // Resolve by name; the local type is authoritative (Java PredicateRemapper).
    let data_type = fields
        .iter()
        .find(|f| f.name() == field)
        .map(|f| f.data_type())
        .ok_or_else(|| rest_json_err(format!("unknown field `{field}`")))?;

    let function = str_prop(value, "function")?;
    // Convert every literal up front (like Java `deserializeLiterals`) so a
    // malformed extra literal fails the parse. JSON null -> `None` (SQL NULL).
    let literals: Vec<Option<Datum>> = value
        .get("literals")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| rest_json_err(format!("{function} without literals")))?
        .iter()
        .map(|v| match v {
            serde_json::Value::Null => Ok(None),
            v => Ok(Some(json_to_datum(v, data_type)?)),
        })
        .collect::<Result<Vec<_>>>()?;
    // Binary/ternary leaves test false for every row on a null literal
    // (`None` -> AlwaysFalse).
    let one_literal = || -> Result<Option<Datum>> {
        literals
            .first()
            .cloned()
            .ok_or_else(|| rest_json_err(format!("{function} without literal")))
    };
    let two_literals = || -> Result<Option<(Datum, Datum)>> {
        match (literals.first(), literals.get(1)) {
            (Some(low), Some(high)) => Ok(low.clone().zip(high.clone())),
            _ => Err(rest_json_err(format!("{function} needs two literals"))),
        }
    };
    let binary =
        |lit: Option<Datum>, build: &dyn Fn(Datum) -> Result<Predicate>| -> Result<Predicate> {
            match lit {
                Some(lit) => build(lit),
                None => Ok(Predicate::AlwaysFalse),
            }
        };

    match function {
        "IS_NULL" => builder.is_null(field),
        "IS_NOT_NULL" => builder.is_not_null(field),
        "EQUAL" => binary(one_literal()?, &|l| builder.equal(field, l)),
        "NOT_EQUAL" => binary(one_literal()?, &|l| builder.not_equal(field, l)),
        "GREATER_THAN" => binary(one_literal()?, &|l| builder.greater_than(field, l)),
        "GREATER_OR_EQUAL" => binary(one_literal()?, &|l| builder.greater_or_equal(field, l)),
        "LESS_THAN" => binary(one_literal()?, &|l| builder.less_than(field, l)),
        "LESS_OR_EQUAL" => binary(one_literal()?, &|l| builder.less_or_equal(field, l)),
        "IN" => {
            // Java In: null literals never match; an empty/all-null list matches nothing.
            let datums: Vec<Datum> = literals.iter().flatten().cloned().collect();
            if datums.is_empty() {
                Ok(Predicate::AlwaysFalse)
            } else {
                builder.is_in(field, datums)
            }
        }
        "NOT_IN" => {
            // Java NotIn: any null literal fails every row; an empty list keeps
            // exactly the non-null rows.
            if literals.iter().any(Option::is_none) {
                Ok(Predicate::AlwaysFalse)
            } else if literals.is_empty() {
                builder.is_not_null(field)
            } else {
                builder.is_not_in(field, literals.iter().flatten().cloned().collect())
            }
        }
        "STARTS_WITH" => binary(one_literal()?, &|l| builder.starts_with(field, l)),
        "ENDS_WITH" => binary(one_literal()?, &|l| builder.ends_with(field, l)),
        "CONTAINS" => binary(one_literal()?, &|l| builder.contains(field, l)),
        "LIKE" => match one_literal()? {
            Some(l) => {
                if let Datum::String(pattern) = &l {
                    validate_rest_like_escapes(pattern)?;
                }
                builder.like(field, l, None)
            }
            None => Ok(Predicate::AlwaysFalse),
        },
        "BETWEEN" => match two_literals()? {
            Some((low, high)) => builder.between(field, low, high),
            None => Ok(Predicate::AlwaysFalse),
        },
        "NOT_BETWEEN" => match two_literals()? {
            Some((low, high)) => builder.not_between(field, low, high),
            None => Ok(Predicate::AlwaysFalse),
        },
        other => Err(rest_json_err(format!("leaf function `{other}`"))),
    }
}

/// Reject the LIKE escapes Java `Like.sqlToRegexLike` rejects (`\` must precede
/// `_`, `%`, or `\` and can't be trailing).
fn validate_rest_like_escapes(pattern: &str) -> Result<()> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '\\' {
            match chars.get(i + 1) {
                Some('_') | Some('%') | Some('\\') => i += 2,
                _ => {
                    return Err(rest_json_err(format!(
                        "invalid LIKE escape sequence in `{pattern}`"
                    )))
                }
            }
        } else {
            i += 1;
        }
    }
    Ok(())
}

fn str_prop<'a>(value: &'a serde_json::Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| rest_json_err(format!("missing `{key}`")))
}

fn json_to_datum(value: &serde_json::Value, data_type: &DataType) -> Result<Datum> {
    let type_err = || rest_json_err(format!("literal {value} for type {data_type:?}"));
    match data_type {
        DataType::Boolean(_) => value.as_bool().map(Datum::Bool).ok_or_else(type_err),
        DataType::TinyInt(_) => int_datum(value, |v| i8::try_from(v).ok().map(Datum::TinyInt)),
        DataType::SmallInt(_) => int_datum(value, |v| i16::try_from(v).ok().map(Datum::SmallInt)),
        DataType::Int(_) => int_datum(value, |v| i32::try_from(v).ok().map(Datum::Int)),
        DataType::BigInt(_) => int_datum(value, |v| Some(Datum::Long(v))),
        // Integral FLOAT forms narrow directly (Java `Long.floatValue()`, one
        // rounding); going through f64 would round twice.
        DataType::Float(_) => {
            let narrowed = if let Some(i) = value.as_i64() {
                Some(i as f32)
            } else if let Some(u) = value.as_u64() {
                Some(u as f32)
            } else {
                value.as_f64().map(|v| v as f32)
            };
            narrowed.map(Datum::Float).ok_or_else(type_err)
        }
        DataType::Double(_) => value.as_f64().map(Datum::Double).ok_or_else(type_err),
        DataType::Char(_) | DataType::VarChar(_) => value
            .as_str()
            .map(|s| Datum::String(s.to_string()))
            .ok_or_else(type_err),
        // Temporal/decimal literals fail closed: Java's own JSON deserializer
        // cannot round-trip them.
        other => Err(rest_json_err(format!(
            "literal conversion for type {other:?} is not supported"
        ))),
    }
}

fn int_datum(value: &serde_json::Value, build: impl Fn(i64) -> Option<Datum>) -> Result<Datum> {
    value
        .as_i64()
        .and_then(build)
        .ok_or_else(|| rest_json_err(format!("integer literal {value}")))
}

/// Java `Transform` counterpart (see `org.apache.paimon.predicate.Transform`):
/// the value-producing side of Java predicates and column masking.
#[derive(Debug, Clone, PartialEq)]
pub enum Transform {
    /// `NULL`: always null.
    Null,
    /// `FIELD_REF`: another field's value.
    FieldRef(usize),
    /// `CAST`: a field cast to a type.
    Cast(usize, DataType),
    /// String transforms over mixed literal / field inputs.
    Upper(Vec<TransformInput>),
    /// Lowercase of the inputs.
    Lower(Vec<TransformInput>),
    /// Concatenation of the inputs.
    Concat(Vec<TransformInput>),
    /// Concatenation where the first input is the separator.
    ConcatWs(Vec<TransformInput>),
}

/// One input of a string [`Transform`].
#[derive(Debug, Clone, PartialEq)]
pub enum TransformInput {
    /// A string literal (`None` = a JSON `null` input, i.e. SQL NULL).
    Literal(Option<String>),
    /// A field reference.
    Field(usize),
}

impl Transform {
    /// Parse a `Transform` from the REST catalog wire format (Java `Transform`
    /// JSON). Fields resolve by name against `fields`; anything unknown errors
    /// (fail-closed).
    pub fn from_rest_json(json: &str, fields: &[DataField]) -> Result<Transform> {
        let value: serde_json::Value = serde_json::from_str(json).map_err(rest_json_err)?;
        parse_rest_transform(&value, fields)
    }

    /// Collect the table-schema field indices this transform reads.
    pub fn collect_field_indices(&self, out: &mut std::collections::HashSet<usize>) {
        match self {
            Transform::Null => {}
            Transform::FieldRef(index) | Transform::Cast(index, _) => {
                out.insert(*index);
            }
            Transform::Upper(inputs)
            | Transform::Lower(inputs)
            | Transform::Concat(inputs)
            | Transform::ConcatWs(inputs) => {
                for input in inputs {
                    if let TransformInput::Field(index) = input {
                        out.insert(*index);
                    }
                }
            }
        }
    }
}

fn parse_rest_transform(value: &serde_json::Value, fields: &[DataField]) -> Result<Transform> {
    let field_index = |name: &str| -> Result<usize> {
        fields
            .iter()
            .position(|f| f.name() == name)
            .ok_or_else(|| rest_json_err(format!("unknown field `{name}`")))
    };
    let field_ref_index = |v: &serde_json::Value| -> Result<usize> {
        v.get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| rest_json_err("FIELD_REF without field name"))
            .and_then(field_index)
    };
    // Mirrors Java StringTransform: inputs are strings, nulls, or string-typed
    // field references; arity checks match the Java constructors.
    let string_inputs = |min: usize, max: usize| -> Result<Vec<TransformInput>> {
        let inputs = value
            .get("inputs")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| rest_json_err("string transform without inputs"))?
            .iter()
            .map(|input| match input {
                serde_json::Value::String(s) => Ok(TransformInput::Literal(Some(s.clone()))),
                serde_json::Value::Null => Ok(TransformInput::Literal(None)),
                serde_json::Value::Object(_) => {
                    let index = field_ref_index(input)?;
                    match fields[index].data_type() {
                        DataType::Char(_) | DataType::VarChar(_) => {
                            Ok(TransformInput::Field(index))
                        }
                        other => Err(rest_json_err(format!(
                            "string transform input `{}` has non-string type {other:?}",
                            fields[index].name()
                        ))),
                    }
                }
                other => Err(rest_json_err(format!("transform input {other}"))),
            })
            .collect::<Result<Vec<_>>>()?;
        if inputs.len() < min || inputs.len() > max {
            return Err(rest_json_err(format!(
                "string transform expects {min}..={max} inputs, got {}",
                inputs.len()
            )));
        }
        Ok(inputs)
    };
    match str_prop(value, "name")? {
        "NULL" => Ok(Transform::Null),
        "FIELD_REF" => Ok(Transform::FieldRef(field_ref_index(
            value
                .get("fieldRef")
                .ok_or_else(|| rest_json_err("FIELD_REF without fieldRef"))?,
        )?)),
        "CAST" => {
            let index = field_ref_index(
                value
                    .get("fieldRef")
                    .ok_or_else(|| rest_json_err("CAST without fieldRef"))?,
            )?;
            let to: DataType = value
                .get("type")
                .and_then(|t| serde_json::from_value(t.clone()).ok())
                .ok_or_else(|| rest_json_err("CAST without a parseable type"))?;
            // Whether the cast is applicable is checked where the mask is
            // applied (arrow cast + output-type equality), not here.
            Ok(Transform::Cast(index, to))
        }
        "UPPER" => Ok(Transform::Upper(string_inputs(1, 1)?)),
        "LOWER" => Ok(Transform::Lower(string_inputs(1, 1)?)),
        "CONCAT" => Ok(Transform::Concat(string_inputs(0, usize::MAX)?)),
        "CONCAT_WS" => Ok(Transform::ConcatWs(string_inputs(2, usize::MAX)?)),
        other => Err(rest_json_err(format!("transform `{other}`"))),
    }
}

// ---------------------------------------------------------------------------
// PredicateBuilder
// ---------------------------------------------------------------------------

/// Builds `Predicate` nodes from field names and typed literals.
///
/// Stores schema field metadata and validates column references in builder
/// methods. Unknown column names cause immediate errors (fail-fast).
///
/// Reference: Java `PredicateBuilder` — but uses field names instead of indices
/// for a more ergonomic Rust API.
pub struct PredicateBuilder {
    field_names: Vec<String>,
    field_types: Vec<DataType>,
    case_sensitive: bool,
}

impl PredicateBuilder {
    /// Create a new builder from schema fields, matching column names
    /// case-sensitively. Infallible.
    pub fn new(fields: &[DataField]) -> Self {
        Self::new_with_case_sensitive(fields, true)
    }

    /// Create a builder with explicit case sensitivity. When `case_sensitive`
    /// is `false`, column names are resolved by ASCII case-folding and an
    /// ambiguous (case-colliding) schema errors. Infallible.
    pub fn new_with_case_sensitive(fields: &[DataField], case_sensitive: bool) -> Self {
        Self {
            field_names: fields.iter().map(|f| f.name().to_string()).collect(),
            field_types: fields.iter().map(|f| f.data_type().clone()).collect(),
            case_sensitive,
        }
    }

    // -- comparison operators --

    pub fn equal(&self, field: &str, literal: Datum) -> Result<Predicate> {
        self.leaf(field, PredicateOperator::Eq, vec![literal])
    }

    pub fn not_equal(&self, field: &str, literal: Datum) -> Result<Predicate> {
        self.leaf(field, PredicateOperator::NotEq, vec![literal])
    }

    pub fn less_than(&self, field: &str, literal: Datum) -> Result<Predicate> {
        self.leaf(field, PredicateOperator::Lt, vec![literal])
    }

    pub fn less_or_equal(&self, field: &str, literal: Datum) -> Result<Predicate> {
        self.leaf(field, PredicateOperator::LtEq, vec![literal])
    }

    pub fn greater_than(&self, field: &str, literal: Datum) -> Result<Predicate> {
        self.leaf(field, PredicateOperator::Gt, vec![literal])
    }

    pub fn greater_or_equal(&self, field: &str, literal: Datum) -> Result<Predicate> {
        self.leaf(field, PredicateOperator::GtEq, vec![literal])
    }

    // -- null operators --

    pub fn is_null(&self, field: &str) -> Result<Predicate> {
        self.leaf(field, PredicateOperator::IsNull, vec![])
    }

    pub fn is_not_null(&self, field: &str) -> Result<Predicate> {
        self.leaf(field, PredicateOperator::IsNotNull, vec![])
    }

    /// Build a partition predicate: AND of equal/is_null for each (field_name, datum) pair.
    pub fn partition_predicate(&self, fields: &[(&str, Option<Datum>)]) -> Result<Predicate> {
        let predicates: Vec<Predicate> = fields
            .iter()
            .map(|(name, value)| match value {
                Some(v) => self.equal(name, v.clone()),
                None => self.is_null(name),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Predicate::and(predicates))
    }

    // -- set operators --

    pub fn is_in(&self, field: &str, literals: Vec<Datum>) -> Result<Predicate> {
        if literals.is_empty() {
            return Ok(Predicate::AlwaysFalse);
        }
        self.leaf(field, PredicateOperator::In, literals)
    }

    pub fn is_not_in(&self, field: &str, literals: Vec<Datum>) -> Result<Predicate> {
        if literals.is_empty() {
            return Ok(Predicate::AlwaysTrue);
        }
        self.leaf(field, PredicateOperator::NotIn, literals)
    }

    // -- string operators --

    /// `field LIKE 'pat%'` shape. Empty pattern → `IsNotNull(field)` (every
    /// non-null string starts with the empty string). Non-string `pattern`
    /// → [`Error::ConfigInvalid`].
    pub fn starts_with(&self, field: &str, pattern: Datum) -> Result<Predicate> {
        self.string_leaf(field, PredicateOperator::StartsWith, pattern)
    }

    /// `field LIKE '%pat'` shape. Empty pattern → `IsNotNull(field)`.
    pub fn ends_with(&self, field: &str, pattern: Datum) -> Result<Predicate> {
        self.string_leaf(field, PredicateOperator::EndsWith, pattern)
    }

    /// `field LIKE '%pat%'` shape. Empty pattern → `IsNotNull(field)`.
    pub fn contains(&self, field: &str, pattern: Datum) -> Result<Predicate> {
        self.string_leaf(field, PredicateOperator::Contains, pattern)
    }

    /// `field LIKE '<pattern>'` with optional `escape` character (default `\`).
    /// Mirrors Java `LikeOptimization`: rewrites `prefix%` / `%suffix` /
    /// `%mid%` / no-wildcard patterns into [`PredicateOperator::StartsWith`] /
    /// [`PredicateOperator::EndsWith`] / [`PredicateOperator::Contains`] /
    /// [`PredicateOperator::Eq`]; falls back to a [`PredicateOperator::Like`]
    /// leaf for anything more complex (`_`, multi-segment `%`, escaped
    /// wildcards). The `Like` evaluator follows arrow_string `like` kernel
    /// semantics for the residual cases.
    ///
    /// `escape == None` defaults to `\`. Any other ESCAPE character is
    /// rejected with [`Error::ConfigInvalid`] (the DataFusion translator turns
    /// that into a fall-open). Empty pattern → [`PredicateOperator::Eq`] of the
    /// empty string (SQL semantics: only the empty string matches).
    pub fn like(&self, field: &str, pattern: Datum, escape: Option<char>) -> Result<Predicate> {
        let pattern_str = match &pattern {
            Datum::String(s) => s.clone(),
            other => {
                return Err(Error::ConfigInvalid {
                    message: format!("LIKE requires a string pattern, got {other}"),
                });
            }
        };
        let escape_char = escape.unwrap_or('\\');
        if escape_char != '\\' {
            return Err(Error::ConfigInvalid {
                message: format!(
                    "LIKE escape character {escape_char:?} is not supported (only '\\\\')"
                ),
            });
        }

        match optimize_like_pattern(&pattern_str) {
            LikeShape::EmptyOrLiteral(s) => self.equal(field, Datum::String(s)),
            LikeShape::StartsWith(prefix) => self.starts_with(field, Datum::String(prefix)),
            LikeShape::EndsWith(suffix) => self.ends_with(field, Datum::String(suffix)),
            LikeShape::Contains(mid) => self.contains(field, Datum::String(mid)),
            LikeShape::Residual => self.leaf(field, PredicateOperator::Like, vec![pattern]),
        }
    }

    // -- range operators --

    /// `field BETWEEN low AND high`. SQL semantics: inclusive on both ends.
    /// `low > high` short-circuits to [`Predicate::AlwaysFalse`] (no value can
    /// be between an empty range).
    pub fn between(&self, field: &str, low: Datum, high: Datum) -> Result<Predicate> {
        if Self::low_strictly_above_high(&low, &high) {
            return Ok(Predicate::AlwaysFalse);
        }
        self.leaf(field, PredicateOperator::Between, vec![low, high])
    }

    /// `field NOT BETWEEN low AND high`. SQL three-valued logic: NULL value
    /// → NULL → treated as false, matching the existing `NotEq` evaluator.
    /// `low > high` short-circuits to `IsNotNull(field)` (every non-null value
    /// is "not between" an empty range).
    pub fn not_between(&self, field: &str, low: Datum, high: Datum) -> Result<Predicate> {
        if Self::low_strictly_above_high(&low, &high) {
            return self.is_not_null(field);
        }
        self.leaf(field, PredicateOperator::NotBetween, vec![low, high])
    }

    fn low_strictly_above_high(low: &Datum, high: &Datum) -> bool {
        matches!(datum_cmp(low, high), Some(Ordering::Greater))
    }

    /// Shared body for the three string operators: empty-string short-circuit
    /// and literal-type guard ([`leaf`] still cross-checks against the column
    /// type, so non-string columns are rejected there).
    fn string_leaf(&self, field: &str, op: PredicateOperator, pattern: Datum) -> Result<Predicate> {
        match &pattern {
            // Every non-null string starts with / ends with / contains the
            // empty string, and a NULL value matches none of them — i.e. the
            // empty pattern is exactly `IsNotNull`. Folding to `AlwaysTrue`
            // would wrongly retain NULL rows (and drop the field reference,
            // keeping the predicate out of the data-pruning path).
            Datum::String(s) if s.is_empty() => return self.is_not_null(field),
            Datum::String(_) => {}
            other => {
                return Err(Error::ConfigInvalid {
                    message: format!("{op} requires a string pattern, got {other}"),
                });
            }
        }
        self.leaf(field, op, vec![pattern])
    }

    // -- internal --

    /// Resolve field name to index + type, validate literals, and build a leaf predicate.
    fn leaf(&self, field: &str, op: PredicateOperator, literals: Vec<Datum>) -> Result<Predicate> {
        let (index, canonical, data_type) = self.resolve_field(field)?;
        Self::validate_literal_count(op, &literals)?;
        for lit in &literals {
            validate_datum_matches_type(lit, &data_type)?;
        }
        Ok(Predicate::Leaf {
            column: canonical,
            index,
            data_type,
            op,
            literals,
        })
    }

    /// Look up a field name, returning its (index, DataType) or an error.
    /// Resolve a column name to `(index, canonical_name, data_type)`.
    ///
    /// The canonical name is the schema's stored field name for the matched
    /// index. It is stored in the resulting [`Predicate::Leaf`] so downstream
    /// name-based lookups (e.g. global-index pruning) match the schema even
    /// when the caller passed a different casing. Case-insensitive matching
    /// errors on an ambiguous (case-colliding) schema.
    fn resolve_field(&self, field: &str) -> Result<(usize, String, DataType)> {
        let index = if self.case_sensitive {
            self.field_names.iter().position(|n| n == field)
        } else {
            let mut hits = self
                .field_names
                .iter()
                .enumerate()
                .filter(|(_, n)| n.eq_ignore_ascii_case(field));
            let first = hits.next();
            if first.is_some() && hits.next().is_some() {
                return Err(Error::ConfigInvalid {
                    message: format!(
                        "Ambiguous column '{field}' matches multiple schema fields case-insensitively: {:?}",
                        self.field_names
                    ),
                });
            }
            first.map(|(i, _)| i)
        };
        index
            .map(|idx| {
                (
                    idx,
                    self.field_names[idx].clone(),
                    self.field_types[idx].clone(),
                )
            })
            .ok_or_else(|| Error::ConfigInvalid {
                message: format!(
                    "Column '{}' not found in schema fields {:?}",
                    field, self.field_names
                ),
            })
    }

    /// Validate that the number of literals matches the operator's expectation.
    fn validate_literal_count(op: PredicateOperator, literals: &[Datum]) -> Result<()> {
        let (expected, actual) = match op {
            PredicateOperator::IsNull | PredicateOperator::IsNotNull => {
                if literals.is_empty() {
                    return Ok(());
                }
                return Err(Error::ConfigInvalid {
                    message: format!("{op} expects 0 literals, got {}", literals.len()),
                });
            }
            PredicateOperator::In | PredicateOperator::NotIn => {
                if !literals.is_empty() {
                    return Ok(());
                }
                // Empty IN is handled at is_in()/is_not_in() level; this guards
                // against direct leaf() misuse.
                return Err(Error::ConfigInvalid {
                    message: format!("{op} expects at least 1 literal, got 0"),
                });
            }
            PredicateOperator::Between | PredicateOperator::NotBetween => (2, literals.len()),
            _ => (1, literals.len()),
        };
        if actual != expected {
            return Err(Error::ConfigInvalid {
                message: format!("{op} expects {expected} literal, got {actual}"),
            });
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Datum-DataType validation
// ---------------------------------------------------------------------------

/// Validate that a `Datum` variant is compatible with a `DataType`.
///
/// This is a fail-fast check at predicate construction time, preventing
/// type mismatches from propagating to evaluators.
fn validate_datum_matches_type(datum: &Datum, data_type: &DataType) -> Result<()> {
    let ok = matches!(
        (datum, data_type),
        (Datum::Bool(_), DataType::Boolean(_))
            | (Datum::TinyInt(_), DataType::TinyInt(_))
            | (Datum::SmallInt(_), DataType::SmallInt(_))
            | (Datum::Int(_), DataType::Int(_))
            | (Datum::Long(_), DataType::BigInt(_))
            | (Datum::Float(_), DataType::Float(_))
            | (Datum::Double(_), DataType::Double(_))
            | (Datum::String(_), DataType::Char(_))
            | (Datum::String(_), DataType::VarChar(_))
            | (Datum::Date(_), DataType::Date(_))
            | (Datum::Time(_), DataType::Time(_))
            | (Datum::Timestamp { .. }, DataType::Timestamp(_))
            | (
                Datum::LocalZonedTimestamp { .. },
                DataType::LocalZonedTimestamp(_)
            )
            | (Datum::Decimal { .. }, DataType::Decimal(_))
            | (Datum::Bytes(_), DataType::Binary(_))
            | (Datum::Bytes(_), DataType::VarBinary(_))
            | (Datum::Variant { .. }, DataType::Variant(_))
    );
    if !ok {
        return Err(Error::ConfigInvalid {
            message: format!("Datum {datum} is incompatible with DataType {data_type:?}"),
        });
    }
    Ok(())
}

/// Build a `_ROW_ID` leaf the way callers have to: the column is in no schema, so
/// [`PredicateBuilder`] cannot resolve it and the index is a placeholder.
#[cfg(test)]
pub(crate) fn row_id_leaf(op: PredicateOperator, literals: Vec<Datum>) -> Predicate {
    Predicate::Leaf {
        column: crate::spec::ROW_ID_FIELD_NAME.to_string(),
        index: 0,
        data_type: DataType::BigInt(crate::spec::BigIntType::new()),
        op,
        literals,
    }
}

// ---------------------------------------------------------------------------
// field_idx_to_partition_idx
// ---------------------------------------------------------------------------

/// Map table schema field indices to partition row indices.
///
/// For each field in `schema_fields`, returns `Some(partition_index)` if the
/// field is a partition key, or `None` otherwise. The partition index is the
/// position of the field name in `partition_keys`.
///
/// # Example
///
/// ```text
/// schema_fields: [id, name, dt, hr]
/// partition_keys: [dt, hr]
/// result:         [None, None, Some(0), Some(1)]
/// ```
///
/// Reference: Java `PredicateBuilder.fieldIdxToPartitionIdx`.
pub fn field_idx_to_partition_idx(
    schema_fields: &[DataField],
    partition_keys: &[String],
) -> Vec<Option<usize>> {
    schema_fields
        .iter()
        .map(|f| partition_keys.iter().position(|k| k == f.name()))
        .collect()
}

// ---------------------------------------------------------------------------
// extract_datum
// ---------------------------------------------------------------------------

/// Extract a typed `Datum` from a `BinaryRow` field based on `DataType`.
///
/// Returns `Ok(None)` if the field is null, `Ok(Some(datum))` on success,
/// or `Err` if the binary data is malformed.
pub(crate) fn extract_datum(
    row: &BinaryRow,
    pos: usize,
    data_type: &DataType,
) -> Result<Option<Datum>> {
    if row.is_null_at(pos) {
        return Ok(None);
    }
    let datum = match data_type {
        DataType::Boolean(_) => Datum::Bool(row.get_boolean(pos)?),
        DataType::TinyInt(_) => Datum::TinyInt(row.get_byte(pos)?),
        DataType::SmallInt(_) => Datum::SmallInt(row.get_short(pos)?),
        DataType::Int(_) => Datum::Int(row.get_int(pos)?),
        DataType::BigInt(_) => Datum::Long(row.get_long(pos)?),
        DataType::Float(_) => Datum::Float(row.get_float(pos)?),
        DataType::Double(_) => Datum::Double(row.get_double(pos)?),
        DataType::Char(_) | DataType::VarChar(_) => Datum::String(row.get_string(pos)?.to_string()),
        DataType::Date(_) => Datum::Date(row.get_int(pos)?),
        DataType::Time(_) => Datum::Time(row.get_int(pos)?),
        DataType::Timestamp(ts) => {
            let (millis, nanos) = row.get_timestamp_raw(pos, ts.precision())?;
            Datum::Timestamp { millis, nanos }
        }
        DataType::LocalZonedTimestamp(ts) => {
            let (millis, nanos) = row.get_timestamp_raw(pos, ts.precision())?;
            Datum::LocalZonedTimestamp { millis, nanos }
        }
        DataType::Decimal(dec) => {
            let precision = dec.precision();
            let scale = dec.scale();
            let unscaled = row.get_decimal_unscaled(pos, precision)?;
            Datum::Decimal {
                unscaled,
                precision,
                scale,
            }
        }
        DataType::Binary(_) | DataType::VarBinary(_) => Datum::Bytes(row.get_binary(pos)?.to_vec()),
        other => {
            return Err(Error::Unsupported {
                message: format!("extract_datum: unsupported DataType {other:?}"),
            });
        }
    };
    Ok(Some(datum))
}

// ---------------------------------------------------------------------------
// eval_row
// ---------------------------------------------------------------------------

/// Evaluate a predicate tree against a `BinaryRow`.
///
/// Each `Leaf` carries its own `data_type` (preserved through `remap_field_index`),
/// so no external type list is needed.
///
/// SQL null semantics: null compared to any value yields `false`.
pub(crate) fn eval_row(predicate: &Predicate, row: &BinaryRow) -> Result<bool> {
    match predicate {
        Predicate::AlwaysTrue => Ok(true),
        Predicate::AlwaysFalse => Ok(false),
        Predicate::And(children) => {
            for child in children {
                if !eval_row(child, row)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Predicate::Or(children) => {
            for child in children {
                if eval_row(child, row)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Predicate::Not(inner) => Ok(!eval_row(inner, row)?),
        Predicate::Leaf {
            index,
            data_type,
            op,
            literals,
            ..
        } => {
            let datum = extract_datum(row, *index, data_type)?;
            Ok(eval_leaf(*op, datum.as_ref(), literals))
        }
    }
}

/// Evaluate a single leaf predicate.
///
/// This function is infallible: all type decoding happens in `extract_datum`
/// before this point, and the operator match is exhaustive.
fn eval_leaf(op: PredicateOperator, datum: Option<&Datum>, literals: &[Datum]) -> bool {
    match op {
        PredicateOperator::IsNull => datum.is_none(),
        PredicateOperator::IsNotNull => datum.is_some(),
        _ => {
            // SQL null semantics: NULL op value → false
            let val = match datum {
                Some(v) => v,
                None => return false,
            };
            match op {
                PredicateOperator::Eq => literals.first().is_some_and(|lit| datum_eq(val, lit)),
                PredicateOperator::NotEq => literals.first().is_some_and(|lit| !datum_eq(val, lit)),
                PredicateOperator::Lt => {
                    literals.first().and_then(|lit| datum_cmp(val, lit)) == Some(Ordering::Less)
                }
                PredicateOperator::LtEq => matches!(
                    literals.first().and_then(|lit| datum_cmp(val, lit)),
                    Some(Ordering::Less | Ordering::Equal)
                ),
                PredicateOperator::Gt => {
                    literals.first().and_then(|lit| datum_cmp(val, lit)) == Some(Ordering::Greater)
                }
                PredicateOperator::GtEq => matches!(
                    literals.first().and_then(|lit| datum_cmp(val, lit)),
                    Some(Ordering::Greater | Ordering::Equal)
                ),
                PredicateOperator::In => literals.iter().any(|lit| datum_eq(val, lit)),
                PredicateOperator::NotIn => !literals.iter().any(|lit| datum_eq(val, lit)),
                PredicateOperator::StartsWith => match (val, literals.first()) {
                    (Datum::String(haystack), Some(Datum::String(needle))) => {
                        haystack.starts_with(needle.as_str())
                    }
                    _ => unreachable!(
                        "STARTS_WITH must have Datum::String value and literal (validated by builder)"
                    ),
                },
                PredicateOperator::EndsWith => match (val, literals.first()) {
                    (Datum::String(haystack), Some(Datum::String(needle))) => {
                        haystack.ends_with(needle.as_str())
                    }
                    _ => unreachable!(
                        "ENDS_WITH must have Datum::String value and literal (validated by builder)"
                    ),
                },
                PredicateOperator::Contains => match (val, literals.first()) {
                    (Datum::String(haystack), Some(Datum::String(needle))) => {
                        haystack.contains(needle.as_str())
                    }
                    _ => unreachable!(
                        "CONTAINS must have Datum::String value and literal (validated by builder)"
                    ),
                },
                PredicateOperator::Like => match (val, literals.first()) {
                    (Datum::String(haystack), Some(Datum::String(pattern))) => {
                        like_match(haystack, pattern)
                    }
                    _ => unreachable!(
                        "LIKE must have Datum::String value and literal (validated by builder)"
                    ),
                },
                PredicateOperator::Between => eval_between(val, literals),
                PredicateOperator::NotBetween => !eval_between(val, literals),
                // IsNull/IsNotNull are handled in the outer match above.
                PredicateOperator::IsNull | PredicateOperator::IsNotNull => unreachable!(),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// LIKE pattern optimization & evaluation
// ---------------------------------------------------------------------------

/// Result of LIKE pattern shape analysis. The `String` payloads are the
/// literal substrings extracted from the pattern (with escape sequences
/// already decoded).
enum LikeShape {
    /// Empty pattern, or a pattern that contains no wildcards / escapes —
    /// equivalent to `Eq <literal>` (where the literal is the unescaped
    /// pattern, possibly empty).
    EmptyOrLiteral(String),
    /// `prefix%` (exactly one trailing `%`, no other wildcards or escapes).
    StartsWith(String),
    /// `%suffix`.
    EndsWith(String),
    /// `%mid%` (exactly one leading and one trailing `%`).
    Contains(String),
    /// Anything else: `_`, multi-segment `%`, or any escape sequence — must
    /// fall through to a `Like` leaf evaluator.
    Residual,
}

/// Classify a SQL LIKE pattern. The escape character is hardcoded to `\` —
/// callers wanting any other escape should bypass optimization and surface a
/// `Like` leaf directly. Any presence of `\` in the pattern forces
/// [`LikeShape::Residual`] (the simple shape rules don't account for escaped
/// wildcards).
fn optimize_like_pattern(pattern: &str) -> LikeShape {
    if pattern.contains('\\') || pattern.contains('_') {
        return LikeShape::Residual;
    }
    let bytes = pattern.as_bytes();
    let percent_count = bytes.iter().filter(|b| **b == b'%').count();
    match percent_count {
        0 => LikeShape::EmptyOrLiteral(pattern.to_string()),
        1 => {
            if let Some(prefix) = pattern.strip_suffix('%') {
                LikeShape::StartsWith(prefix.to_string())
            } else if let Some(suffix) = pattern.strip_prefix('%') {
                LikeShape::EndsWith(suffix.to_string())
            } else {
                LikeShape::Residual
            }
        }
        2 if pattern.starts_with('%') && pattern.ends_with('%') => {
            // `%%` reduces to `Contains('')`, which itself short-circuits to
            // `IsNotNull` at the StartsWith/EndsWith/Contains builder boundary
            // — so no special-casing here.
            LikeShape::Contains(pattern[1..pattern.len() - 1].to_string())
        }
        _ => LikeShape::Residual,
    }
}

/// Evaluate a SQL LIKE pattern against a value. Implements the backtracking
/// matcher used by `arrow_string::like::like`:
/// * `%` matches any (possibly empty) substring,
/// * `_` matches exactly one character,
/// * `\X` matches the literal `X` for any character `X` (the backslash is
///   consumed); a trailing `\` matches a literal backslash.
pub(crate) fn like_match(value: &str, pattern: &str) -> bool {
    let value: Vec<char> = value.chars().collect();
    let pattern: Vec<char> = pattern.chars().collect();
    like_match_chars(&value, 0, &pattern, 0)
}

fn like_match_chars(value: &[char], mut vi: usize, pattern: &[char], mut pi: usize) -> bool {
    while pi < pattern.len() {
        match pattern[pi] {
            '%' => {
                // Collapse runs of `%` and try every possible suffix.
                while pi < pattern.len() && pattern[pi] == '%' {
                    pi += 1;
                }
                if pi == pattern.len() {
                    return true;
                }
                while vi <= value.len() {
                    if like_match_chars(value, vi, pattern, pi) {
                        return true;
                    }
                    if vi == value.len() {
                        return false;
                    }
                    vi += 1;
                }
                return false;
            }
            '_' => {
                if vi == value.len() {
                    return false;
                }
                vi += 1;
                pi += 1;
            }
            '\\' => {
                // Mirror arrow's `like` kernel: a backslash consumes the next
                // character and matches it literally, whatever it is (`%`, `_`,
                // `\`, or any other char such as `a`). A trailing backslash
                // matches a literal backslash.
                let expected = match pattern.get(pi + 1) {
                    Some(&next) => {
                        pi += 2;
                        next
                    }
                    None => {
                        pi += 1;
                        '\\'
                    }
                };
                if vi == value.len() || value[vi] != expected {
                    return false;
                }
                vi += 1;
            }
            other => {
                if vi == value.len() || value[vi] != other {
                    return false;
                }
                vi += 1;
                pi += 1;
            }
        }
    }
    vi == value.len()
}

// ---------------------------------------------------------------------------
// BETWEEN evaluation
// ---------------------------------------------------------------------------

/// Evaluate `value BETWEEN low AND high` (inclusive). `NotBetween` is the
/// boolean complement at the call site, which is correct for non-null
/// `value` — null handling already short-circuits in the outer `eval_leaf`.
/// Returns `false` when either comparison is incomparable (defensive: the
/// builder validates types up front, so this is unreachable in practice).
fn eval_between(value: &Datum, literals: &[Datum]) -> bool {
    let (Some(low), Some(high)) = (literals.first(), literals.get(1)) else {
        unreachable!("BETWEEN must have 2 literals (validated by builder)");
    };
    let above_low = matches!(
        datum_cmp(value, low),
        Some(Ordering::Greater | Ordering::Equal)
    );
    let below_high = matches!(
        datum_cmp(value, high),
        Some(Ordering::Less | Ordering::Equal)
    );
    above_low && below_high
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::types::*;

    fn test_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::default()),
            ),
            DataField::new(2, "dt".to_string(), DataType::Date(DateType::new())),
            DataField::new(3, "hr".to_string(), DataType::Int(IntType::new())),
        ]
    }

    #[test]
    fn test_system_column_never_maps_onto_a_field_space() {
        let fields = test_fields();
        let partition_first = [
            fields[2].clone(),
            fields[0].clone(),
            fields[1].clone(),
            fields[3].clone(),
        ];
        let mapping =
            field_idx_to_partition_idx(&partition_first, &["dt".to_string(), "hr".to_string()]);
        assert_eq!(mapping[0], Some(0), "field 0 is the dt partition key");

        let leaf = row_id_leaf(PredicateOperator::GtEq, vec![Datum::Long(10)]);
        assert_eq!(leaf.project_field_index_inclusive(&mapping), None);
        assert_eq!(leaf.remap_field_index(&mapping), None);
        assert!(!leaf.references_only_mapped_fields(&mapping));
    }

    #[test]
    fn test_system_column_does_not_drag_a_conjunction_onto_a_field_space() {
        let fields = test_fields();
        let mapping = field_idx_to_partition_idx(&fields, &["hr".to_string()]);
        let dt = PredicateBuilder::new(&fields)
            .greater_or_equal("hr", Datum::Int(3))
            .unwrap();

        let conjunction = Predicate::and(vec![
            row_id_leaf(PredicateOperator::GtEq, vec![Datum::Long(10)]),
            dt.clone(),
        ]);
        let projected = conjunction.project_field_index_inclusive(&mapping).unwrap();
        assert!(!matches!(projected, Predicate::And(_)), "only hr survives");
        assert!(!conjunction.references_only_mapped_fields(&mapping));

        let disjunction = Predicate::or(vec![
            row_id_leaf(PredicateOperator::GtEq, vec![Datum::Long(10)]),
            dt,
        ]);
        assert_eq!(disjunction.project_field_index_inclusive(&mapping), None);
    }

    #[test]
    fn test_only_row_id_is_unresolvable_by_index() {
        let fields = vec![
            DataField::new(0, "rowkind".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "hr".to_string(), DataType::Int(IntType::new())),
        ];
        let mapping = field_idx_to_partition_idx(&fields, &["rowkind".to_string()]);
        let leaf = PredicateBuilder::new(&fields)
            .equal("rowkind", Datum::Int(1))
            .unwrap();

        assert!(leaf.project_field_index_inclusive(&mapping).is_some());
        assert!(leaf.remap_field_index(&mapping).is_some());
        assert!(leaf.references_only_mapped_fields(&mapping));
    }

    // ======================== PredicateBuilder basics ========================

    #[test]
    fn test_builder_case_insensitive_matches_wrong_case() {
        let pb = PredicateBuilder::new_with_case_sensitive(&test_fields(), false);
        assert!(pb.equal("ID", Datum::Int(1)).is_ok());
    }

    #[test]
    fn test_builder_case_sensitive_default_rejects_wrong_case() {
        let pb = PredicateBuilder::new(&test_fields());
        assert!(pb.equal("ID", Datum::Int(1)).is_err());
    }

    #[test]
    fn test_builder_case_insensitive_stores_canonical_column_name() {
        // The leaf must carry the canonical schema name, not the caller's
        // casing, so downstream name-based lookups (e.g. index pruning) match.
        let pb = PredicateBuilder::new_with_case_sensitive(&test_fields(), false);
        match pb.equal("ID", Datum::Int(1)).unwrap() {
            Predicate::Leaf { column, .. } => assert_eq!(column, "id"),
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_builder_case_insensitive_ambiguous_errors() {
        let fields = vec![
            DataField::new(0, "Col".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "col".to_string(), DataType::Int(IntType::new())),
        ];
        let pb = PredicateBuilder::new_with_case_sensitive(&fields, false);
        assert!(pb.equal("col", Datum::Int(1)).is_err());
    }

    #[test]
    fn test_builder_equal() {
        let pb = PredicateBuilder::new(&test_fields());
        let pred = pb.equal("id", Datum::Int(42)).unwrap();

        match &pred {
            Predicate::Leaf {
                column,
                index,
                op,
                literals,
                ..
            } => {
                assert_eq!(column, "id");
                assert_eq!(*index, 0);
                assert_eq!(*op, PredicateOperator::Eq);
                assert_eq!(literals, &[Datum::Int(42)]);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_builder_comparison_ops() {
        let pb = PredicateBuilder::new(&test_fields());

        let ops_and_builders: Vec<(PredicateOperator, Result<Predicate>)> = vec![
            (PredicateOperator::NotEq, pb.not_equal("id", Datum::Int(1))),
            (PredicateOperator::Lt, pb.less_than("id", Datum::Int(1))),
            (
                PredicateOperator::LtEq,
                pb.less_or_equal("id", Datum::Int(1)),
            ),
            (PredicateOperator::Gt, pb.greater_than("id", Datum::Int(1))),
            (
                PredicateOperator::GtEq,
                pb.greater_or_equal("id", Datum::Int(1)),
            ),
        ];

        for (expected_op, result) in ops_and_builders {
            let pred = result.unwrap();
            match &pred {
                Predicate::Leaf { op, .. } => assert_eq!(*op, expected_op),
                other => panic!("expected Leaf, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_builder_null_ops() {
        let pb = PredicateBuilder::new(&test_fields());

        let is_null = pb.is_null("name").unwrap();
        match &is_null {
            Predicate::Leaf {
                column,
                op,
                literals,
                ..
            } => {
                assert_eq!(column, "name");
                assert_eq!(*op, PredicateOperator::IsNull);
                assert!(literals.is_empty());
            }
            other => panic!("expected Leaf, got {other:?}"),
        }

        let is_not_null = pb.is_not_null("name").unwrap();
        match &is_not_null {
            Predicate::Leaf { op, .. } => assert_eq!(*op, PredicateOperator::IsNotNull),
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_builder_in_ops() {
        let pb = PredicateBuilder::new(&test_fields());
        let vals = vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)];

        let is_in = pb.is_in("id", vals.clone()).unwrap();
        match &is_in {
            Predicate::Leaf { op, literals, .. } => {
                assert_eq!(*op, PredicateOperator::In);
                assert_eq!(literals, &vals);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }

        let not_in = pb.is_not_in("id", vals.clone()).unwrap();
        match &not_in {
            Predicate::Leaf { op, .. } => assert_eq!(*op, PredicateOperator::NotIn),
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_builder_resolves_correct_type() {
        let pb = PredicateBuilder::new(&test_fields());
        let pred = pb.equal("dt", Datum::Date(19723)).unwrap();

        match &pred {
            Predicate::Leaf {
                index, data_type, ..
            } => {
                assert_eq!(*index, 2);
                assert_eq!(*data_type, DataType::Date(DateType::new()));
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    // ======================== Error cases ========================

    #[test]
    fn test_builder_unknown_column() {
        let pb = PredicateBuilder::new(&test_fields());
        let result = pb.equal("nonexistent", Datum::Int(1));
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("nonexistent"));
    }

    // ======================== Composition helpers ========================

    #[test]
    fn test_and_empty() {
        assert_eq!(Predicate::and(vec![]), Predicate::AlwaysTrue);
    }

    #[test]
    fn test_and_single() {
        let pb = PredicateBuilder::new(&test_fields());
        let p = pb.equal("id", Datum::Int(1)).unwrap();
        let combined = Predicate::and(vec![p.clone()]);
        assert_eq!(combined, p);
    }

    #[test]
    fn test_and_flattens() {
        let pb = PredicateBuilder::new(&test_fields());
        let p1 = pb.equal("id", Datum::Int(1)).unwrap();
        let p2 = pb.equal("id", Datum::Int(2)).unwrap();
        let p3 = pb.equal("id", Datum::Int(3)).unwrap();

        // Nested: AND(AND(p1, p2), p3) should flatten to AND(p1, p2, p3).
        let inner = Predicate::and(vec![p1.clone(), p2.clone()]);
        let outer = Predicate::and(vec![inner, p3.clone()]);

        match &outer {
            Predicate::And(children) => {
                assert_eq!(children.len(), 3);
                assert_eq!(children[0], p1);
                assert_eq!(children[1], p2);
                assert_eq!(children[2], p3);
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn test_and_flattens_deep_nesting() {
        let pb = PredicateBuilder::new(&test_fields());
        let p1 = pb.equal("id", Datum::Int(1)).unwrap();
        let p2 = pb.equal("id", Datum::Int(2)).unwrap();
        let p3 = pb.equal("id", Datum::Int(3)).unwrap();
        let p4 = pb.equal("id", Datum::Int(4)).unwrap();

        // Directly construct nested And via enum (bypassing Predicate::and flatten).
        let deep = Predicate::And(vec![Predicate::And(vec![
            Predicate::And(vec![p1.clone(), p2.clone()]),
            p3.clone(),
        ])]);
        // Now flatten through Predicate::and.
        let flat = Predicate::and(vec![deep, p4.clone()]);

        match &flat {
            Predicate::And(children) => {
                assert_eq!(children.len(), 4);
                assert_eq!(children[0], p1);
                assert_eq!(children[1], p2);
                assert_eq!(children[2], p3);
                assert_eq!(children[3], p4);
            }
            other => panic!("expected And with 4 children, got {other:?}"),
        }
    }

    #[test]
    fn test_or_empty() {
        assert_eq!(Predicate::or(vec![]), Predicate::AlwaysFalse);
    }

    #[test]
    fn test_or_single() {
        let pb = PredicateBuilder::new(&test_fields());
        let p = pb.equal("id", Datum::Int(1)).unwrap();
        let combined = Predicate::or(vec![p.clone()]);
        assert_eq!(combined, p);
    }

    #[test]
    fn test_or_flattens() {
        let pb = PredicateBuilder::new(&test_fields());
        let p1 = pb.equal("id", Datum::Int(1)).unwrap();
        let p2 = pb.equal("id", Datum::Int(2)).unwrap();
        let p3 = pb.equal("id", Datum::Int(3)).unwrap();

        let inner = Predicate::or(vec![p1.clone(), p2.clone()]);
        let outer = Predicate::or(vec![inner, p3.clone()]);

        match &outer {
            Predicate::Or(children) => {
                assert_eq!(children.len(), 3);
            }
            other => panic!("expected Or, got {other:?}"),
        }
    }

    #[test]
    fn test_or_flattens_deep_nesting() {
        let pb = PredicateBuilder::new(&test_fields());
        let p1 = pb.equal("id", Datum::Int(1)).unwrap();
        let p2 = pb.equal("id", Datum::Int(2)).unwrap();
        let p3 = pb.equal("id", Datum::Int(3)).unwrap();
        let p4 = pb.equal("id", Datum::Int(4)).unwrap();

        // Directly construct nested Or via enum (bypassing Predicate::or flatten).
        let deep = Predicate::Or(vec![Predicate::Or(vec![
            Predicate::Or(vec![p1.clone(), p2.clone()]),
            p3.clone(),
        ])]);
        let flat = Predicate::or(vec![deep, p4.clone()]);

        match &flat {
            Predicate::Or(children) => {
                assert_eq!(children.len(), 4);
            }
            other => panic!("expected Or with 4 children, got {other:?}"),
        }
    }

    #[test]
    fn test_not() {
        let pb = PredicateBuilder::new(&test_fields());
        let p = pb.equal("id", Datum::Int(1)).unwrap();
        let negated = Predicate::negate(p.clone());

        match &negated {
            Predicate::Not(inner) => assert_eq!(inner.as_ref(), &p),
            other => panic!("expected Not, got {other:?}"),
        }
    }

    // ======================== field_idx_to_partition_idx ========================

    #[test]
    fn test_field_idx_to_partition_idx_basic() {
        let fields = test_fields(); // [id, name, dt, hr]
        let partition_keys = vec!["dt".to_string(), "hr".to_string()];
        let mapping = field_idx_to_partition_idx(&fields, &partition_keys);

        assert_eq!(mapping, vec![None, None, Some(0), Some(1)]);
    }

    #[test]
    fn test_field_idx_to_partition_idx_no_partitions() {
        let fields = test_fields();
        let mapping = field_idx_to_partition_idx(&fields, &[]);

        assert_eq!(mapping, vec![None, None, None, None]);
    }

    #[test]
    fn test_field_idx_to_partition_idx_all_partitions() {
        let fields = vec![
            DataField::new(0, "a".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "b".to_string(), DataType::Int(IntType::new())),
        ];
        let partition_keys = vec!["a".to_string(), "b".to_string()];
        let mapping = field_idx_to_partition_idx(&fields, &partition_keys);

        assert_eq!(mapping, vec![Some(0), Some(1)]);
    }

    // ======================== Display ========================

    #[test]
    fn test_display_leaf() {
        let pb = PredicateBuilder::new(&test_fields());
        let p = pb.equal("dt", Datum::Date(19723)).unwrap();
        assert_eq!(format!("{p}"), "dt = DATE(19723)");
    }

    #[test]
    fn test_display_null_ops() {
        let pb = PredicateBuilder::new(&test_fields());
        assert_eq!(format!("{}", pb.is_null("name").unwrap()), "name IS NULL");
        assert_eq!(
            format!("{}", pb.is_not_null("name").unwrap()),
            "name IS NOT NULL"
        );
    }

    #[test]
    fn test_display_in() {
        let pb = PredicateBuilder::new(&test_fields());
        let p = pb.is_in("id", vec![Datum::Int(1), Datum::Int(2)]).unwrap();
        assert_eq!(format!("{p}"), "id IN (1, 2)");
    }

    #[test]
    fn test_display_compound() {
        let pb = PredicateBuilder::new(&test_fields());
        let p1 = pb.equal("dt", Datum::Date(19723)).unwrap();
        let p2 = pb.greater_than("id", Datum::Int(10)).unwrap();
        let combined = Predicate::and(vec![p1, p2]);
        assert_eq!(format!("{combined}"), "(dt = DATE(19723) AND id > 10)");
    }

    #[test]
    fn test_display_constants() {
        assert_eq!(format!("{}", Predicate::AlwaysTrue), "TRUE");
        assert_eq!(format!("{}", Predicate::AlwaysFalse), "FALSE");
    }

    #[test]
    fn test_display_not() {
        let pb = PredicateBuilder::new(&test_fields());
        let p = pb.equal("id", Datum::Int(1)).unwrap();
        let negated = Predicate::negate(p);
        assert_eq!(format!("{negated}"), "NOT (id = 1)");
    }

    // ======================== Datum-DataType validation ========================

    #[test]
    fn test_datum_type_mismatch_rejected() {
        let pb = PredicateBuilder::new(&test_fields());
        // dt is DateType, passing Int literal should fail.
        assert!(pb.equal("dt", Datum::Int(42)).is_err());
        // id is IntType, passing String literal should fail.
        assert!(pb.equal("id", Datum::String("hello".into())).is_err());
    }

    #[test]
    fn test_datum_type_validation_in_list() {
        let pb = PredicateBuilder::new(&test_fields());
        // One bad literal in the list should fail the whole is_in.
        let result = pb.is_in("id", vec![Datum::Int(1), Datum::String("bad".into())]);
        assert!(result.is_err());
    }

    // ======================== Empty IN / NOT IN handling ========================

    #[test]
    fn test_in_empty_returns_always_false() {
        let pb = PredicateBuilder::new(&test_fields());
        assert_eq!(pb.is_in("id", vec![]).unwrap(), Predicate::AlwaysFalse);
    }

    #[test]
    fn test_not_in_empty_returns_always_true() {
        let pb = PredicateBuilder::new(&test_fields());
        assert_eq!(pb.is_not_in("id", vec![]).unwrap(), Predicate::AlwaysTrue);
    }

    // ======================== Constant absorption ========================

    #[test]
    fn test_and_absorbs_always_true() {
        let pb = PredicateBuilder::new(&test_fields());
        let p = pb.equal("id", Datum::Int(1)).unwrap();
        assert_eq!(Predicate::and(vec![p.clone(), Predicate::AlwaysTrue]), p);
    }

    #[test]
    fn test_and_short_circuits_always_false() {
        let pb = PredicateBuilder::new(&test_fields());
        let p = pb.equal("id", Datum::Int(1)).unwrap();
        assert_eq!(
            Predicate::and(vec![p, Predicate::AlwaysFalse]),
            Predicate::AlwaysFalse
        );
    }

    #[test]
    fn test_or_absorbs_always_false() {
        let pb = PredicateBuilder::new(&test_fields());
        let p = pb.equal("id", Datum::Int(1)).unwrap();
        assert_eq!(Predicate::or(vec![p.clone(), Predicate::AlwaysFalse]), p);
    }

    #[test]
    fn test_or_short_circuits_always_true() {
        let pb = PredicateBuilder::new(&test_fields());
        let p = pb.equal("id", Datum::Int(1)).unwrap();
        assert_eq!(
            Predicate::or(vec![p, Predicate::AlwaysTrue]),
            Predicate::AlwaysTrue
        );
    }

    // ======================== Negate simplification ========================

    #[test]
    fn test_negate_double_negation() {
        let pb = PredicateBuilder::new(&test_fields());
        let p = pb.equal("id", Datum::Int(1)).unwrap();
        assert_eq!(Predicate::negate(Predicate::negate(p.clone())), p);
    }

    #[test]
    fn test_negate_always_true() {
        assert_eq!(
            Predicate::negate(Predicate::AlwaysTrue),
            Predicate::AlwaysFalse
        );
    }

    #[test]
    fn test_negate_always_false() {
        assert_eq!(
            Predicate::negate(Predicate::AlwaysFalse),
            Predicate::AlwaysTrue
        );
    }

    // ======================== Decimal equivalence ========================

    #[test]
    fn test_decimal_eq_same_scale() {
        let a = Datum::Decimal {
            unscaled: 100,
            precision: 10,
            scale: 2,
        };
        let b = Datum::Decimal {
            unscaled: 100,
            precision: 10,
            scale: 2,
        };
        assert_eq!(a, b);
    }

    #[test]
    fn test_decimal_eq_different_scale_same_value() {
        // 10 / 10^1 = 1.0, 100 / 10^2 = 1.00 — mathematically equal
        let a = Datum::Decimal {
            unscaled: 10,
            precision: 10,
            scale: 1,
        };
        let b = Datum::Decimal {
            unscaled: 100,
            precision: 10,
            scale: 2,
        };
        assert_eq!(a, b);
    }

    #[test]
    fn test_decimal_ne_different_value() {
        let a = Datum::Decimal {
            unscaled: 10,
            precision: 10,
            scale: 1,
        };
        let b = Datum::Decimal {
            unscaled: 20,
            precision: 10,
            scale: 1,
        };
        assert_ne!(a, b);
    }

    #[test]
    fn test_decimal_eq_zero_different_scale() {
        // 0 at any scale is still 0
        let a = Datum::Decimal {
            unscaled: 0,
            precision: 10,
            scale: 0,
        };
        let b = Datum::Decimal {
            unscaled: 0,
            precision: 10,
            scale: 5,
        };
        assert_eq!(a, b);
    }

    // ======================== PartialOrd ========================

    #[test]
    fn test_datum_partial_ord_int() {
        assert!(Datum::Int(1) < Datum::Int(2));
        assert!(Datum::Int(2) > Datum::Int(1));
        assert!(Datum::Int(1) <= Datum::Int(1));
        assert!(Datum::Int(1) >= Datum::Int(1));
    }

    #[test]
    fn test_datum_partial_ord_string() {
        assert!(Datum::String("a".into()) < Datum::String("b".into()));
        assert!(Datum::String("b".into()) > Datum::String("a".into()));
    }

    #[test]
    fn test_datum_partial_ord_decimal_cross_scale() {
        // 10 / 10^1 = 1.0 < 200 / 10^2 = 2.0
        let a = Datum::Decimal {
            unscaled: 10,
            precision: 10,
            scale: 1,
        };
        let b = Datum::Decimal {
            unscaled: 200,
            precision: 10,
            scale: 2,
        };
        assert!(a < b);
    }

    #[test]
    fn test_datum_partial_ord_bytes_matches_java_unsigned_byte_order() {
        assert!(Datum::Bytes(vec![0x00]) < Datum::Bytes(vec![0xFF]));
    }

    #[test]
    fn test_datum_partial_ord_cross_variant_is_none() {
        assert_eq!(Datum::Int(1).partial_cmp(&Datum::Long(1)), None);
    }

    // ======================== eval_row ========================

    /// Minimal BinaryRow builder for predicate evaluation tests.
    struct TestBinaryRowBuilder {
        arity: i32,
        null_bits_size: usize,
        data: Vec<u8>,
    }

    impl TestBinaryRowBuilder {
        fn new(arity: i32) -> Self {
            let null_bits_size = BinaryRow::cal_bit_set_width_in_bytes(arity) as usize;
            let fixed_part_size = null_bits_size + (arity as usize) * 8;
            Self {
                arity,
                null_bits_size,
                data: vec![0u8; fixed_part_size],
            }
        }

        fn field_offset(&self, pos: usize) -> usize {
            self.null_bits_size + pos * 8
        }

        fn set_null_at(&mut self, pos: usize) {
            let bit_index = pos + BinaryRow::HEADER_SIZE_IN_BYTES as usize;
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;
            self.data[byte_index] |= 1 << bit_offset;
            let offset = self.field_offset(pos);
            self.data[offset..offset + 8].fill(0);
        }

        fn write_int(&mut self, pos: usize, value: i32) {
            let offset = self.field_offset(pos);
            self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        }

        fn build(self) -> BinaryRow {
            BinaryRow::from_bytes(self.arity, self.data)
        }
    }

    fn make_leaf(col: &str, idx: usize, op: PredicateOperator, literals: Vec<Datum>) -> Predicate {
        Predicate::Leaf {
            column: col.into(),
            index: idx,
            data_type: DataType::Int(IntType::new()),
            op,
            literals,
        }
    }

    #[test]
    fn test_eval_leaf_operators() {
        // row: [x=10]
        let mut b = TestBinaryRowBuilder::new(1);
        b.write_int(0, 10);
        let row = b.build();

        // Eq
        assert!(eval_row(
            &make_leaf("x", 0, PredicateOperator::Eq, vec![Datum::Int(10)]),
            &row
        )
        .unwrap());
        assert!(!eval_row(
            &make_leaf("x", 0, PredicateOperator::Eq, vec![Datum::Int(99)]),
            &row
        )
        .unwrap());
        // NotEq
        assert!(eval_row(
            &make_leaf("x", 0, PredicateOperator::NotEq, vec![Datum::Int(99)]),
            &row
        )
        .unwrap());
        // Lt / LtEq / Gt / GtEq
        assert!(eval_row(
            &make_leaf("x", 0, PredicateOperator::Lt, vec![Datum::Int(20)]),
            &row
        )
        .unwrap());
        assert!(!eval_row(
            &make_leaf("x", 0, PredicateOperator::Gt, vec![Datum::Int(20)]),
            &row
        )
        .unwrap());
        assert!(eval_row(
            &make_leaf("x", 0, PredicateOperator::LtEq, vec![Datum::Int(10)]),
            &row
        )
        .unwrap());
        assert!(eval_row(
            &make_leaf("x", 0, PredicateOperator::GtEq, vec![Datum::Int(10)]),
            &row
        )
        .unwrap());
        // In / NotIn
        assert!(eval_row(
            &make_leaf(
                "x",
                0,
                PredicateOperator::In,
                vec![Datum::Int(1), Datum::Int(10)]
            ),
            &row
        )
        .unwrap());
        assert!(!eval_row(
            &make_leaf(
                "x",
                0,
                PredicateOperator::In,
                vec![Datum::Int(1), Datum::Int(2)]
            ),
            &row
        )
        .unwrap());
        // NotIn: 10 not in {1, 2} → true; 10 not in {10, 20} → false
        assert!(eval_row(
            &make_leaf(
                "x",
                0,
                PredicateOperator::NotIn,
                vec![Datum::Int(1), Datum::Int(2)]
            ),
            &row
        )
        .unwrap());
        assert!(!eval_row(
            &make_leaf(
                "x",
                0,
                PredicateOperator::NotIn,
                vec![Datum::Int(10), Datum::Int(20)]
            ),
            &row
        )
        .unwrap());
    }

    #[test]
    fn test_eval_null_semantics() {
        let mut b = TestBinaryRowBuilder::new(1);
        b.set_null_at(0);
        let row = b.build();

        // NULL compared to any value → false (SQL null semantics)
        assert!(!eval_row(
            &make_leaf("x", 0, PredicateOperator::Eq, vec![Datum::Int(42)]),
            &row
        )
        .unwrap());
        // IsNull / IsNotNull
        assert!(eval_row(&make_leaf("x", 0, PredicateOperator::IsNull, vec![]), &row).unwrap());
        assert!(!eval_row(
            &make_leaf("x", 0, PredicateOperator::IsNotNull, vec![]),
            &row
        )
        .unwrap());
    }

    #[test]
    fn test_eval_compound_and_constants() {
        let mut b = TestBinaryRowBuilder::new(2);
        b.write_int(0, 10);
        b.write_int(1, 20);
        let row = b.build();

        let p_true = make_leaf("a", 0, PredicateOperator::Eq, vec![Datum::Int(10)]);
        let p_false = make_leaf("b", 1, PredicateOperator::Eq, vec![Datum::Int(99)]);

        assert!(!eval_row(&Predicate::and(vec![p_true.clone(), p_false.clone()]), &row).unwrap());
        assert!(eval_row(&Predicate::or(vec![p_true.clone(), p_false.clone()]), &row).unwrap());
        assert!(!eval_row(&Predicate::negate(p_true), &row).unwrap());

        // Constants
        let empty_row = TestBinaryRowBuilder::new(0).build();
        assert!(eval_row(&Predicate::AlwaysTrue, &empty_row).unwrap());
        assert!(!eval_row(&Predicate::AlwaysFalse, &empty_row).unwrap());
    }

    // ======================== split_and ========================

    #[test]
    fn test_split_and() {
        let pb = PredicateBuilder::new(&test_fields());
        let p1 = pb.equal("id", Datum::Int(1)).unwrap();
        let p2 = pb.equal("dt", Datum::Date(19723)).unwrap();

        // AND → children
        let parts = Predicate::and(vec![p1.clone(), p2.clone()]).split_and();
        assert_eq!(parts, vec![p1.clone(), p2]);
        // Non-AND → single-element vec
        assert_eq!(p1.clone().split_and(), vec![p1]);
    }

    #[test]
    fn test_split_and_recursive_nested() {
        let pb = PredicateBuilder::new(&test_fields());
        let p1 = pb.equal("id", Datum::Int(1)).unwrap();
        let p2 = pb.equal("dt", Datum::Date(19723)).unwrap();
        let p3 = pb.equal("hr", Datum::Int(10)).unwrap();

        // Manually construct nested And (bypassing Predicate::and which flattens).
        // And(And(p1, p2), p3) should still flatten to [p1, p2, p3].
        let inner = Predicate::And(vec![p1.clone(), p2.clone()]);
        let outer = Predicate::And(vec![inner, p3.clone()]);
        let parts = outer.split_and();
        assert_eq!(parts, vec![p1, p2, p3]);
    }

    // ======================== remap_field_index ========================

    #[test]
    fn test_remap_pure_partition_leaf() {
        let pb = PredicateBuilder::new(&test_fields()); // [id(0), name(1), dt(2), hr(3)]
        let p = pb.equal("dt", Datum::Date(19723)).unwrap(); // index=2
        let mapping = vec![None, None, Some(0), Some(1)]; // dt→0, hr→1

        let remapped = p.remap_field_index(&mapping).unwrap();
        match &remapped {
            Predicate::Leaf { index, column, .. } => {
                assert_eq!(column, "dt");
                assert_eq!(*index, 0); // remapped to partition index
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_remap_non_partition_leaf_returns_none() {
        let pb = PredicateBuilder::new(&test_fields());
        let p = pb.equal("id", Datum::Int(1)).unwrap(); // index=0, not a partition key
        let mapping = vec![None, None, Some(0), Some(1)];

        assert!(p.remap_field_index(&mapping).is_none());
    }

    #[test]
    fn test_remap_and_all_partition() {
        let pb = PredicateBuilder::new(&test_fields());
        let p1 = pb.equal("dt", Datum::Date(19723)).unwrap();
        let p2 = pb.equal("hr", Datum::Int(10)).unwrap();
        let combined = Predicate::and(vec![p1, p2]);
        let mapping = vec![None, None, Some(0), Some(1)];

        let remapped = combined.remap_field_index(&mapping).unwrap();
        match &remapped {
            Predicate::And(children) => {
                assert_eq!(children.len(), 2);
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn test_remap_or_with_mixed_returns_none() {
        let pb = PredicateBuilder::new(&test_fields());
        let p_partition = pb.equal("dt", Datum::Date(19723)).unwrap();
        let p_data = pb.equal("id", Datum::Int(1)).unwrap();
        let combined = Predicate::or(vec![p_partition, p_data]);
        let mapping = vec![None, None, Some(0), Some(1)];

        // OR with mixed columns → cannot safely extract → None
        assert!(combined.remap_field_index(&mapping).is_none());
    }

    /// Regression test: `eval_row` must propagate errors from `extract_datum`
    /// as `Err` (fail-fast), not swallow them into `Ok(true)` (fail-open).
    ///
    /// This guards the invariant at `table_scan.rs` partition pruning where
    /// `eval_row(pred, &row)?` was intentionally changed from fail-open to
    /// fail-fast.  An unsupported DataType in a leaf triggers `Err` from
    /// `extract_datum`; we verify it surfaces through `eval_row`.
    #[test]
    fn test_eval_row_propagates_extract_error() {
        let mut b = TestBinaryRowBuilder::new(1);
        b.write_int(0, 42);
        let row = b.build();

        // Leaf with unsupported DataType → extract_datum returns Err.
        let unsupported_leaf = Predicate::Leaf {
            column: "arr".into(),
            index: 0,
            data_type: DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            op: PredicateOperator::Eq,
            literals: vec![Datum::Int(42)],
        };

        // Must be Err, not Ok(true).
        assert!(eval_row(&unsupported_leaf, &row).is_err());

        // Also verify error propagates through compound predicates (And/Or/Not).
        let and_pred = Predicate::And(vec![Predicate::AlwaysTrue, unsupported_leaf.clone()]);
        assert!(eval_row(&and_pred, &row).is_err());

        let or_pred = Predicate::Or(vec![Predicate::AlwaysFalse, unsupported_leaf.clone()]);
        assert!(eval_row(&or_pred, &row).is_err());

        let not_pred = Predicate::Not(Box::new(unsupported_leaf));
        assert!(eval_row(&not_pred, &row).is_err());
    }

    #[test]
    fn test_remap_not_with_mixed_returns_none() {
        let pb = PredicateBuilder::new(&test_fields());
        let p_partition = pb.equal("dt", Datum::Date(19723)).unwrap();
        let p_data = pb.greater_than("id", Datum::Int(10)).unwrap();
        let inner = Predicate::and(vec![p_partition, p_data]);
        let negated = Predicate::negate(inner);
        let mapping = vec![None, None, Some(0), Some(1)];

        // NOT(partition AND data) → mixed under NOT → None
        assert!(negated.remap_field_index(&mapping).is_none());
    }

    // ================== project_field_index_inclusive ==================

    #[test]
    fn test_project_inclusive_and_keeps_partition_children() {
        let pb = PredicateBuilder::new(&test_fields());
        let mixed = Predicate::and(vec![
            pb.equal("dt", Datum::Date(19723)).unwrap(),
            pb.greater_than("id", Datum::Int(10)).unwrap(),
        ]);
        let mapping = vec![None, None, Some(0), Some(1)];

        let projected = mixed.project_field_index_inclusive(&mapping).unwrap();
        match projected {
            Predicate::Leaf { column, index, .. } => {
                assert_eq!(column, "dt");
                assert_eq!(index, 0);
            }
            other => panic!("expected projected partition leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_project_inclusive_and_all_data_returns_none() {
        let pb = PredicateBuilder::new(&test_fields());
        let data_only = Predicate::and(vec![
            pb.equal("id", Datum::Int(1)).unwrap(),
            pb.equal("name", Datum::String("alice".into())).unwrap(),
        ]);
        let mapping = vec![None, None, Some(0), Some(1)];

        assert!(data_only.project_field_index_inclusive(&mapping).is_none());
    }

    #[test]
    fn test_project_inclusive_or_with_mixed_returns_none() {
        let pb = PredicateBuilder::new(&test_fields());
        let p_partition = pb.equal("dt", Datum::Date(19723)).unwrap();
        let p_data = pb.equal("id", Datum::Int(1)).unwrap();
        let combined = Predicate::or(vec![p_partition, p_data]);
        let mapping = vec![None, None, Some(0), Some(1)];

        assert!(combined.project_field_index_inclusive(&mapping).is_none());
    }

    #[test]
    fn test_project_inclusive_or_of_mixed_ands_projects_each_branch() {
        let pb = PredicateBuilder::new(&test_fields());
        let left = Predicate::and(vec![
            pb.equal("dt", Datum::Date(19723)).unwrap(),
            pb.greater_than("id", Datum::Int(10)).unwrap(),
        ]);
        let right = Predicate::and(vec![
            pb.equal("hr", Datum::Int(10)).unwrap(),
            pb.equal("name", Datum::String("alice".into())).unwrap(),
        ]);
        let combined = Predicate::or(vec![left, right]);
        let mapping = vec![None, None, Some(0), Some(1)];

        let projected = combined.project_field_index_inclusive(&mapping).unwrap();
        match projected {
            Predicate::Or(children) => {
                assert_eq!(children.len(), 2);
                assert!(matches!(
                    &children[0],
                    Predicate::Leaf {
                        column,
                        index: 0,
                        ..
                    } if column == "dt"
                ));
                assert!(matches!(
                    &children[1],
                    Predicate::Leaf {
                        column,
                        index: 1,
                        ..
                    } if column == "hr"
                ));
            }
            other => panic!("expected projected OR, got {other:?}"),
        }
    }

    #[test]
    fn test_project_inclusive_not_with_mixed_returns_none() {
        let pb = PredicateBuilder::new(&test_fields());
        let inner = Predicate::and(vec![
            pb.equal("dt", Datum::Date(19723)).unwrap(),
            pb.greater_than("id", Datum::Int(10)).unwrap(),
        ]);
        let mapping = vec![None, None, Some(0), Some(1)];

        assert!(Predicate::negate(inner)
            .project_field_index_inclusive(&mapping)
            .is_none());
    }

    // ======================== string operators ========================

    #[test]
    fn test_builder_starts_with() {
        let pb = PredicateBuilder::new(&test_fields());
        let pred = pb
            .starts_with("name", Datum::String("foo".to_string()))
            .unwrap();
        match &pred {
            Predicate::Leaf { op, literals, .. } => {
                assert_eq!(*op, PredicateOperator::StartsWith);
                assert_eq!(literals, &[Datum::String("foo".to_string())]);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_builder_ends_with() {
        let pb = PredicateBuilder::new(&test_fields());
        let pred = pb
            .ends_with("name", Datum::String("bar".to_string()))
            .unwrap();
        match &pred {
            Predicate::Leaf { op, .. } => assert_eq!(*op, PredicateOperator::EndsWith),
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_builder_contains() {
        let pb = PredicateBuilder::new(&test_fields());
        let pred = pb
            .contains("name", Datum::String("baz".to_string()))
            .unwrap();
        match &pred {
            Predicate::Leaf { op, .. } => assert_eq!(*op, PredicateOperator::Contains),
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_builder_string_ops_empty_pattern_is_not_null() {
        let pb = PredicateBuilder::new(&test_fields());
        // An empty pattern matches every non-null string and no NULL, so it is
        // exactly `IsNotNull` (not `AlwaysTrue`, which would retain NULL rows).
        for build in [
            pb.starts_with("name", Datum::String(String::new())),
            pb.ends_with("name", Datum::String(String::new())),
            pb.contains("name", Datum::String(String::new())),
        ] {
            assert!(matches!(
                build.unwrap(),
                Predicate::Leaf {
                    op: PredicateOperator::IsNotNull,
                    ..
                }
            ));
        }
    }

    #[test]
    fn test_builder_string_ops_reject_non_string_pattern() {
        let pb = PredicateBuilder::new(&test_fields());
        assert!(pb.starts_with("name", Datum::Int(1)).is_err());
        assert!(pb.ends_with("name", Datum::Int(1)).is_err());
        assert!(pb.contains("name", Datum::Int(1)).is_err());
    }

    #[test]
    fn test_builder_string_ops_reject_non_string_column() {
        let pb = PredicateBuilder::new(&test_fields());
        // `id` is Int, so a String literal fails the cross-check inside leaf().
        assert!(pb
            .starts_with("id", Datum::String("x".to_string()))
            .is_err());
    }

    #[test]
    fn test_eval_string_operators() {
        let lit = Datum::String("oo".to_string());
        let val = Datum::String("foobar".to_string());

        assert!(eval_leaf(
            PredicateOperator::Contains,
            Some(&val),
            std::slice::from_ref(&lit),
        ));
        assert!(!eval_leaf(
            PredicateOperator::StartsWith,
            Some(&val),
            std::slice::from_ref(&lit),
        ));
        assert!(eval_leaf(
            PredicateOperator::StartsWith,
            Some(&val),
            &[Datum::String("foo".to_string())],
        ));
        assert!(eval_leaf(
            PredicateOperator::EndsWith,
            Some(&val),
            &[Datum::String("bar".to_string())],
        ));
        assert!(!eval_leaf(
            PredicateOperator::EndsWith,
            Some(&val),
            &[Datum::String("baz".to_string())],
        ));
        // NULL value → false (SQL three-valued logic).
        assert!(!eval_leaf(
            PredicateOperator::StartsWith,
            None,
            &[Datum::String("foo".to_string())],
        ));
    }

    // ======================== LIKE operator ========================

    fn assert_leaf(pred: &Predicate, expected_op: PredicateOperator, expected_lit: &str) {
        match pred {
            Predicate::Leaf { op, literals, .. } => {
                assert_eq!(*op, expected_op, "op mismatch");
                assert_eq!(literals, &[Datum::String(expected_lit.to_string())]);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_like_optimization_rewrites() {
        let pb = PredicateBuilder::new(&test_fields());
        // No wildcards → Eq.
        assert_leaf(
            &pb.like("name", Datum::String("foo".to_string()), None)
                .unwrap(),
            PredicateOperator::Eq,
            "foo",
        );
        // Empty pattern → Eq("") (only empty string matches).
        assert_leaf(
            &pb.like("name", Datum::String(String::new()), None).unwrap(),
            PredicateOperator::Eq,
            "",
        );
        // prefix% → StartsWith.
        assert_leaf(
            &pb.like("name", Datum::String("foo%".to_string()), None)
                .unwrap(),
            PredicateOperator::StartsWith,
            "foo",
        );
        // %suffix → EndsWith.
        assert_leaf(
            &pb.like("name", Datum::String("%bar".to_string()), None)
                .unwrap(),
            PredicateOperator::EndsWith,
            "bar",
        );
        // %mid% → Contains.
        assert_leaf(
            &pb.like("name", Datum::String("%baz%".to_string()), None)
                .unwrap(),
            PredicateOperator::Contains,
            "baz",
        );
    }

    #[test]
    fn test_like_residual_for_non_optimizable_patterns() {
        let pb = PredicateBuilder::new(&test_fields());
        // `_` keeps Like leaf.
        assert_leaf(
            &pb.like("name", Datum::String("f_o".to_string()), None)
                .unwrap(),
            PredicateOperator::Like,
            "f_o",
        );
        // Multi-segment % keeps Like leaf.
        assert_leaf(
            &pb.like("name", Datum::String("a%b%c".to_string()), None)
                .unwrap(),
            PredicateOperator::Like,
            "a%b%c",
        );
        // Escaped wildcards keep Like leaf (optimization is conservative).
        assert_leaf(
            &pb.like("name", Datum::String(r"foo\%".to_string()), None)
                .unwrap(),
            PredicateOperator::Like,
            r"foo\%",
        );
    }

    #[test]
    fn test_like_rejects_custom_escape_char() {
        let pb = PredicateBuilder::new(&test_fields());
        assert!(pb
            .like("name", Datum::String("a$%".to_string()), Some('$'))
            .is_err());
    }

    #[test]
    fn test_like_rejects_non_string_pattern() {
        let pb = PredicateBuilder::new(&test_fields());
        assert!(pb.like("name", Datum::Int(1), None).is_err());
    }

    #[test]
    fn test_like_match_evaluator() {
        // Patterns that fall back to Like leaf must evaluate correctly.
        assert!(super::like_match("foobar", "f_o%"));
        assert!(super::like_match("foobar", "%bar"));
        assert!(super::like_match("foobar", "f%r"));
        assert!(!super::like_match("foobar", "f_x%"));
        // `_` requires exactly one character.
        assert!(super::like_match("ab", "a_"));
        assert!(!super::like_match("a", "a_"));
        assert!(!super::like_match("abc", "a_"));
        // Escape handling.
        assert!(super::like_match("100%", "100\\%"));
        assert!(!super::like_match("1000", "100\\%"));
        assert!(super::like_match("a_b", "a\\_b"));
        assert!(!super::like_match("axb", "a\\_b"));
        // Escaped non-wildcard: `\X` matches literal `X`, not `\X` (arrow
        // semantics, verified against arrow_string's like_escape test).
        assert!(super::like_match("a", "\\a"));
        assert!(!super::like_match("\\a", "\\a"));
        // Trailing backslash matches a literal backslash.
        assert!(super::like_match("\\", "\\"));
        // Empty pattern only matches empty value.
        assert!(super::like_match("", ""));
        assert!(!super::like_match("a", ""));
    }

    // ======================== BETWEEN / NOT BETWEEN ========================

    #[test]
    fn test_builder_between_keeps_inclusive_range() {
        let pb = PredicateBuilder::new(&test_fields());
        let pred = pb.between("id", Datum::Int(1), Datum::Int(10)).unwrap();
        match &pred {
            Predicate::Leaf { op, literals, .. } => {
                assert_eq!(*op, PredicateOperator::Between);
                assert_eq!(literals, &[Datum::Int(1), Datum::Int(10)]);
            }
            other => panic!("expected Leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_builder_between_low_above_high_short_circuits_to_always_false() {
        let pb = PredicateBuilder::new(&test_fields());
        let pred = pb.between("id", Datum::Int(10), Datum::Int(1)).unwrap();
        assert!(matches!(pred, Predicate::AlwaysFalse));
    }

    #[test]
    fn test_builder_not_between_low_above_high_short_circuits_to_is_not_null() {
        let pb = PredicateBuilder::new(&test_fields());
        let pred = pb.not_between("id", Datum::Int(10), Datum::Int(1)).unwrap();
        match &pred {
            Predicate::Leaf { op, .. } => assert_eq!(*op, PredicateOperator::IsNotNull),
            other => panic!("expected IsNotNull leaf, got {other:?}"),
        }
    }

    #[test]
    fn test_builder_between_rejects_type_mismatch() {
        let pb = PredicateBuilder::new(&test_fields());
        // `id` is Int — String literal violates leaf() type cross-check.
        assert!(pb
            .between("id", Datum::String("a".to_string()), Datum::Int(10))
            .is_err());
    }

    #[test]
    fn test_eval_between_inclusive() {
        let lits = [Datum::Int(5), Datum::Int(10)];
        for v in [5, 7, 10] {
            assert!(eval_leaf(
                PredicateOperator::Between,
                Some(&Datum::Int(v)),
                &lits,
            ));
        }
        for v in [4, 11] {
            assert!(!eval_leaf(
                PredicateOperator::Between,
                Some(&Datum::Int(v)),
                &lits,
            ));
        }
        // NULL value → false.
        assert!(!eval_leaf(PredicateOperator::Between, None, &lits));
    }

    #[test]
    fn test_eval_not_between_complement_with_null_false() {
        let lits = [Datum::Int(5), Datum::Int(10)];
        for v in [4, 11] {
            assert!(eval_leaf(
                PredicateOperator::NotBetween,
                Some(&Datum::Int(v)),
                &lits,
            ));
        }
        for v in [5, 7, 10] {
            assert!(!eval_leaf(
                PredicateOperator::NotBetween,
                Some(&Datum::Int(v)),
                &lits,
            ));
        }
        // NULL value → false (matches existing NotEq null-semantics).
        assert!(!eval_leaf(PredicateOperator::NotBetween, None, &lits));
    }

    // ======================== REST catalog Predicate JSON interop ========================

    fn rest_leaf_json_typed(function: &str, field: &str, ftype: &str, literals: &str) -> String {
        format!(
            r#"{{"kind":"LEAF","transform":{{"name":"FIELD_REF","fieldRef":{{"index":0,"name":"{field}","type":"{ftype}"}}}},"function":"{function}","literals":{literals}}}"#
        )
    }

    fn rest_leaf_json(function: &str, field: &str, literals: &str) -> String {
        format!(
            r#"{{"kind":"LEAF","transform":{{"name":"FIELD_REF","fieldRef":{{"index":0,"name":"{field}","type":"INT"}}}},"function":"{function}","literals":{literals}}}"#
        )
    }

    /// Wire strings below are taken verbatim from Java `PredicateJsonSerdeTest`.
    #[test]
    fn test_from_rest_json_matches_java_wire_format() {
        let fields = vec![DataField::new(
            0,
            "f0".to_string(),
            DataType::Int(IntType::new()),
        )];
        let json = r#"{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"f0","type":"INT"}},"function":"EQUAL","literals":[1]}"#;
        let parsed = Predicate::from_rest_json(json, &fields).unwrap();
        assert!(matches!(
            &parsed,
            Predicate::Leaf { column, op: PredicateOperator::Eq, literals, .. }
                if column == "f0" && literals == &vec![Datum::Int(1)]
        ));

        // IS_NULL carries an empty literals array.
        let json = r#"{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"f0","type":"INT"}},"function":"IS_NULL","literals":[]}"#;
        let parsed = Predicate::from_rest_json(json, &fields).unwrap();
        assert!(matches!(
            &parsed,
            Predicate::Leaf {
                op: PredicateOperator::IsNull,
                ..
            }
        ));

        // AlwaysTrue/AlwaysFalse: LEAF with a placeholder NULL transform.
        let json = r#"{"kind":"LEAF","transform":{"name":"NULL"},"function":"TRUE","literals":[]}"#;
        let parsed = Predicate::from_rest_json(json, &fields).unwrap();
        assert!(matches!(parsed, Predicate::AlwaysTrue));
        let json =
            r#"{"kind":"LEAF","transform":{"name":"NULL"},"function":"FALSE","literals":[]}"#;
        let parsed = Predicate::from_rest_json(json, &fields).unwrap();
        assert!(matches!(parsed, Predicate::AlwaysFalse));
        // ... and as a COMPOUND child, absorbed: AND(FALSE, x) -> AlwaysFalse.
        let json = r#"{"kind":"COMPOUND","function":"AND","children":[{"kind":"LEAF","transform":{"name":"NULL"},"function":"FALSE","literals":[]},{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"f0","type":"INT"}},"function":"EQUAL","literals":[1]}]}"#;
        let parsed = Predicate::from_rest_json(json, &fields).unwrap();
        assert!(matches!(&parsed, Predicate::AlwaysFalse));

        // Nested COMPOUND(OR).
        let json = r#"{"kind":"COMPOUND","function":"OR","children":[{"kind":"COMPOUND","function":"OR","children":[{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"f0","type":"INT"}},"function":"EQUAL","literals":[1]},{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"f0","type":"INT"}},"function":"EQUAL","literals":[2]}]},{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"f0","type":"INT"}},"function":"EQUAL","literals":[3]}]}"#;
        assert!(matches!(
            Predicate::from_rest_json(json, &fields).unwrap(),
            Predicate::Or(_)
        ));

        // BETWEEN carries two literals.
        let json = r#"{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"f0","type":"INT"}},"function":"BETWEEN","literals":[3,7]}"#;
        assert!(Predicate::from_rest_json(json, &fields).is_ok());
    }

    #[test]
    fn test_from_rest_json_rejects_empty_compounds() {
        let fields = test_fields();
        for function in ["AND", "OR"] {
            let json = format!(r#"{{"kind":"COMPOUND","function":"{function}","children":[]}}"#);
            assert!(
                Predicate::from_rest_json(&json, &fields).is_err(),
                "empty {function} must fail closed"
            );
        }
    }

    #[test]
    fn test_from_rest_json_resolves_by_name_with_local_type() {
        // Wire says index 0 / INT; by-name resolution and the local type win.
        let parsed =
            Predicate::from_rest_json(&rest_leaf_json("EQUAL", "name", "[\"x\"]"), &test_fields())
                .unwrap();
        assert!(matches!(
            &parsed,
            Predicate::Leaf { index: 1, literals, .. }
                if literals == &vec![Datum::String("x".to_string())]
        ));
    }

    #[test]
    fn test_from_rest_json_parses_every_supported_function() {
        let fields = test_fields();
        for (function, field, literals) in [
            ("IS_NULL", "id", "[]"),
            ("IS_NOT_NULL", "id", "[]"),
            ("EQUAL", "id", "[5]"),
            ("NOT_EQUAL", "id", "[5]"),
            ("GREATER_THAN", "id", "[5]"),
            ("GREATER_OR_EQUAL", "id", "[5]"),
            ("LESS_THAN", "id", "[5]"),
            ("LESS_OR_EQUAL", "id", "[5]"),
            ("IN", "id", "[1,2,3]"),
            ("NOT_IN", "id", "[1,2,3]"),
            ("STARTS_WITH", "name", "[\"a\"]"),
            ("ENDS_WITH", "name", "[\"a\"]"),
            ("CONTAINS", "name", "[\"a\"]"),
            ("LIKE", "name", "[\"%a%\"]"),
            ("BETWEEN", "id", "[1,9]"),
            ("NOT_BETWEEN", "id", "[1,9]"),
        ] {
            let json = rest_leaf_json(function, field, literals);
            assert!(
                Predicate::from_rest_json(&json, &fields).is_ok(),
                "must parse {function}: {json}"
            );
        }
        // DATE has no Java-round-trippable wire form; every shape fails closed.
        for literals in ["[19000]", "[\"2022-01-15\"]", "[[2022,1,15]]"] {
            let json = rest_leaf_json("EQUAL", "dt", literals);
            assert!(
                Predicate::from_rest_json(&json, &fields).is_err(),
                "DATE literal must be rejected: {json}"
            );
        }
    }

    /// Null-literal semantics mirror the Java evaluators (binary/ternary test
    /// false on null; In skips nulls; NotIn fails every row on null).
    #[test]
    fn test_from_rest_json_null_literal_semantics() {
        let fields = test_fields();
        // Binary / ternary functions with a null literal -> AlwaysFalse.
        for (function, literals) in [
            ("EQUAL", "[null]"),
            ("GREATER_THAN", "[null]"),
            ("LIKE", "[null]"),
            ("BETWEEN", "[null,7]"),
            ("NOT_BETWEEN", "[1,null]"),
            // NOT_IN with any null literal fails every row.
            ("NOT_IN", "[1,null]"),
            // IN with only null literals matches nothing.
            ("IN", "[null]"),
            ("IN", "[]"),
        ] {
            let json = rest_leaf_json(function, "id", literals);
            assert!(
                matches!(
                    Predicate::from_rest_json(&json, &fields).unwrap(),
                    Predicate::AlwaysFalse
                ),
                "{function} {literals} must parse to AlwaysFalse"
            );
        }

        // IN drops null literals and keeps the rest.
        let parsed =
            Predicate::from_rest_json(&rest_leaf_json("IN", "id", "[1,null,2]"), &fields).unwrap();
        assert!(matches!(
            &parsed,
            Predicate::Leaf { op: PredicateOperator::In, literals, .. }
                if literals == &vec![Datum::Int(1), Datum::Int(2)]
        ));

        // Empty NOT_IN keeps exactly the non-null rows (Java: null -> false).
        let parsed =
            Predicate::from_rest_json(&rest_leaf_json("NOT_IN", "id", "[]"), &fields).unwrap();
        assert!(matches!(
            &parsed,
            Predicate::Leaf {
                op: PredicateOperator::IsNotNull,
                ..
            }
        ));
    }

    #[test]
    fn test_from_rest_json_like_escape_grammar() {
        let fields = test_fields();
        // Invalid escapes (Java `sqlToRegexLike` throws) fail closed.
        for pattern in ["_\\a", "abc\\"] {
            let json = rest_leaf_json("LIKE", "name", &format!("[{pattern:?}]"));
            assert!(
                Predicate::from_rest_json(&json, &fields).is_err(),
                "invalid LIKE escape must be rejected: {json}"
            );
        }
        // Valid patterns (incl. unescaped `_`) parse.
        for pattern in ["a\\%b", "a\\_b", "a\\\\b", "%abc_", "a_b"] {
            let json = rest_leaf_json("LIKE", "name", &format!("[{pattern:?}]"));
            assert!(
                Predicate::from_rest_json(&json, &fields).is_ok(),
                "valid LIKE pattern must parse: {json}"
            );
        }
    }

    #[test]
    fn test_from_rest_json_accepts_zero_and_less_than_floats() {
        let double_fields = vec![DataField::new(
            0,
            "f".to_string(),
            DataType::Double(crate::spec::DoubleType::new()),
        )];
        // Zero literals and `<` on floats parse.
        for json in [
            rest_leaf_json_typed("EQUAL", "f", "DOUBLE", "[0.0]"),
            rest_leaf_json_typed("EQUAL", "f", "DOUBLE", "[-0.0]"),
            rest_leaf_json_typed("LESS_THAN", "f", "DOUBLE", "[5.0]"),
        ] {
            assert!(
                Predicate::from_rest_json(&json, &double_fields).is_ok(),
                "{json}"
            );
        }
    }

    #[test]
    fn test_from_rest_json_validates_every_literal() {
        let fields = test_fields();
        // A malformed extra literal in a binary leaf fails the whole parse
        // (Java deserializeLiterals converts every element), not just first().
        let json = rest_leaf_json("EQUAL", "id", r#"[1,"bad"]"#);
        assert!(Predicate::from_rest_json(&json, &fields).is_err());
        // A malformed element in an IN list also fails.
        let json = rest_leaf_json("IN", "id", r#"[1,"bad",3]"#);
        assert!(Predicate::from_rest_json(&json, &fields).is_err());
        // A valid extra literal is accepted (Java validates but ignores it).
        let json = rest_leaf_json("EQUAL", "id", "[1,2]");
        assert!(Predicate::from_rest_json(&json, &fields).is_ok());
    }

    #[test]
    fn test_from_rest_json_float_integral_single_rounding() {
        let float_fields = vec![DataField::new(
            0,
            "f".to_string(),
            DataType::Float(crate::spec::FloatType::new()),
        )];
        // An integral FLOAT literal above 2^53 must be narrowed directly to f32
        // (Java `Long.floatValue()`), not via f64 (which would round twice).
        let json = rest_leaf_json_typed("EQUAL", "f", "FLOAT", "[9007199791611905]");
        let pred = Predicate::from_rest_json(&json, &float_fields).unwrap();
        let Predicate::Leaf { literals, .. } = &pred else {
            panic!("expected leaf");
        };
        let Datum::Float(v) = literals[0] else {
            panic!("expected float literal");
        };
        let single = 9_007_199_791_611_905_i64 as f32;
        let double_rounded = 9_007_199_791_611_905_i64 as f64 as f32;
        assert_eq!(v.to_bits(), single.to_bits());
        assert_ne!(v.to_bits(), double_rounded.to_bits());
    }

    #[test]
    fn test_from_rest_json_rejects_unknown_constructs() {
        let fields = test_fields();
        for json in [
            // Unknown leaf function.
            rest_leaf_json("SOME_UDF", "id", "[5]"),
            // Unknown field.
            rest_leaf_json("EQUAL", "missing", "[5]"),
            // Non-FIELD_REF transforms (verbatim shapes from Java serde tests).
            r#"{"kind":"LEAF","transform":{"name":"UPPER","inputs":[{"index":1,"name":"name","type":"STRING"}]},"function":"STARTS_WITH","literals":["A"]}"#.to_string(),
            r#"{"kind":"LEAF","transform":{"name":"CAST","fieldRef":{"index":0,"name":"id","type":"INT"},"type":"BIGINT"},"function":"GREATER_THAN","literals":[10]}"#.to_string(),
            // NULL transform with a field function (TRUE/FALSE is accepted).
            r#"{"kind":"LEAF","transform":{"name":"NULL"},"function":"EQUAL","literals":[5]}"#.to_string(),
            // A constant leaf must still carry a well-formed NULL transform;
            // accepting a malformed one as TRUE would be allow-all.
            r#"{"kind":"LEAF","function":"TRUE","literals":[]}"#.to_string(),
            r#"{"kind":"LEAF","transform":{"name":"BOGUS"},"function":"TRUE","literals":[]}"#.to_string(),
            r#"{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"id","type":"INT"}},"function":"TRUE","literals":[]}"#.to_string(),
            r#"{"kind":"LEAF","transform":{"name":"LOWER","inputs":[{"index":2,"name":"f2","type":"STRING"}]},"function":"STARTS_WITH","literals":["abc"]}"#.to_string(),
            r#"{"kind":"LEAF","transform":{"name":"CONCAT_WS","inputs":["|",{"index":1,"name":"f1","type":"STRING"},"X",null,{"index":2,"name":"f2","type":"STRING"}]},"function":"ENDS_WITH","literals":["z"]}"#.to_string(),
            // Unknown kind, malformed JSON, type-mismatched literal.
            r#"{"kind":"CUSTOM"}"#.to_string(),
            "not json".to_string(),
            rest_leaf_json("EQUAL", "id", "[\"text\"]"),
        ] {
            assert!(
                Predicate::from_rest_json(&json, &fields).is_err(),
                "must reject: {json}"
            );
        }
    }

    /// Wire strings below are taken verbatim from Java `TransformJsonSerdeTest`.
    #[test]
    fn test_transform_from_rest_json_matches_java_wire_format() {
        let fields = vec![
            DataField::new(0, "f0".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "f1".to_string(),
                DataType::VarChar(VarCharType::default()),
            ),
            DataField::new(
                2,
                "f2".to_string(),
                DataType::VarChar(VarCharType::default()),
            ),
        ];
        let cases: Vec<(&str, Transform)> = vec![
            (
                r#"{"name":"FIELD_REF","fieldRef":{"index":0,"name":"f0","type":"INT"}}"#,
                Transform::FieldRef(0),
            ),
            (
                r#"{"name":"CAST","fieldRef":{"index":0,"name":"f0","type":"INT"},"type":"BIGINT"}"#,
                Transform::Cast(0, DataType::BigInt(BigIntType::new())),
            ),
            (
                r#"{"name":"UPPER","inputs":[{"index":1,"name":"f1","type":"STRING"}]}"#,
                Transform::Upper(vec![TransformInput::Field(1)]),
            ),
            (
                r#"{"name":"CONCAT","inputs":[{"index":1,"name":"f1","type":"STRING"},"-",{"index":2,"name":"f2","type":"STRING"},null]}"#,
                Transform::Concat(vec![
                    TransformInput::Field(1),
                    TransformInput::Literal(Some("-".to_string())),
                    TransformInput::Field(2),
                    TransformInput::Literal(None),
                ]),
            ),
            (
                r#"{"name":"CONCAT_WS","inputs":["|",{"index":1,"name":"f1","type":"STRING"},"X",null,{"index":2,"name":"f2","type":"STRING"}]}"#,
                Transform::ConcatWs(vec![
                    TransformInput::Literal(Some("|".to_string())),
                    TransformInput::Field(1),
                    TransformInput::Literal(Some("X".to_string())),
                    TransformInput::Literal(None),
                    TransformInput::Field(2),
                ]),
            ),
            (
                r#"{"name":"LOWER","inputs":[{"index":1,"name":"f1","type":"STRING"}]}"#,
                Transform::Lower(vec![TransformInput::Field(1)]),
            ),
            (r#"{"name":"NULL"}"#, Transform::Null),
        ];
        for (json, expected) in cases {
            assert_eq!(
                Transform::from_rest_json(json, &fields).unwrap(),
                expected,
                "for {json}"
            );
        }

        // A CAST parses structurally (its applicability is checked when the
        // mask is applied, via arrow cast + output-type equality).
        assert!(Transform::from_rest_json(
            r#"{"name":"CAST","fieldRef":{"index":0,"name":"f0","type":"INT"},"type":"STRING"}"#,
            &fields
        )
        .is_ok());

        // Rejections: unknown transform name (verbatim from the Java test),
        // unknown field, non-string/-null/-fieldRef input.
        for json in [
            r#"{"name":"invalid"}"#,
            r#"{"name":"FIELD_REF","fieldRef":{"index":0,"name":"missing","type":"INT"}}"#,
            r#"{"name":"CONCAT","inputs":[42]}"#,
            r#"{"name":"CAST","fieldRef":{"index":0,"name":"f0","type":"INT"}}"#,
            // Java constructor rules: UPPER/LOWER take exactly one input,
            // CONCAT_WS at least two, field inputs must be string-typed.
            r#"{"name":"UPPER","inputs":[{"index":1,"name":"f1","type":"STRING"},{"index":2,"name":"f2","type":"STRING"}]}"#,
            r#"{"name":"CONCAT_WS","inputs":["|"]}"#,
            r#"{"name":"CONCAT","inputs":[{"index":0,"name":"f0","type":"INT"}]}"#,
        ] {
            assert!(
                Transform::from_rest_json(json, &fields).is_err(),
                "must reject: {json}"
            );
        }
    }

    #[test]
    fn test_transform_collect_field_indices() {
        let mut indices = std::collections::HashSet::new();
        Transform::Null.collect_field_indices(&mut indices);
        assert!(indices.is_empty());
        Transform::Cast(3, DataType::Int(IntType::new())).collect_field_indices(&mut indices);
        Transform::Concat(vec![
            TransformInput::Field(1),
            TransformInput::Literal(Some("-".to_string())),
            TransformInput::Field(2),
        ])
        .collect_field_indices(&mut indices);
        assert_eq!(indices, std::collections::HashSet::from([1, 2, 3]));
    }

    #[test]
    fn test_collect_leaf_field_indices() {
        let fields = test_fields();
        let pb = PredicateBuilder::new(&fields);
        let pred = Predicate::and(vec![
            pb.equal("id", Datum::Int(1)).unwrap(),
            Predicate::or(vec![
                pb.is_null("name").unwrap(),
                Predicate::Not(Box::new(pb.equal("hr", Datum::Int(2)).unwrap())),
            ]),
            Predicate::AlwaysTrue,
        ]);
        let mut indices = std::collections::HashSet::new();
        pred.collect_leaf_field_indices(&mut indices);
        assert_eq!(indices, std::collections::HashSet::from([0, 1, 3]));
    }
}
