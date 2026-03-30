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
use crate::spec::data_file::BinaryRow;
use crate::spec::types::DataType;
use crate::spec::DataField;
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

fn datum_cmp(lhs: &Datum, rhs: &Datum) -> Option<Ordering> {
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

/// Match Java `CompareUtils.compare(byte[], byte[])`, which compares signed
/// bytes lexicographically.
fn java_bytes_cmp(a: &[u8], b: &[u8]) -> Ordering {
    for (&lhs, &rhs) in a.iter().zip(b.iter()) {
        let cmp = (lhs as i8).cmp(&(rhs as i8));
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    a.len().cmp(&b.len())
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
    pub(crate) fn remap_field_index(self, mapping: &[Option<usize>]) -> Option<Predicate> {
        match self {
            Predicate::Leaf {
                column,
                index,
                data_type,
                op,
                literals,
            } => {
                let new_index = (*mapping.get(index)?)?;
                Some(Predicate::Leaf {
                    column,
                    index: new_index,
                    data_type,
                    op,
                    literals,
                })
            }
            Predicate::And(children) => {
                let remapped: Option<Vec<_>> = children
                    .into_iter()
                    .map(|c| c.remap_field_index(mapping))
                    .collect();
                Some(Predicate::and(remapped?))
            }
            Predicate::Or(children) => {
                let remapped: Option<Vec<_>> = children
                    .into_iter()
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
}

impl PredicateBuilder {
    /// Create a new builder from schema fields. Infallible.
    pub fn new(fields: &[DataField]) -> Self {
        Self {
            field_names: fields.iter().map(|f| f.name().to_string()).collect(),
            field_types: fields.iter().map(|f| f.data_type().clone()).collect(),
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

    // -- internal --

    /// Resolve field name to index + type, validate literals, and build a leaf predicate.
    fn leaf(&self, field: &str, op: PredicateOperator, literals: Vec<Datum>) -> Result<Predicate> {
        let (index, data_type) = self.resolve_field(field)?;
        Self::validate_literal_count(op, &literals)?;
        for lit in &literals {
            validate_datum_matches_type(lit, &data_type)?;
        }
        Ok(Predicate::Leaf {
            column: field.to_string(),
            index,
            data_type,
            op,
            literals,
        })
    }

    /// Look up a field name, returning its (index, DataType) or an error.
    fn resolve_field(&self, field: &str) -> Result<(usize, DataType)> {
        self.field_names
            .iter()
            .position(|n| n == field)
            .map(|idx| (idx, self.field_types[idx].clone()))
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
    );
    if !ok {
        return Err(Error::ConfigInvalid {
            message: format!("Datum {datum} is incompatible with DataType {data_type:?}"),
        });
    }
    Ok(())
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
                // IsNull/IsNotNull are handled in the outer match above.
                PredicateOperator::IsNull | PredicateOperator::IsNotNull => unreachable!(),
            }
        }
    }
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

    // ======================== PredicateBuilder basics ========================

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
    fn test_datum_partial_ord_bytes_matches_java_signed_byte_order() {
        assert!(Datum::Bytes(vec![0xFF]) < Datum::Bytes(vec![0x00]));
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
}
