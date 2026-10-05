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

//! Left-prefix tuple intervals matching Java `CompositeBTreePredicate`.

use super::key_serde::normalize_key_literal;
use super::CompositeKeyCodec;
use crate::spec::{DataField, Datum, Predicate, PredicateOperator};
use crate::Result;
use std::cmp::Ordering;

pub(crate) const MAX_INTERVALS: usize = 256;
type Component = Option<Vec<u8>>;

pub(crate) struct CompositeBound {
    values: Vec<Component>,
    after: bool,
}

impl CompositeBound {
    fn new(values: Vec<Component>, after: bool) -> Self {
        Self { values, after }
    }

    pub(crate) fn compare_key(&self, key: &[u8], codec: &CompositeKeyCodec) -> Result<Ordering> {
        let mut cursor = codec.cursor(key)?;
        let mut order = Ordering::Equal;
        for (i, value) in self.values.iter().enumerate() {
            order = codec.compare_component(i, cursor.next_component()?, value.as_deref())?;
            if order != Ordering::Equal {
                break;
            }
        }
        cursor.finish()?;
        if order != Ordering::Equal {
            return Ok(order);
        }
        Ok(if self.after {
            Ordering::Less
        } else {
            Ordering::Greater
        })
    }

    fn compare(&self, other: &Self, codec: &CompositeKeyCodec) -> Result<Ordering> {
        for i in 0..self.values.len().min(other.values.len()) {
            let order = codec.compare_component(
                i,
                self.values[i].as_deref(),
                other.values[i].as_deref(),
            )?;
            if order != Ordering::Equal {
                return Ok(order);
            }
        }
        Ok(match self.values.len().cmp(&other.values.len()) {
            Ordering::Less => {
                if self.after {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            Ordering::Greater => {
                if other.after {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            Ordering::Equal => self.after.cmp(&other.after),
        })
    }
}

pub(crate) struct CompositeInterval {
    pub(crate) lower: CompositeBound,
    pub(crate) upper: CompositeBound,
    pub(crate) point_key: Option<Vec<u8>>,
}

pub(crate) struct CompositePlan {
    pub(crate) fields: Vec<DataField>,
    pub(crate) codec: CompositeKeyCodec,
    pub(crate) intervals: Vec<CompositeInterval>,
    pub(crate) bound_columns: usize,
    pub(crate) equal_columns: usize,
}

impl CompositePlan {
    pub(crate) fn is_point_lookup(&self) -> bool {
        self.equal_columns == self.fields.len()
    }

    pub(crate) fn plan(fields: &[DataField], predicate: &Predicate) -> Result<Option<Self>> {
        let codec = CompositeKeyCodec::new(fields);
        let leaves = predicate
            .clone()
            .split_and()
            .into_iter()
            .filter_map(as_leaf)
            .collect::<Vec<_>>();
        let mut prefixes: Vec<Vec<Option<Datum>>> = vec![Vec::new()];
        let mut equal_columns = 0;
        let mut range = Vec::new();
        for (i, field) in fields.iter().enumerate() {
            let mut column = leaves
                .iter()
                .filter(|leaf| {
                    let Predicate::Leaf {
                        column,
                        data_type,
                        op,
                        ..
                    } = leaf
                    else {
                        unreachable!()
                    };
                    column == field.name()
                        && data_type.equals_ignore_nullable(field.data_type())
                        && supported(*op)
                })
                .cloned()
                .collect::<Vec<_>>();
            for leaf in &mut column {
                let Predicate::Leaf { literals, .. } = leaf else {
                    unreachable!()
                };
                for literal in literals {
                    if !normalize_key_literal(literal, field.data_type()) {
                        return Ok(None);
                    }
                }
            }
            let selected = column
                .iter()
                .filter(|leaf| point(operator(leaf)))
                .min_by_key(|leaf| match leaf {
                    Predicate::Leaf { literals, op, .. } => {
                        if *op == PredicateOperator::IsNull {
                            0
                        } else {
                            literals.len()
                        }
                    }
                    _ => unreachable!(),
                });
            let Some(selected) = selected else {
                range = column;
                break;
            };
            let Predicate::Leaf { op, literals, .. } = selected else {
                unreachable!()
            };
            let candidates = if *op == PredicateOperator::IsNull {
                if field.data_type().is_nullable() {
                    vec![None]
                } else {
                    Vec::new()
                }
            } else {
                literals.iter().cloned().map(Some).collect()
            };
            let mut domain: Vec<Option<Datum>> = Vec::new();
            for candidate in candidates {
                let encoded = candidate.as_ref().map(|v| codec.serialize_component(i, v));
                let mut matches = true;
                for leaf in &column {
                    if !matches_leaf(&codec, i, encoded.as_deref(), leaf)? {
                        matches = false;
                        break;
                    }
                }
                if matches {
                    let mut duplicate = false;
                    for value in &domain {
                        let value = value.as_ref().map(|v| codec.serialize_component(i, v));
                        if codec.compare_component(i, encoded.as_deref(), value.as_deref())?
                            == Ordering::Equal
                        {
                            duplicate = true;
                            break;
                        }
                    }
                    if !duplicate {
                        domain.push(candidate);
                    }
                    if domain.len() > MAX_INTERVALS {
                        break;
                    }
                }
            }
            equal_columns += 1;
            if domain.is_empty() {
                return Ok(Some(Self {
                    fields: fields.to_vec(),
                    codec,
                    intervals: Vec::new(),
                    bound_columns: equal_columns,
                    equal_columns,
                }));
            }
            if prefixes.len() * domain.len() > MAX_INTERVALS {
                return Ok(None);
            }
            let mut expanded = Vec::new();
            for prefix in prefixes {
                for value in &domain {
                    let mut next = prefix.clone();
                    next.push(value.clone());
                    expanded.push(next);
                }
            }
            prefixes = expanded;
        }
        let bound_columns = equal_columns + usize::from(!range.is_empty());
        if bound_columns == 0 {
            return Ok(None);
        }
        let mut intervals = Vec::new();
        for prefix in prefixes {
            let values = prefix
                .iter()
                .enumerate()
                .map(|(i, v)| v.as_ref().map(|v| codec.serialize_component(i, v)))
                .collect::<Vec<_>>();
            let mut lower = CompositeBound::new(values.clone(), false);
            let mut upper = CompositeBound::new(values.clone(), true);
            if !range.is_empty() {
                let mut null_prefix = values.clone();
                null_prefix.push(None);
                lower = CompositeBound::new(null_prefix, true);
                for leaf in &range {
                    let Predicate::Leaf { op, literals, .. } = leaf else {
                        unreachable!()
                    };
                    if *op == PredicateOperator::IsNotNull {
                        continue;
                    }
                    for (value, after, is_lower) in match op {
                        PredicateOperator::Gt | PredicateOperator::GtEq => {
                            vec![(&literals[0], *op == PredicateOperator::Gt, true)]
                        }
                        PredicateOperator::Lt | PredicateOperator::LtEq => {
                            vec![(&literals[0], *op == PredicateOperator::LtEq, false)]
                        }
                        PredicateOperator::Between => {
                            vec![(&literals[0], false, true), (&literals[1], true, false)]
                        }
                        _ => unreachable!(),
                    } {
                        let mut next = values.clone();
                        next.push(Some(codec.serialize_component(equal_columns, value)));
                        let next = CompositeBound::new(next, after);
                        if is_lower && next.compare(&lower, &codec)? == Ordering::Greater {
                            lower = next;
                        } else if !is_lower && next.compare(&upper, &codec)? == Ordering::Less {
                            upper = next;
                        }
                    }
                }
            }
            if lower.compare(&upper, &codec)? == Ordering::Less {
                let point_key = if equal_columns == fields.len() {
                    Some(codec.serialize(&prefix)?)
                } else {
                    None
                };
                intervals.push(CompositeInterval {
                    lower,
                    upper,
                    point_key,
                });
            }
        }
        Ok(Some(Self {
            fields: fields.to_vec(),
            codec,
            intervals,
            bound_columns,
            equal_columns,
        }))
    }

    pub(crate) fn may_match(&self, first: Option<&[u8]>, last: Option<&[u8]>) -> Result<bool> {
        if self.intervals.is_empty() {
            return Ok(false);
        }
        let (Some(first), Some(last)) = (first, last) else {
            return Ok(true);
        };
        for interval in &self.intervals {
            if interval.lower.compare_key(last, &self.codec)? == Ordering::Greater
                && interval.upper.compare_key(first, &self.codec)? == Ordering::Less
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn operator(leaf: &Predicate) -> PredicateOperator {
    let Predicate::Leaf { op, .. } = leaf else {
        unreachable!()
    };
    *op
}

fn point(op: PredicateOperator) -> bool {
    matches!(
        op,
        PredicateOperator::Eq | PredicateOperator::In | PredicateOperator::IsNull
    )
}
fn supported(op: PredicateOperator) -> bool {
    point(op)
        || matches!(
            op,
            PredicateOperator::Gt
                | PredicateOperator::GtEq
                | PredicateOperator::Lt
                | PredicateOperator::LtEq
                | PredicateOperator::Between
                | PredicateOperator::IsNotNull
        )
}

fn as_leaf(predicate: Predicate) -> Option<Predicate> {
    match predicate {
        Predicate::Leaf {
            op,
            ref literals,
            ref data_type,
            ..
        } if supported(op) => {
            let valid = match op {
                PredicateOperator::Eq
                | PredicateOperator::Gt
                | PredicateOperator::GtEq
                | PredicateOperator::Lt
                | PredicateOperator::LtEq => literals.len() == 1,
                PredicateOperator::Between => literals.len() == 2,
                _ => true,
            };
            (valid
                && literals.iter().all(|literal| {
                    crate::spec::validate_datum_matches_type(literal, data_type).is_ok()
                }))
            .then_some(predicate)
        }
        Predicate::Or(children) => {
            let mut first: Option<Predicate> = None;
            let mut values = Vec::new();
            for child in children {
                let leaf = as_leaf(child)?;
                let Predicate::Leaf {
                    ref column,
                    ref data_type,
                    ref literals,
                    op,
                    ..
                } = leaf
                else {
                    return None;
                };
                if !matches!(op, PredicateOperator::Eq | PredicateOperator::In) {
                    return None;
                }
                if let Some(Predicate::Leaf {
                    column: name,
                    data_type: ty,
                    ..
                }) = &first
                {
                    if name != column || ty != data_type {
                        return None;
                    }
                }
                values.extend(literals.iter().cloned());
                first.get_or_insert(leaf);
            }
            let Predicate::Leaf {
                column,
                index,
                data_type,
                ..
            } = first?
            else {
                return None;
            };
            Some(Predicate::Leaf {
                column,
                index,
                data_type,
                op: PredicateOperator::In,
                literals: values,
            })
        }
        _ => None,
    }
}

fn matches_leaf(
    codec: &CompositeKeyCodec,
    i: usize,
    value: Option<&[u8]>,
    leaf: &Predicate,
) -> Result<bool> {
    let Predicate::Leaf { op, literals, .. } = leaf else {
        unreachable!()
    };
    if *op == PredicateOperator::IsNull {
        return Ok(value.is_none());
    }
    if value.is_none() {
        return Ok(false);
    }
    if *op == PredicateOperator::IsNotNull {
        return Ok(true);
    }
    let compare = |literal: &Datum| {
        codec.compare_component(i, value, Some(&codec.serialize_component(i, literal)))
    };
    Ok(match op {
        PredicateOperator::Eq => compare(&literals[0])? == Ordering::Equal,
        PredicateOperator::In => {
            let mut found = false;
            for literal in literals {
                found |= compare(literal)? == Ordering::Equal;
            }
            found
        }
        PredicateOperator::Gt => compare(&literals[0])? == Ordering::Greater,
        PredicateOperator::GtEq => compare(&literals[0])? != Ordering::Less,
        PredicateOperator::Lt => compare(&literals[0])? == Ordering::Less,
        PredicateOperator::LtEq => compare(&literals[0])? != Ordering::Greater,
        PredicateOperator::Between => {
            compare(&literals[0])? != Ordering::Less && compare(&literals[1])? != Ordering::Greater
        }
        _ => unreachable!(),
    })
}
