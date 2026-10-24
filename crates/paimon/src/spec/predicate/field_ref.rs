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

//! Resolved predicate operands and Rust's name/reference argument adapter.

use super::Predicate;
use crate::spec::{DataField, DataType, Datum, PredicateOperator};
use std::borrow::Cow;

/// A schema-bound input to predicate comparisons, including a field inside a ROW.
///
/// Construct with [`super::PredicateBuilder::field`] or
/// [`super::PredicateBuilder::nested_field`], then reuse it with the ordinary
/// comparison, null, set, string and range builder methods. Like Java's
/// `NestedFieldTransform`, it describes the value independently of the operator.
/// Its immutable field identities are validated by the receiving builder.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldRef {
    pub(super) root: Box<DataField>,
    pub(super) index: usize,
    pub(super) path: Vec<DataField>,
}

impl FieldRef {
    /// Type of the selected value, rather than the enclosing ROW's type.
    pub fn data_type(&self) -> &DataType {
        self.path.last().unwrap_or(&self.root).data_type()
    }

    pub(super) fn predicate(&self, op: PredicateOperator, literals: Vec<Datum>) -> Predicate {
        if self.path.is_empty() {
            Predicate::Leaf {
                column: self.root.name().into(),
                index: self.index,
                data_type: self.root.data_type().clone(),
                op,
                literals,
            }
        } else {
            Predicate::NestedLeaf {
                root: self.root.clone(),
                index: self.index,
                path: self.path.clone(),
                op,
                literals,
            }
        }
    }
}

/// A column name or resolved [`FieldRef`] accepted by [`super::PredicateBuilder`].
///
/// Builder methods accept `impl Into<PredicateField>` so existing `&str` and
/// `String` names, and either owned or borrowed field references, share the same
/// operators and literal validation. Callers do not need to construct this
/// adapter explicitly.
pub struct PredicateField<'a>(pub(super) FieldInput<'a>);

pub(super) enum FieldInput<'a> {
    Name(Cow<'a, str>),
    Reference(Cow<'a, FieldRef>),
}

impl<'a> From<&'a str> for PredicateField<'a> {
    fn from(name: &'a str) -> Self {
        Self(FieldInput::Name(Cow::Borrowed(name)))
    }
}

impl<'a> From<&'a String> for PredicateField<'a> {
    fn from(name: &'a String) -> Self {
        Self::from(name.as_str())
    }
}

impl From<String> for PredicateField<'_> {
    fn from(name: String) -> Self {
        Self(FieldInput::Name(Cow::Owned(name)))
    }
}

impl<'a> From<&'a FieldRef> for PredicateField<'a> {
    fn from(field: &'a FieldRef) -> Self {
        Self(FieldInput::Reference(Cow::Borrowed(field)))
    }
}

impl From<FieldRef> for PredicateField<'_> {
    fn from(field: FieldRef) -> Self {
        Self(FieldInput::Reference(Cow::Owned(field)))
    }
}
