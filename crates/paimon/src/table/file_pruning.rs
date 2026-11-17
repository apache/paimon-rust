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

//! Prepare conservative predicates for file statistics, keeping the original row filter intact.

use crate::spec::{DataField, DataFileMeta, DataType, Predicate, PredicateOperator};
use std::borrow::Cow;

pub(super) fn file_pruning_predicates<'p, 'f>(
    predicates: &'p [Predicate],
    field_source: impl Fn(usize) -> Option<(&'f DataFileMeta, &'f DataField)>,
) -> Cow<'p, [Predicate]> {
    let safe = |predicate: &Predicate| safe_for_file_bounds(predicate, &field_source);
    if predicates.iter().all(&safe) {
        return Cow::Borrowed(predicates);
    }

    // AND can drop individual conjuncts. Retain an OR or NOT only as a whole.
    fn retain_conjuncts(
        predicates: &[Predicate],
        safe: &impl Fn(&Predicate) -> bool,
        selected: &mut Vec<Predicate>,
    ) {
        for predicate in predicates {
            match predicate {
                Predicate::And(children) => retain_conjuncts(children, safe, selected),
                other if safe(other) => selected.push(other.clone()),
                _ => {}
            }
        }
    }
    let mut selected = Vec::new();
    retain_conjuncts(predicates, &safe, &mut selected);
    Cow::Owned(selected)
}

fn safe_for_file_bounds<'f>(
    predicate: &Predicate,
    field_source: &impl Fn(usize) -> Option<(&'f DataFileMeta, &'f DataField)>,
) -> bool {
    match predicate {
        Predicate::And(children) | Predicate::Or(children) => children
            .iter()
            .all(|p| safe_for_file_bounds(p, field_source)),
        Predicate::Not(inner) => safe_for_file_bounds(inner, field_source),
        Predicate::Leaf { index, op, .. }
            if !matches!(op, PredicateOperator::IsNull | PredicateOperator::IsNotNull) =>
        {
            // Java ORC bounds may include CHAR padding removed from decoded values.
            !field_source(*index).is_some_and(|(file, field)| {
                matches!(field.data_type(), DataType::Char(_)) && file.is_orc()
            })
        }
        _ => true,
    }
}
