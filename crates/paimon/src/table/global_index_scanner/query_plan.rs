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

//! Per-entry query plans and fallback-scan policy.

use super::entry::GlobalIndexFileKind;
use crate::spec::PredicateOperator;
use roaring::RoaringTreemap;

#[derive(Clone, Copy, Default)]
pub(super) struct FallbackScanPlan {
    pub(super) selected_btree: usize,
    pub(super) selected_bitmap: usize,
    pub(super) allow_btree: bool,
    pub(super) allow_bitmap: bool,
}

pub(super) struct EntryQueryPlan {
    pub(super) entry_idx: usize,
    pub(super) between_matches: bool,
    pub(super) between_evaluated: bool,
    pub(super) matching_predicates: Vec<usize>,
}

pub(super) struct EntryQueryResult {
    pub(super) bitmap: Option<RoaringTreemap>,
    pub(super) declined: bool,
}

impl FallbackScanPlan {
    pub(super) fn allowed(self, kind: GlobalIndexFileKind) -> bool {
        match kind {
            GlobalIndexFileKind::BTree => self.allow_btree,
            GlobalIndexFileKind::Bitmap | GlobalIndexFileKind::Multivalue => self.allow_bitmap,
            GlobalIndexFileKind::FM => true,
        }
    }
}

pub(super) fn requires_fallback_scan(op: PredicateOperator) -> bool {
    matches!(
        op,
        PredicateOperator::Lt
            | PredicateOperator::LtEq
            | PredicateOperator::Gt
            | PredicateOperator::GtEq
            | PredicateOperator::Between
            | PredicateOperator::NotBetween
            | PredicateOperator::EndsWith
            | PredicateOperator::Contains
            | PredicateOperator::Like
    )
}

pub(super) fn fallback_plan_evaluates_entry(
    plan: FallbackScanPlan,
    kind: GlobalIndexFileKind,
    selected: bool,
) -> bool {
    !selected || plan.allowed(kind)
}

pub(super) fn add_file_size(total: &mut i64, file_size: i64) -> bool {
    if file_size < 0 {
        return false;
    }
    match total.checked_add(file_size) {
        Some(next) => {
            *total = next;
            true
        }
        None => false,
    }
}
