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

//! Primary-key vector (bucket-local ANN) search kernel.
//!
//! Read-only bucket-local approximate-nearest-neighbour search over the
//! primary-key vector index.

pub(crate) mod ann;
pub(crate) mod bucket;
pub(crate) mod exact;
pub(crate) mod metric;
pub(crate) mod reader;
pub(crate) mod result;

/// Shared constructor for validation failures in this module (mirrors Java
/// `checkArgument` / `IllegalArgumentException`).
pub(crate) fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Per-file physical row ranges, matching Java `rowRangesByFile`.
/// Missing entries impose no restriction; an empty list excludes the file.
/// Non-empty lists contain sorted, merged, inclusive ranges. Engine-planned
/// ranges stay compact, and residual predicates produce the same representation.
pub(crate) type RowRangesByFile = std::collections::HashMap<String, Vec<crate::table::RowRange>>;

/// Test a physical position against sorted, non-overlapping ranges without
/// expanding them, as in Java's PK vector readers.
pub(crate) fn contains_row_position(ranges: &[crate::table::RowRange], position: i64) -> bool {
    ranges
        .binary_search_by(|range| {
            if position < range.from() {
                std::cmp::Ordering::Greater
            } else if position > range.to() {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .is_ok()
}
