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

/// Which physical rows of one data file a bucket search may read.
///
/// Mirrors Java `rowRangesByFile` (`PkVectorAnnSegmentSearcher.liveRowPositions`,
/// `PrimaryKeyVectorBucketSearch.search`), which is a three-state per file and
/// spells the third state as the absence of a map entry:
///
/// * **absent from [`FileRowSelections`]** — unrestricted, every row is readable.
///   Java records an entry only for a file its own pre-filter narrowed, so a
///   split that narrowed nothing carries an empty map and restricts nothing.
/// * [`Ranges`](Self::Ranges)/[`Positions`](Self::Positions) **empty** — excluded,
///   no row of the file is readable (Java's empty `List<Range>`).
/// * non-empty — restricted to what it lists.
///
/// The two non-absent variants differ only in where the restriction came from,
/// which decides its shape. A plan built from an engine's bucket split carries
/// interval [`Ranges`] straight off the wire and must never be expanded into
/// positions: the row counts bounding those intervals are untrusted, so
/// materializing one row per allowed position is unbounded work. A residual data
/// predicate produces [`Positions`], whose size is bounded by the rows its own
/// read actually returned.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FileRowSelection {
    /// Inclusive physical row ranges, sorted and non-overlapping. Empty excludes
    /// the file.
    Ranges(Vec<crate::table::RowRange>),
    /// Physical row positions. Empty excludes the file.
    Positions(roaring::RoaringTreemap),
}

/// Per-data-file row selections for one split. A file with no entry is
/// unrestricted; see [`FileRowSelection`].
pub(crate) type FileRowSelections = std::collections::HashMap<String, FileRowSelection>;

impl FileRowSelection {
    /// Whether the selection permits no row at all, which is how Java's empty
    /// `List<Range>` reads: the file is skipped rather than searched.
    pub(crate) fn is_excluded(&self) -> bool {
        match self {
            Self::Ranges(ranges) => ranges.is_empty(),
            Self::Positions(positions) => positions.is_empty(),
        }
    }

    /// Whether `position` is permitted. Ranges are binary-searched rather than
    /// expanded, mirroring Java `PkVectorAnnSegmentSearcher.contains`.
    pub(crate) fn contains(&self, position: u64) -> bool {
        match self {
            Self::Ranges(ranges) => {
                let position = match i64::try_from(position) {
                    Ok(position) => position,
                    Err(_) => return false,
                };
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
            Self::Positions(positions) => positions.contains(position),
        }
    }
}
