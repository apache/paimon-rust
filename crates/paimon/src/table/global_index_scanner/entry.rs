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

//! Manifest entry parsing and file-level pruning metadata.

use crate::btree::key_serde::DynKeyComparator;
use crate::btree::BTreeIndexMeta;
use crate::spec::{DataType, Datum, PredicateOperator};
use crate::table::bitmap_global_index_format::is_bitmap_floating_residual_sensitive_op;
use crate::table::index_file_path::IndexFileLocation;
use crate::{Error, Result};
use std::collections::HashMap;

/// A resolved global index entry with parsed metadata.
pub(super) struct GlobalIndexEntry {
    pub(super) file_name: String,
    pub(super) index_type: GlobalIndexFileKind,
    pub(super) file_size: i64,
    pub(super) row_count: i64,
    pub(super) row_range_start: i64,
    pub(super) row_range_end: i64,
    pub(super) external_path: Option<String>,
    pub(super) meta: GlobalIndexEntryMeta,
}

impl GlobalIndexEntry {
    /// The entry's on-disk path: its external path if set, else the table's
    /// global index directory. Also the BTree reader-cache key.
    pub(super) fn resolved_path(&self, table_path: &str) -> String {
        IndexFileLocation::Global { table_path }
            .resolve(&self.file_name, self.external_path.as_deref())
    }
}

pub(super) fn sorted_entry_meta(entry: &GlobalIndexEntry) -> &BTreeIndexMeta {
    match &entry.meta {
        GlobalIndexEntryMeta::Sorted(meta) => meta,
        GlobalIndexEntryMeta::FM { .. } => unreachable!("FM entries do not have sorted metadata"),
    }
}

pub(super) enum GlobalIndexEntryMeta {
    Sorted(BTreeIndexMeta),
    FM {
        bytes: Vec<u8>,
        first_row_id: u64,
        row_count: u64,
    },
}

struct FMFileRowRange<'a> {
    file_name: &'a str,
    first_row_id: u64,
    row_count: u64,
}

pub(super) fn validate_fm_file_sets(
    entries_by_field: &HashMap<i32, Vec<GlobalIndexEntry>>,
) -> Result<()> {
    for (field_id, entries) in entries_by_field {
        let mut groups: HashMap<(i64, i64), Vec<FMFileRowRange<'_>>> = HashMap::new();
        for entry in entries {
            let GlobalIndexEntryMeta::FM {
                first_row_id,
                row_count,
                ..
            } = &entry.meta
            else {
                continue;
            };
            groups
                .entry((entry.row_range_start, entry.row_range_end))
                .or_default()
                .push(FMFileRowRange {
                    file_name: &entry.file_name,
                    first_row_id: *first_row_id,
                    row_count: *row_count,
                });
        }

        for ((range_start, range_end), mut files) in groups {
            let expected_row_count = range_end
                .checked_sub(range_start)
                .and_then(|count| count.checked_add(1))
                .and_then(|count| u64::try_from(count).ok())
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "Invalid FM global index source row range [{range_start}, {range_end}] for field {field_id}"
                    ),
                    source: None,
                })?;
            files.sort_unstable_by_key(|file| file.first_row_id);
            let mut expected_first_row_id = 0u64;
            for file in files {
                let FMFileRowRange {
                    file_name,
                    first_row_id,
                    row_count,
                } = file;
                if row_count == 0 || first_row_id != expected_first_row_id {
                    return Err(Error::DataInvalid {
                        message: format!(
                            "FM global index files do not exactly cover source row range [{range_start}, {range_end}] for field {field_id}: expected relative row {expected_first_row_id}, file '{file_name}' starts at {first_row_id}"
                        ),
                        source: None,
                    });
                }
                expected_first_row_id =
                    first_row_id
                        .checked_add(row_count)
                        .ok_or_else(|| Error::DataInvalid {
                            message: format!(
                                "FM global index row range overflows for file '{file_name}'"
                            ),
                            source: None,
                        })?;
                if expected_first_row_id > expected_row_count {
                    return Err(Error::DataInvalid {
                        message: format!(
                            "FM global index file '{file_name}' extends beyond source row range [{range_start}, {range_end}]"
                        ),
                        source: None,
                    });
                }
            }
            if expected_first_row_id != expected_row_count {
                return Err(Error::DataInvalid {
                    message: format!(
                        "FM global index files cover {expected_first_row_id} rows, expected {expected_row_count} for source row range [{range_start}, {range_end}] and field {field_id}"
                    ),
                    source: None,
                });
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GlobalIndexFileKind {
    BTree,
    Bitmap,
    Multivalue,
    FM,
}

pub(super) fn is_floating_point(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Float(_) | DataType::Double(_))
}

/// Java orders every NaN as one value above `+Inf`. BTree keys keep `total_cmp`
/// order, where a NaN with the sign bit set sorts below `-Inf` and each NaN
/// payload is its own key. Literals are canonicalized before querying, so only
/// a file holding NaN keys can disagree with Java: on ranges when its first key
/// is a NaN, and on NaN literals when either end is a NaN.
pub(super) fn btree_nan_order_sensitive(
    meta: &BTreeIndexMeta,
    op: PredicateOperator,
    literals: &[Datum],
    data_type: &DataType,
) -> bool {
    let is_nan = |key: &Option<Vec<u8>>| match (data_type, key.as_deref()) {
        (DataType::Float(_), Some(key)) => {
            <[u8; 4]>::try_from(key).is_ok_and(|bytes| f32::from_le_bytes(bytes).is_nan())
        }
        (DataType::Double(_), Some(key)) => {
            <[u8; 8]>::try_from(key).is_ok_and(|bytes| f64::from_le_bytes(bytes).is_nan())
        }
        _ => false,
    };
    let first_is_nan = is_nan(&meta.first_key);
    let range = matches!(
        op,
        PredicateOperator::Lt
            | PredicateOperator::LtEq
            | PredicateOperator::Gt
            | PredicateOperator::GtEq
    );
    (range && first_is_nan)
        || (literals.iter().any(is_nan_literal) && (first_is_nan || is_nan(&meta.last_key)))
}

pub(super) fn is_nan_literal(literal: &Datum) -> bool {
    match literal {
        Datum::Float(value) => value.is_nan(),
        Datum::Double(value) => value.is_nan(),
        _ => false,
    }
}

pub(super) fn bitmap_meta_may_match(
    meta: &BTreeIndexMeta,
    op: PredicateOperator,
    data_type: &DataType,
    serialized_literals: &[Vec<u8>],
    cmp: &DynKeyComparator<'_>,
) -> bool {
    if is_floating_point(data_type) && is_bitmap_floating_residual_sensitive_op(op) {
        !meta.only_nulls()
    } else {
        meta.may_match(op, serialized_literals, cmp)
    }
}

pub(super) fn bitmap_meta_may_match_between(
    meta: &BTreeIndexMeta,
    data_type: &DataType,
    from_key: &[u8],
    to_key: &[u8],
    cmp: &DynKeyComparator<'_>,
) -> bool {
    if is_floating_point(data_type)
        && is_bitmap_floating_residual_sensitive_op(PredicateOperator::Between)
    {
        !meta.only_nulls()
    } else {
        meta.may_match_between(from_key, to_key, cmp)
    }
}

pub(super) fn multivalue_meta_may_match(
    meta: &BTreeIndexMeta,
    op: PredicateOperator,
    serialized_literals: &[Vec<u8>],
    cmp: &DynKeyComparator<'_>,
) -> bool {
    match op {
        PredicateOperator::ArrayContains => {
            meta.may_match(PredicateOperator::Eq, serialized_literals, cmp)
        }
        PredicateOperator::ArraysOverlap => {
            meta.may_match(PredicateOperator::In, serialized_literals, cmp)
        }
        PredicateOperator::ArrayContainsAll => serialized_literals.iter().all(|literal| {
            meta.may_match(PredicateOperator::Eq, std::slice::from_ref(literal), cmp)
        }),
        _ => false,
    }
}

impl GlobalIndexFileKind {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::BTree => "BTree",
            Self::Bitmap => "bitmap",
            Self::Multivalue => "multivalue",
            Self::FM => "FM",
        }
    }
}
