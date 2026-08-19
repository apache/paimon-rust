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

use crate::lumina::{is_lumina_index_type, LUMINA_IDENTIFIER};
use crate::vindex::{
    is_vindex_index_type, DISKANN_IDENTIFIER, IVF_FLAT_IDENTIFIER, IVF_PQ_IDENTIFIER,
    IVF_RQ_IDENTIFIER, IVF_SQ_IDENTIFIER,
};

pub(crate) const BTREE_GLOBAL_INDEX_TYPE: &str = "btree";
pub(crate) const BITMAP_GLOBAL_INDEX_TYPE: &str = "bitmap";
pub(crate) const MULTIVALUE_GLOBAL_INDEX_TYPE: &str = "multivalue";

pub(crate) fn normalize_sorted_global_index_type(index_type: &str) -> Option<&'static str> {
    if index_type.eq_ignore_ascii_case(BTREE_GLOBAL_INDEX_TYPE) {
        Some(BTREE_GLOBAL_INDEX_TYPE)
    } else if index_type.eq_ignore_ascii_case(BITMAP_GLOBAL_INDEX_TYPE) {
        Some(BITMAP_GLOBAL_INDEX_TYPE)
    } else if index_type.eq_ignore_ascii_case(MULTIVALUE_GLOBAL_INDEX_TYPE) {
        Some(MULTIVALUE_GLOBAL_INDEX_TYPE)
    } else {
        None
    }
}

/// Human-readable list of the global index types the Rust drop path accepts.
/// Used verbatim in the unsupported-type error of both the builder and the
/// DataFusion procedure so the two messages stay in sync.
pub const SUPPORTED_GLOBAL_INDEX_TYPES_FOR_DROP: &str =
    "btree, bitmap, multivalue, lumina, lumina-vector-ann, ivf-flat, ivf-pq, ivf-sq, ivf-rq, diskann";

/// Canonicalize any supported global index type to a stable `&'static str`, or
/// `None` if unsupported. Case-insensitive. Order: sorted -> lumina -> vindex.
///
/// Both lumina aliases (`lumina`, `lumina-vector-ann`) canonicalize to
/// `"lumina"`; each vindex type keeps its own identity so dropping one vindex
/// type never matches another on the same column. Callers should compare the
/// canonical form of BOTH the request and each stored entry's `index_type`.
pub fn normalize_global_index_type_for_drop(index_type: &str) -> Option<&'static str> {
    if let Some(sorted) = normalize_sorted_global_index_type(index_type) {
        return Some(sorted);
    }
    // is_lumina_index_type / is_vindex_index_type match case-sensitively, so
    // lowercase first (normalize_sorted_global_index_type is already case-insensitive).
    let lowered = index_type.to_ascii_lowercase();
    if is_lumina_index_type(&lowered) {
        return Some(LUMINA_IDENTIFIER);
    }
    if is_vindex_index_type(&lowered) {
        return canonical_vindex_identifier(&lowered);
    }
    None
}

// Map a lowercased vindex identifier to its &'static constant (never return the
// borrowed `lowered` — wrong lifetime and not canonical).
fn canonical_vindex_identifier(lowered: &str) -> Option<&'static str> {
    match lowered {
        IVF_FLAT_IDENTIFIER => Some(IVF_FLAT_IDENTIFIER),
        IVF_PQ_IDENTIFIER => Some(IVF_PQ_IDENTIFIER),
        IVF_SQ_IDENTIFIER => Some(IVF_SQ_IDENTIFIER),
        IVF_RQ_IDENTIFIER => Some(IVF_RQ_IDENTIFIER),
        DISKANN_IDENTIFIER => Some(DISKANN_IDENTIFIER),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorted_types_canonicalize_case_insensitively() {
        assert_eq!(normalize_global_index_type_for_drop("BTREE"), Some("btree"));
        assert_eq!(
            normalize_global_index_type_for_drop("Bitmap"),
            Some("bitmap")
        );
        assert_eq!(
            normalize_global_index_type_for_drop("MultiValue"),
            Some("multivalue")
        );
    }

    #[test]
    fn lumina_aliases_canonicalize_to_lumina() {
        assert_eq!(
            normalize_global_index_type_for_drop("lumina"),
            Some("lumina")
        );
        assert_eq!(
            normalize_global_index_type_for_drop("lumina-vector-ann"),
            Some("lumina")
        );
        assert_eq!(
            normalize_global_index_type_for_drop("LUMINA"),
            Some("lumina")
        );
    }

    #[test]
    fn vindex_types_keep_distinct_identity() {
        assert_eq!(
            normalize_global_index_type_for_drop("ivf-flat"),
            Some("ivf-flat")
        );
        assert_eq!(
            normalize_global_index_type_for_drop("IVF-PQ"),
            Some("ivf-pq")
        );
        assert_eq!(
            normalize_global_index_type_for_drop("IVF-SQ"),
            Some("ivf-sq")
        );
        assert_eq!(
            normalize_global_index_type_for_drop("IVF-RQ"),
            Some("ivf-rq")
        );
        assert_eq!(
            normalize_global_index_type_for_drop("DiskANN"),
            Some("diskann")
        );
    }

    #[test]
    fn unsupported_types_return_none() {
        assert_eq!(normalize_global_index_type_for_drop("full-text"), None);
        assert_eq!(normalize_global_index_type_for_drop("hash"), None);
        assert_eq!(normalize_global_index_type_for_drop("ivf-hnsw-flat"), None);
        assert_eq!(normalize_global_index_type_for_drop("ivf-hnsw-sq"), None);
        assert_eq!(normalize_global_index_type_for_drop(""), None);
    }
}
