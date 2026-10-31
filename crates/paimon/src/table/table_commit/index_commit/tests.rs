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

use super::*;
use crate::spec::{
    GlobalIndexMeta, IndexFileMeta, PrimaryKeyIndexSourceFile, PrimaryKeyIndexSourceMeta,
};

fn entry(domain: IndexDomain, name: &str, field: i32, level: i32) -> IndexManifestEntry {
    let source_meta = match domain {
        IndexDomain::GlobalRowIds => None,
        IndexDomain::PrimaryKeyLevel => Some(
            PrimaryKeyIndexSourceMeta::new(
                level,
                vec![PrimaryKeyIndexSourceFile::new(format!("{name}.parquet"), 10).unwrap()],
            )
            .unwrap()
            .serialize()
            .unwrap(),
        ),
    };
    IndexManifestEntry {
        kind: FileKind::Add,
        partition: vec![],
        bucket: 0,
        index_file: IndexFileMeta {
            index_type: "btree".into(),
            file_name: name.into(),
            file_size: 17,
            row_count: 10,
            deletion_vectors_ranges: None,
            external_path: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: 0,
                row_range_end: 9,
                index_field_id: field,
                extra_field_ids: None,
                source_meta,
                index_meta: None,
            }),
        },
        version: 1,
    }
}

#[test]
fn exact_replacements_preserve_unrelated_indexes_for_both_layouts() {
    for domain in [IndexDomain::GlobalRowIds, IndexDomain::PrimaryKeyLevel] {
        let old = entry(domain, "old.index", 0, 1);
        let other = entry(domain, "other.index", 1, 1);
        let new = entry(domain, "new.index", 0, 1);
        let mut deleted = old.clone();
        deleted.kind = FileKind::Delete;
        let merged = TableCommit::merge_index_entries(
            vec![old, other.clone()],
            &[deleted, new.clone()],
            domain,
        )
        .unwrap();
        assert_eq!(merged, vec![other, new]);
    }
}

#[test]
fn retirements_check_current_metadata_and_membership_for_both_layouts() {
    for domain in [IndexDomain::GlobalRowIds, IndexDomain::PrimaryKeyLevel] {
        let old = entry(domain, "old.index", 0, 1);
        for mismatch in ["size", "source", "missing"] {
            let mut deleted = old.clone();
            deleted.kind = FileKind::Delete;
            match mismatch {
                "size" => deleted.index_file.file_size += 1,
                "source" => {
                    deleted
                        .index_file
                        .global_index_meta
                        .as_mut()
                        .unwrap()
                        .source_meta = Some(vec![0])
                }
                "missing" => deleted.index_file.file_name = "missing.index".into(),
                _ => unreachable!(),
            }
            let error = TableCommit::merge_index_entries(vec![old.clone()], &[deleted], domain)
                .expect_err("a stale index retirement must fail");
            let expected = if mismatch == "missing" {
                "does not exist"
            } else {
                "metadata does not match"
            };
            assert!(error.to_string().contains(expected), "{mismatch}: {error}");
        }
    }
}

#[test]
fn pk_level_ordinals_can_overlap_while_global_row_id_ranges_cannot() {
    let first = entry(IndexDomain::PrimaryKeyLevel, "level-1.index", 0, 1);
    let second = entry(IndexDomain::PrimaryKeyLevel, "level-2.index", 0, 2);
    let merged = TableCommit::merge_index_entries(
        vec![first.clone()],
        std::slice::from_ref(&second),
        IndexDomain::PrimaryKeyLevel,
    )
    .unwrap();
    assert_eq!(merged, vec![first, second]);
    let first = entry(IndexDomain::GlobalRowIds, "global-1.index", 0, 1);
    let second = entry(IndexDomain::GlobalRowIds, "global-2.index", 0, 2);
    for retained in [true, false] {
        let (previous, additions) = if retained {
            (vec![first.clone()], vec![second.clone()])
        } else {
            (vec![], vec![first.clone(), second.clone()])
        };
        let error =
            TableCommit::merge_index_entries(previous, &additions, IndexDomain::GlobalRowIds)
                .expect_err("global indexes cannot overlap within or across commits");
        assert!(
            error.to_string().contains("overlapping row range"),
            "{error}"
        );
    }
}
