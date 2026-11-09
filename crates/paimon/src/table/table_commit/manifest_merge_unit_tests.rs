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
use crate::spec::EMPTY_SERIALIZED_ROW;

fn file(name: &str, level: i32) -> DataFileMeta {
    DataFileMeta {
        file_name: name.to_string(),
        file_size: 100,
        row_count: 1,
        min_key: vec![],
        max_key: vec![],
        key_stats: BinaryTableStats::empty(),
        value_stats: BinaryTableStats::empty(),
        min_sequence_number: 0,
        max_sequence_number: 0,
        schema_id: 0,
        level,
        extra_files: vec![],
        creation_time: None,
        delete_row_count: None,
        embedded_index: None,
        file_source: None,
        value_stats_cols: None,
        external_path: None,
        first_row_id: None,
        write_cols: None,
        column_max_sequence_numbers: None,
    }
}

fn entry(kind: FileKind, name: &str, level: i32) -> ManifestEntry {
    ManifestEntry::new(kind, vec![], 0, 1, file(name, level), 2)
}

fn keys(entries: &[ManifestEntry]) -> Vec<(FileKind, &str, i32)> {
    entries
        .iter()
        .map(|e| (*e.kind(), e.file().file_name.as_str(), e.file().level))
        .collect()
}

#[test]
fn add_deleted_in_its_group_is_dropped_and_the_delete_kept() {
    let merged = merge_group_entries(vec![
        entry(FileKind::Add, "a", 0),
        entry(FileKind::Delete, "a", 0),
        entry(FileKind::Add, "b", 0),
    ])
    .unwrap();
    assert_eq!(
        keys(&merged),
        [(FileKind::Delete, "a", 0), (FileKind::Add, "b", 0)]
    );
}

#[test]
fn delete_whose_add_is_outside_the_group_is_kept() {
    // #899: the ADD of "old" lives in an earlier group that is not merged.
    let merged = merge_group_entries(vec![
        entry(FileKind::Delete, "old", 0),
        entry(FileKind::Add, "new", 0),
    ])
    .unwrap();
    assert_eq!(
        keys(&merged),
        [(FileKind::Delete, "old", 0), (FileKind::Add, "new", 0)]
    );
}

#[test]
fn level_is_part_of_file_identity() {
    // An in-place upgrade deletes f@0 and adds f@1 under the same name.
    let merged = merge_group_entries(vec![
        entry(FileKind::Add, "f", 0),
        entry(FileKind::Delete, "f", 0),
        entry(FileKind::Add, "f", 1),
    ])
    .unwrap();
    assert_eq!(
        keys(&merged),
        [(FileKind::Delete, "f", 0), (FileKind::Add, "f", 1)]
    );
}

#[test]
fn repeated_delete_is_kept_once() {
    let merged = merge_group_entries(vec![
        entry(FileKind::Delete, "x", 0),
        entry(FileKind::Delete, "x", 0),
    ])
    .unwrap();
    assert_eq!(keys(&merged), [(FileKind::Delete, "x", 0)]);
}

#[test]
fn repeated_delete_of_a_file_added_in_the_group_is_kept_once() {
    let merged = merge_group_entries(vec![
        entry(FileKind::Add, "x", 0),
        entry(FileKind::Delete, "x", 0),
        entry(FileKind::Delete, "x", 0),
    ])
    .unwrap();
    assert_eq!(keys(&merged), [(FileKind::Delete, "x", 0)]);
}

#[test]
fn group_that_adds_a_file_twice_without_deleting_it_is_not_merged() {
    let entries = vec![entry(FileKind::Add, "x", 0), entry(FileKind::Add, "x", 0)];
    assert!(merge_group_entries(entries).is_none());
}

#[test]
fn every_add_of_a_deleted_file_is_dropped_whatever_the_order() {
    use FileKind::{Add, Delete};
    for kinds in [
        vec![Add, Delete, Add],
        vec![Delete, Add],
        vec![Add, Add, Delete],
    ] {
        let entries = kinds.iter().map(|kind| entry(*kind, "x", 0)).collect();
        let merged = merge_group_entries(entries).expect("merged");
        assert_eq!(keys(&merged), [(Delete, "x", 0)], "{kinds:?}");
    }
}

/// Merging must use the scan's identity, `ManifestEntry::identifier()`:
/// entries that the scan tells apart must not cancel.
#[test]
fn entries_with_different_identity_do_not_cancel() {
    let added = || file("x", 0);
    let mut extra_files = added();
    extra_files.extra_files = vec!["x.index".into()];
    let mut external_path = added();
    external_path.external_path = Some("oss://bucket/x".into());
    let pairs = [
        (
            ManifestEntry::new(
                FileKind::Add,
                EMPTY_SERIALIZED_ROW.clone(),
                0,
                1,
                added(),
                2,
            ),
            ManifestEntry::new(FileKind::Delete, vec![], 0, 1, added(), 2),
        ),
        (
            ManifestEntry::new(FileKind::Add, vec![], 0, 1, added(), 2),
            ManifestEntry::new(FileKind::Delete, vec![], 0, 1, extra_files, 2),
        ),
        (
            ManifestEntry::new(FileKind::Add, vec![], 0, 1, added(), 2),
            ManifestEntry::new(FileKind::Delete, vec![], 0, 1, external_path, 2),
        ),
        (
            ManifestEntry::new(FileKind::Add, vec![], 0, 2, added(), 2),
            ManifestEntry::new(FileKind::Delete, vec![], 1, 2, added(), 2),
        ),
    ];
    for (add, delete) in pairs {
        let merged = merge_group_entries(vec![add.clone(), delete.clone()]).unwrap();
        assert_eq!(merged, [add, delete]);
    }
}

fn rewritable(schema_json: &str) -> bool {
    fields_known(
        &WriterSchema::parse(schema_json).unwrap(),
        &RUST_MANIFEST_SCHEMA,
    )
}

fn with_field_after(anchor: &str, field: &str) -> String {
    let schema = MANIFEST_ENTRY_SCHEMA.replacen(anchor, &format!("{anchor} {field},"), 1);
    assert_ne!(schema, MANIFEST_ENTRY_SCHEMA);
    schema
}

#[test]
fn rust_manifest_schema_is_rewritable() {
    assert!(rewritable(MANIFEST_ENTRY_SCHEMA));
}

#[tokio::test]
async fn java_written_manifest_with_fewer_fields_is_rewritable() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/manifest/manifest-8ded1f09-fcda-489e-9167-582ac0f9f846-0"
    );
    let file_io = FileIO::from_url("file://").unwrap().build().unwrap();
    let cache = SharedSchemaCache::new();
    let contents = read_manifest(&file_io, path, &cache, |bytes| {
        decode_entries(bytes, &cache, DecodeRows::All)
    })
    .await
    .unwrap();
    assert!(contents.rewritable);
    assert_eq!(contents.entries.len(), 2);
}

#[test]
fn manifest_with_an_unknown_file_field_is_not_rewritable() {
    let schema = with_field_after(
        r#"{"name": "_FILE_NAME", "type": "string"},"#,
        r#"{"name": "_NEW", "type": ["null", "long"], "default": null}"#,
    );
    assert!(!rewritable(&schema));
}

#[test]
fn manifest_with_an_unknown_field_in_a_nullable_record_is_not_rewritable() {
    // `_KEY_STATS` is written as `["null", {record}]`; its first
    // `_MIN_VALUES` is the one this anchor matches.
    let schema = with_field_after(
        r#"{"name": "_MIN_VALUES", "type": "bytes"},"#,
        r#"{"name": "_NEW", "type": ["null", "long"], "default": null}"#,
    );
    assert!(!rewritable(&schema));
}

#[test]
fn manifest_with_a_multi_branch_union_is_not_rewritable() {
    let schema = MANIFEST_ENTRY_SCHEMA.replacen(
        r#"{"name": "_FILE_NAME", "type": "string"}"#,
        r#"{"name": "_FILE_NAME", "type": ["null", "string", "bytes"]}"#,
        1,
    );
    assert_ne!(schema, MANIFEST_ENTRY_SCHEMA);
    assert!(!rewritable(&schema));
}

#[test]
fn manifest_with_an_unknown_top_level_field_is_not_rewritable() {
    let schema = with_field_after(
        r#"{"name": "_VERSION", "type": "int"},"#,
        r#"{"name": "_NEW", "type": "int"}"#,
    );
    assert!(!rewritable(&schema));
}

#[test]
fn record_against_a_non_record_is_not_rewritable() {
    let file_as_bytes = r#"{"type": "record", "name": "r", "fields": [
        {"name": "_FILE", "type": "bytes"}
    ]}"#;
    let kind_as_record = r#"{"type": "record", "name": "r", "fields": [
        {"name": "_KIND", "type": {"type": "record", "name": "k", "fields": []}}
    ]}"#;
    assert!(!rewritable(file_as_bytes));
    assert!(!rewritable(kind_as_record));
}
