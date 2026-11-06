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

//! Commit-time manifest merging, ported from Java `ManifestFileLegacyMerger`.
//!
//! In a merged group, a DELETE entry leaves the manifest set only together
//! with the ADD it cancels, so the merged manifests resolve to the same live
//! files as their input. Groups that cannot be merged this way are kept as
//! they are.

use super::*;
use crate::spec::avro::from_avro_bytes_fast;
use crate::spec::avro::ocf::parse_ocf_streaming;
use crate::spec::avro::schema::{FieldSchema, WriterSchema};
use futures::{Stream, StreamExt, TryStreamExt};
use std::sync::LazyLock;

/// Commit-time merge settings. `target_size` comes from the
/// `manifest_target_size` field; the other two are read from table options on
/// every merge.
struct MergeConfig {
    target_size: i64,
    min_count: usize,
    read_parallelism: usize,
}

impl TableCommit {
    /// Merge the previous snapshot's manifests before they become the new base list.
    ///
    /// New manifests are recorded in `created_files`. Input manifests are
    /// neither modified nor deleted.
    pub(super) async fn merge_manifests(
        &self,
        input: Vec<ManifestFileMeta>,
        created_files: &mut Vec<String>,
    ) -> Result<Vec<ManifestFileMeta>> {
        if input.is_empty() {
            return Ok(input);
        }
        let options = self.table.schema().core_options();
        let config = MergeConfig {
            target_size: self.manifest_target_size.max(1),
            min_count: options.manifest_merge_min_count()?,
            read_parallelism: options.scan_manifest_parallelism()?,
        };
        self.compact_minor(input, &config, created_files).await
    }

    /// Java `compactMinor`: merge consecutive manifests once their total size
    /// reaches the target, and the trailing group only with `min_count` files.
    async fn compact_minor(
        &self,
        input: Vec<ManifestFileMeta>,
        config: &MergeConfig,
        created_files: &mut Vec<String>,
    ) -> Result<Vec<ManifestFileMeta>> {
        let mut result = Vec::new();
        let mut candidates = Vec::new();
        let mut total_size = 0i64;
        for manifest in input {
            total_size += manifest.file_size();
            candidates.push(manifest);
            if total_size >= config.target_size {
                let group = std::mem::take(&mut candidates);
                result.extend(self.merge_candidates(group, config, created_files).await?);
                total_size = 0;
            }
        }
        if candidates.len() >= config.min_count {
            result.extend(
                self.merge_candidates(candidates, config, created_files)
                    .await?,
            );
        } else {
            result.extend(candidates);
        }
        Ok(result)
    }

    async fn merge_candidates(
        &self,
        candidates: Vec<ManifestFileMeta>,
        config: &MergeConfig,
        created_files: &mut Vec<String>,
    ) -> Result<Vec<ManifestFileMeta>> {
        if candidates.len() <= 1 {
            return Ok(candidates);
        }
        let manifest_dir = self.snapshot_manager.manifest_dir();
        let contents: Vec<ManifestContents> = read_manifests(
            self.snapshot_manager.file_io(),
            &manifest_dir,
            &candidates,
            config.read_parallelism,
        )
        .try_collect()
        .await?;
        let names = candidates
            .iter()
            .map(ManifestFileMeta::file_name)
            .collect::<Vec<_>>();
        if let Some(index) = contents.iter().position(|content| !content.rewritable) {
            log::warn!(
                "Keeping manifests {names:?} unmerged: {} has fields that a rewrite would drop",
                names[index]
            );
            return Ok(candidates);
        }
        let entries = contents.into_iter().flat_map(|content| content.entries);
        let Some(merged) = merge_group_entries(entries.collect()) else {
            log::warn!(
                "Keeping manifests {names:?} unmerged: they add a file that is already added or deleted among them"
            );
            return Ok(candidates);
        };
        self.write_merged_manifests(&merged, created_files).await
    }

    async fn write_merged_manifests(
        &self,
        entries: &[ManifestEntry],
        created_files: &mut Vec<String>,
    ) -> Result<Vec<ManifestFileMeta>> {
        let manifest_dir = self.snapshot_manager.manifest_dir();
        let prefix = format!("manifest-{}", uuid::Uuid::new_v4());
        self.write_manifest_files(
            self.snapshot_manager.file_io(),
            &manifest_dir,
            &prefix,
            entries,
            created_files,
        )
        .await
    }
}

/// The entries of one manifest, and whether rewriting them keeps every field.
struct ManifestContents {
    entries: Vec<ManifestEntry>,
    rewritable: bool,
}

/// Read manifests with bounded concurrency, yielding their contents in input order.
fn read_manifests(
    file_io: &FileIO,
    manifest_dir: &str,
    manifests: &[ManifestFileMeta],
    parallelism: usize,
) -> impl Stream<Item = Result<ManifestContents>> + Send + use<> {
    let file_io = file_io.clone();
    let paths = manifests
        .iter()
        .map(|manifest| format!("{manifest_dir}/{}", manifest.file_name()))
        .collect::<Vec<_>>();
    // Each read owns its inputs so the commit future stays `Send`.
    futures::stream::iter(paths)
        .map(move |path| {
            let file_io = file_io.clone();
            async move { read_manifest(&file_io, &path).await }
        })
        .buffered(parallelism.max(1))
}

async fn read_manifest(file_io: &FileIO, path: &str) -> Result<ManifestContents> {
    let bytes = file_io.new_input(path)?.read().await?;
    let (header, _) = parse_ocf_streaming(&bytes)?;
    let rewritable = fields_known(
        &WriterSchema::parse(&header.schema_json)?,
        &RUST_MANIFEST_SCHEMA,
    );
    let entries = from_avro_bytes_fast::<ManifestEntry>(&bytes)?;
    Ok(ManifestContents {
        entries,
        rewritable,
    })
}

/// The schema merged manifests are written with.
static RUST_MANIFEST_SCHEMA: LazyLock<WriterSchema> = LazyLock::new(|| {
    WriterSchema::parse(MANIFEST_ENTRY_SCHEMA).expect("MANIFEST_ENTRY_SCHEMA is a valid schema")
});

/// Whether every field of `writer`, at any depth, has a field of the same name
/// in `known`. The decoder skips other fields, so a rewrite would drop them.
fn fields_known(writer: &WriterSchema, known: &WriterSchema) -> bool {
    writer.fields.iter().all(|field| {
        known
            .fields
            .iter()
            .find(|known_field| known_field.name == field.name)
            .is_some_and(|known_field| schema_known(&field.schema, &known_field.schema))
    })
}

fn schema_known(writer: &FieldSchema, known: &FieldSchema) -> bool {
    match (writer, known) {
        (FieldSchema::Record(writer), FieldSchema::Record(known)) => fields_known(writer, known),
        (FieldSchema::Array(writer), FieldSchema::Array(known))
        | (FieldSchema::Map(writer), FieldSchema::Map(known)) => schema_known(writer, known),
        (FieldSchema::Record(_) | FieldSchema::Array(_) | FieldSchema::Map(_), _)
        | (_, FieldSchema::Record(_) | FieldSchema::Array(_) | FieldSchema::Map(_)) => false,
        _ => true,
    }
}

/// Merge one group of manifest entries. Well-formed groups match Java
/// `FileEntry.mergeEntries`, including the entry order: an ADD and a DELETE of
/// the same file cancel, and a DELETE whose ADD is outside the group is kept.
///
/// Returns `None` when the group adds a file that it already adds or deletes.
/// The caller keeps such a group unmerged.
fn merge_group_entries(entries: Vec<ManifestEntry>) -> Option<Vec<ManifestEntry>> {
    let identifiers = entries
        .iter()
        .map(ManifestEntry::identifier)
        .collect::<Vec<_>>();
    let mut added = HashSet::new();
    let mut deleted = HashSet::new();
    for (entry, identifier) in entries.iter().zip(&identifiers) {
        match entry.kind() {
            FileKind::Add => {
                if deleted.contains(identifier) || !added.insert(identifier) {
                    return None;
                }
            }
            FileKind::Delete => {
                deleted.insert(identifier);
            }
        }
    }
    let mut emitted_deletes = HashSet::new();
    let keep = entries
        .iter()
        .zip(&identifiers)
        .map(|(entry, identifier)| match entry.kind() {
            FileKind::Add => !deleted.contains(identifier),
            // A repeated DELETE removes nothing more; keep one.
            FileKind::Delete => !added.contains(identifier) && emitted_deletes.insert(identifier),
        })
        .collect::<Vec<_>>();
    Some(
        entries
            .into_iter()
            .zip(keep)
            .filter_map(|(entry, keep)| keep.then_some(entry))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
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
    fn add_and_delete_in_one_group_cancel() {
        let merged = merge_group_entries(vec![
            entry(FileKind::Add, "a", 0),
            entry(FileKind::Delete, "a", 0),
            entry(FileKind::Add, "b", 0),
        ])
        .unwrap();
        assert_eq!(keys(&merged), [(FileKind::Add, "b", 0)]);
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
        assert_eq!(keys(&merged), [(FileKind::Add, "f", 1)]);
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
    fn group_that_adds_a_file_again_is_not_merged() {
        use FileKind::{Add, Delete};
        for kinds in [vec![Add, Add], vec![Add, Delete, Add], vec![Delete, Add]] {
            let entries = kinds.iter().map(|kind| entry(*kind, "x", 0)).collect();
            assert!(merge_group_entries(entries).is_none(), "{kinds:?}");
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
        let contents = read_manifest(&file_io, path).await.unwrap();
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
}
