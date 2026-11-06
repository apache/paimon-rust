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
//! with the ADD it cancels. When each file is added at most once across the
//! manifest list, as Java `FileEntry.mergeEntries` requires, the merged
//! manifests resolve to the same live files as their input. Groups that cannot
//! be merged this way are kept as they are.

use super::*;
use crate::spec::avro::from_avro_bytes_fast;
use crate::spec::avro::ocf::parse_ocf_streaming;
use crate::spec::avro::schema::{FieldSchema, WriterSchema};
use crate::spec::Identifier;
use futures::{Stream, StreamExt, TryStreamExt};
use indexmap::IndexMap;
use std::sync::LazyLock;

/// Commit-time merge settings. `target_size` comes from the
/// `manifest_target_size` field; the others are read from table options on
/// every merge.
struct MergeConfig {
    target_size: i64,
    full_compaction_threshold: i64,
    min_count: usize,
    read_parallelism: usize,
}

impl MergeConfig {
    /// Java `mustChange`: manifests with DELETE entries or below the target size.
    fn must_change(&self, manifest: &ManifestFileMeta) -> bool {
        manifest.num_deleted_files() > 0 || manifest.file_size() < self.target_size
    }
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
        let options = self.table.schema().core_options();
        if input.is_empty()
            || (options.write_only()? && options.manifest_merge_skip_on_write_only()?)
        {
            return Ok(input);
        }
        let config = MergeConfig {
            target_size: self.manifest_target_size.max(1),
            full_compaction_threshold: options.manifest_full_compaction_threshold_size()?,
            min_count: options.manifest_merge_min_count()?,
            read_parallelism: options.scan_manifest_parallelism()?,
        };
        if let Some(merged) = self
            .try_full_compaction(&input, &config, created_files)
            .await?
        {
            return Ok(merged);
        }
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

    /// Java `tryFullCompaction`: once the manifests that must change reach the
    /// threshold, rewrite them with every manifest holding a deleted file.
    async fn try_full_compaction(
        &self,
        input: &[ManifestFileMeta],
        config: &MergeConfig,
        created_files: &mut Vec<String>,
    ) -> Result<Option<Vec<ManifestFileMeta>>> {
        let (delta_size, delta_deletes) = input
            .iter()
            .filter(|manifest| config.must_change(manifest))
            .fold((0i64, 0i64), |(size, deletes), manifest| {
                (
                    size + manifest.file_size(),
                    deletes + manifest.num_deleted_files(),
                )
            });
        if delta_size < config.full_compaction_threshold {
            return Ok(None);
        }
        log::info!(
            "Start manifest full compaction: {} manifests, {delta_deletes} deleted files in {delta_size} bytes that must change",
            input.len()
        );
        let Some(deleted) = self.read_deleted_entries(input, config).await? else {
            return Ok(None);
        };
        let may_hold_deleted = self.deleted_partition_matcher(&deleted);
        let (mut result, to_be_merged): (Vec<_>, Vec<_>) = input
            .iter()
            .cloned()
            .partition(|manifest| !config.must_change(manifest) && !may_hold_deleted(manifest));
        if to_be_merged.len() <= 1 {
            return Ok(None);
        }
        let Some(entries) = self
            .rewrite_entries(&to_be_merged, deleted, config, &mut result)
            .await?
        else {
            return Ok(None);
        };
        result.extend(self.write_merged_manifests(&entries, created_files).await?);
        Ok(Some(result))
    }

    /// Every DELETE entry of the input, keyed by file identity in first-seen
    /// order, or `None` when a manifest holding DELETEs is not rewritable.
    async fn read_deleted_entries(
        &self,
        input: &[ManifestFileMeta],
        config: &MergeConfig,
    ) -> Result<Option<IndexMap<Identifier, ManifestEntry>>> {
        let with_deletes = input
            .iter()
            .filter(|manifest| manifest.num_deleted_files() > 0)
            .cloned()
            .collect::<Vec<_>>();
        let manifest_dir = self.snapshot_manager.manifest_dir();
        let mut contents = std::pin::pin!(read_manifests(
            self.snapshot_manager.file_io(),
            &manifest_dir,
            &with_deletes,
            config.read_parallelism,
        ));
        let mut deleted = IndexMap::new();
        while let Some(content) = contents.try_next().await? {
            // A manifest with DELETEs must change, so it would be rewritten.
            if !content.rewritable {
                return Ok(None);
            }
            for entry in content.entries {
                if *entry.kind() == FileKind::Delete {
                    deleted.entry(entry.identifier()).or_insert(entry);
                }
            }
        }
        Ok(Some(deleted))
    }

    /// Whether a manifest may hold the ADD of a deleted file. Like Java, only
    /// manifests whose partition stats exclude every deleted partition are
    /// skipped; any doubt reads the manifest.
    fn deleted_partition_matcher(
        &self,
        deleted: &IndexMap<Identifier, ManifestEntry>,
    ) -> impl Fn(&ManifestFileMeta) -> bool {
        let partition_fields = self.table.schema().partition_fields();
        let filter = if deleted.is_empty() || partition_fields.is_empty() {
            None
        } else {
            let partitions = deleted.keys().map(|id| id.partition.clone()).collect();
            match PartitionFilter::from_partition_set(partitions, &partition_fields) {
                Ok(filter) => Some(filter),
                Err(error) => {
                    log::warn!(
                        "Manifest full compaction reads every manifest: no filter for the deleted partitions: {error}"
                    );
                    None
                }
            }
        };
        let any_deleted = !deleted.is_empty();
        move |manifest: &ManifestFileMeta| {
            any_deleted
                && filter
                    .as_ref()
                    .is_none_or(|filter| filter.matches_manifest(manifest, &partition_fields))
        }
    }

    /// Read the manifests to merge and return the entries to rewrite. A manifest
    /// that neither must change nor holds a deleted ADD is kept in `result`.
    /// Returns `None`, before anything is written, when one of them is not
    /// rewritable, adds a deleted file a second time, or holds a DELETE that
    /// `read_deleted_entries` did not see.
    async fn rewrite_entries(
        &self,
        to_be_merged: &[ManifestFileMeta],
        deleted: IndexMap<Identifier, ManifestEntry>,
        config: &MergeConfig,
        result: &mut Vec<ManifestFileMeta>,
    ) -> Result<Option<Vec<ManifestEntry>>> {
        let manifest_dir = self.snapshot_manager.manifest_dir();
        let mut contents = std::pin::pin!(read_manifests(
            self.snapshot_manager.file_io(),
            &manifest_dir,
            to_be_merged,
            config.read_parallelism,
        ));
        let mut kept = Vec::new();
        let mut matched = HashSet::new();
        let mut rewritten = Vec::new();
        for manifest in to_be_merged {
            let content =
                contents
                    .try_next()
                    .await?
                    .ok_or_else(|| crate::Error::UnexpectedError {
                        message: format!("manifest '{}' was not read", manifest.file_name()),
                        source: None,
                    })?;
            let Some((require_change, adds)) =
                classify_entries(manifest, content, &deleted, &mut matched, config)
            else {
                return Ok(None);
            };
            if require_change {
                rewritten.extend(adds);
            } else {
                kept.push(manifest.clone());
            }
        }
        // Java drops every DELETE here and relies on partition pruning to find
        // the ADD it cancels. Keeping unmatched DELETEs makes a missed ADD harmless.
        rewritten.extend(
            deleted
                .into_iter()
                .filter(|(identifier, _)| !matched.contains(identifier))
                .map(|(_, entry)| entry),
        );
        result.extend(kept);
        Ok(Some(rewritten))
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
        let mut contents = std::pin::pin!(read_manifests(
            self.snapshot_manager.file_io(),
            &manifest_dir,
            &candidates,
            config.read_parallelism,
        ));
        let names = || {
            candidates
                .iter()
                .map(ManifestFileMeta::file_name)
                .collect::<Vec<_>>()
        };
        let mut entries = Vec::new();
        let mut read = 0;
        while let Some(manifest) = contents.try_next().await? {
            if !manifest.rewritable {
                log::warn!(
                    "Keeping manifests {:?} unmerged: {} has fields that a rewrite would drop",
                    names(),
                    candidates[read].file_name()
                );
                return Ok(candidates);
            }
            entries.extend(manifest.entries);
            read += 1;
        }
        let Some(merged) = merge_group_entries(entries) else {
            log::warn!(
                "Keeping manifests {:?} unmerged: they add a file that is already added or deleted among them",
                names()
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

/// Classify one manifest read by `rewrite_entries`: whether it must be
/// rewritten, and the ADDs a rewrite keeps. `None` aborts the full compaction.
fn classify_entries(
    manifest: &ManifestFileMeta,
    content: ManifestContents,
    deleted: &IndexMap<Identifier, ManifestEntry>,
    matched: &mut HashSet<Identifier>,
    config: &MergeConfig,
) -> Option<(bool, Vec<ManifestEntry>)> {
    if !content.rewritable {
        log::warn!(
            "Skipping manifest full compaction: {} has fields that a rewrite would drop",
            manifest.file_name()
        );
        return None;
    }
    let mut require_change = config.must_change(manifest);
    let mut adds = Vec::new();
    for entry in content.entries {
        let identifier = entry.identifier();
        match *entry.kind() {
            FileKind::Add if deleted.contains_key(&identifier) => {
                // [ADD x, DELETE x, ADD x] leaves x live for an ordered
                // reader; dropping every ADD of x would remove it.
                if !matched.insert(identifier) {
                    log::warn!(
                        "Skipping manifest full compaction: {} adds a deleted file that is already added",
                        manifest.file_name()
                    );
                    return None;
                }
                require_change = true;
            }
            FileKind::Add => adds.push(entry),
            // Only an understated `num_deleted_files` hides a DELETE
            // from `read_deleted_entries`; a rewrite would drop it.
            FileKind::Delete if !deleted.contains_key(&identifier) => {
                log::warn!(
                    "Skipping manifest full compaction: {} holds more DELETE entries than its metadata counts",
                    manifest.file_name()
                );
                return None;
            }
            FileKind::Delete => {}
        }
    }
    Some((require_change, adds))
}

/// The entries of one manifest, and whether rewriting them keeps every field.
/// `entries` is left empty when the manifest is not rewritable.
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
    let writer_schema = WriterSchema::parse(&header.schema_json)?;
    if !fields_known(&writer_schema, &RUST_MANIFEST_SCHEMA) {
        return Ok(ManifestContents {
            entries: vec![],
            rewritable: false,
        });
    }
    let entries = from_avro_bytes_fast::<ManifestEntry>(&bytes)?;
    Ok(ManifestContents {
        entries,
        rewritable: true,
    })
}

/// The schema merged manifests are written with.
static RUST_MANIFEST_SCHEMA: LazyLock<WriterSchema> = LazyLock::new(|| {
    WriterSchema::parse(MANIFEST_ENTRY_SCHEMA).expect("MANIFEST_ENTRY_SCHEMA is a valid schema")
});

/// Whether every field of `writer` has a field of the same name in `known`,
/// recursing through records, arrays, maps and union branches. The decoder
/// skips other fields, so a rewrite would drop them.
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
    use FieldSchema::{Array, Map, Record, Union};
    match (writer, known) {
        (Record(writer), Record(known)) => fields_known(writer, known),
        (Array(writer), Array(known)) | (Map(writer), Map(known)) => schema_known(writer, known),
        // The parser unwraps only `["null", T]` field types; array items such
        // as `_NULL_COUNTS` keep their union.
        (Union(writer), Union(known)) => {
            writer.len() == known.len()
                && writer
                    .iter()
                    .zip(known)
                    .all(|(writer, known)| schema_known(writer, known))
        }
        (Record(_) | Array(_) | Map(_) | Union(_), _)
        | (_, Record(_) | Array(_) | Map(_) | Union(_)) => false,
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
}
