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
//! `merge_group_entries` and `try_full_compaction` describe the rules.

use super::*;
use crate::spec::avro::ocf::parse_ocf_streaming;
use crate::spec::avro::schema::{FieldSchema, WriterSchema};
use crate::spec::avro::{
    from_manifest_bytes_filtered_shared, visit_slim_manifest_entries, SharedSchemaCache,
};
use crate::spec::Identifier;
use futures::{Stream, StreamExt, TryStreamExt};
use std::sync::LazyLock;

/// Commit-time merge settings, and the schema cache shared by one merge's
/// reads. `target_size` comes from the `manifest_target_size` field; the
/// others are read from table options on every merge.
struct MergeConfig {
    target_size: i64,
    full_compaction_threshold: i64,
    min_count: usize,
    read_parallelism: usize,
    schema_cache: SharedSchemaCache,
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
            schema_cache: SharedSchemaCache::new(),
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
    /// threshold, rewrite them together with every manifest that holds the ADD
    /// of a deleted file, dropping every DELETE entry and those ADDs. Like
    /// Java, it finds those manifests through partition statistics; a table
    /// with a FLOAT or DOUBLE partition key reads every manifest instead. When
    /// the statistics are correct, scans see the same live files afterwards.
    /// The ordered reader stops listing a deleted file with an ADD after its
    /// last DELETE; scans already treat that file as deleted.
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
            .rewrite_entries(&to_be_merged, &deleted, config, &mut result)
            .await?
        else {
            return Ok(None);
        };
        result.extend(self.write_merged_manifests(&entries, created_files).await?);
        Ok(Some(result))
    }

    /// The identities of the files deleted by every manifest whose
    /// `num_deleted_files` is positive, or `None` when one of those manifests
    /// is not rewritable.
    async fn read_deleted_entries(
        &self,
        input: &[ManifestFileMeta],
        config: &MergeConfig,
    ) -> Result<Option<HashSet<Identifier>>> {
        let with_deletes = input
            .iter()
            .filter(|manifest| manifest.num_deleted_files() > 0)
            .cloned()
            .collect::<Vec<_>>();
        let mut contents = std::pin::pin!(self.read_manifests(
            &with_deletes,
            config,
            |_, bytes, cache| decode_deleted_identifiers(bytes, cache)
        ));
        let mut deleted = HashSet::new();
        while let Some((manifest, content)) = contents.try_next().await? {
            // A manifest with DELETEs must change, so it would be rewritten.
            if !content.rewritable {
                warn_full_compaction_skipped(&manifest);
                return Ok(None);
            }
            deleted.extend(content.entries);
        }
        Ok(Some(deleted))
    }

    /// Whether a manifest may hold the ADD of a deleted file. Like Java, only
    /// manifests whose partition stats exclude every deleted partition are
    /// skipped; any doubt reads the manifest.
    fn deleted_partition_matcher(
        &self,
        deleted: &HashSet<Identifier>,
    ) -> impl Fn(&ManifestFileMeta) -> bool {
        let partition_fields = self.table.schema().partition_fields();
        // The filter and the manifest statistics order NaN differently, and Rust
        // before #1190 and pypaimon write order-dependent float statistics.
        // Pruning could skip the ADD of a deleted file and bring that file back.
        let float_key = partition_fields
            .iter()
            .any(|field| matches!(field.data_type(), DataType::Float(_) | DataType::Double(_)));
        let filter = if deleted.is_empty() || partition_fields.is_empty() || float_key {
            None
        } else {
            let partitions = deleted.iter().map(|id| id.partition.clone()).collect();
            match PartitionFilter::from_partition_set(partitions, &partition_fields) {
                Ok(filter) => Some(filter),
                // The filter only prunes; reading every manifest finds a superset.
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

    /// Read the manifests to merge and return the ADDs to rewrite; no DELETE
    /// entry is rewritten. A manifest that neither must change nor holds a
    /// deleted ADD is kept in `result`. Returns `None`, before anything is
    /// written, when one of them is not rewritable or holds a DELETE that
    /// `read_deleted_entries` did not see.
    async fn rewrite_entries(
        &self,
        to_be_merged: &[ManifestFileMeta],
        deleted: &HashSet<Identifier>,
        config: &MergeConfig,
        result: &mut Vec<ManifestFileMeta>,
    ) -> Result<Option<Vec<ManifestEntry>>> {
        let mut contents =
            std::pin::pin!(
                self.read_manifests(to_be_merged, config, |manifest, bytes, cache| {
                    // `read_deleted_entries` read every DELETE of a manifest whose
                    // metadata counts any. Other manifests are decoded in full so
                    // that `classify_entries` sees an uncounted DELETE.
                    let rows = if manifest.num_deleted_files() > 0 {
                        DecodeRows::SkipDeletes
                    } else {
                        DecodeRows::All
                    };
                    decode_entries(bytes, cache, rows)
                })
            );
        let mut kept = Vec::new();
        let mut rewritten = Vec::new();
        while let Some((manifest, content)) = contents.try_next().await? {
            let Some((require_change, adds)) =
                classify_entries(&manifest, content, deleted, config)
            else {
                return Ok(None);
            };
            if require_change {
                rewritten.extend(adds);
            } else {
                kept.push(manifest);
            }
        }
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
        let mut contents =
            std::pin::pin!(self.read_manifests(&candidates, config, |_, bytes, cache| {
                decode_entries(bytes, cache, DecodeRows::All)
            }));
        let names = || {
            candidates
                .iter()
                .map(ManifestFileMeta::file_name)
                .collect::<Vec<_>>()
        };
        let mut entries = Vec::new();
        while let Some((manifest, content)) = contents.try_next().await? {
            if !content.rewritable {
                log::warn!(
                    "Keeping manifests {:?} unmerged: {} has fields that a rewrite would drop",
                    names(),
                    manifest.file_name()
                );
                return Ok(candidates);
            }
            entries.extend(content.entries);
        }
        let Some(merged) = merge_group_entries(entries) else {
            log::warn!(
                "Keeping manifests {:?} unmerged: they add a file twice without deleting it",
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

    /// Read manifests with bounded concurrency, yielding each one with its
    /// contents in input order.
    fn read_manifests<T: Default + Send + 'static>(
        &self,
        manifests: &[ManifestFileMeta],
        config: &MergeConfig,
        decode: Decode<T>,
    ) -> impl Stream<Item = Result<(ManifestFileMeta, ManifestContents<T>)>> + Send + use<T> {
        let file_io = self.snapshot_manager.file_io().clone();
        let manifest_dir = self.snapshot_manager.manifest_dir();
        let cache = config.schema_cache.clone();
        // Each read owns its inputs so the commit future stays `Send`.
        futures::stream::iter(manifests.to_vec())
            .map(move |manifest| {
                let (file_io, cache) = (file_io.clone(), cache.clone());
                let path = format!("{manifest_dir}/{}", manifest.file_name());
                async move {
                    let contents = read_manifest(&file_io, &path, &cache, |bytes| {
                        decode(&manifest, bytes, &cache)
                    })
                    .await?;
                    Ok((manifest, contents))
                }
            })
            .buffered(config.read_parallelism.max(1))
    }
}

fn warn_full_compaction_skipped(manifest: &ManifestFileMeta) {
    log::warn!(
        "Skipping manifest full compaction: {} has fields that a rewrite would drop",
        manifest.file_name()
    );
}

/// Classify one manifest read by `rewrite_entries`: whether it must be
/// rewritten, and the ADDs a rewrite keeps. `None` aborts the full compaction.
fn classify_entries(
    manifest: &ManifestFileMeta,
    content: ManifestContents<Vec<ManifestEntry>>,
    deleted: &HashSet<Identifier>,
    config: &MergeConfig,
) -> Option<(bool, Vec<ManifestEntry>)> {
    if !content.rewritable {
        warn_full_compaction_skipped(manifest);
        return None;
    }
    let mut require_change = config.must_change(manifest);
    let mut adds = Vec::new();
    for entry in content.entries {
        match *entry.kind() {
            // Every ADD of a deleted file is dropped, including one that
            // follows its DELETE.
            FileKind::Add if !deleted.is_empty() && deleted.contains(&entry.identifier()) => {
                require_change = true
            }
            FileKind::Add => adds.push(entry),
            // Only an understated `num_deleted_files` hides a DELETE from
            // `read_deleted_entries`; a rewrite would drop it but keep its
            // ADD. This check sees only the manifests read here.
            FileKind::Delete if !deleted.contains(&entry.identifier()) => {
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

/// The decoded entries of one manifest, and whether rewriting them keeps every
/// field. `entries` is left empty when the manifest is not rewritable.
struct ManifestContents<T> {
    entries: T,
    rewritable: bool,
}

/// Decodes the bytes of one rewritable manifest for a read.
type Decode<T> = fn(&ManifestFileMeta, &[u8], &SharedSchemaCache) -> Result<T>;

async fn read_manifest<T: Default>(
    file_io: &FileIO,
    path: &str,
    cache: &SharedSchemaCache,
    decode: impl FnOnce(&[u8]) -> Result<T>,
) -> Result<ManifestContents<T>> {
    let bytes = file_io.new_input(path)?.read().await?;
    let (header, _) = parse_ocf_streaming(&bytes)?;
    let writer_schema = cache.get_or_parse(&header.schema_json)?;
    if !fields_known(&writer_schema, &RUST_MANIFEST_SCHEMA) {
        return Ok(ManifestContents {
            entries: T::default(),
            rewritable: false,
        });
    }
    Ok(ManifestContents {
        entries: decode(&bytes)?,
        rewritable: true,
    })
}

/// Which rows `decode_entries` decodes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DecodeRows {
    All,
    /// Skips DELETE rows when the writer schema puts `_KIND`, `_PARTITION`,
    /// `_BUCKET` and `_TOTAL_BUCKETS` before `_FILE`; otherwise they are
    /// still decoded.
    SkipDeletes,
}

fn decode_entries(
    bytes: &[u8],
    cache: &SharedSchemaCache,
    rows: DecodeRows,
) -> Result<Vec<ManifestEntry>> {
    from_manifest_bytes_filtered_shared(bytes, cache, &mut |kind, _, _, _| {
        rows == DecodeRows::All || kind == FileKind::Add
    })
}

/// The identities of the DELETE entries in one manifest.
fn decode_deleted_identifiers(bytes: &[u8], cache: &SharedSchemaCache) -> Result<Vec<Identifier>> {
    let mut deleted = Vec::new();
    visit_slim_manifest_entries(bytes, cache, &mut |entry| {
        if entry.kind == FileKind::Delete {
            deleted.push(Identifier {
                partition: entry.partition.to_vec(),
                bucket: entry.bucket,
                level: entry.level,
                file_name: entry.file_name.to_string(),
                extra_files: entry
                    .extra_files
                    .iter()
                    .map(|file| file.to_string())
                    .collect(),
                embedded_index: entry.embedded_index.map(<[u8]>::to_vec),
                external_path: entry.external_path.map(str::to_string),
            });
        }
        Ok(())
    })?;
    Ok(deleted)
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

/// Merge one group of manifest entries, keeping their order. Every ADD of a
/// file that the group deletes is dropped, wherever it is, and each deleted
/// file keeps one DELETE. For an ADD before its DELETE, Java
/// `FileEntry.mergeEntries` drops that DELETE too, but then an ADD of the same
/// file outside the group would become live. For an ADD after its DELETE, Java
/// throws. The set of deleted files does not change, so the scan's
/// order-independent result stays the same for any group.
///
/// The ordered result of `merge_active_entries` stays the same when each
/// deleted file's ADDs in the group come before its first DELETE in the group.
/// For a file added again after its DELETE, the ordered reader can stop listing
/// it, as scans already do.
///
/// Returns `None` when the group adds a file twice without deleting it; the
/// caller keeps such a group unmerged.
fn merge_group_entries(entries: Vec<ManifestEntry>) -> Option<Vec<ManifestEntry>> {
    let identifiers = entries
        .iter()
        .map(ManifestEntry::identifier)
        .collect::<Vec<_>>();
    let deleted = entries
        .iter()
        .zip(&identifiers)
        .filter(|(entry, _)| *entry.kind() == FileKind::Delete)
        .map(|(_, identifier)| identifier)
        .collect::<HashSet<_>>();
    let mut added = HashSet::new();
    let mut emitted_deletes = HashSet::new();
    let mut keep = Vec::with_capacity(entries.len());
    for (entry, identifier) in entries.iter().zip(&identifiers) {
        keep.push(match entry.kind() {
            FileKind::Add if deleted.contains(identifier) => false,
            FileKind::Add => {
                // Scans return both ADDs; dropping either would change that.
                if !added.insert(identifier) {
                    return None;
                }
                true
            }
            // A repeated DELETE removes nothing more; keep one.
            FileKind::Delete => emitted_deletes.insert(identifier),
        });
    }
    Some(
        entries
            .into_iter()
            .zip(keep)
            .filter_map(|(entry, keep)| keep.then_some(entry))
            .collect(),
    )
}

#[cfg(test)]
#[path = "manifest_merge_unit_tests.rs"]
mod tests;
