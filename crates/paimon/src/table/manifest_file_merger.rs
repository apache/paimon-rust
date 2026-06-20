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

use crate::io::FileIO;
use crate::spec::stats::BinaryTableStats;
use crate::spec::{
    extract_datum, BinaryRow, BinaryRowBuilder, DataField, DataType, Datum, FileKind, Identifier,
    Manifest, ManifestEntry, ManifestFileMeta,
};
use crate::Result;
use std::collections::{HashMap, HashSet};

/// Manifest file merger with Java `ManifestFileMerger`-style full and minor compaction.
pub(crate) struct ManifestFileMerger<'a> {
    file_io: &'a FileIO,
    manifest_dir: &'a str,
    partition_fields: &'a [DataField],
    schema_id: i64,
    target_file_size: i64,
    full_compaction_threshold_size: i64,
    merge_min_count: usize,
}

impl<'a> ManifestFileMerger<'a> {
    pub(crate) fn new(
        file_io: &'a FileIO,
        manifest_dir: &'a str,
        partition_fields: &'a [DataField],
        schema_id: i64,
        target_file_size: i64,
        full_compaction_threshold_size: i64,
        merge_min_count: usize,
    ) -> Self {
        Self {
            file_io,
            manifest_dir,
            partition_fields,
            schema_id,
            target_file_size,
            full_compaction_threshold_size,
            merge_min_count,
        }
    }

    pub(crate) async fn merge(
        &self,
        manifest_files: Vec<ManifestFileMeta>,
    ) -> Result<Vec<ManifestFileMeta>> {
        if manifest_files.len() <= 1 {
            return Ok(manifest_files);
        }

        if let Some(compacted) = self.try_full_compaction(&manifest_files).await? {
            return Ok(compacted);
        }

        self.try_minor_compaction(manifest_files).await
    }

    fn should_full_compact(&self, manifest_files: &[ManifestFileMeta]) -> bool {
        let delta_size: i64 = manifest_files
            .iter()
            .filter(|file| self.must_change(file))
            .map(ManifestFileMeta::file_size)
            .sum();
        delta_size >= self.full_compaction_threshold_size
    }

    async fn try_full_compaction(
        &self,
        manifest_files: &[ManifestFileMeta],
    ) -> Result<Option<Vec<ManifestFileMeta>>> {
        if !self.should_full_compact(manifest_files) {
            return Ok(None);
        }

        let delete_identifiers = self.read_deleted_identifiers(manifest_files).await?;
        let delete_partitions: HashSet<Vec<u8>> = delete_identifiers
            .iter()
            .map(|identifier| identifier.partition.clone())
            .collect();

        let mut result = Vec::new();
        let mut to_be_merged = Vec::new();
        for manifest_file in manifest_files {
            if self.must_change(manifest_file)
                || self.may_contain_deleted_partitions(manifest_file, &delete_partitions)
            {
                to_be_merged.push(manifest_file.clone());
            } else {
                result.push(manifest_file.clone());
            }
        }

        if to_be_merged.len() <= 1 {
            return Ok(None);
        }

        let mut merged_entries = Vec::new();
        for manifest_file in to_be_merged {
            let read_result = self
                .read_for_full_compaction(&manifest_file, &delete_identifiers)
                .await?;
            if read_result.require_change {
                merged_entries.extend(read_result.entries);
            } else {
                result.push(read_result.file);
            }
        }

        result.extend(self.write_compacted_entries(&merged_entries).await?);
        Ok(Some(result))
    }

    async fn read_deleted_identifiers(
        &self,
        manifest_files: &[ManifestFileMeta],
    ) -> Result<HashSet<Identifier>> {
        let mut identifiers = HashSet::new();
        for manifest_file in manifest_files {
            if manifest_file.num_deleted_files() == 0 {
                continue;
            }
            let path = self.manifest_path(manifest_file.file_name());
            for entry in Manifest::read(self.file_io, &path).await? {
                if *entry.kind() == FileKind::Delete {
                    identifiers.insert(entry.into_identifier());
                }
            }
        }
        Ok(identifiers)
    }

    async fn read_for_full_compaction(
        &self,
        manifest_file: &ManifestFileMeta,
        delete_identifiers: &HashSet<Identifier>,
    ) -> Result<FullCompactionReadResult> {
        let path = self.manifest_path(manifest_file.file_name());
        let mut require_change = self.must_change(manifest_file);
        let mut entries = Vec::new();

        for entry in Manifest::read(self.file_io, &path).await? {
            if *entry.kind() != FileKind::Add {
                continue;
            }
            if delete_identifiers.contains(&entry.identifier()) {
                require_change = true;
            } else {
                entries.push(entry);
            }
        }

        Ok(FullCompactionReadResult {
            file: manifest_file.clone(),
            require_change,
            entries,
        })
    }

    fn must_change(&self, manifest_file: &ManifestFileMeta) -> bool {
        manifest_file.num_deleted_files() > 0 || manifest_file.file_size() < self.target_file_size
    }

    fn may_contain_deleted_partitions(
        &self,
        manifest_file: &ManifestFileMeta,
        delete_partitions: &HashSet<Vec<u8>>,
    ) -> bool {
        if delete_partitions.is_empty() {
            return false;
        }

        if self.partition_fields.is_empty() {
            return true;
        }

        delete_partitions.iter().any(|partition| {
            self.partition_may_match_manifest_stats(partition, manifest_file.partition_stats())
        })
    }

    fn partition_may_match_manifest_stats(
        &self,
        partition: &[u8],
        stats: &BinaryTableStats,
    ) -> bool {
        let Ok(partition_row) = BinaryRow::from_serialized_bytes(partition) else {
            return true;
        };
        let Ok(min_row) = BinaryRow::from_serialized_bytes(stats.min_values()) else {
            return true;
        };
        let Ok(max_row) = BinaryRow::from_serialized_bytes(stats.max_values()) else {
            return true;
        };
        if partition_row.arity() < self.partition_fields.len() as i32
            || min_row.arity() < self.partition_fields.len() as i32
            || max_row.arity() < self.partition_fields.len() as i32
        {
            return true;
        }

        for (idx, field) in self.partition_fields.iter().enumerate() {
            let data_type = field.data_type();
            let Ok(partition_datum) = extract_datum(&partition_row, idx, data_type) else {
                return true;
            };
            let Ok(min_datum) = extract_datum(&min_row, idx, data_type) else {
                return true;
            };
            let Ok(max_datum) = extract_datum(&max_row, idx, data_type) else {
                return true;
            };

            match partition_datum {
                Some(datum) => {
                    let (Some(min), Some(max)) = (min_datum, max_datum) else {
                        return true;
                    };
                    if datum < min || datum > max {
                        return false;
                    }
                }
                None => {
                    if matches!(stats.null_counts().get(idx), Some(Some(0))) {
                        return false;
                    }
                }
            }
        }

        true
    }

    async fn try_minor_compaction(
        &self,
        manifest_files: Vec<ManifestFileMeta>,
    ) -> Result<Vec<ManifestFileMeta>> {
        let mut result = Vec::new();
        let mut candidates = Vec::new();
        let mut total_size = 0;

        for manifest_file in manifest_files {
            total_size += manifest_file.file_size();
            candidates.push(manifest_file);
            if total_size >= self.target_file_size {
                let merged = self.merge_candidates(&candidates).await?;
                result.extend(merged);
                candidates.clear();
                total_size = 0;
            }
        }

        if candidates.len() >= self.merge_min_count {
            let merged = self.merge_candidates(&candidates).await?;
            result.extend(merged);
        } else {
            result.extend(candidates);
        }

        Ok(result)
    }

    async fn merge_candidates(
        &self,
        candidates: &[ManifestFileMeta],
    ) -> Result<Vec<ManifestFileMeta>> {
        if candidates.len() == 1 {
            return Ok(vec![candidates[0].clone()]);
        }

        let mut entries = Vec::new();
        for manifest_file in candidates {
            let path = self.manifest_path(manifest_file.file_name());
            entries.extend(Manifest::read(self.file_io, &path).await?);
        }

        let merged_entries = merge_entries(entries)?;
        self.write_compacted_entries(&merged_entries).await
    }

    async fn write_compacted_entries(
        &self,
        entries: &[ManifestEntry],
    ) -> Result<Vec<ManifestFileMeta>> {
        if entries.is_empty() {
            return Ok(vec![]);
        }

        let manifest_name = format!("manifest-{}-0", uuid::Uuid::new_v4());
        let manifest_path = self.manifest_path(&manifest_name);
        let manifest_meta = self
            .write_manifest_file(&manifest_path, &manifest_name, entries)
            .await?;
        Ok(vec![manifest_meta])
    }

    async fn write_manifest_file(
        &self,
        path: &str,
        file_name: &str,
        entries: &[ManifestEntry],
    ) -> Result<ManifestFileMeta> {
        Manifest::write(self.file_io, path, entries).await?;

        let mut added_file_count: i64 = 0;
        let mut deleted_file_count: i64 = 0;
        let mut min_bucket: Option<i32> = None;
        let mut max_bucket: Option<i32> = None;
        let mut min_level: Option<i32> = None;
        let mut max_level: Option<i32> = None;
        for entry in entries {
            match entry.kind() {
                FileKind::Add => added_file_count += 1,
                FileKind::Delete => deleted_file_count += 1,
            }
            let b = entry.bucket();
            min_bucket = Some(min_bucket.map_or(b, |cur| cur.min(b)));
            max_bucket = Some(max_bucket.map_or(b, |cur| cur.max(b)));
            let l = entry.file().level;
            min_level = Some(min_level.map_or(l, |cur| cur.min(l)));
            max_level = Some(max_level.map_or(l, |cur| cur.max(l)));
        }

        let status = self.file_io.get_status(path).await?;
        let partition_stats = compute_partition_stats(entries, self.partition_fields)?;

        Ok(ManifestFileMeta::new(
            file_name.to_string(),
            status.size as i64,
            added_file_count,
            deleted_file_count,
            partition_stats,
            self.schema_id,
        )
        .with_bucket_level_stats(min_bucket, max_bucket, min_level, max_level))
    }

    fn manifest_path(&self, file_name: &str) -> String {
        format!("{}/{}", self.manifest_dir, file_name)
    }
}

fn merge_entries(entries: impl IntoIterator<Item = ManifestEntry>) -> Result<Vec<ManifestEntry>> {
    let mut merged_entries = HashMap::new();
    for entry in entries {
        merge_entry(&mut merged_entries, entry)?;
    }
    Ok(merged_entries.into_values().collect())
}

fn merge_entry(
    merged_entries: &mut HashMap<Identifier, ManifestEntry>,
    entry: ManifestEntry,
) -> Result<()> {
    let identifier = entry.identifier();
    match *entry.kind() {
        FileKind::Add => {
            if merged_entries.contains_key(&identifier) {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Trying to add file {:?} which is already in the manifest entry map",
                        identifier
                    ),
                    source: None,
                });
            }
            merged_entries.insert(identifier, entry);
        }
        FileKind::Delete => {
            if merged_entries.contains_key(&identifier) {
                merged_entries.remove(&identifier);
            } else {
                merged_entries.insert(identifier, entry);
            }
        }
    }
    Ok(())
}

fn compute_partition_stats(
    entries: &[ManifestEntry],
    partition_fields: &[DataField],
) -> Result<BinaryTableStats> {
    let num_fields = partition_fields.len();

    if num_fields == 0 || entries.is_empty() {
        return Ok(BinaryTableStats::empty());
    }

    let data_types: Vec<_> = partition_fields
        .iter()
        .map(|f| f.data_type().clone())
        .collect();
    let mut mins: Vec<Option<Datum>> = vec![None; num_fields];
    let mut maxs: Vec<Option<Datum>> = vec![None; num_fields];
    let mut null_counts: Vec<i64> = vec![0; num_fields];

    for entry in entries {
        let partition_bytes = entry.partition();
        if partition_bytes.is_empty() {
            continue;
        }
        let row = BinaryRow::from_serialized_bytes(partition_bytes)?;
        for i in 0..num_fields {
            match extract_datum(&row, i, &data_types[i])? {
                Some(datum) => {
                    mins[i] = Some(match mins[i].take() {
                        Some(cur) if cur <= datum => cur,
                        Some(_) => datum.clone(),
                        None => datum.clone(),
                    });
                    maxs[i] = Some(match maxs[i].take() {
                        Some(cur) if cur >= datum => cur,
                        Some(_) => datum,
                        None => datum,
                    });
                }
                None => {
                    null_counts[i] += 1;
                }
            }
        }
    }

    let min_bytes = build_partition_stats_row(&mins, &data_types);
    let max_bytes = build_partition_stats_row(&maxs, &data_types);
    let null_counts = null_counts.into_iter().map(Some).collect();

    Ok(BinaryTableStats::new(min_bytes, max_bytes, null_counts))
}

fn build_partition_stats_row(datums: &[Option<Datum>], data_types: &[DataType]) -> Vec<u8> {
    let mut builder = BinaryRowBuilder::new(datums.len() as i32);
    for (pos, (datum_opt, data_type)) in datums.iter().zip(data_types.iter()).enumerate() {
        match datum_opt {
            Some(d) => builder.write_datum(pos, d, data_type),
            None => builder.set_null_at(pos),
        }
    }
    builder.build_serialized()
}

struct FullCompactionReadResult {
    file: ManifestFileMeta,
    require_change: bool,
    entries: Vec<ManifestEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{DataFileMeta, IntType, Schema, TableSchema, VarCharType};
    use chrono::{DateTime, Utc};
    use std::collections::HashMap;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn test_schema() -> TableSchema {
        let schema = Schema::builder()
            .column("id", crate::spec::DataType::Int(IntType::new()))
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_partitioned_schema() -> TableSchema {
        let schema = Schema::builder()
            .column(
                "pt",
                crate::spec::DataType::VarChar(VarCharType::string_type()),
            )
            .column("id", crate::spec::DataType::Int(IntType::new()))
            .partition_keys(["pt"])
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_data_file(name: &str, row_count: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 1024,
            row_count,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: Some(
                "2024-09-06T07:45:55.039+00:00"
                    .parse::<DateTime<Utc>>()
                    .unwrap(),
            ),
            delete_row_count: Some(0),
            embedded_index: None,
            first_row_id: None,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
        }
    }

    async fn setup_dirs(file_io: &FileIO, table_path: &str) {
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();
    }

    fn partition_bytes(pt: &str) -> Vec<u8> {
        let mut builder = BinaryRowBuilder::new(1);
        if pt.len() <= 7 {
            builder.write_string_inline(0, pt);
        } else {
            builder.write_string(0, pt);
        }
        builder.build_serialized()
    }

    fn manifest_merger<'a>(
        file_io: &'a FileIO,
        manifest_dir: &'a str,
        partition_fields: &'a [DataField],
        schema_id: i64,
        options: HashMap<String, String>,
    ) -> ManifestFileMerger<'a> {
        let core_options = crate::spec::CoreOptions::new(&options);
        ManifestFileMerger::new(
            file_io,
            manifest_dir,
            partition_fields,
            schema_id,
            core_options.manifest_target_file_size(),
            core_options.manifest_full_compaction_threshold_size(),
            core_options.manifest_merge_min_count(),
        )
    }

    async fn write_manifest(
        merger: &ManifestFileMerger<'_>,
        name: &str,
        entries: &[ManifestEntry],
    ) -> ManifestFileMeta {
        merger
            .write_manifest_file(&format!("{}/{}", merger.manifest_dir, name), name, entries)
            .await
            .unwrap()
    }

    #[test]
    fn test_merge_entries_preserves_unmatched_delete() {
        let merged = merge_entries(vec![
            ManifestEntry::new(
                FileKind::Delete,
                vec![],
                0,
                1,
                test_data_file("deleted-in-base.parquet", 10),
                2,
            ),
            ManifestEntry::new(
                FileKind::Add,
                vec![],
                0,
                1,
                test_data_file("unrelated.parquet", 5),
                2,
            ),
        ])
        .unwrap();

        assert_eq!(merged.len(), 2);
        assert!(merged.iter().any(|entry| {
            *entry.kind() == FileKind::Delete && entry.file().file_name == "deleted-in-base.parquet"
        }));
    }

    #[tokio::test]
    async fn test_minor_merge_preserves_unmatched_delete_entries() {
        let file_io = test_file_io();
        let table_path = "memory:/test_minor_merge_preserves_unmatched_delete_entries";
        setup_dirs(&file_io, table_path).await;
        let schema = test_schema();
        let partition_fields = schema.partition_fields();
        let manifest_dir = format!("{table_path}/manifest");
        let merger = manifest_merger(
            &file_io,
            &manifest_dir,
            &partition_fields,
            schema.id(),
            HashMap::from([("manifest.merge-min-count".to_string(), "2".to_string())]),
        );

        let deleted_file = test_data_file("deleted-in-base.parquet", 10);
        let delete_meta = write_manifest(
            &merger,
            "manifest-delete-only-0",
            &[ManifestEntry::new(
                FileKind::Delete,
                vec![],
                0,
                1,
                deleted_file.clone(),
                2,
            )],
        )
        .await;
        let add_meta = write_manifest(
            &merger,
            "manifest-add-only-0",
            &[ManifestEntry::new(
                FileKind::Add,
                vec![],
                0,
                1,
                test_data_file("unrelated.parquet", 5),
                2,
            )],
        )
        .await;

        let compacted = merger.merge(vec![delete_meta, add_meta]).await.unwrap();
        assert_eq!(compacted.len(), 1);

        let compacted_entries = Manifest::read(
            &file_io,
            &format!("{}/{}", merger.manifest_dir, compacted[0].file_name()),
        )
        .await
        .unwrap();
        assert_eq!(compacted_entries.len(), 2);
        assert!(compacted_entries.iter().any(|entry| {
            *entry.kind() == FileKind::Delete && entry.file().file_name == deleted_file.file_name
        }));
    }

    #[tokio::test]
    async fn test_minor_merge_keeps_tail_below_merge_min_count() {
        let file_io = test_file_io();
        let table_path = "memory:/test_minor_merge_keeps_tail_below_merge_min_count";
        setup_dirs(&file_io, table_path).await;
        let schema = test_schema();
        let partition_fields = schema.partition_fields();
        let manifest_dir = format!("{table_path}/manifest");
        let merger = manifest_merger(
            &file_io,
            &manifest_dir,
            &partition_fields,
            schema.id(),
            HashMap::from([
                ("manifest.merge-min-count".to_string(), "3".to_string()),
                ("manifest.target-file-size".to_string(), "1GB".to_string()),
            ]),
        );

        let first_meta = write_manifest(
            &merger,
            "manifest-first-0",
            &[ManifestEntry::new(
                FileKind::Add,
                vec![],
                0,
                1,
                test_data_file("first.parquet", 5),
                2,
            )],
        )
        .await;
        let second_meta = write_manifest(
            &merger,
            "manifest-second-0",
            &[ManifestEntry::new(
                FileKind::Add,
                vec![],
                0,
                1,
                test_data_file("second.parquet", 5),
                2,
            )],
        )
        .await;

        let merged = merger
            .merge(vec![first_meta.clone(), second_meta.clone()])
            .await
            .unwrap();
        assert_eq!(merged, vec![first_meta, second_meta]);
    }

    #[tokio::test]
    async fn test_full_manifest_compaction_filters_delete_entries() {
        let file_io = test_file_io();
        let table_path = "memory:/test_full_manifest_compaction_filters_delete_entries";
        setup_dirs(&file_io, table_path).await;
        let schema = test_schema();
        let partition_fields = schema.partition_fields();
        let manifest_dir = format!("{table_path}/manifest");
        let merger = manifest_merger(
            &file_io,
            &manifest_dir,
            &partition_fields,
            schema.id(),
            HashMap::from([
                (
                    "manifest.full-compaction-threshold-size".to_string(),
                    "1B".to_string(),
                ),
                ("manifest.target-file-size".to_string(), "1B".to_string()),
            ]),
        );

        let delete_meta = write_manifest(
            &merger,
            "manifest-delete-only-0",
            &[ManifestEntry::new(
                FileKind::Delete,
                vec![],
                0,
                1,
                test_data_file("deleted-in-base.parquet", 10),
                2,
            )],
        )
        .await;
        let add_meta = write_manifest(
            &merger,
            "manifest-add-only-0",
            &[ManifestEntry::new(
                FileKind::Add,
                vec![],
                0,
                1,
                test_data_file("unrelated.parquet", 5),
                2,
            )],
        )
        .await;

        let compacted = merger
            .merge(vec![delete_meta, add_meta.clone()])
            .await
            .unwrap();
        assert_eq!(compacted.len(), 1);
        assert_eq!(compacted[0].file_name(), add_meta.file_name());
    }

    #[tokio::test]
    async fn test_full_manifest_compaction_skips_unaffected_base_manifests() {
        let file_io = test_file_io();
        let table_path = "memory:/test_full_manifest_compaction_skips_unaffected_base_manifests";
        setup_dirs(&file_io, table_path).await;
        let schema = test_partitioned_schema();
        let partition_fields = schema.partition_fields();
        let manifest_dir = format!("{table_path}/manifest");
        let merger = manifest_merger(
            &file_io,
            &manifest_dir,
            &partition_fields,
            schema.id(),
            HashMap::from([
                (
                    "manifest.full-compaction-threshold-size".to_string(),
                    "1B".to_string(),
                ),
                ("manifest.target-file-size".to_string(), "1B".to_string()),
            ]),
        );

        let keep_meta = write_manifest(
            &merger,
            "manifest-keep-0",
            &[ManifestEntry::new(
                FileKind::Add,
                partition_bytes("keep"),
                0,
                1,
                test_data_file("keep.parquet", 10),
                2,
            )],
        )
        .await;

        let drop_file = test_data_file("drop.parquet", 10);
        let drop_meta = write_manifest(
            &merger,
            "manifest-drop-0",
            &[ManifestEntry::new(
                FileKind::Add,
                partition_bytes("drop"),
                0,
                1,
                drop_file.clone(),
                2,
            )],
        )
        .await;
        let delete_meta = write_manifest(
            &merger,
            "manifest-delete-0",
            &[ManifestEntry::new(
                FileKind::Delete,
                partition_bytes("drop"),
                0,
                1,
                drop_file,
                2,
            )],
        )
        .await;

        let compacted = merger
            .merge(vec![keep_meta.clone(), drop_meta, delete_meta])
            .await
            .unwrap();
        assert_eq!(compacted.len(), 1);
        assert_eq!(compacted[0].file_name(), keep_meta.file_name());
    }
}
