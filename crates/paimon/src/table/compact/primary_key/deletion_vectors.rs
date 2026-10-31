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

//! Captured physical deletions and atomic retirement of compacted DV ranges.

use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use roaring::RoaringBitmap;

use super::super::deletion_vectors::DeletionVectorUpdate;
use crate::deletion_vector::DeletionVectorFactory;
use crate::spec::{
    BinaryRow, DataFileMeta, DeletionVectorMeta, FileKind, IndexManifest, IndexManifestEntry,
    ManifestEntry, PartitionComputer, Snapshot,
};
use crate::table::index_file_path::IndexFileLocation;
use crate::table::{DataSplit, Table};
use crate::{DeletionFile, Result};

type FileId = (Vec<u8>, i32, String);
type VectorReference = (String, Option<String>, DeletionVectorMeta);

async fn read_indexes(table: &Table, snapshot: &Snapshot) -> Result<Vec<IndexManifestEntry>> {
    match snapshot.index_manifest() {
        Some(name) => {
            IndexManifest::read(
                table.file_io(),
                &table.snapshot_manager().manifest_path(name),
            )
            .await
        }
        None => Ok(vec![]),
    }
}

fn vector_references(indexes: &[IndexManifestEntry]) -> Result<HashMap<FileId, VectorReference>> {
    let mut references = HashMap::new();
    for entry in indexes {
        if entry.kind != FileKind::Add || entry.index_file.index_type != "DELETION_VECTORS" {
            continue;
        }
        if let Some(ranges) = &entry.index_file.deletion_vectors_ranges {
            for (name, meta) in ranges {
                let id = (entry.partition.clone(), entry.bucket, name.clone());
                if references
                    .insert(
                        id,
                        (
                            entry.index_file.file_name.clone(),
                            entry.index_file.external_path.clone(),
                            meta.clone(),
                        ),
                    )
                    .is_some()
                {
                    return Err(super::super::invalid(
                        "Multiple deletion vectors reference one data file",
                    ));
                }
            }
        }
    }
    Ok(references)
}

fn resolve_vector(table: &Table, bucket_path: &str, reference: &VectorReference) -> DeletionFile {
    let location = IndexFileLocation::BucketLocal {
        table_path: table.location().trim_end_matches('/'),
        bucket_path,
        index_file_in_data_file_dir: table.schema().core_options().index_file_in_data_file_dir(),
    };
    DeletionFile::new(
        location.resolve(&reference.0, reference.1.as_deref()),
        i64::from(reference.2.offset),
        i64::from(reference.2.length),
        reference.2.cardinality,
    )
}

async fn read_bitmap(
    table: &Table,
    path: &str,
    reference: Option<&VectorReference>,
) -> Result<RoaringBitmap> {
    match reference {
        Some(reference) => Ok(DeletionVectorFactory::read(
            table.file_io(),
            &resolve_vector(table, path, reference),
        )
        .await?
        .to_bitmap()),
        None => Ok(RoaringBitmap::new()),
    }
}

fn bucket_path(table: &Table, partition: &[u8], bucket: i32) -> Result<String> {
    let schema = table.schema();
    let options = schema.core_options();
    let computer = PartitionComputer::new(
        schema.partition_keys(),
        schema.fields(),
        options.partition_default_name(),
        options.legacy_partition_name(),
    )?;
    crate::spec::bucket_path(
        &table.data_file_location(),
        Some(&computer),
        &BinaryRow::from_serialized_bytes(partition)?,
        bucket,
    )
}

/// Apply captured physical masks before merging, including masks on L0 files.
pub(in crate::table::compact) async fn with_deletion_files(
    table: &Table,
    split: &DataSplit,
    snapshot: &Snapshot,
) -> Result<DataSplit> {
    let references = vector_references(&read_indexes(table, snapshot).await?)?;
    let partition = split.partition().to_serialized_bytes();
    let files = split
        .data_files()
        .iter()
        .map(|file| {
            references
                .get(&(partition.clone(), split.bucket(), file.file_name.clone()))
                .map(|reference| resolve_vector(table, split.bucket_path(), reference))
        })
        .collect();
    DataSplit::builder()
        .with_snapshot(split.snapshot_id())
        .with_partition(split.partition().clone())
        .with_bucket(split.bucket())
        .with_total_buckets(split.total_buckets())
        .with_bucket_path(split.bucket_path().to_string())
        .with_data_files(split.data_files().to_vec())
        .with_data_deletion_files(files)
        .build()
}

/// Selected masks must be unchanged. Actual point-lookup hits must still be
/// live; unrelated deletions in their files can safely be rebased by union.
pub(in crate::table) async fn validate_dependencies(
    table: &Table,
    captured: &Snapshot,
    latest: &Snapshot,
    delta: &[ManifestEntry],
    updates: &[DeletionVectorUpdate],
) -> Result<()> {
    let captured_vectors = vector_references(&read_indexes(table, captured).await?)?;
    let same_snapshot = captured.has_same_read_metadata(latest);
    let latest_references = if same_snapshot {
        None
    } else {
        Some(vector_references(&read_indexes(table, latest).await?)?)
    };
    let latest_vectors = latest_references.as_ref().unwrap_or(&captured_vectors);
    let selected = delta
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Delete)
        .map(|entry| {
            (
                entry.partition().to_vec(),
                entry.bucket(),
                entry.file().file_name.clone(),
            )
        })
        .collect::<HashSet<_>>();
    for id in &selected {
        let before = captured_vectors.get(id);
        let after = latest_vectors.get(id);
        if before == after {
            continue;
        }
        // Updating another range can repack this mask into a new immutable
        // container without changing any selected physical row.
        let path = bucket_path(table, &id.0, id.1)?;
        if read_bitmap(table, &path, before).await? != read_bitmap(table, &path, after).await? {
            return Err(super::super::invalid(
                "Compaction input deletion vectors changed; create a new plan",
            ));
        }
    }
    let manager = table.snapshot_manager();
    let captured_files =
        crate::table::snapshot_manifest::read_snapshot_entries(&manager, captured, None).await?;
    super::super::validate_dv_schemas(table, captured_files.iter().map(|entry| entry.file()))
        .await?;
    if updates.is_empty() {
        return Ok(());
    }
    let files = |entries: Vec<ManifestEntry>| {
        entries
            .into_iter()
            .map(|entry| {
                (
                    (
                        entry.partition().to_vec(),
                        entry.bucket(),
                        entry.file().file_name.clone(),
                    ),
                    entry,
                )
            })
            .collect::<HashMap<_, _>>()
    };
    let captured_files = files(captured_files);
    let latest_entries = if same_snapshot {
        None
    } else {
        Some(files(
            crate::table::snapshot_manifest::read_snapshot_entries(&manager, latest, None).await?,
        ))
    };
    let latest_files = latest_entries.as_ref().unwrap_or(&captured_files);
    let buckets = selected
        .iter()
        .map(|id| (&id.0, id.1))
        .collect::<HashSet<_>>();
    let mut hits = HashMap::<FileId, RoaringBitmap>::new();
    for update in updates {
        let id = (
            update.partition.clone(),
            update.bucket,
            update.file_name.clone(),
        );
        let bitmap = RoaringBitmap::deserialize_from(update.bitmap.as_slice())
            .map_err(|error| super::super::invalid(&error.to_string()))?;
        let captured_file = captured_files
            .get(&id)
            .ok_or_else(|| super::super::invalid("Lookup deletion target was not captured"))?;
        let latest_file = latest_files.get(&id).ok_or_else(|| {
            super::super::invalid("Lookup deletion target was removed; create a new plan")
        })?;
        if selected.contains(&id)
            || !buckets.contains(&(&id.0, id.1))
            || captured_file.file().level <= 0
            || captured_file.file() != latest_file.file()
            || captured_file.total_buckets() != latest_file.total_buckets()
            || bitmap.is_empty()
            || bitmap
                .max()
                .is_none_or(|position| i64::from(position) >= captured_file.file().row_count)
        {
            return Err(super::super::invalid(
                "Invalid or changed lookup deletion target",
            ));
        }
        *hits.entry(id).or_default() |= bitmap;
    }
    for (id, positions) in hits {
        let path = bucket_path(table, &id.0, id.1)?;
        let before = captured_vectors.get(&id);
        let after = latest_vectors.get(&id);
        for reference in [before, after].into_iter().flatten() {
            let vector = DeletionVectorFactory::read(
                table.file_io(),
                &resolve_vector(table, &path, reference),
            )
            .await?;
            if !vector.to_bitmap().is_disjoint(&positions) {
                return Err(super::super::invalid(
                    "Lookup dependency deletion vectors changed; create a new plan",
                ));
            }
            if before == after {
                break;
            }
        }
    }
    Ok(())
}

/// Retire rewritten files' masks and merge physical point-lookup deletions into
/// the retained files' latest masks.
pub(in crate::table::compact) fn apply_deletion_vectors(
    before: &[DataFileMeta],
    updates: &[&DeletionVectorUpdate],
    vectors: &mut IndexMap<String, RoaringBitmap>,
) -> Result<()> {
    for file in before {
        vectors.shift_remove(&file.file_name);
    }
    for update in updates {
        let bitmap = RoaringBitmap::deserialize_from(update.bitmap.as_slice())
            .map_err(|error| super::super::invalid(&error.to_string()))?;
        *vectors.entry(update.file_name.clone()).or_default() |= bitmap;
    }
    Ok(())
}
