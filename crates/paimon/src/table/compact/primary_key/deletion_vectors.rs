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

//! Snapshot-bound physical deletion filtering and commit-time index retirement.

use std::collections::{HashMap, HashSet};

use crate::spec::{
    DeletionVectorMeta, FileKind, IndexManifest, IndexManifestEntry, ManifestEntry, Snapshot,
};
use crate::table::index_file_path::IndexFileLocation;
use crate::table::{DataSplit, Table};
use crate::{DeletionFile, Result};

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

/// Attach the captured vectors to physical files before the KV reader merges
/// keys. Materialized high-level rows must never be accumulated twice.
pub(super) async fn with_deletion_files(
    table: &Table,
    split: &DataSplit,
    snapshot: &Snapshot,
) -> Result<DataSplit> {
    let location = IndexFileLocation::BucketLocal {
        table_path: table.location().trim_end_matches('/'),
        bucket_path: split.bucket_path(),
        index_file_in_data_file_dir: table.schema().core_options().index_file_in_data_file_dir(),
    };
    let partition = split.partition().to_serialized_bytes();
    let mut by_name = HashMap::new();
    for entry in read_indexes(table, snapshot).await? {
        if entry.kind != FileKind::Add
            || entry.index_file.index_type != "DELETION_VECTORS"
            || entry.partition != partition
            || entry.bucket != split.bucket()
        {
            continue;
        }
        if let Some(ranges) = &entry.index_file.deletion_vectors_ranges {
            let path = location.resolve(
                &entry.index_file.file_name,
                entry.index_file.external_path.as_deref(),
            );
            for (name, meta) in ranges {
                by_name.insert(
                    name.clone(),
                    DeletionFile::new(
                        path.clone(),
                        i64::from(meta.offset),
                        i64::from(meta.length),
                        meta.cardinality,
                    ),
                );
            }
        }
    }
    let deletion_files = split
        .data_files()
        .iter()
        .map(|file| by_name.remove(&file.file_name))
        .collect();
    // Preserve the selected physical files, including L0, irrespective of the
    // query reader's deletion-vectors.merge-on-read option.
    DataSplit::builder()
        .with_snapshot(split.snapshot_id())
        .with_partition(split.partition().clone())
        .with_bucket(split.bucket())
        .with_total_buckets(split.total_buckets())
        .with_bucket_path(split.bucket_path().to_string())
        .with_data_files(split.data_files().to_vec())
        .with_data_deletion_files(deletion_files)
        .build()
}

type FileId = (Vec<u8>, i32, String);
type VectorReference = (String, Option<String>, DeletionVectorMeta);

fn selected_vectors(
    indexes: &[IndexManifestEntry],
    inputs: &HashSet<FileId>,
) -> HashMap<FileId, VectorReference> {
    let mut selected = HashMap::new();
    for entry in indexes {
        if entry.kind != FileKind::Add || entry.index_file.index_type != "DELETION_VECTORS" {
            continue;
        }
        if let Some(ranges) = &entry.index_file.deletion_vectors_ranges {
            for (name, meta) in ranges {
                let id = (entry.partition.clone(), entry.bucket, name.clone());
                if inputs.contains(&id) {
                    selected.insert(
                        id,
                        (
                            entry.index_file.file_name.clone(),
                            entry.index_file.external_path.clone(),
                            meta.clone(),
                        ),
                    );
                }
            }
        }
    }
    selected
}

/// Cache bytes are immutable physical rows. Snapshot vectors must still match
/// every rewrite/lookup dependency before publishing any result.
pub(in crate::table) async fn validate_dependencies(
    table: &Table,
    captured: &Snapshot,
    latest: &Snapshot,
    tasks: &[super::super::CompactTask],
) -> Result<()> {
    let mut inputs = HashSet::new();
    for task in tasks {
        for file in task.files.iter().chain(
            task.primary_key
                .iter()
                .flat_map(|selection| &selection.lookup_files),
        ) {
            inputs.insert((task.partition.clone(), task.bucket, file.file_name.clone()));
        }
    }
    if selected_vectors(&read_indexes(table, captured).await?, &inputs)
        != selected_vectors(&read_indexes(table, latest).await?, &inputs)
    {
        return Err(super::super::invalid(
            "Compaction input or lookup dependency deletion vectors changed; create a new plan",
        ));
    }
    Ok(())
}

/// Apply new historical row deletions and retire rewritten inputs atomically.
/// Untouched ranges sharing an index file and concurrent independent indexes
/// are retained. Called on every CAS attempt against that attempt's snapshot.
pub(in crate::table) async fn rewrite_indexes(
    table: &Table,
    latest: &Snapshot,
    delta: &[ManifestEntry],
    deletions: &[super::lookup_store::LookupDeletion],
    created_files: &mut Vec<String>,
) -> Result<Option<Vec<IndexManifestEntry>>> {
    use crate::table::data_evolution_writer::DataEvolutionDeleteWriter;
    use crate::table::index_file_path::committed_index_file_path;
    use indexmap::IndexMap;
    use roaring::RoaringBitmap;
    use std::collections::BTreeMap;
    let mut indexes = read_indexes(table, latest).await?;
    let mut buckets =
        BTreeMap::<(Vec<u8>, i32), (HashSet<String>, IndexMap<String, RoaringBitmap>)>::new();
    for entry in delta
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Delete)
    {
        buckets
            .entry((entry.partition().to_vec(), entry.bucket()))
            .or_default()
            .0
            .insert(entry.file().file_name.clone());
    }
    for deletion in deletions {
        let bitmap = RoaringBitmap::deserialize_from(deletion.bitmap.as_slice())
            .map_err(|error| super::super::invalid(&error.to_string()))?;
        *buckets
            .entry((deletion.partition.clone(), deletion.bucket))
            .or_default()
            .1
            .entry(deletion.file_name.clone())
            .or_default() |= bitmap;
    }
    let writer = DataEvolutionDeleteWriter::for_primary_key_compaction(table);
    let mut changed = false;
    for ((partition, bucket), (rewritten, additions)) in buckets {
        let targets = rewritten
            .iter()
            .cloned()
            .chain(additions.keys().cloned())
            .collect::<HashSet<_>>();
        let (mut vectors, removed) = writer
            .read_existing_bucket_deletion_vectors(&partition, bucket, latest, Some(&targets))
            .await?;
        let mut retired = false;
        for name in &rewritten {
            retired |= vectors.shift_remove(name).is_some();
        }
        if !retired && additions.is_empty() {
            continue;
        }
        for (name, bitmap) in additions {
            *vectors.entry(name).or_default() |= bitmap;
        }
        indexes.retain(|entry| {
            !removed.iter().any(|file| {
                entry.partition == partition && entry.bucket == bucket && entry.index_file == *file
            })
        });
        if !vectors.values().all(RoaringBitmap::is_empty) {
            let file = writer
                .write_deletion_vector_index_file(&partition, bucket, vectors)
                .await?;
            let options = table.schema().core_options();
            let computer = crate::spec::PartitionComputer::new(
                table.schema().partition_keys(),
                table.schema().fields(),
                options.partition_default_name(),
                options.legacy_partition_name(),
            )?;
            let bucket_path = crate::spec::bucket_path(
                &table.data_file_location(),
                Some(&computer),
                &crate::spec::BinaryRow::from_serialized_bytes(&partition)?,
                bucket,
            )?;
            created_files.push(committed_index_file_path(
                table.location(),
                &bucket_path,
                options.index_file_in_data_file_dir(),
                &file,
            ));
            indexes.push(IndexManifestEntry {
                version: 1,
                kind: FileKind::Add,
                partition,
                bucket,
                index_file: file,
            });
        }
        changed = true;
    }
    Ok(changed.then_some(indexes))
}
