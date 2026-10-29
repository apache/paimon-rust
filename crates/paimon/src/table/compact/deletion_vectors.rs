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

//! Prepare compaction deletion-vector indexes against each commit attempt's snapshot.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::spec::{
    DataFileMeta, FileKind, IndexManifest, IndexManifestEntry, ManifestEntry, Snapshot,
};
use crate::table::deletion_vector_index;
use crate::table::Table;
use crate::Result;

/// Physical rows discovered by an executor, merged with the latest DV at commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::table) struct DeletionVectorUpdate {
    pub partition: Vec<u8>,
    pub bucket: i32,
    pub file_name: String,
    #[serde(with = "serde_bytes")]
    pub bitmap: Vec<u8>,
}

type Bucket = (Vec<u8>, i32);

/// Both modes prepare the same index-manifest replacement. Their container
/// selection and physical-position transformations remain mode-specific.
pub(in crate::table) async fn rewrite_indexes(
    table: &Table,
    latest: &Snapshot,
    delta: &[ManifestEntry],
    updates: &[DeletionVectorUpdate],
    created_files: &mut Vec<String>,
) -> Result<Option<Vec<IndexManifestEntry>>> {
    let data_evolution = table.schema().core_options().data_evolution_enabled();
    let mut buckets = BTreeMap::<
        Bucket,
        (
            Vec<DataFileMeta>,
            Vec<DataFileMeta>,
            Vec<&DeletionVectorUpdate>,
        ),
    >::new();
    for entry in delta {
        let bucket = buckets
            .entry((entry.partition().to_vec(), entry.bucket()))
            .or_default();
        match entry.kind() {
            FileKind::Delete => bucket.0.push(entry.file().clone()),
            FileKind::Add => bucket.1.push(entry.file().clone()),
        }
    }
    for update in updates {
        buckets
            .entry((update.partition.clone(), update.bucket))
            .or_default()
            .2
            .push(update);
    }
    let indexes = match latest.index_manifest() {
        Some(name) => {
            IndexManifest::read(
                table.file_io(),
                &table.snapshot_manager().manifest_path(name),
            )
            .await?
        }
        None if data_evolution => return Ok(None),
        None => Vec::new(),
    };
    // Group only metadata, then decode and release one bucket's masks at a time.
    let mut retained = Vec::new();
    let mut containers = BTreeMap::<Bucket, Vec<IndexManifestEntry>>::new();
    for entry in indexes {
        let key = (entry.partition.clone(), entry.bucket);
        if entry.kind == FileKind::Add
            && entry.index_file.index_type == "DELETION_VECTORS"
            && buckets.contains_key(&key)
        {
            containers.entry(key).or_default().push(entry);
        } else {
            retained.push(entry);
        }
    }
    let mut changed = false;
    for ((partition, bucket), (before, after, additions)) in buckets {
        let selected_files = if data_evolution {
            Some(super::data_evolution::deletion_vector_anchors(&before)?)
        } else {
            None
        };
        let mut selected = Vec::new();
        for entry in containers
            .remove(&(partition.clone(), bucket))
            .into_iter()
            .flatten()
        {
            if selected_files.as_ref().is_none_or(|files| {
                entry
                    .index_file
                    .deletion_vectors_ranges
                    .as_ref()
                    .is_some_and(|ranges| ranges.keys().any(|name| files.contains(name)))
            }) {
                selected.push(entry);
            } else {
                retained.push(entry);
            }
        }
        if !data_evolution && selected.is_empty() && additions.is_empty() {
            continue;
        }
        let mut masks =
            deletion_vector_index::read_containers(table, &partition, bucket, &selected).await?;
        if data_evolution {
            if !super::data_evolution::remap_deletion_vectors(&before, &after, &mut masks)? {
                retained.extend(selected);
                continue;
            }
        } else {
            super::primary_key::deletion_vectors::apply_deletion_vectors(
                &before, &additions, &mut masks,
            )?;
        }
        // DE can explicitly replace an empty container; PK retires it entirely.
        if data_evolution || masks.values().any(|mask| !mask.is_empty()) {
            let file =
                deletion_vector_index::write_bucket(table, &partition, bucket, masks).await?;
            created_files.push(deletion_vector_index::file_path(
                table, &partition, bucket, &file,
            )?);
            retained.push(IndexManifestEntry {
                version: 1,
                kind: FileKind::Add,
                partition,
                bucket,
                index_file: file,
            });
        }
        changed = true;
    }
    Ok(changed.then_some(retained))
}
