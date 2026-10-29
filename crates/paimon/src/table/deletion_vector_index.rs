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

//! Deletion-vector index I/O shared by table writers and compaction.

use std::collections::HashSet;

use bytes::Bytes;
use indexmap::IndexMap;
use roaring::RoaringBitmap;
use uuid::Uuid;

use crate::deletion_vector::{DeletionVector, DeletionVectorFactory};
use crate::spec::{
    bucket_path, BinaryRow, CoreOptions, DeletionVectorMeta, FileKind, IndexFileMeta,
    IndexManifest, IndexManifestEntry, PartitionComputer, Snapshot, EMPTY_BINARY_ROW,
};
use crate::table::index_file_path::IndexFileLocation;
use crate::table::Table;
use crate::Result;

const DELETION_VECTORS_INDEX_TYPE: &str = "DELETION_VECTORS";
const DELETION_VECTORS_INDEX_VERSION_V1: u8 = 1;
const MANIFEST_DIR: &str = "manifest";

/// One bucket's deletion-vector location, owning the strings the shared resolver
/// borrows. Built once per bucket so the read that merges existing vectors and the
/// write that follows it cannot disagree about where the file goes.
struct DeletionVectorLayout {
    table_path: String,
    relative_bucket_path: String,
    bucket_path: String,
    index_file_in_data_file_dir: bool,
}

impl DeletionVectorLayout {
    fn location(&self) -> IndexFileLocation<'_> {
        IndexFileLocation::BucketLocal {
            table_path: &self.table_path,
            bucket_path: &self.bucket_path,
            index_file_in_data_file_dir: self.index_file_in_data_file_dir,
        }
    }
}

/// Where this bucket's deletion vectors live. A deletion vector is an index
/// file, so it sits beside the bucket's data files when the table keeps index
/// files there, and under the table `index/` directory otherwise. Reads and
/// writes both go through this, so a file written here is found again.
fn deletion_vector_layout(
    table: &Table,
    partition: &[u8],
    bucket: i32,
) -> Result<DeletionVectorLayout> {
    let schema = table.schema();
    let partition_keys = schema.partition_keys();
    let core_options = CoreOptions::new(schema.options());
    let computer = if partition_keys.is_empty() {
        None
    } else {
        Some(PartitionComputer::new(
            partition_keys,
            schema.fields(),
            core_options.partition_default_name(),
            core_options.legacy_partition_name(),
        )?)
    };
    let partition_row = if computer.is_some() {
        BinaryRow::from_serialized_bytes(partition)?
    } else {
        EMPTY_BINARY_ROW
    };
    let partition_path = match &computer {
        Some(computer) => computer.generate_partition_path(&partition_row)?,
        None => String::new(),
    };
    Ok(DeletionVectorLayout {
        relative_bucket_path: crate::spec::relative_bucket_path(
            &partition_path,
            bucket,
            core_options.data_file_path_directory(),
        ),
        table_path: table.location().trim_end_matches('/').to_string(),
        bucket_path: bucket_path(
            &table.data_file_location(),
            computer.as_ref(),
            &partition_row,
            bucket,
        )?,
        index_file_in_data_file_dir: core_options.index_file_in_data_file_dir(),
    })
}

/// Read selected containers in a bucket, retaining every range in each container.
/// `selected_files` filters containers by the data files they reference.
pub(super) async fn read_bucket(
    table: &Table,
    partition: &[u8],
    bucket: i32,
    snapshot: &Snapshot,
    selected_files: Option<&HashSet<String>>,
) -> Result<(IndexMap<String, RoaringBitmap>, Vec<IndexFileMeta>)> {
    let Some(index_manifest_name) = snapshot.index_manifest() else {
        return Ok((IndexMap::new(), Vec::new()));
    };
    let manifest_path = format!(
        "{}/{MANIFEST_DIR}/{}",
        table.location().trim_end_matches('/'),
        index_manifest_name
    );
    let index_entries = IndexManifest::read(table.file_io(), &manifest_path).await?;
    let containers = index_entries
        .into_iter()
        .filter(|entry| {
            entry.kind == FileKind::Add
                && entry.bucket == bucket
                && entry.partition == partition
                && entry.index_file.index_type == DELETION_VECTORS_INDEX_TYPE
                && selected_files.is_none_or(|files| {
                    entry
                        .index_file
                        .deletion_vectors_ranges
                        .as_ref()
                        .is_some_and(|ranges| ranges.keys().any(|name| files.contains(name)))
                })
        })
        .collect::<Vec<_>>();
    let bitmaps = read_containers(table, partition, bucket, &containers).await?;
    let deleted_index_files = containers
        .into_iter()
        .map(|entry| entry.index_file)
        .collect();
    Ok((bitmaps, deleted_index_files))
}

/// Decode already selected containers without rereading their index manifest.
pub(super) async fn read_containers(
    table: &Table,
    partition: &[u8],
    bucket: i32,
    containers: &[IndexManifestEntry],
) -> Result<IndexMap<String, RoaringBitmap>> {
    let layout = deletion_vector_layout(table, partition, bucket)?;
    let mut bitmaps = IndexMap::new();
    for entry in containers {
        let Some(ranges) = entry.index_file.deletion_vectors_ranges.as_ref() else {
            continue;
        };
        let index_path = layout.location().resolve(
            &entry.index_file.file_name,
            entry.index_file.external_path.as_deref(),
        );
        for (data_file_name, meta) in ranges {
            let deletion_file = crate::DeletionFile::new(
                index_path.clone(),
                meta.offset as i64,
                meta.length as i64,
                meta.cardinality,
            );
            let bitmap = DeletionVectorFactory::read(table.file_io(), &deletion_file)
                .await?
                .to_bitmap();
            bitmaps.insert(data_file_name.clone(), bitmap);
        }
    }

    Ok(bitmaps)
}

pub(super) fn file_path(
    table: &Table,
    partition: &[u8],
    bucket: i32,
    file: &IndexFileMeta,
) -> Result<String> {
    Ok(deletion_vector_layout(table, partition, bucket)?
        .location()
        .resolve(&file.file_name, file.external_path.as_deref()))
}

/// Write one immutable container. The caller decides which masks to combine.
pub(super) async fn write_bucket(
    table: &Table,
    partition: &[u8],
    bucket: i32,
    mut bitmaps: IndexMap<String, RoaringBitmap>,
) -> Result<IndexFileMeta> {
    bitmaps.sort_keys();

    let file_name = format!("index-{}-1", Uuid::new_v4());
    let layout = deletion_vector_layout(table, partition, bucket)?;
    let external_path = super::external_path::new_index_external_path(
        table.schema().options(),
        layout.index_file_in_data_file_dir,
        &layout.relative_bucket_path,
        &file_name,
    )?;
    let path = layout
        .location()
        .resolve(&file_name, external_path.as_deref());
    let directory = path.rsplit_once('/').expect("index file has a parent").0;
    table.file_io().mkdirs(directory).await?;

    let mut bytes = vec![DELETION_VECTORS_INDEX_VERSION_V1];
    let mut ranges = IndexMap::new();
    for (data_file_name, bitmap) in bitmaps {
        if bitmap.is_empty() {
            continue;
        }
        let offset = i32::try_from(bytes.len()).map_err(|_| crate::Error::DataInvalid {
            message: "Deletion-vector index file is too large".to_string(),
            source: None,
        })?;
        let deletion_vector = DeletionVector::from_bitmap(bitmap);
        let serialized = deletion_vector.serialize_to_bytes()?;
        let length = i32::from_be_bytes(
            serialized[0..4]
                .try_into()
                .expect("serialized deletion vector has length prefix"),
        );
        ranges.insert(
            data_file_name,
            DeletionVectorMeta {
                offset,
                length,
                cardinality: Some(deletion_vector.cardinality() as i64),
            },
        );
        bytes.extend_from_slice(&serialized);
    }

    let file_size = i64::try_from(bytes.len()).map_err(|_| crate::Error::DataInvalid {
        message: "Deletion-vector index file is too large".to_string(),
        source: None,
    })?;
    let row_count = i32::try_from(ranges.len()).map_err(|_| crate::Error::DataInvalid {
        message: "Deletion-vector index file has too many entries".to_string(),
        source: None,
    })?;
    if let Err(error) = table
        .file_io()
        .new_output(&path)?
        .write(Bytes::from(bytes))
        .await
    {
        let _ = table.file_io().delete_file(&path).await;
        return Err(error);
    }

    Ok(IndexFileMeta {
        index_type: DELETION_VECTORS_INDEX_TYPE.to_string(),
        file_name,
        file_size,
        row_count: i64::from(row_count),
        deletion_vectors_ranges: Some(ranges),
        external_path,
        global_index_meta: None,
    })
}
