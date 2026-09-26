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

//! Row-ID-based update writer for data evolution tables.
//!
//! [`DataEvolutionWriter`] accepts rows to update (identified by `_ROW_ID`) along with
//! new column values, then handles file metadata lookup, row grouping,
//! reading original columns, applying updates, and writing partial-column files.
//!
//! The writer does NOT commit — it returns `Vec<CommitMessage>` that the caller
//! passes to [`TableCommit`](super::TableCommit).
//! This separation allows callers to compose multiple operations into a single commit,
//! similar to Iceberg's Transaction/Action pattern.

use crate::deletion_vector::{DeletionVector, DeletionVectorFactory};
use crate::io::FileIO;
use crate::spec::{
    bucket_path, BinaryRow, CoreOptions, DataField, DataFileMeta, DataType, DeletionVectorMeta,
    FileKind, IndexFileMeta, IndexManifest, PartitionComputer, Snapshot, EMPTY_BINARY_ROW,
};
use crate::table::commit_message::CommitMessage;
use crate::table::data_file_index_writer::FileIndexOptions;
use crate::table::data_file_writer::DataFileWriter;
use crate::table::index_file_path::IndexFileLocation;
use crate::table::source::data_evolution_anchor_file;
use crate::table::stats_filter::group_by_overlapping_row_id;
use crate::table::DataSplitBuilder;
use crate::table::Table;
use crate::Result;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::DataType as ArrowDataType;
use arrow_select::concat::concat_batches;
use arrow_select::interleave::interleave;
use bytes::Bytes;
use futures::TryStreamExt;
use indexmap::IndexMap;
use roaring::RoaringBitmap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

const DELETION_VECTORS_INDEX_TYPE: &str = "DELETION_VECTORS";
const DELETION_VECTORS_INDEX_VERSION_V1: u8 = 1;
const MANIFEST_DIR: &str = "manifest";

/// Engine-agnostic writer for partial-column updates via `_ROW_ID`.
///
/// Usage:
/// 1. Create via [`DataEvolutionWriter::new`] (validates preconditions).
/// 2. Feed matched rows via [`add_matched_batch`](Self::add_matched_batch).
///    Each batch must contain a non-null `_ROW_ID` (Int64) column plus the update columns.
/// 3. Call [`prepare_commit`](Self::prepare_commit) to produce `CommitMessage`s.
/// 4. Commit via [`TableCommit`](super::TableCommit) (caller's responsibility).
///
/// The query engine (DataFusion, custom, etc.) is responsible for:
/// - Parsing the MERGE INTO SQL
/// - Executing the JOIN to find matched rows
/// - Computing new column values
/// - Passing the results as `RecordBatch`es with `_ROW_ID` + update columns
#[must_use = "writer must be used to call prepare_commit()"]
pub struct DataEvolutionWriter {
    table: Table,
    update_columns: Vec<String>,
    write_fields: Vec<DataField>,
    matched_batches: Vec<RecordBatch>,
}

impl DataEvolutionWriter {
    /// Create a new writer for the given table and update columns.
    ///
    /// Validates:
    /// - `data-evolution.enabled = true`
    /// - `row-tracking.enabled = true`
    /// - No primary keys
    /// - Update columns don't include partition keys
    pub fn new(table: &Table, update_columns: Vec<String>) -> Result<Self> {
        let schema = table.schema();
        let core_options = CoreOptions::new(schema.options());

        if !core_options.data_evolution_enabled() {
            return Err(crate::Error::Unsupported {
                message:
                    "MERGE INTO is only supported for tables with 'data-evolution.enabled' = 'true'"
                        .to_string(),
            });
        }
        if !core_options.row_tracking_enabled() {
            return Err(crate::Error::Unsupported {
                message: "MERGE INTO requires 'row-tracking.enabled' = 'true'".to_string(),
            });
        }
        if !schema.trimmed_primary_keys().is_empty() {
            return Err(crate::Error::Unsupported {
                message: "MERGE INTO on data evolution tables does not support primary keys"
                    .to_string(),
            });
        }

        super::data_evolution_fields::validate_write_paths(schema.fields(), &update_columns)?;
        let write_fields =
            super::data_evolution_fields::project_by_paths(schema.fields(), &update_columns)?;
        if !core_options.data_evolution_nested_field_enabled()
            && update_columns
                .iter()
                .any(|column| !schema.fields().iter().any(|field| field.name() == column))
        {
            return Err(crate::Error::DataInvalid {
                message: "Nested data-evolution write paths require data-evolution.nested-field.enabled=true"
                    .to_string(),
                source: None,
            });
        }
        let partition_keys = schema.partition_keys();
        let blob_descriptor_fields = core_options.blob_descriptor_fields();
        for col in &update_columns {
            let top_level = DataEvolutionPartialWriter::top_level_write_name(col, schema.fields());
            if partition_keys.iter().any(|key| key == top_level) {
                return Err(crate::Error::Unsupported {
                    message: format!("Cannot update partition column '{col}' in MERGE INTO"),
                });
            }
            if let Some(field) = schema.fields().iter().find(|f| f.name() == top_level) {
                if field.data_type().is_blob_type() && !blob_descriptor_fields.contains(top_level) {
                    return Err(crate::Error::Unsupported {
                        message: format!(
                            "Cannot update raw-data BLOB column '{col}' in MERGE INTO. \
                             Only BLOB columns listed in 'blob-descriptor-field' can be updated"
                        ),
                    });
                }
            }
        }

        Ok(Self {
            table: table.clone(),
            update_columns,
            write_fields,
            matched_batches: Vec::new(),
        })
    }

    /// Add a batch of matched rows.
    ///
    /// The batch must contain:
    /// - A non-null `_ROW_ID` column (Int64) identifying which rows to update
    /// - One column for each entry in `update_columns` with the new values
    pub fn add_matched_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let row_id_col = row_id_column(&batch)?;
        validate_row_id_not_null(row_id_col)?;
        validate_update_columns(&batch, &self.update_columns)?;

        self.matched_batches.push(batch);
        Ok(())
    }

    /// Scan file metadata, group matched rows by file, read originals,
    /// apply updates, and write partial-column files.
    ///
    /// Returns `CommitMessage`s for the caller to commit via [`TableCommit`](super::TableCommit).
    #[must_use = "commit messages must be passed to TableCommit"]
    pub async fn prepare_commit(self) -> Result<Vec<CommitMessage>> {
        // A row-id update reads the original rows it rewrites.
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;

        let total_matched: usize = self.matched_batches.iter().map(|b| b.num_rows()).sum();
        if total_matched == 0 {
            return Ok(Vec::new());
        }

        // 1. Scan file metadata and build row_id -> file group index.
        //    In data-evolution tables, multiple files can share the same first_row_id
        //    (base file + partial-column files). We must group them so the reader
        //    can merge columns correctly.
        let scan = self.table.new_read_builder().new_scan();
        let plan = scan.plan().await?;

        let mut file_index: Vec<FileRowRange> = Vec::new();
        for split in plan.splits() {
            let partition_bytes = split.partition().to_serialized_bytes();
            let bucket = split.bucket();
            let bucket_path = split.bucket_path().to_string();
            let snapshot_id = split.snapshot_id();
            let total_buckets = split.total_buckets();

            let all_files: Vec<DataFileMeta> = split
                .data_files()
                .iter()
                .filter(|f| f.first_row_id.is_some())
                .cloned()
                .collect();

            let groups = group_by_overlapping_row_id(all_files);
            for group in groups {
                // Compute the overall row_id range for this group.
                // The base file has the widest range; partial-column files share it.
                let first_row_id = group.iter().filter_map(|f| f.first_row_id).min().unwrap();
                let last_row_id = group
                    .iter()
                    .filter_map(|f| f.row_id_range().map(|(_, end)| end))
                    .max()
                    .unwrap();
                // The actual row count is the max among the group (base file's count).
                let row_count = group.iter().map(|f| f.row_count).max().unwrap();

                file_index.push(FileRowRange {
                    first_row_id,
                    last_row_id,
                    row_count,
                    partition: partition_bytes.clone(),
                    bucket,
                    bucket_path: bucket_path.clone(),
                    snapshot_id,
                    total_buckets,
                    files: group,
                });
            }
        }
        file_index.sort_by_key(|f| f.first_row_id);

        if file_index.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "No files with row tracking found in target table".to_string(),
                source: None,
            });
        }

        // 2. Group matched rows by their owning file
        let file_matches = group_matched_rows_by_file(&self.matched_batches, &file_index)?;

        // 3. For each affected file: read original columns, apply updates, write partial files
        let mut writer = DataEvolutionPartialWriter::new(&self.table, self.update_columns.clone())?;

        for (&file_pos, matched_rows) in &file_matches {
            let file_range = &file_index[file_pos];
            let first_row_id = file_range.first_row_id;
            let row_count = file_range.row_count as usize;

            // Read original columns from the entire file group (base + partial-column files).
            let col_refs: Vec<&str> = self.write_fields.iter().map(DataField::name).collect();
            let mut rb = self.table.new_read_builder();
            rb.with_projection(&col_refs)?;
            let read = rb.new_read()?;

            // Base + partial-column files share row-id ranges, so physical
            // row counts overcount the group's logical rows.
            let split = DataSplitBuilder::new()
                .with_snapshot(file_range.snapshot_id)
                .with_partition(BinaryRow::from_serialized_bytes(&file_range.partition)?)
                .with_bucket(file_range.bucket)
                .with_bucket_path(file_range.bucket_path.clone())
                .with_total_buckets(file_range.total_buckets)
                .with_data_files(file_range.files.clone())
                .with_raw_convertible(file_range.files.len() == 1)
                .build()?;

            let stream = read.to_arrow(&[split])?;
            let original_batches: Vec<RecordBatch> = stream.try_collect().await?;

            let original_batch = if original_batches.is_empty() {
                continue;
            } else if original_batches.len() == 1 {
                original_batches.into_iter().next().unwrap()
            } else {
                concat_batches(&original_batches[0].schema(), &original_batches).map_err(|e| {
                    crate::Error::DataInvalid {
                        message: format!("Failed to concat batches: {e}"),
                        source: None,
                    }
                })?
            };

            if original_batch.num_rows() != row_count {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Expected {} rows from file, got {}",
                        row_count,
                        original_batch.num_rows()
                    ),
                    source: None,
                });
            }

            // Keep one physical ROW column per top-level field, even when the
            // update names identify several nested leaves of that ROW.
            let mut sorted_matches: Vec<(usize, usize, usize)> = matched_rows
                .iter()
                .map(|m| (m.offset, m.batch_idx, m.row_idx))
                .collect();
            sorted_matches.sort_by_key(|(offset, _, _)| *offset);
            if sorted_matches.windows(2).any(|rows| rows[0].0 == rows[1].0) {
                return Err(crate::Error::DataInvalid {
                    message: "Multiple updates target the same row ID".to_string(),
                    source: None,
                });
            }
            let new_columns = self
                .write_fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    apply_field_updates(
                        field,
                        field.name(),
                        original_batch.column(index),
                        &self.update_columns,
                        &self.matched_batches,
                        &sorted_matches,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let write_schema = crate::arrow::build_target_arrow_schema(&self.write_fields)?;
            let updated_batch = RecordBatch::try_new(write_schema, new_columns).map_err(|e| {
                crate::Error::DataInvalid {
                    message: format!("Failed to create updated batch: {e}"),
                    source: None,
                }
            })?;

            writer
                .write_partial_batch(
                    file_range.partition.clone(),
                    file_range.bucket,
                    first_row_id,
                    file_range.snapshot_id,
                    updated_batch,
                )
                .await?;
        }

        // 4. Collect commit messages (caller is responsible for committing)
        writer.prepare_commit().await
    }
}

/// One bucket's deletion-vector location, owning the strings the shared resolver
/// borrows. Built once per bucket so the read that merges existing vectors and the
/// write that follows it cannot disagree about where the file goes.
struct DeletionVectorLayout {
    table_path: String,
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

/// Engine-agnostic DELETE writer for data evolution tables.
///
/// DELETE is represented by a deletion-vector index file keyed by the normal
/// anchor file of each data-evolution row-id group. The data files themselves
/// are not rewritten.
#[must_use = "writer must be used to call prepare_commit()"]
pub struct DataEvolutionDeleteWriter {
    table: Table,
    row_ids: Vec<i64>,
}

impl DataEvolutionDeleteWriter {
    pub fn new(table: &Table) -> Result<Self> {
        let schema = table.schema();
        let core_options = CoreOptions::new(schema.options());

        if !core_options.data_evolution_enabled() {
            return Err(crate::Error::Unsupported {
                message:
                    "DELETE is only supported for tables with 'data-evolution.enabled' = 'true'"
                        .to_string(),
            });
        }
        if !core_options.row_tracking_enabled() {
            return Err(crate::Error::Unsupported {
                message: "DELETE requires 'row-tracking.enabled' = 'true'".to_string(),
            });
        }
        if !core_options.deletion_vectors_enabled() {
            return Err(crate::Error::Unsupported {
                message:
                    "DELETE on data evolution tables requires 'deletion-vectors.enabled' = 'true'"
                        .to_string(),
            });
        }
        if !schema.trimmed_primary_keys().is_empty() {
            return Err(crate::Error::Unsupported {
                message: "DELETE on data evolution tables does not support primary keys"
                    .to_string(),
            });
        }

        Ok(Self {
            table: table.clone(),
            row_ids: Vec::new(),
        })
    }

    pub fn add_row_ids<I>(&mut self, row_ids: I) -> Result<()>
    where
        I: IntoIterator<Item = i64>,
    {
        self.row_ids.extend(row_ids);
        Ok(())
    }

    pub fn add_matched_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let row_id_col = row_id_column(&batch)?;
        validate_row_id_not_null(row_id_col)?;
        self.row_ids
            .extend((0..row_id_col.len()).map(|row_idx| row_id_col.value(row_idx)));
        Ok(())
    }

    #[must_use = "commit messages must be passed to TableCommit"]
    pub async fn prepare_commit(mut self) -> Result<Vec<CommitMessage>> {
        // A row-id delete reads the files it rewrites.
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;

        dedup_i64_in_place(&mut self.row_ids);
        if self.row_ids.is_empty() {
            return Ok(Vec::new());
        }

        let snapshot = super::time_travel::resolve_snapshot(&self.table)
            .await?
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "No files with row tracking found in target table".into(),
                source: None,
            })?;
        let scan_table = self.table.copy_with_pinned_snapshot(&snapshot);
        let scan = scan_table
            .new_read_builder()
            .new_scan()
            .with_scan_all_files();
        let plan = scan.plan().await?;
        let mut file_index = Vec::new();

        for split in plan.splits() {
            let partition = split.partition().to_serialized_bytes();
            let bucket = split.bucket();
            let snapshot_id = split.snapshot_id();
            let files = split
                .data_files()
                .iter()
                .filter(|file| file.first_row_id.is_some())
                .cloned()
                .collect::<Vec<_>>();

            for group in group_by_overlapping_row_id(files) {
                let anchor = data_evolution_anchor_file(&group)?;
                let first_row_id =
                    anchor
                        .first_row_id
                        .ok_or_else(|| crate::Error::DataInvalid {
                            message: format!(
                                "Data-evolution anchor file '{}' is missing first_row_id",
                                anchor.file_name
                            ),
                            source: None,
                        })?;
                let last_row_id = first_row_id + anchor.row_count - 1;
                file_index.push(DeleteFileRowRange {
                    first_row_id,
                    last_row_id,
                    partition: partition.clone(),
                    bucket,
                    snapshot_id,
                    anchor_file_name: anchor.file_name.clone(),
                });
            }
        }
        file_index.sort_by_key(|range| range.first_row_id);

        if file_index.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "No files with row tracking found in target table".to_string(),
                source: None,
            });
        }

        let mut deletes_by_bucket: HashMap<(Vec<u8>, i32), BucketDeletePlan> = HashMap::new();
        for row_id in &self.row_ids {
            let (file_pos, file_range) =
                find_delete_owning_file(&file_index, *row_id).ok_or_else(|| {
                    crate::Error::DataInvalid {
                        message: format!("No file found for _ROW_ID {row_id}"),
                        source: None,
                    }
                })?;
            let local_position = u32::try_from(row_id - file_range.first_row_id).map_err(|_| {
                crate::Error::DataInvalid {
                    message: format!(
                        "_ROW_ID {row_id} is too large to encode in a deletion vector"
                    ),
                    source: None,
                }
            })?;

            let key = (file_range.partition.clone(), file_range.bucket);
            let entry = deletes_by_bucket
                .entry(key)
                .or_insert_with(|| BucketDeletePlan {
                    check_from_snapshot: file_range.snapshot_id,
                    deletes_by_anchor: HashMap::new(),
                });
            entry.check_from_snapshot = entry.check_from_snapshot.min(file_range.snapshot_id);
            entry
                .deletes_by_anchor
                .entry(file_index[file_pos].anchor_file_name.clone())
                .or_default()
                .insert(local_position);
        }

        let mut messages = Vec::new();
        for ((partition, bucket), delete_plan) in deletes_by_bucket {
            if let Some(message) = self
                .prepare_bucket_delete_message(partition, bucket, delete_plan, &snapshot)
                .await?
            {
                messages.push(message);
            }
        }

        Ok(messages)
    }

    async fn prepare_bucket_delete_message(
        &self,
        partition: Vec<u8>,
        bucket: i32,
        delete_plan: BucketDeletePlan,
        snapshot: &Snapshot,
    ) -> Result<Option<CommitMessage>> {
        let (mut bitmaps, deleted_index_files) = self
            .read_existing_bucket_deletion_vectors(&partition, bucket, snapshot)
            .await?;
        let mut changed = false;

        for (anchor_file_name, positions) in delete_plan.deletes_by_anchor {
            let bitmap = bitmaps.entry(anchor_file_name).or_default();
            for position in positions {
                changed |= bitmap.insert(position);
            }
        }

        if !changed {
            return Ok(None);
        }

        let new_index_file = self
            .write_deletion_vector_index_file(&partition, bucket, bitmaps)
            .await?;
        let mut message = CommitMessage::new(partition, bucket, vec![]);
        message.check_from_snapshot = Some(delete_plan.check_from_snapshot);
        message.new_index_files = vec![new_index_file];
        message.deleted_index_files = deleted_index_files;
        Ok(Some(message))
    }

    /// Where this bucket's deletion vectors live. A deletion vector is an index
    /// file, so it sits beside the bucket's data files when the table keeps index
    /// files there, and under the table `index/` directory otherwise. Reads and
    /// writes both go through this, so a file written here is found again.
    fn deletion_vector_layout(
        &self,
        partition: &[u8],
        bucket: i32,
    ) -> Result<DeletionVectorLayout> {
        let schema = self.table.schema();
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
        Ok(DeletionVectorLayout {
            table_path: self.table.location().trim_end_matches('/').to_string(),
            bucket_path: bucket_path(
                self.table.location(),
                computer.as_ref(),
                &partition_row,
                bucket,
            )?,
            index_file_in_data_file_dir: core_options.index_file_in_data_file_dir(),
        })
    }

    async fn read_existing_bucket_deletion_vectors(
        &self,
        partition: &[u8],
        bucket: i32,
        snapshot: &Snapshot,
    ) -> Result<(IndexMap<String, RoaringBitmap>, Vec<IndexFileMeta>)> {
        let Some(index_manifest_name) = snapshot.index_manifest() else {
            return Ok((IndexMap::new(), Vec::new()));
        };
        let layout = self.deletion_vector_layout(partition, bucket)?;

        let manifest_path = format!(
            "{}/{MANIFEST_DIR}/{}",
            self.table.location().trim_end_matches('/'),
            index_manifest_name
        );
        let index_entries = IndexManifest::read(self.table.file_io(), &manifest_path).await?;
        let mut bitmaps = IndexMap::new();
        let mut deleted_index_files = Vec::new();

        for entry in index_entries {
            if entry.kind != FileKind::Add
                || entry.bucket != bucket
                || entry.partition != partition
                || entry.index_file.index_type != DELETION_VECTORS_INDEX_TYPE
            {
                continue;
            }
            deleted_index_files.push(entry.index_file.clone());
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
                let bitmap = DeletionVectorFactory::read(self.table.file_io(), &deletion_file)
                    .await?
                    .to_bitmap();
                bitmaps.insert(data_file_name.clone(), bitmap);
            }
        }

        Ok((bitmaps, deleted_index_files))
    }

    async fn write_deletion_vector_index_file(
        &self,
        partition: &[u8],
        bucket: i32,
        mut bitmaps: IndexMap<String, RoaringBitmap>,
    ) -> Result<IndexFileMeta> {
        bitmaps.sort_keys();

        let file_name = format!("index-{}-1", Uuid::new_v4());
        // Write where the reader resolves it, so a deletion vector written here is
        // found again on the next scan.
        let layout = self.deletion_vector_layout(partition, bucket)?;
        let location = layout.location();
        let path = location.resolve(&file_name, None);
        self.table.file_io().mkdirs(&location.directory()).await?;

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
        self.table
            .file_io()
            .new_output(&path)?
            .write(Bytes::from(bytes))
            .await?;

        Ok(IndexFileMeta {
            index_type: DELETION_VECTORS_INDEX_TYPE.to_string(),
            file_name,
            file_size,
            row_count: i64::from(row_count),
            deletion_vectors_ranges: Some(ranges),
            external_path: None,
            global_index_meta: None,
        })
    }
}

struct DeleteFileRowRange {
    first_row_id: i64,
    last_row_id: i64,
    partition: Vec<u8>,
    bucket: i32,
    snapshot_id: i64,
    anchor_file_name: String,
}

struct BucketDeletePlan {
    check_from_snapshot: i64,
    deletes_by_anchor: HashMap<String, HashSet<u32>>,
}

fn find_delete_owning_file(
    file_index: &[DeleteFileRowRange],
    row_id: i64,
) -> Option<(usize, &DeleteFileRowRange)> {
    let pos = file_index.partition_point(|f| f.first_row_id <= row_id);
    if pos == 0 {
        return None;
    }
    let idx = pos - 1;
    let candidate = &file_index[idx];
    if row_id <= candidate.last_row_id {
        Some((idx, candidate))
    } else {
        None
    }
}

fn dedup_i64_in_place(values: &mut Vec<i64>) {
    let mut seen = HashSet::new();
    values.retain(|value| seen.insert(*value));
}

/// Binary search for the file that owns a given row_id.
fn find_owning_file(file_index: &[FileRowRange], row_id: i64) -> Option<(usize, &FileRowRange)> {
    let pos = file_index.partition_point(|f| f.first_row_id <= row_id);
    if pos == 0 {
        return None;
    }
    let idx = pos - 1;
    let candidate = &file_index[idx];
    if row_id <= candidate.last_row_id {
        Some((idx, candidate))
    } else {
        None
    }
}

fn group_matched_rows_by_file(
    matched_batches: &[RecordBatch],
    file_index: &[FileRowRange],
) -> Result<HashMap<usize, Vec<MatchedRow>>> {
    let mut file_matches: HashMap<usize, Vec<MatchedRow>> = HashMap::new();
    let mut seen_updates: HashSet<(usize, usize)> = HashSet::new();

    for (batch_idx, batch) in matched_batches.iter().enumerate() {
        let row_id_col = row_id_column(batch)?;
        validate_row_id_not_null(row_id_col)?;

        for row_idx in 0..batch.num_rows() {
            let row_id = row_id_col.value(row_idx);
            let (file_pos, file_range) =
                find_owning_file(file_index, row_id).ok_or_else(|| crate::Error::DataInvalid {
                    message: format!("No file found for _ROW_ID {row_id}"),
                    source: None,
                })?;

            let offset = (row_id - file_range.first_row_id) as usize;
            if !seen_updates.insert((file_pos, offset)) {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "row_offset {offset} has duplicate UPDATE operations; \
                         this may indicate a many-to-many join in the MERGE source"
                    ),
                    source: None,
                });
            }

            file_matches.entry(file_pos).or_default().push(MatchedRow {
                offset,
                batch_idx,
                row_idx,
            });
        }
    }

    Ok(file_matches)
}

fn row_id_column(batch: &RecordBatch) -> Result<&Int64Array> {
    batch
        .column_by_name("_ROW_ID")
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "Matched batch must contain a '_ROW_ID' column".to_string(),
            source: None,
        })?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "_ROW_ID column must be Int64".to_string(),
            source: None,
        })
}

fn validate_row_id_not_null(row_id_col: &Int64Array) -> Result<()> {
    if row_id_col.null_count() == 0 {
        return Ok(());
    }

    for row_idx in 0..row_id_col.len() {
        if row_id_col.is_null(row_idx) {
            return Err(crate::Error::DataInvalid {
                message: format!("_ROW_ID must not be null at matched row {row_idx}"),
                source: None,
            });
        }
    }

    Ok(())
}

fn validate_update_columns(batch: &RecordBatch, update_columns: &[String]) -> Result<()> {
    for col in update_columns {
        matched_column_index(batch, col)?;
    }

    Ok(())
}

fn matched_column_index(batch: &RecordBatch, col: &str) -> Result<usize> {
    batch
        .schema()
        .index_of(col)
        .map_err(|e| crate::Error::DataInvalid {
            message: format!("Column {col} not found in matched batch: {e}"),
            source: None,
        })
}

fn matched_column(batch: &RecordBatch, col: &str) -> Result<ArrayRef> {
    let idx = matched_column_index(batch, col)?;
    Ok(batch.column(idx).clone())
}

/// Apply matched values to one projected field. The projection may be a ROW
/// containing only the leaves named in `update_columns`; its untouched sibling
/// leaves remain available from the read batch but are absent from this file.
fn apply_field_updates(
    write_field: &DataField,
    path: &str,
    original: &ArrayRef,
    update_columns: &[String],
    matched_batches: &[RecordBatch],
    matches: &[(usize, usize, usize)],
) -> Result<ArrayRef> {
    if update_columns.iter().any(|column| column == path) {
        return interleave_updated_leaf(path, original, matched_batches, matches);
    }

    let DataType::Row(row) = write_field.data_type() else {
        return Err(crate::Error::DataInvalid {
            message: format!("No update value was supplied for '{path}'"),
            source: None,
        });
    };
    let source = original
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!("Expected ROW values for nested update '{path}'"),
            source: None,
        })?;
    let children =
        row.fields()
            .iter()
            .map(|child| {
                let child_path = format!("{path}.{}", child.name());
                let source_child = source.column_by_name(child.name()).ok_or_else(|| {
                    crate::Error::DataInvalid {
                        message: format!("Nested update source is missing '{child_path}'"),
                        source: None,
                    }
                })?;
                apply_field_updates(
                    child,
                    &child_path,
                    source_child,
                    update_columns,
                    matched_batches,
                    matches,
                )
            })
            .collect::<Result<Vec<_>>>()?;
    let ArrowDataType::Struct(fields) =
        crate::arrow::paimon_type_to_arrow(write_field.data_type())?
    else {
        unreachable!("ROW type must map to Arrow Struct")
    };
    let mut valid = (0..original.len())
        .map(|row| source.is_valid(row))
        .collect::<Vec<_>>();
    for &(offset, _, _) in matches {
        valid[offset] = true;
    }
    let struct_array = StructArray::try_new(fields, children, Some(NullBuffer::from(valid)))
        .map_err(|error| crate::Error::DataInvalid {
            message: format!("Failed to build nested update '{path}': {error}"),
            source: None,
        })?;
    Ok(Arc::new(struct_array))
}

/// Gather matched rows into one array, then use a two-array interleave over
/// the file's physical row positions. Casts follow the existing top-level
/// MERGE update behavior.
fn interleave_updated_leaf(
    path: &str,
    original: &ArrayRef,
    matched_batches: &[RecordBatch],
    matches: &[(usize, usize, usize)],
) -> Result<ArrayRef> {
    let mut batch_arrays = Vec::<ArrayRef>::new();
    let mut batch_id_map = HashMap::<usize, usize>::new();
    let mut gather = Vec::with_capacity(matches.len());
    for &(_, batch_idx, row_idx) in matches {
        let array_idx = if let Some(index) = batch_id_map.get(&batch_idx) {
            *index
        } else {
            let value = matched_column(&matched_batches[batch_idx], path)?;
            let value = if value.data_type() == original.data_type() {
                value
            } else {
                arrow_cast::cast(value.as_ref(), original.data_type()).map_err(|error| {
                    crate::Error::DataInvalid {
                        message: format!("Failed to cast column {path}: {error}"),
                        source: None,
                    }
                })?
            };
            let index = batch_arrays.len();
            batch_arrays.push(value);
            batch_id_map.insert(batch_idx, index);
            index
        };
        gather.push((array_idx, row_idx));
    }
    let updates = if gather.len() == 1 {
        batch_arrays[gather[0].0].slice(gather[0].1, 1)
    } else {
        let arrays = batch_arrays
            .iter()
            .map(|array| array.as_ref())
            .collect::<Vec<_>>();
        interleave(&arrays, &gather).map_err(|error| crate::Error::DataInvalid {
            message: format!("Failed to gather update values for {path}: {error}"),
            source: None,
        })?
    };
    let mut indices = Vec::with_capacity(original.len());
    let mut matched = 0;
    for row in 0..original.len() {
        if matched < matches.len() && matches[matched].0 == row {
            indices.push((1, matched));
            matched += 1;
        } else {
            indices.push((0, row));
        }
    }
    interleave(&[original.as_ref(), updates.as_ref()], &indices).map_err(|error| {
        crate::Error::DataInvalid {
            message: format!("Failed to interleave column {path}: {error}"),
            source: None,
        }
    })
}

struct FileRowRange {
    first_row_id: i64,
    last_row_id: i64,
    row_count: i64,
    partition: Vec<u8>,
    bucket: i32,
    bucket_path: String,
    snapshot_id: i64,
    total_buckets: i32,
    /// All files in this row-id group (base file + partial-column files).
    files: Vec<DataFileMeta>,
}

struct MatchedRow {
    offset: usize,
    batch_idx: usize,
    row_idx: usize,
}

// ---------------------------------------------------------------------------
// DataEvolutionPartialWriter — writes partial-column parquet files for data evolution
// ---------------------------------------------------------------------------

/// Key: (partition_bytes, bucket, first_row_id)
type WriterBaseKey = (Vec<u8>, i32, i64);
/// Key: (partition_bytes, bucket, first_row_id, file kind)
type WriterKey = (Vec<u8>, i32, i64, PartialFileKind);
type PartialCommitGroup = (Option<i64>, Vec<DataFileMeta>);

/// Writer for data evolution partial-column files.
///
/// Unlike [`TableWrite`](super::TableWrite) which writes full-row files for append-only tables,
/// `DataEvolutionPartialWriter` writes partial-column files used by MERGE INTO on data evolution tables.
/// Each output file contains only the updated columns and shares the same `first_row_id` range
/// as the original file, allowing the reader to merge columns at read time.
///
/// Produces files containing only the specified `write_columns`, with
/// `file_source = APPEND (0)`, caller-supplied `first_row_id`, and `write_cols`.
/// VECTOR columns use dedicated `*.vector.<format>` files when `vector.file.format`
/// is configured.
pub(crate) struct DataEvolutionPartialWriter {
    file_io: FileIO,
    table_location: String,
    partition_computer: PartitionComputer,
    partition_keys: Vec<String>,
    schema_id: i64,
    file_compression: String,
    file_compression_zstd_level: i32,
    write_buffer_size: i64,
    format_options: HashMap<String, String>,
    write_sets: Vec<PartialWriteSet>,
    /// Writers keyed by (partition_bytes, bucket, first_row_id, file kind).
    writers: HashMap<WriterKey, DataFileWriter>,
    check_from_snapshots: HashMap<WriterBaseKey, i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum PartialFileKind {
    Normal,
    Vector,
}

#[derive(Clone)]
struct PartialWriteSet {
    kind: PartialFileKind,
    target_file_size: i64,
    file_format: String,
    write_fields: Vec<DataField>,
    write_columns: Vec<String>,
    column_indices: Vec<usize>,
    schema: Arc<arrow_schema::Schema>,
    file_index_options: Option<Arc<FileIndexOptions>>,
}

impl DataEvolutionPartialWriter {
    /// Create a new writer for partial-column data evolution files.
    ///
    /// `write_columns` specifies which table columns this write covers (the SET targets).
    pub fn new(table: &Table, write_columns: Vec<String>) -> Result<Self> {
        let schema = table.schema();
        let core_options = CoreOptions::new(schema.options());

        if !core_options.data_evolution_enabled() {
            return Err(crate::Error::Unsupported {
                message: "DataEvolutionPartialWriter requires data-evolution.enabled = true"
                    .to_string(),
            });
        }

        let partition_keys: Vec<String> = schema.partition_keys().to_vec();
        let fields = schema.fields();
        let mut write_sets = Self::build_write_sets(&write_columns, fields, &core_options)?;
        let file_index_options = FileIndexOptions::parse(schema.options(), fields)?;
        for write_set in &mut write_sets {
            write_set.file_index_options = file_index_options
                .as_ref()
                .and_then(|options| options.project_to_fields(&write_set.write_fields))
                .map(Arc::new);
            if write_set.kind == PartialFileKind::Vector && write_set.file_index_options.is_some() {
                return Err(crate::Error::Unsupported {
                    message: "FileIndex generation does not support dedicated VECTOR partial files"
                        .to_string(),
                });
            }
        }
        let partition_computer = PartitionComputer::new(
            &partition_keys,
            fields,
            core_options.partition_default_name(),
            core_options.legacy_partition_name(),
        )?;

        Ok(Self {
            file_io: table.file_io().clone(),
            table_location: table.location().to_string(),
            partition_computer,
            partition_keys,
            schema_id: schema.id(),
            file_compression: core_options.file_compression().to_string(),
            file_compression_zstd_level: core_options.file_compression_zstd_level(),
            write_buffer_size: core_options.write_parquet_buffer_size(),
            format_options: schema.options().clone(),
            write_sets,
            writers: HashMap::new(),
            check_from_snapshots: HashMap::new(),
        })
    }

    fn build_write_sets(
        write_columns: &[String],
        fields: &[DataField],
        core_options: &CoreOptions<'_>,
    ) -> Result<Vec<PartialWriteSet>> {
        let vector_file_format = core_options.vector_file_format();
        if !core_options.data_evolution_nested_field_enabled()
            && write_columns
                .iter()
                .any(|column| !fields.iter().any(|field| field.name() == column))
        {
            return Err(crate::Error::DataInvalid {
                message: "Nested data-evolution write paths require data-evolution.nested-field.enabled=true"
                    .to_string(),
                source: None,
            });
        }
        super::data_evolution_fields::validate_write_paths(fields, write_columns)?;
        let projected = super::data_evolution_fields::project_by_paths(fields, write_columns)?;
        let mut normal_fields = Vec::new();
        let mut normal_indices = Vec::new();
        let mut vector_fields = Vec::new();
        let mut vector_indices = Vec::new();

        for (idx, field) in projected.into_iter().enumerate() {
            if vector_file_format.is_some() && matches!(field.data_type(), DataType::Vector(_)) {
                vector_fields.push(field);
                vector_indices.push(idx);
            } else {
                normal_fields.push(field);
                normal_indices.push(idx);
            }
        }

        let normal_columns = write_columns
            .iter()
            .filter(|path| {
                normal_fields
                    .iter()
                    .any(|field| field.name() == Self::top_level_write_name(path, fields))
            })
            .cloned()
            .collect::<Vec<_>>();
        let vector_columns = write_columns
            .iter()
            .filter(|path| {
                vector_fields
                    .iter()
                    .any(|field| field.name() == Self::top_level_write_name(path, fields))
            })
            .cloned()
            .collect::<Vec<_>>();

        let mut write_sets = Vec::new();
        if !normal_fields.is_empty() {
            let schema = crate::arrow::build_target_arrow_schema(&normal_fields)?;
            write_sets.push(PartialWriteSet {
                kind: PartialFileKind::Normal,
                target_file_size: core_options.target_file_size(),
                file_format: core_options.file_format().to_string(),
                write_fields: normal_fields,
                write_columns: normal_columns,
                column_indices: normal_indices,
                schema,
                file_index_options: None,
            });
        }

        if !vector_fields.is_empty() {
            let schema = crate::arrow::build_target_arrow_schema(&vector_fields)?;
            let vector_file_format = vector_file_format.expect("vector fields require a format");
            write_sets.push(PartialWriteSet {
                kind: PartialFileKind::Vector,
                target_file_size: core_options.vector_target_file_size(),
                file_format: format!("vector.{vector_file_format}"),
                write_fields: vector_fields,
                write_columns: vector_columns,
                column_indices: vector_indices,
                schema,
                file_index_options: None,
            });
        }

        Ok(write_sets)
    }

    fn top_level_write_name<'a>(path: &'a str, fields: &[DataField]) -> &'a str {
        if fields.iter().any(|field| field.name() == path) {
            path
        } else {
            path.split_once('.').map_or(path, |(head, _)| head)
        }
    }

    /// Write a partial-column batch for a specific partition, bucket, and row ID range.
    ///
    /// The `batch` must contain only the columns specified in `write_columns`.
    /// `first_row_id` must match the original file's `first_row_id` for the affected rows.
    pub async fn write_partial_batch(
        &mut self,
        partition_bytes: Vec<u8>,
        bucket: i32,
        first_row_id: i64,
        check_from_snapshot: i64,
        batch: RecordBatch,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let base_key = (partition_bytes.clone(), bucket, first_row_id);
        self.check_from_snapshots
            .entry(base_key.clone())
            .and_modify(|snapshot| *snapshot = (*snapshot).min(check_from_snapshot))
            .or_insert(check_from_snapshot);

        let partition_path = if self.partition_keys.is_empty() {
            String::new()
        } else {
            let row = BinaryRow::from_serialized_bytes(&partition_bytes)?;
            self.partition_computer.generate_partition_path(&row)?
        };

        for write_set in self.write_sets.clone() {
            let key = (
                partition_bytes.clone(),
                bucket,
                first_row_id,
                write_set.kind,
            );
            if !self.writers.contains_key(&key) {
                let writer = DataFileWriter::new(
                    self.file_io.clone(),
                    self.table_location.clone(),
                    partition_path.clone(),
                    bucket,
                    self.schema_id,
                    write_set.target_file_size,
                    self.file_compression.clone(),
                    self.file_compression_zstd_level,
                    self.write_buffer_size,
                    write_set.file_format.clone(),
                    write_set.write_fields.clone(),
                    self.format_options.clone(),
                    Some(0), // file_source: APPEND
                    Some(first_row_id),
                    Some(write_set.write_columns.clone()),
                )
                .with_file_index(write_set.file_index_options.clone());
                self.writers.insert(key.clone(), writer);
            }

            let projected_batch = Self::project_batch(&batch, &write_set)?;
            let writer = self.writers.get_mut(&key).unwrap();
            writer.write(&projected_batch).await?;
        }

        Ok(())
    }

    fn project_batch(batch: &RecordBatch, write_set: &PartialWriteSet) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = write_set
            .column_indices
            .iter()
            .map(|&idx| batch.column(idx).clone())
            .collect();
        RecordBatch::try_new(write_set.schema.clone(), columns).map_err(|e| {
            crate::Error::DataInvalid {
                message: format!(
                    "Failed to project data-evolution columns {:?}: {e}",
                    write_set.write_columns
                ),
                source: None,
            }
        })
    }

    /// Close all writers and collect CommitMessages for use with TableCommit.
    pub async fn prepare_commit(&mut self) -> Result<Vec<CommitMessage>> {
        let writers: Vec<(WriterKey, DataFileWriter)> = self.writers.drain().collect();
        let check_from_snapshots = std::mem::take(&mut self.check_from_snapshots);

        let futures: Vec<_> = writers
            .into_iter()
            .map(|(key, mut writer)| {
                let base_key = (key.0.clone(), key.1, key.2);
                let check_from_snapshot = check_from_snapshots.get(&base_key).copied();
                async move {
                    let files = writer.prepare_commit().await?;
                    let (partition_bytes, bucket, _first_row_id, _kind) = key;
                    Ok::<_, crate::Error>((partition_bytes, bucket, check_from_snapshot, files))
                }
            })
            .collect();

        let results = futures::future::try_join_all(futures).await?;

        // Group files by (partition, bucket) since multiple first_row_ids may share the same partition/bucket
        let mut grouped: HashMap<(Vec<u8>, i32), PartialCommitGroup> = HashMap::new();
        for (partition_bytes, bucket, check_from_snapshot, files) in results {
            let entry = grouped
                .entry((partition_bytes, bucket))
                .or_insert_with(|| (None, Vec::new()));
            if let Some(check_from_snapshot) = check_from_snapshot {
                entry.0 = Some(entry.0.map_or(check_from_snapshot, |snapshot| {
                    snapshot.min(check_from_snapshot)
                }));
            }
            entry.1.extend(files);
        }

        let mut messages = Vec::new();
        for ((partition_bytes, bucket), (check_from_snapshot, files)) in grouped {
            if !files.is_empty() {
                let mut message = CommitMessage::new(partition_bytes, bucket, files);
                message.check_from_snapshot = check_from_snapshot;
                messages.push(message);
            }
        }
        Ok(messages)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        DataField, DataType, FloatType, IntType, RowType, Schema, TableSchema, VarCharType,
        VectorType,
    };
    use arrow_array::{Int32Array, StringArray, StructArray};
    use arrow_buffer::NullBuffer;
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn make_test_file_meta(
        file_name: &str,
        row_count: i64,
        first_row_id: Option<i64>,
        max_seq: i64,
        write_cols: Option<Vec<String>>,
    ) -> DataFileMeta {
        use crate::spec::stats::BinaryTableStats;
        let empty_stats = BinaryTableStats::empty();
        DataFileMeta {
            file_name: file_name.to_string(),
            file_size: 0,
            row_count,
            min_key: vec![],
            max_key: vec![],
            key_stats: empty_stats.clone(),
            value_stats: empty_stats,
            min_sequence_number: 0,
            max_sequence_number: max_seq,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: Some(0),
            value_stats_cols: None,
            external_path: None,
            first_row_id,
            write_cols,
            column_max_sequence_numbers: None,
        }
    }

    fn test_data_evolution_schema() -> TableSchema {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .column("value", DataType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_vector_data_evolution_schema(vector_file_format: &str) -> TableSchema {
        let vector_type = DataType::Vector(
            VectorType::try_new(true, 2, DataType::Float(FloatType::new())).unwrap(),
        );
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .column("embedding", vector_type)
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option("vector.file.format", vector_file_format)
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn test_nested_data_evolution_schema() -> TableSchema {
        let profile = DataType::Row(RowType::new(vec![
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            ),
            DataField::new(2, "age".to_string(), DataType::Int(IntType::new())),
        ]));
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("profile", profile)
            .option("bucket", "-1")
            .option("data-evolution.enabled", "true")
            .option("data-evolution.nested-field.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn nested_profile_batch(
        fields: &[DataField],
        columns: Vec<ArrayRef>,
        valid: Vec<bool>,
    ) -> RecordBatch {
        let schema = crate::arrow::build_target_arrow_schema(fields).unwrap();
        let ArrowDataType::Struct(children) = schema.field(0).data_type() else {
            panic!("expected profile ROW")
        };
        let profile =
            StructArray::try_new(children.clone(), columns, Some(NullBuffer::from(valid))).unwrap();
        RecordBatch::try_new(schema, vec![Arc::new(profile)]).unwrap()
    }

    fn full_nested_batch(table: &Table) -> RecordBatch {
        let profile = nested_profile_batch(
            &table.schema().fields()[1..2],
            vec![
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])),
                Arc::new(Int32Array::from(vec![Some(10), Some(20), None])),
            ],
            vec![true, true, false],
        );
        let schema = crate::arrow::build_target_arrow_schema(table.schema().fields()).unwrap();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                profile.column(0).clone(),
            ],
        )
        .unwrap()
    }

    async fn commit_full_nested_batch(table: &Table) {
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&full_nested_batch(table))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        builder.new_commit().commit(messages).await.unwrap();
    }

    async fn commit_partial_nested_batch(
        table: &Table,
        path: &str,
        values: ArrayRef,
        valid: Vec<bool>,
        check_from_snapshot: i64,
    ) -> DataFileMeta {
        let fields = super::super::data_evolution_fields::project_by_paths(
            table.schema().fields(),
            &[path.to_string()],
        )
        .unwrap();
        let batch = nested_profile_batch(&fields, vec![values], valid);
        let mut writer = DataEvolutionPartialWriter::new(table, vec![path.to_string()]).unwrap();
        writer
            .write_partial_batch(
                crate::spec::EMPTY_SERIALIZED_ROW.clone(),
                0,
                0,
                check_from_snapshot,
                batch,
            )
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let file = messages[0].new_files[0].clone();
        assert_eq!(file.write_cols, Some(vec![path.to_string()]));
        table
            .new_write_builder()
            .new_commit()
            .commit(messages)
            .await
            .unwrap();
        file
    }

    #[tokio::test]
    async fn nested_partial_parquet_files_merge_latest_children() {
        let file_io = test_file_io();
        let path = "memory:/test_de_nested_partial";
        setup_dirs(&file_io, path).await;
        let table = Table::new(
            file_io,
            Identifier::new("default", "test_de_nested_partial"),
            path.to_string(),
            test_nested_data_evolution_schema(),
            None,
        );
        commit_full_nested_batch(&table).await;
        let age_file = commit_partial_nested_batch(
            &table,
            "profile.age",
            Arc::new(Int32Array::from(vec![Some(11), Some(21), None])),
            vec![true, true, false],
            1,
        )
        .await;
        assert_eq!(age_file.first_row_id, Some(0));
        commit_partial_nested_batch(
            &table,
            "profile.name",
            Arc::new(StringArray::from(vec![Some("ALICE"), Some("BOB"), None])),
            vec![true, true, false],
            2,
        )
        .await;

        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        let batches = table
            .new_read_builder()
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();
        let mut actual = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let profiles = batch
                .column(1)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let names = profiles
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let ages = profiles
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                actual.push((
                    ids.value(row),
                    profiles.is_valid(row).then(|| names.value(row).to_string()),
                    profiles.is_valid(row).then(|| ages.value(row)),
                ));
            }
        }
        actual.sort_by_key(|row| row.0);
        assert_eq!(
            actual,
            vec![
                (1, Some("ALICE".to_string()), Some(11)),
                (2, Some("BOB".to_string()), Some(21)),
                (3, None, None),
            ]
        );
    }

    #[tokio::test]
    async fn row_id_update_writes_two_nested_leaves_and_revives_null_parent() {
        let file_io = test_file_io();
        let path = "memory:/test_de_nested_row_id_update";
        setup_dirs(&file_io, path).await;
        let table = Table::new(
            file_io,
            Identifier::new("default", "test_de_nested_row_id_update"),
            path.to_string(),
            test_nested_data_evolution_schema(),
            None,
        );
        commit_full_nested_batch(&table).await;

        let matched = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("_ROW_ID", ArrowDataType::Int64, false),
                ArrowField::new("profile.age", ArrowDataType::Int32, true),
                ArrowField::new("profile.name", ArrowDataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![0, 2])),
                Arc::new(Int32Array::from(vec![Some(99), Some(33)])),
                Arc::new(StringArray::from(vec![Some("ALICE"), Some("new")])),
            ],
        )
        .unwrap();
        let mut writer =
            DataEvolutionWriter::new(&table, vec!["profile.age".into(), "profile.name".into()])
                .unwrap();
        writer.add_matched_batch(matched).unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(
            messages[0].new_files[0].write_cols,
            Some(vec!["profile.age".into(), "profile.name".into()])
        );
        table
            .new_write_builder()
            .new_commit()
            .commit(messages)
            .await
            .unwrap();

        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        let batches = table
            .new_read_builder()
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();
        let mut actual = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let profiles = batch
                .column(1)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let names = profiles
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let ages = profiles
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                actual.push((
                    ids.value(row),
                    names.value(row).to_string(),
                    ages.value(row),
                ));
                assert!(profiles.is_valid(row));
            }
        }
        actual.sort_by_key(|row| row.0);
        assert_eq!(
            actual,
            vec![
                (1, "ALICE".into(), 99),
                (2, "bob".into(), 20),
                (3, "new".into(), 33),
            ]
        );
    }

    #[tokio::test]
    async fn nested_projection_uses_sibling_nullness_and_masks_null_source_parent() {
        let file_io = test_file_io();
        let path = "memory:/test_de_nested_projected_nullness";
        setup_dirs(&file_io, path).await;
        let table = Table::new(
            file_io,
            Identifier::new("default", "test_de_nested_projected_nullness"),
            path.to_string(),
            test_nested_data_evolution_schema(),
            None,
        );
        commit_full_nested_batch(&table).await;
        // Row 0's newest age file has a NULL parent. Its older age must not
        // reappear, although the older name keeps the composed parent valid.
        // Row 2 has the inverse: only the new age parent is valid.
        commit_partial_nested_batch(
            &table,
            "profile.age",
            Arc::new(Int32Array::from(vec![Some(99), Some(21), Some(30)])),
            vec![false, true, true],
            1,
        )
        .await;

        for (path, expected_parent, expected_values) in [
            (
                "profile.name",
                vec![true, true, true],
                vec![Some("alice"), Some("bob"), None],
            ),
            (
                "profile.age",
                vec![true, true, true],
                vec![None, Some("21"), Some("30")],
            ),
        ] {
            let projection = super::super::data_evolution_fields::project_by_paths(
                table.schema().fields(),
                &[path.to_string()],
            )
            .unwrap();
            let mut builder = table.new_read_builder();
            builder.with_read_type(projection);
            let plan = builder.new_scan().plan().await.unwrap();
            let batches = builder
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect::<Vec<RecordBatch>>()
                .await
                .unwrap();
            let mut actual = Vec::new();
            for batch in batches {
                let parent = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    let value = if parent.column(0).is_null(row) {
                        None
                    } else if path.ends_with("name") {
                        Some(
                            parent
                                .column(0)
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap()
                                .value(row)
                                .to_string(),
                        )
                    } else {
                        Some(
                            parent
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap()
                                .value(row)
                                .to_string(),
                        )
                    };
                    actual.push((parent.is_valid(row), value));
                }
            }
            assert_eq!(
                actual,
                expected_parent
                    .into_iter()
                    .zip(
                        expected_values
                            .into_iter()
                            .map(|value| value.map(str::to_string))
                    )
                    .collect::<Vec<_>>()
            );
        }
    }

    #[tokio::test]
    async fn projected_new_deep_leaf_preserves_both_parent_null_buffers() {
        let file_io = test_file_io();
        let path = "memory:/test_de_new_deep_leaf";
        setup_dirs(&file_io, path).await;
        let old_schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "profile",
                DataType::Row(RowType::new(vec![
                    DataField::new(
                        0,
                        "sub".into(),
                        DataType::Row(RowType::new(vec![DataField::new(
                            0,
                            "existing".into(),
                            DataType::Int(IntType::new()),
                        )])),
                    ),
                    DataField::new(
                        0,
                        "other".into(),
                        DataType::VarChar(VarCharType::string_type()),
                    ),
                ])),
            )
            .option("bucket", "-1")
            .option("data-evolution.enabled", "true")
            .option("data-evolution.nested-field.enabled", "true")
            .option("row-tracking.enabled", "true")
            .build()
            .unwrap();
        let old_table_schema = TableSchema::new(0, &old_schema);
        let old_table = Table::new(
            file_io.clone(),
            Identifier::new("default", "test_de_new_deep_leaf"),
            path.to_string(),
            old_table_schema.clone(),
            None,
        );
        let arrow_schema =
            crate::arrow::build_target_arrow_schema(old_table.schema().fields()).unwrap();
        let ArrowDataType::Struct(profile_fields) = arrow_schema.field(1).data_type() else {
            panic!("profile must be ROW")
        };
        let ArrowDataType::Struct(sub_fields) = profile_fields[0].data_type() else {
            panic!("sub must be ROW")
        };
        let sub = StructArray::try_new(
            sub_fields.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(10), None]))],
            Some(NullBuffer::from(vec![true, false])),
        )
        .unwrap();
        let profile = StructArray::try_new(
            profile_fields.clone(),
            vec![
                Arc::new(sub),
                Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
            ],
            Some(NullBuffer::from(vec![true, true])),
        )
        .unwrap();
        let old_batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(Int32Array::from(vec![1, 2])), Arc::new(profile)],
        )
        .unwrap();
        let builder = old_table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow_batch(&old_batch).await.unwrap();
        builder
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();

        let schema_path = old_table.schema_manager().schema_path(0);
        file_io
            .mkdirs(schema_path.rsplit_once('/').unwrap().0)
            .await
            .unwrap();
        file_io
            .new_output(&schema_path)
            .unwrap()
            .write(Bytes::from(serde_json::to_vec(&old_table_schema).unwrap()))
            .await
            .unwrap();

        let mut new_fields = old_schema.fields().to_vec();
        let DataType::Row(profile_type) = new_fields[1].data_type() else {
            panic!("profile must be ROW")
        };
        let mut profile_children = profile_type.fields().to_vec();
        let DataType::Row(sub_type) = profile_children[0].data_type() else {
            panic!("sub must be ROW")
        };
        let mut sub_children = sub_type.fields().to_vec();
        sub_children.push(DataField::new(
            old_table_schema.highest_field_id() + 1,
            "added".into(),
            DataType::Int(IntType::new()),
        ));
        profile_children[0] = super::super::data_evolution_fields::field_with_type(
            &profile_children[0],
            DataType::Row(RowType::with_nullable(true, sub_children)),
        );
        new_fields[1] = super::super::data_evolution_fields::field_with_type(
            &new_fields[1],
            DataType::Row(RowType::with_nullable(true, profile_children)),
        );
        let new_schema = old_schema
            .copy(RowType::with_nullable(false, new_fields))
            .unwrap();
        let current = Table::new(
            file_io,
            Identifier::new("default", "test_de_new_deep_leaf"),
            path.to_string(),
            TableSchema::new(1, &new_schema),
            None,
        );
        let projection = super::super::data_evolution_fields::project_by_paths(
            current.schema().fields(),
            &["profile.sub.added".to_string()],
        )
        .unwrap();
        let mut read_builder = current.new_read_builder();
        read_builder.with_read_type(projection);
        let plan = read_builder.new_scan().plan().await.unwrap();
        let batches = read_builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect::<Vec<RecordBatch>>()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let profile = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let sub = profile
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let added = sub.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(profile.is_valid(0));
        assert!(profile.is_valid(1));
        assert!(sub.is_valid(0));
        assert!(sub.is_null(1));
        assert!(added.is_null(0));
        assert!(added.is_null(1));
    }

    fn test_table(file_io: &FileIO, table_path: &str) -> Table {
        Table::new(
            file_io.clone(),
            Identifier::new("default", "test_de_table"),
            table_path.to_string(),
            test_data_evolution_schema(),
            None,
        )
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

    fn make_partial_batch(names: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "name",
            ArrowDataType::Utf8,
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))]).unwrap()
    }

    fn make_partial_name_vector_batch(names: Vec<&str>, vectors: Vec<Vec<f32>>) -> RecordBatch {
        use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};

        let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
        let mut builder =
            FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(element_field.clone());
        for vector in vectors {
            assert_eq!(vector.len(), 2);
            for value in vector {
                builder.values().append_value(value);
            }
            builder.append(true);
        }
        let embedding = builder.finish();

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("name", ArrowDataType::Utf8, true),
            ArrowField::new(
                "embedding",
                ArrowDataType::FixedSizeList(element_field, 2),
                true,
            ),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(names)), Arc::new(embedding)],
        )
        .unwrap()
    }

    fn make_matched_batch(row_ids: Vec<Option<i64>>, names: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("_ROW_ID", ArrowDataType::Int64, true),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(row_ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    fn make_matched_batch_with_schema(
        fields: Vec<ArrowField>,
        columns: Vec<ArrayRef>,
    ) -> RecordBatch {
        RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap()
    }

    fn test_file_index() -> Vec<FileRowRange> {
        vec![FileRowRange {
            first_row_id: 10,
            last_row_id: 12,
            row_count: 3,
            partition: vec![],
            bucket: 0,
            bucket_path: String::new(),
            snapshot_id: 1,
            total_buckets: 1,
            files: vec![make_test_file_meta("base.parquet", 3, Some(10), 1, None)],
        }]
    }

    #[tokio::test]
    async fn test_write_partial_column_file() {
        let file_io = test_file_io();
        let table_path = "memory:/test_de_write";
        setup_dirs(&file_io, table_path).await;

        let table = test_table(&file_io, table_path);
        let mut writer = DataEvolutionPartialWriter::new(&table, vec!["name".to_string()]).unwrap();

        let batch = make_partial_batch(vec!["alice", "bob", "charlie"]);
        writer
            .write_partial_batch(vec![], 0, 0, 7, batch)
            .await
            .unwrap();

        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].new_files.len(), 1);
        assert_eq!(messages[0].check_from_snapshot, Some(7));

        let meta = &messages[0].new_files[0];
        assert_eq!(meta.row_count, 3);
        assert_eq!(meta.first_row_id, Some(0));
        assert_eq!(meta.write_cols, Some(vec!["name".to_string()]));
        assert_eq!(meta.file_source, Some(0));
    }

    #[tokio::test]
    async fn test_write_partial_vector_column_uses_dedicated_file() {
        let file_io = test_file_io();
        let table_path = "memory:/test_de_write_vector";
        setup_dirs(&file_io, table_path).await;

        let table = Table::new(
            file_io,
            Identifier::new("default", "test_de_vector_table"),
            table_path.to_string(),
            test_vector_data_evolution_schema("parquet"),
            None,
        );
        let mut writer = DataEvolutionPartialWriter::new(
            &table,
            vec!["name".to_string(), "embedding".to_string()],
        )
        .unwrap();

        let batch = make_partial_name_vector_batch(
            vec!["alice", "bob"],
            vec![vec![1.0, 0.0], vec![0.0, 1.0]],
        );
        writer
            .write_partial_batch(vec![], 0, 42, 7, batch)
            .await
            .unwrap();

        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].new_files.len(), 2);
        assert_eq!(messages[0].check_from_snapshot, Some(7));

        let normal_file = messages[0]
            .new_files
            .iter()
            .find(|file| !file.file_name.contains(".vector."))
            .unwrap();
        let vector_file = messages[0]
            .new_files
            .iter()
            .find(|file| file.file_name.ends_with(".vector.parquet"))
            .unwrap();

        assert_eq!(normal_file.first_row_id, Some(42));
        assert_eq!(normal_file.write_cols, Some(vec!["name".to_string()]));
        assert_eq!(vector_file.first_row_id, Some(42));
        assert_eq!(vector_file.write_cols, Some(vec!["embedding".to_string()]));
    }

    #[tokio::test]
    async fn test_different_first_row_id_creates_separate_files() {
        let file_io = test_file_io();
        let table_path = "memory:/test_de_write_multi";
        setup_dirs(&file_io, table_path).await;

        let table = test_table(&file_io, table_path);
        let mut writer = DataEvolutionPartialWriter::new(&table, vec!["name".to_string()]).unwrap();

        // Two batches with different first_row_id should produce two files
        let batch1 = make_partial_batch(vec!["alice", "bob"]);
        writer
            .write_partial_batch(vec![], 0, 0, 9, batch1)
            .await
            .unwrap();

        let batch2 = make_partial_batch(vec!["charlie"]);
        writer
            .write_partial_batch(vec![], 0, 100, 8, batch2)
            .await
            .unwrap();

        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].new_files.len(), 2);
        assert_eq!(messages[0].check_from_snapshot, Some(8));

        let mut files = messages[0].new_files.clone();
        files.sort_by_key(|f| f.first_row_id);
        assert_eq!(files[0].first_row_id, Some(0));
        assert_eq!(files[0].row_count, 2);
        assert_eq!(files[1].first_row_id, Some(100));
        assert_eq!(files[1].row_count, 1);
    }

    #[test]
    fn test_add_matched_batch_rejects_null_row_id() {
        let file_io = test_file_io();
        let table = test_table(&file_io, "memory:/test_de_add_batch");
        let mut writer = DataEvolutionWriter::new(&table, vec!["name".to_string()]).unwrap();

        let err = writer
            .add_matched_batch(make_matched_batch(
                vec![Some(10), None],
                vec!["alice", "bob"],
            ))
            .err()
            .unwrap();

        assert!(
            matches!(err, crate::Error::DataInvalid { message, .. } if message.contains("_ROW_ID must not be null"))
        );
    }

    #[test]
    fn test_add_matched_batch_rejects_missing_update_column() {
        let file_io = test_file_io();
        let table = test_table(&file_io, "memory:/test_de_add_batch_missing_col");
        let mut writer = DataEvolutionWriter::new(&table, vec!["value".to_string()]).unwrap();

        let err = writer
            .add_matched_batch(make_matched_batch(vec![Some(10)], vec!["alice"]))
            .err()
            .unwrap();

        assert!(
            matches!(err, crate::Error::DataInvalid { message, .. } if message.contains("Column value not found"))
        );
    }

    #[test]
    fn test_group_matched_rows_rejects_null_row_id() {
        let batches = vec![make_matched_batch(
            vec![Some(10), None],
            vec!["alice", "bob"],
        )];

        let err = group_matched_rows_by_file(&batches, &test_file_index())
            .err()
            .unwrap();

        assert!(
            matches!(err, crate::Error::DataInvalid { message, .. } if message.contains("_ROW_ID must not be null"))
        );
    }

    #[test]
    fn test_group_matched_rows_rejects_duplicate_update() {
        let batches = vec![make_matched_batch(
            vec![Some(10), Some(10)],
            vec!["alice", "ALICE"],
        )];

        let err = group_matched_rows_by_file(&batches, &test_file_index())
            .err()
            .unwrap();

        assert!(
            matches!(err, crate::Error::DataInvalid { message, .. } if message.contains("duplicate UPDATE"))
        );
    }

    #[test]
    fn test_group_matched_rows_rejects_duplicate_update_across_batches() {
        let batches = vec![
            make_matched_batch(vec![Some(10)], vec!["alice"]),
            make_matched_batch(vec![Some(10)], vec!["ALICE"]),
        ];

        let err = group_matched_rows_by_file(&batches, &test_file_index())
            .err()
            .unwrap();

        assert!(
            matches!(err, crate::Error::DataInvalid { message, .. } if message.contains("duplicate UPDATE"))
        );
    }

    #[test]
    fn test_matched_column_uses_batch_schema() {
        let batch = make_matched_batch_with_schema(
            vec![
                ArrowField::new("value", ArrowDataType::Int32, true),
                ArrowField::new("name", ArrowDataType::Utf8, true),
            ],
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![20])),
                Arc::new(StringArray::from(vec!["bob"])),
            ],
        );

        let column = matched_column(&batch, "name").unwrap();
        let names = column.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(names.value(0), "bob");
    }

    #[test]
    fn test_find_owning_file_with_grouped_ranges() {
        // Simulate a file group: base file (3 cols, 100 rows) + partial file (1 col, 100 rows)
        // sharing the same first_row_id range [0, 99].
        let base_file = make_test_file_meta("base-0.parquet", 100, Some(0), 1, None);
        let partial_file = make_test_file_meta(
            "partial-0.parquet",
            100,
            Some(0),
            2,
            Some(vec!["name".to_string()]),
        );

        let file_index = vec![
            FileRowRange {
                first_row_id: 0,
                last_row_id: 99,
                row_count: 100,
                partition: vec![],
                bucket: 0,
                bucket_path: String::new(),
                snapshot_id: 1,
                total_buckets: 1,
                files: vec![base_file, partial_file],
            },
            FileRowRange {
                first_row_id: 100,
                last_row_id: 149,
                row_count: 50,
                partition: vec![],
                bucket: 0,
                bucket_path: String::new(),
                snapshot_id: 1,
                total_buckets: 1,
                files: vec![make_test_file_meta(
                    "base-1.parquet",
                    50,
                    Some(100),
                    1,
                    None,
                )],
            },
        ];

        // row_id 0 -> first group (2 files)
        let (pos, range) = find_owning_file(&file_index, 0).unwrap();
        assert_eq!(pos, 0);
        assert_eq!(range.files.len(), 2);

        // row_id 50 -> still first group
        let (pos, range) = find_owning_file(&file_index, 50).unwrap();
        assert_eq!(pos, 0);
        assert_eq!(range.row_count, 100);

        // row_id 99 -> last row of first group
        let (pos, _) = find_owning_file(&file_index, 99).unwrap();
        assert_eq!(pos, 0);

        // row_id 100 -> second group (1 file)
        let (pos, range) = find_owning_file(&file_index, 100).unwrap();
        assert_eq!(pos, 1);
        assert_eq!(range.files.len(), 1);

        // row_id 200 -> not found
        assert!(find_owning_file(&file_index, 200).is_none());
    }

    #[test]
    fn test_file_group_construction_from_overlapping_files() {
        // Verify that group_by_overlapping_row_id correctly groups base + partial files,
        // and that we can build FileRowRange from the result.
        let base = make_test_file_meta("base.parquet", 100, Some(0), 1, None);
        let partial1 = make_test_file_meta(
            "partial1.parquet",
            100,
            Some(0),
            2,
            Some(vec!["name".to_string()]),
        );
        let partial2 = make_test_file_meta(
            "partial2.parquet",
            100,
            Some(0),
            3,
            Some(vec!["value".to_string()]),
        );
        let separate = make_test_file_meta("separate.parquet", 50, Some(200), 1, None);

        let groups = group_by_overlapping_row_id(vec![base, partial1, partial2, separate]);

        // Should produce 2 groups: [base, partial1, partial2] and [separate]
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 3);
        assert_eq!(groups[1].len(), 1);

        // Build FileRowRange from first group
        let group = &groups[0];
        let first_row_id = group.iter().filter_map(|f| f.first_row_id).min().unwrap();
        let last_row_id = group
            .iter()
            .filter_map(|f| f.row_id_range().map(|(_, end)| end))
            .max()
            .unwrap();
        let row_count = group.iter().map(|f| f.row_count).max().unwrap();

        assert_eq!(first_row_id, 0);
        assert_eq!(last_row_id, 99);
        assert_eq!(row_count, 100);
    }

    #[tokio::test]
    async fn test_rejects_non_data_evolution_table() {
        let file_io = test_file_io();
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let table_schema = TableSchema::new(0, &schema);
        let table = Table::new(
            file_io,
            Identifier::new("default", "test"),
            "memory:/test".to_string(),
            table_schema,
            None,
        );

        let result = DataEvolutionPartialWriter::new(&table, vec!["id".to_string()]);
        assert!(result.is_err());
    }
}
