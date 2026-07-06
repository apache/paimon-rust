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

use crate::lumina::ffi::LuminaBuilder;
use crate::lumina::{
    is_lumina_index_type, LuminaIndexMeta, LuminaVectorIndexOptions, LUMINA_IDENTIFIER,
};
use crate::spec::{
    bucket_dir_name, BinaryRow, CoreOptions, DataField, DataFileMeta, DataType, FileKind,
    GlobalIndexMeta, IndexFileMeta, IndexManifest, ROW_ID_FIELD_NAME,
};
use crate::table::{
    CommitMessage, DataSplitBuilder, RowRange, SnapshotManager, Table, TableCommit,
};
use crate::{Error, Result};
use arrow_array::{Array, FixedSizeListArray, Float32Array, Int64Array, ListArray, RecordBatch};
use bytes::Bytes;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;

const INDEX_DIR: &str = "index";
const COPY_BUFFER_SIZE: usize = 1024 * 1024;

pub struct LuminaIndexBuildBuilder<'a> {
    table: &'a Table,
    index_column: Option<String>,
    index_type: String,
    options: HashMap<String, String>,
}

impl<'a> LuminaIndexBuildBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            index_column: None,
            index_type: LUMINA_IDENTIFIER.to_string(),
            options: HashMap::new(),
        }
    }

    pub fn with_index_column(&mut self, column: &str) -> &mut Self {
        self.index_column = Some(column.to_string());
        self
    }

    pub fn with_index_type(&mut self, index_type: &str) -> &mut Self {
        self.index_type = index_type.to_string();
        self
    }

    pub fn with_options(&mut self, options: HashMap<String, String>) -> &mut Self {
        self.options = options;
        self
    }

    pub async fn execute(&self) -> Result<usize> {
        if !is_lumina_index_type(&self.index_type) {
            return Err(Error::DataInvalid {
                message: format!("Unsupported Lumina index type: {}", self.index_type),
                source: None,
            });
        }

        let index_column = self
            .index_column
            .as_deref()
            .ok_or_else(|| Error::DataInvalid {
                message: "Lumina index column is required".to_string(),
                source: None,
            })?;

        let core_options = CoreOptions::new(self.table.schema().options());
        validate_table_options(self.table, &core_options)?;
        let rows_per_shard = core_options.global_index_row_count_per_shard()?;

        let index_field = find_index_field(self.table, index_column)?;
        validate_vector_field(index_field)?;

        let resolved_options =
            resolve_lumina_options(self.table.schema().options(), &self.options)?;
        let resolved_options = effective_lumina_options(index_field, resolved_options)?;
        let lumina_options = LuminaVectorIndexOptions::new(&resolved_options)?;
        let dimension = lumina_options.dimension;
        let index_meta = LuminaIndexMeta::new(lumina_options.to_lumina_options()).serialize()?;

        let snapshot_manager = SnapshotManager::new(
            self.table.file_io().clone(),
            self.table.location().to_string(),
        );
        let snapshot = snapshot_manager
            .get_latest_snapshot()
            .await?
            .ok_or_else(|| Error::DataInvalid {
                message: "Cannot build Lumina index without a snapshot".to_string(),
                source: None,
            })?;

        let manifest_entries = self
            .table
            .new_read_builder()
            .new_scan()
            .with_scan_all_files()
            .plan_manifest_entries(&snapshot)
            .await?;
        let shards = plan_lumina_shards(
            self.table.location(),
            self.table.schema().partition_keys(),
            self.table.schema().fields(),
            &core_options,
            snapshot.id(),
            manifest_entries,
            rows_per_shard,
        )?;
        if shards.is_empty() {
            return Ok(0);
        }

        validate_existing_index_overlap(
            self.table,
            snapshot.index_manifest(),
            index_field.id(),
            &shards,
        )
        .await?;

        let shard_count = shards.len();
        let mut messages = Vec::with_capacity(shard_count);
        for shard in shards {
            let vectors = extract_vectors(self.table, &shard, index_column, dimension).await?;
            let index_file = self
                .build_index_file(
                    &shard,
                    &vectors,
                    dimension,
                    index_field.id(),
                    index_meta.clone(),
                )
                .await?;
            let mut message = CommitMessage::new(shard.partition_bytes.clone(), 0, vec![]);
            message.new_index_files = vec![index_file];
            messages.push(message);
        }

        TableCommit::new(
            self.table.clone(),
            format!(
                "global-index-{}-create-{}",
                LUMINA_IDENTIFIER,
                uuid::Uuid::new_v4()
            ),
        )
        .commit_if_latest_snapshot(messages, snapshot.id())
        .await?;

        Ok(shard_count)
    }

    async fn build_index_file(
        &self,
        shard: &LuminaIndexShard,
        vectors: &[f32],
        dimension: i32,
        index_field_id: i32,
        index_meta: Vec<u8>,
    ) -> Result<IndexFileMeta> {
        let row_count = checked_row_count(shard.row_range_start, shard.row_range_end)?;
        validate_vector_buffer(vectors, row_count, dimension)?;
        let ids = (0..row_count as u64).collect::<Vec<_>>();
        let native_options = LuminaIndexMeta::deserialize(&index_meta)?.options().clone();

        let temp_path = temp_lumina_path();
        let temp_file = TempFileGuard::new(temp_path.clone());
        let temp_path_str = temp_path.to_string_lossy().to_string();
        let builder = LuminaBuilder::create(&native_options)?;
        builder.pretrain(vectors, row_count, dimension)?;
        builder.insert(vectors, &ids, row_count, dimension)?;
        builder.dump(&temp_path_str)?;

        let file_name = format!("lumina-global-index-{}.index", uuid::Uuid::new_v4());
        self.table
            .file_io()
            .mkdirs(&format!(
                "{}/{INDEX_DIR}/",
                self.table.location().trim_end_matches('/')
            ))
            .await?;
        let index_path = format!(
            "{}/{INDEX_DIR}/{}",
            self.table.location().trim_end_matches('/'),
            file_name
        );
        copy_local_file_to_output(&temp_path, self.table.file_io().new_output(&index_path)?)
            .await?;
        temp_file.cleanup();

        let status = self.table.file_io().get_status(&index_path).await?;
        Ok(IndexFileMeta {
            index_type: LUMINA_IDENTIFIER.to_string(),
            file_name,
            file_size: checked_i32(
                status.size,
                "Index file is too large for Rust IndexFileMeta",
            )?,
            row_count,
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: shard.row_range_start,
                row_range_end: shard.row_range_end,
                index_field_id,
                extra_field_ids: None,
                index_meta: Some(index_meta),
            }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LuminaIndexShard {
    pub partition: BinaryRow,
    pub partition_bytes: Vec<u8>,
    pub files: Vec<DataFileMeta>,
    pub row_range_start: i64,
    pub row_range_end: i64,
    snapshot_id: i64,
    source_bucket: i32,
    total_buckets: i32,
    bucket_path: String,
}

fn validate_table_options(table: &Table, core_options: &CoreOptions) -> Result<()> {
    if !core_options.row_tracking_enabled() {
        return Err(Error::DataInvalid {
            message: "Lumina index build requires 'row-tracking.enabled' = 'true'".to_string(),
            source: None,
        });
    }
    if !core_options.data_evolution_enabled() {
        return Err(Error::DataInvalid {
            message: "Lumina index build requires 'data-evolution.enabled' = 'true'".to_string(),
            source: None,
        });
    }
    if !core_options.global_index_enabled() {
        return Err(Error::DataInvalid {
            message: "Lumina index build requires 'global-index.enabled' = 'true'".to_string(),
            source: None,
        });
    }
    if !table.schema().primary_keys().is_empty() {
        return Err(Error::Unsupported {
            message: "Lumina index build does not support primary-key tables".to_string(),
        });
    }
    if core_options.deletion_vectors_enabled() {
        return Err(Error::Unsupported {
            message:
                "Lumina index build does not support tables with deletion-vectors.enabled=true"
                    .to_string(),
        });
    }
    Ok(())
}

fn find_index_field<'a>(table: &'a Table, column: &str) -> Result<&'a DataField> {
    table
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == column)
        .ok_or_else(|| Error::ColumnNotExist {
            full_name: table.identifier().full_name(),
            column: column.to_string(),
        })
}

fn validate_vector_field(field: &DataField) -> Result<()> {
    let is_array_float = matches!(
        field.data_type(),
        DataType::Array(array) if matches!(array.element_type(), DataType::Float(_))
    );
    let is_vector_float = matches!(
        field.data_type(),
        DataType::Vector(vector) if matches!(vector.element_type(), DataType::Float(_))
    );
    if !is_array_float && !is_vector_float {
        return Err(Error::DataInvalid {
            message: format!(
                "Lumina index requires ARRAY<FLOAT> or VECTOR<FLOAT> column, got {:?} for column '{}'",
                field.data_type(),
                field.name()
            ),
            source: None,
        });
    }
    Ok(())
}

/// For a `VECTOR<FLOAT, N>` column, ensure the effective Lumina options carry
/// `lumina.index.dimension = N`, so the native options and serialized index
/// metadata match the column type. Absent → inject N; present-and-equal → keep;
/// present-and-different → ConfigInvalid. Non-vector columns (e.g. ARRAY<FLOAT>)
/// are returned unchanged so the existing option/default behavior is preserved.
fn effective_lumina_options(
    field: &DataField,
    mut resolved: HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    let DataType::Vector(vector) = field.data_type() else {
        return Ok(resolved);
    };
    let n = vector.length().to_string();
    match resolved.get(crate::lumina::LUMINA_DIMENSION_OPTION) {
        None => {
            resolved.insert(crate::lumina::LUMINA_DIMENSION_OPTION.to_string(), n);
        }
        Some(existing) if *existing == n => {}
        Some(existing) => {
            return Err(Error::ConfigInvalid {
                message: format!(
                    "Vector column '{}' has dimension {} from its type, but '{}' is set to '{}'. \
                     Remove the option or set it to {}.",
                    field.name(),
                    n,
                    crate::lumina::LUMINA_DIMENSION_OPTION,
                    existing,
                    n
                ),
            });
        }
    }
    Ok(resolved)
}

fn resolve_lumina_options(
    table_options: &HashMap<String, String>,
    user_options: &HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    let mut options = table_options.clone();
    options.extend(user_options.clone());
    LuminaVectorIndexOptions::new(&options)?;
    Ok(options)
}

fn plan_lumina_shards(
    table_location: &str,
    partition_keys: &[String],
    schema_fields: &[DataField],
    core_options: &CoreOptions,
    snapshot_id: i64,
    entries: Vec<crate::spec::ManifestEntry>,
    rows_per_shard: i64,
) -> Result<Vec<LuminaIndexShard>> {
    if rows_per_shard <= 0 {
        return Err(Error::DataInvalid {
            message: format!(
                "Option 'global-index.row-count-per-shard' must be greater than 0, got: {rows_per_shard}"
            ),
            source: None,
        });
    }

    let mut by_partition_bucket: HashMap<(Vec<u8>, i32, i32), Vec<DataFileMeta>> = HashMap::new();
    for entry in entries {
        if *entry.kind() != FileKind::Add {
            continue;
        }
        if entry.file().first_row_id.is_none() {
            return Err(Error::DataInvalid {
                message: format!(
                    "Data file '{}' is missing first_row_id; cannot build a complete Lumina index",
                    entry.file().file_name
                ),
                source: None,
            });
        }
        let (partition, bucket, total_buckets, file) = entry.into_parts();
        by_partition_bucket
            .entry((partition, bucket, total_buckets))
            .or_default()
            .push(file);
    }

    let mut result = Vec::new();
    for ((partition_bytes, source_bucket, total_buckets), files) in by_partition_bucket {
        let partition = if partition_keys.is_empty() {
            BinaryRow::new(0)
        } else {
            BinaryRow::from_serialized_bytes(&partition_bytes)?
        };
        let bucket_path = bucket_path(
            table_location,
            partition_keys,
            schema_fields,
            core_options,
            &partition,
            source_bucket,
        )?;
        let mut files_by_shard: HashMap<i64, Vec<DataFileMeta>> = HashMap::new();
        for file in files {
            let (file_start, file_end) = file.row_id_range().ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "Data file '{}' is missing first_row_id; cannot build a complete Lumina index",
                    file.file_name
                ),
                source: None,
            })?;
            let start_shard = file_start / rows_per_shard;
            let end_shard = file_end / rows_per_shard;
            for shard_id in start_shard..=end_shard {
                files_by_shard
                    .entry(shard_id * rows_per_shard)
                    .or_default()
                    .push(file.clone());
            }
        }

        let mut shard_starts = files_by_shard.keys().copied().collect::<Vec<_>>();
        shard_starts.sort_unstable();
        for shard_start in shard_starts {
            let shard_end = shard_start + rows_per_shard - 1;
            let mut shard_files = files_by_shard.remove(&shard_start).unwrap_or_default();
            shard_files.sort_by_key(|file| file.first_row_id);
            let groups = group_contiguous_files(shard_files)?;
            for group in groups {
                let group_start = group
                    .first()
                    .and_then(|file| file.first_row_id)
                    .expect("planned groups are non-empty and row-id assigned");
                let group_end = group
                    .iter()
                    .map(|file| file.row_id_range().unwrap().1)
                    .max()
                    .unwrap();
                let row_range_start = group_start.max(shard_start);
                let row_range_end = group_end.min(shard_end);
                result.push(LuminaIndexShard {
                    partition: partition.clone(),
                    partition_bytes: partition_bytes.clone(),
                    files: group,
                    row_range_start,
                    row_range_end,
                    snapshot_id,
                    source_bucket,
                    total_buckets,
                    bucket_path: bucket_path.clone(),
                });
            }
        }
    }
    result.sort_by(|a, b| {
        a.partition
            .to_serialized_bytes()
            .cmp(&b.partition.to_serialized_bytes())
            .then(a.source_bucket.cmp(&b.source_bucket))
            .then(a.row_range_start.cmp(&b.row_range_start))
    });
    Ok(result)
}

fn group_contiguous_files(mut files: Vec<DataFileMeta>) -> Result<Vec<Vec<DataFileMeta>>> {
    if files.is_empty() {
        return Ok(Vec::new());
    }
    files.sort_by_key(|file| file.first_row_id);
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut current_end = None;
    for file in files {
        let (file_start, file_end) = file.row_id_range().ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Data file '{}' is missing first_row_id; cannot build a complete Lumina index",
                file.file_name
            ),
            source: None,
        })?;
        match current_end {
            None => {
                current.push(file);
                current_end = Some(file_end);
            }
            Some(end) if file_start <= end + 1 => {
                current.push(file);
                current_end = Some(end.max(file_end));
            }
            Some(_) => {
                groups.push(std::mem::take(&mut current));
                current.push(file);
                current_end = Some(file_end);
            }
        }
    }
    if !current.is_empty() {
        groups.push(current);
    }
    Ok(groups)
}

fn bucket_path(
    table_location: &str,
    partition_keys: &[String],
    schema_fields: &[DataField],
    core_options: &CoreOptions,
    partition: &BinaryRow,
    bucket: i32,
) -> Result<String> {
    let base = table_location.trim_end_matches('/');
    if partition_keys.is_empty() {
        return Ok(format!("{base}/{}", bucket_dir_name(bucket)));
    }
    let computer = crate::spec::PartitionComputer::new(
        partition_keys,
        schema_fields,
        core_options.partition_default_name(),
        core_options.legacy_partition_name(),
    )?;
    Ok(format!(
        "{base}/{}{}",
        computer.generate_partition_path(partition)?,
        bucket_dir_name(bucket)
    ))
}

async fn validate_existing_index_overlap(
    table: &Table,
    index_manifest_name: Option<&str>,
    index_field_id: i32,
    shards: &[LuminaIndexShard],
) -> Result<()> {
    let Some(index_manifest_name) = index_manifest_name else {
        return Ok(());
    };
    let path = format!(
        "{}/manifest/{}",
        table.location().trim_end_matches('/'),
        index_manifest_name
    );
    let entries = IndexManifest::read(table.file_io(), &path).await?;
    for entry in entries {
        if entry.kind != FileKind::Add {
            continue;
        }
        let Some(meta) = entry.index_file.global_index_meta else {
            continue;
        };
        if meta.index_field_id != index_field_id {
            continue;
        }
        if shards.iter().any(|shard| {
            ranges_overlap(
                meta.row_range_start,
                meta.row_range_end,
                shard.row_range_start,
                shard.row_range_end,
            )
        }) {
            return Err(Error::DataInvalid {
                message: format!(
                    "Existing global index file '{}' overlaps requested row range for field {}",
                    entry.index_file.file_name, index_field_id
                ),
                source: None,
            });
        }
    }
    Ok(())
}

async fn extract_vectors(
    table: &Table,
    shard: &LuminaIndexShard,
    index_column: &str,
    dimension: i32,
) -> Result<Vec<f32>> {
    let split = DataSplitBuilder::new()
        .with_snapshot(shard.snapshot_id)
        .with_partition(shard.partition.clone())
        .with_bucket(shard.source_bucket)
        .with_bucket_path(shard.bucket_path.clone())
        .with_total_buckets(shard.total_buckets)
        .with_data_files(shard.files.clone())
        .with_row_ranges(vec![RowRange::new(
            shard.row_range_start,
            shard.row_range_end,
        )])
        .build()?;

    let mut read_builder = table.new_read_builder();
    read_builder.with_projection(&[index_column, ROW_ID_FIELD_NAME])?;
    let read = read_builder.new_read()?;
    let batches = read.to_arrow(&[split])?.try_collect::<Vec<_>>().await?;
    extract_vectors_from_batches(
        &batches,
        index_column,
        dimension,
        shard.row_range_start,
        i64::from(checked_row_count(
            shard.row_range_start,
            shard.row_range_end,
        )?),
    )
}

fn extract_vectors_from_batches(
    batches: &[RecordBatch],
    index_column: &str,
    dimension: i32,
    row_range_start: i64,
    expected_row_count: i64,
) -> Result<Vec<f32>> {
    let dimension = usize::try_from(dimension).map_err(|e| Error::DataInvalid {
        message: format!("Invalid Lumina dimension: {dimension}"),
        source: Some(Box::new(e)),
    })?;
    let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let mut vectors = Vec::with_capacity(row_count * dimension);
    let mut expected_row_id = row_range_start;
    for batch in batches {
        let vector_index =
            batch
                .schema()
                .index_of(index_column)
                .map_err(|e| Error::DataInvalid {
                    message: format!("Vector column '{index_column}' not found in read batch: {e}"),
                    source: None,
                })?;
        let row_id_index =
            batch
                .schema()
                .index_of(ROW_ID_FIELD_NAME)
                .map_err(|e| Error::DataInvalid {
                    message: format!("_ROW_ID column not found in read batch: {e}"),
                    source: None,
                })?;
        // Resolve the vector column as either List<Float32> (ARRAY<FLOAT>) or
        // FixedSizeList<Float32> (VECTOR<FLOAT>). Both yield a Float32Array of
        // values plus a per-row [start, end) slice.
        let column = batch.column(vector_index);
        enum VectorLayout<'a> {
            List(&'a ListArray),
            Fixed(&'a FixedSizeListArray),
        }
        let layout = if let Some(a) = column.as_any().downcast_ref::<ListArray>() {
            VectorLayout::List(a)
        } else if let Some(a) = column.as_any().downcast_ref::<FixedSizeListArray>() {
            VectorLayout::Fixed(a)
        } else {
            return Err(Error::DataInvalid {
                message: "Lumina vector extraction requires Arrow List<Float32> or FixedSizeList<Float32>".to_string(),
                source: None,
            });
        };
        let values = match layout {
            VectorLayout::List(a) => a.values(),
            VectorLayout::Fixed(a) => a.values(),
        }
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| Error::DataInvalid {
            message: "Lumina vector extraction requires Float32 vector elements".to_string(),
            source: None,
        })?;
        let row_ids = batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::DataInvalid {
                message: "Lumina vector extraction requires non-null Int64 _ROW_ID".to_string(),
                source: None,
            })?;

        for row in 0..batch.num_rows() {
            if row_ids.is_null(row) {
                return Err(Error::DataInvalid {
                    message: "Lumina vector extraction found null _ROW_ID".to_string(),
                    source: None,
                });
            }
            let row_id = row_ids.value(row);
            if row_id != expected_row_id {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Lumina vector extraction expected _ROW_ID {}, got {}",
                        expected_row_id, row_id
                    ),
                    source: None,
                });
            }
            expected_row_id += 1;

            let is_null = match layout {
                VectorLayout::List(a) => a.is_null(row),
                VectorLayout::Fixed(a) => a.is_null(row),
            };
            if is_null {
                return Err(Error::DataInvalid {
                    message: "Lumina vector extraction found null vector row".to_string(),
                    source: None,
                });
            }
            let (start, end) = match layout {
                VectorLayout::List(a) => {
                    let offsets = a.value_offsets();
                    (offsets[row] as usize, offsets[row + 1] as usize)
                }
                VectorLayout::Fixed(a) => {
                    let len = a.value_length() as usize;
                    (row * len, (row + 1) * len)
                }
            };
            if end - start != dimension {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Lumina vector dimension mismatch: expected {}, got {}",
                        dimension,
                        end - start
                    ),
                    source: None,
                });
            }
            for value_index in start..end {
                if values.is_null(value_index) {
                    return Err(Error::DataInvalid {
                        message: "Lumina vector extraction found null vector element".to_string(),
                        source: None,
                    });
                }
                vectors.push(values.value(value_index));
            }
        }
    }
    let actual_row_count = expected_row_id - row_range_start;
    if actual_row_count != expected_row_count {
        return Err(Error::DataInvalid {
            message: format!(
                "Lumina vector extraction expected {} rows, got {}",
                expected_row_count, actual_row_count
            ),
            source: None,
        });
    }
    Ok(vectors)
}

fn checked_i32(value: u64, context: &str) -> Result<i32> {
    i32::try_from(value).map_err(|_| Error::DataInvalid {
        message: format!("{context}: {value}"),
        source: None,
    })
}

fn checked_row_count(row_range_start: i64, row_range_end: i64) -> Result<i32> {
    if row_range_end < row_range_start {
        return Err(Error::DataInvalid {
            message: format!("Invalid Lumina row range [{row_range_start}, {row_range_end}]"),
            source: None,
        });
    }
    i32::try_from(row_range_end - row_range_start + 1).map_err(|_| Error::DataInvalid {
        message: format!(
            "Lumina row count is too large for Rust IndexFileMeta: [{row_range_start}, {row_range_end}]"
        ),
        source: None,
    })
}

fn validate_vector_buffer(vectors: &[f32], row_count: i32, dimension: i32) -> Result<()> {
    if row_count <= 0 {
        return Err(Error::DataInvalid {
            message: format!("Lumina shard row count must be positive, got: {row_count}"),
            source: None,
        });
    }
    if dimension <= 0 {
        return Err(Error::DataInvalid {
            message: format!("Lumina vector dimension must be positive, got: {dimension}"),
            source: None,
        });
    }
    let row_count = row_count as usize;
    let dimension = dimension as usize;
    let expected_len = row_count
        .checked_mul(dimension)
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Lumina vector buffer length overflows: row_count={row_count}, dimension={dimension}"
            ),
            source: None,
        })?;
    if vectors.len() != expected_len {
        return Err(Error::DataInvalid {
            message: format!(
                "Lumina vector buffer length {} does not match row_count={} and dimension={}",
                vectors.len(),
                row_count,
                dimension
            ),
            source: None,
        });
    }
    Ok(())
}

fn temp_lumina_path() -> PathBuf {
    std::env::temp_dir().join(format!("lumina-index-{}.index", uuid::Uuid::new_v4()))
}

struct TempFileGuard {
    path: Option<PathBuf>,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn cleanup(mut self) {
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

async fn copy_local_file_to_output(
    source_path: &Path,
    output: crate::io::OutputFile,
) -> Result<()> {
    let mut source =
        tokio::fs::File::open(source_path)
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to open temporary Lumina index file: {e}"),
                source: None,
            })?;
    let mut writer = output.writer().await?;
    let mut buffer = vec![0u8; COPY_BUFFER_SIZE];

    loop {
        let len = source
            .read(&mut buffer)
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to read temporary Lumina index file: {e}"),
                source: None,
            })?;
        if len == 0 {
            break;
        }
        writer.write(Bytes::copy_from_slice(&buffer[..len])).await?;
    }
    writer.close().await
}

fn ranges_overlap(left_start: i64, left_end: i64, right_start: i64, right_end: i64) -> bool {
    left_start <= right_end && right_start <= left_end
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIO;
    use crate::io::FileIOBuilder;
    use crate::lumina::LUMINA_DIMENSION_OPTION;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        ArrayType, DoubleType, FloatType, IntType, ManifestEntry, Schema, TableSchema, VectorType,
    };
    use crate::table::TableWrite;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, Int64Builder, ListBuilder};
    use arrow_array::{ArrayRef, Int32Array};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use chrono::{DateTime, Utc};
    use std::sync::Arc;

    fn data_file(name: &str, first_row_id: Option<i64>, row_count: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 128,
            row_count,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::new(vec![], vec![], vec![]),
            value_stats: BinaryTableStats::new(vec![], vec![], vec![]),
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
            delete_row_count: None,
            embedded_index: None,
            first_row_id,
            write_cols: None,
            external_path: None,
            file_source: None,
            value_stats_cols: None,
        }
    }

    fn manifest_entry(file: DataFileMeta) -> ManifestEntry {
        manifest_entry_with_bucket(file, 0, 1)
    }

    fn manifest_entry_with_bucket(
        file: DataFileMeta,
        bucket: i32,
        total_buckets: i32,
    ) -> ManifestEntry {
        ManifestEntry::new(FileKind::Add, vec![], bucket, total_buckets, file, 2)
    }

    fn table_options(rows_per_shard: &str) -> HashMap<String, String> {
        HashMap::from([
            ("row-tracking.enabled".to_string(), "true".to_string()),
            ("data-evolution.enabled".to_string(), "true".to_string()),
            ("global-index.enabled".to_string(), "true".to_string()),
            (
                "global-index.row-count-per-shard".to_string(),
                rows_per_shard.to_string(),
            ),
        ])
    }

    fn test_table(options: HashMap<String, String>) -> Table {
        test_table_with_io(
            FileIOBuilder::new("memory").build().unwrap(),
            "memory:/test_lumina_builder",
            Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column(
                    "embedding",
                    DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
                )
                .options(options)
                .build()
                .unwrap(),
        )
    }

    fn test_table_with_schema(schema: Schema) -> Table {
        test_table_with_io(
            FileIOBuilder::new("memory").build().unwrap(),
            "memory:/test_lumina_builder",
            schema,
        )
    }

    fn test_table_with_io(file_io: FileIO, table_path: &str, schema: Schema) -> Table {
        Table::new(
            file_io,
            Identifier::new("default", "test_table"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    fn vector_schema_builder(options: HashMap<String, String>) -> crate::spec::SchemaBuilder {
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            )
            .options(options)
    }

    fn plan(entries: Vec<ManifestEntry>, rows_per_shard: i64) -> Result<Vec<LuminaIndexShard>> {
        let table = test_table(table_options(&rows_per_shard.to_string()));
        let core = CoreOptions::new(table.schema().options());
        plan_lumina_shards(
            table.location(),
            table.schema().partition_keys(),
            table.schema().fields(),
            &core,
            1,
            entries,
            rows_per_shard,
        )
    }

    #[test]
    fn test_planner_splits_single_file_across_shards() {
        let shards = plan(vec![manifest_entry(data_file("a", Some(0), 25))], 10).unwrap();

        assert_eq!(
            shards
                .iter()
                .map(|s| (s.row_range_start, s.row_range_end))
                .collect::<Vec<_>>(),
            vec![(0, 9), (10, 19), (20, 24)]
        );
    }

    #[test]
    fn test_planner_merges_contiguous_files() {
        let shards = plan(
            vec![
                manifest_entry(data_file("a", Some(0), 5)),
                manifest_entry(data_file("b", Some(5), 5)),
            ],
            20,
        )
        .unwrap();

        assert_eq!(shards.len(), 1);
        assert_eq!((shards[0].row_range_start, shards[0].row_range_end), (0, 9));
        assert_eq!(
            shards[0]
                .files
                .iter()
                .map(|f| f.file_name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn test_planner_keeps_source_buckets_separate() {
        let shards = plan(
            vec![
                manifest_entry_with_bucket(data_file("a", Some(0), 5), 0, 2),
                manifest_entry_with_bucket(data_file("b", Some(5), 5), 1, 2),
            ],
            20,
        )
        .unwrap();

        assert_eq!(shards.len(), 2);
        assert_eq!(
            shards
                .iter()
                .map(|s| (
                    s.source_bucket,
                    s.total_buckets,
                    s.row_range_start,
                    s.row_range_end
                ))
                .collect::<Vec<_>>(),
            vec![(0, 2, 0, 4), (1, 2, 5, 9)]
        );
    }

    #[test]
    fn test_planner_splits_gap_into_separate_groups() {
        let shards = plan(
            vec![
                manifest_entry(data_file("a", Some(0), 5)),
                manifest_entry(data_file("b", Some(10), 5)),
            ],
            20,
        )
        .unwrap();

        assert_eq!(
            shards
                .iter()
                .map(|s| (s.row_range_start, s.row_range_end))
                .collect::<Vec<_>>(),
            vec![(0, 4), (10, 14)]
        );
    }

    #[test]
    fn test_planner_rejects_missing_first_row_id() {
        let err = plan(vec![manifest_entry(data_file("a", None, 5))], 10)
            .expect_err("missing first_row_id should fail");
        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("missing first_row_id"))
        );
    }

    #[test]
    fn test_planner_rejects_invalid_rows_per_shard() {
        let err = plan(vec![manifest_entry(data_file("a", Some(0), 5))], 0)
            .expect_err("invalid rows per shard should fail");
        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("row-count-per-shard"))
        );
    }

    #[test]
    fn test_validate_vector_field_accepts_array_float() {
        let field = DataField::new(
            0,
            "embedding".to_string(),
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        );
        assert!(validate_vector_field(&field).is_ok());
    }

    #[test]
    fn test_validate_vector_field_accepts_vector_float() {
        let field = DataField::new(
            0,
            "embedding".to_string(),
            DataType::Vector(
                VectorType::try_new(true, 4, DataType::Float(FloatType::new())).unwrap(),
            ),
        );
        assert!(validate_vector_field(&field).is_ok());
    }

    #[test]
    fn test_validate_vector_field_rejects_vector_double() {
        let field = DataField::new(
            0,
            "embedding".to_string(),
            DataType::Vector(
                VectorType::try_new(true, 4, DataType::Double(DoubleType::new())).unwrap(),
            ),
        );
        let err = validate_vector_field(&field).expect_err("VECTOR<DOUBLE> must be rejected");
        assert!(matches!(err, Error::DataInvalid { .. }));
    }

    #[test]
    fn test_effective_options_vector_absent_inserts_length() {
        let field = DataField::new(
            0,
            "embedding".to_string(),
            DataType::Vector(
                VectorType::try_new(true, 256, DataType::Float(FloatType::new())).unwrap(),
            ),
        );
        let opts = effective_lumina_options(&field, HashMap::new()).unwrap();
        assert_eq!(
            opts.get(LUMINA_DIMENSION_OPTION).map(String::as_str),
            Some("256")
        );
    }

    #[test]
    fn test_effective_options_vector_matching_option_ok() {
        let field = DataField::new(
            0,
            "embedding".to_string(),
            DataType::Vector(
                VectorType::try_new(true, 256, DataType::Float(FloatType::new())).unwrap(),
            ),
        );
        let resolved = HashMap::from([(LUMINA_DIMENSION_OPTION.to_string(), "256".to_string())]);
        let opts = effective_lumina_options(&field, resolved).unwrap();
        assert_eq!(
            opts.get(LUMINA_DIMENSION_OPTION).map(String::as_str),
            Some("256")
        );
    }

    #[test]
    fn test_effective_options_vector_mismatch_errors() {
        let field = DataField::new(
            0,
            "embedding".to_string(),
            DataType::Vector(
                VectorType::try_new(true, 256, DataType::Float(FloatType::new())).unwrap(),
            ),
        );
        let resolved = HashMap::from([(LUMINA_DIMENSION_OPTION.to_string(), "128".to_string())]);
        let err =
            effective_lumina_options(&field, resolved).expect_err("dimension mismatch must error");
        assert!(matches!(err, Error::ConfigInvalid { .. }));
    }

    #[test]
    fn test_effective_options_array_unchanged() {
        let field = DataField::new(
            0,
            "embedding".to_string(),
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        );
        // No dimension option set: array path must NOT inject one.
        let opts = effective_lumina_options(&field, HashMap::new()).unwrap();
        assert!(!opts.contains_key(LUMINA_DIMENSION_OPTION));
    }

    #[test]
    fn test_vector_without_option_propagates_dimension_to_native_options() {
        use crate::lumina::{LuminaVectorIndexOptions, KEY_DIMENSION};
        let field = DataField::new(
            0,
            "embedding".to_string(),
            DataType::Vector(
                VectorType::try_new(true, 256, DataType::Float(FloatType::new())).unwrap(),
            ),
        );
        // No lumina.index.dimension set by the user.
        let resolved = effective_lumina_options(&field, HashMap::new()).unwrap();
        let opts = LuminaVectorIndexOptions::new(&resolved).unwrap();

        assert_eq!(
            opts.dimension, 256,
            "local dimension must be N, not default 128"
        );
        assert_eq!(
            opts.to_lumina_options()
                .get(KEY_DIMENSION)
                .map(String::as_str),
            Some("256"),
            "native index.dimension must be N, not default 128"
        );
    }

    #[tokio::test]
    async fn test_execute_rejects_primary_key_table() {
        let table = test_table_with_schema(
            vector_schema_builder(table_options("10"))
                .primary_key(["id"])
                .build()
                .unwrap(),
        );

        let err = table
            .new_lumina_index_build_builder()
            .with_index_column("embedding")
            .execute()
            .await
            .expect_err("primary-key table should fail before native build");

        assert!(
            matches!(err, Error::Unsupported { message } if message.contains("primary-key tables"))
        );
    }

    #[tokio::test]
    async fn test_execute_rejects_deletion_vectors_table() {
        let mut options = table_options("10");
        options.insert("deletion-vectors.enabled".to_string(), "true".to_string());
        let table = test_table(options);

        let err = table
            .new_lumina_index_build_builder()
            .with_index_column("embedding")
            .execute()
            .await
            .expect_err("deletion vectors table should fail before native build");

        assert!(
            matches!(err, Error::Unsupported { message } if message.contains("deletion-vectors.enabled=true"))
        );
    }

    fn vector_batch(rows: Vec<Option<Vec<Option<f32>>>>, row_ids: Vec<Option<i64>>) -> RecordBatch {
        let mut vector_builder = ListBuilder::new(Float32Builder::new());
        for row in rows {
            match row {
                Some(values) => {
                    for value in values {
                        match value {
                            Some(value) => vector_builder.values().append_value(value),
                            None => vector_builder.values().append_null(),
                        }
                    }
                    vector_builder.append(true);
                }
                None => vector_builder.append(false),
            }
        }
        let mut row_id_builder = Int64Builder::new();
        for row_id in row_ids {
            match row_id {
                Some(value) => row_id_builder.append_value(value),
                None => row_id_builder.append_null(),
            }
        }
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "embedding",
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "item",
                    ArrowDataType::Float32,
                    true,
                ))),
                true,
            ),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(vector_builder.finish()) as ArrayRef,
                Arc::new(row_id_builder.finish()) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_extract_vectors_accepts_list_float32_and_row_ids() {
        let batch = vector_batch(
            vec![
                Some(vec![Some(1.0), Some(2.0)]),
                Some(vec![Some(3.0), Some(4.0)]),
            ],
            vec![Some(10), Some(11)],
        );

        let vectors = extract_vectors_from_batches(&[batch], "embedding", 2, 10, 2).unwrap();

        assert_eq!(vectors, vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn test_extract_vectors_rejects_null_vector() {
        let batch = vector_batch(vec![None], vec![Some(0)]);

        let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
            .expect_err("null vector should fail");

        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("null vector"))
        );
    }

    #[test]
    fn test_extract_vectors_rejects_null_element() {
        let batch = vector_batch(vec![Some(vec![Some(1.0), None])], vec![Some(0)]);

        let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
            .expect_err("null element should fail");

        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("null vector element"))
        );
    }

    #[test]
    fn test_extract_vectors_rejects_dimension_mismatch() {
        let batch = vector_batch(vec![Some(vec![Some(1.0)])], vec![Some(0)]);

        let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
            .expect_err("dimension mismatch should fail");

        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("dimension mismatch"))
        );
    }

    #[test]
    fn test_extract_vectors_rejects_row_id_gap() {
        let batch = vector_batch(
            vec![
                Some(vec![Some(1.0), Some(2.0)]),
                Some(vec![Some(3.0), Some(4.0)]),
            ],
            vec![Some(0), Some(2)],
        );

        let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 2)
            .expect_err("row id gap should fail");

        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("expected _ROW_ID"))
        );
    }

    #[test]
    fn test_extract_vectors_rejects_non_list_float32() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("embedding", ArrowDataType::Int32, false),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(0)])) as ArrayRef,
            ],
        )
        .unwrap();

        let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
            .expect_err("non-list vector should fail");

        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("List<Float32>"))
        );
    }

    fn fixed_size_vector_batch(
        rows: Vec<Option<Vec<f32>>>,
        row_ids: Vec<Option<i64>>,
        len: i32,
    ) -> RecordBatch {
        let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
        let mut builder =
            FixedSizeListBuilder::new(Float32Builder::new(), len).with_field(element_field);
        for row in rows {
            match row {
                Some(values) => {
                    for v in values {
                        builder.values().append_value(v);
                    }
                    builder.append(true);
                }
                None => {
                    for _ in 0..len {
                        builder.values().append_value(0.0);
                    }
                    builder.append(false);
                }
            }
        }
        let mut row_id_builder = Int64Builder::new();
        for row_id in row_ids {
            match row_id {
                Some(value) => row_id_builder.append_value(value),
                None => row_id_builder.append_null(),
            }
        }
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "embedding",
                ArrowDataType::FixedSizeList(
                    Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
                    len,
                ),
                true,
            ),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(builder.finish()) as ArrayRef,
                Arc::new(row_id_builder.finish()) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_extract_vectors_accepts_fixed_size_list_float32() {
        let batch = fixed_size_vector_batch(
            vec![Some(vec![1.0, 2.0]), Some(vec![3.0, 4.0])],
            vec![Some(10), Some(11)],
            2,
        );
        let vectors = extract_vectors_from_batches(&[batch], "embedding", 2, 10, 2).unwrap();
        assert_eq!(vectors, vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn test_extract_vectors_fixed_size_list_rejects_null_vector() {
        let batch = fixed_size_vector_batch(vec![None], vec![Some(0)], 2);
        let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
            .expect_err("null vector should fail");
        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("null vector"))
        );
    }

    #[test]
    fn test_extract_vectors_fixed_size_list_dimension_mismatch() {
        // Column is FixedSizeList of length 3, but caller expects dimension 2.
        let batch = fixed_size_vector_batch(vec![Some(vec![1.0, 2.0, 3.0])], vec![Some(0)], 3);
        let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
            .expect_err("dimension mismatch should fail");
        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("dimension mismatch"))
        );
    }

    #[test]
    fn test_extract_vectors_fixed_size_list_rejects_null_element() {
        // A non-null vector row whose second child element is null. Mirrors the
        // List path's test_extract_vectors_rejects_null_element so both layouts
        // reject null elements identically.
        let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
        let mut builder =
            FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(element_field);
        builder.values().append_value(1.0);
        builder.values().append_null();
        builder.append(true);
        let row_ids = Arc::new(Int64Array::from(vec![Some(0)])) as ArrayRef;
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "embedding",
                ArrowDataType::FixedSizeList(
                    Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
                    2,
                ),
                true,
            ),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(builder.finish()) as ArrayRef, row_ids],
        )
        .unwrap();

        let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
            .expect_err("null element should fail");

        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("null vector element"))
        );
    }

    #[test]
    fn test_checked_metadata_conversion_rejects_large_file_size() {
        let err = checked_i32(i32::MAX as u64 + 1, "Index file is too large")
            .expect_err("large file size should fail");
        assert!(matches!(err, Error::DataInvalid { message, .. } if message.contains("too large")));
    }

    #[test]
    fn test_temp_file_guard_cleans_up_on_drop() {
        let path = temp_lumina_path();
        std::fs::write(&path, b"temporary lumina data").unwrap();
        {
            let _guard = TempFileGuard::new(path.clone());
            assert!(path.exists());
        }
        assert!(!path.exists());
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

    fn build_vector_batch(ids: Vec<i32>, vectors: Vec<Vec<f32>>) -> RecordBatch {
        let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
        let mut vector_builder =
            ListBuilder::new(Float32Builder::new()).with_field(element_field.clone());
        for vector in vectors {
            for value in vector {
                vector_builder.values().append_value(value);
            }
            vector_builder.append(true);
        }
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("embedding", ArrowDataType::List(element_field), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(vector_builder.finish()) as ArrayRef,
            ],
        )
        .unwrap()
    }

    // Manual run with a local Lumina native library:
    // LUMINA_LIB_PATH=/path/to/liblumina_py.so cargo test -p paimon \
    //     table::lumina_index_build_builder::tests::test_execute_writes_lumina_index_manifest \
    //     --features fulltext,vortex -- --ignored --exact
    #[tokio::test]
    #[ignore = "requires LUMINA_LIB_PATH; see manual run command above"]
    async fn test_execute_writes_lumina_index_manifest() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/test_lumina_builder_e2e";
        setup_dirs(&file_io, table_path).await;

        let mut options = table_options("10");
        options.insert("lumina.index.dimension".to_string(), "2".to_string());
        options.insert("lumina.encoding.type".to_string(), "rawf32".to_string());
        let table = test_table_with_io(
            file_io.clone(),
            table_path,
            vector_schema_builder(options).build().unwrap(),
        );

        let mut table_write = TableWrite::new(&table, "test-user".to_string()).unwrap();
        table_write
            .write_arrow_batch(&build_vector_batch(
                vec![1, 2],
                vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            ))
            .await
            .unwrap();
        let messages = table_write.prepare_commit().await.unwrap();
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(messages)
            .await
            .unwrap();

        let shard_count = table
            .new_lumina_index_build_builder()
            .with_index_column("embedding")
            .execute()
            .await
            .unwrap();
        assert_eq!(shard_count, 1);

        let snapshot_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = snapshot_manager
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.id(), 2);
        assert_eq!(snapshot.total_record_count(), Some(2));
        assert_eq!(snapshot.delta_record_count(), Some(0));
        assert_eq!(snapshot.next_row_id(), Some(2));

        let index_manifest = snapshot.index_manifest().expect("index manifest");
        let index_entries =
            IndexManifest::read(&file_io, &format!("{table_path}/manifest/{index_manifest}"))
                .await
                .unwrap();
        assert_eq!(index_entries.len(), 1);

        let index_file = &index_entries[0].index_file;
        assert_eq!(index_file.index_type, LUMINA_IDENTIFIER);
        assert!(index_file.file_name.starts_with("lumina-global-index-"));
        assert_eq!(index_file.row_count, 2);
        assert!(index_file.file_size > 0);

        let global_meta = index_file
            .global_index_meta
            .as_ref()
            .expect("global index meta");
        assert_eq!(global_meta.row_range_start, 0);
        assert_eq!(global_meta.row_range_end, 1);
        assert_eq!(global_meta.index_field_id, 1);
        assert!(global_meta
            .index_meta
            .as_ref()
            .is_some_and(|m| !m.is_empty()));

        let index_path = format!("{table_path}/index/{}", index_file.file_name);
        let status = file_io.get_status(&index_path).await.unwrap();
        assert_eq!(index_file.file_size as u64, status.size);
    }
}
