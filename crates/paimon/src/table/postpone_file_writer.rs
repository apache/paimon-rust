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

//! Postpone bucket file writer for primary-key tables with `bucket = -2`.
//!
//! Writes data in KV format (`_SEQUENCE_NUMBER`, `_VALUE_KIND` + user columns)
//! but without sorting or deduplication — compaction assigns real buckets later.
//!
//! Uses a special file naming prefix: `data--u-{commitUser}-s-{writeId}-w-`.
//!
//! Reference: [PostponeBucketWriter](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/postpone/PostponeBucketWriter.java)

use super::data_file_path_factory::{DataFilePath, DataFilePathFactory};
use super::managed_blob_reference::{ManagedBlobReferences, REFERENCE_FILE_SUFFIX};
use super::managed_blob_writer::ManagedBlobWriteState;
use super::postpone_retract::PostponeRetractValidator;
use crate::arrow::format::{create_format_writer, with_write_resources, FormatFileWriter};
use crate::io::FileIO;
use crate::resource::ResourceContext;
use crate::spec::stats::BinaryTableStats;
use crate::spec::{
    data_file_to_file_index_file_name, BinaryRow, CoreOptions, DataField, DataFileMeta, RowKind,
    EMPTY_SERIALIZED_ROW, VALUE_KIND_FIELD_NAME,
};
use crate::table::data_file_index_writer::{DataFileIndexWriter, FileIndexOptions};
use crate::table::kv_file_writer::build_physical_schema;
use crate::Result;
use arrow_array::{Int64Array, Int8Array, RecordBatch};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::task::JoinSet;

/// Configuration for [`PostponeFileWriter`].
pub(crate) struct PostponeWriteConfig {
    pub table_name: String,
    pub primary_keys: Vec<String>,
    pub table_options: std::collections::HashMap<String, String>,
    pub table_location: String,
    pub partition_path: String,
    pub bucket: i32,
    pub schema_id: i64,
    pub target_file_size: i64,
    pub target_file_row_num: i64,
    pub primary_key_indices: Vec<usize>,
    pub value_fields: Vec<DataField>,
    pub file_compression: String,
    pub file_compression_zstd_level: i32,
    pub write_buffer_size: i64,
    pub file_format: String,
    /// Data file name prefix: `"data--u-{commitUser}-s-{writeId}-w-"`.
    pub data_file_prefix: String,
    pub file_index_options: Option<Arc<FileIndexOptions>>,
}

/// Writer for postpone bucket mode (`bucket = -2`).
///
/// Streams data directly to a FormatFileWriter in arrival order (no sort/dedup),
/// prepending `_SEQUENCE_NUMBER` and `_VALUE_KIND` columns to each batch.
/// Rolls after a batch reaches the configured size or row limit.
pub(crate) struct PostponeFileWriter {
    paths: DataFilePathFactory,
    file_io: FileIO,
    config: PostponeWriteConfig,
    next_sequence_number: i64,
    current_writer: Option<Box<dyn FormatFileWriter>>,
    current_index: Option<DataFileIndexWriter>,
    current_blob_references: Option<ManagedBlobReferences>,
    managed_blob_writer: ManagedBlobWriteState,
    retract_validator: PostponeRetractValidator,
    current_file_name: Option<String>,
    current_file_path: Option<DataFilePath>,
    current_stats: PostponeFileStats,
    /// Timestamp captured when the current file was opened (used for deterministic replay order).
    current_file_creation_time: DateTime<Utc>,
    written_files: Vec<DataFileMeta>,
    created_paths: Vec<String>,
    /// Background file close tasks spawned during rolling.
    in_flight_closes: JoinSet<Result<DataFileMeta>>,
    resources: Option<ResourceContext>,
}

impl PostponeFileWriter {
    pub(crate) fn new(file_io: FileIO, config: PostponeWriteConfig) -> Result<Self> {
        let paths = DataFilePathFactory::new(
            &config.table_location,
            &config.partition_path,
            config.bucket,
            &config.table_options,
        )?;
        let managed_blob_writer = ManagedBlobWriteState::new(
            &file_io,
            paths.bucket_path(),
            &config.data_file_prefix,
            &config.value_fields,
            &CoreOptions::new(&config.table_options),
        )?;
        let retract_validator = PostponeRetractValidator::new(&config)?;
        Ok(Self {
            paths,
            file_io,
            config,
            next_sequence_number: 0,
            current_writer: None,
            current_index: None,
            current_blob_references: None,
            managed_blob_writer,
            retract_validator,
            current_file_name: None,
            current_file_path: None,
            current_stats: PostponeFileStats::default(),
            current_file_creation_time: Utc::now(),
            written_files: Vec::new(),
            created_paths: Vec::new(),
            in_flight_closes: JoinSet::new(),
            resources: None,
        })
    }

    pub(crate) fn with_resources(mut self, resources: Option<ResourceContext>) -> Self {
        self.resources = resources;
        self
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let result = self.write_batch(batch).await;
        if result.is_err() {
            self.abort().await;
        }
        result
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let batch = self.managed_blob_writer.externalize(batch.clone()).await?;
        self.retract_validator.validate(&batch)?;
        if self.current_writer.is_none() {
            self.open_new_file(batch.schema()).await?;
        }

        let num_rows = batch.num_rows();
        let start_seq = self.next_sequence_number;
        let end_seq = start_seq + num_rows as i64 - 1;

        // Build physical batch: [_SEQUENCE_NUMBER, _VALUE_KIND, all_user_cols...]
        let mut physical_columns: Vec<Arc<dyn arrow_array::Array>> = Vec::new();
        physical_columns.push(Arc::new(Int64Array::from(
            (start_seq..=end_seq).collect::<Vec<_>>(),
        )));
        let vk_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == VALUE_KIND_FIELD_NAME);
        match vk_idx {
            Some(vk_idx) => physical_columns.push(batch.column(vk_idx).clone()),
            None => physical_columns.push(Arc::new(Int8Array::from(vec![0i8; num_rows]))),
        }
        // All user columns (skip _VALUE_KIND if present — already handled above).
        for (i, col) in batch.columns().iter().enumerate() {
            if Some(i) == vk_idx {
                continue;
            }
            physical_columns.push(col.clone());
        }

        let physical_schema = build_physical_schema(&batch.schema());
        let physical_batch =
            RecordBatch::try_new(physical_schema, physical_columns).map_err(|e| {
                crate::Error::DataInvalid {
                    message: format!("Failed to create physical batch: {e}"),
                    source: None,
                }
            })?;

        ManagedBlobReferences::collect(&mut self.current_blob_references, &physical_batch)?;
        self.current_writer
            .as_mut()
            .unwrap()
            .write(&physical_batch)
            .await?;
        if let Some(index) = &mut self.current_index {
            // FileIndex field positions refer to the user schema, while the
            // physical file starts with two KV metadata columns. Project the
            // exact rows written to this file in their arrival order.
            let logical_indices = (2..physical_batch.num_columns()).collect::<Vec<_>>();
            let logical_batch = physical_batch.project(&logical_indices).map_err(|error| {
                crate::Error::DataInvalid {
                    message: format!("Failed to project postpone index values: {error}"),
                    source: None,
                }
            })?;
            index.write(&logical_batch)?;
        }
        self.next_sequence_number = end_seq + 1;
        self.current_stats
            .add_batch(&batch, &self.config, start_seq, end_seq)?;

        // Roll to a new file if target size is reached — close in background
        if self.current_writer.as_ref().unwrap().num_bytes() as i64 >= self.config.target_file_size
            || self.current_stats.row_count >= self.config.target_file_row_num
        {
            self.roll_file();
        }

        // Flush row group if in-progress buffer exceeds write_buffer_size
        if let Some(w) = self.current_writer.as_mut() {
            if w.in_progress_size() as i64 >= self.config.write_buffer_size {
                w.flush().await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn abort(&mut self) {
        if let Some(writer) = self.current_writer.take() {
            let _ = writer.close().await;
        }
        self.current_index = None;
        self.current_blob_references = None;
        self.current_file_name = None;
        self.current_file_path = None;
        while self.in_flight_closes.join_next().await.is_some() {}
        for path in self.created_paths.drain(..) {
            let _ = self.file_io.delete_file(&path).await;
        }
        self.written_files.clear();
        self.managed_blob_writer.abort().await;
    }

    pub(crate) async fn prepare_commit(&mut self) -> Result<Vec<DataFileMeta>> {
        let result = self.finish().await;
        if result.is_err() {
            self.abort().await;
        }
        result
    }

    async fn finish(&mut self) -> Result<Vec<DataFileMeta>> {
        self.close_current_file().await?;
        while let Some(result) = self.in_flight_closes.join_next().await {
            let meta = result.map_err(|e| crate::Error::DataInvalid {
                message: format!("Background file close task panicked: {e}"),
                source: None,
            })??;
            self.written_files.push(meta);
        }
        // Java replays postpone files by millisecond creation time, retaining
        // manifest order for ties. Background closes must not reorder arrivals.
        self.written_files
            .sort_by_key(|file| file.min_sequence_number);
        self.managed_blob_writer.prepare_commit().await?;
        self.created_paths.clear();
        Ok(std::mem::take(&mut self.written_files))
    }

    /// Spawn the current writer's close in the background for non-blocking rolling.
    fn roll_file(&mut self) {
        let writer = match self.current_writer.take() {
            Some(w) => w,
            None => return,
        };
        let file_name = self.current_file_name.take().unwrap();
        let index = self.current_index.take();
        let blob_references = self.current_blob_references.take();
        let file_io = self.file_io.clone();
        let location = self.current_file_path.take().unwrap();
        let bucket_dir = location.parent().to_string();
        let threshold = self
            .config
            .file_index_options
            .as_ref()
            .map(|options| options.in_manifest_threshold);
        let stats = std::mem::take(&mut self.current_stats);
        let schema_id = self.config.schema_id;
        // Capture creation_time from when the file was opened, not when the async close finishes.
        // Java's postpone compaction sorts by creationTime for replay order.
        let creation_time = self.current_file_creation_time;

        self.in_flight_closes.spawn(async move {
            let file_size = writer.close().await?.file_size as i64;
            let mut meta = build_meta(file_name, file_size, stats, schema_id, creation_time);
            ManagedBlobReferences::finish(
                blob_references,
                &file_io,
                &location.path,
                &bucket_dir,
                &mut meta,
            )
            .await?;
            meta.external_path = location.external_path;
            write_index(index, threshold, &file_io, &bucket_dir, &mut meta).await?;
            Ok(meta)
        });
    }

    async fn open_new_file(&mut self, user_schema: arrow_schema::SchemaRef) -> Result<()> {
        let index = self
            .config
            .file_index_options
            .as_ref()
            .map(|options| options.create_writer())
            .transpose()?;
        let file_name = format!(
            "{}{}-{}.{}",
            self.config.data_file_prefix,
            uuid::Uuid::new_v4(),
            self.written_files.len(),
            self.config.file_format,
        );
        let location = self.paths.new_path(&file_name)?;
        let bucket_dir = location.parent();
        self.file_io.mkdirs(&format!("{bucket_dir}/")).await?;
        let physical_schema = build_physical_schema(&user_schema);
        let file_path = location.path.clone();
        self.created_paths.push(file_path.clone());
        if index.is_some() {
            self.created_paths.push(format!(
                "{bucket_dir}/{}",
                data_file_to_file_index_file_name(&file_name)
            ));
        }
        let blob_references = ManagedBlobReferences::new(
            &self.config.value_fields,
            &CoreOptions::new(&self.config.table_options),
            &physical_schema,
            self.managed_blob_writer.enabled(),
            false,
        )?;
        if blob_references.is_some() {
            self.created_paths
                .push(format!("{file_path}{REFERENCE_FILE_SUFFIX}"));
        }
        let output = self.file_io.new_output(&file_path)?;
        let writer = create_format_writer(
            &output,
            physical_schema,
            &self.config.file_compression,
            self.config.file_compression_zstd_level,
            None,
            None,
            None,
        )
        .await?;
        self.current_writer = Some(with_write_resources(writer, self.resources.as_ref()));
        self.current_index = index;
        self.current_blob_references = blob_references;
        self.current_file_name = Some(file_name);
        self.current_file_path = Some(location);
        self.current_stats = PostponeFileStats::default();
        self.current_file_creation_time = Utc::now();
        Ok(())
    }

    async fn close_current_file(&mut self) -> Result<()> {
        let writer = match self.current_writer.take() {
            Some(w) => w,
            None => return Ok(()),
        };
        let file_name = self.current_file_name.take().unwrap();
        let index = self.current_index.take();
        let blob_references = self.current_blob_references.take();
        let stats = std::mem::take(&mut self.current_stats);
        let file_size = writer.close().await?.file_size as i64;

        let mut meta = build_meta(
            file_name,
            file_size,
            stats,
            self.config.schema_id,
            self.current_file_creation_time,
        );
        let location = self.current_file_path.take().unwrap();
        let bucket_dir = location.parent().to_string();
        let threshold = self
            .config
            .file_index_options
            .as_ref()
            .map(|options| options.in_manifest_threshold);
        ManagedBlobReferences::finish(
            blob_references,
            &self.file_io,
            &location.path,
            &bucket_dir,
            &mut meta,
        )
        .await?;
        meta.external_path = location.external_path;
        write_index(index, threshold, &self.file_io, &bucket_dir, &mut meta).await?;
        self.written_files.push(meta);
        Ok(())
    }
}

async fn write_index(
    index: Option<DataFileIndexWriter>,
    threshold: Option<i64>,
    file_io: &FileIO,
    bucket_dir: &str,
    meta: &mut DataFileMeta,
) -> Result<()> {
    if let Some(index) = index {
        let bytes = index.serialize()?;
        if bytes.len() as u64 > threshold.expect("index has threshold") as u64 {
            let name = data_file_to_file_index_file_name(&meta.file_name);
            file_io
                .new_output(&format!("{bucket_dir}/{name}"))?
                .write(bytes)
                .await?;
            meta.extra_files.push(name);
        } else {
            meta.embedded_index = Some(bytes.to_vec());
        }
    }
    Ok(())
}

/// Java KeyValueDataFileWriter retains the first/last keys even for unsorted
/// postpone input. These bounds must still be decodable using the key schema.
#[derive(Default)]
struct PostponeFileStats {
    row_count: i64,
    delete_row_count: i64,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    min_sequence_number: i64,
    max_sequence_number: i64,
}

impl PostponeFileStats {
    fn add_batch(
        &mut self,
        batch: &RecordBatch,
        config: &PostponeWriteConfig,
        min_seq: i64,
        max_seq: i64,
    ) -> Result<()> {
        let key_at = |row| -> Result<Vec<u8>> {
            Ok(BinaryRow::from_arrow(
                batch,
                row,
                &config.primary_key_indices,
                &config.value_fields,
            )?
            .to_serialized_bytes())
        };
        if self.row_count == 0 {
            self.first_key = key_at(0)?;
            self.min_sequence_number = min_seq;
        }
        self.last_key = key_at(batch.num_rows() - 1)?;
        self.max_sequence_number = max_seq;
        self.row_count += batch.num_rows() as i64;
        if let Some(column) = batch.column_by_name(VALUE_KIND_FIELD_NAME) {
            let kinds = column.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                crate::Error::DataInvalid {
                    message: "_VALUE_KIND column must be Int8".into(),
                    source: None,
                }
            })?;
            for kind in kinds.iter().flatten() {
                if !RowKind::from_value(kind)?.is_add() {
                    self.delete_row_count += 1;
                }
            }
        }
        Ok(())
    }
}

fn build_meta(
    file_name: String,
    file_size: i64,
    stats: PostponeFileStats,
    schema_id: i64,
    creation_time: DateTime<Utc>,
) -> DataFileMeta {
    DataFileMeta {
        file_name,
        file_size,
        row_count: stats.row_count,
        min_key: stats.first_key,
        max_key: stats.last_key,
        key_stats: BinaryTableStats::new(
            EMPTY_SERIALIZED_ROW.clone(),
            EMPTY_SERIALIZED_ROW.clone(),
            vec![],
        ),
        value_stats: BinaryTableStats::new(
            EMPTY_SERIALIZED_ROW.clone(),
            EMPTY_SERIALIZED_ROW.clone(),
            vec![],
        ),
        min_sequence_number: stats.min_sequence_number,
        max_sequence_number: stats.max_sequence_number,
        schema_id,
        level: 0,
        extra_files: vec![],
        creation_time: Some(creation_time),
        delete_row_count: Some(stats.delete_row_count),
        embedded_index: None,
        file_source: Some(0), // FileSource.APPEND
        value_stats_cols: Some(vec![]),
        external_path: None,
        first_row_id: None,
        write_cols: None,
        column_max_sequence_numbers: None,
    }
}
