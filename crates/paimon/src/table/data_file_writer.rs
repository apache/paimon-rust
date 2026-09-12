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

//! Low-level data file writer shared by [`TableWrite`](super::TableWrite) and
//! [`DataEvolutionPartialWriter`](super::data_evolution_writer::DataEvolutionPartialWriter).
//!
//! `DataFileWriter` streams Arrow `RecordBatch`es to Parquet files on storage,
//! handles file rolling when `target_file_size` is reached, and collects
//! [`DataFileMeta`] for the commit path.

use super::data_file_index_writer::{DataFileIndexWriter, FileIndexOptions};
use crate::arrow::format::{create_format_writer, FormatFileWriter, FormatValueStats};
use crate::io::FileIO;
use crate::spec::data_file_to_file_index_file_name;
use crate::spec::stats::BinaryTableStats;
use crate::spec::{bucket_dir_name, DataField, DataFileMeta, EMPTY_SERIALIZED_ROW};
use crate::Result;
use arrow_array::RecordBatch;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinSet;

/// Low-level writer that produces Parquet data files for a single (partition, bucket).
///
/// Batches are accumulated into a single `FormatFileWriter` that streams directly
/// to storage. When `target_file_size` is reached the current file is rolled
/// (closed in the background) and a new one is opened on the next write.
///
/// Call [`prepare_commit`](Self::prepare_commit) to finalize and collect file metadata.
pub(crate) struct DataFileWriter {
    file_io: FileIO,
    table_location: String,
    partition_path: String,
    bucket: i32,
    schema_id: i64,
    target_file_size: i64,
    file_compression: String,
    file_compression_zstd_level: i32,
    write_buffer_size: i64,
    file_format: String,
    write_fields: Vec<DataField>,
    format_options: HashMap<String, String>,
    file_source: Option<i32>,
    first_row_id: Option<i64>,
    write_cols: Option<Vec<String>>,
    written_files: Vec<DataFileMeta>,
    /// Background file close tasks spawned during rolling.
    in_flight_closes: JoinSet<Result<DataFileMeta>>,
    /// Current open format writer, lazily created on first write.
    current_writer: Option<Box<dyn FormatFileWriter>>,
    current_file_name: Option<String>,
    current_row_count: i64,
    index_options: Option<Arc<FileIndexOptions>>,
    current_index: Option<DataFileIndexWriter>,
    /// Paths owned by this indexed write until prepare_commit hands them to the caller.
    created_paths: Vec<String>,
    failed: bool,
}

impl DataFileWriter {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        file_io: FileIO,
        table_location: String,
        partition_path: String,
        bucket: i32,
        schema_id: i64,
        target_file_size: i64,
        file_compression: String,
        file_compression_zstd_level: i32,
        write_buffer_size: i64,
        file_format: String,
        write_fields: Vec<DataField>,
        format_options: HashMap<String, String>,
        file_source: Option<i32>,
        first_row_id: Option<i64>,
        write_cols: Option<Vec<String>>,
    ) -> Self {
        Self {
            file_io,
            table_location,
            partition_path,
            bucket,
            schema_id,
            target_file_size,
            file_compression,
            file_compression_zstd_level,
            write_buffer_size,
            file_format,
            write_fields,
            format_options,
            file_source,
            first_row_id,
            write_cols,
            written_files: Vec::new(),
            in_flight_closes: JoinSet::new(),
            current_writer: None,
            current_file_name: None,
            current_row_count: 0,
            index_options: None,
            current_index: None,
            created_paths: Vec::new(),
            failed: false,
        }
    }

    pub(super) fn with_file_index(mut self, options: Option<Arc<FileIndexOptions>>) -> Self {
        self.index_options = options;
        self
    }

    /// Write a RecordBatch. Rolls to a new file when target size is reached.
    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.ensure_writable()?;
        let result = self.write_batch(batch).await;
        if result.is_err() && self.index_options.is_some() {
            self.abort().await;
        }
        result
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        if self.current_writer.is_none() {
            self.open_new_file(batch.schema()).await?;
        }

        self.current_writer.as_mut().unwrap().write(batch).await?;
        if let Some(index) = &mut self.current_index {
            index.write(batch)?;
        }
        self.current_row_count += batch.num_rows() as i64;

        // Roll to a new file if target size is reached — close in background
        if self.current_writer.as_ref().unwrap().num_bytes() as i64 >= self.target_file_size {
            self.roll_file();
        }

        // Flush row group if in-progress buffer exceeds write_buffer_size
        if let Some(w) = self.current_writer.as_mut() {
            if w.in_progress_size() as i64 >= self.write_buffer_size {
                w.flush().await?;
            }
        }

        Ok(())
    }

    async fn open_new_file(&mut self, schema: arrow_schema::SchemaRef) -> Result<()> {
        let index = self
            .index_options
            .as_ref()
            .map(|options| options.create_writer())
            .transpose()?;
        let file_name = format!(
            "data-{}-{}.{}",
            uuid::Uuid::new_v4(),
            self.written_files.len(),
            self.file_format,
        );
        let bucket_dir = self.bucket_dir();
        self.file_io.mkdirs(&format!("{bucket_dir}/")).await?;

        let file_path = format!("{bucket_dir}/{file_name}");
        if self.index_options.is_some() {
            self.created_paths.push(file_path.clone());
            self.created_paths.push(format!(
                "{bucket_dir}/{}",
                data_file_to_file_index_file_name(&file_name)
            ));
        }
        let output = self.file_io.new_output(&file_path)?;
        let writer = create_format_writer(
            &output,
            schema,
            &self.file_compression,
            self.file_compression_zstd_level,
            Some(self.file_io.clone()),
            Some(&self.write_fields),
            Some(&self.format_options),
        )
        .await?;
        self.current_writer = Some(writer);
        self.current_index = index;
        self.current_file_name = Some(file_name);
        self.current_row_count = 0;
        Ok(())
    }

    /// Close the current file writer and record the file metadata.
    pub(crate) async fn close_current_file(&mut self) -> Result<()> {
        if let Some(close) = self.take_close() {
            self.written_files.push(close.await?);
        }
        Ok(())
    }

    /// Spawn the current writer's close in the background for non-blocking rolling.
    fn roll_file(&mut self) {
        if let Some(close) = self.take_close() {
            self.in_flight_closes.spawn(close);
        }
    }

    fn take_close(
        &mut self,
    ) -> Option<impl std::future::Future<Output = Result<DataFileMeta>> + Send + 'static> {
        let writer = self.current_writer.take()?;
        let index = self.current_index.take();
        let file_io = self.file_io.clone();
        let bucket_dir = self.bucket_dir();
        let threshold = self
            .index_options
            .as_ref()
            .map(|options| options.in_manifest_threshold);
        let file_name = self.current_file_name.take().unwrap();
        let row_count = self.current_row_count;
        self.current_row_count = 0;
        let schema_id = self.schema_id;
        let file_source = self.file_source;
        let first_row_id = self.first_row_id;
        let write_cols = self.write_cols.clone();

        Some(async move {
            let write_result = writer.close().await?;
            let mut meta = Self::build_meta(
                file_name,
                write_result.file_size as i64,
                row_count,
                schema_id,
                file_source,
                first_row_id,
                write_cols,
                write_result.value_stats,
            );
            if let Some(index) = index {
                let bytes = index.serialize()?;
                if bytes.len() as u64 > threshold.unwrap() as u64 {
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
            Ok(meta)
        })
    }

    /// Close the current writer and return all written file metadata.
    pub(crate) async fn prepare_commit(&mut self) -> Result<Vec<DataFileMeta>> {
        self.ensure_writable()?;
        let result = self.finish().await;
        if result.is_err() && self.index_options.is_some() {
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
        self.created_paths.clear();
        Ok(std::mem::take(&mut self.written_files))
    }

    pub(super) async fn abort(&mut self) {
        self.failed = true;
        if let Some(writer) = self.current_writer.take() {
            let _ = writer.close().await;
        }
        self.current_index = None;
        self.current_file_name = None;
        while self.in_flight_closes.join_next().await.is_some() {}
        for path in self.created_paths.drain(..) {
            let _ = self.file_io.delete_file(&path).await;
        }
        self.written_files.clear();
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.failed {
            return Err(crate::Error::DataInvalid {
                message: "Cannot reuse a failed indexed data-file writer".to_string(),
                source: None,
            });
        }
        Ok(())
    }

    fn bucket_dir(&self) -> String {
        if self.partition_path.is_empty() {
            format!("{}/{}", self.table_location, bucket_dir_name(self.bucket))
        } else {
            format!(
                "{}/{}/{}",
                self.table_location,
                self.partition_path,
                bucket_dir_name(self.bucket)
            )
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn build_meta(
        file_name: String,
        file_size: i64,
        row_count: i64,
        schema_id: i64,
        file_source: Option<i32>,
        first_row_id: Option<i64>,
        write_cols: Option<Vec<String>>,
        format_value_stats: Option<FormatValueStats>,
    ) -> DataFileMeta {
        let (value_stats, value_stats_cols) = match format_value_stats {
            Some(stats) => (stats.stats, stats.columns),
            None => (BinaryTableStats::empty(), Some(Vec::new())),
        };
        DataFileMeta {
            file_name,
            file_size,
            row_count,
            min_key: EMPTY_SERIALIZED_ROW.clone(),
            max_key: EMPTY_SERIALIZED_ROW.clone(),
            key_stats: BinaryTableStats::empty(),
            value_stats,
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id,
            level: 0,
            extra_files: vec![],
            creation_time: Some(Utc::now()),
            delete_row_count: Some(0),
            embedded_index: None,
            file_source,
            value_stats_cols,
            external_path: None,
            first_row_id,
            write_cols,
            column_max_sequence_numbers: None,
        }
    }
}
