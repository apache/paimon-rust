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
//! Uses a special file naming prefix: `data-u-{commitUser}-s-0-w-`.
//!
//! Reference: [PostponeBucketWriter](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/sink/PostponeBucketWriter.java)

use crate::arrow::format::{create_format_writer, FormatFileWriter};
use crate::io::FileIO;
use crate::spec::stats::BinaryTableStats;
use crate::spec::{bucket_dir_name, DataFileMeta, EMPTY_SERIALIZED_ROW, VALUE_KIND_FIELD_NAME};
use crate::table::kv_file_writer::build_physical_schema;
use crate::Result;
use arrow_array::{Int64Array, Int8Array, RecordBatch};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::task::JoinSet;

/// Configuration for [`PostponeFileWriter`].
pub(crate) struct PostponeWriteConfig {
    pub table_location: String,
    pub partition_path: String,
    pub bucket: i32,
    pub schema_id: i64,
    pub target_file_size: i64,
    pub file_compression: String,
    pub file_compression_zstd_level: i32,
    pub write_buffer_size: i64,
    /// Data file name prefix: `"data-u-{commitUser}-s-0-w-"`.
    pub data_file_prefix: String,
}

/// Writer for postpone bucket mode (`bucket = -2`).
///
/// Streams data directly to a FormatFileWriter in arrival order (no sort/dedup),
/// prepending `_SEQUENCE_NUMBER` and `_VALUE_KIND` columns to each batch.
/// Rolls to a new file when `target_file_size` is reached.
pub(crate) struct PostponeFileWriter {
    file_io: FileIO,
    config: PostponeWriteConfig,
    next_sequence_number: i64,
    current_writer: Option<Box<dyn FormatFileWriter>>,
    current_file_name: Option<String>,
    current_row_count: i64,
    /// Sequence number at which the current file started.
    current_file_start_seq: i64,
    /// Timestamp captured when the current file was opened (used for deterministic replay order).
    current_file_creation_time: DateTime<Utc>,
    written_files: Vec<DataFileMeta>,
    /// Background file close tasks spawned during rolling.
    in_flight_closes: JoinSet<Result<DataFileMeta>>,
}

impl PostponeFileWriter {
    pub(crate) fn new(file_io: FileIO, config: PostponeWriteConfig) -> Self {
        Self {
            file_io,
            config,
            next_sequence_number: 0,
            current_writer: None,
            current_file_name: None,
            current_row_count: 0,
            current_file_start_seq: 0,
            current_file_creation_time: Utc::now(),
            written_files: Vec::new(),
            in_flight_closes: JoinSet::new(),
        }
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        if self.current_writer.is_none() {
            self.open_new_file(batch.schema()).await?;
        }

        let num_rows = batch.num_rows();
        let start_seq = self.next_sequence_number;
        let end_seq = start_seq + num_rows as i64 - 1;
        self.next_sequence_number = end_seq + 1;

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

        self.current_row_count += num_rows as i64;
        self.current_writer
            .as_mut()
            .unwrap()
            .write(&physical_batch)
            .await?;

        // Roll to a new file if target size is reached — close in background
        if self.current_writer.as_ref().unwrap().num_bytes() as i64 >= self.config.target_file_size
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

    pub(crate) async fn prepare_commit(&mut self) -> Result<Vec<DataFileMeta>> {
        self.close_current_file().await?;
        while let Some(result) = self.in_flight_closes.join_next().await {
            let meta = result.map_err(|e| crate::Error::DataInvalid {
                message: format!("Background file close task panicked: {e}"),
                source: None,
            })??;
            self.written_files.push(meta);
        }
        Ok(std::mem::take(&mut self.written_files))
    }

    /// Spawn the current writer's close in the background for non-blocking rolling.
    fn roll_file(&mut self) {
        let writer = match self.current_writer.take() {
            Some(w) => w,
            None => return,
        };
        let file_name = self.current_file_name.take().unwrap();
        let row_count = self.current_row_count;
        let min_seq = self.current_file_start_seq;
        let max_seq = self.next_sequence_number - 1;
        self.current_row_count = 0;
        let schema_id = self.config.schema_id;
        // Capture creation_time from when the file was opened, not when the async close finishes.
        // Java's postpone compaction sorts by creationTime for replay order.
        let creation_time = self.current_file_creation_time;

        self.in_flight_closes.spawn(async move {
            let file_size = writer.close().await? as i64;
            Ok(build_meta(
                file_name,
                file_size,
                row_count,
                min_seq,
                max_seq,
                schema_id,
                creation_time,
            ))
        });
    }

    async fn open_new_file(&mut self, user_schema: arrow_schema::SchemaRef) -> Result<()> {
        let file_name = format!(
            "{}{}-{}.parquet",
            self.config.data_file_prefix,
            uuid::Uuid::new_v4(),
            self.written_files.len()
        );
        let bucket_dir = if self.config.partition_path.is_empty() {
            format!(
                "{}/{}",
                self.config.table_location,
                bucket_dir_name(self.config.bucket)
            )
        } else {
            format!(
                "{}/{}/{}",
                self.config.table_location,
                self.config.partition_path,
                bucket_dir_name(self.config.bucket)
            )
        };
        self.file_io.mkdirs(&format!("{bucket_dir}/")).await?;
        let physical_schema = build_physical_schema(&user_schema);
        let file_path = format!("{bucket_dir}/{file_name}");
        let output = self.file_io.new_output(&file_path)?;
        let writer = create_format_writer(
            &output,
            physical_schema,
            &self.config.file_compression,
            self.config.file_compression_zstd_level,
        )
        .await?;
        self.current_writer = Some(writer);
        self.current_file_name = Some(file_name);
        self.current_row_count = 0;
        self.current_file_start_seq = self.next_sequence_number;
        self.current_file_creation_time = Utc::now();
        Ok(())
    }

    async fn close_current_file(&mut self) -> Result<()> {
        let writer = match self.current_writer.take() {
            Some(w) => w,
            None => return Ok(()),
        };
        let file_name = self.current_file_name.take().unwrap();
        let row_count = self.current_row_count;
        self.current_row_count = 0;
        let file_size = writer.close().await? as i64;

        let min_seq = self.current_file_start_seq;
        let max_seq = self.next_sequence_number - 1;

        let meta = build_meta(
            file_name,
            file_size,
            row_count,
            min_seq,
            max_seq,
            self.config.schema_id,
            self.current_file_creation_time,
        );
        self.written_files.push(meta);
        Ok(())
    }
}

fn build_meta(
    file_name: String,
    file_size: i64,
    row_count: i64,
    min_seq: i64,
    max_seq: i64,
    schema_id: i64,
    creation_time: DateTime<Utc>,
) -> DataFileMeta {
    DataFileMeta {
        file_name,
        file_size,
        row_count,
        min_key: EMPTY_SERIALIZED_ROW.clone(),
        max_key: EMPTY_SERIALIZED_ROW.clone(),
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
        min_sequence_number: min_seq,
        max_sequence_number: max_seq,
        schema_id,
        level: 0,
        extra_files: vec![],
        creation_time: Some(creation_time),
        delete_row_count: Some(0),
        embedded_index: None,
        file_source: Some(0), // FileSource.APPEND
        value_stats_cols: Some(vec![]),
        external_path: None,
        first_row_id: None,
        write_cols: None,
    }
}
