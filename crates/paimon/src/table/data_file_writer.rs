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
//! handles file rolling when the configured file size or row count is reached, and collects
//! [`DataFileMeta`] for the commit path.

use super::data_file_index_writer::{DataFileIndexWriter, FileIndexOptions};
use crate::arrow::format::{
    create_format_writer, with_write_resources, FormatFileWriter, FormatValueStats,
};
use crate::io::FileIO;
use crate::resource::ResourceContext;
use crate::spec::data_file_to_file_index_file_name;
use crate::spec::stats::BinaryTableStats;
use crate::spec::{bucket_path_under, CoreOptions, DataField, DataFileMeta, EMPTY_SERIALIZED_ROW};
use crate::Result;
use arrow_array::RecordBatch;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinSet;

/// Low-level writer that produces Parquet data files for a single (partition, bucket).
///
/// Batches are accumulated into a single `FormatFileWriter` that streams directly
/// to storage. When the size or row count target is reached the current file is rolled
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
    target_file_row_num: i64,
    file_compression: String,
    file_compression_zstd_level: i32,
    write_buffer_size: i64,
    file_format: String,
    data_file_prefix: String,
    write_fields: Vec<DataField>,
    format_options: HashMap<String, String>,
    file_source: Option<i32>,
    first_row_id: Option<i64>,
    write_cols: Option<Vec<String>>,
    written_files: Vec<(usize, DataFileMeta)>,
    next_file_ordinal: usize,
    /// Background file close tasks spawned during rolling.
    in_flight_closes: JoinSet<Result<(usize, DataFileMeta)>>,
    /// Current open format writer, lazily created on first write.
    current_writer: Option<Box<dyn FormatFileWriter>>,
    current_file_name: Option<String>,
    current_row_count: i64,
    index_options: Option<Arc<FileIndexOptions>>,
    current_index: Option<DataFileIndexWriter>,
    resources: Option<ResourceContext>,
    /// Paths owned by this write until prepare_commit hands them to the caller.
    created_paths: Vec<String>,
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
        let data_file_prefix = CoreOptions::new(&format_options)
            .data_file_prefix()
            .to_string();
        Self {
            file_io,
            table_location,
            partition_path,
            bucket,
            schema_id,
            target_file_size,
            target_file_row_num: i64::MAX,
            file_compression,
            file_compression_zstd_level,
            write_buffer_size,
            file_format,
            data_file_prefix,
            write_fields,
            format_options,
            file_source,
            first_row_id,
            write_cols,
            written_files: Vec::new(),
            next_file_ordinal: 0,
            in_flight_closes: JoinSet::new(),
            current_writer: None,
            current_file_name: None,
            current_row_count: 0,
            index_options: None,
            current_index: None,
            resources: None,
            created_paths: Vec::new(),
        }
    }

    pub(super) fn with_file_index(mut self, options: Option<Arc<FileIndexOptions>>) -> Self {
        self.index_options = options;
        self
    }

    pub(crate) fn with_target_file_row_num(mut self, rows: i64) -> Self {
        debug_assert!(rows > 0);
        self.target_file_row_num = rows;
        self
    }

    pub(crate) fn with_resources(mut self, resources: Option<ResourceContext>) -> Self {
        self.resources = resources;
        self
    }

    pub(crate) fn set_resources(&mut self, resources: Option<ResourceContext>) {
        self.resources = resources;
    }

    /// Write a RecordBatch. Rolls when either target size or row count is reached.
    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let result = self.write_batch(batch).await;
        if self.index_options.is_some() && result.is_err() {
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

        // Like Java's bundled write, a batch stays intact even if it crosses
        // the limit. The next batch opens a new file.
        if self.current_row_count >= self.target_file_row_num
            || self.current_writer.as_ref().unwrap().num_bytes() as i64 >= self.target_file_size
        {
            self.roll_file();
        }

        if let Some(writer) = self.current_writer.as_mut() {
            if writer.in_progress_size() as i64 >= self.write_buffer_size {
                writer.flush().await?;
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
            "{}{}-{}.{}",
            self.data_file_prefix,
            uuid::Uuid::new_v4(),
            self.next_file_ordinal,
            self.file_format,
        );
        let bucket_dir = self.bucket_dir();
        self.file_io.mkdirs(&format!("{bucket_dir}/")).await?;

        let file_path = format!("{bucket_dir}/{file_name}");
        self.created_paths.push(file_path.clone());
        if self.index_options.is_some() {
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
        self.current_writer = Some(with_write_resources(writer, self.resources.as_ref()));
        self.current_index = index;
        self.current_file_name = Some(file_name);
        self.current_row_count = 0;
        Ok(())
    }

    /// Close the current file writer and record the file metadata.
    pub(crate) async fn close_current_file(&mut self) -> Result<()> {
        if let Some(close) = self.take_close() {
            let ordinal = self.next_file_ordinal;
            self.next_file_ordinal += 1;
            self.written_files.push((ordinal, close.await?));
        }
        Ok(())
    }

    /// Spawn the current writer's close in the background for non-blocking rolling.
    fn roll_file(&mut self) {
        if let Some(close) = self.take_close() {
            let ordinal = self.next_file_ordinal;
            self.next_file_ordinal += 1;
            self.in_flight_closes
                .spawn(async move { Ok((ordinal, close.await?)) });
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
        let result = self.finish().await;
        if result.is_err() && self.index_options.is_some() {
            self.abort().await;
        }
        result
    }

    /// Prepare a group of writers atomically with respect to file ownership.
    /// Wait for every close before cleanup; dropping close futures can leave
    /// outputs appearing after an abort has already removed their paths.
    pub(super) async fn prepare_all<K>(
        writers: impl IntoIterator<Item = (K, Self)>,
    ) -> Result<Vec<(K, Vec<DataFileMeta>)>> {
        let results =
            futures::future::join_all(writers.into_iter().map(|(key, mut writer)| async {
                let result = writer.prepare_commit().await;
                (key, writer, result)
            }))
            .await;
        if results.iter().any(|(_, _, result)| result.is_err()) {
            let mut first_error = None;
            for (_, mut writer, result) in results {
                match result {
                    Ok(files) => {
                        // Successful prepare transferred these paths to us.
                        for file in files {
                            for path in file.collect_files(&writer.bucket_dir()) {
                                let _ = writer.file_io.delete_file(&path).await;
                            }
                        }
                    }
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
                writer.abort().await;
            }
            return Err(first_error.unwrap());
        }
        results
            .into_iter()
            .map(|(key, _, files)| files.map(|files| (key, files)))
            .collect()
    }

    async fn finish(&mut self) -> Result<Vec<DataFileMeta>> {
        self.close_current_file().await?;
        while let Some(result) = self.in_flight_closes.join_next().await {
            let file = result.map_err(|e| crate::Error::DataInvalid {
                message: format!("Background file close task panicked: {e}"),
                source: None,
            })??;
            self.written_files.push(file);
        }
        self.created_paths.clear();
        let mut files = std::mem::take(&mut self.written_files);
        files.sort_unstable_by_key(|(ordinal, _)| *ordinal);
        Ok(files.into_iter().map(|(_, meta)| meta).collect())
    }

    pub(super) async fn abort(&mut self) {
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

    fn bucket_dir(&self) -> String {
        bucket_path_under(&self.table_location, &self.partition_path, self.bucket)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{FileIOBuilder, FileIOProvider};
    use crate::spec::{DataType, IntType};
    use arrow_array::Int32Array;
    use arrow_schema::{DataType as ArrowDataType, Field, Schema};
    use opendal::Operator;
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct RecordingProvider {
        operator: Operator,
        paths: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl FileIOProvider for RecordingProvider {
        async fn create(&self, path: &str) -> Result<(Operator, String)> {
            let relative_path = path
                .strip_prefix("s3://bucket/")
                .expect("writer path must use the configured bucket")
                .to_string();
            self.paths.lock().unwrap().push(relative_path.clone());
            Ok((self.operator.clone(), relative_path))
        }
    }

    #[tokio::test]
    async fn partitioned_writer_does_not_create_double_slash_bucket_paths() {
        let provider = Arc::new(RecordingProvider {
            operator: Operator::from_config(opendal::services::MemoryConfig::default()).unwrap(),
            paths: Mutex::new(Vec::new()),
        });
        let file_io = FileIOBuilder::new("unused")
            .with_provider(provider.clone())
            .build()
            .unwrap();
        let mut writer = DataFileWriter::new(
            file_io,
            "s3://bucket/table".to_string(),
            "dt=2026-09-17/".to_string(),
            0,
            0,
            i64::MAX,
            "none".to_string(),
            0,
            1024,
            "parquet".to_string(),
            vec![DataField::new(
                0,
                "id".to_string(),
                DataType::Int(IntType::new()),
            )],
            HashMap::new(),
            None,
            None,
            None,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));

        writer.open_new_file(schema).await.unwrap();

        assert!(provider
            .paths
            .lock()
            .unwrap()
            .iter()
            .all(|path| !path.contains("//")));
    }

    #[tokio::test]
    async fn row_limit_rolls_after_whole_batches_and_preserves_file_order() {
        let mut writer = DataFileWriter::new(
            FileIOBuilder::new("memory").build().unwrap(),
            "memory:///row-limit-test".to_string(),
            String::new(),
            0,
            0,
            i64::MAX,
            "none".to_string(),
            0,
            i64::MAX,
            "parquet".to_string(),
            vec![DataField::new(
                0,
                "id".to_string(),
                DataType::Int(IntType::new()),
            )],
            HashMap::new(),
            Some(0),
            None,
            None,
        )
        .with_target_file_row_num(2);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        for values in [vec![1], vec![2], vec![3, 4, 5], vec![6]] {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))])
                    .unwrap();
            writer.write(&batch).await.unwrap();
        }

        let files = writer.prepare_commit().await.unwrap();
        // Two single-row batches share a file; the three-row batch stays intact.
        assert_eq!(
            files.iter().map(|file| file.row_count).collect::<Vec<_>>(),
            vec![2, 3, 1]
        );
    }

    #[tokio::test]
    async fn grouped_prepare_failure_removes_successful_and_failed_outputs() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let mut writers = Vec::new();
        for first_row_id in [0, 1] {
            let mut writer = DataFileWriter::new(
                file_io.clone(),
                "memory:///prepare-failure".into(),
                String::new(),
                0,
                0,
                i64::MAX,
                "none".into(),
                0,
                i64::MAX,
                "parquet".into(),
                vec![DataField::new(
                    0,
                    "id".into(),
                    DataType::Int(IntType::new()),
                )],
                HashMap::new(),
                Some(first_row_id),
                None,
                None,
            );
            let batch = RecordBatch::try_from_iter([(
                "id",
                Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
            )])
            .unwrap();
            writer.write(&batch).await.unwrap();
            if first_row_id == 1 {
                // Simulate a background file close failing after another file
                // in the operation has already finished successfully.
                writer.in_flight_closes.spawn(async {
                    Err(crate::Error::DataInvalid {
                        message: "injected close failure".into(),
                        source: None,
                    })
                });
            }
            writers.push((first_row_id, writer));
        }
        let error = DataFileWriter::prepare_all(writers).await.unwrap_err();
        assert!(error.to_string().contains("injected close failure"));
        assert!(file_io
            .list_status_recursive("memory:///prepare-failure")
            .await
            .unwrap()
            .iter()
            .all(|entry| !entry.path.ends_with(".parquet")));
    }
}
