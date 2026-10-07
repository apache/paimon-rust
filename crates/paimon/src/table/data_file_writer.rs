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
use super::data_file_path_factory::{DataFilePath, DataFilePathFactory};
use super::row_sidecar::RowSidecarWriter;
use crate::arrow::format::{
    create_format_writer_factory, with_write_resources, FormatFileWriter, FormatValueStats,
    FormatWriterFactory,
};
use crate::io::FileIO;
use crate::resource::ResourceContext;
use crate::spec::data_file_to_file_index_file_name;
use crate::spec::stats::BinaryTableStats;
use crate::spec::{CoreOptions, DataField, DataFileMeta, EMPTY_SERIALIZED_ROW};
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
    paths: Arc<DataFilePathFactory>,
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
    format_writer_factory: Option<Arc<dyn FormatWriterFactory>>,
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
    current_file_path: Option<DataFilePath>,
    current_row_count: i64,
    index_options: Option<Arc<FileIndexOptions>>,
    current_index: Option<DataFileIndexWriter>,
    row_sidecar_enabled: bool,
    current_row_sidecar: Option<RowSidecarWriter>,
    resources: Option<ResourceContext>,
    /// Created paths; cleanup respects the format's deletion policy until
    /// prepare_commit hands the files to the caller.
    created_paths: Vec<String>,
    delete_file_upon_abort: bool,
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
    ) -> Result<Self> {
        let paths = Arc::new(DataFilePathFactory::new(
            &table_location,
            &partition_path,
            bucket,
            &format_options,
        )?);
        let data_file_prefix = CoreOptions::new(&format_options)
            .data_file_prefix()
            .to_string();
        let options = CoreOptions::new(&format_options);
        let row_sidecar_enabled = options.data_evolution_enabled()
            && options.data_evolution_row_sidecar_enabled()?
            && !file_format.eq_ignore_ascii_case("blob")
            && !file_format.starts_with("vector.");
        Ok(Self {
            row_sidecar_enabled,
            current_row_sidecar: None,
            format_writer_factory: None,
            file_io,
            paths,
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
            current_file_path: None,
            current_row_count: 0,
            index_options: None,
            current_index: None,
            resources: None,
            created_paths: Vec::new(),
            delete_file_upon_abort: true,
        })
    }

    pub(super) fn with_path_factory(mut self, paths: Arc<DataFilePathFactory>) -> Self {
        self.paths = paths;
        self
    }

    pub(super) fn with_file_index(mut self, options: Option<Arc<FileIndexOptions>>) -> Self {
        self.index_options = options;
        self
    }

    /// Configure only the dedicated BLOB factory; ordinary format factories
    /// and their write paths do not carry callback state.
    pub(super) fn set_blob_consumer(
        &mut self,
        consumer: Arc<dyn crate::spec::BlobConsumer>,
    ) -> Result<()> {
        debug_assert!(self.current_writer.is_none());
        self.format_writer_factory = Some(Arc::new(
            crate::arrow::format::blob::BlobWriterFactory::new(
                Some(self.file_io.clone()),
                self.write_fields.first(),
                Some(&self.format_options),
            )?
            .with_consumer(consumer),
        ));
        // Java BlobFormatWriter relinquishes deletion rights whenever a
        // consumer can expose a descriptor, even if a later callback fails.
        self.delete_file_upon_abort = false;
        Ok(())
    }

    /// Java's dedicated-format writer does not create auxiliary ROW files,
    /// including for its normal columns.
    pub(super) fn without_row_sidecar(mut self) -> Self {
        self.row_sidecar_enabled = false;
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
        if (self.index_options.is_some() || self.row_sidecar_enabled) && result.is_err() {
            self.abort().await;
        }
        result
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        super::inline_blob::validate_inline_blob_columns(batch, &self.format_options)?;
        if self.current_writer.is_none() {
            self.open_new_file(batch.schema()).await?;
        }

        self.current_writer.as_mut().unwrap().write(batch).await?;
        if let Some(sidecar) = self.current_row_sidecar.as_mut() {
            sidecar.write(batch).await?;
        }
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

    pub(super) fn has_open_file(&self) -> bool {
        self.current_writer.is_some()
    }

    async fn open_new_file(&mut self, schema: arrow_schema::SchemaRef) -> Result<()> {
        // Stateful factories may need the previous close callback before they
        // create the next file's plan. Stateless factories keep asynchronous rolling.
        if self
            .format_writer_factory
            .as_ref()
            .is_some_and(|factory| factory.needs_completed_file_stats())
        {
            self.drain_closes().await?;
        }
        let index = self
            .index_options
            .as_ref()
            .map(|options| options.create_writer())
            .transpose()?;
        let file_name = self
            .paths
            .new_file_name(&self.data_file_prefix, &self.file_format);
        let location = self.paths.new_path(&file_name)?;
        let bucket_dir = location.parent();
        self.file_io.mkdirs(&format!("{bucket_dir}/")).await?;

        let file_path = location.path.clone();
        self.created_paths.push(file_path.clone());
        if self.index_options.is_some() {
            self.created_paths.push(format!(
                "{bucket_dir}/{}",
                data_file_to_file_index_file_name(&file_name)
            ));
        }
        let output = self.file_io.new_output(&file_path)?;
        let factory = match &self.format_writer_factory {
            Some(factory) => factory.clone(),
            None => {
                let factory = create_format_writer_factory(
                    &self.file_format,
                    schema.clone(),
                    self.file_compression_zstd_level,
                    Some(self.file_io.clone()),
                    Some(&self.write_fields),
                    Some(&self.format_options),
                    None,
                )?;
                self.format_writer_factory = Some(factory.clone());
                factory
            }
        };
        let writer = factory
            .create_writer(&output, &self.file_compression)
            .await?;
        self.current_writer = Some(with_write_resources(writer, self.resources.as_ref()));
        self.current_index = index;
        self.current_file_name = Some(file_name.clone());
        self.current_file_path = Some(location);
        self.current_row_count = 0;
        self.open_row_sidecar(&file_path, &file_name, schema)
            .await?;
        Ok(())
    }

    async fn open_row_sidecar(
        &mut self,
        path: &str,
        file_name: &str,
        schema: arrow_schema::SchemaRef,
    ) -> Result<()> {
        if self.row_sidecar_enabled {
            let path = format!("{path}.row");
            self.created_paths.push(path.clone());
            self.current_row_sidecar = Some(
                RowSidecarWriter::new(
                    &self.file_io,
                    &path,
                    file_name,
                    schema,
                    &self.write_fields,
                    &self.format_options,
                    self.resources.as_ref(),
                )
                .await?,
            );
        }
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
        let row_sidecar = self.current_row_sidecar.take();
        let file_io = self.file_io.clone();
        let location = self.current_file_path.take().unwrap();
        let bucket_dir = location.parent().to_string();
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
        // Java creates a new row sequence counter for each physical DE file.
        let max_sequence_number = if CoreOptions::new(&self.format_options).data_evolution_enabled()
        {
            row_count - 1
        } else {
            0
        };

        Some(async move {
            // Close both outputs even if an auxiliary close fails. All paths
            // remain producer-owned until the whole prepare succeeds.
            let sidecar_result = match row_sidecar {
                Some(sidecar) => sidecar
                    .writer
                    .close()
                    .await
                    .map(|_| Some(sidecar.file_name)),
                None => Ok(None),
            };
            let write_result = writer.close().await;
            let sidecar_name = sidecar_result?;
            let write_result = write_result?;
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
            meta.max_sequence_number = max_sequence_number;
            meta.external_path = location.external_path;
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
            if let Some(name) = sidecar_name {
                meta.extra_files.push(name);
            }
            Ok(meta)
        })
    }

    /// Close the current writer and return all written file metadata.
    pub(crate) async fn prepare_commit(&mut self) -> Result<Vec<DataFileMeta>> {
        let result = self.finish().await;
        if result.is_err() && (self.index_options.is_some() || self.row_sidecar_enabled) {
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
        let mut writers: Vec<_> = writers.into_iter().collect();
        let files = Self::prepare_group(writers.iter_mut().map(|(_, writer)| writer)).await?;
        Ok(writers
            .into_iter()
            .zip(files)
            .map(|((key, _), files)| (key, files))
            .collect())
    }

    /// Close all physical columns as one operation. Successful closes transfer
    /// ownership here until every other column succeeds, including Blob files.
    pub(super) async fn prepare_group<'a>(
        writers: impl IntoIterator<Item = &'a mut Self>,
    ) -> Result<Vec<Vec<DataFileMeta>>> {
        let results = futures::future::join_all(writers.into_iter().map(|writer| async {
            let result = writer.prepare_commit().await;
            (writer, result)
        }))
        .await;
        if results.iter().any(|(_, result)| result.is_err()) {
            let mut first_error = None;
            for (writer, result) in results {
                match result {
                    Ok(files) => {
                        writer.delete_files(&files).await;
                    }
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
                writer.abort().await;
            }
            return Err(first_error.unwrap());
        }
        results.into_iter().map(|(_, files)| files).collect()
    }

    #[cfg(test)]
    pub(super) fn inject_close_failure(&mut self) {
        self.in_flight_closes.spawn(async {
            Err(crate::Error::DataInvalid {
                message: "injected close failure".into(),
                source: None,
            })
        });
    }

    async fn drain_closes(&mut self) -> Result<()> {
        while let Some(result) = self.in_flight_closes.join_next().await {
            let file = result.map_err(|e| crate::Error::DataInvalid {
                message: format!("Background file close task panicked: {e}"),
                source: None,
            })??;
            self.written_files.push(file);
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<DataFileMeta>> {
        self.close_current_file().await?;
        self.drain_closes().await?;
        self.created_paths.clear();
        let mut files = std::mem::take(&mut self.written_files);
        files.sort_unstable_by_key(|(ordinal, _)| *ordinal);
        Ok(files.into_iter().map(|(_, meta)| meta).collect())
    }

    pub(super) async fn abort(&mut self) {
        if let Some(sidecar) = self.current_row_sidecar.take() {
            let _ = sidecar.writer.close().await;
        }
        if let Some(writer) = self.current_writer.take() {
            let _ = writer.close().await;
        }
        self.current_index = None;
        self.current_file_name = None;
        self.current_file_path = None;
        while self.in_flight_closes.join_next().await.is_some() {}
        for path in self.created_paths.drain(..) {
            if self.delete_file_upon_abort {
                let _ = self.file_io.delete_file(&path).await;
            }
        }
        self.written_files.clear();
    }

    pub(super) async fn delete_files(&mut self, files: &[DataFileMeta]) {
        if !self.delete_file_upon_abort {
            return;
        }
        for file in files {
            for path in file.collect_files(self.bucket_dir()) {
                let _ = self.file_io.delete_file(&path).await;
            }
        }
    }

    fn bucket_dir(&self) -> &str {
        self.paths.bucket_path()
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
        )
        .unwrap();
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
        .unwrap()
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
    async fn variant_inference_reuses_committed_evidence_only_in_adaptive_mode() {
        use crate::arrow::{build_target_arrow_schema, variant_arrow_type};
        use crate::spec::VariantType;
        use crate::variant::GenericVariant;
        use arrow_array::{BinaryArray, StructArray};
        use futures::TryStreamExt;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        for mode in ["per-file", "adaptive"] {
            let file_io = FileIOBuilder::new("memory").build().unwrap();
            let path = format!("memory:/variant-rolling-{mode}");
            let fields = vec![DataField::new(
                0,
                "v".into(),
                DataType::Variant(VariantType::new()),
            )];
            let options = HashMap::from([
                ("variant.inferShreddingSchema".into(), "true".into()),
                ("variant.shredding.inferenceMode".into(), mode.into()),
                ("variant.shredding.maxInferBufferRow".into(), "4".into()),
                (
                    "variant.shredding.adaptive.maxInferBufferRow".into(),
                    "1".into(),
                ),
            ]);
            let mut writer = DataFileWriter::new(
                file_io.clone(),
                path.clone(),
                String::new(),
                0,
                0,
                i64::MAX,
                "zstd".into(),
                1,
                i64::MAX,
                "parquet".into(),
                fields.clone(),
                options,
                None,
                None,
                None,
            )
            .unwrap()
            .with_target_file_row_num(4);
            let mut expected = Vec::new();
            for json in [
                vec![r#"{"legacy":1}"#; 4],
                vec![
                    r#"{"emerging":true}"#,
                    r#"{"late":1}"#,
                    r#"{"late":2}"#,
                    r#"{"late":3}"#,
                ],
            ] {
                let variants = json
                    .iter()
                    .map(|value| GenericVariant::parse_json(value).unwrap())
                    .collect::<Vec<_>>();
                let ArrowDataType::Struct(variant_fields) = variant_arrow_type() else {
                    unreachable!()
                };
                let array = StructArray::new(
                    variant_fields,
                    vec![
                        Arc::new(BinaryArray::from_iter_values(
                            variants.iter().map(GenericVariant::value),
                        )),
                        Arc::new(BinaryArray::from_iter_values(
                            variants.iter().map(GenericVariant::metadata),
                        )),
                    ],
                    None,
                );
                let batch = RecordBatch::try_new(
                    build_target_arrow_schema(&fields).unwrap(),
                    vec![Arc::new(array)],
                )
                .unwrap();
                writer.write(&batch).await.unwrap();
                expected.push(variants);
            }
            let files = writer.prepare_commit().await.unwrap();
            assert_eq!(files.len(), 2);
            for (index, file) in files.iter().enumerate() {
                let file_path = format!("{path}/bucket-0/{}", file.file_name);
                let input = file_io.new_input(&file_path).unwrap();
                let raw =
                    ParquetRecordBatchReaderBuilder::try_new(input.read().await.unwrap()).unwrap();
                let ArrowDataType::Struct(variant_fields) = raw.schema().field(0).data_type()
                else {
                    panic!("expected variant struct")
                };
                let typed = variant_fields
                    .iter()
                    .find(|field| field.name() == "typed_value")
                    .unwrap();
                let ArrowDataType::Struct(object) = typed.data_type() else {
                    panic!("expected object")
                };
                let names = object
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<_>>();
                assert_eq!(
                    names,
                    if index == 0 {
                        vec!["legacy"]
                    } else if mode == "adaptive" {
                        vec!["emerging", "legacy"]
                    } else {
                        vec!["emerging", "late"]
                    }
                );
                let reader =
                    crate::arrow::format::create_format_reader(&file_path, false, &fields).unwrap();
                let batches = reader
                    .read_batch_stream(
                        Box::new(input.reader().await.unwrap()),
                        file.file_size as u64,
                        &fields,
                        None,
                        None,
                        None,
                    )
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                let mut actual = Vec::new();
                for batch in batches {
                    let array = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .unwrap();
                    let values = array
                        .column_by_name("value")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap();
                    let metadata = array
                        .column_by_name("metadata")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap();
                    for row in 0..batch.num_rows() {
                        actual.push(
                            GenericVariant::from_parts(
                                values.value(row).to_vec(),
                                metadata.value(row).to_vec(),
                            )
                            .unwrap()
                            .to_json()
                            .unwrap(),
                        );
                    }
                }
                assert_eq!(
                    actual,
                    expected[index]
                        .iter()
                        .map(|value| value.to_json().unwrap())
                        .collect::<Vec<_>>()
                );
            }
        }
    }

    struct FailingSidecar {
        inner: Box<dyn FormatFileWriter>,
        fail_write: bool,
    }

    #[async_trait::async_trait]
    impl FormatFileWriter for FailingSidecar {
        async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
            if self.fail_write {
                return Err(crate::Error::DataInvalid {
                    message: "injected sidecar write failure".into(),
                    source: None,
                });
            }
            self.inner.write(batch).await
        }
        fn num_bytes(&self) -> usize {
            self.inner.num_bytes()
        }
        fn in_progress_size(&self) -> usize {
            self.inner.in_progress_size()
        }
        async fn flush(&mut self) -> Result<()> {
            self.inner.flush().await
        }
        async fn close(self: Box<Self>) -> Result<crate::arrow::format::FormatWriteResult> {
            self.inner.close().await?;
            Err(crate::Error::DataInvalid {
                message: "injected sidecar close failure".into(),
                source: None,
            })
        }
    }

    #[derive(Debug)]
    struct SidecarFailureProvider {
        operator: Operator,
        fail_create: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl FileIOProvider for SidecarFailureProvider {
        async fn create(&self, path: &str) -> Result<(Operator, String)> {
            if path.ends_with(".row") && self.fail_create.load(std::sync::atomic::Ordering::Relaxed)
            {
                return Err(crate::Error::DataInvalid {
                    message: "injected sidecar create failure".into(),
                    source: None,
                });
            }
            Ok((
                self.operator.clone(),
                path.trim_start_matches("memory:")
                    .trim_start_matches('/')
                    .into(),
            ))
        }
    }

    #[tokio::test]
    async fn sidecar_failures_only_remove_current_producer_outputs() {
        use std::sync::atomic::{AtomicBool, Ordering};
        for failure in ["create", "write", "close"] {
            let provider = Arc::new(SidecarFailureProvider {
                operator: Operator::from_config(opendal::services::MemoryConfig::default())
                    .unwrap(),
                fail_create: AtomicBool::new(false),
            });
            let io = FileIOBuilder::new("unused")
                .with_provider(provider.clone())
                .build()
                .unwrap();
            let fields = vec![DataField::new(
                0,
                "id".into(),
                DataType::Int(IntType::new()),
            )];
            let mut writer = DataFileWriter::new(
                io.clone(),
                "memory:/sidecar-failure".into(),
                String::new(),
                0,
                0,
                i64::MAX,
                "none".into(),
                0,
                i64::MAX,
                "parquet".into(),
                fields,
                HashMap::from([
                    ("data-evolution.enabled".into(), "true".into()),
                    ("data-evolution.row-sidecar.enabled".into(), "true".into()),
                ]),
                Some(0),
                None,
                None,
            )
            .unwrap();
            let batch = RecordBatch::try_from_iter([(
                "id",
                Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
            )])
            .unwrap();
            writer.write(&batch).await.unwrap();
            let prepared = writer.prepare_commit().await.unwrap();
            let prior_paths = prepared[0].collect_files(writer.bucket_dir());
            assert_eq!(prior_paths.len(), 2);
            let error = if failure == "create" {
                provider.fail_create.store(true, Ordering::Relaxed);
                let error = writer.write(&batch).await.unwrap_err();
                provider.fail_create.store(false, Ordering::Relaxed);
                error
            } else {
                writer.write(&batch).await.unwrap();
                let sidecar = writer.current_row_sidecar.take().unwrap();
                let mut sidecar = sidecar;
                sidecar.writer = Box::new(FailingSidecar {
                    inner: sidecar.writer,
                    fail_write: failure == "write",
                });
                writer.current_row_sidecar = Some(sidecar);
                if failure == "write" {
                    writer.write(&batch).await.unwrap_err()
                } else {
                    writer.prepare_commit().await.unwrap_err()
                }
            };
            assert!(error
                .to_string()
                .contains(&format!("sidecar {failure} failure")));
            writer.abort().await;
            let mut actual: Vec<_> = io
                .list_status_recursive(writer.bucket_dir())
                .await
                .unwrap()
                .into_iter()
                .map(|status| status.path)
                .collect();
            actual.sort();
            let mut expected = prior_paths;
            expected.sort();
            assert_eq!(actual, expected, "{failure}");
        }
    }

    #[tokio::test]
    async fn grouped_prepare_failure_removes_successful_and_failed_outputs() {
        for (external, sidecar) in [(false, false), (true, false), (false, true), (true, true)] {
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
                    if external {
                        HashMap::from([
                            (
                                "data-file.external-paths".into(),
                                "memory:/external-a,memory:/external-b".into(),
                            ),
                            (
                                "data-file.external-paths.strategy".into(),
                                "entropy-inject".into(),
                            ),
                        ])
                    } else {
                        HashMap::new()
                    },
                    Some(first_row_id),
                    None,
                    None,
                )
                .unwrap();
                writer.row_sidecar_enabled = sidecar;
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
                .list_status_recursive("memory:/")
                .await
                .unwrap()
                .iter()
                .all(|entry| !entry.path.ends_with(".parquet") && !entry.path.ends_with(".row")));
        }
    }
}
