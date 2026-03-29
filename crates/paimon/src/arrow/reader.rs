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

use crate::deletion_vector::{DeletionVector, DeletionVectorFactory};
use crate::io::{FileIO, FileRead, FileStatus};
use crate::spec::DataField;
use crate::table::ArrowRecordBatchStream;
use crate::{DataSplit, Error};
use async_stream::try_stream;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{StreamExt, TryFutureExt};
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection, RowSelector};
use parquet::arrow::async_reader::{AsyncFileReader, MetadataFetch};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
use std::ops::Range;
use std::sync::Arc;
use tokio::try_join;

/// Builder to create ArrowReader
pub struct ArrowReaderBuilder {
    batch_size: Option<usize>,
    file_io: FileIO,
}

impl ArrowReaderBuilder {
    /// Create a new ArrowReaderBuilder
    pub(crate) fn new(file_io: FileIO) -> Self {
        ArrowReaderBuilder {
            batch_size: None,
            file_io,
        }
    }

    /// Build the ArrowReader with the given read type (logical row type or projected subset).
    /// Used to clip Parquet schema to requested columns only.
    pub fn build(self, read_type: Vec<DataField>) -> ArrowReader {
        ArrowReader {
            batch_size: self.batch_size,
            file_io: self.file_io,
            read_type,
        }
    }
}

/// Reads data from Parquet files
#[derive(Clone)]
pub struct ArrowReader {
    batch_size: Option<usize>,
    file_io: FileIO,
    read_type: Vec<DataField>,
}

impl ArrowReader {
    /// Take a stream of DataSplits and read every data file in each split.
    /// Returns a stream of Arrow RecordBatches from all files.
    /// When a split has deletion files (see [DataSplit::data_deletion_files]), the corresponding
    /// deletion vectors are loaded and applied so that deleted rows are filtered out from the stream.
    /// Row positions are 0-based within each data file, matching Java's ApplyDeletionVectorReader.
    ///
    /// Matches [RawFileSplitRead.createReader](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/operation/RawFileSplitRead.java):
    /// one DV factory per DataSplit (created from that split's data files and deletion files).
    ///
    /// Parquet schema is clipped to this reader's read type (column names from [DataField]s).
    /// File-only columns are not read. See [ParquetReaderFactory.clipParquetSchema](https://github.com/apache/paimon/blob/master/paimon-format/paimon-format-common/src/main/java/org/apache/paimon/format/FormatReaderFactory.java).
    pub fn read(self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        let file_io = self.file_io.clone();
        let batch_size = self.batch_size;
        // Owned list of splits so the stream does not hold references.
        let splits: Vec<DataSplit> = data_splits.to_vec();
        let read_type = self.read_type;
        let projected_column_names: Vec<String> = read_type
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        Ok(try_stream! {
            for split in splits {
                // Create DV factory for this split only (like Java createReader(partition, bucket, files, deletionFiles)).
                let core_data_files = split.data_files();
                let dv_factory = if split
                    .data_deletion_files()
                    .is_some_and(|files| files.iter().any(Option::is_some))
                {
                    Some(
                        DeletionVectorFactory::new(
                            &file_io,
                            core_data_files,
                            split.data_deletion_files(),
                        )
                        .await?,
                    )
                } else {
                    None
                };

                for file_meta in core_data_files {
                    let path_to_read = split.data_file_path(file_meta);
                    if !path_to_read.to_ascii_lowercase().ends_with(".parquet") {
                        Err(Error::Unsupported {
                            message: format!(
                                "unsupported file format: only .parquet is supported, got: {path_to_read}"
                            ),
                        })?
                    }
                    let dv = dv_factory
                        .as_ref()
                        .and_then(|factory| factory.get_deletion_vector(&file_meta.file_name));

                    let parquet_file = file_io.new_input(&path_to_read)?;
                    let (parquet_metadata, parquet_reader) = try_join!(
                        parquet_file.metadata(),
                        parquet_file.reader()
                    )?;
                    let arrow_file_reader = ArrowFileReader::new(parquet_metadata, parquet_reader);

                    let mut batch_stream_builder =
                        ParquetRecordBatchStreamBuilder::new(arrow_file_reader)
                            .await?;
                    // ProjectionMask preserves parquet-schema order; read_type order is restored below.
                    let mask = {
                        let parquet_schema = batch_stream_builder.parquet_schema();
                        ProjectionMask::columns(
                            parquet_schema,
                            projected_column_names.iter().map(String::as_str),
                        )
                    };
                    batch_stream_builder = batch_stream_builder.with_projection(mask);

                    if let Some(dv) = dv {
                        if !dv.is_empty() {
                            let row_selection =
                                build_deletes_row_selection(batch_stream_builder.metadata().row_groups(), dv)?;
                            batch_stream_builder = batch_stream_builder.with_row_selection(row_selection);
                        }
                    }
                    if let Some(size) = batch_size {
                        batch_stream_builder = batch_stream_builder.with_batch_size(size);
                    }
                    let mut batch_stream = batch_stream_builder.build()?;

                    while let Some(batch) = batch_stream.next().await {
                        let batch = batch?;
                        // Reorder columns from parquet-schema order to read_type order.
                        // Every projected column must exist in the batch; a missing
                        // column indicates schema mismatch and must not be silenced.
                        let reorder_indices: Vec<usize> = projected_column_names
                            .iter()
                            .map(|name| {
                                batch.schema().index_of(name).map_err(|_| {
                                    Error::UnexpectedError {
                                        message: format!(
                                            "Projected column '{}' not found in Parquet batch schema of file {}",
                                            name, path_to_read
                                        ),
                                        source: None,
                                    }
                                })
                            })
                            .collect::<crate::Result<Vec<_>>>()?;
                        yield batch.project(&reorder_indices).map_err(|e| {
                            Error::UnexpectedError {
                                message: "Failed to reorder projected columns".to_string(),
                                source: Some(Box::new(e)),
                            }
                        })?;
                    }
                }
            }
        }
            .boxed())
    }
}

/// Builds a Parquet [RowSelection] from deletion vector.
/// Only rows not in the deletion vector are selected; deleted rows are skipped at read time.
/// todo: Uses [DeletionVectorIterator] with [advance_to](DeletionVectorIterator::advance_to) when skipping row groups similar to iceberg-rust
fn build_deletes_row_selection(
    row_group_metadata_list: &[RowGroupMetaData],
    deletion_vector: &DeletionVector,
) -> crate::Result<RowSelection> {
    let mut delete_iter = deletion_vector.iter();

    let mut results: Vec<RowSelector> = Vec::new();
    let mut current_row_group_base_idx: u64 = 0;
    let mut next_deleted_row_idx_opt = delete_iter.next();

    for row_group_metadata in row_group_metadata_list {
        let row_group_num_rows = row_group_metadata.num_rows() as u64;
        let next_row_group_base_idx = current_row_group_base_idx + row_group_num_rows;

        let mut next_deleted_row_idx = match next_deleted_row_idx_opt {
            Some(next_deleted_row_idx) => {
                if next_deleted_row_idx >= next_row_group_base_idx {
                    results.push(RowSelector::select(row_group_num_rows as usize));
                    current_row_group_base_idx += row_group_num_rows;
                    continue;
                }
                next_deleted_row_idx
            }
            None => {
                results.push(RowSelector::select(row_group_num_rows as usize));
                current_row_group_base_idx += row_group_num_rows;
                continue;
            }
        };

        let mut current_idx = current_row_group_base_idx;
        'chunks: while next_deleted_row_idx < next_row_group_base_idx {
            if current_idx < next_deleted_row_idx {
                let run_length = next_deleted_row_idx - current_idx;
                results.push(RowSelector::select(run_length as usize));
                current_idx += run_length;
            }
            let mut run_length = 0u64;
            while next_deleted_row_idx == current_idx
                && next_deleted_row_idx < next_row_group_base_idx
            {
                run_length += 1;
                current_idx += 1;
                next_deleted_row_idx_opt = delete_iter.next();
                next_deleted_row_idx = match next_deleted_row_idx_opt {
                    Some(v) => v,
                    None => {
                        results.push(RowSelector::skip(run_length as usize));
                        break 'chunks;
                    }
                };
            }
            if run_length > 0 {
                results.push(RowSelector::skip(run_length as usize));
            }
        }
        if current_idx < next_row_group_base_idx {
            results.push(RowSelector::select(
                (next_row_group_base_idx - current_idx) as usize,
            ));
        }
        current_row_group_base_idx += row_group_num_rows;
    }

    Ok(results.into())
}

/// ArrowFileReader is a wrapper around a FileRead that impls parquets AsyncFileReader.
///
/// # TODO
///
/// [ParquetObjectReader](https://docs.rs/parquet/latest/src/parquet/arrow/async_reader/store.rs.html#64)
/// contains the following hints to speed up metadata loading, similar to iceberg, we can consider adding them to this struct:
///
/// - `metadata_size_hint`: Provide a hint as to the size of the parquet file's footer.
/// - `preload_column_index`: Load the Column Index  as part of [`Self::get_metadata`].
/// - `preload_offset_index`: Load the Offset Index as part of [`Self::get_metadata`].
struct ArrowFileReader<R: FileRead> {
    meta: FileStatus,
    r: R,
}

impl<R: FileRead> ArrowFileReader<R> {
    /// Create a new ArrowFileReader
    fn new(meta: FileStatus, r: R) -> Self {
        Self { meta, r }
    }

    fn read_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(self.r.read(range.start..range.end).map_err(|err| {
            let err_msg = format!("{err}");
            parquet::errors::ParquetError::External(err_msg.into())
        }))
    }
}

impl<R: FileRead> MetadataFetch for ArrowFileReader<R> {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.read_bytes(range)
    }
}

impl<R: FileRead> AsyncFileReader for ArrowFileReader<R> {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        self.read_bytes(range)
    }

    fn get_metadata(
        &mut self,
        options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        let metadata_opts = options.map(|o| o.metadata_options().clone());
        Box::pin(async move {
            let file_size = self.meta.size;
            let metadata = ParquetMetaDataReader::new()
                .with_metadata_options(metadata_opts)
                .load_and_finish(self, file_size)
                .await?;
            Ok(Arc::new(metadata))
        })
    }
}
