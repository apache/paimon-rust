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

//! Native full-text archives over an ordered, possibly sparse row-ID stream.
use super::copy_local_file_to_output;
use super::IndexPayload;
use crate::io::OutputFile;
use crate::spec::ROW_ID_FIELD_NAME;
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::{Error, Result};
use arrow_array::cast::AsArray;
use arrow_array::{Array, Int64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use arrow_schema::DataType as ArrowDataType;
use futures::TryStreamExt;
use paimon_ftindex_core::io::PosWriter;
use paimon_ftindex_core::{FullTextIndexConfig, FullTextIndexWriter};
use std::collections::HashMap;
const FULL_TEXT_OPTION_PREFIX: &str = "full-text.";

pub(in crate::table) async fn write_payload(
    mut stream: ArrowRecordBatchStream,
    output: OutputFile,
    index_column: &str,
    range: RowRange,
    native_options: &HashMap<String, String>,
    index_meta: Vec<u8>,
) -> Result<Option<IndexPayload>> {
    let config = full_text_config(native_options)?;
    let mut writer =
        run_blocking(move || FullTextIndexWriter::new(config).map_err(full_text_error)).await?;
    let mut cursor = ShardCursor::new(range.from(), range.to());
    while let Some(batch) = stream.try_next().await? {
        let documents = cursor.documents(&batch, index_column)?;
        writer = run_blocking(move || {
            for (row_id, text) in documents {
                writer.add_document(row_id, text).map_err(full_text_error)?;
            }
            Ok(writer)
        })
        .await?;
    }
    if cursor.row_count == 0 {
        return Ok(None);
    }

    let local_index = run_blocking(move || {
        let mut file = tempfile::NamedTempFile::new().map_err(|e| Error::UnexpectedError {
            message: format!("Failed to create temporary full-text index file: {e}"),
            source: None,
        })?;
        writer
            .write(&mut PosWriter::new(file.as_file_mut()))
            .map_err(full_text_error)?;
        file.as_file_mut()
            .sync_all()
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to flush temporary full-text index file: {e}"),
                source: None,
            })?;
        Ok(file)
    })
    .await?;

    copy_local_file_to_output(local_index.path(), output).await?;
    Ok(Some(IndexPayload {
        row_count: cursor.row_count,
        index_meta,
    }))
}
/// Walks one shard's read batches in row-id order and turns them into
/// shard-relative `(row_id, text)` documents, like the loop in Java
/// `GenericIndexTopoBuilder.BuildIndexOperator#processElement`.
pub(in crate::table) struct ShardCursor {
    row_range_start: i64,
    row_range_end: i64,
    last_row_id: Option<i64>,
    /// Rows inside the shard range, NULL text included (Java `rowCount`).
    pub(in crate::table) row_count: i64,
}

impl ShardCursor {
    pub(in crate::table) fn new(row_range_start: i64, row_range_end: i64) -> Self {
        Self {
            row_range_start,
            row_range_end,
            last_row_id: None,
            row_count: 0,
        }
    }

    pub(in crate::table) fn documents(
        &mut self,
        batch: &RecordBatch,
        index_column: &str,
    ) -> Result<Vec<(i64, String)>> {
        let text_index = batch
            .schema()
            .index_of(index_column)
            .map_err(|e| Error::DataInvalid {
                message: format!("Full-text column '{index_column}' not found in read batch: {e}"),
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
        let row_ids = batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::DataInvalid {
                message: "Full-text index build requires Int64 _ROW_ID".to_string(),
                source: None,
            })?;
        let texts = batch.column(text_index);
        let texts = TextColumn::try_new(texts.as_ref())?;

        let mut documents = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if row_ids.is_null(row) {
                return Err(Error::DataInvalid {
                    message: "Full-text index build found null _ROW_ID".to_string(),
                    source: None,
                });
            }
            let row_id = row_ids.value(row);
            if let Some(last_row_id) = self.last_row_id {
                if row_id < last_row_id {
                    return Err(Error::DataInvalid {
                        message: format!(
                            "Row IDs are not monotonically increasing: previous={last_row_id}, \
                             current={row_id} in shard [{}, {}]",
                            self.row_range_start, self.row_range_end
                        ),
                        source: None,
                    });
                }
            }
            self.last_row_id = Some(row_id);
            if row_id < self.row_range_start || row_id > self.row_range_end {
                continue;
            }
            self.row_count += 1;
            if let Some(text) = texts.value(row) {
                documents.push((row_id - self.row_range_start, text.to_string()));
            }
        }
        Ok(documents)
    }
}

/// The Arrow string layouts a CHAR/VARCHAR column can be read as.
enum TextColumn<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> TextColumn<'a> {
    fn try_new(array: &'a dyn Array) -> Result<Self> {
        match array.data_type() {
            ArrowDataType::Utf8 => Ok(Self::Utf8(array.as_string::<i32>())),
            ArrowDataType::LargeUtf8 => Ok(Self::LargeUtf8(array.as_string::<i64>())),
            ArrowDataType::Utf8View => Ok(Self::Utf8View(array.as_string_view())),
            other => Err(Error::DataInvalid {
                message: format!(
                    "Full-text index build requires a string column, got Arrow type {other}"
                ),
                source: None,
            }),
        }
    }

    fn value(&self, row: usize) -> Option<&'a str> {
        match self {
            Self::Utf8(array) => array.is_valid(row).then(|| array.value(row)),
            Self::LargeUtf8(array) => array.is_valid(row).then(|| array.value(row)),
            Self::Utf8View(array) => array.is_valid(row).then(|| array.value(row)),
        }
    }
}

pub(in crate::table) fn native_full_text_options(
    options: &HashMap<String, String>,
) -> HashMap<String, String> {
    options
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix(FULL_TEXT_OPTION_PREFIX)
                .map(|native_key| (native_key.to_string(), value.clone()))
        })
        .collect()
}

pub(in crate::table) fn full_text_config(
    native_options: &HashMap<String, String>,
) -> Result<FullTextIndexConfig> {
    FullTextIndexConfig::from_options(native_options).map_err(|e| Error::ConfigInvalid {
        message: format!("Invalid full-text index options: {e}"),
    })
}

/// Java `NativeFullTextIndexOptions#serialize`: the native options as a flat
/// JSON object. Keys are sorted so the metadata is deterministic.
pub(in crate::table) use super::serialize_options as serialize_index_meta;

fn full_text_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::UnexpectedError {
        message: "Full-text index error".into(),
        source: Some(Box::new(error)),
    }
}

/// The native writer is synchronous and CPU-bound; keep it off the async workers.
async fn run_blocking<T, F>(task: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(task)
        .await
        .map_err(|e| Error::UnexpectedError {
            message: format!("Full-text index build task failed: {e}"),
            source: None,
        })?
}
