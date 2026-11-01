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

//! Shared native payload construction and private output ownership.
//!
//! Callers provide indexed batches and source metadata. PK/DE source planning,
//! replacement policy and publication/recovery stay in their maintenance layers.

pub(in crate::table) mod extraction;
pub(in crate::table) mod fields;
pub(in crate::table) mod full_text;
pub(in crate::table) mod scalar;
pub(in crate::table) mod sort;
pub(in crate::table) mod vector;
pub(in crate::table) mod vector_extraction;
pub(in crate::table) mod vector_validation;

#[cfg(test)]
mod tests;

use crate::io::OutputFile;
use crate::spec::{GlobalIndexMeta, IndexFileMeta};
use crate::table::index_file_path::IndexFileLocation;
use crate::table::{RowRange, Table};
use crate::{Error, Result};
use bytes::Bytes;
use std::future::Future;
use std::path::Path;
use tokio::io::AsyncReadExt;
const COPY_BUFFER_SIZE: usize = 1024 * 1024;

pub(in crate::table) struct IndexPayload {
    pub row_count: i64,
    pub index_meta: Vec<u8>,
}

pub(in crate::table) fn error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::DataInvalid {
        message: "Index build failed".into(),
        source: Some(Box::new(error)),
    }
}

pub(in crate::table) fn row_count(range: &RowRange) -> Result<i64> {
    range
        .to()
        .checked_sub(range.from())
        .and_then(|count| count.checked_add(1))
        .filter(|count| *count > 0)
        .ok_or_else(|| Error::DataInvalid {
            message: format!("Invalid index row range [{}, {}]", range.from(), range.to()),
            source: None,
        })
}

/// A private file until its caller submits the returned metadata to a commit.
/// Every write/stat/size-conversion failure removes this file, never old files.
pub(in crate::table) struct IndexFileWriter {
    path: String,
    meta: IndexFileMeta,
}

impl IndexFileWriter {
    pub async fn new(
        table: &Table,
        index_type: &str,
        file_name: String,
        location: IndexFileLocation<'_>,
        relative_bucket: &str,
        meta: GlobalIndexMeta,
    ) -> Result<Self> {
        let in_data = matches!(
            &location,
            IndexFileLocation::BucketLocal {
                index_file_in_data_file_dir: true,
                ..
            }
        );
        let external_path = crate::table::external_path::new_index_external_path(
            table.schema().options(),
            in_data,
            relative_bucket,
            &file_name,
        )?;
        let path = location.resolve(&file_name, external_path.as_deref());
        if let Some((directory, _)) = path.rsplit_once('/') {
            table.file_io().mkdirs(directory).await?;
        }
        Ok(Self {
            path,
            meta: IndexFileMeta {
                index_type: index_type.into(),
                file_name,
                file_size: 0,
                row_count: 0,
                deletion_vectors_ranges: None,
                external_path,
                global_index_meta: Some(meta),
            },
        })
    }

    pub async fn write<F, Fut>(mut self, table: &Table, build: F) -> Result<Option<IndexFileMeta>>
    where
        F: FnOnce(OutputFile) -> Fut,
        Fut: Future<Output = Result<Option<IndexPayload>>>,
    {
        let result = async {
            let Some(payload) = build(table.file_io().new_output(&self.path)?).await? else {
                return Ok(None);
            };
            self.meta.file_size =
                i64::try_from(table.file_io().get_status(&self.path).await?.size).map_err(error)?;
            self.meta.row_count = payload.row_count;
            self.meta
                .global_index_meta
                .as_mut()
                .expect("native index metadata")
                .index_meta = Some(payload.index_meta);
            Ok(Some(self.meta))
        }
        .await;
        if result.is_err() {
            let _ = table.file_io().delete_file(&self.path).await;
        }
        result
    }
}

pub(in crate::table) fn serialize_options(
    options: &std::collections::HashMap<String, String>,
) -> Result<Vec<u8>> {
    let sorted = options.iter().collect::<std::collections::BTreeMap<_, _>>();
    serde_json::to_vec(&sorted).map_err(error)
}

/// Stream a locally built full-text index to its table location. The native
/// writer serializes to a temporary file because it needs synchronous I/O.
pub(in crate::table) async fn copy_local_file_to_output(
    source_path: &Path,
    output: OutputFile,
) -> Result<()> {
    let mut source =
        tokio::fs::File::open(source_path)
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to open temporary index file: {e}"),
                source: None,
            })?;
    let mut writer = output.writer().await?;
    let mut buffer = vec![0u8; COPY_BUFFER_SIZE];

    loop {
        let len = source
            .read(&mut buffer)
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Failed to read temporary index file: {e}"),
                source: None,
            })?;
        if len == 0 {
            break;
        }
        writer.write(Bytes::copy_from_slice(&buffer[..len])).await?;
    }
    writer.close().await
}
