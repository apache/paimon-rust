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

//! Lumina index-file construction, upload, and cleanup.

use super::planning::LuminaIndexShard;
use super::validation::{checked_i64, checked_row_count};
use super::LuminaIndexBuildBuilder;
use crate::lumina::ffi::LuminaBuilder;
use crate::lumina::{LuminaIndexMeta, LUMINA_IDENTIFIER};
use crate::spec::{GlobalIndexMeta, IndexFileMeta};
use crate::table::{CommitMessage, TableCommit};
use crate::{Error, Result};
use bytes::Bytes;
use std::path::{Path, PathBuf};
use tokio::io::AsyncReadExt;

const INDEX_DIR: &str = "index";
const COPY_BUFFER_SIZE: usize = 1024 * 1024;

impl LuminaIndexBuildBuilder<'_> {
    pub(super) async fn build_index_file(
        &self,
        shard: &LuminaIndexShard,
        vectors: &[f32],
        dimension: i32,
        index_field_id: i32,
        index_meta: Vec<u8>,
    ) -> Result<IndexFileMeta> {
        let row_count = checked_row_count(shard.row_range_start, shard.row_range_end)?;
        // The native Lumina builder counts rows in an i32; the manifest keeps the
        // full width.
        let native_row_count = i32::try_from(row_count).map_err(|_| Error::DataInvalid {
            message: format!(
                "Lumina shard row count {row_count} exceeds what the native builder accepts"
            ),
            source: None,
        })?;
        validate_vector_buffer(vectors, native_row_count, dimension)?;
        let ids = (0..row_count as u64).collect::<Vec<_>>();
        let native_options = LuminaIndexMeta::deserialize(&index_meta)?.options().clone();

        let temp_path = temp_lumina_path();
        let temp_file = TempFileGuard::new(temp_path.clone());
        let temp_path_str = temp_path.to_string_lossy().to_string();
        let builder = LuminaBuilder::create(&native_options)?;
        builder.pretrain(vectors, native_row_count, dimension)?;
        builder.insert(vectors, &ids, native_row_count, dimension)?;
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
        let write_result: Result<i64> = async {
            copy_local_file_to_output(&temp_path, self.table.file_io().new_output(&index_path)?)
                .await?;
            temp_file.cleanup();
            let status = self.table.file_io().get_status(&index_path).await?;
            checked_i64(
                status.size,
                "Index file is too large for Rust IndexFileMeta",
            )
        }
        .await;
        let file_size = match write_result {
            Ok(file_size) => file_size,
            Err(error) => {
                let _ = self.table.file_io().delete_file(&index_path).await;
                return Err(error);
            }
        };
        Ok(IndexFileMeta {
            index_type: LUMINA_IDENTIFIER.to_string(),
            file_name,
            file_size,
            row_count,
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: shard.row_range_start,
                row_range_end: shard.row_range_end,
                index_field_id,
                extra_field_ids: None,
                source_meta: None,
                index_meta: Some(index_meta),
            }),
        })
    }
}

pub(super) async fn abort_on_build_error<T>(
    commit: &TableCommit,
    messages: &[CommitMessage],
    result: Result<T>,
) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            let _ = commit.abort(messages).await;
            Err(error)
        }
    }
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

pub(super) fn temp_lumina_path() -> PathBuf {
    std::env::temp_dir().join(format!("lumina-index-{}.index", uuid::Uuid::new_v4()))
}

pub(super) struct TempFileGuard {
    path: Option<PathBuf>,
}

impl TempFileGuard {
    pub(super) fn new(path: PathBuf) -> Self {
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
