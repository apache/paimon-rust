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

//! Build immutable spatial files from the same snapshot-pinned DE stream as other scalar indexes.
use super::{error, IndexFileWriter};
use crate::io::FileWrite;
use crate::rtree::{RTreeIndexOptions, RTreeIndexWriter, RTreeOutput};
use crate::spec::{GlobalIndexMeta, IndexFileMeta};
use crate::table::index_file_path::{new_global_index_file_name, IndexFileLocation};
use crate::table::{ArrowRecordBatchStream, RowRange, Table};
use crate::{Error, Result};
use arrow_array::{Array, BinaryArray, Int64Array};
use futures::TryStreamExt;
use std::io;

struct ShardOutput<'a> {
    table: &'a Table,
    shard: GlobalIndexMeta,
    files: Vec<IndexFileWriter>,
}

#[async_trait::async_trait]
impl RTreeOutput for &mut ShardOutput<'_> {
    async fn create(&mut self) -> io::Result<(String, Box<dyn FileWrite>)> {
        let name = new_global_index_file_name("rtree");
        let file = IndexFileWriter::new(
            self.table,
            "rtree",
            name.clone(),
            IndexFileLocation::Global {
                table_path: self.table.location().trim_end_matches('/'),
            },
            "",
            self.shard.clone(),
        )
        .await
        .map_err(io::Error::other)?;
        let output = self
            .table
            .file_io()
            .new_output(&file.path)
            .map_err(io::Error::other)?;
        self.files.push(file);
        Ok((name, output.writer().await.map_err(io::Error::other)?))
    }
}

async fn write_files_inner(
    batches: &mut ArrowRecordBatchStream,
    range: &RowRange,
    output: &mut ShardOutput<'_>,
    options: &RTreeIndexOptions,
) -> Result<Vec<crate::rtree::RTreeFileMeta>> {
    let mut writer = RTreeIndexWriter::new(output, options.clone()).map_err(error)?;
    let mut next = 0;
    while let Some(batch) = batches.try_next().await? {
        // The build projection is [geometry, _ROW_ID]. Geometry uses GeoArrow WKB binary storage.
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| Error::DataInvalid {
                message: "R-tree build requires binary WKB values".into(),
                source: None,
            })?;
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::DataInvalid {
                message: "R-tree build requires Int64 _ROW_ID".into(),
                source: None,
            })?;
        for (wkb, id) in values.iter().zip(ids.iter()) {
            if id.and_then(|id| id.checked_sub(range.from())) != Some(next) {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Non-contiguous row IDs in R-tree shard starting at {}",
                        range.from()
                    ),
                    source: None,
                });
            }
            writer.write(wkb, next as u64).await.map_err(error)?;
            next += 1;
        }
    }
    if next != super::row_count(range)? {
        return Err(Error::DataInvalid {
            message: format!("R-tree shard has {next} rows in range {range:?}"),
            source: None,
        });
    }
    writer.finish().await.map_err(error)
}

pub(in crate::table) async fn write_files(
    table: &Table,
    mut batches: ArrowRecordBatchStream,
    range: RowRange,
    shard: GlobalIndexMeta,
    options: &RTreeIndexOptions,
) -> Result<Vec<IndexFileMeta>> {
    let mut output = ShardOutput {
        table,
        shard,
        files: Vec::new(),
    };
    let files = match write_files_inner(&mut batches, &range, &mut output, options).await {
        Ok(files) => files,
        Err(error) => {
            for file in output.files {
                let _ = table.file_io().delete_file(&file.path).await;
            }
            return Err(error);
        }
    };
    Ok(output
        .files
        .into_iter()
        .zip(files)
        .map(|(file, written)| {
            let mut meta = file.meta;
            meta.row_count = written.row_count as i64;
            meta.file_size = written.file_size as i64;
            meta.global_index_meta.as_mut().unwrap().index_meta = Some(written.header.to_vec());
            meta
        })
        .collect())
}
