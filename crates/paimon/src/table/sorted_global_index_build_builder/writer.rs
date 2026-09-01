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

//! Type-specific index-file writing and failure cleanup.

use super::extraction::{build_read_splits_for_shard, extract_index_rows, sort_index_rows};
use super::planning::SortedGlobalIndexShard;
use super::validation::{checked_i64, checked_row_count, index_key_type};
use super::{
    make_index_key_codec, GlobalIndexWriteOptions, SerializeKeyFn, SortedGlobalIndexBuildBuilder,
    INDEX_DIR,
};
use crate::btree::BTreeIndexWriter;
use crate::fm_index::{FMGlobalIndexWriter, FMWriteOptions};
use crate::io::FileWrite;
use crate::spec::{
    extract_datum_from_arrow, DataField, GlobalIndexMeta, IndexFileMeta, ROW_ID_FIELD_NAME,
};
use crate::table::bitmap_global_index_writer::{BitmapGlobalIndexWriter, BitmapWriteResult};
use crate::table::global_index_types::{
    normalize_queryable_global_index_type, BITMAP_GLOBAL_INDEX_TYPE, BTREE_GLOBAL_INDEX_TYPE,
    FM_GLOBAL_INDEX_TYPE, MULTIVALUE_GLOBAL_INDEX_TYPE,
};
use crate::table::Table;
use crate::{Error, Result};
use arrow_array::{Array, Int64Array};
use futures::TryStreamExt;

impl SortedGlobalIndexBuildBuilder<'_> {
    pub(super) async fn build_index_file(
        &self,
        shard: &SortedGlobalIndexShard,
        index_field: &DataField,
        index_column: &str,
        write_options: &GlobalIndexWriteOptions,
    ) -> Result<IndexFileMeta> {
        let index_type = normalize_queryable_global_index_type(&self.index_type).ok_or_else(|| {
            Error::Unsupported {
                message: format!(
                    "Scalar global index build only supports index_type => 'btree', 'bitmap', 'multivalue', or 'fm', got '{}'",
                    self.index_type
                ),
            }
        })?;
        let row_count = checked_row_count(shard.row_range_start, shard.row_range_end)?;
        let key_type = index_key_type(index_type, index_field)?;
        let codec_type = if matches!(
            index_type,
            MULTIVALUE_GLOBAL_INDEX_TYPE | FM_GLOBAL_INDEX_TYPE
        ) {
            BITMAP_GLOBAL_INDEX_TYPE
        } else {
            index_type
        };
        let (cmp, serialize_key) = make_index_key_codec(codec_type, key_type);
        let mut rows = if index_type == FM_GLOBAL_INDEX_TYPE {
            Vec::new()
        } else {
            extract_index_rows(
                self.table,
                shard,
                index_column,
                index_field,
                index_type,
                serialize_key,
            )
            .await?
        };
        if !rows.is_empty() {
            sort_index_rows(&mut rows, &cmp);
        }

        self.table
            .file_io()
            .mkdirs(&format!(
                "{}/{INDEX_DIR}/",
                self.table.location().trim_end_matches('/')
            ))
            .await?;
        let file_name = format!("{index_type}-global-index-{}.index", uuid::Uuid::new_v4());
        let index_path = format!(
            "{}/{INDEX_DIR}/{}",
            self.table.location().trim_end_matches('/'),
            file_name
        );
        let write_result: Result<(u64, Vec<u8>, i64)> = async {
            let output = self.table.file_io().new_output(&index_path)?;
            let writer = output.writer().await?;
            let (written_row_count, index_meta) = match index_type {
                BTREE_GLOBAL_INDEX_TYPE => {
                    let GlobalIndexWriteOptions::Sorted(write_options) = write_options else {
                        unreachable!("BTree uses sorted write options")
                    };
                    let mut writer = BTreeIndexWriter::with_comparator_and_compression_level(
                        writer,
                        write_options.block_size,
                        write_options.compression_type,
                        write_options.compression_level,
                        cmp,
                    );
                    for (key, local_row_id) in &rows {
                        writer
                            .write(key.as_deref(), *local_row_id)
                            .await
                            .map_err(|e| Error::DataInvalid {
                                message: format!(
                                    "Failed to write BTree global index file '{file_name}'"
                                ),
                                source: Some(Box::new(e)),
                            })?;
                    }
                    let write_result = writer.finish().await.map_err(|e| Error::DataInvalid {
                        message: format!("Failed to finish BTree global index file '{file_name}'"),
                        source: Some(Box::new(e)),
                    })?;
                    (write_result.row_count, write_result.meta.serialize())
                }
                BITMAP_GLOBAL_INDEX_TYPE => {
                    let GlobalIndexWriteOptions::Sorted(write_options) = write_options else {
                        unreachable!("bitmap uses sorted write options")
                    };
                    let mut writer = BitmapGlobalIndexWriter::with_compression_level(
                        writer,
                        write_options.block_size,
                        write_options.compression_type,
                        write_options.compression_level,
                        cmp,
                    );
                    for (key, local_row_id) in &rows {
                        writer.write(key.as_deref(), *local_row_id).map_err(|e| {
                            Error::DataInvalid {
                                message: format!(
                                    "Failed to write bitmap global index file '{file_name}'"
                                ),
                                source: Some(Box::new(e)),
                            }
                        })?;
                    }
                    let BitmapWriteResult { row_count, meta } =
                        writer.finish().await.map_err(|e| Error::DataInvalid {
                            message: format!(
                                "Failed to finish bitmap global index file '{file_name}'"
                            ),
                            source: Some(Box::new(e)),
                        })?;
                    (row_count, meta.serialize())
                }
                MULTIVALUE_GLOBAL_INDEX_TYPE => {
                    let GlobalIndexWriteOptions::Sorted(write_options) = write_options else {
                        unreachable!("multivalue uses sorted write options")
                    };
                    let mut writer = BitmapGlobalIndexWriter::with_compression_level(
                        writer,
                        write_options.block_size,
                        write_options.compression_type,
                        write_options.compression_level,
                        cmp,
                    );
                    for (key, local_row_id) in &rows {
                        let key = key
                            .as_deref()
                            .expect("multivalue extraction skips null keys");
                        writer.write_posting(key, *local_row_id).map_err(|e| {
                            Error::DataInvalid {
                                message: format!(
                                    "Failed to write multivalue global index file '{file_name}'"
                                ),
                                source: Some(Box::new(e)),
                            }
                        })?;
                    }
                    let BitmapWriteResult { row_count, meta } = writer
                        .finish_with_source_row_count(u64::try_from(row_count).unwrap())
                        .await
                        .map_err(|e| Error::DataInvalid {
                            message: format!(
                                "Failed to finish multivalue global index file '{file_name}'"
                            ),
                            source: Some(Box::new(e)),
                        })?;
                    (row_count, meta.serialize())
                }
                FM_GLOBAL_INDEX_TYPE => {
                    let GlobalIndexWriteOptions::FM(write_options) = write_options else {
                        unreachable!("FM uses FM write options")
                    };
                    write_fm_index_streaming(
                        self.table,
                        shard,
                        index_column,
                        index_field,
                        serialize_key,
                        writer,
                        *write_options,
                        &file_name,
                    )
                    .await?
                }
                _ => unreachable!("normalized queryable global index type"),
            };

            if written_row_count != u64::try_from(row_count).unwrap() {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Sorted global index expected {} rows, wrote {}",
                        row_count, written_row_count
                    ),
                    source: None,
                });
            }

            let status = self.table.file_io().get_status(&index_path).await?;
            let file_size = checked_i64(
                status.size,
                "Index file is too large for Rust IndexFileMeta",
            )?;
            Ok((written_row_count, index_meta, file_size))
        }
        .await;
        let (_, index_meta, file_size) = match write_result {
            Ok(result) => result,
            Err(error) => {
                let _ = self.table.file_io().delete_file(&index_path).await;
                return Err(error);
            }
        };
        Ok(IndexFileMeta {
            index_type: index_type.to_string(),
            file_name,
            file_size,
            row_count,
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: shard.row_range_start,
                row_range_end: shard.row_range_end,
                index_field_id: index_field.id(),
                extra_field_ids: None,
                source_meta: None,
                index_meta: Some(index_meta),
            }),
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn write_fm_index_streaming(
    table: &Table,
    shard: &SortedGlobalIndexShard,
    index_column: &str,
    index_field: &DataField,
    serialize_key: SerializeKeyFn,
    output: Box<dyn FileWrite>,
    write_options: FMWriteOptions,
    file_name: &str,
) -> Result<(u64, Vec<u8>)> {
    let mut writer =
        FMGlobalIndexWriter::new(output, write_options).map_err(|error| Error::DataInvalid {
            message: format!("Failed to create FM global index file '{file_name}'"),
            source: Some(Box::new(error)),
        })?;
    let splits = build_read_splits_for_shard(shard)?;
    let mut read_builder = table.new_read_builder();
    read_builder.with_projection(&[index_column, ROW_ID_FIELD_NAME])?;
    let read = read_builder.new_read()?;
    let mut batches = read.to_arrow(&splits)?;
    let expected_row_count = checked_row_count(shard.row_range_start, shard.row_range_end)?;
    let mut expected_row_id = shard.row_range_start;

    while let Some(batch) = batches.try_next().await? {
        let value_index =
            batch
                .schema()
                .index_of(index_column)
                .map_err(|error| Error::DataInvalid {
                    message: format!(
                        "Index column '{index_column}' not found in FM read batch: {error}"
                    ),
                    source: None,
                })?;
        let row_id_index = batch
            .schema()
            .index_of(ROW_ID_FIELD_NAME)
            .map_err(|error| Error::DataInvalid {
                message: format!("_ROW_ID column not found in FM read batch: {error}"),
                source: None,
            })?;
        let row_ids = batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::DataInvalid {
                message: "FM global index build requires non-null Int64 _ROW_ID".to_string(),
                source: None,
            })?;

        for row in 0..batch.num_rows() {
            if row_ids.is_null(row) {
                return Err(Error::DataInvalid {
                    message: "FM global index build found null _ROW_ID".to_string(),
                    source: None,
                });
            }
            let row_id = row_ids.value(row);
            if row_id != expected_row_id {
                return Err(Error::DataInvalid {
                    message: format!(
                        "FM global index build expected _ROW_ID {expected_row_id}, got {row_id}"
                    ),
                    source: None,
                });
            }
            expected_row_id = expected_row_id
                .checked_add(1)
                .ok_or_else(|| Error::DataInvalid {
                    message: "FM global index row ID overflow".to_string(),
                    source: None,
                })?;
            let local_row_id =
                u64::try_from(row_id - shard.row_range_start).map_err(|_| Error::DataInvalid {
                    message: format!(
                        "FM global index file '{file_name}' has a negative local row ID"
                    ),
                    source: None,
                })?;
            let key = extract_datum_from_arrow(&batch, row, value_index, index_field.data_type())?
                .map(|datum| serialize_key(&datum, index_field.data_type()));
            writer
                .write(key.as_deref(), local_row_id)
                .await
                .map_err(|error| Error::DataInvalid {
                    message: format!("Failed to write FM global index file '{file_name}'"),
                    source: Some(Box::new(error)),
                })?;
        }
    }

    let actual_row_count = expected_row_id - shard.row_range_start;
    if actual_row_count != expected_row_count {
        return Err(Error::DataInvalid {
            message: format!(
                "FM global index build expected {expected_row_count} rows, got {actual_row_count}"
            ),
            source: None,
        });
    }
    let result = writer.finish().await.map_err(|error| Error::DataInvalid {
        message: format!("Failed to finish FM global index file '{file_name}'"),
        source: Some(Box::new(error)),
    })?;
    Ok((result.row_count, result.index_meta))
}
