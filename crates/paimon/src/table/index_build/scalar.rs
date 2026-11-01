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

//! Native scalar payload writing, independent of PK/DE source planning.
use super::extraction::collect_sorted_rows;
use super::{error, IndexPayload};
use crate::btree::key_serde::KeyComparator;
use crate::btree::{make_key_comparator, serialize_datum, BTreeIndexWriter};
use crate::fm_index::{FMGlobalIndexWriter, FMOptions, FMWriteOptions};
use crate::io::OutputFile;
use crate::spec::{
    extract_datum_from_arrow, CoreOptions, DataField, DataType, Datum, RowType, ROW_ID_FIELD_NAME,
};
use crate::table::bitmap_global_index_format::{
    make_bitmap_key_comparator, serialize_bitmap_datum,
};
use crate::table::bitmap_global_index_writer::BitmapGlobalIndexWriter;
use crate::table::sorted_global_index_options::SortedIndexWriteOptions;
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::{Error, Result};
use arrow_array::{Array, Int64Array};
use futures::TryStreamExt;
use std::collections::HashMap;

pub(in crate::table) type SortedIndexKeyRow = (Option<Vec<u8>>, i64);
pub(in crate::table) type SerializeKeyFn = fn(&Datum, &DataType) -> Vec<u8>;

enum WriteOptions {
    Sorted(SortedIndexWriteOptions),
    FM(FMWriteOptions),
}
pub(in crate::table) struct ScalarIndexOptions {
    kind: &'static str,
    write: WriteOptions,
    buffer_size: usize,
}
impl ScalarIndexOptions {
    pub fn from_options(kind: &str, options: &HashMap<String, String>) -> Result<Self> {
        let kind = crate::table::global_index_types::normalize_queryable_global_index_type(kind)
            .ok_or_else(|| Error::Unsupported {
                message: format!("Unsupported scalar index type: {kind}"),
            })?;
        if kind == "fm" {
            Ok(Self {
                kind,
                write: WriteOptions::FM(FMOptions::from_options(options)?.write),
                buffer_size: 0,
            })
        } else {
            Ok(Self {
                kind,
                write: WriteOptions::Sorted(SortedIndexWriteOptions::from_options(kind, options)?),
                buffer_size: CoreOptions::new(options).write_buffer_size()?,
            })
        }
    }

    pub fn index_type(&self) -> &'static str {
        self.kind
    }
}

pub(in crate::table) fn make_index_key_codec(
    kind: &str,
    data_type: &DataType,
) -> (KeyComparator, SerializeKeyFn) {
    match kind {
        "btree" => (make_key_comparator(data_type), serialize_datum),
        "bitmap" => (
            make_bitmap_key_comparator(data_type),
            serialize_bitmap_datum,
        ),
        _ => unreachable!("normalized scalar codec"),
    }
}

pub(in crate::table) async fn write_payload(
    batches: ArrowRecordBatchStream,
    output: OutputFile,
    fields: &[&DataField],
    range: RowRange,
    options: &ScalarIndexOptions,
) -> Result<IndexPayload> {
    let kind = options.index_type();
    let count = super::row_count(&range)?;
    let composite = DataType::Row(RowType::new(fields.iter().map(|f| (*f).clone()).collect()));
    let key_type = if fields.len() > 1 {
        &composite
    } else {
        super::fields::index_key_type(kind, fields[0])?
    };
    let codec = if kind == "btree" { "btree" } else { "bitmap" };
    let (cmp, serialize) = make_index_key_codec(codec, key_type);
    let (written, index_meta) = match &options.write {
        WriteOptions::FM(options) => {
            write_fm(batches, output, fields[0], range, serialize, *options).await?
        }
        WriteOptions::Sorted(write) => {
            let mut rows = collect_sorted_rows(
                batches,
                fields,
                kind,
                range,
                serialize,
                options.buffer_size,
                cmp,
            )
            .await?;
            let cmp = make_index_key_codec(codec, key_type).0;
            let output = output.writer().await?;
            if kind == "btree" {
                let mut writer = BTreeIndexWriter::with_comparator_and_options(
                    output,
                    write.block_size,
                    write.compression_type,
                    write.compression_level,
                    write.bloom_filter_enabled,
                    cmp,
                )
                .with_file_version(write.btree_file_version)
                .map_err(error)?;
                while let Some((key, id)) = rows.next().await? {
                    writer.write(key.as_deref(), id).await.map_err(error)?;
                }
                let result = writer.finish().await.map_err(error)?;
                (result.row_count, result.meta.serialize())
            } else {
                let mut writer = BitmapGlobalIndexWriter::with_compression_level(
                    output,
                    write.block_size,
                    write.compression_type,
                    write.compression_level,
                    cmp,
                );
                while let Some((key, id)) = rows.next().await? {
                    if kind == "multivalue" {
                        writer
                            .write_posting(key.as_deref().expect("nonnull multivalue posting"), id)
                            .map_err(error)?;
                    } else {
                        writer.write(key.as_deref(), id).map_err(error)?;
                    }
                }
                let result = if kind == "multivalue" {
                    writer.finish_with_source_row_count(count as u64).await
                } else {
                    writer.finish().await
                }
                .map_err(error)?;
                (result.row_count, result.meta.serialize())
            }
        }
    };
    if written != count as u64 {
        return Err(Error::DataInvalid {
            message: format!("Sorted index expected {count} rows, wrote {written}"),
            source: None,
        });
    }
    Ok(IndexPayload {
        row_count: count,
        index_meta,
    })
}

async fn write_fm(
    mut batches: ArrowRecordBatchStream,
    output: OutputFile,
    index_field: &DataField,
    range: RowRange,
    serialize_key: SerializeKeyFn,
    options: FMWriteOptions,
) -> Result<(u64, Vec<u8>)> {
    let mut writer = FMGlobalIndexWriter::new(output.writer().await?, options).map_err(error)?;
    let index_column = index_field.name();
    let expected_row_count = super::row_count(&range)?;
    let mut expected_row_id = range.from();

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
                u64::try_from(row_id - range.from()).map_err(|_| Error::DataInvalid {
                    message: "FM index build has a negative local row ID".into(),
                    source: None,
                })?;
            let key = extract_datum_from_arrow(&batch, row, value_index, index_field.data_type())?
                .map(|datum| serialize_key(&datum, index_field.data_type()));
            writer
                .write(key.as_deref(), local_row_id)
                .await
                .map_err(|error| Error::DataInvalid {
                    message: "Failed to write FM index file".into(),
                    source: Some(Box::new(error)),
                })?;
        }
    }

    let actual_row_count = expected_row_id - range.from();
    if actual_row_count != expected_row_count {
        return Err(Error::DataInvalid {
            message: format!(
                "FM global index build expected {expected_row_count} rows, got {actual_row_count}"
            ),
            source: None,
        });
    }
    let result = writer.finish().await.map_err(|error| Error::DataInvalid {
        message: "Failed to finish FM index file".into(),
        source: Some(Box::new(error)),
    })?;
    Ok((result.row_count, result.index_meta))
}
