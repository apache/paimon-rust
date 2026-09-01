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

//! Arrow vector extraction for Lumina shards.

use super::planning::LuminaIndexShard;
use super::validation::checked_row_count;
use crate::spec::ROW_ID_FIELD_NAME;
use crate::table::{DataSplitBuilder, RowRange, Table};
use crate::{Error, Result};
use arrow_array::{Array, FixedSizeListArray, Float32Array, Int64Array, ListArray, RecordBatch};
use futures::TryStreamExt;

pub(super) async fn extract_vectors(
    table: &Table,
    shard: &LuminaIndexShard,
    index_column: &str,
    dimension: i32,
) -> Result<Vec<f32>> {
    let split = DataSplitBuilder::new()
        .with_snapshot(shard.snapshot_id)
        .with_partition(shard.partition.clone())
        .with_bucket(shard.source_bucket)
        .with_bucket_path(shard.bucket_path.clone())
        .with_total_buckets(shard.total_buckets)
        .with_data_files(shard.files.clone())
        .with_row_ranges(vec![RowRange::new(
            shard.row_range_start,
            shard.row_range_end,
        )])
        .build()?;

    let mut read_builder = table.new_read_builder();
    read_builder.with_projection(&[index_column, ROW_ID_FIELD_NAME])?;
    let read = read_builder.new_read()?;
    let batches = read.to_arrow(&[split])?.try_collect::<Vec<_>>().await?;
    extract_vectors_from_batches(
        &batches,
        index_column,
        dimension,
        shard.row_range_start,
        checked_row_count(shard.row_range_start, shard.row_range_end)?,
    )
}

pub(super) fn extract_vectors_from_batches(
    batches: &[RecordBatch],
    index_column: &str,
    dimension: i32,
    row_range_start: i64,
    expected_row_count: i64,
) -> Result<Vec<f32>> {
    let dimension = usize::try_from(dimension).map_err(|e| Error::DataInvalid {
        message: format!("Invalid Lumina dimension: {dimension}"),
        source: Some(Box::new(e)),
    })?;
    let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let mut vectors = Vec::with_capacity(row_count * dimension);
    let mut expected_row_id = row_range_start;
    for batch in batches {
        let vector_index =
            batch
                .schema()
                .index_of(index_column)
                .map_err(|e| Error::DataInvalid {
                    message: format!("Vector column '{index_column}' not found in read batch: {e}"),
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
        // Resolve the vector column as either List<Float32> (ARRAY<FLOAT>) or
        // FixedSizeList<Float32> (VECTOR<FLOAT>). Both yield a Float32Array of
        // values plus a per-row [start, end) slice.
        let column = batch.column(vector_index);
        enum VectorLayout<'a> {
            List(&'a ListArray),
            Fixed(&'a FixedSizeListArray),
        }
        let layout = if let Some(a) = column.as_any().downcast_ref::<ListArray>() {
            VectorLayout::List(a)
        } else if let Some(a) = column.as_any().downcast_ref::<FixedSizeListArray>() {
            VectorLayout::Fixed(a)
        } else {
            return Err(Error::DataInvalid {
                message: "Lumina vector extraction requires Arrow List<Float32> or FixedSizeList<Float32>".to_string(),
                source: None,
            });
        };
        let values = match layout {
            VectorLayout::List(a) => a.values(),
            VectorLayout::Fixed(a) => a.values(),
        }
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| Error::DataInvalid {
            message: "Lumina vector extraction requires Float32 vector elements".to_string(),
            source: None,
        })?;
        let row_ids = batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::DataInvalid {
                message: "Lumina vector extraction requires non-null Int64 _ROW_ID".to_string(),
                source: None,
            })?;

        for row in 0..batch.num_rows() {
            if row_ids.is_null(row) {
                return Err(Error::DataInvalid {
                    message: "Lumina vector extraction found null _ROW_ID".to_string(),
                    source: None,
                });
            }
            let row_id = row_ids.value(row);
            if row_id != expected_row_id {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Lumina vector extraction expected _ROW_ID {}, got {}",
                        expected_row_id, row_id
                    ),
                    source: None,
                });
            }
            expected_row_id += 1;

            let is_null = match layout {
                VectorLayout::List(a) => a.is_null(row),
                VectorLayout::Fixed(a) => a.is_null(row),
            };
            if is_null {
                return Err(Error::DataInvalid {
                    message: "Lumina vector extraction found null vector row".to_string(),
                    source: None,
                });
            }
            let (start, end) = match layout {
                VectorLayout::List(a) => {
                    let offsets = a.value_offsets();
                    (offsets[row] as usize, offsets[row + 1] as usize)
                }
                VectorLayout::Fixed(a) => {
                    let len = a.value_length() as usize;
                    (row * len, (row + 1) * len)
                }
            };
            if end - start != dimension {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Lumina vector dimension mismatch: expected {}, got {}",
                        dimension,
                        end - start
                    ),
                    source: None,
                });
            }
            for value_index in start..end {
                if values.is_null(value_index) {
                    return Err(Error::DataInvalid {
                        message: "Lumina vector extraction found null vector element".to_string(),
                        source: None,
                    });
                }
                vectors.push(values.value(value_index));
            }
        }
    }
    let actual_row_count = expected_row_id - row_range_start;
    if actual_row_count != expected_row_count {
        return Err(Error::DataInvalid {
            message: format!(
                "Lumina vector extraction expected {} rows, got {}",
                expected_row_count, actual_row_count
            ),
            source: None,
        });
    }
    Ok(vectors)
}
