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

use super::planning::VindexIndexShard;
use super::validation::checked_vector_bytes;
use crate::spec::ROW_ID_FIELD_NAME;
use crate::table::{DataSplit, DataSplitBuilder, RowRange};
use crate::{Error, Result};
use arrow_array::{Array, FixedSizeListArray, Float32Array, Int64Array, ListArray, RecordBatch};

pub(super) fn data_split_for_shard(shard: &VindexIndexShard) -> Result<DataSplit> {
    DataSplitBuilder::new()
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
        .build()
}

pub(super) struct ValidatedVectorBatch<'a> {
    pub(super) values: &'a [f32],
    pub(super) bytes: &'a [u8],
    pub(super) row_count: usize,
}

pub(super) fn validate_vector_batch<'a>(
    batch: &'a RecordBatch,
    index_column: &str,
    dimension: usize,
    expected_row_id: &mut i64,
) -> Result<ValidatedVectorBatch<'a>> {
    let vector_index = batch
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
    let column = batch.column(vector_index);
    let (values, start, end) = if let Some(array) = column.as_any().downcast_ref::<ListArray>() {
        if array.null_count() != 0 {
            return Err(Error::DataInvalid {
                message: "vindex vector extraction found null vector row".to_string(),
                source: None,
            });
        }
        let offsets = array.value_offsets();
        for offsets in offsets.windows(2) {
            let actual = offsets[1] - offsets[0];
            if actual != dimension as i32 {
                return Err(Error::DataInvalid {
                    message: format!(
                        "vindex vector dimension mismatch: expected {dimension}, got {actual}"
                    ),
                    source: None,
                });
            }
        }
        let start = usize::try_from(offsets[0]).map_err(|e| Error::DataInvalid {
            message: "vindex vector offset is negative".to_string(),
            source: Some(Box::new(e)),
        })?;
        let end = usize::try_from(offsets[offsets.len() - 1]).map_err(|e| Error::DataInvalid {
            message: "vindex vector offset is negative".to_string(),
            source: Some(Box::new(e)),
        })?;
        (array.values(), start, end)
    } else if let Some(array) = column.as_any().downcast_ref::<FixedSizeListArray>() {
        let actual = usize::try_from(array.value_length()).map_err(|e| Error::DataInvalid {
            message: format!(
                "Invalid vindex FixedSizeList dimension: {}",
                array.value_length()
            ),
            source: Some(Box::new(e)),
        })?;
        if actual != dimension {
            return Err(Error::DataInvalid {
                message: format!(
                    "vindex vector dimension mismatch: expected {dimension}, got {actual}"
                ),
                source: None,
            });
        }
        if array.null_count() != 0 {
            return Err(Error::DataInvalid {
                message: "vindex vector extraction found null vector row".to_string(),
                source: None,
            });
        }
        let end = batch
            .num_rows()
            .checked_mul(dimension)
            .ok_or_else(|| Error::DataInvalid {
                message: "vindex batch vector length overflows usize".to_string(),
                source: None,
            })?;
        (array.values(), 0, end)
    } else {
        return Err(Error::DataInvalid {
            message:
                "vindex vector extraction requires Arrow List<Float32> or FixedSizeList<Float32>"
                    .to_string(),
            source: None,
        });
    };
    let values = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| Error::DataInvalid {
            message: "vindex vector extraction requires Float32 vector elements".to_string(),
            source: None,
        })?;
    if values.null_count() != 0
        && values
            .nulls()
            .is_some_and(|nulls| nulls.slice(start, end - start).null_count() != 0)
    {
        return Err(Error::DataInvalid {
            message: "vindex vector extraction found null vector element".to_string(),
            source: None,
        });
    }
    let row_ids = batch
        .column(row_id_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::DataInvalid {
            message: "vindex vector extraction requires non-null Int64 _ROW_ID".to_string(),
            source: None,
        })?;
    if row_ids.null_count() != 0 {
        return Err(Error::DataInvalid {
            message: "vindex vector extraction found null _ROW_ID".to_string(),
            source: None,
        });
    }
    for row_id in row_ids.values() {
        if *row_id != *expected_row_id {
            return Err(Error::DataInvalid {
                message: format!(
                    "vindex vector extraction expected _ROW_ID {}, got {}",
                    expected_row_id, row_id
                ),
                source: None,
            });
        }
        *expected_row_id = expected_row_id
            .checked_add(1)
            .ok_or_else(|| Error::DataInvalid {
                message: "vindex expected row id overflows i64".to_string(),
                source: None,
            })?;
    }

    let byte_start = checked_vector_bytes(start, 1)?;
    let byte_end = checked_vector_bytes(end, 1)?;
    Ok(ValidatedVectorBatch {
        values: &values.values()[start..end],
        bytes: &values.values().inner().as_slice()[byte_start..byte_end],
        row_count: batch.num_rows(),
    })
}
