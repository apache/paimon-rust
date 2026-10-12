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
use arrow_buffer::ScalarBuffer;

pub(super) fn data_split_for_shard(shard: &VindexIndexShard) -> Result<DataSplit> {
    data_split_for_shard_ranges(
        shard,
        vec![RowRange::new(shard.row_range_start, shard.row_range_end)],
    )
}

pub(super) fn data_split_for_shard_ranges(
    shard: &VindexIndexShard,
    row_ranges: Vec<RowRange>,
) -> Result<DataSplit> {
    DataSplitBuilder::new()
        .with_snapshot(shard.snapshot_id)
        .with_partition(shard.partition.clone())
        .with_bucket(shard.source_bucket)
        .with_bucket_path(shard.bucket_path.clone())
        .with_total_buckets(shard.total_buckets)
        .with_data_files(shard.files.clone())
        .with_row_ranges(row_ranges)
        .build()
}

pub(super) struct ValidatedVectorBatch {
    pub(super) values: ScalarBuffer<f32>,
    pub(super) row_ids: ScalarBuffer<i64>,
    /// Number of non-null vectors, not the number of source rows.
    pub(super) vector_count: usize,
    pub(super) source_rows: usize,
}

impl ValidatedVectorBatch {
    pub(super) fn bytes(&self) -> &[u8] {
        self.values.inner().as_slice()
    }
}

pub(super) fn contains_null_vectors(batch: &RecordBatch, index_column: &str) -> bool {
    batch
        .column_by_name(index_column)
        .is_some_and(|column| column.null_count() != 0)
}

pub(super) fn extract_vector_batch(
    batch: &RecordBatch,
    index_column: &str,
    dimension: usize,
) -> Result<ValidatedVectorBatch> {
    validate_vector_batch_with(batch, index_column, dimension, |_| Ok(()))
}

pub(super) fn validate_vector_batch(
    batch: &RecordBatch,
    index_column: &str,
    dimension: usize,
    expected_row_id: &mut i64,
) -> Result<ValidatedVectorBatch> {
    validate_vector_batch_with(batch, index_column, dimension, |row_id| {
        if row_id != *expected_row_id {
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
        Ok(())
    })
}

pub(super) fn validate_vector_batch_ranges(
    batch: &RecordBatch,
    index_column: &str,
    dimension: usize,
    ranges: &[RowRange],
    range_index: &mut usize,
    expected_row_id: &mut i64,
) -> Result<ValidatedVectorBatch> {
    validate_vector_batch_with(batch, index_column, dimension, |row_id| {
        let range = ranges.get(*range_index).ok_or_else(|| Error::DataInvalid {
            message: format!("vindex vector extraction got unexpected _ROW_ID {row_id}"),
            source: None,
        })?;
        if row_id != *expected_row_id {
            return Err(Error::DataInvalid {
                message: format!(
                    "vindex vector extraction expected _ROW_ID {}, got {}",
                    expected_row_id, row_id
                ),
                source: None,
            });
        }
        if row_id == range.to() {
            *range_index += 1;
            *expected_row_id = match ranges.get(*range_index) {
                Some(next) => next.from(),
                None => row_id.checked_add(1).ok_or_else(|| Error::DataInvalid {
                    message: "vindex expected row id overflows i64".to_string(),
                    source: None,
                })?,
            };
        } else {
            *expected_row_id = row_id.checked_add(1).ok_or_else(|| Error::DataInvalid {
                message: "vindex expected row id overflows i64".to_string(),
                source: None,
            })?;
        }
        Ok(())
    })
}

fn validate_vector_batch_with(
    batch: &RecordBatch,
    index_column: &str,
    dimension: usize,
    mut validate_row_id: impl FnMut(i64) -> Result<()>,
) -> Result<ValidatedVectorBatch> {
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
        validate_row_id(*row_id)?;
    }

    let column = batch.column(vector_index);
    if column.null_count() != 0 {
        return compact_nullable_vectors(column.as_ref(), row_ids, dimension, batch.num_rows());
    }

    let (values, start, end) = if let Some(array) = column.as_any().downcast_ref::<ListArray>() {
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
    checked_vector_bytes(end - start, 1)?;
    Ok(ValidatedVectorBatch {
        values: values.values().slice(start, end - start),
        row_ids: row_ids.values().clone(),
        vector_count: batch.num_rows(),
        source_rows: batch.num_rows(),
    })
}

fn compact_nullable_vectors(
    column: &dyn Array,
    row_ids: &Int64Array,
    dimension: usize,
    source_rows: usize,
) -> Result<ValidatedVectorBatch> {
    let list = column.as_any().downcast_ref::<ListArray>();
    let fixed = column.as_any().downcast_ref::<FixedSizeListArray>();
    if list.is_none() && fixed.is_none() {
        return Err(Error::DataInvalid {
            message:
                "vindex vector extraction requires Arrow List<Float32> or FixedSizeList<Float32>"
                    .into(),
            source: None,
        });
    }
    if let Some(fixed) = fixed {
        if fixed.value_length() as usize != dimension {
            return Err(Error::DataInvalid {
                message: format!(
                    "vindex vector dimension mismatch: expected {dimension}, got {}",
                    fixed.value_length()
                ),
                source: None,
            });
        }
    }
    let child = list
        .map(|list| list.values())
        .or_else(|| fixed.map(|list| list.values()))
        .unwrap();
    if !child.as_any().is::<Float32Array>() {
        return Err(Error::DataInvalid {
            message: "vindex vector extraction requires Float32 vector elements".into(),
            source: None,
        });
    }
    let count = source_rows - column.null_count();
    checked_vector_bytes(count, dimension)?;
    let mut values = Vec::with_capacity(count * dimension);
    let mut ids = Vec::with_capacity(count);
    for row in 0..source_rows {
        if column.is_null(row) {
            continue;
        }
        let vector = match list {
            Some(list) => list.value(row),
            None => fixed.unwrap().value(row),
        };
        if vector.len() != dimension {
            return Err(Error::DataInvalid {
                message: format!(
                    "vindex vector dimension mismatch: expected {dimension}, got {}",
                    vector.len()
                ),
                source: None,
            });
        }
        let vector = vector.as_any().downcast_ref::<Float32Array>().unwrap();
        if vector.null_count() != 0 {
            return Err(Error::DataInvalid {
                message: "vindex vector extraction found null vector element".into(),
                source: None,
            });
        }
        values.extend_from_slice(vector.values());
        ids.push(row_ids.value(row));
    }
    Ok(ValidatedVectorBatch {
        values: values.into(),
        row_ids: ids.into(),
        vector_count: count,
        source_rows,
    })
}

pub(super) fn local_ids(row_ids: &[i64], start: i64, row_count: usize) -> Result<Vec<i64>> {
    let end = start
        .checked_add(i64::try_from(row_count).map_err(|e| Error::DataInvalid {
            message: "vindex row count does not fit i64".to_string(),
            source: Some(Box::new(e)),
        })?)
        .ok_or_else(|| Error::DataInvalid {
            message: "vindex row range overflows i64".to_string(),
            source: None,
        })?;
    row_ids
        .iter()
        .map(|row_id| {
            if *row_id < start || *row_id >= end {
                Err(Error::DataInvalid {
                    message: format!("vindex row id {row_id} is outside shard [{start}, {end})"),
                    source: None,
                })
            } else {
                Ok(*row_id - start)
            }
        })
        .collect()
}
