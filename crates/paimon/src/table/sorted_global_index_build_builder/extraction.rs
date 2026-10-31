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

//! Projected table reads and conversion into serialized index rows.

use super::planning::SortedGlobalIndexShard;
use super::validation::checked_row_count;
use super::{SerializeKeyFn, SortedIndexKeyRow};
use crate::btree::key_serde::DynKeyComparator;
use crate::spec::{
    extract_datum_from_array, extract_datum_from_arrow, DataField, DataType, ROW_ID_FIELD_NAME,
};
use crate::table::global_index_types::MULTIVALUE_GLOBAL_INDEX_TYPE;
use crate::table::stats_filter::group_by_overlapping_row_id;
use crate::table::{DataSplit, DataSplitBuilder, RowRange, Table};
use crate::{Error, Result};
use arrow_array::{Array, FixedSizeListArray, Int64Array, LargeListArray, ListArray, RecordBatch};
use futures::TryStreamExt;
use std::cmp::Ordering;

pub(super) async fn extract_index_rows(
    table: &Table,
    shard: &SortedGlobalIndexShard,
    index_fields: &[&DataField],
    index_type: &str,
    serialize_key: SerializeKeyFn,
    budget: usize,
    cmp: crate::btree::key_serde::KeyComparator,
) -> Result<super::sort::SortedRows> {
    let splits = build_read_splits_for_shard(shard)?;
    let mut read_builder = table.new_read_builder();
    let mut projection = index_fields
        .iter()
        .map(|field| field.name())
        .collect::<Vec<_>>();
    projection.push(ROW_ID_FIELD_NAME);
    read_builder.with_projection(&projection)?;
    let read = read_builder.new_read()?;
    let mut batches = read.to_arrow(&splits)?;
    let fields = std::sync::Arc::new(
        index_fields
            .iter()
            .map(|field| (*field).clone())
            .collect::<Vec<_>>(),
    );
    let mut sorter = super::sort::IndexSorter::new(budget, cmp);
    let mut next_row_id = shard.row_range_start;
    while let Some(batch) = batches.try_next().await? {
        let fields = fields.clone();
        let index_type = index_type.to_string();
        let row_range_start = shard.row_range_start;
        let row_count = i64::try_from(batch.num_rows()).map_err(|_| Error::DataInvalid {
            message: "Index batch row count overflow".into(),
            source: None,
        })?;
        // Sorting and local scratch-file I/O must not block the async reader.
        sorter = tokio::task::spawn_blocking(move || {
            let refs = fields.iter().collect::<Vec<_>>();
            let mut emit = |row| sorter.push(row);
            let batches = std::slice::from_ref(&batch);
            if refs.len() > 1 {
                emit_composite_rows(
                    batches,
                    &refs,
                    row_range_start,
                    row_count,
                    next_row_id,
                    &mut emit,
                )?;
            } else if index_type == MULTIVALUE_GLOBAL_INDEX_TYPE {
                let DataType::Array(array_type) = refs[0].data_type() else {
                    unreachable!("validated multivalue type")
                };
                emit_multivalue_rows(
                    batches,
                    refs[0].name(),
                    array_type.element_type(),
                    row_range_start,
                    row_count,
                    serialize_key,
                    next_row_id,
                    &mut emit,
                )?;
            } else {
                emit_scalar_rows(
                    batches,
                    refs[0].name(),
                    refs[0].data_type(),
                    row_range_start,
                    row_count,
                    serialize_key,
                    next_row_id,
                    &mut emit,
                )?;
            }
            Ok::<_, Error>(sorter)
        })
        .await
        .map_err(sort_task_error)??;
        next_row_id = next_row_id
            .checked_add(row_count)
            .ok_or_else(|| Error::DataInvalid {
                message: "Index row ID overflow".into(),
                source: None,
            })?;
    }
    let expected = checked_row_count(shard.row_range_start, shard.row_range_end)?;
    if next_row_id - shard.row_range_start != expected {
        return Err(Error::DataInvalid {
            message: format!(
                "Sorted index expected {expected} source rows, got {}",
                next_row_id - shard.row_range_start
            ),
            source: None,
        });
    }
    tokio::task::spawn_blocking(move || sorter.finish())
        .await
        .map_err(sort_task_error)?
}

fn sort_task_error(error: tokio::task::JoinError) -> Error {
    Error::UnexpectedError {
        message: format!("Sorted index sort task failed: {error}"),
        source: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_composite_rows(
    batches: &[RecordBatch],
    fields: &[&DataField],
    row_range_start: i64,
    expected_row_count: i64,
    next_row_id: i64,
    emit: &mut dyn FnMut(SortedIndexKeyRow) -> Result<()>,
) -> Result<()> {
    let fields = fields
        .iter()
        .map(|field| (*field).clone())
        .collect::<Vec<_>>();
    let codec = crate::btree::CompositeKeyCodec::new(&fields);
    let mut expected_row_id = next_row_id;
    for batch in batches {
        let indices = fields
            .iter()
            .map(|field| batch.schema().index_of(field.name()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| Error::DataInvalid {
                message: format!("Composite index column missing: {error}"),
                source: None,
            })?;
        let row_id_index = batch
            .schema()
            .index_of(ROW_ID_FIELD_NAME)
            .map_err(|error| Error::DataInvalid {
                message: format!("_ROW_ID column missing: {error}"),
                source: None,
            })?;
        let row_ids = batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| Error::DataInvalid {
                message: "Composite index requires Int64 _ROW_ID".into(),
                source: None,
            })?;
        for row in 0..batch.num_rows() {
            if row_ids.is_null(row) || row_ids.value(row) != expected_row_id {
                return Err(Error::DataInvalid {
                    message: format!("Composite index expected non-null _ROW_ID {expected_row_id}"),
                    source: None,
                });
            }
            let values = fields
                .iter()
                .zip(&indices)
                .map(|(field, index)| {
                    extract_datum_from_arrow(batch, row, *index, field.data_type())
                })
                .collect::<Result<Vec<_>>>()?;
            emit((
                Some(codec.serialize(&values)?),
                expected_row_id - row_range_start,
            ))?;
            expected_row_id = expected_row_id
                .checked_add(1)
                .ok_or_else(|| Error::DataInvalid {
                    message: "Composite index row ID overflow".into(),
                    source: None,
                })?;
        }
    }
    if expected_row_id - next_row_id != expected_row_count {
        return Err(Error::DataInvalid {
            message: format!(
                "Composite index expected {expected_row_count} rows, got {}",
                expected_row_id - next_row_id
            ),
            source: None,
        });
    }
    Ok(())
}

pub(super) fn build_read_splits_for_shard(
    shard: &SortedGlobalIndexShard,
) -> Result<Vec<DataSplit>> {
    let shard_range = RowRange::new(shard.row_range_start, shard.row_range_end);
    group_by_overlapping_row_id(shard.files.clone())
        .into_iter()
        .filter_map(|files| {
            let ranges = files
                .iter()
                .filter_map(|file| {
                    file.row_id_range()
                        .and_then(|(start, end)| shard_range.intersect_inclusive(start, end))
                })
                .collect::<Vec<_>>();
            let ranges = crate::table::merge_row_ranges(ranges);
            if ranges.is_empty() {
                return None;
            }
            let raw_convertible = files.len() == 1;
            // Like Java's global-index build splits, deliberately omit deletion
            // files: indexes cover physical row IDs. Readers apply the query
            // snapshot's deletion vectors, including deletes after this build.
            Some(
                DataSplitBuilder::new()
                    .with_snapshot(shard.snapshot_id)
                    .with_partition(shard.partition.clone())
                    .with_bucket(shard.source_bucket)
                    .with_bucket_path(shard.bucket_path.clone())
                    .with_total_buckets(shard.total_buckets)
                    .with_data_files(files)
                    .with_row_ranges(ranges)
                    .with_raw_convertible(raw_convertible)
                    .build(),
            )
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(in crate::table) fn emit_scalar_rows(
    batches: &[RecordBatch],
    index_column: &str,
    data_type: &DataType,
    row_range_start: i64,
    expected_row_count: i64,
    serialize_key: SerializeKeyFn,
    next_row_id: i64,
    emit: &mut dyn FnMut(SortedIndexKeyRow) -> Result<()>,
) -> Result<()> {
    let mut expected_row_id = next_row_id;
    for batch in batches {
        let value_index =
            batch
                .schema()
                .index_of(index_column)
                .map_err(|e| Error::DataInvalid {
                    message: format!("Index column '{index_column}' not found in read batch: {e}"),
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
                message: "Sorted global index build requires non-null Int64 _ROW_ID".to_string(),
                source: None,
            })?;

        for row in 0..batch.num_rows() {
            if row_ids.is_null(row) {
                return Err(Error::DataInvalid {
                    message: "Sorted global index build found null _ROW_ID".to_string(),
                    source: None,
                });
            }
            let row_id = row_ids.value(row);
            if row_id != expected_row_id {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Sorted global index build expected _ROW_ID {}, got {}",
                        expected_row_id, row_id
                    ),
                    source: None,
                });
            }
            expected_row_id += 1;

            let key = extract_datum_from_arrow(batch, row, value_index, data_type)?
                .map(|datum| serialize_key(&datum, data_type));
            emit((key, row_id - row_range_start))?;
        }
    }
    let actual_row_count = expected_row_id - next_row_id;
    if actual_row_count != expected_row_count {
        return Err(Error::DataInvalid {
            message: format!(
                "Sorted global index build expected {} rows, got {}",
                expected_row_count, actual_row_count
            ),
            source: None,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(in crate::table) fn emit_multivalue_rows(
    batches: &[RecordBatch],
    index_column: &str,
    element_type: &DataType,
    row_range_start: i64,
    expected_row_count: i64,
    serialize_key: SerializeKeyFn,
    next_row_id: i64,
    emit: &mut dyn FnMut(SortedIndexKeyRow) -> Result<()>,
) -> Result<()> {
    let mut expected_row_id = next_row_id;
    for batch in batches {
        let value_index =
            batch
                .schema()
                .index_of(index_column)
                .map_err(|e| Error::DataInvalid {
                    message: format!("Index column '{index_column}' not found in read batch: {e}"),
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
                message: "Multivalue global index build requires non-null Int64 _ROW_ID"
                    .to_string(),
                source: None,
            })?;

        #[derive(Clone, Copy)]
        enum ArrayLayout<'a> {
            List(&'a ListArray),
            LargeList(&'a LargeListArray),
            Fixed(&'a FixedSizeListArray),
        }
        let column = batch.column(value_index);
        let layout = if let Some(array) = column.as_any().downcast_ref::<ListArray>() {
            ArrayLayout::List(array)
        } else if let Some(array) = column.as_any().downcast_ref::<LargeListArray>() {
            ArrayLayout::LargeList(array)
        } else if let Some(array) = column.as_any().downcast_ref::<FixedSizeListArray>() {
            ArrayLayout::Fixed(array)
        } else {
            return Err(Error::DataInvalid {
                message: format!(
                    "Multivalue global index extraction requires an Arrow list column, got {:?}",
                    column.data_type()
                ),
                source: None,
            });
        };
        let values = match layout {
            ArrayLayout::List(array) => array.values(),
            ArrayLayout::LargeList(array) => array.values(),
            ArrayLayout::Fixed(array) => array.values(),
        };

        for row in 0..batch.num_rows() {
            if row_ids.is_null(row) {
                return Err(Error::DataInvalid {
                    message: "Multivalue global index build found null _ROW_ID".to_string(),
                    source: None,
                });
            }
            let row_id = row_ids.value(row);
            if row_id != expected_row_id {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Multivalue global index build expected _ROW_ID {}, got {}",
                        expected_row_id, row_id
                    ),
                    source: None,
                });
            }
            expected_row_id += 1;

            let is_null = match layout {
                ArrayLayout::List(array) => array.is_null(row),
                ArrayLayout::LargeList(array) => array.is_null(row),
                ArrayLayout::Fixed(array) => array.is_null(row),
            };
            if is_null {
                continue;
            }
            let (start, end) = match layout {
                ArrayLayout::List(array) => {
                    let offsets = array.value_offsets();
                    (
                        usize::try_from(offsets[row]),
                        usize::try_from(offsets[row + 1]),
                    )
                }
                ArrayLayout::LargeList(array) => {
                    let offsets = array.value_offsets();
                    (
                        usize::try_from(offsets[row]),
                        usize::try_from(offsets[row + 1]),
                    )
                }
                ArrayLayout::Fixed(array) => {
                    let start = usize::try_from(array.value_offset(row));
                    let end = usize::try_from(array.value_offset(row) + array.value_length());
                    (start, end)
                }
            };
            let (start, end) = match (start, end) {
                (Ok(start), Ok(end)) => (start, end),
                _ => {
                    return Err(Error::DataInvalid {
                        message: "Multivalue global index found a negative array offset"
                            .to_string(),
                        source: None,
                    })
                }
            };
            for element_index in start..end {
                if let Some(datum) =
                    extract_datum_from_array(values, element_index, value_index, element_type)?
                {
                    emit((
                        Some(serialize_key(&datum, element_type)),
                        row_id - row_range_start,
                    ))?;
                }
            }
        }
    }

    let actual_row_count = expected_row_id - next_row_id;
    if actual_row_count != expected_row_count {
        return Err(Error::DataInvalid {
            message: format!(
                "Multivalue global index build expected {} source rows, got {}",
                expected_row_count, actual_row_count
            ),
            source: None,
        });
    }
    Ok(())
}

/// Sort the extracted rows with the index's key comparator.
///
/// Both keys come from [`super::make_index_key_codec`] for the column's current type, so
/// a comparison failure is a real defect rather than an index built before a type change;
/// it aborts the build instead of degrading.
pub(super) fn sort_index_rows(
    rows: &mut [SortedIndexKeyRow],
    cmp: &DynKeyComparator<'_>,
) -> Result<()> {
    let mut failure = None;
    rows.sort_unstable_by(|left, right| match (&left.0, &right.0) {
        (None, None) => left.1.cmp(&right.1),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left_key), Some(right_key)) => match cmp(left_key, right_key) {
            Ok(order) => order.then_with(|| left.1.cmp(&right.1)),
            Err(error) => {
                failure.get_or_insert(error);
                left.1.cmp(&right.1)
            }
        },
    });
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

#[cfg(test)]
pub(super) fn extract_index_rows_from_batches(
    batches: &[RecordBatch],
    column: &str,
    data_type: &DataType,
    start: i64,
    count: i64,
    serialize: SerializeKeyFn,
) -> Result<Vec<SortedIndexKeyRow>> {
    let mut rows = Vec::new();
    emit_scalar_rows(
        batches,
        column,
        data_type,
        start,
        count,
        serialize,
        start,
        &mut |row| {
            rows.push(row);
            Ok(())
        },
    )?;
    Ok(rows)
}

#[cfg(test)]
pub(super) fn extract_multivalue_index_rows_from_batches(
    batches: &[RecordBatch],
    column: &str,
    data_type: &DataType,
    start: i64,
    count: i64,
    serialize: SerializeKeyFn,
) -> Result<Vec<SortedIndexKeyRow>> {
    let mut rows = Vec::new();
    emit_multivalue_rows(
        batches,
        column,
        data_type,
        start,
        count,
        serialize,
        start,
        &mut |row| {
            rows.push(row);
            Ok(())
        },
    )?;
    Ok(rows)
}
