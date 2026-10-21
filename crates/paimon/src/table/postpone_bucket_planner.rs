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

//! Snapshot-bound sizing for fixed-bucket batch writes.

use super::postpone_bucket_plan::data_invalid;
use crate::spec::{batch_to_serialized_bytes, BinaryRow, CoreOptions, DataField, DataType};
use crate::table::{PostponeBucketPlan, Table, TableScan};
use crate::Result;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, MapArray, RecordBatch,
    StructArray,
};
use arrow_schema::{DataType as ArrowType, Schema};
use std::collections::HashMap;
use std::sync::Arc;

pub(super) struct PostponeBucketPlanner {
    fields: Vec<DataField>,
    partitions: Vec<usize>,
    known: HashMap<Vec<u8>, i32>,
    input: HashMap<Vec<u8>, (u128, u128)>,
    target_rows: Option<u128>,
    target_size: u128,
    maximum: u128,
    batches: Vec<RecordBatch>,
}

impl PostponeBucketPlanner {
    pub(super) fn new(table: &Table) -> Result<Self> {
        let options = CoreOptions::new(table.schema().options());
        let target_rows = options
            .postpone_target_row_num_per_bucket()?
            .map(|n| n as u128);
        Ok(Self {
            fields: table.schema().fields().to_vec(),
            partitions: table
                .schema()
                .partition_keys()
                .iter()
                .map(|name| {
                    table
                        .schema()
                        .fields()
                        .iter()
                        .position(|f| f.name() == name)
                        .unwrap()
                })
                .collect(),
            known: HashMap::new(),
            input: HashMap::new(),
            target_rows,
            // Java ignores byte sizing when a row target is supplied.
            target_size: if target_rows.is_none() {
                options.postpone_target_size_per_bucket()? as u128
            } else {
                1
            },
            maximum: options.postpone_batch_write_fixed_bucket_max_parallelism()? as u128,
            batches: Vec::new(),
        })
    }

    pub(super) async fn load_metadata(&mut self, table: &Table, snapshot_id: i64) -> Result<()> {
        if snapshot_id == 0 {
            return Ok(());
        }
        let snapshot = table.snapshot_manager().get_snapshot(snapshot_id).await?;
        let entries = TableScan::new(table, None, vec![], None, None, None)
            .with_scan_all_files()
            .plan_manifest_entries(&snapshot)
            .await?;
        for entry in entries {
            let partition = entry.partition().to_vec();
            if entry.bucket() >= 0 {
                let count = entry.total_buckets();
                if count <= 0 || entry.bucket() >= count {
                    return Err(data_invalid(
                        "Invalid total bucket count in existing postpone files",
                    ));
                }
                if self
                    .known
                    .insert(partition, count)
                    .is_some_and(|previous| previous != count)
                {
                    return Err(data_invalid("Existing postpone files contain conflicting total bucket counts for one partition"));
                }
            }
        }
        Ok(())
    }

    pub(super) fn known_plan(&self) -> PostponeBucketPlan {
        PostponeBucketPlan::from_counts(self.known.clone())
    }

    pub(super) fn add(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        let partitions = batch_to_serialized_bytes(&batch, &self.partitions, &self.fields)?;
        let mut known_rows = Vec::new();
        let mut unknown_rows = Vec::new();
        for (row, partition) in partitions.into_iter().enumerate() {
            if self.known.contains_key(&partition) {
                known_rows.push(row);
                continue;
            }
            unknown_rows.push(row);
            let size = if self.target_rows.is_none() {
                row_size(&batch, row, &self.fields)?
            } else {
                0
            };
            let stats = self.input.entry(partition).or_default();
            stats.0 += 1;
            stats.1 += size as u128;
        }
        if unknown_rows.is_empty() {
            return Ok(Some(batch));
        }
        if known_rows.is_empty() {
            self.batches.push(batch);
            return Ok(None);
        }
        let known = super::table_write::take_rows(&batch, &known_rows)?;
        self.batches
            .push(super::table_write::take_rows(&batch, &unknown_rows)?);
        Ok(Some(known))
    }

    pub(super) fn finish(mut self) -> (PostponeBucketPlan, Vec<RecordBatch>) {
        for (partition, (rows, size)) in self.input {
            let required = match self.target_rows {
                Some(target) => rows.div_ceil(target),
                None => size.div_ceil(self.target_size),
            };
            // PostponeUtils rounds inferred counts up before applying the cap.
            let count = required.max(1).next_power_of_two().min(self.maximum) as i32;
            self.known.insert(partition, count);
        }
        (PostponeBucketPlan::from_counts(self.known), self.batches)
    }
}

fn round_word(size: usize) -> usize {
    size.div_ceil(8) * 8
}

fn row_size(batch: &RecordBatch, row: usize, fields: &[DataField]) -> Result<usize> {
    let mut size = BinaryRow::cal_fix_part_size_in_bytes(fields.len() as i32) as usize;
    for (column, field) in batch.columns().iter().zip(fields) {
        size += variable_size(column, row, field.data_type())?;
    }
    Ok(size)
}

fn binary_length(array: &ArrayRef, row: usize) -> Result<usize> {
    macro_rules! length {
        ($ty:ty) => {
            if let Some(values) = array.as_any().downcast_ref::<$ty>() {
                return Ok(values.value(row).len());
            }
        };
    }
    length!(arrow_array::BinaryArray);
    length!(arrow_array::LargeBinaryArray);
    length!(arrow_array::FixedSizeBinaryArray);
    length!(arrow_array::StringArray);
    length!(arrow_array::LargeStringArray);
    length!(arrow_array::StringViewArray);
    Err(data_invalid(
        "Expected binary or string input while sizing postpone data",
    ))
}

fn array_width(data_type: &DataType) -> usize {
    match data_type {
        DataType::Boolean(_) | DataType::TinyInt(_) => 1,
        DataType::SmallInt(_) => 2,
        DataType::Int(_) | DataType::Float(_) | DataType::Date(_) | DataType::Time(_) => 4,
        _ => 8,
    }
}

fn array_size(values: &ArrayRef, data_type: &DataType) -> Result<usize> {
    let count = values.len();
    let mut size = round_word(4 + count.div_ceil(32) * 4 + count * array_width(data_type));
    for row in 0..count {
        if !values.is_null(row) {
            size += variable_size(values, row, data_type)?;
        }
    }
    Ok(size)
}

fn variable_size(array: &ArrayRef, row: usize, data_type: &DataType) -> Result<usize> {
    // Decimal and non-compact timestamp writers reserve their variable slot
    // even for NULL, as Java BinaryRowWriter does.
    if let DataType::Decimal(decimal) = data_type {
        return Ok(if decimal.precision() > 18 { 16 } else { 0 });
    }
    if let DataType::Timestamp(timestamp) = data_type {
        return Ok(if timestamp.precision() > 3 { 8 } else { 0 });
    }
    if let DataType::LocalZonedTimestamp(timestamp) = data_type {
        return Ok(if timestamp.precision() > 3 { 8 } else { 0 });
    }
    if array.is_null(row) {
        return Ok(0);
    }
    match data_type {
        DataType::Blob(_)
        | DataType::Binary(_)
        | DataType::VarBinary(_)
        | DataType::Char(_)
        | DataType::VarChar(_) => {
            let length = binary_length(array, row)?;
            Ok(if length <= 7 { 0 } else { round_word(length) })
        }
        DataType::Row(row_type) => {
            let values = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| data_invalid("Expected ROW input while sizing postpone data"))?;
            let mut size =
                BinaryRow::cal_fix_part_size_in_bytes(row_type.fields().len() as i32) as usize;
            for (value, field) in values.columns().iter().zip(row_type.fields()) {
                size += variable_size(value, row, field.data_type())?;
            }
            Ok(round_word(size))
        }
        DataType::Array(element) => {
            let values = if let Some(values) = array.as_any().downcast_ref::<ListArray>() {
                values.value(row)
            } else if let Some(values) = array.as_any().downcast_ref::<LargeListArray>() {
                values.value(row)
            } else if let Some(values) = array.as_any().downcast_ref::<FixedSizeListArray>() {
                values.value(row)
            } else {
                return Err(data_invalid(
                    "Expected ARRAY input while sizing postpone data",
                ));
            };
            Ok(round_word(array_size(&values, element.element_type())?))
        }
        DataType::Map(map) => map_size(array, row, map.key_type(), map.value_type()),
        DataType::Multiset(multiset) => map_size(
            array,
            row,
            multiset.element_type(),
            &DataType::Int(crate::spec::IntType::new()),
        ),
        DataType::Vector(vector) => {
            let values = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| data_invalid("Expected VECTOR input while sizing postpone data"))?;
            Ok(round_word(
                4 + values.value(row).len() * array_width(vector.element_type()),
            ))
        }
        _ => {
            // Reuse the existing Java BinaryRow encoder for scalar layouts,
            // including inline strings/BINARY and Variant serialization.
            if matches!(
                array.data_type(),
                ArrowType::Boolean
                    | ArrowType::Int8
                    | ArrowType::Int16
                    | ArrowType::Int32
                    | ArrowType::Int64
                    | ArrowType::Float32
                    | ArrowType::Float64
                    | ArrowType::Date32
                    | ArrowType::Time32(_)
            ) {
                return Ok(0);
            }
            let field = DataField::new(0, "value".into(), data_type.clone());
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![arrow_schema::Field::new(
                    "value",
                    array.data_type().clone(),
                    true,
                )])),
                vec![array.slice(row, 1)],
            )
            .map_err(|e| data_invalid(e.to_string()))?;
            let encoded = batch_to_serialized_bytes(&batch, &[0], &[field])?;
            Ok(encoded[0].len() - 4 - BinaryRow::cal_fix_part_size_in_bytes(1) as usize)
        }
    }
}

fn map_size(array: &ArrayRef, row: usize, key: &DataType, value: &DataType) -> Result<usize> {
    let entries = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| data_invalid("Expected MAP input while sizing postpone data"))?
        .value(row);
    Ok(round_word(
        4 + array_size(entries.column(0), key)? + array_size(entries.column(1), value)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{BlobType, DecimalType};
    use crate::table::table_write::tests::{
        make_batch, setup_dirs, test_file_io, test_postpone_pk_table,
    };
    use arrow_array::{Decimal128Array, LargeBinaryArray};

    #[test]
    fn binary_row_sizing_handles_blob_and_null_array_values() {
        let blob: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
            Some(b"123456789".as_slice()),
            None,
        ]));
        let blob_type = DataType::Blob(BlobType::new());
        assert_eq!(variable_size(&blob, 0, &blob_type).unwrap(), 16);
        assert_eq!(variable_size(&blob, 1, &blob_type).unwrap(), 0);
        let decimal = DataType::Decimal(DecimalType::new(38, 0).unwrap());
        let values: ArrayRef = Arc::new(
            Decimal128Array::from(vec![None])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        );
        assert_eq!(variable_size(&values, 0, &decimal).unwrap(), 16);
        assert_eq!(array_size(&values, &decimal).unwrap(), 16);
        let values: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(123i128)])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        );
        assert_eq!(array_size(&values, &decimal).unwrap(), 32);
    }

    #[tokio::test]
    async fn inference_excludes_old_pending_files_and_streams_known_partitions() {
        let io = test_file_io();
        let path = "memory:/auto_postpone_pending";
        setup_dirs(&io, path).await;
        let table = test_postpone_pk_table(&io, path).copy_with_options(HashMap::from([
            ("postpone.target-row-num-per-bucket".into(), "1".into()),
            (
                "postpone.batch-write-fixed-bucket.max-parallelism".into(),
                "8".into(),
            ),
        ]));
        let pending = table.new_write_builder();
        let mut writer = pending.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![1, 2, 3, 4], vec![10, 20, 30, 40]))
            .await
            .unwrap();
        pending
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        let mut planner = PostponeBucketPlanner::new(&table).unwrap();
        planner.load_metadata(&table, 1).await.unwrap();
        planner.add(make_batch(vec![5], vec![50])).unwrap();
        let (plan, batches) = planner.finish();
        assert_eq!(
            plan.total_buckets(&crate::spec::EMPTY_SERIALIZED_ROW),
            Some(1)
        );
        assert_eq!(batches.len(), 1);
        let mut planner = PostponeBucketPlanner::new(&table).unwrap();
        planner
            .known
            .insert(crate::spec::EMPTY_SERIALIZED_ROW.clone(), 3);
        let known = planner
            .add(make_batch(vec![6, 7], vec![60, 70]))
            .unwrap()
            .unwrap();
        assert_eq!(known.num_rows(), 2);
        assert!(
            planner.batches.is_empty(),
            "known partitions must not retain the whole input"
        );
    }

    #[test]
    fn size_inference_rounds_up_then_caps_and_row_target_takes_precedence() {
        let io = test_file_io();
        let table = test_postpone_pk_table(&io, "memory:/auto_postpone_size").copy_with_options(
            HashMap::from([
                ("postpone.target-size-per-bucket".into(), "20 b".into()),
                (
                    "postpone.batch-write-fixed-bucket.max-parallelism".into(),
                    "3".into(),
                ),
            ]),
        );
        let mut planner = PostponeBucketPlanner::new(&table).unwrap();
        planner
            .add(make_batch(vec![1, 2, 3], vec![10, 20, 30]))
            .unwrap();
        let (plan, _) = planner.finish();
        assert_eq!(
            plan.total_buckets(&crate::spec::EMPTY_SERIALIZED_ROW),
            Some(3)
        );
        let table = table.copy_with_options(HashMap::from([
            ("postpone.target-row-num-per-bucket".into(), "2".into()),
            ("postpone.target-size-per-bucket".into(), "invalid".into()),
        ]));
        let mut planner = PostponeBucketPlanner::new(&table).unwrap();
        planner
            .add(make_batch(vec![1, 2, 3], vec![10, 20, 30]))
            .unwrap();
        assert_eq!(
            planner
                .finish()
                .0
                .total_buckets(&crate::spec::EMPTY_SERIALIZED_ROW),
            Some(2)
        );
    }
}
