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

//! Pure input statistics and bucket inference for postpone batch writes.

use super::postpone_bucket_plan::{data_invalid, serialized_partition_columns};
use crate::spec::{
    batch_to_serialized_bytes, BinaryRow, BucketFunctionType, CoreOptions, DataField, DataType,
    TableSchema, POSTPONE_BUCKET,
};
use crate::table::{PostponeBucketPlan, Table};
use crate::Result;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Int64Array, LargeListArray, ListArray, MapArray,
    RecordBatch, StructArray,
};
use arrow_schema::{DataType as ArrowType, Field, Schema};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub const POSTPONE_STATISTICS_ROW_COUNT_FIELD: &str = "row_count";
pub const POSTPONE_STATISTICS_DATA_SIZE_FIELD: &str = "data_size";

type PartitionStatistics = HashMap<Vec<u8>, (u128, u128)>;

/// Owns schema and bucket counts without a table, runtime or retained input batches.
/// Workers can collect statistics independently and the caller can concatenate
/// them before planning. Existing partition layouts always take precedence.
pub struct PostponeBucketPlanner {
    fields: Vec<DataField>,
    partition_fields: Vec<DataField>,
    known: PostponeBucketPlan,
    default_bucket_num: Option<i32>,
    target_rows: Option<u128>,
    target_size: u128,
    maximum: u128,
}

impl PostponeBucketPlanner {
    pub fn new(schema: &TableSchema, known: PostponeBucketPlan) -> Result<Self> {
        let options = CoreOptions::new(schema.options());
        if options.bucket() != POSTPONE_BUCKET {
            return Err(data_invalid(
                "Postpone bucket planning requires bucket = -2",
            ));
        }
        if options.bucket_function_type()? != BucketFunctionType::Default {
            return Err(data_invalid(
                "Postpone bucket planning only supports bucket-function.type=default",
            ));
        }
        let partition_fields = schema.partition_fields();
        if partition_fields.len() != schema.partition_keys().len() {
            return Err(data_invalid(
                "Postpone partition fields are missing from the table schema",
            ));
        }
        let mut names = HashSet::new();
        if schema
            .fields()
            .iter()
            .any(|field| !names.insert(field.name()))
        {
            return Err(data_invalid(
                "Postpone table schema contains duplicate field names",
            ));
        }
        if known
            .explicit_bucket_counts()
            .values()
            .any(|count| *count <= 0)
        {
            return Err(data_invalid(
                "Postpone known bucket counts must be positive",
            ));
        }
        let partitions = known
            .explicit_bucket_counts()
            .keys()
            .map(Vec::as_slice)
            .collect::<Vec<_>>();
        serialized_partition_columns(&partition_fields, &partitions)?;
        let default_bucket_num = options.postpone_default_bucket_num()?;
        // Java and Python ignore sizing options when an exact default is set,
        // and ignore the byte target when rows determine the count.
        let (target_rows, target_size, maximum) = if default_bucket_num.is_some() {
            (None, 1, 1)
        } else {
            let target_rows = options
                .postpone_target_row_num_per_bucket()?
                .map(|n| n as u128);
            let target_size = if target_rows.is_none() {
                options.postpone_target_size_per_bucket()? as u128
            } else {
                1
            };
            (
                target_rows,
                target_size,
                options.postpone_batch_write_fixed_bucket_max_parallelism()? as u128,
            )
        };
        Ok(Self {
            fields: schema.fields().to_vec(),
            partition_fields,
            known,
            default_bucket_num,
            target_rows,
            target_size,
            maximum,
        })
    }

    pub fn current_plan(&self) -> PostponeBucketPlan {
        self.known.clone()
    }

    /// Return partition columns followed positionally by Int64 `row_count`
    /// and `data_size`. Partition names may equal either statistics name.
    /// Only the supplied columns contribute to Java BinaryRow sizing.
    pub fn input_statistics(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let stats = self.collect_input(batch)?;
        let mut entries = stats.into_iter().collect::<Vec<_>>();
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let partitions = entries
            .iter()
            .map(|(partition, _)| partition.as_slice())
            .collect::<Vec<_>>();
        let mut columns = serialized_partition_columns(&self.partition_fields, &partitions)?;
        for component in [0, 1] {
            let values = entries
                .iter()
                .map(|(_, (rows, bytes))| {
                    i64::try_from(if component == 0 { *rows } else { *bytes }).map_err(|_| {
                        data_invalid("Postpone input statistics exceed Int64 capacity")
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            columns.push(Arc::new(Int64Array::from(values)));
        }
        RecordBatch::try_new(self.statistics_schema()?, columns)
            .map_err(|error| data_invalid(error.to_string()))
    }

    /// Aggregate worker statistics before inferring each new partition once.
    /// A rejected batch leaves all existing and inferred counts unchanged.
    pub fn plan(&mut self, statistics: &RecordBatch) -> Result<PostponeBucketPlan> {
        let expected = self.statistics_schema()?;
        if statistics.num_columns() != expected.fields().len()
            || !expected
                .fields()
                .iter()
                .zip(statistics.schema().fields())
                .all(|(expected, actual)| {
                    expected.name() == actual.name() && expected.data_type() == actual.data_type()
                })
        {
            return Err(data_invalid("Postpone statistics must contain the partition columns followed by Int64 row_count and data_size"));
        }
        self.validate_partition_nulls(
            statistics,
            &(0..self.partition_fields.len()).collect::<Vec<_>>(),
        )?;
        let rows = statistics
            .column(self.partition_fields.len())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let bytes = statistics
            .column(self.partition_fields.len() + 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let partitions = batch_to_serialized_bytes(
            statistics,
            &(0..self.partition_fields.len()).collect::<Vec<_>>(),
            &self.partition_fields,
        )?;
        let mut totals = PartitionStatistics::new();
        for (row, partition) in partitions.into_iter().enumerate() {
            if rows.is_null(row)
                || bytes.is_null(row)
                || rows.value(row) < 0
                || bytes.value(row) < 0
            {
                return Err(data_invalid(format!("Postpone row_count and data_size must be non-null and nonnegative at row {row}")));
            }
            if self.known.total_buckets(&partition).is_none() {
                add_statistics(
                    &mut totals,
                    partition,
                    rows.value(row) as u128,
                    bytes.value(row) as u128,
                )?;
            }
        }
        self.apply_statistics(totals);
        Ok(self.current_plan())
    }

    fn statistics_schema(&self) -> Result<Arc<Schema>> {
        let mut fields = crate::arrow::build_target_arrow_schema(&self.partition_fields)?
            .fields()
            .to_vec();
        fields.extend([
            Arc::new(Field::new(
                POSTPONE_STATISTICS_ROW_COUNT_FIELD,
                ArrowType::Int64,
                false,
            )),
            Arc::new(Field::new(
                POSTPONE_STATISTICS_DATA_SIZE_FIELD,
                ArrowType::Int64,
                false,
            )),
        ]);
        Ok(Arc::new(Schema::new(fields)))
    }

    fn input_fields(&self, batch: &RecordBatch) -> Result<Vec<DataField>> {
        let mut names = HashSet::new();
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| {
                if !names.insert(field.name().clone()) {
                    return Err(data_invalid(format!(
                        "Duplicate postpone input column '{}'",
                        field.name()
                    )));
                }
                self.fields
                    .iter()
                    .find(|candidate| candidate.name() == field.name())
                    .cloned()
                    .ok_or_else(|| {
                        data_invalid(format!("Unknown postpone input column '{}'", field.name()))
                    })
            })
            .collect()
    }

    fn input_partitions(&self, batch: &RecordBatch, fields: &[DataField]) -> Result<Vec<Vec<u8>>> {
        let indices = self
            .partition_fields
            .iter()
            .map(|field| {
                fields
                    .iter()
                    .position(|candidate| candidate.name() == field.name())
                    .ok_or_else(|| {
                        data_invalid(format!(
                            "Missing postpone partition column '{}'",
                            field.name()
                        ))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        self.validate_partition_nulls(batch, &indices)?;
        // Writers accept both Arrow BINARY layouts. The BinaryRow encoder
        // expects variable binary, while sizing still uses the original input.
        let mut columns = batch.columns().to_vec();
        let mut arrow_fields = batch.schema().fields().to_vec();
        let mut normalized = false;
        for &index in &indices {
            if matches!(columns[index].data_type(), ArrowType::FixedSizeBinary(_)) {
                let expected = crate::arrow::build_target_arrow_schema(&[fields[index].clone()])?;
                columns[index] = super::write_batch_normalize::normalize_write_array(
                    &columns[index],
                    expected.field(0).data_type(),
                )?;
                arrow_fields[index] = Arc::new(
                    arrow_fields[index]
                        .as_ref()
                        .clone()
                        .with_data_type(columns[index].data_type().clone()),
                );
                normalized = true;
            }
        }
        if normalized {
            let normalized = RecordBatch::try_new(Arc::new(Schema::new(arrow_fields)), columns)
                .map_err(|error| data_invalid(error.to_string()))?;
            batch_to_serialized_bytes(&normalized, &indices, fields)
        } else {
            batch_to_serialized_bytes(batch, &indices, fields)
        }
    }

    fn validate_partition_nulls(&self, batch: &RecordBatch, indices: &[usize]) -> Result<()> {
        for (field, index) in self.partition_fields.iter().zip(indices) {
            if !field.data_type().is_nullable() && batch.column(*index).null_count() != 0 {
                return Err(data_invalid(format!(
                    "Postpone partition column '{}' is NOT NULL but contains null values",
                    field.name()
                )));
            }
        }
        Ok(())
    }

    fn collect_input(&self, batch: &RecordBatch) -> Result<PartitionStatistics> {
        let fields = self.input_fields(batch)?;
        let partitions = self.input_partitions(batch, &fields)?;
        let mut stats = PartitionStatistics::new();
        for (row, partition) in partitions.into_iter().enumerate() {
            if self.known.total_buckets(&partition).is_some() {
                continue;
            }
            let size = if self.default_bucket_num.is_none() && self.target_rows.is_none() {
                row_size(batch, row, &fields)? as u128
            } else {
                0
            };
            add_statistics(&mut stats, partition, 1, size)?;
        }
        Ok(stats)
    }

    fn apply_statistics(&mut self, statistics: PartitionStatistics) {
        for (partition, (rows, size)) in statistics {
            if self.known.total_buckets(&partition).is_some() {
                continue;
            }
            let count = self.default_bucket_num.unwrap_or_else(|| {
                let required = match self.target_rows {
                    Some(target) => rows.div_ceil(target),
                    None => size.div_ceil(self.target_size),
                }
                .max(1);
                // Capping before rounding avoids overflow while preserving
                // Java's round-up-then-cap result, including non-power-of-two caps.
                if required >= self.maximum {
                    self.maximum as i32
                } else {
                    required.next_power_of_two().min(self.maximum) as i32
                }
            });
            self.known.insert(partition, count);
        }
    }
}

fn add_statistics(
    stats: &mut PartitionStatistics,
    partition: Vec<u8>,
    rows: u128,
    size: u128,
) -> Result<()> {
    let current = stats.entry(partition).or_default();
    current.0 = current
        .0
        .checked_add(rows)
        .ok_or_else(|| data_invalid("Postpone row count overflow"))?;
    current.1 = current
        .1
        .checked_add(size)
        .ok_or_else(|| data_invalid("Postpone data size overflow"))?;
    Ok(())
}

/// The automatic writer separately owns batches pending their final layout.
pub(super) struct BufferedPostponeBucketPlanner {
    planner: PostponeBucketPlanner,
    input: PartitionStatistics,
    batches: Vec<RecordBatch>,
}

impl BufferedPostponeBucketPlanner {
    pub(super) fn new(table: &Table) -> Result<Self> {
        Ok(Self {
            planner: PostponeBucketPlanner::new(table.schema(), PostponeBucketPlan::default())?,
            input: PartitionStatistics::new(),
            batches: Vec::new(),
        })
    }

    pub(super) async fn load_metadata(&mut self, table: &Table, snapshot_id: i64) -> Result<()> {
        self.planner
            .known
            .load_existing_buckets(table, snapshot_id)
            .await
    }

    pub(super) fn known_plan(&self) -> PostponeBucketPlan {
        self.planner.current_plan()
    }

    pub(super) fn add(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        // Normalization appends _VALUE_KIND to PK batches. Statistics describe
        // the logical row while the buffered write retains the event column.
        let logical_indices = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| {
                (field.name() != crate::spec::VALUE_KIND_FIELD_NAME).then_some(index)
            })
            .collect::<Vec<_>>();
        let logical = batch
            .project(&logical_indices)
            .map_err(|error| data_invalid(error.to_string()))?;
        let fields = self.planner.input_fields(&logical)?;
        let partitions = self.planner.input_partitions(&logical, &fields)?;
        let stats = self.planner.collect_input(&logical)?;
        for (partition, (rows, size)) in stats {
            add_statistics(&mut self.input, partition, rows, size)?;
        }
        let mut known_rows = Vec::new();
        let mut unknown_rows = Vec::new();
        for (row, partition) in partitions.into_iter().enumerate() {
            if self.planner.known.total_buckets(&partition).is_some() {
                known_rows.push(row);
            } else {
                unknown_rows.push(row);
            }
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
        self.planner.apply_statistics(self.input);
        (self.planner.current_plan(), self.batches)
    }
}

fn checked_size(size: Option<usize>) -> Result<usize> {
    size.ok_or_else(|| data_invalid("Postpone BinaryRow size overflow"))
}

fn round_word(size: usize) -> Result<usize> {
    Ok(checked_size(size.checked_add(7))? / 8 * 8)
}

fn row_size(batch: &RecordBatch, row: usize, fields: &[DataField]) -> Result<usize> {
    let mut size = BinaryRow::cal_fix_part_size_in_bytes(fields.len() as i32) as usize;
    for (column, field) in batch.columns().iter().zip(fields) {
        size = checked_size(size.checked_add(variable_size(column, row, field.data_type())?))?;
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
    let nulls = checked_size(count.div_ceil(32).checked_mul(4))?;
    let values_size = checked_size(count.checked_mul(array_width(data_type)))?;
    let fixed = checked_size(checked_size(4usize.checked_add(nulls))?.checked_add(values_size))?;
    let mut size = round_word(fixed)?;
    for row in 0..count {
        if !values.is_null(row) {
            size = checked_size(size.checked_add(variable_size(values, row, data_type)?))?;
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
            if length <= 7 {
                Ok(0)
            } else {
                round_word(length)
            }
        }
        DataType::Row(row_type) => {
            let values = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| data_invalid("Expected ROW input while sizing postpone data"))?;
            let mut size =
                BinaryRow::cal_fix_part_size_in_bytes(row_type.fields().len() as i32) as usize;
            if values.num_columns() != row_type.fields().len() {
                return Err(data_invalid(
                    "ROW input fields do not match the postpone schema",
                ));
            }
            let mut names = HashSet::new();
            for (value, field) in values.columns().iter().zip(values.fields()) {
                if !names.insert(field.name().clone()) {
                    return Err(data_invalid(
                        "Duplicate ROW input field while sizing postpone data",
                    ));
                }
                let field = row_type
                    .fields()
                    .iter()
                    .find(|candidate| candidate.name() == field.name())
                    .ok_or_else(|| {
                        data_invalid("Unknown ROW input field while sizing postpone data")
                    })?;
                size =
                    checked_size(size.checked_add(variable_size(value, row, field.data_type())?))?;
            }
            round_word(size)
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
            round_word(array_size(&values, element.element_type())?)
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
            let payload = checked_size(
                values
                    .value(row)
                    .len()
                    .checked_mul(array_width(vector.element_type())),
            )?;
            round_word(checked_size(4usize.checked_add(payload))?)
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
    let keys = array_size(entries.column(0), key)?;
    let values = array_size(entries.column(1), value)?;
    round_word(checked_size(
        checked_size(4usize.checked_add(keys))?.checked_add(values),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        ArrayType, BinaryType, BlobType, DecimalType, FloatType, IntType, MapType, RowType,
        TimestampType, VarCharType, VectorType,
    };
    use crate::table::table_write::tests::{
        make_batch, setup_dirs, test_file_io, test_postpone_pk_table,
    };
    use arrow_array::builder::{Int32Builder, MapBuilder, StringBuilder};
    use arrow_array::{
        BinaryArray, Decimal128Array, FixedSizeBinaryArray, Float32Array, Int32Array,
        LargeBinaryArray, StringArray, TimestampMicrosecondArray,
    };

    fn planner_schema(options: &[(&str, &str)]) -> TableSchema {
        let mut builder = crate::spec::Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("pt", DataType::VarChar(VarCharType::string_type()))
            .column("payload", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["pt", "id"])
            .partition_keys(["pt"])
            .option("primary-key.nullable", "true")
            .option("bucket", "-2");
        for (key, value) in options {
            builder = builder.option(*key, *value);
        }
        TableSchema::new(0, &builder.build().unwrap())
    }

    fn input(partitions: Vec<Option<&str>>, payloads: Vec<Option<&str>>) -> RecordBatch {
        // Deliberately reorder and project out id: the full schema is not the input layout.
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("payload", ArrowType::Utf8, true),
                Field::new("pt", ArrowType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(payloads)),
                Arc::new(StringArray::from(partitions)),
            ],
        )
        .unwrap()
    }

    fn statistics(
        schema: &TableSchema,
        partitions: Vec<Option<&str>>,
        rows: Vec<Option<i64>>,
        sizes: Vec<Option<i64>>,
    ) -> RecordBatch {
        let mut fields = crate::arrow::build_target_arrow_schema(&schema.partition_fields())
            .unwrap()
            .fields()
            .to_vec();
        fields.extend([
            Arc::new(Field::new(
                POSTPONE_STATISTICS_ROW_COUNT_FIELD,
                ArrowType::Int64,
                true,
            )),
            Arc::new(Field::new(
                POSTPONE_STATISTICS_DATA_SIZE_FIELD,
                ArrowType::Int64,
                true,
            )),
        ]);
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(StringArray::from(partitions)),
                Arc::new(Int64Array::from(rows)),
                Arc::new(Int64Array::from(sizes)),
            ],
        )
        .unwrap()
    }

    fn counts(schema: &TableSchema, plan: &PostponeBucketPlan) -> HashMap<Option<String>, i32> {
        let batch = plan.to_arrow(schema).unwrap();
        let partitions = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|row| {
                (
                    if partitions.is_null(row) {
                        None
                    } else {
                        Some(partitions.value(row).to_string())
                    },
                    counts.value(row),
                )
            })
            .collect()
    }

    #[test]
    fn input_statistics_size_only_actual_columns_and_leave_plan_unchanged() {
        let schema = planner_schema(&[("postpone.target-size-per-bucket", "20 b")]);
        let known = PostponeBucketPlan::from_arrow(
            &schema,
            &RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("pt", ArrowType::Utf8, true),
                    Field::new("total_buckets", ArrowType::Int32, false),
                ])),
                vec![
                    Arc::new(StringArray::from(vec!["old"])),
                    Arc::new(Int32Array::from(vec![3])),
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let mut planner = PostponeBucketPlanner::new(&schema, known).unwrap();
        let batch = input(
            vec![Some("new"), Some("old"), Some("new"), None],
            vec![
                Some("12345678"),
                Some("ignored bytes"),
                Some("1234567"),
                None,
            ],
        );
        let stats = planner.input_statistics(&batch).unwrap();
        let pts = stats
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let rows = stats
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let bytes = stats
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = (0..stats.num_rows())
            .map(|row| {
                (
                    if pts.is_null(row) {
                        None
                    } else {
                        Some(pts.value(row).to_string())
                    },
                    (rows.value(row), bytes.value(row)),
                )
            })
            .collect::<HashMap<_, _>>();
        // Two-field BinaryRows have 24 fixed bytes; eight-byte text needs another word.
        assert_eq!(
            values,
            HashMap::from([(Some("new".into()), (2, 56)), (None, (1, 24))])
        );
        assert_eq!(
            counts(&schema, &planner.current_plan()),
            HashMap::from([(Some("old".into()), 3)])
        );
        assert_eq!(
            counts(&schema, &planner.plan(&stats).unwrap()),
            HashMap::from([(Some("old".into()), 3), (Some("new".into()), 4), (None, 2),])
        );
        assert_eq!(planner.input_statistics(&batch).unwrap().num_rows(), 0);
    }

    #[test]
    fn merged_worker_statistics_are_inferred_once_and_keep_known_counts() {
        let schema = planner_schema(&[("postpone.target-row-num-per-bucket", "2")]);
        let mut planner =
            PostponeBucketPlanner::new(&schema, PostponeBucketPlan::default()).unwrap();
        let worker = planner
            .input_statistics(&input(
                vec![Some("p"), Some("p"), None],
                vec![None, None, None],
            ))
            .unwrap();
        let merged =
            arrow_select::concat::concat_batches(&worker.schema(), [&worker, &worker, &worker])
                .unwrap();
        let first = planner.plan(&merged).unwrap();
        assert_eq!(
            counts(&schema, &first),
            HashMap::from([(Some("p".into()), 4), (None, 2)])
        );
        let repeated = statistics(
            &schema,
            vec![Some("p"), Some("q")],
            vec![Some(100), Some(0)],
            vec![Some(0), Some(0)],
        );
        assert_eq!(
            counts(&schema, &planner.plan(&repeated).unwrap()),
            HashMap::from([(Some("p".into()), 4), (None, 2), (Some("q".into()), 1),])
        );
        assert_eq!(
            counts(&schema, &first).len(),
            2,
            "previous plans own their counts"
        );
    }

    #[test]
    fn fixed_binary_partition_statistics_preserve_slices_nulls_and_routing() {
        let schema = crate::spec::Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("pt", DataType::Binary(BinaryType::new(8).unwrap()))
            .column("payload", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["pt", "id"])
            .partition_keys(["pt"])
            .option("primary-key.nullable", "true")
            .option("bucket", "-2")
            .option("postpone.target-size-per-bucket", "32 b")
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        let partition = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [
                Some(b"discard!".as_slice()),
                None,
                Some(b"12345678".as_slice()),
                Some(b"12345678".as_slice()),
                Some(b"outside!".as_slice()),
            ]
            .into_iter(),
            8,
        )
        .unwrap();
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("payload", ArrowType::Utf8, true),
                Field::new("pt", ArrowType::FixedSizeBinary(8), true),
                Field::new("id", ArrowType::Int32, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["123456789"; 5])),
                Arc::new(partition),
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])),
            ],
        )
        .unwrap()
        .slice(1, 3);
        let canonical = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("payload", ArrowType::Utf8, true),
                Field::new("pt", ArrowType::Binary, true),
                Field::new("id", ArrowType::Int32, false),
            ])),
            vec![
                input.column(0).clone(),
                Arc::new(BinaryArray::from(vec![
                    None,
                    Some(b"12345678".as_slice()),
                    Some(b"12345678".as_slice()),
                ])),
                input.column(2).clone(),
            ],
        )
        .unwrap();
        let mut planner =
            PostponeBucketPlanner::new(&schema, PostponeBucketPlan::default()).unwrap();
        let actual = planner.input_statistics(&input).unwrap();
        assert_eq!(actual, planner.input_statistics(&canonical).unwrap());
        let partitions = actual
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let sizes = actual
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let expected_sizes = (0..actual.num_rows())
            .map(|row| {
                (
                    if partitions.is_null(row) {
                        None
                    } else {
                        Some(partitions.value(row).to_vec())
                    },
                    sizes.value(row),
                )
            })
            .collect::<HashMap<_, _>>();
        // Three logical columns: 32 fixed bytes plus 16 payload bytes;
        // only non-NULL eight-byte partition values need an additional word.
        assert_eq!(
            expected_sizes,
            HashMap::from([(None, 48), (Some(b"12345678".to_vec()), 112)])
        );
        let plan = planner.plan(&actual).unwrap();
        let exported = plan.to_arrow(&schema).unwrap();
        let counts = exported
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let pts = exported
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for row in 0..exported.num_rows() {
            assert_eq!(counts.value(row), if pts.is_null(row) { 2 } else { 4 });
        }
        let router = crate::table::BucketRouter::new(&schema)
            .unwrap()
            .with_bucket_plan(plan)
            .unwrap();
        assert_eq!(
            router.route(&input).unwrap(),
            router.route(&canonical).unwrap()
        );
    }

    #[test]
    fn inference_preserves_default_and_row_precedence_and_caps_large_aggregates() {
        let schema = planner_schema(&[
            ("postpone.default-bucket-num", "7"),
            (
                "postpone.batch-write-fixed-bucket.max-parallelism",
                "invalid",
            ),
            ("postpone.target-row-num-per-bucket", "0"),
            ("postpone.target-size-per-bucket", "invalid"),
        ]);
        let mut planner =
            PostponeBucketPlanner::new(&schema, PostponeBucketPlan::default()).unwrap();
        let stats = planner
            .input_statistics(&input(vec![Some("a")], vec![Some("long payload")]))
            .unwrap();
        assert_eq!(
            stats
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
        assert_eq!(
            counts(&schema, &planner.plan(&stats).unwrap()),
            HashMap::from([(Some("a".into()), 7)])
        );

        let schema = planner_schema(&[
            ("postpone.target-row-num-per-bucket", "2"),
            ("postpone.target-size-per-bucket", "invalid"),
            ("postpone.batch-write-fixed-bucket.max-parallelism", "3"),
        ]);
        let mut planner =
            PostponeBucketPlanner::new(&schema, PostponeBucketPlan::default()).unwrap();
        let stats = statistics(
            &schema,
            vec![Some("a"), Some("b"), Some("large"), Some("large")],
            vec![Some(2), Some(5), Some(i64::MAX), Some(i64::MAX)],
            vec![Some(0); 4],
        );
        assert_eq!(
            counts(&schema, &planner.plan(&stats).unwrap()),
            HashMap::from([
                (Some("a".into()), 1),
                (Some("b".into()), 3),
                (Some("large".into()), 3),
            ])
        );
        assert!(round_word(usize::MAX).is_err());
        let mut totals = HashMap::from([(vec![], (u128::MAX, 0))]);
        assert!(add_statistics(&mut totals, vec![], 1, 0).is_err());
    }

    #[test]
    fn statistics_suffixes_are_positional_when_partition_names_collide() {
        let schema = crate::spec::Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("row_count", DataType::Int(IntType::new()))
            .column("data_size", DataType::Int(IntType::new()))
            .primary_key(["id", "row_count", "data_size"])
            .partition_keys(["row_count", "data_size"])
            .option("bucket", "-2")
            .option("postpone.target-row-num-per-bucket", "1")
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        let mut planner =
            PostponeBucketPlanner::new(&schema, PostponeBucketPlan::default()).unwrap();
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("data_size", ArrowType::Int32, false),
                Field::new("row_count", ArrowType::Int32, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![20, 20, 20])),
                Arc::new(Int32Array::from(vec![10, 10, 10])),
            ],
        )
        .unwrap();
        let stats = planner.input_statistics(&input).unwrap();
        assert_eq!(
            stats
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["row_count", "data_size", "row_count", "data_size"]
        );
        assert_eq!(
            stats
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            10
        );
        let plan = planner.plan(&stats).unwrap().to_arrow(&schema).unwrap();
        assert_eq!(
            plan.column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            4
        );
    }

    #[test]
    fn invalid_input_and_statistics_are_rejected_before_mutating_counts() {
        let schema = planner_schema(&[("postpone.target-row-num-per-bucket", "1")]);
        let mut planner =
            PostponeBucketPlanner::new(&schema, PostponeBucketPlan::default()).unwrap();
        let batch = input(vec![Some("a")], vec![None]);
        for names in [["unknown", "pt"], ["pt", "pt"], ["payload", "id"]] {
            let invalid = RecordBatch::try_new(
                Arc::new(Schema::new(
                    names
                        .map(|name| Field::new(name, ArrowType::Utf8, true))
                        .to_vec(),
                )),
                batch.columns().to_vec(),
            )
            .unwrap();
            assert!(planner.input_statistics(&invalid).is_err());
        }
        for (rows, sizes) in [
            (vec![Some(1), Some(-1)], vec![Some(0), Some(0)]),
            (vec![Some(1), None], vec![Some(0), Some(0)]),
            (vec![Some(1), Some(1)], vec![Some(0), Some(-1)]),
            (vec![Some(1), Some(1)], vec![Some(0), None]),
        ] {
            let stats = statistics(&schema, vec![Some("a"), Some("b")], rows, sizes);
            assert!(planner.plan(&stats).is_err());
            assert_eq!(
                planner.current_plan().to_arrow(&schema).unwrap().num_rows(),
                0
            );
        }
        let conflict = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("pt", ArrowType::Utf8, true),
                Field::new("total_buckets", ArrowType::Int32, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])),
                Arc::new(Int32Array::from(vec![2, 3])),
            ],
        )
        .unwrap();
        assert!(PostponeBucketPlan::from_arrow(&schema, &conflict).is_err());
    }

    #[test]
    fn complex_input_size_matches_java_binary_layout() {
        let string = DataType::VarChar(VarCharType::string_type());
        let integer = DataType::Int(IntType::new());
        let nested = DataType::Row(RowType::new(vec![
            DataField::new(20, "tiny".into(), integer.clone()),
            DataField::new(21, "label".into(), string.clone()),
        ]));
        let schema = crate::spec::Schema::builder()
            .column("id", integer.clone())
            .column("payload", string.clone())
            .column("blob", DataType::Blob(BlobType::new()))
            .column(
                "decimal",
                DataType::Decimal(DecimalType::new(38, 0).unwrap()),
            )
            .column("ts", DataType::Timestamp(TimestampType::new(6).unwrap()))
            .column("items", DataType::Array(ArrayType::new(string.clone())))
            .column("pairs", DataType::Map(MapType::new(string, integer)))
            .column("nested", nested)
            .column(
                "vector",
                DataType::Vector(VectorType::new(3, DataType::Float(FloatType::new())).unwrap()),
            )
            .primary_key(["id"])
            .option("bucket", "-2")
            .build()
            .unwrap();
        let schema = TableSchema::new(0, &schema);
        let mut list = arrow_array::builder::ListBuilder::new(StringBuilder::new());
        list.values().append_value("1234567");
        list.values().append_value("12345678");
        list.values().append_null();
        list.append(true);
        let mut map = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        map.keys().append_value("12345678");
        map.values().append_value(1);
        map.append(true).unwrap();
        let nested = StructArray::from(vec![
            (
                Arc::new(Field::new("label", ArrowType::Utf8, true)),
                Arc::new(StringArray::from(vec!["123456789"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("tiny", ArrowType::Int32, true)),
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            ),
        ]);
        let vector = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", ArrowType::Float32, true)),
            3,
            Arc::new(Float32Array::from(vec![1., 2., 3.])),
            None,
        )
        .unwrap();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["12345678"])),
            Arc::new(LargeBinaryArray::from(vec![Some(b"123456789".as_slice())])),
            Arc::new(
                Decimal128Array::from(vec![None])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(TimestampMicrosecondArray::from(vec![None])),
            Arc::new(list.finish()),
            Arc::new(map.finish()),
            Arc::new(nested),
            Arc::new(vector),
        ];
        let fields = schema
            .fields()
            .iter()
            .zip(&columns)
            .map(|(field, column)| Field::new(field.name(), column.data_type().clone(), true))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
        let planner = PostponeBucketPlanner::new(&schema, PostponeBucketPlan::default()).unwrap();
        let statistics = planner.input_statistics(&batch).unwrap();
        // Fixed row: 80; string/blob: 8+16; NULL decimal/timestamp: 16+8;
        // array/map/nested row/vector: 40+48+40+16. Same layout as Java/Python.
        assert_eq!(
            statistics
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            272
        );
    }

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

    #[test]
    fn buffered_statistics_preserve_normalized_primary_key_events() {
        let table = test_postpone_pk_table(&test_file_io(), "memory:/buffered_postpone_events")
            .copy_with_options(HashMap::from([(
                "postpone.target-row-num-per-bucket".into(),
                "1".into(),
            )]));
        let write = table.new_write_builder().new_write().unwrap();
        let normalized = write
            .normalize_write_batch(&make_batch(vec![1, 2], vec![10, 20]))
            .unwrap()
            .unwrap();
        let expected_kinds = normalized
            .column_by_name(crate::spec::VALUE_KIND_FIELD_NAME)
            .unwrap()
            .clone();
        let mut planner = BufferedPostponeBucketPlanner::new(&table).unwrap();
        assert!(planner.add(normalized).unwrap().is_none());
        let (plan, batches) = planner.finish();
        assert_eq!(
            plan.total_buckets(&crate::spec::EMPTY_SERIALIZED_ROW),
            Some(2)
        );
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0]
                .column_by_name(crate::spec::VALUE_KIND_FIELD_NAME)
                .unwrap(),
            &expected_kinds
        );
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
        let mut planner = BufferedPostponeBucketPlanner::new(&table).unwrap();
        planner.load_metadata(&table, 1).await.unwrap();
        planner.add(make_batch(vec![5], vec![50])).unwrap();
        let (plan, batches) = planner.finish();
        assert_eq!(
            plan.total_buckets(&crate::spec::EMPTY_SERIALIZED_ROW),
            Some(1)
        );
        assert_eq!(batches.len(), 1);
        let mut planner = BufferedPostponeBucketPlanner::new(&table).unwrap();
        planner
            .planner
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
        let mut planner = BufferedPostponeBucketPlanner::new(&table).unwrap();
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
        let mut planner = BufferedPostponeBucketPlanner::new(&table).unwrap();
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
