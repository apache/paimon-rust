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

use crate::spec::{batch_to_serialized_bytes, BinaryRow, DataField, Snapshot, TableSchema};
use crate::table::{Table, TableScan};
use crate::Result;
use arrow_array::{new_empty_array, Array, ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::collections::HashMap;
use std::sync::Arc;

pub const POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD: &str = "total_buckets";

pub(crate) fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

#[derive(Debug, Clone, Default)]
pub struct PostponeBucketPlan {
    bucket_counts: Arc<HashMap<Vec<u8>, i32>>,
    default_bucket_num: Option<i32>,
}

impl PostponeBucketPlan {
    pub(super) fn from_counts(bucket_counts: HashMap<Vec<u8>, i32>) -> Self {
        Self {
            bucket_counts: Arc::new(bucket_counts),
            default_bucket_num: None,
        }
    }
    pub fn from_arrow(schema: &TableSchema, batch: &RecordBatch) -> Result<Self> {
        let partition_fields = schema.partition_fields();
        let partition_count = partition_fields.len();
        if batch.num_columns() != partition_count + 1 {
            return Err(data_invalid(format!(
                "Postpone bucket plan expected {} partition column(s) plus '{POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD}', got {} columns",
                partition_count,
                batch.num_columns()
            )));
        }

        let expected = crate::arrow::build_target_arrow_schema(&partition_fields)?;
        let actual = batch.schema();
        if !expected
            .fields()
            .iter()
            .zip(actual.fields())
            .all(|(expected, actual)| {
                actual.name() == expected.name() && actual.data_type() == expected.data_type()
            })
        {
            return Err(data_invalid(
                "Postpone bucket plan partition fields do not match the table schema",
            ));
        }
        for (index, field) in expected.fields().iter().enumerate() {
            if !field.is_nullable() && batch.column(index).null_count() != 0 {
                return Err(data_invalid(format!(
                    "Postpone bucket plan partition column '{}' is NOT NULL but contains null values",
                    field.name()
                )));
            }
        }

        let count_field = actual.field(partition_count);
        if count_field.name() != POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD
            || count_field.data_type() != &arrow_schema::DataType::Int32
        {
            return Err(data_invalid(format!(
                "Postpone bucket plan final field must be '{POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD}': Int32, got '{}': {:?}",
                count_field.name(),
                count_field.data_type()
            )));
        }
        let counts = batch
            .column(partition_count)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                data_invalid("Postpone bucket plan total_buckets column is not Int32")
            })?;
        let partition_indices = (0..partition_count).collect::<Vec<_>>();
        let partitions = batch_to_serialized_bytes(batch, &partition_indices, &partition_fields)?;
        let mut bucket_counts = HashMap::with_capacity(batch.num_rows());
        for (row, partition) in partitions.into_iter().enumerate() {
            if counts.is_null(row) {
                return Err(data_invalid(format!(
                    "Postpone bucket plan total_buckets is null at row {row}"
                )));
            }
            let total_buckets = counts.value(row);
            if total_buckets <= 0 {
                return Err(data_invalid(format!(
                    "Postpone bucket plan total_buckets must be positive at row {row}, got {total_buckets}"
                )));
            }
            if let Some(previous) = bucket_counts.insert(partition, total_buckets) {
                if previous != total_buckets {
                    return Err(data_invalid(format!(
                        "Postpone bucket plan contains conflicting total bucket counts {previous} and {total_buckets} for one partition"
                    )));
                }
            }
        }
        Ok(Self::from_counts(bucket_counts))
    }

    /// Export known partitions using the same schema accepted by `from_arrow`.
    /// A configured default is not an existing partition and is not exported.
    pub fn to_arrow(&self, schema: &TableSchema) -> Result<RecordBatch> {
        let partition_fields = schema.partition_fields();
        let mut fields = crate::arrow::build_target_arrow_schema(&partition_fields)?
            .fields()
            .to_vec();
        fields.push(Arc::new(Field::new(
            POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD,
            DataType::Int32,
            false,
        )));
        let mut entries = self.bucket_counts.iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(partition, _)| *partition);
        let partitions = entries
            .iter()
            .map(|(bytes, _)| bytes.as_slice())
            .collect::<Vec<_>>();
        let mut columns = serialized_partition_columns(&partition_fields, &partitions)?;
        columns.push(Arc::new(Int32Array::from_iter_values(
            entries.iter().map(|(_, count)| **count),
        )));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| data_invalid(error.to_string()))
    }

    pub(crate) fn with_default_bucket_num(default_bucket_num: i32) -> Self {
        Self {
            default_bucket_num: Some(default_bucket_num),
            ..Self::default()
        }
    }

    /// Read the same baseline used for sequence numbers and commit conflict checks.
    /// Pending (-2) files have no real layout and never override the configured default.
    pub(crate) async fn load_existing_buckets(
        &mut self,
        table: &Table,
        snapshot_id: i64,
    ) -> Result<()> {
        if snapshot_id == 0 {
            return Ok(());
        }
        let snapshot = table.snapshot_manager().get_snapshot(snapshot_id).await?;
        self.load_existing_buckets_from_snapshot(table, &snapshot)
            .await
    }

    pub(super) async fn load_existing_buckets_from_snapshot(
        &mut self,
        table: &Table,
        snapshot: &Snapshot,
    ) -> Result<()> {
        let entries = TableScan::new(table, None, vec![], None, None, None)
            .with_scan_all_files()
            .plan_manifest_entries(snapshot)
            .await?;
        let mut counts = HashMap::new();
        for entry in entries {
            if entry.bucket() < 0 {
                continue;
            }
            let count = entry.total_buckets();
            if count <= 0 || entry.bucket() >= count {
                return Err(data_invalid(
                    "Invalid total bucket count in existing postpone files",
                ));
            }
            if let Some(previous) = counts.insert(entry.partition().to_vec(), count) {
                if previous != count {
                    return Err(data_invalid("Existing postpone files contain conflicting total bucket counts for one partition"));
                }
            }
        }
        self.bucket_counts = Arc::new(counts);
        Ok(())
    }

    pub(crate) fn total_buckets(&self, partition: &[u8]) -> Option<i32> {
        self.bucket_counts
            .get(partition)
            .copied()
            .or(self.default_bucket_num)
    }

    pub(super) fn explicit_bucket_counts(&self) -> &HashMap<Vec<u8>, i32> {
        &self.bucket_counts
    }

    pub(super) fn insert(&mut self, partition: Vec<u8>, count: i32) {
        Arc::make_mut(&mut self.bucket_counts).insert(partition, count);
    }
}

/// Decode the common partition prefix of plans and input statistics.
pub(super) fn serialized_partition_columns(
    fields: &[DataField],
    partitions: &[&[u8]],
) -> Result<Vec<ArrayRef>> {
    let rows = partitions
        .iter()
        .map(|bytes| {
            let row = BinaryRow::from_serialized_bytes(bytes)?;
            if row.arity() as usize != fields.len() {
                return Err(data_invalid(
                    "Postpone partition arity does not match the table schema",
                ));
            }
            Ok(row)
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = crate::arrow::build_target_arrow_schema(fields)?;
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let arrays = rows
                .iter()
                .map(|row| {
                    crate::arrow::partition::partition_array(row, index, field.data_type(), 1)
                })
                .collect::<Result<Vec<_>>>()?;
            if arrays.is_empty() {
                Ok(new_empty_array(schema.field(index).data_type()))
            } else {
                arrow_select::concat::concat(
                    &arrays
                        .iter()
                        .map(|array| array.as_ref())
                        .collect::<Vec<_>>(),
                )
                .map_err(|error| data_invalid(error.to_string()))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::table_write::tests::test_file_io;
    use arrow_array::StringArray;

    #[test]
    fn arrow_plan_round_trip_keeps_null_partition_and_excludes_default_rows() {
        let schema = crate::spec::Schema::builder()
            .column(
                "pt",
                crate::spec::DataType::VarChar(crate::spec::VarCharType::string_type()),
            )
            .column(
                "id",
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            )
            .primary_key(["pt", "id"])
            .partition_keys(["pt"])
            .option("bucket", "-2")
            .option("primary-key.nullable", "true")
            .build()
            .unwrap();
        let table = Table::new(
            test_file_io(),
            crate::catalog::Identifier::new("default", "bucket_plan_arrow"),
            "memory:/bucket_plan_arrow".into(),
            crate::spec::TableSchema::new(0, &schema),
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("pt", DataType::Utf8, true),
                Field::new(
                    POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD,
                    DataType::Int32,
                    false,
                ),
            ])),
            vec![
                Arc::new(StringArray::from(vec![Some("b"), None, Some("a")])),
                Arc::new(Int32Array::from(vec![2, 3, 4])),
            ],
        )
        .unwrap();
        let plan = PostponeBucketPlan::from_arrow(table.schema(), &batch).unwrap();
        let exported = plan.to_arrow(table.schema()).unwrap();
        assert_eq!(exported.schema().field(0).name(), "pt");
        assert_eq!(exported.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(exported.schema().field(1), batch.schema().field(1));
        assert_eq!(exported.num_rows(), 3);
        assert_eq!(
            PostponeBucketPlan::from_arrow(table.schema(), &exported)
                .unwrap()
                .bucket_counts,
            plan.bucket_counts
        );
        let expected_schema = exported.schema();
        let default = PostponeBucketPlan::with_default_bucket_num(7);
        let exported = default.to_arrow(table.schema()).unwrap();
        assert_eq!(exported.schema(), expected_schema);
        assert_eq!(exported.num_rows(), 0);
        assert_eq!(default.total_buckets(&[]), Some(7));
    }
}
