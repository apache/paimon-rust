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

use std::sync::Arc;

use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, ScalarValue, Statistics};
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use futures::{StreamExt, TryStreamExt};
use paimon::spec::{DataField, Datum, MergeEngine, Predicate};
use paimon::table::{ScanTrace, Table};
use paimon::DataSplit;

use crate::error::to_datafusion_error;

fn to_datafusion_batch(batch: RecordBatch, schema: &ArrowSchemaRef) -> DFResult<RecordBatch> {
    if batch.num_columns() != schema.fields().len() {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "Paimon reader returned {} columns for DataFusion schema with {} fields",
            batch.num_columns(),
            schema.fields().len()
        )));
    }

    let row_count = batch.num_rows();
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column.as_ref(), field.data_type()).map_err(Into::into)
            }
        })
        .collect::<DFResult<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));

    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(Into::into)
}

#[derive(Debug)]
struct ColumnStatsAccumulator {
    min_value: Option<Datum>,
    max_value: Option<Datum>,
    null_count: usize,
    min_valid: bool,
    max_valid: bool,
    null_valid: bool,
}

impl Default for ColumnStatsAccumulator {
    fn default() -> Self {
        Self {
            min_value: None,
            max_value: None,
            null_count: 0,
            min_valid: true,
            max_valid: true,
            null_valid: true,
        }
    }
}

impl ColumnStatsAccumulator {
    fn add_file(&mut self, file: &paimon::spec::DataFileMeta, field: &DataField, table: &Table) {
        if file.row_count == 0 {
            return;
        }
        let Some(stats) =
            file.value_stats_for_field(table.schema().id(), table.schema().fields(), field)
        else {
            self.min_valid = false;
            self.max_valid = false;
            self.null_valid = false;
            return;
        };

        let all_null = stats.null_count == Some(file.row_count);
        match stats.min_value {
            Some(value) => match &self.min_value {
                Some(current) => match value.partial_cmp(current) {
                    Some(std::cmp::Ordering::Less) => self.min_value = Some(value),
                    Some(_) => {}
                    None => self.min_valid = false,
                },
                None => self.min_value = Some(value),
            },
            None if !all_null => self.min_valid = false,
            None => {}
        }
        match stats.max_value {
            Some(value) => match &self.max_value {
                Some(current) => match value.partial_cmp(current) {
                    Some(std::cmp::Ordering::Greater) => self.max_value = Some(value),
                    Some(_) => {}
                    None => self.max_valid = false,
                },
                None => self.max_value = Some(value),
            },
            None if !all_null => self.max_valid = false,
            None => {}
        }
        match stats
            .null_count
            .and_then(|count| usize::try_from(count).ok())
            .and_then(|count| self.null_count.checked_add(count))
        {
            Some(count) => self.null_count = count,
            None => self.null_valid = false,
        }
    }

    fn finish(self, data_type: &ArrowDataType, exact_null_count: bool) -> ColumnStatistics {
        let min_value = if self.min_valid {
            self.min_value
                .and_then(|value| datum_to_scalar(value, data_type))
                .map(Precision::Inexact)
                .unwrap_or(Precision::Absent)
        } else {
            Precision::Absent
        };
        let max_value = if self.max_valid {
            self.max_value
                .and_then(|value| datum_to_scalar(value, data_type))
                .map(Precision::Inexact)
                .unwrap_or(Precision::Absent)
        } else {
            Precision::Absent
        };
        let null_count = if exact_null_count && self.null_valid {
            Precision::Exact(self.null_count)
        } else {
            Precision::Absent
        };

        ColumnStatistics {
            null_count,
            min_value,
            max_value,
            distinct_count: Precision::Absent,
            sum_value: Precision::Absent,
            byte_size: Precision::Absent,
        }
    }
}

fn datum_to_scalar(value: Datum, data_type: &ArrowDataType) -> Option<ScalarValue> {
    match (value, data_type) {
        (Datum::Bool(value), ArrowDataType::Boolean) => Some(ScalarValue::Boolean(Some(value))),
        (Datum::TinyInt(value), ArrowDataType::Int8) => Some(ScalarValue::Int8(Some(value))),
        (Datum::SmallInt(value), ArrowDataType::Int16) => Some(ScalarValue::Int16(Some(value))),
        (Datum::Int(value), ArrowDataType::Int32) => Some(ScalarValue::Int32(Some(value))),
        (Datum::Long(value), ArrowDataType::Int64) => Some(ScalarValue::Int64(Some(value))),
        (Datum::Float(value), ArrowDataType::Float32) => Some(ScalarValue::Float32(Some(value))),
        (Datum::Double(value), ArrowDataType::Float64) => Some(ScalarValue::Float64(Some(value))),
        (Datum::String(value), ArrowDataType::Utf8) => Some(ScalarValue::Utf8(Some(value))),
        (Datum::String(value), ArrowDataType::Utf8View) => Some(ScalarValue::Utf8View(Some(value))),
        (Datum::String(value), ArrowDataType::LargeUtf8) => {
            Some(ScalarValue::LargeUtf8(Some(value)))
        }
        (Datum::Date(value), ArrowDataType::Date32) => Some(ScalarValue::Date32(Some(value))),
        (Datum::Time(value), ArrowDataType::Time32(TimeUnit::Millisecond)) => {
            Some(ScalarValue::Time32Millisecond(Some(value)))
        }
        (Datum::Timestamp { millis, nanos }, ArrowDataType::Timestamp(unit, timezone))
        | (
            Datum::LocalZonedTimestamp { millis, nanos },
            ArrowDataType::Timestamp(unit, timezone),
        ) => {
            let value = match unit {
                TimeUnit::Second => millis.checked_div(1_000)?,
                TimeUnit::Millisecond => millis,
                TimeUnit::Microsecond => millis
                    .checked_mul(1_000)?
                    .checked_add(i64::from(nanos / 1_000))?,
                TimeUnit::Nanosecond => millis
                    .checked_mul(1_000_000)?
                    .checked_add(i64::from(nanos))?,
            };
            match unit {
                TimeUnit::Second => {
                    Some(ScalarValue::TimestampSecond(Some(value), timezone.clone()))
                }
                TimeUnit::Millisecond => Some(ScalarValue::TimestampMillisecond(
                    Some(value),
                    timezone.clone(),
                )),
                TimeUnit::Microsecond => Some(ScalarValue::TimestampMicrosecond(
                    Some(value),
                    timezone.clone(),
                )),
                TimeUnit::Nanosecond => Some(ScalarValue::TimestampNanosecond(
                    Some(value),
                    timezone.clone(),
                )),
            }
        }
        (
            Datum::Decimal {
                unscaled,
                precision,
                scale,
            },
            ArrowDataType::Decimal128(_, _),
        ) => Some(ScalarValue::Decimal128(
            Some(unscaled),
            u8::try_from(precision).ok()?,
            i8::try_from(scale).ok()?,
        )),
        // Paimon compares bytes using Java's signed-byte ordering, while Arrow compares
        // binary values using unsigned lexicographic ordering. Publishing the manifest
        // bounds would therefore be unsound for values crossing 0x7f/0x80.
        (
            Datum::Bytes(_),
            ArrowDataType::Binary | ArrowDataType::BinaryView | ArrowDataType::LargeBinary,
        ) => None,
        _ => None,
    }
}

/// Execution plan that scans a Paimon table with optional column projection.
///
/// Planning is performed eagerly in [`super::super::table::PaimonTableProvider::scan`],
/// and the resulting splits are distributed across DataFusion execution partitions
/// so that DataFusion can schedule them in parallel.
#[derive(Debug)]
pub struct PaimonTableScan {
    table: Table,
    /// Full Paimon read type for nested or connector-defined projections.
    read_type: Vec<DataField>,
    /// Filter translated from DataFusion expressions and reused during execute()
    /// so reader-side pruning reaches the actual read path.
    pushed_predicate: Option<Predicate>,
    /// Pre-planned partition assignments: `planned_partitions[i]` contains the
    /// Paimon splits that DataFusion partition `i` will read.
    /// Wrapped in `Arc` to avoid deep-cloning `DataSplit` metadata in `execute()`.
    planned_partitions: Vec<Arc<[DataSplit]>>,
    plan_properties: Arc<PlanProperties>,
    /// Optional limit hint pushed to paimon-core planning.
    limit: Option<usize>,
    /// Whether the pushed predicate is exact (no residual filtering needed).
    /// When true and all splits have known merged_row_count, statistics can be exact.
    filter_exact: bool,
    /// Metadata-pruning trace captured during eager scan planning.
    scan_trace: Option<ScanTrace>,
    /// Human-readable Variant extraction summary for explain output.
    pushed_variants: Option<String>,
    /// Column-name case sensitivity carried from planning to execution so the
    /// read path resolves names the same way the scan was planned.
    case_sensitive: bool,
}

impl PaimonTableScan {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        schema: ArrowSchemaRef,
        table: Table,
        read_type: Vec<DataField>,
        pushed_predicate: Option<Predicate>,
        planned_partitions: Vec<Arc<[DataSplit]>>,
        limit: Option<usize>,
        filter_exact: bool,
        scan_trace: Option<ScanTrace>,
        pushed_variants: Option<String>,
        case_sensitive: bool,
    ) -> Self {
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(planned_partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            table,
            read_type,
            pushed_predicate,
            planned_partitions,
            plan_properties,
            limit,
            filter_exact,
            scan_trace,
            pushed_variants,
            case_sensitive,
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    #[cfg(test)]
    pub(crate) fn planned_partitions(&self) -> &[Arc<[DataSplit]>] {
        &self.planned_partitions
    }

    #[cfg(test)]
    pub(crate) fn pushed_predicate(&self) -> Option<&Predicate> {
        self.pushed_predicate.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn filter_exact(&self) -> bool {
        self.filter_exact
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    fn manifest_column_statistics(&self, partitions: &[Arc<[DataSplit]>]) -> Vec<ColumnStatistics> {
        if self.read_type.len() != self.schema().fields().len() {
            return Statistics::unknown_column(&self.schema());
        }

        let Ok(merge_engine) = self.table.schema().core_options().merge_engine() else {
            return Statistics::unknown_column(&self.schema());
        };
        if merge_engine == MergeEngine::Aggregation {
            // Aggregate functions such as SUM can produce logical values outside every
            // physical file's min/max bounds.
            return Statistics::unknown_column(&self.schema());
        }

        let exact_null_counts = (self.table.schema().primary_keys().is_empty()
            || merge_engine == MergeEngine::Deduplicate)
            && self.pushed_predicate.is_none()
            && self.limit.is_none()
            && partitions
                .iter()
                .flat_map(|splits| splits.iter())
                .all(|split| {
                    split.raw_convertible()
                        && split.row_ranges().is_none()
                        && split
                            .data_deletion_files()
                            .is_none_or(|files| files.iter().all(Option::is_none))
                });
        let mut accumulators = (0..self.read_type.len())
            .map(|_| ColumnStatsAccumulator::default())
            .collect::<Vec<_>>();

        for file in partitions
            .iter()
            .flat_map(|splits| splits.iter())
            .flat_map(|split| split.data_files())
        {
            for (accumulator, field) in accumulators.iter_mut().zip(&self.read_type) {
                accumulator.add_file(file, field, &self.table);
            }
        }

        accumulators
            .into_iter()
            .zip(self.schema().fields())
            .map(|(accumulator, field)| accumulator.finish(field.data_type(), exact_null_counts))
            .collect()
    }
}

impl ExecutionPlan for PaimonTableScan {
    fn name(&self) -> &str {
        "PaimonTableScan"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let splits = Arc::clone(self.planned_partitions.get(partition).ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(format!(
                "PaimonTableScan: partition index {partition} out of range (total {})",
                self.planned_partitions.len()
            ))
        })?);

        let table = self.table.clone();
        let schema = self.schema();
        let read_type = self.read_type.clone();
        let pushed_predicate = self.pushed_predicate.clone();
        let case_sensitive = self.case_sensitive;

        let fut = async move {
            let mut read_builder = table.new_read_builder();

            read_builder.with_case_sensitive(case_sensitive);
            read_builder.with_read_type(read_type);
            if let Some(filter) = pushed_predicate {
                read_builder.with_filter(filter);
            }

            let read = read_builder.new_read().map_err(to_datafusion_error)?;
            let stream = read.to_arrow(&splits).map_err(to_datafusion_error)?;
            let batch_schema = Arc::clone(&schema);
            let stream = stream.map(move |result| {
                result
                    .map_err(to_datafusion_error)
                    .and_then(|batch| to_datafusion_batch(batch, &batch_schema))
            });

            Ok::<_, datafusion::error::DataFusionError>(RecordBatchStreamAdapter::new(
                schema,
                Box::pin(stream),
            ))
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(fut).try_flatten(),
        )))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Arc<Statistics>> {
        let partitions: &[Arc<[DataSplit]>] = match partition {
            Some(idx) => std::slice::from_ref(&self.planned_partitions[idx]),
            None => &self.planned_partitions,
        };

        let mut total_rows: usize = 0;
        let mut all_row_counts_known = true;
        for splits in partitions {
            for split in splits.iter() {
                if let Some(row_count) = split.merged_row_count() {
                    total_rows += row_count as usize;
                } else {
                    all_row_counts_known = false;
                    total_rows += split.row_count() as usize;
                }
            }
        }

        // Return exact statistics when:
        // 1. All splits have known merged_row_count (no deletion files with unknown cardinality)
        // 2. No limit is applied (limit would make row count inexact)
        // 3. Filter is exact (no residual filtering needed above the scan)
        let num_rows_precision =
            if all_row_counts_known && self.limit.is_none() && self.filter_exact {
                Precision::Exact(total_rows)
            } else {
                Precision::Inexact(total_rows)
            };

        Ok(Arc::new(Statistics {
            num_rows: num_rows_precision,
            total_byte_size: Precision::Absent,
            column_statistics: self.manifest_column_statistics(partitions),
        }))
    }
}

impl DisplayAs for PaimonTableScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "PaimonTableScan: table={}", self.table.identifier())?;

        let total_splits: usize = self.planned_partitions.iter().map(|p| p.len()).sum();
        let total_files: usize = self
            .planned_partitions
            .iter()
            .flat_map(|p| p.iter())
            .map(|s| s.data_files().len())
            .sum();
        write!(
            f,
            ", partitions={}, splits={total_splits}, files={total_files}",
            self.planned_partitions.len()
        )?;

        let columns = self
            .read_type
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>();
        write!(f, ", projection=[{}]", columns.join(", "))?;
        if let Some(ref predicate) = self.pushed_predicate {
            write!(f, ", predicate={predicate}")?;
        }
        if let Some(limit) = self.limit {
            write!(f, ", limit={limit}")?;
        }
        if let Some(ref trace) = self.scan_trace {
            write!(f, ", trace={trace}")?;
        }
        if let Some(ref pushed_variants) = self.pushed_variants {
            write!(f, ", PushedVariants=[{pushed_variants}]")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    mod test_utils {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../test_utils.rs"));
    }

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;
    use paimon::catalog::Identifier;
    use paimon::io::FileIOBuilder;
    use paimon::spec::{
        BinaryRow, DataFileMeta, DataType, Datum, IntType, PredicateBuilder,
        Schema as PaimonSchema, TableSchema,
    };
    use paimon::table::{DeletionFile, RowRange, Table};
    use std::fs;
    use tempfile::tempdir;
    use test_utils::{local_file_path, test_data_file, write_int_parquet_file};

    fn test_schema() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]))
    }

    fn test_read_type() -> Vec<DataField> {
        vec![DataField::new(
            0,
            "id".to_string(),
            DataType::Int(IntType::new()),
        )]
    }

    #[test]
    fn test_binary_manifest_bounds_are_not_exposed() {
        for data_type in [
            ArrowDataType::Binary,
            ArrowDataType::BinaryView,
            ArrowDataType::LargeBinary,
        ] {
            assert_eq!(
                datum_to_scalar(Datum::Bytes(vec![0x7f, 0x80]), &data_type),
                None
            );
        }
    }

    #[test]
    fn test_partition_count_empty_plan() {
        let schema = test_schema();
        let scan = PaimonTableScan::new(
            schema,
            dummy_table(),
            test_read_type(),
            None,
            vec![Arc::from(Vec::new())],
            None,
            false,
            None,
            None,
            true,
        );
        assert_eq!(scan.properties().output_partitioning().partition_count(), 1);
    }

    #[test]
    fn test_partition_count_multiple_partitions() {
        let schema = test_schema();
        let planned_partitions = vec![
            Arc::from(Vec::new()),
            Arc::from(Vec::new()),
            Arc::from(Vec::new()),
        ];
        let scan = PaimonTableScan::new(
            schema,
            dummy_table(),
            test_read_type(),
            None,
            planned_partitions,
            None,
            false,
            None,
            None,
            true,
        );
        assert_eq!(scan.properties().output_partitioning().partition_count(), 3);
    }

    /// Constructs a minimal Table for testing (no real files needed since we
    /// only test PlanProperties, not actual reads).
    fn dummy_table() -> Table {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let table_schema = TableSchema::new(0, &schema);
        Table::new(
            file_io,
            Identifier::new("test_db", "test_table"),
            "/tmp/test-table".to_string(),
            table_schema,
            None,
        )
    }

    fn data_file_with_int_stats(
        file_name: &str,
        row_count: i64,
        min: i32,
        max: i32,
        null_count: i64,
    ) -> DataFileMeta {
        let data_type = DataType::Int(IntType::new());
        let min = Datum::Int(min);
        let max = Datum::Int(max);
        let mut file =
            serde_json::to_value(test_data_file::<DataFileMeta>(file_name, row_count, 100))
                .unwrap();
        file["_VALUE_STATS"] = serde_json::json!({
            "_MIN_VALUES": BinaryRow::from_datums(&[(Some(&min), &data_type)]).to_serialized_bytes(),
            "_MAX_VALUES": BinaryRow::from_datums(&[(Some(&max), &data_type)]).to_serialized_bytes(),
            "_NULL_COUNTS": [null_count],
        });
        serde_json::from_value(file).unwrap()
    }

    fn split_with_int_stats(
        row_ranges: Option<Vec<RowRange>>,
        deletion_file: Option<DeletionFile>,
    ) -> DataSplit {
        let mut builder = paimon::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/test-table/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![data_file_with_int_stats("data.parquet", 4, 1, 4, 1)]);
        if let Some(row_ranges) = row_ranges {
            builder = builder.with_row_ranges(row_ranges);
        }
        if let Some(deletion_file) = deletion_file {
            builder = builder.with_data_deletion_files(vec![Some(deletion_file)]);
        }
        builder.build().unwrap()
    }

    #[test]
    fn test_partition_statistics_include_manifest_column_bounds() {
        let split = paimon::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/test-table/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![
                data_file_with_int_stats("a.parquet", 4, 2, 8, 1),
                data_file_with_int_stats("b.parquet", 6, 1, 10, 2),
            ])
            .build()
            .unwrap();
        let scan = PaimonTableScan::new(
            test_schema(),
            dummy_table(),
            test_read_type(),
            None,
            vec![Arc::from(vec![split])],
            None,
            true,
            None,
            None,
            true,
        );

        let statistics = scan.partition_statistics(None).unwrap();
        let id = &statistics.column_statistics[0];

        assert_eq!(
            id.min_value,
            Precision::Inexact(ScalarValue::Int32(Some(1)))
        );
        assert_eq!(
            id.max_value,
            Precision::Inexact(ScalarValue::Int32(Some(10)))
        );
        assert_eq!(id.null_count, Precision::Exact(3));
    }

    #[test]
    fn test_partition_statistics_omit_compressed_file_size() {
        let split = paimon::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/test-table/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![data_file_with_int_stats("data.parquet", 4, 1, 4, 0)])
            .build()
            .unwrap();
        let scan = PaimonTableScan::new(
            test_schema(),
            dummy_table(),
            test_read_type(),
            None,
            vec![Arc::from(vec![split])],
            None,
            true,
            None,
            None,
            true,
        );

        let statistics = scan.partition_statistics(None).unwrap();

        assert_eq!(statistics.num_rows, Precision::Exact(4));
        assert_eq!(statistics.total_byte_size, Precision::Absent);
    }

    #[test]
    fn test_partition_statistics_downgrade_unsafe_null_counts() {
        let table = dummy_table();
        let predicate = PredicateBuilder::new(table.schema().fields())
            .greater_than("id", Datum::Int(1))
            .unwrap();
        let cases = vec![
            (split_with_int_stats(None, None), Some(predicate), None),
            (split_with_int_stats(None, None), None, Some(2)),
            (
                split_with_int_stats(Some(vec![RowRange::new(1, 2)]), None),
                None,
                None,
            ),
            (
                split_with_int_stats(
                    None,
                    Some(DeletionFile::new("dv.bin".to_string(), 0, 16, Some(1))),
                ),
                None,
                None,
            ),
        ];

        for (split, predicate, limit) in cases {
            let scan = PaimonTableScan::new(
                test_schema(),
                table.clone(),
                test_read_type(),
                predicate,
                vec![Arc::from(vec![split])],
                limit,
                true,
                None,
                None,
                true,
            );
            assert_eq!(
                scan.partition_statistics(None).unwrap().column_statistics[0].null_count,
                Precision::Absent
            );
        }
    }

    #[test]
    fn test_partition_statistics_hide_aggregation_value_bounds() {
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &PaimonSchema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("merge-engine", "aggregation")
                .option("fields.value.aggregate-function", "sum")
                .build()
                .unwrap(),
        );
        let value_field = table_schema.fields()[1].clone();
        let table = Table::new(
            file_io,
            Identifier::new("test_db", "aggregation_table"),
            "/tmp/aggregation-table".to_string(),
            table_schema,
            None,
        );
        let mut file = data_file_with_int_stats("data.parquet", 2, 1, 2, 0);
        file.value_stats_cols = Some(vec!["value".to_string()]);
        let split = paimon::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("file:/tmp/aggregation-table/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![file])
            .build()
            .unwrap();
        let scan = PaimonTableScan::new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "value",
                ArrowDataType::Int32,
                true,
            )])),
            table,
            vec![value_field],
            None,
            vec![Arc::from(vec![split])],
            None,
            true,
            None,
            None,
            true,
        );

        let value_stats = &scan.partition_statistics(None).unwrap().column_statistics[0];
        assert_eq!(value_stats.min_value, Precision::Absent);
        assert_eq!(value_stats.max_value, Precision::Absent);
        assert_eq!(value_stats.null_count, Precision::Absent);
    }

    #[tokio::test]
    async fn test_execute_applies_pushed_filter_during_read() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        write_int_parquet_file(
            &bucket_dir.join("data.parquet"),
            vec![("id", vec![1, 2, 3, 4]), ("value", vec![5, 20, 30, 40])],
            Some(2),
        );
        let file_size = fs::metadata(bucket_dir.join("data.parquet")).unwrap().len() as i64;

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &paimon::spec::Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            table_path,
            table_schema,
            None,
        );

        let split = paimon::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 4, file_size)])
            .build()
            .unwrap();

        let pushed_predicate = PredicateBuilder::new(table.schema().fields())
            .greater_or_equal("value", Datum::Int(10))
            .unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let scan = PaimonTableScan::new(
            schema,
            table,
            vec![DataField::new(
                0,
                "id".to_string(),
                DataType::Int(IntType::new()),
            )],
            Some(pushed_predicate),
            vec![Arc::from(vec![split])],
            None,
            false,
            None,
            None,
            true,
        );

        let ctx = SessionContext::new();
        let stream = scan
            .execute(0, ctx.task_ctx())
            .expect("execute should succeed");
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();

        let actual_ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id column should be Int32Array");
                (0..ids.len()).map(|idx| ids.value(idx)).collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(actual_ids, vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_execute_uses_read_batch_size_option() {
        let tempdir = tempdir().unwrap();
        let table_path = local_file_path(tempdir.path());
        let bucket_dir = tempdir.path().join("bucket-0");
        fs::create_dir_all(&bucket_dir).unwrap();

        write_int_parquet_file(
            &bucket_dir.join("data.parquet"),
            vec![("id", vec![1, 2, 3, 4, 5])],
            None,
        );
        let file_size = fs::metadata(bucket_dir.join("data.parquet")).unwrap().len() as i64;

        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table_schema = TableSchema::new(
            0,
            &PaimonSchema::builder()
                .column("id", DataType::Int(IntType::new()))
                .option("read.batch-size", "2")
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            table_path,
            table_schema,
            None,
        );
        let split = paimon::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(local_file_path(&bucket_dir))
            .with_total_buckets(1)
            .with_data_files(vec![test_data_file("data.parquet", 5, file_size)])
            .build()
            .unwrap();
        let scan = PaimonTableScan::new(
            test_schema(),
            table,
            test_read_type(),
            None,
            vec![Arc::from(vec![split])],
            None,
            false,
            None,
            None,
            true,
        );

        let ctx = SessionContext::new();
        let batches = scan
            .execute(0, ctx.task_ctx())
            .expect("execute should succeed")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
    }
}
