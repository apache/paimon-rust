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

//! Mirrors Java [BucketsTable](https://github.com/apache/paimon/blob/release-1.4/paimon-core/src/main/java/org/apache/paimon/table/system/BucketsTable.java).

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::{
    Int32Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::spec::{BinaryRow, DataField};
use paimon::table::Table;

use super::row_string_cast::format_row_as_java_cast_string;
use crate::error::to_datafusion_error;

pub(super) fn build(table: Table) -> DFResult<Arc<dyn TableProvider>> {
    Ok(Arc::new(BucketsTable { table }))
}

fn buckets_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("partition", ArrowDataType::Utf8, true),
                Field::new("bucket", ArrowDataType::Int32, false),
                Field::new("record_count", ArrowDataType::Int64, false),
                Field::new("file_size_in_bytes", ArrowDataType::Int64, false),
                Field::new("file_count", ArrowDataType::Int64, false),
                Field::new(
                    "last_update_time",
                    ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
            ]))
        })
        .clone()
}

#[derive(Debug)]
pub(super) struct BucketsTable {
    table: Table,
}

#[async_trait]
impl TableProvider for BucketsTable {
    fn schema(&self) -> SchemaRef {
        buckets_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table = self.table.clone();
        let rows =
            crate::runtime::await_with_runtime(async move { collect_bucket_rows(&table).await })
                .await
                .map_err(to_datafusion_error)?;
        let batch = bucket_rows_to_record_batch(&rows)?;
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            buckets_schema(),
            projection.cloned(),
        )?)
    }
}

/// A bucket's files aggregated: mirrors Java `BucketEntry`.
#[derive(Default)]
struct BucketAgg {
    record_count: i64,
    file_size_in_bytes: i64,
    file_count: i64,
    last_update_time: Option<i64>,
}

struct BucketRow {
    partition: Option<String>,
    bucket: i32,
    agg: BucketAgg,
}

async fn collect_bucket_rows(table: &Table) -> paimon::Result<Vec<BucketRow>> {
    let scan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await?;
    let partition_fields = table.schema().partition_fields();
    // BTreeMap keys sort by partition string then bucket, matching Java BucketsTable's
    // `Comparator.comparing(partition).thenComparing(bucket)`.
    let mut aggs: BTreeMap<(Option<String>, i32), BucketAgg> = BTreeMap::new();
    for split in scan.splits() {
        let partition = format_partition(split.partition(), &partition_fields)?;
        let agg = aggs.entry((partition, split.bucket())).or_default();
        for file in split.data_files() {
            agg.record_count = agg.record_count.saturating_add(file.row_count);
            agg.file_size_in_bytes = agg.file_size_in_bytes.saturating_add(file.file_size);
            agg.file_count += 1;
            if let Some(t) = file.creation_time.map(|t| t.timestamp_millis()) {
                agg.last_update_time = Some(agg.last_update_time.map_or(t, |cur| cur.max(t)));
            }
        }
    }
    Ok(aggs
        .into_iter()
        .map(|((partition, bucket), agg)| BucketRow {
            partition,
            bucket,
            agg,
        })
        .collect())
}

fn bucket_rows_to_record_batch(rows: &[BucketRow]) -> DFResult<RecordBatch> {
    let n = rows.len();
    let mut partitions = Vec::with_capacity(n);
    let mut buckets = Vec::with_capacity(n);
    let mut record_counts = Vec::with_capacity(n);
    let mut file_sizes = Vec::with_capacity(n);
    let mut file_counts = Vec::with_capacity(n);
    let mut last_update_times = Vec::with_capacity(n);
    for row in rows {
        partitions.push(row.partition.clone());
        buckets.push(row.bucket);
        record_counts.push(row.agg.record_count);
        file_sizes.push(row.agg.file_size_in_bytes);
        file_counts.push(row.agg.file_count);
        last_update_times.push(row.agg.last_update_time);
    }
    Ok(RecordBatch::try_new(
        buckets_schema(),
        vec![
            Arc::new(StringArray::from(partitions)),
            Arc::new(Int32Array::from(buckets)),
            Arc::new(Int64Array::from(record_counts)),
            Arc::new(Int64Array::from(file_sizes)),
            Arc::new(Int64Array::from(file_counts)),
            Arc::new(TimestampMillisecondArray::from(last_update_times)),
        ],
    )?)
}

/// Format `partition` as Java's cast-to-string, matching `$files`; `{}` when the
/// table is not partitioned.
fn format_partition(
    partition: &BinaryRow,
    partition_fields: &[DataField],
) -> paimon::Result<Option<String>> {
    if partition_fields.is_empty() {
        return Ok(Some("{}".to_string()));
    }
    format_row_as_java_cast_string(partition, partition_fields).map(Some)
}
