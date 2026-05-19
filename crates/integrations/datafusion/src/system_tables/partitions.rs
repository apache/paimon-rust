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

//! Mirrors Java [PartitionsTable](https://github.com/apache/paimon/blob/release-1.4/paimon-core/src/main/java/org/apache/paimon/table/system/PartitionsTable.java).

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::{
    BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::catalog::{Catalog, Identifier};
use paimon::spec::{CoreOptions, Partition};
use paimon::table::Table;

use crate::error::to_datafusion_error;

pub(super) fn build(
    catalog: Arc<dyn Catalog>,
    identifier: Identifier,
    table: Table,
) -> DFResult<Arc<dyn TableProvider>> {
    let table_schema = table.schema();
    let partition_keys = table_schema.partition_keys().to_vec();
    let default_partition_name = CoreOptions::new(table_schema.options())
        .partition_default_name()
        .to_string();
    Ok(Arc::new(PartitionsTable {
        catalog,
        identifier,
        partition_keys,
        default_partition_name,
    }))
}

fn partitions_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("partition", DataType::Utf8, true),
                Field::new("record_count", DataType::Int64, false),
                Field::new("file_size_in_bytes", DataType::Int64, false),
                Field::new("file_count", DataType::Int64, false),
                Field::new(
                    "last_update_time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new(
                    "created_at",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new("created_by", DataType::Utf8, true),
                Field::new("updated_by", DataType::Utf8, true),
                Field::new("options", DataType::Utf8, true),
                Field::new("total_buckets", DataType::Int32, false),
                Field::new("done", DataType::Boolean, false),
            ]))
        })
        .clone()
}

struct PartitionsTable {
    catalog: Arc<dyn Catalog>,
    identifier: Identifier,
    partition_keys: Vec<String>,
    default_partition_name: String,
}

impl std::fmt::Debug for PartitionsTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionsTable")
            .field("identifier", &self.identifier)
            .finish()
    }
}

#[async_trait]
impl TableProvider for PartitionsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        partitions_schema()
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
        let catalog = self.catalog.clone();
        let identifier = self.identifier.clone();
        let partitions = crate::runtime::await_with_runtime(async move {
            catalog.list_partitions(&identifier).await
        })
        .await
        .map_err(to_datafusion_error)?;

        let mut rows: Vec<(String, Partition)> = partitions
            .into_iter()
            .map(|p| {
                let s = format_partition_string(
                    &p.spec,
                    &self.partition_keys,
                    &self.default_partition_name,
                );
                (s, p)
            })
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));

        let n = rows.len();
        let mut partition_strings: Vec<Option<String>> = Vec::with_capacity(n);
        let mut record_counts = Vec::with_capacity(n);
        let mut file_sizes = Vec::with_capacity(n);
        let mut file_counts = Vec::with_capacity(n);
        let mut last_update_times: Vec<Option<i64>> = Vec::with_capacity(n);
        let mut created_ats: Vec<Option<i64>> = Vec::with_capacity(n);
        let mut created_bys: Vec<Option<String>> = Vec::with_capacity(n);
        let mut updated_bys: Vec<Option<String>> = Vec::with_capacity(n);
        let mut options_jsons: Vec<Option<String>> = Vec::with_capacity(n);
        let mut total_buckets = Vec::with_capacity(n);
        let mut dones = Vec::with_capacity(n);

        for (s, p) in rows {
            partition_strings.push(Some(s));
            record_counts.push(p.record_count);
            file_sizes.push(p.file_size_in_bytes);
            file_counts.push(p.file_count);
            // 0 marks "no creation_time on any file"; real wall-clock is never
            // <= 0 in practice, so this never nullifies a genuine timestamp.
            last_update_times.push(if p.last_file_creation_time > 0 {
                Some(p.last_file_creation_time)
            } else {
                None
            });
            created_ats.push(p.created_at);
            created_bys.push(p.created_by);
            updated_bys.push(p.updated_by);
            // Sort via BTreeMap so the serialised JSON is deterministic across
            // runs (Partition.options is a HashMap with unspecified order).
            options_jsons.push(
                p.options
                    .as_ref()
                    .map(|m| {
                        let sorted: BTreeMap<&String, &String> = m.iter().collect();
                        serde_json::to_string(&sorted)
                            .map_err(|e| DataFusionError::External(Box::new(e)))
                    })
                    .transpose()?,
            );
            total_buckets.push(p.total_buckets);
            dones.push(p.done);
        }

        let schema = partitions_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(partition_strings)),
                Arc::new(Int64Array::from(record_counts)),
                Arc::new(Int64Array::from(file_sizes)),
                Arc::new(Int64Array::from(file_counts)),
                Arc::new(TimestampMillisecondArray::from(last_update_times)),
                Arc::new(TimestampMillisecondArray::from(created_ats)),
                Arc::new(StringArray::from(created_bys)),
                Arc::new(StringArray::from(updated_bys)),
                Arc::new(StringArray::from(options_jsons)),
                Arc::new(Int32Array::from(total_buckets)),
                Arc::new(BooleanArray::from(dones)),
            ],
        )?;

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            projection.cloned(),
        )?)
    }
}

/// Format `spec` as `key1=val1/key2=val2` in `partition_keys` order. Empty
/// string for non-partitioned tables. NULL spec values fall back to
/// `default_partition_name`.
fn format_partition_string(
    spec: &HashMap<String, String>,
    partition_keys: &[String],
    default_partition_name: &str,
) -> String {
    partition_keys
        .iter()
        .map(|k| {
            let v = spec
                .get(k)
                .map(String::as_str)
                .unwrap_or(default_partition_name);
            format!("{k}={v}")
        })
        .collect::<Vec<_>>()
        .join("/")
}
