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

//! Mirrors Java [SnapshotsTable](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/system/SnapshotsTable.java).

use std::any::Any;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::table::{SnapshotManager, Table};

use crate::error::to_datafusion_error;

pub(super) fn build(table: Table) -> DFResult<Arc<dyn TableProvider>> {
    Ok(Arc::new(SnapshotsTable { table }))
}

fn snapshots_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("snapshot_id", DataType::Int64, false),
                Field::new("schema_id", DataType::Int64, false),
                Field::new("commit_user", DataType::Utf8, false),
                Field::new("commit_identifier", DataType::Int64, false),
                Field::new("commit_kind", DataType::Utf8, false),
                Field::new(
                    "commit_time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("base_manifest_list", DataType::Utf8, false),
                Field::new("delta_manifest_list", DataType::Utf8, false),
                Field::new("changelog_manifest_list", DataType::Utf8, true),
                Field::new("total_record_count", DataType::Int64, true),
                Field::new("delta_record_count", DataType::Int64, true),
                Field::new("changelog_record_count", DataType::Int64, true),
                Field::new("watermark", DataType::Int64, true),
                Field::new("next_row_id", DataType::Int64, true),
            ]))
        })
        .clone()
}

#[derive(Debug)]
struct SnapshotsTable {
    table: Table,
}

#[async_trait]
impl TableProvider for SnapshotsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        snapshots_schema()
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
        let sm = SnapshotManager::new(
            self.table.file_io().clone(),
            self.table.location().to_string(),
        );
        let snapshots = crate::runtime::await_with_runtime(async move { sm.list_all().await })
            .await
            .map_err(to_datafusion_error)?;

        let n = snapshots.len();
        let mut snapshot_ids = Vec::with_capacity(n);
        let mut schema_ids = Vec::with_capacity(n);
        let mut commit_users: Vec<String> = Vec::with_capacity(n);
        let mut commit_identifiers = Vec::with_capacity(n);
        let mut commit_kinds: Vec<String> = Vec::with_capacity(n);
        let mut commit_times = Vec::with_capacity(n);
        let mut base_manifest_lists: Vec<String> = Vec::with_capacity(n);
        let mut delta_manifest_lists: Vec<String> = Vec::with_capacity(n);
        let mut changelog_manifest_lists: Vec<Option<String>> = Vec::with_capacity(n);
        let mut total_record_counts: Vec<Option<i64>> = Vec::with_capacity(n);
        let mut delta_record_counts: Vec<Option<i64>> = Vec::with_capacity(n);
        let mut changelog_record_counts: Vec<Option<i64>> = Vec::with_capacity(n);
        let mut watermarks: Vec<Option<i64>> = Vec::with_capacity(n);
        let mut next_row_ids: Vec<Option<i64>> = Vec::with_capacity(n);

        for snap in &snapshots {
            snapshot_ids.push(snap.id());
            schema_ids.push(snap.schema_id());
            commit_users.push(snap.commit_user().to_string());
            commit_identifiers.push(snap.commit_identifier());
            commit_kinds.push(snap.commit_kind().to_string());
            commit_times.push(snap.time_millis() as i64);
            base_manifest_lists.push(snap.base_manifest_list().to_string());
            delta_manifest_lists.push(snap.delta_manifest_list().to_string());
            changelog_manifest_lists.push(snap.changelog_manifest_list().map(str::to_string));
            total_record_counts.push(snap.total_record_count());
            delta_record_counts.push(snap.delta_record_count());
            changelog_record_counts.push(snap.changelog_record_count());
            watermarks.push(snap.watermark());
            next_row_ids.push(snap.next_row_id());
        }

        let schema = snapshots_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(snapshot_ids)),
                Arc::new(Int64Array::from(schema_ids)),
                Arc::new(StringArray::from(commit_users)),
                Arc::new(Int64Array::from(commit_identifiers)),
                Arc::new(StringArray::from(commit_kinds)),
                Arc::new(TimestampMillisecondArray::from(commit_times)),
                Arc::new(StringArray::from(base_manifest_lists)),
                Arc::new(StringArray::from(delta_manifest_lists)),
                Arc::new(StringArray::from(changelog_manifest_lists)),
                Arc::new(Int64Array::from(total_record_counts)),
                Arc::new(Int64Array::from(delta_record_counts)),
                Arc::new(Int64Array::from(changelog_record_counts)),
                Arc::new(Int64Array::from(watermarks)),
                Arc::new(Int64Array::from(next_row_ids)),
            ],
        )?;

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            projection.cloned(),
        )?)
    }
}
