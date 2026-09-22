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

//! Mirrors Java [TagsTable](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/system/TagsTable.java).

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
use paimon::table::Table;

use crate::error::to_datafusion_error;

pub(super) fn build(table: Table) -> DFResult<Arc<dyn TableProvider>> {
    Ok(Arc::new(TagsTable { table }))
}

fn tags_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("tag_name", DataType::Utf8, false),
                Field::new("snapshot_id", DataType::Int64, false),
                Field::new("schema_id", DataType::Int64, false),
                Field::new(
                    "commit_time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("record_count", DataType::Int64, true),
                Field::new(
                    "create_time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new("time_retained", DataType::Utf8, true),
            ]))
        })
        .clone()
}

#[derive(Debug)]
pub(super) struct TagsTable {
    table: Table,
}

#[async_trait]
impl TableProvider for TagsTable {
    fn schema(&self) -> SchemaRef {
        tags_schema()
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
        let tm = self.table.tag_manager();
        let tags =
            crate::runtime::await_with_runtime(async move { tm.list_all_with_metadata().await })
                .await
                .map_err(to_datafusion_error)?;

        let n = tags.len();
        let mut tag_names: Vec<String> = Vec::with_capacity(n);
        let mut snapshot_ids = Vec::with_capacity(n);
        let mut schema_ids = Vec::with_capacity(n);
        let mut commit_times = Vec::with_capacity(n);
        let mut record_counts: Vec<Option<i64>> = Vec::with_capacity(n);
        let mut create_times: Vec<Option<i64>> = Vec::with_capacity(n);
        let mut time_retained: Vec<Option<String>> = Vec::with_capacity(n);

        for (name, snap, created, retained) in tags {
            tag_names.push(name);
            snapshot_ids.push(snap.id());
            schema_ids.push(snap.schema_id());
            commit_times.push(snap.time_millis() as i64);
            record_counts.push(snap.total_record_count());
            create_times.push(created);
            time_retained.push(retained.map(format_duration_iso8601));
        }

        let schema = tags_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(tag_names)),
                Arc::new(Int64Array::from(snapshot_ids)),
                Arc::new(Int64Array::from(schema_ids)),
                Arc::new(TimestampMillisecondArray::from(commit_times)),
                Arc::new(Int64Array::from(record_counts)),
                Arc::new(TimestampMillisecondArray::from(create_times)),
                Arc::new(StringArray::from(time_retained)),
            ],
        )?;

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            projection.cloned(),
        )?)
    }
}

/// Render a retention in seconds the way Java's `Duration.toString()` does, so
/// the column reads the same across engines: `PT72H`, `PT1M30S`, `PT0.5S`.
fn format_duration_iso8601(total_seconds: f64) -> String {
    if !total_seconds.is_finite() {
        return "PT0S".to_string();
    }
    let negative = total_seconds < 0.0;
    let magnitude = total_seconds.abs();
    let whole = magnitude.trunc() as i64;
    let fraction = magnitude - whole as f64;

    let hours = whole / 3600;
    let minutes = (whole % 3600) / 60;
    let seconds = whole % 60;

    let mut out = String::from(if negative { "-PT" } else { "PT" });
    if hours != 0 {
        out.push_str(&format!("{hours}H"));
    }
    if minutes != 0 {
        out.push_str(&format!("{minutes}M"));
    }
    if seconds != 0 || fraction != 0.0 || (hours == 0 && minutes == 0) {
        if fraction == 0.0 {
            out.push_str(&format!("{seconds}S"));
        } else {
            // Java prints up to nanosecond precision with trailing zeros trimmed.
            let rendered = format!("{:.9}", seconds as f64 + fraction);
            out.push_str(rendered.trim_end_matches('0').trim_end_matches('.'));
            out.push('S');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::format_duration_iso8601;

    #[test]
    fn test_format_duration_iso8601_matches_java() {
        // Whole units, mirroring java.time.Duration.toString().
        assert_eq!(format_duration_iso8601(259_200.0), "PT72H");
        assert_eq!(format_duration_iso8601(3600.0), "PT1H");
        assert_eq!(format_duration_iso8601(90.0), "PT1M30S");
        assert_eq!(format_duration_iso8601(60.0), "PT1M");
        assert_eq!(format_duration_iso8601(1.0), "PT1S");
        // Zero keeps the seconds component so the string is never bare "PT".
        assert_eq!(format_duration_iso8601(0.0), "PT0S");
        // Sub-second retention: trailing zeros trimmed.
        assert_eq!(format_duration_iso8601(0.5), "PT0.5S");
        assert_eq!(format_duration_iso8601(1.25), "PT1.25S");
        // Mixed with larger units.
        assert_eq!(format_duration_iso8601(3661.0), "PT1H1M1S");
        // Not a number cannot panic or produce a bogus unit.
        assert_eq!(format_duration_iso8601(f64::NAN), "PT0S");
        assert_eq!(format_duration_iso8601(f64::INFINITY), "PT0S");
    }
}
