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

//! Materialized MERGE input preserves Arrow batches and source row order.

use super::Table;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use arrow_select::interleave::interleave_record_batch;
use futures::TryStreamExt;
use std::collections::{HashMap, HashSet};

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

pub(super) struct MergeInput {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    ends: Vec<usize>,
    pub row_count: usize,
}

impl MergeInput {
    pub fn new(mut batches: Vec<RecordBatch>) -> crate::Result<Self> {
        let first = batches
            .first()
            .ok_or_else(|| invalid("MERGE source needs an Arrow schema"))?;
        let schema = first.schema();
        if batches.iter().any(|batch| batch.schema() != schema) {
            return Err(invalid("MERGE source batches must share a schema"));
        }
        let names = schema
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<HashSet<_>>();
        if names.len() != schema.fields().len() {
            return Err(invalid("MERGE source has duplicate column names"));
        }
        batches.retain(|batch| batch.num_rows() > 0);
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(schema.clone()));
        }
        let mut row_count = 0usize;
        let ends = batches
            .iter()
            .map(|batch| {
                row_count = row_count
                    .checked_add(batch.num_rows())
                    .ok_or_else(|| invalid("MERGE source row count overflow"))?;
                Ok(row_count)
            })
            .collect::<crate::Result<_>>()?;
        Ok(Self {
            schema,
            batches,
            ends,
            row_count,
        })
    }

    pub async fn read(table: &Table, columns: &HashSet<String>) -> crate::Result<Self> {
        if table.is_format_table() {
            return Err(crate::Error::Unsupported {
                message: "MERGE table sources require a Paimon table".into(),
            });
        }
        crate::spec::CoreOptions::new(table.schema().options()).validate_scan_options()?;
        let schema = crate::arrow::build_target_arrow_schema(table.schema().fields())?;
        for column in columns {
            schema
                .field_with_name(column)
                .map_err(|error| invalid(error.to_string()))?;
        }
        // Schema order makes every projected batch interchangeable. An empty
        // snapshot still carries its declared schema without fetching any file.
        let indices = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(index, field)| columns.contains(field.name()).then_some(index))
            .collect::<Vec<_>>();
        let projected = std::sync::Arc::new(
            schema
                .project(&indices)
                .map_err(|error| invalid(error.to_string()))?,
        );
        let names = projected
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        let incremental = table
            .schema()
            .core_options()
            .incremental_timestamp_window()?
            .is_some();
        let snapshot = if incremental {
            None
        } else {
            super::time_travel::resolve_snapshot(table).await?
        };
        // Pin the source independently, keeping branch/time-travel resolution
        // and the caller's resolved schema. Include L0 as Python plan_for_write.
        let table = if incremental {
            table.clone()
        } else {
            table.copy_with_pinned_snapshot(snapshot.as_ref())
        };
        let mut builder = table.new_read_builder();
        builder.with_projection(&names)?;
        let plan = if incremental || snapshot.is_some() {
            builder.new_scan().with_scan_all_files().plan().await?
        } else {
            // Run the normal reader's policy checks even with no snapshot, but
            // do not observe files from a commit arriving after resolution.
            super::source::Plan::new(Vec::new())
        };
        let mut stream = builder.new_read()?.to_arrow(plan.splits())?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(projected));
        }
        Self::new(batches)
    }

    pub fn row_offset(&self, batch: usize) -> usize {
        if batch == 0 {
            0
        } else {
            self.ends[batch - 1]
        }
    }

    pub fn gather(&self, rows: &[usize]) -> crate::Result<RecordBatch> {
        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        let mut used = Vec::new();
        let mut positions = HashMap::new();
        let mut indices = Vec::with_capacity(rows.len());
        for &row in rows {
            if row >= self.row_count {
                return Err(invalid("MERGE source row is out of range"));
            }
            let batch = self.ends.partition_point(|end| *end <= row);
            let position = *positions.entry(batch).or_insert_with(|| {
                used.push(&self.batches[batch]);
                used.len() - 1
            });
            indices.push((position, row - self.row_offset(batch)));
        }
        // Touch only contributing chunks, and preserve the requested order.
        // Avoid concatenating entire source columns with 32-bit Arrow offsets.
        interleave_record_batch(&used, &indices).map_err(|error| invalid(error.to_string()))
    }
}
