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

//! Mirrors Java [BranchesTable](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/system/BranchesTable.java).

use std::sync::{Arc, OnceLock};

use crate::table::{BranchManager, Table};
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::Result;

pub(super) fn branches_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("branch_name", DataType::Utf8, false),
                Field::new(
                    "create_time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
            ]))
        })
        .clone()
}

pub(super) async fn read(
    table: &Table,
    _projection: Option<&[usize]>,
    _predicate: Option<&crate::spec::Predicate>,
) -> Result<RecordBatch> {
    let (names, create_times) = collect_branches(table).await?;

    let schema = branches_schema();
    let batch = super::record_batch(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(names)),
            super::local_timestamp_column(create_times)?,
        ],
    )?;

    Ok(batch)
}

async fn collect_branches(table: &Table) -> crate::Result<(Vec<String>, Vec<i64>)> {
    let file_io = table.file_io();
    let bm = BranchManager::new(file_io.clone(), table.location().to_string());
    let names = bm.list_all().await?;
    let mut create_times = Vec::with_capacity(names.len());
    for name in &names {
        let status = file_io.get_status(&bm.branch_path(name)).await?;
        create_times.push(
            status
                .last_modified
                .map(|dt| dt.timestamp_millis())
                .unwrap_or(0),
        );
    }
    Ok((names, create_times))
}
