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

//! Mirrors Java [SchemasTable](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/system/SchemasTable.java).

use std::sync::{Arc, OnceLock};

use crate::table::Table;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::Serialize;

use crate::Result;

pub(super) fn schemas_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("schema_id", DataType::Int64, false),
                Field::new("fields", DataType::Utf8, false),
                Field::new("partition_keys", DataType::Utf8, false),
                Field::new("primary_keys", DataType::Utf8, false),
                Field::new("options", DataType::Utf8, false),
                Field::new("comment", DataType::Utf8, true),
                Field::new(
                    "update_time",
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
    let schemas = table.schema_manager().list_all().await?;

    let n = schemas.len();
    let mut schema_ids: Vec<i64> = Vec::with_capacity(n);
    let mut fields_json: Vec<String> = Vec::with_capacity(n);
    let mut partition_keys_json: Vec<String> = Vec::with_capacity(n);
    let mut primary_keys_json: Vec<String> = Vec::with_capacity(n);
    let mut options_json: Vec<String> = Vec::with_capacity(n);
    let mut comments: Vec<Option<String>> = Vec::with_capacity(n);
    let mut update_times: Vec<i64> = Vec::with_capacity(n);

    for schema in &schemas {
        schema_ids.push(schema.id());
        fields_json.push(to_json(schema.fields())?);
        partition_keys_json.push(to_json(schema.partition_keys())?);
        primary_keys_json.push(to_json(schema.primary_keys())?);
        options_json.push(to_json(schema.options())?);
        comments.push(schema.comment().map(str::to_string));
        update_times.push(schema.time_millis());
    }

    let schema = schemas_schema();
    let batch = super::record_batch(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(schema_ids)),
            Arc::new(StringArray::from(fields_json)),
            Arc::new(StringArray::from(partition_keys_json)),
            Arc::new(StringArray::from(primary_keys_json)),
            Arc::new(StringArray::from(options_json)),
            Arc::new(StringArray::from(comments)),
            super::local_timestamp_column(update_times)?,
        ],
    )?;

    Ok(batch)
}

fn to_json<T: Serialize + ?Sized>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|e| crate::Error::DataInvalid {
        message: e.to_string(),
        source: Some(Box::new(e)),
    })
}
