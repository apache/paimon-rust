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

//! Mirrors Java [OptionsTable](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/system/OptionsTable.java).

use std::sync::{Arc, OnceLock};

use crate::table::Table;
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::Result;

pub(super) fn options_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ]))
        })
        .clone()
}

pub(super) async fn read(
    table: &Table,
    _projection: Option<&[usize]>,
    _predicate: Option<&crate::spec::Predicate>,
) -> Result<RecordBatch> {
    let manager = table.schema_manager();
    let latest = manager
        .latest()
        .await?
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "Table does not exist".to_string(),
            source: None,
        })?;
    // Java uses LinkedHashMap insertion order; HashMap has none — sort for stable output.
    let mut entries: Vec<(&String, &String)> = latest.options().iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    let keys = StringArray::from_iter_values(entries.iter().map(|(k, _)| k.as_str()));
    let values = StringArray::from_iter_values(entries.iter().map(|(_, v)| v.as_str()));

    let schema = options_schema();
    let batch = super::record_batch(schema.clone(), vec![Arc::new(keys), Arc::new(values)])?;

    Ok(batch)
}
