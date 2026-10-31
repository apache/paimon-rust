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

//! Physical source schemas are the codec contract for source-backed scalar indexes.

use crate::spec::{DataField, DataFileMeta};
use crate::table::Table;
use std::collections::HashMap;

/// A scalar payload does not record its key type. Never build cast keys on top
/// of unchanged sources, or later reverting a column type could misread them.
/// Reads use the same proof and fall back when any source schema is unknown.
pub(super) async fn source_types_match(
    table: &Table,
    field: &DataField,
    files: &[&DataFileMeta],
) -> bool {
    let manager = table.schema_manager();
    let mut schemas = HashMap::new();
    for file in files {
        if file.schema_id == table.schema().id() {
            continue;
        }
        if let std::collections::hash_map::Entry::Vacant(entry) = schemas.entry(file.schema_id) {
            entry.insert(manager.schema(file.schema_id).await.ok());
        }
        if !schemas[&file.schema_id].as_ref().is_some_and(|schema| {
            schema.fields().iter().any(|old| {
                old.id() == field.id() && old.data_type().equals_ignore_nullable(field.data_type())
            })
        }) {
            return false;
        }
    }
    true
}
