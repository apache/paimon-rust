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

//! Prove the logical field type of a global payload before reusing its coverage.
//!
//! Native scalar keys and full-text tokens do not record their logical source
//! type. New file names identify the actual builder schema, which can differ
//! from its captured data snapshot.
//! Java opens manifest file names unchanged, so no native or manifest format
//! changes are needed. Legacy payloads require an unchanged complete history.

use crate::spec::DataField;
use crate::table::Table;
use std::collections::HashMap;

pub(super) fn index_file_name(index_type: &str, schema_id: i64) -> String {
    format!(
        "{index_type}-global-index-schema-{schema_id}-{}.index",
        uuid::Uuid::new_v4()
    )
}

enum BuildSchema {
    Tagged(i64),
    Legacy,
    Invalid,
}

fn build_schema(file_name: &str, index_type: &str) -> BuildSchema {
    let prefix = format!("{index_type}-global-index-schema-");
    let Some(tag) = file_name.strip_prefix(&prefix) else {
        return BuildSchema::Legacy;
    };
    let Some((id, uuid)) = tag.split_once('-') else {
        return BuildSchema::Invalid;
    };
    let Some(id) = id.parse::<i64>().ok().filter(|id| *id >= 0) else {
        return BuildSchema::Invalid;
    };
    if uuid
        .strip_suffix(".index")
        .and_then(|uuid| uuid::Uuid::parse_str(uuid).ok())
        .is_none()
    {
        return BuildSchema::Invalid;
    }
    BuildSchema::Tagged(id)
}

/// One definition and maintenance round share successful and failed lookups.
/// In particular, a legacy history is not scanned once per payload file.
pub(super) struct GlobalIndexSchema<'a> {
    table: &'a Table,
    field: &'a DataField,
    schemas: HashMap<i64, bool>,
    legacy: Option<bool>,
}

impl<'a> GlobalIndexSchema<'a> {
    pub(super) fn new(table: &'a Table, field: &'a DataField) -> Self {
        Self {
            table,
            field,
            schemas: HashMap::new(),
            legacy: None,
        }
    }

    pub(super) async fn compatible(&mut self, file_name: &str, index_type: &str) -> bool {
        match build_schema(file_name, index_type) {
            BuildSchema::Tagged(id) => self.schema_matches(id).await,
            BuildSchema::Invalid => false,
            BuildSchema::Legacy => {
                if let Some(compatible) = self.legacy {
                    return compatible;
                }
                let current = self.table.schema().id();
                let mut compatible = current >= 0;
                for id in 0..=current {
                    if !self.schema_matches(id).await {
                        compatible = false;
                        break;
                    }
                }
                self.legacy = Some(compatible);
                compatible
            }
        }
    }

    async fn schema_matches(&mut self, id: i64) -> bool {
        if let Some(&compatible) = self.schemas.get(&id) {
            return compatible;
        }
        let compatible = if id == self.table.schema().id() {
            // Includes catalog-resolved schemas without filesystem metadata.
            true
        } else {
            self.table
                .schema_manager()
                .schema(id)
                .await
                .ok()
                .is_some_and(|schema| {
                    schema.id() == id
                        && schema.fields().iter().any(|field| {
                            field.id() == self.field.id()
                                && field
                                    .data_type()
                                    .equals_ignore_nullable(self.field.data_type())
                        })
                })
        };
        self.schemas.insert(id, compatible);
        compatible
    }
}
