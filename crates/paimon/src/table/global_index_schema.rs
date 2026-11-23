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

//! Check Data Evolution scalar index key types against the build schema.

use super::global_index_types::normalize_queryable_global_index_type;
use crate::spec::{DataEvolutionIndexSourceMeta, IndexFileMeta, IndexManifestEntry, TableSchema};
use crate::table::Table;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

const UNKNOWN_SCHEMA_ID: i64 = DataEvolutionIndexSourceMeta::UNKNOWN_SCHEMA_ID;

/// One query or build shares its source schema loads and per-field checks.
pub(super) struct GlobalIndexSchema<'a> {
    table: &'a Table,
    source_schemas: HashMap<i64, Option<Arc<TableSchema>>>,
    field_compatibility: HashMap<(i64, i32), bool>,
}

impl<'a> GlobalIndexSchema<'a> {
    pub(super) fn new(table: &'a Table) -> Self {
        Self {
            table,
            source_schemas: HashMap::new(),
            field_compatibility: HashMap::new(),
        }
    }

    pub(super) async fn filter_entries(
        &mut self,
        entries: Vec<IndexManifestEntry>,
    ) -> Vec<IndexManifestEntry> {
        let mut compatible = Vec::with_capacity(entries.len());
        for entry in entries {
            if self.compatible(&entry.index_file).await {
                compatible.push(entry);
            }
        }
        compatible
    }

    pub(super) async fn compatible(&mut self, file: &IndexFileMeta) -> bool {
        if !self.table.schema().core_options().data_evolution_enabled()
            || normalize_queryable_global_index_type(&file.index_type).is_none()
        {
            return true;
        }
        let Some(meta) = &file.global_index_meta else {
            // The scanner reports missing index metadata.
            return true;
        };
        let source_schema_id = match meta.source_meta.as_deref() {
            Some(bytes) if DataEvolutionIndexSourceMeta::is_data_evolution_meta(Some(bytes)) => {
                match DataEvolutionIndexSourceMeta::deserialize(bytes) {
                    Ok(source) => source.source_schema_id(),
                    Err(_) => return false,
                }
            }
            _ => UNKNOWN_SCHEMA_ID,
        };
        let read_schema = self.table.schema();
        let source_fields =
            if source_schema_id == UNKNOWN_SCHEMA_ID || source_schema_id == read_schema.id() {
                None
            } else {
                if let Entry::Vacant(entry) = self.source_schemas.entry(source_schema_id) {
                    entry.insert(
                        self.table
                            .schema_manager()
                            .schema(source_schema_id)
                            .await
                            .ok()
                            .filter(|schema| schema.id() == source_schema_id),
                    );
                }
                let Some(schema) = &self.source_schemas[&source_schema_id] else {
                    return false;
                };
                Some(schema.fields())
            };
        std::iter::once(meta.index_field_id)
            .chain(meta.extra_field_ids.iter().flatten().copied())
            .all(|field_id| {
                *self
                    .field_compatibility
                    .entry((source_schema_id, field_id))
                    .or_insert_with(|| {
                        let Some(field) = read_schema.fields().iter().find(|f| f.id() == field_id)
                        else {
                            return false;
                        };
                        source_fields.is_none_or(|fields| {
                            fields.iter().any(|source| {
                                source.id() == field_id
                                    && source.data_type().equals_ignore_nullable(field.data_type())
                            })
                        })
                    })
            })
    }
}
