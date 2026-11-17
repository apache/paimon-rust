// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Resolve staged writes by field identity before publishing existing-row updates.

use super::*;
use crate::spec::{DataField, TableSchema};

impl TableCommit {
    pub(super) async fn row_id_file_schema(&self, file: &DataFileMeta) -> Result<Arc<TableSchema>> {
        if file.schema_id == self.table.schema().id() {
            return Ok(Arc::new(self.table.schema().clone()));
        }
        if let Some(schema) = self.table.schema_manager().cached_schema(file.schema_id) {
            return Ok(schema);
        }
        // Conflict detection precedes snapshot preparation. A concurrent REST
        // update may introduce a catalog-only schema the old handle has never
        // seen. Resolve it before attempting to read a missing schema file.
        if self.table.rest_env.is_some() {
            let current = self.latest_row_id_schema().await?;
            if current.id() == file.schema_id {
                return Ok(current);
            }
        }
        self.table.schema_manager().schema(file.schema_id).await
    }

    pub(super) async fn latest_row_id_schema(&self) -> Result<Arc<TableSchema>> {
        if let Some(env) = &self.table.rest_env {
            // REST catalogs need not store schema files. Use one response for
            // both field identities and the snapshot's schema ID.
            let response = env
                .api()
                .get_table(&env.identifier().with_branch(self.table.branch())?)
                .await?;
            let id = response
                .schema_id
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "REST table response is missing schemaId".into(),
                    source: None,
                })?;
            let schema = response.schema.ok_or_else(|| crate::Error::DataInvalid {
                message: "REST table response is missing schema".into(),
                source: None,
            })?;
            let schema = TableSchema::new(id, &schema);
            self.table
                .schema_manager()
                .cache_resolved_schema(schema.clone());
            return Ok(Arc::new(schema));
        }
        if let Some(schema) = self.table.schema_manager().latest().await? {
            return Ok(schema);
        }
        if self.table.is_main_branch() {
            Ok(Arc::new(self.table.schema().clone()))
        } else {
            Err(crate::Error::DataInvalid {
                message: format!("Branch '{}' does not exist.", self.table.branch()),
                source: None,
            })
        }
    }

    pub(super) async fn validate_row_id_update_fields(
        &self,
        current: &TableSchema,
        files: &[DataFileMeta],
    ) -> Result<()> {
        let mut current_ids = HashSet::new();
        collect_field_ids(current.fields(), &mut current_ids);
        let mut checked = HashSet::new();
        for file in files {
            if !checked.insert((file.schema_id, file.write_cols.as_ref())) {
                continue;
            }
            let historical = if file.schema_id == current.id() {
                None
            } else {
                Some(self.row_id_file_schema(file).await?)
            };
            let schema = historical.as_deref().unwrap_or(current);
            let fields = match &file.write_cols {
                Some(columns) => super::super::data_evolution_fields::project_by_paths(
                    schema.fields(),
                    &columns
                        .iter()
                        .filter(|name| !is_system_field(name))
                        .cloned()
                        .collect::<Vec<_>>(),
                )?,
                None => schema.data_file_fields(None),
            };
            let mut written_ids = HashSet::new();
            collect_field_ids(&fields, &mut written_ids);
            if !written_ids.is_subset(&current_ids) {
                return Err(crate::Error::DataInvalid {
                    message: "Row-id update cannot restore a dropped field".into(),
                    source: None,
                });
            }
        }
        Ok(())
    }
}

fn collect_field_ids(fields: &[DataField], ids: &mut HashSet<i32>) {
    for field in fields {
        ids.insert(field.id());
        collect_nested_ids(field.data_type(), ids);
    }
}

fn collect_nested_ids(data_type: &DataType, ids: &mut HashSet<i32>) {
    match data_type {
        DataType::Row(row) => collect_field_ids(row.fields(), ids),
        DataType::Array(array) => collect_nested_ids(array.element_type(), ids),
        DataType::Multiset(multiset) => collect_nested_ids(multiset.element_type(), ids),
        DataType::Map(map) => {
            collect_nested_ids(map.key_type(), ids);
            collect_nested_ids(map.value_type(), ids);
        }
        _ => {}
    }
}
