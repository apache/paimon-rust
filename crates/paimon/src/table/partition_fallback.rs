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

use super::Table;
use crate::spec::TableSchema;
use crate::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// The configured branch is resolved against the filesystem for each batch plan.
#[derive(Debug)]
pub(super) struct PartitionFallback {
    name: String,
    read_options: HashMap<String, String>,
}

pub(super) enum BranchSchema {
    Latest,
    Planned,
}

impl PartitionFallback {
    pub(super) fn configured(table: &Table) -> Result<Option<Arc<Self>>> {
        let Some(branch) = table.schema().options().get("scan.fallback-branch") else {
            return Ok(None);
        };
        if branch.trim().is_empty() || branch == table.branch() {
            return Ok(None);
        }
        super::validate_branch_name(branch)?;
        validate_table(table)?;
        // Copy only explicit query settings. The branch supplies its own storage layout
        // and any read defaults the caller did not override.
        let read_options = table
            .schema()
            .options()
            .iter()
            .filter(|(key, _)| {
                table.applied_dynamic_option_keys.contains(*key) && is_read_option(key)
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Ok(Some(Arc::new(Self {
            name: branch.clone(),
            read_options,
        })))
    }

    pub(super) async fn branch(&self, table: &Table, view: BranchSchema) -> Result<Option<Table>> {
        let Some(mut branch) = table.filesystem_branch(self.name.clone()).await? else {
            return Ok(None);
        };
        validate_table(&branch)?;
        if !matching_schema(table.schema(), branch.schema()) {
            // A transported split carries the branch bit but no logical schema ID.
            // Only an already-planned read may recover its layout after branch DDL.
            let schema = match view {
                BranchSchema::Latest => None,
                BranchSchema::Planned => matching_historical_schema(table.schema(), &branch).await?,
            }.ok_or_else(|| Error::ConfigInvalid {
                message: format!(
                    "Partition fallback requires matching field IDs, names, types, order and partition keys on branch '{}'",
                    self.name
                ),
            })?;
            branch.schema =
                schema.copy_with_options(HashMap::from([("branch".into(), self.name.clone())]));
            validate_table(&branch)?;
        }
        if !self.read_options.is_empty() {
            branch = branch.copy_with_options(self.read_options.clone());
        }
        Ok(Some(branch))
    }
}

fn is_read_option(key: &str) -> bool {
    const READ_PREFIXES: &[&str] = &[
        "read.",
        "scan.manifest.",
        "source.split.",
        "parquet.filter.",
        "file-index.read.",
        "mosaic.read.",
        "table-read.",
    ];
    key == "manifest.sidecar.enabled" || READ_PREFIXES.iter().any(|prefix| key.starts_with(prefix))
}

async fn matching_historical_schema(
    planned: &TableSchema,
    branch: &Table,
) -> Result<Option<Arc<TableSchema>>> {
    let manager = branch.schema_manager();
    for id in manager.list_all_ids().await?.into_iter().rev() {
        if id < branch.schema().id() {
            let schema = manager.schema(id).await?;
            if matching_schema(planned, &schema) {
                return Ok(Some(schema));
            }
        }
    }
    Ok(None)
}

fn matching_schema(current: &TableSchema, other: &TableSchema) -> bool {
    current.primary_keys() == other.primary_keys()
        && current.partition_keys() == other.partition_keys()
        && current
            .fields()
            .iter()
            .map(field_signature)
            .eq(other.fields().iter().map(field_signature))
}

fn field_signature(field: &crate::spec::DataField) -> (i32, &str, &crate::spec::DataType) {
    (field.id(), field.name(), field.data_type())
}

fn validate_table(table: &Table) -> Result<()> {
    let options = table.schema().core_options();
    options.validate_scan_options()?;
    options.validate_data_file_path_directory()?;
    options.ensure_type_paimon_served(&table.identifier().full_name())?;
    if table.rest_env().is_some()
        || options.query_auth_enabled()
        || table.is_format_table()
        || table.schema().partition_keys().is_empty()
        || !table.schema().primary_keys().is_empty()
        || table
            .schema()
            .fields()
            .iter()
            .any(|field| field.data_type().is_blob_file_field())
        || !options.blob_fields().is_empty()
        || options.deletion_vectors_enabled()
        || options.data_evolution_enabled()
        || options.row_tracking_enabled()
        || table.is_time_traveled()
        || options.has_time_travel_selector()
        || options.incremental_timestamp_window()?.is_some()
    {
        return Err(Error::Unsupported {
            message: "Partition fallback requires a latest-snapshot read of a partitioned, ordinary filesystem append table".into(),
        });
    }
    Ok(())
}
