// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashSet;

use crate::catalog::Identifier;
use crate::io::FileIO;
use crate::spec::{FileKind, IndexManifest, SchemaChange, TableSchema};
use crate::table::{SchemaManager, SnapshotManager};
use crate::{Error, Result};

pub(crate) fn apply_schema_changes(
    current_schema: &TableSchema,
    changes: &[SchemaChange],
    identifier: &Identifier,
) -> Result<TableSchema> {
    current_schema
        .apply_changes(changes.to_vec())
        .map_err(|error| fill_table_name(error, identifier))
}

pub(crate) async fn validate_type_evolution_precommit(
    file_io: &FileIO,
    table_path: &str,
    current_schema: &TableSchema,
    new_schema: &TableSchema,
) -> Result<()> {
    let changed_field_ids = type_changed_field_ids(current_schema, new_schema);
    if changed_field_ids.is_empty() {
        return Ok(());
    }

    assert_historical_schema_evolution_casts(file_io, table_path, new_schema, &changed_field_ids)
        .await?;
    assert_no_persisted_index_type_dependencies(
        file_io,
        table_path,
        current_schema,
        &changed_field_ids,
    )
    .await
}

async fn assert_historical_schema_evolution_casts(
    file_io: &FileIO,
    table_path: &str,
    new_schema: &TableSchema,
    changed_field_ids: &HashSet<i32>,
) -> Result<()> {
    let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
    for historical_schema in schema_manager.list_all().await? {
        for target_field in new_schema
            .fields()
            .iter()
            .filter(|field| changed_field_ids.contains(&field.id()))
        {
            let Some(source_field) = historical_schema
                .fields()
                .iter()
                .find(|field| field.id() == target_field.id())
            else {
                continue;
            };
            if !crate::arrow::schema_evolution::schema_evolution_cast_implemented(
                source_field.data_type(),
                target_field.data_type(),
            ) {
                return Err(Error::Unsupported {
                    message: format!(
                        "Cannot update type of column '{}' because historical schema {} requires an unimplemented schema evolution cast from {:?} to {:?} for field id {}",
                        target_field.name(),
                        historical_schema.id(),
                        source_field.data_type(),
                        target_field.data_type(),
                        target_field.id()
                    ),
                });
            }
        }
    }
    Ok(())
}

async fn assert_no_persisted_index_type_dependencies(
    file_io: &FileIO,
    table_path: &str,
    current_schema: &TableSchema,
    changed_field_ids: &HashSet<i32>,
) -> Result<()> {
    let snapshot_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
    let Some(snapshot) = snapshot_manager.get_latest_snapshot().await? else {
        return Ok(());
    };
    let Some(index_manifest_name) = snapshot.index_manifest() else {
        return Ok(());
    };
    let entries = IndexManifest::read(
        file_io,
        &snapshot_manager.manifest_path(index_manifest_name),
    )
    .await?;
    for entry in entries {
        if entry.kind != FileKind::Add {
            continue;
        }
        let Some(index_meta) = entry.index_file.global_index_meta.as_ref() else {
            continue;
        };
        let dependent_field_id = std::iter::once(index_meta.index_field_id)
            .chain(index_meta.extra_field_ids.iter().flatten().copied())
            .find(|field_id| changed_field_ids.contains(field_id));
        let Some(field_id) = dependent_field_id else {
            continue;
        };
        let field_name = current_schema
            .fields()
            .iter()
            .find(|field| field.id() == field_id)
            .map(|field| field.name())
            .unwrap_or("<unknown>");
        return Err(Error::Unsupported {
            message: format!(
                "Cannot update type of column '{field_name}' because persisted global index '{}' depends on field id {field_id}",
                entry.index_file.index_type
            ),
        });
    }
    Ok(())
}

fn type_changed_field_ids(current_schema: &TableSchema, new_schema: &TableSchema) -> HashSet<i32> {
    current_schema
        .fields()
        .iter()
        .filter_map(|current_field| {
            let new_field = new_schema
                .fields()
                .iter()
                .find(|field| field.id() == current_field.id())?;
            (!crate::arrow::schema_evolution::same_type_ignoring_nullability(
                current_field.data_type(),
                new_field.data_type(),
            ))
            .then_some(current_field.id())
        })
        .collect()
}

fn fill_table_name(error: Error, identifier: &Identifier) -> Error {
    match error {
        Error::ColumnNotExist { column, .. } => Error::ColumnNotExist {
            full_name: identifier.full_name(),
            column,
        },
        Error::ColumnAlreadyExist { column, .. } => Error::ColumnAlreadyExist {
            full_name: identifier.full_name(),
            column,
        },
        other => other,
    }
}
