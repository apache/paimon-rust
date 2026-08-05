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

//! Schema manager for reading versioned table schemas.
//!
//! Reference: [org.apache.paimon.schema.SchemaManager](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java)

use crate::arrow::schema_evolution::{
    requires_schema_evolution_storage_normalization, same_type_ignoring_nullability,
    schema_evolution_cast_implemented,
};
use crate::io::FileIO;
use crate::spec::{GlobalIndexMeta, TableSchema};
use futures::future::try_join_all;
use opendal::raw::get_basename;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

const SCHEMA_DIR: &str = "schema";
const SCHEMA_PREFIX: &str = "schema-";

#[derive(Debug)]
struct SchemaEvolutionInfo {
    history_complete: bool,
    type_evolved_field_ids: Arc<HashSet<i32>>,
    storage_normalization_field_ids: Arc<HashSet<i32>>,
}

type SchemaEvolutionCell = Arc<OnceCell<Arc<SchemaEvolutionInfo>>>;

/// Manager for versioned table schema files.
///
/// Each table stores schema versions as JSON files under `{table_path}/schema/schema-{id}`.
/// When a schema evolution occurs (e.g. ADD COLUMN, ALTER COLUMN TYPE), a new schema file
/// is written with an incremented ID. Data files record which schema they were written with
/// via `DataFileMeta.schema_id`.
///
/// The schema cache is shared across clones via `Arc`, so multiple readers
/// (e.g. parallel split streams) benefit from a single cache.
///
/// Reference: [org.apache.paimon.schema.SchemaManager](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java)
#[derive(Debug, Clone)]
pub struct SchemaManager {
    file_io: FileIO,
    table_path: String,
    /// Shared cache of loaded schemas by ID.
    cache: Arc<Mutex<HashMap<i64, Arc<TableSchema>>>>,
    /// Shared history analysis by current schema ID. A `OnceCell` prevents
    /// concurrent writers/readers from repeating the same remote schema LIST.
    evolution_cache: Arc<Mutex<HashMap<i64, SchemaEvolutionCell>>>,
}

impl SchemaManager {
    pub fn new(file_io: FileIO, table_path: String) -> Self {
        Self {
            file_io,
            table_path,
            cache: Arc::new(Mutex::new(HashMap::new())),
            evolution_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Path to the schema directory (e.g. `{table_path}/schema`).
    fn schema_directory(&self) -> String {
        format!("{}/{}", self.table_path.trim_end_matches('/'), SCHEMA_DIR)
    }

    /// Create a SchemaManager for a branch of this table.
    pub fn with_branch(&self, branch_name: &str) -> Self {
        let branch_path = format!(
            "{}/branch/branch-{}",
            self.table_path.trim_end_matches('/'),
            branch_name
        );
        Self::new(self.file_io.clone(), branch_path)
    }

    /// Path to a specific schema file (e.g. `{table_path}/schema/schema-0`).
    pub fn schema_path(&self, schema_id: i64) -> String {
        format!("{}/{}{}", self.schema_directory(), SCHEMA_PREFIX, schema_id)
    }

    /// List all schema ids sorted ascending. Returns an empty vector if the
    /// schema directory is missing or empty.
    ///
    /// Mirrors Java [SchemaManager.listAllIds()](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java).
    pub async fn list_all_ids(&self) -> crate::Result<Vec<i64>> {
        let mut ids: Vec<i64> = self
            .file_io
            .list_status(&self.schema_directory())
            .await?
            .into_iter()
            .filter(|s| !s.is_dir)
            .filter_map(|s| {
                get_basename(s.path.as_str())
                    .strip_prefix(SCHEMA_PREFIX)?
                    .parse::<i64>()
                    .ok()
            })
            .collect();
        ids.sort_unstable();
        Ok(ids)
    }

    /// List all schemas sorted by id ascending.
    ///
    /// Mirrors Java [SchemaManager.listAll()](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java).
    pub async fn list_all(&self) -> crate::Result<Vec<Arc<TableSchema>>> {
        let ids = self.list_all_ids().await?;
        try_join_all(ids.into_iter().map(|id| self.schema(id))).await
    }

    /// Return the schema with the highest id, or `None` when no schema files
    /// exist under the schema directory.
    ///
    /// Mirrors Java [SchemaManager.latest()](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java).
    pub async fn latest(&self) -> crate::Result<Option<Arc<TableSchema>>> {
        let ids = self.list_all_ids().await?;
        match ids.last() {
            Some(&max_id) => Ok(Some(self.schema(max_id).await?)),
            None => Ok(None),
        }
    }

    /// Load a schema by ID. Returns cached version if available.
    ///
    /// The cache is shared across all clones of this `SchemaManager`, so loading
    /// a schema in one stream makes it available to all other streams reading
    /// from the same table.
    ///
    /// Reference: [SchemaManager.schema(long)](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java)
    pub async fn schema(&self, schema_id: i64) -> crate::Result<Arc<TableSchema>> {
        // Fast path: check cache under a short lock.
        {
            let cache = self.cache.lock().unwrap();
            if let Some(schema) = cache.get(&schema_id) {
                return Ok(schema.clone());
            }
        }

        // Cache miss — load from file (no lock held during I/O).
        let path = self.schema_path(schema_id);
        let input = self.file_io.new_input(&path)?;
        let bytes = input.read().await?;
        let schema: TableSchema =
            serde_json::from_slice(&bytes).map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to parse schema file: {path}"),
                source: Some(Box::new(e)),
            })?;
        let schema = Arc::new(schema);

        // Insert into shared cache (short lock).
        {
            let mut cache = self.cache.lock().unwrap();
            cache.entry(schema_id).or_insert_with(|| schema.clone());
        }

        Ok(schema)
    }

    /// Return type-evolved field IDs for `current_schema`, or `None` when its
    /// preceding schema history is incomplete. Results are cached by current
    /// schema ID and shared across `SchemaManager` clones.
    pub async fn type_evolved_field_ids(
        &self,
        current_schema: &TableSchema,
    ) -> crate::Result<Option<Arc<HashSet<i32>>>> {
        let info = self.schema_evolution_info(current_schema).await?;
        Ok(info
            .history_complete
            .then(|| Arc::clone(&info.type_evolved_field_ids)))
    }

    /// Return whether a global index was encoded with field types compatible
    /// with `current_schema`.
    ///
    /// New index entries carry their build schema ID. Legacy entries remain
    /// usable only when complete history proves that none of their dependent
    /// fields has changed type.
    pub(crate) async fn global_index_schema_compatible(
        &self,
        current_schema: &TableSchema,
        global_index: &GlobalIndexMeta,
    ) -> crate::Result<bool> {
        let field_ids = std::iter::once(global_index.index_field_id)
            .chain(global_index.extra_field_ids.iter().flatten().copied())
            .collect::<Vec<_>>();

        if field_ids.iter().any(|field_id| {
            current_schema
                .fields()
                .iter()
                .all(|field| field.id() != *field_id)
        }) {
            return Ok(false);
        }

        let Some(build_schema_id) = global_index.build_schema_id else {
            let Some(evolved_field_ids) = self.type_evolved_field_ids(current_schema).await? else {
                return Ok(false);
            };
            return Ok(field_ids
                .iter()
                .all(|field_id| !evolved_field_ids.contains(field_id)));
        };

        if build_schema_id == current_schema.id() {
            return Ok(true);
        }
        let build_schema = self.schema(build_schema_id).await?;
        Ok(field_ids.iter().all(|field_id| {
            let build_field = build_schema
                .fields()
                .iter()
                .find(|field| field.id() == *field_id);
            let current_field = current_schema
                .fields()
                .iter()
                .find(|field| field.id() == *field_id);
            matches!(
                (build_field, current_field),
                (Some(build), Some(current))
                    if same_type_ignoring_nullability(
                        build.data_type(),
                        current.data_type()
                    )
            )
        }))
    }

    /// Return fields that need target storage normalization after a supported
    /// type evolution. Unlike predicate planning, writer behavior does not
    /// require a complete history and preserves the previous best-effort scan.
    pub(crate) async fn storage_normalization_field_ids(
        &self,
        current_schema: &TableSchema,
    ) -> crate::Result<Arc<HashSet<i32>>> {
        let info = self.schema_evolution_info(current_schema).await?;
        Ok(Arc::clone(&info.storage_normalization_field_ids))
    }

    async fn schema_evolution_info(
        &self,
        current_schema: &TableSchema,
    ) -> crate::Result<Arc<SchemaEvolutionInfo>> {
        let cell = {
            let mut cache = self.evolution_cache.lock().unwrap();
            Arc::clone(
                cache
                    .entry(current_schema.id())
                    .or_insert_with(|| Arc::new(OnceCell::new())),
            )
        };
        let info = cell
            .get_or_try_init(|| self.load_schema_evolution_info(current_schema))
            .await?;
        Ok(Arc::clone(info))
    }

    async fn load_schema_evolution_info(
        &self,
        current_schema: &TableSchema,
    ) -> crate::Result<Arc<SchemaEvolutionInfo>> {
        if current_schema.id() == 0 {
            return Ok(Arc::new(SchemaEvolutionInfo {
                history_complete: true,
                type_evolved_field_ids: Arc::new(HashSet::new()),
                storage_normalization_field_ids: Arc::new(HashSet::new()),
            }));
        }

        let schemas = self.list_all().await?;
        let historical = schemas
            .iter()
            .filter(|schema| schema.id() < current_schema.id())
            .collect::<Vec<_>>();
        let history_complete = usize::try_from(current_schema.id())
            .ok()
            .is_some_and(|count| {
                historical.len() == count
                    && historical
                        .iter()
                        .enumerate()
                        .all(|(id, schema)| schema.id() == id as i64)
            });

        let mut type_evolved_field_ids = HashSet::new();
        let mut storage_normalization_field_ids = HashSet::new();
        for current_field in current_schema.fields() {
            for historical_schema in &historical {
                let Some(historical_field) = historical_schema
                    .fields()
                    .iter()
                    .find(|field| field.id() == current_field.id())
                else {
                    continue;
                };
                if same_type_ignoring_nullability(
                    historical_field.data_type(),
                    current_field.data_type(),
                ) {
                    continue;
                }
                type_evolved_field_ids.insert(current_field.id());
                if requires_schema_evolution_storage_normalization(current_field.data_type())
                    && schema_evolution_cast_implemented(
                        historical_field.data_type(),
                        current_field.data_type(),
                    )
                {
                    storage_normalization_field_ids.insert(current_field.id());
                }
            }
        }

        Ok(Arc::new(SchemaEvolutionInfo {
            history_complete,
            type_evolved_field_ids: Arc::new(type_evolved_field_ids),
            storage_normalization_field_ids: Arc::new(storage_normalization_field_ids),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        BigIntType, CharType, DataType, GlobalIndexMeta, IntType, Schema, SchemaChange,
    };
    use bytes::Bytes;

    fn memory_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    async fn write_schema_file(file_io: &FileIO, dir: &str, id: i64) {
        let schema = Schema::builder().build().unwrap();
        let table_schema = TableSchema::new(id, &schema);
        write_table_schema_file(file_io, dir, &table_schema).await;
    }

    async fn write_table_schema_file(file_io: &FileIO, dir: &str, table_schema: &TableSchema) {
        let json = serde_json::to_vec(&table_schema).unwrap();
        let path = format!("{dir}/{SCHEMA_PREFIX}{}", table_schema.id());
        let out = file_io.new_output(&path).unwrap();
        out.write(Bytes::from(json)).await.unwrap();
    }

    #[tokio::test]
    async fn list_all_ids_returns_empty_for_missing_directory() {
        let file_io = memory_file_io();
        let sm = SchemaManager::new(file_io, "memory:/list_missing".to_string());
        assert!(sm.list_all_ids().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_all_ids_returns_empty_for_empty_directory() {
        let file_io = memory_file_io();
        let table_path = "memory:/list_empty";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();

        let sm = SchemaManager::new(file_io, table_path.to_string());
        assert!(sm.list_all_ids().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_all_ids_sorts_ascending() {
        let file_io = memory_file_io();
        let table_path = "memory:/list_sorted";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        for id in [3, 0, 2, 1] {
            write_schema_file(&file_io, &dir, id).await;
        }

        let sm = SchemaManager::new(file_io, table_path.to_string());
        assert_eq!(sm.list_all_ids().await.unwrap(), vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn list_all_ids_ignores_unrelated_files() {
        let file_io = memory_file_io();
        let table_path = "memory:/list_filter";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        write_schema_file(&file_io, &dir, 0).await;
        // `schema-foo` starts with the prefix but is not an i64.
        let junk = file_io
            .new_output(&format!("{dir}/{SCHEMA_PREFIX}foo"))
            .unwrap();
        junk.write(Bytes::from("{}")).await.unwrap();
        // A completely unrelated file.
        let other = file_io.new_output(&format!("{dir}/README")).unwrap();
        other.write(Bytes::from("hi")).await.unwrap();

        let sm = SchemaManager::new(file_io, table_path.to_string());
        assert_eq!(sm.list_all_ids().await.unwrap(), vec![0]);
    }

    #[tokio::test]
    async fn list_all_loads_schemas_in_order() {
        let file_io = memory_file_io();
        let table_path = "memory:/list_all_load";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        for id in [0, 2, 1] {
            write_schema_file(&file_io, &dir, id).await;
        }

        let sm = SchemaManager::new(file_io, table_path.to_string());
        let schemas = sm.list_all().await.unwrap();
        let ids: Vec<i64> = schemas.iter().map(|s| s.id()).collect();
        assert_eq!(ids, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn latest_returns_none_when_no_schemas() {
        let file_io = memory_file_io();
        let sm = SchemaManager::new(file_io, "memory:/latest_none".to_string());
        assert!(sm.latest().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn latest_returns_max_id_schema() {
        let file_io = memory_file_io();
        let table_path = "memory:/latest_max";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        for id in [0, 5, 2] {
            write_schema_file(&file_io, &dir, id).await;
        }

        let sm = SchemaManager::new(file_io, table_path.to_string());
        let latest = sm.latest().await.unwrap().expect("latest");
        assert_eq!(latest.id(), 5);
    }

    #[tokio::test]
    async fn schema_evolution_info_is_cached_by_current_schema_id() {
        let file_io = memory_file_io();
        let table_path = "memory:/schema_evolution_info";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();

        let initial = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("name", DataType::Char(CharType::new(10).unwrap()))
                .build()
                .unwrap(),
        );
        let renamed = initial
            .apply_changes(vec![SchemaChange::rename_column(
                "name".to_string(),
                "renamed_name".to_string(),
            )])
            .unwrap();
        let evolved = renamed
            .apply_changes(vec![
                SchemaChange::update_column_type(
                    "id".to_string(),
                    DataType::BigInt(BigIntType::new()),
                ),
                SchemaChange::update_column_type(
                    "renamed_name".to_string(),
                    DataType::Char(CharType::new(5).unwrap()),
                ),
            ])
            .unwrap();
        for schema in [&initial, &renamed, &evolved] {
            write_table_schema_file(&file_io, &dir, schema).await;
        }

        let manager = SchemaManager::new(file_io, table_path.to_string());
        let rename_info = manager.schema_evolution_info(&renamed).await.unwrap();
        assert!(rename_info.history_complete);
        assert!(rename_info.type_evolved_field_ids.is_empty());
        assert!(rename_info.storage_normalization_field_ids.is_empty());

        let manager_clone = manager.clone();
        let (first, second) = tokio::join!(
            manager.schema_evolution_info(&evolved),
            manager_clone.schema_evolution_info(&evolved)
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(first.history_complete);
        assert_eq!(
            first.type_evolved_field_ids.as_ref(),
            &HashSet::from([evolved.fields()[0].id(), evolved.fields()[1].id()])
        );
        assert_eq!(
            first.storage_normalization_field_ids.as_ref(),
            &HashSet::from([evolved.fields()[1].id()])
        );

        let public_ids = manager
            .type_evolved_field_ids(&evolved)
            .await
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&public_ids, &first.type_evolved_field_ids));
    }

    #[tokio::test]
    async fn global_index_compatibility_uses_build_schema_field_types() {
        let file_io = memory_file_io();
        let table_path = "memory:/global_index_schema_compatibility";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();

        let initial = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("name", DataType::Char(CharType::new(10).unwrap()))
                .build()
                .unwrap(),
        );
        let current = initial
            .apply_changes(vec![
                SchemaChange::update_column_type(
                    "id".to_string(),
                    DataType::BigInt(BigIntType::new()),
                ),
                SchemaChange::add_column("unrelated".to_string(), DataType::Int(IntType::new())),
            ])
            .unwrap();
        for schema in [&initial, &current] {
            write_table_schema_file(&file_io, &dir, schema).await;
        }

        let manager = SchemaManager::new(file_io, table_path.to_string());
        let id_field_id = initial.fields()[0].id();
        let name_field_id = initial.fields()[1].id();
        let meta = |index_field_id, extra_field_ids, build_schema_id| GlobalIndexMeta {
            row_range_start: 0,
            row_range_end: 9,
            index_field_id,
            extra_field_ids,
            index_meta: None,
            source_meta: None,
            build_schema_id,
        };

        assert!(!manager
            .global_index_schema_compatible(&current, &meta(id_field_id, None, Some(initial.id())),)
            .await
            .unwrap());
        assert!(manager
            .global_index_schema_compatible(
                &current,
                &meta(name_field_id, None, Some(initial.id())),
            )
            .await
            .unwrap());
        assert!(!manager
            .global_index_schema_compatible(
                &current,
                &meta(name_field_id, Some(vec![id_field_id]), Some(initial.id())),
            )
            .await
            .unwrap());

        // Legacy entries have no build schema ID. Complete history still lets
        // unchanged fields use them, while evolved dependencies are rejected.
        assert!(!manager
            .global_index_schema_compatible(&current, &meta(id_field_id, None, None))
            .await
            .unwrap());
        assert!(manager
            .global_index_schema_compatible(&current, &meta(name_field_id, None, None))
            .await
            .unwrap());
    }
}
