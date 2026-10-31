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

use crate::io::FileIO;
use crate::spec::TableSchema;
use futures::future::try_join_all;
use opendal::raw::get_basename;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const SCHEMA_DIR: &str = "schema";
const SCHEMA_PREFIX: &str = "schema-";

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
}

impl SchemaManager {
    pub fn new(file_io: FileIO, table_path: String) -> Self {
        Self {
            file_io,
            table_path,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Retain a trusted schema already resolved by this table's catalog.
    /// REST catalogs need not expose these definitions as filesystem files.
    pub(super) fn cache_resolved_schema(&self, schema: TableSchema) {
        self.cache
            .lock()
            .unwrap()
            .entry(schema.id())
            .or_insert_with(|| Arc::new(schema));
    }

    /// Carry immutable, already loaded metadata to a same-table read view.
    /// Copy the source under its lock before locking the destination so clones
    /// sharing one cache cannot deadlock each other.
    pub(super) fn inherit_cached_schemas(&self, source: &Self) -> crate::Result<()> {
        if self.table_path != source.table_path {
            return Err(crate::Error::DataInvalid {
                message: "Cannot share schema metadata across tables or branches".into(),
                source: None,
            });
        }
        let schemas = source.cache.lock().unwrap().clone();
        let mut cache = self.cache.lock().unwrap();
        for (id, schema) in schemas {
            cache.entry(id).or_insert(schema);
        }
        Ok(())
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
        // Catalog-resolved schemas can have synthetic timestamps. Enumerated
        // schema files are authoritative for the persisted schema history.
        let stored = Self::new(self.file_io.clone(), self.table_path.clone());
        try_join_all(ids.into_iter().map(|id| stored.schema(id))).await
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::Schema;
    use bytes::Bytes;

    fn memory_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    async fn write_schema_file(file_io: &FileIO, dir: &str, id: i64) {
        let schema = Schema::builder().build().unwrap();
        let table_schema = TableSchema::new(id, &schema);
        let json = serde_json::to_vec(&table_schema).unwrap();
        let path = format!("{dir}/{SCHEMA_PREFIX}{id}");
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
    async fn list_all_reads_persisted_schema_after_catalog_cache_seed() {
        let file_io = memory_file_io();
        let path = "memory:/persisted_schema";
        let sm = SchemaManager::new(file_io.clone(), path.to_string());
        let schema = Schema::builder().build().unwrap();
        let mut stored = serde_json::to_value(TableSchema::new(0, &schema)).unwrap();
        stored["timeMillis"] = serde_json::json!(1234);
        file_io
            .new_output(&sm.schema_path(0))
            .unwrap()
            .write(Bytes::from(serde_json::to_vec(&stored).unwrap()))
            .await
            .unwrap();
        let mut resolved = stored.clone();
        resolved["timeMillis"] = serde_json::json!(5678);
        sm.cache_resolved_schema(serde_json::from_value(resolved).unwrap());

        assert_eq!(sm.schema(0).await.unwrap().time_millis(), 5678);
        let schemas = sm.list_all().await.unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].time_millis(), 1234);
        assert_eq!(sm.schema(0).await.unwrap().time_millis(), 5678);
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
}
