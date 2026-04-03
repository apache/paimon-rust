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

    /// Path to the schema directory (e.g. `{table_path}/schema`).
    fn schema_directory(&self) -> String {
        format!("{}/{}", self.table_path.trim_end_matches('/'), SCHEMA_DIR)
    }

    /// Path to a specific schema file (e.g. `{table_path}/schema/schema-0`).
    fn schema_path(&self, schema_id: i64) -> String {
        format!("{}/{}{}", self.schema_directory(), SCHEMA_PREFIX, schema_id)
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
