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

//! Filesystem catalog implementation for Apache Paimon.
//!
//! Reference: [org.apache.paimon.catalog.FileSystemCatalog](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/catalog/FileSystemCatalog.java)

use std::collections::HashMap;

use crate::catalog::{Catalog, Database, Identifier, DB_LOCATION_PROP, DB_SUFFIX};
use crate::error::{Error, Result};
use crate::io::FileIO;
use crate::spec::{Schema, TableSchema};
use crate::table::Table;
use async_trait::async_trait;
use bytes::Bytes;
use opendal::raw::get_basename;

/// Name of the schema directory under each table path.
const SCHEMA_DIR: &str = "schema";
/// Prefix for schema files (under the schema directory).
const SCHEMA_PREFIX: &str = "schema-";

fn make_path(parent: &str, child: &str) -> String {
    format!("{parent}/{child}")
}

/// Filesystem catalog implementation.
///
/// This catalog stores metadata on a filesystem with the following structure:
/// ```text
/// warehouse/
///   ├── database1.db/
///   │   ├── table1/
///   │   │   └── schema/
///   │   │       ├── schema-0
///   │   │       ├── schema-1
///   │   │       └── ...
///   │   └── table2/
///   │       └── schema/
///   │           ├── schema-0
///   │           └── ...
///   └── database2.db/
///       └── ...
/// ```
///
/// Reference: [org.apache.paimon.catalog.FileSystemCatalog](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/catalog/FileSystemCatalog.java)
#[derive(Clone, Debug)]
pub struct FileSystemCatalog {
    file_io: FileIO,
    warehouse: String,
}

#[allow(dead_code)]
impl FileSystemCatalog {
    /// Create a new filesystem catalog.
    ///
    /// # Arguments
    /// * `warehouse` - The root warehouse path
    pub fn new(warehouse: impl Into<String>) -> crate::Result<Self> {
        let warehouse = warehouse.into();
        Ok(Self {
            file_io: FileIO::from_path(warehouse.as_str())?.build()?,
            warehouse,
        })
    }

    /// Get the warehouse path.
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    /// Get the FileIO instance.
    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Get the path for a database (warehouse / `name` + [DB_SUFFIX]).
    fn database_path(&self, database_name: &str) -> String {
        make_path(
            &self.warehouse,
            format!("{database_name}{DB_SUFFIX}").as_str(),
        )
    }

    /// Get the path for a table (warehouse / `database.db` / table).
    fn table_path(&self, identifier: &Identifier) -> String {
        make_path(
            &self.database_path(identifier.database()),
            identifier.object(),
        )
    }

    /// Path to the schema directory under a table path.
    fn schema_dir_path(&self, table_path: &str) -> String {
        make_path(table_path, SCHEMA_DIR)
    }

    /// Get the schema file path for a given version (table_path/schema/schema-{version}).
    fn schema_file_path(&self, table_path: &str, schema_id: i64) -> String {
        make_path(
            make_path(table_path, SCHEMA_DIR).as_str(),
            format!("{SCHEMA_PREFIX}{schema_id}").as_str(),
        )
    }

    /// List directories in the given path.
    async fn list_directories(&self, path: &str) -> Result<Vec<String>> {
        let statuses = self.file_io.list_status(path).await?;
        let mut dirs = Vec::new();
        for status in statuses {
            if status.is_dir {
                if let Some(p) = get_basename(status.path.as_str())
                    // opendal get_basename will contain "/" for directory,
                    // we need to strip suffix to get the real base name
                    .strip_suffix("/")
                {
                    dirs.push(p.to_string());
                }
            }
        }
        Ok(dirs)
    }

    /// Load the latest schema for a table (highest schema-{version} file under table_path/schema).
    async fn load_latest_table_schema(&self, table_path: &str) -> Result<Option<TableSchema>> {
        let schema_dir = self.schema_dir_path(table_path);
        if !self.file_io.exists(&schema_dir).await? {
            return Ok(None);
        }
        let statuses = self.file_io.list_status(&schema_dir).await?;

        let latest_schema_id = statuses
            .into_iter()
            .filter(|s| !s.is_dir)
            .filter_map(|s| {
                get_basename(s.path.as_str())
                    .strip_prefix(SCHEMA_PREFIX)?
                    .parse::<i64>()
                    .ok()
            })
            .max();

        if let Some(schema_id) = latest_schema_id {
            let schema_path = self.schema_file_path(table_path, schema_id);
            let input_file = self.file_io.new_input(&schema_path)?;
            let content = input_file.read().await?;
            let schema: TableSchema =
                serde_json::from_slice(&content).map_err(|e| Error::DataInvalid {
                    message: format!("Failed to parse schema file: {schema_path}"),
                    source: Some(Box::new(e)),
                })?;
            return Ok(Some(schema));
        }

        Ok(None)
    }

    /// Save a table schema to a file.
    async fn save_table_schema(&self, table_path: &str, schema: &TableSchema) -> Result<()> {
        let schema_dir = self.schema_dir_path(table_path);
        self.file_io.mkdirs(&schema_dir).await?;
        let schema_path = self.schema_file_path(table_path, schema.id());
        let output_file = self.file_io.new_output(&schema_path)?;
        let content =
            Bytes::from(
                serde_json::to_string(schema).map_err(|e| Error::DataInvalid {
                    message: format!("Failed to serialize schema: {e}"),
                    source: Some(Box::new(e)),
                })?,
            );
        output_file.write(content).await?;
        Ok(())
    }

    /// Check if a database exists.
    async fn database_exists(&self, name: &str) -> Result<bool> {
        self.file_io.exists(&self.database_path(name)).await
    }

    /// Check if a table exists.
    async fn table_exists(&self, identifier: &Identifier) -> Result<bool> {
        self.file_io.exists(&self.table_path(identifier)).await
    }
}

#[async_trait]
impl Catalog for FileSystemCatalog {
    async fn list_databases(&self) -> Result<Vec<String>> {
        let dirs = self.list_directories(&self.warehouse).await?;
        Ok(dirs
            .into_iter()
            .filter_map(|name| name.strip_suffix(DB_SUFFIX).map(|s| s.to_string()))
            .collect())
    }

    async fn create_database(
        &self,
        name: &str,
        ignore_if_exists: bool,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        if properties.contains_key(DB_LOCATION_PROP) {
            return Err(Error::ConfigInvalid {
                message: "Cannot specify location for a database when using fileSystem catalog."
                    .to_string(),
            });
        }

        let path = self.database_path(name);
        let database_exists = self.database_exists(name).await?;

        if !ignore_if_exists && database_exists {
            return Err(Error::DatabaseAlreadyExist {
                database: name.to_string(),
            });
        }

        if !database_exists {
            self.file_io.mkdirs(&path).await?;
        }

        Ok(())
    }

    async fn get_database(&self, name: &str) -> Result<Database> {
        if !self.database_exists(name).await? {
            return Err(Error::DatabaseNotExist {
                database: name.to_string(),
            });
        }

        Ok(Database::new(name.to_string(), HashMap::new(), None))
    }

    async fn drop_database(
        &self,
        name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> Result<()> {
        let path = self.database_path(name);

        let database_exists = self.database_exists(name).await?;
        if !ignore_if_not_exists && !database_exists {
            return Err(Error::DatabaseNotExist {
                database: name.to_string(),
            });
        }

        if !database_exists {
            return Ok(());
        }

        let tables = self.list_directories(&path).await?;
        if !tables.is_empty() && !cascade {
            return Err(Error::DatabaseNotEmpty {
                database: name.to_string(),
            });
        }

        self.file_io.delete_dir(&path).await?;
        Ok(())
    }

    async fn get_table(&self, identifier: &Identifier) -> Result<Table> {
        let table_path = self.table_path(identifier);

        if !self.table_exists(identifier).await? {
            return Err(Error::TableNotExist {
                full_name: identifier.full_name(),
            });
        }

        let schema = self
            .load_latest_table_schema(&table_path)
            .await?
            .ok_or_else(|| Error::TableNotExist {
                full_name: identifier.full_name(),
            })?;

        Ok(Table::new(
            self.file_io.clone(),
            identifier.clone(),
            table_path,
            schema,
        ))
    }

    async fn list_tables(&self, database_name: &str) -> Result<Vec<String>> {
        let path = self.database_path(database_name);

        if !self.database_exists(database_name).await? {
            return Err(Error::DatabaseNotExist {
                database: database_name.to_string(),
            });
        }

        self.list_directories(&path).await
    }

    async fn create_table(
        &self,
        identifier: &Identifier,
        creation: Schema,
        ignore_if_exists: bool,
    ) -> Result<()> {
        let table_path = self.table_path(identifier);

        let table_exists = self.table_exists(identifier).await?;

        if !ignore_if_exists && table_exists {
            return Err(Error::TableAlreadyExist {
                full_name: identifier.full_name(),
            });
        }

        if !self.database_exists(identifier.database()).await? {
            return Err(Error::DatabaseNotExist {
                database: identifier.database().to_string(),
            });
        }

        // todo: consider with lock
        if !table_exists {
            self.file_io.mkdirs(&table_path).await?;
            let table_schema = TableSchema::new(0, &creation);
            self.save_table_schema(&table_path, &table_schema).await?;
        }

        Ok(())
    }

    async fn drop_table(&self, identifier: &Identifier, ignore_if_not_exists: bool) -> Result<()> {
        let table_path = self.table_path(identifier);

        let table_exists = self.table_exists(identifier).await?;

        if !ignore_if_not_exists && !table_exists {
            return Err(Error::TableNotExist {
                full_name: identifier.full_name(),
            });
        }

        if !table_exists {
            return Ok(());
        }

        self.file_io.delete_dir(&table_path).await?;
        Ok(())
    }

    async fn rename_table(
        &self,
        from: &Identifier,
        to: &Identifier,
        ignore_if_not_exists: bool,
    ) -> Result<()> {
        let from_path = self.table_path(from);
        let to_path = self.table_path(to);

        let table_exists = self.table_exists(from).await?;

        if !ignore_if_not_exists && !table_exists {
            return Err(Error::TableNotExist {
                full_name: from.full_name(),
            });
        }

        if !table_exists {
            return Ok(());
        }

        if self.table_exists(to).await? {
            return Err(Error::TableAlreadyExist {
                full_name: to.full_name(),
            });
        }

        self.file_io.rename(&from_path, &to_path).await?;
        Ok(())
    }

    async fn alter_table(
        &self,
        identifier: &Identifier,
        _changes: Vec<crate::spec::SchemaChange>,
        ignore_if_not_exists: bool,
    ) -> Result<()> {
        if !ignore_if_not_exists && !self.table_exists(identifier).await? {
            return Err(Error::TableNotExist {
                full_name: identifier.full_name(),
            });
        }

        // TODO: Implement alter table with schema versioning
        Err(Error::Unsupported {
            message: "Alter table is not yet implemented for filesystem catalog".to_string(),
        })
    }
}

#[cfg(test)]
#[cfg(not(windows))] // Skip on Windows due to path compatibility issues
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Returns a temp dir guard (keep alive for test duration) and a catalog using it as warehouse.
    fn create_test_catalog() -> (TempDir, FileSystemCatalog) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = temp_dir.path().to_str().unwrap().to_string();
        let catalog = FileSystemCatalog::new(warehouse).unwrap();
        (temp_dir, catalog)
    }

    fn testing_schema() -> Schema {
        Schema::builder()
            .column(
                "id",
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            )
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_database_operations() {
        let (_temp_dir, catalog) = create_test_catalog();

        // create and list
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        catalog
            .create_database("db2", false, HashMap::new())
            .await
            .unwrap();
        let databases = catalog.list_databases().await.unwrap();
        assert_eq!(databases.len(), 2);
        assert!(databases.contains(&"db1".to_string()));
        assert!(databases.contains(&"db2".to_string()));

        // create same db without ignore_if_exists -> error
        let result = catalog.create_database("db1", false, HashMap::new()).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::DatabaseAlreadyExist { .. })));

        // create same db with ignore_if_exists -> ok
        catalog
            .create_database("db1", true, HashMap::new())
            .await
            .unwrap();

        // create_database with location property -> error (filesystem catalog)
        let mut props = HashMap::new();
        props.insert(DB_LOCATION_PROP.to_string(), "/some/path".to_string());
        let result = catalog.create_database("dbx", false, props).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::ConfigInvalid { .. })));

        // drop empty database
        catalog.drop_database("db1", false, false).await.unwrap();
        let databases = catalog.list_databases().await.unwrap();
        assert_eq!(databases.len(), 1);
        assert!(databases.contains(&"db2".to_string()));

        // drop non-empty database without cascade -> error
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        catalog
            .create_table(&Identifier::new("db1", "table1"), testing_schema(), false)
            .await
            .unwrap();
        let result = catalog.drop_database("db1", false, false).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::DatabaseNotEmpty { .. })));

        // drop database with cascade
        catalog.drop_database("db1", false, true).await.unwrap();
        assert!(!catalog.database_exists("db1").await.unwrap());
    }

    #[tokio::test]
    async fn test_table_operations() {
        let (_temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();

        // create and list tables
        let schema = testing_schema();
        catalog
            .create_table(&Identifier::new("db1", "table1"), schema.clone(), false)
            .await
            .unwrap();
        catalog
            .create_table(&Identifier::new("db1", "table2"), schema, false)
            .await
            .unwrap();
        let tables = catalog.list_tables("db1").await.unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"table1".to_string()));
        assert!(tables.contains(&"table2".to_string()));

        // get_table and check schema
        let schema_with_name = Schema::builder()
            .column(
                "id",
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            )
            .column(
                "name",
                crate::spec::DataType::VarChar(crate::spec::VarCharType::string_type()),
            )
            .build()
            .unwrap();
        catalog
            .create_table(&Identifier::new("db1", "table3"), schema_with_name, false)
            .await
            .unwrap();
        let table = catalog
            .get_table(&Identifier::new("db1", "table3"))
            .await
            .unwrap();
        let table_schema = table.schema();
        assert_eq!(table_schema.id(), 0);
        assert_eq!(table_schema.fields().len(), 2);

        // drop table
        catalog
            .drop_table(&Identifier::new("db1", "table1"), false)
            .await
            .unwrap();
        let tables = catalog.list_tables("db1").await.unwrap();
        assert_eq!(tables.len(), 2);
        assert!(!tables.contains(&"table1".to_string()));
    }
}
