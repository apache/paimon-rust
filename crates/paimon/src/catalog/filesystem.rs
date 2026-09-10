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
use crate::common::{CatalogOptions, Options};
use crate::error::{ConfigInvalidSnafu, Error, Result};
use crate::io::cache::{create_local_cache, LocalCache};
use crate::io::FileIO;
use crate::spec::{
    CoreOptions, Schema, TableSchema, TableType, INDEX_FILE_IN_DATA_FILE_DIR_OPTION,
    TABLE_TYPE_OPTION,
};
use crate::table::{ObjectTable, SchemaManager, Table};
use async_trait::async_trait;
use bytes::Bytes;
use opendal::raw::get_basename;
use snafu::OptionExt;

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
    options: Options,
    local_cache: Option<std::sync::Arc<LocalCache>>,
}

impl FileSystemCatalog {
    /// Create a new filesystem catalog from configuration options.
    ///
    /// # Arguments
    /// * `options` - Configuration options containing warehouse path and storage configs (S3, OSS, etc.)
    ///
    /// # Required Options
    /// * `warehouse` - The root warehouse path (e.g., `/path/to/warehouse`, `s3://bucket/warehouse`)
    ///
    /// # Example
    /// ```ignore
    /// use paimon::{FileSystemCatalog, Options, CatalogOptions};
    ///
    /// // Local filesystem
    /// let mut options = Options::new();
    /// options.set(CatalogOptions::WAREHOUSE, "/tmp/warehouse");
    /// let catalog = FileSystemCatalog::new(options)?;
    ///
    /// // S3 with credentials
    /// let mut options = Options::new();
    /// options.set(CatalogOptions::WAREHOUSE, "s3://bucket/warehouse");
    /// options.set("s3.access-key-id", "...");
    /// options.set("s3.secret-access-key", "...");
    /// let catalog = FileSystemCatalog::new(options)?;
    /// ```
    pub fn new(options: Options) -> crate::Result<Self> {
        let warehouse =
            options
                .get(CatalogOptions::WAREHOUSE)
                .cloned()
                .context(ConfigInvalidSnafu {
                    message: format!("Missing required option: {}", CatalogOptions::WAREHOUSE),
                })?;

        let local_cache = create_local_cache(&options)?;
        let mut file_io_builder =
            FileIO::from_path(&warehouse)?.with_props(options.to_map().iter());
        if let Some(local_cache) = &local_cache {
            file_io_builder = file_io_builder.with_local_cache(local_cache.clone());
        }
        let file_io = file_io_builder.build()?;

        Ok(Self {
            file_io,
            warehouse,
            options,
            local_cache,
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

    fn build_file_io(&self, path: &str) -> Result<FileIO> {
        let mut builder = FileIO::from_path(path)?.with_props(self.options.to_map().iter());
        if let Some(local_cache) = &self.local_cache {
            builder = builder.with_local_cache(local_cache.clone());
        }
        builder.build()
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

    /// Fetch the stored path and schema of an existing table, bypassing the
    /// engine-type guard in [`Self::build_table`]: routing and catalog servers
    /// need the declared type before deciding anything.
    pub async fn fetch_table_schema(
        &self,
        identifier: &Identifier,
    ) -> Result<(String, TableSchema)> {
        identifier.validate()?;

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

        Ok((table_path, schema))
    }

    fn build_table(
        &self,
        identifier: &Identifier,
        table_path: String,
        schema: TableSchema,
    ) -> Result<Table> {
        // Fail closed: constructed as Paimon, raw `get_table` paths (writes,
        // procedures, time travel) would misread it.
        let declared = CoreOptions::new(schema.options()).table_type()?;
        if declared.requires_table_engine() {
            return Err(Error::Unsupported {
                message: format!(
                    "table '{}' is declared '{declared}' and cannot be read as a Paimon \
                     table; only plain reads through a registered table engine are supported",
                    identifier.full_name()
                ),
            });
        }

        Ok(Table::new(
            self.file_io.clone(),
            identifier.clone(),
            table_path,
            schema,
            None,
        ))
    }

    /// Load the latest schema for a table (highest schema-{version} file under table_path/schema).
    async fn load_latest_table_schema(&self, table_path: &str) -> Result<Option<TableSchema>> {
        let manager = SchemaManager::new(self.file_io.clone(), table_path.to_string());
        Ok(manager.latest().await?.map(|arc| (*arc).clone()))
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
    ///
    /// Uses a trailing slash so that object stores (OSS, S3, etc.) correctly
    /// identify the path as a directory rather than a file.
    /// Without trailing slash, `op.exists("dir")` may return false on object
    /// stores even when `dir/` prefix contains objects.
    /// See: <https://docs.rs/opendal/latest/opendal/struct.Operator.html#method.stat>
    async fn database_exists(&self, name: &str) -> Result<bool> {
        self.file_io.exists_dir(&self.database_path(name)).await
    }

    /// Check if a table exists by verifying that at least one schema file is
    /// present under `{table_path}/schema/`.
    ///
    /// This mirrors Java's `AbstractCatalog.tableExistsInFileSystem` which
    /// checks `schema-0` first, then falls back to listing all schema IDs.
    /// A bare directory without schema files is NOT considered a valid table.
    async fn table_exists(&self, identifier: &Identifier) -> Result<bool> {
        let table_path = self.table_path(identifier);
        self.table_exists_in_filesystem(&table_path).await
    }

    /// Verify a table path contains valid schema metadata.
    ///
    /// Mirrors Java's `AbstractCatalog.tableExistsInFileSystem`:
    /// 1. Fast path — check if `schema/schema-0` exists.
    /// 2. Slow path — list schema directory for any `schema-{N}` file.
    ///
    /// Only a `NotFound` error from the schema directory is treated as
    /// "table does not exist." All other errors (permission, network, etc.)
    /// are propagated to avoid masking real failures.
    async fn table_exists_in_filesystem(&self, table_path: &str) -> Result<bool> {
        let schema_manager = SchemaManager::new(self.file_io.clone(), table_path.to_string());

        if self.file_io.exists(&schema_manager.schema_path(0)).await? {
            return Ok(true);
        }
        match schema_manager.list_all_ids().await {
            Ok(ids) => Ok(!ids.is_empty()),
            Err(Error::IoUnexpected { ref source, .. })
                if source.kind() == opendal::ErrorKind::NotFound =>
            {
                Ok(false)
            }
            Err(e) => Err(e),
        }
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
        Identifier::validate_database_name(name)?;

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
        Identifier::validate_database_name(name)?;

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
        Identifier::validate_database_name(name)?;

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

        let tables = self.list_tables(name).await?;
        if !tables.is_empty() && !cascade {
            return Err(Error::DatabaseNotEmpty {
                database: name.to_string(),
            });
        }

        self.file_io.delete_dir(&path).await?;
        Ok(())
    }

    async fn get_table(&self, identifier: &Identifier) -> Result<Table> {
        let (table_path, schema) = self.fetch_table_schema(identifier).await?;
        self.build_table(identifier, table_path, schema)
    }

    async fn load_table(&self, identifier: &Identifier) -> Result<crate::catalog::LoadedTable> {
        let (table_path, schema) = self.fetch_table_schema(identifier).await?;
        let options = CoreOptions::new(schema.options());
        let declared = options.table_type()?;
        if declared == crate::spec::TableType::ObjectTable {
            let object_path = options
                .path()
                .filter(|path| !path.trim().is_empty())
                .unwrap_or(&table_path);
            return crate::table::ObjectTable::try_new_with_default_location(
                self.build_file_io(object_path)?,
                identifier.clone(),
                &schema,
                Some(&table_path),
            )
            .map(crate::catalog::LoadedTable::Object);
        }
        if declared.requires_table_engine() {
            return crate::catalog::LoadedTable::external_with_fields(
                declared,
                schema.fields().to_vec(),
                &options,
                &identifier.full_name(),
            );
        }
        self.build_table(identifier, table_path, schema)
            .map(|table| crate::catalog::LoadedTable::Paimon(Box::new(table)))
    }

    async fn list_tables(&self, database_name: &str) -> Result<Vec<String>> {
        Identifier::validate_database_name(database_name)?;

        let path = self.database_path(database_name);

        if !self.database_exists(database_name).await? {
            return Err(Error::DatabaseNotExist {
                database: database_name.to_string(),
            });
        }

        let dirs = self.list_directories(&path).await?;
        let mut tables = Vec::new();
        for dir in dirs {
            let table_path = make_path(&path, &dir);
            if self.table_exists_in_filesystem(&table_path).await? {
                tables.push(dir);
            }
        }
        tables.sort();
        Ok(tables)
    }

    async fn create_table(
        &self,
        identifier: &Identifier,
        mut creation: Schema,
        ignore_if_exists: bool,
    ) -> Result<()> {
        identifier.validate()?;
        // Never persist a type nothing can load.
        let declared = CoreOptions::new(creation.options()).table_type()?;

        let table_path = self.table_path(identifier);
        if declared == TableType::ObjectTable {
            creation = ObjectTable::normalize_creation(&creation, &table_path)?;
        }

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
        identifier.validate()?;

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
        from.validate()?;
        to.validate()?;

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
        changes: Vec<crate::spec::SchemaChange>,
        ignore_if_not_exists: bool,
    ) -> Result<()> {
        identifier.validate()?;

        let table_path = self.table_path(identifier);
        if !self.table_exists(identifier).await? {
            if ignore_if_not_exists {
                return Ok(());
            }
            return Err(Error::TableNotExist {
                full_name: identifier.full_name(),
            });
        }

        let current = self
            .load_latest_table_schema(&table_path)
            .await?
            .ok_or_else(|| Error::TableNotExist {
                full_name: identifier.full_name(),
            })?;

        reject_immutable_option_changes(current.options(), &changes)?;

        let new_schema = current
            .apply_changes(changes)
            .map_err(|e| fill_table_name(e, identifier))?;
        self.save_table_schema(&table_path, &new_schema).await
    }
}

/// Options whose value is baked into the on-disk layout, so changing one on a
/// populated table strands the files already written under the old value.
///
/// Java rejects any alteration of an option annotated `@Immutable`
/// (`SchemaManager.checkAlterTableOption` against `CoreOptions.IMMUTABLE_OPTIONS`);
/// this mirrors the subset this crate acts on:
///
/// * `type` picks the reader, so flipping it strands a populated table behind a
///   reader that cannot see its data. Only case-insensitive no-ops pass, and a
///   snapshot is not required: a format table holds data without ever writing
///   one, so a snapshot check would not catch it. Java special-cases `type` the
///   same way (`SchemaManager.generateTableSchema`).
/// * `index-file-in-data-file-dir` picks the directory every bucket-local index
///   file is written to and read from, so flipping it hides every index file the
///   table already has. A change is rejected once the table exists, and a
///   `SetOption` repeating the stored value or a `RemoveOption` for an option that
///   is not set is let through, since neither moves anything.
///
/// Java is more permissive on the second one: it reaches `checkAlterTableOption`
/// only under `hasSnapshots && !unchanged`, so it allows the layout to be changed
/// until the first snapshot exists. That window is not safe here, and closing it
/// properly is not an index-path change: a writer created before the alter has
/// already cached the old layout, and a commit stamps the schema id it was built
/// with without checking whether the latest schema moved
/// (`TableCommit`), so it can publish the first snapshot after the layout was
/// flipped under it. Ordering schema publication against snapshot commit would fix
/// that for every option at once; until it exists, the layout is chosen at creation
/// and not afterwards.
fn reject_immutable_option_changes(
    current_options: &HashMap<String, String>,
    changes: &[crate::spec::SchemaChange],
) -> Result<()> {
    let current_type = current_options
        .get(TABLE_TYPE_OPTION)
        .map(String::as_str)
        .unwrap_or_else(|| TableType::default().as_str());
    for change in changes {
        match change {
            crate::spec::SchemaChange::SetOption { key, value }
                if key == TABLE_TYPE_OPTION && !value.eq_ignore_ascii_case(current_type) =>
            {
                return Err(Error::Unsupported {
                    message: format!(
                        "changing '{TABLE_TYPE_OPTION}' from '{current_type}' to '{value}' is not supported"
                    ),
                });
            }
            crate::spec::SchemaChange::RemoveOption { key } if key == TABLE_TYPE_OPTION => {
                return Err(Error::Unsupported {
                    message: format!("removing '{TABLE_TYPE_OPTION}' is not supported"),
                });
            }
            crate::spec::SchemaChange::SetOption { key, value }
                if key == INDEX_FILE_IN_DATA_FILE_DIR_OPTION
                    && current_options.get(key.as_str()) != Some(value) =>
            {
                return Err(Error::Unsupported {
                    message: format!(
                        "changing '{INDEX_FILE_IN_DATA_FILE_DIR_OPTION}' after the table exists \
                         is not supported: it selects the directory index files are written to, \
                         so the files already written would no longer be found"
                    ),
                });
            }
            crate::spec::SchemaChange::RemoveOption { key }
                if key == INDEX_FILE_IN_DATA_FILE_DIR_OPTION
                    && current_options.contains_key(key.as_str()) =>
            {
                return Err(Error::Unsupported {
                    message: format!(
                        "removing '{INDEX_FILE_IN_DATA_FILE_DIR_OPTION}' is not supported: \
                         it selects the directory index files are written to, so the files \
                         already written would no longer be found"
                    ),
                });
            }
            _ => {}
        }
    }
    Ok(())
}

/// `TableSchema::apply_changes` returns column errors without a table name;
/// fill in the identifier's full name so the message identifies the table.
fn fill_table_name(err: Error, identifier: &Identifier) -> Error {
    match err {
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

#[cfg(test)]
// Skip on Windows: these tests list directories, and opendal's `fs` lister
// panics (`StripPrefixError`) when listing under a drive-rooted path with the
// `root="/"` operator setup. Single-file ops work after the drive-letter fix;
// directory listing needs the opendal root rework tracked in #397.
#[cfg(not(windows))]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Returns a temp dir guard (keep alive for test duration) and a catalog using it as warehouse.
    fn create_test_catalog() -> (TempDir, FileSystemCatalog) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = temp_dir.path().to_str().unwrap().to_string();
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse);
        let catalog = FileSystemCatalog::new(options).unwrap();
        (temp_dir, catalog)
    }

    fn create_memory_catalog() -> FileSystemCatalog {
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, "memory:/warehouse");
        FileSystemCatalog::new(options).unwrap()
    }

    #[test]
    fn test_filesystem_catalog_builds_local_cache_from_catalog_options() {
        let warehouse = TempDir::new().unwrap();
        let mut options = Options::new();
        options.set(
            CatalogOptions::WAREHOUSE,
            warehouse.path().to_string_lossy(),
        );
        options.set(CatalogOptions::LOCAL_CACHE_ENABLED, "true");

        let catalog = FileSystemCatalog::new(options).unwrap();

        assert!(catalog.file_io().has_local_cache());
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
    async fn test_memory_database_directory_existence() {
        let catalog = create_memory_catalog();

        catalog
            .create_database("empty", false, HashMap::new())
            .await
            .unwrap();
        catalog.get_database("empty").await.unwrap();

        let result = catalog
            .create_database("empty", false, HashMap::new())
            .await;
        assert!(matches!(result, Err(Error::DatabaseAlreadyExist { .. })));

        catalog
            .file_io()
            .new_output("memory:/warehouse/markerless.db/table/data/file")
            .unwrap()
            .write(Bytes::from("data"))
            .await
            .unwrap();
        catalog.get_database("markerless").await.unwrap();
        assert!(catalog
            .list_databases()
            .await
            .unwrap()
            .contains(&"markerless".to_string()));
    }

    #[tokio::test]
    async fn test_drop_database_ignores_incomplete_table_directories() {
        let catalog = create_memory_catalog();
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        catalog
            .file_io()
            .mkdirs("memory:/warehouse/db1.db/incomplete")
            .await
            .unwrap();

        assert!(catalog.list_tables("db1").await.unwrap().is_empty());
        catalog.drop_database("db1", false, false).await.unwrap();
        assert!(!catalog.database_exists("db1").await.unwrap());
    }

    #[tokio::test]
    async fn test_create_database_should_reject_path_traversal_name() {
        let (temp_dir, catalog) = create_test_catalog();
        let escaped_path = temp_dir.path().join("escaped.db");

        let result = catalog
            .create_database("../escaped", false, HashMap::new())
            .await;

        assert!(matches!(result, Err(Error::IdentifierInvalid { .. })));
        assert!(!escaped_path.exists());
    }

    #[tokio::test]
    async fn test_drop_database_should_reject_path_traversal_name() {
        let (temp_dir, catalog) = create_test_catalog();
        let escaped_path = temp_dir.path().join("escaped.db");
        std::fs::create_dir(&escaped_path).unwrap();

        let result = catalog.drop_database("../escaped", false, true).await;

        assert!(matches!(result, Err(Error::IdentifierInvalid { .. })));
        assert!(escaped_path.exists());
    }

    #[tokio::test]
    async fn test_alter_table_cannot_change_the_declared_type() {
        use crate::spec::SchemaChange;

        let (_temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        let schema = Schema::builder()
            .column(
                "id",
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            )
            .build()
            .unwrap();
        let identifier = Identifier::new("db1", "t");
        catalog
            .create_table(&identifier, schema, false)
            .await
            .unwrap();

        for change in [
            SchemaChange::set_option("type".to_string(), "iceberg-table".to_string()),
            SchemaChange::set_option("type".to_string(), "delta-table".to_string()),
            SchemaChange::remove_option("type".to_string()),
        ] {
            let err = catalog
                .alter_table(&identifier, vec![change], false)
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");
        }
        assert!(catalog
            .alter_table(
                &identifier,
                vec![SchemaChange::set_option(
                    "type".to_string(),
                    "TABLE".to_string(),
                )],
                false,
            )
            .await
            .is_ok());
        catalog.get_table(&identifier).await.unwrap();
    }

    /// Give the table a first snapshot, so a test can assert that a rejection does
    /// not depend on whether one exists.
    async fn give_the_table_a_snapshot(catalog: &FileSystemCatalog, identifier: &Identifier) {
        use crate::spec::{CommitKind, Snapshot};
        use crate::table::SnapshotManager;
        let table_path = catalog.table_path(identifier);
        let snapshot = Snapshot::builder()
            .version(3)
            .id(1)
            .schema_id(0)
            .base_manifest_list("base-list".to_string())
            .delta_manifest_list("delta-list".to_string())
            .commit_user("test-user".to_string())
            .commit_identifier(0)
            .commit_kind(CommitKind::APPEND)
            .time_millis(1000)
            .build();
        assert!(
            SnapshotManager::new(catalog.file_io.clone(), table_path)
                .commit_snapshot(&snapshot)
                .await
                .unwrap(),
            "the fixture snapshot must be the table's first"
        );
    }

    async fn create_table_for_alter(
        catalog: &FileSystemCatalog,
        options: HashMap<String, String>,
    ) -> Identifier {
        catalog
            .create_database("db1", true, HashMap::new())
            .await
            .unwrap();
        let schema = Schema::builder()
            .column(
                "id",
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            )
            .options(options)
            .build()
            .unwrap();
        let identifier = Identifier::new("db1", "t");
        catalog
            .create_table(&identifier, schema, false)
            .await
            .unwrap();
        identifier
    }

    #[tokio::test]
    async fn test_alter_table_cannot_change_where_index_files_live() {
        use crate::spec::SchemaChange;

        // The option selects the directory every bucket-local index file is written
        // to and read from, and an index manifest records only a file name, so the
        // layout is chosen at creation and not afterwards. Java gates the same
        // rejection on snapshot existence; that window is not safe while a writer
        // created before the alter can still publish the first snapshot under the
        // old layout.
        let (_temp_dir, catalog) = create_test_catalog();
        let identifier = create_table_for_alter(
            &catalog,
            HashMap::from([(
                "index-file-in-data-file-dir".to_string(),
                "true".to_string(),
            )]),
        )
        .await;

        // No snapshot yet, and still rejected.
        for change in [
            SchemaChange::set_option(
                "index-file-in-data-file-dir".to_string(),
                "false".to_string(),
            ),
            SchemaChange::remove_option("index-file-in-data-file-dir".to_string()),
        ] {
            let err = catalog
                .alter_table(&identifier, vec![change.clone()], false)
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");
        }

        // Repeating the stored value moves nothing, so it stays idempotent.
        catalog
            .alter_table(
                &identifier,
                vec![SchemaChange::set_option(
                    "index-file-in-data-file-dir".to_string(),
                    "true".to_string(),
                )],
                false,
            )
            .await
            .unwrap();

        // Once written, the same rejections hold.
        give_the_table_a_snapshot(&catalog, &identifier).await;
        let err = catalog
            .alter_table(
                &identifier,
                vec![SchemaChange::set_option(
                    "index-file-in-data-file-dir".to_string(),
                    "false".to_string(),
                )],
                false,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn test_alter_table_index_layout_no_ops_are_allowed() {
        use crate::spec::SchemaChange;

        // A table that never stored the option: spelling out the default is still a
        // change, because the comparison is against the stored value and there is
        // none — the same literal comparison Java makes. Removing what is not there
        // moves nothing, so it passes.
        let (_temp_dir, catalog) = create_test_catalog();
        let identifier = create_table_for_alter(&catalog, HashMap::new()).await;

        let err = catalog
            .alter_table(
                &identifier,
                vec![SchemaChange::set_option(
                    "index-file-in-data-file-dir".to_string(),
                    "false".to_string(),
                )],
                false,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");

        catalog
            .alter_table(
                &identifier,
                vec![SchemaChange::remove_option(
                    "index-file-in-data-file-dir".to_string(),
                )],
                false,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_an_alter_cannot_strand_a_writer_that_started_before_it() {
        use crate::spec::{DataType, IntType, Schema, SchemaChange, VarCharType};

        // The interleaving the layout has to survive: a writer resolves the layout
        // when it is created and publishes its files later. If an ALTER could land in
        // between, the writer would place its hash index under the old layout while a
        // freshly loaded table resolved the manifest's file name under the new one.
        let (_temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db1", true, HashMap::new())
            .await
            .unwrap();
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("index-file-in-data-file-dir", "true")
            .build()
            .unwrap();
        let identifier = Identifier::new("db1", "stale_writer");
        catalog
            .create_table(&identifier, schema, false)
            .await
            .unwrap();

        let table = catalog.get_table(&identifier).await.unwrap();
        let builder = table.new_write_builder();
        let mut write = builder.new_write().unwrap();
        let arrow_schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
            arrow_schema::Field::new("name", arrow_schema::DataType::Utf8, false),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            arrow_schema,
            vec![
                std::sync::Arc::new(arrow_array::Int32Array::from(vec![1, 2]))
                    as arrow_array::ArrayRef,
                std::sync::Arc::new(arrow_array::StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        write.write_arrow_batch(&batch).await.unwrap();

        // Between the writer resolving the layout and publishing it.
        for change in [
            SchemaChange::set_option(
                "index-file-in-data-file-dir".to_string(),
                "false".to_string(),
            ),
            SchemaChange::remove_option("index-file-in-data-file-dir".to_string()),
        ] {
            let err = catalog
                .alter_table(&identifier, vec![change], false)
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");
        }

        let messages = write.prepare_commit().await.unwrap();
        let index_name = messages
            .iter()
            .flat_map(|message| message.new_index_files.iter())
            .map(|file| file.file_name.clone())
            .next()
            .expect("a dynamic-bucket write produces a hash index");
        builder.new_commit().commit(messages).await.unwrap();

        // A freshly loaded table must resolve the file where the writer put it.
        let reloaded = catalog.get_table(&identifier).await.unwrap();
        assert!(reloaded
            .schema()
            .core_options()
            .index_file_in_data_file_dir());
        let location = reloaded.location().trim_end_matches('/').to_string();
        assert!(
            catalog
                .file_io
                .exists(&format!("{location}/bucket-0/{index_name}"))
                .await
                .unwrap(),
            "the committed hash index must be in the bucket data-file directory"
        );
        assert!(
            !catalog
                .file_io
                .exists(&format!("{location}/index/{index_name}"))
                .await
                .unwrap(),
            "and not in the table index directory"
        );
    }

    #[tokio::test]
    async fn test_create_table_rejects_an_unknown_type() {
        let (_temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        let valid = Schema::builder()
            .column(
                "id",
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            )
            .build()
            .unwrap();
        let mut payload = serde_json::to_value(&valid).unwrap();
        payload["options"] = serde_json::json!({ "type": "delta-table" });
        let schema: Schema = serde_json::from_value(payload).unwrap();
        let identifier = Identifier::new("db1", "delta_t");

        let err = catalog
            .create_table(&identifier, schema, false)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Unsupported { ref message } if message.contains("delta-table")),
            "{err:?}"
        );
        assert!(!catalog.table_exists(&identifier).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_object_table_uses_fixed_schema_and_default_path() {
        use crate::catalog::LoadedTable;
        use crate::table::ObjectTable;

        let catalog = create_memory_catalog();
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        let identifier = Identifier::new("db1", "objects");
        let schema = Schema::builder()
            .column(
                "ignored",
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            )
            .option("type", "object-table")
            .build()
            .unwrap();

        catalog
            .create_table(&identifier, schema, false)
            .await
            .unwrap();

        let expected_path = "memory:/warehouse/db1.db/objects";
        let (_, stored) = catalog.fetch_table_schema(&identifier).await.unwrap();
        assert_eq!(stored.fields(), ObjectTable::fields());
        assert_eq!(
            stored.options().get(crate::spec::PATH_OPTION),
            Some(&expected_path.to_string())
        );

        let loaded = catalog.load_table(&identifier).await.unwrap();
        let LoadedTable::Object(table) = loaded else {
            panic!("expected a native object table, got {loaded:?}");
        };
        assert_eq!(table.location(), expected_path);

        let explicit_identifier = Identifier::new("db1", "explicit_objects");
        let explicit_path = "memory:/external/objects";
        let explicit_schema = Schema::builder()
            .column(
                "also_ignored",
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            )
            .option("type", "object-table")
            .option(crate::spec::PATH_OPTION, explicit_path)
            .build()
            .unwrap();
        catalog
            .create_table(&explicit_identifier, explicit_schema, false)
            .await
            .unwrap();
        let (_, stored) = catalog
            .fetch_table_schema(&explicit_identifier)
            .await
            .unwrap();
        assert_eq!(stored.fields(), ObjectTable::fields());
        assert_eq!(
            stored
                .options()
                .get(crate::spec::PATH_OPTION)
                .map(String::as_str),
            Some(explicit_path)
        );
        let loaded = catalog.load_table(&explicit_identifier).await.unwrap();
        let LoadedTable::Object(table) = loaded else {
            panic!("expected a native object table, got {loaded:?}");
        };
        assert_eq!(table.location(), explicit_path);
    }

    #[tokio::test]
    async fn test_load_legacy_object_table_without_path_uses_table_directory() {
        use crate::catalog::LoadedTable;

        let catalog = create_memory_catalog();
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        let identifier = Identifier::new("db1", "legacy_objects");
        let table_path = catalog.table_path(&identifier);
        catalog.file_io.mkdirs(&table_path).await.unwrap();
        let legacy = Schema::builder()
            .column(
                "ignored",
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            )
            .option("type", "object-table")
            .build()
            .unwrap();
        catalog
            .save_table_schema(&table_path, &TableSchema::new(0, &legacy))
            .await
            .unwrap();

        let loaded = catalog.load_table(&identifier).await.unwrap();
        let LoadedTable::Object(table) = loaded else {
            panic!("expected a native object table, got {loaded:?}");
        };
        assert_eq!(table.location(), table_path);
    }

    #[tokio::test]
    async fn test_stored_scan_selectors_block_routing() {
        use crate::catalog::LoadedTable;

        let (_temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();

        for (name, key, value) in [
            ("ice_v", "scan.version", "1"),
            ("ice_s", "scan.snapshot-id", "1"),
            ("ice_i", "incremental-between", "1,5"),
        ] {
            let schema = Schema::builder()
                .column(
                    "id",
                    crate::spec::DataType::Int(crate::spec::IntType::new()),
                )
                .option("type", "iceberg-table")
                .option(key, value)
                .build()
                .unwrap();
            let identifier = Identifier::new("db1", name);
            catalog
                .create_table(&identifier, schema, false)
                .await
                .unwrap();

            let err = catalog.load_table(&identifier).await.unwrap_err();
            assert!(matches!(err, Error::Unsupported { .. }), "{key}: {err:?}");
        }

        let schema = Schema::builder()
            .column(
                "id",
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            )
            .option("type", "iceberg-table")
            .build()
            .unwrap();
        let identifier = Identifier::new("db1", "ice_plain");
        catalog
            .create_table(&identifier, schema, false)
            .await
            .unwrap();
        let routed = catalog.load_table(&identifier).await.unwrap();
        assert!(matches!(routed, LoadedTable::External(_)), "{routed:?}");
    }

    #[tokio::test]
    async fn test_routing_cannot_skip_the_read_guards() {
        use crate::catalog::LoadedTable;
        use crate::spec::CoreOptions;

        // The only constructor of the `Engine` variant, so a third-party
        // catalog cannot route past these guards either.
        for (key, value) in [
            ("query-auth.enabled", "true"),
            ("scan.version", "1"),
            ("incremental-between", "1,5"),
        ] {
            let options = HashMap::from([(key.to_string(), value.to_string())]);
            let err = LoadedTable::external(
                TableType::IcebergTable,
                &CoreOptions::new(&options),
                "db1.t",
            )
            .unwrap_err();
            assert!(matches!(err, Error::Unsupported { .. }), "{key}: {err:?}");
        }

        let options = HashMap::new();
        assert!(LoadedTable::external(
            TableType::IcebergTable,
            &CoreOptions::new(&options),
            "db1.t",
        )
        .is_ok());
    }

    #[tokio::test]
    async fn test_types_without_a_paimon_reader_fail_closed() {
        let (_temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();

        for (name, declared) in [("obj_t", "object-table"), ("lance_t", "lance-table")] {
            let schema = Schema::builder()
                .column(
                    "id",
                    crate::spec::DataType::Int(crate::spec::IntType::new()),
                )
                .option("type", declared)
                .build()
                .unwrap();
            let identifier = Identifier::new("db1", name);
            catalog
                .create_table(&identifier, schema, false)
                .await
                .unwrap();

            let err = catalog.get_table(&identifier).await.unwrap_err();
            assert!(
                matches!(err, Error::Unsupported { ref message }
                    if message.contains("cannot be read as a Paimon table")),
                "{declared}: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_declared_engine_type_routes_and_fails_closed() {
        use crate::catalog::LoadedTable;

        let (_temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        let schema = Schema::builder()
            .column(
                "id",
                crate::spec::DataType::Int(crate::spec::IntType::new()),
            )
            .option("type", "iceberg-table")
            .build()
            .unwrap();
        let identifier = Identifier::new("db1", "ice_t");
        catalog
            .create_table(&identifier, schema, false)
            .await
            .unwrap();

        let routed = catalog.load_table(&identifier).await.unwrap();
        assert!(
            matches!(routed, LoadedTable::External(ref e) if e.declared() == TableType::IcebergTable),
            "{routed:?}"
        );

        let err = catalog.get_table(&identifier).await.unwrap_err();
        assert!(
            matches!(err, Error::Unsupported { ref message }
                if message.contains("cannot be read as a Paimon table")),
            "{err:?}"
        );
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

    #[tokio::test]
    async fn test_memory_table_existence_requires_schema() {
        let catalog = create_memory_catalog();
        let valid_table = Identifier::new("db1", "valid");
        let invalid_table = Identifier::new("db1", "invalid");
        let table_schema = TableSchema::new(1, &testing_schema());

        catalog
            .file_io()
            .new_output("memory:/warehouse/db1.db/valid/schema/schema-1")
            .unwrap()
            .write(Bytes::from(serde_json::to_string(&table_schema).unwrap()))
            .await
            .unwrap();
        catalog
            .file_io()
            .mkdirs("memory:/warehouse/db1.db/invalid")
            .await
            .unwrap();

        assert!(catalog.table_exists(&valid_table).await.unwrap());
        let table = catalog.get_table(&valid_table).await.unwrap();
        assert_eq!(table.schema().id(), 1);
        let tables = catalog.list_tables("db1").await.unwrap();
        assert!(tables.contains(&"valid".to_string()));
        assert!(!tables.contains(&"invalid".to_string()));

        assert!(!catalog.table_exists(&invalid_table).await.unwrap());
        let result = catalog.get_table(&invalid_table).await;
        assert!(matches!(result, Err(Error::TableNotExist { .. })));
    }

    #[tokio::test]
    async fn test_create_table_should_reject_path_traversal_object_name() {
        let (temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        let escaped_path = temp_dir.path().join("table_escape");

        let result = catalog
            .create_table(
                &Identifier::new("db1", "../../table_escape"),
                testing_schema(),
                false,
            )
            .await;

        assert!(matches!(result, Err(Error::IdentifierInvalid { .. })));
        assert!(!escaped_path.exists());
    }

    #[tokio::test]
    async fn test_rename_table_should_reject_path_traversal_target_name() {
        let (temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        let source = Identifier::new("db1", "source");
        catalog
            .create_table(&source, testing_schema(), false)
            .await
            .unwrap();
        let escaped_path = temp_dir.path().join("renamed_escape");

        let result = catalog
            .rename_table(
                &source,
                &Identifier::new("db1", "../../renamed_escape"),
                false,
            )
            .await;

        assert!(matches!(result, Err(Error::IdentifierInvalid { .. })));
        assert!(!escaped_path.exists());
        assert!(catalog.get_table(&source).await.is_ok());
    }

    #[tokio::test]
    async fn test_list_partitions_default_table_not_found_errors() {
        let (_temp_dir, catalog) = create_test_catalog();
        let id = Identifier::new("nope_db", "nope_table");
        let result = catalog.list_partitions(&id).await;
        assert!(
            matches!(
                result,
                Err(Error::DatabaseNotExist { .. } | Error::TableNotExist { .. })
            ),
            "expected TableNotExist/DatabaseNotExist, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_list_partitions_default_empty_table_returns_empty() {
        let (_temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        let id = Identifier::new("db1", "t1");
        catalog
            .create_table(&id, testing_schema(), false)
            .await
            .unwrap();
        let parts = catalog.list_partitions(&id).await.unwrap();
        assert!(
            parts.is_empty(),
            "table without snapshots should yield no partitions"
        );
    }

    /// Mirrors Java `CatalogTestBase.testListPartitionsPaged`: the default impl
    /// returns the same full result regardless of `max_results` / `page_token`.
    #[tokio::test]
    async fn test_list_partitions_paged_default_ignores_max_and_token() {
        let (_temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        let id = Identifier::new("db1", "t1");
        catalog
            .create_table(&id, testing_schema(), false)
            .await
            .unwrap();
        for (max_results, page_token) in [
            (None, None),
            (Some(2), None),
            (Some(2), Some("dt=20250101")),
            (Some(8), None),
            (Some(8), Some("dt=20250101")),
        ] {
            let page = catalog
                .list_partitions_paged(&id, max_results, page_token)
                .await
                .unwrap();
            assert!(
                page.elements.is_empty(),
                "empty table → empty page for max_results={max_results:?}, page_token={page_token:?}"
            );
            assert!(
                page.next_page_token.is_none(),
                "default impl never paginates"
            );
        }
    }

    use crate::spec::{
        ColumnMove, DataField, DataType, IntType, RowType, SchemaChange, VarCharType,
    };

    /// Two-column table (id INT, name VARCHAR) used by the alter-table tests.
    fn two_column_schema() -> Schema {
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .build()
            .unwrap()
    }

    async fn setup_table(catalog: &FileSystemCatalog, schema: Schema) -> Identifier {
        catalog
            .create_database("db", false, HashMap::new())
            .await
            .unwrap();
        let id = Identifier::new("db", "t");
        catalog.create_table(&id, schema, false).await.unwrap();
        id
    }

    #[tokio::test]
    async fn test_alter_table_column_changes() {
        let (_tmp, catalog) = create_test_catalog();
        let id = setup_table(&catalog, two_column_schema()).await;

        // Add a column at the end; it must take highest_field_id + 1.
        catalog
            .alter_table(
                &id,
                vec![SchemaChange::add_column(
                    "age".to_string(),
                    DataType::Int(IntType::new()),
                )],
                false,
            )
            .await
            .unwrap();
        let ts = catalog.get_table(&id).await.unwrap();
        let ts = ts.schema();
        let names: Vec<&str> = ts.fields().iter().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id", "name", "age"]);
        let age = ts.fields().iter().find(|f| f.name() == "age").unwrap();
        assert_eq!(age.id(), 2, "new column gets highest_field_id + 1");
        assert_eq!(ts.id(), 1, "schema version bumped");

        // Add a column moved to the front.
        catalog
            .alter_table(
                &id,
                vec![SchemaChange::add_column_with_description_and_column_move(
                    "rowkey".to_string(),
                    DataType::Int(IntType::new()),
                    "primary".to_string(),
                    ColumnMove::move_first("rowkey".to_string()),
                )],
                false,
            )
            .await
            .unwrap();
        let ts = catalog.get_table(&id).await.unwrap();
        let ts = ts.schema();
        assert_eq!(ts.fields()[0].name(), "rowkey");
        assert_eq!(ts.fields()[0].description(), Some("primary"));

        // Converting nullable `id` to NOT NULL below is rejected by default;
        // allow it explicitly. The flag is read from the pre-alter options, so
        // it must be set in a separate alter.
        catalog
            .alter_table(
                &id,
                vec![SchemaChange::set_option(
                    "alter-column-null-to-not-null.disabled".to_string(),
                    "false".to_string(),
                )],
                false,
            )
            .await
            .unwrap();

        // Rename, update comment, update type, update nullability, drop.
        catalog
            .alter_table(
                &id,
                vec![
                    SchemaChange::rename_column("name".to_string(), "full_name".to_string()),
                    SchemaChange::update_column_comment("id".to_string(), "the id".to_string()),
                    SchemaChange::update_column_type(
                        "age".to_string(),
                        DataType::BigInt(crate::spec::BigIntType::new()),
                    ),
                    SchemaChange::update_column_nullability("id".to_string(), false),
                    SchemaChange::drop_column("rowkey".to_string()),
                ],
                false,
            )
            .await
            .unwrap();
        let ts = catalog.get_table(&id).await.unwrap();
        let ts = ts.schema();
        let names: Vec<&str> = ts.fields().iter().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id", "full_name", "age"]);
        let id_field = ts.fields().iter().find(|f| f.name() == "id").unwrap();
        assert_eq!(id_field.description(), Some("the id"));
        assert!(!id_field.data_type().is_nullable());
        let age_field = ts.fields().iter().find(|f| f.name() == "age").unwrap();
        assert!(matches!(age_field.data_type(), DataType::BigInt(_)));
    }

    #[tokio::test]
    async fn test_alter_table_reposition_column() {
        let (_tmp, catalog) = create_test_catalog();
        let id = setup_table(&catalog, two_column_schema()).await;

        // Move `name` before `id`.
        catalog
            .alter_table(
                &id,
                vec![SchemaChange::update_column_position(
                    ColumnMove::move_first("name".to_string()),
                )],
                false,
            )
            .await
            .unwrap();
        let ts = catalog.get_table(&id).await.unwrap();
        let names: Vec<&str> = ts.schema().fields().iter().map(|f| f.name()).collect();
        assert_eq!(names, vec!["name", "id"]);
    }

    #[tokio::test]
    async fn test_alter_table_errors() {
        let (_tmp, catalog) = create_test_catalog();
        let id = setup_table(&catalog, two_column_schema()).await;

        // Add a duplicate column -> ColumnAlreadyExist.
        let err = catalog
            .alter_table(
                &id,
                vec![SchemaChange::add_column(
                    "name".to_string(),
                    DataType::Int(IntType::new()),
                )],
                false,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::ColumnAlreadyExist { .. }),
            "got {err:?}"
        );

        // Drop a missing column -> ColumnNotExist.
        let err = catalog
            .alter_table(
                &id,
                vec![SchemaChange::drop_column("ghost".to_string())],
                false,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::ColumnNotExist { .. }), "got {err:?}");

        // Altering a missing table: ignored vs error.
        let missing = Identifier::new("db", "nope");
        catalog
            .alter_table(
                &missing,
                vec![SchemaChange::update_column_comment(
                    "id".to_string(),
                    "x".to_string(),
                )],
                true,
            )
            .await
            .unwrap();
        let err = catalog
            .alter_table(
                &missing,
                vec![SchemaChange::update_column_comment(
                    "id".to_string(),
                    "x".to_string(),
                )],
                false,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::TableNotExist { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn test_alter_table_drop_primary_key_column_rejected() {
        let (_tmp, catalog) = create_test_catalog();
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .build()
            .unwrap();
        let id = setup_table(&catalog, schema).await;

        let err = catalog
            .alter_table(
                &id,
                vec![SchemaChange::drop_column("id".to_string())],
                false,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn test_alter_table_add_not_null_column_rejected() {
        let (_tmp, catalog) = create_test_catalog();
        let id = setup_table(&catalog, two_column_schema()).await;

        let err = catalog
            .alter_table(
                &id,
                vec![SchemaChange::add_column(
                    "age".to_string(),
                    DataType::Int(IntType::with_nullable(false)),
                )],
                false,
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("cannot specify NOT NULL"),
            "got {err:?}"
        );
    }

    /// Collect the IDs of all fields nested inside row types.
    fn nested_row_field_ids(data_type: &DataType, ids: &mut Vec<i32>) {
        if let DataType::Row(row) = data_type {
            for f in row.fields() {
                ids.push(f.id());
                nested_row_field_ids(f.data_type(), ids);
            }
        }
    }

    #[tokio::test]
    async fn test_alter_table_add_nested_column_reassigns_field_ids() {
        let (_tmp, catalog) = create_test_catalog();
        // Existing columns take IDs 0 and 1.
        let id = setup_table(&catalog, two_column_schema()).await;

        // Nested field IDs as requested by the caller deliberately collide
        // with the existing columns; they must all be reassigned.
        let nested = DataType::Row(RowType::new(vec![
            DataField::new(0, "a".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "b".to_string(),
                DataType::Row(RowType::new(vec![DataField::new(
                    2,
                    "c".to_string(),
                    DataType::Int(IntType::new()),
                )])),
            ),
        ]));
        catalog
            .alter_table(
                &id,
                vec![SchemaChange::add_column("s".to_string(), nested)],
                false,
            )
            .await
            .unwrap();

        let table = catalog.get_table(&id).await.unwrap();
        let ts = table.schema();
        let s = ts.fields().iter().find(|f| f.name() == "s").unwrap();
        assert_eq!(s.id(), 2, "top-level column takes highest_field_id + 1");
        let mut ids = Vec::new();
        nested_row_field_ids(s.data_type(), &mut ids);
        ids.sort_unstable();
        assert_eq!(ids, vec![3, 4, 5], "nested IDs are fresh and unique");
        assert_eq!(ts.highest_field_id(), 5);
    }

    #[tokio::test]
    async fn test_alter_table_drop_all_columns_rejected() {
        let (_tmp, catalog) = create_test_catalog();
        let id = setup_table(&catalog, two_column_schema()).await;

        let err = catalog
            .alter_table(
                &id,
                vec![
                    SchemaChange::drop_column("id".to_string()),
                    SchemaChange::drop_column("name".to_string()),
                ],
                false,
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Cannot drop all fields"),
            "got {err:?}"
        );
    }

    /// Partitioned primary-key table: dt VARCHAR (partition), id INT, v INT,
    /// primary key (dt, id).
    fn partitioned_pk_schema() -> Schema {
        Schema::builder()
            .column("dt", DataType::VarChar(VarCharType::string_type()))
            .column("id", DataType::Int(IntType::new()))
            .column("v", DataType::Int(IntType::new()))
            .partition_keys(["dt"])
            .primary_key(["dt", "id"])
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_alter_table_key_column_guards() {
        let (_tmp, catalog) = create_test_catalog();
        let id = setup_table(&catalog, partitioned_pk_schema()).await;

        // Renaming a partition column is rejected.
        let err = catalog
            .alter_table(
                &id,
                vec![SchemaChange::rename_column(
                    "dt".to_string(),
                    "day".to_string(),
                )],
                false,
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Cannot rename partition column"),
            "got {err:?}"
        );

        // Updating the type of a partition column is rejected.
        let err = catalog
            .alter_table(
                &id,
                vec![SchemaChange::update_column_type(
                    "dt".to_string(),
                    DataType::VarChar(VarCharType::string_type()),
                )],
                false,
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Cannot update partition column"),
            "got {err:?}"
        );

        // Updating the type of a primary key column is rejected.
        let err = catalog
            .alter_table(
                &id,
                vec![SchemaChange::update_column_type(
                    "id".to_string(),
                    DataType::BigInt(crate::spec::BigIntType::new()),
                )],
                false,
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Cannot update primary key"),
            "got {err:?}"
        );

        // Making a primary key column nullable is rejected ...
        let err = catalog
            .alter_table(
                &id,
                vec![SchemaChange::update_column_nullability(
                    "id".to_string(),
                    true,
                )],
                false,
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Cannot change nullability of primary key"),
            "got {err:?}"
        );

        // ... while NOT NULL stays allowed (it is already non-nullable).
        catalog
            .alter_table(
                &id,
                vec![SchemaChange::update_column_nullability(
                    "id".to_string(),
                    false,
                )],
                false,
            )
            .await
            .unwrap();
    }

    // ─── table_exists_in_filesystem regression tests ─────────────────────────

    /// When `schema-0` is absent but another schema file (e.g. `schema-5`) exists,
    /// the slow-path fallback via `list_all_ids` should detect the table.
    #[tokio::test]
    async fn test_table_exists_schema_fallback_without_schema_0() {
        let (temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db", false, HashMap::new())
            .await
            .unwrap();

        let table_path = format!(
            "{}/{}/mytable",
            temp_dir.path().to_str().unwrap(),
            "db.db"
        );
        let schema_dir = format!("{table_path}/schema");
        std::fs::create_dir_all(&schema_dir).unwrap();

        // Write only schema-5 (skip schema-0) to trigger the slow path.
        let schema = testing_schema();
        let table_schema = TableSchema::new(5, &schema);
        let json = serde_json::to_string(&table_schema).unwrap();
        std::fs::write(format!("{schema_dir}/schema-5"), json).unwrap();

        assert!(
            catalog
                .table_exists_in_filesystem(&table_path)
                .await
                .unwrap(),
            "table with schema-5 (but no schema-0) should be detected via fallback"
        );
    }

    /// A directory without any `schema-{N}` file (markerless prefix) should NOT
    /// be recognized as a valid table.
    #[tokio::test]
    async fn test_table_exists_returns_false_for_markerless_directory() {
        let (temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db", false, HashMap::new())
            .await
            .unwrap();

        let table_path = format!("{}/{}/bare_dir", temp_dir.path().to_str().unwrap(), "db.db");
        // Create the directory but no schema/ subdirectory at all.
        std::fs::create_dir_all(&table_path).unwrap();

        assert!(
            !catalog
                .table_exists_in_filesystem(&table_path)
                .await
                .unwrap(),
            "directory without schema/ should not be treated as a table"
        );
    }

    /// A directory with an empty `schema/` subdirectory (no schema-N files)
    /// should NOT be recognized as a valid table.
    #[tokio::test]
    async fn test_table_exists_returns_false_for_empty_schema_dir() {
        let (temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db", false, HashMap::new())
            .await
            .unwrap();

        let table_path = format!(
            "{}/{}/empty_schema",
            temp_dir.path().to_str().unwrap(),
            "db.db"
        );
        let schema_dir = format!("{table_path}/schema");
        std::fs::create_dir_all(&schema_dir).unwrap();

        assert!(
            !catalog
                .table_exists_in_filesystem(&table_path)
                .await
                .unwrap(),
            "directory with empty schema/ should not be treated as a table"
        );
    }

    /// `list_tables` must filter out directories that exist but lack valid schema
    /// metadata (invalid-directory filtering).
    #[tokio::test]
    async fn test_list_tables_filters_invalid_directories() {
        let (temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db", false, HashMap::new())
            .await
            .unwrap();

        // Create a valid table via the catalog API.
        let id = Identifier::new("db", "valid_table");
        catalog
            .create_table(&id, testing_schema(), false)
            .await
            .unwrap();

        // Manually create bare directories (markerless prefixes) that should be
        // excluded from the listing.
        let db_path = format!("{}/{}", temp_dir.path().to_str().unwrap(), "db.db");
        std::fs::create_dir_all(format!("{db_path}/no_schema_at_all")).unwrap();
        std::fs::create_dir_all(format!("{db_path}/has_empty_schema/schema")).unwrap();

        let tables = catalog.list_tables("db").await.unwrap();
        assert_eq!(
            tables,
            vec!["valid_table".to_string()],
            "only directories with valid schema files should be listed"
        );
    }

    /// Non-`NotFound` errors (e.g. permission denied) from the schema directory
    /// must be propagated, not swallowed as "table does not exist".
    #[cfg(unix)]
    #[tokio::test]
    async fn test_table_exists_propagates_non_not_found_error() {
        use std::os::unix::fs::PermissionsExt;

        let (temp_dir, catalog) = create_test_catalog();
        catalog
            .create_database("db", false, HashMap::new())
            .await
            .unwrap();

        let table_path = format!(
            "{}/{}/forbidden",
            temp_dir.path().to_str().unwrap(),
            "db.db"
        );
        let schema_dir = format!("{table_path}/schema");
        std::fs::create_dir_all(&schema_dir).unwrap();

        // Remove all permissions so listing will fail with PermissionDenied.
        std::fs::set_permissions(&schema_dir, std::fs::Permissions::from_mode(0o000)).unwrap();

        // Probe whether the OS actually enforces the restriction (root bypasses).
        let enforced = std::fs::read_dir(&schema_dir).is_err();

        let result = catalog.table_exists_in_filesystem(&table_path).await;

        // Restore permissions before asserting so cleanup can succeed.
        std::fs::set_permissions(&schema_dir, std::fs::Permissions::from_mode(0o755)).unwrap();

        if enforced {
            assert!(
                result.is_err(),
                "permission error should be propagated, not treated as 'table not found'; got {result:?}"
            );
        }
    }

    // ─── end table_exists_in_filesystem regression tests ────────────────────────

    #[tokio::test]
    async fn test_alter_table_rename_primary_key_column_propagates() {
        let (_tmp, catalog) = create_test_catalog();
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .column("v", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket-key", "id")
            .option("sequence.field", "v")
            .build()
            .unwrap();
        let id = setup_table(&catalog, schema).await;

        catalog
            .alter_table(
                &id,
                vec![
                    SchemaChange::rename_column("id".to_string(), "user_id".to_string()),
                    SchemaChange::rename_column("v".to_string(), "ver".to_string()),
                ],
                false,
            )
            .await
            .unwrap();

        let table = catalog.get_table(&id).await.unwrap();
        let ts = table.schema();
        assert_eq!(ts.primary_keys(), ["user_id".to_string()]);
        assert_eq!(
            ts.options().get("bucket-key").map(String::as_str),
            Some("user_id")
        );
        assert_eq!(
            ts.options().get("sequence.field").map(String::as_str),
            Some("ver")
        );
    }
}
