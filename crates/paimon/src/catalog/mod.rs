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

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;

use async_trait::async_trait;
use chrono::Duration;

use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{RowType, SchemaChange, TableSchema};

/// This interface is responsible for reading and writing metadata such as database/table from a paimon catalog.
///
/// Impl References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/catalog/Catalog.java#L42>
#[async_trait]
pub trait Catalog: Send + Sync {
    const DEFAULT_DATABASE: &'static str = "default";
    const SYSTEM_TABLE_SPLITTER: &'static str = "$";
    const SYSTEM_DATABASE_NAME: &'static str = "sys";

    /// Returns the warehouse root path containing all database directories in this catalog.
    fn warehouse(&self) -> &str;

    /// Returns the catalog options.
    fn options(&self) -> &HashMap<String, String>;

    /// Returns the FileIO instance.
    fn file_io(&self) -> &FileIO;

    /// Lists all databases in this catalog.
    async fn list_databases(&self) -> Result<Vec<String>>;

    /// Checks if a database exists in this catalog.
    async fn database_exists(&self, database_name: &str) -> Result<bool>;

    /// Creates a new database.
    async fn create_database(
        &self,
        name: &str,
        ignore_if_exists: bool,
        properties: Option<HashMap<String, String>>,
    ) -> Result<()>;

    /// Loads database properties.
    async fn load_database_properties(&self, name: &str) -> Result<HashMap<String, String>>;

    /// Drops a database.
    async fn drop_database(
        &self,
        name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> Result<()>;

    /// Returns a Table instance for the specified identifier.
    async fn get_table(&self, identifier: &Identifier) -> Result<impl Table>;

    /// Lists all tables in the specified database.
    async fn list_tables(&self, database_name: &str) -> Result<Vec<String>>;

    /// Checks if a table exists.
    async fn table_exists(&self, identifier: &Identifier) -> Result<bool> {
        match self.get_table(identifier).await {
            Ok(_) => Ok(true),
            Err(e) => match e {
                crate::error::Error::TableNotExist { .. } => Ok(false),
                _ => Err(e),
            },
        }
    }

    /// Drops a table.
    async fn drop_table(&self, identifier: &Identifier, ignore_if_not_exists: bool) -> Result<()>;

    /// Creates a new table.
    async fn create_table(
        &self,
        identifier: &Identifier,
        schema: TableSchema,
        ignore_if_exists: bool,
    ) -> Result<()>;

    /// Renames a table.
    async fn rename_table(
        &self,
        from_table: &Identifier,
        to_table: &Identifier,
        ignore_if_not_exists: bool,
    ) -> Result<()>;

    /// Alters an existing table.
    async fn alter_table(
        &self,
        identifier: &Identifier,
        changes: Vec<SchemaChange>,
        ignore_if_not_exists: bool,
    ) -> Result<()>;

    /// Drops a partition from the specified table.
    async fn drop_partition(
        &self,
        identifier: &Identifier,
        partitions: &HashMap<String, String>,
    ) -> Result<()>;

    /// Returns whether this catalog is case-sensitive.
    fn case_sensitive(&self) -> bool {
        true
    }
}

/// Identifies an object in a catalog.
///
/// Impl References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/catalog/Identifier.java#L35>
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Identifier {
    database: String,
    table: String,
}

impl Identifier {
    pub const UNKNOWN_DATABASE: &'static str = "unknown";

    /// Create a new identifier.
    pub fn new(database: String, table: String) -> Self {
        Self { database, table }
    }

    /// Get the table name.
    pub fn database_name(&self) -> &str {
        &self.database
    }

    /// Get the table name.
    pub fn object_name(&self) -> &str {
        &self.table
    }

    /// Get the full name of the identifier.
    pub fn full_name(&self) -> String {
        if self.database == Self::UNKNOWN_DATABASE {
            self.table.clone()
        } else {
            format!("{}.{}", self.database, self.table)
        }
    }

    /// Get the full name of the identifier with a specified character.
    pub fn escaped_full_name(&self) -> String {
        self.escaped_full_name_with_char('`')
    }

    /// Get the full name of the identifier with a specified character.
    pub fn escaped_full_name_with_char(&self, escape_char: char) -> String {
        format!(
            "{0}{1}{0}.{0}{2}{0}",
            escape_char, self.database, self.table
        )
    }

    /// Create a new identifier.
    pub fn create(db: &str, table: &str) -> Self {
        Self::new(db.to_string(), table.to_string())
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.full_name())
    }
}

/// A table provides basic abstraction for a table type and table scan, and table read.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/table/Table.java#L41>
pub trait Table {
    // ================== Table Metadata =====================

    /// A name to identify this table.
    fn name(&self) -> &str;

    /// Returns the row type of this table.
    fn row_type(&self) -> &RowType;

    /// Partition keys of this table.
    fn partition_keys(&self) -> Vec<String>;

    /// Primary keys of this table.
    fn primary_keys(&self) -> Vec<String>;

    /// Options of this table.
    fn options(&self) -> HashMap<String, String>;

    /// Optional comment of this table.
    fn comment(&self) -> Option<&String>;

    // ================= Table Operations ====================

    /// Copy this table with adding dynamic options.
    fn copy(&self, dynamic_options: HashMap<String, String>) -> Box<dyn Table>;

    /// Rollback table's state to a specific snapshot.
    fn rollback_to(&mut self, snapshot_id: u64);

    /// Create a tag from given snapshot.
    fn create_tag(&mut self, tag_name: &str, from_snapshot_id: u64);

    fn create_tag_with_retention(
        &mut self,
        tag_name: &str,
        from_snapshot_id: u64,
        time_retained: Duration,
    );

    /// Create a tag from the latest snapshot.
    fn create_tag_from_latest(&mut self, tag_name: &str);

    fn create_tag_from_latest_with_retention(&mut self, tag_name: &str, time_retained: Duration);

    /// Delete a tag by name.
    fn delete_tag(&mut self, tag_name: &str);

    /// Rollback table's state to a specific tag.
    fn rollback_to_tag(&mut self, tag_name: &str);

    /// Create an empty branch.
    fn create_branch(&mut self, branch_name: &str);

    /// Create a branch from given snapshot.
    fn create_branch_from_snapshot(&mut self, branch_name: &str, snapshot_id: u64);

    /// Create a branch from given tag.
    fn create_branch_from_tag(&mut self, branch_name: &str, tag_name: &str);

    /// Delete a branch by branchName.
    fn delete_branch(&mut self, branch_name: &str);
}
