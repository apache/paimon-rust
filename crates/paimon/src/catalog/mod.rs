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

//! Catalog API for Apache Paimon.
//!
//! Design aligns with [Paimon Java Catalog](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/catalog/Catalog.java)
//! and follows API patterns from Apache Iceberg Rust.

mod database;
mod factory;
mod filesystem;
mod rest;

use std::collections::HashMap;
use std::fmt;

pub use database::*;
pub use factory::*;
pub use filesystem::*;
pub use rest::*;
use serde::{Deserialize, Serialize};

/// Splitter for system table names (e.g. `table$snapshots`).
#[allow(dead_code)]
pub const SYSTEM_TABLE_SPLITTER: &str = "$";
/// Prefix for branch in object name (e.g. `table$branch_foo`).
#[allow(dead_code)]
pub const SYSTEM_BRANCH_PREFIX: &str = "branch_";
/// Default main branch name.
#[allow(dead_code)]
pub const DEFAULT_MAIN_BRANCH: &str = "main";
/// Database value when the database is not known; [`Identifier::full_name`] returns only the object.
pub const UNKNOWN_DATABASE: &str = "unknown";
/// Database property key for custom location. Not allowed for filesystem catalog.
/// See [Catalog.DB_LOCATION_PROP](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/catalog/Catalog.java).
#[allow(dead_code)] // Public API - allow unused until used by external code
pub const DB_LOCATION_PROP: &str = "location";
/// Suffix for database directory names in the filesystem (e.g. `mydb.db`).
/// See [Catalog.DB_SUFFIX](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/catalog/Catalog.java).
#[allow(dead_code)] // Public API - allow unused until used by external code
pub const DB_SUFFIX: &str = ".db";

// ======================= Identifier ===============================

/// Identifies a catalog object (e.g. a table) by database and object name.
///
/// Corresponds to [org.apache.paimon.catalog.Identifier](https://github.com/apache/paimon/blob/release-1.3/paimon-api/src/main/java/org/apache/paimon/catalog/Identifier.java).
/// The object name may be a table name or a qualified name like `table$branch_foo` or
/// `table$snapshots` for system tables.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Identifier {
    /// Database name.
    database: String,
    /// Object name (table name, or table$branch$system for system tables).
    object: String,
}

#[allow(dead_code)]
impl Identifier {
    /// Create an identifier from database and object name.
    pub fn new(database: impl Into<String>, object: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            object: object.into(),
        }
    }

    /// Database name.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Full object name (table name, or with branch/system suffix).
    pub fn object(&self) -> &str {
        &self.object
    }

    /// Full name: when database is [`UNKNOWN_DATABASE`], returns only the object;
    /// otherwise returns `database.object`.
    pub fn full_name(&self) -> String {
        if self.database == UNKNOWN_DATABASE {
            self.object.clone()
        } else {
            format!("{}.{}", self.database, self.object)
        }
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.full_name())
    }
}

impl fmt::Debug for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identifier")
            .field("database", &self.database)
            .field("object", &self.object)
            .finish()
    }
}

// ======================= Catalog trait ===============================

use async_trait::async_trait;

use crate::spec::{Schema, SchemaChange};
use crate::table::Table;
use crate::Result;

/// Catalog API for reading and writing metadata (databases, tables) in Paimon.
///
/// Corresponds to [org.apache.paimon.catalog.Catalog](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/catalog/Catalog.java).
#[async_trait]
#[allow(dead_code)]
pub trait Catalog: Send + Sync {
    // ======================= database methods ===============================

    /// List names of all databases in this catalog.
    ///
    /// # Errors
    /// Implementations may return other errors (e.g. I/O or backend-specific).
    async fn list_databases(&self) -> Result<Vec<String>>;

    /// Create a database.
    ///
    /// * `ignore_if_exists` - if true, do nothing when the database already exists;
    ///   if false, return [`crate::Error::DatabaseAlreadyExist`].
    ///
    /// # Errors
    /// * [`crate::Error::DatabaseAlreadyExist`] - database already exists when `ignore_if_exists` is false.
    async fn create_database(
        &self,
        name: &str,
        ignore_if_exists: bool,
        properties: HashMap<String, String>,
    ) -> Result<()>;

    /// Get a database by name.
    ///
    /// # Errors
    /// * [`crate::Error::DatabaseNotExist`] - database does not exist.
    async fn get_database(&self, name: &str) -> Result<Database>;

    /// Drop a database.
    ///
    /// * `ignore_if_not_exists` - if true, do nothing when the database does not exist.
    /// * `cascade` - if true, delete all tables in the database then delete the database;
    ///   if false, return [`crate::Error::DatabaseNotEmpty`] when not empty.
    ///
    /// # Errors
    /// * [`crate::Error::DatabaseNotExist`] - database does not exist when `ignore_if_not_exists` is false.
    /// * [`crate::Error::DatabaseNotEmpty`] - database is not empty when `cascade` is false.
    async fn drop_database(
        &self,
        name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> Result<()>;

    // ======================= table methods ===============================

    /// Get table metadata for the given identifier.
    ///
    /// # Errors
    /// * [`crate::Error::DatabaseNotExist`] - database in identifier does not exist.
    /// * [`crate::Error::TableNotExist`] - table does not exist.
    async fn get_table(&self, identifier: &Identifier) -> Result<Table>;

    /// List table names in a database. System tables are not listed.
    ///
    /// # Errors
    /// * [`crate::Error::DatabaseNotExist`] - database does not exist.
    async fn list_tables(&self, database_name: &str) -> Result<Vec<String>>;

    /// Create a table.
    ///
    /// * `ignore_if_exists` - if true, do nothing when the table already exists;
    ///   if false, return [`crate::Error::TableAlreadyExist`].
    ///
    /// # Errors
    /// * [`crate::Error::DatabaseNotExist`] - database in identifier does not exist.
    /// * [`crate::Error::TableAlreadyExist`] - table already exists when `ignore_if_exists` is false.
    async fn create_table(
        &self,
        identifier: &Identifier,
        creation: Schema,
        ignore_if_exists: bool,
    ) -> Result<()>;

    /// Drop a table. System tables cannot be dropped.
    ///
    /// # Errors
    /// * [`crate::Error::TableNotExist`] - table does not exist when `ignore_if_not_exists` is false.
    async fn drop_table(&self, identifier: &Identifier, ignore_if_not_exists: bool) -> Result<()>;

    /// Rename a table.
    ///
    /// # Errors
    /// * [`crate::Error::TableNotExist`] - source table does not exist when `ignore_if_not_exists` is false.
    /// * [`crate::Error::TableAlreadyExist`] - target table already exists.
    async fn rename_table(
        &self,
        from: &Identifier,
        to: &Identifier,
        ignore_if_not_exists: bool,
    ) -> Result<()>;

    /// Apply schema changes to a table.
    ///
    /// # Errors
    /// * [`crate::Error::TableNotExist`] - table does not exist when `ignore_if_not_exists` is false.
    /// * [`crate::Error::ColumnAlreadyExist`] - adding a column that already exists.
    /// * [`crate::Error::ColumnNotExist`] - altering or dropping a column that does not exist.
    async fn alter_table(
        &self,
        identifier: &Identifier,
        changes: Vec<SchemaChange>,
        ignore_if_not_exists: bool,
    ) -> Result<()>;
}
