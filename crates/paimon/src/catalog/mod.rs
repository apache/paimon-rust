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
mod function;
mod partition_listing;
mod rest;
mod view;

use std::collections::HashMap;
use std::fmt;

use crate::{Error, Result};
pub use database::*;
pub use factory::*;
pub use filesystem::*;
pub use function::*;
pub use partition_listing::list_partitions_from_file_system;
pub use rest::*;
use serde::{Deserialize, Serialize};
pub use view::*;

/// Splitter for system table names (e.g. `table$snapshots`).
pub const SYSTEM_TABLE_SPLITTER: &str = "$";
/// Prefix for branch in object name (e.g. `table$branch_foo`).
pub const SYSTEM_BRANCH_PREFIX: &str = "branch_";
/// Default main branch name.
pub const DEFAULT_MAIN_BRANCH: &str = "main";
/// Database value when the database is not known; [`Identifier::full_name`] returns only the object.
pub const UNKNOWN_DATABASE: &str = "unknown";
/// Database property key for custom location. Not allowed for filesystem catalog.
/// See [Catalog.DB_LOCATION_PROP](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/catalog/Catalog.java).
pub const DB_LOCATION_PROP: &str = "location";
/// Suffix for database directory names in the filesystem (e.g. `mydb.db`).
/// See [Catalog.DB_SUFFIX](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/catalog/Catalog.java).
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

/// Parsed form of a Paimon object name.
///
/// Mirrors Java `Identifier.splitObjectName`: `table$branch_b1$snapshots`
/// resolves to table `table`, branch `b1`, and system table `snapshots`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedObjectName {
    table: String,
    branch: Option<String>,
    system_table: Option<String>,
}

impl ParsedObjectName {
    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn branch(&self) -> Option<&str> {
        self.branch.as_deref()
    }

    pub fn branch_or_default(&self) -> &str {
        self.branch.as_deref().unwrap_or(DEFAULT_MAIN_BRANCH)
    }

    pub fn system_table(&self) -> Option<&str> {
        self.system_table.as_deref()
    }
}

impl Identifier {
    /// Create an identifier from database and object name.
    pub fn new(database: impl Into<String>, object: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            object: object.into(),
        }
    }

    /// Validate this identifier's database and object names.
    pub(crate) fn validate(&self) -> Result<()> {
        Self::validate_database_name(&self.database)?;
        Self::validate_object_name(&self.object)
    }

    /// Validate a database name for path-safe catalog use.
    pub(crate) fn validate_database_name(name: &str) -> Result<()> {
        validate_identifier_name("database", name)
    }

    /// Validate an object name for path-safe catalog use.
    pub(crate) fn validate_object_name(name: &str) -> Result<()> {
        validate_identifier_name("object", name)
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

    /// Parse the object name into table, branch, and system-table components.
    pub fn parsed_object_name(&self) -> Result<ParsedObjectName> {
        parse_object_name(&self.object)
    }

    pub fn table_name(&self) -> Result<String> {
        Ok(self.parsed_object_name()?.table)
    }

    pub fn branch_name(&self) -> Result<Option<String>> {
        Ok(self.parsed_object_name()?.branch)
    }

    pub fn branch_name_or_default(&self) -> Result<String> {
        Ok(self
            .branch_name()?
            .unwrap_or_else(|| DEFAULT_MAIN_BRANCH.to_string()))
    }

    pub fn system_table_name(&self) -> Result<Option<String>> {
        Ok(self.parsed_object_name()?.system_table)
    }
}

/// Parse a Paimon object name into table, optional branch, and optional system table.
pub fn parse_object_name(object: &str) -> Result<ParsedObjectName> {
    let parts: Vec<&str> = object.split(SYSTEM_TABLE_SPLITTER).collect();
    let invalid = || Error::IdentifierInvalid {
        message: format!("Invalid object name: {object}"),
    };
    let branch_from = |part: &str| {
        let branch = part
            .strip_prefix(SYSTEM_BRANCH_PREFIX)
            .ok_or_else(invalid)?;
        if branch.trim().is_empty() {
            return Err(Error::IdentifierInvalid {
                message: format!("Branch name cannot be empty in object name: {object}"),
            });
        }
        validate_branch_name(branch)?;
        Ok(branch.to_string())
    };

    match parts.as_slice() {
        [table] => Ok(ParsedObjectName {
            table: (*table).to_string(),
            branch: None,
            system_table: None,
        }),
        [table, second] if second.starts_with(SYSTEM_BRANCH_PREFIX) => Ok(ParsedObjectName {
            table: (*table).to_string(),
            branch: Some(branch_from(second)?),
            system_table: None,
        }),
        [table, system_table] => Ok(ParsedObjectName {
            table: (*table).to_string(),
            branch: None,
            system_table: Some((*system_table).to_string()),
        }),
        [table, branch, system_table] if branch.starts_with(SYSTEM_BRANCH_PREFIX) => {
            Ok(ParsedObjectName {
                table: (*table).to_string(),
                branch: Some(branch_from(branch)?),
                system_table: Some((*system_table).to_string()),
            })
        }
        _ => Err(invalid()),
    }
}

pub(crate) fn validate_branch_name(name: &str) -> Result<()> {
    validate_identifier_name("branch", name)
}

fn validate_identifier_name(kind: &str, name: &str) -> Result<()> {
    let invalid = if name.trim().is_empty() {
        Some("cannot be empty or whitespace")
    } else if matches!(name, "." | "..") {
        Some("cannot be '.' or '..'")
    } else if name.contains('/') || name.contains('\\') {
        Some("cannot contain path separators")
    } else if name.chars().any(char::is_control) {
        Some("cannot contain control characters")
    } else {
        None
    };

    if let Some(reason) = invalid {
        return Err(Error::IdentifierInvalid {
            message: format!("{kind} name {reason}: {name:?}"),
        });
    }

    Ok(())
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

use crate::api::PagedList;
use crate::spec::{Partition, Schema, SchemaChange};
use crate::table::Table;

/// Catalog API for reading and writing metadata (databases, tables) in Paimon.
///
/// Corresponds to [org.apache.paimon.catalog.Catalog](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/catalog/Catalog.java).
#[async_trait]
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

    // ======================= view methods ===============================

    /// Create a persistent view.
    async fn create_view(
        &self,
        _identifier: &Identifier,
        _schema: ViewSchema,
        _ignore_if_exists: bool,
    ) -> Result<()> {
        Err(Error::Unsupported {
            message: "Catalog does not support views".to_string(),
        })
    }

    /// Drop a persistent view.
    ///
    /// # Errors
    /// * [`crate::Error::ViewNotExist`] - view does not exist when
    ///   `ignore_if_not_exists` is false.
    async fn drop_view(&self, _identifier: &Identifier, _ignore_if_not_exists: bool) -> Result<()> {
        Err(Error::Unsupported {
            message: "Catalog does not support views".to_string(),
        })
    }

    /// List persistent view names in a database.
    async fn list_views(&self, _database_name: &str) -> Result<Vec<String>> {
        Err(Error::Unsupported {
            message: "Catalog does not support views".to_string(),
        })
    }

    /// Get a persistent view by identifier.
    async fn get_view(&self, _identifier: &Identifier) -> Result<View> {
        Err(Error::Unsupported {
            message: "Catalog does not support views".to_string(),
        })
    }

    // ======================= function methods ===============================

    /// Create a persistent function.
    ///
    /// * `ignore_if_exists` - if true, do nothing when the function already exists;
    ///   if false, return [`crate::Error::FunctionAlreadyExist`].
    ///
    /// # Errors
    /// * [`crate::Error::DatabaseNotExist`] - database in the function identifier does not exist.
    /// * [`crate::Error::FunctionAlreadyExist`] - function already exists when
    ///   `ignore_if_exists` is false.
    async fn create_function(&self, _function: &Function, _ignore_if_exists: bool) -> Result<()> {
        Err(Error::Unsupported {
            message: "Catalog does not support functions".to_string(),
        })
    }

    /// List persistent function names in a database.
    async fn list_functions(&self, _database_name: &str) -> Result<Vec<String>> {
        Err(Error::Unsupported {
            message: "Catalog does not support functions".to_string(),
        })
    }

    /// Get a persistent function by identifier.
    async fn get_function(&self, _identifier: &Identifier) -> Result<Function> {
        Err(Error::Unsupported {
            message: "Catalog does not support functions".to_string(),
        })
    }

    /// List partitions for a table.
    ///
    /// Default impl scans the table's manifest entries via
    /// [`list_partitions_from_file_system`], matching Java
    /// `AbstractCatalog.listPartitions`. Catalogs with metastore-tracked
    /// partitions (e.g. `RESTCatalog`) override to return audit fields too.
    async fn list_partitions(&self, identifier: &Identifier) -> Result<Vec<Partition>> {
        let table = self.get_table(identifier).await?;
        list_partitions_from_file_system(&table).await
    }

    /// Like [`Self::list_partitions`] but paged. Default impl ignores
    /// `max_results` and `page_token`, returning all partitions in a single page.
    /// Catalogs that need true pagination (e.g. `RESTCatalog`) override this.
    async fn list_partitions_paged(
        &self,
        identifier: &Identifier,
        _max_results: Option<u32>,
        _page_token: Option<&str>,
    ) -> Result<PagedList<Partition>> {
        Ok(PagedList::new(
            self.list_partitions(identifier).await?,
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_object_name() {
        let parsed = parse_object_name("orders").unwrap();
        assert_eq!(parsed.table(), "orders");
        assert_eq!(parsed.branch(), None);
        assert_eq!(parsed.branch_or_default(), DEFAULT_MAIN_BRANCH);
        assert_eq!(parsed.system_table(), None);
    }

    #[test]
    fn parses_branch_object_name() {
        let parsed = parse_object_name("orders$branch_b1").unwrap();
        assert_eq!(parsed.table(), "orders");
        assert_eq!(parsed.branch(), Some("b1"));
        assert_eq!(parsed.system_table(), None);
    }

    #[test]
    fn parses_branch_system_table_object_name() {
        let parsed = parse_object_name("orders$branch_b1$snapshots").unwrap();
        assert_eq!(parsed.table(), "orders");
        assert_eq!(parsed.branch(), Some("b1"));
        assert_eq!(parsed.system_table(), Some("snapshots"));
    }

    #[test]
    fn rejects_invalid_three_part_object_name() {
        assert!(parse_object_name("orders$foo$bar").is_err());
    }

    #[test]
    fn rejects_path_unsafe_branch_name() {
        assert!(parse_object_name("orders$branch_../../other").is_err());
        assert!(parse_object_name("orders$branch_nested/name").is_err());
        assert!(parse_object_name("orders$branch_..").is_err());
    }

    #[test]
    fn test_identifier_validate_should_reject_path_control_names() {
        for (database, object) in [
            ("", "table"),
            ("   ", "table"),
            (".", "table"),
            ("..", "table"),
            ("../escaped", "table"),
            ("db\\escaped", "table"),
            ("db\nescaped", "table"),
            ("db", ""),
            ("db", "   "),
            ("db", "."),
            ("db", ".."),
            ("db", "../escaped"),
            ("db", "nested/table"),
            ("db", "nested\\table"),
            ("db", "table\0name"),
        ] {
            let result = Identifier::new(database, object).validate();
            assert!(
                matches!(result, Err(Error::IdentifierInvalid { .. })),
                "expected invalid identifier for database={database:?}, object={object:?}, got {result:?}"
            );
        }
    }

    #[test]
    fn test_identifier_validate_should_allow_system_suffix_and_unicode_names() {
        let identifier = Identifier::new("analytics", "orders$snapshots");
        identifier.validate().unwrap();
        assert_eq!(identifier.database(), "analytics");
        assert_eq!(identifier.object(), "orders$snapshots");

        let identifier = Identifier::new("数据", "订单");
        identifier.validate().unwrap();
        assert_eq!(identifier.database(), "数据");
        assert_eq!(identifier.object(), "订单");
    }
}
