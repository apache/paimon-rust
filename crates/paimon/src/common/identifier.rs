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

//! Common types used in REST API requests and responses.

use serde::{Deserialize, Serialize};

/// System table splitter constant.
pub const SYSTEM_TABLE_SPLITTER: &str = "$";
/// System branch prefix constant.
pub const SYSTEM_BRANCH_PREFIX: &str = "branch-";

/// Identifier for a table or resource.
///
/// Represents a fully qualified identifier consisting of database, object (table) name,
/// and an optional branch.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Identifier {
    /// The database name.
    pub database: String,
    /// The object (table) name.
    #[serde(rename = "object")]
    pub object_name: String,
    /// Optional branch name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

impl Identifier {
    /// Create a new Identifier.
    pub fn new(database: String, object_name: String, branch: Option<String>) -> Self {
        Self {
            database,
            object_name,
            branch,
        }
    }

    /// Create an Identifier without a branch.
    pub fn create(database: String, object_name: String) -> Self {
        Self::new(database, object_name, None)
    }

    /// Parse an identifier from a string in the format "database.object" or "database.object.branch".
    ///
    /// # Arguments
    /// * `full_name` - The full name string to parse
    ///
    /// # Returns
    /// The parsed Identifier, or an error if parsing fails.
    pub fn from_string(full_name: &str) -> Result<Self, String> {
        if full_name.trim().is_empty() {
            return Err("fullName cannot be null or empty".to_string());
        }

        // Check if backticks are used
        if full_name.contains('`') {
            return Self::parse_with_backticks(full_name);
        }

        // Split by period, supporting up to 3 parts (database.object.branch)
        let parts: Vec<&str> = full_name.split('.').collect();

        if parts.len() < 2 || parts.len() > 3 {
            return Err(format!(
                "Cannot get splits from '{}' to get database and object",
                full_name
            ));
        }

        let branch = if parts.len() == 3 {
            Some(parts[2].to_string())
        } else {
            None
        };

        Ok(Self::new(parts[0].to_string(), parts[1].to_string(), branch))
    }

    /// Parse an identifier with backtick quoting support.
    fn parse_with_backticks(full_name: &str) -> Result<Self, String> {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut in_backticks = false;

        for char in full_name.chars() {
            match char {
                '`' => {
                    in_backticks = !in_backticks;
                }
                '.' if !in_backticks => {
                    parts.push(current.clone());
                    current.clear();
                }
                _ => {
                    current.push(char);
                }
            }
        }

        if !current.is_empty() {
            parts.push(current);
        }

        if in_backticks {
            return Err(format!("Unclosed backtick in identifier: {}", full_name));
        }

        if parts.len() < 2 || parts.len() > 3 {
            return Err(format!("Invalid identifier format: {}", full_name));
        }

        let branch = if parts.len() == 3 {
            Some(parts[2].clone())
        } else {
            None
        };

        Ok(Self::new(parts[0].clone(), parts[1].clone(), branch))
    }

    /// Get the full name of this identifier.
    pub fn get_full_name(&self) -> String {
        match &self.branch {
            Some(branch) => format!("{}.{}.{}", self.database, self.object_name, branch),
            None => format!("{}.{}", self.database, self.object_name),
        }
    }

    /// Get the database name.
    pub fn get_database_name(&self) -> &str {
        &self.database
    }

    /// Get the table name (alias for object_name).
    pub fn get_table_name(&self) -> &str {
        &self.object_name
    }

    /// Get the object name.
    pub fn get_object_name(&self) -> &str {
        &self.object_name
    }

    /// Get the branch name, if any.
    pub fn get_branch_name(&self) -> Option<&str> {
        self.branch.as_deref()
    }

    /// Check if this is a system table.
    pub fn is_system_table(&self) -> bool {
        self.object_name.starts_with(SYSTEM_TABLE_SPLITTER)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identifier_from_string() {
        let id = Identifier::from_string("db.table").unwrap();
        assert_eq!(id.get_database_name(), "db");
    assert_eq!(id.get_table_name(), "table");
        assert_eq!(id.get_branch_name(), None);
        assert_eq!(id.get_full_name(), "db.table");
        assert!(!id.is_system_table());
    }

    #[test]
    fn test_identifier_with_branch() {
        let id = Identifier::from_string("db.table.branch").unwrap();
        assert_eq!(id.get_database_name(), "db");
        assert_eq!(id.get_table_name(), "table");
        assert_eq!(id.get_branch_name(), Some("branch"));
        assert_eq!(id.get_full_name(), "db.table.branch");
    }

    #[test]
    fn test_identifier_with_backticks() {
        let id = Identifier::from_string("`db.name`.`table.name`").unwrap();
        assert_eq!(id.get_database_name(), "db.name");
        assert_eq!(id.get_table_name(), "table.name");
    }

    #[test]
    fn test_identifier_system_table() {
        let id = Identifier::create("db".to_string(), "$system_table".to_string());
        assert!(id.is_system_table());
    }
}
