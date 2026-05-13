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

//! Paimon system tables (`<table>$<name>`) as DataFusion table providers.
//!
//! Mirrors Java [SystemTableLoader](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/system/SystemTableLoader.java):
//! `TABLES` maps each system-table name to its builder function.

use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use paimon::catalog::{Catalog, Identifier, SYSTEM_BRANCH_PREFIX, SYSTEM_TABLE_SPLITTER};
use paimon::table::Table;

use crate::error::to_datafusion_error;

mod branches;
mod manifests;
mod options;
mod row_string_cast;
mod schemas;
mod snapshots;
mod tags;

type Builder = fn(Table) -> DFResult<Arc<dyn TableProvider>>;

const TABLES: &[(&str, Builder)] = &[
    ("branches", branches::build),
    ("manifests", manifests::build),
    ("options", options::build),
    ("schemas", schemas::build),
    ("snapshots", snapshots::build),
    ("tags", tags::build),
];

/// Parse a Paimon object name into `(base_table, optional system_table_name)`.
///
/// Mirrors Java [Identifier.splitObjectName](https://github.com/apache/paimon/blob/release-1.3/paimon-api/src/main/java/org/apache/paimon/catalog/Identifier.java).
///
/// - `t` → `("t", None)`
/// - `t$options` → `("t", Some("options"))`
/// - `t$branch_main` → `("t", None)` (branch reference, not a system table)
/// - `t$branch_main$options` → `("t", Some("options"))` (branch + system table)
pub(crate) fn split_object_name(name: &str) -> (&str, Option<&str>) {
    let mut parts = name.splitn(3, SYSTEM_TABLE_SPLITTER);
    let base = parts.next().unwrap_or(name);
    match (parts.next(), parts.next()) {
        (None, _) => (base, None),
        (Some(second), None) => {
            if second.starts_with(SYSTEM_BRANCH_PREFIX) {
                (base, None)
            } else {
                (base, Some(second))
            }
        }
        (Some(second), Some(third)) => {
            if second.starts_with(SYSTEM_BRANCH_PREFIX) {
                (base, Some(third))
            } else {
                // `$` is legal in table names, so `t$foo$bar` falls through as
                // plain `t` and errors later as "table not found".
                (base, None)
            }
        }
    }
}

/// Returns true if `name` is a recognised Paimon system table suffix.
pub(crate) fn is_registered(name: &str) -> bool {
    TABLES.iter().any(|(n, _)| name.eq_ignore_ascii_case(n))
}

/// Wraps an already-loaded base table as the system table `name`.
fn wrap_to_system_table(name: &str, base_table: Table) -> Option<DFResult<Arc<dyn TableProvider>>> {
    TABLES
        .iter()
        .find(|(n, _)| name.eq_ignore_ascii_case(n))
        .map(|(_, build)| build(base_table))
}

/// Loads `<base>$<system_name>` from the catalog and wraps it as a system
/// table provider.
///
/// - Unknown `system_name` → `Ok(None)` (DataFusion reports "table not found")
/// - Base table missing    → `Err(Plan)` so users can distinguish it from an
///   unknown system name
pub(crate) async fn load(
    catalog: Arc<dyn Catalog>,
    database: String,
    base: String,
    system_name: String,
) -> DFResult<Option<Arc<dyn TableProvider>>> {
    if !is_registered(&system_name) {
        return Ok(None);
    }
    let identifier = Identifier::new(database, base.clone());
    match catalog.get_table(&identifier).await {
        Ok(table) => wrap_to_system_table(&system_name, table)
            .expect("is_registered guarantees a builder")
            .map(Some),
        Err(paimon::Error::TableNotExist { .. }) => Err(DataFusionError::Plan(format!(
            "Cannot read system table `${system_name}`: \
             base table `{base}` does not exist"
        ))),
        Err(e) => Err(to_datafusion_error(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::{is_registered, split_object_name};

    #[test]
    fn is_registered_is_case_insensitive() {
        assert!(is_registered("options"));
        assert!(is_registered("Options"));
        assert!(is_registered("OPTIONS"));
        assert!(is_registered("schemas"));
        assert!(is_registered("Schemas"));
        assert!(is_registered("SCHEMAS"));
        assert!(is_registered("branches"));
        assert!(is_registered("Branches"));
        assert!(is_registered("BRANCHES"));
        assert!(is_registered("tags"));
        assert!(is_registered("Tags"));
        assert!(is_registered("TAGS"));
        assert!(is_registered("manifests"));
        assert!(is_registered("Manifests"));
        assert!(is_registered("MANIFESTS"));
        assert!(!is_registered("nonsense"));
    }

    #[test]
    fn plain_table_name() {
        assert_eq!(split_object_name("orders"), ("orders", None));
    }

    #[test]
    fn system_table_only() {
        assert_eq!(
            split_object_name("orders$options"),
            ("orders", Some("options"))
        );
    }

    #[test]
    fn branch_reference_is_not_a_system_table() {
        assert_eq!(split_object_name("orders$branch_main"), ("orders", None));
    }

    #[test]
    fn branch_plus_system_table() {
        assert_eq!(
            split_object_name("orders$branch_main$options"),
            ("orders", Some("options"))
        );
    }

    #[test]
    fn three_parts_without_branch_prefix_is_not_a_system_table() {
        assert_eq!(split_object_name("orders$foo$bar"), ("orders", None));
    }

    #[test]
    fn system_table_name_preserves_case() {
        assert_eq!(
            split_object_name("orders$Options"),
            ("orders", Some("Options"))
        );
    }
}
