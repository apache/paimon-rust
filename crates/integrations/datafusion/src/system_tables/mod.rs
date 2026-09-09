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
use paimon::catalog::{parse_object_name, Catalog, Identifier, ParsedObjectName};
use paimon::table::Table;

use crate::error::to_datafusion_error;

mod branches;
mod consumers;
mod files;
mod manifests;
mod options;
mod partitions;
mod physical_files_size;
mod referenced_files_size;
mod row_string_cast;
mod schemas;
mod snapshots;
mod table_indexes;
mod tags;

type Builder = fn(Table) -> DFResult<Arc<dyn TableProvider>>;

// Most system tables only need the base `Table`. `partitions` is special-cased
// in `load` because it needs the catalog handle (for metastore-tracked audit
// metadata via `Catalog::list_partitions`).
const TABLES: &[(&str, Builder)] = &[
    ("branches", branches::build),
    ("consumers", consumers::build),
    ("files", files::build),
    ("manifests", manifests::build),
    ("options", options::build),
    ("physical_files_size", physical_files_size::build),
    ("referenced_files_size", referenced_files_size::build),
    ("schemas", schemas::build),
    ("snapshots", snapshots::build),
    ("table_indexes", table_indexes::build),
    ("tags", tags::build),
];

const SYSTEM_TABLE_NAMES: &[&str] = &[
    "branches",
    "consumers",
    "files",
    "manifests",
    "options",
    "partitions",
    "physical_files_size",
    "referenced_files_size",
    "schemas",
    "snapshots",
    "table_indexes",
    "tags",
];

/// Parse a Paimon object name into table, branch, and optional system table.
///
/// Mirrors Java [Identifier.splitObjectName](https://github.com/apache/paimon/blob/release-1.3/paimon-api/src/main/java/org/apache/paimon/catalog/Identifier.java).
///
/// - `t` → table `t`
/// - `t$options` → table `t`, system table `options`
/// - `t$branch_b1` → table `t`, branch `b1`
/// - `t$branch_b1$options` → table `t`, branch `b1`, system table `options`
pub(crate) fn parse_object_name_for_datafusion(name: &str) -> DFResult<ParsedObjectName> {
    parse_object_name(name).map_err(to_datafusion_error)
}

/// Returns true if `name` is a recognised Paimon system table suffix.
pub(crate) fn is_registered(name: &str) -> bool {
    SYSTEM_TABLE_NAMES
        .iter()
        .any(|n| name.eq_ignore_ascii_case(n))
}

/// Wraps an already-loaded base table as the system table `name`.
/// Does `provider` serve one of the [`TABLES`] above?
///
/// By type, not by the `base$name` spelling — another engine may name a table
/// with a `$`. Keep in step with `TABLES`: one missing here goes back to
/// having its time-travel clause silently dropped.
pub(crate) fn is_system_table_provider(provider: &dyn TableProvider) -> bool {
    provider.is::<branches::BranchesTable>()
        || provider.is::<consumers::ConsumersTable>()
        || provider.is::<files::FilesTable>()
        || provider.is::<manifests::ManifestsTable>()
        || provider.is::<options::OptionsTable>()
        || provider.is::<partitions::PartitionsTable>()
        || provider.is::<physical_files_size::PhysicalFilesSizeTable>()
        || provider.is::<referenced_files_size::ReferencedFilesSizeTable>()
        || provider.is::<schemas::SchemasTable>()
        || provider.is::<snapshots::SnapshotsTable>()
        || provider.is::<table_indexes::TableIndexesTable>()
        || provider.is::<tags::TagsTable>()
}

fn wrap_to_system_table(name: &str, base_table: Table) -> Option<DFResult<Arc<dyn TableProvider>>> {
    TABLES
        .iter()
        .find(|(n, _)| name.eq_ignore_ascii_case(n))
        .map(|(_, build)| build(base_table))
}

pub(crate) fn provider_for_table(
    catalog: Arc<dyn Catalog>,
    identifier: Identifier,
    table: Table,
    system_name: &str,
) -> DFResult<Option<Arc<dyn TableProvider>>> {
    if !is_registered(system_name) {
        return Ok(None);
    }
    // Fail closed: system tables expose file metadata the client can't authorize.
    paimon::spec::CoreOptions::new(table.schema().options())
        .ensure_read_authorized()
        .map_err(to_datafusion_error)?;
    if system_name.eq_ignore_ascii_case("partitions") {
        return partitions::build(catalog, identifier, table).map(Some);
    }
    wrap_to_system_table(system_name, table)
        .expect("is_registered guarantees a builder")
        .map(Some)
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
    object: ParsedObjectName,
    system_name: String,
) -> DFResult<Option<Arc<dyn TableProvider>>> {
    if !is_registered(&system_name) {
        return Ok(None);
    }
    let identifier = Identifier::new(database, object.table().to_string());
    match catalog.get_table(&identifier).await {
        Ok(mut table) => {
            if let Some(branch) = object.branch() {
                if !system_name.eq_ignore_ascii_case("branches") {
                    table = table
                        .copy_with_branch(branch)
                        .await
                        .map_err(to_datafusion_error)?;
                }
            }
            provider_for_table(catalog, identifier, table, &system_name)
        }
        Err(paimon::Error::TableNotExist { .. }) => Err(DataFusionError::Plan(format!(
            "Cannot read system table `${system_name}`: \
             base table `{}` does not exist",
            object.table()
        ))),
        Err(e) => Err(to_datafusion_error(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::{is_registered, parse_object_name_for_datafusion, SYSTEM_TABLE_NAMES, TABLES};

    /// Guards against the two registries drifting: anything in `TABLES` must
    /// also be in `SYSTEM_TABLE_NAMES`, and the only name allowed to be in
    /// `SYSTEM_TABLE_NAMES` but not `TABLES` is `partitions` (routed via the
    /// special path in `load`).
    #[test]
    fn registries_stay_in_sync() {
        for (name, _) in TABLES {
            assert!(
                SYSTEM_TABLE_NAMES.contains(name),
                "`{name}` is in TABLES but missing from SYSTEM_TABLE_NAMES"
            );
        }
        for name in SYSTEM_TABLE_NAMES {
            let in_tables = TABLES.iter().any(|(n, _)| n == name);
            assert!(
                in_tables || *name == "partitions",
                "`{name}` is in SYSTEM_TABLE_NAMES but has no builder and is not the special-cased `partitions`"
            );
        }
    }

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
        assert!(is_registered("consumers"));
        assert!(is_registered("Consumers"));
        assert!(is_registered("CONSUMERS"));
        assert!(is_registered("files"));
        assert!(is_registered("Files"));
        assert!(is_registered("FILES"));
        assert!(is_registered("tags"));
        assert!(is_registered("Tags"));
        assert!(is_registered("TAGS"));
        assert!(is_registered("manifests"));
        assert!(is_registered("Manifests"));
        assert!(is_registered("MANIFESTS"));
        assert!(is_registered("table_indexes"));
        assert!(is_registered("Table_Indexes"));
        assert!(is_registered("TABLE_INDEXES"));
        assert!(is_registered("partitions"));
        assert!(is_registered("Partitions"));
        assert!(is_registered("PARTITIONS"));
        assert!(!is_registered("nonsense"));
    }

    #[test]
    fn plain_table_name() {
        let parsed = parse_object_name_for_datafusion("orders").unwrap();
        assert_eq!(parsed.table(), "orders");
        assert_eq!(parsed.branch(), None);
        assert_eq!(parsed.system_table(), None);
    }

    #[test]
    fn system_table_only() {
        let parsed = parse_object_name_for_datafusion("orders$options").unwrap();
        assert_eq!(parsed.table(), "orders");
        assert_eq!(parsed.branch(), None);
        assert_eq!(parsed.system_table(), Some("options"));
    }

    #[test]
    fn branch_reference_is_not_a_system_table() {
        let parsed = parse_object_name_for_datafusion("orders$branch_main").unwrap();
        assert_eq!(parsed.table(), "orders");
        assert_eq!(parsed.branch(), Some("main"));
        assert_eq!(parsed.system_table(), None);
    }

    #[test]
    fn branch_reference_does_not_drop_branch_name() {
        let parsed = parse_object_name_for_datafusion("orders$branch_b1").unwrap();
        assert_eq!(parsed.table(), "orders");
        assert_eq!(parsed.branch(), Some("b1"));
        assert_eq!(parsed.system_table(), None);
    }

    #[test]
    fn branch_plus_system_table() {
        let parsed = parse_object_name_for_datafusion("orders$branch_main$options").unwrap();
        assert_eq!(parsed.table(), "orders");
        assert_eq!(parsed.branch(), Some("main"));
        assert_eq!(parsed.system_table(), Some("options"));
    }

    #[test]
    fn three_parts_without_branch_prefix_is_not_a_system_table() {
        assert!(parse_object_name_for_datafusion("orders$foo$bar").is_err());
    }

    #[test]
    fn system_table_name_preserves_case() {
        let parsed = parse_object_name_for_datafusion("orders$Options").unwrap();
        assert_eq!(parsed.table(), "orders");
        assert_eq!(parsed.branch(), None);
        assert_eq!(parsed.system_table(), Some("Options"));
    }
}
