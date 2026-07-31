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

//! Table API for Apache Paimon

pub(crate) mod aggregator;
mod audit_log_table;
pub(crate) mod bin_pack;
mod bitmap_global_index_reader;
mod blob_resolver;
mod branch_manager;
mod btree_global_index_build_builder;
mod bucket_assigner;
mod bucket_assigner_constant;
mod bucket_assigner_cross;
mod bucket_assigner_dynamic;
mod bucket_assigner_fixed;
mod bucket_filter;
mod bucket_function;
mod commit_message;
pub(crate) mod cow_writer;
mod data_evolution_reader;
pub mod data_evolution_writer;
mod data_file_reader;
mod data_file_writer;
mod dedicated_format_file_writer;
mod format_read_builder;
mod format_table_read;
mod format_table_scan;
mod format_write_builder;
#[cfg(feature = "fulltext")]
mod full_text_index_adapter;
#[cfg(feature = "fulltext")]
mod full_text_search_builder;
pub(crate) mod global_index_build_common;
mod global_index_drop_builder;
pub(crate) mod global_index_scanner;
mod global_index_types;
mod hybrid_search_builder;
mod incremental_scan;
mod kv_file_reader;
mod kv_file_writer;
mod lumina_index_build_builder;
pub(crate) mod merge_tree_split_generator;
mod partition_filter;
mod partition_stat;
#[cfg(feature = "fulltext")]
mod pk_full_text_bucket_search;
mod pk_full_text_bucket_state;
#[cfg(feature = "fulltext")]
mod pk_full_text_read;
#[cfg(feature = "fulltext")]
mod pk_full_text_scan;
mod pk_search_position;
mod pk_search_ranker;
mod pk_vector_data_file_reader;
mod pk_vector_indexed_split_read;
mod pk_vector_orchestrator;
mod pk_vector_position_read;
mod pk_vector_scan;
mod postpone_file_writer;
mod prepared_files;
pub(crate) mod query_auth;
mod read_builder;
pub mod referenced_files;
pub(crate) mod rest_env;
pub(crate) mod row_id_predicate;
mod row_kind_generator;
mod scan_trace;
pub(crate) mod schema_manager;
pub(crate) mod snapshot_commit;
mod snapshot_manager;
mod sort_merge;
mod source;
mod stats_filter;
pub(crate) mod table_commit;
mod table_read;
mod table_scan;
mod table_update;
pub(crate) mod table_write;
mod tag_manager;
pub(crate) mod time_travel;
mod vector_search_builder;
mod vindex_index_build_builder;
mod write_builder;

use crate::Result;
use arrow_array::RecordBatch;
pub use audit_log_table::AuditLogTable;
pub use branch_manager::BranchManager;
pub use btree_global_index_build_builder::BTreeGlobalIndexBuildBuilder;
pub use commit_message::CommitMessage;
pub use cow_writer::{CopyOnWriteMergeWriter, FileInfo};
pub use data_evolution_writer::{DataEvolutionDeleteWriter, DataEvolutionWriter};
#[cfg(feature = "fulltext")]
pub use full_text_search_builder::FullTextSearchBuilder;
use futures::stream::BoxStream;
pub use global_index_drop_builder::GlobalIndexDropBuilder;
pub use global_index_types::{
    normalize_global_index_type_for_drop, SUPPORTED_GLOBAL_INDEX_TYPES_FOR_DROP,
};
pub use hybrid_search_builder::{
    HybridSearchBuilder, HybridSearchRanker, HybridSearchRoute, HybridSearchRouteKind,
};
pub use incremental_scan::{
    IncrementalPlan, IncrementalScan, IncrementalScanMode, IncrementalSplit,
};
pub use lumina_index_build_builder::LuminaIndexBuildBuilder;
pub use partition_stat::PartitionStat;
pub use read_builder::ReadBuilder;
pub use rest_env::RESTEnv;
pub use scan_trace::ScanTrace;
pub use schema_manager::SchemaManager;
pub use snapshot_commit::{RESTSnapshotCommit, RenamingSnapshotCommit, SnapshotCommit};
pub use snapshot_manager::SnapshotManager;
pub use source::{
    merge_row_ranges, DataSplit, DataSplitBuilder, DeletionFile, PartitionBucket, Plan, RowRange,
};
pub use table_commit::TableCommit;
pub use table_read::TableRead;
pub use table_scan::TableScan;
pub use table_update::TableUpdate;
pub use table_write::TableWrite;
pub use tag_manager::TagManager;
pub use vector_search_builder::{BatchVectorSearchBuilder, VectorSearchBuilder};
pub use vindex_index_build_builder::VindexIndexBuildBuilder;
pub use write_builder::WriteBuilder;

use crate::catalog::{validate_branch_name, Identifier, DEFAULT_MAIN_BRANCH};
use crate::io::FileIO;
use crate::spec::{CoreOptions, DataField, Snapshot, TableSchema};
use query_auth::QueryAuthGrant;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Table represents a table in the catalog.
#[derive(Debug, Clone)]
pub struct Table {
    file_io: FileIO,
    identifier: Identifier,
    location: String,
    schema: TableSchema,
    schema_manager: SchemaManager,
    branch: String,
    branch_reference: bool,
    rest_env: Option<RESTEnv>,
    /// True when this table copy was switched to a historical schema by
    /// [`Table::copy_with_time_travel`]. Such a copy is read-only.
    time_traveled: bool,
    /// Snapshot resolved by [`Table::copy_with_time_travel`] from this copy's
    /// options, so scans don't have to resolve the same selector again.
    /// Cleared when [`Table::copy_with_options`] changes the selector.
    travel_snapshot: Option<Snapshot>,
}

impl Table {
    /// Create a new table.
    pub fn new(
        file_io: FileIO,
        identifier: Identifier,
        location: String,
        schema: TableSchema,
        rest_env: Option<RESTEnv>,
    ) -> Self {
        let schema_manager = SchemaManager::new(file_io.clone(), location.clone());
        let branch = DEFAULT_MAIN_BRANCH.to_string();
        Self {
            file_io,
            identifier,
            location,
            schema,
            schema_manager,
            branch,
            branch_reference: false,
            rest_env,
            time_traveled: false,
            travel_snapshot: None,
        }
    }

    /// Create a table from an already resolved schema without loading a catalog.
    ///
    /// The supplied schema is preserved as-is (no normalization). Its structural
    /// invariants are validated — primary-key/partition columns must exist and
    /// field names/ids must be unique — so a malformed external schema is
    /// rejected here instead of panicking or reading the wrong column later. The
    /// branch only selects the branch-scoped managers used by subsequent reads.
    pub fn from_resolved_schema(
        file_io: FileIO,
        identifier: Identifier,
        location: String,
        schema: TableSchema,
        branch: impl Into<String>,
    ) -> Result<Self> {
        let branch = branch.into();
        identifier.validate()?;
        validate_branch_name(&branch)?;
        schema.validate_resolved_structure()?;
        let schema_manager = SchemaManager::new(file_io.clone(), location.clone());
        let schema_manager = if branch == DEFAULT_MAIN_BRANCH {
            schema_manager
        } else {
            schema_manager.with_branch(&branch)
        };
        let branch_reference = branch != DEFAULT_MAIN_BRANCH;

        Ok(Self {
            file_io,
            identifier,
            location,
            schema,
            schema_manager,
            branch,
            branch_reference,
            rest_env: None,
            time_traveled: false,
            travel_snapshot: None,
        })
    }

    /// Fetch the per-user row filter / column masking as the grant the read
    /// pipeline enforces; `None` when the table is not `query-auth.enabled`.
    ///
    /// `select` are the queried columns (table-schema indices, `None` = all),
    /// like Java's `readType.getFieldNames()`: a column-restricted user can
    /// read an authorized subset, and a wider read fails closed until it
    /// re-plans. Called per plan, as Java's `CatalogEnvironment.tableQueryAuth`
    /// is, so a revoked grant takes effect on the next one.
    /// `system_select` is `None` for internal raw reads, which run only under a
    /// fully unrestricted grant and legitimately touch `_ROW_ID`; scan planning
    /// always passes `Some(names)`, and an empty slice then approves none.
    pub(crate) async fn verify_query_auth_for_read(
        &self,
        select: Option<&HashSet<usize>>,
        system_select: Option<&[String]>,
    ) -> Result<Option<Arc<QueryAuthGrant>>> {
        if !CoreOptions::new(self.schema.options()).query_auth_enabled() {
            return Ok(None);
        }
        let Some(rest_env) = &self.rest_env else {
            return Err(crate::Error::Unsupported {
                message: "reading a table with 'query-auth.enabled' = true requires a REST \
                          catalog to authorize the query"
                    .to_string(),
            });
        };
        let fields = self.schema.fields();
        // Java's `select` is `readType.getFieldNames()`, so projected system
        // fields go too. They have no index to scope, but the server may still
        // refuse them — which it can only do if they are actually sent, so a
        // full read that wants one must spell out every column rather than rely
        // on a null select.
        let wanted_system: &[String] = system_select.unwrap_or(&[]);
        let select_names = match select {
            Some(indices) => Some(
                fields
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| indices.contains(i))
                    .map(|(_, f)| f.name().to_string())
                    .chain(wanted_system.iter().cloned())
                    .collect::<Vec<_>>(),
            ),
            None if wanted_system.is_empty() => None,
            None => Some(
                fields
                    .iter()
                    .map(|f| f.name().to_string())
                    .chain(wanted_system.iter().cloned())
                    .collect::<Vec<_>>(),
            ),
        };
        let auth = rest_env.table_query_auth(select_names).await?;
        let filters = query_auth::parse_auth_filters(&auth.filter.unwrap_or_default(), fields)?;
        let masks =
            query_auth::parse_column_masking(&auth.column_masking.unwrap_or_default(), fields)?;

        // Rules are expressed against the LATEST schema, but this `Table` may
        // predate a concurrent evolution: a re-added column keeps its name and
        // gets a fresh id. Java re-validates on every plan.
        // Also with no rules: `select` names columns of THIS copy's schema, so a
        // column dropped since would be authorized against a latest schema that
        // no longer has it, while the read still decodes it by field id.
        // Always, not just when rules exist: `select` (and `None`, meaning every
        // column of THIS copy) names columns of this schema, so one dropped
        // since would be authorized against a latest schema that no longer has
        // it while the read still decodes it by field id.
        self.ensure_rules_bind_to_latest_schema(&filters, &masks, select)
            .await?;
        let grant = QueryAuthGrant::new(
            filters,
            masks,
            select.cloned(),
            system_select.map(|names| names.iter().cloned().collect()),
            query_auth::GrantBinding::of(self),
        );

        // Rules are expressed against the CURRENT schema; a time-travelled or
        // branch copy reads a different one, where the same name may be an
        // unrelated field id. Keyed on server rules, not `is_unrestricted()`:
        // the client's own projection scope binds nothing to the schema.
        if grant.has_server_restrictions() && (self.time_traveled || self.branch_reference) {
            return Err(crate::Error::Unsupported {
                message: "a query-auth row filter / column masking grant cannot be applied to a \
                          time-travelled or branch read: the grant is bound to the table's \
                          current schema"
                    .to_string(),
            });
        }

        Ok(Some(Arc::new(grant)))
    }

    /// Fail closed when a rule column binds to a different field in the latest
    /// schema than in this copy — a rename or drop-and-re-add would apply the
    /// rule to unrelated data. Mirrors Java `validateReadableWithoutRename`.
    async fn ensure_rules_bind_to_latest_schema(
        &self,
        filters: &[crate::spec::Predicate],
        masks: &[query_auth::ColumnMask],
        select: Option<&HashSet<usize>>,
    ) -> Result<()> {
        // Compare ids before loading: the directory listing is the freshness
        // check and cannot be skipped, but the schema itself need not be read
        // when this copy is already current (the common case).
        // A table with no on-disk schema directory has no drift to detect — the
        // REST-provided schema is all there is. Listing it is also the only file
        // access on this path, so a table whose location is not readable (mock
        // catalogs, tables served entirely over REST) must not fail the read.
        let Ok(ids) = self.schema_manager.list_all_ids().await else {
            return Ok(());
        };
        let Some(&latest_id) = ids.last() else {
            return Ok(());
        };
        if latest_id == self.schema.id() {
            return Ok(());
        }
        let latest = self.schema_manager.schema(latest_id).await?;
        let mut referenced = HashSet::new();
        for filter in filters {
            filter.collect_leaf_field_indices(&mut referenced);
        }
        for mask in masks {
            referenced.insert(mask.column);
            mask.transform.collect_field_indices(&mut referenced);
        }
        let fields = self.schema.fields();
        match select {
            Some(select) => referenced.extend(select.iter().copied()),
            // `None` = every column of this copy, which is exactly what a full
            // read decodes.
            None => referenced.extend(0..fields.len()),
        }
        for index in referenced {
            let Some(field) = fields.get(index) else {
                continue;
            };
            // Type too, not just name and id: `disable-explicit-type-casting`
            // defaults to false, so a narrowing evolution (DOUBLE -> FLOAT) is
            // allowed. The auth JSON is parsed and evaluated against THIS
            // copy's type, so `x > 0.1` on a stale DOUBLE would admit values the
            // current FLOAT semantics reject.
            let matched = latest
                .fields()
                .iter()
                .find(|f| f.name() == field.name())
                .is_some_and(|f| f.id() == field.id() && f.data_type() == field.data_type());
            if !matched {
                return Err(crate::Error::Unsupported {
                    message: format!(
                        "query-auth read references column `{}`, which the table's latest \
                         schema exposes as a different column (renamed, or dropped and \
                         re-added); refusing to read rather than apply it to unrelated data",
                        field.name()
                    ),
                });
            }
        }
        Ok(())
    }

    /// Authorize a read that cannot enforce filtering / masking on its output
    /// (search, system tables, write-path rewrites). Returns the grant to stamp
    /// on its splits; fails closed on a restricted one, which would either leak
    /// (search/metadata) or commit a filtered view (rewrites).
    pub(crate) async fn authorize_unrestricted_read(&self) -> Result<Option<Arc<QueryAuthGrant>>> {
        match self.verify_query_auth_for_read(None, None).await? {
            Some(grant) if grant.is_unrestricted() => Ok(Some(grant)),
            Some(_) => Err(crate::Error::Unsupported {
                message: "this read on a 'query-auth.enabled' table must see raw rows, so it \
                          cannot apply the server's row filter / column masking and refuses to \
                          run under a restricted grant"
                    .to_string(),
            }),
            None => Ok(None),
        }
    }

    /// Authorize an internal read that rewrites data (copy-on-write DML, index
    /// builds) and stamp the grant on `splits`.
    ///
    /// Rewriting from a filtered or masked view would destroy hidden rows and
    /// persist masked values, so this requires a fully unrestricted grant. Scan
    /// planning's grant must NOT be used: a row filter shifts the positional
    /// row offsets rewrites replay.
    pub async fn authorize_rewrite_splits(&self, splits: Vec<DataSplit>) -> Result<Vec<DataSplit>> {
        // This is the only public API that stamps a grant, so it must not be
        // usable to launder splits: refuse ones that already carry a restricted
        // grant rather than overwriting it.
        if splits.iter().any(DataSplit::carries_query_auth_restriction) {
            return Err(crate::Error::Unsupported {
                message: "cannot re-authorize a split already planned under a query-auth row \
                          filter / column masking grant"
                    .to_string(),
            });
        }
        // Catch another table's splits being handed to a permissive one.
        // Bucket paths only: `external_path` legitimately points anywhere. A
        // sanity guard, not a boundary against in-process code.
        let root = self.location().trim_end_matches('/');
        if let Some(split) = splits.iter().find(|s| {
            // On a component boundary: `.../t` must reject `.../t2`.
            s.bucket_path()
                .trim_end_matches('/')
                .strip_prefix(root)
                .is_none_or(|rest| !rest.is_empty() && !rest.starts_with('/'))
        }) {
            return Err(crate::Error::Unsupported {
                message: format!(
                    "cannot authorize a split at `{}`: it does not belong to table `{}`",
                    split.bucket_path(),
                    self.identifier().full_name()
                ),
            });
        }
        let grant = self.authorize_unrestricted_read().await?;
        Ok(splits
            .into_iter()
            .map(|split| split.with_query_auth_grant(grant.clone()))
            .collect())
    }

    /// Authorize a commit. The data may have come from an enforced read (e.g.
    /// `INSERT OVERWRITE t SELECT * FROM t`), which would destroy hidden rows
    /// and persist masked values; committing cannot tell, so it fails closed
    /// for any restricted grant.
    pub(crate) async fn authorize_unrestricted_write(&self) -> Result<()> {
        self.authorize_unrestricted_read().await.map(|_| ())
    }

    /// Fail closed when a read reaches `TableRead::to_arrow` without a grant
    /// stamped on its splits (an unauthorized path) and the table is
    /// `query-auth.enabled`; a no-op otherwise.
    pub(crate) fn ensure_read_without_grant(&self) -> Result<()> {
        CoreOptions::new(self.schema.options()).ensure_read_authorized()
    }

    /// Get the table's identifier.
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }

    /// Get the table's location.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Get the table's schema.
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Get the FileIO instance for this table.
    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Get the SchemaManager for this table.
    pub fn schema_manager(&self) -> &SchemaManager {
        &self.schema_manager
    }

    pub fn branch(&self) -> &str {
        &self.branch
    }

    pub fn is_main_branch(&self) -> bool {
        self.branch == DEFAULT_MAIN_BRANCH
    }

    pub fn is_branch_reference(&self) -> bool {
        self.branch_reference
    }

    pub(crate) fn ensure_not_branch_reference_for_write(&self) -> Result<()> {
        if self.is_branch_reference() {
            Err(crate::Error::Unsupported {
                message: format!(
                    "Writing to Paimon branch '{}' is not supported",
                    self.branch()
                ),
            })
        } else {
            Ok(())
        }
    }

    pub fn snapshot_manager(&self) -> SnapshotManager {
        let manager = SnapshotManager::new(self.file_io.clone(), self.location.clone());
        if self.is_main_branch() {
            manager
        } else {
            manager.with_branch(&self.branch)
        }
    }

    pub fn tag_manager(&self) -> TagManager {
        let manager = TagManager::new(self.file_io.clone(), self.location.clone());
        if self.is_main_branch() {
            manager
        } else {
            manager.with_branch(&self.branch)
        }
    }

    /// Get the REST environment, if this table was loaded from a REST catalog.
    pub fn rest_env(&self) -> Option<&RESTEnv> {
        self.rest_env.as_ref()
    }

    pub(crate) fn is_format_table(&self) -> bool {
        CoreOptions::new(self.schema.options()).is_format_table()
    }

    /// Create a read builder for scan/read.
    ///
    /// Reference: [pypaimon FileStoreTable.new_read_builder](https://github.com/apache/paimon/blob/release-1.3/paimon-python/pypaimon/table/file_store_table.py).
    pub fn new_read_builder(&self) -> ReadBuilder<'_> {
        ReadBuilder::new(self)
    }

    /// Create a full-text search builder.
    ///
    /// Reference: [FullTextSearchBuilderImpl](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/FullTextSearchBuilderImpl.java)
    #[cfg(feature = "fulltext")]
    pub fn new_full_text_search_builder(&self) -> FullTextSearchBuilder<'_> {
        FullTextSearchBuilder::new(self)
    }

    /// Create a hybrid search builder.
    ///
    /// Reference: [HybridSearchBuilderImpl](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/HybridSearchBuilderImpl.java)
    pub fn new_hybrid_search_builder(&self) -> HybridSearchBuilder<'_> {
        HybridSearchBuilder::new(self)
    }

    pub fn new_vector_search_builder(&self) -> VectorSearchBuilder<'_> {
        VectorSearchBuilder::new(self)
    }

    pub fn new_batch_vector_search_builder(&self) -> BatchVectorSearchBuilder<'_> {
        BatchVectorSearchBuilder::new(self)
    }

    pub fn new_lumina_index_build_builder(&self) -> LuminaIndexBuildBuilder<'_> {
        LuminaIndexBuildBuilder::new(self)
    }

    pub fn new_btree_global_index_build_builder(&self) -> BTreeGlobalIndexBuildBuilder<'_> {
        BTreeGlobalIndexBuildBuilder::new(self)
    }

    pub fn new_global_index_drop_builder(&self) -> GlobalIndexDropBuilder<'_> {
        GlobalIndexDropBuilder::new(self)
    }

    pub fn new_vindex_index_build_builder(&self, index_type: &str) -> VindexIndexBuildBuilder<'_> {
        VindexIndexBuildBuilder::new(self, index_type)
    }

    /// Create a write builder for write/commit.
    ///
    /// Reference: [pypaimon FileStoreTable.new_write_builder](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/table/file_store_table.py).
    pub fn new_write_builder(&self) -> WriteBuilder<'_> {
        WriteBuilder::new(self)
    }

    /// Create a copy of this table with extra options merged into the schema.
    ///
    /// This never switches the schema version; it corresponds to Java
    /// `FileStoreTable.copyWithoutTimeTravel`. Use
    /// [`Table::copy_with_time_travel`] when the options may select a
    /// historical snapshot whose schema should be used for reading.
    pub fn copy_with_options(&self, extra: HashMap<String, String>) -> Self {
        // Changing the time-travel selector invalidates the resolved snapshot
        // (a time-travelled schema then has no matching snapshot anymore, and
        // scans of such a copy fail until `copy_with_time_travel` re-resolves
        // it). Unrelated options keep the snapshot/schema pair intact.
        let selector_changed = extra.keys().any(|k| {
            k == crate::spec::SCAN_VERSION_OPTION
                || k == crate::spec::SCAN_TIMESTAMP_MILLIS_OPTION
                || k == crate::spec::SCAN_SNAPSHOT_ID_OPTION
                || k == crate::spec::SCAN_TAG_NAME_OPTION
        });
        Self {
            file_io: self.file_io.clone(),
            identifier: self.identifier.clone(),
            location: self.location.clone(),
            schema: self.schema.copy_with_options(extra),
            schema_manager: self.schema_manager.clone(),
            branch: self.branch.clone(),
            branch_reference: self.branch_reference,
            rest_env: self.rest_env.clone(),
            time_traveled: self.time_traveled,
            travel_snapshot: if selector_changed {
                None
            } else {
                self.travel_snapshot.clone()
            },
        }
    }

    /// Create a copy of this table with extra options merged in, switching to
    /// the schema of the time-travelled snapshot when the merged options
    /// select one.
    ///
    /// Mirrors Java `AbstractFileStoreTable.copy(dynamicOptions)` →
    /// `tryTimeTravel`: if the merged options contain a time-travel selector
    /// (`scan.version` / `scan.timestamp-millis` / `scan.snapshot-id` /
    /// `scan.tag-name`) that resolves to a snapshot, the table's fields and
    /// keys come from that snapshot's schema while the options stay the merged
    /// ones (Java `TableSchema.copy(newOptions)`).
    /// Like Java, resolution failures fall back silently to the current
    /// schema (the `if let Ok` below swallows them); an invalid selector
    /// still fails later at scan planning.
    pub async fn copy_with_time_travel(&self, extra: HashMap<String, String>) -> Result<Self> {
        let mut table = self.copy_with_options(extra);
        // Reject unimplemented scan options on the merged view before any IO, so
        // both table-level and per-read options are covered.
        CoreOptions::new(table.schema().options()).validate_scan_options()?;
        // travel_to_snapshot returns Ok(None) without IO when the merged
        // options contain no selector.
        if let Ok(Some(snapshot)) = time_travel::travel_to_snapshot(
            &table.snapshot_manager(),
            &table.tag_manager(),
            table.schema.options(),
        )
        .await
        {
            if snapshot.schema_id() != table.schema.id() {
                let snapshot_schema = table.schema_manager.schema(snapshot.schema_id()).await?;
                table.schema =
                    snapshot_schema.copy_with_replaced_options(table.schema.options().clone());
                table.time_traveled = true;
            }
            table.travel_snapshot = Some(snapshot);
        }
        Ok(table)
    }

    pub async fn copy_with_branch(&self, branch_name: &str) -> Result<Self> {
        let branch = if branch_name.trim().is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Branch name cannot be empty.".to_string(),
                source: None,
            });
        } else {
            validate_branch_name(branch_name)?;
            branch_name.to_string()
        };
        let schema_manager = if branch == DEFAULT_MAIN_BRANCH {
            SchemaManager::new(self.file_io.clone(), self.location.clone())
        } else {
            SchemaManager::new(self.file_io.clone(), self.location.clone()).with_branch(&branch)
        };
        let schema = schema_manager
            .latest()
            .await?
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("Branch '{branch}' does not exist."),
                source: None,
            })?;
        let mut options = schema.options().clone();
        options.insert("branch".to_string(), branch.clone());
        // The flag comes from the REST table response, not the branch's on-disk
        // schema. A branch references the same files, so dropping it would make
        // `t$branch_x` read them unauthorized.
        let branch_schema = if CoreOptions::new(self.schema.options()).query_auth_enabled() {
            schema
                .copy_with_replaced_options(options)
                .copy_with_query_auth_enabled()
        } else {
            schema.copy_with_replaced_options(options)
        };
        Ok(Self {
            file_io: self.file_io.clone(),
            identifier: self.identifier.clone(),
            location: self.location.clone(),
            schema: branch_schema,
            schema_manager,
            branch,
            branch_reference: true,
            rest_env: self.rest_env.clone(),
            time_traveled: false,
            travel_snapshot: None,
        })
    }

    /// Whether this table copy reads a historical snapshot with its
    /// historical schema (see [`Table::copy_with_time_travel`]).
    pub fn is_time_traveled(&self) -> bool {
        self.time_traveled
    }

    /// Whether a time-travel selector in this copy's options resolved to a
    /// snapshot. Lets external callers (e.g. the Python binding) distinguish
    /// "selector set but unresolved" (silent fallback to latest) from a real
    /// travelled read, so they can reject the former instead of reading latest.
    pub fn has_resolved_travel_snapshot(&self) -> bool {
        self.travel_snapshot.is_some()
    }

    /// The snapshot resolved by [`Table::copy_with_time_travel`] from this
    /// copy's options, if any. Lets scans skip re-resolving the selector.
    pub fn travel_snapshot(&self) -> Option<&Snapshot> {
        self.travel_snapshot.as_ref()
    }
}

/// A stream of arrow [`RecordBatch`]es.
pub type ArrowRecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

pub(crate) fn find_field_id_by_name(fields: &[DataField], name: &str) -> Option<i32> {
    fields.iter().find(|f| f.name() == name).map(|f| f.id())
}

/// A minimal table with `query-auth.enabled = true`, for the fail-closed read guard.
#[cfg(test)]
pub(crate) fn query_auth_table() -> Table {
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataType, IntType, Schema, TableSchema};

    let file_io = FileIOBuilder::new("file").build().unwrap();
    let table_schema = TableSchema::new(
        0,
        &Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .option("query-auth.enabled", "true")
            .build()
            .unwrap(),
    );
    Table::new(
        file_io,
        Identifier::new("default", "auth_t"),
        "/tmp/test-query-auth-table".to_string(),
        table_schema,
        None,
    )
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_authorize_unrestricted_read_fails_closed() {
        // Every write/build path authorizes through this gate before its
        // internal read, so a table it cannot authorize as unrestricted must
        // fail closed rather than commit a filtered/masked view.
        let table = super::query_auth_table();
        let err = table.authorize_unrestricted_read().await.unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
            "write-path authorization must fail closed, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_latest_schema_check_tolerates_an_unlistable_location() {
        use crate::catalog::Identifier;
        use crate::io::FileIOBuilder;
        use crate::spec::{DataType, IntType, Schema, TableSchema};

        // The check is the only file access on the authorization path. A table
        // served entirely over REST — or a mock catalog — may point at a
        // location with no schema directory, and that must not fail the read.
        let table = super::Table::new(
            FileIOBuilder::new("file").build().unwrap(),
            Identifier::new("default", "t"),
            "/nonexistent-a1b2c3/does/not/exist".to_string(),
            TableSchema::new(
                0,
                &Schema::builder()
                    .column("id", DataType::Int(IntType::new()))
                    .build()
                    .unwrap(),
            ),
            None,
        );
        assert!(
            table
                .ensure_rules_bind_to_latest_schema(&[], &[], None)
                .await
                .is_ok(),
            "an unlistable location must not fail authorization"
        );
    }

    #[tokio::test]
    async fn test_authorize_rewrite_splits_rejects_foreign_splits() {
        use crate::spec::BinaryRow;
        use crate::table::DataSplitBuilder;

        let table = super::query_auth_table();
        let root = table.location().to_string();
        let split_at = |path: &str| {
            DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path(path.to_string())
                .with_total_buckets(1)
                .with_data_files(vec![])
                .build()
                .unwrap()
        };

        // A sibling sharing this location's prefix must not pass.
        let sibling = format!("{root}2/bucket-0");
        let err = table
            .authorize_rewrite_splits(vec![split_at(&sibling)])
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("does not belong to table")),
            "prefix-sharing sibling must be rejected, got: {err}"
        );

        // Its own split passes provenance and fails later, at authorization.
        let own = format!("{root}/bucket-0");
        let err = table
            .authorize_rewrite_splits(vec![split_at(&own)])
            .await
            .unwrap_err();
        assert!(
            !err.to_string().contains("does not belong to table"),
            "own split must pass the provenance check, got: {err}"
        );
    }

    // `copy_with_branch` lists the schema directory, and opendal's fs lister
    // panics on Windows when stripping the prefix of a `file:/C:/…` path.
    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_branch_copy_inherits_query_auth_flag() {
        use crate::catalog::Identifier;
        use crate::io::FileIOBuilder;
        use crate::spec::{CoreOptions, DataType, IntType, Schema, TableSchema};

        // The flag arrives on the REST table response, not the branch's
        // on-disk schema; if `copy_with_branch` dropped it, `t$branch_x` would
        // read the same files unauthorized. Drive the real method.
        let tmp = tempfile::tempdir().unwrap();
        // `file:` URL with forward slashes; a bare path breaks on Windows.
        let location = {
            let p = tmp.path().to_string_lossy().replace('\\', "/");
            if p.starts_with('/') {
                format!("file:{p}")
            } else {
                format!("file:/{p}")
            }
        };
        let file_io = FileIOBuilder::new("file").build().unwrap();

        // The on-disk schema deliberately has NO query-auth option.
        let on_disk = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        let sm = super::SchemaManager::new(file_io.clone(), location.clone());
        file_io
            .new_output(&sm.schema_path(0))
            .unwrap()
            .write(serde_json::to_vec(&on_disk).unwrap().into())
            .await
            .unwrap();

        let build = |schema: TableSchema| {
            super::Table::new(
                file_io.clone(),
                Identifier::new("default", "auth_t"),
                location.clone(),
                schema,
                None,
            )
        };

        let plain = build(on_disk.clone());
        assert!(
            !CoreOptions::new(
                plain
                    .copy_with_branch(super::DEFAULT_MAIN_BRANCH)
                    .await
                    .unwrap()
                    .schema()
                    .options()
            )
            .query_auth_enabled(),
            "a branch of a non-query-auth table must not gain the flag"
        );

        let guarded = build(on_disk.copy_with_query_auth_enabled());
        assert!(
            CoreOptions::new(
                guarded
                    .copy_with_branch(super::DEFAULT_MAIN_BRANCH)
                    .await
                    .unwrap()
                    .schema()
                    .options()
            )
            .query_auth_enabled(),
            "the branch copy must inherit query-auth.enabled from the REST table"
        );
    }
}
