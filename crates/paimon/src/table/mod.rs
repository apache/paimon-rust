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
mod bitmap_global_index_format;
mod bitmap_global_index_reader;
mod bitmap_global_index_writer;
mod blob_resolver;
mod branch_manager;
mod bucket_assigner;
mod bucket_assigner_constant;
mod bucket_assigner_cross;
mod bucket_assigner_dynamic;
mod bucket_assigner_fixed;
mod bucket_filter;
mod bucket_function;
mod commit_message;
mod consumer_manager;
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
pub(crate) mod index_file_path;
mod kv_file_reader;
mod kv_file_writer;
mod lumina_index_build_builder;
pub(crate) mod merge_tree_split_generator;
mod object_table;
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
mod pk_vector_bucket_split;
mod pk_vector_data_file_reader;
mod pk_vector_indexed_split_read;
mod pk_vector_orchestrator;
mod pk_vector_position_read;
mod pk_vector_scan;
mod postpone_bucket_plan;
mod postpone_file_writer;
mod postpone_fixed_bucket_router;
mod postpone_fixed_bucket_write;
mod postpone_fixed_bucket_write_builder;
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
mod sorted_global_index_build_builder;
mod sorted_global_index_options;
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
pub use blob_resolver::{BlobReader, BlobStream};
pub use branch_manager::BranchManager;
pub use commit_message::CommitMessage;
pub use consumer_manager::ConsumerManager;
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
pub use object_table::{ObjectEntry, ObjectTable};
pub use partition_stat::PartitionStat;
pub use pk_vector_bucket_split::{BucketVectorPayload, BucketVectorSearchSplit};
pub use postpone_bucket_plan::{PostponeBucketPlan, POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD};
pub use postpone_fixed_bucket_write::{
    PostponeFixedBucketTableCommit, PostponeFixedBucketTableWrite,
};
pub use postpone_fixed_bucket_write_builder::PostponeFixedBucketWriteBuilder;
pub use read_builder::ReadBuilder;
pub use rest_env::RESTEnv;
pub use scan_trace::ScanTrace;
pub use schema_manager::SchemaManager;
pub use snapshot_commit::{RESTSnapshotCommit, RenamingSnapshotCommit, SnapshotCommit};
pub use snapshot_manager::SnapshotManager;
pub use sorted_global_index_build_builder::{
    BTreeGlobalIndexBuildBuilder, SortedGlobalIndexBuildBuilder,
};
pub use source::{
    merge_row_ranges, DataSplit, DataSplitBuilder, DeletionFile, PartitionBucket, Plan, RowRange,
};
pub use table_commit::TableCommit;
pub use table_read::TableRead;
pub use table_scan::TableScan;
pub use table_update::TableUpdate;
pub use table_write::TableWrite;
pub use tag_manager::TagManager;
pub use vector_search_builder::{
    BatchVectorSearchBuilder, PreparedVectorSearchFilter, VectorSearchBuilder,
};
pub use vindex_index_build_builder::VindexIndexBuildBuilder;
pub use write_builder::WriteBuilder;

use crate::catalog::{validate_branch_name, Identifier, DEFAULT_MAIN_BRANCH};
use crate::io::FileIO;
use crate::spec::{
    CoreOptions, DataField, Snapshot, TableSchema, SCAN_SNAPSHOT_ID_OPTION, SCAN_TAG_NAME_OPTION,
    SCAN_TIMESTAMP_MILLIS_OPTION, SCAN_VERSION_OPTION, SCAN_WATERMARK_OPTION,
};
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

    /// Authorize this user against the REST server when `query-auth.enabled` is
    /// set: fetch and parse the per-user row filter / column masking and return
    /// it as the grant the read pipeline enforces. `select` are the queried
    /// columns (table-schema indices; `None` = all) — like Java's
    /// `readType.getFieldNames()`, so a column-restricted user can still read
    /// an authorized subset; the grant is scoped to exactly those columns and a
    /// wider read fails closed until it re-plans. Returns `None` when the table
    /// is not `query-auth.enabled`. Called from scan planning and search
    /// execution at the same per-plan frequency as Java's
    /// `CatalogEnvironment.tableQueryAuth()`, so a revoked grant takes effect on
    /// the next plan. The grant is threaded to the read on the split it plans
    /// (never a shared mutable slot), so it is per-query and cannot leak into a
    /// concurrent query or a write-path rewrite.
    pub(crate) async fn verify_query_auth_for_read(
        &self,
        select: Option<&HashSet<usize>>,
        system_select: &[String],
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
        // refuse them.
        let select_names = select.map(|indices| {
            fields
                .iter()
                .enumerate()
                .filter(|(i, _)| indices.contains(i))
                .map(|(_, f)| f.name().to_string())
                .chain(system_select.iter().cloned())
                .collect::<Vec<_>>()
        });
        let auth = rest_env.table_query_auth(select_names).await?;
        let filters = query_auth::parse_auth_filters(&auth.filter.unwrap_or_default(), fields)?;
        let masks =
            query_auth::parse_column_masking(&auth.column_masking.unwrap_or_default(), fields)?;
        let grant = QueryAuthGrant::new(
            filters,
            masks,
            select.cloned(),
            query_auth::GrantBinding::of(self),
        );

        // The server expresses its rules against the table's CURRENT schema, but
        // a time-travelled or branch copy reads a DIFFERENT one. Binding those
        // rules by name onto it is unsound — a column dropped and re-added under
        // the same name has a different field id, so a filter or mask would bind
        // to an unrelated historical column. Fail closed rather than mis-bind.
        if !grant.is_unrestricted() && (self.time_traveled || self.branch_reference) {
            return Err(crate::Error::Unsupported {
                message: "a query-auth row filter / column masking grant cannot be applied to a \
                          time-travelled or branch read: the grant is bound to the table's \
                          current schema"
                    .to_string(),
            });
        }

        Ok(Some(Arc::new(grant)))
    }

    /// Authorize a read that cannot enforce a row filter / masking on its output
    /// (search, system tables, or a write-path rewrite that must read raw).
    /// Returns the grant to stamp on the splits it reads (`None` = not a
    /// query-auth table). Fails closed when the grant is restricted — such a
    /// path must never run under a partial grant, since it would either leak
    /// (search/metadata) or silently drop/mask rows into a committed rewrite.
    pub(crate) async fn authorize_unrestricted_read(&self) -> Result<Option<Arc<QueryAuthGrant>>> {
        match self.verify_query_auth_for_read(None, &[]).await? {
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
    /// Such a read must see raw rows — rewriting from a filtered or masked view
    /// would destroy hidden rows and persist masked values — so it requires a
    /// fully unrestricted grant and fails closed otherwise. The grant the caller
    /// would get from scan planning must NOT be used here: under a row filter it
    /// shifts row offsets, and rewrites replay those offsets.
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
        let grant = self.authorize_unrestricted_read().await?;
        Ok(splits
            .into_iter()
            .map(|split| split.with_query_auth_grant(grant.clone()))
            .collect())
    }

    /// Authorize a commit. Writes must require a fully unrestricted grant: the
    /// data being committed may have come from an enforced read (e.g. `INSERT
    /// OVERWRITE t SELECT * FROM t`), which would destroy rows the grant hid and
    /// persist masked values as the stored data. Committing cannot tell where
    /// its rows came from, so it fails closed for any restricted grant.
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

    pub fn consumer_manager(&self) -> ConsumerManager {
        let manager = ConsumerManager::new(self.file_io.clone(), self.location.clone());
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

    pub fn new_sorted_global_index_build_builder(&self) -> SortedGlobalIndexBuildBuilder<'_> {
        SortedGlobalIndexBuildBuilder::new(self)
    }

    pub fn new_btree_global_index_build_builder(&self) -> BTreeGlobalIndexBuildBuilder<'_> {
        self.new_sorted_global_index_build_builder()
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

    /// Create a one-shot fixed-bucket builder for a postpone table.
    pub fn new_postpone_fixed_bucket_write_builder(
        &self,
    ) -> Result<PostponeFixedBucketWriteBuilder<'_>> {
        PostponeFixedBucketWriteBuilder::new(self)
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
                || k == crate::spec::SCAN_WATERMARK_OPTION
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

    /// Create a read-only copy pinned to an already resolved snapshot.
    ///
    /// Replaces any selector that originally resolved the snapshot with an
    /// explicit `scan.snapshot-id`, so every subsequent scan stage observes the
    /// same snapshot. The snapshot's schema is loaded when it differs from the
    /// current table schema.
    pub(crate) async fn copy_with_resolved_snapshot(&self, snapshot: &Snapshot) -> Result<Self> {
        let mut options = self.schema.options().clone();
        for selector in [
            SCAN_TIMESTAMP_MILLIS_OPTION,
            SCAN_WATERMARK_OPTION,
            SCAN_VERSION_OPTION,
            SCAN_SNAPSHOT_ID_OPTION,
            SCAN_TAG_NAME_OPTION,
        ] {
            options.remove(selector);
        }
        options.insert(
            SCAN_SNAPSHOT_ID_OPTION.to_string(),
            snapshot.id().to_string(),
        );

        let schema = if snapshot.schema_id() == self.schema.id() {
            self.schema.copy_with_replaced_options(options)
        } else {
            self.schema_manager
                .schema(snapshot.schema_id())
                .await?
                .copy_with_replaced_options(options)
        };
        Ok(Self {
            file_io: self.file_io.clone(),
            identifier: self.identifier.clone(),
            location: self.location.clone(),
            schema,
            schema_manager: self.schema_manager.clone(),
            branch: self.branch.clone(),
            branch_reference: self.branch_reference,
            rest_env: self.rest_env.clone(),
            time_traveled: true,
            travel_snapshot: Some(snapshot.clone()),
        })
    }

    /// Create a copy of this table with extra options merged in, switching to
    /// the schema of the time-travelled snapshot when the merged options
    /// select one.
    ///
    /// Mirrors Java `AbstractFileStoreTable.copy(dynamicOptions)` →
    /// `tryTimeTravel`: if the merged options contain a time-travel selector
    /// (`scan.version` / `scan.timestamp-millis` / `scan.watermark` /
    /// `scan.snapshot-id` / `scan.tag-name`) that resolves to a snapshot, the
    /// table's fields and keys come from that snapshot's schema while the
    /// options stay the merged ones (Java `TableSchema.copy(newOptions)`).
    /// Like Java, resolution failures fall back silently to the current
    /// schema (the `if let Ok` below swallows them); an invalid selector
    /// still fails later at scan planning.
    pub async fn copy_with_time_travel(&self, extra: HashMap<String, String>) -> Result<Self> {
        self.copy_with_time_travel_mode(extra, false).await
    }

    /// Like [`Self::copy_with_time_travel`], but propagates selector resolution
    /// failures. Services should use this variant so a missing or unreadable
    /// snapshot cannot silently fall back to the current schema.
    pub async fn copy_with_time_travel_strict(
        &self,
        extra: HashMap<String, String>,
    ) -> Result<Self> {
        self.copy_with_time_travel_mode(extra, true).await
    }

    async fn copy_with_time_travel_mode(
        &self,
        extra: HashMap<String, String>,
        strict: bool,
    ) -> Result<Self> {
        // Resolution reads Paimon snapshot paths, so refuse before any IO.
        CoreOptions::new(self.schema.options())
            .ensure_type_paimon_served(&self.identifier.full_name())?;
        let mut table = self.copy_with_options(extra);
        // Reject unimplemented scan options on the merged view before any IO, so
        // both table-level and per-read options are covered.
        CoreOptions::new(table.schema().options()).validate_scan_options()?;
        // travel_to_snapshot returns Ok(None) without IO when the merged
        // options contain no selector.
        let resolved = time_travel::travel_to_snapshot(
            &table.snapshot_manager(),
            &table.tag_manager(),
            table.schema.options(),
        )
        .await;
        let snapshot = if strict {
            resolved?
        } else {
            resolved.ok().flatten()
        };
        if let Some(snapshot) = snapshot {
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
        // The branch schema replaces this one wholesale and could drop the
        // declared type, so refuse before any branch I/O.
        CoreOptions::new(self.schema.options())
            .ensure_type_paimon_served(&self.identifier.full_name())?;
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
        // `query-auth.enabled` is delivered by the REST catalog on the table
        // response, so the branch's on-disk schema need not carry it. Inherit it
        // from this table: a branch references the same data files, and dropping
        // the flag here would make `t$branch_x` read them raw, unauthorized.
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
    use std::collections::HashMap;

    use super::Table;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataType, IntType, Schema, TableSchema};

    /// The value every reader and writer of a bucket-local index file consults.
    ///
    /// Each consumer resolves its own path from it — deletion vectors in
    /// `table_scan` and `data_evolution_writer`, primary-key ANN segments in
    /// `pk_vector_scan`, full-text archives, the dynamic-bucket hash index, and
    /// `TableCommit::abort` — and each of those resolutions is covered where it
    /// lives. What a copy must not do is hand them a different value than the one
    /// the files on disk were written under.
    #[test]
    fn a_dynamic_copy_reads_the_stored_index_layout() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let stored = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .option("index-file-in-data-file-dir", "true")
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "t"),
            "memory:/t".to_string(),
            stored,
            None,
        );

        let copied = table.copy_with_options(HashMap::from([(
            "index-file-in-data-file-dir".to_string(),
            "false".to_string(),
        )]));
        assert!(
            copied.schema().core_options().index_file_in_data_file_dir(),
            "a read through a copied table must resolve index files under the stored layout"
        );

        // The copy is otherwise a normal copy.
        let copied = table.copy_with_options(HashMap::from([(
            "scan.snapshot-id".to_string(),
            "3".to_string(),
        )]));
        assert_eq!(
            copied.schema().options().get("scan.snapshot-id"),
            Some(&"3".to_string())
        );
    }

    #[tokio::test]
    async fn test_authorize_unrestricted_read_fails_closed() {
        // Every write/build path (cow_writer, data_evolution_writer, btree index
        // build, cross-partition bucket assign) authorizes through this gate
        // before its internal read. A query-auth table it cannot authorize as
        // unrestricted must fail closed rather than read raw and rewrite
        // filtered/masked data into a committed result.
        let table = super::query_auth_table();
        let err = table.authorize_unrestricted_read().await.unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
            "write-path authorization must fail closed, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_branch_copy_inherits_query_auth_flag() {
        use crate::catalog::Identifier;
        use crate::io::FileIOBuilder;
        use crate::spec::{CoreOptions, DataType, IntType, Schema, TableSchema};

        // The flag arrives on the REST table response, not the branch's
        // on-disk schema; if `copy_with_branch` dropped it, `t$branch_x` would
        // read the same files unauthorized. Drive the real method.
        let tmp = tempfile::tempdir().unwrap();
        let location = tmp.path().display().to_string();
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
