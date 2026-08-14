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
mod format_partition;
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
mod query_auth;
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
use std::collections::HashMap;

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
    /// Minted only by [`RESTEnv::build_table`], so a handle assembled with the
    /// public [`Table::new`] cannot replay a grant.
    query_auth_session: Option<u64>,
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
            query_auth_session: None,
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
            query_auth_session: None,
            rest_env: None,
            time_traveled: false,
            travel_snapshot: None,
        })
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

    /// The live counterpart of [`CoreOptions::ensure_read_authorized`], which
    /// reads the schema this handle was loaded with. Paths that can await but
    /// cannot apply the server's rules must ask instead.
    pub(crate) async fn ensure_read_authorized_live(&self, path: &str) -> Result<()> {
        let local = CoreOptions::new(self.schema.options());
        local.ensure_type_paimon_served(&self.identifier.full_name())?;
        if self.server_query_auth_enabled().await? {
            return Err(query_auth::unsupported(&format!(
                "{path} reads index files directly and cannot apply a row filter or column masking"
            )));
        }
        Ok(())
    }

    /// Whether the server says this table is `query-auth.enabled` right now: the
    /// handle's schema is a snapshot, and a cached `false` would skip the check.
    pub(crate) async fn server_query_auth_enabled(&self) -> Result<bool> {
        let local = CoreOptions::new(self.schema.options()).query_auth_enabled();
        let Some(rest_env) = &self.rest_env else {
            return Ok(local);
        };
        // Only ever strengthens: the name can be re-created over this handle's
        // files, so the answer may be about a different table.
        if local {
            return Ok(true);
        }
        match rest_env.current_table().await?.schema.as_ref() {
            Some(schema) => Ok(CoreOptions::new(schema.options()).query_auth_enabled()),
            None => Ok(true),
        }
    }

    /// Whether this user may read this table; `None` when it is not
    /// `query-auth.enabled`. `server_query_auth` is the caller's already-fetched
    /// [`Self::server_query_auth_enabled`], so planning asks the server once.
    pub(crate) async fn authorize_read(
        &self,
        server_query_auth: bool,
    ) -> Result<Option<std::sync::Arc<query_auth::QueryAuthGrant>>> {
        let local = CoreOptions::new(self.schema.options());
        // Ask the selector too: `copy_with_options` adds one without the flag.
        let travels = local.try_time_travel_selector()?.is_some();
        // A `$branch_x` or `$files` handle authorizes against the decorated
        // name while its managers read the base table's own files.
        let decorated = self.identifier.branch_name()?.is_some()
            || self.identifier.system_table_name()?.is_some();
        if (travels || self.time_traveled || self.branch_reference || decorated)
            && local.query_auth_enabled()
        {
            return Err(query_auth::unsupported(
                "a time-travelled or branch read authorizes against the table's current schema, \
                 which is not the one it reads",
            ));
        }

        let Some(rest_env) = &self.rest_env else {
            // Only a REST catalog can authorize.
            return if local.query_auth_enabled() {
                Err(query_auth::unsupported(
                    "it requires a REST catalog to authorize the query",
                ))
            } else {
                Ok(None)
            };
        };

        // No freshness assertion yet — an ordinary table must not inherit one.
        if !server_query_auth {
            return Ok(None);
        }
        if travels || self.time_traveled || self.branch_reference || decorated {
            return Err(query_auth::unsupported(
                "a time-travelled or branch read authorizes against the table's current schema, \
                 which is not the one it reads",
            ));
        }

        // Before any RPC: only the catalog mints a session, so a handle the
        // caller assembled stops here whatever name or files it wears.
        let session = self.query_auth_session.ok_or_else(|| {
            query_auth::unsupported("this table handle was assembled rather than loaded")
        })?;

        // Naming a system column here would fail the server's column check.
        let response = rest_env
            .table_query_auth(&self.branch, self.schema.id(), None)
            .await?;
        Ok(Some(std::sync::Arc::new(query_auth::QueryAuthGrant::new(
            response, session,
        ))))
    }

    /// Handed out once per catalog-loaded table; wraps only after 2^64 loads.
    pub(crate) fn with_query_auth_session(mut self) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        self.query_auth_session = Some(NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        self
    }

    pub(crate) fn query_auth_session(&self) -> Option<u64> {
        self.query_auth_session
    }

    /// Get the REST environment, if this table was loaded from a REST catalog.
    pub fn rest_env(&self) -> Option<&RESTEnv> {
        self.rest_env.as_ref()
    }

    pub(crate) fn is_format_table(&self) -> bool {
        CoreOptions::new(self.schema.options()).is_format_table()
    }

    /// Whether this table uses catalog-managed Format Table partitions: a Format Table loaded
    /// from a REST catalog with `metastore.partitioned-table=true`.
    pub fn has_catalog_managed_partitions(&self) -> bool {
        let options = CoreOptions::new(self.schema.options());
        self.rest_env.is_some()
            && options.is_format_table()
            && options.partitioned_table_in_metastore()
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
            query_auth_session: self.query_auth_session,
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
            query_auth_session: self.query_auth_session,
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

    /// Refuse dynamic options that would change where a Format Table loaded from a REST catalog
    /// takes its partitions from, or, when the catalog manages them, how they are read.
    ///
    /// Mirrors Java `FormatTable.copy`. Java also fixes a Format Table's type, location and
    /// format when the table is loaded; this table reads them from its options, so changing
    /// them is refused here too.
    fn ensure_format_table_partition_options_unchanged(
        &self,
        extra: &HashMap<String, String>,
    ) -> Result<()> {
        let current = CoreOptions::new(self.schema.options());
        if self.rest_env.is_none() || !current.is_format_table() {
            return Ok(());
        }
        let mut merged_options = self.schema.options().clone();
        merged_options.extend(
            extra
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        let merged = CoreOptions::new(&merged_options);
        let managed = current.partitioned_table_in_metastore();
        let changed = if merged.partitioned_table_in_metastore() != managed {
            Some("metastore.partitioned-table")
        } else if !managed {
            None
        } else if !merged.is_format_table() {
            Some("type")
        } else if merged.format_table_partition_only_value_in_path()
            != current.format_table_partition_only_value_in_path()
        {
            Some("format-table.partition-path-only-value")
        } else if merged.format_table_implementation_is_engine() {
            Some("format-table.implementation")
        } else if merged.path() != current.path() {
            Some("path")
        } else if merged.file_format() != current.file_format() {
            Some("file.format")
        } else {
            None
        };
        match changed {
            Some(key) => Err(crate::Error::DataInvalid {
                message: format!(
                    "Dynamic option '{key}' cannot change where Format Table {} takes its \
                     partitions from, or how it reads them",
                    self.identifier.full_name()
                ),
                source: None,
            }),
            None => Ok(()),
        }
    }

    async fn copy_with_time_travel_mode(
        &self,
        extra: HashMap<String, String>,
        strict: bool,
    ) -> Result<Self> {
        // Resolution reads Paimon snapshot paths, so refuse before any IO.
        CoreOptions::new(self.schema.options())
            .ensure_type_paimon_served(&self.identifier.full_name())?;
        self.ensure_format_table_partition_options_unchanged(&extra)?;
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
        Ok(Self {
            file_io: self.file_io.clone(),
            identifier: self.identifier.clone(),
            location: self.location.clone(),
            schema: schema.copy_with_replaced_options(options),
            schema_manager,
            branch,
            branch_reference: true,
            query_auth_session: self.query_auth_session,
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

/// A `query-auth.enabled` table wired to its own REST session.
#[cfg(test)]
pub(crate) async fn rest_query_auth_table() -> Table {
    use crate::api::rest_api::RESTApi;
    use crate::common::{CatalogOptions, Options};

    let mut options = Options::default();
    options.set(CatalogOptions::URI, "http://127.0.0.1:1");
    options.set("token.provider", "bear");
    options.set("token", "test_token");
    let api = std::sync::Arc::new(RESTApi::new(options.clone(), false).await.unwrap());
    let table = query_auth_table();
    Table {
        rest_env: Some(RESTEnv::new(
            table.identifier.clone(),
            "uuid-1".to_string(),
            api,
            options,
            false,
            None,
        )),
        ..table
    }
    .with_query_auth_session()
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
}
